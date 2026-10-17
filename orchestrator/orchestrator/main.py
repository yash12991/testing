"""Main loop: wires Observer, Diagnoser, Approver, Fixer, and Verifier together
through Postgres state, as the plan's architecture requires ("Stages never
call each other directly — each reads the incident's existing rows in
Postgres and, if its own stage's row is missing, computes it and appends
it.").

`run_once` never passes a `Diagnosis`/`ApprovalDecision` object straight from
one stage's computation into the next stage's call in the same Python
expression. Instead, after appending a stage's event, it re-reads the
incident's events from Postgres and reconstructs the typed object a later
stage needs from the *persisted* payload (`_diagnosis_from_payload`,
`_decision_from_payload`). This is what makes the loop resumable/replayable
across separate `run_once` calls (and separate orchestrator process
restarts) rather than only within a single call's local variables.

Design decision — advancing more than one stage per call:
The brief's literal wording is "append exactly the one next missing stage"
per incident per call. Taken fully literally, that reading cannot satisfy
this task's own pinned integration test: with one stage appended per call,
after the second `run_once` call an incident would have at most
`[anomaly_detected, diagnosis, approval]` (the first call's `observer.poll_once`
creates `anomaly_detected`, then the first call's single stage-advance adds
`diagnosis`, then the second call's single stage-advance adds `approval`) —
`fix_executed` would not exist yet, contradicting the test's
`assert "fix_executed" in stages` after the second call.

So `_advance_incident` instead cascades through `diagnosis`, `approval`, and
`fix_executed` eagerly within one call once an incident is found incomplete,
since those are pure/fast, synchronous computations with no reason to wait a
whole poll interval between them once the anomaly is confirmed. `verification`
is deliberately held back to a *later* call: it is only attempted once
`fix_executed` was already present *before* this call's own processing of the
incident began, never for a `fix_executed` row this very call just wrote. This
guarantees the fix gets at least one full `POLL_INTERVAL_SECONDS` gap (plus
Verifier's own `VERIFY_BACKOFF_SECONDS` wait) to take effect before recovery is
checked — the real reason `verifier.verify()` has a backoff wait at all — and
it is exactly what keeps the brand-new incident in this task's test still open
(not yet verified) after the very first `run_once` call.

An incident the Approver escalates (not whitelisted, below
`confidence_threshold`, or no action proposed) never gets a `fix_executed` or
`verification` row: `_advance_incident` stops at `approval` and every later
call is a cheap no-op for that incident (all its existing stages are already
present, so no stage is recomputed or re-appended) until a human resolves it
out of band.

Consequence for repeated-trial evaluation (read this before building Task
17's chaos harness): `db.get_open_incident(scenario, target)` treats an
incident as "open" until it has a `verification` row, so an escalated
incident — which never gets one — stays "open" for that exact
`(scenario, target)` pair forever. `observer.poll_once`'s dedup check skips
opening a new incident whenever one is already open for that pair. So the
moment a single incident for, say, `(pod_crash, cartservice)` escalates,
every *subsequent* `pod_crash` breach on `cartservice` is silently ignored —
no new `anomaly_detected` row, no diagnosis, nothing — until a human
resolves the stuck incident out of band (there is currently no in-repo
mechanism to do this; none is built as part of this task). A harness that
runs N repeated trials of the same scenario against the same target (e.g.
Task 17's planned 10 repeats per scenario) must check for this between
runs and clear/resolve any escalated incident first, or every run after the
first escalation will silently appear as "not detected" rather than
reflecting the actual fix/verify outcome.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import psycopg
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config
from kubernetes.config.config_exception import ConfigException

from . import approver, config, db, diagnoser, fixer, observer, verifier
from .loki_client import LokiClient
from .models import ApprovalDecision, Diagnosis, EvidenceSummary
from .prom_client import PrometheusClient

logger = logging.getLogger(__name__)

# Namespace hosting the Online Boutique services the Fixer acts on, per the
# plan's Global Constraints. `run_once`'s signature is fixed by the task
# brief with no namespace parameter, so this is a module constant rather
# than a configurable value threaded through the pipeline.
NAMESPACE = "online-boutique"

# The five scenario/target pairs the system supports; used by the production
# `main()` loop. `run_once` itself takes `scenarios` as an explicit
# parameter so tests can pass their own list.
SCENARIOS: list[tuple[str, str]] = [
    ("pod_crash", "cartservice"),
    ("cpu_stress", "recommendationservice"),
    ("memory_pressure", "productcatalogservice"),
    ("network_latency", "shippingservice"),
    ("service_unavailability", "paymentservice"),
]

# `build_evidence`'s lookback window used by the verification lambda in
# `_advance_incident`. This is intentionally NOT the same 300s window
# `observer.poll_once` always passes for original detection (see
# observer.py's `poll_once`, which hardcodes window_seconds=300
# independently of this constant).
VERIFY_WINDOW_SECONDS = 60

# Default verifier backoff per the plan's Global Constraints ("Verifier
# backoff: fixed 30s wait before re-checking (env-overridable
# VERIFY_BACKOFF_SECONDS)"). Read fresh from the environment on every call
# (not cached at import time) so a test can override it per-run via
# monkeypatch/os.environ without needing to reload this module.
DEFAULT_VERIFY_BACKOFF_SECONDS = 30


def _verify_backoff_seconds() -> int:
    return int(os.environ.get("VERIFY_BACKOFF_SECONDS", str(DEFAULT_VERIFY_BACKOFF_SECONDS)))


def _verify_sample_count() -> int:
    return int(os.environ.get("VERIFY_SAMPLE_COUNT", "1"))


def _verify_sample_interval_seconds() -> float:
    return float(os.environ.get("VERIFY_SAMPLE_INTERVAL_SECONDS", "5.0"))


def _max_retries() -> int:
    return int(os.environ.get("MAX_RETRIES", "1"))


def _incident_timeout_seconds() -> int:
    return int(os.environ.get("INCIDENT_TIMEOUT_SECONDS", "600"))


def _evidence_from_anomaly_payload(target: str, payload: dict[str, Any]) -> EvidenceSummary:
    """Rebuild the EvidenceSummary the Observer captured at anomaly_detected time.

    Diagnosis is based on this persisted snapshot, not a fresh Prometheus/Loki
    query, so it reflects exactly the evidence that triggered the incident.
    """
    return EvidenceSummary(
        target=target,
        restart_count_delta=payload.get("restart_count_delta", 0),
        error_rate=payload.get("error_rate", 0.0),
        cpu_ratio=payload.get("cpu_ratio", 0.0),
        p99_latency_ms=payload.get("p99_latency_ms", 0.0),
        p99_latency_rising=payload.get("p99_latency_rising", False),
        oom_killed=payload.get("oom_killed", False),
        mem_ratio=payload.get("mem_ratio", 0.0),
        available_replicas=payload.get("available_replicas", 1),
    )


def _diagnosis_from_payload(payload: dict[str, Any]) -> Diagnosis:
    return Diagnosis(
        cause=payload["cause"],
        confidence=payload["confidence"],
        evidence_cited=payload["evidence_cited"],
        action=payload["action"],
    )


def _decision_from_payload(payload: dict[str, Any]) -> ApprovalDecision:
    return ApprovalDecision(
        action=payload["action"],
        approved=payload["approved"],
        reason=payload["reason"],
    )


def _reconnect(conn: Any, database_url: str) -> Any:
    """Close `conn` if possible and open a fresh connection to `database_url`.

    Final-review Fix 4: `main()` used to open one psycopg connection at
    startup and never reconnect; a dropped connection (Postgres restart,
    network blip) raised `psycopg.OperationalError` from inside the poll
    loop, which the old bare `except Exception` logged and swallowed
    forever, leaving the orchestrator looping on a permanently dead
    connection with no liveness probe to force a restart. `main()`'s loop
    now catches `psycopg.OperationalError` specifically and calls this
    helper to get a working connection before the next poll cycle.

    `conn.close()` on an already-dead connection can itself raise; that is
    swallowed here (there is nothing useful to do about it) since the
    point is just to discard the unusable connection object and obtain a
    fresh one regardless.
    """
    try:
        conn.close()
    except Exception:
        logger.warning("error closing dead Postgres connection; discarding it anyway", exc_info=True)
    return psycopg.connect(database_url, autocommit=True)


def _get_incident_scenario_and_target(conn: Any, incident_id: Any) -> tuple[str, str] | None:
    row = conn.execute(
        "SELECT scenario, target FROM incidents WHERE id = %s", (incident_id,)
    ).fetchone()
    return (row[0], row[1]) if row is not None else None


def _advance_incident(
    conn: Any,
    prom_client: Any,
    loki_client: Any,
    core_v1: Any,
    apps_v1: Any,
    incident_id: Any,
    confidence_threshold: float,
) -> None:
    """Append every stage this incident is ready for, driven by Postgres state.

    See this module's docstring for why diagnosis/approval/fix_executed
    cascade within one call but verification is deliberately held back to a
    later call.
    """
    scenario_target = _get_incident_scenario_and_target(conn, incident_id)
    if scenario_target is None:
        return
    scenario, target = scenario_target

    events = db.get_events(conn, incident_id)
    stages = {e.stage: e for e in events}
    if "anomaly_detected" not in stages:
        return  # invariant violated; nothing to advance from
    fix_executed_already_present = "fix_executed" in stages

    if "diagnosis" not in stages:
        evidence = _evidence_from_anomaly_payload(target, stages["anomaly_detected"].payload)
        start = time.monotonic()
        diagnosis = diagnoser.diagnose(evidence)
        latency_ms = int((time.monotonic() - start) * 1000)
        db.append_event(
            conn,
            incident_id,
            "diagnosis",
            {
                "cause": diagnosis.cause,
                "confidence": diagnosis.confidence,
                "evidence_cited": diagnosis.evidence_cited,
                "action": diagnosis.action,
            },
            confidence=diagnosis.confidence,
            latency_ms=latency_ms,
        )
        stages = {e.stage: e for e in db.get_events(conn, incident_id)}

    if "approval" not in stages:
        diagnosis = _diagnosis_from_payload(stages["diagnosis"].payload)
        start = time.monotonic()
        decision = approver.approve(diagnosis, confidence_threshold)
        latency_ms = int((time.monotonic() - start) * 1000)
        db.append_event(
            conn,
            incident_id,
            "approval",
            {"action": decision.action, "approved": decision.approved, "reason": decision.reason},
            confidence=diagnosis.confidence,
            latency_ms=latency_ms,
        )
        stages = {e.stage: e for e in db.get_events(conn, incident_id)}

    decision = _decision_from_payload(stages["approval"].payload)
    if decision.approved and "fix_executed" not in stages:
        start = time.monotonic()
        fix_result = fixer.execute(decision.action, core_v1, apps_v1, NAMESPACE, target)
        latency_ms = int((time.monotonic() - start) * 1000)
        db.append_event(
            conn,
            incident_id,
            "fix_executed",
            {
                "action": fix_result.action,
                "target": fix_result.target,
                "success": fix_result.success,
                "detail": fix_result.detail,
            },
            latency_ms=latency_ms,
        )
        stages = {e.stage: e for e in db.get_events(conn, incident_id)}

    if fix_executed_already_present and "verification" not in stages:
        start = time.monotonic()
        result = verifier.verify(
            scenario,
            lambda: observer.build_evidence(
                prom_client, loki_client, scenario, target, VERIFY_WINDOW_SECONDS
            ),
            observer.check_threshold,
            backoff_seconds=_verify_backoff_seconds(),
            sample_count=_verify_sample_count(),
            sample_interval_seconds=_verify_sample_interval_seconds(),
        )
        latency_ms = int((time.monotonic() - start) * 1000)

        # Multi-tier retry logic: if verification failed and retries remain, attempt escalation/retry
        all_events = db.get_events(conn, incident_id)
        fix_events = [e for e in all_events if e.stage == "fix_executed"]
        max_retries = _max_retries()
        if not result.recovered and len(fix_events) < max_retries:
            # If scaling did not relieve CPU stress on pinned pod, escalate to restart_pod
            retry_action = (
                "restart_pod"
                if (scenario == "cpu_stress" and decision.action == "scale_replicas")
                else decision.action
            )
            fix_result = fixer.execute(retry_action, core_v1, apps_v1, NAMESPACE, target)
            db.append_event(
                conn,
                incident_id,
                "fix_executed",
                {
                    "action": fix_result.action,
                    "target": fix_result.target,
                    "success": fix_result.success,
                    "detail": f"retry attempt {len(fix_events) + 1}: {fix_result.detail}",
                    "retry_attempt": len(fix_events) + 1,
                },
                latency_ms=latency_ms,
            )
            return

        db.append_event(
            conn,
            incident_id,
            "verification",
            {
                "recovered": result.recovered,
                "checked_at": result.checked_at.isoformat(),
                "attempts": len(fix_events),
            },
            latency_ms=latency_ms,
        )


def run_once(
    conn: Any,
    prom_client: Any,
    loki_client: Any,
    core_v1: Any,
    apps_v1: Any,
    scenarios: list[tuple[str, str]],
    confidence_threshold: float,
) -> None:
    """Run one full poll cycle: detect new anomalies, then advance every
    incomplete incident's pipeline state, driven entirely by Postgres.

    Each incident's `_advance_incident` call is wrapped in its own
    try/except so that one incident raising (a non-`ApiException` Fixer
    error, a Prometheus/Loki HTTP error from `build_evidence`, a `KeyError`
    from a malformed persisted payload, a DB error, ...) is logged and
    skipped rather than propagating out of `run_once` — otherwise one bad
    incident would abort the whole poll cycle and every *other* incident
    due for advancement in this cycle would silently miss its turn.
    """
    db.close_stale_incidents(conn, _incident_timeout_seconds())
    observer.poll_once(prom_client, loki_client, conn, scenarios)
    for incident_id in db.get_incomplete_incidents(conn):
        try:
            _advance_incident(conn, prom_client, loki_client, core_v1, apps_v1, incident_id, confidence_threshold)
        except Exception:
            logger.exception("failed to advance incident %s; will retry next poll cycle", incident_id)


def _poll_cycle(
    conn: Any,
    prom_client: Any,
    loki_client: Any,
    core_v1: Any,
    apps_v1: Any,
    scenarios: list[tuple[str, str]],
    confidence_threshold: float,
    database_url: str,
) -> Any:
    """Run one `run_once` cycle, reconnecting to Postgres if it raises
    `psycopg.OperationalError`, and swallowing any other exception so a bad
    cycle doesn't crash the process. Returns the connection to use for the
    *next* cycle (either `conn` unchanged, or a fresh reconnected one).

    Factored out of `main()`'s `while True` loop (final-review Fix 4) so
    the reconnect-on-OperationalError behavior is directly unit-testable
    without needing a real Postgres/Kubernetes/Prometheus/Loki setup.
    """
    try:
        run_once(conn, prom_client, loki_client, core_v1, apps_v1, scenarios, confidence_threshold)
        return conn
    except psycopg.OperationalError:
        # `conn` is dead (Postgres restarted, a network blip killed the
        # socket, ...). Reconnect now rather than looping forever on an
        # unusable connection with every subsequent cycle silently doing
        # nothing but logging the same error.
        logger.exception("Postgres connection lost; reconnecting before the next poll cycle")
        return _reconnect(conn, database_url)
    except Exception:
        # One bad poll cycle (e.g. a transient K8s/Prometheus outage not
        # already caught inside run_once) must not crash the process and
        # crash-loop the pod; log it and try again next interval.
        logger.exception("poll cycle failed; will retry after the usual interval")
        return conn


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    cfg = config.load_config()

    conn = psycopg.connect(cfg.database_url, autocommit=True)
    prom_client = PrometheusClient(cfg.prometheus_url)
    loki_client = LokiClient(cfg.loki_url)

    try:
        k8s_config.load_incluster_config()
    except ConfigException:
        k8s_config.load_kube_config()
    core_v1 = k8s_client.CoreV1Api()
    apps_v1 = k8s_client.AppsV1Api()

    logger.info(
        "orchestrator starting: poll_interval=%ss confidence_threshold=%s",
        cfg.poll_interval_seconds,
        cfg.confidence_threshold,
    )
    while True:
        conn = _poll_cycle(
            conn, prom_client, loki_client, core_v1, apps_v1, SCENARIOS, cfg.confidence_threshold, cfg.database_url
        )
        logger.info("poll cycle complete, sleeping %ss", cfg.poll_interval_seconds)
        time.sleep(cfg.poll_interval_seconds)


if __name__ == "__main__":
    main()
