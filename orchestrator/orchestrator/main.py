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
from .models import ApprovalDecision, Diagnosis, EvidenceSummary

logger = logging.getLogger(__name__)

# Namespace hosting the Online Boutique services the Fixer acts on, per the
# plan's Global Constraints. `run_once`'s signature is fixed by the task
# brief with no namespace parameter, so this is a module constant rather
# than a configurable value threaded through the pipeline.
NAMESPACE = "online-boutique"

# The three scenario/target pairs the plan defines; used by the production
# `main()` loop. `run_once` itself takes `scenarios` as an explicit
# parameter so tests can pass their own list.
SCENARIOS: list[tuple[str, str]] = [
    ("pod_crash", "cartservice"),
    ("cpu_stress", "recommendationservice"),
    ("memory_pressure", "productcatalogservice"),
]

# `build_evidence`'s lookback window used by the verification lambda in
# `_advance_incident`. This is intentionally NOT the same 300s window
# `observer.poll_once` uses for original detection (see observer.py, which
# hardcodes its own 300s independently). Verification runs only ~one poll
# interval plus VERIFY_BACKOFF_SECONDS after the fix was applied (tens of
# seconds), but a 300s window would still contain the pre-fix breach
# signal: for pod_crash, `increase(...restarts_total[300s])` would still
# see the restarts that happened before the fix; for memory_pressure,
# pre-fix OOMKilled log lines would still be inside a 300s-equivalent
# lookback. A verification row permanently resolves (or fails to resolve)
# the incident — there's no retry — so a stale window would read the old
# breach and wrongly report `recovered=False` forever, silently corrupting
# every chaos run's measured recovery outcome (Task 17's harness depends
# on this being accurate). This window is sized to roughly "time since the
# fix was applied" instead: long enough for a stable reading, short enough
# to exclude the pre-fix breach.
VERIFY_WINDOW_SECONDS = 60

# Default verifier backoff per the plan's Global Constraints ("Verifier
# backoff: fixed 30s wait before re-checking (env-overridable
# VERIFY_BACKOFF_SECONDS)"). Read fresh from the environment on every call
# (not cached at import time) so a test can override it per-run via
# monkeypatch/os.environ without needing to reload this module.
DEFAULT_VERIFY_BACKOFF_SECONDS = 30


def _verify_backoff_seconds() -> int:
    return int(os.environ.get("VERIFY_BACKOFF_SECONDS", str(DEFAULT_VERIFY_BACKOFF_SECONDS)))


def _evidence_from_anomaly_payload(target: str, payload: dict[str, Any]) -> EvidenceSummary:
    """Rebuild the EvidenceSummary the Observer captured at anomaly_detected time.

    Diagnosis is based on this persisted snapshot, not a fresh Prometheus/Loki
    query, so it reflects exactly the evidence that triggered the incident.
    """
    return EvidenceSummary(
        target=target,
        restart_count_delta=payload["restart_count_delta"],
        error_rate=payload["error_rate"],
        cpu_ratio=payload["cpu_ratio"],
        p99_latency_ms=payload["p99_latency_ms"],
        p99_latency_rising=payload["p99_latency_rising"],
        oom_killed=payload["oom_killed"],
        mem_ratio=payload["mem_ratio"],
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
        )
        latency_ms = int((time.monotonic() - start) * 1000)
        db.append_event(
            conn,
            incident_id,
            "verification",
            {"recovered": result.recovered, "checked_at": result.checked_at.isoformat()},
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
    observer.poll_once(prom_client, loki_client, conn, scenarios)
    for incident_id in db.get_incomplete_incidents(conn):
        try:
            _advance_incident(conn, prom_client, loki_client, core_v1, apps_v1, incident_id, confidence_threshold)
        except Exception:
            logger.exception("failed to advance incident %s; will retry next poll cycle", incident_id)


class PrometheusClient:
    """Minimal real Prometheus HTTP API client satisfying observer.PromClient.

    This is a stopgap for Task 13 so `main()` is runnable end-to-end; the
    plan assigns the real, hardened Prometheus/Loki clients to Task 16
    ("Task 16 builds the real PrometheusClient/LokiClient to the same
    shape"), which may replace this implementation entirely.
    """

    def __init__(self, base_url: str):
        self._base_url = base_url.rstrip("/")

    def query(self, promql: str) -> float:
        import httpx

        response = httpx.get(f"{self._base_url}/api/v1/query", params={"query": promql}, timeout=10.0)
        response.raise_for_status()
        result = response.json()["data"]["result"]
        if not result:
            return 0.0
        return float(result[0]["value"][1])


class LokiClient:
    """Minimal real Loki HTTP API client satisfying observer.LokiClient.

    See `PrometheusClient`'s docstring: a Task 13 stopgap, not the hardened
    Task 16 implementation.
    """

    def __init__(self, base_url: str):
        self._base_url = base_url.rstrip("/")

    def query_range(self, logql: str) -> list[str]:
        import httpx

        response = httpx.get(
            f"{self._base_url}/loki/api/v1/query_range", params={"query": logql}, timeout=10.0
        )
        response.raise_for_status()
        streams = response.json()["data"]["result"]
        return [value[1] for stream in streams for value in stream.get("values", [])]


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
        try:
            run_once(conn, prom_client, loki_client, core_v1, apps_v1, SCENARIOS, cfg.confidence_threshold)
        except Exception:
            # One bad poll cycle (e.g. a transient Postgres/K8s/Prometheus
            # outage not already caught inside run_once) must not crash the
            # process and crash-loop the pod; log it and try again next
            # interval.
            logger.exception("poll cycle failed; will retry after the usual interval")
        logger.info("poll cycle complete, sleeping %ss", cfg.poll_interval_seconds)
        time.sleep(cfg.poll_interval_seconds)


if __name__ == "__main__":
    main()
