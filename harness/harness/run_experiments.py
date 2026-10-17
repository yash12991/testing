"""Repeated-trial chaos evaluation harness — drives the LIVE cluster.

For each `(scenario, target)` pair in `SCENARIOS` (mirrors
`orchestrator.main.SCENARIOS`), this script repeats `--runs-per-scenario`
times: record the real chaos-injection wall-clock timestamp, `kubectl
apply` that scenario's chaos manifest (Task 15), poll Postgres directly for
a brand-new incident to appear for that `(scenario, target)` pair, then
poll until that incident reaches a terminal state, then `kubectl delete`
the manifest before moving on. The already-running orchestrator Deployment
(polling independently every `POLL_INTERVAL_SECONDS`, Task 13) is what
actually performs detection/diagnosis/approval/fix/verification — this
script only injects chaos and observes Postgres; it never calls any
pipeline stage directly.

Most of this script drives the live cluster/Postgres directly and is NOT
unit tested (see `harness/tests/test_report.py` for `report.aggregate_results`,
which is pure and tested). The one pure function in *this* module,
`compute_true_mttr_times`, is unit tested in `harness/tests/test_run_experiments.py`.
It requires:
  - `kubectl` on PATH, configured against the running Minikube cluster.
  - `DATABASE_URL` in the environment, pointing at the cluster's Postgres.
    Postgres is a ClusterIP Service (`postgres.online-boutique.svc.cluster
    .local:5432`, database `incidents`), not reachable directly from
    outside the cluster, so run (in a separate terminal, left running for
    the whole harness invocation):
        kubectl port-forward -n online-boutique svc/postgres 5432:5432
    then set (using the real POSTGRES_USER/POSTGRES_PASSWORD from the
    gitignored infra/minikube/.env.postgres — never hardcode or print the
    password):
        export DATABASE_URL="postgresql://<user>:<password>@localhost:5432/incidents"

Known system limitation this script works around (documented in
`orchestrator/orchestrator/main.py`'s module docstring, Task 13's
escalation design, and repeated here since it's load-bearing for this
harness): once an incident's `approval` stage comes back `approved: False`
(escalated — below the confidence threshold or a non-whitelisted action),
it never gets a `fix_executed`/`verification` row. `db.get_open_incident`
treats any incident lacking a `verification` row as "open" for its exact
`(scenario, target)` pair forever, and `observer.poll_once`'s dedup check
silently skips opening a new incident whenever one is already open for
that pair — so a single escalation would permanently block every
*subsequent* repeat of that scenario from ever being detected again, no
matter how many more times this harness injects chaos. There is currently
no in-repo mechanism to resolve a stuck incident. This script's only way
around it, since it already has direct Postgres access, is to manually
append a synthetic `verification` row for that stuck incident itself
(`db.append_event(conn, incident_id, "verification", {"recovered": False,
"manually_closed_by_harness": True, ...})`) purely so `get_open_incident`
stops seeing it as open and the harness's *own next* repeat can be
detected. This is a documented workaround for a known system limitation
that this task deliberately did not fix upstream (Task 17's brief scopes
this task to the harness only) — it is NOT how a real escalation should be
resolved operationally; a real one should be reviewed and closed by a
human, not auto-closed by a test harness. The same manual-close is applied
if a trial times out waiting for a terminal state (`outcome="timed_out"`),
for the identical reason: an incident stuck mid-pipeline (e.g. waiting on
`verification`) is still "open" and would otherwise block every later
repeat too.

Known cross-scenario interaction this script also works around:
`fixer.scale_replicas` (the `cpu_stress` remediation) permanently leaves
the target Deployment scaled up (3 replicas) — it is not reverted as part
of "the fix," by design. `observer.build_evidence`'s `cpu_ratio` is a
cluster-wide average across every matching pod, and Chaos Mesh's `mode:
one` selector only ever stresses a single pod. So once a target has 3
replicas, the same single-pod stress load this manifest applies is diluted
to roughly 1/3 of the cluster-wide average and would likely never cross
the `cpu_ratio > 0.85` threshold again on a repeat — not because the
pipeline is broken, but because the fixture's starting condition silently
changed. To keep repeats comparable, this script resets
`recommendationservice` to 1 replica (and waits for it to actually be
ready) immediately before every `cpu_stress` trial.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg

_REPO_ROOT = Path(__file__).resolve().parents[2]
_ORCHESTRATOR_SRC = _REPO_ROOT / "orchestrator"
if str(_ORCHESTRATOR_SRC) not in sys.path:
    sys.path.insert(0, str(_ORCHESTRATOR_SRC))

from orchestrator import db  # noqa: E402

from harness import report  # noqa: E402

logger = logging.getLogger(__name__)


class TerminationRequested(SystemExit):
    """Raised by `_handle_termination_signal` instead of letting the
    process die outright.

    Python's default SIGTERM disposition terminates the interpreter
    immediately, WITHOUT running any `try/finally` block that happens to
    be on the stack — including `run_trial`'s `finally: _delete_manifest
    (...)`, whose entire job is to remove the chaos manifest it just
    applied. That is almost certainly how the `cpu-stress-
    recommendationservice` StressChaos resource got orphaned earlier,
    when a background harness process running this script was
    interrupted mid-trial (task-17-report.md section 4). Raising a
    `SystemExit` subclass instead means the interpreter unwinds the stack
    like any other exception — every `finally` block (and the `conn.close
    ()` in `main`'s `finally`) still runs — and `SystemExit` still exits
    the process once unwound, so `kubectl delete` for the in-flight trial
    actually happens before the process goes away.
    """


def _handle_termination_signal(signum: int, frame: Any) -> None:
    signal_name = signal.Signals(signum).name
    logger.warning(
        "received %s; finishing the current trial's cleanup before exiting", signal_name
    )
    raise TerminationRequested(f"terminated by {signal_name}")


def install_termination_handlers() -> None:
    """Install handlers for SIGTERM (e.g. a backgrounded process being
    killed) and SIGINT (Ctrl+C) so in-flight `finally` cleanup — notably
    `kubectl delete` of the current trial's chaos manifest — still runs
    on external termination. Python's own default SIGINT disposition
    already raises `KeyboardInterrupt` (which also unwinds `finally`
    blocks), but SIGTERM's default disposition does not; both are handled
    identically here for a single, well-documented termination path.
    """
    signal.signal(signal.SIGTERM, _handle_termination_signal)
    signal.signal(signal.SIGINT, _handle_termination_signal)

NAMESPACE = "online-boutique"
CHAOS_DIR = _REPO_ROOT / "infra" / "manifests" / "chaos-experiments"

# Mirrors orchestrator.main.SCENARIOS (Task 13) paired with each scenario's
# chaos manifest from Task 15.
SCENARIOS: list[tuple[str, str, Path]] = [
    ("pod_crash", "cartservice", CHAOS_DIR / "pod-crash.yaml"),
    ("cpu_stress", "recommendationservice", CHAOS_DIR / "cpu-stress.yaml"),
    ("memory_pressure", "productcatalogservice", CHAOS_DIR / "memory-pressure.yaml"),
    ("network_latency", "shippingservice", CHAOS_DIR / "network-latency.yaml"),
    ("service_unavailability", "paymentservice", CHAOS_DIR / "service-unavailability.yaml"),
]

# cpu_stress's cpu_ratio uses a 300s rate() window and, per task-16-report.md's
# live validation, took ~235s of sustained stress to actually cross 0.85 from
# an idle baseline. Generous margin beyond that, plus the orchestrator's own
# 15s poll interval.
DEFAULT_DETECTION_TIMEOUT_S = 420
# The diagnosis -> approval -> fix -> verification cascade was observed live
# (task-16-report.md) to complete in ~45-90s once an anomaly is detected.
DEFAULT_RESOLUTION_TIMEOUT_S = 180
POLL_INTERVAL_S = 5
REPLICA_RESET_TIMEOUT_S = 120


def _run_kubectl(args: list[str]) -> None:
    subprocess.run(["kubectl", *args], check=True)


def _apply_manifest(manifest_path: Path) -> None:
    _run_kubectl(["apply", "-f", str(manifest_path)])


def _delete_manifest(manifest_path: Path) -> None:
    _run_kubectl(["delete", "-f", str(manifest_path), "--ignore-not-found=true"])


def _reset_replicas(target: str, replicas: int, timeout_s: int = REPLICA_RESET_TIMEOUT_S) -> None:
    """Scale `target`'s Deployment to `replicas` and wait until it reports
    that many ready replicas. See this module's docstring ("Known
    cross-scenario interaction") for why this matters for cpu_stress.
    """
    logger.info("resetting %s to %d replica(s) before this trial", target, replicas)
    _run_kubectl(["scale", f"deployment/{target}", "-n", NAMESPACE, f"--replicas={replicas}"])
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        result = subprocess.run(
            [
                "kubectl",
                "get",
                f"deployment/{target}",
                "-n",
                NAMESPACE,
                "-o",
                "jsonpath={.status.readyReplicas}",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        if result.stdout.strip() == str(replicas):
            return
        time.sleep(2)
    logger.warning("timed out waiting for %s to reach %d ready replica(s); proceeding anyway", target, replicas)


def _wait_for_new_incident(
    conn: Any, scenario: str, target: str, after_ts: datetime, timeout_s: int
) -> uuid.UUID | None:
    """Poll `incidents` for a row created at/after `after_ts` for this
    (scenario, target) pair. Returns None on timeout (chaos never tripped
    the Observer's threshold within the timeout).
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        row = conn.execute(
            """
            SELECT id FROM incidents
            WHERE scenario = %s AND target = %s AND created_at >= %s
            ORDER BY created_at ASC
            LIMIT 1
            """,
            (scenario, target, after_ts),
        ).fetchone()
        if row is not None:
            return row[0]
        time.sleep(POLL_INTERVAL_S)
    return None


def _wait_for_terminal_state(conn: Any, incident_id: uuid.UUID, timeout_s: int) -> str:
    """Poll `incident_events` until this incident reaches a terminal state.

    Returns "resolved" (a `verification` row exists), "escalated" (an
    `approval` row exists with `approved: False`), or "timed_out".
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        stages = {e.stage: e for e in db.get_events(conn, incident_id)}
        if "verification" in stages:
            return "resolved"
        if "approval" in stages and stages["approval"].payload.get("approved") is False:
            return "escalated"
        time.sleep(POLL_INTERVAL_S)
    return "timed_out"


def run_trial(
    conn: Any,
    scenario: str,
    target: str,
    manifest_path: Path,
    repeat: int,
    detection_timeout_s: int,
    resolution_timeout_s: int,
) -> dict[str, Any]:
    """Run exactly one chaos-inject-and-observe trial. Always cleans up the
    chaos manifest before returning, even on error.
    """
    incident_id: uuid.UUID | None = None
    outcome = "error"
    injection_ts = datetime.now(timezone.utc)
    try:
        if scenario == "cpu_stress":
            _reset_replicas(target, 1)

        injection_ts = datetime.now(timezone.utc)
        logger.info("[%s #%d] applying %s at %s", scenario, repeat, manifest_path.name, injection_ts.isoformat())
        _apply_manifest(manifest_path)

        incident_id = _wait_for_new_incident(conn, scenario, target, injection_ts, detection_timeout_s)
        if incident_id is None:
            outcome = "not_detected"
            logger.warning("[%s #%d] no incident detected within %ss", scenario, repeat, detection_timeout_s)
        else:
            outcome = _wait_for_terminal_state(conn, incident_id, resolution_timeout_s)
            if outcome == "escalated":
                logger.warning(
                    "[%s #%d] incident %s escalated (approved=False); manually closing it "
                    "so future repeats aren't blocked - see module docstring",
                    scenario,
                    repeat,
                    incident_id,
                )
                db.append_event(
                    conn,
                    incident_id,
                    "verification",
                    {
                        "recovered": False,
                        "manually_closed_by_harness": True,
                        "reason": "escalated_by_approver",
                    },
                )
            elif outcome == "timed_out":
                logger.warning(
                    "[%s #%d] incident %s did not reach a terminal state within %ss; "
                    "manually closing it so future repeats aren't blocked",
                    scenario,
                    repeat,
                    incident_id,
                    resolution_timeout_s,
                )
                db.append_event(
                    conn,
                    incident_id,
                    "verification",
                    {
                        "recovered": False,
                        "manually_closed_by_harness": True,
                        "reason": "harness_resolution_timeout",
                    },
                )
            else:
                logger.info("[%s #%d] incident %s resolved", scenario, repeat, incident_id)
    except Exception:
        logger.exception("[%s #%d] trial raised an unexpected error", scenario, repeat)
        outcome = "error"
    finally:
        logger.info("[%s #%d] cleaning up %s", scenario, repeat, manifest_path.name)
        try:
            _delete_manifest(manifest_path)
        except Exception:
            logger.exception("[%s #%d] cleanup of %s failed", scenario, repeat, manifest_path.name)

    return {
        "scenario": scenario,
        "target": target,
        "repeat": repeat,
        "outcome": outcome,
        "incident_id": incident_id,
        "injection_ts": injection_ts,
    }


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _fmt(value: float | None) -> str:
    return f"{value:.1f}" if value is not None else "n/a"


def compute_true_mttr_times(
    trial_log: list[dict[str, Any]], all_rows: list[db.EventRow]
) -> dict[str, list[float]]:
    """Compute the true injection-to-recovery MTTR per scenario: the
    wall-clock gap between this harness's own `injection_ts` (recorded
    right before `kubectl apply`) and the incident's real `verification.ts`
    in Postgres.

    Final-review Fix 1: `report.aggregate_results`'s `mttr_s` field is
    `anomaly_detected.ts -> verification.ts` (detection-to-recovery), not
    the spec's actual MTTR (injection-to-recovery) — and in practice it's
    dominated by the Verifier's fixed ~30s backoff regardless of scenario,
    badly understating the true repair time. This function fills that gap
    the same way `main()` already overwrites `detection_times_s` after the
    fact with the true `injection -> anomaly_detected` wall-clock
    measurement (see `report.py`'s module docstring): `aggregate_results`
    itself has no `injection_ts` to work from, so only this harness (which
    issued the `kubectl apply`) can compute the true MTTR.

    An incident is excluded entirely if any of its `verification` rows has
    `manually_closed_by_harness: True` (mirrors `aggregate_results`'s own
    `mttr_s` exclusion — that timestamp reflects how long the harness
    polled before giving up, not a real repair time). Among the remaining,
    genuine `verification` rows for an incident, only rows with
    `recovered: True` are eligible, and the earliest such row's `ts` is
    used; an incident with no `recovered: True` verification row never
    actually recovered, so it contributes nothing to this average rather
    than a bogus "repair time" (mirrors `aggregate_results`'s own
    `remediation_success`/`mttr_s` pattern of gating on `recovered is
    True` — see `report.py`'s module docstring).
    """
    rows_by_incident: dict[Any, list[db.EventRow]] = {}
    for row in all_rows:
        rows_by_incident.setdefault(row.incident_id, []).append(row)

    true_mttr_times: dict[str, list[float]] = {}
    for t in trial_log:
        incident_id = t["incident_id"]
        if incident_id is None:
            continue
        incident_rows = rows_by_incident.get(incident_id, [])
        verification_rows = [r for r in incident_rows if r.stage == "verification"]
        if not verification_rows:
            continue
        if any(r.payload.get("manually_closed_by_harness") for r in verification_rows):
            continue
        recovered_rows = [r for r in verification_rows if r.payload.get("recovered") is True]
        if not recovered_rows:
            continue
        verification_ts = min(r.ts for r in recovered_rows)
        true_mttr_times.setdefault(t["scenario"], []).append(
            (verification_ts - t["injection_ts"]).total_seconds()
        )
    return true_mttr_times


def write_report(
    stats: dict[str, report.ScenarioStats],
    trial_log: list[dict[str, Any]],
    scenarios_run: list[tuple[str, str, Path]],
    runs_per_scenario: int,
    out_dir: Path,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = out_dir / f"{timestamp}.md"

    lines: list[str] = []
    lines.append(f"# Chaos-remediation evaluation run — {timestamp}")
    lines.append("")
    lines.append(f"Requested `--runs-per-scenario`: {runs_per_scenario}")
    lines.append("")
    lines.append(
        "| scenario | attempted | detected | diagnosis_correct | remediation_success | "
        "mean detection (s) | mean detection→recovery (s) | mean MTTR (s) [injection→recovery] | "
        "escalations | timed_out | not_detected |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|")

    for scenario, _target, _manifest in scenarios_run:
        attempted = sum(1 for t in trial_log if t["scenario"] == scenario)
        not_detected = sum(1 for t in trial_log if t["scenario"] == scenario and t["outcome"] == "not_detected")
        timed_out = sum(1 for t in trial_log if t["scenario"] == scenario and t["outcome"] == "timed_out")
        s = stats.get(scenario, report.ScenarioStats(scenario=scenario))
        lines.append(
            f"| {scenario} | {attempted} | {s.runs} | {s.diagnosis_correct} | {s.remediation_success} | "
            f"{_fmt(_mean(s.detection_times_s))} | {_fmt(_mean(s.mttr_s))} | {_fmt(_mean(s.true_mttr_s))} | "
            f"{s.escalations} | {timed_out} | {not_detected} |"
        )

    lines.append("")
    lines.append(
        "`detection (s)` is the true wall-clock gap between this harness's `kubectl apply` "
        "and the Observer's `anomaly_detected` row (see `report.py`'s module docstring for why "
        "`aggregate_results` itself cannot compute this and `main()` overwrites it below). "
        "`detection→recovery (s)` is `aggregate_results`'s `mttr_s` field: `anomaly_detected -> "
        "verification`, recorded for every incident that reached verification regardless of "
        "outcome. Despite what its field name suggests, THIS IS NOT THE SPEC'S MTTR METRIC — it "
        "is detection-to-recovery, dominated in practice by the Verifier's fixed ~30s backoff "
        "regardless of scenario, so it badly understates true repair time. "
        "`MTTR (s) [injection→recovery]` is the actual spec metric: the wall-clock gap between "
        "this harness's `kubectl apply` (`injection_ts`) and the incident's real `verification` "
        "row in Postgres, computed the same way `detection (s)` is (see "
        "`compute_true_mttr_times`). `diagnosis_correct`/`remediation_success` are counted "
        "against `detected`, not `attempted`."
    )
    lines.append("")
    lines.append("## Interpreting these results")
    lines.append("")
    lines.append(
        "- **pod_crash's \"remediation success\" may partly reflect Kubernetes' own "
        "self-healing, not solely the orchestrator's fix.** `pod-crash.yaml` uses Chaos Mesh's "
        "`container-kill` action, which is one-shot: the kubelet restarts the killed container "
        "in place within seconds, independent of whether the Fixer's `restart_pod` action (which "
        "deletes the pod so Kubernetes reschedules it) ever ran. Verification happens within the "
        "~60s window after `fix_executed`, by which point the kubelet may have already restarted "
        "the container on its own. So a `recovered: True` / `remediation_success` count for "
        "pod_crash does not cleanly isolate \"the orchestrator fixed it\" from \"Kubernetes "
        "would have fixed it anyway regardless of what the orchestrator did.\" This is a known, "
        "documented limitation of the current chaos-injection timing for this scenario, not "
        "something this report's numbers correct for — read pod_crash's remediation_success "
        "with that caveat in mind."
    )
    lines.append(
        "- **cpu_stress's `cpu_ratio` now uses `max(...)` across matching pods, not `avg(...)` "
        "(final-review Fix 3).** Averaging previously meant that once `scale_replicas` added "
        "idle replicas, the cluster-wide average diluted below threshold even though Chaos "
        "Mesh's `mode: one` selector kept stressing only the original pod — so `recovered: True` "
        "used to mean \"the average got diluted,\" not \"the stressed pod itself recovered.\" "
        "With `max(...)`, verification now reflects the worst/most-stressed matching pod, which "
        "is a more honest (and typically slower-to-satisfy, or possibly never-satisfied within "
        "the resolution timeout) signal of real recovery."
    )
    lines.append("")
    lines.append("## Raw trial log")
    lines.append("")
    lines.append("| scenario | repeat | outcome | incident_id | injection_ts (UTC) |")
    lines.append("|---|---|---|---|---|")
    for t in trial_log:
        lines.append(
            f"| {t['scenario']} | {t['repeat']} | {t['outcome']} | {t['incident_id'] or ''} | "
            f"{t['injection_ts'].isoformat()} |"
        )

    out_path.write_text("\n".join(lines) + "\n")
    return out_path


def main() -> None:
    install_termination_handlers()

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs-per-scenario", type=int, default=10)
    parser.add_argument(
        "--scenarios",
        nargs="*",
        default=None,
        help="subset of scenario names to run (default: all three, pod_crash/cpu_stress/memory_pressure)",
    )
    parser.add_argument("--detection-timeout-s", type=int, default=DEFAULT_DETECTION_TIMEOUT_S)
    parser.add_argument("--resolution-timeout-s", type=int, default=DEFAULT_RESOLUTION_TIMEOUT_S)
    parser.add_argument("--database-url", default=None, help="defaults to $DATABASE_URL")
    parser.add_argument("--out-dir", default=str(Path(__file__).resolve().parents[1] / "results"))
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    database_url = args.database_url or os.environ.get("DATABASE_URL")
    if not database_url:
        print(
            "ERROR: set DATABASE_URL, e.g. via:\n"
            "  kubectl port-forward -n online-boutique svc/postgres 5432:5432 &\n"
            "  export DATABASE_URL=\"postgresql://<user>:<password>@localhost:5432/incidents\"\n"
            "(credentials come from infra/minikube/.env.postgres, gitignored - never hardcode or "
            "print the password), or pass --database-url.",
            file=sys.stderr,
        )
        sys.exit(1)

    scenarios_to_run = SCENARIOS
    if args.scenarios:
        wanted = set(args.scenarios)
        scenarios_to_run = [s for s in SCENARIOS if s[0] in wanted]
        if not scenarios_to_run:
            print(f"ERROR: no match for --scenarios {args.scenarios}", file=sys.stderr)
            sys.exit(1)

    trial_log: list[dict[str, Any]] = []
    conn = psycopg.connect(database_url, autocommit=True)
    try:
        for scenario, target, manifest_path in scenarios_to_run:
            for repeat in range(1, args.runs_per_scenario + 1):
                result = run_trial(
                    conn,
                    scenario,
                    target,
                    manifest_path,
                    repeat,
                    args.detection_timeout_s,
                    args.resolution_timeout_s,
                )
                trial_log.append(result)
                logger.info("[%s #%d] done, outcome=%s", scenario, repeat, result["outcome"])

        all_rows: list[db.EventRow] = []
        for t in trial_log:
            if t["incident_id"] is not None:
                all_rows.extend(db.get_events(conn, t["incident_id"]))
    finally:
        conn.close()

    stats = report.aggregate_results(all_rows)

    # Overwrite the EventRow-derived detection_times_s with the true
    # injection -> anomaly_detected wall-clock measurement this script
    # actually observed (see report.py's module docstring for why
    # aggregate_results itself cannot compute this).
    true_detection_times: dict[str, list[float]] = {}
    for t in trial_log:
        if t["incident_id"] is None:
            continue
        anomaly_row = next(
            (r for r in all_rows if r.incident_id == t["incident_id"] and r.stage == "anomaly_detected"),
            None,
        )
        if anomaly_row is not None:
            true_detection_times.setdefault(t["scenario"], []).append(
                (anomaly_row.ts - t["injection_ts"]).total_seconds()
            )
    for scenario_name, times in true_detection_times.items():
        if scenario_name in stats:
            stats[scenario_name].detection_times_s = times

    # True injection-to-recovery MTTR (final-review Fix 1) — see
    # compute_true_mttr_times's docstring for why aggregate_results's own
    # mttr_s field cannot compute this and this harness must overwrite it
    # after the fact, mirroring true_detection_times above.
    true_mttr_times = compute_true_mttr_times(trial_log, all_rows)
    for scenario_name, times in true_mttr_times.items():
        if scenario_name in stats:
            stats[scenario_name].true_mttr_s = times

    out_path = write_report(
        stats, trial_log, scenarios_to_run, args.runs_per_scenario, Path(args.out_dir)
    )
    logger.info("wrote report to %s", out_path)
    print(str(out_path))


if __name__ == "__main__":
    main()
