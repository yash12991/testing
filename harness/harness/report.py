"""Pure aggregation of `incident_events` rows into per-scenario statistics.

`aggregate_results` is a pure function: it only reads the `EventRow`-shaped
objects it's given (see `orchestrator.db.EventRow` / `db.get_events` —
`(id, incident_id, scenario, stage, ts, payload, confidence, latency_ms)`)
and returns plain data. It does not import `orchestrator`, open a database
connection, or touch the cluster, so it can be exercised in
`harness/tests/test_report.py` with synthetic rows and no live Postgres or
Kubernetes. `harness/harness/run_experiments.py` is the only module in this
package that talks to the real cluster/Postgres; it calls `aggregate_results`
once it has collected real rows.

Design decisions made here that the Task 17 brief leaves to the
implementer (the brief's own worked examples only pin `runs`,
`remediation_success`, `mttr_s`, and `escalations` — not
`detection_times_s`/`diagnosis_correct`):

- `detection_times_s`: the brief's interface is `aggregate_results(rows)`
  with no injection timestamp, so this pure function cannot know how long
  *before* `anomaly_detected` the chaos was actually injected — that's a
  wall-clock fact only `run_experiments.py` (which issues the `kubectl
  apply`) can observe. As a reasonable EventRow-only proxy, this function
  fills `detection_times_s` with the `diagnosis.ts - anomaly_detected.ts`
  gap per incident (how fast the pipeline turns a detected anomaly into a
  diagnosis). `run_experiments.py` OVERWRITES this per-scenario list after
  the fact with the true `injection -> anomaly_detected` wall-clock
  measurement it recorded itself, which is the more meaningful number for
  the evaluation report; see that module's docstring.

- `diagnosis_correct`: scores the `diagnosis` stage's `cause` against the
  expected cause(s) for the incident's scenario, using `EXPECTED_CAUSES`
  below. `memory_pressure` deliberately accepts two causes
  (`transient_pod_failure` as well as `undersized_limit_or_leak`) because
  of a documented, non-buggy quirk in `orchestrator.diagnoser.diagnose`'s
  fixed rule-priority ordering (see `orchestrator/orchestrator/main.py`'s
  module docstring and task-16-report.md section 11): a real OOM kill
  almost always also increments `restart_count_delta`, so the pod_crash
  rule (checked first) wins and the cause comes back
  `transient_pod_failure` even though the true chaos scenario was
  `memory_pressure` — both rules map to the same `restart_pod` action, so
  this is correct, intended behavior, not a misdiagnosis, and scoring it as
  "wrong" would be measuring an artifact of rule order rather than the
  diagnoser's actual accuracy.

- `mttr_s`: the `anomaly_detected.ts -> verification.ts` gap, recorded for
  every incident that reached a `verification` row regardless of whether
  it actually recovered (`remediation_success` separately tracks the
  recovered=True count) — this reports "how long until the pipeline reached
  a resolution attempt," a meaningful number even on a failed attempt.
  EXCEPTION: an incident any of whose `verification` rows has
  `manually_closed_by_harness: True` (see `run_experiments.py`'s module
  docstring — the harness synthesizes this row when an incident escalates
  or times out, purely so its own dedup-blocking workaround doesn't wedge
  future repeats) is excluded from `mttr_s` entirely and never counted
  toward `remediation_success`, regardless of that row's `recovered`
  value. That "verification" timestamp reflects how long the *harness*
  took to poll and give up, or the harness's own resolution-timeout
  constant — not a real repair time — so folding it into "mean MTTR" would
  silently contaminate the metric with values that have nothing to do with
  the pipeline's actual performance. This check looks at *every*
  `verification` row for the incident, not just the last one by `id`: if
  the harness's synthetic failure row is later followed by a genuine
  orchestrator `verification` row (e.g. a race where the real pipeline
  finishes resolving an incident the harness had already given up on and
  closed), the incident must still count as harness-closed — a late real
  row must not silently flip a timed-out/escalated incident into a
  success.

- "escalation": an incident whose `approval` stage has `approved: False`.
  Per `orchestrator.approver.approve`, an escalated incident never gets a
  `fix_executed`/`verification` row, so checking `approval.approved is
  False` alone is sufficient and matches the brief's worked example
  exactly.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


class EventRowLike(Protocol):
    """Structural shape this module needs from a row — matches
    `orchestrator.db.EventRow` without importing that package."""

    incident_id: Any
    scenario: str
    stage: str
    ts: Any
    payload: dict


@dataclass
class ScenarioStats:
    scenario: str
    runs: int = 0
    detection_times_s: list[float] = field(default_factory=list)
    diagnosis_correct: int = 0
    remediation_success: int = 0
    mttr_s: list[float] = field(default_factory=list)
    escalations: int = 0


# Expected `diagnosis.cause` value(s) for each chaos scenario, per
# `orchestrator.diagnoser.diagnose`'s fixed rule table. See this module's
# docstring for why memory_pressure accepts two causes.
EXPECTED_CAUSES: dict[str, frozenset[str]] = {
    "pod_crash": frozenset({"transient_pod_failure"}),
    "cpu_stress": frozenset({"resource_exhaustion"}),
    "memory_pressure": frozenset({"undersized_limit_or_leak", "transient_pod_failure"}),
}


def aggregate_results(rows: list[EventRowLike]) -> dict[str, ScenarioStats]:
    """Group `rows` by `incident_id`, then fold each incident's stages into
    its scenario's running `ScenarioStats`.

    One incident contributes exactly one `run` to its scenario, regardless
    of how many stage rows it has (an escalated incident may have only
    `anomaly_detected`/`diagnosis`/`approval`; a fully-resolved one has all
    five stages).
    """
    incidents: dict[Any, list[EventRowLike]] = {}
    for row in rows:
        incidents.setdefault(row.incident_id, []).append(row)

    stats: dict[str, ScenarioStats] = {}
    for incident_id, incident_rows in incidents.items():
        scenario = incident_rows[0].scenario
        by_stage = {row.stage: row for row in incident_rows}

        if scenario not in stats:
            stats[scenario] = ScenarioStats(scenario=scenario)
        s = stats[scenario]
        s.runs += 1

        anomaly = by_stage.get("anomaly_detected")
        diagnosis = by_stage.get("diagnosis")
        approval = by_stage.get("approval")
        verification = by_stage.get("verification")

        # Look at EVERY verification row for this incident (not just the
        # last one `by_stage` kept) so a genuine orchestrator verification
        # that happens to arrive after the harness's synthetic
        # escalated/timed-out close can't silently override it — see this
        # module's docstring.
        verification_harness_closed = any(
            row.stage == "verification" and row.payload.get("manually_closed_by_harness")
            for row in incident_rows
        )

        if anomaly is not None and diagnosis is not None:
            s.detection_times_s.append((diagnosis.ts - anomaly.ts).total_seconds())

        if diagnosis is not None and diagnosis.payload.get("cause") in EXPECTED_CAUSES.get(
            scenario, frozenset()
        ):
            s.diagnosis_correct += 1

        if approval is not None and approval.payload.get("approved") is False:
            s.escalations += 1

        if anomaly is not None and verification is not None and not verification_harness_closed:
            s.mttr_s.append((verification.ts - anomaly.ts).total_seconds())
            if verification.payload.get("recovered") is True:
                s.remediation_success += 1

    return stats
