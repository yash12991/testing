"""Tests for `harness.report.aggregate_results`.

Pure-function tests: no cluster, no Postgres. `EventRow` instances are
constructed directly (mirroring the shape of `orchestrator.db.get_events`'s
return value, per Task 17's brief) using a small sys.path shim to import the
real `orchestrator.db.EventRow` dataclass from the sibling `orchestrator`
package without requiring it to be `pip install`ed — this keeps the test
fixtures byte-for-byte faithful to the real row shape `run_experiments.py`
will actually see, while `harness.report` itself stays a pure, orchestrator-
independent module (see its module docstring).
"""

from __future__ import annotations

import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

_ORCHESTRATOR_SRC = Path(__file__).resolve().parents[2] / "orchestrator"
if str(_ORCHESTRATOR_SRC) not in sys.path:
    sys.path.insert(0, str(_ORCHESTRATOR_SRC))

from orchestrator.db import EventRow  # noqa: E402

from harness import report  # noqa: E402

ID1 = uuid.uuid4()
ID2 = uuid.uuid4()
T0 = datetime(2026, 10, 14, 12, 0, 0, tzinfo=timezone.utc)
T1 = T0 + timedelta(seconds=1)
T2 = T0 + timedelta(seconds=45)


def test_aggregate_results_computes_per_scenario_stats():
    rows = [
        EventRow(1, ID1, "pod_crash", "anomaly_detected", T0, {}, None, None),
        EventRow(
            2,
            ID1,
            "pod_crash",
            "diagnosis",
            T1,
            {"cause": "transient_pod_failure", "confidence": 0.95, "evidence_cited": ["restart_count_delta"], "action": "restart_pod"},
            0.95,
            50,
        ),
        EventRow(3, ID1, "pod_crash", "approval", T1, {"action": "restart_pod", "approved": True, "reason": ""}, 0.95, 5),
        EventRow(4, ID1, "pod_crash", "fix_executed", T1, {"action": "restart_pod", "target": "cartservice", "success": True, "detail": ""}, None, 20),
        EventRow(5, ID1, "pod_crash", "verification", T2, {"recovered": True}, None, 30),
    ]
    stats = report.aggregate_results(rows)
    assert stats["pod_crash"].runs == 1
    assert stats["pod_crash"].remediation_success == 1
    assert stats["pod_crash"].mttr_s == [(T2 - T0).total_seconds()]
    assert stats["pod_crash"].diagnosis_correct == 1
    assert stats["pod_crash"].escalations == 0


def test_aggregate_results_counts_escalation_when_no_fix_executed():
    rows = [
        EventRow(6, ID2, "pod_crash", "anomaly_detected", T0, {}, None, None),
        EventRow(7, ID2, "pod_crash", "diagnosis", T1, {"cause": "unknown", "confidence": 0.0, "evidence_cited": [], "action": None}, 0.0, 50),
        EventRow(8, ID2, "pod_crash", "approval", T1, {"action": None, "approved": False, "reason": "no action proposed"}, None, 5),
    ]
    stats = report.aggregate_results(rows)
    assert stats["pod_crash"].escalations == 1
    assert stats["pod_crash"].remediation_success == 0
    assert stats["pod_crash"].mttr_s == []
    assert stats["pod_crash"].diagnosis_correct == 0


def test_aggregate_results_groups_multiple_incidents_and_scenarios():
    id3 = uuid.uuid4()
    rows = [
        EventRow(1, ID1, "pod_crash", "anomaly_detected", T0, {}, None, None),
        EventRow(2, ID1, "pod_crash", "diagnosis", T1, {"cause": "transient_pod_failure", "confidence": 0.95, "evidence_cited": [], "action": "restart_pod"}, 0.95, 50),
        EventRow(3, ID1, "pod_crash", "approval", T1, {"action": "restart_pod", "approved": True, "reason": ""}, 0.95, 5),
        EventRow(4, ID1, "pod_crash", "fix_executed", T1, {"action": "restart_pod", "target": "cartservice", "success": True, "detail": ""}, None, 20),
        EventRow(5, ID1, "pod_crash", "verification", T2, {"recovered": True}, None, 30),
        EventRow(9, id3, "cpu_stress", "anomaly_detected", T0, {}, None, None),
        EventRow(10, id3, "cpu_stress", "diagnosis", T1, {"cause": "resource_exhaustion", "confidence": 0.9, "evidence_cited": [], "action": "scale_replicas"}, 0.9, 50),
        EventRow(11, id3, "cpu_stress", "approval", T1, {"action": "scale_replicas", "approved": True, "reason": ""}, 0.9, 5),
        EventRow(12, id3, "cpu_stress", "fix_executed", T1, {"action": "scale_replicas", "target": "recommendationservice", "success": True, "detail": ""}, None, 20),
        EventRow(13, id3, "cpu_stress", "verification", T2, {"recovered": False}, None, 30),
    ]
    stats = report.aggregate_results(rows)
    assert set(stats.keys()) == {"pod_crash", "cpu_stress"}
    assert stats["pod_crash"].runs == 1
    assert stats["cpu_stress"].runs == 1
    assert stats["cpu_stress"].remediation_success == 0
    assert stats["cpu_stress"].mttr_s == [(T2 - T0).total_seconds()]


def test_aggregate_results_empty_input_returns_empty_dict():
    assert report.aggregate_results([]) == {}


def test_aggregate_results_excludes_harness_closed_verification_from_mttr():
    """A `verification` row the harness synthesized after an escalation or
    resolution timeout (`manually_closed_by_harness: True`, see
    `run_experiments.py`'s module docstring) must not feed `mttr_s` or
    `remediation_success` — that timestamp reflects how long the harness
    polled before giving up (or its own timeout constant), not a real
    repair time.
    """
    id5 = uuid.uuid4()
    rows = [
        EventRow(1, id5, "pod_crash", "anomaly_detected", T0, {}, None, None),
        EventRow(2, id5, "pod_crash", "diagnosis", T1, {"cause": "transient_pod_failure", "confidence": 0.95, "evidence_cited": [], "action": "restart_pod"}, 0.95, 50),
        EventRow(3, id5, "pod_crash", "approval", T1, {"action": None, "approved": False, "reason": "no action proposed"}, None, 5),
        EventRow(
            4,
            id5,
            "pod_crash",
            "verification",
            T2,
            {"recovered": False, "manually_closed_by_harness": True, "reason": "escalated_by_approver"},
            None,
            None,
        ),
    ]
    stats = report.aggregate_results(rows)
    assert stats["pod_crash"].mttr_s == []
    assert stats["pod_crash"].remediation_success == 0
    # escalation counting (approval.approved is False) is untouched by this
    assert stats["pod_crash"].escalations == 1


def test_aggregate_results_harness_closed_wins_even_if_real_verification_is_later():
    """`by_stage` only keeps the LAST row per stage (rows are id-ordered),
    so if the harness's synthetic failure row is followed by a later,
    genuine orchestrator `verification` row with `recovered: True` (e.g. a
    race where the pipeline actually finishes resolving an incident the
    harness had already given up on), the incident must still be treated
    as harness-closed — a late real row must not flip a timed-out/
    escalated incident into a counted success.
    """
    id6 = uuid.uuid4()
    rows = [
        EventRow(1, id6, "pod_crash", "anomaly_detected", T0, {}, None, None),
        EventRow(
            2,
            id6,
            "pod_crash",
            "verification",
            T1,
            {"recovered": False, "manually_closed_by_harness": True, "reason": "harness_resolution_timeout"},
            None,
            None,
        ),
        # Arrives later (higher id) and would win in a naive "last row per
        # stage" dict.
        EventRow(3, id6, "pod_crash", "verification", T2, {"recovered": True}, None, 30),
    ]
    stats = report.aggregate_results(rows)
    assert stats["pod_crash"].mttr_s == []
    assert stats["pod_crash"].remediation_success == 0


def test_aggregate_results_memory_pressure_accepts_only_undersized_limit_or_leak():
    """Final-review Fix 2: diagnoser.diagnose now checks the memory_pressure
    rule BEFORE the pod_crash rule, so a real OOM kill (which also
    increments restart_count_delta by construction) correctly diagnoses as
    `undersized_limit_or_leak`, not `transient_pod_failure`. EXPECTED_CAUSES
    for memory_pressure no longer accepts `transient_pod_failure` as an
    "accommodation" for the old (now-fixed) rule-priority quirk - accepting
    it would mask a genuine misdiagnosis.
    """
    id4 = uuid.uuid4()
    rows = [
        EventRow(1, id4, "memory_pressure", "anomaly_detected", T0, {}, None, None),
        EventRow(2, id4, "memory_pressure", "diagnosis", T1, {"cause": "undersized_limit_or_leak", "confidence": 0.85, "evidence_cited": ["oom_killed"], "action": "restart_pod"}, 0.85, 50),
    ]
    stats = report.aggregate_results(rows)
    assert stats["memory_pressure"].diagnosis_correct == 1


def test_aggregate_results_memory_pressure_rejects_transient_pod_failure_as_incorrect():
    """A memory_pressure incident diagnosed as `transient_pod_failure` after
    the rule-order fix would mean the pod_crash rule fired instead of the
    memory rule - a genuine misdiagnosis, not the old accepted quirk - and
    must now be scored as incorrect.
    """
    id7 = uuid.uuid4()
    rows = [
        EventRow(1, id7, "memory_pressure", "anomaly_detected", T0, {}, None, None),
        EventRow(2, id7, "memory_pressure", "diagnosis", T1, {"cause": "transient_pod_failure", "confidence": 0.95, "evidence_cited": ["restart_count_delta"], "action": "restart_pod"}, 0.95, 50),
    ]
    stats = report.aggregate_results(rows)
    assert stats["memory_pressure"].diagnosis_correct == 0
