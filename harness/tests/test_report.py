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


def test_aggregate_results_memory_pressure_diagnosis_correct_accepts_rule_priority_quirk():
    """documented in orchestrator/orchestrator/main.py's module docstring and
    task-16-report.md section 11: because diagnoser.diagnose checks the
    pod_crash rule before the memory_pressure rule, a real OOM kill that
    also increments restart_count_delta (the common case) is classified as
    `transient_pod_failure`, not `undersized_limit_or_leak` - both map to
    the same `restart_pod` action and this is documented, correct,
    non-buggy behavior, so a harness scoring diagnosis correctness for
    memory_pressure must accept either cause.
    """
    id4 = uuid.uuid4()
    rows = [
        EventRow(1, id4, "memory_pressure", "anomaly_detected", T0, {}, None, None),
        EventRow(2, id4, "memory_pressure", "diagnosis", T1, {"cause": "transient_pod_failure", "confidence": 0.95, "evidence_cited": ["restart_count_delta"], "action": "restart_pod"}, 0.95, 50),
    ]
    stats = report.aggregate_results(rows)
    assert stats["memory_pressure"].diagnosis_correct == 1
