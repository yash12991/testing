"""Tests for `harness.run_experiments.compute_true_mttr_times`.

Pure-function test: no cluster, no Postgres. `EventRow` instances are
constructed directly (mirroring `tests/test_report.py`'s sys.path shim) to
import the real `orchestrator.db.EventRow` dataclass from the sibling
`orchestrator` package without requiring it to be `pip install`ed.
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

from harness.run_experiments import compute_true_mttr_times  # noqa: E402

INCIDENT_RECOVERED = uuid.uuid4()
INCIDENT_UNRECOVERED = uuid.uuid4()
T_INJECTION = datetime(2026, 10, 14, 12, 0, 0, tzinfo=timezone.utc)
T_VERIFICATION = T_INJECTION + timedelta(seconds=120)


def test_compute_true_mttr_times_includes_recovered_incident():
    trial_log = [
        {
            "scenario": "cpu_stress",
            "incident_id": INCIDENT_RECOVERED,
            "injection_ts": T_INJECTION,
            "outcome": "resolved",
            "target": "recommendationservice",
            "repeat": 1,
        }
    ]
    all_rows = [
        EventRow(1, INCIDENT_RECOVERED, "cpu_stress", "anomaly_detected", T_INJECTION, {}, None, None),
        EventRow(
            2,
            INCIDENT_RECOVERED,
            "cpu_stress",
            "verification",
            T_VERIFICATION,
            {"recovered": True},
            None,
            30,
        ),
    ]
    true_mttr_times = compute_true_mttr_times(trial_log, all_rows)
    assert true_mttr_times == {"cpu_stress": [(T_VERIFICATION - T_INJECTION).total_seconds()]}


def test_compute_true_mttr_times_excludes_unrecovered_incident():
    """A `cpu_stress` trial whose incident never actually recovered
    (`recovered: False`, e.g. the genuine outcome now that the
    `cpu_ratio` averaging artifact is fixed) must NOT contribute a
    "repair time" to the true-MTTR average — it never recovered, so
    there is no repair time to report."""
    trial_log = [
        {
            "scenario": "cpu_stress",
            "incident_id": INCIDENT_UNRECOVERED,
            "injection_ts": T_INJECTION,
            "outcome": "resolved",
            "target": "recommendationservice",
            "repeat": 1,
        }
    ]
    all_rows = [
        EventRow(1, INCIDENT_UNRECOVERED, "cpu_stress", "anomaly_detected", T_INJECTION, {}, None, None),
        EventRow(
            2,
            INCIDENT_UNRECOVERED,
            "cpu_stress",
            "verification",
            T_VERIFICATION,
            {"recovered": False},
            None,
            30,
        ),
    ]
    true_mttr_times = compute_true_mttr_times(trial_log, all_rows)
    assert true_mttr_times == {}


def test_compute_true_mttr_times_mixed_recovered_and_unrecovered():
    """Only the recovered incident's time should land in the average;
    the unrecovered one must be dropped, not zero-filled or averaged in."""
    trial_log = [
        {
            "scenario": "cpu_stress",
            "incident_id": INCIDENT_RECOVERED,
            "injection_ts": T_INJECTION,
            "outcome": "resolved",
            "target": "recommendationservice",
            "repeat": 1,
        },
        {
            "scenario": "cpu_stress",
            "incident_id": INCIDENT_UNRECOVERED,
            "injection_ts": T_INJECTION,
            "outcome": "resolved",
            "target": "recommendationservice",
            "repeat": 2,
        },
    ]
    all_rows = [
        EventRow(1, INCIDENT_RECOVERED, "cpu_stress", "verification", T_VERIFICATION, {"recovered": True}, None, 30),
        EventRow(2, INCIDENT_UNRECOVERED, "cpu_stress", "verification", T_VERIFICATION, {"recovered": False}, None, 30),
    ]
    true_mttr_times = compute_true_mttr_times(trial_log, all_rows)
    assert true_mttr_times == {"cpu_stress": [(T_VERIFICATION - T_INJECTION).total_seconds()]}


def test_compute_true_mttr_times_excludes_harness_closed_even_if_recovered():
    """A harness-synthesized `manually_closed_by_harness` row must still
    exclude the incident entirely, regardless of its `recovered` value —
    that timestamp reflects the harness giving up, not a real repair."""
    trial_log = [
        {
            "scenario": "cpu_stress",
            "incident_id": INCIDENT_RECOVERED,
            "injection_ts": T_INJECTION,
            "outcome": "timed_out",
            "target": "recommendationservice",
            "repeat": 1,
        }
    ]
    all_rows = [
        EventRow(
            1,
            INCIDENT_RECOVERED,
            "cpu_stress",
            "verification",
            T_VERIFICATION,
            {"recovered": True, "manually_closed_by_harness": True},
            None,
            30,
        ),
    ]
    true_mttr_times = compute_true_mttr_times(trial_log, all_rows)
    assert true_mttr_times == {}
