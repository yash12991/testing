"""Integration test for orchestrator.main.run_once — the Postgres-driven main loop.

Unlike the other test modules, this one does not call any single stage
module directly: it drives `main.run_once` the same way the real poll loop
would, and asserts purely on what ends up persisted in `incident_events`.
This is the one test in the suite that exercises Observer, Diagnoser,
Approver, Fixer, and Verifier together, wired only through Postgres state
(`db.get_incomplete_incidents`/`db.get_events`), never through direct
function-to-function calls between stages.

Requires `TEST_DATABASE_URL` (see conftest.py / test_db.py) and sets
`VERIFY_BACKOFF_SECONDS=0` for the duration of each test so the Verifier's
real backoff wait (30s by default per the plan's Global Constraints,
env-overridable) doesn't make this test slow — the env var is read fresh by
`main.py` on every call, so overriding it here does not require reloading
the module.
"""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from orchestrator import db, main


class FakePromClient:
    """Routes on the same real PromQL substrings as test_observer.py's fake.

    With `breach_then_healthy_sequence=True`: the first `build_evidence`
    call (5 `.query()` calls, in the fixed order `build_evidence` issues
    them) reports a pod_crash breach (restart_count_delta > 0); every
    `build_evidence` call after that reports a fully healthy pod (used by
    `main.run_once`'s diagnosis step is read from the stored
    anomaly_detected payload rather than re-querying, so in practice only
    the Observer's initial poll and the Verifier's post-fix check ever call
    `build_evidence` for this test).
    """

    QUERIES_PER_BUILD = 5

    def __init__(self, breach_then_healthy_sequence: bool = False):
        self.breach_then_healthy_sequence = breach_then_healthy_sequence
        self.call_count = 0
        self.queries: list[str] = []

    def query(self, promql: str) -> float:
        self.queries.append(promql)
        self.call_count += 1
        breaching = self.breach_then_healthy_sequence and self.call_count <= self.QUERIES_PER_BUILD
        if "restart" in promql:
            return 3.0 if breaching else 0.0
        if 'status=~"5.."' in promql:
            return 0.0
        if "cpu" in promql:
            return 0.1
        if "bucket" in promql:
            return 50.0
        if "mem" in promql:
            return 0.1
        return 0.0


class FakeLokiClient:
    def query_range(self, logql: str) -> list[str]:
        return []


@pytest.fixture(autouse=True)
def fast_verify_backoff(monkeypatch):
    """Keep the Verifier's real wait out of this test's runtime."""
    monkeypatch.setenv("VERIFY_BACKOFF_SECONDS", "0")


def test_run_once_drives_one_incident_from_anomaly_to_verified_recovery(db_conn):
    fake_prom = FakePromClient(breach_then_healthy_sequence=True)  # breaches once, then reports healthy
    core_v1, apps_v1 = Mock(), Mock()
    # Mock(name=...) does not set a real .name attribute back (the `name`
    # kwarg is reserved for the mock's own repr — see test_fixer.py's
    # docstring), so it must be assigned after construction instead.
    pod = Mock()
    pod.metadata.name = "cartservice-abc"
    core_v1.list_namespaced_pod.return_value = Mock(items=[pod])

    main.run_once(
        db_conn, fake_prom, FakeLokiClient(), core_v1, apps_v1,
        scenarios=[("pod_crash", "cartservice")], confidence_threshold=0.8,
    )
    incident_id = db.get_open_incident(db_conn, "pod_crash", "cartservice")
    assert incident_id is not None  # anomaly created, not yet resolved

    main.run_once(
        db_conn, fake_prom, FakeLokiClient(), core_v1, apps_v1,
        scenarios=[("pod_crash", "cartservice")], confidence_threshold=0.8,
    )
    # second pass: diagnosis + approval + fix_executed should now exist
    stages = [e.stage for e in db.get_events(db_conn, incident_id)]
    assert "fix_executed" in stages

    main.run_once(
        db_conn, fake_prom, FakeLokiClient(), core_v1, apps_v1,
        scenarios=[("pod_crash", "cartservice")], confidence_threshold=0.8,
    )
    stages = [e.stage for e in db.get_events(db_conn, incident_id)]
    assert stages == ["anomaly_detected", "diagnosis", "approval", "fix_executed", "verification"]
    assert db.get_open_incident(db_conn, "pod_crash", "cartservice") is None

    # The pod really was deleted via the K8s mock, confirming the Fixer stage
    # actually ran against core_v1 rather than being skipped/faked.
    core_v1.delete_namespaced_pod.assert_called_once_with(name="cartservice-abc", namespace="online-boutique")


def test_run_once_does_not_advance_past_approval_when_escalated(db_conn):
    """A diagnosis the Approver rejects must never reach fix_executed/verification.

    `pod_crash` always diagnoses as `transient_pod_failure` with the fixed
    confidence 0.95 (see diagnoser.py). Setting `confidence_threshold=0.99`
    (above every rule's fixed confidence) forces the Approver to escalate
    regardless of which rule fired — Fixer must never be invoked.
    """
    fake_prom = FakePromClient(breach_then_healthy_sequence=True)
    core_v1, apps_v1 = Mock(), Mock()

    main.run_once(
        db_conn, fake_prom, FakeLokiClient(), core_v1, apps_v1,
        scenarios=[("pod_crash", "cartservice")], confidence_threshold=0.99,
    )
    incident_id = db.get_open_incident(db_conn, "pod_crash", "cartservice")
    assert incident_id is not None

    main.run_once(
        db_conn, fake_prom, FakeLokiClient(), core_v1, apps_v1,
        scenarios=[("pod_crash", "cartservice")], confidence_threshold=0.99,
    )
    stages = [e.stage for e in db.get_events(db_conn, incident_id)]
    assert stages == ["anomaly_detected", "diagnosis", "approval"]

    events = db.get_events(db_conn, incident_id)
    approval_event = next(e for e in events if e.stage == "approval")
    assert approval_event.payload["approved"] is False

    core_v1.delete_namespaced_pod.assert_not_called()

    # Polling further must not change anything: no fix, no verification, no
    # re-diagnosis/re-approval, and the incident stays "incomplete" forever
    # (an escalated incident awaits a human, by design).
    main.run_once(
        db_conn, fake_prom, FakeLokiClient(), core_v1, apps_v1,
        scenarios=[("pod_crash", "cartservice")], confidence_threshold=0.99,
    )
    stages_after = [e.stage for e in db.get_events(db_conn, incident_id)]
    assert stages_after == stages
    assert db.get_incomplete_incidents(db_conn) == [incident_id]
