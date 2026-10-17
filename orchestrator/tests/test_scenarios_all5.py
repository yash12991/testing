"""End-to-end test exercising all five deterministic fault scenarios.

Scenarios tested:
1. pod_crash (cartservice) -> transient_pod_failure -> restart_pod
2. cpu_stress (recommendationservice) -> resource_exhaustion -> scale_replicas
3. memory_pressure (productcatalogservice) -> undersized_limit_or_leak -> restart_pod
4. network_latency (shippingservice) -> degraded_dependency_or_network_congestion -> restart_pod
5. service_unavailability (paymentservice) -> service_down_or_unreachable -> rollback_deployment
"""

from __future__ import annotations

from unittest.mock import Mock
import pytest

from orchestrator import db, main
from orchestrator.models import EvidenceSummary


class MultiScenarioPromClient:
    def __init__(self, scenario_states: dict[str, bool]):
        # scenario_states: map from scenario to whether it's currently breaching (True) or healthy (False)
        self.scenario_states = scenario_states
        self.queries = []

    def query(self, promql: str) -> float:
        self.queries.append(promql)
        # Identify target from query
        for scenario, is_breaching in self.scenario_states.items():
            if not is_breaching:
                continue
            if "cartservice" in promql and "restart" in promql:
                return 3.0
            if "recommendationservice" in promql and "cpu" in promql:
                return 0.95
            if "productcatalogservice" in promql and "OOMKilled" in promql:
                return 1.0
            if "productcatalogservice" in promql and "mem" in promql:
                return 0.95
            if "shippingservice" in promql and "bucket" in promql:
                return 250.0
            if "paymentservice" in promql and "replicas_available" in promql:
                return 0.0

        # Default healthy metrics
        if "replicas_available" in promql:
            return 1.0
        if "cpu" in promql or "mem" in promql:
            return 0.1
        if "bucket" in promql:
            return 40.0
        return 0.0


class DummyLokiClient:
    def query_range(self, logql: str, window_seconds: int = 3600):
        return []


@pytest.fixture(autouse=True)
def fast_verify_backoff(monkeypatch):
    monkeypatch.setenv("VERIFY_BACKOFF_SECONDS", "0")
    monkeypatch.setenv("VERIFY_SAMPLE_COUNT", "1")


@pytest.mark.parametrize(
    "scenario,target,expected_cause,expected_action",
    [
        ("pod_crash", "cartservice", "transient_pod_failure", "restart_pod"),
        ("cpu_stress", "recommendationservice", "resource_exhaustion", "scale_replicas"),
        ("memory_pressure", "productcatalogservice", "undersized_limit_or_leak", "restart_pod"),
        ("network_latency", "shippingservice", "degraded_dependency_or_network_congestion", "restart_pod"),
        ("service_unavailability", "paymentservice", "service_down_or_unreachable", "rollback_deployment"),
    ],
)
def test_end_to_end_scenario_lifecycle(db_conn, scenario, target, expected_cause, expected_action):
    # Phase 1: Breach active
    prom_client = MultiScenarioPromClient({scenario: True})
    core_v1, apps_v1 = Mock(), Mock()
    pod = Mock()
    pod.metadata.name = f"{target}-pod-1"
    core_v1.list_namespaced_pod.return_value = Mock(items=[pod])

    # Run cycle 1: Anomaly detected, Diagnosed, Approved, Fix executed
    main.run_once(
        db_conn,
        prom_client,
        DummyLokiClient(),
        core_v1,
        apps_v1,
        scenarios=[(scenario, target)],
        confidence_threshold=0.8,
    )

    incident_id = db.get_open_incident(db_conn, scenario, target)
    assert incident_id is not None
    events_1 = db.get_events(db_conn, incident_id)
    stages_1 = [e.stage for e in events_1]
    assert stages_1 == ["anomaly_detected", "diagnosis", "approval", "fix_executed"]

    diag_event = next(e for e in events_1 if e.stage == "diagnosis")
    assert diag_event.payload["cause"] == expected_cause
    assert diag_event.payload["action"] == expected_action

    appr_event = next(e for e in events_1 if e.stage == "approval")
    assert appr_event.payload["approved"] is True
    assert appr_event.payload["action"] == expected_action

    fix_event = next(e for e in events_1 if e.stage == "fix_executed")
    assert fix_event.payload["action"] == expected_action
    assert fix_event.payload["success"] is True

    # Phase 2: Remediated (Healthy metrics)
    prom_client.scenario_states[scenario] = False

    # Run cycle 2: Verification runs and reports recovered=True
    main.run_once(
        db_conn,
        prom_client,
        DummyLokiClient(),
        core_v1,
        apps_v1,
        scenarios=[(scenario, target)],
        confidence_threshold=0.8,
    )

    events_2 = db.get_events(db_conn, incident_id)
    stages_2 = [e.stage for e in events_2]
    assert stages_2 == ["anomaly_detected", "diagnosis", "approval", "fix_executed", "verification"]

    verif_event = next(e for e in events_2 if e.stage == "verification")
    assert verif_event.payload["recovered"] is True
    assert db.get_open_incident(db_conn, scenario, target) is None
