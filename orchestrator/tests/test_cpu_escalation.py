"""Test verifying the multi-tier escalation control flow for CPU stress.

Proves:
1. When CPU stress occurs, initial action is `scale_replicas`.
2. When verification fails after scaling (simulating single-pod pinned stress chaos),
   and MAX_RETRIES >= 2, the pipeline escalates to `restart_pod`.
3. When subsequent verification passes after restarting the pod, the incident resolves.
"""

from __future__ import annotations

from unittest.mock import Mock
import pytest

from orchestrator import db, main
from orchestrator.models import EvidenceSummary


class EscalationPromClient:
    def __init__(self):
        # Calls:
        # 1. Detection (queries 1..6): breaching CPU
        # 2. First verification (queries 7..12): still breaching CPU (scaling did not cure pinned pod)
        # 3. Second verification (queries 13..18): healthy (pod restart cured pinned pod)
        self.call_count = 0

    def query(self, promql: str) -> float:
        self.call_count += 1
        if "cpu" in promql:
            # Breach during detection and first verification; healthy on second verification
            if self.call_count <= 14:
                return 0.98  # Still breaching!
            return 0.05  # Healthy after restart!
        if "replicas_available" in promql:
            return 1.0
        return 0.0


class DummyLokiClient:
    def query_range(self, logql: str, window_seconds: int = 3600):
        return []


def test_cpu_stress_escalation_from_scale_to_restart(db_conn, monkeypatch):
    monkeypatch.setenv("VERIFY_BACKOFF_SECONDS", "0")
    monkeypatch.setenv("VERIFY_SAMPLE_COUNT", "1")
    monkeypatch.setenv("MAX_RETRIES", "2")  # Enable multi-tier retry

    prom_client = EscalationPromClient()
    core_v1, apps_v1 = Mock(), Mock()
    pod = Mock()
    pod.metadata.name = "recommendationservice-hotpod"
    core_v1.list_namespaced_pod.return_value = Mock(items=[pod])

    # --- Cycle 1: Detection & Initial Fix (scale_replicas) ---
    main.run_once(
        db_conn,
        prom_client,
        DummyLokiClient(),
        core_v1,
        apps_v1,
        scenarios=[("cpu_stress", "recommendationservice")],
        confidence_threshold=0.8,
    )

    incident_id = db.get_open_incident(db_conn, "cpu_stress", "recommendationservice")
    assert incident_id is not None
    events_1 = db.get_events(db_conn, incident_id)
    stages_1 = [e.stage for e in events_1]
    assert stages_1 == ["anomaly_detected", "diagnosis", "approval", "fix_executed"]
    assert events_1[3].payload["action"] == "scale_replicas"
    apps_v1.patch_namespaced_deployment_scale.assert_called_once()
    core_v1.delete_namespaced_pod.assert_not_called()

    # --- Cycle 2: First Verification Fails -> Multi-tier Escalation to restart_pod ---
    main.run_once(
        db_conn,
        prom_client,
        DummyLokiClient(),
        core_v1,
        apps_v1,
        scenarios=[("cpu_stress", "recommendationservice")],
        confidence_threshold=0.8,
    )

    events_2 = db.get_events(db_conn, incident_id)
    stages_2 = [e.stage for e in events_2]
    # Expect second fix_executed row with action="restart_pod"
    assert stages_2 == ["anomaly_detected", "diagnosis", "approval", "fix_executed", "fix_executed"]
    assert events_2[4].payload["action"] == "restart_pod"
    assert events_2[4].payload["retry_attempt"] == 2
    core_v1.delete_namespaced_pod.assert_called_once_with(name="recommendationservice-hotpod", namespace="online-boutique")

    # --- Cycle 3: Second Verification Succeeds (Pod restart cleared CPU stress) ---
    main.run_once(
        db_conn,
        prom_client,
        DummyLokiClient(),
        core_v1,
        apps_v1,
        scenarios=[("cpu_stress", "recommendationservice")],
        confidence_threshold=0.8,
    )

    events_3 = db.get_events(db_conn, incident_id)
    stages_3 = [e.stage for e in events_3]
    assert stages_3 == ["anomaly_detected", "diagnosis", "approval", "fix_executed", "fix_executed", "verification"]
    assert events_3[5].payload["recovered"] is True
    assert events_3[5].payload["attempts"] == 2
    assert db.get_open_incident(db_conn, "cpu_stress", "recommendationservice") is None
