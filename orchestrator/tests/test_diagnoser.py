"""Tests for orchestrator.diagnoser — rule-based decision table.

Pure function tests: no database, cluster, or fakes required.
"""

from __future__ import annotations

from orchestrator import diagnoser
from orchestrator.models import EvidenceSummary


def test_diagnose_pod_crash_evidence_yields_transient_pod_failure():
    evidence = EvidenceSummary(restart_count_delta=2, error_rate=0.2, cpu_ratio=0.1,
                                p99_latency_ms=50, p99_latency_rising=False,
                                oom_killed=False, mem_ratio=0.1, target="cartservice")
    d = diagnoser.diagnose(evidence)
    assert d.cause == "transient_pod_failure"
    assert d.confidence == 0.95
    assert d.action == "restart_pod"

def test_diagnose_cpu_stress_evidence_yields_resource_exhaustion():
    evidence = EvidenceSummary(restart_count_delta=0, error_rate=0.0, cpu_ratio=0.9,
                                p99_latency_ms=800, p99_latency_rising=True,
                                oom_killed=False, mem_ratio=0.1, target="recommendationservice")
    d = diagnoser.diagnose(evidence)
    assert d.cause == "resource_exhaustion"
    assert d.confidence == 0.9
    assert d.action == "scale_replicas"

def test_diagnose_memory_pressure_evidence_yields_undersized_limit_or_leak():
    evidence = EvidenceSummary(restart_count_delta=0, error_rate=0.0, cpu_ratio=0.1,
                                p99_latency_ms=50, p99_latency_rising=False,
                                oom_killed=True, mem_ratio=0.95, target="productcatalogservice")
    d = diagnoser.diagnose(evidence)
    assert d.cause == "undersized_limit_or_leak"
    assert d.confidence == 0.85
    assert d.action == "restart_pod"

def test_diagnose_unrecognized_evidence_yields_unknown():
    evidence = EvidenceSummary(restart_count_delta=0, error_rate=0.0, cpu_ratio=0.1,
                                p99_latency_ms=50, p99_latency_rising=False,
                                oom_killed=False, mem_ratio=0.1, target="loadgenerator")
    d = diagnoser.diagnose(evidence)
    assert d.cause == "unknown"
    assert d.confidence == 0.0
    assert d.action is None
    assert d.evidence_cited == []
