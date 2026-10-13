"""Tests for orchestrator.approver — policy gate.

Pure function tests: no database, cluster, or fakes required.
"""

from __future__ import annotations

from orchestrator import approver
from orchestrator.models import ApprovalDecision, Diagnosis


def test_approve_accepts_known_cause_above_threshold():
    d = Diagnosis(cause="transient_pod_failure", confidence=0.95, evidence_cited=[], action="restart_pod")
    result = approver.approve(d, confidence_threshold=0.8)
    assert result == ApprovalDecision(action="restart_pod", approved=True, reason="")


def test_approve_accepts_confidence_exactly_at_threshold():
    d = Diagnosis(cause="undersized_limit_or_leak", confidence=0.8, evidence_cited=[], action="restart_pod")
    result = approver.approve(d, confidence_threshold=0.8)
    assert result.approved is True


def test_approve_escalates_unknown_cause():
    d = Diagnosis(cause="unknown", confidence=0.0, evidence_cited=[], action=None)
    result = approver.approve(d, confidence_threshold=0.8)
    assert result.approved is False
    assert result.action is None


def test_approve_escalates_below_threshold():
    d = Diagnosis(cause="undersized_limit_or_leak", confidence=0.5, evidence_cited=[], action="restart_pod")
    result = approver.approve(d, confidence_threshold=0.8)
    assert result.approved is False


def test_approve_escalates_non_whitelisted_action():
    d = Diagnosis(cause="service_down", confidence=0.99, evidence_cited=[], action="reroute_traffic")
    result = approver.approve(d, confidence_threshold=0.8)
    assert result.approved is False
