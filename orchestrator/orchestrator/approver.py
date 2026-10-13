"""Policy gate deciding whether a diagnosis may be auto-remediated.

Pure function: no database or cluster access. A diagnosis is approved only
when it names a whitelisted action and meets the confidence threshold;
everything else is escalated to a human.
"""

from __future__ import annotations

from orchestrator.models import ApprovalDecision, Diagnosis

WHITELISTED_ACTIONS: frozenset[str] = frozenset({"restart_pod", "scale_replicas", "rollback_deployment"})


def approve(diagnosis: Diagnosis, confidence_threshold: float = 0.8) -> ApprovalDecision:
    """Return an ApprovalDecision for the given diagnosis."""
    if diagnosis.action is None:
        reason = "no action proposed"
    elif diagnosis.action not in WHITELISTED_ACTIONS:
        reason = f"action {diagnosis.action!r} is not whitelisted"
    elif diagnosis.confidence < confidence_threshold:
        reason = f"confidence {diagnosis.confidence} below threshold {confidence_threshold}"
    else:
        return ApprovalDecision(action=diagnosis.action, approved=True, reason="")
    return ApprovalDecision(action=diagnosis.action, approved=False, reason=reason)
