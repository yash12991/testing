"""Data models shared across the orchestrator pipeline stages.

These dataclasses are the typed contracts passed between the Observer
(diagnosis), Decision (approval), Fix (remediation), and Verification
stages of the chaos-to-remediation loop. They intentionally carry no
behavior beyond simple field storage, so each stage's unit tests can
construct them directly without a database or cluster connection.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass
class EvidenceSummary:
    """Aggregated signals the Observer gathered about a target service.

    Fields are populated opportunistically depending on which metrics/logs
    were available and relevant to the scenario being diagnosed; fields
    that don't apply to a given incident are left at their defaults.
    """

    target: str
    restart_count_delta: int = 0
    error_rate: float = 0.0
    cpu_ratio: float = 0.0
    p99_latency_ms: float = 0.0
    p99_latency_rising: bool = False
    oom_killed: bool = False
    mem_ratio: float = 0.0
    # True when the worst matching pod's CPU usage is far above the average
    # across all matching pods (or there's only one pod at all) - i.e. one
    # specific pod is saturated while its siblings, if any, are not. False
    # means the load is roughly spread across all matching pods instead.
    # Live-verified (2026-10-20): deleting the single hotspot pod under a
    # real Chaos Mesh StressChaos injection ended the stress immediately -
    # the stress process lives inside that specific container and Chaos
    # Mesh does not re-select a replacement pod - so `restart_pod` is the
    # correct fix for a hotspot, not `scale_replicas` (which only adds idle
    # siblings next to the still-saturated pod and never touches it).
    cpu_hotspot: bool = False


@dataclass
class Diagnosis:
    cause: str
    confidence: float
    evidence_cited: list[str]
    action: str | None


@dataclass
class ApprovalDecision:
    action: str | None
    approved: bool
    reason: str


@dataclass
class FixResult:
    action: str
    target: str
    success: bool
    detail: str


@dataclass
class VerificationResult:
    recovered: bool
    checked_at: datetime
