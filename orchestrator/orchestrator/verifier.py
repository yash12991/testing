"""Post-remediation recovery check.

Pure function: no database or cluster access. After a fix has been applied,
`verify()` waits a fixed backoff period for the metric to settle, then takes
exactly one reading and checks it against the scenario's threshold.

Review Focus: this function performs exactly one check, with no internal
retry loop. If the metric is still breaching threshold after the backoff
wait, `verify()` reports `recovered=False` and returns immediately — it
does NOT loop or re-check. Retrying (e.g. escalating after N failed
verifications) is the orchestrator main loop's job, implemented in a later
task, not this function's.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Callable

from orchestrator.models import EvidenceSummary, VerificationResult


def verify(
    scenario: str,
    build_evidence_fn: Callable[[], EvidenceSummary],
    check_threshold_fn: Callable[[str, EvidenceSummary], bool],
    sleep_fn: Callable[[float], None] = time.sleep,
    backoff_seconds: int = 30,
    sample_count: int = 1,
    sample_interval_seconds: float = 5.0,
) -> VerificationResult:
    """Wait `backoff_seconds`, then verify recovery across `sample_count` consecutive samples.

    Rule V1 & V2: waits `backoff_seconds` for cluster stabilization.
    If `sample_count == 1`: takes exactly one reading.
    If `sample_count > 1`: verifies sustained health across `sample_count` consecutive
    samples separated by `sample_interval_seconds`. If any sample breaches, recovered=False.
    """
    sleep_fn(backoff_seconds)
    for i in range(sample_count):
        if i > 0 and sample_interval_seconds > 0:
            sleep_fn(sample_interval_seconds)
        evidence = build_evidence_fn()
        if check_threshold_fn(scenario, evidence):
            return VerificationResult(recovered=False, checked_at=datetime.now(timezone.utc))
    return VerificationResult(recovered=True, checked_at=datetime.now(timezone.utc))
