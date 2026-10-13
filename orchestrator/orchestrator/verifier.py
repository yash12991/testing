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
) -> VerificationResult:
    """Wait `backoff_seconds`, take one reading, and report whether it recovered.

    Calls `sleep_fn(backoff_seconds)` once, then `build_evidence_fn()` once,
    then `check_threshold_fn(scenario, evidence)` once. `recovered` is True
    iff the threshold check comes back False (no longer breaching).
    """
    sleep_fn(backoff_seconds)
    evidence = build_evidence_fn()
    recovered = not check_threshold_fn(scenario, evidence)
    return VerificationResult(recovered, checked_at=datetime.now(timezone.utc))
