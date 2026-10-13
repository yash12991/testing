"""Diagnoser stage: rule-based mapping from evidence to a likely cause.

`diagnose` is a pure function over an `EvidenceSummary`. It evaluates three
rules in a fixed order and returns the first match:

1. Memory pressure-> `undersized_limit_or_leak` (confidence 0.85, restart_pod)
   Fires on `oom_killed` or `mem_ratio > 0.9`.
2. Pod crash      -> `transient_pod_failure`  (confidence 0.95, restart_pod)
   Fires on `restart_count_delta > 0`.
3. CPU stress     -> `resource_exhaustion`    (confidence 0.9,  scale_replicas)
   Fires on `cpu_ratio > 0.85`.

Memory pressure is checked FIRST, ahead of pod crash, even though it has the
lower fixed confidence. This is a deliberate fix (final-review Fix 2): since
`observer.build_evidence` gates `oom_killed` on `restart_count_delta > 0`
within the same short lookback window (a real OOM kill always also bumps the
restart count by construction — see observer.py's comment on `oom_killed`),
checking the pod_crash rule first would mean the pod_crash rule always wins
for a genuine OOM incident and the memory_pressure rule's `oom_killed` branch
could never fire in production. Checking memory_pressure first means a real
OOM-with-restart now correctly diagnoses as `undersized_limit_or_leak`. A
plain container-kill with no OOM signal still falls through to the pod_crash
rule unaffected, since `oom_killed` is false for it.

Rules 2 and 3 were simplified from their original two-leg conditions
(restart_count_delta > 0 OR error_rate > 0.1; cpu_ratio > 0.85 AND
p99_latency_rising) down to their single reliable leg each, mirroring
`observer.check_threshold`'s simplification: no Online Boutique service
exports http_requests_total or any latency histogram, so error_rate and
p99_latency_rising are always 0.0/False in practice.

The thresholds mirror `observer.check_threshold` so that anything the Observer
flags is diagnosable. The order only matters when evidence satisfies more than
one rule; the first rule in the list above wins. If no rule matches, the result
is `unknown` (confidence 0.0, action None) rather than a guessed cause.
"""

from __future__ import annotations

from orchestrator.models import Diagnosis, EvidenceSummary

TRANSIENT_POD_FAILURE_CONFIDENCE = 0.95
RESOURCE_EXHAUSTION_CONFIDENCE = 0.9
UNDERSIZED_LIMIT_OR_LEAK_CONFIDENCE = 0.85
UNKNOWN_CONFIDENCE = 0.0


def diagnose(evidence: EvidenceSummary) -> Diagnosis:
    """Map `evidence` to a `Diagnosis` using the fixed decision table.

    Rule order is memory_pressure, then pod_crash, then cpu_stress — see
    this module's docstring for why memory_pressure must be checked before
    pod_crash.
    """
    # Rule 1: memory pressure. Checked BEFORE pod crash: see module docstring.
    memory_fields = []
    if evidence.oom_killed:
        memory_fields.append("oom_killed")
    if evidence.mem_ratio > 0.9:
        memory_fields.append("mem_ratio")
    if memory_fields:
        return Diagnosis(
            cause="undersized_limit_or_leak",
            confidence=UNDERSIZED_LIMIT_OR_LEAK_CONFIDENCE,
            evidence_cited=memory_fields,
            action="restart_pod",
        )

    # Rule 2: pod crash.
    if evidence.restart_count_delta > 0:
        return Diagnosis(
            cause="transient_pod_failure",
            confidence=TRANSIENT_POD_FAILURE_CONFIDENCE,
            evidence_cited=["restart_count_delta"],
            action="restart_pod",
        )

    # Rule 3: CPU stress.
    if evidence.cpu_ratio > 0.85:
        return Diagnosis(
            cause="resource_exhaustion",
            confidence=RESOURCE_EXHAUSTION_CONFIDENCE,
            evidence_cited=["cpu_ratio"],
            action="scale_replicas",
        )

    # No rule matched.
    return Diagnosis(
        cause="unknown",
        confidence=UNKNOWN_CONFIDENCE,
        evidence_cited=[],
        action=None,
    )
