"""Tests for orchestrator.verifier — post-remediation recovery check.

Pure function tests: no database, cluster, or real Prometheus required.
`build_evidence_fn`, `check_threshold_fn`, and `sleep_fn` are injected fakes.

Review Focus: verify() performs exactly one check after the backoff wait —
no internal retry loop. If the metric is still breaching after the wait,
verify() must report recovered=False and return immediately. Retrying is
the orchestrator main loop's job (a later task), not this function's.
"""

from __future__ import annotations

from orchestrator import verifier
from orchestrator.models import EvidenceSummary

HEALTHY_EVIDENCE = EvidenceSummary(target="checkoutservice", restart_count_delta=0, error_rate=0.0)
BREACHING_EVIDENCE = EvidenceSummary(target="checkoutservice", restart_count_delta=1, error_rate=0.5)


def test_verify_recovered_when_no_longer_breaching():
    result = verifier.verify(
        "pod_crash",
        lambda: HEALTHY_EVIDENCE,
        lambda scenario, ev: False,
        sleep_fn=lambda s: None,
        backoff_seconds=0,
    )
    assert result.recovered is True


def test_verify_not_recovered_when_still_breaching():
    result = verifier.verify(
        "cpu_stress",
        lambda: BREACHING_EVIDENCE,
        lambda scenario, ev: True,
        sleep_fn=lambda s: None,
        backoff_seconds=0,
    )
    assert result.recovered is False


def test_verify_sleeps_for_backoff_before_checking():
    calls = []
    verifier.verify(
        "memory_pressure",
        lambda: HEALTHY_EVIDENCE,
        lambda scenario, ev: False,
        sleep_fn=calls.append,
        backoff_seconds=30,
    )
    assert calls == [30]


def test_verify_checks_threshold_exactly_once_no_retry_loop():
    """Review Focus: if the metric is still breaching after the backoff wait,
    verify() must write recovered=False and stop — it must NOT call
    build_evidence_fn or check_threshold_fn more than once, no matter how
    long backoff_seconds is or how persistently the metric breaches.
    """
    build_calls = []
    check_calls = []

    def build_evidence_fn():
        build_calls.append(1)
        return BREACHING_EVIDENCE

    def check_threshold_fn(scenario, evidence):
        check_calls.append(1)
        return True  # still breaching, every time it's asked

    result = verifier.verify(
        "pod_crash",
        build_evidence_fn,
        check_threshold_fn,
        sleep_fn=lambda s: None,
        backoff_seconds=30,
    )

    assert result.recovered is False
    assert len(build_calls) == 1
    assert len(check_calls) == 1


def test_verify_sets_checked_at_timestamp():
    result = verifier.verify(
        "pod_crash",
        lambda: HEALTHY_EVIDENCE,
        lambda scenario, ev: False,
        sleep_fn=lambda s: None,
        backoff_seconds=0,
    )
    assert result.checked_at is not None
