"""Observer stage: evidence gathering, threshold checks, and incident dedup.

This module is the entry point of the chaos-to-remediation loop: it polls
Prometheus/Loki for signals about a `(scenario, target)` pair, decides
whether those signals breach a scenario-specific threshold, and — if so and
there isn't already an open incident for that pair — opens a new incident
and logs the `anomaly_detected` event that downstream stages (diagnosis,
approval, fix, verification) pick up.

`prom_client`/`loki_client` are thin wrapper objects (see Task 16 for the
real implementations against a live Prometheus/Loki) exposing:

    prom_client.query(promql: str) -> float
    loki_client.query_range(logql: str) -> list[str]

Tests pass in fakes implementing the same interface, so this module never
makes real HTTP calls itself.
"""

from __future__ import annotations

from typing import Any, Protocol

from . import db
from .models import EvidenceSummary


class PromClient(Protocol):
    def query(self, promql: str) -> float: ...


class LokiClient(Protocol):
    def query_range(self, logql: str) -> list[str]: ...


def build_evidence(
    prom_client: PromClient,
    loki_client: LokiClient,
    scenario: str,
    target: str,
    window_seconds: int,
) -> EvidenceSummary:
    """Query Prometheus/Loki for the signals relevant to `scenario` and
    `target`, and assemble them into an `EvidenceSummary`.

    The exact PromQL/LogQL strings issued here are an implementation detail
    not pinned by unit tests (the real queries against a live Prometheus/
    Loki are written in Task 16); what matters is that every signal a
    scenario's threshold check depends on gets populated — with one known
    exception: `p99_latency_rising` is a stub (see the comment at its
    assignment below) and is always False until Task 16 lands real trend
    detection.
    """
    restart_count_delta = int(
        prom_client.query(
            f'increase(kube_pod_container_status_restarts_total{{pod=~"{target}.*"}}[{window_seconds}s])'
        )
    )
    error_rate = prom_client.query(
        f'sum(rate(http_requests_total{{service="{target}",status=~"5.."}}[{window_seconds}s]))'
        f' / sum(rate(http_requests_total{{service="{target}"}}[{window_seconds}s]))'
    )
    cpu_ratio = prom_client.query(
        f'avg(rate(container_cpu_usage_seconds_total{{pod=~"{target}.*"}}[{window_seconds}s]))'
        f' / avg(kube_pod_container_resource_limits{{pod=~"{target}.*",resource="cpu"}})'
    )
    p99_latency_ms = prom_client.query(
        f'histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket'
        f'{{service="{target}"}}[{window_seconds}s])) by (le)) * 1000'
    )
    mem_ratio = prom_client.query(
        f'avg(container_memory_working_set_bytes{{pod=~"{target}.*"}})'
        f' / avg(kube_pod_container_resource_limits{{pod=~"{target}.*",resource="memory"}})'
    )
    log_lines = loki_client.query_range(f'{{pod=~"{target}.*"}} |= "OOMKilled"')
    oom_killed = any("OOMKilled" in line for line in log_lines)

    return EvidenceSummary(
        target=target,
        restart_count_delta=restart_count_delta,
        error_rate=error_rate,
        cpu_ratio=cpu_ratio,
        p99_latency_ms=p99_latency_ms,
        # Known stub: real trend detection (comparing p99 latency across
        # windows to decide whether it's rising) needs a second, earlier
        # query and is deferred to Task 16's real Prometheus client. Until
        # then this is hardcoded False, so cpu_stress incidents can only be
        # detected once that lands.
        p99_latency_rising=False,
        oom_killed=oom_killed,
        mem_ratio=mem_ratio,
    )


def check_threshold(scenario: str, evidence: EvidenceSummary) -> bool:
    """Return True if `evidence` breaches the threshold for `scenario`."""
    if scenario == "pod_crash":
        return evidence.restart_count_delta > 0 or evidence.error_rate > 0.1
    if scenario == "cpu_stress":
        return evidence.cpu_ratio > 0.85 and evidence.p99_latency_rising
    if scenario == "memory_pressure":
        return evidence.oom_killed or evidence.mem_ratio > 0.9
    raise ValueError(f"unknown scenario: {scenario!r}")


def poll_once(
    prom_client: PromClient,
    loki_client: LokiClient,
    conn: Any,
    scenarios: list[tuple[str, str]],
) -> None:
    """Poll each `(scenario, target)` pair once.

    For each pair: skip it if there's already an open incident (dedup);
    otherwise build evidence and, if it breaches the scenario's threshold,
    open a new incident and log the `anomaly_detected` event.
    """
    for scenario, target in scenarios:
        if db.get_open_incident(conn, scenario, target) is not None:
            continue

        evidence = build_evidence(prom_client, loki_client, scenario, target, window_seconds=300)
        if not check_threshold(scenario, evidence):
            continue

        incident_id = db.create_incident(conn, scenario, target)
        db.append_event(
            conn,
            incident_id,
            "anomaly_detected",
            {
                "restart_count_delta": evidence.restart_count_delta,
                "error_rate": evidence.error_rate,
                "cpu_ratio": evidence.cpu_ratio,
                "p99_latency_ms": evidence.p99_latency_ms,
                "p99_latency_rising": evidence.p99_latency_rising,
                "oom_killed": evidence.oom_killed,
                "mem_ratio": evidence.mem_ratio,
            },
        )
