"""Observer stage: evidence gathering, threshold checks, and incident dedup.

This module is the entry point of the chaos-to-remediation loop: it polls
Prometheus/Loki for signals about a `(scenario, target)` pair, decides
whether those signals breach a scenario-specific threshold, and - if so and
there isn't already an open incident for that pair - opens a new incident
and logs the `anomaly_detected` event that downstream stages (diagnosis,
approval, fix, verification) pick up.

`prom_client`/`loki_client` are thin wrapper objects (see
orchestrator/prom_client.py and orchestrator/loki_client.py for the real
implementations against a live Prometheus/Loki) exposing:

    prom_client.query(promql: str) -> float
    loki_client.query_range(logql: str, window_seconds: int) -> list[str]

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
    def query_range(self, logql: str, window_seconds: int) -> list[str]: ...


# The real container name inside each target's pod, for every
# (scenario, target) pair this pipeline watches (see main.SCENARIOS).
# Verified directly against
# infra/manifests/online-boutique/kubernetes-manifests.yaml (not assumed):
# cartservice, recommendationservice, and productcatalogservice each
# define exactly one container, named "server". Without this filter,
# avg(...) over pod=~"<target>.*" also pulls in cAdvisor's per-pod-sandbox
# "pause" container series (near-zero CPU/mem usage), diluting the real
# ratio - confirmed live in Task 15 as the reason cpu_stress/
# memory_pressure's cpu_ratio/mem_ratio never crossed their thresholds
# despite genuine chaos load.
TARGET_CONTAINER = "server"

# restart_count_delta uses a lookback window of its own, independent of the
# `window_seconds` the other signals use (e.g. 300s for original
# detection). Found live in Task 16: with a 300s window, a one-time restart
# stayed inside the lookback window for a full 5 minutes after a fix
# successfully resolved the incident, so the very next poll still saw
# restart_count_delta > 0 and opened a spurious second incident for the
# same (scenario, target) pair about a minute after the first was verified
# recovered. 60s is long enough to span several poll intervals
# (POLL_INTERVAL_SECONDS defaults to 15s) but short enough that a one-off
# restart clears out quickly once the fix lands.
RESTART_WINDOW_SECONDS = 60


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
    not pinned by unit tests; what matters is that every signal a
    scenario's threshold check depends on gets populated - with one known
    exception: `p99_latency_rising` is a stub (see the comment at its
    assignment below) and is always False, since no Online Boutique
    service exports a latency histogram to detect a trend from (confirmed
    structurally absent live).
    """
    # Wrapped in sum(...): without it, PrometheusClient.query only reads
    # result[0] of whatever Prometheus returns, i.e. an arbitrary one of
    # the matching series when more than one pod matches `pod=~"{target}.*"`
    # (a real situation - e.g. the old and new pod briefly coexist during
    # restart_pod, or a target now runs multiple replicas after a
    # scale_replicas fix). sum() is the right aggregation here because this
    # is a count ("how many restarts happened"), not a flag - summing
    # across matching pods gives the true total instead of picking
    # whichever single series happened to come back first.
    restart_count_delta = int(
        prom_client.query(
            f'sum(increase(kube_pod_container_status_restarts_total{{pod=~"{target}.*"}}[{RESTART_WINDOW_SECONDS}s]))'
        )
    )
    error_rate = prom_client.query(
        f'sum(rate(http_requests_total{{service="{target}",status=~"5.."}}[{window_seconds}s]))'
        f' / sum(rate(http_requests_total{{service="{target}"}}[{window_seconds}s]))'
    )
    # Wrapped in max(...), not avg(...): final-review Fix 3. After a
    # scale_replicas fix adds idle replicas, averaging cpu_ratio across all
    # matching pods dilutes the cluster-wide ratio well below threshold even
    # though Chaos Mesh's `mode: one` selector keeps stressing only the
    # ORIGINAL pod for its full duration - so Verifier's recovered=True was
    # really just "the average got diluted by idle replicas," not "the
    # stressed pod recovered." max() reports the worst/most-stressed
    # matching pod's ratio instead, mirroring the max(...) pattern already
    # used for oom_killed above. Since every matching pod (replicas of the
    # same Deployment) shares the same resource limit, max(usage)/max(limit)
    # equals max(usage)/limit, i.e. the true worst-case ratio - not an
    # average-of-ratios artifact.
    cpu_ratio = prom_client.query(
        f'max(rate(container_cpu_usage_seconds_total'
        f'{{pod=~"{target}.*",container="{TARGET_CONTAINER}"}}[{window_seconds}s]))'
        f' / max(kube_pod_container_resource_limits'
        f'{{pod=~"{target}.*",container="{TARGET_CONTAINER}",resource="cpu"}})'
    )
    p99_latency_ms = prom_client.query(
        f'histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket'
        f'{{service="{target}"}}[{window_seconds}s])) by (le)) * 1000'
    )
    mem_ratio = prom_client.query(
        f'avg(container_memory_working_set_bytes{{pod=~"{target}.*",container="{TARGET_CONTAINER}"}})'
        f' / avg(kube_pod_container_resource_limits'
        f'{{pod=~"{target}.*",container="{TARGET_CONTAINER}",resource="memory"}})'
    )
    # oom_killed used to search Loki for the literal text "OOMKilled", but
    # Loki only scrapes container stdout/stderr - OOMKilled is a container
    # termination *reason* kubelet/kube-state-metrics report, never a log
    # line, so that search could never match. Query kube-state-metrics
    # instead: this gauge is 1 when the container's last termination
    # reason was OOMKilled, absent (query returns no series, i.e. 0.0 per
    # PromClient.query's convention) otherwise. loki_client/query_range is
    # left in place for other, future log-based signals; it's just no
    # longer used for this one.
    #
    # Wrapped in max(...) rather than sum(): this is a boolean-ish flag
    # across pods ("is ANY matching pod currently showing this"), not a
    # count, so summing multiple matching pods could produce a value > 1
    # that still means exactly the same thing as 1 - max() is both correct
    # and avoids the same result[0]-picks-an-arbitrary-series problem
    # sum() fixes for restart_count_delta above.
    #
    # Gated on a RECENT restart (the same RESTART_WINDOW_SECONDS window
    # computed above for restart_count_delta): the kube-state-metrics gauge
    # above stays at 1 for as long as the pod object exists, with no time
    # scoping at all - a pod that OOMed hours ago and has been healthy
    # since would otherwise report oom_killed=True forever, and because a
    # verification row permanently resolves an incident with no retry, any
    # fix that doesn't literally replace the pod object (e.g. anything
    # other than restart_pod) would then always read back recovered=False.
    # Requiring restart_count_delta > 0 (a real restart within the last
    # RESTART_WINDOW_SECONDS) means a stale, long-resolved OOM event no
    # longer counts as a live signal.
    oom_killed_flag = (
        prom_client.query(
            f'max(kube_pod_container_status_last_terminated_reason'
            f'{{reason="OOMKilled",pod=~"{target}.*",container="{TARGET_CONTAINER}"}})'
        )
        > 0
    )
    oom_killed = oom_killed_flag and restart_count_delta > 0

    available_replicas = 1
    if scenario == "service_unavailability":
        try:
            available_val = prom_client.query(
                f'max(kube_deployment_status_replicas_available{{deployment="{target}"}})'
            )
            available_replicas = int(available_val)
        except Exception:
            available_replicas = 1

    return EvidenceSummary(
        target=target,
        restart_count_delta=restart_count_delta,
        error_rate=error_rate,
        cpu_ratio=cpu_ratio,
        p99_latency_ms=p99_latency_ms,
        # Known stub: no Online Boutique service exports
        # http_request_duration_seconds_bucket (or any latency histogram),
        # so there is nothing to compute a trend from. Hardcoded False;
        # cpu_stress detection no longer depends on it (see
        # check_threshold below).
        p99_latency_rising=False,
        oom_killed=oom_killed,
        mem_ratio=mem_ratio,
        available_replicas=available_replicas,
    )


def check_threshold(scenario: str, evidence: EvidenceSummary) -> bool:
    """Return True if `evidence` breaches the threshold for `scenario`.

    Supports all 5 scenarios: pod_crash, cpu_stress, memory_pressure,
    network_latency, and service_unavailability.
    """
    if scenario == "pod_crash":
        return evidence.restart_count_delta > 0
    if scenario == "cpu_stress":
        return evidence.cpu_ratio > 0.85
    if scenario == "memory_pressure":
        return evidence.oom_killed or evidence.mem_ratio > 0.9
    if scenario == "network_latency":
        return evidence.p99_latency_ms > 150.0
    if scenario == "service_unavailability":
        return evidence.available_replicas == 0 or evidence.error_rate > 0.5
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
                "available_replicas": evidence.available_replicas,
            },
        )
