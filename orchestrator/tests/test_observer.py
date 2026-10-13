"""Tests for orchestrator.observer — evidence gathering, threshold checks, dedup.

`test_poll_once_*` tests use the `db_conn` fixture (see conftest.py /
test_db.py) and therefore require TEST_DATABASE_URL to point at a scratch
Postgres with `db/schema.sql` applied. The threshold/evidence tests are
pure and need no database.
"""

from __future__ import annotations

from orchestrator import db, observer
from orchestrator.models import EvidenceSummary


class FakePromClient:
    """Returns a fixed value for every PromQL query, regardless of content.

    Real usage issues different queries for different metrics; these fakes
    are deliberately simplistic since the exact PromQL strings are not
    pinned by these tests (per the task brief, that's Task 16's job).
    """

    def __init__(
        self,
        restart_count_delta: float = 0.0,
        error_rate: float = 0.0,
        cpu_ratio: float = 0.0,
        p99_latency_ms: float = 0.0,
        mem_ratio: float = 0.0,
        oom_killed: bool = False,
    ):
        self.restart_count_delta = restart_count_delta
        self.error_rate = error_rate
        self.cpu_ratio = cpu_ratio
        self.p99_latency_ms = p99_latency_ms
        self.mem_ratio = mem_ratio
        self.oom_killed = oom_killed
        self.queries: list[str] = []

    def query(self, promql: str) -> float:
        self.queries.append(promql)
        # Route on substrings that are actually present in the real PromQL
        # strings `build_evidence` issues (see observer.py), not on words
        # like "error" or "latency" that never appear verbatim in them —
        # the real error_rate query is built from `http_requests_total` and
        # `status=~"5.."`, with no literal "error" substring, so matching
        # on "error" silently fell through to the 0.0 default below
        # regardless of what was configured on this fake.
        #
        # The OOMKilled check must be routed before "restart"/"mem": the
        # kube_pod_container_status_last_terminated_reason query contains
        # neither "restart" nor "mem", but routing order is checked
        # defensively anyway since all the substring checks are on the
        # same string.
        if "OOMKilled" in promql:
            return 1.0 if self.oom_killed else 0.0
        if "restart" in promql:
            return self.restart_count_delta
        if 'status=~"5.."' in promql:
            return self.error_rate
        if "cpu" in promql:
            return self.cpu_ratio
        if "bucket" in promql:
            return self.p99_latency_ms
        if "mem" in promql:
            return self.mem_ratio
        return 0.0


class FakeLokiClient:
    """Returns a fixed list of log lines for every LogQL query.

    `build_evidence` no longer calls `query_range` at all (oom_killed is
    now detected via a Prometheus kube-state-metrics query instead — see
    FakePromClient's "OOMKilled" routing above and
    `test_build_evidence_does_not_query_loki`), but the fake is kept around
    for tests exercising the interface directly and for any future
    log-based signal.
    """

    def __init__(self, lines: list[str] | None = None, oom_killed: bool = False):
        self.lines = lines if lines is not None else []
        self.oom_killed = oom_killed
        self.queries: list[str] = []

    def query_range(self, logql: str, window_seconds: int = 3600) -> list[str]:
        self.queries.append(logql)
        if self.oom_killed:
            return self.lines + ["OOMKilled"]
        return self.lines


def test_check_threshold_pod_crash_breaches_on_restart_delta():
    evidence = EvidenceSummary(
        restart_count_delta=2,
        error_rate=0.0,
        cpu_ratio=0.1,
        p99_latency_ms=50,
        p99_latency_rising=False,
        oom_killed=False,
        mem_ratio=0.1,
        target="cartservice",
    )
    assert observer.check_threshold("pod_crash", evidence) is True


def test_check_threshold_pod_crash_ignores_error_rate_alone():
    # error_rate was dropped from pod_crash's threshold: no Online Boutique
    # service exports http_requests_total, so error_rate is always 0.0 in
    # practice and could never contribute a real signal anyway. Simplified
    # per Task 16: restart_count_delta > 0 is the only leg now.
    evidence = EvidenceSummary(
        restart_count_delta=0,
        error_rate=0.2,
        cpu_ratio=0.1,
        p99_latency_ms=50,
        p99_latency_rising=False,
        oom_killed=False,
        mem_ratio=0.1,
        target="cartservice",
    )
    assert observer.check_threshold("pod_crash", evidence) is False


def test_check_threshold_pod_crash_does_not_breach_with_no_signal():
    evidence = EvidenceSummary(
        restart_count_delta=0,
        error_rate=0.0,
        cpu_ratio=0.1,
        p99_latency_ms=50,
        p99_latency_rising=False,
        oom_killed=False,
        mem_ratio=0.1,
        target="cartservice",
    )
    assert observer.check_threshold("pod_crash", evidence) is False


def test_check_threshold_cpu_stress_breaches_on_cpu_ratio_alone():
    # p99_latency_rising was dropped from cpu_stress's threshold: no Online
    # Boutique service exports a latency histogram, so p99_latency_rising
    # is always False in practice and could never contribute a real
    # signal anyway. Simplified per Task 16: cpu_ratio > 0.85 is the only
    # leg now, so this breaches even though latency isn't (and can never
    # be reported as) rising.
    evidence = EvidenceSummary(
        restart_count_delta=0,
        error_rate=0.0,
        cpu_ratio=0.95,
        p99_latency_ms=50,
        p99_latency_rising=False,
        oom_killed=False,
        mem_ratio=0.1,
        target="recommendationservice",
    )
    assert observer.check_threshold("cpu_stress", evidence) is True


def test_check_threshold_cpu_stress_does_not_breach_below_ratio():
    evidence = EvidenceSummary(
        restart_count_delta=0,
        error_rate=0.0,
        cpu_ratio=0.5,
        p99_latency_ms=50,
        p99_latency_rising=True,
        oom_killed=False,
        mem_ratio=0.1,
        target="recommendationservice",
    )
    assert observer.check_threshold("cpu_stress", evidence) is False


def test_check_threshold_cpu_stress_breaches_when_both_signals_present():
    evidence = EvidenceSummary(
        restart_count_delta=0,
        error_rate=0.0,
        cpu_ratio=0.95,
        p99_latency_ms=800,
        p99_latency_rising=True,
        oom_killed=False,
        mem_ratio=0.1,
        target="recommendationservice",
    )
    assert observer.check_threshold("cpu_stress", evidence) is True


def test_check_threshold_memory_pressure_breaches_on_oom_killed():
    evidence = EvidenceSummary(
        restart_count_delta=0,
        error_rate=0.0,
        cpu_ratio=0.1,
        p99_latency_ms=50,
        p99_latency_rising=False,
        oom_killed=True,
        mem_ratio=0.1,
        target="productcatalogservice",
    )
    assert observer.check_threshold("memory_pressure", evidence) is True


def test_check_threshold_memory_pressure_breaches_on_mem_ratio():
    evidence = EvidenceSummary(
        restart_count_delta=0,
        error_rate=0.0,
        cpu_ratio=0.1,
        p99_latency_ms=50,
        p99_latency_rising=False,
        oom_killed=False,
        mem_ratio=0.95,
        target="productcatalogservice",
    )
    assert observer.check_threshold("memory_pressure", evidence) is True


def test_check_threshold_memory_pressure_does_not_breach_below_both():
    evidence = EvidenceSummary(
        restart_count_delta=0,
        error_rate=0.0,
        cpu_ratio=0.1,
        p99_latency_ms=50,
        p99_latency_rising=False,
        oom_killed=False,
        mem_ratio=0.1,
        target="productcatalogservice",
    )
    assert observer.check_threshold("memory_pressure", evidence) is False


def test_build_evidence_assembles_summary_from_clients():
    fake_prom = FakePromClient(
        restart_count_delta=3,
        error_rate=0.42,
        cpu_ratio=0.2,
        p99_latency_ms=120,
        mem_ratio=0.3,
    )
    fake_loki = FakeLokiClient()
    evidence = observer.build_evidence(fake_prom, fake_loki, "pod_crash", "cartservice", 300)
    assert isinstance(evidence, EvidenceSummary)
    # Assert every field build_evidence populates, not just target and
    # restart_count_delta — a fake-routing bug that silently defaulted one
    # of the other signals to 0.0 would otherwise go unnoticed here (as
    # happened previously with error_rate; see FakePromClient.query above).
    assert evidence.target == "cartservice"
    assert evidence.restart_count_delta == 3
    assert evidence.error_rate == 0.42
    assert evidence.cpu_ratio == 0.2
    assert evidence.p99_latency_ms == 120
    assert evidence.p99_latency_rising is False
    assert evidence.mem_ratio == 0.3
    assert evidence.oom_killed is False
    assert fake_prom.queries  # the client was actually queried


def test_build_evidence_detects_oom_killed_from_prometheus():
    # oom_killed now comes from a kube-state-metrics Prometheus query, not
    # Loki (Loki only scrapes container stdout/stderr; OOMKilled is a
    # termination *reason*, never a log line, so searching Loki for it
    # could never match). It also requires a recent restart (see the next
    # two tests) - restart_count_delta=1 here stands in for that.
    fake_prom = FakePromClient(oom_killed=True, restart_count_delta=1)
    fake_loki = FakeLokiClient()
    evidence = observer.build_evidence(
        fake_prom, fake_loki, "memory_pressure", "productcatalogservice", 300
    )
    assert evidence.oom_killed is True


def test_build_evidence_oom_killed_false_when_prometheus_reports_no_match():
    fake_prom = FakePromClient(oom_killed=False, restart_count_delta=1)
    fake_loki = FakeLokiClient()
    evidence = observer.build_evidence(
        fake_prom, fake_loki, "memory_pressure", "productcatalogservice", 300
    )
    assert evidence.oom_killed is False


def test_build_evidence_oom_killed_false_without_recent_restart():
    # The kube_pod_container_status_last_terminated_reason gauge has no
    # time scoping at all - it stays at 1 for as long as the pod object
    # lives, even if the OOM happened hours ago and the pod has been
    # healthy since. Gating on a recent restart (restart_count_delta > 0,
    # the same RESTART_WINDOW_SECONDS window) is what stops a stale,
    # long-resolved OOM from triggering memory_pressure detection forever.
    fake_prom = FakePromClient(oom_killed=True, restart_count_delta=0)
    fake_loki = FakeLokiClient()
    evidence = observer.build_evidence(
        fake_prom, fake_loki, "memory_pressure", "productcatalogservice", 300
    )
    assert evidence.oom_killed is False
    assert (
        observer.check_threshold(
            "memory_pressure",
            EvidenceSummary(
                target="productcatalogservice",
                restart_count_delta=0,
                error_rate=0.0,
                cpu_ratio=0.0,
                p99_latency_ms=0.0,
                p99_latency_rising=False,
                oom_killed=evidence.oom_killed,
                mem_ratio=0.1,
            ),
        )
        is False
    )


def test_build_evidence_does_not_query_loki():
    # Loki's query_range machinery is left in place on the client (for
    # potential future log-based signals) but build_evidence no longer
    # calls it for anything — oom_killed moved to Prometheus.
    fake_prom = FakePromClient()
    fake_loki = FakeLokiClient()
    observer.build_evidence(fake_prom, fake_loki, "memory_pressure", "productcatalogservice", 300)
    assert fake_loki.queries == []


def test_build_evidence_filters_cpu_and_mem_ratio_by_container():
    # cpu_ratio/mem_ratio must filter on container="server" (the real
    # container name for every target, verified against
    # infra/manifests/online-boutique/kubernetes-manifests.yaml) so the
    # avg() doesn't get diluted by the near-zero "pause" sandbox
    # container's series.
    fake_prom = FakePromClient()
    fake_loki = FakeLokiClient()
    observer.build_evidence(fake_prom, fake_loki, "cpu_stress", "recommendationservice", 300)

    cpu_queries = [q for q in fake_prom.queries if "container_cpu_usage_seconds_total" in q]
    cpu_limit_queries = [
        q for q in fake_prom.queries if "kube_pod_container_resource_limits" in q and 'resource="cpu"' in q
    ]
    mem_queries = [q for q in fake_prom.queries if "container_memory_working_set_bytes" in q]
    mem_limit_queries = [
        q
        for q in fake_prom.queries
        if "kube_pod_container_resource_limits" in q and 'resource="memory"' in q
    ]
    assert cpu_queries and all('container="server"' in q for q in cpu_queries)
    assert cpu_limit_queries and all('container="server"' in q for q in cpu_limit_queries)
    assert mem_queries and all('container="server"' in q for q in mem_queries)
    assert mem_limit_queries and all('container="server"' in q for q in mem_limit_queries)


def test_build_evidence_restart_query_uses_short_window_regardless_of_arg():
    # The restart-count query always uses observer.RESTART_WINDOW_SECONDS
    # (60s), independent of whatever window_seconds the caller passes in
    # for the other signals — a stale restart inside a long lookback
    # window caused a spurious second incident shortly after a fix
    # resolved the first one (see observer.py's RESTART_WINDOW_SECONDS
    # comment).
    fake_prom = FakePromClient()
    fake_loki = FakeLokiClient()
    observer.build_evidence(fake_prom, fake_loki, "pod_crash", "cartservice", window_seconds=300)

    restart_queries = [q for q in fake_prom.queries if "restarts_total" in q]
    assert restart_queries
    assert all(f"[{observer.RESTART_WINDOW_SECONDS}s]" in q for q in restart_queries)
    assert all("[300s]" not in q for q in restart_queries)

    cpu_queries = [q for q in fake_prom.queries if "container_cpu_usage_seconds_total" in q]
    assert cpu_queries and all("[300s]" in q for q in cpu_queries)


def test_build_evidence_restart_query_aggregates_across_matching_pods():
    # PrometheusClient.query only reads result[0] of whatever Prometheus
    # returns, so an unaggregated query over pod=~"<target>.*" could read
    # an arbitrary one of several matching pods (e.g. old+new pod briefly
    # coexisting during restart_pod, or multiple replicas after
    # scale_replicas). sum(...) aggregates the restart count across all
    # matching series into a single value instead.
    fake_prom = FakePromClient()
    fake_loki = FakeLokiClient()
    observer.build_evidence(fake_prom, fake_loki, "pod_crash", "cartservice", 300)

    restart_queries = [q for q in fake_prom.queries if "restarts_total" in q]
    assert restart_queries
    assert all(q.startswith("sum(increase(") for q in restart_queries)


def test_build_evidence_oom_query_aggregates_across_matching_pods():
    # Same result[0]-picks-an-arbitrary-series risk as restart_count_delta,
    # but max(...) is the correct aggregation here (not sum()): this is a
    # boolean-ish flag ("is ANY matching pod currently showing this"), not
    # a count to total up across pods.
    fake_prom = FakePromClient()
    fake_loki = FakeLokiClient()
    observer.build_evidence(fake_prom, fake_loki, "memory_pressure", "productcatalogservice", 300)

    oom_queries = [q for q in fake_prom.queries if "OOMKilled" in q]
    assert oom_queries
    assert all(q.startswith("max(kube_pod_container_status_last_terminated_reason") for q in oom_queries)


def test_poll_once_skips_scenario_with_open_incident(db_conn):
    incident_id = db.create_incident(db_conn, "pod_crash", "cartservice")
    db.append_event(db_conn, incident_id, "anomaly_detected", {})  # existing open incident

    fake_prom = FakePromClient(restart_count_delta=5)  # would otherwise breach
    observer.poll_once(fake_prom, FakeLokiClient(), db_conn, [("pod_crash", "cartservice")])

    # A correct skip happens before any evidence-building, so the dedup check
    # must short-circuit prior to querying Prometheus at all. This is the
    # strongest guarantee against a regression that deletes the dedup check:
    # since `incidents` has no uniqueness constraint on (scenario, target)
    # and `create_incident` always mints a fresh UUID, a missing dedup check
    # would create a *second*, separate incident (with its own
    # anomaly_detected event) rather than appending to the existing one — so
    # asserting only the original incident's event list would not catch it.
    assert fake_prom.queries == []  # no evidence was ever queried

    rows = db.get_events(db_conn, incident_id)
    assert len(rows) == 1  # no second anomaly_detected row created on the original incident

    # Still exactly one open incident for this (scenario, target) pair, and
    # it's still the original one — not a second incident created alongside it.
    assert db.get_open_incident(db_conn, "pod_crash", "cartservice") == incident_id


def test_poll_once_creates_incident_on_breach(db_conn):
    fake_prom = FakePromClient(restart_count_delta=3)
    observer.poll_once(fake_prom, FakeLokiClient(), db_conn, [("pod_crash", "cartservice")])

    incident_id = db.get_open_incident(db_conn, "pod_crash", "cartservice")
    assert incident_id is not None

    events = db.get_events(db_conn, incident_id)
    assert [e.stage for e in events] == ["anomaly_detected"]


def test_poll_once_does_not_create_incident_when_below_threshold(db_conn):
    fake_prom = FakePromClient(restart_count_delta=0, error_rate=0.0)
    observer.poll_once(fake_prom, FakeLokiClient(), db_conn, [("pod_crash", "cartservice")])

    assert db.get_open_incident(db_conn, "pod_crash", "cartservice") is None


def test_poll_once_handles_multiple_scenarios_independently(db_conn):
    fake_prom = FakePromClient(restart_count_delta=3, cpu_ratio=0.1)
    observer.poll_once(
        fake_prom,
        FakeLokiClient(),
        db_conn,
        [("pod_crash", "cartservice"), ("cpu_stress", "recommendationservice")],
    )

    assert db.get_open_incident(db_conn, "pod_crash", "cartservice") is not None
    assert db.get_open_incident(db_conn, "cpu_stress", "recommendationservice") is None
