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
    ):
        self.restart_count_delta = restart_count_delta
        self.error_rate = error_rate
        self.cpu_ratio = cpu_ratio
        self.p99_latency_ms = p99_latency_ms
        self.mem_ratio = mem_ratio
        self.queries: list[str] = []

    def query(self, promql: str) -> float:
        self.queries.append(promql)
        if "restart" in promql:
            return self.restart_count_delta
        if "error" in promql:
            return self.error_rate
        if "cpu" in promql:
            return self.cpu_ratio
        if "latency" in promql or "p99" in promql or "bucket" in promql:
            return self.p99_latency_ms
        if "mem" in promql:
            return self.mem_ratio
        return 0.0


class FakeLokiClient:
    """Returns a fixed list of log lines for every LogQL query."""

    def __init__(self, lines: list[str] | None = None, oom_killed: bool = False):
        self.lines = lines if lines is not None else []
        self.oom_killed = oom_killed
        self.queries: list[str] = []

    def query_range(self, logql: str) -> list[str]:
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


def test_check_threshold_pod_crash_breaches_on_error_rate():
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
    assert observer.check_threshold("pod_crash", evidence) is True


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


def test_check_threshold_cpu_stress_requires_both_signals():
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
    assert observer.check_threshold("cpu_stress", evidence) is False  # latency not rising yet


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
        error_rate=0.0,
        cpu_ratio=0.2,
        p99_latency_ms=120,
        mem_ratio=0.3,
    )
    fake_loki = FakeLokiClient()
    evidence = observer.build_evidence(fake_prom, fake_loki, "pod_crash", "cartservice", 300)
    assert isinstance(evidence, EvidenceSummary)
    assert evidence.target == "cartservice"
    assert evidence.restart_count_delta == 3
    assert fake_prom.queries  # the client was actually queried


def test_build_evidence_detects_oom_killed_from_logs():
    fake_prom = FakePromClient()
    fake_loki = FakeLokiClient(oom_killed=True)
    evidence = observer.build_evidence(
        fake_prom, fake_loki, "memory_pressure", "productcatalogservice", 300
    )
    assert evidence.oom_killed is True


def test_poll_once_skips_scenario_with_open_incident(db_conn):
    incident_id = db.create_incident(db_conn, "pod_crash", "cartservice")
    db.append_event(db_conn, incident_id, "anomaly_detected", {})  # existing open incident

    fake_prom = FakePromClient(restart_count_delta=5)  # would otherwise breach
    observer.poll_once(fake_prom, FakeLokiClient(), db_conn, [("pod_crash", "cartservice")])

    rows = db.get_events(db_conn, incident_id)
    assert len(rows) == 1  # no second anomaly_detected row created


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
