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
        # Route on substrings that are actually present in the real PromQL
        # strings `build_evidence` issues (see observer.py), not on words
        # like "error" or "latency" that never appear verbatim in them —
        # the real error_rate query is built from `http_requests_total` and
        # `status=~"5.."`, with no literal "error" substring, so matching
        # on "error" silently fell through to the 0.0 default below
        # regardless of what was configured on this fake.
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
