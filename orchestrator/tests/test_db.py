"""Tests for orchestrator.db — the Postgres repository layer.

These tests exercise `db.py` against a REAL scratch Postgres instance; they
do not use the live cluster's Postgres from Task 5 (deploy_postgres.sh)
and must not depend on it being reachable.

Set TEST_DATABASE_URL to point at a throwaway Postgres before running this
file. This machine has no Docker — use rootless Podman instead (confirmed
working on this project for the Minikube cluster itself), substituting
`podman run` wherever a brief or doc says `docker run`:

    podman run -d --name pg-test -e POSTGRES_PASSWORD=test \\
        -p 5433:5432 postgres:16

Then:

    export TEST_DATABASE_URL="postgresql://postgres:test@localhost:5433/postgres"
    pytest orchestrator/tests/test_db.py -v

Tear down afterwards with `podman rm -f pg-test`.

The `db_conn` fixture (orchestrator/tests/conftest.py) applies
`db/schema.sql` against this connection on each test and truncates
`incident_events` on teardown, so tests don't interfere with each other
and don't require a fresh container per run.
"""

from __future__ import annotations

from orchestrator import db


def test_create_incident_and_append_events_round_trip(db_conn):
    incident_id = db.create_incident(db_conn, "pod_crash", "cartservice")
    db.append_event(
        db_conn, incident_id, "anomaly_detected", {"metric": "restart_count"}, latency_ms=12
    )
    events = db.get_events(db_conn, incident_id)
    assert [e.stage for e in events] == ["anomaly_detected"]
    assert events[0].payload["metric"] == "restart_count"


def test_get_open_incident_returns_none_when_resolved(db_conn):
    incident_id = db.create_incident(db_conn, "cpu_stress", "recommendationservice")
    db.append_event(db_conn, incident_id, "anomaly_detected", {})
    db.append_event(db_conn, incident_id, "verification", {"recovered": True})
    assert db.get_open_incident(db_conn, "cpu_stress", "recommendationservice") is None


def test_get_open_incident_returns_id_when_unresolved(db_conn):
    incident_id = db.create_incident(db_conn, "memory_pressure", "productcatalogservice")
    db.append_event(db_conn, incident_id, "anomaly_detected", {})
    assert db.get_open_incident(db_conn, "memory_pressure", "productcatalogservice") == incident_id


def test_get_incomplete_incidents_excludes_verified_ones(db_conn):
    open_id = db.create_incident(db_conn, "pod_crash", "cartservice")
    db.append_event(db_conn, open_id, "anomaly_detected", {})
    closed_id = db.create_incident(db_conn, "cpu_stress", "recommendationservice")
    db.append_event(db_conn, closed_id, "anomaly_detected", {})
    db.append_event(db_conn, closed_id, "verification", {"recovered": True})
    assert db.get_incomplete_incidents(db_conn) == [open_id]
