"""Tests for orchestrator.main._reconnect and _poll_cycle (final-review Fix 4).

Pure mock-based tests: no live Postgres needed. `main()` used to open one
psycopg connection at startup and never reconnect; a dropped connection
raised `psycopg.OperationalError` from inside the poll loop, which the old
bare `except Exception` logged and swallowed forever, leaving the
orchestrator looping on a permanently dead connection. `_poll_cycle` (factored
out of `main()`'s `while True` body specifically so this is unit-testable)
now catches `psycopg.OperationalError` and reconnects before the next cycle.
"""

from __future__ import annotations

from unittest.mock import Mock

import psycopg

from orchestrator import main


def test_reconnect_closes_dead_connection_and_opens_a_fresh_one(monkeypatch):
    dead_conn = Mock()
    fresh_conn = Mock()
    connect_calls = []

    def fake_connect(database_url, autocommit=False):
        connect_calls.append((database_url, autocommit))
        return fresh_conn

    monkeypatch.setattr(main.psycopg, "connect", fake_connect)

    result = main._reconnect(dead_conn, "postgresql://fake/incidents")

    dead_conn.close.assert_called_once()
    assert result is fresh_conn
    assert connect_calls == [("postgresql://fake/incidents", True)]


def test_reconnect_tolerates_close_raising_on_an_already_dead_connection(monkeypatch):
    dead_conn = Mock()
    dead_conn.close.side_effect = psycopg.OperationalError("connection already closed")
    fresh_conn = Mock()
    monkeypatch.setattr(main.psycopg, "connect", lambda database_url, autocommit=False: fresh_conn)

    # Must not raise, despite close() failing.
    result = main._reconnect(dead_conn, "postgresql://fake/incidents")

    assert result is fresh_conn


def test_poll_cycle_reconnects_when_run_once_raises_operational_error(monkeypatch):
    dead_conn = Mock()
    fresh_conn = Mock()

    def fake_run_once(conn, *args, **kwargs):
        raise psycopg.OperationalError("server closed the connection unexpectedly")

    monkeypatch.setattr(main, "run_once", fake_run_once)
    monkeypatch.setattr(main.psycopg, "connect", lambda *a, **k: fresh_conn)

    result = main._poll_cycle(
        dead_conn, None, None, None, None, [], 0.8, "postgresql://fake/incidents"
    )

    assert result is fresh_conn
    dead_conn.close.assert_called_once()


def test_poll_cycle_returns_same_connection_on_success(monkeypatch):
    conn = Mock()
    monkeypatch.setattr(main, "run_once", lambda *a, **k: None)

    result = main._poll_cycle(conn, None, None, None, None, [], 0.8, "postgresql://fake/incidents")

    assert result is conn


def test_poll_cycle_swallows_other_exceptions_and_keeps_same_connection(monkeypatch):
    """A non-connection error (e.g. a transient K8s/Prometheus outage) must
    not trigger a reconnect - only psycopg.OperationalError should.
    """
    conn = Mock()

    def fake_run_once(*args, **kwargs):
        raise RuntimeError("some other transient failure")

    monkeypatch.setattr(main, "run_once", fake_run_once)
    connect_mock = Mock()
    monkeypatch.setattr(main.psycopg, "connect", connect_mock)

    result = main._poll_cycle(conn, None, None, None, None, [], 0.8, "postgresql://fake/incidents")

    assert result is conn
    connect_mock.assert_not_called()
    conn.close.assert_not_called()
