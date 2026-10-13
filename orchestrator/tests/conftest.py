"""Shared pytest fixtures for the orchestrator test suite.

See test_db.py's module docstring for how to stand up the scratch
Postgres instance that TEST_DATABASE_URL must point at.
"""

from __future__ import annotations

import os
from pathlib import Path

import psycopg
import pytest

SCHEMA_PATH = Path(__file__).resolve().parent.parent.parent / "db" / "schema.sql"


@pytest.fixture
def db_conn():
    """Yield a psycopg connection to a scratch Postgres with the schema applied.

    Truncates `incident_events` and `incidents` on teardown so each test
    starts from a clean slate, without needing to tear down/recreate the
    whole database per test.
    """
    database_url = os.environ["TEST_DATABASE_URL"]
    conn = psycopg.connect(database_url, autocommit=True)
    conn.execute(SCHEMA_PATH.read_text())
    try:
        yield conn
    finally:
        conn.execute("TRUNCATE TABLE incident_events, incidents")
        conn.close()
