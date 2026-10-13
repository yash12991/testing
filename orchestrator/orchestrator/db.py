"""Postgres repository layer for the incident lifecycle.

Backed by two tables (see `db/schema.sql`):

- `incidents`: one row per incident, fixing the (scenario, target) pair an
  `incident_id` refers to.
- `incident_events`: an append-only log of stages an incident has passed
  through (`anomaly_detected`, `diagnosis`, `approval`, `fix_applied`,
  `verification`, ...), each carrying a JSON payload.

Every function here takes an already-open `psycopg.Connection` passed in by
the caller (e.g. the main loop, or a test fixture pointed at a scratch
Postgres) — this module never opens or closes connections itself.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime

from psycopg import Connection
from psycopg.types.json import Jsonb


@dataclass
class EventRow:
    id: int
    incident_id: uuid.UUID
    scenario: str
    stage: str
    ts: datetime
    payload: dict
    confidence: float | None
    latency_ms: int | None


def create_incident(conn: Connection, scenario: str, target: str) -> uuid.UUID:
    """Create a new incident header row and return its generated id."""
    incident_id = uuid.uuid4()
    conn.execute(
        "INSERT INTO incidents (id, scenario, target) VALUES (%s, %s, %s)",
        (incident_id, scenario, target),
    )
    return incident_id


def append_event(
    conn: Connection,
    incident_id: uuid.UUID,
    stage: str,
    payload: dict,
    confidence: float | None = None,
    latency_ms: int | None = None,
) -> None:
    """Append one lifecycle-stage event to an existing incident's log."""
    row = conn.execute(
        "SELECT scenario FROM incidents WHERE id = %s", (incident_id,)
    ).fetchone()
    if row is None:
        raise ValueError(f"no incident found for incident_id={incident_id!r}")
    scenario = row[0]
    conn.execute(
        """
        INSERT INTO incident_events
            (incident_id, scenario, stage, payload, confidence, latency_ms)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (incident_id, scenario, stage, Jsonb(payload), confidence, latency_ms),
    )


def get_events(conn: Connection, incident_id: uuid.UUID) -> list[EventRow]:
    """Return all events logged for an incident, in the order they occurred."""
    rows = conn.execute(
        """
        SELECT id, incident_id, scenario, stage, ts, payload, confidence, latency_ms
        FROM incident_events
        WHERE incident_id = %s
        ORDER BY id
        """,
        (incident_id,),
    ).fetchall()
    return [EventRow(*row) for row in rows]


def get_open_incident(conn: Connection, scenario: str, target: str) -> uuid.UUID | None:
    """Return the id of the most recent unresolved incident for (scenario, target).

    "Unresolved" means it has an `anomaly_detected` event but no
    `verification` event yet. Returns None if there isn't one.
    """
    row = conn.execute(
        """
        SELECT i.id
        FROM incidents i
        WHERE i.scenario = %s
          AND i.target = %s
          AND EXISTS (
              SELECT 1 FROM incident_events e
              WHERE e.incident_id = i.id AND e.stage = 'anomaly_detected'
          )
          AND NOT EXISTS (
              SELECT 1 FROM incident_events e
              WHERE e.incident_id = i.id AND e.stage = 'verification'
          )
        ORDER BY i.created_at DESC
        LIMIT 1
        """,
        (scenario, target),
    ).fetchone()
    return row[0] if row else None


def get_incomplete_incidents(conn: Connection) -> list[uuid.UUID]:
    """Return all incident ids (any scenario) that have an `anomaly_detected`
    event but no `verification` event yet — the incidents whose next stage
    the main loop still needs to run.
    """
    rows = conn.execute(
        """
        SELECT DISTINCT e.incident_id
        FROM incident_events e
        WHERE e.stage = 'anomaly_detected'
          AND NOT EXISTS (
              SELECT 1 FROM incident_events e2
              WHERE e2.incident_id = e.incident_id AND e2.stage = 'verification'
          )
        """
    ).fetchall()
    return [row[0] for row in rows]
