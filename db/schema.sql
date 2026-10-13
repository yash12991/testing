-- db/schema.sql
--
-- Schema for the `incidents` Postgres database used by the chaos-to-
-- remediation pipeline to record every stage of an incident's lifecycle
-- (detection, diagnosis, remediation action, outcome, ...).
--
-- Applied by infra/minikube/deploy_postgres.sh via `psql` against the
-- `incidents` database inside the bitnami/postgresql pod running in the
-- `online-boutique` namespace. Idempotent (IF NOT EXISTS) so the script
-- can be re-run safely.

-- `incidents` is the one-row-per-incident header record: it fixes the
-- (scenario, target) pair an incident_id refers to at creation time, so
-- `incident_events` itself never needs a `target` column and
-- `append_event` can look up `scenario` for its NOT NULL column without
-- the caller re-passing it on every call. Added in Task 7 alongside the
-- orchestrator/db.py repository layer that reads/writes it.
CREATE TABLE IF NOT EXISTS incidents (
    id UUID PRIMARY KEY,
    scenario TEXT NOT NULL,
    target TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS incident_events (
    id SERIAL PRIMARY KEY,
    incident_id UUID NOT NULL,
    scenario TEXT NOT NULL,
    stage TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload JSONB NOT NULL,
    confidence DOUBLE PRECISION,
    latency_ms INTEGER
);

CREATE INDEX IF NOT EXISTS idx_incident_events_incident_id ON incident_events (incident_id);
