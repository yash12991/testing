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
