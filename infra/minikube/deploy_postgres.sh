#!/usr/bin/env bash
#
# deploy_postgres.sh - Deploy Postgres (bitnami/postgresql) into the
# `online-boutique` namespace of the running Minikube cluster and apply
# db/schema.sql to create the `incident_events` table used by the
# chaos-to-remediation pipeline.
#
# Prerequisites:
#   - kubectl and helm, already configured against the running Minikube
#     cluster
#   - the `online-boutique` namespace already exists (created by
#     infra/minikube/bootstrap.sh / infra/manifests/namespaces.yaml)
#   - infra/minikube/.env.postgres present, defining POSTGRES_USER and
#     POSTGRES_PASSWORD. This file is gitignored and MUST NOT be committed
#     - copy infra/minikube/.env.postgres.example to
#     infra/minikube/.env.postgres and fill in a real generated password
#     (e.g. `openssl rand -base64 24 | tr -d '/+=' | head -c 32`) before
#     running this script. The committed infra/helm-values/postgres-values.yaml
#     deliberately does NOT contain auth.username/auth.password - those are
#     passed to Helm below via --set, sourced from this env file, so no
#     secret ever lands in a tracked file.
#
# Installs bitnami/postgresql (release: postgres) into `online-boutique`,
# using infra/helm-values/postgres-values.yaml for non-secret settings
# (auth.database: incidents, architecture: standalone, 1Gi persistence).
# Waits for the pod to be ready (--wait), then kubectl exec's into the pod
# to run db/schema.sql via psql against the `incidents` database.
#
set -euo pipefail

NAMESPACE="online-boutique"
RELEASE="postgres"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
POSTGRES_VALUES="${REPO_ROOT}/infra/helm-values/postgres-values.yaml"
SCHEMA_SQL="${REPO_ROOT}/db/schema.sql"
ENV_FILE="${SCRIPT_DIR}/.env.postgres"

log() {
  echo "[deploy_postgres] $*"
}

load_env() {
  if [[ ! -f "${ENV_FILE}" ]]; then
    echo "[deploy_postgres] ERROR: ${ENV_FILE} not found." >&2
    echo "[deploy_postgres] Copy ${SCRIPT_DIR}/.env.postgres.example to ${ENV_FILE} and set a real generated password first." >&2
    exit 1
  fi
  # shellcheck disable=SC1090
  set -a
  source "${ENV_FILE}"
  set +a

  if [[ -z "${POSTGRES_USER:-}" || -z "${POSTGRES_PASSWORD:-}" ]]; then
    echo "[deploy_postgres] ERROR: POSTGRES_USER and POSTGRES_PASSWORD must both be set in ${ENV_FILE}." >&2
    exit 1
  fi
}

add_helm_repo() {
  log "Adding/updating Helm repo (bitnami)"
  helm repo add bitnami https://charts.bitnami.com/bitnami >/dev/null 2>&1 || true
  helm repo update
}

install_postgres() {
  log "Installing/upgrading bitnami/postgresql (release: ${RELEASE}) into ${NAMESPACE}"
  helm upgrade --install "${RELEASE}" bitnami/postgresql \
    --namespace "${NAMESPACE}" \
    --values "${POSTGRES_VALUES}" \
    --set auth.username="${POSTGRES_USER}" \
    --set auth.password="${POSTGRES_PASSWORD}" \
    --wait \
    --timeout 10m
}

apply_schema() {
  local pod
  pod="$(kubectl get pods -n "${NAMESPACE}" -l "app.kubernetes.io/instance=${RELEASE}" -o jsonpath='{.items[0].metadata.name}')"
  log "Applying ${SCHEMA_SQL} to database 'incidents' in pod ${pod}"
  kubectl exec -n "${NAMESPACE}" "${pod}" -i -- env PGPASSWORD="${POSTGRES_PASSWORD}" \
    psql -U "${POSTGRES_USER}" -d incidents < "${SCHEMA_SQL}"
}

verify() {
  local pod
  pod="$(kubectl get pods -n "${NAMESPACE}" -l "app.kubernetes.io/instance=${RELEASE}" -o jsonpath='{.items[0].metadata.name}')"
  log "Verifying incident_events table exists:"
  kubectl exec -n "${NAMESPACE}" "${pod}" -- env PGPASSWORD="${POSTGRES_PASSWORD}" \
    psql -U "${POSTGRES_USER}" -d incidents -c '\dt'
}

main() {
  load_env
  add_helm_repo
  install_postgres
  apply_schema
  verify

  log "Pods in namespace ${NAMESPACE} (postgres):"
  kubectl get pods -n "${NAMESPACE}" -l "app.kubernetes.io/instance=${RELEASE}"
  log "Postgres reachable at ${RELEASE}.${NAMESPACE}.svc.cluster.local:5432, database 'incidents', user '${POSTGRES_USER}'."
}

main "$@"
