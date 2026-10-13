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
#     passed to Helm via a `--values -` YAML fragment on stdin, sourced from
#     this env file, so no secret ever lands in a tracked file, nor as a
#     literal `--set` argument visible to other local users via
#     `ps aux`/`/proc/<pid>/cmdline`.
#
# Installs bitnami/postgresql (release: postgres) into `online-boutique`,
# using infra/helm-values/postgres-values.yaml for non-secret settings
# (auth.database: incidents, architecture: standalone, 1Gi persistence).
# Waits for the pod to be ready (--wait), then kubectl exec's into the pod
# to run db/schema.sql via psql against the `incidents` database - the
# credentials are fed to that remote psql call over stdin too (see
# `_psql_remote_env_prelude`'s comment below), not via a `kubectl exec ...
# -- env PGPASSWORD=...` argument.
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
  # auth.username/auth.password are passed via `--values -` (a YAML
  # fragment on stdin), not `--set auth.password=...`, so the password
  # never appears as a literal command-line argument - `--set` would be
  # visible to any other local user via `ps aux`/`/proc/<pid>/cmdline` for
  # the whole duration of this (potentially slow, --wait'ed) helm command.
  # Matches the stdin-based pattern deploy_orchestrator.sh already uses for
  # the orchestrator-db-credentials Secret's database-url.
  helm upgrade --install "${RELEASE}" bitnami/postgresql \
    --namespace "${NAMESPACE}" \
    --values "${POSTGRES_VALUES}" \
    --values - \
    --wait \
    --timeout 10m <<EOF
auth:
  username: "${POSTGRES_USER}"
  password: "${POSTGRES_PASSWORD}"
EOF
}

# Feeds POSTGRES_PASSWORD/POSTGRES_USER to a remote psql invocation entirely
# over `kubectl exec`'s stdin, never as a command-line argument - `kubectl
# exec ... -- env PGPASSWORD=...` (the previous approach) puts the password
# directly into THIS machine's own `kubectl` process argv, visible via
# `ps aux`/`/proc/<pid>/cmdline` to any other local user for as long as the
# exec runs. The remote shell script below is static (no local variable
# interpolated into it at all); it reads exactly two lines off its own
# stdin, `export`s each as an env var, then runs psql - which, for
# apply_schema, continues reading the REST of that same stdin stream (the
# schema SQL, appended after the two env lines) as its own input, exactly
# as `psql ... < schema.sql` would. Matches the stdin-based pattern
# deploy_orchestrator.sh already uses for the orchestrator-db-credentials
# Secret.
_psql_remote_env_prelude() {
  printf '%s' 'read -r pw_line; eval "export $pw_line"; read -r user_line; eval "export $user_line"; '
}

apply_schema() {
  local pod
  pod="$(kubectl get pods -n "${NAMESPACE}" -l "app.kubernetes.io/instance=${RELEASE}" -o jsonpath='{.items[0].metadata.name}')"
  log "Applying ${SCHEMA_SQL} to database 'incidents' in pod ${pod}"
  {
    printf 'PGPASSWORD=%q\n' "${POSTGRES_PASSWORD}"
    printf 'PGUSER=%q\n' "${POSTGRES_USER}"
    cat "${SCHEMA_SQL}"
  } | kubectl exec -n "${NAMESPACE}" "${pod}" -i -- sh -c "$(_psql_remote_env_prelude)"'psql -U "$PGUSER" -d incidents'
}

verify() {
  local pod
  pod="$(kubectl get pods -n "${NAMESPACE}" -l "app.kubernetes.io/instance=${RELEASE}" -o jsonpath='{.items[0].metadata.name}')"
  log "Verifying incident_events table exists:"
  {
    printf 'PGPASSWORD=%q\n' "${POSTGRES_PASSWORD}"
    printf 'PGUSER=%q\n' "${POSTGRES_USER}"
  } | kubectl exec -n "${NAMESPACE}" "${pod}" -i -- sh -c "$(_psql_remote_env_prelude)"'psql -U "$PGUSER" -d incidents -c "\dt"'
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
