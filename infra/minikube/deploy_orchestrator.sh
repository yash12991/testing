#!/usr/bin/env bash
#
# deploy_orchestrator.sh - Build the orchestrator image, load it into the
# running Minikube cluster, and deploy it into the `online-boutique`
# namespace as a Deployment running under the `orchestrator-sa`
# ServiceAccount (Task 6).
#
# Image build/load path:
#   This host has no Docker daemon and runs Minikube with the `podman`
#   driver in a mode where minikube's own CLI (`minikube image build`,
#   `minikube podman-env`, `minikube ssh`, `minikube status`) all shell out
#   to `sudo podman ...` and fail non-interactively with
#   "sudo: a password is required" - even though the Minikube node is a
#   perfectly reachable *rootless* Podman container (`podman container
#   inspect minikube` works fine as the invoking user, no sudo). So instead
#   of `eval $(minikube docker-env/podman-env)`, this script:
#     1. Builds the image directly with the host's rootless `podman build`
#        (no minikube involvement at all).
#     2. `podman save`s it to a tarball.
#     3. `podman cp`s that tarball into the running `minikube` node
#        container.
#     4. `podman exec`s into the node and `ctr -n k8s.io images import`s
#        the tarball directly into containerd's (the node's CRI runtime)
#        image store - the same store the kubelet pulls from.
#   All four steps talk to the `minikube` node container as a plain Podman
#   container by name/ID; none of them go through minikube's own CLI, so
#   none hit the broken sudo status check.
#
#   Verification that the image actually landed uses
#   `podman exec minikube ctr -n k8s.io images ls` (NOT `minikube image ls`,
#   which hits the same broken sudo check and silently reports nothing).
#
# Secret handling:
#   DATABASE_URL (which embeds the Postgres password) is never written to
#   the committed Deployment manifest. This script reads
#   infra/minikube/.env.postgres (gitignored, created by Task 5's
#   deploy_postgres.sh flow - POSTGRES_USER/POSTGRES_PASSWORD) and creates/
#   updates a `orchestrator-db-credentials` Secret holding the full
#   connection string, imperatively, at deploy time - the same
#   read-the-gitignored-env-file-at-deploy-time pattern deploy_postgres.sh
#   uses for Helm's --set.
#
# Prerequisites:
#   - kubectl, podman on PATH, already configured against the running
#     Minikube cluster (podman driver)
#   - infra/minikube/.env.postgres present (see .env.postgres.example),
#     matching the credentials Task 5's deploy_postgres.sh actually used
#   - the `online-boutique` namespace, `orchestrator-sa` ServiceAccount/
#     Role/RoleBinding (Task 6), and Postgres (Task 5) already deployed
#
set -euo pipefail

NAMESPACE="online-boutique"
IMAGE_TAG="orchestrator:dev"
MINIKUBE_NODE_CONTAINER="minikube"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ORCHESTRATOR_DIR="${REPO_ROOT}/orchestrator"
DEPLOYMENT_MANIFEST="${REPO_ROOT}/infra/manifests/orchestrator-deployment.yaml"
ENV_FILE="${SCRIPT_DIR}/.env.postgres"
TAR_PATH="$(mktemp -t orchestrator-dev.XXXXXX.tar)"

log() {
  echo "[deploy_orchestrator] $*"
}

cleanup() {
  rm -f "${TAR_PATH}"
}
trap cleanup EXIT

load_env() {
  if [[ ! -f "${ENV_FILE}" ]]; then
    echo "[deploy_orchestrator] ERROR: ${ENV_FILE} not found." >&2
    echo "[deploy_orchestrator] Run deploy_postgres.sh's setup first (copy .env.postgres.example to .env.postgres and fill in the real password)." >&2
    exit 1
  fi
  # shellcheck disable=SC1090
  set -a
  source "${ENV_FILE}"
  set +a

  if [[ -z "${POSTGRES_USER:-}" || -z "${POSTGRES_PASSWORD:-}" ]]; then
    echo "[deploy_orchestrator] ERROR: POSTGRES_USER and POSTGRES_PASSWORD must both be set in ${ENV_FILE}." >&2
    exit 1
  fi
}

build_image() {
  log "Building ${IMAGE_TAG} with rootless podman (context: ${ORCHESTRATOR_DIR})"
  podman build -t "${IMAGE_TAG}" "${ORCHESTRATOR_DIR}"
}

load_image_into_minikube() {
  log "Saving ${IMAGE_TAG} to ${TAR_PATH}"
  podman save "localhost/${IMAGE_TAG}" -o "${TAR_PATH}"

  log "Copying image tarball into the ${MINIKUBE_NODE_CONTAINER} node container"
  podman cp "${TAR_PATH}" "${MINIKUBE_NODE_CONTAINER}:/tmp/orchestrator-dev.tar"

  log "Importing image into the node's containerd (CRI) image store"
  podman exec "${MINIKUBE_NODE_CONTAINER}" ctr -n k8s.io images import /tmp/orchestrator-dev.tar
  podman exec "${MINIKUBE_NODE_CONTAINER}" rm -f /tmp/orchestrator-dev.tar
}

verify_image_loaded() {
  log "Verifying localhost/${IMAGE_TAG} is present in the node's containerd image store"
  # Capture full output before grepping it (rather than piping `ctr images
  # ls` straight into `grep -q`): `grep -q` exits the instant it matches,
  # which under `set -o pipefail` can race `ctr`'s still-writing output and
  # make the whole pipeline report a spurious non-zero (SIGPIPE) failure
  # even though the image IS present.
  local images_output
  images_output="$(podman exec "${MINIKUBE_NODE_CONTAINER}" ctr -n k8s.io images ls)"
  if ! grep -q "localhost/${IMAGE_TAG}" <<<"${images_output}"; then
    echo "[deploy_orchestrator] ERROR: localhost/${IMAGE_TAG} not found in the node's containerd image store after import." >&2
    exit 1
  fi
}

create_db_secret() {
  local database_url
  database_url="$(python3 -c '
import os
import urllib.parse as up
user = up.quote(os.environ["POSTGRES_USER"], safe="")
pw = up.quote(os.environ["POSTGRES_PASSWORD"], safe="")
print(f"postgresql://{user}:{pw}@postgres.online-boutique.svc.cluster.local:5432/incidents")
')"

  log "Creating/updating orchestrator-db-credentials Secret in ${NAMESPACE}"
  # Built as YAML and piped to `kubectl apply -f -` over stdin (rather than
  # passed via `--from-literal=...` on the command line) so the password
  # embedded in database_url never appears as a process argument visible to
  # other local users via `ps`/`/proc/<pid>/cmdline`.
  local encoded_database_url
  encoded_database_url="$(printf '%s' "${database_url}" | base64 -w0)"
  kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: orchestrator-db-credentials
  namespace: ${NAMESPACE}
type: Opaque
data:
  database-url: ${encoded_database_url}
EOF
}

apply_deployment() {
  log "Applying ${DEPLOYMENT_MANIFEST}"
  kubectl apply -f "${DEPLOYMENT_MANIFEST}"

  # `kubectl apply` alone is a no-op whenever the manifest YAML itself
  # didn't change - but the image behind the static ${IMAGE_TAG} tag can
  # still have changed CONTENT (that's the whole point of this script:
  # rebuild + reload a new image under the same tag). Combined with
  # imagePullPolicy: Never (required since there's no registry - see the
  # Deployment manifest), the kubelet has no trigger to ever notice: it
  # never re-pulls, so the OLD container keeps running under the SAME pod,
  # and `rollout status` below reports success even though the code did
  # not actually change. Confirmed live. `rollout restart` forces a fresh
  # pod (and therefore a fresh image pull/use) unconditionally on every
  # run of this script, regardless of whether the manifest YAML changed.
  log "Restarting the orchestrator Deployment to pick up the freshly loaded image"
  kubectl rollout restart deployment/orchestrator -n "${NAMESPACE}"

  log "Waiting for the orchestrator Deployment to become available"
  kubectl rollout status deployment/orchestrator -n "${NAMESPACE}" --timeout=120s
}

main() {
  load_env
  build_image
  load_image_into_minikube
  verify_image_loaded
  create_db_secret
  apply_deployment

  log "Pods in namespace ${NAMESPACE} (orchestrator):"
  kubectl get pods -n "${NAMESPACE}" -l app=orchestrator
}

main "$@"
