#!/usr/bin/env bash
#
# bootstrap.sh - Bring up the Minikube cluster for the chaos-engineering /
# auto-remediation pipeline and create its baseline namespaces.
#
# Prerequisites (must already be on PATH):
#   - kubectl
#   - minikube
#   - helm
#
# Driver selection:
#   By default this starts Minikube with the Docker driver
#   (`minikube start --cpus=4 --memory=8192 --driver=docker`), which is the
#   preferred path when a Docker daemon is available.
#
#   On hosts with no Docker daemon but a working *rootless* Podman install,
#   this script falls back to:
#     minikube start --driver=podman --rootless --container-runtime=containerd \
#       --cpus=4 --memory=8192
#   Passing `minikube config set rootless true` alone is NOT sufficient:
#   without `--rootless` on the `start` invocation itself, minikube still
#   shells out to `sudo podman` and fails non-interactively with
#   "sudo: a password is required". `--rootless` must be passed directly to
#   `minikube start`.
#
# Health check:
#   `minikube status` is unreliable under rootless Podman on some hosts: it
#   shells out to `sudo podman container inspect` and can report failure via
#   that path even when the cluster is actually healthy. This script instead
#   polls `kubectl get nodes` and waits for the node to reach `Ready`
#   (observed to take ~45-60s after `minikube start` returns), retrying for
#   up to 2 minutes before giving up.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
NAMESPACES_MANIFEST="${REPO_ROOT}/infra/manifests/namespaces.yaml"

CPUS="${MINIKUBE_CPUS:-4}"
MEMORY="${MINIKUBE_MEMORY:-8192}"

log() {
  echo "[bootstrap] $*"
}

start_minikube() {
  if docker info >/dev/null 2>&1; then
    log "Docker daemon detected; starting Minikube with --driver=docker"
    minikube start --cpus="${CPUS}" --memory="${MEMORY}" --driver=docker
  else
    log "No usable Docker daemon found; falling back to rootless Podman driver"
    minikube start \
      --driver=podman \
      --rootless \
      --container-runtime=containerd \
      --cpus="${CPUS}" \
      --memory="${MEMORY}"
  fi
}

wait_for_node_ready() {
  local timeout_seconds=120
  local interval_seconds=5
  local elapsed=0

  log "Waiting for node to become Ready (up to ${timeout_seconds}s)..."
  while (( elapsed < timeout_seconds )); do
    if kubectl get nodes --no-headers 2>/dev/null | awk '{print $2}' | grep -qx "Ready"; then
      log "Node is Ready."
      return 0
    fi
    sleep "${interval_seconds}"
    elapsed=$(( elapsed + interval_seconds ))
  done

  log "ERROR: node did not reach Ready within ${timeout_seconds}s"
  kubectl get nodes || true
  return 1
}

main() {
  start_minikube
  wait_for_node_ready

  log "Applying namespaces from ${NAMESPACES_MANIFEST}"
  kubectl apply -f "${NAMESPACES_MANIFEST}"

  log "Current namespaces:"
  kubectl get ns
}

main "$@"
