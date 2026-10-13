#!/usr/bin/env bash
#
# deploy_online_boutique.sh - Deploy the Google Online Boutique demo
# microservices app into the `online-boutique` namespace of the running
# Minikube cluster.
#
# Prerequisites:
#   - kubectl, already configured against the running Minikube cluster
#   - the `online-boutique` namespace already exists (created by
#     infra/minikube/bootstrap.sh / infra/manifests/namespaces.yaml)
#
# This applies the vendored upstream manifest at
# infra/manifests/online-boutique/kubernetes-manifests.yaml, downloaded as-is
# from the GoogleCloudPlatform/microservices-demo repo
# (release/kubernetes-manifests.yaml).
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
MANIFEST="${REPO_ROOT}/infra/manifests/online-boutique/kubernetes-manifests.yaml"
NAMESPACE="online-boutique"

log() {
  echo "[deploy_online_boutique] $*"
}

main() {
  log "Applying ${MANIFEST} into namespace ${NAMESPACE}"
  kubectl apply -n "${NAMESPACE}" -f "${MANIFEST}"

  log "Deployed resources in namespace ${NAMESPACE}:"
  kubectl get deployments,services -n "${NAMESPACE}"
}

main "$@"
