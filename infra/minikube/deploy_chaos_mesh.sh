#!/usr/bin/env bash
#
# deploy_chaos_mesh.sh - Deploy Chaos Mesh into the `chaos-mesh` namespace
# of the running Minikube cluster, scoped to inject faults into pods in
# `online-boutique`.
#
# Prerequisites:
#   - kubectl and helm, already configured against the running Minikube
#     cluster
#   - the `chaos-mesh` namespace already exists (created by
#     infra/minikube/bootstrap.sh / infra/manifests/namespaces.yaml)
#
# Installs the chaos-mesh/chaos-mesh Helm chart (release: chaos-mesh) into
# `chaos-mesh`, using infra/helm-values/chaos-mesh-values.yaml. See that
# file's comments for why chaosDaemon.runtime is set to containerd (not
# the chart's default docker) and why clusterScoped/targetNamespace (not a
# webhook namespaceSelector) is used to restrict Chaos Mesh to the
# online-boutique namespace.
#
set -euo pipefail

NAMESPACE="chaos-mesh"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CHAOS_MESH_VALUES="${REPO_ROOT}/infra/helm-values/chaos-mesh-values.yaml"

log() {
  echo "[deploy_chaos_mesh] $*"
}

add_helm_repo() {
  log "Adding/updating Helm repo (chaos-mesh)"
  helm repo add chaos-mesh https://charts.chaos-mesh.org >/dev/null 2>&1 || true
  helm repo update
}

install_chaos_mesh() {
  log "Installing/upgrading chaos-mesh (release: chaos-mesh) into ${NAMESPACE}"
  helm upgrade --install chaos-mesh chaos-mesh/chaos-mesh \
    --namespace "${NAMESPACE}" \
    --values "${CHAOS_MESH_VALUES}" \
    --wait \
    --timeout 10m
}

main() {
  add_helm_repo
  install_chaos_mesh

  log "Pods in namespace ${NAMESPACE}:"
  kubectl get pods -n "${NAMESPACE}"

  log "Chaos Mesh CRDs:"
  kubectl get crds | grep chaos-mesh || true
}

main "$@"
