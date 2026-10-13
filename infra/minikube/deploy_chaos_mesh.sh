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
# the chart's default docker).
#
# IMPORTANT (corrected, final-review Fix 5 — this previously claimed the
# opposite): `clusterScoped` is left at the chart default (`true`), with
# cluster-wide RBAC — `targetNamespace`/`clusterScoped: false` is NOT used
# and does NOT restrict Chaos Mesh to the online-boutique namespace at the
# Helm/RBAC level (see chaos-mesh-values.yaml's comments for why that
# namespace-scoped mode is actually broken in chart 2.8.4). Namespace
# scoping here is ONLY a naming convention every chaos CR in this project
# must follow: each manifest under infra/manifests/chaos-experiments/ sets
# `spec.selector.namespaces: [online-boutique]` itself. Nothing in Helm or
# RBAC enforces that convention — a future chaos manifest that omits or
# changes that selector could target any namespace in the cluster, and
# nothing here would stop it.
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
