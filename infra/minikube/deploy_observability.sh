#!/usr/bin/env bash
#
# deploy_observability.sh - Deploy the observability stack (Prometheus +
# Loki/Promtail) into the `monitoring` namespace of the running Minikube
# cluster.
#
# Prerequisites:
#   - kubectl and helm, already configured against the running Minikube
#     cluster
#   - the `monitoring` namespace already exists (created by
#     infra/minikube/bootstrap.sh / infra/manifests/namespaces.yaml)
#
# Installs two Helm releases into `monitoring`:
#   - `prometheus` from prometheus-community/kube-prometheus-stack, using
#     infra/helm-values/prometheus-values.yaml (Grafana and Alertmanager
#     disabled; serviceMonitorSelectorNilUsesHelmValues: false so it scrapes
#     all namespaces; resources/retention trimmed for a shared, memory
#     constrained Minikube node).
#   - `loki` from grafana/loki-stack, using
#     infra/helm-values/loki-values.yaml (single-binary Loki + Promtail
#     daemonset shipping pod logs cluster-wide).
#
# Resulting Service DNS names (as actually installed by these charts with
# release names `prometheus` / `loki` — recorded here after a real install,
# per the task-3 brief):
#   - Prometheus: prometheus-kube-prometheus-prometheus.monitoring.svc.cluster.local:9090
#   - Loki:       loki.monitoring.svc.cluster.local:3100
#
# First-time installs pull several images (prometheus-operator,
# kube-state-metrics, node-exporter, prometheus itself, loki, promtail) and
# can take a few minutes on a host with no local image cache yet. Both
# `helm upgrade --install` calls below pass `--wait` (plus `--timeout 10m`),
# so each one blocks until its release's pods/deployments report Ready
# before the script moves on. That means the final `kubectl get pods -n
# monitoring` in main() reflects pods that have already reached
# Running/Ready, not just pods that were freshly submitted.
#
set -euo pipefail

NAMESPACE="monitoring"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PROMETHEUS_VALUES="${REPO_ROOT}/infra/helm-values/prometheus-values.yaml"
LOKI_VALUES="${REPO_ROOT}/infra/helm-values/loki-values.yaml"

log() {
  echo "[deploy_observability] $*"
}

add_helm_repos() {
  log "Adding/updating Helm repos (prometheus-community, grafana)"
  helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null 2>&1 || true
  helm repo add grafana https://grafana.github.io/helm-charts >/dev/null 2>&1 || true
  helm repo update
}

install_prometheus() {
  log "Installing/upgrading kube-prometheus-stack (release: prometheus) into ${NAMESPACE}"
  helm upgrade --install prometheus prometheus-community/kube-prometheus-stack \
    --namespace "${NAMESPACE}" \
    --values "${PROMETHEUS_VALUES}" \
    --wait \
    --timeout 10m
}

install_loki() {
  log "Installing/upgrading loki-stack (release: loki) into ${NAMESPACE}"
  helm upgrade --install loki grafana/loki-stack \
    --namespace "${NAMESPACE}" \
    --values "${LOKI_VALUES}" \
    --wait \
    --timeout 10m
}

main() {
  add_helm_repos
  install_prometheus
  install_loki

  log "Pods in namespace ${NAMESPACE}:"
  kubectl get pods -n "${NAMESPACE}"
}

main "$@"
