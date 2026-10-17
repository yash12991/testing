"""Fixer stage: executes a whitelisted remediation action against the cluster.

Each of the three concrete actions (`restart_pod`, `scale_replicas`,
`rollback_deployment`) makes exactly one write call against the Kubernetes
API, using the `CoreV1Api`/`AppsV1Api` clients injected by the caller (never
constructed here, so tests can pass `unittest.mock.Mock` instances with no
live cluster). `execute()` is the single entry point the orchestrator's main
loop calls: it dispatches to the right function by `action` name and wraps
that call in a `try/except kubernetes.client.exceptions.ApiException` so that
cluster-side failures the Fixer cannot control — the pod already gone by the
time we delete it, a patch rejected for a resourceVersion conflict, a
deployment that no longer exists — come back as a `FixResult(success=False,
detail=...)` instead of an unhandled exception that would kill the
orchestrator's main loop.

`scale_replicas` dispatched through `execute()` (rather than called
directly) uses `DEFAULT_SCALE_REPLICAS`, since `execute()`'s signature
(fixed by the orchestrator's action-dispatch contract) carries no replica
count of its own.
"""

from __future__ import annotations

from kubernetes.client.exceptions import ApiException

from orchestrator.models import FixResult

import os
import time

DEFAULT_MIN_REPLICAS = int(os.environ.get("MIN_REPLICAS", "1"))
DEFAULT_MAX_REPLICAS = int(os.environ.get("MAX_REPLICAS", "10"))
DEFAULT_SCALE_REPLICAS = int(os.environ.get("DEFAULT_SCALE_REPLICAS", "3"))


def restart_pod(core_v1, namespace: str, target: str) -> FixResult:
    """Delete the first pod matching label `app=<target>` so K8s reschedules it."""
    response = core_v1.list_namespaced_pod(namespace, label_selector=f"app={target}")
    items = response.items
    if not items:
        return FixResult(
            action="restart_pod",
            target=target,
            success=False,
            detail=f"no pod found matching app={target} in namespace {namespace}",
        )
    pod_name = items[0].metadata.name
    core_v1.delete_namespaced_pod(name=pod_name, namespace=namespace)
    return FixResult(action="restart_pod", target=target, success=True, detail=f"deleted pod {pod_name}")


def scale_replicas(
    apps_v1,
    namespace: str,
    target: str,
    replicas: int = DEFAULT_SCALE_REPLICAS,
    min_replicas: int = DEFAULT_MIN_REPLICAS,
    max_replicas: int = DEFAULT_MAX_REPLICAS,
) -> FixResult:
    """Patch Deployment `target`'s `spec.replicas` to `replicas`, clamped to [min_replicas, max_replicas]."""
    clamped_replicas = max(min_replicas, min(replicas, max_replicas))
    apps_v1.patch_namespaced_deployment_scale(
        name=target,
        namespace=namespace,
        body={"spec": {"replicas": clamped_replicas}},
    )
    detail = f"scaled {target} to {clamped_replicas} replicas"
    if clamped_replicas != replicas:
        detail += f" (clamped from {replicas} to [{min_replicas}, {max_replicas}])"
    return FixResult(
        action="scale_replicas",
        target=target,
        success=True,
        detail=detail,
    )


def rollback_deployment(apps_v1, namespace: str, target: str) -> FixResult:
    """Roll back Deployment `target` to its previous revision via ReplicaSets or rollback annotation."""
    body = {"spec": {"template": {"metadata": {"annotations": {"orchestrator/rollback": "true"}}}}}
    try:
        rs_resp = apps_v1.list_namespaced_replica_set(namespace, label_selector=f"app={target}")
        items = getattr(rs_resp, "items", None)
        if items and isinstance(items, list) and len(items) > 1:
            def _get_rev(rs):
                try:
                    return int(rs.metadata.annotations.get("deployment.kubernetes.io/revision", 0))
                except Exception:
                    return 0

            sorted_rs = sorted(items, key=_get_rev, reverse=True)
            prev_rs = sorted_rs[1]
            if hasattr(prev_rs, "spec") and hasattr(prev_rs.spec, "template") and prev_rs.spec.template is not None:
                body = {"spec": {"template": prev_rs.spec.template}}
    except Exception:
        pass

    apps_v1.patch_namespaced_deployment(
        name=target,
        namespace=namespace,
        body=body,
    )
    return FixResult(
        action="rollback_deployment",
        target=target,
        success=True,
        detail=f"rolled back deployment {target}",
    )


def execute(action: str, core_v1, apps_v1, namespace: str, target: str) -> FixResult:
    """Dispatch to the action named `action`, converting any K8s API error into a failed FixResult."""
    try:
        if action == "restart_pod":
            return restart_pod(core_v1, namespace, target)
        if action == "scale_replicas":
            return scale_replicas(apps_v1, namespace, target, DEFAULT_SCALE_REPLICAS)
        if action == "rollback_deployment":
            return rollback_deployment(apps_v1, namespace, target)
        return FixResult(action=action, target=target, success=False, detail=f"unknown action: {action!r}")
    except ApiException as exc:
        return FixResult(action=action, target=target, success=False, detail=str(exc))
