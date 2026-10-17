"""Tests for orchestrator.fixer — constrained remediation executor.

Pure mock tests: no live cluster. `CoreV1Api`/`AppsV1Api` are replaced with
`unittest.mock.Mock` instances, so these tests exercise only the Fixer's own
logic (which calls it makes, with what arguments, and how it turns cluster
responses/errors into a `FixResult`) and never touch a real Kubernetes API.

The Review Focus for this task is that a Kubernetes API error during
`execute()` — the pod already deleted by the time we go to delete it, a
patch rejected for a resourceVersion conflict, a deployment that no longer
exists — must come back as `FixResult(success=False, detail=...)` rather
than propagate as an unhandled exception that would kill the orchestrator's
main loop. `test_execute_catches_*` below pins exactly that for each of the
three whitelisted actions, not just the single "list fails" case.

Note on Mock pods: `unittest.mock.Mock(name="x")` does NOT set an attribute
named `name` to `"x"` — the `name` keyword is reserved by the Mock
constructor to label the mock's own repr. To make a fake pod whose
`.metadata.name` attribute actually reads back as the given string, the
attribute must be assigned after construction (`pod.metadata.name = "x"`),
which is what `_make_pod` below does.
"""

from __future__ import annotations

from unittest.mock import Mock

from kubernetes.client.exceptions import ApiException

from orchestrator import fixer
from orchestrator.models import FixResult

NAMESPACE = "online-boutique"


def _make_pod(name: str) -> Mock:
    pod = Mock()
    pod.metadata.name = name
    return pod


# --- restart_pod ---------------------------------------------------------


def test_restart_pod_deletes_matching_pod():
    core_v1 = Mock()
    core_v1.list_namespaced_pod.return_value = Mock(items=[_make_pod("cartservice-abc123")])
    result = fixer.restart_pod(core_v1, NAMESPACE, "cartservice")
    core_v1.delete_namespaced_pod.assert_called_once_with(name="cartservice-abc123", namespace=NAMESPACE)
    assert result == FixResult(
        action="restart_pod", target="cartservice", success=True, detail="deleted pod cartservice-abc123"
    )


def test_restart_pod_uses_app_label_selector():
    core_v1 = Mock()
    core_v1.list_namespaced_pod.return_value = Mock(items=[_make_pod("cartservice-abc123")])
    fixer.restart_pod(core_v1, NAMESPACE, "cartservice")
    core_v1.list_namespaced_pod.assert_called_once_with(NAMESPACE, label_selector="app=cartservice")


def test_restart_pod_no_matching_pod_returns_failure_without_deleting():
    core_v1 = Mock()
    core_v1.list_namespaced_pod.return_value = Mock(items=[])
    result = fixer.restart_pod(core_v1, NAMESPACE, "cartservice")
    core_v1.delete_namespaced_pod.assert_not_called()
    assert result.success is False
    assert "cartservice" in result.detail


# --- scale_replicas --------------------------------------------------------


def test_scale_replicas_patches_deployment():
    apps_v1 = Mock()
    result = fixer.scale_replicas(apps_v1, NAMESPACE, "recommendationservice", 3)
    apps_v1.patch_namespaced_deployment_scale.assert_called_once()
    assert result.success is True


def test_scale_replicas_patches_with_requested_replica_count():
    apps_v1 = Mock()
    fixer.scale_replicas(apps_v1, NAMESPACE, "recommendationservice", 5)
    _, kwargs = apps_v1.patch_namespaced_deployment_scale.call_args
    assert kwargs["name"] == "recommendationservice"
    assert kwargs["namespace"] == NAMESPACE
    assert kwargs["body"]["spec"]["replicas"] == 5


def test_scale_replicas_clamps_to_max_replicas():
    apps_v1 = Mock()
    result = fixer.scale_replicas(apps_v1, NAMESPACE, "recommendationservice", 25, min_replicas=1, max_replicas=10)
    _, kwargs = apps_v1.patch_namespaced_deployment_scale.call_args
    assert kwargs["body"]["spec"]["replicas"] == 10
    assert "clamped from 25" in result.detail


def test_scale_replicas_clamps_to_min_replicas():
    apps_v1 = Mock()
    result = fixer.scale_replicas(apps_v1, NAMESPACE, "recommendationservice", 0, min_replicas=1, max_replicas=10)
    _, kwargs = apps_v1.patch_namespaced_deployment_scale.call_args
    assert kwargs["body"]["spec"]["replicas"] == 1
    assert "clamped from 0" in result.detail


# --- rollback_deployment ----------------------------------------------------


def test_rollback_deployment_patches_deployment():
    apps_v1 = Mock()
    result = fixer.rollback_deployment(apps_v1, NAMESPACE, "productcatalogservice")
    apps_v1.patch_namespaced_deployment.assert_called_once()
    _, kwargs = apps_v1.patch_namespaced_deployment.call_args
    assert kwargs["name"] == "productcatalogservice"
    assert kwargs["namespace"] == NAMESPACE
    assert result == FixResult(
        action="rollback_deployment",
        target="productcatalogservice",
        success=True,
        detail="rolled back deployment productcatalogservice",
    )


def test_rollback_deployment_restores_previous_replicaset_template():
    apps_v1 = Mock()
    rs_current = Mock()
    rs_current.metadata.annotations = {"deployment.kubernetes.io/revision": "2"}
    rs_current.spec.template = {"metadata": {"labels": {"version": "v2"}}}

    rs_prev = Mock()
    rs_prev.metadata.annotations = {"deployment.kubernetes.io/revision": "1"}
    rs_prev.spec.template = {"metadata": {"labels": {"version": "v1"}}}

    apps_v1.list_namespaced_replica_set.return_value = Mock(items=[rs_current, rs_prev])
    result = fixer.rollback_deployment(apps_v1, NAMESPACE, "frontend")
    apps_v1.patch_namespaced_deployment.assert_called_once()
    _, kwargs = apps_v1.patch_namespaced_deployment.call_args
    assert kwargs["body"]["spec"]["template"] == {"metadata": {"labels": {"version": "v1"}}}
    assert result.success is True



# --- execute: dispatch -------------------------------------------------------


def test_execute_dispatches_by_action_name():
    core_v1, apps_v1 = Mock(), Mock()
    core_v1.list_namespaced_pod.return_value = Mock(items=[_make_pod("cartservice-abc123")])
    result = fixer.execute("restart_pod", core_v1, apps_v1, NAMESPACE, "cartservice")
    assert result.action == "restart_pod"
    assert result.success is True
    apps_v1.patch_namespaced_deployment_scale.assert_not_called()
    apps_v1.patch_namespaced_deployment.assert_not_called()


def test_execute_dispatches_scale_replicas():
    core_v1, apps_v1 = Mock(), Mock()
    result = fixer.execute("scale_replicas", core_v1, apps_v1, NAMESPACE, "recommendationservice")
    assert result.action == "scale_replicas"
    assert result.success is True
    apps_v1.patch_namespaced_deployment_scale.assert_called_once()
    core_v1.list_namespaced_pod.assert_not_called()


def test_execute_dispatches_rollback_deployment():
    core_v1, apps_v1 = Mock(), Mock()
    result = fixer.execute("rollback_deployment", core_v1, apps_v1, NAMESPACE, "productcatalogservice")
    assert result.action == "rollback_deployment"
    assert result.success is True
    apps_v1.patch_namespaced_deployment.assert_called_once()


def test_execute_rejects_unknown_action_without_raising():
    core_v1, apps_v1 = Mock(), Mock()
    result = fixer.execute("delete_everything", core_v1, apps_v1, NAMESPACE, "cartservice")
    assert result.success is False
    assert result.action == "delete_everything"
    core_v1.list_namespaced_pod.assert_not_called()
    apps_v1.patch_namespaced_deployment_scale.assert_not_called()
    apps_v1.patch_namespaced_deployment.assert_not_called()


# --- execute: Review Focus — K8s API errors must not escape as exceptions ---


def test_execute_catches_k8s_api_exception_and_returns_failure():
    """Brief's pinned case: listing pods fails outright (e.g. RBAC/connectivity)."""
    core_v1, apps_v1 = Mock(), Mock()
    core_v1.list_namespaced_pod.side_effect = ApiException(status=404, reason="Not Found")
    result = fixer.execute("restart_pod", core_v1, apps_v1, NAMESPACE, "cartservice")
    assert result.success is False
    assert "Not Found" in result.detail
    assert isinstance(result, FixResult)


def test_execute_catches_pod_already_deleted_race_during_delete():
    """Pod existed at list time but is gone by delete time (404 from delete, not list)."""
    core_v1, apps_v1 = Mock(), Mock()
    core_v1.list_namespaced_pod.return_value = Mock(items=[_make_pod("cartservice-abc123")])
    core_v1.delete_namespaced_pod.side_effect = ApiException(status=404, reason="Not Found")
    result = fixer.execute("restart_pod", core_v1, apps_v1, NAMESPACE, "cartservice")
    assert result == FixResult(action="restart_pod", target="cartservice", success=False, detail=result.detail)
    assert result.success is False
    assert "Not Found" in result.detail


def test_execute_catches_patch_conflict_on_scale_replicas():
    """Deployment was patched concurrently; the API rejects ours with a 409 Conflict."""
    core_v1, apps_v1 = Mock(), Mock()
    apps_v1.patch_namespaced_deployment_scale.side_effect = ApiException(status=409, reason="Conflict")
    result = fixer.execute("scale_replicas", core_v1, apps_v1, NAMESPACE, "recommendationservice")
    assert result.success is False
    assert result.action == "scale_replicas"
    assert "Conflict" in result.detail


def test_execute_catches_deployment_not_found_on_rollback():
    """Target deployment doesn't exist (e.g. deleted or never deployed)."""
    core_v1, apps_v1 = Mock(), Mock()
    apps_v1.patch_namespaced_deployment.side_effect = ApiException(status=404, reason="Not Found")
    result = fixer.execute("rollback_deployment", core_v1, apps_v1, NAMESPACE, "productcatalogservice")
    assert result.success is False
    assert result.action == "rollback_deployment"
    assert "Not Found" in result.detail


def test_execute_returns_fixresult_not_raises_for_every_whitelisted_action_on_api_error():
    """Belt-and-suspenders: no whitelisted action lets an ApiException escape execute()."""
    for action, raiser in (
        ("restart_pod", "list_namespaced_pod"),
        ("scale_replicas", "patch_namespaced_deployment_scale"),
        ("rollback_deployment", "patch_namespaced_deployment"),
    ):
        core_v1, apps_v1 = Mock(), Mock()
        getattr(core_v1 if raiser == "list_namespaced_pod" else apps_v1, raiser).side_effect = ApiException(
            status=500, reason="Internal Server Error"
        )
        try:
            result = fixer.execute(action, core_v1, apps_v1, NAMESPACE, "some-target")
        except ApiException:
            raise AssertionError(f"execute({action!r}) let an ApiException escape")
        assert isinstance(result, FixResult)
        assert result.success is False
