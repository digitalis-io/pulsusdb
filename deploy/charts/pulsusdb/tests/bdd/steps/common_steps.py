"""Shared Given/When/Then step implementations, imported by every other
step module in this package (issue #38 plan round-2 amendment §3's
`common_steps.py`). Feature-specific step modules `scenarios(...)`-bind
their own `.feature` file and `import` this module for its side effect of
registering these shared steps with pytest-bdd.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

# Plain sys.path insertion (not a package-relative import) so every step
# module in this directory can `import conftest` regardless of pytest's
# import mode — `tests/bdd/` (conftest.py's directory) has no `__init__.py`
# and is not meant to be a package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pytest_bdd import given, parsers, then, when  # noqa: E402

from conftest import DEFAULT_TIMEOUT, HelmRelease, wait_for_condition  # noqa: E402


def _pods_ready(k8s_core_v1, namespace: str, label_selector: str | None = None) -> bool:
    pods = k8s_core_v1.list_namespaced_pod(namespace, label_selector=label_selector or "").items
    if not pods:
        return False
    for pod in pods:
        # Helm test-hook Pods (annotated helm.sh/hook: test) are excluded —
        # they are not part of the release's steady-state workload and are
        # only run on-demand via `helm test`.
        if (pod.metadata.annotations or {}).get("helm.sh/hook") == "test":
            continue
        conditions = {c.type: c.status for c in (pod.status.conditions or [])}
        if conditions.get("Ready") != "True":
            return False
    return True


@given("a Kind cluster with the locally-built pulsusdb image loaded", target_fixture="cluster_ready")
def _cluster_ready(kind_cluster, pulsusdb_image):
    return pulsusdb_image


@when(parsers.parse("I helm install pulsusdb with default values"))
def _install_default(helm_release: HelmRelease):
    result = helm_release.install()
    assert result.returncode == 0, result.stderr


@given("a running pulsusdb release installed with default values", target_fixture="running_release")
def _running_release(helm_release: HelmRelease, k8s_core_v1):
    result = helm_release.install()
    assert result.returncode == 0, result.stderr
    wait_for_condition(
        lambda: _pods_ready(k8s_core_v1, helm_release.namespace),
        timeout=DEFAULT_TIMEOUT,
        description=f"initial install Ready in namespace {helm_release.namespace}",
    )
    return helm_release


@when(parsers.parse('I helm install pulsusdb with "{extra}"'))
def _install_with_extra(helm_release: HelmRelease, extra: str):
    args = extra.split()
    result = helm_release.install(*args)
    helm_release.last_result = result  # type: ignore[attr-defined]


@then(parsers.parse("every pod in the release reaches Ready within the timeout budget"))
def _pods_reach_ready(helm_release: HelmRelease, k8s_core_v1):
    wait_for_condition(
        lambda: _pods_ready(k8s_core_v1, helm_release.namespace),
        timeout=DEFAULT_TIMEOUT,
        description=f"all pods Ready in namespace {helm_release.namespace}",
    )


@then("the bundled helm test hook exits successfully")
def _helm_test_passes(helm_release: HelmRelease):
    result = helm_release.test()
    assert result.returncode == 0, f"helm test failed:\n{result.stdout}\n{result.stderr}"


def port_forward(namespace: str, service: str, local_port: int, remote_port: int):
    """Starts `kubectl port-forward` as a background process; caller is
    responsible for terminating it. Returns the `Popen` handle."""
    proc = subprocess.Popen(
        [
            "kubectl", "port-forward",
            "-n", namespace,
            f"svc/{service}",
            f"{local_port}:{remote_port}",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # kubectl port-forward has no readiness signal on stdout we can trust
    # portably; poll the local port instead of a fixed sleep.
    wait_for_condition(
        lambda: _port_open(local_port),
        timeout=30,
        description=f"port-forward {service}:{remote_port} -> localhost:{local_port}",
    )
    return proc


def _port_open(port: int) -> bool:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1)
        return sock.connect_ex(("127.0.0.1", port)) == 0
