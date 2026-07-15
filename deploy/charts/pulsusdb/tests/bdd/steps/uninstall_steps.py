"""Binds `helm_uninstall.feature` — AC #8, PVC-retention behaviour. The
`Given` steps this scenario uses are shared and live in `conftest.py`
(no import needed — see its docstring for why)."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pytest_bdd import scenarios, then, when  # noqa: E402

from conftest import HelmRelease, run  # noqa: E402

scenarios("../features/helm_uninstall.feature")


@when("I helm uninstall the release")
def _uninstall(helm_release: HelmRelease):
    result = helm_release.uninstall()
    helm_release.last_result = result  # type: ignore[attr-defined]
    assert result.returncode == 0, f"uninstall failed:\n{result.stdout}\n{result.stderr}"


@then("no labelled Deployments, Services, ConfigMaps, or Secrets remain for the release")
def _no_objects_remain(helm_release: HelmRelease):
    result = run(
        [
            "kubectl", "get", "deployment,service,configmap,secret",
            "-n", helm_release.namespace,
            "-l", f"app.kubernetes.io/instance={helm_release.name}",
            "-o", "name",
        ],
        check=False,
    )
    assert result.stdout.strip() == "", f"objects still present after uninstall:\n{result.stdout}"


@then("any PersistentVolumeClaims are retained per the chart's documented persistence policy")
def _pvcs_retained(helm_release: HelmRelease):
    # `helm uninstall` never deletes PVCs created by a StatefulSet's
    # volumeClaimTemplates (Kubernetes' own documented StatefulSet
    # behaviour, not something this chart configures) — this assertion
    # documents that expectation and lets a future PVC-retention policy
    # change (e.g. an explicit Job to reclaim them) be caught here.
    result = run(
        [
            "kubectl", "get", "pvc",
            "-n", helm_release.namespace,
            "-l", f"app.kubernetes.io/instance={helm_release.name}",
            "-o", "name",
        ],
        check=False,
    )
    # Default values have persistence.enabled=true, so bundled ClickHouse's
    # data PVC is expected to still exist post-uninstall.
    assert "persistentvolumeclaim" in result.stdout, (
        f"expected the ClickHouse data PVC to be retained after uninstall, found:\n{result.stdout}"
    )
