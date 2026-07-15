"""Binds `helm_install_invalid.feature` — AC #4's negative cases proven
against a real API server (the `requests > limits` admission case) and a
real `helm install` (the schema-rejection cases, already covered
statically by `tests/unit/template_invalid_test.yaml`; re-asserted here as
the end-to-end "no objects ever reach the cluster" behaviour).
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pytest_bdd import scenarios, then  # noqa: E402

from conftest import HelmRelease, run  # noqa: E402

import common_steps  # noqa: F401,E402  (supplies the generic "I helm install pulsusdb with ..." When step)

scenarios("../features/helm_install_invalid.feature")


@then("the install fails")
def _install_fails(helm_release: HelmRelease):
    result = getattr(helm_release, "last_result", None)
    assert result is not None and result.returncode != 0, "expected the install to fail, it succeeded"


@then("the failure message names a requests/limits conflict")
def _failure_names_requests_limits(helm_release: HelmRelease):
    result = helm_release.last_result  # type: ignore[attr-defined]
    combined = (result.stdout + result.stderr).lower()
    assert "limit" in combined and ("request" in combined or "must be less than or equal to" in combined), (
        f"expected a requests/limits admission error, got:\n{result.stdout}\n{result.stderr}"
    )


@then("no namespaced objects are created for the release")
def _no_objects_created(helm_release: HelmRelease):
    result = run(
        [
            "kubectl", "get", "all,configmap,secret",
            "-n", helm_release.namespace,
            "-l", f"app.kubernetes.io/instance={helm_release.name}",
            "-o", "name",
        ],
        check=False,
    )
    assert result.stdout.strip() == "", f"expected no objects, found:\n{result.stdout}"
