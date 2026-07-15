"""Binds `oci_publish.feature` — packaging/push mechanics against a
throwaway local OCI registry (`registry:2`), independent of the Kind
cluster fixtures (this feature exercises `helm package`/`push`/`pull`
only, not a live install).
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from typing import Iterator

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pytest_bdd import given, scenarios, then, when  # noqa: E402

from conftest import CHART_DIR, CONTAINER_ENGINE, run  # noqa: E402

scenarios("../features/oci_publish.feature")

REGISTRY_CONTAINER_NAME = "pulsusdb-bdd-oci-registry"
REGISTRY_PORT = 15500


@pytest.fixture
def oci_registry() -> Iterator[str]:
    run([CONTAINER_ENGINE, "rm", "-f", REGISTRY_CONTAINER_NAME], check=False)
    run(
        [
            CONTAINER_ENGINE, "run", "-d",
            "--name", REGISTRY_CONTAINER_NAME,
            "-p", f"{REGISTRY_PORT}:5000",
            "docker.io/library/registry:2",
        ],
        timeout=60,
    )
    endpoint = f"localhost:{REGISTRY_PORT}"
    try:
        yield endpoint
    finally:
        run([CONTAINER_ENGINE, "rm", "-f", REGISTRY_CONTAINER_NAME], check=False)


@given("a throwaway OCI registry running locally", target_fixture="registry_endpoint")
def _registry_running(oci_registry: str) -> str:
    return oci_registry


@when("I helm package the chart", target_fixture="packaged_chart")
def _helm_package() -> dict:
    chart_yaml = yaml.safe_load((CHART_DIR / "Chart.yaml").read_text())
    with tempfile.TemporaryDirectory() as tmp:
        run(["helm", "package", str(CHART_DIR), "--destination", tmp])
        tgz = next(Path(tmp).glob("*.tgz"))
        contents = tgz.read_bytes()
        return {"name": chart_yaml["name"], "version": chart_yaml["version"], "bytes": contents, "path": None}


@when("I helm push the packaged chart to the throwaway registry")
def _helm_push(packaged_chart: dict, registry_endpoint: str):
    with tempfile.TemporaryDirectory() as tmp:
        tgz_path = Path(tmp) / f"{packaged_chart['name']}-{packaged_chart['version']}.tgz"
        tgz_path.write_bytes(packaged_chart["bytes"])
        result = run(
            ["helm", "push", str(tgz_path), f"oci://{registry_endpoint}"],
            check=False,
        )
        packaged_chart["push_result"] = result


@then("the push succeeds")
def _push_succeeded(packaged_chart: dict):
    result = packaged_chart["push_result"]
    assert result.returncode == 0, f"helm push failed:\n{result.stdout}\n{result.stderr}"


@when("I helm pull the chart back from the throwaway registry", target_fixture="pulled_chart")
def _helm_pull(packaged_chart: dict, registry_endpoint: str) -> dict:
    with tempfile.TemporaryDirectory() as tmp:
        run(
            [
                "helm", "pull",
                f"oci://{registry_endpoint}/{packaged_chart['name']}",
                "--version", packaged_chart["version"],
                "--destination", tmp,
            ]
        )
        tgz = next(Path(tmp).glob("*.tgz"))
        run(["tar", "xzf", str(tgz), "-C", tmp])
        pulled_chart_yaml = yaml.safe_load((Path(tmp) / packaged_chart["name"] / "Chart.yaml").read_text())
        return pulled_chart_yaml


@then("the pulled Chart.yaml name and version match the packaged chart")
def _pulled_matches(pulled_chart: dict, packaged_chart: dict):
    assert pulled_chart["name"] == packaged_chart["name"]
    assert pulled_chart["version"] == packaged_chart["version"]
