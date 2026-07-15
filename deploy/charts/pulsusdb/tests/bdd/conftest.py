"""Session fixtures for the pytest-bdd Kind-cluster behavioural suite
(issue #38 plan round-2 amendment §3): a real `kind` cluster, the
locally-built `pulsusdb` image `kind load`ed into it (never pulled — no
image is published yet, M7), a `kubernetes.client` handle, and a
per-scenario `helm install` -> yield -> `helm uninstall` lifecycle.

**Shared Given/When/Then step definitions also live in this file**
(first real CI run, issue #38: every scenario failed with
`pytest_bdd.exceptions.StepDefinitionNotFoundError` for steps that were
defined in a sibling `steps/common_steps.py` module and merely
`import`ed — plain-Python-side-effect — by each `steps/*.py` module that
needed them). pytest-bdd resolves step definitions from pytest's plugin
registry, which is scoped per test module; a step decorated with
`@given`/`@when`/`@then` in one module is only visible to `scenarios()`
calls in *that same* module unless it is registered somewhere pytest
shares across every test module automatically — which is exactly what
`conftest.py` is for (unlike a plain sibling module, importing it is not
even required: pytest auto-discovers every `conftest.py` up the directory
tree for every test module it collects). `pytest --collect-only` cannot
catch this class of bug — step binding is a runtime concern, collection
only proves the `.feature` files parse and `scenarios()` calls resolve a
file path.

Run (from the repo root, `deploy/charts/pulsusdb/tests/bdd/`'s own
`requirements.txt` installed):

    pytest deploy/charts/pulsusdb/tests/bdd -v --tb=short

Requires `kind`, `docker` (or `podman` via `KIND_EXPERIMENTAL_PROVIDER`),
`helm`, and `kubectl` on PATH. `chart-test-kind`
(.github/workflows/helm-chart.yml) provisions all four.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Iterator

import pytest
import yaml
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config
from pytest_bdd import given, parsers, then, when

BDD_DIR = Path(__file__).resolve().parent
CHART_DIR = BDD_DIR.parents[1]
REPO_ROOT = BDD_DIR.parents[4]

# Same artifact-directory convention the Rust e2e harness already uses for
# its own on-failure dumps (`target/e2e-artifacts/**`,
# `e2e/src/scenarios.rs`) — `chart-test-kind` uploads this directory via
# `actions/upload-artifact` `if: failure()` (code review round-1 finding
# #10).
DIAG_DIR = REPO_ROOT / "target" / "bdd-diagnostics"

CLUSTER_NAME = os.environ.get("PULSUS_BDD_CLUSTER_NAME", "pulsusdb-bdd")
IMAGE_TAG = os.environ.get("PULSUS_BDD_IMAGE_TAG", "pulsusdb:bdd")
KEEP_CLUSTER = os.environ.get("PULSUS_BDD_KEEP_CLUSTER") == "1"
SKIP_BUILD = os.environ.get("PULSUS_BDD_SKIP_BUILD") == "1"
CONTAINER_ENGINE = os.environ.get("PULSUS_BDD_CONTAINER_ENGINE", "docker")

DEFAULT_TIMEOUT = 300  # seconds; generous — bundled ClickHouse + reader cache warmup


def run(
    cmd: list[str],
    *,
    timeout: int = 120,
    check: bool = True,
    env: dict | None = None,
    cwd: str | Path | None = None,
) -> subprocess.CompletedProcess:
    """Runs a subprocess, always capturing stdout/stderr as text so a
    failure's diagnostic is visible in the pytest report (never a swallowed
    `subprocess.DEVNULL`). `cwd` (code review round-1 [high] finding #2 —
    this parameter was missing entirely, so every call site passing it,
    e.g. `docker build .` below, raised a `TypeError` before the Kind
    cluster fixture ever got that far) defaults to the repo root when
    unset, since most commands here (`docker build`, `helm` against a
    relative `CHART_DIR`) are meant to resolve relative to it, not
    whatever directory pytest happened to be invoked from."""
    merged_env = {**os.environ, **(env or {})}
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=merged_env,
        cwd=str(cwd) if cwd is not None else str(REPO_ROOT),
    )
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"command failed ({proc.returncode}): {' '.join(cmd)}\n"
            f"--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}"
        )
    return proc


def dump_diagnostics(label: str, namespace: str | None = None) -> None:
    """Captures Helm/Kubernetes diagnostic state into `DIAG_DIR/<label>/`
    — called from a failing scenario's own fixture teardown, always
    *before* that scenario's namespace (or, at session end, the whole Kind
    cluster) is deleted (code review round-1 finding #10: "session
    teardown deletes Kind before the workflow's failure step, and no
    artifact upload exists" — diagnostics must be captured while the
    thing being diagnosed still exists, not after)."""
    out_dir = DIAG_DIR / label
    out_dir.mkdir(parents=True, exist_ok=True)
    if namespace:
        get_all = run(
            ["kubectl", "get", "all,configmap,secret,pvc,events", "-n", namespace, "-o", "wide"],
            check=False,
        )
        (out_dir / "kubectl-get.txt").write_text(get_all.stdout + get_all.stderr)

        describe = run(["kubectl", "describe", "all", "-n", namespace], check=False)
        (out_dir / "kubectl-describe.txt").write_text(describe.stdout + describe.stderr)

        logs = run(
            ["kubectl", "logs", "-n", namespace, "-l", "app.kubernetes.io/name=pulsusdb", "--all-containers", "--prefix"],
            check=False,
        )
        (out_dir / "pod-logs.txt").write_text(logs.stdout + logs.stderr)
    else:
        # Session-level (kind_cluster teardown) catch-all: every
        # namespace's control-plane + container logs via `kind export
        # logs`, for failures that happen outside any single
        # `helm_release`-scoped scenario (e.g. `oci_publish.feature`,
        # which doesn't use `helm_release` at all, or a `kind_cluster`/
        # `pulsusdb_image` fixture-setup failure itself).
        run(["kind", "export", "logs", str(out_dir / "kind-logs"), "--name", CLUSTER_NAME], check=False)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo):
    """Standard pytest idiom: stashes each phase's outcome on the test
    item as `rep_<when>` so fixture teardown code (which only has
    `request.node`, not the report) can check `request.node.rep_call.failed`
    to decide whether *this* test failed — used by `helm_release`'s
    teardown to capture diagnostics only on failure, before deleting that
    test's namespace."""
    outcome = yield
    report = outcome.get_result()
    setattr(item, f"rep_{report.when}", report)


def _test_failed(request: pytest.FixtureRequest) -> bool:
    setup = getattr(request.node, "rep_setup", None)
    call = getattr(request.node, "rep_call", None)
    return bool((setup and setup.failed) or (call and call.failed))


@pytest.fixture(scope="session")
def kind_cluster(request: pytest.FixtureRequest) -> Iterator[str]:
    """Creates (or reuses, if `PULSUS_BDD_CLUSTER_NAME` already exists) a
    Kind cluster for the whole session, and points `KUBECONFIG` at it for
    every subsequent `helm`/`kubectl` subprocess call this suite makes.
    On teardown, if any test in the session failed, dumps `kind export
    logs` (code review round-1 finding #10) *before* deleting the
    cluster — order matters, this is not just an artifact-upload
    afterthought."""
    existing = run(["kind", "get", "clusters"], check=False)
    if CLUSTER_NAME not in existing.stdout.split():
        run(["kind", "create", "cluster", "--name", CLUSTER_NAME], timeout=DEFAULT_TIMEOUT)

    kubeconfig_path = BDD_DIR / f".kubeconfig-{CLUSTER_NAME}"
    kubeconfig = run(["kind", "get", "kubeconfig", "--name", CLUSTER_NAME]).stdout
    kubeconfig_path.write_text(kubeconfig)
    os.environ["KUBECONFIG"] = str(kubeconfig_path)

    yield CLUSTER_NAME

    if request.session.testsfailed:
        dump_diagnostics("session")

    if not KEEP_CLUSTER:
        run(["kind", "delete", "cluster", "--name", CLUSTER_NAME], check=False)
        kubeconfig_path.unlink(missing_ok=True)


@pytest.fixture(scope="session")
def pulsusdb_image(kind_cluster: str) -> str:
    """Builds the repo `Dockerfile` locally and `kind load`s it — the
    application image is not published yet (M7, issue #38 Dependencies
    section), so this suite never pulls from a registry. Depends on
    `kind_cluster` (fixture ordering, code review round-1 finding #2): the
    image must be loaded into an already-existing cluster, and every
    per-scenario `helm_release` fixture below depends on *both*
    `kind_cluster` and this fixture, in that order, so a scenario's
    `helm install` never races either one being ready."""
    if not SKIP_BUILD:
        run(
            [CONTAINER_ENGINE, "build", "-t", IMAGE_TAG, "."],
            cwd=REPO_ROOT,
            timeout=900,
        )
    run(
        ["kind", "load", "docker-image", IMAGE_TAG, "--name", kind_cluster],
        timeout=300,
    )
    return IMAGE_TAG


@pytest.fixture(scope="session")
def k8s_core_v1(kind_cluster: str) -> k8s_client.CoreV1Api:
    k8s_config.load_kube_config(config_file=os.environ["KUBECONFIG"])
    return k8s_client.CoreV1Api()


@pytest.fixture(scope="session")
def k8s_apps_v1(kind_cluster: str) -> k8s_client.AppsV1Api:
    k8s_config.load_kube_config(config_file=os.environ["KUBECONFIG"])
    return k8s_client.AppsV1Api()


class HelmRelease:
    """A thin handle bundling a release's name/namespace with the `helm`
    subprocess wrapper every step module needs, plus a base
    `--set image.repository=... --set image.tag=...` pin to the
    locally-built-and-loaded image (never a registry pull, see
    `pulsusdb_image`)."""

    def __init__(self, name: str, namespace: str, image: str):
        self.name = name
        self.namespace = namespace
        repo, tag = image.split(":", 1)
        self._image_args = [
            "--set", f"image.repository={repo}",
            "--set", f"image.tag={tag}",
            "--set", "image.pullPolicy=Never",
        ]

    def install(self, *extra_args: str, timeout: str = "5m", wait: bool = True) -> subprocess.CompletedProcess:
        cmd = [
            "helm", "install", self.name, str(CHART_DIR),
            "--namespace", self.namespace, "--create-namespace",
            "--timeout", timeout,
        ]
        if wait:
            cmd.append("--wait")
        cmd += self._image_args
        cmd += list(extra_args)
        # Subprocess-level timeout is a fixed ceiling independent of
        # helm's own `--timeout` above — it exists only to bound a truly
        # hung subprocess; helm's own timeout is what reports a clean
        # "not ready in time" error in the common case.
        return run(cmd, timeout=600, check=False)

    def upgrade(self, *extra_args: str, timeout: str = "5m", wait: bool = True) -> subprocess.CompletedProcess:
        cmd = [
            "helm", "upgrade", self.name, str(CHART_DIR),
            "--namespace", self.namespace,
            "--timeout", timeout,
        ]
        if wait:
            cmd.append("--wait")
        cmd += self._image_args
        cmd += list(extra_args)
        return run(cmd, timeout=600, check=False)

    def uninstall(self) -> subprocess.CompletedProcess:
        return run(
            ["helm", "uninstall", self.name, "--namespace", self.namespace],
            check=False,
        )

    def test(self) -> subprocess.CompletedProcess:
        return run(
            ["helm", "test", self.name, "--namespace", self.namespace],
            timeout=180,
            check=False,
        )

    def get_values(self) -> dict:
        out = run(["helm", "get", "values", self.name, "--namespace", self.namespace, "-o", "yaml"])
        return yaml.safe_load(out.stdout) or {}


@pytest.fixture
def helm_release(request: pytest.FixtureRequest, kind_cluster: str, pulsusdb_image: str) -> Iterator[HelmRelease]:
    """Per-scenario release: a unique namespace + release name so
    scenarios never collide. On teardown: if *this* test failed, dump
    diagnostics for its namespace first (code review round-1 finding #10
    — must happen before the namespace is deleted below, not after);
    always uninstall + delete the namespace afterward regardless of
    pass/fail, to avoid resource exhaustion across a long run of many
    scenarios in one ephemeral cluster."""
    suffix = uuid.uuid4().hex[:8]
    namespace = f"pulsusdb-bdd-{suffix}"
    release = HelmRelease(name="pulsusdb", namespace=namespace, image=pulsusdb_image)

    run(["kubectl", "create", "namespace", namespace], check=False)

    yield release

    if _test_failed(request):
        dump_diagnostics(request.node.name, namespace=namespace)

    release.uninstall()
    run(["kubectl", "delete", "namespace", namespace, "--wait=false"], check=False)


class HttpResponse:
    """A minimal, `requests`-shaped response object — this suite is
    stdlib-only for HTTP (no `requests` dependency: `requirements.txt` is
    pinned to exactly `pytest`/`pytest-bdd`/`kubernetes`/`pyyaml`, issue
    #38 plan round-2 amendment §3)."""

    def __init__(self, status_code: int, text: str):
        self.status_code = status_code
        self.text = text

    def json(self):
        return json.loads(self.text)


def http_get(url: str, params: dict | None = None, timeout: int = 10) -> HttpResponse:
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310 - test-only, localhost port-forward
            return HttpResponse(resp.status, resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return HttpResponse(exc.code, exc.read().decode("utf-8", errors="replace"))


def http_post_json(url: str, body: dict, timeout: int = 10) -> HttpResponse:
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 - test-only, localhost port-forward
            return HttpResponse(resp.status, resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return HttpResponse(exc.code, exc.read().decode("utf-8", errors="replace"))


def wait_for_condition(predicate, *, timeout: int = DEFAULT_TIMEOUT, interval: float = 2.0, description: str = "condition") -> None:
    """Polls `predicate` (a zero-arg callable returning bool) until it
    returns true or `timeout` elapses — the shared polling primitive every
    step module uses instead of a fixed `sleep`, since pod readiness and
    ClickHouse startup latency vary."""
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except Exception as exc:  # noqa: BLE001 - re-raised as the final timeout error below
            last_err = exc
        time.sleep(interval)
    detail = f" (last error: {last_err})" if last_err else ""
    raise TimeoutError(f"timed out waiting for: {description}{detail}")


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


def _port_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def port_forward(namespace: str, service: str, local_port: int, remote_port: int):
    """Starts `kubectl port-forward` as a background process; caller is
    responsible for terminating it. Returns the `Popen` handle. A plain
    helper function (not a pytest-bdd step), so — unlike the steps below —
    it still needs an explicit `from conftest import port_forward` in any
    module that calls it; conftest.py's automatic cross-module visibility
    is a pytest/pytest-bdd fixture-and-step-registry mechanism, not a
    general Python import shortcut."""
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


# === Shared Given/When/Then step definitions ===
# Moved here from the former `steps/common_steps.py` (first real CI run,
# issue #38 — see this module's docstring for the full root-cause
# explanation). Every `steps/*.py` module's `scenarios(...)` call can use
# these without any import at all.


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
