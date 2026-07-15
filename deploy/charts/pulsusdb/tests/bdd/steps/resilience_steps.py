"""Binds `helm_clickhouse_resilience.feature` — the required
"prolonged-ClickHouse-outage" and "reconcile-retry on transient failure"
scenarios (task-manager final ruling #3), plus the round-2 code-review
test gap #5 (startup-time reconcile-retry, not just post-startup outage
recovery): proves the probe-contract claim (issue #38 plan amendment §1)
against a real kubelet — a pod stays running and simply loses readiness
during an outage (or never gains it, if ClickHouse was never up in the
first place), is never restarted, and self-heals (re-reconciles) once
ClickHouse is back, all without any external intervention.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pytest_bdd import scenarios, then, when  # noqa: E402

from conftest import DEFAULT_TIMEOUT, HelmRelease, run, wait_for_condition  # noqa: E402

# The `Given`/`And` steps this scenario uses (cluster readiness, default
# install) are shared and live in `conftest.py` — no import needed, see
# its docstring for why.

scenarios("../features/helm_clickhouse_resilience.feature")


def _pulsusdb_pod(k8s_core_v1, namespace: str):
    pods = k8s_core_v1.list_namespaced_pod(
        namespace, label_selector="app.kubernetes.io/component=all"
    ).items
    assert pods, f"expected exactly one all-mode pod in {namespace}"
    return pods[0]


def _pod_ready(k8s_core_v1, namespace: str) -> bool:
    pod = _pulsusdb_pod(k8s_core_v1, namespace)
    conditions = {c.type: c.status for c in (pod.status.conditions or [])}
    return conditions.get("Ready") == "True"


def _pod_restart_count(k8s_core_v1, namespace: str) -> int:
    pod = _pulsusdb_pod(k8s_core_v1, namespace)
    statuses = pod.status.container_statuses or []
    return sum(s.restart_count for s in statuses if s.name == "pulsusdb")


@when("the bundled ClickHouse StatefulSet is scaled to 0 replicas", target_fixture="baseline_restart_count")
def _scale_clickhouse_down(helm_release: HelmRelease, k8s_core_v1):
    baseline = _pod_restart_count(k8s_core_v1, helm_release.namespace)
    run(
        [
            "kubectl", "scale", "statefulset",
            f"{helm_release.name}-clickhouse",
            "--replicas=0",
            "-n", helm_release.namespace,
        ]
    )
    return baseline


@when(
    "I helm install pulsusdb with pulsusdb.replicaCount=0 and the bundled ClickHouse StatefulSet is confirmed scaled to 0 replicas",
    target_fixture="ch_statefulset_name",
)
def _install_with_zero_pulsusdb_replicas_then_confirm_ch_down(helm_release: HelmRelease, k8s_core_v1):
    # `pulsusdb.replicaCount=0` means no pulsusdb pod is created by this
    # install at all — there is nothing yet that could race against
    # ClickHouse's own startup. `--wait=false` because `--wait` would
    # otherwise block on the ClickHouse StatefulSet (replicas=1 by
    # default) becoming Ready, which we're about to undo anyway.
    result = helm_release.install("--set", "pulsusdb.replicaCount=0", wait=False)
    assert result.returncode == 0, f"install failed:\n{result.stdout}\n{result.stderr}"

    ch_sts = f"{helm_release.name}-clickhouse"
    run(["kubectl", "scale", "statefulset", ch_sts, "--replicas=0", "-n", helm_release.namespace])
    # Force-delete any ClickHouse pod the StatefulSet controller may
    # already have created before the scale-down above landed, then
    # confirm zero ClickHouse pods remain — this is what makes "ClickHouse
    # is down" a *confirmed precondition* rather than a race outcome
    # (code review round-3 disposition: "provably", not "probably").
    run(
        [
            "kubectl", "delete", "pod",
            "-l", "app.kubernetes.io/component=clickhouse",
            "-n", helm_release.namespace,
            "--force", "--grace-period=0", "--ignore-not-found",
        ],
        check=False,
    )
    wait_for_condition(
        lambda: not k8s_core_v1.list_namespaced_pod(
            helm_release.namespace, label_selector="app.kubernetes.io/component=clickhouse"
        ).items,
        timeout=60,
        description="the bundled ClickHouse StatefulSet to be confirmed at zero pods",
    )
    return ch_sts


@when("I scale the pulsusdb Deployment up to 1 replica", target_fixture="baseline_restart_count")
def _scale_pulsusdb_up(helm_release: HelmRelease, k8s_core_v1, ch_statefulset_name: str):
    # Only now — with ClickHouse's absence already confirmed above, not
    # merely assumed — does the pulsusdb pod get created at all. Its
    # *very first* reconcile attempt (crates/pulsus-server/src/serve.rs's
    # `ensure_schema_then_connect`) is therefore provably made against an
    # already-down ClickHouse, not a maybe-still-starting one.
    run(
        [
            "kubectl", "scale", "deployment", helm_release.name,
            "--replicas=1",
            "-n", helm_release.namespace,
        ]
    )
    wait_for_condition(
        lambda: bool(
            k8s_core_v1.list_namespaced_pod(
                helm_release.namespace, label_selector="app.kubernetes.io/component=all"
            ).items
        ),
        timeout=60,
        description="the pulsusdb pod object to exist after scaling up",
    )
    # ClickHouse must still be confirmed absent at this point — otherwise
    # the "before the pod was created" precondition wouldn't hold.
    assert not k8s_core_v1.list_namespaced_pod(
        helm_release.namespace, label_selector="app.kubernetes.io/component=clickhouse"
    ).items, f"expected {ch_statefulset_name} to still have zero pods when the pulsusdb pod was created"
    return _pod_restart_count(k8s_core_v1, helm_release.namespace)


@then("the pulsusdb pod stays NotReady without restarting for a sustained window")
def _stays_not_ready_sustained(helm_release: HelmRelease, k8s_core_v1, baseline_restart_count: int):
    # Samples repeatedly over a sustained window rather than checking
    # once — a single point-in-time check could miss a brief flap
    # (Ready-then-restarted-then-NotReady) that a real restart-storm
    # would produce. `serve.rs`'s retry loop keeps attempting
    # `ensure_schema_then_connect` in a loop the whole time; `/ready`
    # must stay 503 (pool_slot never published) throughout, and TCP
    # liveness must never fail this fresh pod out from under it.
    deadline_checks = 6
    interval = 5
    for _ in range(deadline_checks):
        assert not _pod_ready(k8s_core_v1, helm_release.namespace), (
            "expected the pod to remain NotReady while ClickHouse is unavailable "
            "from install time, but it reported Ready"
        )
        current = _pod_restart_count(k8s_core_v1, helm_release.namespace)
        assert current == baseline_restart_count, (
            f"expected no restarts while ClickHouse was unavailable from install time, "
            f"baseline={baseline_restart_count} current={current}"
        )
        time.sleep(interval)


@when("I wait for the pulsusdb pod to report NotReady")
def _wait_not_ready(helm_release: HelmRelease, k8s_core_v1):
    wait_for_condition(
        lambda: not _pod_ready(k8s_core_v1, helm_release.namespace),
        timeout=180,
        description="pulsusdb pod to report NotReady after ClickHouse outage",
    )


@then("the pulsusdb pod has not restarted")
def _pod_not_restarted(helm_release: HelmRelease, k8s_core_v1, baseline_restart_count: int):
    current = _pod_restart_count(k8s_core_v1, helm_release.namespace)
    assert current == baseline_restart_count, (
        f"expected no restarts during the ClickHouse outage, "
        f"baseline={baseline_restart_count} current={current}"
    )


@when("the bundled ClickHouse StatefulSet is scaled back to 1 replica")
def _scale_clickhouse_up(helm_release: HelmRelease):
    run(
        [
            "kubectl", "scale", "statefulset",
            f"{helm_release.name}-clickhouse",
            "--replicas=1",
            "-n", helm_release.namespace,
        ]
    )
    # Bundled ClickHouse itself needs to become ready before pulsusdb's own
    # reconcile-retry loop (crates/pulsus-server/src/serve.rs) can succeed.
    wait_for_condition(
        lambda: run(
            [
                "kubectl", "get", "statefulset", f"{helm_release.name}-clickhouse",
                "-n", helm_release.namespace,
                "-o", "jsonpath={.status.readyReplicas}",
            ],
            check=False,
        ).stdout.strip()
        == "1",
        timeout=DEFAULT_TIMEOUT,
        description="bundled ClickHouse StatefulSet Ready again",
    )


@then("the pulsusdb pod reports Ready again within the timeout budget")
def _pod_ready_again(helm_release: HelmRelease, k8s_core_v1):
    wait_for_condition(
        lambda: _pod_ready(k8s_core_v1, helm_release.namespace),
        timeout=DEFAULT_TIMEOUT,
        description="pulsusdb pod to self-reconcile and report Ready again",
    )


@then("the pulsusdb pod still has not restarted")
def _pod_still_not_restarted(helm_release: HelmRelease, k8s_core_v1, baseline_restart_count: int):
    _pod_not_restarted(helm_release, k8s_core_v1, baseline_restart_count)
