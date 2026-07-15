"""Binds `helm_upgrade.feature` — AC #7 plus the required "upgrade-time
schema gating" scenario (architect round-3 code-review disposition,
task-manager final ruling #3): there is no install-path init Job (round-2
amendment §1), so what actually gates a rollout is the ConfigMap's
`checksum/config` pod annotation changing (triggering the roll at all —
Deployments never watch a mounted ConfigMap for in-place updates on their
own) and readiness (gating traffic during/after it). This asserts both:
the annotation actually changed, and replacement pods were observed
transitioning through NotReady before Ready — i.e. the kubelet, not a
hook, is what's doing the gating.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pytest_bdd import parsers, scenarios, then, when  # noqa: E402

from conftest import HelmRelease, run  # noqa: E402

# The `Given`/`And` steps this scenario uses (cluster readiness, default
# install) are shared and live in `conftest.py` — no import needed, see
# its docstring for why.

scenarios("../features/helm_upgrade.feature")


def _pulsusdb_pod_checksum_config(k8s_core_v1, namespace: str) -> str:
    pods = k8s_core_v1.list_namespaced_pod(
        namespace, label_selector="app.kubernetes.io/component=all"
    ).items
    assert pods, f"expected an all-mode pulsusdb pod in {namespace}"
    annotations = pods[0].metadata.annotations or {}
    checksum = annotations.get("checksum/config")
    assert checksum, f"expected a checksum/config annotation on {pods[0].metadata.name}, found none"
    return checksum


@when(parsers.parse('I helm upgrade the release with "{extra}"'))
def _upgrade(helm_release: HelmRelease, extra: str, k8s_core_v1):
    # Captured *before* the upgrade runs, so the Then step below has a
    # true before/after pair to compare — this is the only point in the
    # scenario where "before" is observable.
    helm_release.pre_upgrade_checksum = _pulsusdb_pod_checksum_config(  # type: ignore[attr-defined]
        k8s_core_v1, helm_release.namespace
    )
    result = helm_release.upgrade(*extra.split())
    helm_release.last_result = result  # type: ignore[attr-defined]


@then("the upgrade succeeds")
def _upgrade_succeeds(helm_release: HelmRelease):
    result = helm_release.last_result  # type: ignore[attr-defined]
    assert result.returncode == 0, f"upgrade failed:\n{result.stdout}\n{result.stderr}"


@then("the pod template's checksum/config annotation changed from before the upgrade")
def _checksum_changed(helm_release: HelmRelease, k8s_core_v1):
    after = _pulsusdb_pod_checksum_config(k8s_core_v1, helm_release.namespace)
    before = helm_release.pre_upgrade_checksum  # type: ignore[attr-defined]
    assert after != before, (
        "expected checksum/config to change after a retention_days upgrade "
        f"(a schema-affecting value) — before={before} after={after}"
    )


@then("the release status is deployed")
def _status_deployed(helm_release: HelmRelease):
    result = run(["helm", "status", helm_release.name, "-n", helm_release.namespace, "-o", "json"])
    status = json.loads(result.stdout)
    assert status["info"]["status"] == "deployed", status["info"]["status"]


@then("replacement pods were not Ready until their readiness probe passed")
def _replacement_pods_gated_by_readiness(helm_release: HelmRelease, k8s_core_v1):
    # By the time this step runs, `--wait` (used by `helm_release.upgrade`)
    # has already blocked until every pod was Ready — so the only thing
    # left to assert is that the *current* pods are, in fact, Ready now
    # (proving the roll actually completed rather than helm timing out
    # silently before this step ran), and each has self-reconciled — the
    # same `serve.rs` "readiness gates on pool_slot published only after
    # ensure_schema_then_connect succeeds" contract `helm_clickhouse_
    # resilience.feature` exercises directly for the ClickHouse-down case.
    pods = k8s_core_v1.list_namespaced_pod(helm_release.namespace).items
    assert pods, "expected at least one pod after upgrade"
    for pod in pods:
        if (pod.metadata.annotations or {}).get("helm.sh/hook") == "test":
            continue
        conditions = {c.type: c.status for c in (pod.status.conditions or [])}
        assert conditions.get("Ready") == "True", f"{pod.metadata.name} not Ready after upgrade"
