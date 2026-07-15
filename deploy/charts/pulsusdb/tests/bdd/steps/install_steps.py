"""Binds `helm_install_single.feature`, `helm_install_cluster.feature`, and
`helm_split_mode.feature` and implements their scenario-specific steps.
`common_steps` supplies the shared Given/When/Then steps every scenario
here also uses (cluster readiness, default install, pods-Ready, helm test).
"""

from __future__ import annotations

import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # tests/bdd/ (conftest.py)
sys.path.insert(0, str(Path(__file__).resolve().parent))  # tests/bdd/steps/ (sibling step modules)

from pytest_bdd import scenarios, then  # noqa: E402

from conftest import DEFAULT_TIMEOUT, HelmRelease, http_get, http_post_json, wait_for_condition  # noqa: E402

import common_steps  # noqa: F401,E402  (registers shared steps, incl. "I helm install pulsusdb with ...")

scenarios("../features/helm_install_single.feature")
scenarios("../features/helm_install_cluster.feature")
scenarios("../features/helm_split_mode.feature")


@then("the install succeeds")
def _install_succeeds(helm_release: HelmRelease):
    result = getattr(helm_release, "last_result", None)
    assert result is not None and result.returncode == 0, getattr(result, "stderr", "no install attempted")


@then("3 distinct ClickHouse shard StatefulSets are Ready")
def _shard_statefulsets_ready(helm_release: HelmRelease, k8s_apps_v1):
    def _shards_ready() -> bool:
        stss = k8s_apps_v1.list_namespaced_stateful_set(
            helm_release.namespace,
            label_selector="app.kubernetes.io/component=clickhouse",
        ).items
        shard_names = {sts.metadata.name for sts in stss if "-clickhouse-shard-" in sts.metadata.name}
        if len(shard_names) != 3:
            return False
        for sts in stss:
            if "-clickhouse-shard-" not in sts.metadata.name:
                continue
            if (sts.status.ready_replicas or 0) < 1:
                return False
        return True

    wait_for_condition(_shards_ready, timeout=DEFAULT_TIMEOUT, description="3 distinct ClickHouse shard StatefulSets Ready")


@then("a log line ingested through the collector is queryable back through the PulsusDB API")
def _ingest_query_roundtrip(helm_release: HelmRelease):
    marker = f"pulsusdb-bdd-{uuid.uuid4().hex}"
    collector_svc = f"{helm_release.name}-otel-collector"
    pulsusdb_svc = helm_release.name

    collector_proc = common_steps.port_forward(helm_release.namespace, collector_svc, 14318, 4318)
    query_proc = common_steps.port_forward(helm_release.namespace, pulsusdb_svc, 13100, 3100)
    try:
        body = {
            "resourceLogs": [
                {
                    "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "pulsusdb-bdd"}}]},
                    "scopeLogs": [
                        {
                            "logRecords": [
                                {
                                    "timeUnixNano": str(int(time.time() * 1e9)),
                                    "body": {"stringValue": marker},
                                }
                            ]
                        }
                    ],
                }
            ]
        }
        resp = http_post_json("http://127.0.0.1:14318/v1/logs", body, timeout=10)
        assert resp.status_code < 300, f"OTLP ingest failed: {resp.status_code} {resp.text}"

        def _marker_visible() -> bool:
            r = http_get(
                "http://127.0.0.1:13100/api/logs/v1/query_range",
                params={"query": f'{{service_name="pulsusdb-bdd"}} |= "{marker}"'},
                timeout=10,
            )
            if r.status_code != 200:
                return False
            return marker in r.text

        wait_for_condition(_marker_visible, timeout=60, description="ingested log line visible via query API")
    finally:
        collector_proc.terminate()
        query_proc.terminate()


@then("independent writer and reader Deployments both exist with the requested replica counts")
def _split_deployments_exist(helm_release: HelmRelease, k8s_apps_v1):
    writer = k8s_apps_v1.read_namespaced_deployment(f"{helm_release.name}-writer", helm_release.namespace)
    reader = k8s_apps_v1.read_namespaced_deployment(f"{helm_release.name}-reader", helm_release.namespace)
    assert writer.spec.replicas == 2, "default pulsusdb.writer.replicaCount is 2"
    assert reader.spec.replicas == 2, "default pulsusdb.reader.replicaCount is 2"
