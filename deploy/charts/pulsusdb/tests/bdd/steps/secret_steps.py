"""Binds `helm_clickhouse_secret.feature` — the required "bundled-
ClickHouse Secret non-default + redaction" scenario (task-manager final
ruling #3, extended per the architect's round-3 code-review disposition's
"secret-not-in-logs" test gap): proves against a live cluster that the
bundled ClickHouse is never passwordless, its password never lands in the
ConfigMap or the pulsusdb pod's own logs, and the running pulsusdb
process's own `/config` endpoint redacts it.
"""

from __future__ import annotations

import base64
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pytest_bdd import scenarios, then  # noqa: E402

from conftest import HelmRelease, http_get, run  # noqa: E402

import common_steps  # noqa: F401,E402

scenarios("../features/helm_clickhouse_secret.feature")


@then("a ClickHouse credentials Secret exists with a non-empty, non-default password")
def _secret_has_password(helm_release: HelmRelease, k8s_core_v1):
    secret = k8s_core_v1.read_namespaced_secret(f"{helm_release.name}-clickhouse", helm_release.namespace)
    password_b64 = secret.data["password"]
    password = base64.b64decode(password_b64).decode("utf-8")
    assert password, "expected a non-empty generated ClickHouse password"
    assert password not in ("", "default", "password", "changeme"), (
        f"generated password looks like a placeholder default: {password!r}"
    )


@then("the release's ConfigMap contains no password value anywhere in its data")
def _configmap_has_no_password(helm_release: HelmRelease, k8s_core_v1):
    secret = k8s_core_v1.read_namespaced_secret(f"{helm_release.name}-clickhouse", helm_release.namespace)
    password = base64.b64decode(secret.data["password"]).decode("utf-8")

    cm = k8s_core_v1.read_namespaced_config_map(f"{helm_release.name}-config", helm_release.namespace)
    config_yaml = cm.data["config.yaml"]
    assert password not in config_yaml, "ClickHouse password leaked into the ConfigMap"
    assert "auth_password" not in config_yaml, "auth_password key should never be rendered into the ConfigMap"


@then("the pulsusdb pod's own logs never contain the ClickHouse password")
def _logs_have_no_password(helm_release: HelmRelease, k8s_core_v1):
    secret = k8s_core_v1.read_namespaced_secret(f"{helm_release.name}-clickhouse", helm_release.namespace)
    password = base64.b64decode(secret.data["password"]).decode("utf-8")

    logs = run(
        [
            "kubectl", "logs",
            "-n", helm_release.namespace,
            "-l", "app.kubernetes.io/component=all",
            "--all-containers",
            "--prefix",
        ],
        check=False,
    )
    combined = logs.stdout + logs.stderr
    assert password not in combined, (
        "the generated ClickHouse password appeared verbatim in the pulsusdb pod's own logs "
        "(pulsus-config's Secret redaction, or the process's own logging, has a leak)"
    )


@then("GET /config on the pulsusdb API redacts the ClickHouse password")
def _api_config_redacts_password(helm_release: HelmRelease, k8s_core_v1):
    secret = k8s_core_v1.read_namespaced_secret(f"{helm_release.name}-clickhouse", helm_release.namespace)
    password = base64.b64decode(secret.data["password"]).decode("utf-8")

    proc = common_steps.port_forward(helm_release.namespace, helm_release.name, 13101, 3100)
    try:
        resp = http_get("http://127.0.0.1:13101/config", timeout=10)
        assert resp.status_code == 200, f"/config returned {resp.status_code}: {resp.text}"
        assert password not in resp.text, "/config leaked the live ClickHouse password"
        assert "***" in resp.text, "/config is expected to redact the ClickHouse auth as `user:***`"
    finally:
        proc.terminate()
