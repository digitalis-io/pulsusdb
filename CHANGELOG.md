# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
for the `pulsusdb` application. The Helm chart under `deploy/charts/pulsusdb/`
is versioned independently — see `deploy/charts/pulsusdb/Chart.yaml` and
`docs/releasing.md`.

## [Unreleased]

### Added

- Helm chart (`deploy/charts/pulsusdb/`) for deploying PulsusDB to
  Kubernetes: single all-mode or split writer/reader topologies, an
  optional bundled single-node or sharded ClickHouse (with a Keeper
  StatefulSet for the sharded case), an OpenTelemetry Collector for OTLP +
  Prometheus remote-write ingestion, and an optional Grafana Loki-compat
  datasource ConfigMap. Config is rendered 1:1 from `docs/configuration.md`
  §9's YAML schema into a ConfigMap; credentials are Secret-managed and
  never appear in the ConfigMap. See `deploy/charts/pulsusdb/README.md`
  for the full values reference and topology/probe-contract documentation.
- `.github/workflows/helm-chart.yml`: per-PR `chart-lint` (helm lint
  --strict, `helm template` + kubeconform schema validation for both
  topologies, golden-snapshot drift check), `chart-unittest`
  (helm-unittest render/schema specs), and `chart-test-kind` (a pytest-bdd
  behavioural suite against a real Kind cluster — install/upgrade/
  uninstall lifecycle, split mode, sharded ClickHouse, a prolonged
  ClickHouse-outage resilience scenario, and an OCI package/push/pull
  round trip).
- `.github/workflows/helm-release.yml`: publishes the chart as an OCI
  artifact to `oci://ghcr.io/digitalis-io/charts/pulsusdb` on `helm-v*`
  tags, gated on an already-exists preflight guard and a digest-verified
  `helm pull` round trip.
- Local-only Grafana demo stack (`make grafana-up`/`grafana-down`/
  `grafana-logs`): a `deploy/e2e/compose.grafana.yaml` overlay on the
  existing single-node e2e fixture adding Tempo-compat and native
  Prometheus Grafana datasources against pulsusdb (alongside the existing
  Loki-compat one), a vendored copy of the `firehose` synthetic
  logs/metrics/traces generator (`deploy/e2e/firehose/`, real-time
  `SPEEDUP=1` against a laptop-sized 5-host fleet), a dedicated
  otel-collector config (`deploy/e2e/otel-config.grafana.yaml`, kept
  separate from the CI-shared `otel-config.single.yaml`), and a
  provisioned dashboard (`deploy/e2e/grafana/provisioning/dashboards/`)
  covering log rate/stream, host/region metrics, and a Tempo traces
  panel. Not wired into CI or the `pulsus-e2e` harness.
- `deploy/e2e/compose.tier.yaml`, part of the same `make grafana-up`
  stack: a three-tier (frontend/middletier/backend) HTTP checkout
  service vendored from `terraform-google-monitoring`'s
  `traffic-gen/loaders/otel-tier` (`deploy/e2e/otel-tier/`), each hop
  propagating W3C `traceparent` so a single trace spans all three real
  HTTP calls, with realistic nested spans, injected slow "db-query"
  spans (~20%), and hard backend failures (~14%) — richer, multi-hop
  traces than firehose's single-service ones, for Tempo-view
  exploration. A `curlimages/curl` loop drives traffic (otel-tier ships
  no load generator of its own).
- Helm chart: a `pulsusdb.validateAuth` render-time guard rejects
  partial/ambiguous `pulsusdb.auth` combinations (one-sided `user`,
  one-sided password source, or `password`+`existingSecret` together);
  `image.digest` (preferred over `image.tag`) and validated optional
  `@sha256:` suffixes on `clickhouse.image`/`clickhouse.keeperImage`/
  `otelCollector.image` make every chart-rendered image digest-pinnable;
  `.github/workflows/helm-release.yml` and `.github/workflows/release.yml`
  now mechanically enforce, as their first post-checkout step, that a
  release tag is an ancestor of `origin/main`.
