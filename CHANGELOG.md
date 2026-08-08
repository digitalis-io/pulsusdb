# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
for the `pulsusdb` application. The Helm chart under `deploy/charts/pulsusdb/`
is versioned independently — see `deploy/charts/pulsusdb/Chart.yaml` and
`docs/releasing.md`.

## [Unreleased]

### Added

- `service_name` discovery at log ingest, on both receivers (issue #379).
  A stream pushed without a `service_name` label now gains one, as Loki
  3.7.4 gives it one: on `/loki/api/v1/push` the first of thirteen
  configured names present with a non-empty value, scanned in the
  reference's list order, and `unknown_service` when none is; on
  `/otlp/v1/logs` the reference's separate, wire-ordered rule over the
  resource attributes it indexes. The discovered value also populates the
  physical `service` column that `log_samples`' ordering key leads on,
  which was `''` for every such stream before. Stream identities therefore
  change for every stream stored without an explicit `service_name`, and
  `{service_name="…"}` becomes a usable selector for them. Ships with the
  reference's empty-label rejection (`400 error at least one label pair is
  required per stream`), which its OTLP discovery rule makes reachable.
  See `docs/api.md` §8.2 and
  `docs/benchmarks/logs-differential-ledger.md`.
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
- `CHANGELOG.md` (this file).
- Helm chart: a `pulsusdb.validateAuth` render-time guard rejects
  partial/ambiguous `pulsusdb.auth` combinations (one-sided `user`,
  one-sided password source, or `password`+`existingSecret` together);
  `image.digest` (preferred over `image.tag`) and validated optional
  `@sha256:` suffixes on `clickhouse.image`/`clickhouse.keeperImage`/
  `otelCollector.image` make every chart-rendered image digest-pinnable;
  `.github/workflows/helm-release.yml` and `.github/workflows/release.yml`
  now mechanically enforce, as their first post-checkout step, that a
  release tag is an ancestor of `origin/main`.
