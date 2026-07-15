# PulsusDB

**PulsusDB** is a lightweight, all-in-one observability database for **logs, metrics, traces, and profiles**, written in **Rust** and backed by **ClickHouse**.

Ship everything through the **OpenTelemetry Collector**, query with the languages you already know — no query languages to relearn, no lock-in.

* **OpenTelemetry-native** — the OTel Collector is the ingestion path: OTLP for logs, metrics, traces, and profiles (plus Prometheus remote write); additional push protocols available as optional compatibility receivers
* **Familiar** — query with **LogQL**, **PromQL**, and **TraceQL** through PulsusDB's own product-neutral HTTP API; the standard Prometheus API is served natively, with **full PromQL compliance** (validated against the upstream Prometheus test corpus) as a hard requirement
* **Compatible when you want it** — a config flag mounts third-party-compatible API endpoints so existing datasources and dashboards keep working unmodified
* **Fast where it matters** — the storage schema and query planner are designed **read-path first**, for the dashboard and search workloads that observability systems actually serve
* **Simple to run** — a single static binary that scales from a laptop to a sharded ClickHouse cluster
* **Open** — AGPLv3, community-driven, no vendor lock-in

> **Status: design phase.** PulsusDB is under active development. The architecture, feature set, and API surface are being specified in [`docs/`](docs/) before implementation begins. Nothing here is production-ready yet.

---

## Why PulsusDB?

Compatibility layers over ClickHouse are a proven idea, but most implementations optimize for flexible ingestion and pay for it at query time: generic one-size-fits-all sample tables, label lookups that fan out to every shard, log searches that brute-force scan message bodies, and translated queries that read far more data than they return.

PulsusDB starts from the opposite end — the queries — and works backwards to the schema:

* **Purpose-built tables per signal.** Logs, metrics, traces, and profiles have different query shapes, so they get different schemas, ordering keys, and rollups — not one generic samples table.
* **Shard-aware label indexing.** Series lookups are laid out so that label resolution can prune shards instead of broadcasting to all of them, and intermediate fingerprint sets are bounded by the planner.
* **Indexed log search.** Message bodies carry token/n-gram skip indexes so `|= "connection refused"` doesn't mean scanning a week of raw log lines.
* **Trace search that matches how people search.** Span data is ordered and indexed for service + time + attribute queries, not just exact trace-ID fetches.
* **A query planner that respects ClickHouse.** Time filters pushed into `PREWHERE`, automatic rollup selection for wide time ranges, partial aggregation on shards, and no redundant index scans.
* **Rust end to end.** Predictable memory use under ingest bursts and heavy dashboard fan-out, with no GC pauses in the hot path.

## Architecture at a glance

PulsusDB compiles to a single binary whose role is selected at runtime:

| Mode | Role |
|------|------|
| `all` (default) | Ingestion + query + rule evaluation in one process |
| `writer` | Ingestion only — parses all push protocols and batches columnar inserts into ClickHouse |
| `reader` | Query only — serves the PulsusDB query APIs (logs, metrics, traces, profiles) |

Internally it is organized as:

* **writer** — protocol parsers (OTLP logs/metrics/traces/profiles, remote write, pprof, plus flag-gated compatibility receivers), stream fingerprinting, size/age-based batch inserts
* **reader** — LogQL / PromQL / TraceQL front ends, a ClickHouse-native SQL planner, and the HTTP query APIs (including WebSocket live tail)
* **ruler** — recording-rule evaluation with write-back through the ingestion path
* **ctrl** — schema lifecycle: creation, migration, retention/TTL rotation, distributed and replicated table management

All state lives in ClickHouse; PulsusDB processes are stateless and scale horizontally by mode.

## Documentation

Design documents live in [`docs/`](docs/) and are written before the code:

| Document | Contents |
|----------|----------|
| `docs/architecture.md` | Component design, storage schemas, query planning, clustering |
| `docs/schemas.md` | Authoritative ClickHouse DDL: per-table rationale, generated-SQL read paths, distributed layout, latency targets |
| `docs/features.md` | Full feature list and compatibility matrix |
| `docs/api.md` | Every ingestion and query endpoint, with parameters and wire formats |
| `docs/configuration.md` | Environment variables, deployment modes, ClickHouse setup |
| `docs/releasing.md` | Cutting a release: GHCR image publishing procedure and tag policy |

Development is tracked through GitHub issues; each issue maps to a scoped unit of work from these documents.

## Getting started

PulsusDB is not yet runnable — the first milestone is a working ingest + query path for logs. Watch the repository or the issue tracker to follow progress.

## Deploying with Helm

A Helm chart is available under [`deploy/charts/pulsusdb`](deploy/charts/pulsusdb), published as an OCI artifact:

```sh
helm install my-pulsusdb oci://ghcr.io/digitalis-io/charts/pulsusdb --version <chart-version>
```

The default install is a self-contained single-node stack — bundled ClickHouse, one all-mode `pulsusdb` Deployment, and an OTel Collector — the Kubernetes equivalent of the docker-compose quickstart in [docs/configuration.md §10](docs/configuration.md). See the [chart README](deploy/charts/pulsusdb/README.md) for the full values reference, the two orthogonal topology axes (bundled-ClickHouse sharding vs. writer/reader process split), and the probe contract.

**The `pulsusdb` application image is not published to a registry yet** (tracked separately) — real installs need `--set image.tag=<a locally built or future released tag>` until that lands.

## Contributing

Contributions are welcome once the initial design documents land. Until then, feedback on the architecture and API documents via issues is the most useful way to help.

## License

Released under the [GNU Affero General Public License v3.0](LICENSE).

---

*Grafana®, Loki™, and Tempo® are trademarks of Raintank/Grafana Labs. Prometheus is a trademark of The Linux Foundation. ClickHouse® is a trademark of ClickHouse, Inc. PulsusDB is an independent project, not affiliated with or endorsed by any of them.*
