# PulsusDB Features & Compatibility Matrix

This document enumerates every feature PulsusDB commits to and the milestone in which it lands. It is the source of truth for scoping GitHub issues — every row here should trace to one or more issues.

Two product decisions frame the matrix:

1. **The OpenTelemetry Collector is the ingestion path.** Logs, metrics, traces, and profiles arrive via OTLP (metrics alternatively via the collector's Prometheus remote-write exporter). OTLP endpoints are always on and land with each signal's milestone. Foreign push protocols are optional compatibility receivers, off by default.
2. **The PulsusDB API is product-neutral.** Primary query paths live under `/api/{logs,traces,profiles,rules}/v1/...` plus the standard Prometheus HTTP API for metrics. Third-party API surfaces (log/trace/profile datasource protocols) are flag-gated aliases (`PULSUS_COMPAT_ENDPOINTS`) onto the same handlers — see [api.md §8](api.md).

Milestones: **M0** scaffolding · **M1** logs proof · **M2** metrics proof · **M3** downsampling · **M4** traces · **M5** profiles · **M6** full language compliance + compat ingest breadth · **M7** native histograms + operations. See §7.

M1 and M2 are deliberately **narrow proof milestones**: the project exists because reads are slow, so the earliest code must validate the storage and planner design against captured real-world slow queries — full language parity comes in M6, after the read path has earned it with benchmark evidence.

---

## 1. Ingestion

### 1.1 Primary (always on)

| Protocol | Endpoint | Formats | Milestone |
|----------|----------|---------|-----------|
| OTLP logs | `POST /v1/logs` | protobuf | M1 |
| OTLP metrics | `POST /v1/metrics` | protobuf (exponential histograms flatten to classic `_bucket`/`_sum`/`_count` series until native-histogram storage lands in M7) | M2 |
| OTLP traces | `POST /v1/traces` | protobuf | M4 |
| OTLP profiles | `POST /v1development/profiles` | protobuf (experimental OTLP signal) | M5 |
| Prometheus remote write | `POST /api/v1/write` | snappy protobuf (`prompb.WriteRequest`) — supports the collector's `prometheusremotewrite` exporter | M2 |
| Profile ingest (native) | `POST /api/profiles/v1/ingest` | pprof | M5 |
| OTLP/JSON encoding | all OTLP endpoints | JSON per OTLP/HTTP spec | M6 |

### 1.2 Compatibility receivers (flag-gated, M6)

`POST /loki/api/v1/push` (JSON/protobuf) · Zipkin `POST /tempo/spans`, `/api/v2/spans` · `POST /ingest` (pprof alias, ships M5) · InfluxDB line protocol `POST /influx/api/v2/write` · Datadog `POST /api/v2/logs`, `/api/v2/series` · Elastic `POST /_bulk`, `/{target}/_bulk`, doc endpoints · remote-write path aliases.

### 1.3 Cross-cutting ingest features (M1 onward)

gzip/snappy/zstd request decompression · sync/async insert selection per request (`X-Pulsus-Async`) · per-request database routing (`X-Pulsus-Database`) · bounded buffering with `429` backpressure · per-protocol ingest metrics. Retention is per-table/database configuration; there is deliberately no per-write TTL override in v1.

## 2. Query — logs (LogQL)

| Feature | Endpoint | Milestone |
|---------|----------|-----------|
| Range queries | `GET /api/logs/v1/query_range` | M1 |
| Instant queries | `GET /api/logs/v1/query` | M1 |
| Labels / label values | `GET|POST /api/logs/v1/labels`, `/label/{name}/values` | M1 |
| Series | `GET|POST /api/logs/v1/series` | M1 |
| Live tail (WebSocket) | `GET /api/logs/v1/tail` — `limit`, `start`, `dropped_entries` | M6 |
| Index stats | `GET /api/logs/v1/stats` | M6 |
| Compat query aliases (`/loki/api/v1/{query_range,query,labels,label/*/values,series}`) | flag-gated | M1 (tail/stats M6, drilldown M7) |
| Log volume | `GET /api/logs/v1/volume` | M7 |
| Detected labels / fields | `GET /api/logs/v1/detected_labels`, `/detected_fields` | M7 |
| Log patterns | `GET /api/logs/v1/patterns` (ingest-time pattern extraction) | M7 |

**LogQL — M1 proof subset:** stream selectors with `=`, `!=`, `=~`, `!~`; line filters `|=`, `!=`, `|~`, `!~`; range aggregations `rate`, `count_over_time`, `bytes_rate`, `bytes_over_time`; vector aggregations `sum`, `avg`, `min`, `max`, `count` with `by`/`without`. This subset is exactly what's needed to prove the read path: index-served stream resolution, skip-index line filters, and rollup-served counts.

**LogQL — parity (M6):** parsers `json`, `logfmt`, `regexp`, `pattern`; label filters (string + numeric with duration/bytes units); `line_format`, `label_format`, `unwrap`; full over-time set (`sum/avg/min/max/stddev/stdvar/quantile/first/last/absent_over_time`); `stddev`, `stdvar`, `topk`, `bottomk` vector aggregations; binary operations between range vectors. Count-only range aggregations are served from the configurable-resolution rollup automatically when eligible (M1 onward). The stream (log) pipeline landed with M6-09; M6-10 delivered the metric language on top of it: pipelines and `unwrap` execute inside range aggregations (client-aggregated over a full-window raw scan — **complete or a named "query too broad" error, never a silently truncated aggregate**; three independent bounds enforced before any over-cap allocation — the evaluation-bucket grid (11,000 buckets), `quantile_over_time` sample retention (4,000,000 values), and the derived-series count of distinct output groups (500 series, oracle-parity default) — each rejects the whole query rather than truncating it; un-piped count/bytes aggregations keep their SQL-aggregated rollup auto-routing byte-identically), the full over-time set, `stddev`/`stdvar`/`topk`/`bottomk`, and binary operations (vector⊗scalar in both orientations, vector⊗vector and matrix⊗matrix with `bool`, `and`/`or`/`unless`, and — issue #91 — the full `on`/`ignoring`/`group_left`/`group_right` vector-matching modifiers, semantics oracle-verified against `grafana/loki:3.4.2` and applied per step for range vectors). One-to-one matching output carries the reduced (`on`/`ignoring`) signature; `group_left`/`group_right` pass the many side through whole and copy the include labels from the one side; a cardinality violation is the reference's exact `many-to-one matching must be explicit` / `many-to-many matching not allowed` error. Streams-path error series carry both `__error__` and its human-readable `__error_details__` companion (issue #99), byte-exact against `grafana/loki:3.4.2` for the representative `JSONParserErr`/`LogfmtParserErr`/`LabelFilterErr` classes and faithful-format for the value-interpolated long tail (ledgered in docs/benchmarks/logs-differential-ledger.md); the metric pipeline-error path is unchanged. A metric line that retains a nonempty `__error__` after its pipeline (e.g. a failed `unwrap` conversion with no `__error__` filter) fails the query with the reference store's exact `pipeline error: ...` surface, oracle-verified. `first_over_time`/`last_over_time` ties at an identical timestamp resolve by a pinned, input-order-independent PulsusDB rule — the smallest (first) / largest (last) value at the boundary timestamp; the reference's tie order for identical timestamps is unspecified, so PulsusDB pins determinism rather than mirroring nondeterminism (the client-aggregation scan also carries a stable `timestamp, fingerprint, body` total order so float accumulation is bit-reproducible). Line filters placed before any parser/`line_format` keep their skip-index pushdown byte-identically — parsers run only on the index-reduced row set. Range-query metric bucketing remains the documented tumbling contract (ledgered against the oracle's sliding windows — docs/benchmarks/logs-differential-ledger.md); instant metric queries are semantically identical to the reference.

**LogQL pipeline exact fetch-until-limit (M6-09, #90):** pipelines containing an in-engine dropping stage that cannot push down (a label filter, or a line filter after `line_format`) are served by keyset-cursor paging: the engine pages `reader.logql_pipeline_scan_factor × limit` rows at a time (default factor 10) through the pipeline until the true `limit` fills, the query window is exhausted, or the byte scan budget is spent. Responses fill exactly to `limit` (no under-return) and never over-return; the fast/non-dropping paths keep a single byte-identical `LIMIT` scan. The byte scan budget (`reader.logql_scan_budget_bytes`) is the hard cumulative scan ceiling and aborts first — a budget-truncated result returns the survivors so far and signals incompleteness via `data.stats.pulsus_partial` (configuration.md §6, api.md §2.1/§2.2).

**Read-path differentiators:** token/ngram skip indexes accelerate line filters (no full body scans within the selected streams); stream-selector-bounded scans otherwise; per-query scan budget with explicit "query too broad" errors instead of OOM.

## 3. Query — metrics (PromQL, Prometheus HTTP API)

| Feature | Endpoint | Milestone |
|---------|----------|-----------|
| Instant query | `GET|POST /api/v1/query` | M2 |
| Range query | `GET|POST /api/v1/query_range` (11k-point cap) | M2 |
| Labels / values | `GET|POST /api/v1/labels`, `GET /api/v1/label/{name}/values` | M2 |
| Series | `GET|POST /api/v1/series` | M2 |
| Metadata | `GET /api/v1/metadata` | M2 |
| Exemplars | `GET|POST /api/v1/query_exemplars` (empty-success stub) | M2 |
| Status endpoints | `/api/v1/status/buildinfo`, `/config`, `/flags`, `/runtimeinfo`, `/tsdb` | M2 |
| Downsampling tiers + query routing | automatic; configurable tiers | M3 |

**PromQL — M2 proof subset, evaluated with Prometheus-exact semantics** (counter resets, extrapolation, staleness markers, lookback delta, Kahan summation — validated by differential testing against Prometheus on exactly this subset): selectors with all matcher types; `offset`; `rate`, `irate`, `increase`, `delta`; `avg/min/max/sum/count_over_time`; aggregations `sum`, `avg`, `min`, `max`, `count`, `topk`, `bottomk` with `by`/`without`; vector–scalar and vector–vector arithmetic/comparison with `bool`, `on()`, `ignoring()`; `histogram_quantile`. Enough to power a typical dashboard and prove the hybrid engine.

**PromQL — parity (M6):**

- *Rate family:* `idelta`, `deriv`, `predict_linear`, `resets`, `changes`
- *Over-time:* `stddev/stdvar/last/present/quantile/mad_over_time`, `double_exponential_smoothing`
- *Aggregations:* `group`, `stddev`, `stdvar`, `quantile`, `count_values`, `limitk`, `limit_ratio`
- *Math & trig:* `abs`, `ceil`, `floor`, `round`, `sqrt`, `exp`, `ln`, `log2`, `log10`, `sgn`, `clamp{,_min,_max}`, full trig set, `deg`, `rad`, `pi`
- *Label & sort:* `label_replace`, `label_join`, `sort{,_desc}`, `sort_by_label{,_desc}`
- *Other:* `absent`, `absent_over_time`, `scalar`, `vector`, `time`, `timestamp`; `group_left()`/`group_right()` matching; set operators
- *Documented deviation (M6-03):* the date/time-field functions (`year`, `month`, `day_of_month`, `day_of_week`, `day_of_year`, `days_in_month`, `hour`, `minute`) map `NaN`/`±Inf`/out-of-`int64`-range input values to `NaN` — Go's `int64(float64)` conversion is platform-defined on exactly those inputs, so PulsusDB pins a total, documented behavior instead; finite in-range inputs truncate toward zero identically to Go
- *Modifiers:* `@` timestamp lands with M3
- *Subqueries* `metric[1h:5m]`, duration expressions, UTF-8 label names, and flag-gated experimental functions — all part of M6 full compliance
- *Native histograms:* **M7** (committed, not aspirational) — histogram sample storage, OTLP exponential-histogram native ingestion, the histogram function set, and the upstream `native_histograms.test` corpus file passing

**Full compliance is the M6 bar, defined mechanically:** every function in the pinned Prometheus release's registry (89 at v3.13 — the function-only count: the 14 aggregation operators and the tracked language features are separate, separately-counted dimensions), the full operator/modifier matrix, and a 100% pass on the pinned upstream PromQL test corpus (~11.7k lines across 21 scenario files, fetched at test time and checksum-verified against a committed manifest) replayed against PulsusDB in CI. The machine-checked coverage authority is `crates/pulsus-promql/tests/promqltest/coverage/function-coverage.json` (M6-01): every registry function, aggregation operator, and tracked language feature carries a CI-verified status there, and the drift test fails when the evaluator's real surface disagrees with it in either direction. PulsusDB never relies on ClickHouse's own (small-subset) PromQL support — evaluation is entirely in the engine, so language coverage is never constrained by what ClickHouse can express ([architecture.md §5.1](architecture.md)).

## 4. Query — traces (TraceQL)

| Feature | Endpoint | Milestone |
|---------|----------|-----------|
| Trace by ID | `GET /api/traces/v1/trace/{traceId}` (+ `/json`) | M4 |
| Search | `GET /api/traces/v1/search` (TraceQL `q` + legacy tag params) | M4 |
| Tag names / values | `GET /api/traces/v1/tags`, `/tag/{tag}/values` | M4 |
| TraceQL metrics — range | `GET /api/traces/v1/metrics/query_range` (`q`, `start`/`end`/`since`, `step`) | M4 |
| TraceQL metrics — instant | `GET /api/traces/v1/metrics/query` | M4 |
| Compat query aliases (trace-datasource surface) | flag-gated | M4 |
| Service graph data | span-metrics MVs feeding service-graph panels | M7 |

**TraceQL coverage (M4):** span attribute selectors (`span.`, `resource.`, and the unscoped `.attr` form), intrinsics `name`, `duration`, `status`, `kind` (`service` is not an intrinsic — it is the `resource.service.name` attribute), comparison operators incl. regex, `&&`/`||` within and across spansets (`{...} && {...}`), aggregate filters (`count()`, `avg(duration)` etc.), `select()`. Structural operators (`>`, `>>`, sibling) are M7.

## 5. Query — profiles

| Feature | Endpoint | Milestone |
|---------|----------|-----------|
| Profile types, labels, label values, series | `GET /api/profiles/v1/{types,labels,label/{name}/values,series}` | M5 |
| Merged flamegraph | `GET /api/profiles/v1/merge` | M5 |
| Time series of profile values | `GET /api/profiles/v1/select_series` | M5 |
| Merged pprof export | `GET /api/profiles/v1/export` | M5 |
| Stats | `GET /api/profiles/v1/stats` | M5 |
| Render (flamebearer) | `GET /api/profiles/v1/render` | M5 |
| Diff render | `GET /api/profiles/v1/render-diff` | M5 |
| DOT export | `render?format=dot` — `maxNodes`, unit-aware labels, scaled fonts | M5 |
| Compat query aliases (Connect-protocol querier surface + render paths) | flag-gated | M5 |

## 6. Platform features

| Feature | Notes | Milestone |
|---------|-------|-----------|
| Single-binary modes | `all` / `writer` / `reader` / `init` | M0 |
| Schema controller | idempotent DDL, append-only migrations, TTL rotation, MV checksum lifecycle | M0 |
| Health & introspection | `/ready`, `/metrics`, `/config`, `/buildinfo` | M0 |
| Basic auth | `PULSUS_AUTH_USER` / `PULSUS_AUTH_PASSWORD` | M0 |
| Compatibility endpoint flag | `PULSUS_COMPAT_ENDPOINTS` (default off) | M1 |
| Inbound TLS | native TLS termination on the listener | M7 |
| Label cache | bounded active-series window, JOIN fallback | M2 |
| Downsampling | insert-triggered in-database MVs (real-time tiers), one-shot backfill, checksum-gated MV recreation, `PULSUS_TIER_POLICY` exact/fast routing | M3 |
| Clustered ClickHouse | Replicated engines, co-sharded Distributed tables, shard-local pushdown | M3 (metrics), M4 (logs/traces validation) |
| Cross-cluster reads | distributed-suffix read targeting | M7 |
| Recording rules (ruler) | LogQL + PromQL kinds, CRUD API, write-back | M7 |
| Alerting rules | stored/validated from M7 API; evaluation + notification delivery | post-1.0 |
| Query explain | `X-Pulsus-Explain` returns planner SQL | M2 |

**Explicit non-goals for 1.0:** multi-tenant org isolation, built-in UI, DuckDB or object-store backends other than via ClickHouse storage policies, Prometheus remote *read*.

## 7. Milestones

Every signal milestone gates on an end-to-end pipeline: **OTel Collector → PulsusDB → query API**, exercised in CI with a real collector instance.

| Milestone | Deliverable | Definition of done |
|-----------|-------------|--------------------|
| **M0 — Foundation** | workspace, config loader, ClickHouse client + pool (benchmarked choice), schema controller with logs+metrics DDL, health endpoints, CI, compose-based dev env (podman compose or docker compose, incl. OTel Collector) | `pulsusdb --mode init` creates a correct schema on a fresh ClickHouse; e2e harness skeleton green |
| **M1 — Logs proof** | OTLP logs ingest, LogQL proof subset, `/api/logs/v1/{query_range,query,labels,series}`, one flag-gated compat query path | collector logs pipeline queryable end-to-end; `EXPLAIN indexes = 1` shows primary-index + skip-index use; benchmarked against captured real-world slow queries with `system.query_log` evidence (read_rows, read_bytes, marks); multi-shard behavior measured, not assumed |
| **M2 — Metrics proof** | OTLP metrics + remote write, time-aware label cache, PromQL proof subset, Prometheus API surface | differential test vs Prometheus: 100% value match on the M2 subset over 10k series fed through the collector; label-resolution correctness tests incl. historical windows; **three-path label-resolution benchmark started on the 5M-series scale corpus** (cache matcher + refresh cost / SQL fallback across metric cardinalities / prototype inverted index) |
| **M3 — Downsampling** | tier tables, insert-triggered MVs, one-shot backfill, tier router + `PULSUS_TIER_POLICY`, `@` modifier | tier accuracy suite green (misaligned windows, single-reset counters, duplicate/late data) with documented error bounds; `exact` policy bit-identical to Prometheus on raw segments; insert-throughput impact of tier MVs measured; **label-resolution decision gate closed with benchmark evidence** — ship `metric_series_idx` and/or incremental cache refresh only if the data demands it |
| **M4 — Traces** | OTLP traces ingest, trace-by-ID, TraceQL search + metrics, tags APIs, compat aliases | collector traces pipeline lands and is searchable; search served via projection + attr index (verified with EXPLAIN); distributed fan-out claims validated on a multi-shard cluster |
| **M5 — Profiles** | OTLP profiles + native pprof ingest, `/api/profiles/v1/*`, render/render-diff/DOT, compat aliases | collector profiles pipeline (or pprof fallback) lands and renders; storage amplification measured |
| **M6 — Full language compliance + compat ingest breadth** | **full PromQL compliance** (all 89 registry functions — the function-only count, per the pinned v3.13 `functions.go`; the 14 aggregation operators and the tracked language features are separate dimensions, all inventoried in the machine-checked coverage manifest `crates/pulsus-promql/tests/promqltest/coverage/function-coverage.json` — plus subqueries, duration expressions, experimental flag-gated functions), full LogQL parity (§2 parity list), live tail, index stats, remaining compat query aliases; flag-gated receivers: log push, Zipkin, Influx, Datadog, Elastic; OTLP/JSON | **100% pass on the pinned upstream PromQL test corpus (Prometheus 3.13, 21 files / ~11.7k lines, fetched at test time and checksum-verified; native-histogram file excluded until M7)**; LogQL parity differential tests green; fixture-driven e2e per protocol |
| **M7 — Native histograms + operations** | native histograms end-to-end (histogram sample storage, OTLP exponential-histogram native ingestion, histogram function set), ruler (recording), drilldown/patterns/volume, structural TraceQL, service graphs, inbound TLS, cross-cluster reads | upstream `native_histograms.test` passing — completing 100% of the corpus; production-readiness review checklist |
