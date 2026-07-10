# PulsusDB API Reference

PulsusDB exposes two API surfaces:

1. **The PulsusDB API** — the primary, always-on surface. Product-neutral paths under `/api/{logs,traces,profiles,rules}/v1/...`, the standard Prometheus HTTP API for metrics, and standard OTLP paths for ingestion. This is the API PulsusDB documents, versions, and guarantees.
2. **Compatibility endpoints** — optional aliases and foreign-protocol receivers matching third-party API surfaces (log/trace/profile datasources, legacy push protocols). Disabled by default; enabled with `PULSUS_COMPAT_ENDPOINTS=true`. They map onto the same handlers and add no new semantics.

**Ingestion model:** the OpenTelemetry Collector is the expected shipper for all signals — logs, metrics, traces, and profiles arrive via OTLP (metrics alternatively via the collector's Prometheus remote-write exporter). Foreign push protocols exist only behind the compatibility flag.

Conventions:

- Default listener: `0.0.0.0:3100`. All endpoints relative to that root.
- Timestamps: log APIs use nanoseconds; metrics APIs use RFC3339 or unix seconds; trace APIs accept unix seconds/nanoseconds/RFC3339.
- Errors: `{"status":"error","errorType":...,"error":...}` envelopes; `429` on ingest backpressure; `400` for malformed queries with parser position where available.
- Compression: requests may be `gzip`, `snappy`, or `zstd` (`Content-Encoding`); responses gzip when accepted.

## Request headers (all optional)

| Header | Applies to | Effect |
|--------|-----------|--------|
| `X-Pulsus-Database` | ingest + query | route to an alternate ClickHouse database (retention is per-database configuration; there is no per-write TTL override in v1) |
| `X-Pulsus-Async` | ingest | `1` = enqueue and return `202`; `0` = confirm flush (default from config) |
| `X-Pulsus-Explain` | query | `1` = include generated SQL, plan, and per-segment exactness (raw-exact vs tier-approximate) in the response envelope |
| `Authorization` | all | Basic auth when `PULSUS_AUTH_USER` is set |

---

## 1. Ingestion

### 1.1 OTLP (primary)

Standard OTLP/HTTP paths, always enabled:

```
POST /v1/logs                    ExportLogsServiceRequest
POST /v1/metrics                 ExportMetricsServiceRequest
POST /v1/traces                  ExportTraceServiceRequest
POST /v1development/profiles     ExportProfilesServiceRequest (OTLP profiles, experimental signal)
Content-Type: application/x-protobuf   (OTLP/JSON accepted from M6)
```

- Resource + scope attributes flatten into labels under the canonical label model ([architecture.md §2.3](architecture.md)): for logs and metrics, attribute keys are normalized to Prometheus-style names at ingest (`service.name` → `service_name`); trace attributes keep their OTel names verbatim and are queried as such in TraceQL. Log body → line; spans → trace tables with original protobuf retained as payload; metric data points → metric samples with `__name__` from the metric name; profiles → pprof-equivalent tree precomputation.
- Responses: `200` with OTLP partial-success message when applicable; `429` on backpressure.
- The `/v1development/profiles` path tracks the OTLP spec's experimental profiles signal and will follow it to `/v1/profiles` on stabilization (the old path remains as an alias).

### 1.2 Prometheus remote write

```
POST /api/v1/write
Content-Type: application/x-protobuf, Content-Encoding: snappy
```

`prompb.WriteRequest`. Supported as a first-class alternative for metrics because the OTel Collector's `prometheusremotewrite` exporter is a common metrics pipeline. `__name__` becomes `metric_name`; remaining labels are fingerprinted (xxhash64, sorted `k\xffv\xff` serialization). Stale markers (NaN `0x7FF0000000000002`) stored verbatim. Success: `204`.

### 1.3 Profile ingest (native)

```
POST /api/profiles/v1/ingest?name=<app>{tags}&from=<ts>&until=<ts>&sampleRate=<hz>&format=<fmt>
Content-Type: multipart/form-data | binary pprof
```

Direct pprof ingestion for SDKs/agents that don't route through the collector. Flamegraph tree + function table precomputed at ingest. Success: `200`.

---

## 2. Logs query API

### 2.1 `GET /api/logs/v1/query_range`

| Param | Type | Notes |
|-------|------|-------|
| `query` | LogQL | required |
| `start`, `end` | ns / RFC3339 | default: last hour |
| `step` | duration | metric queries only |
| `limit` | int | max entries per stream direction (default 100) |
| `direction` | `forward`\|`backward` | default `backward` |

Response: `{"status":"success","data":{"resultType":"streams"|"matrix","result":[...],"stats":{...}}}` — log selector queries return `streams` (values as `[<ts_ns>, <line>]`), metric queries return `matrix`.

### 2.2 `GET /api/logs/v1/query`

Instant evaluation at `time` (ns). Returns `vector` or `streams`.

### 2.3 Labels & series

```
GET|POST /api/logs/v1/labels                 ?start=&end=
GET      /api/logs/v1/label/{name}/values    ?start=&end=&query=
GET|POST /api/logs/v1/series                 ?match[]=<selector>&start=&end=
```

Responses: `{"status":"success","data":[...]}`; series returns an array of label maps.

### 2.4 `GET /api/logs/v1/tail` (WebSocket)

| Param | Notes |
|-------|-------|
| `query` | LogQL stream selector + optional line filters |
| `limit` | cap on entries per frame |
| `start` | starting timestamp (ns), default now − 1h |
| `delay_for` | seconds to delay to tolerate late arrivals |

Frames: `{"streams":[...],"dropped_entries":[{"labels":...,"timestamp":...}]}`. Slow consumers get entries dropped and reported, never unbounded buffering.

### 2.5 `GET /api/logs/v1/stats`

`?query={selector}&start=<ns>&end=<ns>` → `{"streams":N,"chunks":N,"entries":N,"bytes":N}` (chunks reported as partition-part counts).

### 2.6 Drilldown (M7)

```
GET /api/logs/v1/volume             ?query=&start=&end=&limit=&aggregateBy=
GET /api/logs/v1/detected_labels    ?query=&start=&end=
GET /api/logs/v1/detected_fields    ?query=&start=&end=
GET /api/logs/v1/patterns           ?query=&start=&end=
```

---

## 3. Metrics query API (Prometheus HTTP API)

The standard Prometheus API is PulsusDB's native metrics API — its paths are product-neutral and it is what every metrics client speaks. The query language target is **full PromQL compliance** against a pinned upstream Prometheus release (v3.13): all registry functions (experimental ones behind the same feature gate as upstream), subqueries, `@`, duration expressions — verified by replaying the upstream PromQL test corpus in CI ([architecture.md §5.1](architecture.md)).

### 3.1 `GET|POST /api/v1/query`

| Param | Notes |
|-------|-------|
| `query` | PromQL, required |
| `time` | evaluation time (RFC3339 or unix); default now |
| `timeout` | overrides server default up to the hard cap |

Response: `{"status":"success","data":{"resultType":"vector"|"scalar"|"matrix","result":[...]}}`. Values formatted as Prometheus does (shortest round-trip float; `NaN`, `+Inf`, `-Inf` as strings).

### 3.2 `GET|POST /api/v1/query_range`

`query`, `start`, `end`, `step` (required). Hard cap 11,000 points per series. Long ranges are transparently served from downsampling tiers (M3); the segmentation is visible via `X-Pulsus-Explain`.

### 3.3 Metadata & discovery

```
GET|POST /api/v1/labels                    ?match[]=&start=&end=
GET      /api/v1/label/{name}/values       ?match[]=&start=&end=
GET|POST /api/v1/series                    ?match[]=&start=&end=   (match[] required)
GET      /api/v1/metadata                  ?metric=&limit=
GET|POST /api/v1/query_exemplars           (empty-success stub in v1)
```

`__name__` is always present in labels responses. Metadata is sourced from `metric_metadata` (populated from remote-write metadata and OTLP).

### 3.4 Status

```
GET /api/v1/status/buildinfo     → version, revision, build metadata
GET /api/v1/status/config        → effective config (redacted), Prometheus envelope
GET /api/v1/status/flags         → static-equivalent flag map
GET /api/v1/status/runtimeinfo   → process start time, storage retention
GET /api/v1/status/tsdb          → numSeries, numSamples, top metrics/labels by cardinality
```

---

## 4. Traces query API

### 4.1 Trace fetch

```
GET /api/traces/v1/trace/{traceId}         → OTLP-shaped trace (protobuf or JSON by Accept)
GET /api/traces/v1/trace/{traceId}/json    → force JSON
```

`traceId` is hex (16 or 32 chars, left-padded). `404` with an error envelope when absent.

### 4.2 `GET /api/traces/v1/search`

| Param | Notes |
|-------|-------|
| `q` | TraceQL query (preferred) |
| `tags`, `minDuration`, `maxDuration` | legacy search params, compiled to TraceQL internally |
| `start`, `end` | unix seconds |
| `limit`, `spss` | result and spans-per-spanset caps |

Response: `{"traces":[{"traceID","rootServiceName","rootTraceName","startTimeUnixNano","durationMs","spanSets":[...]}],"metrics":{...}}`.

### 4.3 Tags

```
GET /api/traces/v1/tags                   ?scope=&start=&end=      (scoped response shape)
GET /api/traces/v1/tag/{tag}/values       ?q=&start=&end=          (typed values)
```

Served from `trace_tag_catalog` (bounded, deduplicated) — never by scanning span payloads.

### 4.4 TraceQL metrics

```
GET /api/traces/v1/metrics/query_range
GET /api/traces/v1/metrics/query
```

| Param | Notes |
|-------|-------|
| `q` / `query` | TraceQL metrics expression (e.g. `{span.http.status_code=200} \| rate()`) |
| `start`, `end` | unix s / ns / RFC3339 |
| `since` | relative alternative to start/end (`1h`, `30m`) |
| `step` | resolution; auto-derived when omitted |

Aggregation is executed in ClickHouse (`GROUP BY toStartOfInterval`); responses use the Prometheus matrix/vector shape.

---

## 5. Profiles query API

```
GET      /api/profiles/v1/types                            → available profile types
GET|POST /api/profiles/v1/labels          ?query=&from=&until=
GET      /api/profiles/v1/label/{name}/values
GET|POST /api/profiles/v1/series          ?match[]=&from=&until=
GET      /api/profiles/v1/merge           ?query=<type>{selector}&from=&until=   → merged flamegraph tree (JSON)
GET      /api/profiles/v1/select_series   ?query=&from=&until=&step=             → time series of profile values
GET      /api/profiles/v1/export          ?query=&from=&until=                   → merged pprof (binary)
GET      /api/profiles/v1/stats                                                  → ingested-profile stats
```

Render endpoints:

```
GET /api/profiles/v1/render
    ?query=<type>{selector}&from=&until=&format=json|dot&maxNodes=<n>
GET /api/profiles/v1/render-diff
    ?leftQuery=&leftFrom=&leftUntil=&rightQuery=&rightFrom=&rightUntil=
```

- `format=json` → flamebearer envelope (`names`, `levels`, `numTicks`, `maxSelf`, plus `metadata` and a timeline).
- `format=dot` → Graphviz digraph; `maxNodes` limits nodes (0 = unlimited); values human-formatted per unit (`1.23s`, `1.23 MB`); node font size scales 8–24pt with self-sample share.

---

## 6. Rules API (ruler, M7)

YAML request/response bodies (standard rule-group format). `kind` is `logs` (LogQL rules) or `metrics` (PromQL rules):

```
GET    /api/rules/v1/{kind}                          → all namespaces
GET    /api/rules/v1/{kind}/{namespace}
GET    /api/rules/v1/{kind}/{namespace}/{group}
POST   /api/rules/v1/{kind}/{namespace}              (upsert group)
DELETE /api/rules/v1/{kind}/{namespace}/{group}
DELETE /api/rules/v1/{kind}/{namespace}

GET    /api/v1/rules                                 → Prometheus-JSON view of metric rule groups
```

Recording rules are evaluated on the poll interval; alerting rules are accepted and stored (validation errors → `400`) with evaluation arriving post-1.0. When the ruler is disabled all rule endpoints return `404`.

---

## 7. Operational endpoints

```
GET /ready        → 200 when ClickHouse reachable (+ label cache warm in reader mode); 503 otherwise
GET /metrics      → Prometheus exposition of PulsusDB internals
GET /config       → effective configuration, secrets redacted
GET /buildinfo    → {"version","revision","builtAt","rustc"}
```

---

## 8. Compatibility endpoints (optional, `PULSUS_COMPAT_ENDPOINTS=true`)

Disabled by default. When enabled, PulsusDB additionally mounts third-party API surfaces so existing datasources, agents, and dashboards work unmodified. These are aliases onto the native handlers (or foreign-format parsers feeding the same pipeline); they carry no additional semantics and are not part of the versioned PulsusDB API.

### 8.1 Query aliases

| Compatibility path | Native equivalent | Ships with |
|--------------------|-------------------|------------|
| `/loki/api/v1/query_range`, `/query`, `/labels`, `/label/{name}/values`, `/series` | `/api/logs/v1/{query_range,query,labels,label/*/values,series}` | M1 |
| `/loki/api/v1/tail`, `/loki/api/v1/index/stats` | `/api/logs/v1/{tail,stats}` | M6 |
| `/loki/api/v1/index/volume`, `/detected_labels`, `/detected_fields`, `/patterns` | `/api/logs/v1/{volume,detected_labels,detected_fields,patterns}` | M7 |
| `/api/traces/{traceId}[/json]`, `/tempo/api/traces/{traceId}` | `/api/traces/v1/trace/{traceId}` | M4 |
| `/api/search`, `/api/search/tags`, `/api/search/tag/{tag}/values`, `/api/v2/search/*`, `/api/echo` | `/api/traces/v1/search`, `/api/traces/v1/tags`, `/api/traces/v1/tag/*` | M4 |
| `/api/metrics/query_range`, `/api/metrics/query` (+ `/tempo/` prefixes) | `/api/traces/v1/metrics/*` | M4 |
| `POST /querier.v1.QuerierService/{ProfileTypes,LabelNames,LabelValues,Series,SelectMergeStacktraces,SelectSeries,SelectMergeProfile,GetProfileStats,AnalyzeQuery}`, `POST /settings.v1.SettingsService/Get` (Connect-protocol, protobuf) | `/api/profiles/v1/*` | M5 |
| `/pyroscope/render`, `/pyroscope/render-diff` | `/api/profiles/v1/render{,-diff}` | M5 |
| `/loki/api/v1/rules[...]`, `/api/prom/rules[...]`, `/prometheus/api/v1/rules` | `/api/rules/v1/*` | M7 |

Routing note: the alias `GET /api/traces/{traceId}` coexists with native `/api/traces/v1/...`; the literal `v1` segment is matched first.

### 8.2 Ingest receivers (M6)

| Compatibility path | Format |
|--------------------|--------|
| `POST /loki/api/v1/push` | log push, JSON or snappy protobuf |
| `POST /tempo/spans`, `POST /api/v2/spans` | Zipkin v2 JSON |
| `POST /ingest` | pprof multipart (alias of `/api/profiles/v1/ingest`, ships M5) |
| `POST /influx/api/v2/write` (+ health endpoints) | line protocol, `?precision=` honored |
| `POST /api/v2/logs` | Datadog logs JSON |
| `POST /api/v2/series` | Datadog metrics JSON |
| `POST /_bulk`, `/{target}/_bulk`, `/{target}/_doc[/{id}]`, `/{target}/_create/{id}` | Elastic NDJSON / doc |
| remote-write aliases `/api/prom/push`, `/api/v1/prom/remote/write`, `/prom/remote/write`, `/api/prom/remote/write` | snappy prompb (native path `/api/v1/write` is always on) |
