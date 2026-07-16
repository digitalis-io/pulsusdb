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

M1 ships the five core endpoints below (§2.1-2.3); `/tail` (§2.4) and
`/stats` (§2.5) ship M6, and the drilldown endpoints (§2.6) ship M7.

### 2.1 `GET|POST /api/logs/v1/query_range`

| Param | Type | Notes |
|-------|------|-------|
| `query` | LogQL | required |
| `start`, `end` | ns / RFC3339 | default: `end = now`, `start = end - 1h` |
| `step` | duration \| int (seconds) | metric queries only; derived `clamp((end-start)/250, >=1s)` when omitted |
| `limit` | int | max **total** entries returned across the response, ordered by `direction` (newest-first for `backward`); global, not per-stream (default 100, hard cap 5000 — values above the cap are rejected with `400`) |
| `direction` | `forward`\|`backward` | default `backward` |

`POST` accepts the same param names as an `application/x-www-form-urlencoded` body (large queries/long ranges can exceed URL length limits; mainstream Loki-datasource clients POST this endpoint).

`limit` bounds the total number of log entries in the response (global), consistent with the reference log-API semantic; it is not applied per stream.

Response: `{"status":"success","data":{"resultType":"streams"|"matrix","result":[...],"stats":{...}}}` — log selector queries return `streams`, metric queries return `matrix`. Streams are sorted by label set for a deterministic response.

- **streams**: `result: [{"stream":{k:v,...},"values":[["<ts_ns>", "<line>"],...]}, ...]`. `ts_ns` is a **string** (nanosecond precision overflows JS's safe-integer range). `stats: {"streams":N,"entries":N,"bytes":N}` (`bytes` = decoded line bytes).
- **matrix**: `result: [{"metric":{k:v,...},"values":[[<unix_seconds>, "<value>"],...]}, ...]`. Timestamps are Prometheus-style unix-seconds numbers (millisecond resolution — exact for every M1 step, which is always `>= 1s`); `value` is a quoted string (`"NaN"`/`"+Inf"`/`"-Inf"` as applicable, matching §3.1's convention). `stats: {"series":N}`.
- With `X-Pulsus-Explain: 1`, `data.explain = {"result_type","routing":{"chosen":"rollup"|"raw","reason":"..."}|null,"stages":[{"name","sql","note"|null},...]}` is added alongside `data.stats`.

### 2.2 `GET|POST /api/logs/v1/query`

Instant evaluation at `time` (ns / RFC3339, default now). Returns `vector` (`result: [{"metric":{...},"value":[<unix_seconds>, "<value>"]}, ...]`) or `streams`, plus `stats`/`explain` per §2.1's shapes. `POST` accepts the same param names as an `application/x-www-form-urlencoded` body (same rationale as `query_range`).

### 2.3 Labels & series

```
GET|POST /api/logs/v1/labels                 ?start=&end=
GET      /api/logs/v1/label/{name}/values    ?start=&end=
GET|POST /api/logs/v1/series                 ?match[]=<selector>&start=&end=
```

`start`/`end` default the same way as §2.1 (`end = now`, `start = end - 1h`). POST accepts the same params as an `application/x-www-form-urlencoded` body (`match[]` repeated for `/series`); `/label/{name}/values` is `GET`-only. `match[]` selectors are bare LogQL stream selectors (e.g. `{service_name="checkout"}`); at least one is required.

Responses: `{"status":"success","data":[...]}` — `labels`/`label/{name}/values` return an array of strings, `series` returns an array of label maps (sorted for a deterministic response). With `X-Pulsus-Explain: 1`, `explain` (the §2.1 shape, `routing` always `null`) is added as a **top-level sibling of `data`** (not nested under it — these responses' `data` is an array, not an object).

**`label/{name}/values` M1 scope:** returns every distinct value of `name` within `[start, end]`; `query=`-selector narrowing (restricting to values seen only on streams matching a selector) is deferred to M6 parity.

#### Errors (§2.1-2.3)

`{"status":"error","errorType":"...","error":"...","position":<byte offset>?}` — `position` is present only for LogQL parse errors.

| Cause | HTTP | `errorType` |
|-------|------|-------------|
| Malformed params, malformed LogQL, empty/contradictory matchers, invalid `step` | `400` | `bad_data` |
| Query rejected as too broad (scan-budget or stream-count cap exceeded) | `422` | `query_too_broad` |
| ClickHouse read timed out | `504` | `timeout` |
| Unclassified ClickHouse/internal failure | `500` | `internal` |

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

For a historical window (outside the resident label cache's `PULSUS_CACHE_WINDOW`), `/series`, `/labels`, and `/label/{name}/values` resolve from `metric_series` with bucket-floored bounds (docs/schemas.md §2.1) — their result is the **bucket-granularity active set**, a documented, bounded superset of Prometheus's exact-sample-window set (never a subset — over-inclusion is bounded by the activity-bucket size, and it is never a false empty).

### 3.4 Status

```
GET /api/v1/status/buildinfo     → version, revision, build metadata
GET /api/v1/status/config        → effective config (redacted), Prometheus envelope
GET /api/v1/status/flags         → static-equivalent flag map
GET /api/v1/status/runtimeinfo   → process start time, storage retention
GET /api/v1/status/tsdb          → numSeries, top metrics by cardinality
```

`status/tsdb` is served entirely from the resident reader label cache (zero ClickHouse), fresh to within `PULSUS_CACHE_TTL`; it reports `numSeries` and `seriesCountByMetricName` (top cardinality). `numSamples` is **omitted** — it is not a Prometheus `headStats` field and cannot be served without a live sample scan, which the zero-ClickHouse contract forbids.

#### Errors (§3.1-3.4)

`{"status":"error","errorType":"...","error":"..."}` — exactly these three fields, **no `position` field** (unlike the log API's §2.3 envelope): a PromQL parse error's position is embedded verbatim inside the `error` message string, Prometheus-style, never split out.

| Cause | HTTP | `errorType` |
|-------|------|-------------|
| Malformed params, malformed PromQL (parser position **in the message**), 11,000-point cap exceeded | `400` | `bad_data` |
| Out-of-subset construct / binary-op matching failure / histogram-bucket error | `422` | `execution` |
| ClickHouse read timed out | `503` | `timeout` |
| Pool or label cache not yet ready, ClickHouse unreachable | `503` | `unavailable` |
| Unclassified internal failure | `500` | `internal` |

---

## 4. Traces query API

### 4.1 Trace fetch

```
GET /api/traces/v1/trace/{traceId}         → OTLP-shaped trace (protobuf or JSON by Accept)
GET /api/traces/v1/trace/{traceId}/json    → force JSON
```

`traceId` is hex (16 or 32 chars, left-padded). `404` with an error envelope when absent.

**Content negotiation.** The default representation is OTLP-canonical JSON (protojson: hex trace/span ids, camelCase fields, 64-bit integers as strings) with `Content-Type: application/json`; no `Accept` header means JSON. `Accept: application/protobuf` (or its request-side alias `application/x-protobuf`) selects the protobuf `TracesData` encoding, returned as `Content-Type: application/protobuf` — deliberately asymmetric with OTLP *ingest*, which uses `application/x-protobuf` per the OTLP/HTTP spec; the query response follows the Tempo/Grafana client convention instead, and never emits `x-protobuf`. Quality values are honored per RFC 9110 (`;q=` weights, exact `type/subtype` > `type/*` > `*/*` specificity, `q=0` excludes; an equal-quality tie resolves to JSON). An `Accept` header under which neither served representation is acceptable (e.g. `text/plain`, or every matching range at `q=0`) is rejected with `406 not_acceptable`. The `/json` suffix forces JSON unconditionally — it never consults `Accept` and never returns `406`.

**Response shape.** One `TracesData` assembling every stored span of the trace; at-least-once ingest duplicates are deduplicated by span id at read time. Spans are returned in a canonical order — ascending `(startTimeUnixNano, spanId)` — so responses are byte-deterministic regardless of storage read order.

**Errors** are always the JSON envelope (`{"status":"error","errorType":...,"error":...}`), regardless of `Accept`:

| Cause | HTTP | `errorType` |
|-------|------|-------------|
| Malformed `traceId` (not 16/32 hex chars) | `400` | `bad_data` |
| Trace absent | `404` | `not_found` |
| No acceptable representation under `Accept` | `406` | `not_acceptable` |
| ClickHouse read timed out | `504` | `timeout` |
| Unclassified ClickHouse/internal failure (incl. undecodable or unsupported stored payloads) | `500` | `internal` |

### 4.2 `GET /api/traces/v1/search`

| Param | Notes |
|-------|-------|
| `q` | TraceQL query (preferred) |
| `tags`, `minDuration`, `maxDuration` | legacy search params, compiled to TraceQL internally (below) |
| `start`, `end` | unix seconds; **both required**, `end > start` |
| `limit`, `spss` | result cap (default 20) and spans-per-spanset cap (default 3); positive integers |

**`q` vs legacy params:** mutually exclusive — supplying `q` together with any of `tags`/`minDuration`/`maxDuration` is a `400 bad_data`, never silent precedence. Supplying neither is a valid time-range-only search (`{}`).

**Legacy compilation:** `tags` is logfmt — space-separated `key=value` pairs; a value may be double-quoted to contain spaces/`=`, and inside quotes `\"` and `\\` are the only escapes. Each pair compiles to an **unscoped** `.key="value"` conjunct; `minDuration`/`maxDuration` compile to `duration >= <lit>` / `duration <= <lit>`; all conjuncts join with `&&` in one `{ … }` and the result goes through the ordinary TraceQL parser (one validation path). The grammar is enforced strictly: a bare key with no `=`, an empty key, an unterminated quote, an `=` or `"` inside an **unquoted** value (quote the value instead), a quoted value not followed by whitespace/end-of-input, or any escape other than `\"`/`\\` is a `400 bad_data` carrying `position` — the byte offset into the decoded `tags` value.

**Duration literals** (in `q`, e.g. `duration > 2s`): an **unsigned** decimal number (integer or fraction — `2`, `1.5`, `.5`) **immediately** followed by exactly **one** unit from `{ns, us, µs, ms, s, m, h}`. No sign; no compound literals (`1h30m` is rejected). A fractional literal is valid only if it resolves to an exact whole number of nanoseconds (`0.5s` = 500000000ns is valid; `0.1ns` is a positioned parse error) — no rounding, no truncation.

**Regex operators** (`=~`/`!~`) are full-value anchored (`^(?:…)$`), matching the label-matcher convention across PulsusDB's query languages. `!=`/`!~` on an attribute match spans **lacking the key entirely** as well as spans whose value differs.

Response: `{"traces":[...],"metrics":{"partial":<bool>,"limit":<n>,"returned":<n>}}`. Each trace carries `traceID`, `rootServiceName`, `rootTraceName`, `startTimeUnixNano` (string nanoseconds; root metadata comes from the **whole** trace, so a root that predates `start` is still reported correctly), `durationMs` (the root span's duration), and `spanSets`: one entry of `{"matched":<total matched spans>,"spans":[...]}` where each span summary carries `spanID`, `name`, `startTimeUnixNano`, `durationMs`, plus an `attributes` list (`{"key","value":{"stringValue"}}`) for `select()`-projected fields.

**Ordering contract:** `traces[]` is ordered by the max timestamp of each trace's exactly-matched spans, **descending**, with `trace_id` ascending as the tiebreak — deterministic under timestamp ties.

**Partial results:** the response returns at most `limit` traces (the top-K under the ordering contract above). Candidate generation and consumption are capped **separately** from `limit`, both at `PULSUS_TRACEQL_MAX_CANDIDATES`: each candidate generator is a top-K-by-recency read of that depth, and the merged candidate stream is evaluated up to that many candidates — so the engine may evaluate up to `PULSUS_TRACEQL_MAX_CANDIDATES` candidates even for a small `limit` (stopping earlier only when no unseen candidate can still enter the top `limit`). `metrics.partial` is `true` whenever any internal bound engaged before natural exhaustion — a candidate generator hit its `PULSUS_TRACEQL_MAX_CANDIDATES` depth, the candidate consumption ceiling was reached with candidates still unconsumed, or a single trace exceeded the 10,000 hydrated-spans-per-trace cap (that trace is evaluated on its truncated span set, never silently reported complete). `metrics.limit` echoes the request's `limit`; `metrics.returned` is the returned trace count.

**Errors** use the §4.1 JSON envelope; a TraceQL parse error carries `position` (byte offset into `q`), and a `tags` logfmt error carries `position` (byte offset into the decoded `tags` value):

| Cause | HTTP | `errorType` |
|-------|------|-------------|
| Malformed `q` / params / `tags` logfmt / `q`+legacy conflict / unsupported operator-type combination | `400` | `bad_data` |
| Scan or memory budget exceeded (`PULSUS_TRACEQL_SCAN_BUDGET_ROWS` rows read, read/result byte ceilings, or the engine's 256 MiB retention budget) — too broad to bound, never silently slow or quietly incomplete | `422` | `query_too_broad` |
| ClickHouse read timed out | `504` | `timeout` |
| Unclassified failure | `500` | `internal` |

### 4.3 Tags

```
GET /api/traces/v1/tags                   ?scope=&start=&end=      (scoped response shape)
GET /api/traces/v1/tag/{tag}/values       ?q=&start=&end=          (typed values)
```

Served exclusively from `trace_tag_catalog` (bounded, deduplicated) — never by scanning span payloads or the attribute index.

| Param | Notes |
|-------|-------|
| `scope` | `resource` or `span`; omitted = both scopes. Anything else (incl. `intrinsic`/`none`) is a `400 bad_data`, never silently widened |
| `{tag}` | `resource.<key>` / `span.<key>` scope the lookup; a leading-`.` or bare key is unscoped (values from both scopes) |
| `start`, `end` | accepted for client compatibility and **ignored**: the catalog has no timestamp column, so tag discovery is time-less. Catalog entries can therefore **outlive** the 7-day span retention (the source `trace_attrs_idx` is TTL'd; `trace_tag_catalog` has no TTL) |
| `q` | accepted and **ignored** (best-effort narrowing, Tempo semantics): when `q` cannot be evaluated against the catalog, results may be a **superset** of what a narrowing query would return |

Response shapes (native; the §8.1 Tempo aliases are projections of these):

```json
{"scopes":[{"name":"resource","tags":["env","service.name"]},{"name":"span","tags":["http.status_code"]}],"truncated":false}
{"tagValues":[{"type":"string","value":"checkout"},{"type":"int","value":"500"}],"truncated":false}
```

Tag names are ordered `(scope, key)` ascending; values are ordered ascending. Responses are capped at **10 000** tag names / **1 000** values per request (documented constants `TAG_NAMES_MAX`/`TAG_VALUES_MAX`); a capped response sets the top-level `"truncated": true` — never an indistinguishable silent subset.

**Typed values are best-effort inference** from the stored string (the catalog stores values type-lessly): exact `true`/`false` → `bool`; a valid §4.2 duration literal (by the normative parser — `.5s` yes, `0.1ns`/`1h30m`/`1d` no) → `duration`; optional-sign integers → `int`; `f64`-parseable → `float`; else `string`. Known limit: a numeric- or duration-*looking* string attribute infers as numeric/duration.

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
GET /ready        → 200 when ClickHouse reachable (+ label cache warm in reader mode, from M2); 503 otherwise
GET /metrics      → Prometheus exposition of PulsusDB internals
GET /config       → effective configuration, secrets redacted (rendered as YAML text, served as `text/plain; charset=utf-8` — not a YAML media type)
GET /buildinfo    → {"version","revision","builtAt","rustc"}
```

When basic auth is enabled, `/ready` and `/metrics` remain **unauthenticated** (liveness probes and metric scrapers must work without credentials); `/config`, `/buildinfo`, and every data-plane route require auth.

---

## 8. Compatibility endpoints (optional, `PULSUS_COMPAT_ENDPOINTS=true`)

Disabled by default. When enabled, PulsusDB additionally mounts third-party API surfaces so existing datasources, agents, and dashboards work unmodified. These are aliases onto the native handlers (or foreign-format parsers feeding the same pipeline); they carry no additional semantics and are not part of the versioned PulsusDB API.

### 8.1 Query aliases

The M1 log-query aliases (`/loki/api/v1/{query_range,query,labels,label/*/values,series}`) are pure route bindings onto the native `/api/logs/v1` handlers — responses are byte-identical to native, including `X-Pulsus-Explain` passthrough. They mount iff `PULSUS_COMPAT_ENDPOINTS=true` **and** the Reader subsystem is mounted (docs/architecture.md §1's mode table); they 404 exactly where native does (e.g. writer-only mode never mounts either surface). Gating is decided once at router-build time, not per request.

When `PULSUS_AUTH_*` is set, the perimeter returns 401 to every unauthenticated request regardless of path existence; authenticated requests to an unmounted alias (flag off, or non-Reader mode) return 404, indistinguishable from any nonexistent route.

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
