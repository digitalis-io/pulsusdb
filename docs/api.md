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
Content-Type: application/x-protobuf   (default; OTLP/JSON via application/json since M6)
```

- Content negotiation: the body encoding is selected by `Content-Type` — `application/json` decodes the body as OTLP/JSON (proto3-JSON: hex `trace_id`/`span_id`, camelCase fields, u64 timestamps as strings, non-finite doubles as `"NaN"`/`"Infinity"`/`"-Infinity"`); anything else (including absent, or `application/x-protobuf`) decodes as protobuf. Both encodings feed the identical parse/row path, so they are byte-identical downstream. Enum fields are accepted as either the integer form (the form real OTLP/JSON emitters send) or the proto3-JSON string name (e.g. `"kind": "SPAN_KIND_SERVER"`); an unknown enum name is rejected `400`. `Content-Encoding` (gzip/zstd/snappy) applies to a JSON body unchanged.

- Resource + scope attributes flatten into labels under the canonical label model ([architecture.md §2.3](architecture.md)): for logs and metrics, attribute keys are normalized to Prometheus-style names at ingest (`service.name` → `service_name`); trace attributes keep their OTel names verbatim and are queried as such in TraceQL. Log body → line; spans → trace tables with original protobuf retained as payload; metric data points → metric samples with `__name__` from the metric name; profiles → pprof-equivalent tree precomputation.
- Responses: `200` with OTLP partial-success message when applicable; `429` on backpressure.
- A metric data point whose `time_unix_nano` resolves to a UTC day outside the supported storage time range (before 1970-01-01 or after 2106-02-06) is rejected per-point as OTLP partial success, matching the Zipkin unrepresentable-timestamp precedent (§8.2) — a day past 2106-02-06 would wrap in the 32-bit `DateTime` domain the `metric_samples`/`metric_hist_samples` delete-TTL evaluates in (and a day past 2149-06-06 additionally falls outside the `Date` domain the tables partition on), so such a point cannot be stored safely (docs/schemas.md §2.1, issue #137).
- The `/v1development/profiles` path tracks the OTLP spec's experimental profiles signal and will follow it to `/v1/profiles` on stabilization (the old path remains as an alias).

### 1.2 Prometheus remote write

```
POST /api/v1/write
Content-Type: application/x-protobuf, Content-Encoding: snappy
```

`prompb.WriteRequest`. Supported as a first-class alternative for metrics because the OTel Collector's `prometheusremotewrite` exporter is a common metrics pipeline. `__name__` becomes `metric_name`; remaining labels are fingerprinted (xxhash64, sorted `k\xffv\xff` serialization). Stale markers (NaN `0x7FF0000000000002`) stored verbatim. A sample whose timestamp resolves to a UTC day outside the supported storage time range (before 1970-01-01 or after 2106-02-06, same cutoff and 32-bit-`DateTime` delete-TTL rationale as OTLP metrics above; docs/schemas.md §2.1, issue #137) is dropped and counted in `rejected_total`, not surfaced in the response body. Success: `204`.

**Native histograms (issue #140).** Integer native histograms (`TimeSeries` tag 4, the RW-1.0 `Histogram` message) are decoded and stored to `metric_hist_samples`; the wire form is already the stored integer shape (spans + delta-encoded buckets), copied verbatim, including NHCB (schema −53 + `custom_values`). The per-sample `ResetHint` maps `GAUGE` → `counter_reset_hint = 3` and everything else (UNKNOWN/YES/NO and forward-compatible unknown values) → `0` (Unknown) — YES/NO are deliberately not persisted (per-sample reset hints are unreliable across sender resharding; the read side re-detects counter resets, so only the series-level gauge property is stored). Float-flavor histograms (`count_float`/`zero_count_float`/`*_counts`) are structurally unstorable in the integer-delta columns and are dropped per-point into `rejected_total`, as are histograms whose `timestamp` fails the same storage-time-range gate as samples. A float sample colliding with a native histogram at the same `(series, timestamp)` within one request loses to the histogram (histogram-wins, matching OTLP ingest and the read-side tie-break). Exemplars (tag 3) and RW-2.0 remain unsupported.

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

- **streams**: `result: [{"stream":{k:v,...},"values":[["<ts_ns>", "<line>"],...]}, ...]`. `ts_ns` is a **string** (nanosecond precision overflows JS's safe-integer range). `stats: {"streams":N,"entries":N,"bytes":N}` (`bytes` = decoded line bytes). A pipeline with an in-engine dropping stage (a label filter, or a line filter after `line_format`) is served by fetch-until-limit keyset paging that fills exactly to `limit`; when the byte scan budget (`reader.logql_scan_budget_bytes`) is exhausted before the limit fills, the response returns the survivors gathered so far and adds `stats.pulsus_partial: true` — a PulsusDB-contract signal (Loki has no byte-budget-truncation equivalent; mirrors the traces-search `metrics.partial` precedent in §4.2) distinguishing a budget-truncated result from a complete one. The field is **omitted** on complete results (the fast path, the non-dropping path, and genuine window exhaustion), so ordinary responses are byte-identical to before; clients that don't know the key ignore it.
- **matrix**: `result: [{"metric":{k:v,...},"values":[[<unix_seconds>, "<value>"],...]}, ...]`. Timestamps are Prometheus-style unix-seconds numbers (millisecond resolution — exact for every M1 step, which is always `>= 1s`); `value` is a quoted string (`"NaN"`/`"+Inf"`/`"-Inf"` as applicable, matching §3.1's convention). `stats: {"series":N}`.
- With `X-Pulsus-Explain: 1`, `data.explain = {"result_type","routing":{"chosen":"rollup"|"raw","reason":"..."}|null,"stages":[{"name","sql","note"|null},...]}` is added alongside `data.stats`.

**Metric binary operations & vector matching (issue #91).** LogQL metric expressions support binary operations between range vectors — arithmetic, comparison (with `bool`), and the `and`/`or`/`unless` set operators — with the full `on(...)`/`ignoring(...)` and `group_left(...)`/`group_right(...)` vector-matching modifiers (semantics oracle-verified against `grafana/loki:3.4.2`). One-to-one matches output the reduced (`on`/`ignoring`) signature; `group_left`/`group_right` pass the many side's labels through whole and copy the include labels from the one side. A cardinality violation is a `400 bad_data` carrying the reference store's exact message (`multiple matches for labels: many-to-one matching must be explicit …` / `… many-to-many matching not allowed: matching labels must be unique on one side`); a bare `group_left`/`group_right` with no preceding `on`/`ignoring` is a parse-time `400`. Matrix (range) binops apply the vector match independently per step. Note: the reference store returns HTTP `500` for these runtime matching errors while PulsusDB returns `400` (the semantically correct bad-request code); the error bodies agree. Streams-path error series carry both `__error__` and its human-readable `__error_details__` companion (issue #99), byte-exact against the reference where feasible; the metric pipeline-error path is unchanged.

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
| `query` | LogQL log stream query (selector + pipeline, evaluated by the same engine as §2.1); metric queries are rejected `400` |
| `limit` | cap on entries per frame (default 100; values above `PULSUS_TAIL_MAX_FETCH_LIMIT` are silently clamped) |
| `start` | starting timestamp (ns), default now − 1h |
| `delay_for` | seconds to delay to tolerate late arrivals (default 0; values above `PULSUS_TAIL_MAX_DELAY` — 5s — are clamped) |

Frames: `{"streams":[...],"dropped_entries":[{"labels":{...},"timestamp":"<ns>"}],"dropped_total":<n>}`. Slow consumers get the **oldest** undelivered frames evicted and reported, never unbounded buffering: `dropped_entries` is a bounded representative sample (at most `PULSUS_TAIL_MAX_ENTRIES_PER_FRAME` rows), and `dropped_total` — a PulsusDB **additive** field next to the reference frame shape; clients that don't know it ignore the extra key — carries the *exact* cumulative count dropped since the previous frame (`0` on a normal frame). Exceeding `PULSUS_TAIL_MAX_CONNECTIONS` concurrent tail connections rejects the next one `429 too_many_requests` before the upgrade.

Delivery: tail polls ClickHouse (there is no push channel) with a deterministic composite keyset cursor — `(timestamp_ns, fingerprint, cityHash64(body))` plus an occurrence count — catching up over a backlog one `PULSUS_TAIL_CATCHUP_SLICE` window per query, so no single query scans unbounded history. Every row from `start` forward is delivered **exactly once**, including timestamp tie groups split across fetch pages and byte-identical duplicate lines inside a scanned window. Sole documented limitation: an entry arriving later than `delay_for` at an already-scanned position — at or below the cursor/watermark, e.g. a late byte-identical duplicate of an already-delivered same-nanosecond line — is genuinely late and is not delivered.

### 2.5 `GET /api/logs/v1/stats`

`?query={selector}&start=<ns>&end=<ns>` → `{"streams":N,"chunks":N,"entries":N,"bytes":N}`. `query` accepts a stream selector plus optional line filters; anything else (parsers, formats, label filters, metric queries) is rejected `400`. `chunks` is a **partition-count proxy**: the selector-scoped distinct count of partition dates touched, not a physical MergeTree part count (per-part fidelity, if ever demanded, routes to the scale-validation milestone). Without a line filter the counters are served from the rollup with zero body reads (entries/bytes are 5s-bucket-granular at window edges, the same rollup-routing caveat as `count_over_time`); a line filter forces an exact `log_samples` scan. With `X-Pulsus-Explain: 1`, `explain` (the §2.1 shape) is added as a sibling key of the four counters.

### 2.6 Drilldown (M7)

```
GET      /api/logs/v1/volume             ?query=&start=&end=&limit=&targetLabels=&aggregateBy=
GET|POST /api/logs/v1/detected_labels    ?query=&start=&end=
GET|POST /api/logs/v1/detected_fields    ?query=&start=&end=&line_limit=&limit=
GET      /api/logs/v1/patterns           ?query=&start=&end=&step=
```

#### 2.6.1 `GET /api/logs/v1/volume`

Per-label-set log byte volumes over `[start, end]` — the drilldown UI's "which streams are loud" aggregation. **GET-only** (the reference also mounts POST; additive later if a client demands it). Served **entirely from the 5s rollup with zero body reads** — the endpoint accepts a matchers-only selector, so unlike §2.5 there is no raw fallback at all.

| Param | Notes |
|-------|-------|
| `query` | LogQL **stream selector, matchers only** — required. ANY pipeline stage is rejected `400` (line filters included, unlike §2.5: the rollup is body-content-blind and volume has no raw scan to fall back on), as are metric queries. The match-all `{}` is rejected `400` (PulsusDB's ≥1-positive-matcher rule; the reference accepts `{}` here — documented deviation; `targetLabels` remains fully usable with any non-empty selector, e.g. `{env=~".+"}`) |
| `start`, `end` | ns / RFC3339; default `end = now`, `start = end - 1h` (§2.1). `end < start` is `400` |
| `limit` | top-N entries kept **after** the bytes-desc sort. Absent **or `0`** → 100 (the reference resets an explicit 0 to its default); above 5000 → `400`, never clamped (§2.1's cap rule) |
| `aggregateBy` | `series` (default): group by the matched label **pairs**. `labels`: group by bare label **names** — each entry's metric is `{"<name>":""}` (the reference's empty-value shape) |
| `targetLabels` | comma-separated label names re-keying the aggregation. When supplied, entries key on these names alone (both modes); each target with no matcher of its name in the selector is injected as `name=~".+"` before planning, so negative-only or unrelated selectors still resolve target-keyed streams. **Bounded** (documented deviation — the reference has no caps; same defensive 400-not-clamp posture as `limit`): at most **32** names post-dedupe, each at most **256** bytes (post-percent-decode) — oversized requests are rejected `400` in pure param parsing, before any planning or SQL |

Without `targetLabels`, the aggregation keys on the selector's **own matcher names** — every operator, including `!=`/`!~` (so `{env!="dev"}` keys results by each stream's `env` value); a stream lacking a keyed label omits that pair from its key, and a stream matching none of the names groups under `{}`.

Response `200`: the §2.2 vector envelope evaluated at `end` — `{"status":"success","data":{"resultType":"vector","result":[{"metric":{...},"value":[<end_unix_seconds>,"<bytes>"]},...],"stats":{"series":N}}}`. **Result order is bytes-desc (tie-break: label set asc), truncated to `limit` — NOT label-sorted** (the top-N presentation is the contract; deliberately different from §2.2's label-sorted vectors). `stats` is a PulsusDB-additive key (same clients-ignore-extras precedent as §2.4's `dropped_total`). `bytes` is the sum of line-body bytes (the same basis as §2.5's `bytes`), 5s-bucket-granular at window edges (the same rollup caveat as §2.5/`count_over_time`). With `X-Pulsus-Explain: 1`, `data.explain` (the §2.1 shape) is added — its `volume_read` stage always targets `log_metrics_5s`.

Errors: `400 bad_data` (missing/malformed `query`, metric query, any pipeline stage, invalid `aggregateBy`/`limit`, oversized `targetLabels`, `end < start`), `422 query_too_broad`, and `503`/`504`/`500` per §2.3's table.

#### 2.6.2 `GET|POST /api/logs/v1/detected_labels`

Indexed stream labels with exact per-key value cardinalities — the drilldown UI's label picker. **Reads ONLY the stream index (`log_streams_idx`)**, via one month-partition-pruned server-side aggregation (one row per distinct key crosses the network, never one per value); it never touches `log_samples`. Structured metadata is deliberately absent (it never enters the stream index — matching the reference, whose detected-labels reads only the label index). `GET|POST` form-encoded (the §2.3 `/labels` precedent; a documented deviation from this section's earlier GET-only sketch, ratified on issue #170).

| Param | Notes |
|-------|-------|
| `query` | **optional, matchers only** — absent or empty is the unscoped form (every stream in the window, matching the reference's empty-string handling); when present, stage-1 resolution scopes the same aggregation with `fingerprint IN`. A pipeline stage or metric expression is a `400` parse error carrying `position` (the selector-only grammar rejects it) |
| `start`, `end` | ns / RFC3339; default `end = now`, `start = end - 1h` (§2.1) |

Relevance filter (reference-pinned): labels named `cluster`/`namespace`/`instance`/`pod` are always kept; any other label is dropped iff **every** one of its values parses as a float or a UUID (all four `uuid.Parse` forms — hyphenated, `urn:uuid:`-prefixed, `{hyphenated}`, bare 32-hex — case-insensitive). The float test is ClickHouse `toFloat64OrNull` (single SQL implementation, no Rust twin to drift); margins vs Go `ParseFloat` (hex floats like `0x1p-2`, underscore literals) are accepted, documented divergences.

Response `200`: `{"detectedLabels":[{"label":"…","cardinality":N},…]}`, sorted by label. Documented divergences from the reference, all deliberate: `cardinality` is **exact** (`uniqExact`) rather than a hyperloglog estimate; no `sketch` key is emitted (valid under the reference's own `omitempty`); the top-level key is always present (never omitted when empty); deterministic label-sorted order vs Go map order. With `X-Pulsus-Explain: 1`, `explain` (the §2.1 shape) is added as a sibling key — its `detected_labels` stage always targets `log_streams_idx`.

Errors: `400 bad_data` (malformed/piped `query`), `422 query_too_broad`, and `503`/`504`/`500` per §2.3's table.

#### 2.6.3 `GET|POST /api/logs/v1/detected_fields`

Per-entry **fields** detected from a bounded sample of matching log entries: structured-metadata keys (no parser attribution), the query pipeline's own extracted labels, and automatic json-first/logfmt-fallback parsing of each (post-pipeline) line — a parser counts as successful only when it sets no `__error__`. `GET|POST` form-encoded.

| Param | Notes |
|-------|-------|
| `query` | **required** — a full LogQL log-selector expression including pipeline stages; metric queries are rejected `400` |
| `start`, `end` | ns / RFC3339; §2.1 defaults |
| `line_limit` | entries sampled (default 100). `0` or non-numeric → `400`; above 5000 → `400`, never clamped (§2.1's cap rule) |
| `limit` (legacy alias `field_limit`) | max distinct field **names**, first-seen wins — later names are skipped entirely (default 1000; `0`/non-numeric → `400`; above 5000 → `400`). `limit` is read first, then the alias |
| `step`, `since` | accepted and **ignored** (documented deviation: the reference validates `step` only as a shared-codec artifact and neither param affects detection) |

Sampling contract (issue #170 plan v2): the sample is up to `line_limit` **post-pipeline matching** entries, newest first. With no in-engine dropping stage (a bare selector, line filters, non-dropping transforms — the dominant drilldown shape) one index-served `LIMIT line_limit` scan is provably that sample (line-filter pushdown carries the exact predicate). A dropping stage (a label filter, or a line filter after `line_format`) engages the §2.1 fetch-until-limit keyset paging under the **same byte scan budget an equivalent `/query_range` would pay** (`reader.logql_scan_budget_bytes`), so matches occurring long after the first `line_limit` raw rows are still found. If the budget is spent mid-paging, the response returns the fields found so far and adds the additive `"pulsus_partial": true` key — **omitted** on complete responses (the §2.1 `stats.pulsus_partial` convention), so complete responses stay byte-identical to the reference shape. A first page alone overflowing the budget is `422 query_too_broad`.

Type detection: `type` ∈ `string`\|`int`\|`float`\|`boolean`\|`duration`\|`bytes`, detected in the reference's pinned order int → float → boolean → duration → bytes → string, re-detected per observation (the last sampled entry wins). Duration/bytes reuse the §2.1 label-filter unit parsers; margins vs the reference (Go hex/underscore float literals; `d`/`w` duration suffixes accepted here but not by Go's `time.ParseDuration`; spaced byte quantities like `"42 MB"` accepted there but not here) are accepted, documented divergences.

Response `200`: `{"fields":[{"label":"…","type":"…","cardinality":N,"parsers":["json"|"logfmt",…]},…],"limit":N}`, sorted by label. `parsers` is always an array (`[]` for fields observed only from structured metadata or the query's own pipeline — deterministic-shape divergence from the reference's nil-slice marshaling); `cardinality` is exact over the sampled values (vs the reference's sketch estimate); the empty result is `{"fields":[],"limit":N}` where the reference returns `{}`; `__error__`/`__error_details__` never surface as fields. With `X-Pulsus-Explain: 1`, `explain` is added as a sibling key — its `detected_fields_read` stage carries the single stage-3 scan (note `single-scan: no unpushed dropping stage`) or the first keyset page (note `paged: unpushed dropping stage`).

Errors: `400 bad_data` (missing/malformed `query`, metric query, invalid `line_limit`/`limit`), `422 query_too_broad`, and `503`/`504`/`500` per §2.3's table.

#### 2.6.4 `GET /api/logs/v1/patterns`

Detected **log patterns** — the drilldown UI's "group these lines by shape" view. Each pattern is a **deterministic, stateless** token-class template of the line body (extracted at ingest, aggregated per `(fingerprint, 10s-bucket, template)` into `log_patterns`; docs/schemas.md §3.1): digit/length classification (a fragment with an ASCII digit, or longer than 64 bytes, becomes `<_>`), `key=value`/`key:value` awareness (only the value is classified), wrapper-punctuation preservation, and 1 KiB-prefix / 64-token / 512-byte caps. Templates are **normalized (whitespace-collapsed), not round-trip matchable**; grouping is deliberately coarser than an online clusterer (a digit-free variable word stays literal) in exchange for identity that survives merges across batches, shards, replicas, and retries. Served by ONE pushed-down aggregate over `log_patterns` with `fingerprint` primary-key prefix pruning and a server-side top-1000 — **no hydration, no body read** (the response carries no labels). **GET-only.**

| Param | Notes |
|-------|-------|
| `query` | LogQL **stream selector, matchers only** — required. ANY pipeline stage is rejected `400` (line filters included, like §2.6.1: templates are precomputed and the bodies are gone), as are metric queries |
| `start`, `end` | ns / RFC3339; default `end = now`, `start = end - 1h` (§2.1). Half-open `[start, end)` over the pattern buckets |
| `step` | optional bucket size; a duration string or bare seconds. Absent → derived `clamp((end-start)/250, ≥1s)`. **Floored to the 10s ingest bucket** (never smaller — a finer step would invent sub-bucket granularity the stored data lacks). The `(end-start)/step` grid is capped at **11,000** (else `400`), the same bound as the metrics endpoints |

Response `200`: the Loki-interop envelope `{"status":"success","data":[{"pattern":"<_> ...","samples":[[<unix_seconds>,<count>],...]},...]}`. `samples` are ascending by second, zero-count steps omitted, both elements bare integers (`unix_seconds` is the floor of the bucket ns). **`data` is ordered total-count desc then pattern asc, truncated to the top 1000 — NOT re-sorted client-side** (the top-N presentation is the contract; a PulsusDB determinism pin — upstream order is unspecified). **Count semantics** are exact on the clean ingest path and **best-effort approximate under ingest-failure re-sends**, at parity with §2.2's `log_metrics` (the writer never auto-replays a block that could have committed; a per-request burst of >10 000 distinct templates is an under-count event, folded into the same approximate semantics — see docs/schemas.md §3.1). With `X-Pulsus-Explain: 1`, `data.explain` (the §2.1 shape) is added — its `patterns_read` stage always targets `log_patterns`.

Errors: `400 bad_data` (missing/malformed `query`, metric query, any pipeline stage, non-positive `step`, over-11k grid), `422 query_too_broad`, and `503`/`504`/`500` per §2.3's table.

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

`match[]` selectors accept the full discovery selector surface: a concrete metric name (`up`), a matcher-only selector (`{job="api"}`), and a regex/negated `__name__` matcher (`{__name__=~"up.*"}`, `{__name__!="up",job="api"}`) — the last at parity with the query path. A regex/negated-`__name__` selector resolves its candidate metric names through the resident label cache under `PULSUS_PROMQL_MAX_METRIC_FANOUT`, then fetches with one flat `metric_name IN (…) AND fingerprint IN (…)` query against `metric_series`. When the resident cache is **degraded/cold** (cold / stale / out-of-window / regex-cache-full) the discovery path falls back to a bounded two-stage `metric_series` read: a `SELECT DISTINCT metric_name` probe (the name matchers pushed as `metric_name` predicates, `LIMIT PULSUS_PROMQL_MAX_METRIC_FANOUT + 1`) followed by the same flat `metric_name IN (…)` fetch with the label matchers applied in SQL — so a degraded regex/negated-`__name__` selector now **resolves** rather than returning a named `422`. A resolved (warm) or probed (degraded) candidate-name set past the cap is `422 execution` (`QueryTooBroad`), never an unbounded scan. The degraded probe caps on **names matching the name predicate** — a superset of the warm cap, which counts names with ≥1 label-matching series — so at the cap boundary the degraded path may `422` where warm would serve; below the cap the two results are byte-identical. A non-vector-selector `match[]` value (e.g. `sum(up)`) or brace-level `or` remains a parse-time rejection (`422 execution` / `400 bad_data` respectively). A broad regex/negated-`__name__` discovery selector can also independently exceed `PULSUS_PROMQL_MAX_CACHE_SCAN` (a selector whose matchers match few or no metric names can still *examine* the entire resident label cache) — this is `422 execution` too, on a **warm** cache, and never falls back to the degraded-cache probe; narrow the `__name__` matcher or use a metric-scoped/matcher-only selector instead.

For a historical window (outside the resident label cache's `PULSUS_CACHE_WINDOW`), `/series`, `/labels`, and `/label/{name}/values` resolve **all three** discovery selector shapes — concrete-name, matcher-only, and regex/negated-`__name__` — from `metric_series` with bucket-floored bounds (docs/schemas.md §2.1): their result is the **bucket-granularity active set**, a documented, bounded superset of Prometheus's exact-sample-window set (never a subset — over-inclusion is bounded by the activity-bucket size, and it is never a false empty). The regex/negated-`__name__` route reaches this set through the degraded-cache probe fallback described above (with the superset-cap caveat at the fan-out boundary); the only remaining discovery `422` is the fan-out-cap breach. The never-false-empty guarantee therefore covers every discovery selector shape. (The **query** path — `/query`, `/query_range` — keeps its degraded `422` for a name-less/regex-`__name__` selector: the never-false-empty guarantee is a discovery guarantee, and a query has no metric-scoped SQL-fallback shape for the name set.)

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

**Content negotiation.** The default representation is OTLP-canonical JSON (protojson: hex trace/span ids, camelCase fields, 64-bit integers as strings) with `Content-Type: application/json`; no `Accept` header means JSON. `Accept: application/protobuf` (or its request-side alias `application/x-protobuf`) selects the protobuf `TracesData` encoding, returned as `Content-Type: application/protobuf` — deliberately asymmetric with OTLP *ingest*, which uses `application/x-protobuf` per the OTLP/HTTP spec; the query response follows the Tempo/Grafana client convention instead, and never emits `x-protobuf`. Quality values are honored per RFC 9110 (`;q=` weights, exact `type/subtype` > `type/*` > `*/*` specificity, `q=0` excludes; an equal-quality tie resolves to JSON). An `Accept` header under which neither served representation is acceptable (e.g. `text/plain`, or every matching range at `q=0`) is rejected with `406 not_acceptable`. The `/json` suffix forces JSON unconditionally — it never consults `Accept` and never returns `406`. Every response from the negotiating route (success or error) carries `Vary: accept` per RFC 9110 §12.5.5; the `/json` route serves one representation and never adds `accept` to `Vary` (the global compression layer independently appends `accept-encoding` where applicable).

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
| `start`, `end` | unix s / ns / RFC3339 (§1's trace-API forms; integers with magnitude ≥ 10^12 are nanoseconds, smaller ones seconds); **both required**, `end > start` |
| `limit`, `spss` | result cap (default 20) and spans-per-spanset cap (default 3); positive integers |

**`q` vs legacy params:** mutually exclusive — supplying `q` together with any of `tags`/`minDuration`/`maxDuration` is a `400 bad_data`, never silent precedence. Supplying neither is a valid time-range-only search (`{}`).

**Legacy compilation:** `tags` is logfmt — space-separated `key=value` pairs; a value may be double-quoted to contain spaces/`=`, and inside quotes `\"` and `\\` are the only escapes. Each pair compiles to an **unscoped** `.key="value"` conjunct; `minDuration`/`maxDuration` compile to `duration >= <lit>` / `duration <= <lit>`; all conjuncts join with `&&` in one `{ … }` and the result goes through the ordinary TraceQL parser (one validation path). The grammar is enforced strictly: a bare key with no `=`, an empty key, an unterminated quote, an `=` or `"` inside an **unquoted** value (quote the value instead), a quoted value not followed by whitespace/end-of-input, or any escape other than `\"`/`\\` is a `400 bad_data` carrying `position` — the byte offset into the decoded `tags` value.

**Duration literals** (in `q`, e.g. `duration > 2s`): an **unsigned** decimal number (integer or fraction — `2`, `1.5`, `.5`) **immediately** followed by exactly **one** unit from `{ns, us, µs, ms, s, m, h}`. No sign; no compound literals (`1h30m` is rejected). A fractional literal is valid only if it resolves to an exact whole number of nanoseconds (`0.5s` = 500000000ns is valid; `0.1ns` is a positioned parse error) — no rounding, no truncation.

**Regex operators** (`=~`/`!~`) are full-value anchored (`^(?:…)$`), matching the label-matcher convention across PulsusDB's query languages. `!=`/`!~` on an attribute match spans **lacking the key entirely** as well as spans whose value differs.

**Structural operators** (issue #172, completed by #183): `{A} > {B}` (child — spans matching B whose **direct parent** matches A), `{A} >> {B}` (descendant — spans matching B with **any transitive ancestor** matching A, i.e. strictly below an A-matching span in the parent chain; an A-matching span is never itself yielded as a descendant, so a span is never its own descendant even under malformed cyclic parent links), `{A} < {B}` (parent — spans matching B that are the **direct parent** of an A-matching span), `{A} << {B}` (ancestor — spans matching B that are a **transitive ancestor** of an A-matching span; a span is never its own ancestor, cycle-safe), `{A} ~ {B}` (sibling — spans matching B sharing a `parent_id` with a **distinct** span matching A; spans with an all-zero `parent_id` — roots/no recorded parent — have no parent to share and **never** match `~`). Each of the five base relations also has a **negated** form (`!>`, `!>>`, `!<`, `!<<`, `!~`) and a **union** form (`&>`, `&>>`, `&<`, `&<<`, `&~`). The trace matches iff the relation's result set is non-empty. For the **plain** relation the result set is the **right-hand side's matching spans only** (`matched`, `spanSets`, aggregate filters, `select()`, and the ordering sort key all reflect the RHS spans — deliberately different from `&&`'s union of both operands' matches). The **negated** form returns the RHS spans that do **not** satisfy the relation; the edge case that matters: with an **empty LHS** (no A-matching span in the trace) every RHS span is a negated match, so the whole RHS set is returned. The **union** form returns **both** participating sides — the RHS spans satisfying the relation plus the LHS spans that participate (e.g. `{A} &> {B}` returns the child-side B spans **and** the parent-side A spans). Structural operators bind **tighter** than `&&`/`||` and are left-associative: `{a} && {b} > {c}` ≡ `{a} && ({b} > {c})`, `{a} > {b} > {c}` ≡ `({a} > {b}) > {c}`; parentheses override. Relations are evaluated over the trace's **hydrated** span set — window-bounded and capped at the 10,000-spans-per-trace hydration limit (an overflowing trace is already reported `partial`) — so an out-of-window intermediate hop breaks a `>>`/`<<` chain, and orphan spans (non-zero `parent_id` with no hydrated parent) never match `>`/`>>` on the child side. `>=`/`<=` between spansets are not real Tempo operators and stay 400 with the named construct.

**Field-vs-field comparison** (issue #183, `comparison.rhs_attribute`): a comparison whose right-hand side is another attribute or intrinsic, e.g. `{ .a = .b }`, `{ duration = span.slo }`, `{ .a > .b }` — either side an attribute or an intrinsic, with operators `= != < <= > >=` (a regex operator against a field RHS is rejected 400). Values are resolved per candidate span and compared engine-side under a **type gate** (verified against grafana/tempo:3.0.2): the two operands must be the same type — a **cross-type** pair (one numeric, one string) is **no match for every operator**, even on coincident text (`.a = "5"` string vs `.b = 5` int is not a match, and neither is `!=`). Same-type operands compare normally for all six operators: both numeric ⇒ numeric compare; both string ⇒ lexicographic string compare (`apple < banana`). An absent attribute key on either side is no match. (Arithmetic on the RHS — `{ .a = .b + 1 }` — stays the interim `arith.*` boundary.) **Bare boolean statics** `{ true }` / `{ false }` and **unary field negation** `{ !(.a = 1) }` / `{ !(.a = 1 && .b = 2) }` (`logic.not`) are also accepted; a spanset-level `!{…}` is rejected 400.

Response: `{"traces":[...],"metrics":{"partial":<bool>,"limit":<n>,"returned":<n>}}`. Each trace carries `traceID`, `rootServiceName`, `rootTraceName`, `startTimeUnixNano` (string nanoseconds; root metadata comes from the **whole** trace, so a root that predates `start` is still reported correctly), `durationMs` (the root span's duration), and `spanSets`: one entry of `{"matched":<total matched spans>,"spans":[...]}` where each span summary carries `spanID`, `name`, `startTimeUnixNano`, `durationMs`, plus an `attributes` list (`{"key","value":{"stringValue"}}`) for `select()`-projected fields.

**Response string truncation (issue #57 re-audit, owner-approved).** `rootServiceName`, `name` (span/root), and any `select()`-projected attribute `stringValue` are truncated at a hard **8192-byte** ceiling: strings at or under the cap are returned byte-identical; a longer string is cut to its first **2048 UTF-8 code points** instead (2048–8192 bytes, depending on code-point width — a UTF-8 code point is at most 4 bytes, so the 2048-code-point fallback itself never exceeds the 8192-byte ceiling). This bounds the search path's transient result-block memory at the source (docs/schemas.md §7) and is invisible for realistic telemetry (span/service names and projected attribute values are almost always well under 8 KiB); it is a documented, visible change only on pathological rows.

**Ordering contract:** `traces[]` is ordered by the max timestamp of each trace's exactly-matched spans, **descending**, with `trace_id` ascending as the tiebreak — deterministic under timestamp ties.

**Partial results:** the response returns at most `limit` traces (the top-K under the ordering contract above). Candidate generation and consumption are capped **separately** from `limit`, both at `PULSUS_TRACEQL_MAX_CANDIDATES`: each candidate generator is a top-K-by-recency read of that depth, and the merged candidate stream is evaluated up to that many candidates — so the engine may evaluate up to `PULSUS_TRACEQL_MAX_CANDIDATES` candidates even for a small `limit` (stopping earlier only when no unseen candidate can still enter the top `limit`). `metrics.partial` is `true` whenever any internal bound engaged before natural exhaustion — a candidate generator hit its `PULSUS_TRACEQL_MAX_CANDIDATES` depth, the candidate consumption ceiling was reached with candidates still unconsumed, or a single trace exceeded the 10,000 hydrated-spans-per-trace cap (that trace is evaluated on its truncated span set, never silently reported complete). `metrics.limit` echoes the request's `limit`; `metrics.returned` is the returned trace count.

**Errors** use the §4.1 JSON envelope; a TraceQL parse error carries `position` (byte offset into `q`), and a `tags` logfmt error carries `position` (byte offset into the decoded `tags` value):

| Cause | HTTP | `errorType` |
|-------|------|-------------|
| Malformed `q` / params / `tags` logfmt / `q`+legacy conflict / unsupported operator-type combination | `400` | `bad_data` |
| Scan or memory budget exceeded (`PULSUS_TRACEQL_SCAN_BUDGET_ROWS` rows read, read/result byte ceilings, the engine's 256 MiB retention budget, or the phase-1 candidate-generator's `PULSUS_TRACEQL_GENERATOR_MAX_MEMORY_BYTES` memory ceiling) — too broad to bound, never silently slow or quietly incomplete | `422` | `query_too_broad` |
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

**Scan bound.** A `scope`-confined `/tags` read and a scoped `/tag/{tag}/values` read prune to a `(scope)`/`(scope, key)` primary-key prefix; an unscoped `/tags` read or a bare-key (`{tag}` with no `resource.`/`span.` prefix) `/tag/{tag}/values` read cannot prune on `scope` and is a full catalog scan. That scan carries the same Layer-1 read-row budget the §4.2 search path uses (`PULSUS_TRACEQL_SCAN_BUDGET_ROWS`, `read_overflow_mode='throw'`): on a catalog large enough that the scan would exceed it, the request is rejected with `422 query_too_broad` rather than served as a slow unbounded scan. The `TAG_NAMES_MAX`/`TAG_VALUES_MAX` response caps above bound only a *successful* request's returned rows, not the rows a scan reads.

| Cause | HTTP | `errorType` |
|-------|------|-------------|
| `scope` outside `{resource, span}` (incl. `intrinsic`/`none`) | `400` | `bad_data` |
| Empty `{tag}` key | `400` | `bad_data` |
| Discovery scan exceeded the reader row budget (unscoped `/tags`, or a bare-key `/values` on a high-cardinality key) | `422` | `query_too_broad` |
| ClickHouse read timed out | `504` | `timeout` |
| Unclassified failure | `500` | `internal` |

### 4.4 TraceQL metrics

```
GET /api/traces/v1/metrics/query_range
GET /api/traces/v1/metrics/query
```

| Param | Notes |
|-------|-------|
| `q` / `query` | TraceQL metrics expression (e.g. `{span.http.status_code=200} \| rate()`) — exactly one of the two keys |
| `start`, `end` | unix s / ns / RFC3339 (§1's trace-API forms, the same parser as §4.2 search: integers with magnitude ≥ 10^12 are nanoseconds, smaller ones seconds) |
| `since` | relative alternative to start/end (`1h`, `30m`) — mutually exclusive with them |
| `step` | resolution in whole seconds (`60`, `60s`, `5m`, `1h`); auto-derived when omitted |

**Function set (issue #182 — Tempo v3.0.2 parity).** First-stage: `rate()`, `count_over_time()`, and `sum`/`min`/`max`/`avg`/`quantile`/`histogram` `_over_time` over the `duration` target. `quantile_over_time(duration, q, …)` returns one series per quantile (`p=<q>` label); `histogram_over_time(duration)` returns one cumulative-count series per fixed exponential `le` bucket (`__bucket=<le seconds>` label). Grouping: `by(resource.service.name)` returns one series per group value (the group label carries the value). Second stage: `topk(n)`/`bottomk(n)` reduce the series set per timestamp. Hints: `with(sample=…)` is accepted and returns the exact (superset) result; `with(exemplars=…)` attaches representative `trace:id` exemplars per bucket. `compare({selection})` partitions the outer spanset into a `selection` (the inner filter's matching spans) and a `baseline` (the complement) and emits per-attribute meta-series labelled `__meta_type` ∈ {`baseline`,`selection`,`baseline_total`,`selection_total`} plus one scoped attribute label (`key=value`, or `key=nil` for the complement/totals). A trailing metrics-result comparison (`… > 5`) post-filters samples above/below a threshold. Aggregation is executed entirely in ClickHouse (time-bucketed `GROUP BY`, a per-`(trace_id, span_id)` replay-dedup inner query, `quantilesTDigest`/conditional-count pushdown, the compare() attribute cross-tab — docs/schemas.md §4.2). *Numeric value/quantile/bucket-boundary parity vs Tempo is Tier-2 (issue #25); attribute value targets, attribute grouping keys, multi-key grouping, and grouped quantile/histogram route to follow-ups (a clean `400`). `compare()`'s **attribute-key universe is complete and Tempo-matched**: it enumerates every present attribute (span.\*/resource.\*/name/kind/status) plus the **fixed 25-key well-known-attribute set** Tempo v3.0.2 always emits (including well-known-but-absent keys as `key=nil`) — that set is derived clean-room from black-box container observation + the published OTLP semantic-convention docs, and matches Tempo's live key universe byte-for-byte (25/25). Tracked carve-out: five of those keys — `statusMessage`, `rootName`, `rootServiceName`, `instrumentation:name`, `instrumentation:version` — currently emit `key=nil` rather than their per-span value, because those values require trace-schema storage outside this (TraceQL-metrics) scope; tracked as **#189** (dependency #184 for the three trace-level intrinsics; instrumentation scope is a separate ingest capability). The label conventions match Tempo byte-for-byte; exact per-value counts are Tier-2 (#25).*

**Response body (Tempo-native, breaking change from earlier versions).** These endpoints are consumed only by the Tempo datasource and now emit the **Tempo-native metrics body**, replacing the earlier Prometheus matrix/vector envelope:

```json
{"series":[{"labels":[{"key":"__name__","value":{"stringValue":"rate"}}],
            "samples":[{"timestampMs":"1700000000000","value":0.88},{"timestampMs":"1700000060000"}],
            "exemplars":[{"labels":[{"key":"trace:id","value":{"stringValue":"abcd…"}}],
                         "value":0.88,"timestampMs":"1700000012345"}]}],
 "metrics":{"completedJobs":1,"totalJobs":1}}
```

Labels are OTLP protojson `AnyValue` (camelCase `stringValue`/`doubleValue`); `timestampMs` is a JSON **string** int64; a sample `value` is **omitted when zero** (protojson default omission); `exemplars` is present only under `with(exemplars=…)` and carries the trace reference as a `trace:id` label (not a top-level `traceId`). The instant `query` form carries one sample per series stamped at the snapped right edge `E`.

**`by()` series cap.** A grouped query runs a same-predicate distinct-by-key probe (`GROUP BY <by-keys> LIMIT cap+1`) before the main query; more than `reader.traceql_max_series` (default 1000) distinct series is a static **`422 query_too_broad`**, never a silent subset. Ungrouped queries skip the probe.

**Bucketing (normative):** buckets are epoch-aligned, **left-closed** intervals `[b, b + step)`. The evaluated window is snapped outward: `S = ⌊start/step⌋·step`, `E = ⌈end/step⌉·step` — an unaligned request over-includes by at most one step on each edge, and every bucket divides by the full step. Empty buckets are omitted (no gap-filling). The instant `query` form evaluates one bucket over the whole snapped window `[S, E)` — `rate` divides by `E − S` seconds — and stamps its single sample at `E`; on an empty window it returns no series (count/rate) or a single zero sample (aggregations).

**Step derivation and the point cap (committed contract):** when `step` is omitted, `step_s = max(1, ⌊(end_s − start_s) / DEFAULT_METRICS_POINTS⌋)` with `DEFAULT_METRICS_POINTS` = 100. The snapped bucket count `(E − S) / step_s` is capped at `MAX_METRICS_POINTS` = 11000: a range resolving more buckets is rejected **statically before execution** with `422 query_too_broad` — deliberately 422 (the bounded-response family), not Prometheus's 400, and never a silent truncation. Attribute-filter semi-joins carry throwing IN-set limits with the same 422 semantics (docs/schemas.md §4.2).

### 4.5 Service graph

```
GET /api/traces/v1/service_graph
```

Derives the service-graph edges (directed `client → server` call counts, error counts, and latency quantiles per connection type) over a time window, from the `trace_edges` half-row ledger populated at ingest (docs/schemas.md §4.1/§4.2). PulsusDB-native — there is **no** Tempo-compat alias (the interop reference has no service-graph HTTP endpoint; its panels read edge metrics as Prometheus series).

| Param | Notes |
|-------|-------|
| `start`, `end` | unix s / ns / RFC3339 (§1's trace-API forms, the same parser as §4.2/§4.4) |
| `since` | relative alternative to start/end (`1h`, `30m`) — mutually exclusive with them |

There is no `q` expression and no `step`: the read is a fixed `(client, server, connectionType)` aggregation over `[start, end)`.

**Response** (a bare object, not the `{status,data}` query envelope):

```json
{"edges":[{"client":"checkout","server":"payments","connectionType":"rpc",
           "calls":123,"failed":4,"p50Ns":1200000.0,"p95Ns":8400000.0,"p99Ns":21000000.0}],
 "truncated":false}
```

- `connectionType` is `"rpc"` (CLIENT→SERVER) or `"messaging"` (PRODUCER→CONSUMER) — the pairing is within-type, so cross-kind combinations never form an edge (docs/schemas.md §4.1).
- `calls`/`failed` are replay-deduped exact counts; `p50Ns`/`p95Ns`/`p99Ns` are TDigest latency quantiles in nanoseconds (`f64` — the SQL pins `CAST(... AS Array(Float64))`, no f32 on the wire), computed over the SERVER-side span durations.
- `edges` are ordered `calls` descending, then `client`/`server` ascending, and capped at `SERVICE_GRAPH_MAX_EDGES` = 1000 distinct edges; `truncated` is `true` iff more edges existed (never a silent subset).

**Window boundary (normative):** an edge is reported iff **both** its halves' own timestamps fall in `[start, end)` — a call whose client and server spans straddle the window edge (or a daily partition boundary) is attributed only when both contributing rows are in-window. Results are **merge-invariant**: identical before and after a background merge or `OPTIMIZE ... FINAL` (docs/schemas.md §4.2), and unchanged under byte-identical re-ingest.

**Errors:** a missing/invalid/inverted window, or `since` supplied together with `start`/`end`, is `400 bad_data`. A window too broad to bound within the reader scan budget is `422 query_too_broad` (the same bounded-response family as §4.2/§4.4). Errors are always the JSON envelope, never with `position`.

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
| `/loki/api/v1/index/volume` | `/api/logs/v1/volume` | M7 |
| `/loki/api/v1/detected_labels`, `/loki/api/v1/detected_fields` | `/api/logs/v1/detected_labels`, `/api/logs/v1/detected_fields` (pure prefix swaps, `GET|POST` like native) | M7 |
| `/loki/api/v1/patterns` | `/api/logs/v1/patterns` | M7 |
| `/api/traces/{traceId}`, `/api/traces/{traceId}/json`, `/tempo/api/traces/{traceId}` | `/api/traces/v1/trace/{traceId}`, `/api/traces/v1/trace/{traceId}/json` | M4 |
| `/api/search` | `/api/traces/v1/search` | M4 |
| `/api/search/tags`, `/api/search/tag/{tag}/values` | `/api/traces/v1/tags`, `/api/traces/v1/tag/{tag}/values` (Tempo v1 flat projection) | M4 |
| `/api/v2/search/tags`, `/api/v2/search/tag/{tag}/values` | `/api/traces/v1/tags`, `/api/traces/v1/tag/{tag}/values` (native shape minus `truncated`) | M4 |
| `/api/echo` | — (constant `echo` body) | M4 |
| `/api/metrics/query_range`, `/api/metrics/query`, `/tempo/api/metrics/query_range`, `/tempo/api/metrics/query` | `/api/traces/v1/metrics/query_range`, `/api/traces/v1/metrics/query` | M4 |
| `POST /querier.v1.QuerierService/{ProfileTypes,LabelNames,LabelValues,Series,SelectMergeStacktraces,SelectSeries,SelectMergeProfile,GetProfileStats,AnalyzeQuery}`, `POST /settings.v1.SettingsService/Get` (Connect-protocol, protobuf) | `/api/profiles/v1/*` | M5 |
| `/pyroscope/render`, `/pyroscope/render-diff` | `/api/profiles/v1/render{,-diff}` | M5 |
| `/loki/api/v1/rules[...]`, `/api/prom/rules[...]`, `/prometheus/api/v1/rules` | `/api/rules/v1/*` | M7 |

Routing note: the alias `GET /api/traces/{traceId}` coexists with native `/api/traces/v1/...`; the literal `v1` segment is matched first.

**M4 Tempo query aliases (all `GET`).** The trace-by-ID, search, and TraceQL-metrics aliases are pure route bindings onto the native handlers — responses are byte-identical to native, including §4.1's `Accept` negotiation on trace-by-ID (the `/json` alias binds the forcing handler and never negotiates). Deltas and reshapings:

- **Metrics envelope:** the `/api/metrics/*` aliases serve the native Prometheus matrix/vector envelope (§4.4), not Tempo's own metrics wire format — a documented, deliberate delta.
- **v1 flat tags:** `/api/search/tags` and `/api/search/tag/{tag}/values` serve Tempo's legacy v1 flat shapes — `{"tagNames":[...]}` (distinct keys, catalog order, deduplicated across scopes) and `{"tagValues":["a","b"]}` (bare strings). A server-side projection of the native scoped/typed §4.3 result: scope, value types, and `truncated` are dropped.
- **v2 tags:** `/api/v2/search/tags` and `/api/v2/search/tag/{tag}/values` serve the native scoped/typed shapes minus the PulsusDB-only top-level `truncated` field (Tempo's v2 wire shape has no equivalent — alias consumers lose the truncation signal; use the native routes to observe it).
- **Intrinsic scope:** not synthesized — `scope=intrinsic` is a `400 bad_data` on native and alias alike (§4.3), a delta from Tempo, which reports a static `intrinsic` scope. If intrinsic autocomplete proves load-bearing for real Grafana usage, the fix is adding intrinsic scope to the **native** v2 tags endpoint in a follow-up (the alias stays a pure projection of native) — never alias-side synthesis.
- **`/api/echo`:** `200` with the constant body `echo`.

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

**Zipkin v2 JSON receiver (M6, `POST /api/v2/spans`, `POST /tempo/spans`).** A foreign-format decoder + model adapter feeding the *native* trace-storage path — each Zipkin v2 JSON span is adapted into one self-contained OTLP `ResourceSpans` and handed to the same parser the native `POST /v1/traces` receiver uses, so a Zipkin-ingested span stores with `payload_type = 1` (OTLP) and is queryable via trace-by-ID (§4.1) and TraceQL search (§4.2) with no read-path difference. Both documented paths bind to the same handler. Mounts iff `PULSUS_COMPAT_ENDPOINTS=true` **and** the Writer subsystem is mounted (`Gate::CompatAndWriter`, the Loki push precedent below); it 404s wherever the writer subsystem does. **Scope is Zipkin v2 JSON only** (v1 JSON, protobuf, and thrift are deferred). The body is always decoded as a Zipkin v2 JSON span array — `Content-Type` is not a fork discriminator (v2 JSON is the sole supported encoding, so there is nothing to content-negotiate, unlike native OTLP which forks JSON vs protobuf on CT) — decompressed per `Content-Encoding` for gzip; the decompressed body is capped at 64 MiB (400). **Documented `Content-Type` divergence:** because the decode is unconditionally JSON, a well-formed JSON span array sent under `Content-Type: application/x-protobuf` is accepted (**202**) where the OpenZipkin oracle would answer 400. This is the sole divergence — no real Zipkin client emits it, and it lets the ratified conformance harness (which sends `application/x-protobuf` generically on ingest success paths) pass; a genuinely non-JSON body under any CT is a clean JSON-parse **400** (never a mis-parse or panic). Field mapping: `traceId` (16-hex 64-bit → left-padded to 16 bytes, 32-hex 128-bit verbatim — byte-identical to the same trace sent as OTLP) / `id` / `parentId` (absent → root); `name`; `kind` CLIENT/SERVER/PRODUCER/CONSUMER → OTLP `SpanKind` (missing → INTERNAL); `timestamp` + `duration` **microseconds** → nanoseconds; `localEndpoint.serviceName` → the `service` dimension (resource `service.name`), its `ipv4`/`ipv6`/`port` → resource `net.host.ip`/`net.host.port`; `remoteEndpoint` → span `net.peer.*`; `tags` → span attributes (verbatim); `annotations` (timestamp + value) → span events; `debug`/`shared` → span attributes `zipkin.debug`/`zipkin.shared`. **Shared spans:** a Zipkin shared span reports the same `(traceId, id)` from both RPC ends with different `kind` (SERVER vs CLIENT); both are stored and **both are returned by trace-by-ID** — the assembler de-duplicates on `(span_id, kind)`, so neither side is dropped (a genuine no-op for native OTLP, whose span ids are unique per trace). Success is an empty **202** Accepted (both sync and async `X-Pulsus-Async: 1` — the OpenZipkin oracle answers 202 regardless), matched against openzipkin/zipkin:3; a malformed span array, or any span with a non-hex/wrong-length id or an unrepresentable timestamp, is a whole-request **400** plain-text error (Zipkin has no partial-success channel — all-or-nothing, unlike the native OTLP receiver's per-span rejection), an unsupported `Content-Encoding` is **400**, and sink backpressure is **429** plain-text.

**Loki push receiver (M6, `POST /loki/api/v1/push`).** A foreign-format decoder feeding the *native* log-storage path — a pushed stream's labels flatten through the same canonical model (`LabelSet::from_normalized` → `stream_fingerprint`) an OTLP log does, so pushed logs are queryable via LogQL (§2) and appear in `/api/logs/v1/tail` with no read-path difference. Mounts iff `PULSUS_COMPAT_ENDPOINTS=true` **and** the Writer subsystem is mounted (the writer-side analog of the §8.1 Reader gating); it 404s wherever the writer subsystem does, and the compat flag alone never mounts it without the writer role. Both request encodings are accepted: `Content-Type: application/json` selects the JSON body (`{"streams":[{"stream":{…},"values":[["<unix_nano>","<line>"],…]}]}`, honoring `Content-Encoding` for gzip); anything else or an absent `Content-Type` selects the snappy-compressed protobuf body (`logproto.PushRequest`, pinned to grafana/loki 3.4.2), which is *always* block-snappy-decompressed regardless of `Content-Encoding` — the agent default, so uncompressed protobuf is unsupported, exactly as upstream Loki. Success is an empty **204** (both encodings; **202** for async `X-Pulsus-Async: 1`); a malformed body, label string, or timestamp is a whole-request **400** plain-text error (Loki has no partial-success channel — all-or-nothing), and sink backpressure is **429** plain-text. Response codes match grafana/loki 3.4.2 where it has an equivalent (204 success, 400 malformed/oversize); 202/async and 429/backpressure are PulsusDB-contract additions. The decompressed body is capped at 64 MiB (mapping to 400, like Loki's own over-limit rejection — the cap *size* differs from Loki's per-line/per-stream limits, a deliberate divergence). **Structured metadata** (per-entry labels — protobuf `EntryAdapter.structuredMetadata`, or a trailing third element in a JSON `values` entry) is **stored per-entry and surfaced in LogQL/tail** (issue #97). It is decoded into the `log_samples.structured_metadata` column (a canonical sorted-key JSON String, the same representation as `log_streams.labels`), bounded by a per-entry cardinality limit charged before the canonical JSON is built (over-limit is a whole-request 400). On the read path it fans into the response stream labels alongside the base labels — matching grafana/loki 3.4.2's default (`categorize_labels` off) — so an entry carrying distinct structured metadata forms its own result stream, and a `| key="value"` pipeline label filter selects on it. Structured metadata is per-entry: it never enters `stream_fingerprint` (a stream pushed with vs. without it fingerprints identically) nor the tail keyset cursor. Server-side structured-metadata filter pushdown is a deferred optimization (client-side filtering is the baseline, consistent with parsed-label filters).
