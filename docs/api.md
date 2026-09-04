# PulsusDB API Reference

PulsusDB exposes two API surfaces:

1. **The PulsusDB API** — the primary, always-on surface. Product-neutral paths under `/api/{logs,traces,profiles,rules}/v1/...`, the standard Prometheus HTTP API for metrics, and standard OTLP paths for ingestion. This is the API PulsusDB documents, versions, and guarantees.
2. **Compatibility endpoints** — optional aliases and foreign-protocol receivers matching third-party API surfaces (log/trace/profile datasources, legacy push protocols). Disabled by default; enabled with `PULSUS_COMPAT_ENDPOINTS=true`. They map onto the same handlers and add no new semantics.

**Ingestion model:** the OpenTelemetry Collector is the expected shipper for all signals — logs, metrics, traces, and profiles arrive via OTLP (metrics alternatively via the collector's Prometheus remote-write exporter). Foreign push protocols exist only behind the compatibility flag.

Conventions:

- Default listener: `0.0.0.0:3100`. All endpoints relative to that root.
- Timestamps: the log APIs read an integer `start`/`end`/`time` as unix **seconds** when the value is **ten characters or fewer** and as unix **nanoseconds** otherwise — the test is the length of the string, not the magnitude of the number, so `1786342706` is 2026 and `01786342706` is 1970 (§2). A value containing `.` is seconds with a fraction, rounded to milliseconds, at any length; anything else is RFC3339. Metrics APIs use RFC3339 or unix seconds; trace APIs accept unix seconds/nanoseconds/RFC3339 (magnitude-based there: `>= 10^12` is nanoseconds — a different rule from a different reference, deliberately not unified).
- Errors: the log query API (§2) and the trace query API (§4) return a bare `text/plain` body carrying the message and nothing else — but not the same container: §2 also sets `X-Content-Type-Options: nosniff` and §4 does not, because their references differ (see §2.3 and §4). The metrics API (§3) returns a `{"status":"error","errorType":...,"error":...}` JSON envelope, because upstream Prometheus does. `429` on ingest backpressure; `400` for malformed queries.
- Compression: requests may be `gzip`, `snappy`, or `zstd` (`Content-Encoding`); responses gzip when accepted.
- Regular expressions: RE2 in every query language — §9 documents the dialect and the measured differences from Loki/Prometheus/Tempo.
- Parity: PulsusDB answers as Loki, Prometheus and Tempo answer, **except where they are wrong**. The places where that exception applies — and why each one is a defect rather than a different choice — are listed in docs/reference-defects-we-do-not-copy.md; the full divergence record, defects and non-defects alike, is in the three ledgers under docs/benchmarks/.

## Request headers (all optional)

| Header | Applies to | Effect |
|--------|-----------|--------|
| `X-Pulsus-Database` | ingest + query | route to an alternate ClickHouse database (retention is per-database configuration; there is no per-write TTL override in v1) |
| `X-Pulsus-Async` | ingest | `1` = enqueue and return `202`; `0` = confirm flush (default from config) |
| `X-Pulsus-Explain` | query | `1` = include generated SQL, plan, and per-segment exactness (raw-exact vs tier-approximate) in the response envelope |
| `X-Loki-Response-Encoding-Flags` | log query (`query`, `query_range`, `tail`) | comma-separated encoding flags. `categorize-labels` switches the `streams` result to the three-element `values` shape — see §2.1. Unknown flags are echoed back and change nothing. Absent, the response is exactly what it was before the header existed |
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

- **Logs, traces, profiles.** Resource + scope attributes flatten into labels under the canonical label model ([architecture.md §2.3](architecture.md)): for logs, attribute keys are normalized to Prometheus-style names at ingest (`service.name` → `service_name`); trace attributes keep their OTel names verbatim and are queried as such in TraceQL. Log body → line; spans → trace tables with original protobuf retained as payload; profiles → pprof-equivalent tree precomputation.
- **Metrics follow Prometheus v3.13.0's OTLP receiver, not the log convention** (issue #461). `POST /v1/metrics` rewrites what it stores, and the rewrite is the reference's, value for value:
  - the **metric name** is escaped to `[a-zA-Z0-9:_]` and gains its unit and type suffixes — `app.checkout.request.count` (unit `1`, monotonic Sum) is stored as `app_checkout_request_count_total`, `http.server.duration` (unit `s`) as `http_server_duration_seconds`, `cpu.utilization` (unit `1`, Gauge) as `cpu_utilization_ratio`;
  - **attribute keys** are escaped to `[a-zA-Z0-9_]`, with the reference's `key`/`key_` prefix where the result would start with `_` or a digit (`9lives` → `key_9lives`, `_priv` → `key_priv`), and two keys that escape alike merge into one label whose values are joined with `;` in raw-key byte order;
  - an attribute with an **empty value** stores nothing;
  - **resource attributes are not promoted onto the metric series.** `service.namespace`/`service.name` become `job`, `service.instance.id` becomes `instance`, and everything else becomes a `target_info` series carrying those same identifiers — so `service.name` does **not** appear as a `service_name` label here, which is the one place the metrics path departs from the log convention. The override is **conditional**, and the condition is on the composite: `job` is `<service.namespace>/<service.name>` whenever the **namespace key is present**, whatever either value holds, and the label is set only when that composite is non-empty. So `{name: "", namespace: "ns"}` stores `job="ns/"`, `{name: "svc", namespace: ""}` stores `job="/svc"`, and two empty values still store `job="/"` — all three override a caller's own `job`. Only when `service.name` is absent altogether, or present with no namespace key and an empty value, does the derivation yield nothing and a caller's own `job`/`instance` attribute survive intact — and a caller-supplied `job` on the resource is itself enough to make that resource eligible for a `target_info` series;
  - the **instrumentation scope** is promoted to `otel_scope_*` labels only when `PULSUS_OTLP_PROMOTE_SCOPE_METADATA` is on (off by default, as in the reference).

  The naming strategy is `PULSUS_OTLP_TRANSLATION_STRATEGY`, which takes the reference's own four values at the reference's own default ([configuration.md §5](configuration.md)). `job`/`instance` synthesis and `target_info` are **not** gated by it and happen under every value. Every deliberate difference from the reference on this route is recorded in [benchmarks/metrics-differential-ledger.md](benchmarks/metrics-differential-ledger.md).

- **Which faults are whole-request, and which are per-point.** A fault in the request's **shape or naming** is whole-request: `400` with `google.rpc.Status.code = 3`, and nothing from the request is stored. A fault in **one data point's data** is per-point: the response is `200` carrying `partial_success.rejected_data_points` and `error_message`, and every other data point in the request is stored.

  The two are not the same kind of thing. A naming fault changes **series identity** — there is nothing to partially accept, because the series the request describes is not a series we can name. A bad data point is genuinely per-point: the rest of the request is well-formed and independently valid. Collapsing them into one model would mean either discarding good data because one point was bad, or accepting a request whose series identity is wrong. Prometheus v3.13.0 has one model rather than two — it rolls the whole request back on any error — so both halves are ledgered, as `otlp-request-atomic-faults` and `otlp-delta-partial-success`.
- Responses: `200` with OTLP partial-success message when applicable; `429` on backpressure. On `/v1/logs` only, a request carrying **no log records at all** is `422` rather than an empty `200` — the log receivers' stream-less-push rule, §8.2 — unless that same body also nests an `AnyValue` past the depth cap, which is charged during decode and answers `400` first (§8.2).
- A metric data point whose `time_unix_nano` resolves to a UTC day outside the supported storage time range (before 1970-01-01 or after 2106-02-06) is rejected per-point as OTLP partial success, matching the Zipkin unrepresentable-timestamp precedent (§8.2) — a day past 2106-02-06 would wrap in the 32-bit `DateTime` domain the `metric_samples`/`metric_hist_samples` delete-TTL evaluates in (and a day past 2149-06-06 additionally falls outside the `Date` domain the tables partition on), so such a point cannot be stored safely (docs/schemas.md §2.1, issue #137).
- The `/v1development/profiles` path tracks the OTLP spec's experimental profiles signal and will follow it to `/v1/profiles` on stabilization (the old path remains as an alias).

### 1.2 Prometheus remote write

```
POST /api/v1/write
Content-Type: application/x-protobuf, Content-Encoding: snappy
```

`prompb.WriteRequest`. Supported as a first-class alternative for metrics because the OTel Collector's `prometheusremotewrite` exporter is a common metrics pipeline. `__name__` becomes `metric_name`; remaining labels are fingerprinted (xxhash64, sorted `k\xffv\xff` serialization). Stale markers (NaN `0x7FF0000000000002`) stored verbatim. A sample whose timestamp resolves to a UTC day outside the supported storage time range (before 1970-01-01 or after 2106-02-06, same cutoff and 32-bit-`DateTime` delete-TTL rationale as OTLP metrics above; docs/schemas.md §2.1, issue #137) is dropped and counted in `rejected_total`, not surfaced in the response body. Success: `204`.

**Measured `Content-Type` difference (issue #385, accepted as-is).** The reference answers **415** `expected application/x-protobuf as the first (media) part, got application/json` when a request's `Content-Type` is not protobuf (measured on `prom/prometheus:v3.13.0`); PulsusDB does not consult `Content-Type` on this endpoint at all and answers **400** once the unconditional snappy decode fails. Prometheus's own sender cannot reach that case — it always sets `Content-Type: application/x-protobuf` (`storage/remote/client.go:274` @ v3.13.0), captured on the wire on every request of a live send run. Other remote-write senders (the OTel Collector's `prometheusremotewrite` exporter, Grafana Alloy, vmagent) were not checked, so this is **not** a claim that no sender emits it.

<!-- copied-rule:rw-backpressure:start -->
Sink backpressure answers **`429`** with a plain-text body. The reference has no
`429` on this endpoint at all — Prometheus's remote-write receiver does not rate-limit,
and every error it writes goes through one `http.Error` call
(`exp/api/remote/remote_api.go:611` @ client_golang/exp `3537b20ac86b`, the module
`prometheus` v3.13.0 pins at `go.mod:67`). A **default-configured** Prometheus sender
treats `429` as non-recoverable and **drops** the batch, where it would retry a `5xx`
indefinitely (`storage/remote/client.go:321-324` @ v3.13.0; `retry_on_http_429` is
absent from `DefaultQueueConfig` and so defaults to false). That data loss is
deliberate and is what backpressure asks for: shedding load is the designed response
to a rate-limit signal, and answering `5xx` instead would tell the sender we are broken
and make it retry into a queue we are already failing to drain. Senders that prefer to
retry set `retry_on_http_429: true`, which flips exactly this branch. `Retry-After` is
never sent, matching the reference, which never sets it either.
<!-- copied-rule:rw-backpressure:end -->

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

**A present-but-empty scalar parameter is an absent one** (issue #391).
`?limit=` is answered exactly as `limit` omitted altogether — same status,
byte-identical body — and the same holds for every scalar parameter in
§2.1-§2.6: `query`, `start`, `end`, `time`, `step`, `limit`, `direction`,
`delay_for`, `aggregateBy`, `targetLabels`, `line_limit`, `field_limit`.
This is the reference's rule and not a PulsusDB one: it reads scalar
parameters through Go's `r.Form.Get`, which returns `""` for an absent key
and for an empty one alike and so cannot tell them apart, and every parse
helper behind it defaults on `""` (`parseInt`, `parseTimestamp`,
`parseDirection` — `pkg/loghttp/params.go:152-159` @ grafana/loki v3.7.4 =
`b318f2829f0ae2094ab3a1e90780450e9e4b03be`). Three details of the rule,
each container-measured against `grafana/loki:3.7.4` rather than inferred:

- **Only the literal empty string counts.** `?limit=` collapses, and so
  does a bare `?limit` with no `=` (which decodes to the empty string).
  `?limit=%20`, `?limit=+` (a space), `?limit=%09` and `?limit=%00` are
  **values**, and are rejected `400` as the malformed values they are.
- **Duplicate keys are first-wins, and the collapse applies afterwards.**
  `?limit=&limit=5` uses the *first* occurrence, finds it empty and falls
  back to the default — it does not skip ahead to `5`. `?limit=5&limit=`
  uses `5`.
- **A required parameter is still required.** An empty `query=` is a `400`
  identical to omitting `query` (§2.1-§2.2, §2.5, §2.6), because absent is
  what empty means here.

The one exception is `match` / `match[]`, the only **repeated** parameters
on this surface — see §2.3.

**Timestamps: ten characters or fewer means unix seconds** (issue #406).
`?start=1786341082&end=1786341542` — the form a Prometheus-style client
sends by default — is a 460-second window, not a 460-nanosecond one.
`parseTimestamp` (`pkg/loghttp/params.go:161-186` @ grafana/loki v3.7.4
`b318f2829f0ae2094ab3a1e90780450e9e4b03be`) switches on `len(value) <= 10`,
which is the length of the **string**: `01786342706` is eleven characters
because of the leading zero and is read as nanoseconds, landing in 1970.
A value containing `.` is seconds with a fraction whatever its length, and
the fraction is rounded to **three decimal places** before it becomes
nanoseconds (`1786342706.123456` is `…26.123`). Anything else is RFC3339.
One deviation: PulsusDB's whole timestamp domain is `i64` nanoseconds, so
it ends at **2262-04-11T23:47:16.854775807Z** (and begins at
1677-09-21T00:12:43.145224192Z). A seconds value past that — `9999999999`,
which the reference reads as the year 2286 — is a `400`, exactly as the
RFC3339 spelling of that same instant always was. The largest accepted
ten-character seconds value is `9223372036`. Registered as
`logs-timestamp-i64-nanosecond-domain` in
`docs/benchmarks/logs-differential-ledger.md`.

**`since` sets the default `start`** (issue #406). Read on every route in
§2.1 and §2.3-§2.6 — everything carrying a `start`/`end` pair, i.e. all but
§2.2's instant `/query` — as a duration literal defaulting to `1h`, and
used **only when `start` is absent**. An unparseable value is a `400`, not
silently ignored. The default is `min(end, now) - since`, not
`end - since`: a future `end` does not drag the default `start` into the
future with it (the reference's `endOrNow`, `params.go:105-111`).

**`end` before `start` is a `400`** on `/query_range`, `/labels`,
`/label/{name}/values`, `/series`, `/stats`, `/volume`, `/detected_labels`
and `/detected_fields` — `invalid time range: 'end' precedes 'start'` —
where it used to be a silently empty `200`. The reference deliberately
does **not** apply the check on `/query` (an instant query has no range)
or on `/patterns`, and neither do we — both exemptions verified against a
reference whose `/patterns` can actually answer (it needs
`pattern_ingester.enabled: true`, or the route 404s and nothing about it
can be measured). `end == start` is served everywhere: every one of the
reference's call sites spells `End.Before(Start)`, despite its message's
"or equal".

**A `POST` reads the URL query alongside the body.** `POST
/query_range?limit=5` with a body carrying no `limit` serves 5 entries, not
the default 100. Go's `ParseForm` copies the parsed body into `r.Form` and
then appends the URL query per key, so on a collision the **body wins** for
a scalar parameter (`?limit=7` + body `limit=5` serves 5) and a **repeated**
parameter takes **both, concatenated** (`?match[]={app="b"}` + body
`match[]={app="a"}` returns both series). An empty body value is still the
body's value and still collapses to the default — `?limit=5` + body
`limit=` serves 100, not 5.

**A `POST`'s `Content-Type` decides whether its BODY is read, not whether
the request is served.** The body is parsed as form pairs only under
`application/x-www-form-urlencoded` (case-insensitively, parameters such as
`; charset=UTF-8` allowed). Under **any other** well-formed media type —
`application/json`, `text/plain`, `multipart/form-data`, a bare token like
`garbage`, or **no `Content-Type` header at all** — the body is not read,
and the request is answered from its URL query alone. So a POST carrying
every parameter in its URL is served whatever the client labels the body,
which matters because plenty of HTTP clients send `application/json` by
default. What *is* rejected `400` is a `Content-Type` that cannot be
**parsed**: an empty subtype (`application/`), a missing type (`;`,
`/json`), trailing content after the subtype (`application/json/x`), or a
malformed parameter (`application/json; charset`, or a repeated parameter
whose values disagree — `; a=1; a=2`; a repeat whose values *agree* is
allowed, as is one trailing `;`). This is the reference's rule throughout,
in its own `mime.ParseMediaType`/`ParseForm` terms; the rejection message
prose is PulsusDB's.

**"Not read" means the upload is never awaited.** The `Content-Type` is
examined before any of the body is consumed, so a POST that is answerable
from its URL is answered while the client may still be sending — a client
that advertises a large body it did not need to send does not pay for the
transfer. The one place PulsusDB stops short of the reference here is a
form `Content-Type` with a **malformed parameter**
(`application/x-www-form-urlencoded; bogus`): the reference reads and
parses the whole body before returning the error it had already decided on,
where PulsusDB returns it immediately. Same `400`, same body, less
transfer. A body-size limit still applies on the branch that *does* read
the body: over roughly 2 MiB the request is rejected `413` (the reference's
own form cap is 10 MiB and it answers `400` — a difference in a limit, not
in the parameter surface).

### 2.1 `GET|POST /api/logs/v1/query_range`

| Param | Type | Notes |
|-------|------|-------|
| `query` | LogQL | required |
| `start`, `end` | unix s (<= 10 chars) / ns / fractional s / RFC3339 (§2 preamble) | default: `end = now`, `start = min(end, now) - since`; `since` defaults to `1h`. `end < start` is `400` |
| `step` | duration \| int (seconds) | metric queries only; when omitted, derived as **whole seconds**: `max(floor((end-start) in seconds / 250), 1)` — the reference's `defaultQueryRangeStep` (`pkg/loghttp/params.go:140-142 @ grafana/loki v3.7.4 b318f282`). A 1 h window derives `14s`, a 900 s window `3s`, a 499 s window `1s`. The point count therefore varies with the window instead of being fixed, and is **at most 500** — the maximum, at a 499 s window. There is no lower bound worth quoting: any window under 250 s derives `1s`, so a 10 s window is 11 points and a zero-length one is a single point |
| `limit` | int | max **total** entries returned across the response, ordered by `direction` (newest-first for `backward`); global, not per-stream (default 100, hard cap 5000 — values above the cap are rejected with `400`). This is the **entry** axis; §2.6.3's same-named `limit` counts field *names* and carries no ceiling |
| `direction` | `forward`\|`backward` | default `backward` |
| `since` | duration | the default `start`'s lookback (default `1h`); ignored when `start` is present |

`POST` accepts the same param names as an `application/x-www-form-urlencoded` body (large queries/long ranges can exceed URL length limits; mainstream Loki-datasource clients POST this endpoint).

`limit` bounds the total number of log entries in the response (global), consistent with the reference log-API semantic; it is not applied per stream.

Response: `{"status":"success","data":{"resultType":"streams"|"matrix","result":[...],"stats":{...}}}` — log selector queries return `streams`, metric queries return `matrix`. Streams are sorted by label set for a deterministic response.

- **streams**: `result: [{"stream":{k:v,...},"values":[["<ts_ns>", "<line>"],...]}, ...]`. `ts_ns` is a **string** (nanosecond precision overflows JS's safe-integer range). `stats: {"streams":N,"entries":N,"bytes":N}` (`bytes` = decoded line bytes). A pipeline with an in-engine dropping stage (a label filter, or a line filter after `line_format`) is served by fetch-until-limit keyset paging that fills exactly to `limit`; when the byte scan budget (`reader.logql_scan_budget_bytes`) is exhausted before the limit fills, the response returns the survivors gathered so far and adds `stats.pulsus_partial: true` — a PulsusDB-contract signal (Loki has no byte-budget-truncation equivalent; the traces-search route signals its own truncation differently, on `metrics.completedJobs < metrics.totalJobs` in §4.2, because `tempopb.SearchMetrics` has no partiality field to mirror) distinguishing a budget-truncated result from a complete one. The field is **omitted** on complete results (the fast path, the non-dropping path, and genuine window exhaustion), so ordinary responses are byte-identical to before; clients that don't know the key ignore it. **Result-byte bound (issue #312):** a streams query whose peak retention — staged rows plus the assembled result — would exceed `MAX_STREAMS_RESULT_BYTES` (1 GiB) is refused `422 query_too_broad`, never truncated and never returned as a partial; `stats.bytes` on any served response is therefore `<=` that cap. The cap is denominated in retained bytes, so the reachable wire ceiling is about half of it (~512 MiB of line bytes), and results whose label sets are wide are charged up to 3x their true footprint — see `docs/benchmarks/logs-differential-ledger.md` entry `streams-result-budget` for the derivation and for what the reference does at its own (much smaller) ceiling. **Template render budget (issues #230, #294):** everything one row's `line_format`/`label_format` renders is charged against `MAX_TEMPLATE_RENDER_BYTES` (64 MiB per row) before it is allocated, and that includes the text of a per-line template ERROR **whose size a caller can grow** — one that embeds an argument, or part of one, into the retained `__error_details__`. `duration`/`duration_seconds`/`unixToTime` charge the exact rendered length; `bytes` charges a bound, because which of its three failure texts fires is decided by the parse. A breach is the same `422 query_too_broad`, never a truncation. **Error texts of bounded constant size are deliberately not charged** — `{{ repeat -1 "ab" }}` allocates 30 bytes for `strings: negative Repeat count` against nothing, and no caller input makes it larger; the budget bounds what a query can grow, not every allocation. Measured, the reference stops serving these long before we refuse them (`500 String too long to encode as label.` from a 16,777,216-byte label value); the ledger entry `template-output-budget` carries the boundary table and the two remaining U+FFFD divergences in template error text.
- **`categorize-labels` (issue #463):** when the request carries `X-Loki-Response-Encoding-Flags: categorize-labels`, the `streams` result changes shape: `data.encodingFlags` is emitted **before** `data.result` carrying the flags the request sent, and every `values` entry gains a **third** element — `[["<ts_ns>", "<line>", {"structuredMetadata":{...},"parsed":{...}}], ...]`. `structuredMetadata` holds the entry's per-entry metadata, `parsed` holds what the pipeline extracted (including `__error__`/`__error_details__`, which are always parsed however they arrived); each key is omitted when its object is empty, and an entry with neither renders `{}`. The `stream` object then carries the **indexed stream labels only**, so entries that differ only in metadata collapse back into one stream object — `stats.streams`/`entries`/`bytes` follow that grouping. The switch is **all-or-nothing**: a body advertising the flag has three elements on every entry, and a body that does not has two on every entry, because the client's decoder dispatches on the advertisement. If any stream cannot serve a third element the whole response downgrades to the two-element shape and the flag is dropped from the echo — a feature loss, never a body a client cannot parse. Unknown flags are echoed verbatim in first-occurrence request order and change nothing; the reference echoes the same tokens in an unstable order, recorded as `encoding-flags-echo-order`. Only a `streams` result carries the key: a `matrix` or `vector` response never does, whatever the request headed. The tail frame (§2.4) carries the same third element with the advertisement **last**, which is where the reference puts it there.
- **matrix**: `result: [{"metric":{k:v,...},"values":[[<unix_seconds>, "<value>"],...]}, ...]`. Timestamps are Prometheus-style unix-seconds numbers at **millisecond** resolution — the reference store's own matrix resolution, so a step point carrying sub-millisecond nanoseconds (a range query's grid is anchored on the request `start`, issue #227) is floored to the millisecond on both stores; `value` is a quoted string (`"NaN"`/`"+Inf"`/`"-Inf"` as applicable, matching §3.1's convention). `stats: {"series":N}`.
- **Range-query window semantics (issue #227):** a metric range query re-evaluates the `[range]` selector at every point of the **start-anchored** grid `{start + k·step ≤ end}`, over the **half-open** window `(t − range, t]` — the reference store's sliding evaluation, not fixed step-aligned buckets. Windows **overlap** when `range > step` (one entry contributes to several points), an **empty window emits no point** (a gap, never a zero), and `rate`/`bytes_rate` divide by the **`[range]`** seconds, so `rate({…}[1m])` and `rate({…}[10m])` differ. Instant queries are unaffected (one window `(time − range, time]`). The grid stays anchored on the request `start` — an **accepted divergence** from the reference, recorded below.
- With `X-Pulsus-Explain: 1`, `data.explain = {"result_type","routing":{"chosen":"rollup"|"raw","reason":"..."}|null,"stages":[{"name","sql","note"|null},...]}` is added alongside `data.stats`. Since issue #492 the object may carry one **additive** fourth key, `plan` — the compiled plan's shape. It is **omitted entirely when absent**, which it is on every path today (no read path compiles a plan yet), so every explain response is byte-identical to one from before the key existed, and the three keys above have not moved.

**The `plan` key's complete shape** (issue #492): an ordered list of `parts` — each part either one SQL statement or work in our own process, with the value set crossing between two parts named, typed and bounded — plus one `links` entry per chain link, so every link in the user's pipeline can be traced to the part that runs it. `issue` is `once`, `per_seed:chunks` or `per_seed:keyset`; `yields` is `candidates`, `exact` or `reduced`; `cut` says why a part is its own statement and is `null` only on a part that opens the plan. The example below carries **every key the renderer can emit** — a real response carries only the keys its own plan has:

```json
{
  "parts": [
    {"kind": "sql", "name": "log_streams_idx", "issue": "once", "cut": null,
     "seed": null, "yields": "exact"},
    {"kind": "sql", "name": "log_samples", "issue": "per_seed:keyset",
     "cut": {"why": "source_handoff", "source": "log_samples", "key": "fingerprint"},
     "seed": {"from": 0,
              "bound": {"kind": "constant", "name": "DEFAULT_MAX_STREAMS", "value": 100000}},
     "yields": "candidates"},
    {"kind": "sql", "name": "trace_attrs_idx", "issue": "per_seed:chunks",
     "cut": {"why": "handoff_exceeds_bound",
             "cost": {"text_bytes": 1409081, "ast_elements": 65540}},
     "seed": {"from": 0, "bound": {"kind": "request_limit", "value": 20}},
     "yields": "candidates"},
    {"kind": "sql", "name": "trace_spans", "issue": "once",
     "cut": {"why": "disjoint_sources", "sources": ["trace_spans", "trace_attrs_idx"]},
     "seed": null, "yields": "reduced"},
    {"kind": "engine", "links": [2, 3]}
  ],
  "links": [
    {"i": 0, "part": 0, "stage": "Source", "how": "lowered", "fidelity": "equivalent"},
    {"i": 1, "part": 0, "stage": "LineFilter", "how": "lowered", "fidelity": "wider"},
    {"i": 2, "part": 4, "stage": "Parser(Json)", "how": "residual", "why": "not_yet_lowered"},
    {"i": 3, "part": 4, "stage": "Emit", "how": "residual", "why": "response_build"}
  ]
}
```

A part whose `cut` is `{"why": "inexact_limit"}` carries neither `source`/`key`, `sources` nor `cost`: the request's `LIMIT` could not enter the statement, so the same statement is issued once per page.
- **`warnings` (issue #277):** an array of strings added as the **last top-level key**, a sibling of `data` and after it — `{"status":"success","data":{...},"warnings":["..."]}`. Note the asymmetry with `explain`, which lives *inside* `data`. The key is **omitted entirely when there are no warnings**, so every response that carries none is byte-identical to one from before the field existed, and a client that ignores the key sees a normal success with fewer series rather than an error. Messages are deduplicated and rendered in byte-lexicographic order, so `…variant (10)` precedes `…variant (2)`. PulsusDB emits exactly one message today: `maximum of series (<cap>) reached for variant (<index>)`, when a `variants(...)` query's variant breaches the result-series cap — see §2.2's note. The reference's two other warning families are deliberately not emitted and are recorded, with what was measured, in `docs/benchmarks/logs-differential-ledger.md` entry `(d)`.

**Accepted divergence — the range grid is anchored on `start`, not on the
step (issue #425 Part A, owner ruling 2026-08-12, closed with no code
change; supersedes nothing — it upholds the earlier #301 ruling).**
PulsusDB emits points at `{start + k·step ≤ end}`, so **every point lies
inside the window the caller asked for**. The reference rewrites the
request first: `metricQuerySplitter.split` calls
`alignStartEnd(step, start, end)` on every range metric request before
its engine sees it, flooring `start` and ceiling `end` to absolute
multiples of `step` (`pkg/querier/queryrange/splitters.go:236 @
grafana/loki v3.7.4 b318f282`). The measurement — the window, the two
point counts and the two last-point offsets — is recorded once, in
`docs/benchmarks/logs-differential-ledger.md`'s
`range-step-grid-start-anchored`, and deliberately not restated here.
It is **accepted rather than fixed because the alignment is an artefact
of query splitting, not a documented contract**: the reference splits a
range query into hour chunks to run them in parallel, the chunks must
line up on a grid, and rather than resampling back onto the caller's
timestamps it hands the chunk boundaries out. It is switched off entirely
by one tenant limit (`split_queries_by_interval: 0`) — the same binary
gives two different answers to the same request — and it contradicts the
reference's own engine (`pkg/logql`, start-anchored, which is the
semantics issue #227 ported). Returning data outside the requested
`[start, end]` is the reference being wrong, and the standing rule is to
match it except where it is wrong. The migration argument (a dashboard
moved between the two stores shifts) is about existing deployments and
does not apply to new ones. Ledger:
`range-step-grid-start-anchored` and `frontend-step-alignment` in
docs/benchmarks/logs-differential-ledger.md. Pinned by
`crates/pulsus-read/tests/logqltest/corpus/b9_range_sliding.test:48`,
which fails if an aligned grid is ever reintroduced. Reopen only if the
owner reverses the ruling.

**Metric binary operations & vector matching (issue #91).** LogQL metric expressions support binary operations between range vectors — arithmetic, comparison (with `bool`), and the `and`/`or`/`unless` set operators — with the full `on(...)`/`ignoring(...)` and `group_left(...)`/`group_right(...)` vector-matching modifiers (semantics oracle-verified against `grafana/loki:3.4.2`). One-to-one matches output the reduced (`on`/`ignoring`) signature; `group_left`/`group_right` pass the many side's labels through whole and copy the include labels from the one side. A cardinality violation is a `400` carrying the reference store's exact message (`multiple matches for labels: many-to-one matching must be explicit …` / `… many-to-many matching not allowed: matching labels must be unique on one side`); a bare `group_left`/`group_right` with no preceding `on`/`ignoring` is a parse-time `400`. Matrix (range) binops apply the vector match independently per step. Note: the reference store returns HTTP `500` for these runtime matching errors while PulsusDB returns `400` (the semantically correct bad-request code); the error bodies agree. Streams-path error series carry both `__error__` and its human-readable `__error_details__` companion (issue #99), byte-exact against the reference where feasible; the metric pipeline-error path is unchanged.

### 2.2 `GET|POST /api/logs/v1/query`

Instant evaluation at `time` (unix s / ns / fractional s / RFC3339 per §2's preamble, default now). `since` is **not** read here and `end < start` cannot apply — an instant query has no range, and both are the reference's own exemptions. Returns `vector` (`result: [{"metric":{...},"value":[<unix_seconds>, "<value>"]}, ...]`) or `streams`, plus `stats`/`explain`/`warnings` per §2.1's shapes. `POST` accepts the same param names as an `application/x-www-form-urlencoded` body (same rationale as `query_range`).

**`variants(...)` and the result-series cap (issue #277).** The 500-series result cap is applied **per variant** for a query whose *root* is `variants(...)`, never to the concatenation — so a three-variant query returning 400 series each is served with 1 200 result series. A variant that breaches the cap is **skipped**: it is removed from the result entirely (never truncated), the remaining variants are served, the status is **`200`**, and the response carries `"warnings":["maximum of series (500) reached for variant (<index>)"]`, one message per skipped variant. `<index>` is the variant's `__variant__` label value, a plain decimal.

The cap is per-variant only at the root. Any other root — including `variants(...) + 1` — takes the ordinary whole-result cap and is rejected with `maximum number of series (500) reached for a single query; …` (note the different wording; the reference has two distinct messages here and so do we). PulsusDB applies `> 500` uniformly at instant and at range, which serves one case the reference drops; the divergence and its cause are recorded as entry `(d)` in `docs/benchmarks/logs-differential-ledger.md`.

**`sort`/`sort_desc` ordering.** A `sort`/`sort_desc` sets the wire order of the `vector` result — ascending/descending by value, with equal values ordered by label set ascending — **whenever its order reaches the root through order-preserving wrappers**: `label_replace`, a scalar binary operand (`sort(…) * 1`, `1 * sort(…)`), the many side of a vector binary operand (the left side normally, the right side under `group_right`), `and`/`unless`, and `or` when both sides are sorted (issue #406). Everywhere else the response is sorted by label set — including under a vector aggregation such as `sum by (…) (sort(…))` or `topk(k, sort(…))`, and on the non-many side of a vector binop, where the reference's own answer is an unordered map walk that varies between runs. That deliberate difference is registered as `nested-sort-order` in docs/benchmarks/logs-differential-ledger.md.

The tie rule is a PulsusDB determinism pin, not a mirror of the reference, which orders on the value alone through an unstable sort and so specifies no order among equal-valued samples. Registered as `sort-tie-order` in the same ledger. A client must key on the value and the label set, never on a sample's position within a run of equal values.

### 2.3 Labels & series

```
GET|POST /api/logs/v1/labels                 ?query=<selector>&start=&end=&since=
GET|POST /api/logs/v1/label/{name}/values    ?query=<selector>&start=&end=&since=
GET|POST /api/logs/v1/series                 ?match[]=<selector>&start=&end=&since=
```

`start`/`end`/`since` default the same way as §2.1, and `end < start` is a `400` on all three. POST accepts the same params as an `application/x-www-form-urlencoded` body (`match[]` repeated for `/series`), and — per §2's preamble — also reads the URL query alongside it. `match[]` selectors are bare LogQL stream selectors (e.g. `{service_name="checkout"}`).

**`match[]` is optional, and so is `match`.** The reference reads both keys, unions them, sorts and dedupes (`ParseSeriesQuery`, `pkg/loghttp/series.go:23-38` @ grafana/loki v3.7.4 `b318f2829f0ae2094ab3a1e90780450e9e4b03be`), and an empty group set is legal: `MatchForSeriesRequest(nil)` returns no error (`pkg/logql/matchers.go:13-26`). So, matching it (issue #406):

- **No `match[]` at all is a `200`** listing every series active in the window — the discovery call, and the first thing a new integration tries. It is bounded by the window and by `max_streams` (100,000), with no result-count cap, and it costs two statements rather than the matched path's three (the rollup activity scan, then stream hydration — `log_samples` is never touched).
- **A lone `?match[]={}`** — or `{ }`, after stripping ASCII spaces — means the same thing. The collapse applies only when the deduped set has **exactly one** element: `?match[]={}&match[]={app="a"}` is a `400` on both stores.
- **`?match={app="a"}`** (Prometheus's unbracketed spelling) is read as well, and unions with `match[]`.

**An empty `match[]` is the exception to §2's present-but-empty rule.** These are the only *repeated* parameters here, and the reference reads repeated parameters through `r.Form[...]` rather than `r.Form.Get` (`pkg/loghttp/series.go:23-25`), which keeps `""` as a value instead of collapsing it. So `?match[]=` is an empty **selector** and a `400` parse error — not an absent `match[]` — on both stores, measured 2026-08-09.

Responses: `{"status":"success","data":[...]}` — `labels`/`label/{name}/values` return an array of strings, `series` returns an array of label maps (sorted for a deterministic response). With `X-Pulsus-Explain: 1`, `explain` (the §2.1 shape, `routing` always `null`) is added as a **top-level sibling of `data`** (not nested under it — these responses' `data` is an array, not an object).

`/label/{name}/values` accepts `GET|POST` from issue #406 Part B2 (the reference registers it `Methods("GET","POST")`, `pkg/loki/modules.go:687` @ v3.7.4 `b318f282`).

**`query=` narrows `/labels` and `/label/{name}/values` (issue #482).** It is **optional and matchers only**, with exactly the acceptance and the error text §2.6.2's `/detected_labels` has — the three share one parse seam, so they cannot drift apart:

| `query` | Behaviour |
|---------|-----------|
| absent, or present but empty | the unscoped form — every label/value of every stream active in the window, byte-identical to what these two endpoints answered before |
| a stream selector | stage-1 resolution first, then the same index scan restricted to the matched fingerprints. `/labels` returns the union of those streams' label names; `/label/{name}/values` returns `name`'s distinct values on those streams — a key present in the store but absent from every matched stream yields `[]` |
| a selector matching no stream | `{"status":"success","data":[]}`, answered without a second statement |
| a pipeline stage, or a metric expression, or unparseable text | `400`, the bare `text/plain` parse error of §2.3's error table |

The scoped form costs **one extra round trip** (stage-1 resolution) and reads strictly less at stage 2: the fingerprint list is pushed *inside* the activity semi-join, which turns its whole-bucket-range scan into primary-key point ranges. The stage-1 read is capped at `reader.logql_max_streams` like every other stream resolution. The unscoped path issues exactly what it issued before and pays nothing new.

The accept surface moved with it: `?query=<garbage>` on these two routes was a silent `200` with the full label set and is now a `400`.

**All three answer the requested window, not the month containing it (issue #399).** `log_streams_idx` carries no time column, so `[start, end]` is applied as a semi-join against the log rollup (`log_metrics_<res>`) — `fingerprint IN (SELECT DISTINCT fingerprint FROM log_metrics_5s WHERE bucket_ns >= … AND bucket_ns <= …)` — alongside the month predicate, which stays where it is as the partition-pruning bound. A stream with no log line in the window is absent from all three answers. `/series` applies it as its own statement after the `max_streams` cap, so the cap keeps counting the pre-window union.

The lower bound is the rollup **bucket containing** `start`, not `start` itself (the rollup stores `bucket_ns = intDiv(timestamp_ns, res) * res`), so the filter can over-include by at most one rollup resolution — 5s by default — at each edge, and can never drop a stream with a line in the window. The reference is looser here, not tighter: its `/labels` and `/label/{name}/values` ignore `from`/`through` entirely and are bounded only by which index files overlap the window, while its `/series` is chunk-granular. Registered as `detected-labels-window-scoped-to-rollup-bucket` in `docs/benchmarks/logs-differential-ledger.md`.

Because the rollup read is window-proportional, a very broad **unscoped** window on `/labels` or `/label/{name}/values` can exhaust `reader.logql_scan_budget_bytes` and answer `422 query_too_broad` where it previously answered `200`. The work is window-proportional, which is the correct direction — and a `query=` selector is now the way to narrow it (above).

#### Errors (§2.1-2.3)

Every error a §2 **handler** writes — every row of the table below — is a
**bare `text/plain` body**: the message and nothing else, no JSON, no
keys, no trailing newline, under `Content-Type: text/plain; charset=utf-8`
and `X-Content-Type-Options: nosniff`. That **container** matches the
reference (the message prose does not always — see the cosmetic
divergence below), which writes all of these through one function
(`pkg/util/server/error.go:46-52` @ grafana/loki v3.7.4: two header sets,
`WriteHeader(status)`, then `fmt.Fprint(w, err.Error())`). Issue #264
replaced PulsusDB's earlier `{"status","errorType","error","position"?}`
envelope with it; a parse error's byte offset now travels inside the
message, as the reference's line/column does.

**Scope of that claim: handler-written errors only.** Rejections made
*above* the handlers — the router's own `404`/`405`, and the server-wide
request-deadline layer's `408` — are not written by this container. They diverge
from the reference, they are pre-existing, and #264 neither changed nor
covers them.

The status code is the whole machine-readable classification — there is no
`errorType` field on this surface (the reference has none either).

The other two query surfaces were checked against their own references
rather than made symmetric with this one, and they landed differently:

- **§3 (metrics) keeps a JSON envelope, and matches its reference.**
  Upstream Prometheus writes every API error as `application/json`
  carrying exactly `{status, errorType, error}` — `respondError`,
  `web/api/v1/api.go:2200-2230`, read at
  `vendor/github.com/prometheus/prometheus/` @ grafana/loki v3.7.4. Making
  §3 plain text to match §2 would have *created* a divergence.
- **§4 (traces) writes plain text too, since issue #384 — but not this
  container.** Tempo's user-facing `/api/*` query routes are served by its
  query frontend (`cmd/tempo/app/modules.go:500-512` @ tempo v3.0.2),
  whose 4xx rejections are `*http.Response` values with a nil `Header`
  map, copied out verbatim by `modules/frontend/handler.go:113-116`. So
  §4 sets **no** `X-Content-Type-Options`, where this section's writer
  does. The trailing byte, which is what separates §2's writer from
  `/loki/api/v1/push`'s, is the same on both (neither writes one) — so
  the two surfaces agree on the terminator and disagree on the header,
  and neither expectation may be reused for the other. See §4's own
  errors note.

The three surfaces have never shared an error writer in PulsusDB, so
changing §2 changed only §2.

**"Plain text" is not one contract.** `POST /loki/api/v1/push` (§8.2)
also answers plain text, but **LF-terminated** — its reference binds a
different writer (`http.Error` -> `fmt.Fprintln`, issue #374) from the
one above (`fmt.Fprint`). The query surface terminates nothing; the push
surface always terminates. Do not carry a trailing-byte expectation from
one to the other.

| Cause | HTTP |
|-------|------|
| Malformed params, malformed LogQL, empty/contradictory matchers, invalid `step` | `400` |
| LogQL query text of **131,072 bytes or more** (`pulsus_logql::MAX_QUERY_BYTES`, an exclusive maximum — the longest accepted query is **131,071 bytes**; the reference's `maxInputSize` at grafana/loki v3.7.4 `pkg/logql/syntax/parser.go:42`, enforced `>=` at `:86`; applies at every LogQL parse, incl. per `match[]` value and `/tail`) | `400` |
| LogQL nesting past **64 levels** (`MAX_DEPTH`, `crates/pulsus-logql/src/error.rs:24`) or label-filter parenthesis nesting past **91** (`LABEL_FILTER_MAX_DEPTH`, `:43`), body `query nesting exceeds the N level limit`. The reference has no such bound — accepted divergence, below | `400` |
| Pipeline/plan rejection (bad regex — **including an uncompilable pushed-down line-filter or stream-matcher regex, since #240** — bad parser expression, unwrap-arity, …) | `400` |
| Query rejected as too broad (scan-budget or stream-count cap exceeded) | `422` |
| Metric result over **500 series** (`max_query_series`, the reference's own threshold and `> cap` test, applied to the FINAL result — never to scanned or inner-aggregation groups) | `422` |
| Metric evaluation over **12,000,000 result point-slots** or over the post-aggregation byte bound **`MAX_POST_AGG_BYTES` (8 GiB)** — both charged BEFORE the allocation they guard, so an over-wide query is a clean refusal rather than an OOM. The byte bound is a registered divergence with no reference equivalent (it evaluates step-ordered and never materialises the inner matrix); its `by(...)` and `group_left/right(include)` amplifier thresholds are in `docs/benchmarks/logs-differential-ledger.md` §"Issue #236" (d)/(e) | `422` |
| ClickHouse read timed out | `504` |
| Unclassified ClickHouse/internal failure | `500` |

The `422` rows are a **status** divergence and are unchanged by #264's
container change: the reference's classifier
(`pkg/util/server/error.go:54-131` @ grafana/loki v3.7.4) maps every LogQL
error class it names — parse, pipeline, limit, interval-limit, blocked,
matchers-only, unsupported-instant-syntax — to `400`, and returns `422`
for none of them.

LogQL rejection **bodies** carry the bare reason with no PulsusDB prefix
(issue #240); where a body has a reference counterpart it is byte-identical
and corpus-gated. PulsusDB does **not** reproduce the reference's
`parse error : …` / `stage '…' :` envelope wording (accepted cosmetic
divergence, owner-ruled: status, container and accept/reject decision must
match; message prose need not). The WebSocket close frame truncates reasons
at 123 bytes. The LogQL corpus runner did not execute the line filters
the planner pushes into SQL, so no corpus row could gate them; issue #278
closed that, and the corpus now carries rows on that path.

**Transport bound on the query-text cap (#279).** The 131,072-byte row
above is reachable only where the query arrives in a **POST form body**,
on one of the eight routes that **both take a query parameter and accept
a POST form body**: `/query`, `/query_range`, `/series` (per `match[]`),
`/detected_labels`, `/detected_fields`, and — since issue #406 Part B2
added `POST` to them — `/stats`, `/volume` and `/patterns`, each on both
the native and `/loki/api/v1` prefixes
(`crates/pulsus-server/src/logs_api/mod.rs:56-94,128-174`; all three of
the late additions reach the cap through the same `parse_logql` seam).
That set is narrower than "the `GET|POST` form routes", of which there
are ten: `/labels` and `/label/{name}/values` are `GET|POST`
form-encoded too but accept no `query` parameter (only `start`/`end`, per
the route table above), so no query of any length reaches the cap through
them. Our HTTP stack caps the whole request-target at
**65,534 bytes** (`http::Uri`), so
on any GET a request-target past that is answered `414 URI Too Long` with
an empty body by hyper, before routing — never the `400`
envelope above.
That ceiling is below the cap, so it also blocks legitimate sub-cap
queries above roughly 65.5 KB, and it applies to every route alike —
it is charged on the request-target, not on any one parameter, so a
`GET` to `/tail`, `/stats`, `/volume`, `/patterns`,
`/label/{name}/values` or `/index/*` meets it exactly as `/query` does.
The reference serves such GETs; measured divergence and re-derivation in
docs/benchmarks/logs-differential-ledger.md
(`get-request-target-uri-bound`).

**Accepted divergence — the GET request-target bound (issue #296, closed
2026-08-11 with no code change).** The 65,534-byte ceiling is neither a
PulsusDB choice nor configurable: the `http` crate packs a `Uri`'s
component offsets into 16-bit fields, so its hard maximum is
`u16::MAX - 1` (`http-1.4.2/src/uri/mod.rs:145`, refused at parse at
`:296`), and that type sits under the whole HTTP stack — raising it means
not using it. The reference sets no equivalent bound (no `MaxHeaderBytes`
anywhere in grafana/loki @ v3.7.4 `b318f282`), so it inherits Go's 1 MB
`DefaultMaxHeaderBytes`. It is **accepted rather than fixed because no
deployed path reaches it**: every layer in front of the database cuts
first — nginx at a 4 KB request line, Apache httpd at 8 KB, Chrome at
32 KiB, all below ours, against the reference's 1 MB (the ceilings
recorded in issue #296's closing comment). A browser-originated query is
capped before it leaves the client and a proxied one an order of
magnitude below our limit, so what remains is a non-browser client
talking straight to the database — and that client has a carrier: since
issue #406 Part B2 **every query-carrying log route accepts a POST form
body** (`crates/pulsus-server/src/logs_api/mod.rs:56-94,128-174`), where
the 131,071-byte text cap is the only bound. The sole exception is
`/tail`, which is `GET`-only here and effectively `GET`-only there too (a
POST cannot carry a WebSocket handshake; the reference's `POST`
registration is nominal and it answers `400` to one — measured
2026-08-10, `mod.rs:122-127`). Reopen if a client turns up that can only
`GET` and needs more than ~65.5 KB of request-target.

**Accepted divergence — LogQL nesting depth (issue #256, closed
2026-08-11 with no code change).** The nesting row above refuses at 64
and 91 levels; the reference serves far deeper, probed to
20,000. Exactly three shapes consume that depth — nested aggregations
(`sum(sum(… sum(count_over_time({app="x"}[5m])) …))`), nested parentheses
(`((((… ))))`), and label-filter parentheses
(`{app="x"} | ((((… (status="500") …))))`, the 91 limit). **A long binary
chain does not**: `parse_binary_expr`
(`crates/pulsus-logql/src/parser.rs:405-425`) loops over same-precedence
operands and recurses only on a right operand at *higher* precedence, so
`a + b + c + …` over fifty series stays shallow. That is why this is
accepted — the machine sources that emit LogQL at scale (dashboard
templating, query builders, recording-rule expansion) generate long
chains, not 65-deep aggregations, and **the 20,000 is a probe result
rather than any client's requirement**. Raising the limit would also be a
regression today: after issue #272 converted the LogQL AST/plan walks to
iterative form the parser's own recursive descent is still what this
guard bounds (`crates/pulsus-logql/src/error.rs:16-23`), and the
equivalent walks on the other query languages are issue #262, open and
not scheduled — so a higher ceiling trades a clean `400` for a stack
abort until that work lands. This is the same pragmatism as the five-year
LogQL time-range cap: where the reference attempts something no real
client sends, we answer `400`. Reopen if a query generator emitting
nested aggregations or bracket chains at this depth turns up.

### 2.4 `GET /api/logs/v1/tail` (WebSocket)

| Param | Notes |
|-------|-------|
| `query` | LogQL log stream query (selector + pipeline, evaluated by the same engine as §2.1); metric queries are rejected `400` |
| `limit` | cap on entries per frame (default 100; values above `PULSUS_TAIL_MAX_FETCH_LIMIT` are silently clamped) |
| `start` | starting timestamp (ns), default now − 1h |
| `delay_for` | seconds to delay to tolerate late arrivals (default 0; values above `PULSUS_TAIL_MAX_DELAY` — 5s — are clamped) |

`/tail` also **accepts a `regexp` parameter and ignores it.** That is not a gap: the reference reads it (`pkg/loghttp/tail.go:84` @ grafana/loki v3.7.4 `b318f282`) only after having already built `Plan.AST` from the pre-rewrite query (`:73-81`), and its tailer dispatches on `Plan` (`pkg/querier/tail/querier.go:71-94`) — measured 2026-08-10, byte-identical 10,571-byte first frames with and without it. There is no behaviour to match, so there is no divergence to register; use an inline `|~ "..."` line filter in `query`, which both stores honour.

**Object granularity: one stream object per entry.** A frame carries one `{"stream":{...},"values":[["<ns>","<line>"]]}` object per *entry*, not per label set — two entries of the same stream come back as two objects with the identical `stream` map, and a third stream's entry between them in time sits between them on the wire. Objects are ordered by `(timestamp_ns, labels_json, fingerprint, entry_index)`. This differs from §2.1's query response, which packs a stream's entries into one object, and the difference is deliberate: the reference behaves the same way on each of the two routes (`pkg/querier/tail/tail.go:114-125` @ grafana/loki v3.7.4 `b318f282` appends one `logproto.Stream` per entry with no grouping), and a tail consumer that walks objects first and values second renders packed frames' lines out of chronological order. **Equal timestamps within one stream are not reference-parity**: the reference keeps the order the entries were appended in, we order by the storage key, and we store no ordinal — the standing `timestamp-tie-order` divergence, so our order is deterministic but not theirs. **Cost:** splitting repeats the label map once per entry, `(entries − streams) × (len(labels_json) + 14)` bytes on a surface that buffers the whole frame; `PULSUS_TAIL_MAX_FETCH_LIMIT` bounds the entries in one frame and is the operator's knob for it.

Frames: `{"streams":[...],"dropped_entries":[{"labels":{...},"timestamp":"<ns>"}],"dropped_total":<n>}`. Slow consumers get the **oldest** undelivered frames evicted and reported, never unbounded buffering: `dropped_entries` is a bounded representative sample (at most `PULSUS_TAIL_MAX_ENTRIES_PER_FRAME` rows), and `dropped_total` — a PulsusDB **additive** field next to the reference frame shape; clients that don't know it ignore the extra key — carries the *exact* cumulative count dropped since the previous frame (`0` on a normal frame). Exceeding `PULSUS_TAIL_MAX_CONNECTIONS` concurrent tail connections rejects the next one `429` before the upgrade.

Delivery: tail polls ClickHouse (there is no push channel) with a deterministic composite keyset cursor — `(timestamp_ns, fingerprint, cityHash64(body))` plus an occurrence count — catching up over a backlog one `PULSUS_TAIL_CATCHUP_SLICE` window per query, so no single query scans unbounded history. Every row from `start` forward is delivered **exactly once**, including timestamp tie groups split across fetch pages and byte-identical duplicate lines inside a scanned window. Sole documented limitation: an entry arriving later than `delay_for` at an already-scanned position — at or below the cursor/watermark, e.g. a late byte-identical duplicate of an already-delivered same-nanosecond line — is genuinely late and is not delivered.

### 2.5 `GET|POST /api/logs/v1/stats`

`?query={selector}&start=&end=&since=` → `{"streams":N,"chunks":N,"entries":N,"bytes":N}`. `GET|POST` since issue #406 Part B2 (the reference registers `/loki/api/v1/index/stats` `Methods("GET","POST")`, `pkg/loki/modules.go:690` @ v3.7.4 `b318f282`); `end < start` is `400`. `query` accepts a stream selector plus optional line filters; anything else (parsers, formats, label filters, metric queries) is rejected `400`. `chunks` is a **partition-count proxy**: the selector-scoped distinct count of partition dates touched, not a physical MergeTree part count (per-part fidelity, if ever demanded, routes to the scale-validation milestone). Without a line filter the counters are served from the rollup with zero body reads (entries/bytes are 5s-bucket-granular at window edges, the same rollup-routing caveat as `count_over_time`); a line filter forces an exact `log_samples` scan. With `X-Pulsus-Explain: 1`, `explain` (the §2.1 shape) is added as a sibling key of the four counters.

### 2.6 Drilldown (M7)

```
GET|POST /api/logs/v1/volume             ?query=&start=&end=&since=&limit=&targetLabels=&aggregateBy=
GET|POST /api/logs/v1/detected_labels    ?query=&start=&end=&since=
GET|POST /api/logs/v1/detected_fields    ?query=&start=&end=&since=&line_limit=&limit=
GET|POST /api/logs/v1/detected_field/{name}/values
                                         ?query=&start=&end=&since=&line_limit=&limit=
GET|POST /api/logs/v1/patterns           ?query=&start=&end=&since=&step=
```

#### 2.6.1 `GET|POST /api/logs/v1/volume`

Per-label-set log byte volumes over `[start, end]` — the drilldown UI's "which streams are loud" aggregation. `GET|POST` since issue #406 Part B2 (the reference registers it `Methods("GET","POST")`, `pkg/loki/modules.go:691` @ v3.7.4 `b318f282`). Served **entirely from the 5s rollup with zero body reads** — the endpoint accepts a matchers-only selector, so unlike §2.5 there is no raw fallback at all.

| Param | Notes |
|-------|-------|
| `query` | LogQL **stream selector, matchers only** — required. ANY pipeline stage is rejected `400` (line filters included, unlike §2.5: the rollup is body-content-blind and volume has no raw scan to fall back on), as are metric queries. The match-all `{}` is rejected `400` (PulsusDB's ≥1-positive-matcher rule; the reference accepts `{}` here — documented deviation; `targetLabels` remains fully usable with any non-empty selector, e.g. `{env=~".+"}`) |
| `start`, `end`, `since` | §2's timestamp rules; default `end = now`, `start = min(end, now) - since`, `since = 1h`. `end < start` is `400` |
| `limit` | top-N entries kept **after** the bytes-desc sort. Absent **or `0`** → 100 (the reference resets an explicit 0 to its default); above 5000 → `400`, never clamped (§2.1's cap rule) |
| `aggregateBy` | `series` (default): group by the matched label **pairs**. `labels`: group by bare label **names** — each entry's metric is `{"<name>":""}` (the reference's empty-value shape) |
| `targetLabels` | comma-separated label names re-keying the aggregation. When supplied, entries key on these names alone (both modes); each target with no matcher of its name in the selector is injected as `name=~".+"` before planning, so negative-only or unrelated selectors still resolve target-keyed streams. **Bounded** (documented deviation — the reference has no caps; same defensive 400-not-clamp posture as `limit`): at most **32** names post-dedupe, each at most **256** bytes (post-percent-decode) — oversized requests are rejected `400` in pure param parsing, before any planning or SQL |

Without `targetLabels`, the aggregation keys on the selector's **own matcher names** — every operator, including `!=`/`!~` (so `{env!="dev"}` keys results by each stream's `env` value); a stream lacking a keyed label omits that pair from its key, and a stream matching none of the names groups under `{}`.

Response `200`: the §2.2 vector envelope evaluated at `end` — `{"status":"success","data":{"resultType":"vector","result":[{"metric":{...},"value":[<end_unix_seconds>,"<bytes>"]},...],"stats":{"series":N}}}`. **Result order is bytes-desc (tie-break: label set asc), truncated to `limit` — NOT label-sorted** (the top-N presentation is the contract; deliberately different from §2.2's label-sorted vectors). `stats` is a PulsusDB-additive key (same clients-ignore-extras precedent as §2.4's `dropped_total`). `bytes` is the sum of line-body bytes (the same basis as §2.5's `bytes`), 5s-bucket-granular at window edges (the same rollup caveat as §2.5/`count_over_time`). With `X-Pulsus-Explain: 1`, `data.explain` (the §2.1 shape) is added — its `volume_read` stage always targets `log_metrics_5s`.

Errors: `400` (missing/malformed `query`, metric query, any pipeline stage, invalid `aggregateBy`/`limit`, oversized `targetLabels`, `end < start`), `422`, and `503`/`504`/`500` per §2.3's table.

#### 2.6.2 `GET|POST /api/logs/v1/detected_labels`

Indexed stream labels with exact per-key value cardinalities — the drilldown UI's label picker. **Reads the stream index (`log_streams_idx`) and the log rollup (`log_metrics_<res>`), never `log_samples`**, in one server-side aggregation: the index scan stays month-partition-pruned with one row per distinct key crossing the network (never one per value), and the request's own `[start, end]` arrives as the §2.3 activity semi-join over the rollup's `(fingerprint, bucket_ns)` primary key. Before issue #399 the only time bound was the month, so a one-hour request was answered from the whole calendar month; the lower bound now floors to the rollup bucket containing `start` (≤ one rollup resolution, 5s by default, of over-inclusion per edge — see §2.3). Structured metadata is deliberately absent (it never enters the stream index — matching the reference, whose detected-labels reads only the label index). `GET|POST` form-encoded (the §2.3 `/labels` precedent; a documented deviation from this section's earlier GET-only sketch, ratified on issue #170).

| Param | Notes |
|-------|-------|
| `query` | **optional, matchers only** — absent or empty is the unscoped form (every stream in the window, matching the reference's empty-string handling); when present, stage-1 resolution scopes the same aggregation with `fingerprint IN`. A pipeline stage or metric expression is a `400` parse error carrying `position` (the selector-only grammar rejects it) |
| `start`, `end` | ns / RFC3339; default `end = now`, `start = end - 1h` (§2.1) |

Relevance filter (reference-pinned): labels named `cluster`/`namespace`/`instance`/`pod` are always kept; any other label is dropped iff **every** one of its values parses as a float or a UUID (all four `uuid.Parse` forms — hyphenated, `urn:uuid:`-prefixed, `{hyphenated}`, bare 32-hex — case-insensitive). The float test is ClickHouse `toFloat64OrNull` (single SQL implementation, no Rust twin to drift); margins vs Go `ParseFloat` (hex floats like `0x1p-2`, underscore literals) are accepted, documented divergences.

Response `200`: `{"detectedLabels":[{"label":"…","cardinality":N},…]}`, sorted by label. Documented divergences from the reference, all deliberate: `cardinality` is **exact** (`uniqExact`) rather than a hyperloglog estimate — the registered `detected-cardinality-exact-not-estimated` ledger entry, which states the agreement threshold as what it is, a property of the **value strings** rather than a number of distinct values, and carries the audit this endpoint's cardinality was given (issue #261: staying exact is measurably *cheaper* than reproducing the reference's estimate, which has no ClickHouse-side equivalent and would mean shipping every distinct value out to the coordinator to hash); the top-level key is always present (never omitted when empty); deterministic label-sorted order vs Go map order. With `X-Pulsus-Explain: 1`, `explain` (the §2.1 shape) is added as a sibling key — its `detected_labels` stage always targets `log_streams_idx`, with the configured `log_metrics_<res>` rollup named inside the activity semi-join.

**Parity, not a divergence — no `sketch` key.** This was listed above as a deliberate divergence until issue #261; it never was one. The reference does not emit `sketch` on its HTTP surface at all: `/loki/api/v1/detected_labels` is routed to the query-frontend handler in every deployment shape (`pkg/loki/modules.go:1368` @ `grafana/loki` v3.7.4 = `b318f2829f0ae2094ab3a1e90780450e9e4b03be`), and that tripperware's last stage, `NewDetectedLabelsCardinalityFilter`, rebuilds every entry as `&logproto.DetectedLabel{Label: …, Cardinality: …}` — dropping the sketch unconditionally (`pkg/querier/queryrange/roundtrip.go:347-370`, the rebuild at `:365`); the field is `json:"sketch,omitempty"` in any case (`pkg/logproto/logproto.pb.go:3054`). Measured 2026-08-08 against `grafana/loki:3.7.4`, whose body carries only `label` and `cardinality` per entry. Recording it as a divergence overstated what we differ on; it is recorded here as parity so the next reader does not defend a difference that does not exist.

An over-broad `/detected_labels` that exhausts ClickHouse's memory currently answers `500` rather than the `422` the too-broad family uses. That is a whole-LogQL-read-path gap, not this endpoint's — it is issue #398, with the measurement in the ledger's memory bullet.

Errors: `400` (malformed/piped `query`), `422`, and `503`/`504`/`500` per §2.3's table.

#### 2.6.3 `GET|POST /api/logs/v1/detected_fields`

Per-entry **fields** detected from a bounded sample of matching log entries: structured-metadata keys (no parser attribution), the query pipeline's own extracted labels, and automatic json-first/logfmt-fallback parsing of each (post-pipeline) line — a parser counts as successful only when it sets no `__error__`. `GET|POST` form-encoded.

| Param | Notes |
|-------|-------|
| `query` | **required** — a full LogQL log-selector expression including pipeline stages; metric queries are rejected `400` |
| `start`, `end` | ns / RFC3339; §2.1 defaults |
| `line_limit` | entries sampled — the **entry** axis. Absent **or empty** → 100. Non-positive or outside the accepted integer set → `400`. That set is an optional `+`/`-` then ASCII digits only — no surrounding whitespace, underscores, radix prefixes or exponents — with the value fitting an `i64`, which is exactly Go `strconv.Atoi`'s set on a 64-bit platform; greater than 5000 → `400` — refused outright, never clamped and never saturated, at any magnitude. That 5000 is **parity, not a house cap**: the reference rejects `line_limit=5001` at its own `validation.max-entries-limit` (default 5000), enforced by `validateMaxEntriesLimits` (`pkg/querier/queryrange/limits.go`) from the detected-fields handler, and at that boundary only the 400's message text differs. Above 2^32 the reference's unchecked `uint32()` cast wraps back into its legal range and it accepts where we go on refusing — recorded in `detected-fields-limit-saturates-not-wraps` |
| `limit` (legacy alias `field_limit`) | max distinct field **names** — a different axis from §2.1's `limit`, with **no ceiling**: any positive value is accepted, as on the reference. First-seen wins; later names are skipped entirely. `limit` is read first, then the alias, and a **present-but-empty** value on either counts as absent (default 1000). Non-positive or outside the accepted integer set → `400`. That set is an optional `+`/`-` then ASCII digits only — no surrounding whitespace, underscores, radix prefixes or exponents — with the value fitting an `i64`, which is exactly Go `strconv.Atoi`'s set on a 64-bit platform. Above `u32::MAX` PulsusDB **saturates** at `u32::MAX` where the reference's unchecked `uint32()` cast wraps and returns *fewer* fields for a larger limit — a deliberate divergence, `detected-fields-limit-saturates-not-wraps` in docs/benchmarks/logs-differential-ledger.md. What actually bounds the response is the retention model below, not this parameter |
| `step`, `since` | accepted and **ignored** (documented deviation: the reference validates `step` only as a shared-codec artifact and neither param affects detection) |

Sampling contract (issue #170 plan v2): the sample is up to `line_limit` **post-pipeline matching** entries, newest first. With no in-engine dropping stage (a bare selector, line filters, non-dropping transforms — the dominant drilldown shape) one index-served `LIMIT line_limit` scan is provably that sample (line-filter pushdown carries the exact predicate). A dropping stage (a label filter, or a line filter after `line_format`) engages the §2.1 fetch-until-limit keyset paging under the **same byte scan budget an equivalent `/query_range` would pay** (`reader.logql_scan_budget_bytes`), so matches occurring long after the first `line_limit` raw rows are still found. Pages stream row by row (issue #244): if the budget is spent mid-paging — including **mid-page**; the accumulated prefix is a prefix of the newest-first sampled row sequence and is **not** required to align with a page boundary — the response returns the fields found so far and adds the additive `"pulsus_partial": true` key, **omitted** on complete responses (the §2.1 `stats.pulsus_partial` convention), so complete responses stay byte-identical to the reference shape. A `ScanBudgetBytes` overflow while draining the **first** page is `422` regardless of how many rows were already delivered — the prefix is discarded with the request (the pre-#244 contract, preserved).

Retention model (issue #244): everything the accumulator **retains** — distinct value strings and admitted field names — is charged against a server-side per-request byte ceiling (`MAX_DETECTED_FIELD_BYTES`, 64 MiB, the house per-query retained-state magnitude) **before** each retaining allocation. A refused charge **freezes that field's value set and keeps serving** — the type still re-detects and parsers still attribute; the request is never rejected — and the response carries the same additive `"pulsus_partial": true` key (its meaning is therefore "budget-truncated sampling **or** a retention-capped cardinality"). The reference has no analogous bound because it retains no value bytes at all — its per-field state is a p14 HyperLogLog sketch.

Type detection: `type` ∈ `string`\|`int`\|`float`\|`boolean`\|`duration`\|`bytes`, detected in the reference's pinned order int → float → boolean → duration → bytes → string, re-detected per observation (the last sampled entry wins). Duration/bytes reuse the §2.1 label-filter unit parsers; margins vs the reference (Go hex/underscore float literals; `d`/`w` duration suffixes accepted here but not by Go's `time.ParseDuration`; spaced byte quantities like `"42 MB"` accepted there but not here) are accepted, documented divergences.

Response `200`: `{"fields":[{"label":"…","type":"…","cardinality":N,"parsers":["json"|"logfmt",…],"jsonPath":["…",…]},…],"limit":N}`. The zero-field body is byte-exact against the reference, and so is every per-field object *against the reference's single-response path* — with two deliberate exceptions, both ledgered: the **array order** (first bullet) and **`jsonPath` on a sharded reference** (second bullet).

- `fields` is **sorted by label, ascending**. The reference emits it in Go map iteration order at both the single-response and the sharded-merge build sites, with no sort before marshaling. The Go spec leaves map iteration order unspecified and does not guarantee it is the same from one iteration to the next, so no reproducible order is guaranteed to exist to mirror; PulsusDB pins a deterministic one, the same treatment every irreproducible reference tie in this repo gets. Registered as `detected-fields-array-order-pinned` in docs/benchmarks/logs-differential-ledger.md, which carries the source citations. The SET of fields is reference-exact; a client must key on `label`, never on position.
- `parsers` is **`null`**, not `[]`, for a field observed only from structured metadata or the query's own pipeline: the reference's jsontag carries no `omitempty` (the key is always present) and its handler maps the empty slice to nil before marshaling.
- `jsonPath` is the **raw** JSON key path the field was flattened from — `["user","id"]` for the nested field `user_id`, and the unsanitized key for a top-level one (`["a-b"]` for label `a_b`). It is **omitted entirely** for a field never seen through the json auto-parse (structured metadata, logfmt, pipeline extractions), matching the reference's `omitempty`, and reflects the LAST json-attributed observation. PulsusDB emits it on **every** response; the reference emits it from a single querier but **drops it in its sharded merge**, so the same query returns paths on one reference deployment and not another. We decline to reproduce that — a registered exception, `detected-fields-jsonpath-survives-merge` in docs/benchmarks/logs-differential-ledger.md. Edge: a component that trims to whitespace contributes nothing to the label name but still occupies a path slot (`{"x":{"  ":{"b":1}}}` → label `x_b`, path `["x","  ","b"]`) — the reference's behaviour.
- `cardinality` is **exact** over the sampled-and-retained values (see the retention model above), where the reference reports a p14 HyperLogLog **estimate**. **The only distinct-value count at which they are guaranteed to agree is the degenerate one — a single value, which has nothing to collide with; from two values upward there is no useful bound**, because what makes the estimate exact depends on the value strings themselves — whether two of them have collided in the sketch's sparse key space, and whether the sketch has abandoned that sparse representation — rather than on how many values there are. Past those points the two often still agree; they simply stop being guaranteed to. An earlier revision of this bullet gave 5327 as a universal such bound; that number is one measured value family's threshold (`v0`…`v{n-1}`, still captured in `crates/pulsus-read/tests/golden/detected_cardinality/reference_divergence.tsv`), and another family diverges at 4533. Per-family measurements, the mechanism, and the source citations are in the registered `detected-cardinality-exact-not-estimated` ledger entry (docs/benchmarks/logs-differential-ledger.md); no endpoint-reachability bound is claimed there either.
- The **empty result is the bare `{}`**: the reference omits `fields` when it is empty and only assigns `limit` when at least one field exists, so `limit` appears exactly alongside a populated `fields`. The additive `pulsus_partial`/`explain` keys, when present, are that body's only members.
- `__error__`/`__error_details__` never surface as fields.

With `X-Pulsus-Explain: 1`, `explain` is added as a sibling key — its `detected_fields_read` stage carries the single stage-3 scan (note `single-scan: no unpushed dropping stage`) or the first keyset page (note `paged: unpushed dropping stage`).

Errors: `400` (missing/malformed `query`, metric query, invalid `line_limit`/`limit`), `422`, and `503`/`504`/`500` per §2.3's table.

#### 2.6.5 `GET|POST /api/logs/v1/detected_field/{name}/values`

The distinct **values** of one detected field, over the same bounded sample §2.6.3 describes — the drilldown UI's per-field value picker, and the one resource call whose errors that UI surfaces rather than swallows. Identical read path to §2.6.3: stage-1 resolution, stage-2 hydration, one bounded stage-3 sample scan with line filters pushed down. No new SQL shape, no new index, no extra round trip.

| Param | Notes |
|-------|-------|
| `query` | **required** — the same full LogQL log-selector grammar §2.6.3 accepts, including pipeline stages; metric queries are rejected `400` |
| `start`, `end`, `since` | §2.1 defaults |
| `line_limit` | entries sampled — §2.6.3's parameter, same parser, same bounds |
| `limit` (legacy alias `field_limit`) | max distinct **values** of `{name}` retained. Absent or empty → 1000. §2.6.3's parser and error text; on this route it caps the value axis rather than the field-name axis. The cap is checked **between sampled entries**: everything one entry contributes is added before it is consulted again, so a response may carry more than `limit` values when a single entry contributed several |

Response `200`: `{"limit":N,"values":["…",…]}` — `limit` first, then `values`, **sorted ascending by byte order**. The empty result (an unknown `{name}`, or a selector matching no stream) is the bare `{}`: `limit` is emitted only alongside a populated `values`, exactly as §2.6.3's zero-field body works. The empty string is a real value and appears when observed.

The reference emits `values` in Go map iteration order, so there is no reproducible order to mirror; the ascending pin is a clause of the `detected-fields-array-order-pinned` entry in docs/benchmarks/logs-differential-ledger.md. The additive `pulsus_partial` and `explain` keys behave exactly as in §2.6.3 and survive into the empty body; the `explain` `result_type` is `detected_field_values`.

Errors: `400` (missing/malformed `query`, metric query, invalid `line_limit`/`limit`, `end < start`), `422`, and `503`/`504`/`500` per §2.3's table.

#### 2.6.4 `GET|POST /api/logs/v1/patterns`

Detected **log patterns** — the drilldown UI's "group these lines by shape" view. Each pattern is a **deterministic, stateless** token-class template of the line body (extracted at ingest, aggregated per `(fingerprint, 10s-bucket, template)` into `log_patterns`; docs/schemas.md §3.1): digit/length classification (a fragment with an ASCII digit, or longer than 64 bytes, becomes `<_>`), `key=value`/`key:value` awareness (only the value is classified), wrapper-punctuation preservation, and 1 KiB-prefix / 64-token / 512-byte caps. Templates are **normalized (whitespace-collapsed), not round-trip matchable**; grouping is deliberately coarser than an online clusterer (a digit-free variable word stays literal) in exchange for identity that survives merges across batches, shards, replicas, and retries. Served by ONE pushed-down aggregate over `log_patterns` with `fingerprint` primary-key prefix pruning and a server-side top-1000 — **no hydration, no body read** (the response carries no labels). `GET|POST` since issue #406 Part B2.

| Param | Notes |
|-------|-------|
| `query` | LogQL **stream selector, matchers only** — required. ANY pipeline stage is rejected `400` (line filters included, like §2.6.1: templates are precomputed and the bodies are gone), as are metric queries |
| `start`, `end`, `since` | §2's timestamp rules; default `end = now`, `start = min(end, now) - since`, `since = 1h`. Half-open `[start, end)` over the pattern buckets. `end < start` is **not** checked here — the reference's own exemption (§2 preamble) |
| `step` | optional bucket size; a duration string or bare seconds. Absent → derived as **whole seconds**, the same `max(floor((end-start) in seconds / 250), 1)` rule §2.1 gives, and for the same reason: the reference derives this endpoint's step through the *same* shared helper — `ParsePatternsQuery` calls `step(r, start, end)` (`pkg/loghttp/patterns.go:20`), which falls back to `defaultQueryRangeStep` (`params.go:122-126,140-142`, @ grafana/loki v3.7.4 `b318f282`). The 10s floor is applied **after** that derivation: the derived (or supplied) step is **floored to the 10s ingest bucket**, never smaller — a finer step would invent sub-bucket granularity the stored data lacks. The floor is not a no-op over the derivation: it changes the answer whenever the derived whole second is not a multiple of ten. The `(end-start)/step` grid is capped at **11,000** (else `400`), the same bound as the metrics endpoints |

Response `200`: the Loki-interop envelope `{"status":"success","data":[{"pattern":"<_> ...","samples":[[<unix_seconds>,<count>],...]},...]}`. `samples` are ascending by second, zero-count steps omitted, both elements bare integers (`unix_seconds` is the floor of the bucket ns). **`data` is ordered total-count desc then pattern asc, truncated to the top 1000 — NOT re-sorted client-side** (the top-N presentation is the contract; a PulsusDB determinism pin — upstream order is unspecified). **Count semantics** are exact on the clean ingest path and **best-effort approximate under ingest-failure re-sends**, at parity with §2.2's `log_metrics` (the writer never auto-replays a block that could have committed; a per-request burst of >10 000 distinct templates is an under-count event, folded into the same approximate semantics — see docs/schemas.md §3.1). With `X-Pulsus-Explain: 1`, `data.explain` (the §2.1 shape) is added — its `patterns_read` stage always targets `log_patterns`.

Errors: `400` (missing/malformed `query`, metric query, any pipeline stage, non-positive `step`, over-11k grid), `422`, and `503`/`504`/`500` per §2.3's table.

---

## 3. Metrics query API (Prometheus HTTP API)

The standard Prometheus API is PulsusDB's native metrics API — its paths are product-neutral and it is what every metrics client speaks. The query language target is **full PromQL compliance** against a pinned upstream Prometheus release (v3.13): all registry functions (experimental ones behind the same feature gate as upstream), subqueries, `@`, duration expressions — verified by replaying the upstream PromQL test corpus in CI ([architecture.md §5.1](architecture.md)).

### 3.1 `GET|POST /api/v1/query`

| Param | Notes |
|-------|-------|
| `query` | PromQL, required |
| `time` | evaluation time (RFC3339 or unix); default now |
| `timeout` | an additional, strictly shorter deadline for this request. A bare (possibly fractional) seconds literal (`60`, `0.001`) or a duration string (`1ms`, `1m30s`); `ns` is not an accepted unit. Installed only when it is **strictly shorter** than `PULSUS_QUERY_TIMEOUT` — an equal or longer value leaves the server deadline governing, so the two are never installed at once. A breach is `503` `timeout` with `query exceeded the requested timeout of <duration> (timeout parameter)`. Unparseable, zero or negative is `400 bad_data`. Read on `/api/v1/query` and `/api/v1/query_range` only |

Response: `{"status":"success","data":{"resultType":"vector"|"scalar"|"matrix","result":[...]}}`. Values formatted as Prometheus does (shortest round-trip float; `NaN`, `+Inf`, `-Inf` as strings).

### 3.2 `GET|POST /api/v1/query_range`

`query`, `start`, `end`, `step` (required), plus §3.1's `timeout`. **Hard resolution cap: `(end - start) / step` must not exceed 11,000 step intervals.** 11,000 intervals — 11,001 grid points — is served; 11,001 intervals is `400 bad_data` with `exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)`. The rule counts **intervals** even though its message says *points*: that is the reference's own predicate, and implementing its sentence instead rejected one step early (issue #471). Long ranges are transparently served from downsampling tiers (M3); the segmentation is visible via `X-Pulsus-Explain`.

### 3.3 Metadata & discovery

```
GET|POST /api/v1/labels                    ?match[]=&start=&end=&limit=
GET      /api/v1/label/{name}/values       ?match[]=&start=&end=&limit=
GET|POST /api/v1/series                    ?match[]=&start=&end=&limit=  (match[] required)
GET      /api/v1/metadata                  ?metric=&limit=
GET|POST /api/v1/query_exemplars           (empty-success stub in v1)
```

`__name__` is always present in labels responses. Metadata is sourced from `metric_metadata` (populated from remote-write metadata and OTLP).

**`limit` on the three discovery endpoints** (`/labels`, `/label/{name}/values`, `/series`): absent, empty and `0` all mean *no limit*; a negative value is `400 bad_data` with `invalid parameter "limit": limit must be non-negative`; a non-integer or out-of-range value is `400 bad_data` with `invalid parameter "limit": cannot parse "<raw>" to an integer` (our own wording — the reference emits its runtime's integer-parse text there, which we deliberately do not reproduce; the status and `errorType` are identical). When the limit actually cuts the result the response carries `"warnings":["results truncated due to limit"]` as a **top-level sibling of `data`**, and when it does not there is no `warnings` key at all. Truncation is applied last, to the already-sorted, already-deduplicated result — it is a **response-size** cap, never a scan bound (`PULSUS_PROMQL_MAX_METRIC_FANOUT` and `PULSUS_PROMQL_MAX_CACHE_SCAN` remain the scan bounds).

**`limit` on `/metadata` is a different rule and is unchanged:** there `limit=0` means *return nothing* (`{"status":"success","data":{}}`), on this server and on the reference alike. The two meanings are not unified.

**A `U__`-escaped label name is unescaped before any storage lookup** (`/api/v1/label/{name}/values`). Clients escape a label name into the URL path when it is not legacy-legal: a `U__` prefix, `_` → `__`, valid legacy runes kept, anything else `_<hex>_` (at most five hex digits). The unescape happens at the HTTP boundary, so `/api/v1/label/U__job/values` answers exactly what `/api/v1/label/job/values` answers. Any malformed escape — a non-hex byte, a missing closing `_`, a six-digit hex escape, a surrogate — returns the path segment **unchanged** rather than erroring, and reaches the engine as written. `U__` alone unescapes to the empty string and is `400 bad_data` with `invalid label name: ""`; a name that is merely not legacy-legal (`a-b`, no prefix) is not rejected at all.

`match[]` selectors accept the full discovery selector surface: a concrete metric name (`up`), a matcher-only selector (`{job="api"}`), and a regex/negated `__name__` matcher (`{__name__=~"up.*"}`, `{__name__!="up",job="api"}`) — the last at parity with the query path. A regex/negated-`__name__` selector resolves its candidate metric names through the resident label cache under `PULSUS_PROMQL_MAX_METRIC_FANOUT`, then fetches with one flat `metric_name IN (…) AND fingerprint IN (…)` query against `metric_series`. When the resident cache is **degraded/cold** (cold / stale / out-of-window / regex-cache-full) the discovery path falls back to a bounded two-stage `metric_series` read: a `SELECT DISTINCT metric_name` probe (the name matchers pushed as `metric_name` predicates, `LIMIT PULSUS_PROMQL_MAX_METRIC_FANOUT + 1`) followed by the same flat `metric_name IN (…)` fetch with the label matchers applied in SQL — so a degraded regex/negated-`__name__` selector now **resolves** rather than returning a named `422`. A resolved (warm) or probed (degraded) candidate-name set past the cap is `422 execution` (`QueryTooBroad`), never an unbounded scan. The degraded probe caps on **names matching the name predicate** — a superset of the warm cap, which counts names with ≥1 label-matching series — so at the cap boundary the degraded path may `422` where warm would serve; below the cap the two results are byte-identical. A non-vector-selector `match[]` value (e.g. `sum(up)`) or brace-level `or` remains a parse-time rejection (`422 execution` / `400 bad_data` respectively). A broad regex/negated-`__name__` discovery selector can also independently exceed `PULSUS_PROMQL_MAX_CACHE_SCAN` (a selector whose matchers match few or no metric names can still *examine* the entire resident label cache) — this is `422 execution` too, on a **warm** cache, and never falls back to the degraded-cache probe; narrow the `__name__` matcher or use a metric-scoped/matcher-only selector instead.

`/api/v1/label/__name__/values` reads a **narrow name projection**, not the series' label sets: for a concrete-name or matcher-only `match[]` (and for no `match[]` at all) it issues `SELECT DISTINCT metric_name` over the same `WHERE` the ordinary discovery query would have used — `metric_name` is the leading component of `metric_series ORDER BY (metric_name, fingerprint, unix_milli)`, so the distinct set comes off the sorted key. The answer is unchanged. With no `match[]` the `labels` column is not referenced at all; with a **label** matcher it is still read to evaluate the filter, and the gain there is transport and parse count (one row per metric name instead of one per series) rather than bytes read. The regex/negated-`__name__` routes above are unchanged and keep their own projections.

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

`{"status":"error","errorType":"...","error":"..."}` — exactly these three fields, **no `position` field**: a PromQL parse error's position is embedded verbatim inside the `error` message string, Prometheus-style, never split out. This is the **only** query surface that keeps a JSON envelope: the log API (§2.3) and the trace API (§4) both write a bare `text/plain` body. Upstream Prometheus writes its errors as `application/json`, Loki writes LogQL's as bare `text/plain`, and Tempo writes its query frontend's as bare `text/plain` too — three surfaces, two shapes, each matched against its own reference independently and never made symmetric with the others.

| Cause | HTTP | `errorType` |
|-------|------|-------------|
| Malformed params, malformed PromQL (parser position **in the message**), 11,000-interval resolution cap exceeded, invalid `limit`/`timeout`, an escaped label name that unescapes to the empty string | `400` | `bad_data` |
| A label-matcher regex **RE2 rejects** (issues #280, #309, #316) — see the note below | `400` | `bad_data` |
| Out-of-subset construct / binary-op matching failure / histogram-bucket error | `422` | `execution` |
| A deadline expired: the server request deadline (`PULSUS_QUERY_TIMEOUT`), the `timeout` request parameter, the ClickHouse stream deadline, the ClickHouse pool-permit wait, or ClickHouse's own server-side `max_execution_time` | `503` | `timeout` |
| Pool or label cache not yet ready, ClickHouse unreachable | `503` | `unavailable` |
| Unclassified internal failure | `500` | `internal` |

**Label-matcher regexes are RE2's, not the Rust `regex` crate's (issue #280).** Upstream Prometheus compiles every `=~`/`!~` matcher with Go's `regexp` — RE2 — so RE2's syntax is the accepted set, and a pattern RE2 rejects is a `400 bad_data`. PulsusDB matches that boundary rather than its host language's: patterns the Rust `regex` crate cannot compile but RE2 can (e.g. `a{bbb}c`) are pushed to ClickHouse's RE2 and **answered**, and patterns RE2 rejects (e.g. `\p{Alphabetic}`, which the Rust crate accepts) are a `400 bad_data` carrying RE2's own reason — never a `500`. The verdict is ClickHouse's, so the rejection is reported at execution rather than at parse; the status, `errorType` and accepted set are Prometheus's, the message prose is not. A `427` body that does not carry ClickHouse's recognised `cannot compile re2: … . Look at …` framing is reported with a generic reason instead, never echoed — an unrecognised body's tail carries the executed SQL.

**The warm label cache defers to that same authority (issue #309).** A `/query`/`/query_range` selector resolved in-process by a warm, in-window label cache never asks ClickHouse, so it used to answer `200` for a pattern RE2 rejects. It no longer evaluates such a pattern at all: before the cache is consulted, every `=~`/`!~` pattern is screened for the constructs where the Rust crate's accepted grammar is known to exceed RE2's (`\p{…}`/`\P{…}`, `\u`/`\U`/`\<`/`\>`/`\b{…}` escapes, group heads other than `(?:` and `i`/`m`/`s`/`U` flags, repetition bounds above 1000, a repetition of a repetition), and a screened pattern degrades to the storage path where ClickHouse returns the verdict. The screen is deliberately conservative and **never rejects on its own**: a pattern RE2 accepts but the screen defers is still answered, one ClickHouse round-trip slower.

**In-process matching uses RE2's reading of the pattern (issue #317).** Screening covers patterns whose *acceptance* the two engines disagree on; a second class is accepted by both and **means** something different, so the query succeeds and selects the wrong series. The Rust `regex` crate reads `\d`/`\w`/`\s` (and their negations) as Unicode classes where RE2 reads them as ASCII, reads `\b`/`\B` as Unicode word boundaries where RE2's are ASCII, reads `&&`/`~~`/`--` and a nested `[` inside a character class as set operators where RE2 reads them as ordinary characters, and rejects a brace that opens no repetition (`a{bbb}c`, `a{,5}`) where RE2 reads it as a literal. Every `=~`/`!~` pattern is therefore rewritten into RE2's reading before any in-process compile — `\d` becomes `[0-9]`, `[a&&b]` becomes a class of three characters, `a{bbb}c` becomes a literal. The pattern sent to ClickHouse is unchanged (RE2 already reads it correctly), and a rewrite the Rust crate cannot compile degrades to the storage path exactly as a screened one does. Consequences a client can see: `{job=~"\d+"}` no longer matches non-ASCII digits (upstream never did), and `a{bbb}c`-style literal-brace patterns are now answered from the warm cache instead of costing a round-trip.

**A name-less selector's uncompilable matcher is `400 bad_data` (issue #316).** `{__name__=~"…"}` on `/query`/`/query_range` has no metric-scoped SQL fallback (issue #85), so a matcher this engine cannot compile — in RE2's reading of it — has no storage authority to delegate to. Upstream Prometheus compiles every matcher in its parser and answers `400 bad_data`, and so does PulsusDB, uniformly: previously the same input gave `422 execution` when the cache was cold or the name matched, and `200` with an empty result when the name matchers matched nothing (which left the label matchers uncompiled). The verdict is the in-process engine's, so it is reached only for a pattern that is genuinely uncompilable — a merely *screened* pattern (undecidable, possibly valid) keeps its named `422` rather than being called invalid.

One boundary remains, and it is storage's, not the cache's: ClickHouse compiles a matcher regex only when it evaluates `match()` on a row, so a selector naming a metric with **no stored rows in the queried window** is answered `200` with an empty result on every path — degraded and warm alike — whatever the pattern.

### 3.5 Limits and accepted divergences

**Every divergence row now lives in [`docs/benchmarks/metrics-differential-ledger.md`](benchmarks/metrics-differential-ledger.md).** This section used to carry a single row, `promql-expression-depth-cap`, with the rule that *a second metrics divergence graduates this row to its own file*. Issue #461 landed several, so the rule fired: the row moved, with its route, both statuses and its evidence, and this section keeps only what the ledger table cannot hold — the measurements below of what the cap does **not** cover.

**The same cap governs `match[]`** on `/api/v1/series`, `/api/v1/labels` and `/api/v1/label/{name}/values`. An over-deep `match[]` value was previously a `422 execution` here; it is now `400 bad_data`, which moves this surface **toward** the reference — Prometheus parses `match[]` with a selector-only parser, so a non-selector value is a parse-time `400` there too.

**What this cap does not cover.** The cap is measured on the *parsed tree*, so it exists only once `promql_parser::parser::parse` has returned. Parsing itself recurses on **grammar nesting**, a pre-existing property the vendored parser documents in its own source (`vendor/promql-parser/src/parser/ast.rs:2318-2329`). The quantity that drives it is the **number of nesting levels**, not the query's size. Measured on right-deep expression nesting — `1 + (1 + (1 + … 1 …))` — release build, 2 MiB stack, `parse` only:

| nesting levels | `parse` |
|---|---|
| 100,000 | returns |
| 150,000 | **stack overflow; the process aborts** |

**The threshold lies in `(100,000, 150,000]` levels and is deliberately not established.** Bisecting it requires repeatedly parsing hundreds of thousands of nesting levels, and its value changes no decision recorded here.

**Byte counts belong to a spelling, never to the residual.** The same nesting is `6N + 1` bytes written as `1 + (…` and `4N + 1` written as `1+(…`. The smallest input observed to abort is **600,001 bytes**, at the tighter of the two spellings. **No lower bound on size is claimed** — a denser spelling, or a different nesting construct, could abort on less. Do not read any byte figure here as a size floor below which input is safe.

A 2 MiB `DefaultBodyLimit` (axum's default; nothing in this workspace calls `DefaultBodyLimit` at all, so no route raises or lowers it) caps how many levels can arrive at once — **at least 524,287** of them, at the tighter of the two spellings measured (`4N + 1` bytes; the looser `6N + 1` admits 349,525). It is a ceiling on input, not a threshold on behaviour: the largest query that fits at either spelling aborts.

**The trigger is nesting through the expression rule — not length, and not tree depth.** Controls, same build and stack: `((((… 1 …))))` at 400,000 levels (800,001 bytes) parses and returns; `- - - … - 1` at 1,000 unary operators (2,001 bytes) parses and returns, folding to a tree of depth **1**.

**Every figure above is this toolchain, target and profile** (release, 2 MiB stack); a compiler change moves all of them.

**So:** this cap closes every *width* vector — the flat chains this issue was filed for, including the 4,881-byte `GET` — because those parse iteratively and the guard runs on the finished tree. It does not close deep grammar nesting, which aborts inside the parser before any after-parse guard exists to run. That residual is pre-existing, is not introduced by this change, and is closed only by converting the parser's grammar recursion.

---

## 4. Traces query API

#### Errors (§4.1-§4.5)

Every error a §4 **handler** writes — every row of every table below — is
a **bare `text/plain` body**: the message and nothing else, no JSON, no
keys, no trailing newline, under `Content-Type: text/plain; charset=utf-8`
and **no** `X-Content-Type-Options`. Issue #384 replaced PulsusDB's
earlier `{"status","errorType","error","position"?}` envelope with it; a
parse error's byte offset now travels inside the message, as the
reference's `line, col` does.

That container is the reference's, and *which* reference writer matters.
Tempo has two and they disagree:

- **The query frontend** serves every user-facing `/api/*` query route
  (`cmd/tempo/app/modules.go:500-512` @ grafana/tempo v3.0.2, each
  `base.Wrap(queryFrontend.…Handler)`). Its 4xx rejections are
  `*http.Response` values built with a **nil `Header` map**
  (`httpInvalidRequest`,
  `modules/frontend/metrics_query_range_handler.go:266-272`;
  `extractTenant`, `modules/frontend/util.go:15-25`; the same literal
  recurs across the search, tag, trace-by-ID and metrics handler files),
  which `handler.ServeHTTP` copies out verbatim
  (`modules/frontend/handler.go:113-116`: `copyHeader`,
  `WriteHeader`, `io.Copy`). Nothing sets a header, so Go sniffs the
  content type and no `nosniff` is emitted; nothing appends a terminator.
- **The querier's own handlers** call `http.Error`, which *does* set both
  headers and *does* append a newline — but they are registered only
  under `path.Join(api.PathPrefixQuerier, …)`
  (`cmd/tempo/app/modules.go:438-459`, `PathPrefixQuerier = "/querier"`
  at `pkg/api/http.go:67`), an internal path no client meets.

**This is not §2's container**, and the difference is exactly one header:
§2's writer (Loki's `WriteError`, `pkg/util/server/error.go:46-52` @
grafana/loki v3.7.4) sets `X-Content-Type-Options: nosniff` and this one
does not. The two agree on the content type and on the absent terminator.
The status code is the whole machine-readable classification on this
surface — there is no `errorType` field (the reference has none either).

**Scope of that claim: handler-written errors only.** Rejections made
*above* the handlers — the router's own `404`/`405`, and the server-wide
request-deadline layer's `408` — are not written by this container. They diverge
from the reference, they are pre-existing, and #384 neither changed nor
covers them (the same boundary #264 drew for §2).

### 4.1 Trace fetch

```
GET /api/traces/v1/trace/{traceId}         → OTLP-shaped trace (protobuf or JSON by Accept)
GET /api/traces/v1/trace/{traceId}/json    → force JSON
```

`traceId` is hex (16 or 32 chars, left-padded). `404` with the plain-text body `trace not found` when absent.

**Content negotiation.** The default representation is OTLP-canonical JSON (protojson: hex trace/span ids, camelCase fields, 64-bit integers as strings) with `Content-Type: application/json`; no `Accept` header means JSON. `Accept: application/protobuf` (or its request-side alias `application/x-protobuf`) selects the protobuf `TracesData` encoding, returned as `Content-Type: application/protobuf` — deliberately asymmetric with OTLP *ingest*, which uses `application/x-protobuf` per the OTLP/HTTP spec; the query response follows the Tempo/Grafana client convention instead, and never emits `x-protobuf`. Quality values are honored per RFC 9110 (`;q=` weights, exact `type/subtype` > `type/*` > `*/*` specificity, `q=0` excludes; an equal-quality tie resolves to JSON). An `Accept` header under which neither served representation is acceptable (e.g. `text/plain`, or every matching range at `q=0`) is rejected with `406`. The `/json` suffix forces JSON unconditionally — it never consults `Accept` and never returns `406`. Every response from the negotiating route (success or error) carries `Vary: accept` per RFC 9110 §12.5.5; the `/json` route serves one representation and never adds `accept` to `Vary` (the global compression layer independently appends `accept-encoding` where applicable).

**Response shape.** One `TracesData` assembling every stored span of the trace; at-least-once ingest duplicates are deduplicated by span id at read time. Spans are returned in a canonical order — ascending `(startTimeUnixNano, spanId)` — so responses are byte-deterministic regardless of storage read order.

**The JSON representation emits proto3 defaults; the protobuf one is byte-identical to the reference's.** Our protojson spells out every field at its default — `"traceState":""`, `"flags":0`, `"attributes":[]`, `"droppedAttributesCount":0`, `"schemaUrl":""`, and a materialised `"status":{"message":"","code":0}` — where the reference omits them and answers `"status":{}`. Ids are hex rather than base64, `kind` and `status.code` are numbers rather than enum names, and the reference's own v1 route names its top-level key `batches` where ours (and its v2 route) say `resourceSpans`. **No consumer branches on any of this** — an omitted proto3 field and a field present at its default are the same value — and the protobuf representation, which is what the trace datasource requests on trace-by-ID, matches the reference byte for byte. Recorded with both sides measured as `traces-fetch-json-emits-proto3-defaults` in `docs/benchmarks/traces-differential-ledger.md`; note that this is the opposite of the omission rule §4.2 and §4.4 follow for a zero `completedJobs` and a zero sample `value`, which is why it is written down rather than left to be inferred.

**Absent nullable submessages are materialised on read (issue #474).** OTLP makes `ResourceSpans.resource`, `ScopeSpans.scope` and `Span.status` optional, and ingest stores exactly what the sender wrote, so a sender that omitted any of the three has that absence stored permanently. On every trace fetch each `None` becomes a present, default-valued message, which protobuf encodes as the field key plus a zero length rather than as an omitted field — the same shape the reference emits, because its columnar schema has no "absent" state to store and it re-materialises all three on read. The fix is on the read path, not the write path, so it also repairs rows already stored. Only an absent submessage is filled: a `status` the sender actually sent keeps its code and message, and a present-but-empty `resource` stays exactly as sent. **A resource that is present with zero attributes is not "repaired" into anything** — the reference emits the same present-but-empty resource, and a Grafana Tempo datasource drops the spans under it either way; that is reference behaviour, not a PulsusDB defect, and no `service.name` is synthesised.

**Errors** are always the §4 plain-text body, regardless of `Accept` — an error never re-encodes as protobuf:

| Cause | HTTP |
|-------|------|
| Malformed `traceId` (not 16/32 hex chars) | `400` |
| Trace absent | `404` |
| No acceptable representation under `Accept` | `406` |
| ClickHouse read timed out | `504` |
| Unclassified ClickHouse/internal failure (incl. undecodable or unsupported stored payloads) | `500` |

**Named residual — the absent-trace `404` body.** Tempo answers an absent trace with an **empty** `404` carrying no `Content-Type` at all; PulsusDB answers `trace not found` as `text/plain`. This is deliberate and is the one §4 error where our body differs from the reference's: `api_conformance`'s fetch-surface mounting oracle distinguishes *mounted-but-absent* from *unmounted* precisely by the body being non-empty (axum's unrouted `404` is empty), and `route_inventory` cannot stand in for it — that guard is a hermetic scan of the router **source**, with no server and no request, so it proves the route is registered in the tree, never that a running spawn in a given mode serves it. Ledgered as `traces-absent-trace-404-body` in `docs/benchmarks/traces-differential-ledger.md`. The `406` row is likewise PulsusDB-native (Tempo ignores an unacceptable `Accept` rather than rejecting) — an RFC 9110 behaviour #384 did not change, only re-containered.

### 4.2 `GET /api/traces/v1/search`

| Param | Notes |
|-------|-------|
| `q` | TraceQL query (preferred) |
| `tags`, `minDuration`, `maxDuration` | legacy search params, compiled to TraceQL internally (below) |
| `start`, `end` | unix s / ns / RFC3339 (§1's trace-API forms; integers with magnitude ≥ 10^12 are nanoseconds, smaller ones seconds); **both required**, `end > start` |
| `limit`, `spss` | result cap (default 20) and spans-per-spanset cap (default 3); positive integers |

**`q` vs legacy params:** mutually exclusive — supplying `q` together with any of `tags`/`minDuration`/`maxDuration` is a `400`, never silent precedence. Supplying neither is a valid time-range-only search (`{}`).

**Legacy compilation:** `tags` is logfmt — space-separated `key=value` pairs; a value may be double-quoted to contain spaces/`=`, and inside quotes `\"` and `\\` are the only escapes. Each pair compiles to an **unscoped** `.key="value"` conjunct; `minDuration`/`maxDuration` compile to `duration >= <lit>` / `duration <= <lit>`; all conjuncts join with `&&` in one `{ … }` and the result goes through the ordinary TraceQL parser (one validation path). The grammar is enforced strictly: a bare key with no `=`, an empty key, an unterminated quote, an `=` or `"` inside an **unquoted** value (quote the value instead), a quoted value not followed by whitespace/end-of-input, or any escape other than `\"`/`\\` is a `400` whose message names the byte offset into the decoded `tags` value.

**Duration literals** (in `q`, e.g. `duration > 2s`): an **unsigned** decimal number (integer or fraction — `2`, `1.5`, `.5`) **immediately** followed by exactly **one** unit from `{ns, us, µs, ms, s, m, h}`. No sign; no compound literals (`1h30m` is rejected). A fractional literal is valid only if it resolves to an exact whole number of nanoseconds (`0.5s` = 500000000ns is valid; `0.1ns` is a positioned parse error) — no rounding, no truncation.

**Static keywords** (in `q`, issue #335): thirteen bare words are VALUES wherever an operand or a hint value may appear, never attribute names — `true`, `false`, the three `status` keywords (`ok`/`error`/`unset`) and the **six** `kind` keywords (`unspecified`/`internal`/`server`/`client`/`producer`/`consumer`), plus `minInt` and `maxInt`, which resolve to the 64-bit integer bounds (-9223372036854775808 / 9223372036854775807 — the pinned Tempo v3.0.2 image is `linux/amd64`, so its platform int is 64-bit). The identifier spelling does not survive: `{ .a = minInt }` is the integer, and renders as one. `unspecified` is the kind PulsusDB itself emits for a span with no OTLP kind — it appears in `by(kind)` group values, `select(kind)` projections and `rate() by(kind)` series labels — so a filter written back from one of those responses (`{ kind = unspecified }`) is accepted, matching Tempo v3.0.2. Every other bare identifier in a value position is a positioned parse error (`400`), so a typo never silently becomes an attribute reference.

**Hint clauses** (in `q`): a query may carry **more than one** trailing `with(...)`, and the **last one wins** — `{ .a = 1 } with(a=1) with(b=2)` means `with(b=2)`, matching Tempo v3.0.2, whose root hint rule is recursive and whose action ASSIGNS the clause rather than appending it. Accumulation happens inside a single clause's comma list (`with(a=1, b=2)`), not across clauses. A hint value is any static keyword or literal above.

**Attribute paths** (in `q`) are a single unbroken token: no whitespace on either side of any `.` separator, for every scope — `{ . hi }`, `{ span . hi }`, `{ span. hi }`, `{ span .hi }` and `{ .a .b }` are all positioned parse errors (`400`), matching Tempo v3.0.2. Whitespace *before* a leading `.` is unaffected (`{ .hi = 1 }`, `{ .a = .b }` stay valid).

**Colon-scoped intrinsics** (`span:id`, `trace:duration`, `event:name`, `link:spanID`, `instrumentation:version`) bind the scope keyword to the `:` **on the left only**, for every colon scope and every operand position: `{ span :id = "x" }` is a positioned parse error (`400`), while `{ span: id = "x" }` is valid. This asymmetry with `.` above is deliberate Tempo v3.0.2 parity, not an inconsistency — the reference likewise rejects only the pre-colon gap and accepts a space, tab or newline after the colon.

**Operator precedence** (in `q`, issue #335 — verified against grafana/tempo:3.0.2, tightest first): `^`, then `* / %`, then unary `-`, then `+ -`, then the comparison operators, then `&&`/`||`. Two placements are deliberately unusual and match the reference rather than the common convention: **`^` is right-associative while every other arithmetic level is left-associative**, and **unary `-` sits between `* / %` and `+ -`** (`-2 ^ 2` is -4, not 4; `-.a * 2` is `-(.a * 2)`). `^` also carries a **deliberate value divergence**: the reference's integer `^` swaps its operands (`3 ^ 4` answers 64, `3.0 ^ 4` answers 81.0 — the same operands, decided by literal spelling), which is a defect we do not copy. PulsusDB computes `lhs ^ rhs` throughout, so `{ .a = 2 ^ 3 ^ 2 }` is `2 ^ (3 ^ 2)` = 512 here and 64 there. See `traceql-pow-integer-operand-swap` in docs/benchmarks/traces-differential-ledger.md. **`&&` and `||` share one precedence level and are left-associative** at both the field and the spanset level — `{ .a=1 || .b=2 && .c=3 }` is `((.a=1 || .b=2) && .c=3)`, and `{A} || {B} && {C}` is `({A} || {B}) && {C}`. Spanset structural operators bind tighter than `&&`/`||` and are left-associative (§ below).

**Regex operators** (`=~`/`!~`) are full-value anchored (`^(?:…)$`), matching the label-matcher convention across PulsusDB's query languages. `!=`/`!~` on an attribute match spans **lacking the key entirely** as well as spans whose value differs.

**Structural operators** (issue #172, completed by #183): `{A} > {B}` (child — spans matching B whose **direct parent** matches A), `{A} >> {B}` (descendant — spans matching B with **any transitive ancestor** matching A, i.e. strictly below an A-matching span in the parent chain; an A-matching span is never itself yielded as a descendant, so a span is never its own descendant even under malformed cyclic parent links), `{A} < {B}` (parent — spans matching B that are the **direct parent** of an A-matching span), `{A} << {B}` (ancestor — spans matching B that are a **transitive ancestor** of an A-matching span; a span is never its own ancestor, cycle-safe), `{A} ~ {B}` (sibling — spans matching B sharing a `parent_id` with a **distinct** span matching A; spans with an all-zero `parent_id` — roots/no recorded parent — have no parent to share and **never** match `~`). Each of the five base relations also has a **negated** form (`!>`, `!>>`, `!<`, `!<<`, `!~`) and a **union** form (`&>`, `&>>`, `&<`, `&<<`, `&~`). The trace matches iff the relation's result set is non-empty. For the **plain** relation the result set is the **right-hand side's matching spans only** (`matched`, `spanSets`, aggregate filters, `select()`, and the ordering sort key all reflect the RHS spans — deliberately different from `&&`'s union of both operands' matches). The **negated** form returns the RHS spans that do **not** satisfy the relation; the edge case that matters: with an **empty LHS** (no A-matching span in the trace) every RHS span is a negated match, so the whole RHS set is returned. The **union** form returns **both** participating sides — the RHS spans satisfying the relation plus the LHS spans that participate (e.g. `{A} &> {B}` returns the child-side B spans **and** the parent-side A spans). Structural operators bind **tighter** than `&&`/`||` and are left-associative: `{a} && {b} > {c}` ≡ `{a} && ({b} > {c})`, `{a} > {b} > {c}` ≡ `({a} > {b}) > {c}`; parentheses override. Relations are evaluated over the trace's **hydrated** span set — window-bounded and capped at the 10,000-spans-per-trace hydration limit (an overflowing trace is already reported `partial`) — so an out-of-window intermediate hop breaks a `>>`/`<<` chain, and orphan spans (non-zero `parent_id` with no hydrated parent) never match `>`/`>>` on the child side. `>=`/`<=` between spansets are not real Tempo operators and stay 400 with the named construct.

**Field-vs-field comparison** (issue #183, `comparison.rhs_attribute`): a comparison whose right-hand side is another attribute or intrinsic, e.g. `{ .a = .b }`, `{ duration = span.slo }`, `{ .a > .b }`, `{ .a = span:duration }` — either side an attribute, a bare intrinsic, or a colon-scoped intrinsic (issue #335 class D2 widened the right-hand side to all eighteen colon forms, which the reference has always accepted), with operators `= != < <= > >=` (a regex operator against a field RHS is rejected 400). Values are resolved per candidate span and compared engine-side under a **type gate** (verified against grafana/tempo:3.0.2): the two operands must be the same type — a **cross-type** pair (one numeric, one string) is **no match for every operator**, even on coincident text (`.a = "5"` string vs `.b = 5` int is not a match, and neither is `!=`). Same-type operands compare normally for all six operators: both numeric ⇒ numeric compare; both string ⇒ lexicographic string compare (`apple < banana`). An absent attribute key on either side is no match. (Arithmetic operands — e.g. `{ .a = .b + 1 }`, `{ .duration_ms * 1000 > 5000 }` — are supported: single-attribute arithmetic with literal operands pushes to a column-side predicate; genuinely cross-attribute or non-total (`/ % ^`) forms evaluate engine-side — see §4.4.) **Bare boolean statics** `{ true }` / `{ false }` and **unary field negation** `{ !(.a = 1) }` / `{ !(.a = 1 && .b = 2) }` (`logic.not`) are also accepted; a spanset-level `!{…}` is rejected 400.

Response: `{"traces":[...],"metrics":{"completedJobs":<n>,"totalJobs":<n>}}`. **`metrics` is `tempopb.SearchMetrics` and nothing else** (issue #464): PulsusDB runs one search plan, so `totalJobs` is `1` and `completedJobs` is `1` on a complete result and `0` on a truncated one — and a zero `completedJobs` is **omitted**, protojson-style, so a truncated search answers the bare `{"totalJobs":1}`. An absent key is a zero, not a missing value. The block previously carried `partial`, `limit` and `returned`; none of the three is a field of `tempopb.SearchMetrics` (`pkg/tempopb/tempo.proto:164-172` @ v3.0.2), and a strict Tempo client rejects the whole response over one unknown field, so they are **removed** — a breaking change to this route and to its `/api/search` alias. `limit` is the caller's own request parameter and `returned` is the length of `traces`; the truncation signal moved onto `completedJobs < totalJobs`, the pair the reference's own search route uses for incompleteness (see `docs/benchmarks/traces-differential-ledger.md`, `traceql-search-metrics-completed-jobs`). Each trace carries `traceID`, `rootServiceName`, `rootTraceName`, `startTimeUnixNano` and `durationMs` — **both TRACE-level, and both the trace's own envelope rather than the root span's window** (issue #464): `startTimeUnixNano` (string nanoseconds) is the earliest span start of the whole trace and `durationMs` is `max(span end) - min(span start)` over the whole trace, in integer milliseconds, **omitted when it is zero milliseconds** — see below. Both are computed over the trace-wide root read, so they are exact even when a span sits outside the requested window, and they differ from the root span's own window whenever a child starts before the root (clock skew), ends after it, or extends the trace past it. `rootServiceName` and `rootTraceName` remain the ROOT SPAN's, and root metadata comes from the **whole** trace, so a root that predates `start` is still reported correctly. An **empty** `rootServiceName` is replaced by the literal `<root span not yet received>`, the substitution the reference applies to every trace of a search response; `rootTraceName` is never substituted, on either system. The 8192-byte truncation described below cannot turn a non-empty string into an empty one, so the two rules do not interact. Each trace also carries `spanSets`: for a plain query a **single** entry of `{"matched":<total matched spans>,"spans":[...]}` where each span summary carries `spanID`, `startTimeUnixNano`, `durationNanos` (**string** nanoseconds), plus `name` and an `attributes` list (`{"key","value":{"stringValue"}}`) under the projection rule below. **The matched-span projection** (issue #479): `attributes` carries the fields the query filtered on with a **single-field condition that matched THAT span**, plus every `select()`ed field, keyed by the **bare** attribute name — `http.method`, never `span.http.method`; the scope is not part of the wire key, though it IS part of the dedupe identity, so `{span.foo="S" && resource.foo="R"}` emits **two** entries both keyed `foo`. **A condition is single-field when exactly ONE DISTINCT field appears across BOTH its operands** — it is not the operand SHAPE `(field, literal)`: `{.a = 1}`, `{.duration_ms * 1000 > 5000}` (which projects the RAW stored `duration_ms`, not the computed product) and the degenerate same-field comparisons `{.a = .a}`, `{name = name}`, `{nestedSetLeft = nestedSetLeft}` and `{resource.service.name = resource.service.name}` all project that one field, matching the reference; a comparison naming two DIFFERENT fields does not. The `name` key is present **only** when the query referenced `name` (as a condition that matched, or through `select(name)`) **and** the collected name is non-empty; an empty name emits no key at all, while an empty attribute VALUE is emitted as `{"stringValue":""}`. **Seven fields the response envelope already carries are never projected as attributes** — the span name (it fills the `name` field instead), the span duration, the trace duration, the root service name, the root name, the trace id and the span id — so `{duration>1s}` and `{} | select(duration)` both add nothing, and `{} | select(span:id)` is byte-identical to `{}`. **`span:childCount` is never projected** in either position. In this wave a **negated** attribute condition (`!=`, `!~`, or any leaf under `!`/`= nil`) and a **multi-field** condition — one naming two or more DISTINCT fields (field-vs-field between two DIFFERENT fields, cross-attribute arithmetic that did not push down, boolean-vs-boolean, event/link set comparisons, trace-context leaves, folded constants) — and a `by()` key project nothing; each is recorded in `docs/benchmarks/traces-differential-ledger.md`. **The two duration fields differ by level and by unit** (issue #458): the trace carries `durationMs` (`TraceSearchMetadata.durationMs`, `pkg/tempopb/tempo.proto:139` @ v3.0.2 — integer milliseconds) and a span carries `durationNanos` (`Span.durationNanos`, `pkg/tempopb/tempo.proto:160`, filled from `span.DurationNanos()` at `pkg/traceql/engine.go:311` @ v3.0.2 — nanoseconds, rendered as a JSON **string** because protojson renders a `uint64` as one). A span carries no `durationMs`; it never did in the reference. **A zero-width span carries no `durationNanos` key at all** — protojson omits a default-valued scalar, and this is captured behaviour rather than a reading: against the pinned reference a `0` ns span returns `{"spanID":…,"name":…,"startTimeUnixNano":…}` with the field absent, while a `1` ns span returns `"durationNanos":"1"`. A client must therefore treat an absent field as zero, not as missing data. **The same omission applies to the trace's `durationMs`, and there the threshold is a MILLISECOND**: `0`, `1` and `545000` ns traces all come back from the pinned reference with no `durationMs` key, because all three round to `0` ms, while a `42000000` ns trace returns `"durationMs":42`. One deliberate divergence remains at that field: the reference truncates it to a 32-bit unsigned integer (`pkg/traceql/engine.go:295` @ v3.0.2), so a trace longer than ~49.7 days **wraps**. PulsusDB **saturates** at `4294967295` ms instead. Measured against the pinned reference, an `i64::MAX`-nanosecond trace returns `durationMs: 2077252342` there and `4294967295` here, and a 2^53 + 1-nanosecond trace returns `417264662` there and `4294967295` here — the same number for two different inputs, which is what saturation means and wrapping does not. Neither value is the true duration; a saturated one is at least a true lower bound, and a wrapped one is a plausible-looking lie. The same rule covers the other three integers this response carries: `startTimeUnixNano` at both levels and a span's `durationNanos` are 64-bit **unsigned** on the wire, so a value below zero — reachable only from a write that bypassed ingest — is emitted as `0` and never as a negative number a strict client would refuse. See `docs/benchmarks/traces-differential-ledger.md`, `traceql-search-duration-ms-saturates-not-wraps`.

**`by()`/`coalesce()` grouped spanSets (issue #193).** A `| by(<key>)` stage takes **exactly one** grouping key and reshapes a trace's response into **one `spanSets` entry per distinct group key** (in first-appearance order), each carrying a group `attributes` list — the group-key attribute is named with Tempo's **`by(<key-expr>)`** form (`by(name)`, `by(resource.service.name)`, …) and carries the value **rendered by its TraceQL type** (verified live against Tempo v3.0.2): a string/attribute → `{"stringValue"}`; a numeric attribute → `{"doubleValue":<f>}`; a numeric intrinsic (`nestedSetParent`/`Left`/`Right`, `span:childCount`) → `{"intValue":"<n>"}`; `status`/`kind` → their lowercase **keyword** `{"stringValue"}` (`"ok"`/`"error"`/`"unset"`, `"server"`/`"client"`/…); a `duration`/`traceDuration` → Go's `time.Duration.String()` form as `{"stringValue"}` (`"1.5s"`, `"500µs"`); a span lacking the key groups under a null value — alongside that group's own `matched` total and its `spss`-capped `spans`. `spss` is applied **per group** (on the full pre-`spss` matched set), so a group never under-reports its membership. A `| coalesce()` stage **merges whatever survives at its written position back into the single flat spanSet** (no per-spanSet `attributes`). **The pipeline is one ordered fold, and every stage's written position decides what it sees** (issue #492): each stage maps the current spanSet list to a new one — `by()` sub-divides **each** current spanSet and **appends** its attribute to that spanSet's existing ones (so `by(name) | by(kind)` gives one spanSet per distinct `(name, kind)` pair carrying both attributes in written order, not one per `kind`); `coalesce()` merges the list into one attribute-free spanSet; and an aggregate filter (`count() > 2`, `max(duration) > 1s`, …) keeps the spanSets whose aggregate passes and drops the rest, so `by(name) | count() > 2` filters the GROUPS while `count() > 2 | by(name)` filters the whole matched set and then groups it. An empty list ends the trace: `{…} | by(name) | count() > 3` returns **no trace at all** when no group holds more than three spans, while `{…} | count() > 3 | by(name)` returns every group of a trace whose matched set does. A trace's flat `matched`/`spans` view is the **union of the spanSets that survive**, which is why `by(name) | count() > 2 | coalesce()` reports the three spans that passed and `by(name) | coalesce() | count() > 2` reports all four. A trace's position in `traces[]` never moves for any of this: the ordering key stays the max timestamp of the trace's exactly-**matched** spans (the ordering contract below), matching the reference, whose inter-trace order is likewise insensitive to the pipeline. Float group keys are value-normalised — `-0.0` folds into `+0.0` and every NaN into one group — matching the reference. **Every by-key that resolves to a per-span scalar is grouped** — the physical columns (`name`/`resource.service.name`/`duration`/`status`/`kind`), the nested-set intrinsics (`nestedSetParent`/`Left`/`Right`), the trace-level intrinsics (`traceDuration`/`rootName`/`rootServiceName`/`span:childCount`/`span:id`/`span:parentID`/`trace:id`/`statusMessage`/`instrumentation:name`/`instrumentation:version`), and attributes — never a silent flat fallback. **One key, and that key is a field expression** (issue #335): the reference's grouping stage carries a single operand and that operand is a full field expression, so `by(.a + 1)`, `by(-.a)`, `by(!.a)`, `by((.a))` and `by(.a = 1)` all parse — and a comma list (`by(.a, .b)`) is a **positioned parse error** (`400`), matching Tempo v3.0.2, which has no comma in that production. PulsusDB used to accept and serve the comma list; that accept was withdrawn because it was the one shape a user could build on here and could not run there (`traceql-spanset-by-multi-key-withdrawn` in docs/benchmarks/traces-differential-ledger.md). The METRICS `by(...)` clause (`… | rate() by(.a, .b)`) is a different production and keeps its comma list. A key that does not resolve to a single per-span value — an arithmetic, unary or comparison expression — is a clean `400` naming the rendered key. The only excluded by-keys are the **span-event / span-link intrinsics** (`event:name`/`event:timeSinceStart`/`link:spanID`/`link:traceID`): a span carries a *collection* of events/links, so there is no single scalar group value, and grouping by one is a clean **`400`** (never a flat 200). A plain (non-`by()`, or `by()`-then-`coalesce()`) response is byte-identical to the single-spanSet shape above.

**Response string truncation (issue #57 re-audit, owner-approved).** `rootServiceName`, `name` (span/root), and any `select()`-projected attribute `stringValue` are truncated at a hard **8192-byte** ceiling: strings at or under the cap are returned byte-identical; a longer string is cut to its first **2048 UTF-8 code points** instead (2048–8192 bytes, depending on code-point width — a UTF-8 code point is at most 4 bytes, so the 2048-code-point fallback itself never exceeds the 8192-byte ceiling). This bounds the search path's transient result-block memory at the source (docs/schemas.md §7) and is invisible for realistic telemetry (span/service names and projected attribute values are almost always well under 8 KiB); it is a documented, visible change only on pathological rows.

**Ordering contract:** `traces[]` is ordered by the max timestamp of each trace's exactly-matched spans, **descending**, with `trace_id` ascending as the tiebreak — deterministic under timestamp ties.

**`X-Pulsus-Explain: 1` (issue #492):** this route answers the header, with the same contract as the logs and metrics routes — one execution, never a second run. The response gains one **sibling key** `explain`, carrying §2.1's shape: `{"result_type":"traces","routing":null,"stages":[{"name","sql","note"|null},...]}`, plus the additive `plan` key when a compiled plan is present. `routing` is always `null` here — a search never routes between tables. Without the header nothing changes, and `traces`/`metrics` are byte-identical either way.

**Partial results:** the response returns at most `limit` traces (the top-K under the ordering contract above). Candidate generation and consumption are capped **separately** from `limit`, both at `PULSUS_TRACEQL_MAX_CANDIDATES`: each candidate generator is a top-K-by-recency read of that depth, and the merged candidate stream is evaluated up to that many candidates — so the engine may evaluate up to `PULSUS_TRACEQL_MAX_CANDIDATES` candidates even for a small `limit` (stopping earlier only when no unseen candidate can still enter the top `limit`). The result is **incomplete** — `completedJobs` omitted (zero) against `totalJobs` `1` — whenever any internal bound engaged before natural exhaustion: a candidate generator hit its `PULSUS_TRACEQL_MAX_CANDIDATES` depth, the candidate consumption ceiling was reached with candidates still unconsumed, or a single trace exceeded the 10,000 hydrated-spans-per-trace cap (that trace is evaluated on its truncated span set, never silently reported complete). A complete result answers `{"completedJobs":1,"totalJobs":1}`. Our signal is exact and deterministic where the reference's is a racy shard-arrival count, and it is one-directional: we never report completeness we do not have, but a client reading the pair as "shards outstanding" over-reads `{"totalJobs":1}` when the truncation was a single trace's spanset overflow. The request's `limit` and the returned trace count are no longer echoed back — the caller holds the first and `traces.length` is the second.

**Errors** use the §4 plain-text body; a TraceQL parse error names its byte offset into the rejected expression (`q`, or the `query` parameter validated below) **inside the message**, and so does a `tags` logfmt error (byte offset into the decoded `tags` value) — there is no separate `position` field to read:

| Cause | HTTP |
|-------|------|
| Malformed `q` / params / `tags` logfmt / `q`+legacy conflict / unsupported operator-type combination. **Three fields are excluded from the last of these** (issue #476): `resource.service.name`, `instrumentation:name` and `instrumentation:version` carry no implied type in the reference, so a cross-type `=`/`!=` there — `{resource.service.name=12345}`, `{instrumentation:name=5}`, and the bare-truthiness forms `{ resource.service.name }` / `{ instrumentation:name }` — is a **`200` matching no span**, not a `400`. A service literally NAMED `12345` is still not returned: the operand is a number, not the string it renders as. A cross-type `=~`/`!~` and any ordered comparison at those fields stay `400` | `400` |
| TraceQL expression text of **more than 131,072 bytes** (`traces_api::querytext::MAX_QUERY_EXPRESSION_BYTES`, an **inclusive** maximum — exactly 131,072 bytes is accepted, 131,073 is the shortest rejected; grafana/tempo v3.0.2's `max_query_expression_size_bytes`, defaulted `128 * 1024` at `modules/frontend/config.go:141` and enforced `>` at `modules/frontend/pipeline/async_query_validator_middleware.go:45`). Scoped exactly as the reference scopes it — the `q`/`query` **parameter** on search and TraceQL metrics only (the reference's validator is wired at `modules/frontend/frontend.go:159` search, `:215` query_range, `:229` query_instant, and reads `q` then `query` on all three); NOT the legacy `tags`/`minDuration`/`maxDuration` params (whose compiled expression the reference never measures), not tag discovery, not trace-by-ID. On **search**, `query` carries the cap but is NOT an alias for `q`: the reference's search request parser reads `q` alone (`pkg/api/http.go:180`), folding a lone `query` into its legacy tag map, so `query` never becomes the searched expression here either. One deliberate divergence: the reference's selection is last-write-wins, so an over-cap `q` accompanied by an under-cap `query` escapes its cap and is then executed unbounded — PulsusDB caps both parameters and rejects that shape. Note the boundary is the opposite of LogQL's, whose reference compares `>=` on the same number. Not reachable over the wire today: these routes are `GET`-only and `http::Uri` caps a request target at 65,534 bytes (the #296 transport band) | `400` |
| A `query` parameter on **search** that does not parse as TraceQL, e.g. `?query=%7B&start=…&end=…`. The reference's query-frontend validator parses the parameter it selected (`traceql.ParseNoOptimizations` at `modules/frontend/pipeline/async_query_validator_middleware.go:49`, wrapped as `invalid TraceQL query: …` at `:54`) **after** the size check at `:45` and regardless of pipeline, so on search it rejects text its own request parser never reads as an expression. PulsusDB reproduces that: the rejection is `400` with the reference's `invalid TraceQL query: ` prefix and a byte offset inside the message, and `query` still does not become the searched expression. Unlike the size cap this IS reachable over the wire — a malformed expression is short. The validator's second half is reproduced too (#328): `traceql.Validate` (`:51`) is ported as the route-independent `pulsus_traceql::validate`, so an expression that parses but fails the semantic checks (operand types, per-type operator sets, regex literals via the shared RE2 verdict, intrinsic `= nil`, quantile bounds, `by(...)` arity, `topk`/`bottomk` limits, `compare()` exclusivity) is a `400` with the same `invalid TraceQL query: ` wrapping and NO byte offset (the reference's Validate errors name none either), on `q`, on the shadow `query`, and on the metrics parameter alike. The narrowed residual is #336's: the regex verdict's `Unknown` classes — enumerated from `pulsus-re2`'s own return sites and measured per class: lookarounds (`(?=` and friends, the commonest member), out-of-table `\p{…}` properties, `\u`/`\U` escapes, a trailing backslash, non-portable `(?…` heads, repetition beyond 1000 or applied to a repetition, over-budget compilations — are accepted on the validation-only shadow parameter where the reference rejects (storage-bound paths still reject at execution) — ledgered with the full class table as `traceql-validate-re2-unknown-residual`, alongside `traceql-validate-nil-spelling-conflation` for the `!(x != nil)` spelling | `400` |
| Scan or memory budget exceeded (`PULSUS_TRACEQL_SCAN_BUDGET_ROWS` rows read, read/result byte ceilings, the engine's 256 MiB retention budget, or the phase-1 candidate-generator's `PULSUS_TRACEQL_GENERATOR_MAX_MEMORY_BYTES` memory ceiling) — too broad to bound, never silently slow or quietly incomplete | `422` |
| ClickHouse read timed out | `504` |
| Unclassified failure | `500` |

### 4.3 Tags

```
GET /api/traces/v1/tags                   ?scope=&start=&end=      (scoped response shape)
GET /api/traces/v1/tag/{tag}/values       ?q=&start=&end=          (typed values)
```

Tag **names** are served exclusively from `trace_tag_catalog` (bounded, deduplicated) — never by scanning span payloads or the attribute index. Tag **values** are served from one of three places, and which one is decided by the `{tag}` and by whether `q` narrows (issue #478):

| lookup | read |
|---|---|
| an attribute key, no narrowing `q` | `trace_tag_catalog`, exactly as before — same query, same cost |
| an attribute key, narrowing `q` | `trace_attrs_idx`, intersected with the span set the query matches |
| `name` / `span:name` | `trace_spans` — the span-name column, through a day-grain projection |
| any other intrinsic (`status`, `kind`, `duration`, `span:id`, …) | nothing: the static vocabulary |

The **intrinsic** vocabulary (the `intrinsic` scope, and the closed `status`/`kind` value sets) is a static list derived from the TraceQL grammar and is served **without reading the store at all**: those values exist by definition rather than by observation, so a catalog lookup for them answers with whatever attribute happens to carry the same key. `name` is the one intrinsic whose values exist by observation instead, which is why it reads the store — and it reads `trace_spans` rather than the catalog, because the catalog's materialized view projects `trace_attrs_idx` alone and holds no span-`name` row.

| Param | Notes |
|-------|-------|
| `scope` | one of `event`, `instrumentation`, `link`, `resource`, `span` (the attribute scopes); `intrinsic` (the static vocabulary, no catalog read); `trace` (accepted, answers an empty list — it names no stored scope); `none`, the empty string, or omitted = every attribute scope, plus the `intrinsic` scope on the two scoped shapes. Case-sensitive and closed: anything else is a `400`, never silently widened |
| `{tag}` | `<scope>.<key>` for any attribute scope (`resource.`, `span.`, `event.`, `link.`, `instrumentation.`) scopes the lookup; a leading-`.` or a bare key that is not an intrinsic spelling is unscoped (values from the five attribute scopes, never the writer-reserved intrinsic ones). A bare or colon-scoped **intrinsic spelling** (`status`, `kind`, `span:kind`, `link:spanID`, …) is answered from the static vocabulary with no catalog read — so `span.name` is the attribute keyed `name` while `span:name` is the intrinsic. A `?` in the key is data, not syntax. What this route does with such a key is defined case by case by the `question_mark_keys` section of `crates/pulsus-server/tests/fixtures/reference-tag-values.json`, replayed against us by `crates/pulsus-server/tests/traces_tag_values_narrow_live.rs` and against the reference by `crates/pulsus-server/tests/trace_tag_values_differential.rs`. Those cases are the specification; this row does not restate them |
| `start`, `end` | **On the names routes**: accepted for client compatibility and ignored — the catalog has no timestamp column, so name discovery is time-less, and catalog entries can therefore **outlive** the 7-day span retention (the source `trace_attrs_idx` is TTL'd; `trace_tag_catalog` has no TTL). **On the values route**: they bound the reads that touch the span tables — the span-`name` values and any `q`-narrowed values — widened to the whole UTC days the window touches; an absent range means the last `PULSUS_TRACEQL_TAG_LOOKBACK` (24 h). An unnarrowed attribute-value read is still the time-less catalog read and ignores them. On **every** §4.3 route a range FAULT is a `400`: an unparseable bound, exactly one bound supplied, or `end` earlier than `start`. A bound of `0` counts as **not supplied**, so `start=0&end=0` is an absent range while `start=0&end=<t>` is a half-supplied one; `end == start` is accepted and answers that one UTC day |
| `q` | **narrows the value list** (issue #478). The conjuncts on the query's root `&&` spine are pushed into the read: an intrinsic comparison inline on the span columns, an attribute comparison as an index membership probe. Anything the lowering cannot use is **dropped, never rejected** — a query that does not parse, a subtree under `\|\|` or `!`, a negated attribute condition, a structural root, a pipeline stage, or a conjunct past the eighth. Every drop widens the answer, so the result is always a superset of the exact one, and an unparseable `q` is answered as if it were absent. **A `q` that is well-formed input and does not parse as TraceQL is never an error**: the query editor sends the whole half-typed expression on every distinct prefix a user types through, and rejecting it would break autocomplete for input the client cannot avoid sending. Two classes are rejected below the interpretation layer, by the HTTP transport, and both are faults a client can avoid: **raw invalid UTF-8 in the request target** is `400` before any handler runs (the same bytes percent-encoded are served `200`, so what is refused is a malformed request line rather than a `q` value), and an **enormous `q`** is refused by the transport past the 64 KiB request-target bound — measured by bisection at 65,493 bytes `200` and 65,494 refused, with the status `414` or `431` depending on how the request arrives (both were observed for the same 524,194-byte request on two machines). On this route that transport bound is tighter than the §4.2 expression cap, so it is what a client meets first. The v1 flat values route (§8.1) ignores `q` entirely |

Response shapes (native; the §8.1 Tempo aliases are projections of these):

```json
{"scopes":[{"name":"intrinsic","tags":["duration","event:name","…"]},{"name":"resource","tags":["env","service.name"]},{"name":"span","tags":["http.status_code"]}],"truncated":false}
{"tagValues":[{"type":"string","value":"checkout"},{"type":"int","value":"500"}],"truncated":false}
{"tagValues":[{"type":"keyword","value":"ok"},{"type":"keyword","value":"error"},{"type":"keyword","value":"unset"}],"truncated":false}
```

The third body is a static answer (`{tag}` = `status`). Static values carry the type `keyword`, not `string`: a client quotes a value only when its type is `string`, and `{status=error}` is what parses, where `{status="error"}` does not. A static answer is never `truncated` — that flag continues to mean "the catalog read hit its cap" and nothing else. An **empty** value omits the `value` key entirely (`{"type":"string"}`), the canonical protobuf JSON mapping for a default-valued scalar; the §8.1 v1 flat projection is unchanged and still emits the empty string as an element.

The `intrinsic` scope, when present, leads the `scopes` array; the catalog scopes follow, ordered `(scope, key)` ascending, and values are ordered ascending. Intrinsic names and static values are served in a fixed order (names ascending; `status`/`kind` in grammar order). Responses are capped at **10 000** tag names / **1 000** values per request (documented constants `TAG_NAMES_MAX`/`TAG_VALUES_MAX`); a capped response sets the top-level `"truncated": true` — never an indistinguishable silent subset.

**The `type` is the STORED type, not a reading of the value's text** (issue #476). `trace_tag_catalog.val_type` carries the OTLP kind the sender sent, written at ingest and projected by the catalog's materialized view, and the wire `type` is that column: `string`, `int`, `float` or `bool` (plus `keyword` for a static answer). A string attribute whose text reads as a number, a duration or a boolean therefore reports `string`, which is what a client needs in order to quote it. This **replaces** the earlier best-effort inference over `val`'s characters, and `duration` is no longer in the domain a catalog answer can carry — the reference never emits it for an attribute either.

The type is **per value, not per key**: one key holding a string `"8080"` from one service and an integer `8080` from another returns **two entries** with identical `value` and different `type`. A consequence for the cap: `TAG_VALUES_MAX` and `truncated` count `(value, type)` pairs, so a value stored at two types spends two of them. The §8.1 v1 flat projection, which drops the type, deduplicates such a pair back to one element.

**Rows written before the `val_type` migration report `string`.** Nothing stored distinguishes their original type — `val` is the rendered text and `val_num` is a parse of that text — so no better answer exists, and `string` is what those rows already reported for non-numeric text. `trace_tag_catalog` has no TTL (below), so they do not age out on their own. Where a value exists both with and without a stored type — what a rolling upgrade produces — the typed entry is served and the untyped one is dropped, so the value is never listed twice.

**Scan bound, store-backed reads** (issue #478). A span-`name` read is bounded by the window's daily partitions and served by the `span_name_day` projection, which holds one row per `(UTC day, name)` — so the unnarrowed dropdown reads the distinct names rather than the spans. A `q`-narrowed attribute-value read prunes on the `(key[, scope])` primary-key prefix and the daily partition; the `(trace_id, span_id)` semi-join it carries is a **correctness** mechanism, not a pruning one, and the differential ledger records what was measured. Both carry the same Layer-1 row budget as the search path, so a window wide enough to exceed it is a `422` rather than an unbounded scan. There is no maximum-window-width rule.

**Scan bound, catalog reads.** A `scope`-confined `/tags` read and a scoped `/tag/{tag}/values` read prune to a `(scope)`/`(scope, key)` primary-key prefix. An unscoped `/tags` read or a bare-key `/tag/{tag}/values` read carries `WHERE scope IN (…)` over the five attribute scopes — still the catalog's leading primary-key column, so it prunes the writer-reserved intrinsic scopes away rather than scanning the whole table, but it reads every attribute scope. A request answered from the static vocabulary reads nothing at all. That scan carries the same Layer-1 read-row budget the §4.2 search path uses (`PULSUS_TRACEQL_SCAN_BUDGET_ROWS`, `read_overflow_mode='throw'`): on a catalog large enough that the scan would exceed it, the request is rejected with `422` rather than served as a slow unbounded scan. The `TAG_NAMES_MAX`/`TAG_VALUES_MAX` response caps above bound only a *successful* request's returned rows, not the rows a scan reads.

| Cause | HTTP |
|-------|------|
| `scope` outside the accepted set (case-sensitive) | `400` |
| Empty `{tag}` key | `400` |
| A range fault: an unparseable `start`/`end`, exactly one of the two supplied, or `end` earlier than `start` | `400` |
| Discovery scan exceeded the reader row budget (unscoped `/tags`, or a bare-key `/values` on a high-cardinality key) | `422` |
| ClickHouse read timed out | `504` |
| Unclassified failure | `500` |

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
| `step` | resolution, any positive **whole number of milliseconds**: a bare number is seconds (`60`), and `60s`, `500ms`, `1.5s`, `1m30s`, `1s500ms`, `1h30m`, `.5s` and `+30s` all parse. A step that is not a whole number of milliseconds (`1.5ms`, `3.5ms`, `100.25ms`) is a `400` — see the ledger entry `traceql-metrics-fractional-ms-step-rejected`. Auto-derived when omitted, and the derived floor is still one whole second |

**Function set (issue #182 — Tempo v3.0.2 parity).** First-stage: `rate()`, `count_over_time()`, and `sum`/`min`/`max`/`avg`/`quantile`/`histogram` `_over_time` over the `duration` target. `quantile_over_time(duration, q, …)` returns one series per quantile (`p=<q>` label); `histogram_over_time(duration)` returns one **plain-count** series per power-of-two nanosecond bucket that actually occurred (`__bucket=<bucket seconds>` label) — the reference's `Log2Bucketize` model, matched exactly in label values, tallies, membership and non-cumulativity; series ORDER is ascending by bucket, a ledgered divergence (issue #252, §4.4.1 below). Grouping: `by(resource.service.name)` returns one series per group value (the group label carries the value). Second stage: `topk(n)`/`bottomk(n)` reduce the series set per timestamp. Hints: `with(sample=…)` is accepted and returns the exact (superset) result; `with(exemplars=…)` requests representative `trace:id` exemplars.

**Exemplars are attached by default**, to a plain `{…} | rate()` with no hint and no parameter, and there are two inputs that control how many. **Both the `with(exemplars=…)` hint and the `exemplars` request parameter are a TOTAL budget for the whole response** — not a per-bucket sample size — and the precedence is one rule: the **hint wins** when present, otherwise the **parameter**, otherwise a default of 100. The ceiling on both is 100. `with(exemplars=false)` (or `0`) is the way to turn them off; the `exemplars` parameter accepts `0`, a negative value and a non-numeric value and behaves as if it were absent for each, never a `400`. **The hint's unit changed**: it used to mean N *per bucket*, so a deployment relying on that gets fewer exemplars now — at a 182-point grid `with(exemplars=2)` went from up to 364 exemplars to at most 2. Recorded as ledger entry `traceql-metrics-exemplars-total-budget`. **An exemplar is attached to the series that produced it**, on every shape: a grouped `rate`/`count_over_time`/`*_over_time` puts a sampled span on its own group's series, `quantile_over_time` on the `p=` series nearest the span's own duration (and the exemplar's `value` is that duration, not the series' sample), `histogram_over_time` on the `__bucket=` series the span's duration bucketizes to, and `compare()` on that span's side (`baseline_total` or `selection_total`) for each attribute key the span carries. A sample whose series the answer does not contain is dropped rather than attached elsewhere. The quantile placement compares against the quantile values of the exemplar's **own bucket** — the numbers the `p=` series draws at that timestamp — rather than of the whole window pooled: a deliberate divergence, ledger entry `traceql-metrics-quantile-exemplar-placement-domain`. The reference pools every interval into one distribution and places against that while drawing per-interval values, so it picks an exemplar's series using numbers no series carries; ours follows from `2026-08-05-traceql-quantile-over-time-tdigest` (§4.4.1), which already makes our `p=` values our own. With a single span in a bucket every quantile of that bucket is that span's duration, so every candidate ties and the lowest `p` wins — a property of degenerate input, not a rule of its own. `compare({selection}[, topN[, start, end]])` partitions the outer spanset into a `selection` (the inner filter's matching spans) and a `baseline` (the complement) and emits per-attribute meta-series labelled `__meta_type` ∈ {`baseline`,`selection`,`baseline_total`,`selection_total`} plus one scoped attribute label (`key=value`, or `key=nil` for the complement/totals). **All three of the reference's arities are served (issue #460), with its defaults** (`expr.y:324-326` @ v3.0.2): `compare(f)` means `compare(f, 10, 0, 0)`, and there is no three-argument form — `compare(f, 10, <ns>)` is a positioned parse error on both systems, as is a non-integer or negative argument. `topN` (default **10**) keeps, **per attribute and per side independently**, that side's `topN` values ranked by the sum of their counts over the whole window; the rest are **dropped, not folded** — `key=nil` and the `*_total` denominators are computed from the untrimmed population, so trimming a value never moves a total. Because the default is 10 rather than unbounded, it bites on the one-argument form too. `start`/`end` are unix **nanoseconds** and narrow which spans may be *selection* on the half-open interval `(start, end]` — lower bound exclusive, upper bound inclusive. That window **repartitions, it does not filter**: a span the outer filter and the request window admit but the selection window excludes still counts, in `baseline`, so `baseline_total + selection_total` is the whole population whatever window is asked for. `(0, 0)` means “no window” and is legal both as the default and written out. The argument rules are 400s with the reference's own messages, checked in its order: `topN <= 0` first (`compare() top number of values must be integer greater than 0`), then a non-positive timestamp where the pair is not `(0, 0)` (`compare() timestamps must be positive integer unix nanoseconds`), then `end <= start` (`compare() end timestamp must be greater than start timestamp`). The reference's `__meta_error="__too_many_values__"` series is **unreachable on the wire** — it carries no samples and `SeriesSet.ToProto` drops zero-sample series — so we emit none either (measured absent from the container's body at `topN` 1 and 3). Tie order among values with EQUAL counts is not a specification: the reference sorts with `sort.Slice`, which is not stable, so its survivors are arbitrary; ours are deterministic (descending count, then ascending value), a deliberate refinement recorded in the differential ledger as `traceql-compare-topn-tie-order`. A trailing metrics-result comparison (`… > 5`) post-filters samples above/below a threshold. Aggregation is executed entirely in ClickHouse (time-bucketed `GROUP BY`, a per-`(trace_id, span_id)` replay-dedup inner query, `quantilesTDigest` for quantiles, a `GROUP BY toUInt64(roundToExp2(val - 1)) * 2` log2 tally for the histogram, the compare() attribute cross-tab — docs/schemas.md §4.2). *The **ns→seconds conversion** of duration values, `__bucket` labels and duration thresholds is settled Tier-1 (issue #237): it is the single-rounding `float64(ns)/1e9`, pinned against 17-significant-digit raw-wire captures from the pinned Tempo v3.0.2 container — **not** the two-rounding form issue #232 established for the LogQL rate divisor (a different reference; "fixing" it like #232 would introduce a divergence). The pins are bit-exact unit tests plus wire-byte tests guarded so a bare-substring rewrite has no *accidental* landing site; deliberate circumvention routes are named, bounded residuals (R5/R6 in the `metrics_response.rs` scanner doc), not closed. `histogram_over_time`'s bucket rule, membership and non-cumulativity are **matched and Tier-1-gated** (issue #252: a hermetic replay of a committed reference capture, plus live membership/tally-sum identities); the **quantile algorithm and the histogram's series ORDER are deliberate, ledgered divergences** (`quantilesTDigest` over the raw durations, ledger `2026-08-05-traceql-quantile-over-time-tdigest`; ascending-by-bucket ordering, ledger `2026-08-05-traceql-histogram-series-order` — §4.4.1 for both), each with its own Tier-1 gates; per-value counts and the new tally query's throughput at 1 TB stay Tier-2 (issues #25/#251); attribute value targets, attribute grouping keys, multi-key grouping, and grouped quantile/histogram route to follow-ups (a clean `400`). `compare()`'s **attribute-key universe is complete and Tempo-matched**: it enumerates every present attribute (span.\*/resource.\*/name/kind/status) plus the **fixed 25-key well-known-attribute set** Tempo v3.0.2 always emits (including well-known-but-absent keys as `key=nil`) — that set is derived clean-room from black-box container observation + the published OTLP semantic-convention docs, and matches Tempo's live key universe byte-for-byte (25/25). `statusMessage`, `rootName`, and `rootServiceName` emit their real values (issue #189, via #184 trace-schema storage) — an empty `statusMessage` is a distinct `""` value, matching Tempo v3.0.2 (not folded into the `key=nil` complement). `instrumentation:name` and `instrumentation:version` likewise emit their real per-span values (issue #192, via the `scope_name`/`scope_version` trace-schema columns) — an empty scope is a distinct `""` value, the same `statusMessage` treatment. The label conventions match Tempo byte-for-byte; exact per-value counts are Tier-2 (#25).*

**Response body (Tempo-native, breaking change from earlier versions).** These endpoints are consumed only by the Tempo datasource and now emit the **Tempo-native metrics body**, replacing the earlier Prometheus matrix/vector envelope:

```json
{"series":[{"labels":[{"key":"__name__","value":{"stringValue":"rate"}}],
            "samples":[{"timestampMs":"1700000000000","value":0.88},{"timestampMs":"1700000060000"}],
            "exemplars":[{"labels":[{"key":"trace:id","value":{"stringValue":"abcd…"}}],
                         "value":0.88,"timestampMs":"1700000012345"}]}],
 "metrics":{"completedJobs":1,"totalJobs":1}}
```

Labels are OTLP protojson `AnyValue` (camelCase `stringValue`/`doubleValue`); `timestampMs` is a JSON **string** int64; a sample `value` is **omitted when zero** (protojson default omission); `exemplars` is present only under `with(exemplars=…)` and carries the trace reference as a `trace:id` label (not a top-level `traceId`). **That body is the `query_range` route's alone** — the instant `query` route returns a different message, below.

**Instant response body (`query`, a different `tempopb` message — issue #464).** `query_range` returns `tempopb.QueryRangeResponse`; `query` returns `tempopb.QueryInstantResponse`, whose `InstantSeries` carries `labels` and a scalar `double value` — **no `samples` array, no `timestampMs`, no `exemplars`** (`pkg/tempopb/tempo.proto:339-355` @ v3.0.2):

```json
{"series":[{"labels":[{"key":"resource.service.name","value":{"stringValue":"w2a"}}],"value":10},
           {"labels":[{"key":"resource.service.name","value":{"stringValue":"w2b"}}],"value":4}],
 "metrics":{"completedJobs":1,"totalJobs":1}}
```

The scalar is the **first** sample's value, and a series with **no** samples is dropped from the response entirely (`modules/frontend/metrics_query_handler.go:187-204` @ v3.0.2). A **zero** `value` is omitted, the same protojson default-omission the range route's sample `value` follows — so a `count_over_time()` that matches nothing comes back as `{"series":[{"labels":[…]}],"metrics":{…}}`, with no `value` key at all, and a client reading an absent `value` must read it as `0` rather than as an error (the full empty-window table for both forms is under **Bucketing** below). `QueryInstantResponse` also declares `status` and `message`; PulsusDB emits neither, exactly as it emits neither on the range route — a pushed-down query is wholly answered or is an error, so there is no partial state to report.

**Why the shape decides whether the client works at all.** Grafana's Tempo datasource decodes the instant body with a **bare** `jsonpb.Unmarshal` (`pkg/tempo/traceql_query.go:102,104` @ `v13.2.0`), which **rejects an unknown field** and returns the error instead of results. The range decode immediately beside it sets `AllowUnknownFields: true` (`pkg/tempo/traceql_query.go:113-116` @ `v13.2.0`, marked temporary in its own comment), so the range route's tolerance does not extend here. An extra key on this route does not degrade a panel; it blanks it.

**And the instant route is reached without anyone choosing it.** `metricsQueryType` defaults to `Range` (`src/traceql/TempoQueryBuilderOptions.tsx:42-47` @ `v13.2.0`), but a query opened under Unified Alerting is rewritten to `Instant` (`src/traceql/TempoQueryBuilderOptions.tsx:39,49-51` @ `v13.2.0`) — so every Tempo-datasource **alert rule** over a TraceQL metrics expression takes this route by construction, whatever the panel does.

**`by()` series cap (shared by metrics and search).** A grouped query runs a same-predicate distinct-by-key probe (`GROUP BY <by-keys> LIMIT cap+1`) before the main query; more than `reader.traceql_max_series` (default 1000) distinct series is a static **`422`**, never a silent subset. Ungrouped queries skip the probe. **One shared cap, one shared error:** the same `reader.traceql_max_series` cap and the same pre-flight probe bound BOTH the metric `by(...)` clause (`… | rate() by(resource.service.name)`) and the search-side spanset `| by(...)` stage (`{…} | by(resource.service.name)`); the search probe fires when the `by()` key is `resource.service.name` over a single `{…}` filter. For **every other** search-side `by()` form (other keys, composite spansets) the same `reader.traceql_max_series` cap is enforced by an **in-engine distinct-group backstop** — the regroup counts distinct group key-tuples across the evaluated candidate set at grouping-production time (before any `coalesce()` collapse and before result-limit eviction) and returns the identical static `422` on breach — so `by()|coalesce()` and fan-out concentrated in limit-evicted traces cannot bypass the cap. Search-side `by()` regroups the response into per-group spanSet arrays and `coalesce()` collapses them (issue #193; response shape above); grouping adds **no** new Phase-1/Phase-2 scan (it is a client-side post-pass over already-hydrated spans, group values riding the existing index-served attribute batch). **The backstop counts the ACCUMULATED key tuple** (issue #492): with two `by()` stages the cap is charged on the composite `(first key, second key)` value tuple the response actually retains, not on the last stage's keys alone — so a query with two `by()` stages over a high product cardinality can answer `422 query_too_broad` where a single `by()` over either key would not. That is the direction that bounds what is held rather than what is scanned; the shape and its numbers are recorded as `traceql-nested-by-composite-series-cap` in docs/benchmarks/traces-differential-ledger.md.

**Bucketing (normative):** the window is snapped outward first — `S = ⌊start/step⌋·step`, `E = ⌈end/step⌉·step`, epoch-aligned — so an unaligned request over-includes by at most one step on each edge and every bucket divides by the full step.

On the **range** route a bucket is labelled by its **right edge** and is **right-closed**: label `L` covers the instants `(L − step, L]`, so a span landing exactly on a grid point belongs to **that** point rather than to the next one. The emitted grid is `S, S + step, …, E` — **`intervals + 1` samples**, one more than the number of intervals, because the first label `S` is the right edge of an extra **leading** bucket `(S − step, S]` whose data sits before the requested window. The range query therefore reads the instants `(S − step, E]`, one whole step wider on the left than the instant form and inclusive of `E` itself.

**Every bucket in that grid is emitted**, whether or not any span fell in it: `count_over_time()`, `rate()`, `quantile_over_time`, `histogram_over_time`, the grouped `by(…)` count forms and every series of a `compare()` come back dense, with a zero value in each empty bucket — and a zero `value` is omitted from the JSON, so an empty bucket arrives as a bare `timestampMs` that a client reads back as `0`. The `*_over_time(duration)` value aggregations stay **sparse** and emit only the buckets that have data; that is deliberate and matches the reference, which is also sparse for those functions (ledger entry `traceql-metrics-density-by-function`). The sample count of a dense range series is therefore a property of the window and the step alone — `(E − S) / step + 1` — and never of the rows. **The metrics `by(…)` key set is one key today** (issue #182): `by(resource.service.name)` is served, and on a two-service corpus its density matches the reference sample for sample in both halves of the split above; every other metrics by-key — `by(name)` included — is a clean `400` reading `type mismatch: by() currently supports grouping by resource.service.name only (issue #182); attribute grouping keys route to a follow-up`, never a silent ungrouped answer. Both sides are measured in `traceql-metrics-by-key-restricted-to-service-name` (docs/benchmarks/traces-differential-ledger.md).

The instant `query` form evaluates one bucket over the whole snapped window `[S, E)` — left-closed, unchanged, `rate` dividing by `E − S` seconds — and reports it as a single scalar per series, carrying no timestamp at all (the instant body above). **On an empty window the answer depends on the form**, and the two cases mean different things. The **ungrouped** forms — `count_over_time()`, `rate()`, every `*_over_time(duration)` aggregation, and `quantile_over_time(duration, …)` — return exactly one labelled series whose zero `value` is omitted (`{"series":[{"labels":[…]}],"metrics":{…}}`). The **grouped** `by(…)` forms and `histogram_over_time(duration)` return an empty `series` list. So on this route an absent `value` is a numeric zero, never no-data, and an empty `series` list is the only no-data signal. Where the reference differs — it reports no series at all for an empty aggregation or quantile, and omits the `series` key rather than emitting an empty list — is measured and recorded as ledger entry `traceql-metrics-instant-empty-window-series`.

**Step derivation and the point cap (committed contract):** when `step` is omitted the derived resolution is `max(1, ⌊(end_s − start_s) / DEFAULT_METRICS_POINTS⌋)` whole **seconds**, with `DEFAULT_METRICS_POINTS` = 100; only an explicit `step` can be sub-second. The step is carried internally in **milliseconds** end to end. The snapped interval count `(E − S) / step` is capped at `MAX_METRICS_POINTS` = 11000 — so the emitted grid is at most 11001 samples — and a range resolving more intervals is rejected **statically before execution** with `422`: deliberately 422 (the bounded-response family), not Prometheus's 400, and never a silent truncation. The cap is what bounds a sub-second step: `step=1ms` over 30 days resolves 2.6 billion intervals and is a `422`, not a 2.6-billion-sample body. Attribute-filter semi-joins carry throwing IN-set limits with the same 422 semantics (docs/schemas.md §4.2).


#### 4.4.1 `histogram_over_time` matches Tempo; its percentile and series order deliberately do not

Read this if a Tempo dashboard and a PulsusDB dashboard disagree and you
need to know which number to believe. Every figure below is captured from
the pinned reference (`grafana/tempo:3.0.2@sha256:cda87c21…`, committed at
`crates/pulsus-read/tests/golden/traces_metrics/log2_reference_capture.json`)
and is pinned by a test.

**1. The histogram matches exactly.** `histogram_over_time(duration)`
rounds each span's duration **up to the next power of two** and labels the
bucket in float seconds (`Log2Bucketize`,
`pkg/traceql/engine_metrics.go:2038 @ v3.0.2`; a duration below 2 ns is
dropped from the series while `count_over_time` still counts the span).
Only buckets that actually occurred emit a series — there is no ladder,
and an empty bucket is **absent**, not zero — and each value is a plain
per-step tally, never cumulative. Nobody should come away thinking this
half differs. Ingesting twenty 300 ms spans and asking both stores:

```traceql
{ resource.service.name = "checkout" } | histogram_over_time(duration)
```

both return exactly one series, `__bucket = 0.536870912` (that is
`2^29 ns`), with the value `20`.

**2. The percentile deliberately differs.** For

```traceql
{ resource.service.name = "checkout" } | quantile_over_time(duration, 0.5, 0.9, 0.99, 1.0)
```

the reference walks its bucket tallies until it has `ceil(p × total)`
samples and reports the **bucket label** it stopped on, interpolating
exponentially between occupied buckets when the count lands mid-bucket
(`Log2QuantileWithBucket`, `engine_metrics.go:2058`). PulsusDB estimates
from the durations themselves, via `quantilesTDigest` over the
replay-deduped `duration_ns`.

**3. The measurement, which is the argument.** Three corpora of twenty
spans each — every span 280 ms, 300 ms and 520 ms respectively. All three
fall inside the single bucket `2^29 ns = 0.536870912 s`, and the
reference returns **byte-identical output for all three**:

| p | reference (280 ms, 300 ms and 520 ms corpora — the same bytes) | PulsusDB (300 ms) | PulsusDB (520 ms) |
|---|---|---|---|
| 0.5 | `0.3796250624970063` | `0.3` | `0.52` |
| 0.9 | `0.5009182730924541` | `0.3` | `0.52` |
| 0.99 | `0.536870912` | `0.3` | `0.52` |
| 1.0 | `0.536870912` | `0.3` | `0.52` |

Over identical values every quantile is exactly that value, which is why
the PulsusDB columns are constant down the rows; the point of the table
is the reference column, which is constant **across corpora**. Its
estimator is a function of the occupied bucket, not of the durations in
it. Two consequences you will actually meet:

- a service whose true p99 is 300 ms is reported at 536.87 ms — 79% high
  — and trips a 500 ms alert the real data never crosses;
- a rise from 280 ms to 520 ms (86%) produces an unchanged graph.

**4. This is not an inconsistency between our own two functions.** The
reference's percentile is an upper bound consistent with the histogram;
ours is a sharper value inside the same bucket. Both agree with the
buckets we emit, and because the histogram is byte-matched you can still
reconstruct the reference's bound from our buckets — what you gain is a
percentile that moves when the data moves.

**5. Why the reference's design is right for the reference.** Tempo
computes in memory over spans fetched from object storage, so it needs a
per-span step that is cheap, mergeable across workers and bounded to ~64
values; a log2 tally is a good answer to that problem. PulsusDB
aggregates inside ClickHouse next to the data, where the exact estimator
costs no more, so the constraint that motivates the approximation does
not apply. The divergence is a consequence of architecture, not a claim
of superior arithmetic.

**6. Series order also differs, and for the same reason.** The reference
returns histogram series in lexicographic order of a *rendering* of the
bucket label — `sortResponse` compares `AnyValue.String()`, which is Go's
`%g`, not the JSON text of the response and not the value. Measured, for
spans at 1 µs, 16 µs, 1 ms and 1 s:

| position | reference |
|---|---|
| 1 | `0.001048576` (1 ms) |
| 2 | `0.000001024` (1 µs) |
| 3 | `1.073741824` (1 s) |
| 4 | `0.000016384` (16 µs) |

Each column is that store's own JSON text: the two agree on every bucket
except `2^10 .. 2^13 ns`, where we write `1.024e-6` and the reference
writes `0.000001024` — the same `f64`, parsing to the same value, spelled
differently (`serde_json` and protojson pick exponent form at different
thresholds). That difference is recorded, not filed, and it is **not**
what produces the different order.

The reference's order is not ascending, not descending, and not the order
of its own body; it is a determinism device so that two runs agree, and
it needs nothing exotic to look strange — a 16 µs span beside a 1 ms span
is enough. **PulsusDB emits ascending by bucket**, which is how a
histogram is drawn everywhere a user has seen one. What this changes is
ORDER only: **label values, tallies, counts and membership are
identical**, so a client that reads the `__bucket` label — rather than
indexing the array by position, which the reference's order does not
support anyway — sees no difference.

**7. The full record**: ledger ids
`2026-08-05-traceql-quantile-over-time-tdigest` and
`2026-08-05-traceql-histogram-series-order` in
`docs/benchmarks/traces-differential-ledger.md`.

**Which filter constructs the metrics routes serve** (issue #458). The
`{...}` filter on a metrics query is compiled to a single fully-pushed-down
SQL predicate, so a construct is served here only when it has an exact
per-span SQL form. Served: every physical and attribute comparison §4.2
lists, attribute existence (`!= nil`), boolean statics, **bare attribute
truthiness** (`{ .flag }`, which is exactly `{ .flag = true }`), and the
**`nestedSetParent` root/non-root family** — `nestedSetParent < 0`,
`>= 1`, `= 0`, `!= 0` and every other comparison whose truth is constant
over the whole non-root domain. The root test lowers to the reference's
own `IsRoot` identity, an all-zero `parent_id`
(`tempodb/encoding/vparquet4/nested_set_model.go:11-12,57` @ v3.0.2), so
it costs one unindexed column comparison and never displaces the
`resource.service.name` PREWHERE hoist.

Still a clean `400` on the metrics routes, each with its exact body and
its witness query, in ledger entry
`traceql-metrics-filter-residual-refusals`: field-vs-field and arithmetic
comparisons (`{ .a = .b }`, `{ .a + 1 > 2 }`, and — because a negative
literal parses as a unary negation rather than a literal —
`{ nestedSetParent = -1 }`, whose served spelling is
`{ nestedSetParent < 0 }`); trace-level intrinsics
(`{ trace:duration > 1s }`); absence checks (`{ .a = nil }`); field
negation (`{ !.a }`); `nestedSetParent` comparisons **inside** the
numbering range (`{ nestedSetParent < 2 }` — a span whose parent is the
trace root carries `1` and one two levels down carries `2`, so no
per-span predicate can answer it); and `nestedSetLeft`/`nestedSetRight`
in any form (the Euler numbering is a per-trace tree walk). The reference
serves all of these — there is no metrics-specific filter guard in it at
all — so they are gaps, and every one is on the search route today.


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

**Errors:** a missing/invalid/inverted window, or `since` supplied together with `start`/`end`, is `400`. A window too broad to bound within the reader scan budget is `422` (the same bounded-response family as §4.2/§4.4). Errors are the §4 plain-text body, never carrying a byte offset.

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

`/metrics` exposes three implemented families (see `architecture.md §8` for the exact metric set): **ingest** (`pulsus_ingest_*`, `writer`/`all` roles only — absent on a reader-only process), **label cache** (`pulsus_label_cache_*`, reader/all), and **query eval gate** (`pulsus_query_eval_*`, reader/all). Ingest errors are attributed per signal + error-class, not per ingest protocol; on-disk spool size/file-count gauges, per-API/per-planner-stage query latencies, tier-router segment decisions, and tail-session counters are not yet exposed.

When basic auth is enabled, `/ready` and `/metrics` remain **unauthenticated** (liveness probes and metric scrapers must work without credentials); `/config`, `/buildinfo`, and every data-plane route require auth.

---

## 8. Compatibility endpoints (optional, `PULSUS_COMPAT_ENDPOINTS=true`)

Disabled by default. When enabled, PulsusDB additionally mounts third-party API surfaces so existing datasources, agents, and dashboards work unmodified. These are aliases onto the native handlers (or foreign-format parsers feeding the same pipeline); they carry no additional semantics and are not part of the versioned PulsusDB API.

### 8.1 Query aliases

The M1 log-query aliases (`/loki/api/v1/{query_range,query,labels,label/*/values,series}`) are pure route bindings onto the native `/api/logs/v1` handlers — responses are byte-identical to native, including `X-Pulsus-Explain` passthrough. They mount iff `PULSUS_COMPAT_ENDPOINTS=true` **and** the Reader subsystem is mounted (docs/architecture.md §1's mode table); they 404 exactly where native does (e.g. writer-only mode never mounts either surface). Gating is decided once at router-build time, not per request.

When `PULSUS_AUTH_*` is set, the perimeter returns 401 to every unauthenticated request regardless of path existence; authenticated requests to an unmounted alias (flag off, or non-Reader mode) return 404, indistinguishable from any nonexistent route.

| Compatibility path | Native equivalent | Ships with |
|--------------------|-------------------|------------|
| `/loki/api/v1/query_range`, `/query`, `/labels`, `/label/{name}/values`, `/series` (all `GET|POST`) | `/api/logs/v1/{query_range,query,labels,label/*/values,series}` | M1 |
| `/loki/api/v1/tail` (`GET`), `/loki/api/v1/index/stats` (`GET|POST`) | `/api/logs/v1/{tail,stats}` | M6 |
| `/loki/api/v1/index/volume` (`GET|POST`) | `/api/logs/v1/volume` | M7 |
| `/loki/api/v1/detected_labels`, `/loki/api/v1/detected_fields`, `/loki/api/v1/detected_field/{name}/values` | `/api/logs/v1/detected_labels`, `/api/logs/v1/detected_fields`, `/api/logs/v1/detected_field/{name}/values` (pure prefix swaps, `GET|POST` like native) | M7 |
| `/loki/api/v1/patterns` (`GET|POST`) | `/api/logs/v1/patterns` | M7 |
| `/api/traces/{traceId}`, `/api/traces/{traceId}/json`, `/tempo/api/traces/{traceId}` | `/api/traces/v1/trace/{traceId}`, `/api/traces/v1/trace/{traceId}/json` | M4 |
| `/api/v2/traces/{traceId}` | `/api/traces/v1/trace/{traceId}`, re-wrapped in the v2 fetch envelope (absent ⇒ `200` with an empty trace, not `404`) | M4 |
| `/api/search` | `/api/traces/v1/search` | M4 |
| `/api/search/tags`, `/api/search/tag/{tag}/values` | `/api/traces/v1/tags`, `/api/traces/v1/tag/{tag}/values` (Tempo v1 flat projection) | M4 |
| `/api/v2/search/tags`, `/api/v2/search/tag/{tag}/values` | `/api/traces/v1/tags`, `/api/traces/v1/tag/{tag}/values` (native shape minus `truncated`) | M4 |
| `/api/echo` | — (constant `echo` body) | M4 |
| `/api/metrics/query_range`, `/api/metrics/query`, `/tempo/api/metrics/query_range`, `/tempo/api/metrics/query` | `/api/traces/v1/metrics/query_range`, `/api/traces/v1/metrics/query` | M4 |
| `POST /querier.v1.QuerierService/{ProfileTypes,LabelNames,LabelValues,Series,SelectMergeStacktraces,SelectSeries,SelectMergeProfile,GetProfileStats,AnalyzeQuery}`, `POST /settings.v1.SettingsService/Get` (Connect-protocol, protobuf) | `/api/profiles/v1/*` | M5 |
| `/pyroscope/render`, `/pyroscope/render-diff` | `/api/profiles/v1/render{,-diff}` | M5 |
| `/loki/api/v1/rules[...]`, `/api/prom/rules[...]`, `/prometheus/api/v1/rules` | `/api/rules/v1/*` | M7 |

Routing note: the alias `GET /api/traces/{traceId}` coexists with native `/api/traces/v1/...`; the literal `v1` segment is matched first. `GET /api/v2/traces/{traceId}` does not participate in that resolution at all — its second segment is the literal `v2`, so it shares no prefix with either, and the `v1`-wins rule above is unchanged by its arrival.

**M4 Tempo query aliases (all `GET`).** The v1 trace-by-ID, search, and TraceQL-metrics aliases are pure route bindings onto the native handlers — responses are byte-identical to native, including §4.1's `Accept` negotiation on trace-by-ID (the `/json` alias binds the forcing handler and never negotiates). Deltas and reshapings:

- **Metrics envelope:** the `/api/metrics/*` aliases are pure route bindings like the rest of this list — they serve §4.4's native `{series, metrics}` body, byte-identical to `/api/traces/v1/metrics/*` for the same request, and there is no delta here. This bullet used to say the opposite; §4.4 of this file and `crates/pulsus-server/tests/api_conformance.rs` have both said otherwise since #182. Corrected on issue #502.
- **v1 flat tags:** `/api/search/tags` and `/api/search/tag/{tag}/values` serve Tempo's legacy v1 flat shapes — `{"tagNames":[...]}` (distinct keys, catalog order, deduplicated across scopes) and `{"tagValues":["a","b"]}` (bare strings). A server-side projection of the native scoped/typed §4.3 result: scope, value types, and `truncated` are dropped. Because the type is dropped, a value stored at two types (§4.3) is deduplicated back to **one** element here. **The v1 values route is deliberately attribute-only** (issue #478): it serves no intrinsic values — a `{tag}` of `name` there is the attribute keyed `name`, not the span-name intrinsic — and it ignores `q`. Both halves match the reference, which answers the same lookup differently on its v1 and v2 routes; a range FAULT is still a `400` there, like every other §4.3 route.
- **v2 tags:** `/api/v2/search/tags` and `/api/v2/search/tag/{tag}/values` serve the native scoped/typed shapes minus the PulsusDB-only top-level `truncated` field (Tempo's v2 wire shape has no equivalent — alias consumers lose the truncation signal; use the native routes to observe it).
- **Intrinsic scope:** served, on the **native** endpoint, with the aliases staying pure projections of it (issue #475 — the disposition this bullet used to record, now carried out). `scope=intrinsic` answers `200` with the static vocabulary on all three names routes, an unscoped request on the two scoped shapes leads with the `intrinsic` scope, and a `{tag}` that is an intrinsic spelling is answered from the vocabulary on the native and v2 values routes. Three deltas remain, each ledgered in `docs/benchmarks/traces-differential-ledger.md`: the v1 flat **values** route serves no static values (`traceql-v1-tag-values-statics-unimplemented` — the reference answers that lookup from its store on v1 too); an intrinsic with no closed value set answers `200 {"tagValues":[]}` where the reference returns stored span names (`traceql-intrinsic-values-empty-pending-span-names`); and our scope and value ORDER is deterministic where the reference's varies between requests (`traceql-tag-discovery-ordering`).
- **v2 trace fetch:** `/api/v2/traces/{traceId}` is the one M4 alias that is not a pure route binding. It runs the same point read as `/api/traces/v1/trace/{traceId}` and negotiates by `Accept` exactly as §4.1 describes (including the `406`, which the reference does not send — §4.1's existing row, now applying to a second route), but wraps the result in the v2 envelope: field 1 the trace, field 2 a read-accounting block. **An absent trace is `200`, not `404`** — protobuf body `0a 00 12 00` (4 bytes), JSON body `{"trace":{},"metrics":{}}` (25 bytes), byte-identical to the reference in both representations. That is the whole reason the route exists: the datasource tries v2 first and falls back to v1 on `404`, so without it every trace open cost two requests and a trace outside the queried window rendered as a raw HTTP error string instead of a sentence about the time range. Two named deltas, both ledgered in `docs/benchmarks/traces-differential-ledger.md`: our `metrics` block is always present and empty (`traces-v2-fetch-metrics-not-populated` — the reference's own counter is not stable between two fetches of the same trace, so there is nothing to match), and `POST` to this path is `405 Allow: GET,HEAD` where the reference answers `200` (`traces-v2-fetch-get-only` — the whole alias set is `GET`-only). The **populated** JSON body is not byte-comparable to the reference's: the `trace` object is rendered by §4.1's existing protojson serializer, which emits proto3 defaults and hex ids where the reference omits defaults and uses base64 — a pre-existing property of that representation, not something this route introduces, measured on both sides and recorded as `traces-fetch-json-emits-proto3-defaults` in `docs/benchmarks/traces-differential-ledger.md`. The v1 route's absent-trace `404 trace not found` is unchanged; there is no `/api/v2/traces/{traceId}/json` suffix route and no `/tempo`-prefixed sibling.
- **`/api/echo`:** `200` with the constant body `echo` — four bytes, no trailing newline, `text/plain; charset=utf-8`, and no `X-Content-Type-Options`. **Accepted divergence (issue #405, closed 2026-08-11 with no code change):** Tempo answers `echo\n` and sets `X-Content-Type-Options: nosniff`. Neither shape was chosen — its handler is `http.Error(w, "echo", http.StatusOK)` (`cmd/tempo/app/modules.go:846-850` @ tempo v3.0.2 `0c4b926d`) and Go's `http.Error` sets `Content-Type` and `nosniff` before calling `fmt.Fprintln`, so the trailing `0x0a` and the header both fall out of the standard-library call it happens to use; ours writes the bytes directly. **This one closed for a weaker reason than §2.3's three, and that is worth stating plainly.** Their grounds are not even the same as each other: #296's input **cannot arrive** on any proxied or browser path (each cuts below our limit) and the direct path that remains has a POST carrier, while #291's and #256's inputs can arrive perfectly well and are accepted because **no known client sends them** — no generator emitting a thousand line filters or sixty-five nested `sum(` has been named. This endpoint is weaker than either: it is reachable by anyone who calls it, and a caller gets the divergent bytes on every single request without having to try. It is accepted only because **nothing branches on the difference** — a trailing newline on a plain-text body and a sniff header over a four-byte constant give a client nothing to act on; there is no content to sniff and no value to parse. The counter-argument is real and was weighed: this is the first endpoint a compatibility probe or health check touches, so it is disproportionately likely to be the thing someone diffs, and the fix is two lines. If a diff-based compatibility claim ever becomes a goal, reopen this first — it is the cheapest item on the list. `crates/pulsus-server/tests/api_conformance.rs` pins the four bytes, and that assertion pins **this divergence**, not parity.

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

**Zipkin v2 JSON receiver (M6, `POST /api/v2/spans`, `POST /tempo/spans`).** A foreign-format decoder + model adapter feeding the *native* trace-storage path — each Zipkin v2 JSON span is adapted into one self-contained OTLP `ResourceSpans` and handed to the same parser the native `POST /v1/traces` receiver uses, so a Zipkin-ingested span stores with `payload_type = 1` (OTLP) and is queryable via trace-by-ID (§4.1) and TraceQL search (§4.2) with no read-path difference. Both documented paths bind to the same handler. Mounts iff `PULSUS_COMPAT_ENDPOINTS=true` **and** the Writer subsystem is mounted (`Gate::CompatAndWriter`, the Loki push precedent below); it 404s wherever the writer subsystem does. **Scope is Zipkin v2 JSON only** (v1 JSON, protobuf, and thrift are deferred). The body is always decoded as a Zipkin v2 JSON span array — `Content-Type` is not a fork discriminator (v2 JSON is the sole supported encoding, so there is nothing to content-negotiate, unlike native OTLP which forks JSON vs protobuf on CT) — decompressed per `Content-Encoding` for gzip; the decompressed body is capped at 64 MiB (400). **Documented `Content-Type` divergence:** because the decode is unconditionally JSON, a well-formed JSON span array sent under `Content-Type: application/x-protobuf` is accepted (**202**) where the OpenZipkin oracle would answer 400. This is the sole divergence — no real Zipkin client emits it, and it lets the ratified conformance harness (which sends `application/x-protobuf` generically on ingest success paths) pass; a genuinely non-JSON body under any CT is a clean JSON-parse **400** (never a mis-parse or panic). Field mapping: `traceId` (16-hex 64-bit → left-padded to 16 bytes, 32-hex 128-bit verbatim — byte-identical to the same trace sent as OTLP) / `id` / `parentId` (absent → root); `name`; `kind` CLIENT/SERVER/PRODUCER/CONSUMER → OTLP `SpanKind` (missing → INTERNAL); `timestamp` + `duration` **microseconds** → nanoseconds; `localEndpoint.serviceName` → the `service` dimension (resource `service.name`), its `ipv4`/`ipv6`/`port` → resource `net.host.ip`/`net.host.port`; `remoteEndpoint` → span `net.peer.*`; `tags` → span attributes (verbatim); `annotations` (timestamp + value) → span events; `debug`/`shared` → span attributes `zipkin.debug`/`zipkin.shared`. **Shared spans:** a Zipkin shared span reports the same `(traceId, id)` from both RPC ends with different `kind` (SERVER vs CLIENT); both are stored and **both are returned by trace-by-ID** — the assembler de-duplicates on `(span_id, kind)`, so neither side is dropped (a genuine no-op for native OTLP, whose span ids are unique per trace). Success is an empty **202** Accepted (both sync and async `X-Pulsus-Async: 1` — the OpenZipkin oracle answers 202 regardless), matched against openzipkin/zipkin:3. The **error-shape oracle is Tempo**, not OpenZipkin (issue #385): our reference stack is Loki/Prometheus/Tempo, and Tempo accepts Zipkin through the OpenTelemetry Collector's Zipkin receiver (`grafana/tempo` v3.0.2 `0c4b926d`, the CI-pinned digest). Measured there, every pre-admission rejection — malformed body, bad ids, unsupported `Content-Encoding` — is Go's `http.Error` container: `text/plain; charset=utf-8`, `X-Content-Type-Options: nosniff`, and exactly one trailing newline, which is what PulsusDB now writes. Concretely: a malformed span array, or any span with a non-hex/wrong-length id or an unrepresentable timestamp, is a whole-request **400** plain-text error (Zipkin has no partial-success channel — all-or-nothing, unlike the native OTLP receiver's per-span rejection), an unsupported `Content-Encoding` is **400**.

<!-- copied-rule:zipkin-backpressure:start -->
and sink backpressure is **429** plain-text — a **deliberate divergence** from the
reference, which answers its ingestion rate-limit rejection **500** with the body
`"Internal Server Error"` (measured on the pinned `grafana/tempo` v3.0.2 image; its
Zipkin receiver has no `429` path at all). `500` would tell a sender we are broken
when we are asking it to slow down, so we keep `429`; recorded as
`zipkin-backpressure-429-not-500` in docs/benchmarks/traces-differential-ledger.md.
<!-- copied-rule:zipkin-backpressure:end -->

**Loki push receiver (M6, `POST /loki/api/v1/push`).** A foreign-format decoder feeding the *native* log-storage path — a pushed stream's labels flatten through the same canonical model (`LabelSet::from_normalized` → `stream_fingerprint`) an OTLP log does, so pushed logs are queryable via LogQL (§2) and appear in `/api/logs/v1/tail` with no read-path difference. Mounts iff `PULSUS_COMPAT_ENDPOINTS=true` **and** the Writer subsystem is mounted (the writer-side analog of the §8.1 Reader gating); it 404s wherever the writer subsystem does, and the compat flag alone never mounts it without the writer role. Both request encodings are accepted: `Content-Type: application/json` selects the JSON body (`{"streams":[{"stream":{…},"values":[["<unix_nano>","<line>"],…]}]}`, honoring `Content-Encoding` for gzip); anything else or an absent `Content-Type` selects the snappy-compressed protobuf body (`logproto.PushRequest`, pinned to grafana/loki 3.4.2), which is *always* block-snappy-decompressed regardless of `Content-Encoding` — the agent default, so uncompressed protobuf is unsupported, exactly as upstream Loki. Success is an empty **204** (both encodings; **202** for async `X-Pulsus-Async: 1`); a malformed body, label string, or timestamp is a whole-request **400** plain-text error (upstream fails the whole body on those too — its JSON decoder aborts on a bad timestamp, and a label literal it cannot parse discards the request), and sink backpressure is **429** plain-text. A stream that parses but breaks a *label bound* is stream-local rather than whole-request — see "Per-stream label rules" below. Response codes match grafana/loki 3.4.2 where it has an equivalent (204 success, 400 malformed/oversize, 422 for a push carrying no streams — see "Per-stream label rules" below); 202/async and 429/backpressure are PulsusDB-contract additions. The decompressed body is capped at 64 MiB (mapping to 400, like Loki's own over-limit rejection — the cap *size* differs from Loki's per-line/per-stream limits, a deliberate divergence). **Structured metadata** (per-entry labels — protobuf `EntryAdapter.structuredMetadata`, or a trailing third element in a JSON `values` entry) is **stored per-entry and surfaced in LogQL/tail** (issue #97). It is decoded into the `log_samples.structured_metadata` column (a canonical sorted-key JSON String, the same representation as `log_streams.labels`), bounded by a per-entry cardinality limit charged before the canonical JSON is built (over-limit is a whole-request 400). On the read path it fans into the response stream labels alongside the base labels — matching grafana/loki 3.4.2's default (`categorize_labels` off) — so an entry carrying distinct structured metadata forms its own result stream, and a `| key="value"` pipeline label filter selects on it. Structured metadata is per-entry: it never enters `stream_fingerprint` (a stream pushed with vs. without it fingerprints identically) nor the tail keyset cursor. Server-side structured-metadata filter pushdown is a deferred optimization (client-side filtering is the baseline, consistent with parsed-label filters). **Empty-valued pairs are dropped at ingest and never stored** (issue #259) — both in structured metadata and in the stream label set, on both encodings and on the OTLP log path. "Empty" is **four different rules** here (rows 1-4 below), and they are listed together because assuming they were one rule is what produced that issue's early rounds. Rows 5 and 6 are not about emptiness at all — they are the REST of the same structured-metadata builder (issue #381), and they sit in this table because separating them is exactly the mistake rows 1-4 record: the empty-value delete, the collision resolution and the invalid-rune rewrite are `Del`, `Set` and `Set` of one `labels.Builder` run, not three rules that happen to share a seam. Each row is the reference's, applied at the seam that carries it; every discriminating case in the last column was measured on `grafana/loki:3.7.4` (`b318f282`) and is reproduced by PulsusDB.

| # | seam | rule | discriminating case |
|---|------|------|---------------------|
| 1 | stream labels | drop the empty **pair** — a same-named non-empty twin survives (`syntax.ParseLabels` returns `ls.WithoutEmpty()`, `pkg/logql/syntax/parser.go:279-296` @ v3.7.4, for hash determinism) | `{d="", d="keep"}` stores `d="keep"` |
| 2 | structured metadata | delete **by name** — every pair carrying an empty-valued pair's name goes, not only the empty one (Prometheus' `labels.Builder`, `pkg/distributor/distributor.go:698-722` @ v3.7.4) | `{a="", a="keep"}` stores nothing |
| 3 | structured metadata | …and by the **normalized** name, because the distributor renames each pair into the builder *before* the delete takes effect — and a rename also *re-adds*, so it can resurrect a name a delete had taken | `{a.b="", a_b="keep"}` stores nothing, while `{a.b="keep", a_b=""}` stores `a_b="keep"` |
| 4 | label **names** | "empty" means `len == 0` exactly, with no trimming anywhere: a whitespace-only name is not empty, it is refused by the *other* rejection condition, with different text | `""` → `label name is empty`; `" "` → `normalization for label name " " resulted in invalid name "_"` |
| 5 | structured metadata | …and all of that is ONE builder, which also decides which of several pairs sharing a stored name WINS (issue #381): a pair that was `Set` — renamed, or carrying `utf8.RuneError` — beats a pair that was not, wherever either sits in wire order; among `Set` pairs the last wins; among pairs never `Set` the last of the wire duplicates is what a JSON consumer observes. Not last-write-wins, which cannot explain both orders of the case beside | `{a.b="x", a_b="keep"}` stores `a_b="x"` in EITHER wire order, while `{a_b="2", a_b="1"}` stores `a_b="1"` |
| 6 | structured-metadata **values** | a value containing `utf8.RuneError` (U+FFFD) has every occurrence rewritten to a SPACE, and the rewrite is a `Set` — so it also promotes its pair above the un-`Set` ones (`removeInvalidUtf`, `pkg/distributor/distributor.go:75-80`, `:714-715` @ v3.7.4), rule 2's `Del` included. That interaction is the rule copied verbatim from `pulsus_model::resolve_structured_metadata`'s primitive 7, which `pulsus-model/tests/copied_rule.rs` asserts this row still contains: <!-- copied-rule:del-vs-set:start -->**`del` drops BASE entries only, so a `Set` outranks it.** An empty value deletes every pair stored under its name that the builder did not `Set`, and a rename or a U+FFFD rewrite re-adds the name in either wire order, because `add` is emitted whether or not `del` holds that name.<!-- copied-rule:del-vs-set:end --> This is a value change on its own, with no collision needed | `{a_b="p\ufffdq"}` stores `a_b="p q"`; `{a_b="p\ufffd", a.b="x"}` stores `a_b="x"`; `{a_b="", a_b="p\ufffd"}` stores `a_b="p "` in EITHER order, where the U+FFFD-free control `{a_b="", a_b="p"}` stores nothing |

Rules 2 and 3 are one seam's behaviour, separated because they are separately observable: rule 2 alone would keep `a_b="keep"` in rule 3's case, which the reference does not. Rule 3 is also order-dependent for a duplicate renameable name — `{a.b="", a.b="keep"}` stores `a_b="keep"` and the reverse order stores nothing — and PulsusDB reproduces that, on both push encodings. **Known residual:** rule 3 groups names by PulsusDB's canonical key rule rather than the reference's `LabelNamer.Build`, so wherever the two renamings differ the suppression differs with them — `{a..b="", a_b="keep"}`, `{a__b="", a_b="keep"}` and `{9bad="", key_9bad="keep"}` each store nothing on the reference and keep the non-empty twin here. That is the renaming divergence recorded below, not a second rule: it disappears when that one does. The same residual applies to rule 5, which groups colliding pairs by the same canonical key rule. **Two residuals belong to rules 5 and 6 alone.** First, the reference's `base` is ordered by Go's `slices.SortFunc`, which is insertion sort up to 12 elements and pdqsort above, so when one canonical name is repeated AND a rename lands on it its own answer stops being a function of wire order once the entry carries 13 pairs: measured, `k` copies of `a_b` followed by `a.b="REN"` returns the last wire copy for k=3,5,8,11 and returns `a_b="2"` for k=12,13,15,20, while with no rename present it returns the last wire copy at k=13,20,40. PulsusDB sorts stably and returns the last wire copy at every k, so the two agree on every shape except that one, where the reference is unspecified. Second, `{a_b="1", a_b="p\ufffd"}` is a push the reference accepts (204) and then cannot serve: its merge emits two `a_b` entries, one still carrying the invalid rune, and its own read answers `500 failed to parse series labels to categorize labels: 1:6: parse error: invalid UTF-8 rune` with and without the categorize header. There is no observable reference value, so PulsusDB's is a choice — the last pair, `a_b="p\ufffd"`. Both are recorded in `docs/benchmarks/logs-differential-ledger.md`. Otherwise the rule is exact: only a value that is exactly `""` is dropped — a whitespace-only value is kept verbatim, and nothing is trimmed. An entry whose entire metadata set was empty-valued is still stored, with no structured metadata; two streams differing only by an empty-valued label are ONE stream and share a fingerprint. **A structured-metadata NAME is validated before any of that** (issue #259), on both push encodings and on the OTLP log path, because that is where the reference validates it: every free-form name reaches `otlptranslator.LabelNamer.Build` (`vendor/github.com/prometheus/otlptranslator/label_namer.go:66-90` @ v3.7.4), which rejects exactly two classes — a name that is **exactly** `""` (`label name is empty`) and one that sanitizes to nothing but underscores (`normalization for label name "…" resulted in invalid name "…"`). Both are whole-request rejections carrying the reference's error text — with each transport's own envelope, because the reference does not use one text for both. On the push transport the whole response body is byte-identical, terminating `\n` included (its receiver writes every error through `http.Error` -> `fmt.Fprintln`, `pkg/loghttp/push/push.go:606-608` @ v3.7.4; measured, the same 20 bytes `label name is empty\n`), and PulsusDB LF-terminates every error body on that endpoint for the same reason. On the OTLP transport the reference prefixes the same text with `symbolizer lookup: ` (`fmt.Errorf("symbolizer lookup: %w", …)`, `pkg/loghttp/push/otlp.go:613` @ v3.7.4) and PulsusDB reproduces that prefix; the `message` string is then byte-identical, though the enclosing `google.rpc.Status` is not — the reference deliberately leaves `code` unset (`push.go:571-582` @ v3.7.4) while every PulsusDB OTLP receiver sets `code = 3` for a 400-class failure (a receiver-wide contract from issue #8, not a choice made here). This is rule 4 of the table above, and it is none of the other three: the empty-NAME rule is `len == 0` only, so a whitespace-only name is *not* "empty" — it is rejected by the second condition instead (`" "` sanitizes to `"_"`), and nothing is trimmed anywhere. The check runs on the RAW name, ahead of every empty-value strip, so a pair with name `" "` and value `""` is rejected rather than silently stripped; it runs *after* the per-entry metadata caps, matching the reference's order. On the OTLP path the same check covers every resource, scope **and log-record** attribute key, mirroring the single reference function that carries it there (`attributeToLabels`, `pkg/loghttp/push/otlp.go:603-614` @ v3.7.4, reached for a record attribute from `:488-499`). Record-attribute *values* are still discarded (issue #109's placement), so validating their keys stores nothing new — it closes an admissibility hole where the reference answered `400 symbolizer lookup: label name is empty` and PulsusDB answered `200`. **Deliberate divergence, status only:** PulsusDB answers **400**; `grafana/loki:3.7.4` answers **500** on `/loki/api/v1/push` and **400** on its OTLP receiver for the identical condition. The 500 is an unhandled path rather than a classification — the error escapes the distributor's validation closure with no gRPC status attached (`pkg/distributor/distributor.go:703-706` @ v3.7.4) and falls into `pushHandler`'s status-less `else` branch (`pkg/distributor/http.go:170-180`), where every sibling failure in that same loop is client-classified `400` through `validationErrors`. The input is entirely client-controlled and no retry can succeed, while `5xx` is precisely the class log agents retry, so PulsusDB returns the client-error status on both transports. **Divergence, message text only — user-visible:** the normalization message echoes the offending name through each runtime's debug quoter — Go's `%q` there, Rust's `{:?}` here — which spell an escape differently for a name containing an unprintable character (`"\x01"` vs `"\u{1}"`, `"\u00a0"` vs `"\u{a0}"`), and disagree on whether a lone combining mark is printable at all. **A client CAN hit this**: a structured-metadata key of `U+0301` is exactly the sort of name the rule refuses, and the two refusal bodies then differ — the reference's carries the raw UTF-8 bytes `cc 81` inside the quotes where PulsusDB's carries the eight ASCII bytes `\u{301}` (measured on both push encodings and on OTLP). What is bounded is the CLASS of characters that differ, never the reachability: over all 1 112 064 codepoints the two renderings agree on every assigned, printable, non-combining character, the only letter/digit/punctuation/symbol exceptions being U+FF9E and U+FF9F. It is not mirrored because Go's `%q` printability is `strconv.IsPrint`'s own generated table while Rust's `{:?}` additionally escapes `Grapheme_Extend`, and neither table is reachable from the other's standard library — matching bytes would mean vendoring a Unicode category table and pinning it to the Go release that built the image. Verdict, status and sentence are unaffected — the rule itself was replayed against the reference's own compiled `Build` over an 80-name matrix and agrees 80/80 on accept/reject, `naïve` (admitted) against `µ` and `日本` (refused) included. **Accepted** names are stored under PulsusDB's canonical key rule (`[^a-zA-Z0-9_] -> _`), which differs from the reference's renaming for some admitted inputs — `a__b` and `a..b` both store as `a__b` here and `a_b` there, and `9bad` stores as `9bad` here and `key_9bad` there. That rendering difference is tracked separately from the accept/reject surface fixed here. **`service_name` is synthesized on both transports** (issue #379), as the reference synthesizes it. On `/loki/api/v1/push`, after the empty-value strip and before validation, a stream that carries no `service_name` gains one: the first of thirteen names present with a non-empty value, scanned in the reference's LIST order — `service`, `app`, `application`, `app_name`, `name`, `app_kubernetes_io_name`, `container`, `container_name`, `k8s_container_name`, `component`, `workload`, `job`, `k8s_job_name` (`pkg/validation/limits.go:329-343` @ v3.7.4) — and `unknown_service` when none is present (`pkg/loghttp/push/push.go:442-456` @ v3.7.4). An internal stream (`__aggregated_metric__`, `__pattern__`) and a stream that already carries `service_name` are left alone. The list is not configurable here, exactly as the four label bounds above are not. On `/otlp/v1/logs` the reference uses a DIFFERENT algorithm and PulsusDB reproduces that one instead: the resource attributes are scanned in WIRE order, only those the reference promotes to index labels are considered, an empty value is not skipped, and a `service.name` attribute writes the slot wherever it appears (`pkg/loghttp/push/otlp.go:174-220` @ v3.7.4). The two disagree observably — `{k8s.container.name, container.name}` resolves to whichever came first on OTLP and always to `container_name` on push — so they are two functions, not one. Consequences a client sees: a push of `{}` or of `{"onlyempty":""}` is **204** and stores one `{service_name="unknown_service"}` stream (measured identically on `grafana/loki@sha256:87f0a067…`, stock config), and the label set rendered into every label-bound `400` message carries the synthesized label, including when an over-long discovered value is copied into it verbatim. **The empty-label rejection lands with it:** a stream whose label set is empty when it is validated is **400** `error at least one label pair is required per stream` (`pkg/validation/validate.go:25` @ v3.7.4). That is unreachable from `/loki/api/v1/push`, where synthesis always refills the set, and reachable from `/otlp/v1/logs` with a resource whose only index attribute is `container.name=""` — measured as that `400` on the pinned oracle.

**Per-stream label rules (M6, both log receivers — issue #374).** A log push's streams are validated as grafana/loki 3.7.4 validates them at its distributor (`pkg/distributor/validator.go:157-199`), reached for every push transport through one seam (`pkg/distributor/http.go:28-33`). Three things happen there, in this order. First, a label with an **empty value** is dropped — before validation and before the stream is fingerprinted, so `{a="1", ignored=""}` and `{a="1"}` are one stream with one identity (`pkg/logql/syntax/parser.go:279-296`). Second, a stream carrying `__aggregated_metric__` or `__pattern__` is exempt from the bounds below (`validator.go:164-167`); PulsusDB never generates such a stream, but a client can push one on either side. Third, the four bounds, in an order a stream breaking several will report first: **at most 15 label names** (`entry for stream '{…}' has N label names; limit 15`), **label names at most 1024 bytes** (`stream '{…}' has label name too long: '…'`), **label values at most 2048 bytes** (`stream '{…}' has label value too long: '…'`), **no repeated label name** (`stream '{…}' has duplicate label name: '…'`). `service_name` does not count toward the 15, so the effective rule is "at most 15 labels other than `service_name`", and a stream carrying **no entries** is not validated at all. A duplicate label name is only reachable on the protobuf `labels` literal: a JSON `stream` object and OTLP resource attributes both collapse a repeat first, exactly as upstream. The rules apply to `POST /loki/api/v1/push` (both encodings) **and** to OTLP logs (`POST /v1/logs` here; the reference serves the same receiver at `/otlp/v1/logs`, a path PulsusDB does not mount), but on OTLP they are charged on a *subset* of the resource attributes: the 18 the reference turns into stream labels (`distributor.otlp.default_resource_attributes_as_index_labels` — `service.name`, `service.namespace`, `service.instance.id`, `deployment.environment`, `deployment.environment.name`, `cloud.region`, `cloud.availability_zone`, `container.name` and the ten `k8s.*` names). Upstream every other resource attribute becomes structured metadata and is never seen by these bounds; PulsusDB stores them all as stream labels, so bounding all of them would refuse ordinary OTLP payloads the reference accepts — measured, a resource carrying a 2049-byte `app` attribute or sixteen arbitrary attributes is accepted by both. **The subset is matched on the attribute name exactly as it arrives on the wire**, before PulsusDB canonicalizes `.` to `_`, because that is where the reference decides (`pkg/loghttp/push/otlp.go:193`, an exact string comparison made before the name is canonicalized). So `service.name` is bounded and a raw attribute spelled `service_name`, `service-name` or `k8s_pod_name` is not — upstream those are structured metadata, and each is measured `204` there carrying a 2049-byte value. The empty-value drop, by contrast, applies to every resource attribute, because it is about identity rather than about the bounds. Two OTLP corners are *not* matched and are recorded in the ledger: a resource that repeats one index attribute key with two different values, and a map-valued index attribute with an over-long nested key.

**Ingest-time log level (M6, both log receivers — issue #483).** Every log entry is stored carrying a `detected_level` structured-metadata pair, on `POST /loki/api/v1/push` (both encodings) and on `POST /v1/logs`, exactly as grafana/loki 3.7.4 does by default (`shouldDiscoverLogLevels`, `pkg/distributor/field_detection.go:88` @ v3.7.4). **Every entry gets a value, `unknown` included** — there is no "nothing to add" path once the line has been read. The value is resolved in this precedence (`extractLogLevel`, `field_detection.go:96-124` @ v3.7.4): (1) an entry that already carries a `detected_level` pair has that value **normalized in place**, and nothing is added beside it — with one measured exception on OTLP, below; (2) otherwise the first of fourteen allowed field names present in the **stream labels**, normalized; (3) otherwise the same names over the entry's other structured metadata — on OTLP that is the record's own attributes, the record's `severityText` and the instrumentation scope's attributes, and PulsusDB consults them in that order; (4) otherwise the entry itself — an OTLP severity number maps by the OTLP bands (`1-4` trace, `5-8` debug, `9-12` info, `13-16` warn, `17-20` error, `21-24` fatal, anything above `unknown`), and otherwise the line is parsed as JSON (document order, string values only, two object levels) or as logfmt (allowed-list order) for those same field names, falling back to the earliest **word-bounded** occurrence of `trace`, `debug`, `fatal`, `critical`, `error`, `err`, `warning`, `warn` or `info` in the lowercased line. The allowed field names are `level`, `LEVEL`, `Level`, `log.level`, `severity`, `SEVERITY`, `Severity`, `SeverityText`, `lvl`, `LVL`, `Lvl`, `severity_text`, `Severity_Text`, `SEVERITY_TEXT` (`pkg/validation/limits.go:70-85` @ v3.7.4) — **separate list entries per spelling, not case-insensitive matching**, so a stream label `LeVeL=warning` or a JSON member `"lEvEl":"information"` contributes nothing. A field VALUE is matched case-insensitively against `trace|trc`, `debug|dbg`, `info|inf|information`, `warn|wrn|warning`, `error|err`, `critical`, `fatal`, and an **unmatched value is stored unchanged** — `{"detected_level":"banana"}` stays `banana`; `dbug` is not an accepted abbreviation. The word-bounded scan's left boundaries are space, tab, newline, `[`, `(`, `{`, `"`, `=` (`:` deliberately excluded, so `misc:error` is not a level) and its right boundaries add `]`, `)`, `}`, `:`, `,`, `!` (so `debug:` is one and `info` inside `information` is not). Names are matched **after** PulsusDB's canonical key rule, so a pushed `detected.level` pair is the level and a pushed `log.level` pair is not (its canonical name is `log_level`, which is not the `log.level` entry in the list); empty-valued pairs are deleted first, per the table above, so `{"detected_level":""}` falls through to the line. Two consequences a client sees: the pair merges into the returned stream label set on the unflagged read path (§2.1), which is what makes a level column and a `sum by (detected_level) (...)` volume breakdown work for a stream whose level is in the line body; and a stream whose entries carry several distinct levels comes back as one result stream per level. **The caps do not charge it.** The per-entry structured-metadata **count** and **byte** budgets are charged on the client's pairs before the level is added, so an entry at exactly either limit is still accepted and comes back carrying one more pair; and the byte budget additionally **exempts a pair whose raw name is exactly `detected_level`**, matching `ExcludedStructuredMetadataLabels` (`pkg/util/entry_size.go:23-33` @ v3.7.4) — the count budget still counts it. Set `PULSUS_DISCOVER_LOG_LEVELS=false` to store no `detected_level` at all and to store a client-supplied one exactly as sent. **One OTLP case is measured and is stated as two separate things, because only the first of them is observable.** *Observed:* when one allowed field name arrives in both a record attribute and a scope attribute, the level PulsusDB stores is the one the reference answers with, which is the **record attribute's** value; and when that name is `detected_level`, the reference's answer — and ours — is the **scope attribute's** value, exactly as sent and not normalized. *Inferred, from reading the reference and not visible in its response:* that it holds one ordered per-entry metadata list with record attributes ahead of scope attributes and rewrites the first `detected_level` in it. Its response carries a single pair under that name, so no rewrite and no ordering can be read off it; the inference explains the four answers but nothing in PulsusDB depends on it being right. The name never enters `log_streams_idx`, so `/labels`, `/label/{name}/values` and `/detected_labels` do not list it (§2.6.2); `/detected_fields` does (§2.6.1), because that endpoint reads the entries.

**What these bounds do not cover.** On OTLP they are charged on that 18-name subset while PulsusDB *stores* every resource attribute as a stream label, so a stored OTLP stream label can be wider than 2048 bytes, carry a name longer than 1024 bytes, or take a stream's stored label count past 15. It reaches a validated label too: `{service.name="ok", service_name=<2049 bytes>}` is accepted by Loki and by PulsusDB, and PulsusDB then stores 2049 bytes under `service_name` — both spellings canonicalize onto that one name, and the collision resolves in favour of the greatest original key (`_` sorts after `.`), which is the spelling no bound was charged on. None of this is a difference in what is *accepted*: upstream accepts these payloads too, routing the same attributes to structured metadata, whose own limits do not reach them either. It is a difference in what is *stored*; it follows from PulsusDB indexing every OTLP resource attribute (issue #109) rather than from these bounds, and it is recorded in the ledger.

A breach is **stream-local, not whole-request**: the streams that passed are written and the response is then **400** — plain text on the Loki path, a `google.rpc.Status` with `code = 3` on the OTLP path — carrying the failing streams' messages grouped as upstream groups them (`pkg/util/errors.go:105-131`: identical messages counted, distinct ones joined with `"; "`, a lone failure rendered bare). On the Loki path that body is **LF-terminated**, like every other error body that endpoint writes — the reference hands a label-bound breach to the same `push.HTTPError` writer as a decode failure (`pkg/distributor/http.go:27-30,161-171` -> `http.Error` -> `fmt.Fprintln`), and the measured `400` for a 2049-byte label value ends in `0x0a`. The OTLP path has no terminator: its body is a `google.rpc.Status` protobuf. When no stream survives, nothing is written. Statuses and bodies were measured case-by-case against the digest-pinned `grafana/loki:3.7.4` container; the differences that remain are recorded in `docs/benchmarks/logs-differential-ledger.md` under `ingest-label-bounds`.

**A push with no streams at all is `422`**, not an empty success — `error at least one valid stream is required for ingestion`, plain text on the Loki path and the same message inside the `google.rpc.Status` on the OTLP path. It is refused before any of the rules above run: upstream `PushWithResolver` answers it for `len(req.Streams) == 0` (`pkg/distributor/distributor.go:579-581`), which on the OTLP receiver means any payload carrying no log records — its translation returns an empty push request when `ld.LogRecordCount() == 0` (`pkg/loghttp/push/otlp.go:144-146`), so `{}`, an empty `resourceLogs`, a resource with no `scopeLogs`, and a scope with an empty `logRecords` are one case. A stream that carries labels but **no entries** is a different case and stays accepted (`204`): it is still a stream, and only the per-stream validation skips it. Both shapes were measured on both receivers. **One PulsusDB-only bound outranks it**: an OTLP body that carries no log records *and* nests an attribute `AnyValue` past the 32-level depth cap answers `400` here (the cap is charged during decode, on both the protobuf and the JSON transport) where the reference answers this `422` — the reference has no depth cap at all, so nothing upstream fixes the order between them. Measured on both transports and pre-existing (the same `400` comes out of the branch point this change was cut from); recorded as residual 6 of the `ingest-label-bounds` ledger row. **Whether a JSON push has streams at all depends on how its top-level key is matched, and that match is ASCII case-insensitive**: `Streams`, `STREAMS`, `StReAmS` (and an escaped spelling of any of them) all name the `streams` field, exactly as upstream's decoder does — `loghttp.PushRequest` is decoded by jsoniter under `ConfigDefault`, whose `CaseSensitive` is false. A repeated key is last-wins rather than an error, and a `null` value empties the request just as `[]` does, so `{"Streams":[…],"streams":[]}` is this `422`. The keys **inside** a stream object (`stream`, `values`) are matched exactly, on both sides — upstream decodes those with a hand-written unmarshaler that switches on the raw key — so `{"Stream":{…},"Values":[…]}` is a stream with no labels and no entries (`204`, nothing stored); a repeat of them is last-wins there too, except that `"values":null` leaves already-parsed entries alone. **A superseded occurrence cannot decide the request through a size cap**, so PulsusDB's own decode-time size caps — streams per request, entries per stream, label pairs per stream, structured-metadata pairs per entry — are charged on what *survives* that resolution: a body whose first `streams`/`stream`/`values` occurrence breaks one of them and whose last occurrence is valid is accepted, and the last one's lines are what get stored, as upstream. Duplicate keys inside one `stream` object collapse first for the same reason, so repeating one key does not consume the per-stream label count. **A discarded value is still read**, on both sides: its types, its structure and its nesting depth are checked exactly as a retained value's are, so a body whose superseded occurrence is malformed is a `400` — crossing a cap stops the retention, never the checking. Two request-wide budgets are deliberately not deferred — a 5,000,000-entry aggregate and a 256 MiB decode-size estimate, both measuring what the whole body has already made us materialize — so a superseded occurrence past either is refused here (`400`) and accepted upstream, which applies no size bound *while decoding* beyond its 100 MiB request-body limit (what survives decoding then meets a separate 4 MiB gRPC ceiling on its way to the ingesters, a `500` rather than a `400`); recorded as residual 7 of the `ingest-label-bounds` ledger row. **JSON nesting is bounded at 10,000 levels** anywhere in a push body, including under keys PulsusDB does not read and past a cap — the reference's own ceiling (`maxDepth = 10000`), counted the way it counts: the envelope object is a level and so is a stream object and everything under it, while the `streams` array is not, so the deepest accepted nest is 9,999 under an unknown envelope key, 9,998 under an unknown key inside a stream and 9,996 as an entry's fourth element. All six boundaries were measured on both servers and agree. A well-formed push nests six deep. **A number under a key PulsusDB does not read is range-checked, with one deliberate difference.** An out-of-range exponent form (`1e999`, `1e309`, anything past `1.7976931348623157e308`) is `400` here exactly as it is upstream, because the reference's skip evaluates it too. **Which range applies is decided by the number's first character**, on both sides: one written with a leading `0` is checked against a 32-bit float and everything else against a 64-bit one, so `0.35e39` is `400` while the same magnitude written `3.5e38` or `-0.35e39` is accepted. A plain run of DIGITS is accepted here at any length. Upstream skips such a run *unevaluated* only while it fits inside its decoder's current 512-byte read buffer; a run that spans the end of that buffer is parsed instead, and is then refused only if its value overflows a 64-bit float. So upstream's answer to the same bytes changes with how the client chunked its writes and with where the run happens to sit in the body — a 400-digit run is accepted there in one write and refused in 256-byte chunks, while a 309-digit run is refused if it is all nines and accepted if it is a `1` followed by 308 zeros. PulsusDB does not reproduce that: what is accepted here is a function of the request and nothing else. Recorded as residual 10 of the `ingest-label-bounds` row in `docs/benchmarks/logs-differential-ledger.md`.

---

## 9. Regular expressions

Every regular expression in a PulsusDB query is **RE2** ([google/re2](https://github.com/google/re2)) — the same dialect Loki, Prometheus and Tempo accept, since Go's `regexp` is an RE2 port. RE2 trades features for a linear-time matching guarantee: **there are no backreferences and no lookaround**, and PulsusDB does not support them either — with one route's exception, noted in §9.3. The full syntax is [RE2's own reference](https://github.com/google/re2/wiki/Syntax).

The rest of this section records where PulsusDB's behaviour is *not* identical to those references. Everything in it was measured, not inferred; §9.5 says how, and names the cases still unverified.

### 9.1 Where each pattern is compiled

Two engines evaluate regexes here: **ClickHouse's `match()`, which is RE2 itself**, for any predicate pushed into the scan, and the Rust [`regex`](https://docs.rs/regex) crate in process, for a pattern that must run over an already-materialised result. The two grammars **overlap without either containing the other** — each accepts constructs the other rejects (§9.3 and §9.4 give both directions), and several constructs both accept mean different things (§9.2). So which engine compiles a pattern, and whether it compiles it as the user wrote it, decides which of the following sections applies. That is a property of each construct, so it is a column rather than a rule.

**One check runs ahead of every row of the table below** (issue #400 Stage 2). Before a LogQL pattern is compiled at all, it is put to a reject-only pre-check that decides the constructs RE2 *demonstrably* refuses — a repetition of a repetition, a bound above 1000, a `(?x`/`(?u`/`(?R` group flag, a `\u`/`\U` escape, an unknown POSIX class or Unicode property name, an inverted `[X--Y]` range, or a capture name outside `[A-Za-z0-9_]`. Those are `400` here whichever engine the row names, because for them a successful compile is the *wrong answer* rather than a slow one: the Rust crate reads several of them as a different pattern. The check never claims a pattern the reference serves; §9.3 records what it closed.


| Signal | Construct | Anchoring | Compiled by |
|--------|-----------|-----------|-------------|
| Logs | stream selector `{app=~"…"}` | `^(?:…)$` | ClickHouse (RE2) |
| Logs | line filter `\|~` / `!~` — before any `line_format`, no `ip(…)` alternative | unanchored | ClickHouse (RE2) |
| Logs | line filter `\|~` / `!~` — after a `line_format`, **or** with an `ip(…)` alternative | unanchored | in process, **as written** |
| Logs | `\| regexp "…"` | unanchored | in process, **as written** |
| Logs | label filter `\| a=~"…"` over a parser-produced label | `^(?:…)$` | in process, **as written** |
| Logs | `\| drop` / `\| keep` matcher | `^(?:…)$` | in process, **as written** |
| Logs | `label_replace(…)` | `^(?:…)$` | in process, **rewritten** |
| Metrics | label matcher `=~` / `!~` | `^(?:…)$` | ClickHouse (RE2) when the selector reaches storage; in process, **rewritten**, when it is answered from the warm label cache |
| Metrics | `label_replace`, `info()` ignore set | `^(?s:…)$` / `^(?:…)$` | in process, **rewritten** |
| Traces | attribute/intrinsic comparison `=~` / `!~` | `^(?:…)$` | ClickHouse (RE2) |
| Traces | search's `query=` parameter | — | **not compiled** — validated by a three-valued syntax check, never executed |

**Rewritten** means the pattern is first translated into the Rust syntax that carries RE2's meaning, so the in-process engine reads it as RE2 does; over the 4,315-pattern corpus of §9.5, every pattern the metrics in-process path evaluates then matches the same subjects under both engines. **As written** means no such translation happens — that is §9.2's known divergence, and it is a defect rather than a design choice.

Separately from evaluation, **every LogQL regex except `label_replace`'s is also compiled in process at plan time as a validity check**, including the ones ClickHouse goes on to evaluate. That is why a LogQL pattern the Rust crate cannot compile is rejected even when it would have been pushed down (§9.4).

**Every in-process compile in the table above goes through one budgeted entry point** (issue #291). Before a pattern is compiled, the memory that compiling it would take is bounded from the pattern itself, and a pattern whose compile could exceed **96 MiB** is refused with `400 bad_data` and the message `expression too large`. The refusal is decidable from the query text alone, before any data is read, which is why it is a `400` rather than a `422`; and it costs less than serving it would, because it happens before the expansion it is refusing. This is a PulsusDB-specific boundary the references do not have — §9.4's size row says where it sits relative to theirs.

### 9.2 Constructs that mean different things in the two engines

| Construct | RE2 — and what PulsusDB should mean | Rust `regex`, unrewritten |
|-----------|------------------------------------|---------------------------|
| `\d` `\w` `\s` and their negations | ASCII: `[0-9]`, `[0-9A-Za-z_]`, `[\t\n\f\r ]` (note: `\s` excludes the vertical tab) | Unicode: `\d` matches `٥`, `\w` matches `ｗ`, `\s` matches `\v` |
| `\b` `\B` | ASCII word boundary | Unicode word boundary |
| `[a&&b]` | a class of `a`, `&`, `b` | set *intersection* — matches nothing |
| `[a~~b]` | a class of `a`, `~`, `b` | symmetric difference |
| `[a--b]` | **rejected** (`invalid character class range`) | set *difference* — matches `a`. Since issue #400 Stage 2 PulsusDB rejects it too, so this row is agreement on the VERDICT and is kept for the reading |
| `[!--b]` `[+--b]` `[ --a]` | the RANGE `!`..`-` (or `+`..`-`, ` `..`-`) plus `b`/`a` — accepted, because the start is at or below `-` | set *difference* — matches only `!`, `+` or a space |
| `[a[b]]` | a class of `a`, `[`, `b`, followed by a literal `]` | a nested class (union) — no literal `[` |
| `[[:foo:]]` — an unrecognised POSIX class name | **rejected** | a nested class — matches `:`, `f`, `o` |
| `a{bbb}c`, `a{,5}`, `a{}` | literal braces | **rejected** (`repetition quantifier expects a valid decimal`) |

(`[]a]` is *not* on this list: both engines read a leading `]` as a literal, measured on both.)

**A SECOND wrong-rows cause, measured 2026-08-10 and NOT closed by the escape fix (issue #400).** The rows of the table above marked "set *intersection*", "symmetric difference" and "a nested class" are not merely different readings — at the **as written** positions they are the same severity as the string-escape defect §9.4 records: **both stores answer `200` and return different lines, with no error on either side.** Measured at a line filter after a `| decolorize` (which clears the pushdown, so the pattern is evaluated in process) over one stream of seven lines, each carrying one of `a`, `b`, `&`, `~`, `-`, `[`, or none of them:

| pattern | the reference returns | PulsusDB in process returns |
|---|---|---|
| `[a&&b]` | the lines containing `a`, `b` or `&` | **nothing** — the crate intersects `[a]` with `[b]` |
| `[a~~b]` | those, plus the line containing `~` | the same minus the `~` line — the crate takes a symmetric difference |
| `[a[b]]` | **nothing** (the class is `a`, `[`, `b`, then a literal `]`, and no line carries one) | five lines — the crate reads a nested class, leaving no literal `]` to match |

`[a--b]` is not in this group: the reference answers `400` there, so it is an accept-surface class rather than a wrong-rows one — and since issue #400 Stage 2 PulsusDB answers `400` too, which is why §9.3 no longer carries it.

**The `[X--Y]` family splits at `0x2D`, and only one half is an accept-surface question** (measured 2026-08-12 against the pinned container, twenty-one shapes). RE2 reads `X--` as the range `X`..`-`, so it rejects exactly when `X > 0x2D`: `[a--b]`, `[a--]`, `[^a--b]`, `[a---b]`, `[.--z]`, `[0--9]`, `[-a--b]`, `[é--b]`, `[\x41--b]` and `[a-\-b]` are all `400` there. When `X <= 0x2D` the range is valid and the reference SERVES it — `[!--b]`, `[+--b]`, `[ --a]`, `[--a]`, `[^--a]`, `[--]`, `[a-z--b]`, `[\w--a]`, `[a\--b]`, `[[:alpha:]--b]` and `[\n--b]` are all `200` — while the Rust crate reads a set difference. Those are **wrong rows**, the row added to the table above, and they are an accepted divergence — issue #400's owner ruling of 2026-08-12, recorded in §9.4; a `-` that is not in range-operator position (at class start, after `^`, after a completed range, after a class-shaped item such as `\w` or `[:alpha:]`, or escaped) is an ordinary member on both sides.

The domain is narrower than the escape defect's was — at every position the planner pushes down, ClickHouse's own RE2 evaluates the pattern and agrees with the reference, so this is confined to §9.1's **as written** rows plus `label_replace`. It is still a query that silently reads different lines. Owned by issue #400 and **not** fixed by its Stage 1, which is the string-literal lexer and cannot see a construct inside the regex — nor by its Stage 2, which refuses only what RE2 itself refuses. **The eight patterns are an accepted divergence**, owner-ruled 2026-08-12 and recorded with their measurement in §9.4.

**Where this table bites.** It applies to exactly the constructs §9.1 marks **as written** — every one of them a LogQL pipeline stage — and to nothing else. Two consequences:

- **Known divergence, open and unfixed (issue #336).** The five **as written** rows of §9.1 do not apply the rewrite, so every row of the table above is live in them. The LogQL line filter appears in §9.1 twice, once on each side, and that is the whole of the problem: the same filter compiles in ClickHouse before a `line_format` and in process after one — so **the same pattern can mean two things in the same query language depending on whether the planner pushed it down**, and moving a filter across a `line_format`, or adding an `ip(…)` alternative to it, can silently change what it matches. Measured against Loki 3.7.4, which answers the opposite in each case (double-quoted LogQL strings take Go escapes, hence `"\\d"` for the regex `\d`): `{app="x"} | logfmt | a=~"\\d"` matches the line `a=٥`; `{app="x"} | logfmt | a=~"\\w"` matches `ｗ`; ``{app="x"} | logfmt | a=~`[a&&b]` `` does not match `&`; ``{app="x"} | logfmt | a=~`a{bbb}c` `` is rejected outright. Across the 4,315-pattern differential corpus, 120 of the 1,152 patterns both engines compile read differently at these sites.

  **If a LogQL query gives results you did not expect from a `\d`/`\w`/`\s`/`\b` or a character-class pattern, this is the first thing to check.** The workaround that always works is to spell the class out — `[0-9]` for `\d`, `[0-9A-Za-z_]` for `\w`, `[\t\n\f\r ]` for `\s` — which both engines read the same way. For a *line* filter you can also move it onto §9.1's ClickHouse row — ahead of every `line_format`, with no `ip(…)` alternative; the other four **as written** rows have no ClickHouse counterpart, so only the spelled-out class helps there. **Nothing outside the five **as written** rows is affected** — every other row of §9.1 is either ClickHouse's RE2 or a **rewritten** compile. This is a defect rather than a design choice; it is not fixed as of this writing, and issue #336 carries the measurement.
- LogQL `label_format` templates use Go template functions with their own regex arguments, which are compiled by the Rust crate directly.

### 9.3 Patterns PulsusDB accepts that the reference rejects

These are patterns Loki, Prometheus and Tempo answer with `400`, and at least one PulsusDB route does not — the *where* column says which route, and for several rows it is only one. **Two different mechanisms produce that, and the "how" column says which** — they behave differently, so do not read the table as one phenomenon:

- **Rust accepts it** — the pattern reaches the Rust `regex` crate, which compiles it, and the query is answered with the Rust crate's reading. The crate simply has no equivalent of the limit RE2 imposes: no repetition cap, the full UCD rather than RE2's fixed property table, and its own extensions.
- **nothing decides it** — the Rust crate rejects the pattern *too*, so every route that compiles agrees with the reference. It survives only on trace search's `query=` parameter, which is validated and never executed: the validator's verdict is three-valued and an undecidable pattern is treated as accepted. Nothing is evaluated, so there is no wrong answer — only a `200` where the reference sends `400`.

The "where" column then names the routes, because a query only reaches the Rust crate on some of them:

- **in process** — the warm metrics label cache, PromQL `label_replace`/`info()`, and LogQL pipeline stages the planner could not push down. A route that reaches storage instead hands the pattern to ClickHouse's RE2, which rejects (`400`) every row marked *in process* or *LogQL in process only* — measured on all of them — so the *same* metrics query can be answered or rejected depending on the label cache's state.
- **LogQL in process only** — accepted only where the pattern is compiled as written (§9.2); the metrics rewrite turns it into something the Rust crate itself rejects.
- **trace validation only** — as above; that route's residual is enumerated per class in `docs/benchmarks/traces-differential-ledger.md` (`traceql-validate-re2-unknown-residual`). Every row below is accepted there, including the rows that are also accepted elsewhere.

| Pattern class | Example | How | Where |
|---------------|---------|-----|-------|
**Nine classes left this table with issue #400 Stage 2 (2026-08-12), and this is the record of what they were.** Each is a construct the Rust `regex` crate compiles — reading several of them as a *different pattern* — while Loki, Prometheus and Tempo answer `400`. PulsusDB now refuses each one at the compile seams, before any engine sees it, so they are agreement rather than divergence:

| Class that left | Example | What the Rust crate read it as |
|---|---|---|
| Unicode properties outside RE2's fixed table | `\p{Alphabetic}`, `[\p{Alphabetic}]` | the full UCD, rather than RE2's `"Any"` + `unicode.Categories` + `unicode.Scripts` — 202 names in all |
| Repetition above RE2's `kMaxRepeat` of 1000 | `a{1001}`, `a{2,1001}`, `a{1001,}` | the repetition, uncapped |
| Repetition of a repetition | `a**`, `a*+`, `a++`, `a?*`, `a{2}{3}`, `a*??`, `a{2,3}+` | `a**` as `(a*)*`, **which matches every subject** including the empty string — a line filter carrying it returned the whole stream |
| An unrecognised POSIX class name | `[[:foo:]]`, `[a[:zzz:]]`, `[[:^foo:]]` | a nested class of the literal members `:`, `f`, `o` |
| Non-RE2 group heads | `(?x…)`, `(?u…)`, `(?i-u:…)`, `(?R)` | its own flags. **`(?R)` is NOT a match-everything construct** — `(?R)a` matches `"a"` and not `""`, `"b"` or `"x\r\ny"`; that correction was made on this issue and is kept here rather than dropped with the row |
| `\U`-form escapes | `\U0001F600` | a code point; RE2 has no `\u`/`\U` escape in any spelling |
| `\u{…}` escapes | `\u{263A}` | as above |
| `[a--b]` | `[a--b]` | set *difference* (§9.2) |
| A malformed line filter in a BARE `variants(...)` variant | `variants(count_over_time({app="x"} \|~ "(" [5m])) of (...)` | nothing compiled it at all — a pushable line filter is skipped by the pipeline compiler and a discarded prefix renders no SQL |

`(?#c)a` is deliberately **not** in that list and stays below: `#` is not one of the flags RE2's `parsePerlFlags` refuses on, and the Rust crate rejects the head as well, so it was already agreement everywhere but trace validation. The readings above are pinned over eleven subjects by `crates/pulsus-re2/tests/re2_reject_classes.rs`, the statuses by `crates/pulsus-read/tests/logqltest/corpus/b25_re2_reject_parity.test`, and the whole surface by `crates/pulsus-read/tests/logql_regex_accept_matrix.rs` — whose divergence table now contains **no** row in this direction at all.

| Lookaround | `(?=x)`, `(?!x)`, `(?<=x)`, `(?<!x)` | nothing decides it | trace validation only |
| Comment groups; a trailing backslash | `(?#c)a`, `a\` | nothing decides it | trace validation only |
| A pattern over PulsusDB's compile budget | `(?:(?:(?:(?:[0-9a-f]{32}){32}){32}){32})` (the crate's `CompiledTooBig`), or any pattern over §9.4's 96 MiB compile-allocation cap | nothing decides it — neither budget is RE2's, so a refusal for either says nothing about RE2's verdict | trace validation only |

**Correction (issue #291, measured 2026-08-09).** The row above used to read as though the *divergence* were confined to trace validation. Only the **acceptance** is. On every other route the same two budgets produce the **opposite** divergence — a `400` here against the reference's `200` — and that is measured at five sizes in §9.4's size row. Trace validation is where an over-budget pattern is still *accepted*; it is not where the disagreement lives.
**Removed by issue #400 Stage 1 (2026-08-10).** This table used to carry a row for an invalid-UTF-8 **escape** in the pattern's string literal (`"\xff"`), on the ground that our lexer dropped the backslash on an unknown escape and compiled the three ASCII characters `xff` while the reference saw one 0xFF byte. The lexer now carries the reference's own string grammar, so that pattern is a `400` here at every position — the opposite direction, and a deliberate narrowing recorded in §9.4 and ledgered as `logql-string-escape-non-utf8`.

**For LogQL this direction is measured position by position and checked in** (issue #246): `crates/pulsus-read/tests/logql_regex_accept_matrix.rs` puts every class to each of the sixteen LogQL constructs that carry a regex — on the digest-pinned v3.7.4 reference and through PulsusDB's own `parse → plan → compile` chain — and its live leg re-measures the reference half in CI. **After issue #400 Stage 2 it has no row in this direction left**: every LogQL point where PulsusDB served a pattern the reference refuses is now a refusal here too.

The variant case is worth naming because it was the last one and it was not a construct at all. A malformed regex in a **bare** `variants(...)` variant's own line filter was served here and refused by the reference — and only while that filter was PUSHABLE, and only while the variant was BARE. PulsusDB validates a bare variant's discarded prefix through the pipeline compiler, which skipped a pushable line filter entirely (its regex is validated on the SQL-rendering path, and a discarded prefix renders no SQL); putting the same filter after a `line_format` cleared the pushdown, so it was compiled and both sides refused it, and wrapping the variant in a vector aggregation made its whole pipeline live (issue #397) with the same effect. The planner now validates that filter's regex directly, and the pushdown DECISION is unchanged. Everything measured is enumerated in `logql-regex-accept-surface-divergence` (docs/benchmarks/logs-differential-ledger.md).

### 9.4 Patterns PulsusDB rejects that the reference accepts

The narrower and more disruptive direction. **Two** mechanisms produce it. The first, and the one the table below is entirely about, is *syntactic*: each row is a construct RE2 accepts and the Rust `regex` crate rejects, so a route that hands it to the crate answers `400`. Which routes those are differs by row, because the §9.1 rewrite changes some patterns and not others — that is the last column, using §9.1's two labels. The second is about *size* rather than syntax and applies to every row of §9.1's **in process** and **rewritten** rows equally; it has its own subsection after the table.

| Pattern class | Example | Rust `regex` error | Rejected on |
|---------------|---------|--------------------|-------------|
| `\Q…\E` literal quoting | `\Qa*\E`, a bare `\Q` | `unrecognized escape sequence` | **as written** and **rewritten** alike — the rewrite leaves it unchanged |
| Octal escapes | `\0`, `\12`, `\101` | `backreferences are not supported` | **as written** and **rewritten** alike |
| A repetition applied to a flag-setting group | `a(?i){2}`, `a(?i)*`, `a(?i)+`, `a(?i)?` | `repetition operator missing expression` | **as written** and **rewritten** alike |
| A repeated flag letter in a group head | `(?ss:ab)`, `(?ii)a`, `(?i-ii)a` | `duplicate flag` | **as written** and **rewritten** alike |
| An empty flag group | `(?)a` | `repetition operator missing expression` | **as written** and **rewritten** alike |
| Literal braces | `a{bbb}c`, `a{,5}`, `a{}` | `repetition quantifier expects a valid decimal` | **as written** only — the rewrite escapes the braces, so every **rewritten** route (all of metrics, and LogQL `label_replace`) accepts these |
| A repeated capture-group name | `(?P<n>a)(?P<n>b)` | `duplicate capture group name` | **as written** and **rewritten** alike. The reference's vendored regex parser has no duplicate-name check at all; its `| regexp` PARSER has one of its own (`duplicate extracted label name 'n'`), so that single LogQL construct agrees and every other one does not |
| A `Cs` (surrogate) property class | `\p{Cs}` | `Unicode property value not found` | **as written** and **rewritten** alike. `Cs` IS a `unicode.Categories` key, so `unicodeTable` resolves it and the reference serves it (`\p{Cs}` is `200`, measured); the Rust crate has no surrogate class. Found by issue #400 Stage 2's corpus sweep, in none of that issue's original eighteen classes |
| A digit-leading capture name | `(?P<1n>a)`, `(?P<0>a)` | `invalid capture group character` | **as written** and **rewritten** alike. The reference's rule is `isValidCaptureName` = `[A-Za-z0-9_]+`, whose own comment says "Python rejects names starting with digits. We don't enforce either of those" (`vendor/github.com/grafana/regexp/syntax/parse.go:1261-1272 @ v3.7.4`); the Rust crate requires an XID start. Also found by that sweep. Its mirror image — a name carrying a byte OUTSIDE `[A-Za-z0-9_]`, e.g. `(?P<n.x>a)` — is a `400` there and is now a `400` here too |

ClickHouse's RE2 accepts every row above, so a predicate it compiles is answered. On logs that rarely helps: §9.1's plan-time validity check compiles every LogQL regex except `label_replace`'s as written, so these are rejected before reaching ClickHouse even when they would have been pushed down — measured, `{app="x"} |~ "a{bbb}c"` is a plan-time rejection here and a `200` on Loki 3.7.4. On metrics the outcome follows §9.1's row: a selector that reaches storage is answered, one served from the warm label cache compiles the rewrite.

#### The size boundary — `regex-compile-budget` (issue #291)

Separately from syntax, a pattern can be refused here for what compiling it would COST. Two limits do that, and they are ours rather than RE2's:

- the Rust `regex` crate's own **10 MiB compiled-program limit** (`CompiledTooBig`), which has been in force since the first release; and
- PulsusDB's **96 MiB compile-allocation cap** (issue #291), which bounds the memory spent PRODUCING the program. The crate's limit does not: it governs only the last of three compile phases, so a valid 100 KB pattern could allocate 887 MB and still be refused afterwards.

Both answer `400 bad_data`; the second's message is `expression too large`, the same wording class the reference uses at its own boundary (`ErrLarge`, `vendor/github.com/grafana/regexp/syntax/parse.go:47 @ v3.7.4`).

**The reference has a boundary here too, and ours is tighter.** Loki, Prometheus and Tempo all parse user regexes with Go's `regexp/syntax` — Loki and Prometheus through the vendored `github.com/grafana/regexp` fork, Tempo through `prometheus/model/labels.NewFastRegexMatcher`, which uses the same fork — and it caps `maxHeight = 1000`, `maxSize = 128<<20/40` instructions and `maxRunes = 128<<20/4` (`syntax/parse.go:93,102-103,122-123 @ v3.7.4`; Go's standard library carries the identical constants). Those are *per-limit* caps on a parse tree, and they admit a **128 MB** one. Adopting them would mean adopting the unboundedness this cap exists to remove, which is why our boundary is not theirs.

Measured against `grafana/loki:3.7.4` at the wire, both before and after the cap landed:

| Pattern | Reference | PulsusDB |
|---------|-----------|----------|
| `\p{L}`×200 (1,000 B) | `200`, 49 ms | `200` |
| `(?i)\p{L}`×1000 | `200`, 1.33 s | `400` |
| `\w`×20000 | `200`, 35 ms | `400` |
| `\w`×40000 | `200`, 62 ms | `400` |
| `\p{L}`×20000 | `200`, 2.31 s | `400` |
| `\p{L}\|…` alternation, 10,013 atoms | `200` | `200` — the last size we accept |
| `\p{L}\|…` alternation, 10,014 atoms | `200` | `400` `expression too large` — the band opens |
| `\p{L}\|…` alternation, 12,728 atoms | `200` — the last size it accepts | `400` |
| `\p{L}\|…` alternation, 12,729 atoms | `400` `error parsing regexp: expression too large` | `400` |
| `a`×130000 (a plain literal) | `200`, 34 ms | `200` — length alone never refuses |

The rows before `(?i)\p{L}`×1000 and from 12,729 atoms on are agreement; the middle is the divergence. Both alternation boundaries were bisected one atom at a time rather than projected — ours over the estimator, the reference's against the pinned container — so the residue can be named exactly: **the reference serves and we refuse over `\p{L}|…` alternations of 10,014 to 12,728 atoms, a band 2,715 atoms wide against the 12,728 it accepts.** Outside that band the two agree on this family. **We refuse nothing on length alone**: a literal is the cheapest shape there is, `a`×292624 still estimates under the cap, and the 131,071-byte query-text cap bites long before this one does.

Ledgered as `regex-compile-budget` in `docs/benchmarks/logs-differential-ledger.md`, with the cap itself pinned by `crates/pulsus-re2/tests/regex_compile_budget.rs`.

**Accepted limit — the cap is per compile, not per query (issue #291, remainder closed 2026-08-11 with no code change).** Both bounds above are charged on **one** pattern. Nothing bounds what a single query's patterns cost in aggregate, and the shape that shows it was measured: 1,000 line filters of 89 bytes each — 95,009 query bytes, comfortably inside the 131,071-byte text cap — allocate **5.24 GB over 5.9 s** while peak resident memory stays at **3.6 MB**, because each compiled pattern is dropped as soon as it has been validated. Every individual compile is small and well-behaved, so a per-compile cap is structurally incapable of seeing it. **The reference has nothing query-scoped here either** — its `maxHeight`/`maxSize`/`maxRunes` are per-limit caps on one parse tree, as cited above — so this is an availability question rather than an accept/reject divergence: one request buying seconds of work and gigabytes of churn. It is accepted because nobody writes a thousand line filters, and where untrusted users *can* send queries the answer is a query timeout and a rate limit at the front door, which are operator controls rather than engine work. **If it is ever wanted, build the cheap version**: one counter capping the number of compiled patterns per query, or their total pattern bytes — not a budget threaded through the nine compile sites, which was planned and deliberately abandoned. Reopen if untrusted query access enters the threat model.

**`[a--b]` and `\u{263A}` — the correction, and its closure.** Issue #246 corrected this paragraph in 2026-08-08: it had said `[a--b]` was rejected by PulsusDB *and* by the reference, so agreement, when in fact that held only on the **rewritten** routes. On every LogQL route that compiled a pattern **as written** — twelve of the thirteen — `{app=~"[a--b]"}` was a `200` here and a `400` there, because the Rust crate reads it as set difference (§9.2); only `label_replace`, which rewrites first, agreed. `\u{263A}` split the same way. **Issue #400 Stage 2 closed both** at the compile seams rather than by widening the rewrite, so the original sentence is true again — and it is true for a different reason, which is why the trail is kept: agreement now comes from a reject-only pre-check that reads the reference's own parser, not from `re2_pattern_to_rust`. The deferred pattern-rewrite work on #331/#336 must not be credited with either. Point by point in `crates/pulsus-read/tests/logql_regex_accept_matrix.rs`.

**String escapes in a LogQL pattern: one root cause, two divergences, both FIXED by issue #400 Stage 1 (2026-08-10) — with one deliberate residual.** A LogQL double-quoted string used to be lexed by a `scan_double_quoted` that knew `\n`, `\t` and `\r` and handled **everything else by dropping the backslash and keeping the character**. The reference's grammar knows a great deal more and rejects what it does not know: `prometheus/util/strutil.Unquote` (`vendor/github.com/prometheus/prometheus/util/strutil/quote.go:66-231 @ v3.7.4`), which Loki's lexer calls on every string token (`pkg/logql/syntax/lex.go:190-201`). PulsusDB now implements that grammar, so **the spelling you write means what it means there**: `\a \b \f \n \r \t \v \\ \"`, `\xHH` (two hex digits, a raw byte), `\NNN` (exactly three octal digits), `\uXXXX` and `\UXXXXXXXX` — and anything else is a `400` with the offending escape named.

Three consequences worth knowing, each measured against the pinned v3.7.4 reference:

- **`\xHH` and `\NNN` are BYTES, so consecutive escapes compose.** `"\xc3\xa9"` is the single character `é`, not two characters.
- **A `\u`/`\U` surrogate decodes to U+FFFD** rather than being refused, because the reference's `utf8.EncodeRune` writes `RuneError` for an invalid rune. `"\ud800"` is a pattern for U+FFFD.
- **A backtick raw string still takes no escapes at all** (`quote.go:76-81`), which is why `` `\d+` `` remains the portable way to spell a regex class in LogQL. So does `"\\d+"`.

**What this changed for a user, and it is the reason the issue was raised.** Before the fix, `{app=~"\101"}` selected the stream whose `app` label is the literal text `101`, where the reference selects the stream whose `app` is `A` (Go octal for `A`). Both systems answered `200`, neither reported anything, and the two selections are **disjoint** — a query silently reading a different part of the store. `\x41`, `\u0041` and `\U00000041` had the same shape. The lines each escape now selects are pinned by `crates/pulsus-read/tests/logqltest/corpus/b24_string_escapes.test`, captured from the pinned container, and the decoded values byte by byte by `crates/pulsus-logql/tests/string_escapes.rs`.

**The other half was an accept-surface divergence and is also closed.** `\d`, `\w`, `\s`, `\0`, `\q` and `\'` are escapes the reference's string grammar does not define, so it answers `400 parse error at line 1, col N: invalid char escape` at its LEXER — before any regex parser, and therefore at every construct rather than at particular positions. PulsusDB used to accept them all and compile the backslash-stripped text (`d+`, `0`, `q`). It now refuses them. **The status is what is claimed, not the message text** (owner ruling on issue #246); ours names the escape and its byte offset.

**The one residual, and it is a deliberate narrowing rather than a leftover.** A literal whose decoded BYTES are not valid UTF-8 — reachable only through a lone `\xHH`/`\NNN` escape above `0x7F`, canonically `"\xff"` — is a `400` here at every position. The reference refuses it too wherever the pattern reaches its regex parser (the line filter, `| regexp`, `label_replace`, both `variants(...)` positions), but SERVES it at the five `NewFastRegexMatcher` sites — the selector, both label filters, `drop` and `keep` — which short-circuit a plain literal before parsing it. **At those five it answers `200` and nothing**, and it can only ever answer nothing here: every mounted ingest route materialises a line body and a label value into a Rust `String` (`crates/pulsus-write/src/protocols/otlp_logs.rs:37-55`), so no line and no label value in this store can contain an invalid-UTF-8 byte to match. You get an error where the reference gives you an empty result. Ledgered as `logql-string-escape-non-utf8`; TraceQL carries the identical ruling, and refuses `"\xc3\xa9"` as well, which LogQL does not.

**What is NOT closed by this.** The escape grammar is the STRING literal's. A backslash that survives into the REGEX — `\\u{263A}` written in a double-quoted string, or `\u{263A}` in a backtick one — is a regex-dialect question, and it is issue #400 **Stage 2** that closed it: that spelling is now a `400` here as it is there. What neither stage closed is §9.2's wrong-rows family — the class-algebra constructs and the `[X--Y]` shapes with `X <= 0x2D` — where both stores answer `200` and return different lines. That family is now an **accepted divergence** rather than an open defect; the ruling and the measurement are the next block.

**The class-algebra family — an accepted divergence, not a defect (issue #400, owner ruling 2026-08-12).** Eight patterns are accepted by **both** engines and read differently by each, so at §9.1's **as written** positions the same filter selects different lines with **no error on either side**:

```
[a&&b]   [a~~b]   [a[b]]   [[a][b]]   [\w&&\d]   [!--b]   [+--b]   [ --a]
```

The mechanism is §9.2's table, and it is one sentence: the Rust `regex` crate reads `&&`, `~~` and `--` inside a character class as **set operations** and `[…]` inside a class as a **nested class**, while RE2 has neither — its class parser recognises only `[:name:]`, `\p{…}` and a Perl class escape as structured members, so `&`, `~` and `[` are ordinary characters and `X-Y` is the only operator it has (`vendor/github.com/grafana/regexp/syntax/parse.go:1736-1825 @ grafana/loki v3.7.4 b318f282`; the fall-through to "single character or simple range" is `:1793`, the `hi < lo` rejection that makes `[a--b]` an error and `[!--b]` a valid `!`..`-` range is `:1806`). The consequence, measured 2026-08-12 against the pinned container over one line per subject:

```
{app="x"} | decolorize |~ "[a&&b]"
  reference: matches lines containing a, & or b
  PulsusDB:  matches nothing
```

The same shape holds for the other seven: `[\w&&\d]` matches every word character and `&` there (it is the union `[0-9A-Za-z_&]`) and only the digits here; `[!--b]` matches `! " # $ % & ' ( ) * + , -` and `b` there and only `!` here; `[a[b]]` matches an `a`, `[` or `b` followed by a literal `]` there, and an `a` or `b` anywhere here.

**Only the positions §9.1 marks as written are affected** — a line filter after a `line_format` or with an `ip(…)` alternative, `| regexp`, a parsed-label filter, `drop`/`keep`, plus `label_replace`. That is why the example carries a `| decolorize`, and it is not decoration: without a stage that clears the pushdown, the filter sits directly on the selector and is evaluated by ClickHouse's `match()`, which is RE2 itself and answers exactly as the reference does — measured subject for subject on ClickHouse 26.3.17.110, its hit set is identical to the reference's for all eight patterns. So `{app="x"} |~ "[a&&b]"` **agrees** with the reference, and the same filter one stage later does not.

**Why it is not fixed, and why that is a judgement rather than a finding.** The owner ruled on 2026-08-12 that the family stays as it is: writing `&&`, `~~` or a nested class inside a log-filter regex is not something a Grafana user does by accident, nobody has hit it, and while the fix is small the review cycle around it is not. It is recorded rather than closed because a silent difference in returned lines is exactly the kind of thing you cannot debug without being told it exists. If a class-algebra pattern gives you results you did not expect, spell the class out — `[ab&]` for `[a&&b]`, `[0-9A-Za-z_&]` for `[\w&&\d]` — which both engines read the same way. Ledgered as `logql-class-algebra-wrong-rows` in `docs/benchmarks/logs-differential-ledger.md`, with both sides' full selections and the subjects they were measured over.


### 9.5 How this was measured, and what is not covered

The tables above were produced by compiling each pattern against four engines and crossing the verdicts: Go's `regexp` run locally (**go1.25.5**, the toolchain on the machine that did the extraction — **the reference's own toolchain is `go1.26.5`**, declared at `go.mod:3 @ b318f282`, and the local one is named only so the extraction is reproducible), the digest-pinned `grafana/loki:3.7.4` oracle at the wire (`|~` and `{app=~…}`, accept = `2xx`, reject = `400`), ClickHouse 26.3.17.110's RE2, and the Rust `regex` crate 1.13.0 as PulsusDB compiles it. Go's `regexp` is the oracle of record because it can be run over the whole corpus; the container was probed on 26 patterns spanning most of the classes above, and agreed with Go on all 26, which is what licenses using Go in its place for the classes not probed at the wire. **The two toolchains were also measured to agree on the one set where a version difference would show** (issue #400 Stage 2): all 202 names `unicodeTable` accepts, extracted from the local go1.25.5 `unicode` package, were put to the go1.26.5 container individually — **202 accepted, 0 rejected** — and 23 names outside it (eleven Unicode-16/17 scripts such as `Garay`, and twelve PCRE-ish or case-variant spellings such as `Alphabetic`, `Assigned`, `Lc`) were each a `400`. Go and ClickHouse's RE2 agreed on everything except `\C`, which C++ RE2 accepts as "any byte" and Go rejects (PulsusDB rejects it, matching the reference).

**Re-measured on the ClickHouse version move (issue #376), and nothing moved.** The tables above were originally crossed against ClickHouse 24.8.14.39; the supported floor is now 26.3 LTS, so the ClickHouse-evaluated verdicts were re-taken on `26.3.17.110` and crossed against `24.8.14.39` construct by construct — the 26 constructs §9.2/§9.3/§9.4 name, each as `SELECT match(<subject>, <pattern>)` against both servers. **Every verdict is identical on the two versions**, accept and reject alike, including the `\C` disagreement above (`match('a', '\C')` is `1` on both, so C++ RE2 still accepts it as "any byte" where Go rejects it), the `a{1000}` accept / `a{1001}` reject boundary, `\p{L}` accept / `\p{Word}` and `\p{Alphabetic}` reject, and the `[a&&b]`/`[a~~b]`/`[a[b]]`/`[a--b]`/`[[:foo:]]` class readings. The four live crossings were re-run green against 26.3 in the same commit (`re2_screen_differential`, `logql_regex_accept_matrix`, `traces_regex_seal`, `accept_surface_wire`, plus the Tempo-backed `compare_value_differential`), which is what licenses re-attributing the version rather than only re-labelling it.

One ClickHouse regex behaviour DID move on that upgrade, and it is not in these tables because it was never a dialect difference: `OptimizedRegularExpression`'s required-substring analysis used to mis-read a `(?…)` flag-group head carrying no `i`, so `match('xaby', '(?s:ab)')` answered `0` on 24.8.14.39 where RE2 matches. That is fixed on 26.3.17.110 — all 28 recorded forms now agree with RE2. It is a ClickHouse defect and its workaround is tracked by issue #331, which stays open; the workaround is still rendered, so no pattern's meaning changes for a user either way.

Stated gaps:

- The lists are **not proved exhaustive.** They come from the 4,315-pattern differential corpus plus targeted probes of every construct where the two grammars are known to differ; a construct in neither is not covered by anything above.
- **For LogQL, one narrower claim IS proved** (issue #246), and it is a claim about the reference's error taxonomy rather than about patterns. Of the sixteen `ErrorCode` constants the reference's vendored regex parser declares (`vendor/github.com/grafana/regexp/syntax/parse.go:28-48 @ v3.7.4`), **fourteen are raised by a named pattern** in `crates/pulsus-read/tests/logql_regex_accept_matrix.rs`, each tied to the container's own captured error body and re-put to the container by the live leg. The two that are not — `ErrInternalError` and `ErrInvalidCharClass` — are **declared and never raised anywhere in that package**, which is a statement about the source rather than the result of a probe; the live leg additionally fails if either is ever observed on the wire. The test fails if a code has neither. **This does not make the PATTERN set closed** — a taxonomy is not a grammar, and the classes above are still the ones we found rather than all the ones that exist.
- Two of those fourteen were briefly recorded as *unreachable*, and the mistake is worth naming because its shape recurs. `ErrInvalidUTF8` was excused on a probe that put a **raw** `%FF` byte in the `query=` parameter, which the LogQL scanner refuses first (`pkg/logql/syntax/query_scanner.go:264 @ v3.7.4`) — but the string **escape** `"\xff"` reaches the regex parser normally, and `{app="x"} |~ "\xff"` is a `400 … invalid UTF-8` (only the `NewFastRegexMatcher` sites — the selector, label filters, `drop`/`keep` — serve it, because they short-circuit a plain literal before parsing it). `ErrLarge` was excused on a probe of a *nested* repeat, which the repeat-product cap pre-empts (`repeatIsValid(re, 1000)`, `parse.go:434-437`) — but 4,000 copies of `a{999}`, 24,000 characters, reaches `maxSize`, which is `128<<20 / instSize` with `instSize = 40` = **3,355,443** instructions (the 33,554,432 quoted at the time is `maxRunes`, a different limit). **An unreachability claim is a claim about every route into a rule, so the probe's domain has to be the claim's domain.**
- ClickHouse's `match()` short-circuits some patterns to a plain substring search before compiling them, so a handful of syntactically invalid patterns (`x)(y`, `a\`) are neither rejected nor matched as regexes there. PulsusDB rejects both in process, matching the reference, so no query is known to reach that behaviour; it is recorded because it means ClickHouse is not, by itself, a complete acceptance oracle.
- Whether the reference's *error text* matches PulsusDB's is out of scope here: only accept-versus-reject is claimed. That is now an **owner ruling** rather than a convenience (issue #246, 2026-07-26 and 2026-08-08): the prose is not reproduced, no translation table exists, and two measurements license it — nothing branches on the text (the reference's own four non-vendor occurrences of `error parsing regexp` are all in its `_test.go` files), and byte parity is structurally unreachable without porting Go's parser, which was refused on #331, because Go quotes the offending **sub-token** rather than the pattern and `label_replace` quotes the anchored form where every other site quotes the bare one. What IS claimed and pinned is the **status**: `400` on both sides at every one of the 810 points that fixture probes. Ledgered as `logql-error-envelope` (the wording) and `logql-regex-accept-surface-divergence` (the decisions), docs/benchmarks/logs-differential-ledger.md.

## 10. Identifiers in a LogQL query

An **identifier** is the bare word a LogQL query uses for a label name: `{éx="m"}`, `| logfmt éx="b"`, `| json éx="b"`, `| label_format éx=ax`, `| drop éx`, `| unwrap éx`, `| éx > 1s`, `sum by (éx)`. PulsusDB accepts the same rune set for all of them, and that rune set is **not** ASCII-only.

Everything below was measured against the digest-pinned `grafana/loki:3.7.4` oracle (`.github/workflows/ci.yml`; the container's own `/loki/api/v1/status/buildinfo` reports `3.7.4` / `b318f282`) and is replayed on every CI run by `crates/pulsus-logql/tests/identifier_charset.rs`.

### 10.1 The rule, and where it comes from

An identifier is:

- **first rune:** `_`, or any rune in Unicode general category **`L`** (letter);
- **every later rune:** `_`, any rune in **`L`**, or any rune in general category **`Nd`** (decimal digit).

That is the reference's rule verbatim. Loki v3.7.4 builds its LogQL scanner on a vendored Go `text/scanner` and never assigns `IsIdentRune` (`pkg/logql/syntax/query_scanner.go:157` declares the hook, `:339-340` is its only use, and it is never set anywhere in the tree), so `text/scanner`'s default predicate applies unchanged:

```go
// pkg/logql/syntax/query_scanner.go:338-343 @ v3.7.4
return ch == '_' || unicode.IsLetter(ch) || unicode.IsDigit(ch) && i > 0
```

The leading rune goes through the same predicate at `i == 0` (`:675`), which is why a decimal digit may not lead.

### 10.2 The boundary — narrower than "non-ASCII is allowed"

The rule is **general category `L` and `Nd` only**. Three classes of rune that are commonly assumed to be letters or digits are not identifier runes, and PulsusDB refuses them exactly as the reference does:

| Spelling | Category | PulsusDB | Reference |
|---|---|---|---|
| `\| drop éx`, `\| drop 日本語`, `\| drop ʰx`, `\| drop ǅx` | `L` (Ll/Lo/Lm/Lt) | accept | `200` |
| `\| drop x٣`, `\| drop x३` | `Nd`, non-leading | accept | `200` |
| `\| drop ٣x`, `\| drop 3x` | `Nd`, leading | reject | `400` |
| `\| drop का` (U+0915 U+093E), `\| drop e\u{301}x` | `Mc` / `Mn` combining mark | reject | `400` |
| `\| drop xⅧ` (U+2167) | `Nl` letter-number | reject | `400` |
| `\| drop x½`, `\| drop x³` | `No` other-number | reject | `400` |
| `\| drop x🙂`, `\| drop x\u{200d}y`, `\| drop x\u{e000}`, `\| drop x\u{378}` | `So` / `Cf` / `Co` / `Cn` | reject | `400` |

No Rust standard-library predicate is this rule, which is why PulsusDB carries generated general-category tables rather than calling one: `char::is_alphabetic` covers 147,421 code points against Go's `L` 136,104 (it is true for U+093E and U+2167), and `char::is_numeric` covers 1,924 against Go's `Nd` 680 (true for U+00BD).

**Keywords fold with Go's simple case mapping, not with ASCII lowercasing.** The reference resolves a keyword by lowercasing the scanned token at lex time (`pkg/logql/syntax/lex.go:226`), and exactly two non-ASCII identifier runes lower to an ASCII letter — U+0130 `İ` → `i` and U+212A `K` (KELVIN SIGN) → `k`, enumerated over the whole code space. So `| <U+212A>EEP ax` is the `keep` stage here as it is there, and `| logfmt | addr = İP("1.2.3.4")` is the `ip` filter.

**Unicode version.** PulsusDB's tables are Unicode **16.0.0**; the reference's Go runtime is **15.0.0**. Measured across the whole code space, the difference is a strict one-directional superset — **+4,924** code points in `L` and **+80** in `Nd` here that are not there, and **zero** in the other direction — so no identifier the reference accepts is refused by PulsusDB. All of the extra code points are unassigned in Unicode 15.0. A committed 15.0.0 baseline (`crates/pulsus-logql/tests/unicode15/go-1.25.5-general-categories.txt`) makes that a test rather than a remembered measurement: it fails if the skew ever becomes two-directional.

### 10.3 Two positions PulsusDB serves and the reference does not

The reference tokenises a non-ASCII identifier at **every** position its grammar has (every production carrying `IDENTIFIER` in `pkg/logql/syntax/syntax.y`; the eighteen probed positions are enumerated in `crates/pulsus-logql/tests/identifier_charset.rs`), and then fails to serve two of them for reasons below its own lexer. PulsusDB serves both.

- **`{éx="m"}` — the reference answers `400`.** Its query-frontend re-serialises the parsed AST, and vendored Prometheus `labels.Matcher.String()` quotes any name outside `[A-Za-z_][A-Za-z0-9_]*` (`vendor/github.com/prometheus/prometheus/model/labels/matcher.go:81-104`, `shouldQuoteName` at `:97-104`), producing `{"éx"="m"}` — a form LogQL's own grammar has no production for. The proof that this is the round trip and not the lexer: `{"éx"="m"}` returns the byte-identical error at the identical column, `parse error at line 1, col 2: syntax error: unexpected STRING, expecting IDENTIFIER or }`.
- **`{app="x"} | éx="v"` — the reference does not answer at all.** The same re-serialisation, but here the rewritten query reaches the querier and `500`s, and the frontend retries it. Measured on the pinned container: the first probe returned `500` after 28.1 s; four further probes returned no HTTP status at all (`curl` exit 52, *Empty reply from server*) after 37.4, 37.5, 39.0 and 37.4 s. The container log shows `code=Code(500)` on `try=0` … `try=4` and then `(500) 37.39s Response: "failed to enqueue request"`.

Both are the reference's own defects, so reproducing them would not be parity — and the second one is a ~40 s hang PulsusDB would never reproduce anyway. Both are recorded in `docs/benchmarks/logs-differential-ledger.md` under `lexer-identifier-charset` and censused in `crates/pulsus-logql/tests/case_folding.rs`.

One further reference behaviour that "serves" must not paper over: with a **matching** stream, `{app="x"} | logfmt éx="b"` and `{app="x"} | label_format éx="y"` are a `500` there — `could not write JSON response: 1:75: parse error: unexpected character inside braces: 'é'` — because the response encoder re-parses the rendered label set. PulsusDB returns the label. The extraction destination keeps its bytes on both sides: the reference validates the identifier rather than sanitizing it (`pkg/logql/log/parser.go:518`), and over the line `ax=7 bx=8`, `sum by (éx) (count_over_time({…} | logfmt éx="ax" [1m]))` returns a series labelled `éx` there while `sum by (_x) (…)` returns none.

### 10.4 What is still stricter here

Keyword resolution. The reference resolves keywords at **lex** time, so a keyword spelling can never be an identifier payload there: `| drop json`, `| drop by`, `| drop ignoring`, `| drop KEEP` and `sum by (ignoring)` are all `400`, as is `| drop İgnoring` (U+0130 folds to `ignoring`). PulsusDB resolves keywords only at grammar positions that expect one, so it accepts all of them. This is an over-acceptance — a query that works here would fail against the reference, never the reverse — and it is recorded on issue #392, which stays open for it.
