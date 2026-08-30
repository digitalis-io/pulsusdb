# Metrics differential divergence ledger

Every place PulsusDB's metrics surface deliberately answers differently
from Prometheus **v3.13.0** (`prom/prometheus:v3.13.0`, revision
`40af9c2cdc0eda00f3622e867a27f6359f7295f3` — the tag pinned in
`deploy/e2e/compose.single.yaml`), with the route each answer belongs to,
both statuses, and the evidence for the choice.

This file exists because `docs/api.md` §3.5 said it would: *"a second
metrics divergence graduates this row to its own file."* Issue #461 landed
several, so the `promql-expression-depth-cap` row moved here and §3.5
became a pointer.

**A row without its route is a stale row waiting to happen** — the
reference answers the same logical question differently on its OTLP
receiver, its remote-write receiver and its query API — so every row below
names both routes and both statuses in cells of their own, and
`crates/pulsus-write/tests/otlp_prom_translation.rs`'s
`every_divergence_has_a_ledger_row` reads those cells rather than
searching the row for loose tokens. The **Limit** column holds the
constant a row is about, alone in its own cell, so
`pulsus-promql`'s `depth_cap_ledger.rs` can assert cell equality
against `MAX_EXPR_DEPTH` — a `contains` over the row would be
satisfied by digits appearing anywhere in the prose beside it.

**One reference mechanism, two status classes.** Prometheus has exactly one
OTLP metrics admission path: `rwExporter.ConsumeMetrics` runs the whole
translation and a deferred block rolls the appender back on any error,
committing only on success (`storage/remote/write_otlp_handler.go:132-138`
@ `v3.13.0`); the error then reaches one switch (`:177-189`) which answers
`400` for the four storage sentinels — `ErrOutOfOrderSample`,
`ErrOutOfBounds`, `ErrDuplicateSampleForTimestamp`, `ErrTooOldSample` — and
`500` for everything else. So the rows below that describe a reference
rejection are **not** describing separate mechanisms; they are one rollback
reached by two error kinds, and each names the other rather than implying a
path of its own.

## Divergences

| Divergence | Limit | PulsusDB route | PulsusDB status | Reference route | Reference status | Rule, and why we diverge |
|---|---|---|---|---|---|---|
| `otlp-name-reject-status-400` | — | `POST /v1/metrics` | `400` | `POST /api/v1/otlp/v1/metrics` | `500` | A metric name or attribute key that Prometheus v3.13.0's namers refuse fails the **whole request** on both sides, with the **identical message**; only the status and the envelope differ. The reference writes a bare `text/plain; charset=utf-8` body — `normalization for metric "..." resulted in empty name\n`, `label name is empty\n`, `normalization for label name "--" resulted in invalid name "__"\n` and the three siblings, all measured, all storing zero sibling series. PulsusDB answers `400` with `google.rpc.Status { code: 3, message: <the same text> }`. This is the `#259` adjudication applied verbatim (docs/api.md §8.2): the input is entirely client-controlled, no retry can succeed, and `5xx` is precisely the class OTLP exporters retry. This row is the name half of `otlp-request-atomic-faults`, which carries the model. Gated by `otlp_prom_translation::reference_rejections_are_whole_request_400`, whose six expected statuses, `Content-Type`s and bodies are captured from the running reference into `crates/pulsus-write/tests/fixtures/otlp-metrics/prom-translation/cases.json`. |
| `otlp-request-atomic-faults` | — | `POST /v1/metrics` | `400` | `POST /api/v1/otlp/v1/metrics` | `500` | **The whole-request model, and the rule that decides when it applies.** A fault in the request's **shape or naming** is whole-request here: `400` with `google.rpc.Status.code = 3`, nothing stored. Two conditions reach it, and both are enumerable: the six name rejections of `otlp-name-reject-status-400`, and the expansion budget `MAX_EXPANDED_BYTES`. The reference has one model rather than two — every error rolls the request back (`write_otlp_handler.go:132-138`, status at `:177-189`) — so where our other model applies it is a divergence, and that is `otlp-delta-partial-success`, which this row names and which names this one. **Body limits, stated positively:** the reference reads its OTLP body through `io.LimitReader(reader, decodeReadLimit)` at `storage/remote/codec.go:1011`, inside `DecodeOTLPWriteRequest`, with `decodeReadLimit = 32 * 1024 * 1024` (`:43-44`); the gzip reader is substituted into `reader` above that line, so the 32 MiB bounds **decompressed** bytes — the same quantity our 64 MiB `MAX_DECOMPRESSED_BYTES` measures. Neither bounds **expanded output**, which is the quantity `MAX_EXPANDED_BYTES` exists for. The dividing rule and its reason are published to clients in docs/api.md §1.1 and gated by `otlp_prom_translation::api_md_documents_the_fault_model`. |
| `otlp-delta-partial-success` | — | `POST /v1/metrics` | `200` | `POST /api/v1/otlp/v1/metrics` | `500` | **The per-point model: PulsusDB answers with OTLP partial success where the reference is request-atomic.** A fault in one data point's data is `200` carrying `partial_success.rejected_data_points` and `error_message`; every other data point in the request is stored. The reference rolls the whole request back instead (`write_otlp_handler.go:132-138`; status at `:177-189`), so it discards valid siblings — see `otlp-request-atomic-faults` for the model and `otlp-reference-admission-window` for the same rollback reached by a storage sentinel. **The envelopes differ in kind, not in a field's value:** the reference carries no OTLP partial-success message on this route at all — `accepted`, `rejected` and `message` absent, body plain text — so ours is a protobuf `ExportMetricsServiceResponse` where it writes `text/plain; charset=utf-8`. Measured on the named instance, delta temporality: reference `500`, body `invalid temporality and type combination for metric "delta.count"\n` (`metrics_to_prw.go:224-233`), and its sibling `ok.gauge` **not** stored; PulsusDB `200`, `Content-Type: application/x-protobuf`, 76 bytes, `rejected_data_points: 2`, `error_message: "metric delta.count: delta temporality is not ingested; send cumulative"` (hex `0a4a080212466d65747269632064656c74612e636f756e743a2064656c74612074656d706f72616c697479206973206e6f7420696e6765737465643b2073656e642063756d756c6174697665`), and `ok_gauge` stored. **The sibling survival is the whole justification:** an OTLP exporter retries `5xx`, a delta Sum never becomes cumulative, so the reference's answer is an unbounded retry of a payload that can never succeed while also discarding every valid metric beside it. The conditions this model covers, and how each compares with the reference, are the **Fault classification** table below. Gated by `otlp_prom_translation::delta_temporality_is_partial_success_with_the_bytes_the_ledger_quotes` and `::ledgered_divergences_match_the_recorded_answers`. |
| `otlp-float-native-histogram-collision` | — | `POST /v1/metrics` | `200` | `POST /api/v1/otlp/v1/metrics` | `200` | Two samples of **different kinds** — one float, one native histogram — landing on the **same translated series identity (metric name and label set) at the same timestamp**. PulsusDB resolves it deterministically: the histogram wins, in either arrival order, matching the read-side tie-break. The reference keeps the **first arrival**: within one request it answers `200` either way and silently retains whichever came first in the payload, and across two requests it answers `200` then `400` with a direction-dependent body — `duplicate sample for timestamp\n` when the float arrived first, and `duplicate sample for timestamp <ts>; overrides not allowed: existing is a histogram, new value <v>\n` when the histogram did, the timestamp and value being the pushed sample's own rather than constants. So the reference is silently order-dependent where we are deterministic. Two of the label sets that can produce such an identity are generated by the **translation**, not sent by the caller — `quantile` (`helper.go:472`) and `le` — so a caller cannot avoid the collision by choosing its own attributes. Which OTLP shapes can produce a colliding identity is being enumerated separately; this row states the rule the resolution follows, not that enumeration. |
| `otlp-target-info-sample-cap` | `4096` | `POST /v1/metrics` | `400` | `POST /api/v1/otlp/v1/metrics` | `200` | `target_info` is emitted at `earliest`, then every `lookback / 2` (150 s at the default 5-minute lookback), then once at `latest` (`helper.go:560-604 @ v3.13.0`). **The reference's count grows linearly with the span and nothing intervenes** — measured positively: a 50-minute accepted span produced **21** `target_info` samples, exactly `span / 150 s + 1`. So one `ResourceMetrics` whose points straddle our admitted `Date` domain (1970-01-01 … 2106-02-06) would generate ≈28.6 M samples from one small resource. PulsusDB refuses at **4096** samples per `ResourceMetrics` — `4096 × 150 s ≈ 7.1 days`, one default `PULSUS_RETENTION_DAYS` window — as an `OversizeMessage` whole-request `400`/`code = 3`, never a silent truncation. Gated by `protocols::otlp_metrics::tests::target_info_over_the_sample_cap_is_a_whole_request_400`. |
| `otlp-target-info-span-accepted-points-only` | — | `POST /v1/metrics` | `200` | `POST /api/v1/otlp/v1/metrics` | `200` | Both accept; the difference is which `target_info` samples exist. The reference computes the emission span with `findMinAndMaxTimestamps` **before** validation (`metrics_to_prw.go:217`), so a data point it later rejects still stretches the span. PulsusDB takes the span over **accepted** data points only, because a point our `Date` domain refuses (before 1970-01-01, after 2106-02-06) would otherwise place a `target_info` sample in a partition `metric_samples` cannot hold. Only reachable on payloads we already reject and the reference does not. Gated by `protocols::otlp_metrics::tests::target_info_span_ignores_rejected_data_points`. |
| `otlp-duplicate-attribute-key-order` | — | `POST /v1/metrics` | `200` | `POST /api/v1/otlp/v1/metrics` | `200` | Two attributes with the **same raw key** are merged into one label value joined with `;`, in the order the sort left them. The reference sorts with `ScratchBuilder.Sort` → `slices.SortFunc`, which is insertion sort up to 12 elements and pdqsort above and is **unstable**, so its own order is unspecified past 12 pairs. Measured at n = 2 it preserves wire order (`{dup:"first", dup:"second"}` → `dup="first;second"`, `{dup:"z", dup:"a"}` → `dup="z;a"`); above 12 pairs it is a property of `slices.SortFunc` read from the source, not measured. PulsusDB sorts stably and therefore pins wire order at **every** n. Same unspecified-reference tie the repo already records for `#259`'s `base` ordering. Captured as the `duplicate-key-wire-order` cases in the corpus. |
| `otlp-reject-message-escape-syntax` | — | `POST /v1/metrics` | `400` | `POST /api/v1/otlp/v1/metrics` | `500` | The rejection messages above echo the offending name through Rust's `{:?}` where Go uses `%q`. The two agree on printable ASCII and disagree for unprintable and combining characters, exactly as `#259` measured over all 1 112 064 codepoints. Closing it needs Go's `strconv.IsPrint` table pinned to the Go release that built the image, which is not fixable inside this repo. So the corpus asserts reject **verdicts** exhaustively and reject **text** only for names made of printable ASCII; a gate claiming byte-identical reject text for arbitrary names would be lying. |
| `otlp-reference-admission-window` | `60 min` | `POST /v1/metrics` | `200` | `POST /api/v1/otlp/v1/metrics` | `400` | **Not a divergence we chose — a property of the reference that constrains every differential fixture in this repo.** Prometheus refuses a sample older than `head.maxTime − 60 min` (`chunkRange / 2` at the default two-hour chunk range), which is **head-relative, not wall-clock**: a fresh server admits a historical fixture and a warmed one refuses the identical push. Measured 2026-08-28 on a fresh `prom/prometheus:v3.13.0` after anchoring the head at `now`: offsets 30/55/59/**60** minutes → `200`; **61**/65/90 → `400 text/plain; charset=utf-8`. This is the same single rollback as `otlp-delta-partial-success`, reached by a storage sentinel instead — `ErrOutOfBounds` is one of the four the switch at `write_otlp_handler.go:177-189` maps to `400`, where the delta error matches none and falls to `500`; the rollback itself is `write_otlp_handler.go:132-138` in both cases. PulsusDB has no admission window at all and stores the sample. **Two bodies, against their conditions:** a historical point answers `out of bounds\n`, and a point too far ahead answers `out of bounds: timestamp is too far in the future\n` — different strings for the same status, so a gate pinning one would misreport the other. The body is **one line per rejected emitted series, `target_info` counted as one** — measured 1/2/2/3/4 lines for 1 metric, 1 metric + `target_info`, 2 metrics, 3 metrics, 3 metrics + `target_info`. Any gate asserting this body derives the expected line count from the payload's own rejected-series count; the constant would break the first time someone added a metric to the fixture, and it would look like a regression. Two consequences for fixtures: **timestamps must be relative to run time**, and **`service.name` must vary per run**, because `target_info` is resource-level and carries no data-point run id, so a second run collides with the first as `400 'out of order sample'`. **Admission is request-atomic, and the mixed shape is no exception.** If any emitted series is rejected the reference commits **none** of the request, including otherwise valid siblings, and accepted siblings do not affect the line count. Measured 2026-08-29 against a head warmed to `now`: one out-of-window metric paired with one in-window metric answered `400` with **one** line and stored neither; two rejected paired with two accepted answered `400` with **two** lines and stored none of the four. **The response carries no OTLP partial-success message** — `accepted`, `rejected` and `message` absent, body plain text — so the envelope differs in kind from ours, the same contrast `otlp-delta-partial-success` records for the delta path. |
| `promql-expression-depth-cap` | `250` | `POST /api/v1/query` | `400` | `POST /api/v1/query` | `200` | Reject a query iff the **depth of the tree the parser built** exceeds `250`. Depth is measured after parsing and before anything plans or evaluates, so a flat chain of N terms — which the parser reads in a loop at grammar-nesting depth 1 — is depth N, and 250 `+` terms are accepted while 250 nested parentheses are refused. Body: `bad_data`, `query expression nesting depth <measured> exceeds the 250 level limit`, naming **both** the depth measured and the limit. The reference has **no limit of any kind**: `promql/parser/lex.go`'s `parenDepth` is an unbalanced-paren counter tested only for `< 0`, `promql/parser/parse.go` has no input-length guard, and `web/api/v1/api.go` has no `MaxBytesReader` (read at `40af9c2cdc0eda00f3622e867a27f6359f7295f3`). The exact query PulsusDB refuses is a `200` there, and so is the same shape at 20,000 terms. A Rust stack overflow **aborts the process** and cannot be caught, so one deep-enough request kills every other in-flight query on the node. Measured through the HTTP surface on the release binary: `label_replace` nesting aborts at **888** levels and `1 + 1 + …` at **1,220**; the cap is a 3.55× margin on that floor, against a Prometheus conformance corpus whose deepest expression across all 2,183 `eval` queries is **10**. The same cap governs `match[]` on `/api/v1/series`, `/api/v1/labels` and `/api/v1/label/{name}/values`. What it does **not** cover — deep grammar nesting inside the parser itself — stays in `docs/api.md` §3.5, which keeps its measurements. |
| `promql-timeout-message-names-the-layer` | — | `GET /api/v1/query` | `503` | `GET /api/v1/query` | `503` | **The status and `errorType` are the reference's; the `error` string is ours, and names the deadline that expired.** A request-deadline breach answers `503` with `errorType` `timeout` on all **twelve** mounted `/api/v1/*` query routes. The reference has no HTTP-level deadline on that surface at all — its own `timeout` request parameter is read on `/query` and `/query_range` only, and a discovery request carrying it is served normally — so on the other ten of our twelve there is no counterpart answer to match. `/api/v1/write` is excluded and keeps a bare `408`: it is remote-write ingest, not a query surface. Five producers, one `errorType`, one message each, every message true on every path that can produce it. The request-deadline layer says `request exceeded the server deadline of <duration> (PULSUS_QUERY_TIMEOUT)`. The `timeout` request parameter says `query exceeded the requested timeout of <duration> (timeout parameter)`. The ClickHouse stream deadline and the pool-permit wait keep their existing `clickhouse: timeout: …` strings byte-unchanged. ClickHouse's own server-side execution-time breach, previously `500`/`internal`, joins them at `503`/`timeout`. **Why not the reference's sentence.** Its wording attributes the expiry to expression evaluation, which is true for it because it is handed the phase that expired; ours can expire in parameter parsing, PromQL parsing, planning, the ClickHouse round trip, client-side evaluation or encoding. Measured, the reference has no single sentence here to match at all: 60 repeats of one request-`timeout` breach on an idle host gave three different answers — `200` (the modal one), `503` naming expression evaluation, and `503` naming query execution — because the phase in its message is whichever checkpoint happened to notice the deadline. Matching it was never an available option. Row one says *request*, not *query*, because five of the twelve classified paths are `status/*` and `/api/v1/status/tsdb` is served entirely from the resident label-cache snapshot with zero ClickHouse and no await. A message that is false some of the time is worse than one that differs from the reference, and `errorType` — the field a client branches on — is identical either way. Issue #471. |

## Fault classification

The eight fault conditions PulsusDB's OTLP metrics parser recognises, each
against what the reference does with the same payload. The parser reaches
them through 12 `reject_point` call sites (which reduce to six conditions),
3 `reject_whole_metric` sites (delta temporality) and the within-request
histogram-wins drop.

**`accept`** means the reference stores something where we refuse.
**`atomic`** means the reference recognises the same fault and rejects the
whole request where we reject one point and keep the rest. **`unique`**
would mean no reference equivalent — the column exists so the claim can be
falsified, and it is currently empty, which is itself the finding: every
earlier version of this table asserted conditions the reference "has no
equivalent for", and each was wrong.

| Condition | Class | PulsusDB | Prometheus v3.13.0 |
|---|---|---|---|
| `value-less-number-point` | accept | reject the point, partial success | stores `0` — `number_data_points.go:54-60` declares `var val float64` and switches on Int/Double with **no default**, so an unset value appends zero. Measured: `noval_g{job="novalcase"}` = `0`. |
| `inconsistent-classic-histogram` | accept | reject the point, partial success | stores an internally inconsistent histogram — `helper.go:270-312` accumulates without a cross-check and writes `+Inf` from `pt.Count()`. Measured with `bucketCounts=[4,6]`, `count=99`: `_bucket{le="1"}=4`, `_bucket{le="+Inf"}=99`, `_count=99`, `_sum=5`, the trailing `6` silently dropped because the loop is bounded by the explicit-bounds length. |
| `u64-bucket-overflow` | accept | reject the point, partial success | wraps silently — same unchecked `cumulativeCount += …` accumulation. Measured with `bucketCounts=[18446744073709551615,1]`, `count=0`: `_bucket{le="1"}=18446744073709552000`, `_count=0`. |
| `float-native-histogram-collision` | accept | the histogram wins, deterministically, in either order | first arrival wins: `200` and silently order-dependent within one request, `200` then `400` across two. See `otlp-float-native-histogram-collision`. |
| `out-of-domain-timestamp` | atomic | reject the point, partial success | `400`, whole request rolled back — `ErrOutOfBounds`, one of the four sentinels at `write_otlp_handler.go:177-189`. Bodies in `otlp-reference-admission-window`. |
| `exp-histogram-scale-below-minimum` | atomic | reject the point, partial success (`native`/`dual` modes only) | `500`, whole request rolled back, body `cannot convert exponential to native histogram. Scale must be >= -4, was -100\n`, siblings discarded. |
| `native-histogram-validation-failure` | atomic | reject the point, partial success (`native`/`dual` modes only) | `500`, whole request rolled back — `tsdb/head_append_v2.go:115-127` calls `Validate()` on every histogram before appending. Measured body: `5 observations found in buckets, but the Count field is 1: histogram's observation count should equal the number of observations found in the buckets (in absence of NaN)\n`. |
| `delta-temporality` | atomic | reject the metric's data points, partial success | `500`, whole request rolled back, body `invalid temporality and type combination for metric "delta.count"\n` (`metrics_to_prw.go:224-233`). |

**Why we keep the strict side of the four `accept` rows.** The reference's
alternative is to store a value nobody sent, a histogram whose buckets
cannot sum to its count, a wrapped count, or whichever of two colliding
samples happened to arrive first. All four corrupt rates, alerts and
quantiles **silently**, and a caller gets a number rather than an error —
the same reasoning as saturate-versus-wrap: a wrong value a client cannot
detect loses to a refusal it can. A caller can only predict us where a row
exists, which is why all four have one.

Four of the eight conditions are pinned by `cases.json`'s `divergence-*`
cases, which carry the reference's captured status, `Content-Type`, body
and stored series alongside our own required answer. **They are not all of
one shape**, and the split matters: `divergence-value-less-number-point`
and `divergence-inconsistent-classic-histogram` run under the default
`classic` mode and the reference answers them `200`, storing something we
refuse; `divergence-exp-histogram-scale-below-minimum` and
`divergence-native-histogram-validation-failure` run under `native` and the
reference answers `500`, discarding the batch we keep. Two accept-shaped,
two atomic-shaped. The exponential-histogram cases declare
`exp_histogram_mode: native` as part of the case: `to_native_histogram` has
**one non-test production call site**
(`crates/pulsus-write/src/protocols/otlp_metrics.rs`, inside
`emit_native_exponential_histogram`), reached only from `Native` and
`Dual`, so the default `classic` path has no scale floor at all and a
fixture left on it cannot reach its own rejection.

## Not divergences

Four reference behaviours that earlier drafts of this file described as
absences. Each is restated as the positive fact that was actually
established, because "the reference has no X" reached by not finding X was
wrong three times running here.

- **Metadata is attached per append, not deduplicated.**
  `metrics_to_prw.go:241-248` builds `AOptions.Metadata` per metric
  descriptor and attaches it to **every** `Append`. PulsusDB writes one
  `metric_metadata` row per family per request because that table is a
  `ReplacingMergeTree(updated_ns)` keyed by family name (docs/schemas.md
  §2.1). Stated positively, the reference's own metadata store is the
  per-series metadata the appender carries, surfaced at `/api/v1/metadata`
  and keyed by metric name at query time — a different structure reached by
  a different write, which is why the two are not comparable row for row
  rather than because one side is missing something.
- **The reference's metadata surface exists and is not populated from this
  route.** `/api/v1/metadata` answers
  `{"status":"success","data":{}}` for OTLP-ingested metrics (measured), so
  the metadata name/unit translation rule has **no live oracle** and is
  asserted hermetically against `metrics_to_prw.go:241-248` rather than
  against a differential.
- **Exemplars and `_created` produce no series on either side.** Exemplars
  are dropped without `--enable-feature=exemplar-storage`; Prometheus's
  OTLP path passes `startTimeUnixNano` as a created-timestamp argument to
  `Append` and never synthesises a `_created` series. A push carrying
  `startTimeUnixNano` produces exactly one sample per data point on both
  sides.
- **`target_info`'s sample count grows linearly with the accepted span.**
  Measured at 21 samples over 50 minutes, exactly `span / 150 s + 1` — see
  `otlp-target-info-sample-cap`, which is the bound we add on top.
