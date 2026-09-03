//! Assembles the documented `GET /api/traces/v1/search` JSON response
//! (docs/api.md §4.2) from `pulsus_read::SearchOutput` — response
//! shaping stays server-side so `pulsus-read` stays format-agnostic
//! (issue #55 layering). 64-bit nanosecond timestamps are emitted as
//! JSON strings (protojson convention, same as the trace-fetch surface).
//!
//! **The two duration fields sit at two different levels and are not the
//! same field** (issue #458). The reference's `TraceSearchMetadata` has
//! `uint32 durationMs` (`pkg/tempopb/tempo.proto:139` @ v3.0.2) — integer
//! MILLISECONDS, emitted here by [`trace_json`]. Its `Span` has
//! `uint64 durationNanos` (`pkg/tempopb/tempo.proto:160` @ v3.0.2), filled
//! from `span.DurationNanos()` (`pkg/traceql/engine.go:311` @ v3.0.2) —
//! NANOSECONDS, and protojson renders a `uint64` as a JSON **string**, so
//! [`span_json`] emits it as one. The string is load-bearing rather than
//! cosmetic: a span of 9007199254740993 ns (2^53 + 1) survives it and
//! would not survive a JSON number.
//!
//! **protojson OMITS a default-valued scalar**, so a zero-width span
//! carries no `durationNanos` key at all — captured, not reasoned:
//! against `grafana/tempo@sha256:aa8df8d0…` on
//! `GET /api/search?q={name="fresh-w0"}` the span object comes back as
//! `{"spanID":"…","name":"fresh-w0","startTimeUnixNano":"…"}` with the
//! field absent, while a 1 ns span carries `"durationNanos":"1"`.
//! [`span_json`] reproduces that. Emitting `"durationNanos":"0"` was the
//! shape this module shipped for one review round, and the tests were
//! green because they pinned OUR output at that width instead of the
//! reference's — which is why every width the unit test asserts is now a
//! captured response fragment.
//!
//! **The `metrics` block is `tempopb.SearchMetrics` and nothing else**
//! (issue #464). It used to carry `partial`, `limit` and `returned`, and
//! none of the three is a field of that message
//! (`pkg/tempopb/tempo.proto:164-172` @ v3.0.2). Grafana's Tempo
//! datasource decodes a search response with `jsonpb.Unmarshal`
//! (`pkg/tempo/search.go:95` @ `v13.1.5-11-g3c7375b`), which **rejects**
//! an unknown field and returns the error instead of results — so those
//! keys did not degrade trace search through that client, they disabled
//! it. [`render`] documents where the truncation signal went.
//!
//! **Every integer this module emits is projected into the domain of the
//! wire field it lands in** (issue #473). Removing the invented keys was
//! not sufficient for a strict client: three of these fields are
//! narrower on the wire than the Rust type feeding them, and a strict
//! protobuf-JSON decoder has no per-field recovery — it returns on the
//! first out-of-domain number and the caller gets that error instead of
//! results, so one bad row discards every row of the response. The
//! projection is SATURATION: below `0` becomes `0`, above the target
//! maximum becomes that maximum. Saturation is monotonic and preserves a
//! lower bound; wrapping does not, and a wrapped 60-day trace reads as
//! an ordinary 10.3-day one that no reader can tell from a genuinely
//! short trace. The same rule for the same reason is already recorded
//! for the logs surface (`docs/benchmarks/logs-differential-ledger.md`,
//! `detected-fields-limit-saturates-not-wraps`); this one is
//! `traceql-search-duration-ms-saturates-not-wraps` in
//! `docs/benchmarks/traces-differential-ledger.md`.
//!
//! **The projected sites are an ENUMERATION of four, not a count**:
//! [`trace_json`]'s `startTimeUnixNano` (`uint64`) and `durationMs`
//! (`uint32`), and [`span_json`]'s `startTimeUnixNano` (`uint64`) and
//! `durationNanos` (`uint64`). Each goes through [`duration_ms`] or
//! [`wire_nanos`], which RETURN the wire type — so at those four call
//! sites an out-of-domain `durationMs` and a minus-signed nanosecond
//! string are unconstructible, whatever the caller does. The other five
//! integers [`render`] emits are in domain by their Rust type and are
//! deliberately NOT projected: the two `matched` counts and the two job
//! counters are already `u32`, and [`group_value_json`]'s `intValue` is
//! a SIGNED 64-bit integer on the wire, which is exactly the `i64` we
//! hold. Returning the wire type is construction, not proof — it says
//! nothing about a FIFTH call site written without a projection, which
//! is what `every_integer_the_response_emits_lies_in_its_wire_domain`
//! exists to catch.
//!
//! **A saturation is surfaced off the response, not on it.** A negative
//! stored start or width can only arrive from a write that bypassed
//! `pulsus-write`, and that must surface — but a body a strict client
//! discards whole is not a diagnostic anyone can act on, and the
//! decoder's own error names neither the field nor the trace.
//! [`DomainReport`] carries one `tracing::warn!` per response and one
//! [`SATURATION_COUNTER`] increment per (field, bound) instead.

use serde_json::{Value, json};

use pulsus_read::{GroupValue, SearchOutput, SpanSetGroup, SpanSummary, TraceSearchResult};

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// The reference's own substitute for an EMPTY root service on a search
/// response — `RootSpanNotYetReceivedText` (`pkg/search/util.go:15` @
/// v3.0.2), applied to every trace of a response by
/// `addRootSpanNotReceivedText` (`modules/frontend/combiner/search.go:141-147`,
/// called at `:83` and `:119`). It substitutes `RootServiceName` and
/// nothing else: `rootTraceName` is never substituted, and neither is a
/// `by(rootServiceName)` group value, which is a different field
/// produced by a different code path (issue #473).
///
/// **A hand transcription, and nothing in CI can check it** — the same
/// standing caveat as `SEARCH_METRICS_FIELDS` below. The reference is
/// not vendored and this workspace has no Go step, so a typo here would
/// ship green; it is checkable by a human against the checkout.
const ROOT_SPAN_NOT_YET_RECEIVED: &str = "<root span not yet received>";

/// The counter [`DomainReport::surface`] increments, labelled `field`
/// and `bound` (docs/architecture.md §8).
const SATURATION_COUNTER: &str = "pulsus_traceql_search_wire_domain_saturations_total";

/// The four integers this module projects into an unsigned wire domain
/// (issue #473). See the module doc for why the other five integers
/// [`render`] emits are not projected.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum WireField {
    TraceStartTimeUnixNano,
    TraceDurationMs,
    SpanStartTimeUnixNano,
    SpanDurationNanos,
}

impl WireField {
    /// Every variant, in [`WireField::index`] order — the enumeration
    /// [`DomainReport::surface`] walks and the one the tests iterate, so
    /// a fifth field cannot be added without both seeing it.
    const ALL: [WireField; 4] = [
        WireField::TraceStartTimeUnixNano,
        WireField::TraceDurationMs,
        WireField::SpanStartTimeUnixNano,
        WireField::SpanDurationNanos,
    ];

    /// The `field` label value. **Level-qualified on purpose**:
    /// `startTimeUnixNano` exists at both the trace and the span level,
    /// and one shared label value could not tell a trace-level event
    /// from a span-level one.
    fn label(self) -> &'static str {
        match self {
            WireField::TraceStartTimeUnixNano => "trace.startTimeUnixNano",
            WireField::TraceDurationMs => "trace.durationMs",
            WireField::SpanStartTimeUnixNano => "span.startTimeUnixNano",
            WireField::SpanDurationNanos => "span.durationNanos",
        }
    }

    fn index(self) -> usize {
        match self {
            WireField::TraceStartTimeUnixNano => 0,
            WireField::TraceDurationMs => 1,
            WireField::SpanStartTimeUnixNano => 2,
            WireField::SpanDurationNanos => 3,
        }
    }
}

/// Which side of a wire field's domain a value was clamped from — the
/// `bound` label value, and the [`DomainReport::counts`] slot.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Bound {
    /// Clamped up from under the field's minimum.
    Below,
    /// Clamped down from over the field's maximum.
    Above,
}

impl Bound {
    /// Both variants, in [`Bound::index`] order — the enumeration
    /// [`DomainReport::surface`] walks.
    const ALL: [Bound; 2] = [Bound::Below, Bound::Above];

    fn label(self) -> &'static str {
        match self {
            Bound::Below => "below",
            Bound::Above => "above",
        }
    }

    fn index(self) -> usize {
        match self {
            Bound::Below => 0,
            Bound::Above => 1,
        }
    }
}

/// Per-response record of every integer moved into its wire field's
/// domain (issue #473).
///
/// O(1) memory and no allocation on a healthy response: `counts` is
/// inline and `first` stays `None`.
#[derive(Default, Debug)]
struct DomainReport {
    /// `[field.index()][bound.index()]`, so one slot per (field, bound)
    /// pair.
    counts: [[u64; 2]; 4],
    /// The first event, for the log line: the field, the id that names
    /// the trace or the span it came from, and the value the renderer
    /// was asked to emit.
    first: Option<(WireField, String, i64)>,
}

impl DomainReport {
    fn record(&mut self, field: WireField, id: &str, value: i64, bound: Bound) {
        self.counts[field.index()][bound.index()] += 1;
        if self.first.is_none() {
            self.first = Some((field, id.to_string(), value));
        }
    }

    fn total(&self) -> u64 {
        self.counts.iter().flatten().sum()
    }

    /// One `tracing::warn!` per RESPONSE — never per row, so a store
    /// full of negative widths cannot flood the log — plus one
    /// [`SATURATION_COUNTER`] increment per (field, bound) that actually
    /// fired. A no-op when nothing saturated.
    fn surface(&self) {
        let Some((first_field, first_id, first_value)) = &self.first else {
            return;
        };
        tracing::warn!(
            field = first_field.label(),
            id = %first_id,
            value = *first_value,
            saturations = self.total(),
            "traceql search: an integer was outside its wire field's domain and was saturated \
             into it; a value below zero means a stored start or width is negative, which no \
             mounted ingest route can produce"
        );
        for field in WireField::ALL {
            for bound in Bound::ALL {
                let n = self.counts[field.index()][bound.index()];
                if n != 0 {
                    metrics::counter!(
                        SATURATION_COUNTER,
                        "field" => field.label(),
                        "bound" => bound.label(),
                    )
                    .increment(n);
                }
            }
        }
    }
}

/// Projects an `i64` into a wire 64-bit UNSIGNED NANOSECOND domain by
/// saturation (issue #473). Every non-negative `i64` fits a `u64`, so
/// the upper clamp is inert and the operation here is exactly
/// "below `0` -> `0`" — which is what stops a minus-signed protojson
/// string, the form a strict decoder refuses, from ever being built.
///
/// `id` is the hex id of the trace or span the value came from,
/// recorded on `report` so the log line names what the decoder's own
/// error message cannot.
fn wire_nanos(value: i64, field: WireField, id: &str, report: &mut DomainReport) -> u64 {
    match u64::try_from(value) {
        Ok(nanos) => nanos,
        Err(_) => {
            report.record(field, id, value, Bound::Below);
            0
        }
    }
}

/// Trace-level milliseconds only — `TraceSearchMetadata.durationMs`
/// (`pkg/tempopb/tempo.proto:139` @ v3.0.2). A span never carries this,
/// and the nanoseconds fed in are the TRACE's envelope width, not the
/// root span's (issue #464).
///
/// **Projected into the wire's 32-bit UNSIGNED domain by SATURATION**
/// (issue #473): a millisecond count below `0` renders `0`, one above
/// `u32::MAX` renders `u32::MAX`. Returning `u32` is what makes an
/// out-of-domain `durationMs` unconstructible at the call site.
///
/// The reference computes `uint32(spanset.DurationNanos / 1_000_000)`
/// (`pkg/traceql/engine.go:295` @ v3.0.2), so a trace longer than ~49.7
/// days WRAPS there: captured on the pinned reference build, a 2^53 + 1
/// ns trace comes back `durationMs: 417264662` and an `i64::MAX` one
/// `2077252342`. We answer `4294967295` for BOTH — the same number for
/// two different inputs, which is what saturation means and what
/// wrapping cannot produce. Neither answer is the true duration;
/// saturated is at least a true lower bound a consumer can act on,
/// while wrapped is an ordinary-looking number indistinguishable from a
/// genuinely short trace. Recorded as
/// `traceql-search-duration-ms-saturates-not-wraps` in
/// docs/benchmarks/traces-differential-ledger.md.
///
/// `id` is the trace's hex id, recorded on `report` so the log line
/// names the trace that a strict decoder's own error message cannot.
fn duration_ms(duration_ns: i64, id: &str, report: &mut DomainReport) -> u32 {
    let ms = duration_ns / 1_000_000;
    match u32::try_from(ms) {
        Ok(ms) => ms,
        Err(_) if ms < 0 => {
            report.record(WireField::TraceDurationMs, id, ms, Bound::Below);
            0
        }
        Err(_) => {
            report.record(WireField::TraceDurationMs, id, ms, Bound::Above);
            u32::MAX
        }
    }
}

/// One `SpanSet.spans[]` entry. `durationNanos` is the reference's
/// `uint64` (`pkg/tempopb/tempo.proto:160` @ v3.0.2) rendered the way
/// protojson renders a `uint64`: as a JSON string, and **omitted entirely
/// when it is zero**, because protojson drops a default-valued scalar.
/// Zero is the width where a hand-written encoder and a protojson encoder
/// part company, so it is the one the tests must carry (see the module
/// doc for the captured bytes).
///
/// `start_ns` and `duration_ns` are non-negative by ingest construction
/// (`otlp_traces::resolve_duration_ns` clamps `end < start` and
/// `end == 0` to `0`), but both wire fields are `uint64` and both are
/// projected through [`wire_nanos`] anyway (issue #473): a value from a
/// write that bypassed `pulsus-write` would otherwise render a
/// minus-signed protojson string, which a strict decoder refuses,
/// discarding the whole response. **The omission test moves onto the
/// PROJECTED value**, so a negative width omits `durationNanos` exactly
/// as a zero does rather than emitting `"-5"`.
///
/// The intent this replaces — that a negative must surface a writer
/// regression rather than be hidden by the renderer — survives, off the
/// response: [`DomainReport`] logs it and counts it. A poisoned body
/// names no span.
fn span_json(span: &SpanSummary, report: &mut DomainReport) -> Value {
    let span_id = hex(&span.span_id);
    let start = wire_nanos(
        span.start_ns,
        WireField::SpanStartTimeUnixNano,
        &span_id,
        report,
    );
    let nanos = wire_nanos(
        span.duration_ns,
        WireField::SpanDurationNanos,
        &span_id,
        report,
    );
    let mut obj = json!({
        "spanID": span_id,
        "startTimeUnixNano": start.to_string(),
    });
    // Issue #479 — the `name` key is present IFF the query collected a
    // name for THIS span AND that name is non-empty. Both conditions are
    // the reference's, measured on its own response marshaller run on its
    // own span message: an empty name emits no key at all, while an empty
    // ATTRIBUTE VALUE is emitted as `"stringValue":""`. The asymmetry is
    // deliberate and both halves are asserted.
    //
    // The key ORDER on the wire does not move by writing this after the
    // literal: `serde_json`'s object map is ordered, not insertion-ordered
    // (`preserve_order` appears in zero workspace manifests), which is
    // what keeps the byte-exact response goldens in this file valid.
    if let Some(name) = span.name().filter(|n| !n.is_empty()) {
        obj["name"] = Value::String(name.to_string());
    }
    if nanos != 0 {
        obj["durationNanos"] = Value::String(nanos.to_string());
    }
    if !span.attributes.is_empty() {
        obj["attributes"] = Value::Array(
            span.attributes
                .iter()
                .map(|attr| json!({"key": attr.key(), "value": {"stringValue": attr.value()}}))
                .collect(),
        );
    }
    obj
}

/// One `by()` group-key value → the reference's typed `value:{…}` object
/// (issue #193). A `Double` renders from its `canonical_double_bits`
/// pattern via `f64::from_bits`; `Nil` renders no value object (the span
/// carried no value for this key).
fn group_value_json(value: &GroupValue) -> Value {
    match value {
        GroupValue::Str(s) => json!({ "stringValue": s }),
        GroupValue::Int(i) => json!({ "intValue": i.to_string() }),
        GroupValue::Double(bits) => json!({ "doubleValue": f64::from_bits(*bits) }),
        GroupValue::Bool(b) => json!({ "boolValue": b }),
        GroupValue::Nil => Value::Null,
    }
}

/// One `by()`-produced spanSet (issue #193): the typed group-key
/// `attributes` plus the per-group `matched` count and `spss`-capped span
/// summaries.
fn group_json(group: &SpanSetGroup, report: &mut DomainReport) -> Value {
    let attributes = group
        .attributes
        .iter()
        .map(|(key, value)| json!({"key": key, "value": group_value_json(value)}))
        .collect::<Vec<_>>();
    let spans = group
        .spans
        .iter()
        .map(|span| span_json(span, report))
        .collect::<Vec<_>>();
    json!({
        "attributes": attributes,
        "matched": group.matched,
        "spans": spans,
    })
}

/// One `traces[]` entry.
///
/// **`startTimeUnixNano` and `durationMs` are TRACE-level and come from
/// the trace's envelope, not from the root span** (issue #464). The
/// reference fills both from the spanset (`pkg/traceql/engine.go:294-295`
/// @ v3.0.2), whose writer computes `traceStart` and
/// `traceEnd - traceStart` over EVERY span of the trace
/// (`tempodb/encoding/vparquet4/schema.go:558-560`). Filling them from the
/// root span answers wrongly whenever a child starts before the root
/// (clock skew), ends after it, or extends the trace past the root's end —
/// measured on an adversarial four-trace corpus, three of four traces.
/// `rootServiceName`/`rootTraceName` stay the root span's, because those
/// are what the reference takes from it.
///
/// `durationMs` follows the SAME protojson
/// default-omission rule as the span level (issue #458, review round 2):
/// it is dropped when it is zero, and it is zero for every SUB-MILLISECOND
/// trace, not only for a zero-width one. Captured against
/// `grafana/tempo@sha256:aa8df8d0…`: `0`, `1` and `545000` ns all come
/// back with **no `durationMs` key**, while `42000000` ns comes back
/// `"durationMs":42`. The unit is what the omission tests, so the test is
/// on `duration_ms(...)`, never on `duration_ns`.
///
/// `durationMs` and `startTimeUnixNano` are projected into their wire
/// domains (issue #473) — see [`duration_ms`] and [`wire_nanos`]. Above
/// ~49.7 days the reference's `uint32` WRAPS and we SATURATE, so the two
/// part company for every such trace rather than only at exact multiples
/// of 2^32 ms; that divergence is
/// `traceql-search-duration-ms-saturates-not-wraps` in
/// docs/benchmarks/traces-differential-ledger.md. The omission rule runs
/// on the projected value, so a negative width omits the key.
///
/// **An EMPTY `rootServiceName` renders [`ROOT_SPAN_NOT_YET_RECEIVED`]**
/// (issue #473), which is what the reference substitutes for every trace
/// of a search response. `rootTraceName` is not substituted, on either
/// system, and neither is a `by(rootServiceName)` group value — that one
/// goes through [`group_value_json`], where the marker must never
/// appear.
fn trace_json(trace: &TraceSearchResult, report: &mut DomainReport) -> Value {
    // Issue #193: when a `by()` grouping is active (`groups` is `Some`),
    // emit one spanSet per group carrying typed `attributes`; otherwise
    // the flat single-spanSet path is byte-identical to the pre-#193
    // response.
    let trace_id = hex(&trace.trace_id);
    let span_sets = match &trace.groups {
        Some(groups) => groups
            .iter()
            .map(|group| group_json(group, report))
            .collect::<Vec<_>>(),
        None => vec![json!({
            "matched": trace.matched,
            "spans": trace
                .spans
                .iter()
                .map(|span| span_json(span, report))
                .collect::<Vec<_>>(),
        })],
    };
    let start = wire_nanos(
        trace.trace_start_ns,
        WireField::TraceStartTimeUnixNano,
        &trace_id,
        report,
    );
    let ms = duration_ms(trace.trace_duration_ns, &trace_id, report);
    let root_service = if trace.root.service.is_empty() {
        ROOT_SPAN_NOT_YET_RECEIVED
    } else {
        trace.root.service.as_str()
    };
    let mut obj = json!({
        "traceID": trace_id,
        "rootServiceName": root_service,
        "rootTraceName": trace.root.name,
        "startTimeUnixNano": start.to_string(),
        "spanSets": span_sets,
    });
    if ms != 0 {
        obj["durationMs"] = Value::from(ms);
    }
    obj
}

/// The full documented response envelope: `traces` in the engine's public
/// order (max matched-span timestamp DESC, trace id ASC), plus `metrics` —
/// which is `tempopb.SearchMetrics` (`pkg/tempopb/tempo.proto:164-172` @
/// v3.0.2) and NOTHING else.
///
/// This block used to carry `partial`, `limit` and `returned`, and all
/// three were invented (issue #464). Grafana's Tempo datasource decodes a
/// search response with `jsonpb.Unmarshal` (`pkg/tempo/search.go:95` @
/// `v13.1.5-11-g3c7375b`), which REJECTS an unknown field and returns the
/// error instead of results — so those keys did not degrade trace search
/// through that client, they disabled it.
///
/// `limit` and `returned` are dropped outright: `limit` is the caller's own
/// request parameter and `returned` is `traces.len()`. `partial` has no
/// like-for-like home — `PartialStatus` (`tempo.proto:383-386`) is on
/// `TraceByIDResponse`, `QueryRangeResponse` and `QueryInstantResponse`,
/// never on `SearchResponse` — so it rides the pair the search route DOES
/// use for incompleteness: `completedJobs < totalJobs`
/// (`modules/frontend/combiner/response_metrics.go:19-38`,
/// `combiner/search.go:126-135`). We run one plan, so `totalJobs` is 1 and
/// `completedJobs` is 1 or 0. **A zero `completedJobs` is OMITTED**, the way
/// protojson omits a default scalar and the way Grafana's own
/// `(metrics.completedJobs || 0) / (metrics.totalJobs || 1)`
/// (`src/streaming.ts:316`) reads it.
pub(crate) fn render(output: &SearchOutput) -> Value {
    // Issue #473: one report for the whole response, so a store full of
    // out-of-domain values logs once rather than once per row.
    let mut report = DomainReport::default();
    let traces = output
        .traces
        .iter()
        .map(|trace| trace_json(trace, &mut report))
        .collect::<Vec<_>>();
    let mut search_metrics = serde_json::Map::new();
    if !output.partial {
        search_metrics.insert("completedJobs".to_string(), Value::from(1u32));
    }
    search_metrics.insert("totalJobs".to_string(), Value::from(1u32));
    report.surface();
    json!({
        "traces": traces,
        "metrics": Value::Object(search_metrics),
    })
}

/// The `X-Pulsus-Explain: 1` trace for the search route (issue #492),
/// rendered with the same three documented keys the logs and metrics
/// routes use — `result_type`, `routing`, `stages` — plus the additive
/// `plan` key when the compiled plan's shape is present.
///
/// `routing` is always `null` here: a search never routes between
/// tables. `plan` is always absent today, because no read path calls the
/// compile core yet.
pub(crate) fn explain_value(e: &pulsus_read::PlanExplain) -> Value {
    let stages = e
        .stages
        .iter()
        .map(|s| {
            json!({
                "name": s.name,
                "sql": s.sql,
                "note": s.note,
            })
        })
        .collect::<Vec<_>>();
    let mut out = serde_json::Map::new();
    out.insert("result_type".to_string(), Value::from(e.result_type));
    out.insert("routing".to_string(), Value::Null);
    out.insert("stages".to_string(), Value::Array(stages));
    if let Some(plan) = &e.plan
        && let Ok(v) = serde_json::to_value(plan)
    {
        out.insert("plan".to_string(), v);
    }
    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use pulsus_read::{ProjectedAttribute, RootSummary};

    use super::*;

    /// Every field of `tempopb.SearchMetrics`
    /// (`pkg/tempopb/tempo.proto:164-172` @ grafana/tempo v3.0.2 /
    /// `0c4b926d09234186de39833e9c7ecb5b7614c8b9`), transcribed by hand.
    ///
    /// **This list is a transcription and nothing in CI checks it.**
    /// `tempopb` is not vendored and this workspace has no Go step, so a
    /// field added to the reference will not be noticed here, and a name
    /// mistyped into this list would let an invented key spelled that way
    /// through. It is checkable by a human against the checkout.
    const SEARCH_METRICS_FIELDS: [&str; 7] = [
        "inspectedTraces",
        "inspectedBytes",
        "totalBlocks",
        "completedJobs",
        "totalJobs",
        "totalBlockBytes",
        "inspectedSpans",
    ];

    /// The values of `SearchOutput::partial` — the ONLY field
    /// [`render`] branches on when it builds the `metrics` block, and so
    /// the exact enumeration the documented metrics shape is derived over
    /// (issue #464).
    ///
    /// **This is not "every branch of `render`".** `render` reaches at
    /// least five other branch points, all of them inside the `traces`
    /// array and none of them able to move a `metrics` key:
    /// [`trace_json`]'s `groups` split and its zero-`durationMs`
    /// omission, [`span_json`]'s zero-`durationNanos` omission and its
    /// empty-`attributes` omission, and [`group_value_json`]'s type
    /// match. The constant covers the metrics projection only.
    const METRICS_PARTIALITY_BRANCHES: [bool; 2] = [false, true];

    fn sample_output() -> SearchOutput {
        SearchOutput {
            traces: vec![TraceSearchResult {
                trace_id: [0xab; 16],
                // Issue #464: the root span's window is deliberately NOT
                // the trace's — it starts 1 s in and is 1 ms wide, while
                // the trace starts at the base instant and runs 2500 ms.
                // A fixture where the two coincide cannot tell the
                // trace-level rule from the root-span rule, which is how
                // the root-span reading survived until now.
                root: RootSummary {
                    service: "checkout".to_string(),
                    name: "GET /pay".to_string(),
                    start_ns: 1_700_000_001_000_000_000,
                    duration_ns: 1_000_000,
                },
                trace_start_ns: 1_700_000_000_000_000_000,
                trace_duration_ns: 2_500_000_000,
                matched: 5,
                spans: vec![SpanSummary::new(
                    [0xcd; 8],
                    Some("charge".to_string()),
                    1_700_000_000_100_000_000,
                    42_000_000,
                    // Issue #479: the wire key is the BARE attribute name.
                    // A fixture cannot invent one — `ProjectedAttribute`'s
                    // fields are private and its only constructor takes
                    // the query's own `Field`.
                    vec![ProjectedAttribute::new(
                        &pulsus_traceql::Field::Attribute {
                            scope: pulsus_traceql::AttrScope::Span,
                            key: "foo".to_string(),
                        },
                        "bar".to_string(),
                    )],
                )],
                groups: None,
            }],
            partial: true,
            returned: 1,
            limit: 20,
        }
    }

    // ---- issue #479: the matched-span projection -------------------------

    /// One projected attribute built from a span-scoped attribute field —
    /// a fixture cannot invent a key, only choose the field the key is
    /// derived from.
    fn projected(key: &str, value: &str) -> ProjectedAttribute {
        ProjectedAttribute::new(
            &pulsus_traceql::Field::Attribute {
                scope: pulsus_traceql::AttrScope::Span,
                key: key.to_string(),
            },
            value.to_string(),
        )
    }

    /// The five keys [`span_json`] can emit. Used both by the name axis
    /// (one name value equal to each of them) and by the predicted key
    /// set, so the two cannot drift apart.
    const SPAN_WIRE_KEYS: [&str; 5] = [
        "spanID",
        "startTimeUnixNano",
        "durationNanos",
        "attributes",
        "name",
    ];

    /// PRE-COMMITTED SELECTION RULE. Per axis: for `name`, the absent
    /// case, the empty string, one ordinary value, one non-ASCII value,
    /// and ONE VALUE EQUAL TO EACH KEY THIS RENDERER CAN EMIT; for
    /// `duration_ns`, both saturating extremes and `-1`, `0`, `1`; for
    /// `attributes`, empty, one entry with an empty value, two entries
    /// sharing a key, one entry keyed with a wire key name, one entry
    /// with an empty key; for `start_ns`, `1` and `i64::MAX`.
    /// 9 x 5 x 5 x 2 = 450 inputs.
    ///
    /// THE START AXIS CARRIES NO ZERO, and that is not a break-driven
    /// edit: at a zero start the reference's measured behaviour and ours
    /// differ, that difference is recorded, undiagnosed and out of scope
    /// for this issue, and no criterion may assert either answer there.
    /// [`axis_guard`] keeps it out.
    ///
    /// THIS IS NOT RENDERER-WIDE: a key inserted on a value outside these
    /// axes is admitted. The residual is stated rather than hidden.
    fn derived_span_inputs() -> Vec<SpanSummary> {
        let mut names: Vec<Option<String>> = vec![
            None,
            Some(String::new()),
            Some("GET /pay".to_string()),
            Some("München".to_string()),
        ];
        names.extend(SPAN_WIRE_KEYS.iter().map(|k| Some((*k).to_string())));

        let durations = [i64::MIN, -1, 0, 1, i64::MAX];
        let attribute_shapes: [Vec<ProjectedAttribute>; 5] = [
            vec![],
            vec![projected("foo", "")],
            vec![projected("foo", "S-span"), projected("foo", "R-resource")],
            vec![projected("name", "wire-key-name")],
            vec![projected("", "empty-key")],
        ];
        let starts = [1i64, i64::MAX];

        let mut out = Vec::new();
        for name in &names {
            for duration_ns in durations {
                for attrs in &attribute_shapes {
                    for start_ns in starts {
                        out.push(SpanSummary::new(
                            [1u8; 8],
                            name.clone(),
                            start_ns,
                            duration_ns,
                            attrs.clone(),
                        ));
                    }
                }
            }
        }
        out
    }

    /// Every key [`predicted_keys`] requires UNCONDITIONALLY must be
    /// non-default on every input, so the criterion can never mandate a
    /// key the reference's measured default-omission rule omits. Asserted
    /// FIRST, before any input is rendered.
    fn axis_guard(all: &[SpanSummary]) -> Result<(), String> {
        for s in all {
            if s.start_ns == 0 {
                return Err(format!("axis carries start_ns=0: {:?}", s.name()));
            }
            if s.span_id == [0u8; 8] {
                return Err("axis carries an all-zero span id".to_string());
            }
        }
        Ok(())
    }

    /// The key set the RULE predicts — computed here, never from the
    /// renderer, so the renderer cannot be its own oracle.
    ///
    /// `spanID` and `startTimeUnixNano` are unconditional; `durationNanos`
    /// is present iff the projected width is non-zero (a negative width
    /// clamps to zero and a zero width is omitted, exactly as the
    /// reference's default-value omission does); `attributes` iff the list
    /// is non-empty; `name` iff a name was collected AND it is non-empty.
    fn predicted_keys(span: &SpanSummary) -> std::collections::BTreeSet<String> {
        let mut keys = std::collections::BTreeSet::new();
        keys.insert("spanID".to_string());
        keys.insert("startTimeUnixNano".to_string());
        if span.duration_ns > 0 {
            keys.insert("durationNanos".to_string());
        }
        if !span.attributes.is_empty() {
            keys.insert("attributes".to_string());
        }
        if span.name().is_some_and(|n| !n.is_empty()) {
            keys.insert("name".to_string());
        }
        keys
    }

    /// AC16 — the rendered span object is exact for every input in the
    /// derived set.
    ///
    /// The test name understates what the test does, and that is
    /// deliberate: it also asserts the `name` VALUE, the attribute array
    /// and the axis guard. A name that says less than the truth misleads
    /// nobody.
    #[test]
    fn the_rendered_span_key_set_is_exact_over_the_derived_inputs() {
        let inputs = derived_span_inputs();
        assert_eq!(
            inputs.len(),
            450,
            "9 x 5 x 5 x 2 — silently shrinking an axis must fail rather than reduce coverage"
        );
        axis_guard(&inputs).expect("the axis guard must pass before anything is rendered");

        for span in &inputs {
            let got = span_json(span, &mut DomainReport::default());
            let obj = got.as_object().expect("span_json renders an object");
            let keys: std::collections::BTreeSet<String> = obj.keys().cloned().collect();

            // (1) exact set equality against the RULE's prediction.
            assert_eq!(
                keys,
                predicted_keys(span),
                "key set at name={:?} duration_ns={} attrs={} start_ns={}",
                span.name(),
                span.duration_ns,
                span.attributes.len(),
                span.start_ns
            );

            // (2) the SERIALISED body carries neither forbidden literal
            // anywhere in it, at ANY depth. Narrowing this to the
            // top-level object defeats it: a renderer that keeps every
            // predicted top-level key and renders a NESTED field as a
            // null- or empty-valued `name` passes (1), (3) and (4).
            let body = serde_json::to_string(&got).expect("serialises");
            assert!(
                !body.contains("\"name\":null"),
                "a JSON null name is never a wire value: {body}"
            );
            assert!(
                !body.contains("\"name\":\"\""),
                "an empty name is omitted, not emitted: {body}"
            );

            // (3) when the `name` key is present its value is EXACTLY the
            // collected name.
            if let Some(rendered) = obj.get("name") {
                assert_eq!(
                    rendered.as_str(),
                    span.name(),
                    "the rendered name must be the collected one, unaltered"
                );
            }

            // (4) the attribute array, in order, including an entry whose
            // value is the empty string.
            if !span.attributes.is_empty() {
                let want: Vec<Value> = span
                    .attributes
                    .iter()
                    .map(|a| json!({"key": a.key(), "value": {"stringValue": a.value()}}))
                    .collect();
                assert_eq!(obj["attributes"], Value::Array(want));
            }
        }

        // Outside the axes and on purpose: the adversarial input assertion
        // (2) looks most likely to false-positive on. An attribute KEYED
        // `name` and VALUED with the empty string renders
        // `{"key":"name","value":{"stringValue":""}}`, which contains
        // neither forbidden literal.
        let adversarial = SpanSummary::new(
            [1u8; 8],
            Some("name".to_string()),
            1,
            1,
            vec![projected("name", ""), projected("", "")],
        );
        let body = serde_json::to_string(&span_json(&adversarial, &mut DomainReport::default()))
            .expect("serialises");
        assert!(!body.contains("\"name\":null"), "{body}");
        assert!(!body.contains("\"name\":\"\""), "{body}");
    }

    /// AC13's wire half — a summary that collected no name renders NO
    /// `name` key at all, not `"name":null` and not `"name":""`.
    ///
    /// `get("name").is_none()` rather than `is_null()`: the two are
    /// different bodies and only one of them is the reference's.
    #[test]
    fn a_none_name_writes_no_wire_field() {
        for name in [None, Some(String::new())] {
            let span = SpanSummary::new([0xab; 8], name.clone(), 1, 1, vec![]);
            let got = span_json(&span, &mut DomainReport::default());
            assert!(
                got.get("name").is_none(),
                "name={name:?} must write no key at all, got {got}"
            );
            let body = serde_json::to_string(&got).expect("serialises");
            assert!(!body.contains("\"name\""), "{body}");
        }
    }

    /// AC12 — the wire-cost identity of a projected attribute list.
    ///
    /// `,"attributes":[]` is 16 bytes, `{"key":"","value":{"stringValue":""}}`
    /// is 37, and one comma joins entries, so the delta between the two
    /// renderings is an ARITHMETIC identity, not a fitted number. It is
    /// asserted for a flat span AND for a span inside a `by()` group,
    /// because the identity is per span and must not depend on the
    /// response shape.
    #[test]
    fn projected_attributes_cost_exactly_the_rendered_bytes() {
        let attrs = vec![
            projected("http.method", "GET"),
            projected("service.name", "checkout"),
            projected("", ""),
        ];
        let expected: usize = 16
            + attrs
                .iter()
                .map(|a| 37 + a.key().len() + a.value().len())
                .sum::<usize>()
            + (attrs.len() - 1);

        // Flat.
        let without = SpanSummary::new([0xcd; 8], Some("charge".to_string()), 1, 1, vec![]);
        let with = SpanSummary::new([0xcd; 8], Some("charge".to_string()), 1, 1, attrs.clone());
        let len = |s: &SpanSummary| {
            serde_json::to_string(&span_json(s, &mut DomainReport::default()))
                .expect("serialises")
                .len()
        };
        assert_eq!(len(&with) - len(&without), expected);

        // Inside a `by()` group — the same identity, a different envelope.
        let group_len = |spans: Vec<SpanSummary>| {
            let group = SpanSetGroup {
                attributes: vec![("by(name)".to_string(), GroupValue::Str("x".to_string()))],
                matched: 1,
                spans,
            };
            serde_json::to_string(&group_json(&group, &mut DomainReport::default()))
                .expect("serialises")
                .len()
        };
        assert_eq!(
            group_len(vec![with]) - group_len(vec![without]),
            expected,
            "the identity is per span and does not depend on the response shape"
        );
    }

    #[test]
    fn render_emits_the_documented_envelope() {
        let v = render(&sample_output());
        assert_eq!(
            v["traces"][0]["traceID"],
            "abababababababababababababababab"
        );
        assert_eq!(v["traces"][0]["rootServiceName"], "checkout");
        assert_eq!(v["traces"][0]["rootTraceName"], "GET /pay");
        // Issue #464: both are TRACE-level. The fixture's root span
        // starts 1 s later and is 1 ms wide, so neither assertion below
        // can be satisfied by the root span's own window.
        let root = &sample_output().traces[0].root;
        assert_ne!(root.start_ns, 1_700_000_000_000_000_000);
        assert_ne!(root.duration_ns, 2_500_000_000);
        assert_eq!(v["traces"][0]["startTimeUnixNano"], "1700000000000000000");
        assert_eq!(v["traces"][0]["durationMs"], 2500);
        // Issue #464: the whole block, not three keys of it — the
        // retired `partial`/`limit`/`returned` were invented and a
        // per-key assertion cannot see a fourth key reappearing.
        assert_eq!(
            v["metrics"],
            serde_json::json!({"totalJobs": 1}),
            "the sample output is truncated: completedJobs is zero and protojson omits a zero"
        );
    }

    /// Issue #458 defect A: a span carries `durationNanos` as a protojson
    /// `uint64` — a JSON **string** of NANOSECONDS
    /// (`pkg/tempopb/tempo.proto:160`, `pkg/traceql/engine.go:311` @
    /// v3.0.2) — and carries no `durationMs` at all. The Grafana Tempo
    /// datasource reads exactly this field: `src/types.ts:89` types it
    /// `durationNanos: string` and `src/resultTransformer.ts:942` does
    /// `parseInt(span.durationNanos, 10)` into an `ns`-unit frame column,
    /// so an absent field renders the Duration column as `NaN`.
    #[test]
    fn span_sets_carry_matched_count_and_span_summaries() {
        let v = render(&sample_output());
        let set = &v["traces"][0]["spanSets"][0];
        assert_eq!(set["matched"], 5);
        assert_eq!(set["spans"][0]["spanID"], "cdcdcdcdcdcdcdcd");
        assert_eq!(set["spans"][0]["name"], "charge");
        assert_eq!(
            set["spans"][0]["durationNanos"],
            serde_json::Value::String("42000000".to_string()),
            "protojson renders a uint64 as a JSON string, not a number"
        );
        assert!(
            set["spans"][0].get("durationNanos").is_some(),
            "a non-zero width is present; only zero is omitted"
        );
        assert!(
            set["spans"][0].get("durationMs").is_none(),
            "the reference's Span message has no durationMs field; it belongs to \
             TraceSearchMetadata one level up"
        );
        // Issue #479: the projected attribute key is the BARE attribute
        // name. The reference carries the scope as a separate field of
        // its own attribute struct and sends `http.method`, never
        // `span.http.method`; ours is the same shape.
        assert_eq!(
            set["spans"][0]["attributes"][0],
            serde_json::json!({"key": "foo", "value": {"stringValue": "bar"}})
        );
    }

    /// Issue #458 defect A: every width is asserted against a span object
    /// **captured from the reference**, not against one written from our
    /// own output.
    ///
    /// That distinction is the whole point of this test. The first cut of
    /// it listed six widths and spelled the expected value for each from
    /// what this module already emitted; at `0` that was
    /// `"durationNanos":"0"` and the reference emits **no key at all**, so
    /// the suite was green while the byte-parity contract it exists to
    /// enforce was false. An expected value taken from the implementation
    /// cannot fail the way the implementation fails.
    ///
    /// **Provenance.** `grafana/tempo@sha256:aa8df8d069f77b82e978464daf551`
    /// `69bb8d135852ad58700aa96880653c3d8f7` — the digest
    /// `.github/workflows/ci.yml` pins — started with this repo's
    /// `ci/tempo/tempo-compare.yaml` unmodified, one OTLP JSON span per
    /// width pushed to `POST /v1/traces`, then
    /// `GET /api/search?q={name="<n>"}&start=<now-300>&end=<now+60>&limit=5`.
    /// The `i64::MAX` row needed a different window: the reference parses
    /// `end` as a `uint32` of seconds and answers
    /// `invalid end: strconv.ParseUint: parsing "9223372037": value out of
    /// range`, and it caps a range at 168h — so that span was anchored at
    /// `startTimeUnixNano = 1` and captured over `start=0&end=604800`.
    ///
    /// **Capturing these is fiddlier than it looks, and none of it is
    /// about this repo.** Two things bit, and both cost time:
    ///
    /// * A search over a RECENT window can come back `{"traces":[]}` on a
    ///   freshly written block while `inspectedBytes` climbs — data
    ///   demonstrably read, nothing matched — so an empty answer is not
    ///   evidence of an empty store. Poll it; it starts answering.
    /// * Pushing the epoch-anchored `i64::MAX` fixture in the SAME batch
    ///   as the recent widths made every recent width unsearchable for
    ///   minutes. Two pushes into two blocks, not one payload.
    ///
    /// **Key ORDER is deliberately not compared.** `serde_json` sorts
    /// object keys and the reference emits declaration order; a JSON
    /// object is unordered and the datasource reads `span.durationNanos`
    /// by key (`src/resultTransformer.ts:942`). What IS compared is the
    /// key SET — which is how the missing-at-zero case is caught — and
    /// every value with its JSON type.
    #[test]
    fn every_span_width_renders_the_reference_captured_span_object() {
        // (duration_ns, the span object the reference returned for it).
        // Verbatim capture: `spanID`, `name` and `startTimeUnixNano` are
        // the fixture's own, so the comparison is total rather than
        // field-by-field.
        let captures: [(i64, &str); 7] = [
            (
                0,
                r#"{"spanID":"4620000000000000","name":"fresh-w0","startTimeUnixNano":"1787855428000000000"}"#,
            ),
            (
                1,
                r#"{"spanID":"4620000000000001","name":"fresh-w1","startTimeUnixNano":"1787855429000000000","durationNanos":"1"}"#,
            ),
            (
                545_000,
                r#"{"spanID":"4620000000000002","name":"fresh-w545000","startTimeUnixNano":"1787855430000000000","durationNanos":"545000"}"#,
            ),
            (
                42_000_000,
                r#"{"spanID":"4620000000000003","name":"fresh-w42000000","startTimeUnixNano":"1787855431000000000","durationNanos":"42000000"}"#,
            ),
            (
                9_007_199_254_740_993,
                r#"{"spanID":"4620000000000004","name":"fresh-w2p53p1","startTimeUnixNano":"1787855432000000000","durationNanos":"9007199254740993"}"#,
            ),
            (
                2_500_000_000,
                r#"{"spanID":"4620000000000005","name":"fresh-wtrace2500","startTimeUnixNano":"1787855433000000000","durationNanos":"2500000000"}"#,
            ),
            (
                i64::MAX,
                r#"{"spanID":"4600000064000000","name":"width-i64max-only","startTimeUnixNano":"1","durationNanos":"9223372036854775807"}"#,
            ),
        ];

        // The capture set must actually contain the discriminating widths;
        // a future edit that drops one of them fails here rather than
        // quietly narrowing what this test covers.
        let widths: Vec<i64> = captures.iter().map(|(ns, _)| *ns).collect();
        for required in [0, 1, 9_007_199_254_740_993, i64::MAX] {
            assert!(
                widths.contains(&required),
                "the captured set must carry the {required} ns width"
            );
        }

        for (duration_ns, captured) in captures {
            let want: serde_json::Value =
                serde_json::from_str(captured).expect("the capture is valid JSON");
            let span_id: Vec<u8> = (0..8)
                .map(|i| {
                    let hex = &want["spanID"].as_str().expect("spanID")[i * 2..i * 2 + 2];
                    u8::from_str_radix(hex, 16).expect("hex byte")
                })
                .collect();
            let got = span_json(
                &SpanSummary::new(
                    span_id.try_into().expect("8 bytes"),
                    Some(want["name"].as_str().expect("name").to_string()),
                    want["startTimeUnixNano"]
                        .as_str()
                        .expect("startTimeUnixNano")
                        .parse()
                        .expect("i64 nanos"),
                    duration_ns,
                    vec![],
                ),
                &mut DomainReport::default(),
            );
            assert_eq!(
                got, want,
                "{duration_ns} ns: our span object must be the reference's, key set and all \
                 (a JSON string is not a JSON number, and an ABSENT key is not a zero one)"
            );
        }
    }

    /// Issue #458 review round 2: the TRACE level follows the **same**
    /// protojson default-omission rule as the span level, and every width
    /// here is a trace object **captured from the reference**.
    ///
    /// This test replaces one that asserted `durationMs == 2500` and
    /// nothing else. That assertion was true and useless: it was chosen
    /// because the issue recorded the trace level as correct, and the
    /// widths that would have shown otherwise — every SUB-MILLISECOND one
    /// — were never tried. The reference omits `durationMs` for `0`, `1`
    /// and `545000` ns alike, because the field is MILLISECONDS and all
    /// three of those round to zero. The unit is what the omission tests.
    ///
    /// **Provenance.** Same container and route as
    /// [`every_span_width_renders_the_reference_captured_span_object`].
    /// `spanSet` (the deprecated singular) and `serviceStats` are removed
    /// from each capture before comparison: we do not emit either, which
    /// is a separately recorded gap and not this issue's to close.
    /// `spanSets` is removed from BOTH sides — its contents are span
    /// objects, and those are the other test's subject.
    ///
    /// **Where we deliberately differ, and it is pinned rather than
    /// skipped.** The reference truncates to `uint32`
    /// (`engine.go:295`), so a trace over ~49.7 days wraps; we SATURATE
    /// (issue #473). Two captured widths are above that boundary, and
    /// they are the only rows in the fixture whose reference value
    /// differs from ours: 2^53 + 1 ns is `417264662` there and
    /// `4294967295` here, and `i64::MAX` is `2077252342` there and
    /// `4294967295` here.
    ///
    /// **Those two rows are compared as a PAIR, after the loop, and no
    /// whole-object equality can express what they prove.** The
    /// reference's two values DIFFER from each other; ours are EQUAL to
    /// each other and equal to `u32::MAX`. The same number for two
    /// different inputs is the saturation property: a wrapping renderer
    /// answers `417264662` and `2077252342`, and the unclamped `i64`
    /// this module used to emit answers `9007199254` and
    /// `9223372036854` — both unequal pairs, so both redden here.
    #[test]
    fn every_trace_width_renders_the_reference_captured_trace_object() {
        let captures: [(i64, &str); 7] = [
            (
                0,
                r#"{"traceID":"46300000000000000000000000000000","rootServiceName":"rev458c-w0","rootTraceName":"tl-w0","startTimeUnixNano":"1787856404000000000"}"#,
            ),
            (
                1,
                r#"{"traceID":"46300000000000000000000000000001","rootServiceName":"rev458c-w1","rootTraceName":"tl-w1","startTimeUnixNano":"1787856405000000000"}"#,
            ),
            (
                545_000,
                r#"{"traceID":"46300000000000000000000000000002","rootServiceName":"rev458c-w545000","rootTraceName":"tl-w545000","startTimeUnixNano":"1787856406000000000"}"#,
            ),
            (
                42_000_000,
                r#"{"traceID":"46300000000000000000000000000003","rootServiceName":"rev458c-w42000000","rootTraceName":"tl-w42000000","startTimeUnixNano":"1787856407000000000","durationMs":42}"#,
            ),
            (
                9_007_199_254_740_993,
                r#"{"traceID":"46300000000000000000000000000004","rootServiceName":"rev458c-w2p53p1","rootTraceName":"tl-w2p53p1","startTimeUnixNano":"1787856408000000000","durationMs":417264662}"#,
            ),
            (
                2_500_000_000,
                r#"{"traceID":"46300000000000000000000000000005","rootServiceName":"rev458c-w2500ms","rootTraceName":"tl-w2500ms","startTimeUnixNano":"1787856409000000000","durationMs":2500}"#,
            ),
            (
                9_223_372_036_854_775_807,
                r#"{"traceID":"46300000000000000000000064000000","rootServiceName":"rev458c-i64max","rootTraceName":"tl-i64max","startTimeUnixNano":"1","durationMs":2077252342}"#,
            ),
        ];
        // The set must keep the widths that discriminate: a zero, a
        // sub-millisecond non-zero (the case that makes this about
        // MILLISECONDS rather than nanoseconds), and a whole-millisecond
        // one.
        let widths: Vec<i64> = captures.iter().map(|(ns, _)| *ns).collect();
        for required in [0, 545_000, 42_000_000, i64::MAX] {
            assert!(
                widths.contains(&required),
                "the captured set must carry the {required} ns width"
            );
        }
        // DO NOT DELETE either of the two over-`u32::MAX` rows, and DO
        // NOT edit either captured value to match ours. They are the only
        // captures whose reference value differs from ours, and therefore
        // the only rows that can tell saturation from wrapping and both
        // from an unclamped `i64`; the rest agree under all three rules.
        // Issue #464 makes this field carry the TRACE's width rather than
        // the root span's, which can only ENLARGE the set of stores that
        // reach them. Issue #473 is the "later change" the previous
        // version of this comment anticipated: these two rows were made
        // to PASS under saturation, not removed.
        let over_u32: Vec<i64> = widths
            .iter()
            .copied()
            .filter(|ns| ns / 1_000_000 > i64::from(u32::MAX))
            .collect();
        assert_eq!(
            over_u32.len(),
            2,
            "exactly the two widths above u32::MAX milliseconds must remain — they are the only rows that can tell a truncating durationMs from a non-truncating one, got {over_u32:?}"
        );

        // (duration_ns, the reference's value, ours) for every row above
        // the 32-bit millisecond maximum — compared as a pair after the
        // loop, because the property is a RELATION between the two rows.
        let mut saturating: Vec<(i64, i64, u32)> = Vec::new();

        for (duration_ns, captured) in captures {
            let want: serde_json::Value =
                serde_json::from_str(captured).expect("the capture is valid JSON");
            let trace_id: Vec<u8> = (0..16)
                .map(|i| {
                    let hex = &want["traceID"].as_str().expect("traceID")[i * 2..i * 2 + 2];
                    u8::from_str_radix(hex, 16).expect("hex byte")
                })
                .collect();
            let trace_start_ns: i64 = want["startTimeUnixNano"]
                .as_str()
                .expect("startTimeUnixNano")
                .parse()
                .expect("i64 nanos");
            let mut got = trace_json(
                &TraceSearchResult {
                    trace_id: trace_id.try_into().expect("16 bytes"),
                    // Issue #464: the widths under test are the TRACE's, so
                    // the root span is given a deliberately different window —
                    // 7 s later, 1 ns wide. If `trace_json` ever reads the
                    // root span again, every row here fails.
                    root: RootSummary {
                        service: want["rootServiceName"].as_str().expect("svc").to_string(),
                        name: want["rootTraceName"].as_str().expect("name").to_string(),
                        start_ns: trace_start_ns.saturating_add(7_000_000_000),
                        duration_ns: 1,
                    },
                    trace_start_ns,
                    trace_duration_ns: duration_ns,
                    matched: 1,
                    spans: vec![],
                    groups: None,
                },
                &mut DomainReport::default(),
            );
            got.as_object_mut().expect("object").remove("spanSets");

            if duration_ns / 1_000_000 > i64::from(u32::MAX) {
                assert!(
                    got.get("durationMs").is_some() && want.get("durationMs").is_some(),
                    "{duration_ns} ns: both sides must carry the key — the divergence is the \
                     VALUE, and an absent key on either side would hide it"
                );
                let theirs = want["durationMs"].as_i64().expect("reference durationMs");
                let raw = got["durationMs"].as_u64().expect("our durationMs");
                let ours = u32::try_from(raw).unwrap_or_else(|_| {
                    panic!("{duration_ns} ns: durationMs must fit the wire uint32, got {raw}")
                });
                saturating.push((duration_ns, theirs, ours));
            } else {
                assert_eq!(
                    got, want,
                    "{duration_ns} ns: our trace object must be the reference's, key set and all \
                     — durationMs is MILLISECONDS, so every sub-millisecond width omits it"
                );
            }
        }

        // The saturation identity, on the two captured over-maximum rows.
        assert_eq!(
            saturating.len(),
            2,
            "both over-maximum captures must have been compared, got {saturating:?}"
        );
        assert_ne!(
            saturating[0].1, saturating[1].1,
            "the two CAPTURED reference values must differ from each other — a pair that agrees \
             cannot discriminate anything: {saturating:?}"
        );
        for (duration_ns, theirs, ours) in &saturating {
            assert_ne!(
                *theirs,
                i64::from(*ours),
                "{duration_ns} ns: the CAPTURE is evidence and is never edited to match us — a \
                 captured value equal to ours means the fixture was changed, not the reference"
            );
        }
        assert_eq!(
            saturating[0].2, saturating[1].2,
            "ours must be the SAME number for two different inputs: that is what saturation \
             means, and neither wrapping nor an unclamped i64 can produce it: {saturating:?}"
        );
        assert_eq!(
            saturating[0].2,
            u32::MAX,
            "and that shared value is the wire field's maximum: {saturating:?}"
        );
    }

    /// Issue #464: the trace level's `startTimeUnixNano` and `durationMs`
    /// are the TRACE's envelope — the spanset's `StartTimeUnixNanos` and
    /// `DurationNanos` (`pkg/traceql/engine.go:294-295` @ v3.0.2,
    /// computed over every span at
    /// `tempodb/encoding/vparquet4/schema.go:558-560`) — and not the root
    /// span's own window.
    ///
    /// **Provenance.** One identical OTLP/JSON push to the pinned
    /// reference container
    /// (`grafana/tempo@sha256:aa8df8d069f77b82e978464daf55169bb8d135852ad58700aa96880653c3d8f7`,
    /// this repo's `ci/tempo/tempo-compare.yaml` unmodified) and to
    /// PulsusDB, service `rev464b`, then
    /// `GET /api/search?q={resource.service.name="rev464b"}&start=<base-300>&end=<base+300>&limit=20`.
    /// The four trace objects below are the reference's answers verbatim,
    /// with `spanSet`/`spanSets`/`serviceStats` removed (we emit none of
    /// the three; separately recorded).
    ///
    /// **The corpus is adversarial by construction, and that is asserted
    /// before anything is compared.** Three of the four traces answer
    /// differently under the two rules — a root that starts after its
    /// child, a later child that extends the trace, and a child that ends
    /// after the root — and the fourth is a single-span control where the
    /// root IS the trace. A corpus whose root windows coincide with its
    /// traces' cannot tell the two rules apart, which is exactly how the
    /// root-span reading survived issue #458.
    #[test]
    fn the_trace_envelope_renders_the_reference_captured_start_and_duration() {
        // (root start_ns, root duration_ns, trace start_ns, trace
        //  duration_ns, the trace object the reference returned).
        const BASE: i64 = 1_787_922_982_000_000_000;
        let captures: [(i64, i64, i64, i64, &str); 4] = [
            (
                // root +5 s, 42 ms — it starts AFTER its own child.
                BASE + 5_000_000_000,
                42_000_000,
                BASE,
                5_042_000_000,
                r#"{"traceID":"a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1","rootServiceName":"rev464b","rootTraceName":"root-skew","startTimeUnixNano":"1787922982000000000","durationMs":5042}"#,
            ),
            (
                // root +0, 42 ms; a child 1 s later extends the trace.
                BASE,
                42_000_000,
                BASE,
                1_000_000_000,
                r#"{"traceID":"b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1","rootServiceName":"rev464b","rootTraceName":"root-short","startTimeUnixNano":"1787922982000000000","durationMs":1000}"#,
            ),
            (
                // root +0, 10 ms; a child starts inside it and ends after.
                BASE,
                10_000_000,
                BASE,
                35_000_000,
                r#"{"traceID":"d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1","rootServiceName":"rev464b","rootTraceName":"root-overrun","startTimeUnixNano":"1787922982000000000","durationMs":35}"#,
            ),
            (
                // The control: one span, so the root IS the trace and
                // both rules agree.
                BASE,
                2_500_000_000,
                BASE,
                2_500_000_000,
                r#"{"traceID":"c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1","rootServiceName":"rev464b","rootTraceName":"root-only","startTimeUnixNano":"1787922982000000000","durationMs":2500}"#,
            ),
        ];

        // The corpus relation, asserted BEFORE any value comparison: a
        // narrowed corpus fails here rather than passing under both rules.
        assert!(
            captures
                .iter()
                .any(|(root_start, _, trace_start, _, _)| root_start > trace_start),
            "the corpus must carry a trace whose ROOT starts after the trace does"
        );
        assert!(
            captures
                .iter()
                .filter(|(_, root_dur, _, trace_dur, _)| root_dur != trace_dur)
                .count()
                >= 2,
            "the corpus must carry at least two traces whose ROOT width differs from the trace's"
        );

        for (root_start_ns, root_duration_ns, trace_start_ns, trace_duration_ns, captured) in
            captures
        {
            let want: serde_json::Value =
                serde_json::from_str(captured).expect("the capture is valid JSON");
            let trace_id: Vec<u8> = (0..16)
                .map(|i| {
                    let hex = &want["traceID"].as_str().expect("traceID")[i * 2..i * 2 + 2];
                    u8::from_str_radix(hex, 16).expect("hex byte")
                })
                .collect();
            let mut got = trace_json(
                &TraceSearchResult {
                    trace_id: trace_id.try_into().expect("16 bytes"),
                    root: RootSummary {
                        service: want["rootServiceName"].as_str().expect("svc").to_string(),
                        name: want["rootTraceName"].as_str().expect("name").to_string(),
                        start_ns: root_start_ns,
                        duration_ns: root_duration_ns,
                    },
                    trace_start_ns,
                    trace_duration_ns,
                    matched: 1,
                    spans: vec![],
                    groups: None,
                },
                &mut DomainReport::default(),
            );
            got.as_object_mut().expect("object").remove("spanSets");
            assert_eq!(
                got, want,
                "trace envelope ({trace_start_ns}, {trace_duration_ns} ns) against root span \
                 ({root_start_ns}, {root_duration_ns} ns): the rendered object must be the \
                 reference's — startTimeUnixNano and durationMs are TRACE-level"
            );
        }
    }

    #[test]
    fn an_empty_output_renders_the_documented_empty_envelope() {
        let v = render(&SearchOutput {
            traces: vec![],
            partial: false,
            returned: 0,
            limit: 20,
        });
        assert_eq!(v["traces"], serde_json::json!([]));
        // Issue #464 AC 3: zero traces is not a truncated result, so
        // `completedJobs` must be PRESENT. This is the case a "just drop
        // the invented keys" fix sails through and a wrong-branch fix
        // does not.
        assert_eq!(
            v["metrics"],
            serde_json::json!({"completedJobs": 1, "totalJobs": 1}),
            "an empty complete search: one job, one completed"
        );
    }

    /// Issue #464 AC 1: the `metrics` block is the reference's own
    /// `completedJobs`/`totalJobs` pair, asserted as a WHOLE OBJECT on
    /// each completeness branch — a per-key assertion is what let three
    /// invented keys ship.
    ///
    /// `completedJobs` is omitted on the truncated branch because
    /// protojson omits a default-valued scalar, and an omitted key is a
    /// zero rather than a missing value: `completedJobs (absent => 0) <
    /// totalJobs (1)` is the incompleteness signal the search route
    /// carries (`modules/frontend/combiner/response_metrics.go:19-38`,
    /// `combiner/search.go:126-135` @ v3.0.2).
    #[test]
    fn the_metrics_block_is_the_reference_jobs_pair_on_both_partiality_branches() {
        let mut complete = sample_output();
        complete.partial = false;
        assert_eq!(
            render(&complete)["metrics"],
            serde_json::json!({"completedJobs": 1, "totalJobs": 1}),
            "a complete search: one job, one completed"
        );

        let mut truncated = sample_output();
        truncated.partial = true;
        assert_eq!(
            render(&truncated)["metrics"],
            serde_json::json!({"totalJobs": 1}),
            "a truncated search: completedJobs is zero, and protojson omits a zero"
        );
    }

    /// Issue #464 AC 2: no key outside `tempopb.SearchMetrics` survives on
    /// either branch. Grafana's Tempo datasource decodes the search
    /// response with unknown fields REJECTED (`pkg/tempo/search.go:95` @
    /// `v13.1.5-11-g3c7375b`), so one invented key costs the whole
    /// response, not one field of it.
    ///
    /// **Scope, stated exactly.** This detects an *invented key* and
    /// nothing else. It does not detect a wrong value, a wrong branch or
    /// a missing key — those belong to
    /// [`the_metrics_block_is_the_reference_jobs_pair_on_both_partiality_branches`],
    /// and every value/branch break leaves this test green.
    #[test]
    fn every_metrics_key_is_a_tempopb_search_metrics_field() {
        for partial in METRICS_PARTIALITY_BRANCHES {
            let mut output = sample_output();
            output.partial = partial;
            let rendered = render(&output);
            let metrics = rendered["metrics"].as_object().expect("metrics object");
            for key in metrics.keys() {
                assert!(
                    SEARCH_METRICS_FIELDS.contains(&key.as_str()),
                    "partial={partial}: {key:?} is not a tempopb.SearchMetrics field"
                );
            }
        }
    }

    /// Issue #464 AC 6b: the metrics shape in docs/api.md §4.2 is
    /// DERIVED from [`render`] over both partiality branches, never
    /// restated. That sentence is the public contract for
    /// `/api/traces/v1/search` and its `/api/search` alias, and the
    /// hermetic docs pin in `crates/pulsus-read/tests/traces_search_sql.rs`
    /// compares the document against a *constant*; this is the half that
    /// compares it against the code.
    ///
    /// **What it couples:** the union of the `metrics` key sets across
    /// [`METRICS_PARTIALITY_BRANCHES`], against the documentation. Both
    /// branches, because a key emitted only when `output.partial` slips
    /// past a single-call version of this test.
    ///
    /// **What it is not.** It is not "every branch of `render`" (see
    /// [`METRICS_PARTIALITY_BRANCHES`] for the five it does not reach),
    /// it does not reach the VALUES, and it does not reach the omission
    /// rule: a key present on one branch and absent on the other is
    /// indistinguishable here from a key present on both. Those are the
    /// subjects of the two tests above and of the empty-envelope test.
    #[test]
    fn the_documented_metrics_shape_is_derived_from_both_partiality_branches() {
        let mut keys: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for partial in METRICS_PARTIALITY_BRANCHES {
            let mut output = sample_output();
            output.partial = partial;
            let rendered = render(&output);
            let metrics = rendered["metrics"].as_object().expect("metrics object");
            keys.extend(metrics.keys().cloned());
        }
        let shape = format!(
            "{{{}}}",
            keys.iter()
                .map(|key| format!("\"{key}\":<n>"))
                .collect::<Vec<_>>()
                .join(",")
        );
        let api = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/api.md"),
        )
        .expect("read docs/api.md");
        assert!(
            api.contains(&shape),
            "docs/api.md §4.2 must document the metrics-key union over both partiality \
             branches, {shape:?} — derived from render(), never restated"
        );
    }

    #[test]
    fn spans_without_selected_fields_omit_the_attributes_key() {
        let mut output = sample_output();
        output.traces[0].spans[0].attributes.clear();
        let v = render(&output);
        assert!(
            v["traces"][0]["spanSets"][0]["spans"][0]
                .get("attributes")
                .is_none()
        );
    }

    fn group_span(id: u8, name: &str) -> SpanSummary {
        SpanSummary::new(
            [id; 8],
            Some(name.to_string()),
            1_700_000_000_100_000_000,
            1_000_000,
            vec![],
        )
    }

    /// Issue #193: an active `by()` grouping emits ONE spanSet per group,
    /// each carrying typed `attributes`; the flat `matched`/`spans` are not
    /// serialized.
    #[test]
    fn grouped_output_emits_one_span_set_per_group_with_typed_attributes() {
        let mut output = sample_output();
        output.traces[0].groups = Some(vec![
            SpanSetGroup {
                attributes: vec![(
                    "by(resource.service.name)".to_string(),
                    GroupValue::Str("checkout".to_string()),
                )],
                matched: 2,
                spans: vec![group_span(0x01, "a")],
            },
            SpanSetGroup {
                attributes: vec![(
                    "by(resource.service.name)".to_string(),
                    GroupValue::Str("billing".to_string()),
                )],
                matched: 3,
                spans: vec![group_span(0x02, "b"), group_span(0x03, "c")],
            },
        ]);
        let v = render(&output);
        let sets = v["traces"][0]["spanSets"].as_array().expect("array");
        assert_eq!(sets.len(), 2, "one spanSet per group");
        assert_eq!(sets[0]["matched"], 2);
        assert_eq!(
            sets[0]["attributes"][0],
            serde_json::json!({
                "key": "by(resource.service.name)",
                "value": {"stringValue": "checkout"}
            })
        );
        assert_eq!(sets[0]["spans"][0]["spanID"], "0101010101010101");
        assert_eq!(sets[1]["matched"], 3);
        assert_eq!(sets[1]["spans"].as_array().expect("spans").len(), 2);
    }

    // ------------------------------------------------------------------
    // Issue #473: the wire domains of the four projected integers, the
    // empty-root-service marker, and the off-response surfacing.
    // ------------------------------------------------------------------

    /// A one-trace, one-span output whose four PROJECTED integers are the
    /// four arguments, and whose every string is free of the `-` byte —
    /// so the minus-sign assertion in
    /// [`negative_widths_and_starts_render_inside_the_wire_domain`] is
    /// about the NUMBERS and cannot be satisfied, or defeated, by a name.
    fn extreme_output(
        trace_start_ns: i64,
        trace_duration_ns: i64,
        span_start_ns: i64,
        span_duration_ns: i64,
    ) -> SearchOutput {
        SearchOutput {
            traces: vec![TraceSearchResult {
                trace_id: [0xab; 16],
                root: RootSummary {
                    service: "checkout".to_string(),
                    name: "charge".to_string(),
                    start_ns: span_start_ns,
                    duration_ns: span_duration_ns,
                },
                trace_start_ns,
                trace_duration_ns,
                matched: 1,
                spans: vec![SpanSummary::new(
                    [0xcd; 8],
                    Some("charge".to_string()),
                    span_start_ns,
                    span_duration_ns,
                    vec![],
                )],
                groups: None,
            }],
            partial: false,
            returned: 1,
            limit: 20,
        }
    }

    /// Issue #473 AC 1: [`duration_ms`] returns the WIRE type and
    /// saturates into it.
    ///
    /// Returning `u32` is what makes an out-of-domain `durationMs`
    /// unconstructible at the call site; the values below are what make
    /// the operation saturation rather than a wrap. `4294967296000000` ns
    /// is `4294967296` ms — the exact first out-of-domain width, one past
    /// the maximum, chosen because it is the boundary and not a
    /// comfortable round number. A wrapping body answers `0` there.
    #[test]
    fn duration_ms_saturates_into_the_wire_uint32_domain() {
        let mut report = DomainReport::default();
        assert_eq!(
            duration_ms(4_294_967_296_000_000, "t", &mut report),
            4_294_967_295,
            "the first out-of-domain width saturates at the wire maximum; a wrap answers 0"
        );
        assert_eq!(
            duration_ms(4_294_967_295_999_999, "t", &mut report),
            4_294_967_295,
            "one nanosecond below the boundary is IN domain and must be unchanged"
        );
        assert_eq!(duration_ms(i64::MIN, "t", &mut report), 0);
        assert_eq!(duration_ms(-1, "t", &mut report), 0);
        // The saturation identity: two very different inputs, one output.
        // A wrapping body gives 417264662 and 2077252342 here; the
        // unclamped i64 this module used to return does not even fit the
        // type.
        assert_eq!(
            duration_ms(9_007_199_254_740_993, "t", &mut report),
            duration_ms(i64::MAX, "t", &mut report),
            "saturation is the SAME number for two different over-maximum inputs"
        );
        assert_eq!(
            u32::MAX,
            4_294_967_295,
            "the wire field is a 32-bit unsigned integer"
        );
    }

    /// Issue #473 AC 2: [`wire_nanos`] clamps below zero and is the
    /// identity above it. Every non-negative `i64` fits a `u64`, so the
    /// upper clamp is inert by construction and `i64::MAX` must survive
    /// exactly — a blanket "clamp everything to 32 bits" fix is wrong
    /// here, and this is the case that says so.
    #[test]
    fn wire_nanos_clamps_below_zero_and_is_the_identity_above_it() {
        let mut report = DomainReport::default();
        for field in WireField::ALL {
            assert_eq!(
                wire_nanos(-1, field, "i", &mut report),
                0,
                "{}",
                field.label()
            );
            assert_eq!(
                wire_nanos(i64::MIN, field, "i", &mut report),
                0,
                "{}",
                field.label()
            );
            assert_eq!(
                wire_nanos(0, field, "i", &mut report),
                0,
                "{}",
                field.label()
            );
            assert_eq!(
                wire_nanos(i64::MAX, field, "i", &mut report),
                9_223_372_036_854_775_807,
                "{}: the upper clamp is inert, so i64::MAX must be the identity",
                field.label()
            );
        }
    }

    /// Issue #473 AC 4 / AC 6 (the plan's Q6): a trace whose stored start
    /// and width are NEGATIVE still renders, inside the wire domain, and
    /// is still RETURNED — a wrong rejection is as bad as a wrong answer.
    ///
    /// The whole body is asserted byte-for-byte, so an added key, a
    /// dropped trace and a changed value all fail here. The `-`-byte
    /// assertion is the registry-free half: no field name, no key list,
    /// nothing that has to be kept in step with the renderer. The fixture
    /// carries no `-` in any string precisely so that byte can be the
    /// subject.
    ///
    /// **This body reaches the changed code but not through HTTP.** The
    /// negative branch is not reachable through any mounted write route:
    /// all three funnel through `otlp_traces::resolve_duration_ns`, which
    /// returns `0` for `end == 0` and for `end < start`, and the trace
    /// envelope is folded in Rust with `saturating_add`. This case is
    /// therefore unit-level by necessity, and the lower clamp has no live
    /// coverage anywhere — stated rather than implied.
    #[test]
    fn negative_widths_and_starts_render_inside_the_wire_domain() {
        let output = extreme_output(i64::MIN, -1, -1, i64::MIN);
        let body = serde_json::to_string(&render(&output)).expect("render to JSON");
        assert!(
            !body.contains('-'),
            "no integer this response emits may carry a minus sign — every one of them lands in \
             an UNSIGNED wire field: {body}"
        );
        assert_eq!(
            body,
            r#"{"metrics":{"completedJobs":1,"totalJobs":1},"traces":[{"rootServiceName":"checkout","rootTraceName":"charge","spanSets":[{"matched":1,"spans":[{"name":"charge","spanID":"cdcdcdcdcdcdcdcd","startTimeUnixNano":"0"}]}],"startTimeUnixNano":"0","traceID":"abababababababababababababababab"}]}"#,
            "the clamped-to-zero fields are OMITTED (protojson drops a default scalar) and the \
             trace itself is still present: an absent FIELD is not an absent TRACE"
        );
        let v = render(&output);
        assert!(v["traces"][0].get("durationMs").is_none());
        assert!(
            v["traces"][0]["spanSets"][0]["spans"][0]
                .get("durationNanos")
                .is_none()
        );
        assert_eq!(v["traces"][0]["startTimeUnixNano"], "0");
        assert_eq!(
            v["traces"][0]["spanSets"][0]["spans"][0]["startTimeUnixNano"],
            "0"
        );
        assert_eq!(
            v["traces"].as_array().expect("traces array").len(),
            1,
            "the trace is still returned"
        );
    }

    /// The uint64-STRING keys the walk below knows about.
    ///
    /// **A registry of two names, and it cannot see a third.** A new
    /// `uint64` field rendered as a protojson string under a key not
    /// listed here would pass this test silently. The NUMBER half of the
    /// walk is registry-free and has no such gap; this half is the
    /// stated limit of the criterion, not an oversight.
    const UINT64_STRING_KEYS: [&str; 2] = ["startTimeUnixNano", "durationNanos"];

    /// Recursively asserts every integer-backed JSON number and every
    /// listed uint64-string in `value` lies in its wire domain. `key` is
    /// the object key `value` was found under, if any.
    fn assert_wire_domains(value: &Value, key: Option<&str>, body: &str) {
        match value {
            // A `doubleValue` is a JSON float and is not the subject: its
            // wire type is `double`, which has no integer domain.
            Value::Number(n) if !n.is_f64() => {
                let v = n.as_u64().unwrap_or_else(|| {
                    panic!("{key:?}: {n} is not a non-negative integer, body {body}")
                });
                assert!(
                    v <= u64::from(u32::MAX),
                    "{key:?}: {v} is outside every unsigned integer domain this response \
                     declares, body {body}"
                );
            }
            Value::String(s) if key.is_some_and(|k| UINT64_STRING_KEYS.contains(&k)) => {
                assert!(
                    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()),
                    "{key:?}: {s:?} must be ASCII digits only — a protojson uint64 is a STRING \
                     and a minus sign in it is what a strict decoder refuses, body {body}"
                );
                s.parse::<u64>().unwrap_or_else(|e| {
                    panic!("{key:?}: {s:?} must parse as u64: {e}, body {body}")
                });
            }
            Value::Array(items) => {
                for item in items {
                    assert_wire_domains(item, key, body);
                }
            }
            Value::Object(map) => {
                for (k, v) in map {
                    assert_wire_domains(v, Some(k.as_str()), body);
                }
            }
            _ => {}
        }
    }

    /// Issue #473 AC 5: the response-wide domain walk.
    ///
    /// The projections return the wire type, which makes an out-of-domain
    /// value unconstructible **at the four call sites that use them**.
    /// That is construction, not proof: it says nothing about a FIFTH
    /// call site written without a projection. This walk is what catches
    /// that — for JSON numbers it is registry-free, naming no field and
    /// needing no list, so `obj["durationMs"] = Value::from(raw_ns /
    /// 1_000_000)` written directly fails it. For uint64 STRINGS it is a
    /// registry of two key names and cannot see a new one; see
    /// [`UINT64_STRING_KEYS`].
    #[test]
    fn every_integer_the_response_emits_lies_in_its_wire_domain() {
        for output in [
            extreme_output(i64::MIN, i64::MIN, i64::MIN, i64::MIN),
            extreme_output(i64::MAX, i64::MAX, i64::MAX, i64::MAX),
            sample_output(),
        ] {
            let v = render(&output);
            let body = serde_json::to_string(&v).expect("render to JSON");
            assert_wire_domains(&v, None, &body);
        }
    }

    /// Issue #473 AC 6: the report counts one event per PROJECTED FIELD,
    /// not one per response and not one per row.
    ///
    /// Dropping the `report` argument at any one of the four call sites
    /// leaves three; recording once per response leaves one.
    #[test]
    fn the_domain_report_counts_one_event_per_projected_field() {
        let output = extreme_output(i64::MIN, i64::MIN, i64::MIN, i64::MIN);
        let mut report = DomainReport::default();
        let _ = trace_json(&output.traces[0], &mut report);
        assert_eq!(
            report.total(),
            4,
            "all four projected integers are out of domain here: {report:?}"
        );
        for field in WireField::ALL {
            assert_eq!(
                report.counts[field.index()].iter().sum::<u64>(),
                1,
                "{} recorded no event: {report:?}",
                field.label()
            );
        }

        let mut healthy = DomainReport::default();
        let _ = trace_json(&sample_output().traces[0], &mut healthy);
        assert_eq!(
            healthy.total(),
            0,
            "an ordinary response records nothing and allocates nothing: {healthy:?}"
        );
        assert!(healthy.first.is_none());
    }

    /// Renders `f` against a local Prometheus recorder and returns the
    /// exposition text (the `ops.rs` pattern).
    fn render_local(f: impl FnOnce()) -> String {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, f);
        handle.render()
    }

    /// Asserts the exposition carries a `SATURATION_COUNTER` sample with
    /// both label pairs, without depending on the exporter's label order.
    fn assert_saturation_sample(rendered: &str, field: WireField, bound: Bound) {
        let name = format!("{SATURATION_COUNTER}{{");
        let field_label = format!("field=\"{}\"", field.label());
        let bound_label = format!("bound=\"{}\"", bound.label());
        assert!(
            rendered.lines().any(|line| line.starts_with(&name)
                && line.contains(&field_label)
                && line.contains(&bound_label)),
            "missing sample {SATURATION_COUNTER}{{{field_label},{bound_label}}} in:\n{rendered}"
        );
    }

    /// Issue #473 AC 7: the surfacing is a counter and a log line, not a
    /// field of the response body. A poisoned body names no span; a
    /// labelled counter names the field and the bound.
    #[test]
    fn the_saturation_counter_is_emitted_once_per_projected_field() {
        let below = extreme_output(i64::MIN, i64::MIN, i64::MIN, i64::MIN);
        let above = extreme_output(i64::MAX, i64::MAX, i64::MAX, i64::MAX);
        let rendered = render_local(|| {
            render(&below);
            render(&above);
        });
        for field in WireField::ALL {
            assert_saturation_sample(&rendered, field, Bound::Below);
        }
        // `i64::MAX` ns is 9223372036854 ms, above the 32-bit maximum —
        // the only one of the four whose UPPER clamp any `i64` can reach.
        assert_saturation_sample(&rendered, WireField::TraceDurationMs, Bound::Above);

        let healthy = render_local(|| {
            render(&sample_output());
        });
        assert!(
            !healthy.contains(SATURATION_COUNTER),
            "an ordinary response emits no sample at all: {healthy}"
        );
    }

    /// Issue #473 AC 8: the empty-root-service marker, and only where the
    /// reference puts it. `rootTraceName` is never substituted, a
    /// non-empty service is never substituted, and a
    /// `by(rootServiceName)` group VALUE is never substituted — that is a
    /// different field on a different code path, and the marker reaching
    /// it would invent a group key nothing grouped by.
    #[test]
    fn an_empty_root_service_renders_the_reference_marker() {
        let mut empty_service = sample_output();
        empty_service.traces[0].root.service = String::new();
        assert_eq!(
            render(&empty_service)["traces"][0]["rootServiceName"],
            "<root span not yet received>"
        );

        let mut empty_name = sample_output();
        empty_name.traces[0].root.name = String::new();
        assert_eq!(
            render(&empty_name)["traces"][0]["rootTraceName"],
            "",
            "the reference substitutes the root SERVICE and nothing else"
        );

        assert_eq!(
            render(&sample_output())["traces"][0]["rootServiceName"],
            "checkout",
            "a present service is never substituted"
        );

        let mut grouped = sample_output();
        grouped.traces[0].groups = Some(vec![SpanSetGroup {
            attributes: vec![(
                "by(rootServiceName)".to_string(),
                GroupValue::Str(String::new()),
            )],
            matched: 1,
            spans: vec![],
        }]);
        assert_eq!(
            render(&grouped)["traces"][0]["spanSets"][0]["attributes"][0]["value"],
            serde_json::json!({"stringValue": ""}),
            "a by(rootServiceName) group value is a different field on a different path and the \
             marker must not reach it"
        );
    }

    fn read_doc(rel: &str) -> String {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(rel);
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {rel}: {e}"))
    }

    /// Issue #473 AC 11(b): the saturation value documented in
    /// docs/api.md §4.2 is DERIVED from [`duration_ms`], never restated.
    /// Changing the documented number without changing the code fails
    /// here, and so does the reverse.
    #[test]
    fn the_documented_saturation_value_is_derived_from_the_code() {
        let api = read_doc("docs/api.md");
        let saturated = duration_ms(i64::MAX, "", &mut DomainReport::default());
        assert!(
            api.contains(&format!("saturates** at `{saturated}` ms")),
            "docs/api.md §4.2 must document the value duration_ms() actually returns"
        );
    }

    /// Issue #473 AC 11(c): the new metric family is named in
    /// docs/architecture.md §8, from the constant rather than a copy of
    /// it — renaming the metric without touching the document fails here.
    ///
    /// The LABEL VOCABULARY is pinned the same way, and for a reason
    /// worth stating: [`assert_saturation_sample`] builds its needles
    /// from [`WireField::label`] and [`Bound::label`], so a renamed label
    /// value would move the code and the assertion together and stay
    /// green. The document is the independent side. A label rename now
    /// has to be a deliberate documented change, which is what a metric
    /// label is.
    #[test]
    fn the_saturation_counter_is_documented_in_the_architecture_metric_families() {
        let arch = read_doc("docs/architecture.md");
        assert!(
            arch.contains(SATURATION_COUNTER),
            "docs/architecture.md §8 must name {SATURATION_COUNTER}"
        );
        let bullet = arch
            .lines()
            .find(|line| line.contains(SATURATION_COUNTER))
            .unwrap_or_else(|| {
                panic!("docs/architecture.md §8 must carry a bullet naming {SATURATION_COUNTER}")
            });
        // Scoped to the VOCABULARY clause — not the whole document, and
        // not even the whole bullet. Measured: the bullet's own closing
        // sentence explains what a `below` sample means, so a search over
        // the file, or over the line, stayed green when the vocabulary
        // clause itself was edited to a different label. The domain the
        // assertion checks now matches the domain its message claims.
        let (vocabulary, rest) = bullet
            .split_once("Incremented once per")
            .unwrap_or_else(|| {
                panic!(
                    "the {SATURATION_COUNTER} bullet must keep its `Incremented once per …` \
                 sentence: it is what bounds the label vocabulary clause, and without it this \
                 check silently widens to the whole bullet"
                )
            });
        assert!(
            !rest.trim().is_empty(),
            "the bullet must say what the counter counts after its vocabulary clause"
        );
        for field in WireField::ALL {
            assert!(
                vocabulary.contains(&format!("`{}`", field.label())),
                "the {SATURATION_COUNTER} bullet's label vocabulary must name the field label \
                 {:?}, got {vocabulary:?}",
                field.label()
            );
        }
        for bound in Bound::ALL {
            assert!(
                vocabulary.contains(&format!("`{}`", bound.label())),
                "the {SATURATION_COUNTER} bullet's label vocabulary must name the bound label \
                 {:?}, got {vocabulary:?}",
                bound.label()
            );
        }
    }

    /// Issue #473, review round 1: every label is pinned to its OWN
    /// variant, as a literal.
    ///
    /// **A permutation is what the other two checks cannot see, and it is
    /// a wrong answer rather than a cosmetic one.** Exchange
    /// [`Bound::Below`]'s and [`Bound::Above`]'s labels and every
    /// saturation event is reported under the opposite bound — a `below`
    /// sample would mean a value clamped down from over the maximum,
    /// which is the reverse of what the metric family documents, and an
    /// operator reading `bound="below"` would conclude a stored value was
    /// negative when it was enormous. Neither existing check fires on it:
    /// [`assert_saturation_sample`] builds its needles from
    /// [`WireField::label`] and [`Bound::label`], so the code and the
    /// assertion move together, and
    /// [`the_saturation_counter_is_documented_in_the_architecture_metric_families`]
    /// tests only that each label STRING appears in the document, which a
    /// permutation leaves true. Measured before this test existed: all 22
    /// tests in this module passed with the two bound labels exchanged.
    ///
    /// The general form, since it has cost this issue two rounds: a check
    /// that lists which values appear cannot see a change that reorders
    /// which value goes with which. The claim here is a MAPPING — this
    /// variant carries that label — so the test has to be a mapping too,
    /// and the break that exercises it is a permutation, never a rename.
    /// A rename is the break a membership check can still catch, which is
    /// exactly why passing it proved less than it looked.
    ///
    /// [`WireField`] has the same shape and is pinned the same way: its
    /// four labels are level-qualified precisely so a trace-level event
    /// can be told from a span-level one, and a permutation would report
    /// each event under the wrong level while leaving all four strings
    /// present everywhere they are checked.
    #[test]
    fn every_label_belongs_to_its_own_variant() {
        assert_eq!(Bound::Below.label(), "below");
        assert_eq!(Bound::Above.label(), "above");

        assert_eq!(
            WireField::TraceStartTimeUnixNano.label(),
            "trace.startTimeUnixNano"
        );
        assert_eq!(WireField::TraceDurationMs.label(), "trace.durationMs");
        assert_eq!(
            WireField::SpanStartTimeUnixNano.label(),
            "span.startTimeUnixNano"
        );
        assert_eq!(WireField::SpanDurationNanos.label(), "span.durationNanos");

        // The labels must also be DISTINCT, or a mapping that collapsed
        // two variants onto one label would satisfy every equality above
        // if both literals were edited to match. Cheap, and it closes the
        // one edit that could make the block self-consistent and wrong.
        let bounds: std::collections::BTreeSet<&str> =
            Bound::ALL.iter().map(|b| b.label()).collect();
        assert_eq!(bounds.len(), Bound::ALL.len(), "bound labels must differ");
        let fields: std::collections::BTreeSet<&str> =
            WireField::ALL.iter().map(|f| f.label()).collect();
        assert_eq!(
            fields.len(),
            WireField::ALL.len(),
            "field labels must differ"
        );
    }

    /// Issue #473 AC 12: the ledger row exists and carries the content it
    /// is load-bearing for — both the reference's values, the wrapped
    /// 60-day number that makes the lower-bound argument concrete, the
    /// cross-reference that makes this a rule rather than a one-off call,
    /// and the route, without which the row goes stale invisibly.
    #[test]
    fn the_saturation_divergence_is_recorded_in_the_traces_ledger() {
        let ledger = read_doc("docs/benchmarks/traces-differential-ledger.md");
        for needle in [
            "traceql-search-duration-ms-saturates-not-wraps",
            "detected-fields-limit-saturates-not-wraps",
            "889032704",
            "2077252342",
            "417264662",
            "/api/search",
        ] {
            assert!(
                ledger.contains(needle),
                "docs/benchmarks/traces-differential-ledger.md must carry {needle:?}"
            );
        }
    }

    /// Issue #193: numeric / double / bool / nil group-key values render
    /// their reference-typed `value:{…}` objects.
    #[test]
    fn grouped_output_renders_each_group_value_type() {
        let mut output = sample_output();
        output.traces[0].groups = Some(vec![SpanSetGroup {
            attributes: vec![
                ("span.count".to_string(), GroupValue::Int(7)),
                (
                    "span.ratio".to_string(),
                    GroupValue::Double(pulsus_read::canonical_double_bits(1.5)),
                ),
                ("span.ok".to_string(), GroupValue::Bool(true)),
                ("span.missing".to_string(), GroupValue::Nil),
            ],
            matched: 1,
            spans: vec![group_span(0x09, "z")],
        }]);
        let v = render(&output);
        let attrs = &v["traces"][0]["spanSets"][0]["attributes"];
        assert_eq!(attrs[0]["value"], serde_json::json!({"intValue": "7"}));
        assert_eq!(attrs[1]["value"], serde_json::json!({"doubleValue": 1.5}));
        assert_eq!(attrs[2]["value"], serde_json::json!({"boolValue": true}));
        assert_eq!(attrs[3]["value"], serde_json::Value::Null);
    }
}
