//! OTLP traces parser (issue #54, docs/architecture.md §4): a pure
//! `bytes -> ExportTraceServiceRequest -> ParsedTraces` pipeline with no
//! I/O. Unlike the logs/metrics parsers there is **no** label model here:
//! traces carry no fingerprint (a span's identity is `(trace_id,
//! span_id)`, distribution shards by the server-side `cityHash64(trace_id)`
//! — docs/architecture.md §2.2), and attribute keys are stored **verbatim**
//! in the index (docs/architecture.md §2.3), discriminated by a `scope`
//! column (`'resource'`, `'span'`, `'instrumentation'`) so scoped TraceQL
//! selectors never collide across scopes. `InstrumentationScope` attributes
//! are indexed under `scope='instrumentation'` (issue #192, superseding the
//! #54 adjudication #2 that dropped them) so the `instrumentation.<key>`
//! selector resolves; the scope's `name`/`version` are promoted onto the
//! `SpanRecord`'s `scope_name`/`scope_version` columns for the
//! `instrumentation:name`/`instrumentation:version` intrinsics. Everything
//! remains fully preserved in the span payload as well. Span **events**
//! (issue #192 PR-B) are indexed the same way: each event's attributes
//! ride `scope='event'` (verbatim keys, so `event.<key>` resolves), and
//! each event's `name`/`timeSinceStart` intrinsics ride a dedicated
//! `scope='event:intrinsic'` (reserved keys `name`/`timeSinceStart`,
//! `val_num` = ns for `timeSinceStart`) — a hard namespace partition so no
//! sender-supplied event attribute can collide with an intrinsic row. Span
//! **links** (issue #192 PR-C) mirror events exactly: each link's attributes
//! ride `scope='link'`, and each link's `spanID`/`traceID` intrinsics ride a
//! dedicated `scope='link:intrinsic'` (reserved keys `spanID`/`traceID`,
//! `val` = lowercase hex of the referenced id bytes).
//!
//! **Payload contract (pinned for T2/T3, issue #54 adjudication #3):**
//! every [`SpanRecord::payload`] is a self-contained single-`ResourceSpans`
//! [`TracesData`] — this span, its own resource, its own scope, both
//! schema URLs — so the trace-by-ID fetch path decodes each span's payload
//! independently and concatenates the results into a valid `TracesData`.
//!
//! **Expansion budget (issue #54 code-review [high] fix):** that payload
//! contract and the per-span attribute fan-out make this the one parser
//! whose output is **multiplicative** in its input — every span's payload
//! re-carries its whole resource + scope, and every resource attribute
//! becomes one attr row *per span*. A body inside the 64 MiB decompressed
//! cap can therefore describe gigabytes of parse output (e.g. 32 MiB of
//! resource attributes × thousands of near-empty spans), all allocated
//! *before* the writer's byte-reservation backpressure ever runs. [`parse`]
//! guards this with [`MAX_EXPANDED_BYTES`]: an allocation-free,
//! wire-length-based estimate of each span's produced rows — with
//! worst-case rendering-expansion multipliers for the attribute kinds
//! whose stored `val` can outgrow its wire bytes (JSON escaping up to 6×,
//! base64 4/3; see [`attr_budget_charge`]) — is accumulated
//! and checked **before** that span's rows are materialized
//! (reserve-before-materialize, the writer's own admission pattern); the
//! moment the running total exceeds the budget the whole request fails
//! atomically with [`LogsIngestError::OversizeMessage`] (the same
//! structural-oversize class `remote_write::decode`'s count bounds use —
//! HTTP 400 / `google.rpc.Status.code = 3`), never a partial write.
//! Diagnostic/rejection messages are bounded by [`diag_snippet`]'s hard
//! truncation instead of budget accounting (they are not payload — see
//! that helper's doc comment).

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, TracesData};
use prost::Message;
use pulsus_model::{Date, Fingerprint};

use crate::error::LogsIngestError;
use crate::ingest::traces::{
    AttrRecord, AttrValueType, LandingEvent, LandingLink, LandingResource, LandingSpan,
    LandingTagName, LandingTagValue, ParsedTraceLanding, ParsedTraces, SpanRecord, TagScope,
};
use crate::writer::trace_json::{TraceJson, TraceJsonEntry, TraceJsonScalar, TraceJsonValue};

/// The `scope` discriminator value for a resource attribute row.
const SCOPE_RESOURCE: &str = "resource";
/// The `scope` discriminator value for a span attribute row.
const SCOPE_SPAN: &str = "span";
/// The `scope` discriminator value for an instrumentation-scope attribute
/// row (issue #192) — `scope_spans.scope.attributes`, indexed verbatim so
/// the `instrumentation.<key>` TraceQL selector resolves.
const SCOPE_INSTRUMENTATION: &str = "instrumentation";
/// The `scope` discriminator value for a span-event **attribute** row
/// (issue #192 PR-B) — `span.events[].attributes`, indexed verbatim so the
/// `event.<key>` TraceQL selector resolves. Multi-valued (0..N per span),
/// exactly like resource/span attributes.
const SCOPE_EVENT: &str = "event";
/// The **dedicated intrinsic-scope** discriminator for span-event
/// intrinsics (issue #192 PR-B, plan v2 Δ1): a hard namespace partition
/// disjoint from the sender-supplied [`SCOPE_EVENT`] attribute scope. The
/// writer emits this scope *only* from the intrinsic-emission code below,
/// never from a sender attribute (whose keys are stored verbatim), so no
/// OTLP event attribute — even one literally keyed `name`/`timeSinceStart`,
/// reachable via `event."name"` — can collide with an intrinsic row.
const SCOPE_EVENT_INTRINSIC: &str = "event:intrinsic";
/// The reserved intrinsic key for a span event's name, under
/// [`SCOPE_EVENT_INTRINSIC`] — resolves the `event:name` intrinsic.
const EVENT_INTRINSIC_NAME_KEY: &str = "name";
/// The reserved intrinsic key for a span event's time-since-span-start (ns,
/// carried in `val_num`), under [`SCOPE_EVENT_INTRINSIC`] — resolves the
/// `event:timeSinceStart` intrinsic.
const EVENT_INTRINSIC_TIME_SINCE_START_KEY: &str = "timeSinceStart";
/// The `scope` discriminator value for a span-link **attribute** row (issue
/// #192 PR-C) — `span.links[].attributes`, indexed verbatim so the
/// `link.<key>` TraceQL selector resolves. Multi-valued (0..N per span),
/// exactly like span events.
const SCOPE_LINK: &str = "link";
/// The **dedicated intrinsic-scope** discriminator for span-link intrinsics
/// (issue #192 PR-C, plan v2 Δ1): a hard namespace partition disjoint from
/// the sender-supplied [`SCOPE_LINK`] attribute scope, mirroring
/// [`SCOPE_EVENT_INTRINSIC`]. The writer emits this scope *only* from the
/// intrinsic-emission code below, so no OTLP link attribute — even one
/// literally keyed `spanID`/`traceID`, reachable via `link."spanID"` — can
/// collide with an intrinsic row.
const SCOPE_LINK_INTRINSIC: &str = "link:intrinsic";
/// The reserved intrinsic key for a span link's referenced span id
/// (lowercase hex in `val`), under [`SCOPE_LINK_INTRINSIC`] — resolves the
/// `link:spanID` intrinsic.
const LINK_INTRINSIC_SPAN_ID_KEY: &str = "spanID";
/// The reserved intrinsic key for a span link's referenced trace id
/// (lowercase hex in `val`), under [`SCOPE_LINK_INTRINSIC`] — resolves the
/// `link:traceID` intrinsic.
const LINK_INTRINSIC_TRACE_ID_KEY: &str = "traceID";

/// The per-request cap on [`parse`]'s **estimated expanded output bytes**
/// (see the module doc's "Expansion budget" section). Derivation: the
/// decompressed body is already capped at 64 MiB
/// (`crate::ingest::decompress::MAX_DECOMPRESSED_BYTES`); a legitimate
/// batch's expansion over its wire size comes from each span's payload
/// re-carrying its resource + scope (collector resources are ≤ a few KiB —
/// even 10k spans × 4 KiB is ~40 MiB of duplication) plus per-row fixed
/// columns, so 4× the body cap (256 MiB) accommodates every legitimate
/// shape with ample headroom, while a pathological fan-out (32 MiB
/// resource × 1000 empty spans ≈ 32 GiB) trips within its first handful
/// of spans. Byte-denominated rather than row-counted because each
/// estimated row carries a fixed >= [`ATTR_ROW_OVERHEAD`]-byte floor, so
/// the byte budget bounds the row count for free (≤ ~4M rows). This is an
/// order-of-magnitude admission DoS guard, deliberately distinct from the
/// writer's exact `est_bytes` queue reservation, which still runs (and can
/// still push back) at sink admission.
pub const MAX_EXPANDED_BYTES: usize = 4 * crate::ingest::decompress::MAX_DECOMPRESSED_BYTES;

/// Estimated fixed heap cost of one [`AttrRecord`] beyond its key/value
/// wire bytes: the fixed-width columns (`date`/`val_num`/`timestamp_ns`/
/// `trace_id`/`span_id`/`duration_ns` ≈ 51 bytes) plus the `scope` string
/// and container overhead, floored to a round constant.
const ATTR_ROW_OVERHEAD: usize = 64;
/// Estimated heap cost of one span-row ARRAY element (issue #556) beyond a
/// second copy of its rendered key/value text. Priced in the
/// `TraceSpanRow` shape the writer queue holds, so the admission charge
/// and the queue reservation count the same object — a charge priced in
/// the narrower `SpanRecord` shape stops dominating the reservation it is
/// meant to pre-filter.
const ATTR_ARRAY_ELEMENT_OVERHEAD: usize = crate::writer::rows::ARRAY_ELEMENT_SLOT_BYTES
    + crate::writer::rows::VAL_TYPE_SPELLING_BYTES
    + SCOPE_SPELLING_BYTES;
/// The longest scope spelling the writer emits (`instrumentation` and
/// `event:intrinsic` are both 15 bytes); a fixed term, because the scope
/// is not a field of the `KeyValue` this allocation-free charge can read.
const SCOPE_SPELLING_BYTES: usize = 15;
/// The five `Vec` headers one span's attribute arrays carry, charged once
/// per span (issue #556).
const ATTR_ARRAY_HEADER_OVERHEAD: usize = 5 * std::mem::size_of::<Vec<String>>();
/// Estimated fixed heap cost of one [`SpanRecord`] beyond its name/service/
/// payload bytes (ids + fixed-width columns + container overhead).
const SPAN_ROW_OVERHEAD: usize = 128;
/// Estimated `TracesData`/`ResourceSpans`/`ScopeSpans` nesting overhead
/// (tags + length prefixes) added to a payload's summed part lengths.
const PAYLOAD_ENVELOPE_OVERHEAD: usize = 32;

/// The maximum per-byte expansion `serde_json` string escaping can produce:
/// a control byte (e.g. NUL) renders as its 6-byte `\uXXXX` escape. The
/// budget charge for an array/kvlist-kind attribute — whose stored `val`
/// goes through [`any_value_to_json`] → `serde_json::to_string` — must
/// assume this worst case, or an escape-dense payload materializes up to
/// 6× its wire length past the estimate (issue #54 code-review round 2).
const MAX_JSON_ESCAPE_FACTOR: usize = 6;
/// The (ceiled) base64 expansion factor for a bytes-kind attribute's
/// stored `val` ([`base64_encode`] emits 4 output bytes per 3 input bytes)
/// — same undercharge class as [`MAX_JSON_ESCAPE_FACTOR`], smaller bound.
const BASE64_EXPANSION_FACTOR: usize = 2;

/// Byte cap on any untrusted wire-derived string embedded in a
/// diagnostic/rejection message via [`diag_snippet`] — 128 bytes of
/// original content identifies a span/attribute more than adequately for
/// a human reading a partial-success message.
const DIAG_SNIPPET_MAX_BYTES: usize = 128;

/// Truncates untrusted wire-derived text for embedding in a diagnostic/
/// rejection message (issue #54 code-review round 4 [high] fix): message
/// construction happens on validation paths **before** any
/// [`charge_budget`] reservation, so it must never materialize unbounded
/// attacker-controlled content — a near-body-cap control-character-dense
/// `span.name` would otherwise Debug-escape to ~6x its wire bytes into
/// `rejected_message`, uncharged. Budget-accounting diagnostics would be
/// the wrong tool (they are not payload); a hard cap removes the
/// amplification class outright. Truncation lands on a `char` boundary
/// (never splits a code point) and appends an explicit marker naming the
/// elided byte count. EVERY `format!` in this parser that embeds a
/// wire-derived string must route through this helper.
fn diag_snippet(s: &str, max: usize) -> Cow<'_, str> {
    if s.len() <= max {
        return Cow::Borrowed(s);
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    Cow::Owned(format!("{}…[{} bytes truncated]", &s[..end], s.len() - end))
}

/// One attribute's budget charge: its wire length, multiplied up to the
/// worst-case expansion its **stored rendering** can reach — chosen over
/// render-once-and-measure (option (b) of the round-2 review) because the
/// estimate must stay allocation-free to keep its reserve-before-
/// materialize guarantee: rendering at estimation time would itself
/// allocate the (up to 6×) expansion being guarded against, per span,
/// before any check could reject it. The trade-off is deliberate slight
/// over-rejection: a *legitimate* batch whose array/kvlist attributes are
/// escape-free gets charged 6× their true rendering, so it trips the
/// 256 MiB budget at ~43 MiB of such attributes × spans — far beyond any
/// real collector batch (array/kvlist span attributes are rare and small),
/// and the failure is an explicit, actionable 400, never a silent drop.
/// String/scalar kinds render ≤ their wire length (strings verbatim, no
/// JSON quoting — [`any_value_to_string`]), so they stay charged 1×.
fn attr_budget_charge(kv: &KeyValue) -> usize {
    let wire = kv.encoded_len();
    match kv.value.as_ref().and_then(|v| v.value.as_ref()) {
        Some(Value::ArrayValue(_) | Value::KvlistValue(_)) => {
            wire.saturating_mul(MAX_JSON_ESCAPE_FACTOR)
        }
        Some(Value::BytesValue(_)) => wire.saturating_mul(BASE64_EXPANSION_FACTOR),
        _ => wire,
    }
}

/// Decodes a (decompressed) OTLP `/v1/traces` request body. The sole
/// decode boundary: a malformed/truncated protobuf is a whole-request,
/// atomic failure (mirrors `otlp_logs::decode`) — never partially applied.
pub fn decode(body: &[u8]) -> Result<ExportTraceServiceRequest, LogsIngestError> {
    // Wire pre-scan (issue #115, track 5): reject an over-cap / over-deep
    // request by walking the raw protobuf bytes BEFORE `decode` materializes
    // the amplified structure. `Ok` for an in-bounds or malformed body — a
    // malformed body is deferred to `decode` below for identical
    // classification.
    crate::protocols::otlp_prescan::prescan_traces(body)?;
    Ok(ExportTraceServiceRequest::decode(body)?)
}

/// Decodes a (decompressed) OTLP/JSON (proto3-JSON) `/v1/traces` request body
/// — the `Content-Type: application/json` sibling of [`decode`] (issue #76),
/// feeding the same [`parse`] as protobuf (so the [`MAX_EXPANDED_BYTES`]
/// expansion budget, enforced in `parse`, applies to JSON unchanged). A
/// malformed body maps to 400/code 3 via [`LogsIngestError::DecodeJson`].
pub fn decode_json(body: &[u8]) -> Result<ExportTraceServiceRequest, LogsIngestError> {
    // Issue #115 track 6a: bounded proto3-JSON building wrappers replace the
    // vendored derive's UNBOUNDED repeated-field decode, rejecting a DoS-shaped
    // body DURING deserialization at the SAME per-level / aggregate / depth
    // thresholds the protobuf wire pre-scan (`otlp_prescan`) enforces.
    crate::protocols::otlp_json::decode_traces(body)
}

/// Parses a decoded `ExportTraceServiceRequest` into normalized rows.
/// Pure: a function of `req` and `now_ns` only, no I/O, no clock reads —
/// the caller (the ingest handler) is the only clock/IO boundary. `now_ns`
/// is the fallback `timestamp_ns` for a span whose `start_time_unix_nano`
/// is `0` ("unknown or missing" per the OTLP wire format), mirroring
/// `otlp_logs::resolve_timestamp_ns`'s now-fallback (unlike a metric
/// sample, a span with no timestamp is still a usable record).
///
/// `Err` iff the request's estimated expanded output exceeds
/// [`MAX_EXPANDED_BYTES`] (see the module doc's "Expansion budget"
/// section) — a whole-request, atomic structural failure, exactly like a
/// decode error; everything else (bad ids, bad timestamps) stays a
/// per-span partial-success rejection inside the `Ok`.
pub fn parse(
    req: &ExportTraceServiceRequest,
    now_ns: i64,
) -> Result<ParsedTraces, LogsIngestError> {
    // Whole-request `AnyValue` recursion-depth guard (finding #54): reject a
    // maliciously deep attribute tree before any value is rendered or a row
    // materialized, so the recursive `any_value_to_string` render below can
    // never overflow the stack.
    crate::protocols::otlp_depth::ensure_trace_anyvalue_depth(req)?;

    let mut out = ParsedTraces::default();
    let mut expanded_bytes: usize = 0;

    for resource_spans in &req.resource_spans {
        let resource = resource_spans.resource.as_ref();
        // Check-then-render (issue #54 code-review round 3 [high] fix):
        // the promoted `service` column is itself a rendered untrusted
        // `AnyValue` — an escape-dense array-kind `service.name` expands
        // up to 6x its wire bytes the moment it is rendered, so the
        // rendering must be charged (same conservative
        // [`attr_budget_charge`] the attr rows use) and admitted BEFORE it
        // happens. Zero-span blocks are charged too, deliberately: the
        // rendering is the materialization being guarded, and it happens
        // once per `ResourceSpans` block regardless of span count.
        let service_kv = find_service_kv(resource);
        if let Some(kv) = service_kv {
            charge_budget(&mut expanded_bytes, attr_budget_charge(kv))?;
        }
        let service = service_kv
            .map(|kv| any_value_to_string(kv.value.as_ref()))
            .unwrap_or_default();
        // Hoisted per resource/scope (not recomputed per span): with a
        // pathological multi-MiB resource, walking its wire length per
        // span would itself be quadratic work before the budget trips.
        let resource_wire_len = resource.map(Message::encoded_len).unwrap_or(0);
        for scope_spans in &resource_spans.scope_spans {
            let ctx = SpanContext {
                resource,
                resource_spans,
                scope_spans,
                service: &service,
                now_ns,
                payload_base_estimate: resource_wire_len
                    + scope_spans
                        .scope
                        .as_ref()
                        .map(Message::encoded_len)
                        .unwrap_or(0)
                    + resource_spans.schema_url.len()
                    + scope_spans.schema_url.len()
                    + PAYLOAD_ENVELOPE_OVERHEAD,
            };
            for span in &scope_spans.spans {
                parse_span(&mut out, &mut expanded_bytes, span, &ctx)?;
            }
        }
    }

    Ok(out)
}

/// The per-`ScopeSpans` context [`parse_span`] reads: the span's
/// resource/scope envelope, the promoted service, the clock fallback, and
/// the precomputed per-span payload-size estimate base (resource + scope +
/// schema URLs wire lengths — identical for every span in this scope).
/// Bundled to keep `parse_span`'s argument count within clippy's default
/// threshold, mirroring `otlp_metrics::DataPointContext`.
struct SpanContext<'a> {
    resource: Option<&'a Resource>,
    resource_spans: &'a ResourceSpans,
    scope_spans: &'a ScopeSpans,
    service: &'a str,
    now_ns: i64,
    payload_base_estimate: usize,
}

/// Parses one `Span` into a [`SpanRecord`] plus its indexed resource ⊕
/// span [`AttrRecord`]s, or rejects it wholesale into partial success
/// (invalid IDs, unrepresentable timestamp) — a rejected span contributes
/// no attr rows either. `Err` only on the [`MAX_EXPANDED_BYTES`] budget
/// (whole-request abort), checked against `expanded_bytes` **before** this
/// span's rows/payload are materialized.
fn parse_span(
    out: &mut ParsedTraces,
    expanded_bytes: &mut usize,
    span: &Span,
    ctx: &SpanContext<'_>,
) -> Result<(), LogsIngestError> {
    let Ok(trace_id) = <[u8; 16]>::try_from(span.trace_id.as_slice()) else {
        reject_span(
            out,
            format!(
                "span {:?}: trace_id must be exactly 16 bytes, got {}",
                diag_snippet(&span.name, DIAG_SNIPPET_MAX_BYTES),
                span.trace_id.len()
            ),
        );
        return Ok(());
    };
    let Ok(span_id) = <[u8; 8]>::try_from(span.span_id.as_slice()) else {
        reject_span(
            out,
            format!(
                "span {:?}: span_id must be exactly 8 bytes, got {}",
                diag_snippet(&span.name, DIAG_SNIPPET_MAX_BYTES),
                span.span_id.len()
            ),
        );
        return Ok(());
    };
    // Empty means "root span" (no parent) and maps to the all-zero sentinel;
    // any other non-8-byte length is malformed.
    let parent_id = if span.parent_span_id.is_empty() {
        [0u8; 8]
    } else {
        match <[u8; 8]>::try_from(span.parent_span_id.as_slice()) {
            Ok(parent_id) => parent_id,
            Err(_) => {
                reject_span(
                    out,
                    format!(
                        "span {:?}: parent_span_id must be empty or exactly 8 bytes, got {}",
                        diag_snippet(&span.name, DIAG_SNIPPET_MAX_BYTES),
                        span.parent_span_id.len()
                    ),
                );
                return Ok(());
            }
        }
    };

    let timestamp_ns = if span.start_time_unix_nano == 0 {
        ctx.now_ns
    } else {
        match i64::try_from(span.start_time_unix_nano) {
            Ok(ts) => ts,
            Err(_) => {
                reject_span(
                    out,
                    format!(
                        "span {:?}: start_time_unix_nano {} exceeds the representable i64 \
                         nanosecond range",
                        diag_snippet(&span.name, DIAG_SNIPPET_MAX_BYTES),
                        span.start_time_unix_nano
                    ),
                );
                return Ok(());
            }
        }
    };
    let duration_ns = resolve_duration_ns(span.start_time_unix_nano, span.end_time_unix_nano);

    // Truncating `as` casts, deliberate: `Status.code` is a 0..=2 enum and
    // `Span.kind` a 0..=5 enum on the wire, both well inside i8; an
    // out-of-enum-range value (only producible by a non-conformant sender)
    // is stored as its truncated discriminant rather than rejected — the
    // columns are plain Int8, not enums (docs/schemas.md §4.1).
    let status_code = span.status.as_ref().map(|s| s.code).unwrap_or(0) as i8;
    // OTLP `Status.message` verbatim (issue #184), `""` when absent — the
    // parser previously dropped it. Bytes charged in `span_expansion_charge`.
    let status_message = span
        .status
        .as_ref()
        .map(|s| s.message.clone())
        .unwrap_or_default();
    let kind = span.kind as i8;
    let shared = shared_flag(span);

    // OTLP `InstrumentationScope.name`/`version` verbatim (issue #192), `""`
    // when the scope is absent — promoted onto the span row for the
    // `instrumentation:name`/`instrumentation:version` intrinsics. Bytes
    // charged in `span_expansion_charge`.
    let (scope_name, scope_version) = ctx
        .scope_spans
        .scope
        .as_ref()
        .map(|s| (s.name.clone(), s.version.clone()))
        .unwrap_or_default();

    let resource_attrs = ctx.resource.map(|r| r.attributes.as_slice()).unwrap_or(&[]);
    // The instrumentation-scope attributes (issue #192): indexed per span in
    // this scope under `scope='instrumentation'`, exactly as resource attrs
    // re-emit per span — so `instrumentation.<key>` membership resolves
    // against the owning span's rows.
    let scope_attrs = ctx
        .scope_spans
        .scope
        .as_ref()
        .map(|s| s.attributes.as_slice())
        .unwrap_or(&[]);

    // Expansion-budget reservation (module doc, issue #54 code-review
    // [high] fix): estimate this span's produced bytes from wire lengths
    // only — `encoded_len` never allocates — and check the running total
    // BEFORE materializing a single row, so an over-budget request is
    // rejected without ever paying for the expansion it describes.
    // (`ctx.service` is already-rendered here, charged pre-render by
    // `parse` — its `.len()` below is the exact per-span clone cost.)
    charge_budget(
        expanded_bytes,
        span_expansion_charge(
            span,
            ctx.resource,
            ctx.scope_spans.scope.as_ref(),
            ctx.service.len(),
            ctx.payload_base_estimate,
        ),
    )?;

    let date = match Date::start_of_day_utc_datetime_safe(timestamp_ns) {
        Some(date) => date.days_since_epoch(),
        None => {
            // The timestamp is representable as `i64` ns but its day falls
            // outside the storage-safe range: before 1970-01-01, or past
            // day 49_709 (2106-02-06) — the last UTC day fully inside the
            // 32-bit DateTime domain the trace tables' delete-TTL evaluates
            // in (issue #131; days 49_710..=65_535 would partition
            // correctly but wrap in the TTL expression, and later days fall
            // outside the `Date` range entirely). Saturating would orphan
            // or silently early-expire the span, so it is rejected
            // wholesale into partial success.
            reject_span(
                out,
                format!(
                    "span {:?}: start_time_unix_nano {} is outside the supported \
                     storage time range (1970-01-01 to 2106-02-06 UTC)",
                    diag_snippet(&span.name, DIAG_SNIPPET_MAX_BYTES),
                    span.start_time_unix_nano
                ),
            );
            return Ok(());
        }
    };
    // Where this span's OWN attr rows begin (issue #556). Every
    // `out.attrs.push` in this file is inside `parse_span`, between here
    // and the `out.spans.push` below, and every rejection path returns
    // BEFORE the first of them — so a rejected span contributes neither a
    // row nor an element, and `out.attrs[attrs_start..]` is exactly this
    // span's slice.
    let attrs_start = out.attrs.len();
    for (scope, attrs) in [
        (SCOPE_RESOURCE, resource_attrs),
        (SCOPE_SPAN, span.attributes.as_slice()),
        (SCOPE_INSTRUMENTATION, scope_attrs),
    ] {
        for kv in attrs {
            out.attrs.push(attr_record(
                kv,
                scope,
                date,
                timestamp_ns,
                trace_id,
                span_id,
                duration_ns,
            ));
        }
    }

    // Span events (issue #192 PR-B): each event fans out into indexed rows —
    // two intrinsic rows under the dedicated `event:intrinsic` scope (the
    // event `name`, and its `timeSinceStart` in ns carried in `val_num`)
    // followed by one `event`-scoped row per event attribute (verbatim key).
    // Emitted AFTER the resource/span/instrumentation attrs, charged in
    // `span_expansion_charge` in lockstep. Membership resolves against this
    // owning span's rows, exactly as attributes do.
    for event in &span.events {
        // Intrinsic: event name (dedicated `event:intrinsic` scope,
        // reserved key `name`).
        out.attrs.push(AttrRecord {
            date,
            key: EVENT_INTRINSIC_NAME_KEY.to_string(),
            scope: SCOPE_EVENT_INTRINSIC.to_string(),
            val: event.name.clone(),
            // An event name is an OTLP `string` field.
            val_type: AttrValueType::String,
            val_num: numeric_val_num(&event.name),
            timestamp_ns,
            trace_id,
            span_id,
            duration_ns,
        });
        // Intrinsic: timeSinceStart (ns in `val_num`, key-only `val_num`
        // scan resolves `event:timeSinceStart <op> <duration>`).
        let time_since_start =
            resolve_time_since_start_ns(span.start_time_unix_nano, event.time_unix_nano);
        out.attrs.push(AttrRecord {
            date,
            key: EVENT_INTRINSIC_TIME_SINCE_START_KEY.to_string(),
            scope: SCOPE_EVENT_INTRINSIC.to_string(),
            val: time_since_start.to_string(),
            // A nanosecond count, computed as an `i64`.
            val_type: AttrValueType::Int,
            val_num: Some(time_since_start as f64),
            timestamp_ns,
            trace_id,
            span_id,
            duration_ns,
        });
        for kv in &event.attributes {
            out.attrs.push(attr_record(
                kv,
                SCOPE_EVENT,
                date,
                timestamp_ns,
                trace_id,
                span_id,
                duration_ns,
            ));
        }
    }

    // Span links (issue #192 PR-C): each link fans out exactly like an event —
    // two intrinsic rows under the dedicated `link:intrinsic` scope (the
    // referenced `spanID`/`traceID` as lowercase hex in `val`) followed by one
    // `link`-scoped row per link attribute (verbatim key). The referenced ids
    // are stored as `val` (not `val_num` — hex is non-numeric), so `AttrEq`
    // resolves them. Emitted AFTER the events, charged in
    // `span_expansion_charge` in lockstep.
    for link in &span.links {
        // Intrinsic: link:spanID (dedicated `link:intrinsic` scope, reserved
        // key `spanID`, lowercase hex of the link's raw span-id bytes).
        out.attrs.push(AttrRecord {
            date,
            key: LINK_INTRINSIC_SPAN_ID_KEY.to_string(),
            scope: SCOPE_LINK_INTRINSIC.to_string(),
            val: hex_lower(&link.span_id),
            // Lowercase hex text.
            val_type: AttrValueType::String,
            val_num: None,
            timestamp_ns,
            trace_id,
            span_id,
            duration_ns,
        });
        // Intrinsic: link:traceID (reserved key `traceID`, lowercase hex of
        // the link's raw trace-id bytes).
        out.attrs.push(AttrRecord {
            date,
            key: LINK_INTRINSIC_TRACE_ID_KEY.to_string(),
            scope: SCOPE_LINK_INTRINSIC.to_string(),
            val: hex_lower(&link.trace_id),
            // Lowercase hex text.
            val_type: AttrValueType::String,
            val_num: None,
            timestamp_ns,
            trace_id,
            span_id,
            duration_ns,
        });
        for kv in &link.attributes {
            out.attrs.push(attr_record(
                kv,
                SCOPE_LINK,
                date,
                timestamp_ns,
                trace_id,
                span_id,
                duration_ns,
            ));
        }
    }

    // The five arrays are this span's OWN attr rows, projected. Deriving
    // them rather than rebuilding them is what makes the two stores agree
    // element by element, and it is what keeps the numeric rule right: a
    // link's `spanID` of `0000000000000001` parses as 1.0 and is stored
    // NULL, because the record that built it stores NULL.
    let span_attrs = &out.attrs[attrs_start..];
    let attr_key: Vec<String> = span_attrs.iter().map(|a| a.key.clone()).collect();
    let attr_scope: Vec<String> = span_attrs.iter().map(|a| a.scope.clone()).collect();
    let attr_val: Vec<String> = span_attrs.iter().map(|a| a.val.clone()).collect();
    let attr_type: Vec<AttrValueType> = span_attrs.iter().map(|a| a.val_type).collect();
    let attr_num: Vec<Option<f64>> = span_attrs.iter().map(|a| a.val_num).collect();

    out.spans.push(SpanRecord {
        trace_id,
        span_id,
        parent_id,
        name: span.name.clone(),
        service: ctx.service.to_string(),
        timestamp_ns,
        duration_ns,
        status_code,
        status_message,
        kind,
        shared,
        scope_name,
        scope_version,
        payload: build_payload(span, ctx.resource, ctx.resource_spans, ctx.scope_spans),
        attr_key,
        attr_scope,
        attr_val,
        attr_type,
        attr_num,
    });
    Ok(())
}

/// `1` iff `span` carries the `zipkin.shared` attribute with the exact
/// string value `"true"` — the wire contract `zipkin::to_otlp` emits for a
/// Zipkin shared span (issue #173, `str_kv("zipkin.shared", "true")`),
/// documented so an OTLP-native sender can set it too. Matched on a literal
/// `StringValue` (not the rendered form) so only that precise pair flips the
/// bit; anything else (a bool value, a different string, absence) is `0`.
/// The attribute itself still flows to `trace_attrs_idx` unchanged.
fn shared_flag(span: &Span) -> u8 {
    let is_true = span.attributes.iter().any(|kv| {
        kv.key == "zipkin.shared"
            && matches!(
                kv.value.as_ref().and_then(|v| v.value.as_ref()),
                Some(Value::StringValue(s)) if s == "true"
            )
    });
    u8::from(is_true)
}

/// Rejects a single span into partial success.
fn reject_span(out: &mut ParsedTraces, message: String) {
    out.rejected += 1;
    if out.rejected_message.is_none() {
        out.rejected_message = Some(message);
    }
}

/// The self-contained single-`ResourceSpans` `TracesData` payload for one
/// span (the pinned T2/T3 contract — see the module doc): this span, its
/// own resource + scope, and both original schema URLs, `prost`-encoded.
fn build_payload(
    span: &Span,
    resource: Option<&Resource>,
    resource_spans: &ResourceSpans,
    scope_spans: &ScopeSpans,
) -> Vec<u8> {
    TracesData {
        resource_spans: vec![ResourceSpans {
            resource: resource.cloned(),
            scope_spans: vec![ScopeSpans {
                scope: scope_spans.scope.clone(),
                spans: vec![span.clone()],
                schema_url: scope_spans.schema_url.clone(),
            }],
            schema_url: resource_spans.schema_url.clone(),
        }],
    }
    .encode_to_vec()
}

/// One indexed attribute row: key verbatim, value via the shared
/// `AnyValue` string rendering, `val_num` populated iff the rendered value
/// parses as a finite float (docs/schemas.md §4.1: "populated when val
/// parses numeric" — non-finite parses like `"inf"`/`"NaN"` are excluded,
/// a `Nullable(Float64)` comparison column has no meaningful ordering for
/// them).
fn attr_record(
    kv: &KeyValue,
    scope: &str,
    date: u16,
    timestamp_ns: i64,
    trace_id: [u8; 16],
    span_id: [u8; 8],
    duration_ns: i64,
) -> AttrRecord {
    let val = any_value_to_string(kv.value.as_ref());
    let val_num = numeric_val_num(&val);
    AttrRecord {
        date,
        key: kv.key.clone(),
        scope: scope.to_string(),
        val,
        val_type: any_value_type(kv.value.as_ref()),
        val_num,
        timestamp_ns,
        trace_id,
        span_id,
        duration_ns,
    }
}

/// The `val_num` for a stored `val` string: its `f64` parse when finite,
/// else `None` (docs/schemas.md §4.1 — non-finite parses like `inf`/`NaN`
/// are excluded, a `Nullable(Float64)` comparison column has no meaningful
/// ordering for them).
fn numeric_val_num(val: &str) -> Option<f64> {
    val.parse::<f64>().ok().filter(|n| n.is_finite())
}

/// A span event's `timeSinceStart` in nanoseconds: `event.time_unix_nano −
/// span.start_time_unix_nano` (issue #192 PR-B). Both operands are `u64`
/// wire values; the difference is computed in `i128` and clamped into the
/// `i64` range, so it preserves sign (a non-conformant sender whose event
/// predates its span start yields a negative value, never a wrap) and
/// saturates rather than overflowing. The RAW `start_time_unix_nano` is
/// used (never the now-fallback resolved `timestamp_ns`), matching the
/// reference's literal `time − start` definition — a span with an unknown
/// (`0`) start therefore reports the event's absolute time, the degenerate
/// case a conformant sender never produces.
fn resolve_time_since_start_ns(start_time_unix_nano: u64, event_time_unix_nano: u64) -> i64 {
    let delta = i128::from(event_time_unix_nano) - i128::from(start_time_unix_nano);
    delta.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

/// Lowercase-hex rendering of a span link's referenced id bytes (issue #192
/// PR-C). The bytes are stored verbatim (a link references some OTHER span,
/// so — unlike this span's own 16/8-byte ids — they are never length-validated
/// here): whatever the sender put on the wire is rendered as-is, so an
/// off-length id round-trips honestly rather than being rejected. Matches the
/// `link:spanID`/`link:traceID` intrinsics' `AttrEq` value classification (the
/// TraceQL probe supplies lowercase hex).
fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        // Infallible: writing to a String never errors.
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// The resource attribute backing the promoted `service` column: the one
/// literally keyed `service.name`, **verbatim** — traces never normalize
/// keys (docs/architecture.md §2.3), so unlike logs/metrics a
/// `service_name`-keyed attribute does not match. Returns the raw
/// `KeyValue` (never a rendering): the caller must charge the budget for
/// the rendering before performing it (check-then-render, issue #54
/// code-review round 3).
fn find_service_kv(resource: Option<&Resource>) -> Option<&KeyValue> {
    resource
        .map(|r| r.attributes.as_slice())
        .unwrap_or(&[])
        .iter()
        .find(|kv| kv.key == "service.name")
}

/// The estimated expanded-output byte charge for one span's rows +
/// payload: the fixed per-span floor, its name/service clone cost, the
/// per-span payload duplication (`payload_base` = resource + scope +
/// schema-URL wire lengths, precomputed once per scope), the span's own
/// wire length, and every resource ⊕ span attribute row (fixed overhead +
/// [`attr_budget_charge`], which multiplies the kinds whose stored
/// rendering can outgrow their wire bytes). Allocation-free: `encoded_len`
/// never allocates. Extracted from [`parse_span`] so the identical measure
/// is reachable from [`resource_spans_expansion_charge`].
fn span_expansion_charge(
    span: &Span,
    resource: Option<&Resource>,
    scope: Option<&InstrumentationScope>,
    service_len: usize,
    payload_base: usize,
) -> usize {
    let resource_attrs = resource.map(|r| r.attributes.as_slice()).unwrap_or(&[]);
    let scope_attrs = scope.map(|s| s.attributes.as_slice()).unwrap_or(&[]);
    // The promoted `status_message` (issue #184) is an extra materialized
    // copy beyond the wire `span.encoded_len()`, like `name`/`service`. The
    // promoted `scope_name`/`scope_version` (issue #192) are the same shape.
    let status_message_len = span.status.as_ref().map(|s| s.message.len()).unwrap_or(0);
    let scope_name_len = scope.map(|s| s.name.len() + s.version.len()).unwrap_or(0);
    // The five `Vec` headers this span's attribute arrays carry, once per
    // span (issue #556); the per-element terms are charged in lockstep
    // with the emission loops below.
    let mut charge = SPAN_ROW_OVERHEAD
        + ATTR_ARRAY_HEADER_OVERHEAD
        + span.name.len()
        + service_len
        + payload_base
        + span.encoded_len()
        + status_message_len
        + scope_name_len;
    // Every resource ⊕ span ⊕ instrumentation-scope attribute becomes one
    // indexed row per span (issue #192 adds the scope arm) — charged BEFORE
    // materialization, in lockstep with the emission loop above.
    // Each also becomes one ARRAY element on the span's own row (issue
    // #556): the element's slot cost plus a SECOND copy of its rendered
    // key/value text.
    for kv in resource_attrs
        .iter()
        .chain(&span.attributes)
        .chain(scope_attrs)
    {
        charge += ATTR_ROW_OVERHEAD + ATTR_ARRAY_ELEMENT_OVERHEAD + 2 * attr_budget_charge(kv);
    }
    // Every span event (issue #192 PR-B) fans out into indexed rows: two
    // intrinsic rows (`event:intrinsic` name/timeSinceStart — the name's
    // `val` is the event name; timeSinceStart is a small numeric string
    // within the row-overhead floor) plus one row per event attribute —
    // charged BEFORE materialization, in lockstep with the event emission
    // loop above. `span.encoded_len()` already covers the events' wire
    // bytes (they nest inside the span); this is the extra index-row cost.
    for event in &span.events {
        // Both intrinsics also become array elements on the span's own row
        // (issue #556), and the event name is then STORED TWICE — once in
        // the index row's `val`, once in `attr_val[i]`.
        charge += 2 * ATTR_ROW_OVERHEAD + 2 * ATTR_ARRAY_ELEMENT_OVERHEAD + 2 * event.name.len();
        for kv in &event.attributes {
            charge += ATTR_ROW_OVERHEAD + ATTR_ARRAY_ELEMENT_OVERHEAD + 2 * attr_budget_charge(kv);
        }
    }
    // Every span link (issue #192 PR-C) fans out identically: two intrinsic
    // rows (`link:intrinsic` spanID/traceID, whose `val` is lowercase hex —
    // 2× the referenced id's byte length) plus one row per link attribute —
    // charged BEFORE materialization, in lockstep with the link emission loop
    // above. `span.encoded_len()` already covers the links' wire bytes.
    for link in &span.links {
        // Each intrinsic also becomes an array element (issue #556). The
        // hex rendering is twice the referenced id's byte length and it is
        // now stored twice, hence `4 *` where the index row alone was
        // `2 *`.
        charge += ATTR_ROW_OVERHEAD + ATTR_ARRAY_ELEMENT_OVERHEAD + 4 * link.span_id.len();
        charge += ATTR_ROW_OVERHEAD + ATTR_ARRAY_ELEMENT_OVERHEAD + 4 * link.trace_id.len();
        for kv in &link.attributes {
            charge += ATTR_ROW_OVERHEAD + ATTR_ARRAY_ELEMENT_OVERHEAD + 2 * attr_budget_charge(kv);
        }
    }
    charge
}

/// The estimated expanded-output byte charge [`parse`] accumulates for one
/// `ResourceSpans` block: the promoted-`service` rendering charge (once per
/// block, escape-multiplied like any rendered attribute) plus every span's
/// [`span_expansion_charge`]. Byte-identical to what `parse` sums over the
/// same block (it factors through the same two helpers), so an upstream
/// adapter can charge the SAME budget against an adapted block and reach
/// the identical accept/reject verdict. Allocation-free except the one
/// per-block `service` render `parse` itself performs.
fn resource_spans_expansion_charge(rs: &ResourceSpans) -> usize {
    let resource = rs.resource.as_ref();
    let service_kv = find_service_kv(resource);
    let mut charge = service_kv.map(attr_budget_charge).unwrap_or(0);
    let service_len = service_kv
        .map(|kv| any_value_to_string(kv.value.as_ref()).len())
        .unwrap_or(0);
    let resource_wire_len = resource.map(Message::encoded_len).unwrap_or(0);
    for scope_spans in &rs.scope_spans {
        let payload_base = resource_wire_len
            + scope_spans
                .scope
                .as_ref()
                .map(Message::encoded_len)
                .unwrap_or(0)
            + rs.schema_url.len()
            + scope_spans.schema_url.len()
            + PAYLOAD_ENVELOPE_OVERHEAD;
        for span in &scope_spans.spans {
            charge = charge.saturating_add(span_expansion_charge(
                span,
                resource,
                scope_spans.scope.as_ref(),
                service_len,
                payload_base,
            ));
        }
    }
    charge
}

/// Charges one already-adapted `ResourceSpans` block against a running
/// [`MAX_EXPANDED_BYTES`] budget, failing the whole request the moment the
/// running total exceeds it. The reserve-before-materialize hook for an
/// upstream model adapter (the Zipkin receiver, issue #75) that produces
/// self-contained single-`ResourceSpans` blocks: charging each block as it
/// is adapted — BEFORE the full batch is materialized — makes an
/// over-budget foreign request abort mid-adaptation rather than exhaust
/// memory ahead of [`parse`]'s post-materialization check, and (because the
/// measure is [`resource_spans_expansion_charge`], the exact per-block sum
/// `parse` uses) rejects it byte-identically to the equivalent native OTLP
/// request.
pub(crate) fn charge_resource_spans_expansion(
    expanded_bytes: &mut usize,
    rs: &ResourceSpans,
) -> Result<(), LogsIngestError> {
    charge_budget(expanded_bytes, resource_spans_expansion_charge(rs))
}

/// Adds `amount` to the running expansion estimate and fails the whole
/// request the moment it exceeds [`MAX_EXPANDED_BYTES`] — the single
/// charge/check point every materialization site (per-span rows/payload,
/// per-resource `service` rendering) reserves through before allocating.
fn charge_budget(expanded_bytes: &mut usize, amount: usize) -> Result<(), LogsIngestError> {
    *expanded_bytes = expanded_bytes.saturating_add(amount);
    if *expanded_bytes > MAX_EXPANDED_BYTES {
        return Err(LogsIngestError::OversizeMessage {
            field: "expanded trace row bytes (estimated)",
            limit: MAX_EXPANDED_BYTES,
            actual: *expanded_bytes,
        });
    }
    Ok(())
}

/// `end - start` when both are set and ordered; `0` when `end` is unset
/// (`0` on the wire) or precedes `start` (a non-conformant sender — a
/// negative duration would poison duration predicates); saturates to
/// `i64::MAX` on an unrepresentable (u64-overflowing) difference.
fn resolve_duration_ns(start_time_unix_nano: u64, end_time_unix_nano: u64) -> i64 {
    if end_time_unix_nano == 0 || end_time_unix_nano < start_time_unix_nano {
        return 0;
    }
    i64::try_from(end_time_unix_nano - start_time_unix_nano).unwrap_or(i64::MAX)
}

/// Renders an OTLP attribute's `AnyValue` to its stored string form: a
/// string value verbatim; a scalar (bool/int/double) via `Display`; an
/// array/kvlist via `serde_json`; bytes as base64. Absent (`None`) or an
/// entirely unspecified `AnyValue` both render as `""`. Mirrors
/// `otlp_logs::any_value_to_string` byte-for-byte (duplicated here rather
/// than shared: the codebase already duplicates it in both `otlp_logs` and
/// `otlp_metrics` — the established per-parser convention, see
/// `otlp_metrics::attr_pairs`'s doc comment).
fn any_value_to_string(value: Option<&AnyValue>) -> String {
    let Some(value) = value.and_then(|v| v.value.as_ref()) else {
        return String::new();
    };
    match value {
        Value::StringValue(s) => s.clone(),
        Value::BoolValue(b) => b.to_string(),
        Value::IntValue(i) => i.to_string(),
        Value::DoubleValue(d) => d.to_string(),
        Value::ArrayValue(_) | Value::KvlistValue(_) => {
            serde_json::to_string(&any_value_to_json(value)).expect(
                "a JSON value tree built only from strings/numbers/bools/arrays/objects \
                 cannot fail to serialize",
            )
        }
        Value::BytesValue(bytes) => base64_encode(bytes),
        // Profiling-signal-only reference; non-profiling receivers treat
        // its presence as a non-fatal issue and process the value as
        // absent/empty (mirrors `otlp_logs::any_value_to_string`).
        Value::StringValueStrindex(_) => String::new(),
    }
}

/// The stored type discriminator for an OTLP `AnyValue` (issue #476) —
/// the companion of [`any_value_to_string`], and deliberately written
/// beside it so the two cannot classify the same `AnyValue` differently.
///
/// Array, kvlist and bytes values RENDER to a string
/// ([`any_value_to_string`] JSON-encodes the first two and base64s the
/// third), so they are [`AttrValueType::String`] here: the column states
/// the type of what we stored, which is what it means. An absent or
/// entirely unspecified `AnyValue` renders `""` and is likewise a string.
///
/// The reference emits no tag-value row at all for a bytes or
/// heterogeneous-array attribute; that is a difference in row PRESENCE,
/// not in `type`, and is out of scope here.
fn any_value_type(value: Option<&AnyValue>) -> AttrValueType {
    let Some(value) = value.and_then(|v| v.value.as_ref()) else {
        return AttrValueType::String;
    };
    match value {
        Value::StringValue(_) => AttrValueType::String,
        Value::BoolValue(_) => AttrValueType::Bool,
        Value::IntValue(_) => AttrValueType::Int,
        Value::DoubleValue(_) => AttrValueType::Float,
        Value::ArrayValue(_) | Value::KvlistValue(_) => AttrValueType::String,
        Value::BytesValue(_) => AttrValueType::String,
        Value::StringValueStrindex(_) => AttrValueType::String,
    }
}

/// Recursively renders an `AnyValue`'s `value` oneof to a `serde_json`
/// tree, used for the array/kvlist branch of [`any_value_to_string`] —
/// mirrors `otlp_logs::any_value_to_json` byte-for-byte (same duplication
/// rationale as [`any_value_to_string`]).
fn any_value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::StringValue(s) => serde_json::Value::String(s.clone()),
        Value::BoolValue(b) => serde_json::Value::Bool(*b),
        Value::IntValue(i) => serde_json::Value::Number((*i).into()),
        Value::DoubleValue(d) => serde_json::Number::from_f64(*d)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::ArrayValue(array) => serde_json::Value::Array(
            array
                .values
                .iter()
                .map(|v| {
                    v.value
                        .as_ref()
                        .map(any_value_to_json)
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        Value::KvlistValue(kvlist) => {
            let mut map = serde_json::Map::with_capacity(kvlist.values.len());
            for entry in &kvlist.values {
                let rendered = entry
                    .value
                    .as_ref()
                    .and_then(|v| v.value.as_ref())
                    .map(any_value_to_json)
                    .unwrap_or(serde_json::Value::Null);
                map.insert(entry.key.clone(), rendered);
            }
            serde_json::Value::Object(map)
        }
        Value::BytesValue(bytes) => serde_json::Value::String(base64_encode(bytes)),
        Value::StringValueStrindex(_) => serde_json::Value::Null,
    }
}

/// Minimal RFC 4648 standard base64 encoder (with padding), duplicated
/// from `otlp_logs::base64_encode` for the same reason (see that fn's doc
/// comment and [`any_value_to_string`]'s duplication note).
fn base64_encode(input: &[u8]) -> String {
    const CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied();
        let b2 = chunk.get(2).copied();
        let n =
            (u32::from(b0) << 16) | (u32::from(b1.unwrap_or(0)) << 8) | u32::from(b2.unwrap_or(0));
        out.push(CHARS[((n >> 18) & 0x3F) as usize] as char);
        out.push(CHARS[((n >> 12) & 0x3F) as usize] as char);
        out.push(if b1.is_some() {
            CHARS[((n >> 6) & 0x3F) as usize] as char
        } else {
            '='
        });
        out.push(if b2.is_some() {
            CHARS[(n & 0x3F) as usize] as char
        } else {
            '='
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{ArrayValue, InstrumentationScope, KeyValueList};
    use opentelemetry_proto::tonic::trace::v1::Status;
    use opentelemetry_proto::tonic::trace::v1::span::Event;
    use opentelemetry_proto::tonic::trace::v1::span::Link;
    use opentelemetry_proto::tonic::trace::v1::span::SpanKind;
    use opentelemetry_proto::tonic::trace::v1::status::StatusCode;

    fn kv(key: &str, value: Value) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue { value: Some(value) }),
            key_strindex: 0,
        }
    }

    fn span(trace_id: Vec<u8>, span_id: Vec<u8>) -> Span {
        Span {
            trace_id,
            span_id,
            name: "op-a".to_string(),
            kind: SpanKind::Server as i32,
            start_time_unix_nano: 1_700_000_000_000_000_000,
            end_time_unix_nano: 1_700_000_001_000_000_000,
            ..Default::default()
        }
    }

    fn valid_span() -> Span {
        span(vec![1; 16], vec![2; 8])
    }

    fn request_with(
        resource: Option<Resource>,
        scope: Option<InstrumentationScope>,
        spans: Vec<Span>,
    ) -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource,
                scope_spans: vec![ScopeSpans {
                    scope,
                    spans,
                    schema_url: "https://example.com/scope-schema".to_string(),
                }],
                schema_url: "https://example.com/resource-schema".to_string(),
            }],
        }
    }

    fn checkout_resource() -> Resource {
        Resource {
            attributes: vec![kv(
                "service.name",
                Value::StringValue("checkout".to_string()),
            )],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        }
    }

    // -- decode ---------------------------------------------------------

    #[test]
    fn decode_rejects_malformed_bytes() {
        let err = decode(b"\xFF\xFF\xFF not a protobuf message").unwrap_err();
        assert!(matches!(err, LogsIngestError::Decode(_)));
    }

    #[test]
    fn decode_round_trips_an_encoded_request() {
        let req = request_with(Some(checkout_resource()), None, vec![valid_span()]);
        let decoded = decode(&req.encode_to_vec()).expect("valid protobuf decodes");
        assert_eq!(decoded, req);
    }

    // -- parse: empty / pure ---------------------------------------------

    #[test]
    fn parse_of_empty_request_returns_empty_output() {
        let out = parse(&ExportTraceServiceRequest::default(), 1_000)
            .expect("within the expansion budget");
        assert_eq!(out, ParsedTraces::default());
    }

    #[test]
    fn parse_is_a_pure_function_of_its_arguments() {
        let req = request_with(Some(checkout_resource()), None, vec![valid_span()]);
        assert_eq!(
            parse(&req, 42).expect("within the expansion budget"),
            parse(&req, 42).expect("within the expansion budget")
        );
    }

    // -- span fields -------------------------------------------------------

    #[test]
    fn parse_promotes_resource_service_name_verbatim() {
        let out = parse(
            &request_with(Some(checkout_resource()), None, vec![valid_span()]),
            0,
        )
        .expect("within the expansion budget");
        assert_eq!(out.spans.len(), 1);
        assert_eq!(out.spans[0].service, "checkout");
    }

    #[test]
    fn parse_service_is_empty_when_resource_or_service_name_is_absent() {
        let out = parse(&request_with(None, None, vec![valid_span()]), 0)
            .expect("within the expansion budget");
        assert_eq!(out.spans[0].service, "");

        // Verbatim key semantics: a normalized `service_name` key does NOT
        // populate the column (traces never normalize keys).
        let resource = Resource {
            attributes: vec![kv(
                "service_name",
                Value::StringValue("checkout".to_string()),
            )],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };
        let out = parse(&request_with(Some(resource), None, vec![valid_span()]), 0)
            .expect("within the expansion budget");
        assert_eq!(out.spans[0].service, "");
    }

    #[test]
    fn parse_copies_ids_status_kind_and_times() {
        let mut s = valid_span();
        s.parent_span_id = vec![3; 8];
        s.status = Some(Status {
            message: "deadline exceeded".to_string(),
            code: StatusCode::Error as i32,
        });
        let out =
            parse(&request_with(None, None, vec![s]), 0).expect("within the expansion budget");
        let span = &out.spans[0];
        assert_eq!(span.trace_id, [1; 16]);
        assert_eq!(span.span_id, [2; 8]);
        assert_eq!(span.parent_id, [3; 8]);
        assert_eq!(span.name, "op-a");
        assert_eq!(span.timestamp_ns, 1_700_000_000_000_000_000);
        assert_eq!(span.duration_ns, 1_000_000_000);
        assert_eq!(span.status_code, StatusCode::Error as i8);
        assert_eq!(
            span.status_message, "deadline exceeded",
            "issue #184: Status.message is stored verbatim, no longer dropped"
        );
        assert_eq!(span.kind, SpanKind::Server as i8);
    }

    /// Issue #173 (M7-E1) AC7(e): an OTLP-native span with no `zipkin.shared`
    /// attribute stores `shared = 0`; a span carrying `zipkin.shared = "true"`
    /// (the wire contract `zipkin::to_otlp` emits) stores `shared = 1`. The
    /// attribute still flows to `trace_attrs_idx` as an ordinary span attr.
    #[test]
    fn parse_promotes_the_zipkin_shared_attribute_to_the_shared_column() {
        // Native span, no marker → shared = 0.
        let out = parse(&request_with(None, None, vec![valid_span()]), 0)
            .expect("within the expansion budget");
        assert_eq!(out.spans[0].shared, 0);

        // A shared-flagged span → shared = 1, and the attr is still indexed.
        let mut s = valid_span();
        s.attributes = vec![kv("zipkin.shared", Value::StringValue("true".to_string()))];
        let out =
            parse(&request_with(None, None, vec![s]), 0).expect("within the expansion budget");
        assert_eq!(out.spans[0].shared, 1);
        assert!(
            out.attrs
                .iter()
                .any(|a| a.key == "zipkin.shared" && a.val == "true"),
            "the shared attribute still flows to trace_attrs_idx"
        );

        // Only the exact string "true" flips the bit — a bool value or a
        // different string does not (the literal wire contract).
        let mut s = valid_span();
        s.attributes = vec![kv("zipkin.shared", Value::BoolValue(true))];
        let out =
            parse(&request_with(None, None, vec![s]), 0).expect("within the expansion budget");
        assert_eq!(
            out.spans[0].shared, 0,
            "a bool value is not the string contract"
        );

        let mut s = valid_span();
        s.attributes = vec![kv("zipkin.shared", Value::StringValue("false".to_string()))];
        let out =
            parse(&request_with(None, None, vec![s]), 0).expect("within the expansion budget");
        assert_eq!(out.spans[0].shared, 0);
    }

    #[test]
    fn parse_missing_status_resolves_to_code_zero() {
        let out = parse(&request_with(None, None, vec![valid_span()]), 0)
            .expect("within the expansion budget");
        assert_eq!(out.spans[0].status_code, 0);
        assert_eq!(
            out.spans[0].status_message, "",
            "no Status ⇒ the empty message (issue #184)"
        );
    }

    #[test]
    fn parse_empty_parent_span_id_maps_to_the_zero_sentinel() {
        let out = parse(&request_with(None, None, vec![valid_span()]), 0)
            .expect("within the expansion budget");
        assert_eq!(out.spans[0].parent_id, [0u8; 8]);
    }

    // -- timestamp / duration rules ---------------------------------------

    #[test]
    fn parse_zero_start_time_falls_back_to_now_ns() {
        let mut s = valid_span();
        s.start_time_unix_nano = 0;
        s.end_time_unix_nano = 0;
        let out =
            parse(&request_with(None, None, vec![s]), 999).expect("within the expansion budget");
        assert_eq!(out.spans[0].timestamp_ns, 999);
        assert_eq!(out.spans[0].duration_ns, 0);
    }

    #[test]
    fn parse_rejects_a_span_with_an_unrepresentable_start_time_as_partial_success() {
        let mut bad = valid_span();
        bad.start_time_unix_nano = u64::MAX; // top bit set: does not fit in i64
        bad.attributes = vec![kv("http.method", Value::StringValue("GET".to_string()))];
        let good = valid_span();
        let out = parse(&request_with(None, None, vec![bad, good]), 0)
            .expect("within the expansion budget");
        assert_eq!(out.rejected, 1);
        assert!(out.rejected_message.is_some());
        assert_eq!(out.spans.len(), 1);
        // The rejected span contributes no attr rows either.
        assert!(out.attrs.is_empty());
    }

    #[test]
    fn parse_rejects_a_far_future_span_instead_of_orphaning_it_into_the_max_date_partition() {
        // Representable as i64 ns but ~year 2200 — past the 2149-06-06
        // ClickHouse `Date` cutoff. Before #8's fix `trace_attrs_idx.date`
        // saturated to day 65535, silently orphaning the span; now it is a
        // clean per-span rejection (partial success), with no span/attr rows.
        let far_future_ns: u64 = 86_400_000_000_000 * 84_000;
        let mut bad = valid_span();
        bad.start_time_unix_nano = far_future_ns;
        bad.end_time_unix_nano = far_future_ns;
        bad.attributes = vec![kv("http.method", Value::StringValue("GET".to_string()))];
        let good = valid_span();
        let out = parse(&request_with(None, None, vec![bad, good]), 0)
            .expect("within the expansion budget");
        assert_eq!(out.rejected, 1);
        assert!(
            out.rejected_message.as_deref().unwrap().contains(
                "outside the supported storage time range (1970-01-01 to 2106-02-06 UTC)"
            )
        );
        assert_eq!(out.spans.len(), 1);
        // No attr row registered at the max-`Date` boundary.
        assert!(out.attrs.iter().all(|a| a.date != u16::MAX));
    }

    /// Issue #131: a span on day 49_710 (2106-02-07) partitions correctly
    /// (inside the `Date` range) but its timestamp exceeds the 32-bit
    /// DateTime domain the trace delete-TTL evaluates in — before the
    /// DateTime-safe gate such a span was accepted and stored with a
    /// wrap-prone TTL input. It is now a clean per-span rejection (partial
    /// success), with no span/attr rows.
    #[test]
    fn parse_rejects_a_span_on_the_first_datetime_unsafe_day_as_partial_success() {
        let first_unsafe_ns: u64 = 86_400_000_000_000 * 49_710;
        let mut bad = valid_span();
        bad.start_time_unix_nano = first_unsafe_ns;
        bad.end_time_unix_nano = first_unsafe_ns;
        bad.attributes = vec![kv("http.method", Value::StringValue("GET".to_string()))];
        let good = valid_span();
        let out = parse(&request_with(None, None, vec![bad, good]), 0)
            .expect("within the expansion budget");
        assert_eq!(out.rejected, 1);
        assert!(
            out.rejected_message
                .as_deref()
                .unwrap()
                .contains("outside the supported storage time range")
        );
        assert_eq!(out.spans.len(), 1);
        assert!(out.attrs.is_empty());
    }

    /// Issue #131 boundary acceptance: the last nanosecond of day 49_709
    /// (2106-02-06, the last fully DateTime-safe UTC day) is still admitted,
    /// and every attr row carries `date == 49_709`.
    #[test]
    fn parse_accepts_a_span_at_the_last_datetime_safe_nanosecond() {
        let last_safe_ns: u64 = 86_400_000_000_000 * 49_710 - 1;
        let mut s = valid_span();
        s.start_time_unix_nano = last_safe_ns;
        s.end_time_unix_nano = last_safe_ns;
        s.attributes = vec![kv("http.method", Value::StringValue("GET".to_string()))];
        let out =
            parse(&request_with(None, None, vec![s]), 0).expect("within the expansion budget");
        assert_eq!(out.rejected, 0);
        assert_eq!(out.spans.len(), 1);
        assert!(!out.attrs.is_empty());
        assert!(out.attrs.iter().all(|a| a.date == 49_709));
    }

    #[test]
    fn parse_zero_or_inverted_end_time_yields_zero_duration() {
        let mut unset_end = valid_span();
        unset_end.end_time_unix_nano = 0;
        let mut inverted = valid_span();
        inverted.end_time_unix_nano = inverted.start_time_unix_nano - 1;
        let out = parse(&request_with(None, None, vec![unset_end, inverted]), 0)
            .expect("within the expansion budget");
        assert_eq!(out.spans[0].duration_ns, 0);
        assert_eq!(out.spans[1].duration_ns, 0);
    }

    #[test]
    fn parse_saturates_an_i64_overflowing_duration_to_max() {
        let mut s = valid_span();
        s.start_time_unix_nano = 1;
        s.end_time_unix_nano = u64::MAX;
        let out =
            parse(&request_with(None, None, vec![s]), 0).expect("within the expansion budget");
        assert_eq!(out.spans[0].duration_ns, i64::MAX);
    }

    // -- id validation ------------------------------------------------------

    #[test]
    fn parse_rejects_spans_with_wrong_length_ids() {
        let short_trace = span(vec![1; 15], vec![2; 8]);
        let short_span = span(vec![1; 16], vec![2; 7]);
        let mut bad_parent = valid_span();
        bad_parent.parent_span_id = vec![3; 4];
        let out = parse(
            &request_with(None, None, vec![short_trace, short_span, bad_parent]),
            0,
        )
        .expect("within the expansion budget");
        assert_eq!(out.rejected, 3);
        assert!(out.spans.is_empty());
        assert!(out.attrs.is_empty());
        assert!(
            out.rejected_message
                .as_ref()
                .is_some_and(|m| m.contains("trace_id")),
            "first rejection's message is surfaced: {:?}",
            out.rejected_message
        );
    }

    // -- attribute indexing ---------------------------------------------

    #[test]
    fn parse_indexes_resource_and_span_attrs_with_scopes_and_verbatim_keys() {
        let resource = Resource {
            attributes: vec![
                kv("service.name", Value::StringValue("checkout".to_string())),
                kv(
                    "deployment.environment",
                    Value::StringValue("prod".to_string()),
                ),
            ],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };
        let mut s = valid_span();
        s.attributes = vec![
            kv("http.status_code", Value::IntValue(500)),
            kv("http.method", Value::StringValue("GET".to_string())),
        ];
        let out = parse(&request_with(Some(resource), None, vec![s]), 0)
            .expect("within the expansion budget");

        let rows: Vec<(&str, &str, &str, Option<f64>)> = out
            .attrs
            .iter()
            .map(|a| (a.scope.as_str(), a.key.as_str(), a.val.as_str(), a.val_num))
            .collect();
        assert_eq!(
            rows,
            vec![
                ("resource", "service.name", "checkout", None),
                ("resource", "deployment.environment", "prod", None),
                ("span", "http.status_code", "500", Some(500.0)),
                ("span", "http.method", "GET", None),
            ],
            "verbatim keys, resource-then-span order, scope discriminators, numeric val_num"
        );
        for attr in &out.attrs {
            assert_eq!(attr.trace_id, [1; 16]);
            assert_eq!(attr.span_id, [2; 8]);
            assert_eq!(attr.timestamp_ns, 1_700_000_000_000_000_000);
            assert_eq!(attr.duration_ns, 1_000_000_000);
            // 1_700_000_000s / 86_400s = day 19675 (2023-11-14 UTC).
            assert_eq!(attr.date, 19_675);
        }
    }

    /// Issue #54 plan v2 test-gap fix: the same verbatim key at BOTH scopes
    /// yields two distinct rows separated only by `scope` — the exact
    /// collision the scope discriminator exists to prevent.
    #[test]
    fn parse_same_key_at_resource_and_span_scope_yields_two_scoped_rows() {
        let resource = Resource {
            attributes: vec![kv(
                "deployment.environment",
                Value::StringValue("prod".to_string()),
            )],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };
        let mut s = valid_span();
        s.attributes = vec![kv(
            "deployment.environment",
            Value::StringValue("prod".to_string()),
        )];
        let out = parse(&request_with(Some(resource), None, vec![s]), 0)
            .expect("within the expansion budget");
        let scopes: Vec<&str> = out
            .attrs
            .iter()
            .filter(|a| a.key == "deployment.environment" && a.val == "prod")
            .map(|a| a.scope.as_str())
            .collect();
        assert_eq!(scopes, vec!["resource", "span"]);
    }

    /// Issue #192 (supersedes the #54 adjudication #2 that dropped them):
    /// `InstrumentationScope` attributes ARE indexed under
    /// `scope='instrumentation'` with their key verbatim, in
    /// resource→span→instrumentation emission order; the scope `name`/
    /// `version` are promoted onto the `SpanRecord` columns.
    #[test]
    fn parse_indexes_instrumentation_scope_attributes_and_promotes_name_version() {
        let resource = Resource {
            attributes: vec![kv("res.attr", Value::StringValue("r".to_string()))],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };
        let mut s = valid_span();
        s.attributes = vec![kv("span.attr", Value::StringValue("s".to_string()))];
        let scope = InstrumentationScope {
            name: "my-scope".to_string(),
            version: "1.0.0".to_string(),
            attributes: vec![kv("scope.attr", Value::StringValue("x".to_string()))],
            dropped_attributes_count: 0,
        };
        let out = parse(&request_with(Some(resource), Some(scope), vec![s]), 0)
            .expect("within the expansion budget");

        // The instrumentation attribute is indexed verbatim, last in the
        // resource→span→instrumentation emission order.
        let rows: Vec<(&str, &str, &str)> = out
            .attrs
            .iter()
            .map(|a| (a.scope.as_str(), a.key.as_str(), a.val.as_str()))
            .collect();
        assert_eq!(
            rows,
            vec![
                ("resource", "res.attr", "r"),
                ("span", "span.attr", "s"),
                ("instrumentation", "scope.attr", "x"),
            ]
        );

        // The scope name/version are promoted onto the span row.
        assert_eq!(out.spans.len(), 1);
        assert_eq!(out.spans[0].scope_name, "my-scope");
        assert_eq!(out.spans[0].scope_version, "1.0.0");
    }

    /// A span whose `ScopeSpans` carries no `scope` yields no
    /// instrumentation attr rows and empty `scope_name`/`scope_version`.
    #[test]
    fn parse_absent_instrumentation_scope_leaves_scope_columns_empty() {
        let out = parse(&request_with(None, None, vec![valid_span()]), 0)
            .expect("within the expansion budget");
        assert!(out.attrs.iter().all(|a| a.scope != "instrumentation"));
        assert_eq!(out.spans.len(), 1);
        assert_eq!(out.spans[0].scope_name, "");
        assert_eq!(out.spans[0].scope_version, "");
    }

    /// Issue #192 PR-B: a span event fans out into indexed rows — two
    /// intrinsic rows under the dedicated `event:intrinsic` scope (`name`,
    /// `timeSinceStart` in ns via `val_num`) followed by one `event`-scoped
    /// row per event attribute (verbatim key), emitted after the
    /// resource/span/instrumentation attrs.
    #[test]
    fn parse_indexes_span_events_intrinsics_and_attributes() {
        let mut s = valid_span(); // start = 1_700_000_000_000_000_000
        s.attributes = vec![kv("span.attr", Value::StringValue("s".to_string()))];
        s.events = vec![Event {
            time_unix_nano: s.start_time_unix_nano + 3_000_000, // +3ms
            name: "exception".to_string(),
            attributes: vec![kv(
                "exception.type",
                Value::StringValue("IOError".to_string()),
            )],
            dropped_attributes_count: 0,
        }];
        let out =
            parse(&request_with(None, None, vec![s]), 0).expect("within the expansion budget");

        let rows: Vec<(&str, &str, &str, Option<f64>)> = out
            .attrs
            .iter()
            .map(|a| (a.scope.as_str(), a.key.as_str(), a.val.as_str(), a.val_num))
            .collect();
        assert_eq!(
            rows,
            vec![
                ("span", "span.attr", "s", None),
                // ...then the event: intrinsics first (dedicated scope,
                // reserved keys), then its verbatim attribute.
                ("event:intrinsic", "name", "exception", None),
                (
                    "event:intrinsic",
                    "timeSinceStart",
                    "3000000",
                    Some(3_000_000.0)
                ),
                ("event", "exception.type", "IOError", None),
            ]
        );
    }

    /// The event intrinsic scope is a HARD partition: an event attribute
    /// literally keyed `name` lands under `scope='event'`, never colliding
    /// with the `event:intrinsic`/`name` intrinsic row.
    #[test]
    fn span_event_attribute_named_like_an_intrinsic_stays_in_the_event_scope() {
        let mut s = valid_span();
        s.events = vec![Event {
            time_unix_nano: s.start_time_unix_nano,
            name: "evt".to_string(),
            attributes: vec![kv("name", Value::StringValue("shadow".to_string()))],
            dropped_attributes_count: 0,
        }];
        let out =
            parse(&request_with(None, None, vec![s]), 0).expect("within the expansion budget");
        // The intrinsic `name` row (event:intrinsic) and the sender's `name`
        // attribute (event) are two distinct rows separated by scope.
        let name_rows: Vec<(&str, &str)> = out
            .attrs
            .iter()
            .filter(|a| a.key == "name")
            .map(|a| (a.scope.as_str(), a.val.as_str()))
            .collect();
        assert_eq!(
            name_rows,
            vec![("event:intrinsic", "evt"), ("event", "shadow")]
        );
    }

    /// `timeSinceStart` preserves sign and saturates (issue #192 PR-B): a
    /// conformant event (after start) is positive; a non-conformant event
    /// before its span start is negative, never a wrap.
    #[test]
    fn span_event_time_since_start_is_signed_and_saturating() {
        let mut s = valid_span();
        s.start_time_unix_nano = 1_000;
        s.events = vec![
            Event {
                time_unix_nano: 1_500, // +500ns
                name: "after".to_string(),
                attributes: vec![],
                dropped_attributes_count: 0,
            },
            Event {
                time_unix_nano: 400, // −600ns (before start)
                name: "before".to_string(),
                attributes: vec![],
                dropped_attributes_count: 0,
            },
        ];
        let out =
            parse(&request_with(None, None, vec![s]), 0).expect("within the expansion budget");
        let tss: Vec<f64> = out
            .attrs
            .iter()
            .filter(|a| a.scope == "event:intrinsic" && a.key == "timeSinceStart")
            .map(|a| a.val_num.expect("timeSinceStart carries val_num"))
            .collect();
        assert_eq!(tss, vec![500.0, -600.0]);
    }

    /// A span carrying no events yields no `event`/`event:intrinsic` rows.
    #[test]
    fn parse_absent_events_leave_no_event_rows() {
        let out = parse(&request_with(None, None, vec![valid_span()]), 0)
            .expect("within the expansion budget");
        assert!(
            out.attrs
                .iter()
                .all(|a| a.scope != "event" && a.scope != "event:intrinsic")
        );
    }

    /// Issue #192 PR-C: a span link fans out into indexed rows — two intrinsic
    /// rows under the dedicated `link:intrinsic` scope (`spanID`/`traceID` as
    /// lowercase hex in `val`) followed by one `link`-scoped row per link
    /// attribute (verbatim key), emitted after the events.
    #[test]
    fn parse_indexes_span_links_intrinsics_and_attributes() {
        let mut s = valid_span();
        s.attributes = vec![kv("span.attr", Value::StringValue("s".to_string()))];
        s.links = vec![Link {
            trace_id: vec![
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
                0x0e, 0x0f,
            ],
            span_id: vec![0x0a, 0x1b, 0x2c, 0x3d, 0x4e, 0x5f, 0x60, 0x71],
            trace_state: String::new(),
            attributes: vec![kv(
                "link.relation",
                Value::StringValue("child_of".to_string()),
            )],
            dropped_attributes_count: 0,
            flags: 0,
        }];
        let out =
            parse(&request_with(None, None, vec![s]), 0).expect("within the expansion budget");

        let rows: Vec<(&str, &str, &str, Option<f64>)> = out
            .attrs
            .iter()
            .map(|a| (a.scope.as_str(), a.key.as_str(), a.val.as_str(), a.val_num))
            .collect();
        assert_eq!(
            rows,
            vec![
                ("span", "span.attr", "s", None),
                // ...then the link: intrinsics first (dedicated scope, reserved
                // keys, lowercase-hex ids in `val`), then its verbatim attribute.
                ("link:intrinsic", "spanID", "0a1b2c3d4e5f6071", None),
                (
                    "link:intrinsic",
                    "traceID",
                    "000102030405060708090a0b0c0d0e0f",
                    None
                ),
                ("link", "link.relation", "child_of", None),
            ]
        );
    }

    /// The link intrinsic scope is a HARD partition: a link attribute literally
    /// keyed `spanID` lands under `scope='link'`, never colliding with the
    /// `link:intrinsic`/`spanID` intrinsic row.
    #[test]
    fn span_link_attribute_named_like_an_intrinsic_stays_in_the_link_scope() {
        let mut s = valid_span();
        s.links = vec![Link {
            trace_id: vec![0xaa; 16],
            span_id: vec![0xbb; 8],
            trace_state: String::new(),
            attributes: vec![kv("spanID", Value::StringValue("shadow".to_string()))],
            dropped_attributes_count: 0,
            flags: 0,
        }];
        let out =
            parse(&request_with(None, None, vec![s]), 0).expect("within the expansion budget");
        // The intrinsic `spanID` row (link:intrinsic) and the sender's `spanID`
        // attribute (link) are two distinct rows separated by scope.
        let span_id_rows: Vec<(&str, &str)> = out
            .attrs
            .iter()
            .filter(|a| a.key == "spanID")
            .map(|a| (a.scope.as_str(), a.val.as_str()))
            .collect();
        assert_eq!(
            span_id_rows,
            vec![("link:intrinsic", "bbbbbbbbbbbbbbbb"), ("link", "shadow")]
        );
    }

    /// A span carrying no links yields no `link`/`link:intrinsic` rows.
    #[test]
    fn parse_absent_links_leave_no_link_rows() {
        let out = parse(&request_with(None, None, vec![valid_span()]), 0)
            .expect("within the expansion budget");
        assert!(
            out.attrs
                .iter()
                .all(|a| a.scope != "link" && a.scope != "link:intrinsic")
        );
    }

    #[test]
    fn parse_val_num_excludes_non_finite_and_non_numeric_parses() {
        let mut s = valid_span();
        s.attributes = vec![
            kv("a", Value::StringValue("inf".to_string())),
            kv("b", Value::StringValue("NaN".to_string())),
            kv("c", Value::StringValue("1.5".to_string())),
            kv("d", Value::DoubleValue(2.5)),
        ];
        let out =
            parse(&request_with(None, None, vec![s]), 0).expect("within the expansion budget");
        let by_key: Vec<Option<f64>> = out.attrs.iter().map(|a| a.val_num).collect();
        assert_eq!(by_key, vec![None, None, Some(1.5), Some(2.5)]);
    }

    // -- payload contract -------------------------------------------------

    #[test]
    fn payload_is_a_self_contained_single_resource_spans_traces_data() {
        let scope = InstrumentationScope {
            name: "my-scope".to_string(),
            version: "1.0.0".to_string(),
            attributes: vec![],
            dropped_attributes_count: 0,
        };
        let out = parse(
            &request_with(
                Some(checkout_resource()),
                Some(scope.clone()),
                vec![valid_span()],
            ),
            0,
        )
        .expect("within the expansion budget");
        let payload = TracesData::decode(out.spans[0].payload.as_slice()).expect("payload decodes");
        assert_eq!(payload.resource_spans.len(), 1);
        let rs = &payload.resource_spans[0];
        assert_eq!(rs.resource, Some(checkout_resource()));
        assert_eq!(rs.schema_url, "https://example.com/resource-schema");
        assert_eq!(rs.scope_spans.len(), 1);
        let ss = &rs.scope_spans[0];
        assert_eq!(ss.scope, Some(scope));
        assert_eq!(ss.schema_url, "https://example.com/scope-schema");
        assert_eq!(ss.spans, vec![valid_span()]);
    }

    // -- expansion budget --------------------------------------------------

    /// Issue #54 code-review [high] fix: a small wire body whose resource ×
    /// span fan-out describes an over-budget expansion is rejected as a
    /// whole-request structural failure BEFORE the expansion is
    /// materialized — no partial output survives, and the error is the
    /// `OversizeMessage` class the handler maps to 400/code 3. The crafted
    /// request is ~1 MiB on the wire (one 1 MiB resource attribute) but
    /// its per-span payload duplication alone estimates past
    /// [`MAX_EXPANDED_BYTES`] within the first few hundred spans.
    #[test]
    fn expansion_budget_rejects_a_pathological_resource_by_span_fan_out() {
        let big_value = "v".repeat(1024 * 1024); // 1 MiB resource attr value
        let resource = Resource {
            attributes: vec![kv("big.attr", Value::StringValue(big_value))],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };
        // Enough spans that spans × ~1 MiB payload duplication exceeds the
        // budget with certainty, derived from the constant rather than
        // hard-coded so a budget retune cannot silently weaken this test.
        let span_count = MAX_EXPANDED_BYTES / (1024 * 1024) + 2;
        let spans: Vec<Span> = (0..span_count).map(|_| valid_span()).collect();

        let err = parse(&request_with(Some(resource), None, spans), 0)
            .expect_err("pathological fan-out must trip the expansion budget");
        assert!(
            matches!(
                err,
                LogsIngestError::OversizeMessage { limit, actual, .. }
                    if limit == MAX_EXPANDED_BYTES && actual > MAX_EXPANDED_BYTES
            ),
            "unexpected error: {err}"
        );
    }

    /// Issue #54 code-review round 2 [high] fix: an escape-dense
    /// array-kind resource attribute (NUL bytes render as 6-byte `\u0000`
    /// JSON escapes) must be charged at [`MAX_JSON_ESCAPE_FACTOR`] × its
    /// wire length. This test proves the fix two-sidedly: it recomputes
    /// the round-1 1×-wire proxy for the exact crafted request and asserts
    /// that proxy stays UNDER the budget (i.e. round 1 would have admitted
    /// it, then materialized ~7 MiB/span ≈ 0.6 GiB of rendered vals +
    /// payloads), while the multiplied charge trips `parse` before any
    /// materialization. Span count and the under-budget bound both derive
    /// from the constants, so a budget retune cannot silently break either
    /// side.
    #[test]
    fn expansion_budget_charges_escape_dense_array_attributes_at_worst_case() {
        const MIB: usize = 1024 * 1024;
        // One array attribute wrapping a 1 MiB NUL-dense string: ~1 MiB on
        // the wire, ~6 MiB once JSON-rendered into `val`.
        let nul_dense = "\0".repeat(MIB);
        let resource = Resource {
            attributes: vec![kv(
                "escape.bomb",
                Value::ArrayValue(ArrayValue {
                    values: vec![AnyValue {
                        value: Some(Value::StringValue(nul_dense)),
                    }],
                }),
            )],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };
        // Old per-span charge ~2 MiB (payload_base ~1 MiB + attr 1× ~1 MiB);
        // new per-span charge ~7 MiB. One third of budget/old-charge keeps
        // the 1× proxy comfortably under budget while 6× trips.
        let attr_wire = resource.attributes[0].encoded_len();
        let span_count = MAX_EXPANDED_BYTES / (3 * attr_wire);
        let spans: Vec<Span> = (0..span_count).map(|_| valid_span()).collect();
        let req = request_with(Some(resource.clone()), None, spans);

        // Side 1: the round-1 proxy (1 × encoded_len for every attr kind)
        // over the exact same request stays under the budget — the old
        // estimate would have admitted this request.
        let rs = &req.resource_spans[0];
        let ss = &rs.scope_spans[0];
        let payload_base = resource.encoded_len()
            + rs.schema_url.len()
            + ss.schema_url.len()
            + PAYLOAD_ENVELOPE_OVERHEAD;
        let one_x_per_span = SPAN_ROW_OVERHEAD
            + valid_span().name.len()
            + payload_base
            + valid_span().encoded_len()
            + ATTR_ROW_OVERHEAD
            + attr_wire;
        assert!(
            one_x_per_span * span_count <= MAX_EXPANDED_BYTES,
            "precondition: the round-1 1x-wire proxy must admit this request \
             ({} <= {MAX_EXPANDED_BYTES}) or this test proves nothing",
            one_x_per_span * span_count
        );

        // Side 2: the worst-case-multiplied charge trips before any
        // materialization.
        let err =
            parse(&req, 0).expect_err("escape-dense array fan-out must trip the multiplied budget");
        assert!(
            matches!(
                err,
                LogsIngestError::OversizeMessage { limit, actual, .. }
                    if limit == MAX_EXPANDED_BYTES && actual > MAX_EXPANDED_BYTES
            ),
            "unexpected error: {err}"
        );
    }

    /// Issue #192 PR-B: span events are multiplicative in span output
    /// (0..N per span, each fanning out into intrinsic + attribute index
    /// rows), so their materialization must be charged BEFORE it happens.
    /// A single span with an over-budget event fan-out (each event carrying
    /// an escape-dense array attribute charged at [`MAX_JSON_ESCAPE_FACTOR`]×)
    /// trips the `OversizeMessage` guard rather than exhausting memory. Event
    /// count derives from the budget constants, so a retune cannot silently
    /// weaken it.
    #[test]
    fn expansion_budget_rejects_a_pathological_span_event_fan_out() {
        const MIB: usize = 1024 * 1024;
        let nul_dense = "\0".repeat(MIB); // ~6 MiB once JSON-rendered
        let event_attr = kv(
            "evt.bomb",
            Value::ArrayValue(ArrayValue {
                values: vec![AnyValue {
                    value: Some(Value::StringValue(nul_dense)),
                }],
            }),
        );
        let attr_wire = event_attr.encoded_len();
        let event = Event {
            time_unix_nano: 1_700_000_000_500_000_000,
            name: "exception".to_string(),
            attributes: vec![event_attr],
            dropped_attributes_count: 0,
        };
        // Per-event charge is dominated by the 6× attr rendering (~6 MiB);
        // enough events on ONE span to exceed the budget with certainty.
        let event_count = MAX_EXPANDED_BYTES / (6 * attr_wire) + 2;
        let mut s = valid_span();
        s.events = (0..event_count).map(|_| event.clone()).collect();

        let err = parse(&request_with(None, None, vec![s]), 0)
            .expect_err("pathological event fan-out must trip the expansion budget");
        assert!(
            matches!(
                err,
                LogsIngestError::OversizeMessage { limit, actual, .. }
                    if limit == MAX_EXPANDED_BYTES && actual > MAX_EXPANDED_BYTES
            ),
            "unexpected error: {err}"
        );
    }

    /// Issue #192 PR-C: span links are multiplicative in span output exactly
    /// like events (0..N per span, each fanning out into intrinsic + attribute
    /// index rows), so their materialization must be charged BEFORE it happens.
    /// A single span with an over-budget link fan-out (each link carrying an
    /// escape-dense array attribute charged at [`MAX_JSON_ESCAPE_FACTOR`]×)
    /// trips the `OversizeMessage` guard rather than exhausting memory. Link
    /// count derives from the budget constants, so a retune cannot silently
    /// weaken it.
    #[test]
    fn expansion_budget_rejects_a_pathological_span_link_fan_out() {
        const MIB: usize = 1024 * 1024;
        let nul_dense = "\0".repeat(MIB); // ~6 MiB once JSON-rendered
        let link_attr = kv(
            "link.bomb",
            Value::ArrayValue(ArrayValue {
                values: vec![AnyValue {
                    value: Some(Value::StringValue(nul_dense)),
                }],
            }),
        );
        let attr_wire = link_attr.encoded_len();
        let link = Link {
            trace_id: vec![0x11; 16],
            span_id: vec![0x22; 8],
            trace_state: String::new(),
            attributes: vec![link_attr],
            dropped_attributes_count: 0,
            flags: 0,
        };
        // Per-link charge is dominated by the 6× attr rendering (~6 MiB);
        // enough links on ONE span to exceed the budget with certainty.
        let link_count = MAX_EXPANDED_BYTES / (6 * attr_wire) + 2;
        let mut s = valid_span();
        s.links = (0..link_count).map(|_| link.clone()).collect();

        let err = parse(&request_with(None, None, vec![s]), 0)
            .expect_err("pathological link fan-out must trip the expansion budget");
        assert!(
            matches!(
                err,
                LogsIngestError::OversizeMessage { limit, actual, .. }
                    if limit == MAX_EXPANDED_BYTES && actual > MAX_EXPANDED_BYTES
            ),
            "unexpected error: {err}"
        );
    }

    /// Issue #54 code-review round 3 [high] fix: rendering the promoted
    /// `service` column is itself a guarded materialization — an
    /// escape-dense array-kind `service.name` must be charged and admitted
    /// BEFORE it is rendered, once per `ResourceSpans` block, spans or no
    /// spans. Two-sided proof: the request carries ZERO spans, so no
    /// per-span charge exists anywhere — under the round-2 order (render
    /// first, charge only inside `parse_span`) this request was admitted
    /// as an empty `Ok` after freely rendering every block's ~6x-expanded
    /// service string; under check-then-render the cumulative pre-render
    /// charges alone must trip. Block count derives from the charge
    /// constants (retune-proof).
    #[test]
    fn expansion_budget_charges_service_rendering_before_resolving_it() {
        const MIB: usize = 1024 * 1024;
        // `service.name` as an array wrapping a 1 MiB NUL-dense string:
        // ~1 MiB on the wire, ~6 MiB the moment it is rendered.
        let service_value = Value::ArrayValue(ArrayValue {
            values: vec![AnyValue {
                value: Some(Value::StringValue("\0".repeat(MIB))),
            }],
        });
        let resource = Resource {
            attributes: vec![kv("service.name", service_value)],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };
        let per_block_charge = attr_budget_charge(&resource.attributes[0]);
        let block_count = MAX_EXPANDED_BYTES / per_block_charge + 2;
        let req = ExportTraceServiceRequest {
            resource_spans: (0..block_count)
                .map(|_| ResourceSpans {
                    resource: Some(resource.clone()),
                    // Deliberately span-less: the only materialization in
                    // this request is the per-block service rendering.
                    scope_spans: vec![],
                    schema_url: String::new(),
                })
                .collect(),
        };

        // Side 1: zero spans anywhere — the round-2 order had no charge
        // site left to trip, so it admitted this request (rendering every
        // block's service first).
        assert!(
            req.resource_spans
                .iter()
                .all(|rs| rs.scope_spans.iter().all(|ss| ss.spans.is_empty())),
            "precondition: span-less request, or this test proves nothing about \
             the pre-render charge"
        );

        // Side 2: the pre-render charges alone trip the budget.
        let err = parse(&req, 0)
            .expect_err("escape-dense service.name fan-out must trip before rendering");
        assert!(
            matches!(
                err,
                LogsIngestError::OversizeMessage { limit, actual, .. }
                    if limit == MAX_EXPANDED_BYTES && actual > MAX_EXPANDED_BYTES
            ),
            "unexpected error: {err}"
        );
    }

    /// Issue #54 code-review round 4 [high] fix: rejection-message
    /// construction happens before any budget charge, so it must never
    /// materialize unbounded untrusted content. The reviewer's exact
    /// construction — a near-cap escape-dense `span.name` (32 MiB of
    /// 0x01 bytes, each Debug-escaping to a 6-byte `\u{1}`) on a span with
    /// an invalid `trace_id` — previously retained a ~192 MiB Debug render
    /// in `rejected_message`, uncharged. The assertion bound derives from
    /// [`DIAG_SNIPPET_MAX_BYTES`] (x10 covers the worst per-byte
    /// `escape_debug` expansion, +256 the fixed message prefix/marker), so
    /// a cap retune cannot silently weaken it.
    #[test]
    fn rejection_message_is_bounded_for_an_escape_dense_span_name() {
        let mut bad = valid_span();
        bad.name = "\u{1}".repeat(32 * 1024 * 1024); // near-body-cap, escape-dense
        bad.trace_id = vec![1; 15]; // invalid: triggers the rejection path
        let out = parse(&request_with(None, None, vec![bad]), 0)
            .expect("a rejected span is partial success, not a whole-request error");

        assert_eq!(out.rejected, 1);
        assert!(out.spans.is_empty());
        assert!(out.attrs.is_empty());
        let msg = out.rejected_message.expect("rejection message present");
        assert!(
            msg.len() <= DIAG_SNIPPET_MAX_BYTES * 10 + 256,
            "rejection message must be bounded by the snippet cap, got {} bytes",
            msg.len()
        );
        assert!(
            msg.contains("bytes truncated"),
            "over-cap input must be visibly truncated: {msg:?}"
        );
        assert!(
            msg.contains("trace_id"),
            "still names the violation: {msg:?}"
        );
    }

    /// [`diag_snippet`]'s own contract: short input passes through
    /// borrowed (no allocation); over-cap input truncates on a `char`
    /// boundary (never splits a code point) and names the elided count.
    #[test]
    fn diag_snippet_truncates_on_char_boundaries_and_borrows_short_input() {
        let short = "ordinary-span-name";
        assert!(matches!(
            diag_snippet(short, DIAG_SNIPPET_MAX_BYTES),
            Cow::Borrowed(s) if s == short
        ));

        // 4-byte code points straddling the cap: 127 % 4 != 0, so a naive
        // byte slice at the cap would panic mid-code-point.
        let emoji = "\u{1F600}".to_string().repeat(64); // 256 bytes
        let snipped = diag_snippet(&emoji, 127);
        assert!(snipped.len() < emoji.len());
        assert!(snipped.contains("bytes truncated"));
        // Truncated at 124 (the last 4-byte boundary <= 127): 132 bytes
        // elided.
        assert!(snipped.starts_with(&"\u{1F600}".to_string().repeat(31)));
    }

    /// [`attr_budget_charge`]'s per-kind multipliers, pinned directly:
    /// array/kvlist at [`MAX_JSON_ESCAPE_FACTOR`]×, bytes at
    /// [`BASE64_EXPANSION_FACTOR`]×, strings/scalars at 1×.
    #[test]
    fn attr_budget_charge_multiplies_rendered_expanding_kinds_only() {
        let string_kv = kv("k", Value::StringValue("plain".to_string()));
        assert_eq!(attr_budget_charge(&string_kv), string_kv.encoded_len());

        let int_kv = kv("k", Value::IntValue(42));
        assert_eq!(attr_budget_charge(&int_kv), int_kv.encoded_len());

        let array_kv = kv(
            "k",
            Value::ArrayValue(ArrayValue {
                values: vec![AnyValue {
                    value: Some(Value::StringValue("x".to_string())),
                }],
            }),
        );
        assert_eq!(
            attr_budget_charge(&array_kv),
            array_kv.encoded_len() * MAX_JSON_ESCAPE_FACTOR
        );

        let kvlist_kv = kv(
            "k",
            Value::KvlistValue(KeyValueList {
                values: vec![kv("nested", Value::StringValue("v".to_string()))],
            }),
        );
        assert_eq!(
            attr_budget_charge(&kvlist_kv),
            kvlist_kv.encoded_len() * MAX_JSON_ESCAPE_FACTOR
        );

        let bytes_kv = kv("k", Value::BytesValue(vec![0xFF; 9]));
        assert_eq!(
            attr_budget_charge(&bytes_kv),
            bytes_kv.encoded_len() * BASE64_EXPANSION_FACTOR
        );
    }

    /// The budget is a whole-request bound, not a per-span truncation: a
    /// request comfortably inside it (the ordinary fixtures above) parses
    /// `Ok` — pinned here explicitly against the same code path.
    #[test]
    fn expansion_budget_admits_an_ordinary_request() {
        let out = parse(
            &request_with(Some(checkout_resource()), None, vec![valid_span()]),
            0,
        );
        assert!(out.is_ok());
    }

    /// Two spans under one scope: each payload carries ONLY its own span
    /// (independently decodable, concatenable by T3) — never the sibling.
    #[test]
    fn each_spans_payload_carries_only_that_span() {
        let mut second = span(vec![9; 16], vec![8; 8]);
        second.name = "op-b".to_string();
        let out = parse(
            &request_with(None, None, vec![valid_span(), second.clone()]),
            0,
        )
        .expect("within the expansion budget");
        assert_eq!(out.spans.len(), 2);
        let payload_b =
            TracesData::decode(out.spans[1].payload.as_slice()).expect("payload decodes");
        assert_eq!(
            payload_b.resource_spans[0].scope_spans[0].spans,
            vec![second]
        );
    }

    // -- AnyValue recursion-depth guard (finding #54) --------------------

    /// The inner `Value` of an `AnyValue` tree `levels` nodes deep (a scalar
    /// leaf wrapped in `levels - 1` `ArrayValue` containers). The `kv` helper
    /// wraps it back into the depth-1 root `AnyValue`, so the resulting
    /// attribute value tree is exactly `levels` `AnyValue` nodes deep. Built
    /// iteratively; used only at `levels <= MAX_ANYVALUE_DEPTH + 1`, so its
    /// `Drop` recursion is trivially safe.
    fn deep_value(levels: usize) -> Value {
        let mut value = AnyValue {
            value: Some(Value::StringValue("leaf".to_string())),
        };
        for _ in 1..levels {
            value = AnyValue {
                value: Some(Value::ArrayValue(ArrayValue {
                    values: vec![value],
                })),
            };
        }
        value.value.expect("nested value is present")
    }

    fn span_with_deep_attr(levels: usize) -> Span {
        let mut span = valid_span();
        span.attributes = vec![kv("deep", deep_value(levels))];
        span
    }

    #[test]
    fn parse_accepts_span_attribute_nesting_at_the_depth_cap() {
        let req = request_with(
            None,
            None,
            vec![span_with_deep_attr(
                crate::protocols::otlp_depth::MAX_ANYVALUE_DEPTH,
            )],
        );
        let out = parse(&req, 0).expect("at-cap span attribute is within the depth guard");
        assert_eq!(out.spans.len(), 1);
    }

    #[test]
    fn parse_rejects_span_attribute_nesting_past_the_depth_cap() {
        // One container level deeper than the accepted case above — WITHOUT
        // the guard this parses identically (renders and yields one span);
        // the guard makes it a whole-request reject before any row is
        // materialized, proving the reject is non-vacuous.
        let req = request_with(
            None,
            None,
            vec![span_with_deep_attr(
                crate::protocols::otlp_depth::MAX_ANYVALUE_DEPTH + 1,
            )],
        );
        let err = parse(&req, 0).expect_err("over-depth span attribute is rejected whole-request");
        assert!(matches!(err, LogsIngestError::OversizeMessage { .. }));
    }

    /// Issue #475: every scope constant this module declares is
    /// REACHABLE from parse output.
    ///
    /// What this proves that `trace_scope_vocabulary.rs` cannot: that
    /// each declared constant is actually emitted, not merely declared.
    /// What that gate proves that this cannot: that the string VALUES
    /// match the reader's. Neither substitutes for the other, and this
    /// one is deliberately written against the constants rather than a
    /// second literal list, so it is a reachability check and not a
    /// spelling check.
    #[test]
    fn every_declared_scope_constant_is_emitted_by_parse() {
        let mut span = valid_span();
        span.attributes = vec![kv("status", Value::StringValue("degraded".to_string()))];
        span.events = vec![Event {
            time_unix_nano: 1_700_000_000_005_000_000,
            name: "exception".to_string(),
            attributes: vec![kv("kind", Value::StringValue("retry".to_string()))],
            dropped_attributes_count: 0,
        }];
        span.links = vec![Link {
            trace_id: vec![7; 16],
            span_id: vec![9; 8],
            trace_state: String::new(),
            attributes: vec![kv(
                "spanID",
                Value::StringValue("from-attribute".to_string()),
            )],
            dropped_attributes_count: 0,
            flags: 0,
        }];
        let scope = InstrumentationScope {
            name: "checkout-lib".to_string(),
            version: "1.0.0".to_string(),
            attributes: vec![kv("sdk", Value::StringValue("rust".to_string()))],
            dropped_attributes_count: 0,
        };
        let req = request_with(Some(checkout_resource()), Some(scope), vec![span]);

        let parsed = parse(&req, 0).expect("the corpus parses");
        let mut emitted: Vec<&str> = parsed.attrs.iter().map(|a| a.scope.as_str()).collect();
        emitted.sort_unstable();
        emitted.dedup();

        let mut declared = [
            SCOPE_EVENT,
            SCOPE_EVENT_INTRINSIC,
            SCOPE_INSTRUMENTATION,
            SCOPE_LINK,
            SCOPE_LINK_INTRINSIC,
            SCOPE_RESOURCE,
            SCOPE_SPAN,
        ];
        declared.sort_unstable();
        assert_eq!(emitted, declared, "every declared scope must be reachable");
    }

    // -- issue #476: the stored OTLP type ---------------------------------

    /// AC3 — the ORDERED `(key, val_type)` list for one attribute of every
    /// OTLP `AnyValue` kind, plus the adversarial string cases the issue
    /// names: a string whose text is digits, a duration literal, a boolean
    /// word, a float, and the empty string.
    ///
    /// This is a PERMUTATION break, not a rename check. The expected list
    /// pairs each key with its own spelling, so swapping two arms of
    /// `AttrValueType::as_str` (`Int` <-> `Float`, say) moves `port` to
    /// `float` and `cpu` to `int` and fails here. A test that asserted only
    /// that the SET `{string,int,float,bool}` appears would pass under that
    /// swap and must not be written.
    #[test]
    fn every_otlp_value_kind_stores_its_own_type_spelling() {
        let mut s = valid_span();
        s.attributes = vec![
            // Strings whose TEXT reads as something else — the whole point
            // of the column. Before it, `build` was reported `int`,
            // `timeout` `duration`, `enabled` `bool` and `ratio` `float`.
            kv("build", Value::StringValue("007".to_string())),
            kv("timeout", Value::StringValue("2s".to_string())),
            kv("enabled", Value::StringValue("true".to_string())),
            kv("ratio", Value::StringValue("1.5".to_string())),
            kv("note", Value::StringValue(String::new())),
            // Genuine scalars.
            kv("status_code", Value::IntValue(500)),
            kv("sampled", Value::BoolValue(true)),
            kv("cpu", Value::DoubleValue(1.5)),
            // Composite values RENDER to a string, so `string` is the truth
            // about what we stored.
            kv(
                "tags",
                Value::ArrayValue(ArrayValue {
                    values: vec![AnyValue {
                        value: Some(Value::StringValue("a".to_string())),
                    }],
                }),
            ),
            kv(
                "meta",
                Value::KvlistValue(KeyValueList {
                    values: vec![kv("k", Value::StringValue("v".to_string()))],
                }),
            ),
            kv("blob", Value::BytesValue(vec![0xDE, 0xAD])),
        ];
        // An attribute carrying NO value at all renders `""` and is a
        // string; it cannot be spelled through `kv`.
        s.attributes.push(KeyValue {
            key: "absent".to_string(),
            value: None,
            key_strindex: 0,
        });
        let out =
            parse(&request_with(None, None, vec![s]), 0).expect("within the expansion budget");
        let got: Vec<(&str, &str)> = out
            .attrs
            .iter()
            .map(|a| (a.key.as_str(), a.val_type.as_str()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("build", "string"),
                ("timeout", "string"),
                ("enabled", "string"),
                ("ratio", "string"),
                ("note", "string"),
                ("status_code", "int"),
                ("sampled", "bool"),
                ("cpu", "float"),
                ("tags", "string"),
                ("meta", "string"),
                ("blob", "string"),
                ("absent", "string"),
            ]
        );
    }

    /// The type is NOT a function of the rendered text: a string `"1.5"`
    /// and a double `1.5` store identical `val` bytes and different types.
    /// This is the pair no read-side rule could ever separate, which is
    /// why the column exists.
    #[test]
    fn a_string_and_a_double_with_identical_text_store_different_types() {
        let mut s = valid_span();
        s.attributes = vec![
            kv("as_text", Value::StringValue("1.5".to_string())),
            kv("as_double", Value::DoubleValue(1.5)),
        ];
        let out =
            parse(&request_with(None, None, vec![s]), 0).expect("within the expansion budget");
        let rows: Vec<(&str, &str, &str)> = out
            .attrs
            .iter()
            .map(|a| (a.key.as_str(), a.val.as_str(), a.val_type.as_str()))
            .collect();
        assert_eq!(
            rows,
            vec![("as_text", "1.5", "string"), ("as_double", "1.5", "float")]
        );
        // `val_num` is populated for BOTH — it is a parse of the text, so
        // it cannot tell them apart. Asserted so the next reader does not
        // reach for it as a type source (issue #493 owns its own limits).
        assert_eq!(out.attrs[0].val_num, Some(1.5));
        assert_eq!(out.attrs[1].val_num, Some(1.5));
    }

    /// The writer-synthesised intrinsic rows carry a type too: an event
    /// name and the two link hex ids are strings, `timeSinceStart` is the
    /// integer nanosecond count.
    #[test]
    fn writer_synthesised_intrinsic_rows_carry_their_own_types() {
        let mut s = valid_span();
        s.events = vec![Event {
            time_unix_nano: 1_700_000_000_500_000_000,
            name: "cache.miss".to_string(),
            attributes: vec![],
            dropped_attributes_count: 0,
        }];
        s.links = vec![Link {
            trace_id: vec![0x0a; 16],
            span_id: vec![0x0b; 8],
            trace_state: String::new(),
            attributes: vec![],
            dropped_attributes_count: 0,
            flags: 0,
        }];
        let out =
            parse(&request_with(None, None, vec![s]), 0).expect("within the expansion budget");
        let rows: Vec<(&str, &str, &str)> = out
            .attrs
            .iter()
            .map(|a| (a.scope.as_str(), a.key.as_str(), a.val_type.as_str()))
            .collect();
        assert_eq!(
            rows,
            vec![
                ("event:intrinsic", "name", "string"),
                ("event:intrinsic", "timeSinceStart", "int"),
                ("link:intrinsic", "spanID", "string"),
                ("link:intrinsic", "traceID", "string"),
            ]
        );
    }

    // -- the admission charge counts the span-row arrays (issue #556) ----

    /// The request the byte figures on issue #556 are taken on. Fully
    /// determined by this text: no helper supplies an attribute.
    fn metered_request(long_renderings: bool) -> ExportTraceServiceRequest {
        let mut sp = Span {
            trace_id: vec![1; 16],
            span_id: vec![2; 8],
            name: "op-a".to_string(),
            kind: SpanKind::Server as i32,
            start_time_unix_nano: 1_700_000_000_000_000_000,
            end_time_unix_nano: 1_700_000_001_000_000_000,
            ..Default::default()
        };
        sp.attributes = vec![
            kv("http.status_code", Value::IntValue(500)),
            kv("http.method", Value::StringValue("GET".into())),
        ];
        if long_renderings {
            sp.attributes.push(kv("d", Value::DoubleValue(f64::MIN)));
            sp.attributes.push(kv("i", Value::IntValue(i64::MIN)));
            sp.attributes.push(kv("b", Value::BoolValue(true)));
        }
        sp.events = vec![Event {
            time_unix_nano: 1_700_000_000_003_000_000,
            name: "exception".into(),
            attributes: vec![kv("exception.type", Value::StringValue("IOError".into()))],
            dropped_attributes_count: 0,
        }];
        sp.links = vec![Link {
            trace_id: vec![9; 16],
            span_id: vec![8; 8],
            attributes: vec![kv("rel", Value::StringValue("child_of".into()))],
            ..Default::default()
        }];
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", Value::StringValue("checkout".into()))],
                    dropped_attributes_count: 0,
                    ..Default::default()
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![sp],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    fn admission_charge(req: &ExportTraceServiceRequest) -> usize {
        req.resource_spans
            .iter()
            .map(resource_spans_expansion_charge)
            .sum()
    }

    fn queue_reservation(req: &ExportTraceServiceRequest) -> u64 {
        let out = parse(req, 0).expect("within the expansion budget");
        out.spans
            .iter()
            .map(crate::writer::rows::TraceSpanRow::est_source_bytes)
            .sum::<u64>()
            + out
                .attrs
                .iter()
                .map(crate::writer::rows::TraceAttrRow::est_source_bytes)
                .sum::<u64>()
    }

    /// One more attribute costs the admission charge at least one more
    /// array element (issue #556). The charge is compared with ITSELF over
    /// two requests, so the figure it must clear is not one this test
    /// computes.
    #[test]
    fn one_more_attribute_charges_at_least_one_more_array_element() {
        let mut three = metered_request(false);
        let mut four = metered_request(false);
        three.resource_spans[0].scope_spans[0].spans[0]
            .attributes
            .push(kv("a", Value::StringValue("x".into())));
        four.resource_spans[0].scope_spans[0].spans[0]
            .attributes
            .push(kv("a", Value::StringValue("x".into())));
        four.resource_spans[0].scope_spans[0].spans[0]
            .attributes
            .push(kv("b", Value::StringValue("y".into())));
        let delta = admission_charge(&four) - admission_charge(&three);
        assert!(
            delta >= ATTR_ARRAY_ELEMENT_OVERHEAD,
            "one more attribute must charge at least one array element \
             ({ATTR_ARRAY_ELEMENT_OVERHEAD}); it charged {delta}"
        );
    }

    /// On an ordinary span the coarse admission charge covers the exact
    /// queue reservation — two numbers from two functions over two shapes,
    /// so neither produces the other.
    ///
    /// Asserted on `metered_request(false)` only. It does NOT hold in
    /// general: on values whose rendering is far longer than their wire
    /// bytes it fails at the BASE, before these arrays exist, because
    /// `attr_budget_charge` measures the protobuf length while the queue
    /// reserves the RENDERED string. That gap predates issue #556 and is
    /// not closed by it, so asserting the relation generally would pin a
    /// defect that is not this change's.
    #[test]
    fn the_admission_charge_covers_the_queue_reservation_on_an_ordinary_span() {
        let req = metered_request(false);
        let charge = admission_charge(&req) as u64;
        let reservation = queue_reservation(&req);
        assert!(
            charge >= reservation,
            "the admission charge ({charge}) must cover the queue reservation ({reservation})"
        );
    }
}

// === The landing-shaped decode (issues #584 to #586) ======================
//
// [`parse`] above is the OLD two-table path's walk and is untouched. This
// one produces [`ParsedTraceLanding`], which carries what the landing path
// stores and the old path keeps inside the span's protobuf `payload` blob
// instead: the events, the links, the scope's attributes, the resource's
// attributes, the trace state, the flags and the four dropped counts.
//
// The two walks share this module's depth guard, its expansion budget and
// its value renderer, and nothing else. **Neither path's behaviour depends
// on the other's**: a change to one walk cannot move the other's rows.

/// The reserved key the resource-identity buffer appends the schema url
/// under.
///
/// **A single `0xFF` byte, which no OTLP key can spell.** An OTLP key is a
/// protobuf `string` and so valid UTF-8, and `0xFF` is not a valid byte in
/// any UTF-8 sequence — so no sender can construct an attribute whose key
/// collides with this one. It is also the buffer's own separator, which
/// costs nothing for the same reason: a real key contains no `0xFF`, so the
/// reserved pair cannot be read as part of one.
const IDENTITY_SCHEMA_URL_KEY: &[u8] = &[0xFF];

/// The separator the identity buffer uses, which is
/// `pulsus_model::build_stream_buffer`'s own.
const IDENTITY_SEP: u8 = 0xFF;

/// The one-byte type tag the identity buffer puts before a value, so an
/// integer `1` and the string `"1"` are different resources.
///
/// One tag per arm of the `AnyValue` oneof, plus `u` for an `AnyValue` with
/// no arm set and for an absent one — the protocol calls both "empty" and
/// the stored attributes cannot tell them apart either.
fn identity_type_tag(value: Option<&Value>) -> u8 {
    match value {
        None => b'u',
        Some(Value::StringValue(_)) => b's',
        Some(Value::BoolValue(_)) => b'b',
        Some(Value::IntValue(_)) => b'i',
        Some(Value::DoubleValue(_)) => b'f',
        Some(Value::ArrayValue(_)) => b'a',
        Some(Value::KvlistValue(_)) => b'm',
        Some(Value::BytesValue(_)) => b'y',
        Some(Value::StringValueStrindex(_)) => b'x',
    }
}

/// Appends a length and then the bytes it counts, so a field whose width
/// the type does not fix cannot run into its neighbour.
fn identity_push_len_prefixed(bytes: &[u8], buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    buf.extend_from_slice(bytes);
}

/// Appends one value to the identity buffer: **its type tag, then its own
/// bytes, at every depth**.
///
/// The shapes that can nest are the protocol's, not a chosen few:
/// `AnyValue.value` is an optional oneof of eight arms, and exactly two of
/// them carry further `AnyValue`s — `ArrayValue.values`, and
/// `KeyValueList.values`' `KeyValue.value`. So this walks those two arms
/// and tags what it finds, which makes the encoding injective over the
/// whole value: the arms whose width the type does not fix carry a length,
/// a container carries its element count, and every element and every
/// kvlist entry's value carries its own tag.
///
/// **Rendering the value to text instead collapses distinct resources**, at
/// any depth below the first: a bytes value renders as its base64 text and
/// so cannot be told from a string carrying that text, and a non-finite
/// double, a profiling string reference and an unset value all render as
/// JSON `null`. `resources` is a `ReplacingMergeTree` keyed on
/// `(service, resource_id)`, so a shared identity means one of the two
/// resources replaces the other and the spans carrying that id join to
/// whichever survived.
///
/// A double is encoded by its bits rather than its text, which keeps `+inf`,
/// `-inf` and a NaN apart from each other and from every finite value.
/// Equal wire values give equal bytes, which is what the identity needs; a
/// finer distinction than the stored column can show costs nothing, because
/// two identities for one stored content are two rows under two keys, while
/// one identity for two contents loses one of them.
///
/// The recursion is bounded by the whole-request depth guard
/// ([`crate::protocols::otlp_depth::MAX_ANYVALUE_DEPTH`], 32), which runs at the top of
/// [`parse_landing`] before any value is encoded — the same bound
/// [`any_value_to_json`] relies on.
fn identity_encode_value(value: Option<&AnyValue>, buf: &mut Vec<u8>) {
    let value = value.and_then(|v| v.value.as_ref());
    buf.push(identity_type_tag(value));
    match value {
        None => {}
        Some(Value::StringValue(s)) => identity_push_len_prefixed(s.as_bytes(), buf),
        Some(Value::BoolValue(b)) => buf.push(u8::from(*b)),
        Some(Value::IntValue(i)) => buf.extend_from_slice(&i.to_le_bytes()),
        Some(Value::DoubleValue(d)) => buf.extend_from_slice(&d.to_bits().to_le_bytes()),
        Some(Value::BytesValue(bytes)) => identity_push_len_prefixed(bytes, buf),
        Some(Value::StringValueStrindex(index)) => buf.extend_from_slice(&index.to_le_bytes()),
        Some(Value::ArrayValue(array)) => {
            buf.extend_from_slice(&(array.values.len() as u64).to_le_bytes());
            for element in &array.values {
                identity_encode_value(Some(element), buf);
            }
        }
        Some(Value::KvlistValue(kvlist)) => {
            buf.extend_from_slice(&(kvlist.values.len() as u64).to_le_bytes());
            for entry in &kvlist.values {
                identity_push_len_prefixed(entry.key.as_bytes(), buf);
                identity_encode_value(entry.value.as_ref(), buf);
            }
        }
    }
}

/// The canonical buffer one resource's identity is taken over.
///
/// **Pairs sorted by key, key-unique, `key ++ 0xFF ++ <encoded value> ++
/// 0xFF`** — `pulsus_model::build_stream_buffer`'s layout, with
/// [`identity_encode_value`] in place of the value's text — then the schema
/// url under [`IDENTITY_SCHEMA_URL_KEY`].
///
/// The encoded value starts with a type tag, which is an ASCII letter, and
/// is self-delimiting from there; a key holds no `0xFF` because it is valid
/// UTF-8. So the buffer can be read back as the pairs it was built from,
/// which is what makes two different resources two buffers.
///
/// Three properties this gives, each with the case that holds it:
///
/// - **The identity does not depend on arrival order**, because the buffer
///   is built from sorted, key-unique pairs. That is the invariant
///   `LabelSet`'s own doc comment states, and an identity taken over the
///   encoded bytes in arrival order breaks it. The sort is stable and the
///   deduplication keeps the earlier pair, so a repeated key keeps its
///   first value here as it does in the stored attributes.
/// - **`service.name` is in the hash.** Only the *stored* `attrs` drops it,
///   because `spans.service` already carries it. Leaving it out of the
///   buffer would make two different services with otherwise equal
///   resources share one `resource_id`, and the damage is on the read side:
///   `resources`' key is `(service, resource_id)`, so the two rows have
///   different keys and do not collapse, while the attribute join compiles
///   to `resource_id IN (SELECT resource_id FROM resources WHERE …)` with
///   no service term.
/// - **A key present with an empty value differs from an absent key**,
///   because a present key contributes its own `key ++ 0xFF ++ tag ++ 0xFF`.
fn resource_identity_buffer(resource: Option<&Resource>, schema_url: &str) -> Vec<u8> {
    let mut pairs: Vec<(&str, Option<&AnyValue>)> = Vec::new();
    if let Some(resource) = resource {
        for kv in &resource.attributes {
            pairs.push((kv.key.as_str(), kv.value.as_ref()));
        }
    }
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    pairs.dedup_by(|a, b| a.0 == b.0);

    let mut buf = Vec::new();
    for (key, value) in &pairs {
        buf.extend_from_slice(key.as_bytes());
        buf.push(IDENTITY_SEP);
        identity_encode_value(*value, &mut buf);
        buf.push(IDENTITY_SEP);
    }
    buf.extend_from_slice(IDENTITY_SCHEMA_URL_KEY);
    buf.push(IDENTITY_SEP);
    buf.extend_from_slice(schema_url.as_bytes());
    buf.push(IDENTITY_SEP);
    buf
}

/// One resource's 128-bit identity.
///
/// **`pulsus_model::compose128`, not a third hash primitive.**
/// `docs/TraceQL/server-implementation.md` §2.2 named `sipHash128` until it
/// was corrected to name this function; that primitive is not in this
/// workspace and no dependency provides it. Issue #498 settled this
/// composition — `cityHash64` in the high half, `xxHash64` seed 0 in the low
/// half, over one canonical buffer — for both label families, so a change to
/// either primitive moves every identity and the golden vectors catch it
/// once. A third primitive for a third identity gives up exactly that.
pub fn resource_identity(resource: Option<&Resource>, schema_url: &str) -> Fingerprint {
    pulsus_model::compose128(&resource_identity_buffer(resource, schema_url))
}

/// What one attribute's value becomes: a stored JSON value at one or more
/// paths, or a key whose value no JSON path can hold.
enum LandedValue {
    /// One path under the attribute's own escaped key.
    One(TraceJsonValue),
    /// A non-empty kvlist: one leaf path per leaf, each already escaped and
    /// prefixed with the attribute's own escaped key. The second element of
    /// each pair is the leaf's **original** dotted key, which is what the
    /// `tag_names` catalog lists.
    Leaves(Vec<(String, String, TraceJsonValue)>),
    /// `attrs_other`: a bytes value, an empty kvlist, an array holding a
    /// bytes or kvlist element, or an `AnyValue` with no arm set. None of
    /// these has a JSON representation this engine can store at a path, so
    /// the key is carried in the protobuf side-channel under its original
    /// OTLP key.
    Other,
    /// **Nowhere.** The attribute lands as though it were not present: no
    /// path in `attrs`, no key in `attrs_other`, no row in `tag_names`, no
    /// row in `tag_values`. Returned for the profiling signal's string
    /// reference — the eighth and last arm of the `AnyValue` oneof — and for
    /// nothing else.
    ///
    /// The proto's own comment on that arm is the authority: a receiver for
    /// a signal other than Profiling should "process the data as if this
    /// value were absent or empty, ignoring its semantic content for the
    /// non-Profiling signal". It offers two readings and this takes
    /// **absent**, because the value is an index into
    /// `ProfilesDictionary.string_table`, which a trace receiver does not
    /// carry (issue #586).
    ///
    /// **Nothing is logged.** The quote's first sentence asks for an error
    /// or a warning; neither signal logs one today, and one line per
    /// attribute on the ingest path is a flood at trace volumes.
    ///
    /// **The resource identity is not touched**: `identity_encode_value`
    /// tags and hashes all eight arms, so two resources differing only in a
    /// dropped attribute hold two ids over one stored content — two rows
    /// under two keys rather than one row losing a content.
    Absent,
}

/// Classifies one OTLP `AnyValue` into [`LandedValue`], applying §4.2's
/// table.
fn land_value(key: &str, value: Option<&AnyValue>) -> LandedValue {
    let Some(value) = value.and_then(|v| v.value.as_ref()) else {
        return LandedValue::Other;
    };
    match value {
        Value::StringValue(s) => {
            LandedValue::One(TraceJsonValue::Scalar(TraceJsonScalar::Str(s.clone())))
        }
        Value::BoolValue(b) => LandedValue::One(TraceJsonValue::Scalar(TraceJsonScalar::Bool(*b))),
        Value::IntValue(i) => LandedValue::One(TraceJsonValue::Scalar(TraceJsonScalar::Int(*i))),
        Value::DoubleValue(d) => {
            LandedValue::One(TraceJsonValue::Scalar(TraceJsonScalar::Double(*d)))
        }
        Value::ArrayValue(array) => land_array(array),
        Value::KvlistValue(kvlist) => {
            if kvlist.values.is_empty() {
                return LandedValue::Other;
            }
            let mut leaves = Vec::new();
            collect_leaves(key, key, kvlist, &mut leaves);
            if leaves.is_empty() {
                LandedValue::Other
            } else {
                LandedValue::Leaves(leaves)
            }
        }
        // A bytes value has no JSON type on this engine, so the key is
        // carried in the protobuf side-channel.
        Value::BytesValue(_) => LandedValue::Other,
        // A profiling string reference lands NOWHERE (issue #586).
        Value::StringValueStrindex(_) => LandedValue::Absent,
    }
}

/// An array's stored form: one typed array where every element is the same
/// scalar arm, `Array(Dynamic)` where the arms are mixed,
/// `Array(Nullable(String))` where it is empty, and `attrs_other` where any
/// element is a bytes value or a kvlist.
fn land_array(array: &ArrayValue) -> LandedValue {
    let mut scalars: Vec<TraceJsonScalar> = Vec::with_capacity(array.values.len());
    for element in &array.values {
        match element.value.as_ref() {
            Some(Value::StringValue(s)) => scalars.push(TraceJsonScalar::Str(s.clone())),
            Some(Value::BoolValue(b)) => scalars.push(TraceJsonScalar::Bool(*b)),
            Some(Value::IntValue(i)) => scalars.push(TraceJsonScalar::Int(*i)),
            Some(Value::DoubleValue(d)) => scalars.push(TraceJsonScalar::Double(*d)),
            // A profiling string reference is skipped, not fatal to the
            // array: the remaining elements land, and an array of nothing
            // else lands as the empty array (issue #586).
            Some(Value::StringValueStrindex(_)) => continue,
            // A nested array, a kvlist, a bytes value or an unset element:
            // the whole attribute goes to `attrs_other`, because a stored
            // array's element type has to be one thing.
            _ => return LandedValue::Other,
        }
    }
    // **After the loop**, so an array whose every element was skipped is the
    // empty array rather than reaching `all_str` — which is vacuously true
    // over no elements and lands `StrArray(vec![])`. The two are one stored
    // value: both serialise to `1e 23 15` and a zero element count.
    if scalars.is_empty() {
        return LandedValue::One(TraceJsonValue::EmptyArray);
    }
    let all_str = scalars.iter().all(|s| matches!(s, TraceJsonScalar::Str(_)));
    let all_bool = scalars
        .iter()
        .all(|s| matches!(s, TraceJsonScalar::Bool(_)));
    let all_int = scalars.iter().all(|s| matches!(s, TraceJsonScalar::Int(_)));
    let all_double = scalars
        .iter()
        .all(|s| matches!(s, TraceJsonScalar::Double(_)));
    if all_str {
        return LandedValue::One(TraceJsonValue::StrArray(
            scalars
                .into_iter()
                .map(|s| match s {
                    TraceJsonScalar::Str(s) => s,
                    _ => unreachable!("checked by all_str"),
                })
                .collect(),
        ));
    }
    if all_bool {
        return LandedValue::One(TraceJsonValue::BoolArray(
            scalars
                .into_iter()
                .map(|s| match s {
                    TraceJsonScalar::Bool(b) => b,
                    _ => unreachable!("checked by all_bool"),
                })
                .collect(),
        ));
    }
    if all_int {
        return LandedValue::One(TraceJsonValue::IntArray(
            scalars
                .into_iter()
                .map(|s| match s {
                    TraceJsonScalar::Int(i) => i,
                    _ => unreachable!("checked by all_int"),
                })
                .collect(),
        ));
    }
    if all_double {
        return LandedValue::One(TraceJsonValue::DoubleArray(
            scalars
                .into_iter()
                .map(|s| match s {
                    TraceJsonScalar::Double(d) => d,
                    _ => unreachable!("checked by all_double"),
                })
                .collect(),
        ));
    }
    LandedValue::One(TraceJsonValue::MixedArray(scalars))
}

/// Walks a kvlist into `(escaped path, original dotted key, value)` leaves.
///
/// **A nested object is dotted leaf paths and nothing else** — the engine
/// stores no value at a parent path, measured: for
/// `CAST('{"k":{"a":{"b":1}}}' AS JSON)`, `dynamicType(attrs.`k`)` answers
/// `None` while `JSONAllPathsWithTypes(attrs)` answers `{'k.a.b':'Int64'}`.
/// A leaf whose own value cannot be stored (a bytes value, an empty kvlist)
/// is simply not a leaf: the composite key is listed in `tag_names` and no
/// value is listed for it, which is the disagreement `docs/api.md` §4.3's
/// two halves carry for a composite key.
fn collect_leaves(
    escaped_prefix: &str,
    original_prefix: &str,
    kvlist: &KeyValueList,
    out: &mut Vec<(String, String, TraceJsonValue)>,
) {
    for entry in &kvlist.values {
        let escaped = format!(
            "{escaped_prefix}.{}",
            crate::writer::trace_json::escape_json_path(&entry.key)
        );
        let original = format!("{original_prefix}.{}", entry.key);
        match entry.value.as_ref().and_then(|v| v.value.as_ref()) {
            Some(Value::KvlistValue(nested)) if !nested.values.is_empty() => {
                collect_leaves(&escaped, &original, nested, out);
            }
            _ => match land_value(&entry.key, entry.value.as_ref()) {
                LandedValue::One(value) => out.push((escaped, original, value)),
                // A leaf whose value goes to `attrs_other`, a leaf that is
                // itself an empty kvlist, and a leaf of the profiling arm all
                // store no path. Inside a kvlist that arm already landed
                // nowhere before issue #586: only `One` is kept here.
                LandedValue::Leaves(_) | LandedValue::Other | LandedValue::Absent => {}
            },
        }
    }
}

/// One attribute scope's landed attributes: the JSON column, the keys and
/// values the catalogs list, and the keys whose values go to `attrs_other`.
struct LandedAttrs {
    json: TraceJson,
    other: Vec<KeyValue>,
}

/// The catalog entries one part of a push contributes, distinct inside
/// themselves.
///
/// **A stage is held back until a span has passed every check.** A
/// resource's, a scope's and a span's entries each get their own, and
/// [`TagStage::absorb`] moves them into the push's accepted sets at the one
/// point a span is known to land. Writing them straight into the accepted
/// sets leaves `tag_names` and `tag_values` rows behind for a push that
/// stores no span — rows for data the store does not hold.
#[derive(Default)]
struct TagStage {
    names: BTreeSet<LandingTagName>,
    values: BTreeSet<LandingTagValue>,
}

impl TagStage {
    /// Takes `staged`'s entries into this stage, which is the push's
    /// accepted set. Both sides are sets, so a resource's or a scope's
    /// entries taken once per landed span land once.
    ///
    /// **Not named `merge`.** `pulsus-server`'s route-inventory guard pins
    /// the whole body of every function whose text contains a `.merge(`
    /// token, and the one call site below is in a decode function that
    /// composes no routes: pinning it would put that body in the route
    /// snapshot and make every later edit to it a re-derivation there.
    fn absorb(&mut self, staged: &TagStage) {
        self.names.extend(staged.names.iter().cloned());
        self.values.extend(staged.values.iter().cloned());
    }
}

/// Lands one attribute list.
///
/// `scope` decides which catalog scope the names and values are recorded
/// under; `stage` is the holding set for the part of the push these
/// attributes belong to, merged into the accepted sets only once a span
/// under them lands.
fn land_attrs(
    attrs: &[KeyValue],
    scope: TagScope,
    skip_key: Option<&str>,
    stage: &mut TagStage,
) -> LandedAttrs {
    let TagStage { names, values } = stage;
    let mut entries: Vec<TraceJsonEntry> = Vec::with_capacity(attrs.len());
    let mut other: Vec<KeyValue> = Vec::new();
    let mut seen: BTreeSet<&str> = BTreeSet::new();

    for kv in attrs {
        // The writer's own duplicate-key rule: the first value of a key
        // wins and the rest are dropped, per scope.
        // `type_json_skip_duplicated_paths` is `0` on this server, so a
        // repeated path in the block is an exception rather than a silent
        // drop, and this has to be the only mechanism.
        if !seen.insert(kv.key.as_str()) {
            continue;
        }
        if Some(kv.key.as_str()) == skip_key {
            continue;
        }
        // **The classifying runs BEFORE the name is listed** (issue #586),
        // because one arm's key is not listed at all: a profiling string
        // reference lands nowhere, catalogs included. One function still
        // does the classifying.
        let landed = land_value(&kv.key, kv.value.as_ref());
        if matches!(landed, LandedValue::Absent) {
            continue;
        }
        // **Every other key the push carries in any of the five scopes gets
        // a kind-2 row**, including a key whose value went to `attrs_other`
        // and a key whose value is a kvlist.
        names.insert(LandingTagName {
            scope,
            key: kv.key.clone(),
        });
        match landed {
            LandedValue::One(value) => {
                record_values(scope, &kv.key, &value, values);
                entries.push(TraceJsonEntry {
                    path: crate::writer::trace_json::escape_json_path(&kv.key),
                    value,
                });
            }
            LandedValue::Leaves(leaves) => {
                // The leaf paths each get their own kind-2 row under their
                // full dotted key, and **no** kind-3 row: a kvlist has no
                // scalar text and no `tag_type`.
                for (path, original, value) in leaves {
                    names.insert(LandingTagName {
                        scope,
                        key: original,
                    });
                    entries.push(TraceJsonEntry { path, value });
                }
            }
            LandedValue::Other => other.push(kv.clone()),
            // Unreachable: the `continue` above takes this arm out.
            LandedValue::Absent => {}
        }
    }

    LandedAttrs {
        json: TraceJson::from_entries(entries),
        other,
    }
}

/// Records the kind-3 rows one landed value produces: one for a scalar and
/// one for each element of a scalar array.
///
/// **No row for a mixed array**: its elements have no one `tag_type`, and
/// an unnarrowed value lookup on such a key answers an empty list while a
/// narrowed one reads the store and answers. That is a decision, not parity
/// with the old `trace_tag_catalog`.
fn record_values(
    scope: TagScope,
    key: &str,
    value: &TraceJsonValue,
    out: &mut BTreeSet<LandingTagValue>,
) {
    let mut push = |scalar: TraceJsonScalar| {
        out.insert(LandingTagValue {
            scope,
            key: key.to_string(),
            value: scalar.render(),
            val_type: scalar.val_type(),
        });
    };
    match value {
        TraceJsonValue::Scalar(s) => push(s.clone()),
        TraceJsonValue::StrArray(v) => {
            for s in v {
                push(TraceJsonScalar::Str(s.clone()));
            }
        }
        TraceJsonValue::IntArray(v) => {
            for i in v {
                push(TraceJsonScalar::Int(*i));
            }
        }
        TraceJsonValue::DoubleArray(v) => {
            for d in v {
                push(TraceJsonScalar::Double(*d));
            }
        }
        TraceJsonValue::BoolArray(v) => {
            for b in v {
                push(TraceJsonScalar::Bool(*b));
            }
        }
        TraceJsonValue::MixedArray(_) | TraceJsonValue::EmptyArray => {}
    }
}

/// The protobuf serialization of an
/// `opentelemetry.proto.common.v1.KeyValueList` carrying `attrs`, or the
/// empty vector when there are none.
///
/// The keys are the **original OTLP keys** rather than escaped paths: they
/// are not JSON paths, and `traces_api/assemble.rs` decodes this back into
/// the OTLP response.
fn encode_attrs_other(attrs: Vec<KeyValue>) -> Vec<u8> {
    if attrs.is_empty() {
        return Vec::new();
    }
    KeyValueList { values: attrs }.encode_to_vec()
}

/// Refuses a value whose stored path count exceeds
/// `format_binary_max_object_size`.
///
/// **The decode gate and the pin carry the same constant**, so neither can
/// admit what the other refuses: without the gate the push is admitted, the
/// insert fails after the block was sent, and the caller gets an uncertain
/// ending for a push that could never have succeeded.
fn check_json_paths(json: &TraceJson) -> Result<(), LogsIngestError> {
    let limit = pulsus_clickhouse::MAX_JSON_PATHS_PER_VALUE as usize;
    if json.len() > limit {
        return Err(LogsIngestError::OversizeMessage {
            field: "stored JSON paths in one trace attribute value",
            limit,
            actual: json.len(),
        });
    }
    Ok(())
}

/// Decodes `req` into the landing path's four landed event shapes.
///
/// `Err` on the same two whole-request structural failures [`parse`] has —
/// the `AnyValue` recursion-depth guard and the [`MAX_EXPANDED_BYTES`]
/// expansion budget — plus one of its own: a value whose stored path count
/// would exceed `format_binary_max_object_size`. Everything else (bad ids,
/// bad timestamps) stays a per-span partial-success rejection inside the
/// `Ok`.
pub fn parse_landing(
    req: &ExportTraceServiceRequest,
    now_ns: i64,
) -> Result<ParsedTraceLanding, LogsIngestError> {
    crate::protocols::otlp_depth::ensure_trace_anyvalue_depth(req)?;

    let mut out = ParsedTraceLanding::default();
    let mut expanded_bytes: usize = 0;
    // The catalog entries of the spans that landed. Every entry reaches it
    // through a [`TagStage`] merged at the one point below where a span has
    // passed every check, the UTC-day check included.
    let mut accepted = TagStage::default();
    // One row per distinct `(resource_id, day)` in the push. The day comes
    // from the spans that resource carried, so a push whose spans straddle
    // midnight emits two rows for one resource.
    let mut resources: BTreeMap<(Fingerprint, u16), LandingResource> = BTreeMap::new();

    for resource_spans in &req.resource_spans {
        let resource = resource_spans.resource.as_ref();
        let service_kv = find_service_kv(resource);
        if let Some(kv) = service_kv {
            charge_budget(&mut expanded_bytes, attr_budget_charge(kv))?;
        }
        let service = service_kv
            .map(|kv| any_value_to_string(kv.value.as_ref()))
            .unwrap_or_default();
        let resource_id = resource_identity(resource, &resource_spans.schema_url);

        // The resource's own landed attributes, built once per
        // `ResourceSpans` block: every span in it shares them, and so do
        // the catalog entries they contribute.
        let mut resource_stage = TagStage::default();
        let resource_attrs = match resource {
            Some(resource) => {
                for kv in &resource.attributes {
                    charge_budget(&mut expanded_bytes, attr_budget_charge(kv))?;
                }
                land_attrs(
                    &resource.attributes,
                    TagScope::Resource,
                    // `service.name` is dropped from the STORED attributes
                    // because `spans.service` already carries it. It stays
                    // in the identity buffer above.
                    Some("service.name"),
                    &mut resource_stage,
                )
            }
            None => LandedAttrs {
                json: TraceJson::empty(),
                other: Vec::new(),
            },
        };
        check_json_paths(&resource_attrs.json)?;
        let resource_dropped = resource.map(|r| r.dropped_attributes_count).unwrap_or(0);

        for scope_spans in &resource_spans.scope_spans {
            let scope = scope_spans.scope.as_ref();
            let scope_name = scope.map(|s| s.name.clone()).unwrap_or_default();
            let scope_version = scope.map(|s| s.version.clone()).unwrap_or_default();
            let mut scope_stage = TagStage::default();
            let scope_attrs = match scope {
                Some(scope) => {
                    for kv in &scope.attributes {
                        charge_budget(&mut expanded_bytes, attr_budget_charge(kv))?;
                    }
                    land_attrs(
                        &scope.attributes,
                        TagScope::Instrumentation,
                        None,
                        &mut scope_stage,
                    )
                }
                None => LandedAttrs {
                    json: TraceJson::empty(),
                    other: Vec::new(),
                },
            };
            check_json_paths(&scope_attrs.json)?;

            for span in &scope_spans.spans {
                let mut span_stage = TagStage::default();
                let Some(landed) = land_span(
                    &mut out,
                    &mut expanded_bytes,
                    span,
                    now_ns,
                    resource_id,
                    &service,
                    &scope_name,
                    &scope_version,
                    &scope_attrs,
                    &mut span_stage,
                )?
                else {
                    continue;
                };
                let day = match pulsus_model::Date::start_of_day_utc_datetime_safe(landed.start_ns)
                {
                    Some(date) => date.days_since_epoch(),
                    None => {
                        reject_landing(
                            &mut out,
                            format!(
                                "span {:?}: start time {} is outside the admitted UTC day domain",
                                diag_snippet(&span.name, DIAG_SNIPPET_MAX_BYTES),
                                landed.start_ns
                            ),
                        );
                        continue;
                    }
                };
                // **The staging boundary.** The span has passed every check
                // from here, so its own entries and the resource's and the
                // scope's are merged, and its resource row is emitted. A
                // span refused above — by its ids, its timestamp or the day
                // check — reaches neither.
                accepted.absorb(&resource_stage);
                accepted.absorb(&scope_stage);
                accepted.absorb(&span_stage);
                resources
                    .entry((resource_id, day))
                    .or_insert_with(|| LandingResource {
                        resource_id,
                        day,
                        service: service.clone(),
                        attrs: resource_attrs.json.clone(),
                        attrs_other: encode_attrs_other(resource_attrs.other.clone()),
                        dropped_attrs: resource_dropped,
                        schema_url: resource_spans.schema_url.clone(),
                        // ISSUE #587 STUB: row 8's carrier is not populated
                        // yet.
                        entity_refs: Vec::new(),
                    });
                out.spans.push(landed);
            }
        }
    }

    out.resources = resources.into_values().collect();
    out.tag_names = accepted.names.into_iter().collect();
    out.tag_values = accepted.values.into_iter().collect();
    Ok(out)
}

/// Lands one span, or rejects it wholesale into partial success.
///
/// `stage` is this span's own holding set — its attributes', its events'
/// and its links' catalog entries. The caller merges it only once the span
/// has passed the UTC-day check as well, which this function does not run.
#[allow(clippy::too_many_arguments)]
fn land_span(
    out: &mut ParsedTraceLanding,
    expanded_bytes: &mut usize,
    span: &Span,
    now_ns: i64,
    resource_id: Fingerprint,
    service: &str,
    scope_name: &str,
    scope_version: &str,
    scope_attrs: &LandedAttrs,
    stage: &mut TagStage,
) -> Result<Option<LandingSpan>, LogsIngestError> {
    let Ok(trace_id) = <[u8; 16]>::try_from(span.trace_id.as_slice()) else {
        reject_landing(
            out,
            format!(
                "span {:?}: trace_id must be exactly 16 bytes, got {}",
                diag_snippet(&span.name, DIAG_SNIPPET_MAX_BYTES),
                span.trace_id.len()
            ),
        );
        return Ok(None);
    };
    let Ok(span_id) = <[u8; 8]>::try_from(span.span_id.as_slice()) else {
        reject_landing(
            out,
            format!(
                "span {:?}: span_id must be exactly 8 bytes, got {}",
                diag_snippet(&span.name, DIAG_SNIPPET_MAX_BYTES),
                span.span_id.len()
            ),
        );
        return Ok(None);
    };
    let parent_span_id = if span.parent_span_id.is_empty() {
        [0u8; 8]
    } else {
        match <[u8; 8]>::try_from(span.parent_span_id.as_slice()) {
            Ok(parent) => parent,
            Err(_) => {
                reject_landing(
                    out,
                    format!(
                        "span {:?}: parent_span_id must be empty or exactly 8 bytes, got {}",
                        diag_snippet(&span.name, DIAG_SNIPPET_MAX_BYTES),
                        span.parent_span_id.len()
                    ),
                );
                return Ok(None);
            }
        }
    };
    let start_ns = if span.start_time_unix_nano == 0 {
        now_ns
    } else {
        match i64::try_from(span.start_time_unix_nano) {
            Ok(ns) => ns,
            Err(_) => {
                reject_landing(
                    out,
                    format!(
                        "span {:?}: start_time_unix_nano {} is not representable",
                        diag_snippet(&span.name, DIAG_SNIPPET_MAX_BYTES),
                        span.start_time_unix_nano
                    ),
                );
                return Ok(None);
            }
        }
    };

    for kv in &span.attributes {
        charge_budget(expanded_bytes, attr_budget_charge(kv))?;
    }
    let span_attrs = land_attrs(&span.attributes, TagScope::Span, None, stage);
    check_json_paths(&span_attrs.json)?;

    let mut events = Vec::with_capacity(span.events.len());
    for event in &span.events {
        for kv in &event.attributes {
            charge_budget(expanded_bytes, attr_budget_charge(kv))?;
        }
        let landed = land_attrs(&event.attributes, TagScope::Event, None, stage);
        check_json_paths(&landed.json)?;
        events.push(LandingEvent {
            // ISSUE #587 STUB: the member is `u64` and the saturation is
            // still here, so `W-12` is red on the value rather than on a
            // type error.
            time_ns: u64::try_from(i64::try_from(event.time_unix_nano).unwrap_or(i64::MAX))
                .unwrap_or(0),
            name: event.name.clone(),
            attrs: landed.json,
            // ISSUE #587 STUB: row 4's carrier is not populated yet.
            attrs_other: Vec::new(),
            dropped_attrs: event.dropped_attributes_count,
        });
    }

    let mut links = Vec::with_capacity(span.links.len());
    for link in &span.links {
        for kv in &link.attributes {
            charge_budget(expanded_bytes, attr_budget_charge(kv))?;
        }
        let landed = land_attrs(&link.attributes, TagScope::Link, None, stage);
        check_json_paths(&landed.json)?;
        links.push(LandingLink {
            // ISSUE #587 STUB: the members are byte buffers and the
            // rewrite to all-zero bytes is still here, so `W-2` is red on
            // the value rather than on a type error.
            trace_id: <[u8; 16]>::try_from(link.trace_id.as_slice())
                .unwrap_or([0u8; 16])
                .to_vec(),
            span_id: <[u8; 8]>::try_from(link.span_id.as_slice())
                .unwrap_or([0u8; 8])
                .to_vec(),
            trace_state: link.trace_state.clone(),
            flags: link.flags,
            attrs: landed.json,
            // ISSUE #587 STUB: row 4's carrier is not populated yet.
            attrs_other: Vec::new(),
            dropped_attrs: link.dropped_attributes_count,
        });
    }

    Ok(Some(LandingSpan {
        trace_id,
        span_id,
        parent_span_id,
        start_ns,
        duration_ns: resolve_duration_ns(span.start_time_unix_nano, span.end_time_unix_nano),
        resource_id,
        name: span.name.clone(),
        // ISSUE #587 STUB: the fields are `i32` and the narrowing to a byte
        // is still here, so `W-5` and `W-13` are red on the value rather
        // than on a type error.
        kind: i32::from(u8::try_from(span.kind).unwrap_or(0)),
        status_code: span
            .status
            .as_ref()
            .map(|s| i32::from(u8::try_from(s.code).unwrap_or(0)))
            .unwrap_or(0),
        status_message: span
            .status
            .as_ref()
            .map(|s| s.message.clone())
            .unwrap_or_default(),
        trace_state: span.trace_state.clone(),
        flags: span.flags,
        scope_name: scope_name.to_string(),
        scope_version: scope_version.to_string(),
        scope_attrs: scope_attrs.json.clone(),
        // ISSUE #587 STUB: rows 1, 2, 3 and 9 are not carried yet.
        scope_schema_url: String::new(),
        scope_dropped_attrs: 0,
        scope_attrs_other: Vec::new(),
        end_ns: 0,
        events,
        dropped_events: span.dropped_events_count,
        links,
        dropped_links: span.dropped_links_count,
        service: service.to_string(),
        attrs: span_attrs.json,
        attrs_other: encode_attrs_other(span_attrs.other),
        dropped_attrs: span.dropped_attributes_count,
    }))
}

/// Records one span's rejection into the landing decode's partial-success
/// accounting, keeping only the first message — [`reject_span`]'s rule.
fn reject_landing(out: &mut ParsedTraceLanding, message: String) {
    out.rejected += 1;
    if out.rejected_message.is_none() {
        out.rejected_message = Some(message);
    }
}

#[cfg(test)]
mod landing_tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::EntityRef;
    use opentelemetry_proto::tonic::trace::v1::span;

    fn str_value(s: &str) -> AnyValue {
        AnyValue {
            value: Some(Value::StringValue(s.to_string())),
        }
    }

    fn int_value(i: i64) -> AnyValue {
        AnyValue {
            value: Some(Value::IntValue(i)),
        }
    }

    fn double_value(d: f64) -> AnyValue {
        AnyValue {
            value: Some(Value::DoubleValue(d)),
        }
    }

    fn bool_value(b: bool) -> AnyValue {
        AnyValue {
            value: Some(Value::BoolValue(b)),
        }
    }

    fn array_value(values: Vec<AnyValue>) -> AnyValue {
        AnyValue {
            value: Some(Value::ArrayValue(ArrayValue { values })),
        }
    }

    fn kvlist_value(pairs: Vec<(&str, AnyValue)>) -> AnyValue {
        AnyValue {
            value: Some(Value::KvlistValue(KeyValueList {
                values: pairs.into_iter().map(|(k, v)| kv(k, v)).collect(),
            })),
        }
    }

    fn bytes_value(bytes: &[u8]) -> AnyValue {
        AnyValue {
            value: Some(Value::BytesValue(bytes.to_vec())),
        }
    }

    /// The profiling signal's reference into its own string table, which a
    /// non-profiling receiver processes as absent or empty.
    fn strindex_value(index: i32) -> AnyValue {
        AnyValue {
            value: Some(Value::StringValueStrindex(index)),
        }
    }

    /// An `AnyValue` with no arm set, which the protocol calls "empty".
    fn unset_value() -> AnyValue {
        AnyValue { value: None }
    }

    fn kv(key: &str, value: AnyValue) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(value),
            key_strindex: 0,
        }
    }

    const TS: u64 = 1_700_000_000_000_000_000;

    fn resource_of(pairs: Vec<KeyValue>) -> Resource {
        Resource {
            attributes: pairs,
            dropped_attributes_count: 0,
            entity_refs: Vec::new(),
        }
    }

    fn span_of(attrs: Vec<KeyValue>) -> Span {
        Span {
            trace_id: vec![0xab; 16],
            span_id: vec![0xcd; 8],
            parent_span_id: Vec::new(),
            trace_state: String::new(),
            flags: 0,
            name: "GET /api".to_string(),
            kind: 2,
            start_time_unix_nano: TS,
            end_time_unix_nano: TS + 1_000_000,
            attributes: attrs,
            dropped_attributes_count: 0,
            events: Vec::new(),
            dropped_events_count: 0,
            links: Vec::new(),
            dropped_links_count: 0,
            status: None,
        }
    }

    fn request_of(service: &str, spans: Vec<Span>) -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(resource_of(vec![kv("service.name", str_value(service))])),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    fn landed(req: &ExportTraceServiceRequest) -> ParsedTraceLanding {
        parse_landing(req, TS as i64).expect("the landing decode")
    }

    /// The stored paths one span's `attrs` carries, in the encoder's own
    /// sorted order.
    fn paths(parsed: &ParsedTraceLanding) -> Vec<&str> {
        parsed.spans[0]
            .attrs
            .entries()
            .iter()
            .map(|e| e.path.as_str())
            .collect()
    }

    /// **A span attribute carrying a profiling string reference lands
    /// nowhere** (issue #586): no path in `attrs`, no key in `attrs_other`,
    /// no row in `tag_names` and no row in `tag_values`, in any of the five
    /// scopes.
    ///
    /// The proto's own comment on that arm is the authority: a receiver for
    /// a signal other than Profiling should "process the data as if this
    /// value were absent or empty, ignoring its semantic content". It offers
    /// two readings and this takes **absent**.
    ///
    /// `k4` is the control — a bytes value still goes to `attrs_other` — and
    /// the scope attribute is what shows the drop is not the span scope's
    /// alone.
    #[test]
    fn a_profiling_string_reference_lands_nowhere() {
        let span = span_of(vec![
            kv("k1", strindex_value(7)),
            kv(
                "k2",
                array_value(vec![str_value("a"), strindex_value(7), str_value("b")]),
            ),
            kv("k3", array_value(vec![strindex_value(7)])),
            kv("k4", bytes_value(b"x")),
        ]);
        let req = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(resource_of(vec![kv("service.name", str_value("checkout"))])),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "io.otel.http".to_string(),
                        version: String::new(),
                        attributes: vec![kv("s1", strindex_value(7))],
                        dropped_attributes_count: 0,
                    }),
                    spans: vec![span],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let parsed = landed(&req);
        assert_eq!(parsed.spans.len(), 1);

        // `attrs`: no path for `k1`; `k2` is a two-element string array;
        // `k3` is a landed array of no elements.
        assert_eq!(
            paths(&parsed),
            vec!["k2", "k3"],
            "only the two arrays store a path; `k1`'s arm lands nowhere and \
             `k4` is carried in `attrs_other`"
        );
        let by_path = |path: &str| {
            parsed.spans[0]
                .attrs
                .entries()
                .iter()
                .find(|e| e.path == path)
                .map(|e| e.value.clone())
                .unwrap_or_else(|| panic!("no entry at {path}"))
        };
        assert_eq!(
            by_path("k2"),
            TraceJsonValue::StrArray(vec!["a".to_string(), "b".to_string()]),
            "the element of that arm is dropped and the rest land"
        );
        // **Not the variant**: §4.2's two empty-array variants serialise to
        // the same four bytes and no read can tell them apart.
        let k3 = by_path("k3");
        assert!(
            matches!(k3, TraceJsonValue::EmptyArray)
                || matches!(&k3, TraceJsonValue::StrArray(a) if a.is_empty()),
            "an array whose every element is that arm lands as the empty \
             array: {k3:?}"
        );

        // `attrs_other` carries `k4` and nothing else.
        let other = KeyValueList::decode(parsed.spans[0].attrs_other.as_slice())
            .expect("attrs_other decodes as a KeyValueList");
        let keys: Vec<&str> = other.values.iter().map(|kv| kv.key.as_str()).collect();
        assert_eq!(
            keys,
            vec!["k4"],
            "the index itself is never carried in the protobuf side-channel"
        );

        // Neither catalog lists the key, in either scope.
        let names: Vec<&str> = parsed.tag_names.iter().map(|t| t.key.as_str()).collect();
        assert!(!names.contains(&"k1"), "tag_names: {names:?}");
        assert!(!names.contains(&"s1"), "nor the scope's own key: {names:?}");
        for present in ["k2", "k3", "k4"] {
            assert!(
                names.contains(&present),
                "every other key is still listed: {present} missing from {names:?}"
            );
        }
        let values: Vec<(&str, &str)> = parsed
            .tag_values
            .iter()
            .map(|t| (t.key.as_str(), t.value.as_str()))
            .collect();
        assert!(
            !values.iter().any(|(k, _)| *k == "k1"),
            "tag_values: {values:?}"
        );
        assert!(values.contains(&("k2", "a")), "{values:?}");
        assert!(values.contains(&("k2", "b")), "{values:?}");
    }

    /// **Each OTLP value arm lands where §4.2's table says.**
    ///
    /// The four shapes with no JSON representation — a bytes value, an empty
    /// kvlist, an array holding a bytes or kvlist element, and an `AnyValue`
    /// with no arm set — store no path at all and are carried in
    /// `attrs_other` under their original OTLP keys.
    #[test]
    fn each_value_arm_lands_where_the_type_table_says() {
        let req = request_of(
            "checkout",
            vec![span_of(vec![
                kv("s", str_value("v")),
                kv("b", bool_value(true)),
                kv("i", int_value(1)),
                kv("f", double_value(1.5)),
                kv("arr_s", array_value(vec![str_value("a")])),
                kv("arr_i", array_value(vec![int_value(1), int_value(2)])),
                kv("arr_f", array_value(vec![double_value(1.5)])),
                kv("arr_b", array_value(vec![bool_value(false)])),
                kv("arr_mixed", array_value(vec![str_value("a"), int_value(1)])),
                kv("arr_empty", array_value(vec![])),
                kv("nested", kvlist_value(vec![("a", int_value(1))])),
                // The four with no JSON representation.
                kv("opaque", bytes_value(&[1, 2])),
                kv("empty_map", kvlist_value(vec![])),
                kv("arr_opaque", array_value(vec![bytes_value(&[1])])),
                KeyValue {
                    key: "unset".to_string(),
                    value: Some(AnyValue { value: None }),
                    key_strindex: 0,
                },
            ])],
        );
        let parsed = landed(&req);
        assert_eq!(parsed.spans.len(), 1);

        assert_eq!(
            paths(&parsed),
            vec![
                "arr_b",
                "arr_empty",
                "arr_f",
                "arr_i",
                "arr_mixed",
                "arr_s",
                "b",
                "f",
                "i",
                "nested.a",
                "s",
            ],
            "eleven stored paths, and the four opaque keys store none; a \
             nested object stores its dotted leaf and nothing at the parent"
        );

        let by_path = |path: &str| {
            parsed.spans[0]
                .attrs
                .entries()
                .iter()
                .find(|e| e.path == path)
                .map(|e| e.value.clone())
                .unwrap_or_else(|| panic!("no entry at {path}"))
        };
        assert_eq!(
            by_path("s"),
            TraceJsonValue::Scalar(TraceJsonScalar::Str("v".to_string()))
        );
        assert_eq!(
            by_path("b"),
            TraceJsonValue::Scalar(TraceJsonScalar::Bool(true))
        );
        assert_eq!(
            by_path("i"),
            TraceJsonValue::Scalar(TraceJsonScalar::Int(1))
        );
        assert_eq!(
            by_path("f"),
            TraceJsonValue::Scalar(TraceJsonScalar::Double(1.5))
        );
        assert_eq!(
            by_path("arr_s"),
            TraceJsonValue::StrArray(vec!["a".to_string()])
        );
        assert_eq!(by_path("arr_i"), TraceJsonValue::IntArray(vec![1, 2]));
        assert_eq!(by_path("arr_f"), TraceJsonValue::DoubleArray(vec![1.5]));
        assert_eq!(by_path("arr_b"), TraceJsonValue::BoolArray(vec![false]));
        assert_eq!(
            by_path("arr_mixed"),
            TraceJsonValue::MixedArray(vec![
                TraceJsonScalar::Str("a".to_string()),
                TraceJsonScalar::Int(1),
            ])
        );
        assert_eq!(by_path("arr_empty"), TraceJsonValue::EmptyArray);
        assert_eq!(
            by_path("nested.a"),
            TraceJsonValue::Scalar(TraceJsonScalar::Int(1))
        );

        // The four opaque keys, under their original OTLP keys.
        let other = KeyValueList::decode(parsed.spans[0].attrs_other.as_slice())
            .expect("attrs_other decodes as a KeyValueList");
        let mut keys: Vec<&str> = other.values.iter().map(|kv| kv.key.as_str()).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec!["arr_opaque", "empty_map", "opaque", "unset"],
            "the keys no JSON path can hold are carried in the side channel"
        );
    }

    /// **The two catalogs disagree on a composite key: the name is listed,
    /// the value is not.**
    ///
    /// A kind-2 row is emitted for every key the push carries in any scope,
    /// including a key whose value went to `attrs_other` and a key whose
    /// value is a kvlist, whose stored leaf paths also each get their own
    /// row under their full dotted key. A kind-3 row is emitted for a scalar
    /// and for each element of a scalar array, and for nothing else.
    #[test]
    fn the_catalogs_list_every_key_and_only_the_scalar_values() {
        let req = request_of(
            "checkout",
            vec![span_of(vec![
                kv("plain", str_value("v")),
                kv("tags", array_value(vec![str_value("x"), str_value("y")])),
                kv("mixed", array_value(vec![str_value("x"), int_value(1)])),
                kv("opaque", bytes_value(&[1])),
                kv(
                    "composite",
                    kvlist_value(vec![("a", kvlist_value(vec![("b", int_value(1))]))]),
                ),
            ])],
        );
        let parsed = landed(&req);

        let span_names: Vec<&str> = parsed
            .tag_names
            .iter()
            .filter(|t| t.scope == TagScope::Span)
            .map(|t| t.key.as_str())
            .collect();
        assert_eq!(
            span_names,
            vec![
                "composite",
                "composite.a.b",
                "mixed",
                "opaque",
                "plain",
                "tags",
            ],
            "every key the push carried is named, the composite's leaf included"
        );

        let span_values: Vec<String> = parsed
            .tag_values
            .iter()
            .filter(|t| t.scope == TagScope::Span)
            .map(|t| format!("{}={}:{}", t.key, t.value, t.val_type))
            .collect();
        assert_eq!(
            span_values,
            vec![
                "plain=v:string".to_string(),
                "tags=x:string".to_string(),
                "tags=y:string".to_string(),
            ],
            "one value row per scalar and per element of a scalar array; none \
             for a mixed array, a kvlist or a value in attrs_other"
        );

        // The resource's own `service.name` is neither named nor valued: the
        // span row carries it as a column.
        assert!(
            !parsed
                .tag_names
                .iter()
                .any(|t| t.scope == TagScope::Resource && t.key == "service.name"),
            "service.name is a column, not a catalog entry: {:?}",
            parsed.tag_names
        );
    }

    /// **The five scopes, each under its own discriminator.** A same-named
    /// key in two scopes is two catalog entries.
    #[test]
    fn the_five_scopes_are_discriminated() {
        let mut span = span_of(vec![kv("k", str_value("span"))]);
        span.events = vec![span::Event {
            time_unix_nano: TS,
            name: "e".to_string(),
            attributes: vec![kv("k", str_value("event"))],
            dropped_attributes_count: 0,
        }];
        span.links = vec![span::Link {
            trace_id: vec![0x11; 16],
            span_id: vec![0x22; 8],
            trace_state: String::new(),
            attributes: vec![kv("k", str_value("link"))],
            dropped_attributes_count: 0,
            flags: 0,
        }];
        let req = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(resource_of(vec![
                    kv("service.name", str_value("checkout")),
                    kv("k", str_value("resource")),
                ])),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "io.otel.http".to_string(),
                        version: "1.4.2".to_string(),
                        attributes: vec![kv("k", str_value("instrumentation"))],
                        dropped_attributes_count: 0,
                    }),
                    spans: vec![span],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let parsed = landed(&req);

        let mut scopes: Vec<&str> = parsed
            .tag_names
            .iter()
            .filter(|t| t.key == "k")
            .map(|t| t.scope.as_str())
            .collect();
        scopes.sort_unstable();
        assert_eq!(
            scopes,
            vec!["event", "instrumentation", "link", "resource", "span"],
            "one key in five scopes is five catalog entries"
        );

        let mut valued: Vec<String> = parsed
            .tag_values
            .iter()
            .filter(|t| t.key == "k")
            .map(|t| format!("{}:{}", t.scope.as_str(), t.value))
            .collect();
        valued.sort();
        assert_eq!(
            valued,
            vec![
                "event:event".to_string(),
                "instrumentation:instrumentation".to_string(),
                "link:link".to_string(),
                "resource:resource".to_string(),
                "span:span".to_string(),
            ]
        );
    }

    /// **Kinds 1, 2 and 3 are deduplicated inside the push only.** Two spans
    /// of one resource on one day land **one** resource row, and a key or a
    /// value repeated across spans lands once.
    ///
    /// There is no cache of anything already written: a second push repeats
    /// every one of these rows, which is what makes a fan-out failure heal
    /// without a repair that cannot be expressed.
    #[test]
    fn the_push_is_deduplicated_inside_itself_and_nowhere_else() {
        let req = request_of(
            "checkout",
            vec![
                span_of(vec![kv("k", str_value("v"))]),
                span_of(vec![kv("k", str_value("v"))]),
                span_of(vec![kv("k", str_value("w"))]),
            ],
        );
        let parsed = landed(&req);
        assert_eq!(parsed.spans.len(), 3, "one row per decoded span");
        assert_eq!(
            parsed.resources.len(),
            1,
            "one row per distinct (resource_id, day) in the push"
        );
        assert_eq!(
            parsed
                .tag_names
                .iter()
                .filter(|t| t.scope == TagScope::Span && t.key == "k")
                .count(),
            1,
            "one row per distinct (scope, key)"
        );
        assert_eq!(
            parsed
                .tag_values
                .iter()
                .filter(|t| t.scope == TagScope::Span)
                .map(|t| t.value.as_str())
                .collect::<Vec<_>>(),
            vec!["v", "w"],
            "one row per distinct (scope, key, value, type)"
        );
        assert_eq!(
            parsed.total_rows(),
            3 + 1 + 1 + 2,
            "the per-push row ceiling counts all four kinds"
        );

        // A second decode of the same request produces the same rows again:
        // nothing is remembered across pushes.
        let again = landed(&req);
        assert_eq!(again.resources.len(), 1);
        assert_eq!(again.tag_names, parsed.tag_names);
        assert_eq!(again.tag_values, parsed.tag_values);
    }

    /// **A duplicate key inside one span keeps the first value**, per scope.
    /// `type_json_skip_duplicated_paths` is `0` on this server, so a
    /// repeated path in the block is an exception rather than a silent drop
    /// and the deduplication has to be the writer's.
    #[test]
    fn a_duplicate_key_in_one_span_keeps_the_first_value() {
        let req = request_of(
            "checkout",
            vec![span_of(vec![
                kv("k", str_value("first")),
                kv("k", str_value("second")),
            ])],
        );
        let parsed = landed(&req);
        assert_eq!(paths(&parsed), vec!["k"], "one stored path, not two");
        assert_eq!(
            parsed.spans[0].attrs.entries()[0].value,
            TraceJsonValue::Scalar(TraceJsonScalar::Str("first".to_string()))
        );
        assert_eq!(
            parsed
                .tag_values
                .iter()
                .filter(|t| t.scope == TagScope::Span)
                .map(|t| t.value.as_str())
                .collect::<Vec<_>>(),
            vec!["first"],
            "and the dropped value is not catalogued either"
        );
    }

    /// **A span whose stored path count exceeds
    /// `format_binary_max_object_size` is refused at decode**, naming the
    /// count and the limit, and a span at the limit is accepted.
    ///
    /// The decode gate and the query pin carry the **same constant**, so
    /// neither can admit what the other refuses. Without the gate the push
    /// is admitted, the insert fails after the block was sent, and the
    /// caller gets an uncertain ending for a push that could never have
    /// succeeded.
    #[test]
    fn a_span_with_too_many_paths_is_refused_at_decode() {
        let limit = pulsus_clickhouse::MAX_JSON_PATHS_PER_VALUE as usize;
        let attrs = |n: usize| -> Vec<KeyValue> {
            (0..n)
                .map(|i| kv(&format!("k{i:06}"), int_value(i as i64)))
                .collect()
        };

        let at_limit = request_of("checkout", vec![span_of(attrs(limit))]);
        let parsed = parse_landing(&at_limit, TS as i64).expect("a span at the limit is admitted");
        assert_eq!(parsed.spans[0].attrs.len(), limit);

        let over = request_of("checkout", vec![span_of(attrs(limit + 1))]);
        let err = parse_landing(&over, TS as i64).expect_err("one path over is refused");
        match err {
            LogsIngestError::OversizeMessage {
                field,
                limit: reported,
                actual,
            } => {
                assert_eq!(reported, limit, "the message names the limit");
                assert_eq!(actual, limit + 1, "and the push's own count");
                assert!(
                    field.contains("JSON paths"),
                    "and what was exceeded: {field}"
                );
            }
            other => panic!("wanted OversizeMessage, got {other:?}"),
        }
    }

    /// **The resource identity is 128 bits and stable.** Eight pairs, each
    /// one property; the eighth is the one an implementation that strips
    /// `service.name` before hashing fails while passing the first seven.
    #[test]
    fn the_resource_id_is_128_bits_and_stable() {
        let id = |pairs: Vec<KeyValue>, schema_url: &str| {
            resource_identity(Some(&resource_of(pairs)), schema_url)
        };
        let base = || {
            vec![
                kv("service.name", str_value("checkout")),
                kv("host.name", str_value("node-a")),
                kv("k", int_value(1)),
            ]
        };

        // 1. Byte-identical resources.
        assert_eq!(id(base(), ""), id(base(), ""));

        // 2. The same pairs in a different order.
        assert_eq!(
            id(base(), ""),
            id(
                vec![
                    kv("k", int_value(1)),
                    kv("service.name", str_value("checkout")),
                    kv("host.name", str_value("node-a")),
                ],
                ""
            ),
            "the identity does not depend on arrival order"
        );

        // 3. One changed value.
        let mut changed_value = base();
        changed_value[1] = kv("host.name", str_value("node-b"));
        assert_ne!(id(base(), ""), id(changed_value, ""));

        // 4. One changed key.
        let mut changed_key = base();
        changed_key[1] = kv("host.id", str_value("node-a"));
        assert_ne!(id(base(), ""), id(changed_key, ""));

        // 5. A different schema url.
        assert_ne!(
            id(base(), ""),
            id(base(), "https://example.invalid/v1"),
            "the schema url is part of the identity"
        );

        // 6. An integer `1` and the string `"1"`.
        let mut as_text = base();
        as_text[2] = kv("k", str_value("1"));
        assert_ne!(id(base(), ""), id(as_text, ""), "the value is type-tagged");

        // 7. A key present with an empty value against an absent key.
        let mut empty_value = base();
        empty_value[2] = kv("k", str_value(""));
        assert_ne!(
            id(empty_value, ""),
            id(
                vec![
                    kv("service.name", str_value("checkout")),
                    kv("host.name", str_value("node-a")),
                ],
                ""
            ),
            "a present key with an empty value is not an absent key"
        );

        // 8. Two resources differing only in `service.name`.
        let mut other_service = base();
        other_service[0] = kv("service.name", str_value("pay"));
        assert_ne!(
            id(base(), ""),
            id(other_service, ""),
            "service.name is in the hash, and only the STORED copy drops it"
        );

        // And the stored copy does drop it.
        let req = request_of("checkout", vec![span_of(Vec::new())]);
        let parsed = landed(&req);
        assert_eq!(parsed.resources.len(), 1);
        assert!(
            parsed.resources[0].attrs.is_empty(),
            "the stored resource attributes omit service.name: {:?}",
            parsed.resources[0].attrs
        );
        assert_eq!(parsed.resources[0].service, "checkout");
        assert_eq!(
            parsed.resources[0].resource_id, parsed.spans[0].resource_id,
            "the span and its resource row carry one identity"
        );
    }

    /// **W-10's hermetic half (issue #587 rows 8 and 13).** The identity
    /// covers **all four** of its inputs, and it covers each one's
    /// **value** rather than its presence.
    ///
    /// **Four inputs, and that is the whole set**, which is what makes this
    /// exhaustive rather than illustrative: `Resource` has exactly three
    /// fields in the generated protocol source — `attributes` (tag 1),
    /// `dropped_attributes_count` (tag 2), `entity_refs` (tag 3) — and
    /// `resource_identity` takes `ResourceSpans.schema_url` as its second
    /// parameter. A fourth `Resource` field added upstream has to appear
    /// here.
    ///
    /// **Seven sub-cases, and the two value pairs are the load-bearing
    /// ones.** A section appended merely *because* the count is non-zero,
    /// or *because* the vector is non-empty, passes `0` against `5` and
    /// `empty` against `one` and **collides** on `5` against `6` and on two
    /// different single-reference vectors — so those two pairs are what
    /// force the field's own value into the buffer. The seventh pins that
    /// no existing identity moved.
    #[test]
    fn the_resource_identity_covers_every_field_the_resource_carries() {
        const SCHEMA: &str = "https://example.invalid/v1";
        let attrs = || {
            vec![
                kv("service.name", str_value("checkout")),
                kv("host.name", str_value("node-a")),
            ]
        };
        let entity = |kind: &str| EntityRef {
            schema_url: SCHEMA.to_string(),
            r#type: kind.to_string(),
            id_keys: vec!["host.name".to_string()],
            description_keys: Vec::new(),
        };
        let id = |dropped: u32, refs: Vec<EntityRef>, schema_url: &str| {
            resource_identity(
                Some(&Resource {
                    attributes: attrs(),
                    dropped_attributes_count: dropped,
                    entity_refs: refs,
                }),
                schema_url,
            )
        };
        let base = || id(0, Vec::new(), SCHEMA);

        // 1. `Resource.attributes` — the input that was already covered.
        assert_ne!(
            base(),
            resource_identity(
                Some(&Resource {
                    attributes: vec![
                        kv("service.name", str_value("checkout")),
                        kv("host.name", str_value("node-b")),
                    ],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                SCHEMA,
            ),
            "the attributes are in the hash"
        );

        // 2. `ResourceSpans.schema_url` — likewise.
        assert_ne!(
            base(),
            id(0, Vec::new(), "https://example.invalid/v2"),
            "the schema url is in the hash"
        );

        // 3. `Resource.dropped_attributes_count`, zero against non-zero.
        assert_ne!(
            base(),
            id(5, Vec::new(), SCHEMA),
            "a dropped count of 5 is not a dropped count of 0"
        );

        // 4. And **non-zero against non-zero**, which a presence-only
        // section fails: the count's own value has to be in the buffer.
        assert_ne!(
            id(5, Vec::new(), SCHEMA),
            id(6, Vec::new(), SCHEMA),
            "a dropped count of 6 is not a dropped count of 5"
        );

        // 5. `Resource.entity_refs`, empty against one reference.
        assert_ne!(
            id(5, Vec::new(), SCHEMA),
            id(5, vec![entity("host")], SCHEMA),
            "an entity reference is not the absence of one"
        );

        // 6. And **one non-empty vector against another**, the same point.
        assert_ne!(
            id(5, vec![entity("host")], SCHEMA),
            id(5, vec![entity("service")], SCHEMA),
            "a `service` entity reference is not a `host` one"
        );

        // 7. **No existing identity moves.** A resource with an empty
        // `entity_refs` and a zero dropped count hashes to exactly the
        // value it hashed to before the two sections were added, which is
        // why both are conditional and appended after the schema url. The
        // literal was read off this function on `origin/main` at
        // `619414e4`.
        assert_eq!(
            base(),
            Fingerprint::from_raw(307_209_891_247_568_146_312_164_646_737_311_135_069),
            "a resource with neither field set must keep the identity it \
             had before issue #587: the two new sections are conditional"
        );

        // And one value twice is one resource, so none of the assertions
        // above can pass on an unstable identity.
        assert_eq!(base(), base());
        assert_eq!(
            id(6, vec![entity("service")], SCHEMA),
            id(6, vec![entity("service")], SCHEMA),
        );
    }

    /// **Every value a resource carries is type-tagged at every depth.**
    ///
    /// The shapes that can nest are taken from the protocol definition
    /// rather than from one example: `AnyValue.value` is an optional oneof
    /// of eight arms (`opentelemetry.proto.common.v1`, tags 1 to 8) and
    /// exactly two of them carry further `AnyValue`s — `ArrayValue.values`,
    /// and `KeyValueList.values`' `KeyValue.value`. Every nesting shape is
    /// a composition of those two, every arm can appear at every depth, and
    /// so the tag has to be written at every depth.
    ///
    /// Four pairs, each of which one flat render of the whole value
    /// collapses: a bytes value against the string of its own base64 text,
    /// `+inf` against `-inf`, and a NaN and a profiling string reference
    /// each against an unset value. A collapse is wrong data rather than
    /// untidiness: `resources` is a `ReplacingMergeTree` keyed on
    /// `(service, resource_id)`, so one of the two replaces the other and
    /// the spans carrying that id join to whichever survived.
    #[test]
    fn nested_values_are_type_tagged_at_every_depth() {
        type Wrap = fn(AnyValue) -> AnyValue;
        let wrappers: Vec<(&str, Wrap)> = vec![
            ("array", |v| array_value(vec![v])),
            ("array beside a sibling", |v| {
                array_value(vec![int_value(0), v])
            }),
            ("kvlist", |v| kvlist_value(vec![("n", v)])),
            ("kvlist beside a sibling", |v| {
                kvlist_value(vec![("m", int_value(0)), ("n", v)])
            }),
            ("array(array)", |v| array_value(vec![array_value(vec![v])])),
            ("array(kvlist)", |v| {
                array_value(vec![kvlist_value(vec![("n", v)])])
            }),
            ("kvlist(array)", |v| {
                kvlist_value(vec![("n", array_value(vec![v]))])
            }),
            ("kvlist(kvlist)", |v| {
                kvlist_value(vec![("n", kvlist_value(vec![("n", v)]))])
            }),
            ("array(kvlist(array))", |v| {
                array_value(vec![kvlist_value(vec![("n", array_value(vec![v]))])])
            }),
        ];

        // `base64("x")` is `eA==`, so one render of the whole value gives
        // the bytes value and that string the same text.
        let pairs: Vec<(&str, AnyValue, AnyValue)> = vec![
            (
                "a bytes value against its own base64 text",
                bytes_value(b"x"),
                str_value("eA=="),
            ),
            (
                "+inf against -inf",
                double_value(f64::INFINITY),
                double_value(f64::NEG_INFINITY),
            ),
            (
                "a NaN against an unset value",
                double_value(f64::NAN),
                unset_value(),
            ),
            (
                "a string reference against an unset value",
                strindex_value(7),
                unset_value(),
            ),
        ];

        let id_of = |value: AnyValue| {
            resource_identity(
                Some(&resource_of(vec![
                    kv("service.name", str_value("checkout")),
                    kv("k", value),
                ])),
                "",
            )
        };

        for (shape, wrap) in &wrappers {
            for (what, left, right) in &pairs {
                assert_ne!(
                    id_of(wrap(left.clone())),
                    id_of(wrap(right.clone())),
                    "{shape}: {what} are two resources"
                );
                // And one nested value is one resource, so none of the
                // assertions above can pass on an unstable identity.
                assert_eq!(
                    id_of(wrap(left.clone())),
                    id_of(wrap(left.clone())),
                    "{shape}: {what}, the first value twice, is one resource"
                );
            }
        }
    }

    /// **A push that lands no span catalogs nothing.** A push carrying no
    /// span at all, and a push whose only span the UTC-day check rejects,
    /// each produce no row of any kind — so the push makes no block and no
    /// insert and the store gains nothing for data it does not hold.
    ///
    /// The entries a resource, a scope and a span contribute are staged and
    /// merged only once a span has passed **every** check, the day check
    /// included: that check sits after the span itself has been landed, so
    /// a stage merged any earlier leaves `tag_names` and `tag_values` rows
    /// behind for a span that was refused.
    #[test]
    fn a_push_that_lands_no_span_catalogs_nothing() {
        // The first nanosecond of day 49_710: inside `i64`, inside `Date`'s
        // own range, and outside the DateTime-safe day domain the landing
        // tables partition by.
        const OUTSIDE_THE_DAY_DOMAIN: u64 = 86_400_000_000_000 * 49_710;

        let request = |spans: Vec<Span>| ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(resource_of(vec![
                    kv("service.name", str_value("checkout")),
                    kv("host.name", str_value("node-a")),
                ])),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "io.otel.http".to_string(),
                        version: "1.4.2".to_string(),
                        attributes: vec![kv("scope.key", str_value("scope-value"))],
                        dropped_attributes_count: 0,
                    }),
                    spans,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        // 1. A push carrying no span.
        let parsed = landed(&request(Vec::new()));
        assert!(
            parsed.tag_names.is_empty(),
            "a zero-span push names no tag: {:?}",
            parsed.tag_names
        );
        assert!(
            parsed.tag_values.is_empty(),
            "and catalogs no value: {:?}",
            parsed.tag_values
        );
        assert!(parsed.spans.is_empty());
        assert!(parsed.resources.is_empty());
        assert_eq!(parsed.total_rows(), 0);
        assert!(
            parsed.is_empty(),
            "a push carrying no span makes no block and no insert"
        );

        // 2. A push whose only span the UTC-day check rejects. Its own
        // attributes, its event's and its link's are all refused with it.
        let doomed = || {
            let mut span = span_of(vec![kv("span.key", str_value("span-value"))]);
            span.span_id = vec![0x01; 8];
            span.start_time_unix_nano = OUTSIDE_THE_DAY_DOMAIN;
            span.end_time_unix_nano = OUTSIDE_THE_DAY_DOMAIN + 1;
            span.events = vec![span::Event {
                time_unix_nano: OUTSIDE_THE_DAY_DOMAIN,
                name: "e".to_string(),
                attributes: vec![kv("event.key", str_value("event-value"))],
                dropped_attributes_count: 0,
            }];
            span.links = vec![span::Link {
                trace_id: vec![0x11; 16],
                span_id: vec![0x22; 8],
                trace_state: String::new(),
                attributes: vec![kv("link.key", str_value("link-value"))],
                dropped_attributes_count: 0,
                flags: 0,
            }];
            span
        };
        let parsed = landed(&request(vec![doomed()]));
        assert_eq!(parsed.rejected, 1, "the span is refused, not stored");
        assert!(
            parsed
                .rejected_message
                .as_deref()
                .unwrap_or_default()
                .contains("UTC day domain"),
            "and the day check is what refused it: {:?}",
            parsed.rejected_message
        );
        assert!(parsed.spans.is_empty());
        assert!(parsed.resources.is_empty());
        assert!(
            parsed.tag_names.is_empty(),
            "a push that stores no span names no tag: {:?}",
            parsed.tag_names
        );
        assert!(
            parsed.tag_values.is_empty(),
            "and catalogs no value: {:?}",
            parsed.tag_values
        );
        assert_eq!(parsed.total_rows(), 0);
        assert!(
            parsed.is_empty(),
            "a push that stores no span makes no block and no insert"
        );

        // 3. One span landing beside the refused one: the catalogs carry
        // the landed span's keys, the resource's and the scope's, and
        // nothing the refused span brought.
        let parsed = landed(&request(vec![
            doomed(),
            span_of(vec![kv("kept.key", str_value("kept-value"))]),
        ]));
        assert_eq!(parsed.spans.len(), 1, "one span of the two lands");
        assert_eq!(parsed.resources.len(), 1, "and its resource with it");
        assert_eq!(parsed.rejected, 1);
        let named: Vec<String> = parsed
            .tag_names
            .iter()
            .map(|t| format!("{}:{}", t.scope.as_str(), t.key))
            .collect();
        assert_eq!(
            named,
            vec![
                "span:kept.key".to_string(),
                "resource:host.name".to_string(),
                "instrumentation:scope.key".to_string(),
            ],
            "the refused span's own, its event's and its link's keys are absent"
        );
        let valued: Vec<String> = parsed
            .tag_values
            .iter()
            .map(|t| format!("{}:{}", t.scope.as_str(), t.value))
            .collect();
        assert_eq!(
            valued,
            vec![
                "span:kept-value".to_string(),
                "resource:node-a".to_string(),
                "instrumentation:scope-value".to_string(),
            ],
            "and so are their values"
        );
    }

    /// A push whose spans straddle midnight lands **two** rows for one
    /// resource, which is what `resources`' "one row per distinct resource
    /// per day" means: a resource joined at read time by a day-bounded
    /// predicate needs a row in each day it appears in.
    #[test]
    fn a_push_straddling_midnight_lands_a_resource_row_per_day() {
        // 2026-09-22T00:00:00Z and one nanosecond before it.
        let midnight: u64 = 1_790_035_200_000_000_000;
        let mut before = span_of(Vec::new());
        before.span_id = vec![0x01; 8];
        before.start_time_unix_nano = midnight - 1;
        before.end_time_unix_nano = midnight;
        let mut after = span_of(Vec::new());
        after.span_id = vec![0x02; 8];
        after.start_time_unix_nano = midnight;
        after.end_time_unix_nano = midnight + 1;

        let parsed = landed(&request_of("checkout", vec![before, after]));
        assert_eq!(parsed.spans.len(), 2);
        let mut days: Vec<u16> = parsed.resources.iter().map(|r| r.day).collect();
        days.sort_unstable();
        assert_eq!(days.len(), 2, "one resource row per day: {days:?}");
        assert_eq!(days[1], days[0] + 1, "and the two days are adjacent");
        assert_eq!(
            parsed.resources[0].resource_id, parsed.resources[1].resource_id,
            "one resource, two days"
        );
    }
}
