//! OTLP traces parser (issue #54, docs/architecture.md §4): a pure
//! `bytes -> ExportTraceServiceRequest -> ParsedTraces` pipeline with no
//! I/O. Unlike the logs/metrics parsers there is **no** label model here:
//! traces carry no fingerprint (a span's identity is `(trace_id,
//! span_id)`, distribution shards by the server-side `cityHash64(trace_id)`
//! — docs/architecture.md §2.2), and attribute keys are stored **verbatim**
//! in the index (docs/architecture.md §2.3), discriminated by a `scope`
//! column (`'resource'` vs `'span'`) so scoped TraceQL selectors never
//! collide across scopes. `InstrumentationScope` attributes are not
//! indexed at all (M4 TraceQL exposes only `resource.`/`span.` selectors,
//! issue #54 adjudication #2); they remain fully preserved in the span
//! payload.
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

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, TracesData};
use prost::Message;
use pulsus_model::Date;

use crate::error::LogsIngestError;
use crate::ingest::traces::{AttrRecord, ParsedTraces, SpanRecord};

/// The `scope` discriminator value for a resource attribute row.
const SCOPE_RESOURCE: &str = "resource";
/// The `scope` discriminator value for a span attribute row.
const SCOPE_SPAN: &str = "span";

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
    Ok(ExportTraceServiceRequest::decode(body)?)
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
    let kind = span.kind as i8;

    let resource_attrs = ctx.resource.map(|r| r.attributes.as_slice()).unwrap_or(&[]);

    // Expansion-budget reservation (module doc, issue #54 code-review
    // [high] fix): estimate this span's produced bytes from wire lengths
    // only — `encoded_len` never allocates — and check the running total
    // BEFORE materializing a single row, so an over-budget request is
    // rejected without ever paying for the expansion it describes.
    // (`ctx.service` is already-rendered here, charged pre-render by
    // `parse` — its `.len()` below is the exact per-span clone cost.)
    let mut span_expansion = SPAN_ROW_OVERHEAD
        + span.name.len()
        + ctx.service.len()
        + ctx.payload_base_estimate
        + span.encoded_len();
    for kv in resource_attrs.iter().chain(&span.attributes) {
        span_expansion += ATTR_ROW_OVERHEAD + attr_budget_charge(kv);
    }
    charge_budget(expanded_bytes, span_expansion)?;

    let date = Date::start_of_day_utc(timestamp_ns).days_since_epoch();
    for (scope, attrs) in [
        (SCOPE_RESOURCE, resource_attrs),
        (SCOPE_SPAN, &span.attributes),
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

    out.spans.push(SpanRecord {
        trace_id,
        span_id,
        parent_id,
        name: span.name.clone(),
        service: ctx.service.to_string(),
        timestamp_ns,
        duration_ns,
        status_code,
        kind,
        payload: build_payload(span, ctx.resource, ctx.resource_spans, ctx.scope_spans),
    });
    Ok(())
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
    let val_num = val.parse::<f64>().ok().filter(|n| n.is_finite());
    AttrRecord {
        date,
        key: kv.key.clone(),
        scope: scope.to_string(),
        val,
        val_num,
        timestamp_ns,
        trace_id,
        span_id,
        duration_ns,
    }
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
            message: String::new(),
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
        assert_eq!(span.kind, SpanKind::Server as i8);
    }

    #[test]
    fn parse_missing_status_resolves_to_code_zero() {
        let out = parse(&request_with(None, None, vec![valid_span()]), 0)
            .expect("within the expansion budget");
        assert_eq!(out.spans[0].status_code, 0);
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

    /// Issue #54 adjudication #2: `InstrumentationScope` attributes are
    /// never indexed — they exist only inside the payload.
    #[test]
    fn parse_never_indexes_instrumentation_scope_attributes() {
        let scope = InstrumentationScope {
            name: "my-scope".to_string(),
            version: "1.0.0".to_string(),
            attributes: vec![kv("scope.attr", Value::StringValue("x".to_string()))],
            dropped_attributes_count: 0,
        };
        let out = parse(&request_with(None, Some(scope), vec![valid_span()]), 0)
            .expect("within the expansion budget");
        assert!(out.attrs.is_empty());
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
}
