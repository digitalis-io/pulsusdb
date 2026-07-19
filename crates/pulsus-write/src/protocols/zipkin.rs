//! Zipkin v2 JSON trace receiver (issue #75, docs/api.md §8.2): a pure
//! `bytes -> Vec<ZipkinSpan> -> ExportTraceServiceRequest` decoder + model
//! adapter with no I/O. **It is not a new storage path.** Each decoded
//! Zipkin span is adapted into one self-contained OTLP `ResourceSpans`
//! (this span, its `localEndpoint` promoted to a `service.name` resource
//! attribute, its `remoteEndpoint`/`tags`/`debug`/`shared` as span
//! attributes, its `annotations` as span events) and the whole batch is
//! handed to the **existing** `otlp_traces::parse`
//! (`protocols/otlp_traces.rs`). Reusing that one function is the
//! load-bearing correctness guarantee: id length/validation, the
//! self-contained single-`ResourceSpans` payload contract, `payload_type =
//! 1` (OTLP — the reserved `payload_type = 2` "Zipkin JSON" is deliberately
//! NOT used, which would fork a divergent assembly path), verbatim scoped
//! attribute indexing, and the `MAX_EXPANDED_BYTES` expansion budget all
//! come for free and are **byte-identical** to the native OTLP path, so a
//! Zipkin-ingested span is queryable via trace-by-ID (#55) and TraceQL
//! search (#56) with no read-path divergence.
//!
//! **Scope: Zipkin v2 JSON only** (the issue title and `docs/api.md` §8.2
//! scope it exactly there). Zipkin v1 JSON, protobuf, and thrift are out of
//! scope and deferred.
//!
//! **All-or-nothing** (Zipkin has no partial-success channel, unlike the
//! native OTLP receiver's per-span rejection): a malformed span array, or a
//! single span carrying a non-hex / wrong-length id or an unrepresentable
//! timestamp, fails the whole request with
//! [`LogsIngestError::ZipkinDecode`] (HTTP 400 / `google.rpc.Status.code =
//! 3`). [`to_otlp`] validates every id/timestamp up front, so the adapted
//! request that reaches `otlp_traces::parse` never triggers that parser's
//! per-span rejection path.

use std::collections::BTreeMap;

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::span::{Event, SpanKind};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

use crate::error::LogsIngestError;
use crate::protocols::otlp_traces;

/// Decode-time cap on the number of spans in one request's array — the
/// structural DoS bound enforced **during** deserialization by
/// [`BoundedSpans`] (issue #75: the span `Vec` cannot grow past the cap
/// before the count is checked, so an over-cap array is rejected before it is
/// fully materialized). Matches `loki_push::MAX_STREAMS_PER_REQUEST`:
/// generous, far above anything a 64 MiB body can encode, so it never bounds
/// a legitimate batch; it only rejects a pathological count before the
/// multiplicative OTLP adaptation runs. An over-count array is a whole-request
/// 400 ([`LogsIngestError::ZipkinDecode`], the same structural-reject class a
/// malformed body uses).
pub const MAX_SPANS_PER_REQUEST: usize = 1_000_000;

/// Decode-time cap on the number of `tags` on ONE span, enforced **during**
/// deserialization (issue #75) counting RAW key/value pairs so a duplicate
/// JSON key cannot evade it — the same anti-evasion posture as
/// `loki_push::BoundedLabelMap`. Generous (2^16, far above any real span,
/// which carries a handful of tags): it only bounds the single-huge-span clone
/// that the per-block expansion charge would otherwise have to materialize
/// before it could reject. Over-cap ⇒ whole-request
/// [`LogsIngestError::ZipkinDecode`] (400/code 3).
pub const MAX_TAGS_PER_SPAN: usize = 65_536;

/// Decode-time cap on the number of `annotations` on ONE span, enforced
/// **during** deserialization (issue #75). Generous (2^16); over-cap ⇒
/// whole-request [`LogsIngestError::ZipkinDecode`]. See [`MAX_TAGS_PER_SPAN`].
pub const MAX_ANNOTATIONS_PER_SPAN: usize = 65_536;

/// One OpenZipkin v2 span (zipkin.io/zipkin-api, `zipkin2.Span`). Only the
/// v2 JSON fields this receiver maps are modeled; unknown fields are
/// ignored by serde (forward-compatible with newer Zipkin agents).
#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
pub struct ZipkinSpan {
    /// 16 lower-hex chars (64-bit) or 32 (128-bit).
    #[serde(rename = "traceId")]
    pub trace_id: String,
    /// 16 lower-hex chars (64-bit span id).
    pub id: String,
    /// Absent ⇒ root span.
    #[serde(rename = "parentId", default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    /// `CLIENT`/`SERVER`/`PRODUCER`/`CONSUMER`; absent ⇒ OTLP `INTERNAL`.
    #[serde(default)]
    pub kind: Option<String>,
    /// Epoch **microseconds**.
    #[serde(default)]
    pub timestamp: Option<i64>,
    /// **Microseconds**.
    #[serde(default)]
    pub duration: Option<i64>,
    #[serde(rename = "localEndpoint", default)]
    pub local_endpoint: Option<Endpoint>,
    #[serde(rename = "remoteEndpoint", default)]
    pub remote_endpoint: Option<Endpoint>,
    /// `BTreeMap` so the adapted span-attribute order is deterministic
    /// (sorted by key) — the adaptation golden and the payload bytes depend
    /// on a stable order. Bounded at [`MAX_TAGS_PER_SPAN`] **during**
    /// deserialization (issue #75).
    #[serde(default, deserialize_with = "deserialize_bounded_tags")]
    pub tags: BTreeMap<String, String>,
    /// Bounded at [`MAX_ANNOTATIONS_PER_SPAN`] **during** deserialization
    /// (issue #75).
    #[serde(default, deserialize_with = "deserialize_bounded_annotations")]
    pub annotations: Vec<Annotation>,
    #[serde(default)]
    pub debug: bool,
    #[serde(default)]
    pub shared: bool,
}

/// A Zipkin endpoint (`localEndpoint`/`remoteEndpoint`).
#[derive(Debug, Clone, PartialEq, Default, serde::Deserialize)]
pub struct Endpoint {
    #[serde(rename = "serviceName", default)]
    pub service_name: Option<String>,
    #[serde(default)]
    pub ipv4: Option<String>,
    #[serde(default)]
    pub ipv6: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
}

/// A Zipkin annotation — a timestamped event on the span.
#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
pub struct Annotation {
    /// Epoch **microseconds**.
    pub timestamp: i64,
    pub value: String,
}

/// Decodes a (decompressed) Zipkin v2 JSON request body — a JSON array of
/// spans — through [`BoundedSpans`], which enforces [`MAX_SPANS_PER_REQUEST`]
/// (span count), [`MAX_TAGS_PER_SPAN`] and [`MAX_ANNOTATIONS_PER_SPAN`]
/// (per-span fan-out) **during** deserialization, so an over-cap payload is
/// rejected before the span `Vec` (or a single span's tags/annotations) is
/// fully materialized (issue #75). Any structural violation — malformed body
/// or an over-cap count — is a whole-request [`LogsIngestError::ZipkinDecode`]
/// (400/code 3).
pub fn decode(body: &[u8]) -> Result<Vec<ZipkinSpan>, LogsIngestError> {
    let BoundedSpans(spans) =
        serde_json::from_slice(body).map_err(|e| LogsIngestError::ZipkinDecode(e.to_string()))?;
    Ok(spans)
}

/// The top-level span array with its element count bounded at
/// [`MAX_SPANS_PER_REQUEST`] **during** deserialization: the `SeqAccess`
/// visitor rejects the moment the accumulated count would exceed the cap, so
/// the `Vec<ZipkinSpan>` never grows past it (the DoS bound the derived
/// `Vec<ZipkinSpan>` deserialize lacked). Mirrors `loki_push::StreamsSeed`.
struct BoundedSpans(Vec<ZipkinSpan>);

impl<'de> serde::Deserialize<'de> for BoundedSpans {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct SpansVisitor;
        impl<'de> serde::de::Visitor<'de> for SpansVisitor {
            type Value = Vec<ZipkinSpan>;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an array of Zipkin v2 spans")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut spans: Vec<ZipkinSpan> = Vec::new();
                while let Some(span) = seq.next_element::<ZipkinSpan>()? {
                    if spans.len() >= MAX_SPANS_PER_REQUEST {
                        // Charge-before-allocate: reject the over-cap span
                        // without retaining the remainder of the array.
                        return Err(serde::de::Error::custom(format!(
                            "spans exceeds the {MAX_SPANS_PER_REQUEST} per-request bound"
                        )));
                    }
                    spans.push(span);
                }
                Ok(spans)
            }
        }
        deserializer.deserialize_seq(SpansVisitor).map(Self)
    }
}

/// Bounded `deserialize_with` for [`ZipkinSpan::tags`]: caps the map at
/// [`MAX_TAGS_PER_SPAN`] **during** deserialization, counting RAW pairs so a
/// duplicate JSON key cannot evade the cap (last-write-wins dedup is preserved
/// for the retained value, as with the prior `BTreeMap` deserialize). Mirrors
/// `loki_push::BoundedLabelMap`.
fn deserialize_bounded_tags<'de, D>(deserializer: D) -> Result<BTreeMap<String, String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct TagsVisitor;
    impl<'de> serde::de::Visitor<'de> for TagsVisitor {
        type Value = BTreeMap<String, String>;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a Zipkin span tag map of string values")
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::MapAccess<'de>,
        {
            let mut tags = BTreeMap::new();
            let mut seen = 0usize;
            while let Some((k, v)) = map.next_entry::<String, String>()? {
                if seen >= MAX_TAGS_PER_SPAN {
                    return Err(serde::de::Error::custom(format!(
                        "tags exceeds the {MAX_TAGS_PER_SPAN} per-span bound"
                    )));
                }
                seen += 1;
                tags.insert(k, v);
            }
            Ok(tags)
        }
    }
    deserializer.deserialize_map(TagsVisitor)
}

/// Bounded `deserialize_with` for [`ZipkinSpan::annotations`]: caps the array
/// at [`MAX_ANNOTATIONS_PER_SPAN`] **during** deserialization, rejecting the
/// moment the count would exceed the cap so the `Vec<Annotation>` never grows
/// past it.
fn deserialize_bounded_annotations<'de, D>(deserializer: D) -> Result<Vec<Annotation>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct AnnotationsVisitor;
    impl<'de> serde::de::Visitor<'de> for AnnotationsVisitor {
        type Value = Vec<Annotation>;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("an array of Zipkin span annotations")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut annotations: Vec<Annotation> = Vec::new();
            while let Some(annotation) = seq.next_element::<Annotation>()? {
                if annotations.len() >= MAX_ANNOTATIONS_PER_SPAN {
                    return Err(serde::de::Error::custom(format!(
                        "annotations exceeds the {MAX_ANNOTATIONS_PER_SPAN} per-span bound"
                    )));
                }
                annotations.push(annotation);
            }
            Ok(annotations)
        }
    }
    deserializer.deserialize_seq(AnnotationsVisitor)
}

/// Adapts a decoded Zipkin v2 span array into one OTLP
/// `ExportTraceServiceRequest` — one self-contained `ResourceSpans` per
/// span (see the module doc). Whole-request atomic: any span with an
/// invalid id or an unrepresentable timestamp fails the entire request
/// with [`LogsIngestError::ZipkinDecode`] (Zipkin is all-or-nothing).
///
/// **Charge-before-allocate (issue #75 code-review [high] fix):** the
/// adapted OTLP is *multiplicative* in the decoded input (every span's
/// self-contained block re-carries its resource) and [`MAX_SPANS_PER_REQUEST`]
/// alone (1M) bounds only the count, not the expanded bytes — so a
/// large-but-under-count request could exhaust memory here, *ahead* of the
/// [`otlp_traces::parse`] size gate this batch is later handed to. Each
/// block is therefore charged against the SAME
/// [`otlp_traces::MAX_EXPANDED_BYTES`] budget the moment it is adapted and
/// BEFORE the rest of the batch is materialized (reserve-before-materialize):
/// an over-budget request aborts mid-adaptation with
/// [`LogsIngestError::OversizeMessage`], the identical verdict the
/// equivalent native OTLP request gets from `parse` (the charge is
/// `otlp_traces`' own per-block measure).
pub fn to_otlp(spans: Vec<ZipkinSpan>) -> Result<ExportTraceServiceRequest, LogsIngestError> {
    // Never reserve output capacity for the (attacker-influenced, up to
    // `MAX_SPANS_PER_REQUEST`) decoded span count — the per-block
    // `charge_resource_spans_expansion` below is the single expansion charge
    // (issue #75 [high] fix: no allocation-before-limit, no double-count).
    let mut resource_spans = Vec::new();
    let mut expanded_bytes: usize = 0;
    for span in spans {
        let block = adapt_span(span)?;
        otlp_traces::charge_resource_spans_expansion(&mut expanded_bytes, &block)?;
        resource_spans.push(block);
    }
    Ok(ExportTraceServiceRequest { resource_spans })
}

/// One Zipkin span → one self-contained OTLP `ResourceSpans`.
fn adapt_span(zs: ZipkinSpan) -> Result<ResourceSpans, LogsIngestError> {
    let trace_id = decode_trace_id(&zs.trace_id)?;
    let span_id = decode_span_id(&zs.id, "id")?;
    let parent_span_id = match &zs.parent_id {
        Some(parent) => decode_span_id(parent, "parentId")?.to_vec(),
        None => Vec::new(),
    };
    let (start_time_unix_nano, end_time_unix_nano) = timestamps(zs.timestamp, zs.duration)?;

    // Resource attrs: the `localEndpoint` — `service.name` (promoted by
    // `otlp_traces::find_service_kv` into the `service` dimension verbatim),
    // plus its ip/port as `net.host.*`.
    let mut resource_attrs = Vec::new();
    if let Some(ep) = &zs.local_endpoint {
        if let Some(name) = &ep.service_name {
            resource_attrs.push(str_kv("service.name", name.clone()));
        }
        if let Some(ip) = ep.ipv4.as_ref().or(ep.ipv6.as_ref()) {
            resource_attrs.push(str_kv("net.host.ip", ip.clone()));
        }
        if let Some(port) = ep.port {
            resource_attrs.push(str_kv("net.host.port", port.to_string()));
        }
    }

    // Span attrs: tags (sorted), the `remoteEndpoint` as `net.peer.*`, then
    // the boolean flags (only when set).
    let mut span_attrs = Vec::new();
    for (key, val) in &zs.tags {
        span_attrs.push(str_kv(key, val.clone()));
    }
    if let Some(ep) = &zs.remote_endpoint {
        if let Some(name) = &ep.service_name {
            span_attrs.push(str_kv("net.peer.name", name.clone()));
        }
        if let Some(ip) = ep.ipv4.as_ref().or(ep.ipv6.as_ref()) {
            span_attrs.push(str_kv("net.peer.ip", ip.clone()));
        }
        if let Some(port) = ep.port {
            span_attrs.push(str_kv("net.peer.port", port.to_string()));
        }
    }
    if zs.debug {
        span_attrs.push(str_kv("zipkin.debug", "true".to_string()));
    }
    if zs.shared {
        span_attrs.push(str_kv("zipkin.shared", "true".to_string()));
    }

    // Annotations → span events (carried in the payload only, never
    // indexed — same as native OTLP events). No `with_capacity` on the
    // per-span annotation count (issue #75: never preallocate to an
    // untrusted length; the count is bounded by `MAX_ANNOTATIONS_PER_SPAN`
    // during decode, and the events grow lazily).
    let mut events = Vec::new();
    for annotation in &zs.annotations {
        events.push(Event {
            time_unix_nano: micros_to_nanos(annotation.timestamp)?,
            name: annotation.value.clone(),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
        });
    }

    let span = Span {
        trace_id: trace_id.to_vec(),
        span_id: span_id.to_vec(),
        trace_state: String::new(),
        parent_span_id,
        flags: 0,
        name: zs.name.unwrap_or_default(),
        kind: span_kind(zs.kind.as_deref()) as i32,
        start_time_unix_nano,
        end_time_unix_nano,
        attributes: span_attrs,
        dropped_attributes_count: 0,
        events,
        dropped_events_count: 0,
        links: Vec::new(),
        dropped_links_count: 0,
        // Zipkin has no OTLP-`Status` equivalent (an error is conveyed as an
        // `error` tag, which flows through `tags` above); leave it Unset.
        status: None,
    };

    Ok(ResourceSpans {
        resource: Some(Resource {
            attributes: resource_attrs,
            dropped_attributes_count: 0,
            entity_refs: Vec::new(),
        }),
        scope_spans: vec![ScopeSpans {
            scope: None,
            spans: vec![span],
            schema_url: String::new(),
        }],
        schema_url: String::new(),
    })
}

/// A string-valued OTLP attribute (every adapted Zipkin attribute is a
/// string — tags, rendered ports, promoted endpoints, boolean flags).
fn str_kv(key: &str, val: String) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(val)),
        }),
        key_strindex: 0,
    }
}

/// CLIENT/SERVER/PRODUCER/CONSUMER → the matching OTLP `SpanKind`; anything
/// else (including a missing `kind`) → `Internal` — matching the OTel
/// collector's Zipkin translator (`SPAN_KIND_UNSPECIFIED` is never
/// produced).
fn span_kind(kind: Option<&str>) -> SpanKind {
    match kind {
        Some("CLIENT") => SpanKind::Client,
        Some("SERVER") => SpanKind::Server,
        Some("PRODUCER") => SpanKind::Producer,
        Some("CONSUMER") => SpanKind::Consumer,
        _ => SpanKind::Internal,
    }
}

/// Zipkin `traceId`: 16 hex chars (64-bit) → 8 bytes **left-padded with 8
/// zero bytes** to 16; 32 hex chars (128-bit) → 16 bytes verbatim. The
/// left-pad is load-bearing: it is `parse_span`'s exactly-16-byte
/// requirement AND the fingerprint-identity gate — the stored bytes are
/// identical whether the trace arrives via Zipkin or OTLP.
fn decode_trace_id(hex: &str) -> Result<[u8; 16], LogsIngestError> {
    match hex.len() {
        16 => {
            let low = decode_hex8(hex, "traceId")?;
            let mut out = [0u8; 16];
            out[8..].copy_from_slice(&low);
            Ok(out)
        }
        32 => {
            let bytes = decode_hex_exact::<16>(hex, "traceId")?;
            Ok(bytes)
        }
        other => Err(LogsIngestError::ZipkinDecode(format!(
            "traceId must be 16 or 32 hex chars, got {other}"
        ))),
    }
}

/// Zipkin `id`/`parentId`: exactly 16 hex chars → 8 bytes.
fn decode_span_id(hex: &str, field: &str) -> Result<[u8; 8], LogsIngestError> {
    if hex.len() != 16 {
        return Err(LogsIngestError::ZipkinDecode(format!(
            "{field} must be 16 hex chars, got {}",
            hex.len()
        )));
    }
    decode_hex8(hex, field)
}

fn decode_hex8(hex: &str, field: &str) -> Result<[u8; 8], LogsIngestError> {
    decode_hex_exact::<8>(hex, field)
}

/// Decodes exactly `N` bytes (`2*N` hex chars) — the length is checked by
/// the caller; this validates the hex digits themselves.
fn decode_hex_exact<const N: usize>(hex: &str, field: &str) -> Result<[u8; N], LogsIngestError> {
    let bytes = hex.as_bytes();
    debug_assert_eq!(bytes.len(), 2 * N);
    let mut out = [0u8; N];
    for (i, slot) in out.iter_mut().enumerate() {
        let hi = hex_nibble(bytes[2 * i], field)?;
        let lo = hex_nibble(bytes[2 * i + 1], field)?;
        *slot = (hi << 4) | lo;
    }
    Ok(out)
}

fn hex_nibble(byte: u8, field: &str) -> Result<u8, LogsIngestError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(LogsIngestError::ZipkinDecode(format!(
            "{field} contains a non-hex character"
        ))),
    }
}

/// `(start_time_unix_nano, end_time_unix_nano)` from Zipkin's microsecond
/// `timestamp`/`duration`. Absent `timestamp` ⇒ start `0` (`otlp_traces::
/// parse` falls back to `now_ns`) and end `0` (⇒ duration 0). An i64
/// overflow in the micros→nanos conversion or in `timestamp + duration` is
/// a whole-request [`LogsIngestError::ZipkinDecode`].
fn timestamps(
    timestamp: Option<i64>,
    duration: Option<i64>,
) -> Result<(u64, u64), LogsIngestError> {
    let start = match timestamp {
        None => 0,
        Some(ts) => micros_to_nanos(ts)?,
    };
    let end = match (timestamp, duration) {
        (Some(ts), Some(dur)) => {
            let end_micros = ts.checked_add(dur).ok_or_else(|| {
                LogsIngestError::ZipkinDecode("timestamp + duration overflows i64".to_string())
            })?;
            micros_to_nanos(end_micros)?
        }
        _ => 0,
    };
    Ok((start, end))
}

/// Microseconds → nanoseconds (`× 1000`) as a `u64` OTLP timestamp. Rejects
/// an i64-overflowing or negative result as a whole-request decode error.
fn micros_to_nanos(micros: i64) -> Result<u64, LogsIngestError> {
    let nanos = micros.checked_mul(1000).ok_or_else(|| {
        LogsIngestError::ZipkinDecode("microsecond timestamp overflows i64 nanoseconds".to_string())
    })?;
    u64::try_from(nanos)
        .map_err(|_| LogsIngestError::ZipkinDecode("negative microsecond timestamp".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A worked example decoded straight from JSON (the wire form), then
    /// pinned field-by-field against the adapted OTLP span — the adaptation
    /// golden: 64-bit trace-id left-pad, micros→nanos, kind map,
    /// localEndpoint→service+net.host, remoteEndpoint→net.peer, tags→span
    /// attrs, annotations→events.
    #[test]
    fn adaptation_golden_maps_every_field() {
        let body = br#"[
          {
            "traceId": "0000000000000001",
            "id": "0000000000000002",
            "parentId": "0000000000000003",
            "name": "get /widgets",
            "kind": "CLIENT",
            "timestamp": 1700000000000000,
            "duration": 1500,
            "localEndpoint": {"serviceName": "frontend", "ipv4": "10.0.0.1", "port": 8080},
            "remoteEndpoint": {"serviceName": "backend", "ipv4": "10.0.0.2", "port": 9090},
            "tags": {"http.method": "GET", "http.status_code": "200"},
            "annotations": [{"timestamp": 1700000000000100, "value": "cache.miss"}],
            "debug": true,
            "shared": false
          }
        ]"#;
        let spans = decode(body).expect("valid zipkin json");
        assert_eq!(spans.len(), 1);
        let req = to_otlp(spans).expect("adapt");
        assert_eq!(req.resource_spans.len(), 1);
        let rs = &req.resource_spans[0];

        // Resource: localEndpoint promoted.
        let resource = rs.resource.as_ref().expect("resource present");
        assert_eq!(
            resource.attributes,
            vec![
                str_kv("service.name", "frontend".to_string()),
                str_kv("net.host.ip", "10.0.0.1".to_string()),
                str_kv("net.host.port", "8080".to_string()),
            ]
        );

        let span = &rs.scope_spans[0].spans[0];
        // 64-bit trace id left-padded to 16 bytes.
        assert_eq!(
            span.trace_id,
            vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
        );
        assert_eq!(span.span_id, vec![0, 0, 0, 0, 0, 0, 0, 2]);
        assert_eq!(span.parent_span_id, vec![0, 0, 0, 0, 0, 0, 0, 3]);
        assert_eq!(span.name, "get /widgets");
        assert_eq!(span.kind, SpanKind::Client as i32);
        // micros → nanos.
        assert_eq!(span.start_time_unix_nano, 1_700_000_000_000_000_000);
        assert_eq!(span.end_time_unix_nano, 1_700_000_000_001_500_000);

        // Span attrs: sorted tags, then remoteEndpoint → net.peer.*, then
        // the debug flag.
        assert_eq!(
            span.attributes,
            vec![
                str_kv("http.method", "GET".to_string()),
                str_kv("http.status_code", "200".to_string()),
                str_kv("net.peer.name", "backend".to_string()),
                str_kv("net.peer.ip", "10.0.0.2".to_string()),
                str_kv("net.peer.port", "9090".to_string()),
                str_kv("zipkin.debug", "true".to_string()),
            ]
        );

        // Annotation → event.
        assert_eq!(span.events.len(), 1);
        assert_eq!(span.events[0].name, "cache.miss");
        assert_eq!(span.events[0].time_unix_nano, 1_700_000_000_000_100_000);
    }

    #[test]
    fn trace_id_128_bit_is_verbatim() {
        let id = decode_trace_id("00112233445566778899aabbccddeeff").expect("128-bit id");
        assert_eq!(
            id,
            [
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
                0xee, 0xff
            ]
        );
    }

    #[test]
    fn trace_id_64_bit_is_left_padded() {
        let id = decode_trace_id("00000000deadbeef").expect("64-bit id");
        assert_eq!(
            id,
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xde, 0xad, 0xbe, 0xef]
        );
    }

    #[test]
    fn missing_kind_maps_to_internal() {
        assert_eq!(span_kind(None), SpanKind::Internal);
        assert_eq!(span_kind(Some("SERVER")), SpanKind::Server);
        assert_eq!(span_kind(Some("PRODUCER")), SpanKind::Producer);
        assert_eq!(span_kind(Some("CONSUMER")), SpanKind::Consumer);
        // An unknown value degrades to INTERNAL rather than erroring.
        assert_eq!(span_kind(Some("weird")), SpanKind::Internal);
    }

    #[test]
    fn absent_parent_id_is_a_root_span() {
        let body = br#"[{"traceId":"0000000000000001","id":"0000000000000002"}]"#;
        let req = to_otlp(decode(body).expect("json")).expect("adapt");
        let span = &req.resource_spans[0].scope_spans[0].spans[0];
        assert!(span.parent_span_id.is_empty(), "root span has empty parent");
    }

    #[test]
    fn absent_timestamp_yields_zero_start_and_end() {
        let body = br#"[{"traceId":"0000000000000001","id":"0000000000000002"}]"#;
        let req = to_otlp(decode(body).expect("json")).expect("adapt");
        let span = &req.resource_spans[0].scope_spans[0].spans[0];
        assert_eq!(span.start_time_unix_nano, 0);
        assert_eq!(span.end_time_unix_nano, 0);
    }

    #[test]
    fn shared_flag_becomes_a_span_attribute() {
        let body = br#"[{"traceId":"0000000000000001","id":"0000000000000002","kind":"SERVER","shared":true}]"#;
        let req = to_otlp(decode(body).expect("json")).expect("adapt");
        let span = &req.resource_spans[0].scope_spans[0].spans[0];
        assert!(
            span.attributes
                .contains(&str_kv("zipkin.shared", "true".to_string()))
        );
        assert_eq!(span.kind, SpanKind::Server as i32);
    }

    #[test]
    fn ipv6_is_used_when_ipv4_is_absent() {
        let body = br#"[{"traceId":"0000000000000001","id":"0000000000000002","localEndpoint":{"serviceName":"s","ipv6":"::1"}}]"#;
        let req = to_otlp(decode(body).expect("json")).expect("adapt");
        let resource = req.resource_spans[0].resource.as_ref().expect("resource");
        assert!(
            resource
                .attributes
                .contains(&str_kv("net.host.ip", "::1".to_string()))
        );
    }

    #[test]
    fn malformed_json_is_a_zipkin_decode_error() {
        let err = decode(b"not json").expect_err("malformed");
        assert!(matches!(err, LogsIngestError::ZipkinDecode(_)));
    }

    #[test]
    fn non_hex_trace_id_is_a_zipkin_decode_error() {
        let body = br#"[{"traceId":"zzzzzzzzzzzzzzzz","id":"0000000000000002"}]"#;
        let err = to_otlp(decode(body).expect("json")).expect_err("bad hex");
        assert!(matches!(err, LogsIngestError::ZipkinDecode(_)));
    }

    #[test]
    fn wrong_length_span_id_is_a_zipkin_decode_error() {
        let body = br#"[{"traceId":"0000000000000001","id":"00"}]"#;
        let err = to_otlp(decode(body).expect("json")).expect_err("short id");
        assert!(matches!(err, LogsIngestError::ZipkinDecode(_)));
    }

    #[test]
    fn overflowing_timestamp_is_a_zipkin_decode_error() {
        let err = micros_to_nanos(i64::MAX).expect_err("overflow");
        assert!(matches!(err, LogsIngestError::ZipkinDecode(_)));
    }

    /// Runs [`decode`] on a body expected to be rejected and returns the
    /// [`LogsIngestError::ZipkinDecode`] message (the bounded-visitor's
    /// `serde::de::Error::custom` text) — the non-vacuity signal that the
    /// reject fired inside the bounded deserialize, not some later gate.
    fn zipkin_decode_message(body: &[u8]) -> String {
        match decode(body) {
            Err(LogsIngestError::ZipkinDecode(msg)) => msg,
            other => panic!("expected ZipkinDecode, got {other:?}"),
        }
    }

    /// Issue #75 (span count): an array of more than
    /// [`MAX_SPANS_PER_REQUEST`] spans is rejected **during** deserialization
    /// by [`BoundedSpans`] — the `Vec<ZipkinSpan>` is never grown past the cap
    /// before the count is checked. Minimal empty-id spans (id/hex validation
    /// happens later in `adapt_span`, not at deserialize) keep the body small
    /// while still forcing the count cap. The bounded-seed message is the
    /// non-vacuity proxy vs. the derived `Vec<ZipkinSpan>` (which accepted any
    /// count).
    #[test]
    fn too_many_spans_rejected_during_deserialize() {
        let mut body = String::with_capacity(24 * MAX_SPANS_PER_REQUEST);
        body.push('[');
        for i in 0..=MAX_SPANS_PER_REQUEST {
            if i > 0 {
                body.push(',');
            }
            body.push_str(r#"{"traceId":"","id":""}"#);
        }
        body.push(']');
        let msg = zipkin_decode_message(body.as_bytes());
        assert!(
            msg.contains("spans exceeds"),
            "the reject must be the bounded-seed spans message: {msg:?}"
        );
    }

    /// Issue #75 (tags/span): one span carrying more than
    /// [`MAX_TAGS_PER_SPAN`] tags is rejected **during** deserialization by
    /// [`deserialize_bounded_tags`], before the `BTreeMap` (and later the
    /// adapted span attributes) fully materialize.
    #[test]
    fn too_many_tags_per_span_rejected_during_deserialize() {
        let mut body =
            String::from(r#"[{"traceId":"0000000000000001","id":"0000000000000002","tags":{"#);
        for i in 0..=MAX_TAGS_PER_SPAN {
            if i > 0 {
                body.push(',');
            }
            body.push_str(&format!(r#""k{i}":"v""#));
        }
        body.push_str("}}]");
        let msg = zipkin_decode_message(body.as_bytes());
        assert!(
            msg.contains("tags exceeds"),
            "the reject must be the bounded tag-map message: {msg:?}"
        );
    }

    /// Issue #75 anti-evasion (tags/span): a tag map whose keys are all the
    /// SAME string would collapse to one entry in a `BTreeMap`, evading the
    /// cap; counting RAW pairs during the visit rejects it.
    #[test]
    fn duplicate_tag_keys_cannot_evade_the_tag_cap() {
        let mut body =
            String::from(r#"[{"traceId":"0000000000000001","id":"0000000000000002","tags":{"#);
        for i in 0..=MAX_TAGS_PER_SPAN {
            if i > 0 {
                body.push(',');
            }
            body.push_str(r#""dup":"v""#);
        }
        body.push_str("}}]");
        let msg = zipkin_decode_message(body.as_bytes());
        assert!(
            msg.contains("tags exceeds"),
            "duplicate keys must still trip the RAW-pair tag cap: {msg:?}"
        );
    }

    /// Issue #75 (annotations/span): one span carrying more than
    /// [`MAX_ANNOTATIONS_PER_SPAN`] annotations is rejected **during**
    /// deserialization by [`deserialize_bounded_annotations`].
    #[test]
    fn too_many_annotations_per_span_rejected_during_deserialize() {
        let mut body = String::from(
            r#"[{"traceId":"0000000000000001","id":"0000000000000002","annotations":["#,
        );
        for i in 0..=MAX_ANNOTATIONS_PER_SPAN {
            if i > 0 {
                body.push(',');
            }
            body.push_str(r#"{"timestamp":1,"value":"x"}"#);
        }
        body.push_str("]}]");
        let msg = zipkin_decode_message(body.as_bytes());
        assert!(
            msg.contains("annotations exceeds"),
            "the reject must be the bounded annotations message: {msg:?}"
        );
    }

    /// Positive (no false reject): a span with exactly [`MAX_TAGS_PER_SPAN`]
    /// distinct tags and [`MAX_ANNOTATIONS_PER_SPAN`] annotations — at the
    /// boundary — still decodes, and a small valid array round-trips through
    /// `to_otlp`. Confirms the caps admit at-cap input and never reject
    /// legitimate traffic.
    #[test]
    fn at_cap_tags_and_annotations_still_decode() {
        let mut body =
            String::from(r#"[{"traceId":"0000000000000001","id":"0000000000000002","tags":{"#);
        for i in 0..MAX_TAGS_PER_SPAN {
            if i > 0 {
                body.push(',');
            }
            body.push_str(&format!(r#""k{i}":"v""#));
        }
        body.push_str(r#"},"annotations":["#);
        for i in 0..MAX_ANNOTATIONS_PER_SPAN {
            if i > 0 {
                body.push(',');
            }
            body.push_str(r#"{"timestamp":1,"value":"x"}"#);
        }
        body.push_str("]}]");
        let spans = decode(body.as_bytes()).expect("at-cap span decodes");
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].tags.len(), MAX_TAGS_PER_SPAN);
        assert_eq!(spans[0].annotations.len(), MAX_ANNOTATIONS_PER_SPAN);

        // A small, ordinary array still decodes and adapts fine.
        let ok = br#"[{"traceId":"0000000000000001","id":"0000000000000002"}]"#;
        let decoded = decode(ok).expect("under bound");
        assert_eq!(decoded.len(), 1);
        assert_eq!(to_otlp(decoded).expect("adapt").resource_spans.len(), 1);
    }

    /// Issue #75 code-review [high] fix: a request whose span COUNT is far
    /// under [`MAX_SPANS_PER_REQUEST`] but whose adapted OTLP expands past
    /// [`otlp_traces::MAX_EXPANDED_BYTES`] is rejected as the named
    /// oversize error DURING adaptation — charged block-by-block against the
    /// same budget `otlp_traces::parse` enforces, and aborted the moment the
    /// running total crosses it, BEFORE the whole batch is materialized.
    ///
    /// Two-sided, retune-proof (all counts derive from the live
    /// budget/charge). The crafted spans each carry a large
    /// `localEndpoint.serviceName`, so one adapted block's charge `c`
    /// (measured through `otlp_traces`' own per-block measure) dominates;
    /// `trip_at = budget/c + 1` blocks cross the budget, and the input holds
    /// a handful MORE. The discriminating proxy: an incremental
    /// charge-before-allocate aborts at the first crossing block, so the
    /// reported `actual` lands in `(budget, budget + c]`; a
    /// materialize-then-check would instead have summed ALL blocks to
    /// `total * c` (many `c` past the budget) before noticing — the asserted
    /// `actual <= budget + c` bound holds only for the early abort.
    #[test]
    fn expansion_budget_aborts_during_adaptation_before_materializing_all() {
        // Big enough that one block's charge is a meaningful slice of the
        // budget (so `trip_at` stays small and the input is bounded), yet a
        // single span alone cannot exceed it.
        const SERVICE_LEN: usize = 1024 * 1024;
        let big_service = "s".repeat(SERVICE_LEN);
        let sample = |service: &str| ZipkinSpan {
            trace_id: "0000000000000001".to_string(),
            id: "0000000000000002".to_string(),
            parent_id: None,
            name: None,
            kind: None,
            timestamp: Some(1_700_000_000_000_000),
            duration: Some(1000),
            local_endpoint: Some(Endpoint {
                service_name: Some(service.to_string()),
                ipv4: None,
                ipv6: None,
                port: None,
            }),
            remote_endpoint: None,
            tags: BTreeMap::new(),
            annotations: Vec::new(),
            debug: false,
            shared: false,
        };

        // `c`: one adapted block's charge, via `otlp_traces`' OWN per-block
        // measure — so the two paths provably agree and a budget retune
        // reshapes this test automatically.
        let mut c: usize = 0;
        otlp_traces::charge_resource_spans_expansion(
            &mut c,
            &adapt_span(sample(&big_service)).expect("adapt one block"),
        )
        .expect("a single block is well under the budget");
        assert!(c > 0 && c < otlp_traces::MAX_EXPANDED_BYTES);

        let trip_at = otlp_traces::MAX_EXPANDED_BYTES / c + 1;
        // A few MORE blocks than needed to trip: if adaptation materialized
        // all before checking, `actual` would be `total * c` (>= budget +
        // 16c), not the `<= budget + c` an early abort yields.
        let total = trip_at + 16;
        assert!(
            total < MAX_SPANS_PER_REQUEST,
            "the trip must happen well under the span-count cap ({total} < {MAX_SPANS_PER_REQUEST})"
        );

        let spans: Vec<ZipkinSpan> = (0..total).map(|_| sample(&big_service)).collect();
        // `is_err` (not `expect_err`) so a regression to the un-charged
        // `to_otlp` does not Debug-dump the ~100 MiB materialized batch.
        let result = to_otlp(spans);
        assert!(
            result.is_err(),
            "over-budget adaptation must be rejected before materializing the batch"
        );
        match result.expect_err("checked is_err above") {
            LogsIngestError::OversizeMessage { limit, actual, .. } => {
                assert_eq!(
                    limit,
                    otlp_traces::MAX_EXPANDED_BYTES,
                    "same budget as parse"
                );
                assert!(
                    actual > otlp_traces::MAX_EXPANDED_BYTES,
                    "the running total did cross the budget: {actual}"
                );
                // The load-bearing proxy: the abort fired at the FIRST block
                // to cross the budget (charge-before-allocate), not after
                // summing every block (materialize-then-check would report
                // ~`total * c`, i.e. >= budget + 16c).
                assert!(
                    actual <= otlp_traces::MAX_EXPANDED_BYTES + c,
                    "abort must happen at the first over-budget block (actual {actual} \
                     <= budget {} + one block {c}), not after materializing all {total} \
                     blocks (~{})",
                    otlp_traces::MAX_EXPANDED_BYTES,
                    total.saturating_mul(c)
                );
            }
            other => panic!("expected OversizeMessage, got {other:?}"),
        }
    }
}
