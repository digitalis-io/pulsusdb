//! Trace assembly (issue #55): decode each stored per-span payload as the
//! pinned issue #54 contract type (a self-contained single-`ResourceSpans`
//! `TracesData` — `pulsus-write/src/protocols/otlp_traces.rs::build_payload`),
//! de-duplicate at-least-once replays by `span_id`, and concatenate every
//! surviving `ResourceSpans` into one valid `TracesData`. Pure functions,
//! unit-tested — the OTLP layer lives here so `pulsus-read` stays
//! OTLP-agnostic (task-manager adjudication, open question 1).
//!
//! **Dedup is a total order, evaluated per `span_id`** (plan v3 §1):
//! `trace_spans` is a plain `MergeTree` (no dedup engine, no ingest-order/
//! version column), so at-least-once duplicates are physically retained
//! and row order carries no tiebreak information. The winner per `span_id`
//! is the row maximal by
//! `((payload_type == 1) as u8, payload.len(), payload_bytes, payload_type)`:
//! a supported row always beats an unsupported duplicate (serving the
//! supported copy is honest — the unsupported duplicate is version-skew
//! noise), and the remaining components break every tie deterministically,
//! including identical bytes under different `payload_type`s. The same
//! winner emerges regardless of the order ClickHouse returned rows in.
//!
//! **Unsupported `payload_type` ⇒ explicit 500, never a partial 200**
//! (plan v2 §3 / v3 §1): the rule is evaluated on POST-dedup *winners*
//! only, so a span with both a supported and an unsupported copy serves
//! the supported row (200), while a span with no supported copy fails the
//! whole fetch — a silent partial trace would lie to the caller.
//!
//! **Canonical output order** (plan v3 §2): retained spans are sorted by
//! `(start_time_unix_nano, span_id)` before concatenation — deterministic
//! response bytes/JSON regardless of ClickHouse read order or map
//! iteration order (`span_id` totalizes equal-start ties, since span ids
//! are unique post-dedup). Documented in docs/api.md §4.1.

use std::collections::HashMap;

use opentelemetry_proto::tonic::trace::v1::TracesData;
use prost::Message;
use pulsus_read::StoredSpan;
use thiserror::Error;

/// The one `payload_type` this assembler understands (docs/schemas.md
/// §4.1: `1 = OTLP protobuf`; `2 = Zipkin JSON` is a compat-receiver
/// concern no writer produces yet).
const PAYLOAD_TYPE_OTLP: i8 = 1;

/// Errors from assembling stored spans into a `TracesData` — mapped to
/// `500 internal` by `error::ApiError` (both variants indicate stored-data
/// version skew or corruption, never caller error).
#[derive(Debug, Error)]
pub(crate) enum AssembleError {
    /// One or more post-dedup winners carry a `payload_type` this build
    /// cannot decode — version skew; a partial (or empty) `200` would lie
    /// to the caller (plan v2 §3).
    #[error("unsupported payload_type on {count} span(s)")]
    UnsupportedPayloadType { count: usize },
    /// A `payload_type == 1` payload failed to decode as `TracesData` —
    /// stored-data corruption.
    #[error("stored payload for span {span_id_hex} failed to decode: {source}")]
    Decode {
        span_id_hex: String,
        source: prost::DecodeError,
    },
    /// The protojson rendering of an already-assembled `TracesData`
    /// failed — should be unreachable (the `with-serde` serializers have
    /// no fallible shapes for these message types), kept as a structured
    /// 500 rather than a panic.
    #[error("protojson encoding failed: {0}")]
    EncodeJson(#[from] serde_json::Error),
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The plan v3 §1 total-order dedup key (see the module doc). Borrows the
/// span so comparing two candidates allocates nothing.
fn dedup_key(span: &StoredSpan) -> (u8, usize, &[u8], i8) {
    (
        u8::from(span.payload_type == PAYLOAD_TYPE_OTLP),
        span.payload.len(),
        span.payload.as_slice(),
        span.payload_type,
    )
}

/// The smallest `start_time_unix_nano` across the decoded payload's spans
/// (the pinned contract carries exactly one span; `min` is the defensive
/// generalization) — the canonical-ordering sort key's first component.
fn start_time_ns(data: &TracesData) -> u64 {
    data.resource_spans
        .iter()
        .flat_map(|rs| &rs.scope_spans)
        .flat_map(|ss| &ss.spans)
        .map(|s| s.start_time_unix_nano)
        .min()
        .unwrap_or(0)
}

/// Decode + dedup + order + merge (module doc has the full contract).
/// Empty input yields an empty `TracesData` — the handler maps an empty
/// *fetch* to `404` before ever calling this, so the empty case only
/// matters for the unit-level contract.
pub(crate) fn assemble(spans: Vec<StoredSpan>) -> Result<TracesData, AssembleError> {
    // Order-independent dedup: reduce into a map keyed by span_id, keeping
    // the row maximal under the total-order key.
    let mut winners: HashMap<[u8; 8], StoredSpan> = HashMap::with_capacity(spans.len());
    for span in spans {
        match winners.entry(span.span_id) {
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(span);
            }
            std::collections::hash_map::Entry::Occupied(mut slot) => {
                if dedup_key(&span) > dedup_key(slot.get()) {
                    slot.insert(span);
                }
            }
        }
    }

    // The unsupported-type rule runs on winners only (plan v3 §1): a span
    // whose supported copy won is served; a span with no supported copy
    // fails the fetch.
    let unsupported = winners
        .values()
        .filter(|s| s.payload_type != PAYLOAD_TYPE_OTLP)
        .count();
    if unsupported > 0 {
        return Err(AssembleError::UnsupportedPayloadType { count: unsupported });
    }

    let mut decoded: Vec<([u8; 8], TracesData)> = Vec::with_capacity(winners.len());
    for (span_id, span) in winners {
        let data = TracesData::decode(span.payload.as_slice()).map_err(|source| {
            AssembleError::Decode {
                span_id_hex: hex(&span_id),
                source,
            }
        })?;
        decoded.push((span_id, data));
    }

    // Canonical output order (plan v3 §2).
    decoded.sort_by(|(a_id, a), (b_id, b)| (start_time_ns(a), a_id).cmp(&(start_time_ns(b), b_id)));

    Ok(TracesData {
        resource_spans: decoded
            .into_iter()
            .flat_map(|(_, data)| data.resource_spans)
            .collect(),
    })
}

/// The protobuf rendering (`Content-Type: application/protobuf`).
pub(crate) fn encode_protobuf(data: &TracesData) -> Vec<u8> {
    data.encode_to_vec()
}

/// The OTLP-canonical protojson rendering (`Content-Type:
/// application/json`): the crate's own `with-serde` serializers — hex
/// trace/span ids, camelCase field names, u64 as strings — so T9's Tempo
/// alias needs no shape translation.
pub(crate) fn encode_json(data: &TracesData) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    fn kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(value.to_string())),
            }),
            key_strindex: 0,
        }
    }

    /// Mirrors the pinned issue #54 payload contract: one self-contained
    /// single-`ResourceSpans` `TracesData` per span, with its resource and
    /// scope context.
    fn payload(span_id: [u8; 8], name: &str, start_ns: u64) -> Vec<u8> {
        TracesData {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", "checkout")],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "test-scope".to_string(),
                        version: String::new(),
                        attributes: vec![kv("scope.attr", "sv")],
                        dropped_attributes_count: 0,
                    }),
                    spans: vec![Span {
                        trace_id: vec![0xab; 16],
                        span_id: span_id.to_vec(),
                        name: name.to_string(),
                        start_time_unix_nano: start_ns,
                        end_time_unix_nano: start_ns + 1_000,
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
        .encode_to_vec()
    }

    fn stored(span_id: [u8; 8], payload_type: i8, payload: Vec<u8>) -> StoredSpan {
        StoredSpan {
            span_id,
            payload_type,
            payload,
        }
    }

    fn span_names(data: &TracesData) -> Vec<String> {
        data.resource_spans
            .iter()
            .flat_map(|rs| &rs.scope_spans)
            .flat_map(|ss| &ss.spans)
            .map(|s| s.name.clone())
            .collect()
    }

    fn span_ids(data: &TracesData) -> Vec<Vec<u8>> {
        data.resource_spans
            .iter()
            .flat_map(|rs| &rs.scope_spans)
            .flat_map(|ss| &ss.spans)
            .map(|s| s.span_id.clone())
            .collect()
    }

    #[test]
    fn identical_replays_dedup_to_one_span_each() {
        let a = stored([1; 8], 1, payload([1; 8], "span-a", 10));
        let b = stored([2; 8], 1, payload([2; 8], "span-b", 20));
        let dup_a = stored([1; 8], 1, payload([1; 8], "span-a", 10));
        let out = assemble(vec![a, b, dup_a]).expect("assemble");
        assert_eq!(out.resource_spans.len(), 2);
        assert_eq!(span_ids(&out), vec![vec![1u8; 8], vec![2u8; 8]]);
        assert_eq!(span_names(&out), vec!["span-a", "span-b"]);
    }

    #[test]
    fn empty_input_yields_an_empty_traces_data() {
        let out = assemble(Vec::new()).expect("assemble");
        assert!(out.resource_spans.is_empty());
    }

    /// Plan v2 §3: an all-unsupported trace is an explicit error, never an
    /// empty 200/404 masquerade.
    #[test]
    fn all_unsupported_payload_types_error_with_the_count() {
        let a = stored([1; 8], 2, b"zipkin-json-a".to_vec());
        let b = stored([2; 8], 2, b"zipkin-json-b".to_vec());
        match assemble(vec![a, b]) {
            Err(AssembleError::UnsupportedPayloadType { count }) => assert_eq!(count, 2),
            other => panic!("expected UnsupportedPayloadType, got {other:?}"),
        }
    }

    /// Plan v2 §3: a partially-unsupported trace (distinct span_ids) is an
    /// error, never a partial 200.
    #[test]
    fn a_partially_unsupported_trace_errors_rather_than_serving_a_partial_200() {
        let supported = stored([1; 8], 1, payload([1; 8], "span-a", 10));
        let unsupported = stored([2; 8], 2, b"zipkin-json".to_vec());
        match assemble(vec![supported, unsupported]) {
            Err(AssembleError::UnsupportedPayloadType { count }) => assert_eq!(count, 1),
            other => panic!("expected UnsupportedPayloadType, got {other:?}"),
        }
    }

    /// Plan v3 §1: a span with both a supported and an unsupported copy
    /// serves the supported row — 200, not 500 — under both input orders.
    #[test]
    fn a_supported_copy_beats_an_unsupported_duplicate_in_both_orders() {
        let supported = stored([1; 8], 1, payload([1; 8], "span-a", 10));
        let unsupported = stored([1; 8], 2, b"some-longer-unsupported-payload".to_vec());
        for input in [
            vec![supported.clone(), unsupported.clone()],
            vec![unsupported.clone(), supported.clone()],
        ] {
            let out = assemble(input).expect("supported copy must win");
            assert_eq!(span_names(&out), vec!["span-a"]);
        }
    }

    /// Plan v2 §2 / v3 §1: conflicting duplicate payloads resolve to the
    /// identical winner under both input orders.
    #[test]
    fn conflicting_duplicate_payloads_yield_the_same_winner_in_both_orders() {
        let short = stored([1; 8], 1, payload([1; 8], "v1", 10));
        let long = stored([1; 8], 1, payload([1; 8], "v2-with-a-longer-name", 10));
        let a = assemble(vec![short.clone(), long.clone()]).expect("assemble");
        let b = assemble(vec![long, short]).expect("assemble");
        assert_eq!(a, b);
        // Longer payload wins under the (len, bytes) components.
        assert_eq!(span_names(&a), vec!["v2-with-a-longer-name"]);
    }

    /// Plan v3 §1 (the v2 gap): identical bytes under different
    /// `payload_type`s still resolve deterministically — the supported
    /// type-1 copy wins in both orders.
    #[test]
    fn identical_bytes_with_conflicting_payload_types_resolve_deterministically() {
        let bytes = payload([1; 8], "span-a", 10);
        let as_otlp = stored([1; 8], 1, bytes.clone());
        let as_other = stored([1; 8], 2, bytes);
        for input in [
            vec![as_otlp.clone(), as_other.clone()],
            vec![as_other.clone(), as_otlp.clone()],
        ] {
            let out = assemble(input).expect("the type-1 copy must win, not 500");
            assert_eq!(span_names(&out), vec!["span-a"]);
        }
    }

    /// Plan v3 §2: retained spans come back ordered by
    /// `(start_time_unix_nano, span_id)` under every input permutation.
    #[test]
    fn output_order_is_canonical_across_input_permutations() {
        // span 3 starts earliest; spans 1 and 2 share a start time (span_id
        // breaks the tie).
        let s1 = stored([1; 8], 1, payload([1; 8], "s1", 50));
        let s2 = stored([2; 8], 1, payload([2; 8], "s2", 50));
        let s3 = stored([3; 8], 1, payload([3; 8], "s3", 10));
        let expected = vec!["s3".to_string(), "s1".to_string(), "s2".to_string()];
        let perms: Vec<Vec<StoredSpan>> = vec![
            vec![s1.clone(), s2.clone(), s3.clone()],
            vec![s3.clone(), s2.clone(), s1.clone()],
            vec![s2.clone(), s3.clone(), s1.clone()],
            vec![s2.clone(), s1.clone(), s3.clone()],
        ];
        let mut renderings = Vec::new();
        for input in perms {
            let out = assemble(input).expect("assemble");
            assert_eq!(span_names(&out), expected);
            renderings.push(encode_json(&out).expect("encode json"));
        }
        assert!(
            renderings.windows(2).all(|w| w[0] == w[1]),
            "JSON renderings must be byte-identical across input permutations"
        );
    }

    /// v2 test-gap closure: the ratified resource and scope context
    /// survives assembly on every `ResourceSpans`/`ScopeSpans`.
    #[test]
    fn resource_and_scope_context_survive_assembly_per_span() {
        let a = stored([1; 8], 1, payload([1; 8], "span-a", 10));
        let b = stored([2; 8], 1, payload([2; 8], "span-b", 20));
        let out = assemble(vec![b, a]).expect("assemble");
        assert_eq!(out.resource_spans.len(), 2);
        for rs in &out.resource_spans {
            let resource = rs.resource.as_ref().expect("resource preserved");
            assert_eq!(resource.attributes, vec![kv("service.name", "checkout")]);
            assert_eq!(rs.scope_spans.len(), 1);
            let scope = rs.scope_spans[0].scope.as_ref().expect("scope preserved");
            assert_eq!(scope.name, "test-scope");
            assert_eq!(scope.attributes, vec![kv("scope.attr", "sv")]);
        }
    }

    #[test]
    fn an_undecodable_supported_payload_is_a_decode_error() {
        let bad = stored([9; 8], 1, b"\xff\xff not protobuf".to_vec());
        match assemble(vec![bad]) {
            Err(AssembleError::Decode { span_id_hex, .. }) => {
                assert_eq!(span_id_hex, "0909090909090909");
            }
            other => panic!("expected Decode, got {other:?}"),
        }
    }

    #[test]
    fn encode_protobuf_round_trips_through_prost() {
        let out =
            assemble(vec![stored([1; 8], 1, payload([1; 8], "span-a", 10))]).expect("assemble");
        let bytes = encode_protobuf(&out);
        let back = TracesData::decode(bytes.as_slice()).expect("round trip");
        assert_eq!(back, out);
    }

    /// The `with-serde` protojson shape: hex span ids, camelCase keys,
    /// u64 timestamps as strings.
    #[test]
    fn encode_json_is_otlp_canonical_protojson() {
        let out =
            assemble(vec![stored([1; 8], 1, payload([1; 8], "span-a", 10))]).expect("assemble");
        let json: serde_json::Value =
            serde_json::from_slice(&encode_json(&out).expect("encode")).expect("valid json");
        let span = &json["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
        assert_eq!(span["spanId"], "0101010101010101");
        assert_eq!(span["traceId"], "ab".repeat(16));
        assert_eq!(span["startTimeUnixNano"], "10");
    }
}
