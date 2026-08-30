//! `GET /api/v2/traces/{traceId}` (issue #474) — the fourteenth compat
//! alias and the only one that wraps the fetch result in its own envelope.
//!
//! The reference build answers this route `200` with a present-but-empty
//! trace when the id is unknown, where its v1 route answers `404`; the
//! client always tries v2 first and falls back to v1 on `404`, so the
//! missing route cost one wasted request per trace open and turned "no
//! trace in this time range" into a raw HTTP error string. The route
//! itself is `handlers::trace_by_id_v2`; this module owns only the two
//! renderings of its envelope.
//!
//! The envelope is `{1: trace, 2: metrics}`. Field 2 is always our
//! constant `12 00` — present, empty: PulsusDB has no read-accounting to
//! report, and the reference's own counter is not stable between two
//! fetches of the same trace, so there is nothing to match (see the
//! `traces-v2-fetch-metrics-not-populated` ledger row). Fields 3 and 4
//! (the reference's partial-trace signal) are never emitted: `assemble`
//! either returns the whole trace or an error, so PulsusDB never produces
//! a partial trace, and their proto3 defaults encode to zero bytes.

use serde::Serialize;

use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use prost::Message;

use super::assemble::AssembledTrace;

/// The v2 envelope, written field by field. There is no message type
/// here, and no `Option` anywhere in this module: field 1 is emitted by
/// straight-line code with no conditional, so there is no construction
/// site at which it could be left absent and no type for a test to be
/// pointed at instead of this function.
///
/// An envelope type whose fields are `Option` and are populated
/// `Some(<default>)` emits the same 4 bytes when empty and the same 96
/// bytes when populated, so no wire witness can separate that shape from
/// this one. This function's whole body is therefore exact-pinned by
/// `crates/pulsus-server/tests/route_inventory.rs`'s
/// `every_pinned_function_body_matches_the_snapshot_exactly` through
/// `support::manifest::EXPLICITLY_PINNED_FUNCTIONS` — any edit to it,
/// including that one, fails that test until the new body text is spelled
/// out in the snapshot.
pub(super) fn encode_protobuf(trace: &AssembledTrace) -> Vec<u8> {
    let trace_bytes = trace.as_traces_data().encode_to_vec();
    let mut out = Vec::with_capacity(trace_bytes.len() + 8);
    prost::encoding::encode_key(1, prost::encoding::WireType::LengthDelimited, &mut out);
    prost::encoding::encode_varint(trace_bytes.len() as u64, &mut out);
    out.extend_from_slice(&trace_bytes);
    out.extend_from_slice(&[0x12, 0x00]);
    out
}

/// The `trace` object. `resourceSpans` is the only key it can carry and it
/// is omitted when empty, which is what makes the absent-trace body
/// byte-identical to the reference's `{"trace":{},"metrics":{}}`.
/// Serialising a bare `TracesData` here would emit `"resourceSpans":[]`
/// and a different answer.
#[derive(Serialize)]
struct TraceField<'a> {
    #[serde(rename = "resourceSpans", skip_serializing_if = "<[_]>::is_empty")]
    resource_spans: &'a [ResourceSpans],
}

/// The metrics object, always present, always empty (see the module doc).
#[derive(Serialize)]
struct MetricsField {}

/// The envelope. The `trace` key carries no `skip`, so it is always
/// present — the JSON counterpart of `encode_protobuf`'s unconditional
/// field 1.
#[derive(Serialize)]
struct EnvelopeJson<'a> {
    trace: TraceField<'a>,
    metrics: MetricsField,
}

/// The JSON rendering (`Content-Type: application/json`). The populated
/// `trace` object uses docs/api.md §4.1's existing OTLP protojson
/// convention (hex ids, no default omission), which differs from the
/// reference's protojson for reasons that predate this route; only the
/// absent-trace body is byte-identical.
pub(super) fn encode_json(trace: &AssembledTrace) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&EnvelopeJson {
        trace: TraceField {
            resource_spans: &trace.as_traces_data().resource_spans,
        },
        metrics: MetricsField {},
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::assemble::fixture474;
    use pulsus_read::StoredSpan;

    fn t1_trace() -> AssembledTrace {
        AssembledTrace::from_stored(vec![StoredSpan {
            span_id: [0xbb, 0, 0, 0, 0, 0, 0, 0x01],
            payload_type: 1,
            kind: 0,
            payload: fixture474::from_hex(fixture474::T1_STORED_HEX),
        }])
        .expect("assemble the T1 probe")
    }

    /// Issue #474 AC-8a: the absent-trace envelope is the reference's four
    /// bytes — a length-zero field 1 and a length-zero field 2. An
    /// envelope that omitted field 1 would be two bytes.
    #[test]
    fn the_absent_trace_envelope_is_the_reference_four_bytes() {
        assert_eq!(
            encode_protobuf(&AssembledTrace::empty()),
            [0x0a, 0x00, 0x12, 0x00]
        );
    }

    /// Issue #474 AC-8b: the framing is gated on a populated input too, not
    /// only on the degenerate one — `0a 5c`, the 92 materialized T1 bytes,
    /// then exactly `12 00`.
    #[test]
    fn the_populated_envelope_frames_the_materialized_trace_then_empty_metrics() {
        let trace_bytes = fixture474::from_hex(fixture474::T1_MATERIALIZED_HEX);
        assert_eq!(trace_bytes.len(), 92);
        let mut expected = vec![0x0a, 0x5c];
        expected.extend_from_slice(&trace_bytes);
        expected.extend_from_slice(&[0x12, 0x00]);
        assert_eq!(encode_protobuf(&t1_trace()), expected);
    }

    /// Issue #474 AC-9's hermetic half: the absent-trace JSON body is the
    /// reference's exact 25 bytes.
    #[test]
    fn the_absent_trace_json_envelope_is_the_reference_twenty_five_bytes() {
        let body = encode_json(&AssembledTrace::empty()).expect("encode");
        assert_eq!(
            String::from_utf8(body).expect("utf8"),
            r#"{"trace":{},"metrics":{}}"#
        );
    }

    /// The populated JSON envelope keeps the `trace` key and nests the
    /// materialized resource spans under it.
    #[test]
    fn the_populated_json_envelope_nests_resource_spans_under_trace() {
        let body = encode_json(&t1_trace()).expect("encode");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("valid json");
        assert!(
            json["metrics"]
                .as_object()
                .expect("metrics object")
                .is_empty()
        );
        let span = &json["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
        assert_eq!(span["spanId"], "bb00000000000001");
        // The materialized status is present. It renders with its proto3
        // defaults spelled out, not as `{}`: docs/api.md §4.1's protojson
        // convention emits defaults rather than omitting them, and that
        // convention predates this route.
        assert_eq!(
            span["status"],
            serde_json::json!({"code": 0, "message": ""})
        );
    }
}
