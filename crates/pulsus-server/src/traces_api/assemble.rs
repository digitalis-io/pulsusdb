//! Trace assembly (issue #55): decode each stored per-span payload as the
//! pinned issue #54 contract type (a self-contained single-`ResourceSpans`
//! `TracesData` — `pulsus-write/src/protocols/otlp_traces.rs::build_payload`),
//! de-duplicate at-least-once replays by `(span_id, kind)`, and concatenate
//! every surviving `ResourceSpans` into one valid `TracesData`. Pure
//! functions, unit-tested — the OTLP layer lives here so `pulsus-read` stays
//! OTLP-agnostic (task-manager adjudication, open question 1).
//!
//! **Dedup is a total order, evaluated per `(span_id, kind)`** (plan v3 §1;
//! `kind` added for issue #75): `trace_spans` is a plain `MergeTree` (no
//! dedup engine, no ingest-order/version column), so at-least-once
//! duplicates are physically retained and row order carries no tiebreak
//! information. The winner per `(span_id, kind)` is the row maximal by
//! `((payload_type == 1) as u8, payload.len(), payload_bytes, payload_type)`:
//! a supported row always beats an unsupported duplicate (serving the
//! supported copy is honest — the unsupported duplicate is version-skew
//! noise), and the remaining components break every tie deterministically,
//! including identical bytes under different `payload_type`s. The same
//! winner emerges regardless of the order ClickHouse returned rows in.
//!
//! **`kind` in the dedup key is the Zipkin shared-span fix** (issue #75):
//! a Zipkin shared span reports the SAME `(trace_id, span_id)` from both
//! sides of an RPC with different `kind` (SERVER vs CLIENT) — a single
//! logical span. Keying only on `span_id` would silently drop one side on
//! retrieval; keying on `(span_id, kind)` keeps both. It is a genuine no-op
//! for native OTLP (span ids are unique per trace, so `kind` never
//! disambiguates a real dedup) and still collapses identical at-least-once
//! replays (same `(span_id, kind)` + bytes). The response's rendered `kind`
//! comes from each winner's decoded payload, never from the projected
//! column — so OTLP trace-by-ID responses are byte-identical to before.
//!
//! **Unsupported `payload_type` ⇒ explicit 500, never a partial 200**
//! (plan v2 §3 / v3 §1): the rule is evaluated on POST-dedup *winners*
//! only, so a span with both a supported and an unsupported copy serves
//! the supported row (200), while a span with no supported copy fails the
//! whole fetch — a silent partial trace would lie to the caller.
//!
//! **Canonical output order** (plan v3 §2): retained spans are sorted by
//! `(start_time_unix_nano, span_id, kind)` before concatenation —
//! deterministic response bytes/JSON regardless of ClickHouse read order or
//! map iteration order. `kind` is the final tiebreak (issue #75): once two
//! kinds can share a `span_id`, `span_id` alone no longer totalizes
//! equal-start ties. Documented in docs/api.md §4.1.
//!
//! **Absent nullable submessages are materialised on READ** (issue #474):
//! OTLP makes `ResourceSpans.resource`, `ScopeSpans.scope` and
//! `Span.status` optional, and `build_payload` stores exactly what the
//! sender wrote — so a sender that omitted any of the three has that
//! absence in ClickHouse forever. [`materialize_optional_submessages`]
//! turns each `None` into `Some(<default>)` after concatenation, which
//! prost encodes as a present, length-zero field rather than as an
//! omitted one. Read-side and not write-side for two reasons: the
//! reference re-materialises all three on read (its columnar schema has
//! no "absent" state to store), and a read-side fill repairs rows that
//! are already stored, which a write-side one does not. Only `None` is
//! replaced — a submessage the sender actually sent is never rewritten,
//! including a present-but-empty one. Documented in docs/api.md §4.1.

use std::collections::HashMap;

use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{Status, TracesData};
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

/// Every `ResourceSpans.resource`, `ScopeSpans.scope` and `Span.status`
/// left absent by the sender becomes a present, DEFAULT-valued message —
/// which prost encodes as the field key plus a zero length, never as an
/// omitted field (issue #474). Only `None` is replaced: a submessage the
/// sender actually sent is never rewritten, so a non-default `status`
/// survives untouched and a present-but-empty one is already what the
/// fill would have produced.
fn materialize_optional_submessages(data: &mut TracesData) {
    for rs in &mut data.resource_spans {
        rs.resource.get_or_insert_with(Resource::default);
        for ss in &mut rs.scope_spans {
            ss.scope.get_or_insert_with(InstrumentationScope::default);
            for span in &mut ss.spans {
                span.status.get_or_insert_with(Status::default);
            }
        }
    }
}

/// A `TracesData` that has been through [`materialize_optional_submessages`].
/// The encoders below take only this, so a future handler cannot encode a
/// raw `TracesData` that skipped the walk. It proves the walk RAN, not
/// that its contents are right — that is what the wire witnesses in
/// `tests/traces_api_live.rs` are for.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AssembledTrace(TracesData);

impl AssembledTrace {
    /// [`assemble`] plus the materialisation walk — the only way to build
    /// one from stored rows.
    pub(crate) fn from_stored(spans: Vec<StoredSpan>) -> Result<Self, AssembleError> {
        let mut data = assemble(spans)?;
        materialize_optional_submessages(&mut data);
        Ok(Self(data))
    }

    /// The v2 route's absent-trace value: no resource spans at all, so the
    /// walk has nothing to visit and the invariant holds vacuously.
    pub(crate) fn empty() -> Self {
        Self(TracesData::default())
    }

    pub(crate) fn as_traces_data(&self) -> &TracesData {
        &self.0
    }
}

/// Decode + dedup + order + merge (module doc has the full contract).
/// Empty input yields an empty `TracesData` — the handler maps an empty
/// *fetch* to `404` before ever calling this, so the empty case only
/// matters for the unit-level contract.
///
/// Private since issue #474: every caller goes through
/// [`AssembledTrace::from_stored`], so the materialisation walk cannot be
/// skipped by a new call site.
fn assemble(spans: Vec<StoredSpan>) -> Result<TracesData, AssembleError> {
    // Order-independent dedup: reduce into a map keyed by (span_id, kind)
    // (issue #75 — see the module doc), keeping the row maximal under the
    // total-order key.
    let mut winners: HashMap<([u8; 8], i8), StoredSpan> = HashMap::with_capacity(spans.len());
    for span in spans {
        match winners.entry((span.span_id, span.kind)) {
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

    let mut decoded: Vec<(([u8; 8], i8), TracesData)> = Vec::with_capacity(winners.len());
    for (key, span) in winners {
        let data = TracesData::decode(span.payload.as_slice()).map_err(|source| {
            AssembleError::Decode {
                span_id_hex: hex(&key.0),
                source,
            }
        })?;
        decoded.push((key, data));
    }

    // Canonical output order (plan v3 §2; `kind` tiebreak for issue #75).
    decoded.sort_by(|(a_key, a), (b_key, b)| {
        (start_time_ns(a), *a_key).cmp(&(start_time_ns(b), *b_key))
    });

    Ok(TracesData {
        resource_spans: decoded
            .into_iter()
            .flat_map(|(_, data)| data.resource_spans)
            .collect(),
    })
}

/// The protobuf rendering (`Content-Type: application/protobuf`).
pub(crate) fn encode_protobuf(trace: &AssembledTrace) -> Vec<u8> {
    trace.as_traces_data().encode_to_vec()
}

/// The OTLP-canonical protojson rendering (`Content-Type:
/// application/json`): the crate's own `with-serde` serializers — hex
/// trace/span ids, camelCase field names, u64 as strings — so T9's Tempo
/// alias needs no shape translation.
pub(crate) fn encode_json(trace: &AssembledTrace) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(trace.as_traces_data())
}

/// The issue #474 probe payloads, shared by this module's tests and
/// `fetch_v2`'s. Each `*_STORED_HEX` is the exact `build_payload` output
/// this repository stores for one probe span (measured end to end against
/// a spawned `pulsusdb`, plan v4's queries section); each
/// `*_MATERIALIZED_HEX` is what the pinned reference build answers for the
/// same trace, and therefore what [`encode_protobuf`] must emit once
/// [`materialize_optional_submessages`] has run.
#[cfg(test)]
pub(super) mod fixture474 {
    /// T1 — `resource`, `scope` and `status` all absent.
    pub(crate) const T1_STORED_HEX: &str = "0a54125212500a10bb0000000000000000000000000000011208bb000000000000012a1e6e6f2d7265736f757263652d6e6f2d73636f70652d6e6f2d73746174757330013900002a36fe9c97174140423936fe9c9717";
    /// T1 after the fill: `0a00` resource, `0a00` scope, `7a00` status.
    pub(crate) const T1_MATERIALIZED_HEX: &str = "0a5a0a0012560a0012520a10bb0000000000000000000000000000011208bb000000000000012a1e6e6f2d7265736f757263652d6e6f2d73636f70652d6e6f2d73746174757330013900002a36fe9c97174140423936fe9c97177a00";
    /// T3 — the over-reach control: resource, scope and a NON-default
    /// status all present, so the fill must leave every byte alone.
    pub(crate) const T3_STORED_HEX: &str = "0a7b0a1e0a1c0a0c736572766963652e6e616d65120c0a0a70756c7375732d34373412590a060a0173120131124f0a10bb0000000000000000000000000000031208bb000000000000032a13636f6e74726f6c2d616c6c2d70726573656e7430013900002a36fe9c97174140423936fe9c97177a081204626f6f6d1802";

    /// Bytes of a lowercase hex string. Panics on malformed input — a
    /// test-only literal, checked by every test that decodes it.
    pub(crate) fn from_hex(hex: &str) -> Vec<u8> {
        assert!(hex.len().is_multiple_of(2), "odd-length hex fixture");
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex fixture"))
            .collect()
    }
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
        stored_kind(span_id, payload_type, 0, payload)
    }

    fn stored_kind(span_id: [u8; 8], payload_type: i8, kind: i8, payload: Vec<u8>) -> StoredSpan {
        StoredSpan {
            span_id,
            payload_type,
            kind,
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

    /// Issue #75 shared-span fix: a Zipkin shared span's SERVER and CLIENT
    /// sides carry the SAME `span_id` with different `kind` — keying on
    /// `(span_id, kind)` keeps BOTH (they are one logical RPC span,
    /// reported from both ends), rather than collapsing to one as a
    /// `span_id`-only key would. Order-independent.
    #[test]
    fn a_zipkin_shared_span_returns_both_the_server_and_client_sides() {
        let server = stored_kind([7; 8], 1, 2, payload([7; 8], "server-side", 10));
        let client = stored_kind([7; 8], 1, 3, payload([7; 8], "client-side", 10));
        for input in [
            vec![server.clone(), client.clone()],
            vec![client.clone(), server.clone()],
        ] {
            let out = assemble(input).expect("assemble");
            assert_eq!(
                out.resource_spans.len(),
                2,
                "both shared-span sides must survive dedup"
            );
            let mut names = span_names(&out);
            names.sort();
            assert_eq!(names, vec!["client-side", "server-side"]);
        }
    }

    /// Issue #75: an identical at-least-once replay of the SAME
    /// `(span_id, kind)` still de-duplicates to one span — the shared-span
    /// fix widens the key, it does not disable replay dedup.
    #[test]
    fn an_identical_span_id_kind_replay_still_dedups_to_one() {
        let a = stored_kind([7; 8], 1, 2, payload([7; 8], "server-side", 10));
        let replay = stored_kind([7; 8], 1, 2, payload([7; 8], "server-side", 10));
        let out = assemble(vec![a, replay]).expect("assemble");
        assert_eq!(out.resource_spans.len(), 1);
        assert_eq!(span_names(&out), vec!["server-side"]);
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
            let out = AssembledTrace::from_stored(input).expect("assemble");
            assert_eq!(span_names(out.as_traces_data()), expected);
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

    /// Issue #474 AC-1: the three OTLP-optional submessages a sender left
    /// absent come back present and empty, every other field untouched,
    /// and the encoded bytes equal what the pinned reference build answers
    /// for the same trace.
    #[test]
    fn absent_resource_scope_and_status_are_materialized_present_and_empty() {
        let stored_bytes = fixture474::from_hex(fixture474::T1_STORED_HEX);
        let before = TracesData::decode(stored_bytes.as_slice()).expect("fixture decodes");
        let rs = &before.resource_spans[0];
        assert!(
            rs.resource.is_none(),
            "the T1 fixture must store no resource"
        );
        assert!(
            rs.scope_spans[0].scope.is_none(),
            "the T1 fixture must store no scope"
        );
        assert!(
            rs.scope_spans[0].spans[0].status.is_none(),
            "the T1 fixture must store no status"
        );

        let out = AssembledTrace::from_stored(vec![stored(
            [0xbb, 0, 0, 0, 0, 0, 0, 0x01],
            1,
            stored_bytes,
        )])
        .expect("assemble");

        // Field-by-field: the three become `Some(<default>)`, and nothing
        // else moves — the expected value is the decoded fixture with
        // exactly those three fields filled.
        let mut expected = before.clone();
        expected.resource_spans[0].resource = Some(Resource::default());
        expected.resource_spans[0].scope_spans[0].scope = Some(InstrumentationScope::default());
        expected.resource_spans[0].scope_spans[0].spans[0].status = Some(Status::default());
        assert_eq!(out.as_traces_data(), &expected);

        assert_eq!(
            encode_protobuf(&out),
            fixture474::from_hex(fixture474::T1_MATERIALIZED_HEX),
            "the materialized T1 bytes must equal the pinned reference build's answer"
        );
    }

    /// Issue #474 AC-2, the over-reach control: a resource, a scope and a
    /// NON-default status the sender really sent survive byte-identically.
    /// An implementation that assigns instead of filling only `None`
    /// produces a 117-byte body here instead of 125.
    #[test]
    fn a_sender_supplied_non_default_status_survives_the_fill_untouched() {
        let stored_bytes = fixture474::from_hex(fixture474::T3_STORED_HEX);
        let out = AssembledTrace::from_stored(vec![stored(
            [0xbb, 0, 0, 0, 0, 0, 0, 0x03],
            1,
            stored_bytes.clone(),
        )])
        .expect("assemble");
        assert_eq!(
            encode_protobuf(&out),
            stored_bytes,
            "T3 carries all three submessages already; the fill must be a no-op"
        );
    }

    /// The v2 route's absent-trace value carries no resource spans, so its
    /// protobuf rendering is empty — the four-byte envelope the reference
    /// answers is built in `fetch_v2`, not here.
    #[test]
    fn an_empty_assembled_trace_encodes_to_no_bytes() {
        assert!(
            AssembledTrace::empty()
                .as_traces_data()
                .resource_spans
                .is_empty()
        );
        assert!(encode_protobuf(&AssembledTrace::empty()).is_empty());
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
            AssembledTrace::from_stored(vec![stored([1; 8], 1, payload([1; 8], "span-a", 10))])
                .expect("assemble");
        let bytes = encode_protobuf(&out);
        let back = TracesData::decode(bytes.as_slice()).expect("round trip");
        assert_eq!(&back, out.as_traces_data());
    }

    /// The `with-serde` protojson shape: hex span ids, camelCase keys,
    /// u64 timestamps as strings.
    #[test]
    fn encode_json_is_otlp_canonical_protojson() {
        let out =
            AssembledTrace::from_stored(vec![stored([1; 8], 1, payload([1; 8], "span-a", 10))])
                .expect("assemble");
        let json: serde_json::Value =
            serde_json::from_slice(&encode_json(&out).expect("encode")).expect("valid json");
        let span = &json["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
        assert_eq!(span["spanId"], "0101010101010101");
        assert_eq!(span["traceId"], "ab".repeat(16));
        assert_eq!(span["startTimeUnixNano"], "10");
    }
}
