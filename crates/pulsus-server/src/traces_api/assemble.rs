//! Trace assembly: rebuild one OTLP `TracesData` from the columns the
//! fetch read (issues #55 and #587).
//!
//! Pure functions, unit-tested — the OTLP layer lives here so
//! `pulsus-read` stays OTLP-agnostic.
//!
//! **Nothing is decoded from a stored payload any more** (issue #587). The
//! fetch projects the span, per-trace and resource tables' own columns, and
//! this module is what turns them back into the protocol's messages. The
//! response's SHAPE does not move: one `ResourceSpans` per span, each with
//! one `ScopeSpans` holding one `Span`, in the canonical
//! `(start_time_unix_nano, span_id, kind)` order — which is what every
//! golden that pins it, and the byte-frozen reference capture, depend on.
//!
//! **Dedup is evaluated per `(span_id, kind)`** and the winner is the row
//! with the greatest `start_ns`. That totalises it: two rows sharing
//! `(span_id, kind)` AND `start_ns` share the whole span sort key, and the
//! fetch's `final = 1` collapses them — so two survivors with one
//! `(span_id, kind)` differ in `start_ns`, and with `final` off the two
//! rows are byte-identical and either choice gives the same answer. `kind`
//! is `i32` because the stored column is, so the map key and the
//! canonical-order tiebreak are `(span_id, i32)`.
//!
//! **`kind` in the dedup key is the Zipkin shared-span fix** (issue #75): a
//! Zipkin shared span reports the SAME `(trace_id, span_id)` from both
//! sides of an RPC with different `kind` — a single logical span. Keying
//! only on `span_id` would silently drop one side on retrieval.
//!
//! **Attributes come back ascending by reconstructed OTLP key, in all five
//! scopes, with no exception** (issue #587). The stored order is the
//! encoder's sorted path order rather than the sender's wire order, and
//! what a merge may do to a part's path order is not a question this tree
//! can answer — so the reader sorts and does not trust the column. That is
//! what keeps the documented property that responses are byte-deterministic
//! regardless of storage read order; emitting the column's order would
//! make it false the first time a merge changed a part's path order, and
//! the failure would look like a flaky golden. The divergence from the
//! sender's order is ledgered as
//! `traces-fetch-attribute-order-not-preserved`.
//!
//! **A kvlist comes back from one carrier or the other, never both.** The
//! write rule is that an attribute whose value is a kvlist is written as
//! leaf paths in `attrs` **iff** every leaf at every depth is storable, and
//! otherwise the whole attribute goes to `attrs_other` under its own key.
//! So this module has two disjoint sources and no reassembly to do.
//!
//! **Read a stored path by splitting on `.` FIRST, then unescaping each
//! segment.** Unescaping before splitting turns the one-segment
//! `http%2Eroute` into the two-segment `http.route` and reports a kvlist
//! where the sender sent a dotted key.
//!
//! **Absent nullable submessages are materialised** (issue #474):
//! [`materialize_optional_submessages`] turns each `None` into
//! `Some(<default>)`, which prost encodes as a present, length-zero field.
//! Since issue #587 the reader always builds a present `resource`, `scope`
//! and `status`, so the walk is a no-op in effect; it stays because it is
//! what proves `AssembledTrace`'s "the walk ran" invariant and it costs
//! nothing.

use std::collections::{BTreeMap, HashMap};

use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status, TracesData};
use prost::Message;
use pulsus_clickhouse::json_column::{
    JsonColumn, TraceJsonScalar, TraceJsonValue, unescape_json_path,
};
use pulsus_read::traces::spans::rows::{FetchedResource, FetchedSpan, FetchedTrace};
use thiserror::Error;

/// The OTLP key the reader reconstructs a service name under, and the one
/// key whose presence in either stored carrier makes the reconstruction
/// stand down.
const SERVICE_NAME_KEY: &str = "service.name";

/// Errors from rebuilding stored columns into a `TracesData` — mapped to
/// `500 internal` by `error::ApiError` (every variant indicates stored-data
/// corruption or version skew, never caller error).
#[derive(Debug, Error)]
pub(crate) enum AssembleError {
    /// A stored attribute carrier failed to decode as the OTLP message it
    /// is written as — the `attrs_other` columns, which hold a
    /// `KeyValueList`, and `resources.entity_refs`, which holds a
    /// `Resource` with only its entity-reference field populated. Stored
    /// data corruption.
    ///
    /// **A `JSON` attribute column that cannot be decoded does not reach
    /// here**: that column is decoded by the driver while the row streams,
    /// so an out-of-range type tag fails the fetch itself. Both end in the
    /// same `500`.
    #[error("the stored {column} for {subject} failed to decode: {source}")]
    Decode {
        column: &'static str,
        subject: String,
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

/// One attribute list, from the two disjoint carriers that can hold it,
/// **ascending by reconstructed OTLP key**.
///
/// `column` and `subject` name the carrier in a decode error, because a
/// span carries four of them and a message naming none of them is a `500`
/// nobody can act on.
fn otlp_attributes(
    attrs: &JsonColumn,
    other: &[u8],
    column: &'static str,
    subject: &str,
) -> Result<Vec<KeyValue>, AssembleError> {
    let mut tree = AttrTree::default();
    for entry in attrs.entries() {
        // **Split on `.` FIRST, then unescape each segment.** Unescaping
        // first turns a one-segment `http%2Eroute` into the two-segment
        // `http.route` and reports a kvlist where the sender sent a dotted
        // key.
        let segments: Vec<String> = entry.path.split('.').map(unescape_json_path).collect();
        tree.insert(&segments, stored_value(&entry.value));
    }
    for kv in decode_other(other, column, subject)? {
        // The second carrier's keys are OTLP keys already — not paths —
        // and they merge into the SAME sort as the first's.
        tree.insert(std::slice::from_ref(&kv.key), kv.value.unwrap_or_default());
    }
    Ok(tree.into_attributes())
}

/// One attribute list under construction: a `BTreeMap` at every level, so
/// the emitted order is ascending by key at the top AND inside every
/// kvlist, with no second sort pass and no key exempt.
#[derive(Default)]
struct AttrTree(BTreeMap<String, AttrNode>);

enum AttrNode {
    Leaf(AnyValue),
    Nested(AttrTree),
}

impl AttrTree {
    fn insert(&mut self, segments: &[impl AsRef<str>], value: AnyValue) {
        let Some((head, rest)) = segments.split_first() else {
            return;
        };
        let head = head.as_ref().to_string();
        if rest.is_empty() {
            self.0.insert(head, AttrNode::Leaf(value));
            return;
        }
        match self
            .0
            .entry(head)
            .or_insert_with(|| AttrNode::Nested(AttrTree::default()))
        {
            AttrNode::Nested(child) => child.insert(rest, value),
            // A path and a longer path through it cannot both be stored:
            // the write rule puts a kvlist in `attrs` only when every leaf
            // at every depth is storable, and a scalar at a parent path is
            // then not written. Keeping the deeper value is the one that
            // carries the kvlist.
            slot @ AttrNode::Leaf(_) => {
                let mut child = AttrTree::default();
                child.insert(rest, value);
                *slot = AttrNode::Nested(child);
            }
        }
    }

    fn into_attributes(self) -> Vec<KeyValue> {
        self.0
            .into_iter()
            .map(|(key, node)| KeyValue {
                key,
                value: Some(match node {
                    AttrNode::Leaf(value) => value,
                    AttrNode::Nested(child) => AnyValue {
                        value: Some(Value::KvlistValue(KeyValueList {
                            values: child.into_attributes(),
                        })),
                    },
                }),
                key_strindex: 0,
            })
            .collect()
    }
}

/// One stored value as the OTLP arm it was written from. **Each arm is its
/// own**: an `Int64` is never a `Double`, and an array keeps its element
/// order.
fn stored_value(value: &TraceJsonValue) -> AnyValue {
    let arm = match value {
        TraceJsonValue::Scalar(s) => stored_scalar(s),
        TraceJsonValue::StrArray(v) => array_of(v.iter().map(|s| Value::StringValue(s.clone()))),
        TraceJsonValue::IntArray(v) => array_of(v.iter().map(|i| Value::IntValue(*i))),
        TraceJsonValue::DoubleArray(v) => array_of(v.iter().map(|d| Value::DoubleValue(*d))),
        TraceJsonValue::BoolArray(v) => array_of(v.iter().map(|b| Value::BoolValue(*b))),
        TraceJsonValue::MixedArray(v) => array_of(v.iter().map(stored_scalar_value)),
        TraceJsonValue::EmptyArray => array_of(std::iter::empty()),
    };
    AnyValue { value: Some(arm) }
}

fn stored_scalar(scalar: &TraceJsonScalar) -> Value {
    stored_scalar_value(scalar)
}

fn stored_scalar_value(scalar: &TraceJsonScalar) -> Value {
    match scalar {
        TraceJsonScalar::Str(s) => Value::StringValue(s.clone()),
        TraceJsonScalar::Bool(b) => Value::BoolValue(*b),
        TraceJsonScalar::Int(i) => Value::IntValue(*i),
        TraceJsonScalar::Double(d) => Value::DoubleValue(*d),
    }
}

fn array_of(values: impl Iterator<Item = Value>) -> Value {
    Value::ArrayValue(ArrayValue {
        values: values
            .map(|value| AnyValue { value: Some(value) })
            .collect(),
    })
}

/// The second carrier's attributes: a `KeyValueList`, with empty meaning
/// zero bytes. **An undecodable carrier is an error naming the column and
/// the subject**, never a silently dropped attribute.
fn decode_other(
    other: &[u8],
    column: &'static str,
    subject: &str,
) -> Result<Vec<KeyValue>, AssembleError> {
    if other.is_empty() {
        return Ok(Vec::new());
    }
    Ok(KeyValueList::decode(other)
        .map_err(|source| AssembleError::Decode {
            column,
            subject: subject.to_string(),
            source,
        })?
        .values)
}

/// The `Resource` and its schema url for one span, from the resource entry
/// its `resource_id` resolves to.
///
/// `None` means the span's `resource_id` has **no row** in the fetched
/// resource array — reachable on a cluster and after a partial view
/// fan-out. The reader then renders the service name alone and the caller
/// is told through an operational counter rather than through an error: a
/// trace one attribute short beats a `500`.
///
/// The service name is reconstructed from the span's own `service` column
/// **iff** that column is a non-empty string **and** neither decoded
/// carrier already holds a `service.name` key. The span's column is the
/// carrier rather than the resource row's because it is on the row the
/// response is being built from, and because it is still right when the
/// resource row is missing.
///
/// **The reconstructed attribute is sorted in with the rest, not placed
/// first.** One rule with no exception is cheaper to state and cheaper to
/// test.
fn render_resource(
    service: &str,
    entry: Option<&FetchedResource>,
) -> Result<(Resource, String), AssembleError> {
    let Some(entry) = entry else {
        // A resource that is present with zero attributes is not
        // "repaired" into anything, so an empty service column
        // synthesises nothing.
        let attributes = if service.is_empty() {
            Vec::new()
        } else {
            vec![service_name_kv(service)]
        };
        return Ok((
            Resource {
                attributes,
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            },
            String::new(),
        ));
    };
    let subject = format!("resource {:#034x}", entry.resource_id);
    let mut attributes = otlp_attributes(
        &entry.attrs,
        &entry.attrs_other,
        "resources.attrs_other",
        &subject,
    )?;
    if !service.is_empty() && !attributes.iter().any(|a| a.key == SERVICE_NAME_KEY) {
        attributes.push(service_name_kv(service));
        // Sorted in, not appended: the one rule has no exception.
        attributes.sort_by(|a, b| a.key.cmp(&b.key));
    }
    let entity_refs = if entry.entity_refs.is_empty() {
        Vec::new()
    } else {
        // The carrier is a `Resource` with only its entity-reference
        // field populated — an OTLP message in its own right used as a
        // carrier, so its other two fields come back at their defaults
        // and are NOT the resource's.
        Resource::decode(entry.entity_refs.as_slice())
            .map_err(|source| AssembleError::Decode {
                column: "resources.entity_refs",
                subject: subject.clone(),
                source,
            })?
            .entity_refs
    };
    Ok((
        Resource {
            attributes,
            dropped_attributes_count: entry.dropped_attrs,
            entity_refs,
        },
        entry.schema_url.clone(),
    ))
}

fn service_name_kv(service: &str) -> KeyValue {
    KeyValue {
        key: SERVICE_NAME_KEY.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(service.to_string())),
        }),
        key_strindex: 0,
    }
}

/// The OTLP `parent_span_id` for a stored one.
///
/// `parent_span_id` is a `FixedString(8)`, so a root span's empty OTLP
/// field is stored as eight zero bytes and there is no "absent" state to
/// store. The reader emits an EMPTY field when all eight are zero, which
/// is the same value the per-trace view already treats them as when it
/// picks a root.
///
/// **Links are not treated the same way**, and that is a decision rather
/// than a consequence: a link's `trace_id`/`span_id` are emitted as
/// stored. Nothing in the tree pins an all-zero link id, and the root rule
/// has two independent witnesses where this one would have none.
fn otlp_parent_span_id(stored: &[u8; 8]) -> Vec<u8> {
    if stored.iter().all(|b| *b == 0) {
        return Vec::new();
    }
    stored.to_vec()
}

/// Every `ResourceSpans.resource`, `ScopeSpans.scope` and `Span.status`
/// left absent becomes a present, DEFAULT-valued message — which prost
/// encodes as the field key plus a zero length, never as an omitted field
/// (issue #474). Only `None` is replaced.
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

/// A `TracesData` that has been through [`materialize_optional_submessages`],
/// with the count of spans whose resource was missing beside it.
///
/// The encoders below take only this, so a future handler cannot encode a
/// raw `TracesData` that skipped the walk. It proves the walk RAN, not that
/// its contents are right — that is what the wire witnesses in
/// `tests/traces_api_live.rs` are for.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AssembledTrace {
    data: TracesData,
    missing_resources: u64,
}

impl AssembledTrace {
    /// [`rebuild`] plus the materialisation walk — the only way to build
    /// one from fetched columns.
    pub(crate) fn from_fetched(
        trace_id: &[u8; 16],
        fetched: FetchedTrace,
    ) -> Result<Self, AssembleError> {
        let (mut data, missing_resources) = rebuild(trace_id, fetched)?;
        materialize_optional_submessages(&mut data);
        Ok(Self {
            data,
            missing_resources,
        })
    }

    /// The v2 route's absent-trace value: no resource spans at all, so the
    /// walk has nothing to visit and the invariant holds vacuously.
    pub(crate) fn empty() -> Self {
        Self {
            data: TracesData::default(),
            missing_resources: 0,
        }
    }

    pub(crate) fn as_traces_data(&self) -> &TracesData {
        &self.data
    }

    /// How many spans referenced a `resource_id` with no row in the
    /// fetched resource array. The handler reports it through the metric
    /// surface; the only degraded state a fetch can detect from its own
    /// inputs.
    pub(crate) fn missing_resources(&self) -> u64 {
        self.missing_resources
    }
}

/// Dedup + order + rebuild (the module doc has the full contract).
///
/// Returns the data and the number of spans whose resource was missing.
/// Empty input yields an empty `TracesData` — the v1 handler maps an empty
/// fetch to `404` before ever calling this, so the empty case only matters
/// for the unit-level contract.
fn rebuild(trace_id: &[u8; 16], fetched: FetchedTrace) -> Result<(TracesData, u64), AssembleError> {
    // Order-independent dedup: reduce into a map keyed on
    // `(span_id, kind)`, keeping the row with the greatest start. The
    // module doc has why that totalises it.
    let mut winners: HashMap<([u8; 8], i32), FetchedSpan> =
        HashMap::with_capacity(fetched.spans.len());
    for span in fetched.spans {
        match winners.entry((span.span_id, span.kind)) {
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(span);
            }
            std::collections::hash_map::Entry::Occupied(mut slot) => {
                if span.start_ns > slot.get().start_ns {
                    slot.insert(span);
                }
            }
        }
    }

    let resources: HashMap<u128, FetchedResource> = fetched
        .resources
        .into_iter()
        .map(|r| (r.resource_id, r))
        .collect();

    let mut spans: Vec<FetchedSpan> = winners.into_values().collect();
    // The canonical order, unchanged from before this part:
    // `(start_time_unix_nano, span_id, kind)`. The start is ordered as the
    // unsigned field the protocol declares, so a stored zero sorts first.
    spans.sort_by_key(|s| (s.start_ns as u64, s.span_id, s.kind));

    let mut missing_resources = 0u64;
    let mut groups = Vec::with_capacity(spans.len());
    for span in spans {
        let span_subject = format!("span {}", hex(&span.span_id));
        let entry = resources.get(&span.resource_id);
        if entry.is_none() {
            missing_resources += 1;
        }
        let (resource, resource_schema_url) = render_resource(&span.service, entry)?;
        let scope = InstrumentationScope {
            name: span.scope_name,
            version: span.scope_version,
            attributes: otlp_attributes(
                &span.scope_attrs,
                &span.scope_attrs_other,
                "spans.scope_attrs_other",
                &span_subject,
            )?,
            dropped_attributes_count: span.scope_dropped_attrs,
        };
        let attributes = otlp_attributes(
            &span.attrs,
            &span.attrs_other,
            "spans.attrs_other",
            &span_subject,
        )?;
        let mut events = Vec::with_capacity(span.events.len());
        for event in span.events {
            events.push(Event {
                // Verbatim: the stored column is unsigned and so is the
                // protocol's field.
                time_unix_nano: event.time_ns,
                name: event.name,
                attributes: otlp_attributes(
                    &event.attrs,
                    &event.attrs_other,
                    "spans.events[].attrs_other",
                    &span_subject,
                )?,
                dropped_attributes_count: event.dropped_attrs,
            });
        }
        let mut links = Vec::with_capacity(span.links.len());
        for link in span.links {
            links.push(Link {
                // As stored, whatever their length.
                trace_id: link.trace_id.into_vec(),
                span_id: link.span_id.into_vec(),
                trace_state: link.trace_state,
                flags: link.flags,
                attributes: otlp_attributes(
                    &link.attrs,
                    &link.attrs_other,
                    "spans.links[].attrs_other",
                    &span_subject,
                )?,
                dropped_attributes_count: link.dropped_attrs,
            });
        }
        groups.push(ResourceSpans {
            resource: Some(resource),
            scope_spans: vec![ScopeSpans {
                scope: Some(scope),
                spans: vec![Span {
                    // The REQUEST's own sixteen bytes: the span table
                    // carries the trace id as a key and the statements do
                    // not project it.
                    trace_id: trace_id.to_vec(),
                    span_id: span.span_id.to_vec(),
                    trace_state: span.trace_state,
                    parent_span_id: otlp_parent_span_id(&span.parent_span_id),
                    flags: span.flags,
                    name: span.name,
                    // The protocol's own signed values, verbatim.
                    kind: span.kind,
                    start_time_unix_nano: span.start_ns as u64,
                    end_time_unix_nano: span.end_ns,
                    attributes,
                    dropped_attributes_count: span.dropped_attrs,
                    events,
                    dropped_events_count: span.dropped_events,
                    links,
                    dropped_links_count: span.dropped_links,
                    status: Some(Status {
                        code: span.status_code,
                        message: span.status_message,
                    }),
                }],
                schema_url: span.scope_schema_url,
            }],
            schema_url: resource_schema_url,
        });
    }
    Ok((
        TracesData {
            resource_spans: groups,
        },
        missing_resources,
    ))
}

/// The protobuf rendering (`Content-Type: application/protobuf`).
pub(crate) fn encode_protobuf(trace: &AssembledTrace) -> Vec<u8> {
    trace.as_traces_data().encode_to_vec()
}

/// The OTLP-canonical protojson rendering (`Content-Type:
/// application/json`): the crate's own `with-serde` serializers — hex
/// trace/span ids, camelCase field names, u64 as strings — so the Tempo
/// alias needs no shape translation.
pub(crate) fn encode_json(trace: &AssembledTrace) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(trace.as_traces_data())
}

#[cfg(test)]
pub(super) mod fixture474 {
    //! The issue #474 probe's materialized wire bytes, shared by this
    //! module's cases and `fetch_v2`'s.
    //!
    //! **The `*_STORED_HEX` constants are gone with the payload column**
    //! (issue #587): they were the exact `build_payload` output this
    //! repository used to store for one probe span, and nothing stores or
    //! reads a payload any more. The `*_MATERIALIZED_HEX` ones stay —
    //! they are what the pinned reference build answers for the same
    //! trace, and therefore what the response must still equal.

    /// T1 — `resource`, `scope` and `status` all absent on the way in, so
    /// all three come back present and length-zero: `0a00`, `0a00`,
    /// `7a00`.
    pub(crate) const T1_MATERIALIZED_HEX: &str = "0a5a0a0012560a0012520a10bb0000000000000000000000000000011208bb000000000000012a1e6e6f2d7265736f757263652d6e6f2d73636f70652d6e6f2d73746174757330013900002a36fe9c97174140423936fe9c97177a00";

    /// T1's own trace id, span id, name and the two instants the probe was
    /// captured with — the inputs a fetched row has to carry to render
    /// [`T1_MATERIALIZED_HEX`].
    pub(crate) const T1_TRACE_ID: [u8; 16] = [0xbb, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01];
    /// See [`T1_TRACE_ID`].
    pub(crate) const T1_SPAN_ID: [u8; 8] = [0xbb, 0, 0, 0, 0, 0, 0, 0x01];
    /// See [`T1_TRACE_ID`].
    pub(crate) const T1_NAME: &str = "no-resource-no-scope-no-status";
    /// See [`T1_TRACE_ID`].
    pub(crate) const T1_START_NS: i64 = 1_700_000_000_000_000_000;
    /// See [`T1_TRACE_ID`].
    pub(crate) const T1_END_NS: u64 = 1_700_000_000_001_000_000;
    /// See [`T1_TRACE_ID`].
    pub(crate) const T1_KIND: i32 = 1;

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
pub(super) mod fixture {
    //! Shared fixture builders for this module's cases and `fetch_v2`'s.

    use super::*;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValueList};
    use pulsus_clickhouse::json_column::{
        TraceJson, TraceJsonEntry, TraceJsonScalar, TraceJsonValue, escape_json_path,
    };
    use pulsus_read::traces::spans::rows::{
        FetchRoute, FetchedResourceTuple, FetchedSpanTuple, FetchedTrace,
    };
    use serde_bytes::ByteBuf;

    /// A `JSON` column holding the given `(stored path, value)` pairs.
    pub(crate) fn json(entries: &[(&str, TraceJsonValue)]) -> TraceJson {
        TraceJson::from_entries(
            entries
                .iter()
                .map(|(path, value)| TraceJsonEntry {
                    path: (*path).to_string(),
                    value: value.clone(),
                })
                .collect(),
        )
    }

    /// A `JSON` column holding one string-valued attribute under the
    /// ESCAPED form of an OTLP key.
    pub(crate) fn json_str(key: &str, value: &str) -> TraceJson {
        json(&[(
            &escape_json_path(key),
            TraceJsonValue::Scalar(TraceJsonScalar::Str(value.to_string())),
        )])
    }

    /// The `attrs_other` carrier for a list of OTLP attributes — the
    /// write side's own shape, a `KeyValueList`, with empty meaning zero
    /// bytes.
    pub(crate) fn other(attrs: &[KeyValue]) -> ByteBuf {
        if attrs.is_empty() {
            return ByteBuf::new();
        }
        ByteBuf::from(
            KeyValueList {
                values: attrs.to_vec(),
            }
            .encode_to_vec(),
        )
    }

    pub(crate) fn kv(key: &str, value: Value) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue { value: Some(value) }),
            key_strindex: 0,
        }
    }

    /// A span row with every field at a harmless default, for a case that
    /// varies one of them.
    pub(crate) fn span_row(span_id: [u8; 8], start_ns: i64) -> FetchedSpanTuple {
        FetchedSpanTuple {
            span_id,
            parent_span_id: [0u8; 8],
            start_ns,
            end_ns: u64::try_from(start_ns).unwrap_or(0) + 1_000_000,
            service: "checkout".to_string(),
            resource_id: 1,
            name: "op".to_string(),
            kind: 2,
            status_code: 0,
            status_message: String::new(),
            trace_state: String::new(),
            flags: 0,
            scope_name: "live-scope".to_string(),
            scope_version: String::new(),
            scope_attrs: TraceJson::empty(),
            attrs: TraceJson::empty(),
            attrs_other: ByteBuf::new(),
            dropped_attrs: 0,
            events: Vec::new(),
            dropped_events: 0,
            links: Vec::new(),
            dropped_links: 0,
            scope_schema_url: String::new(),
            scope_dropped_attrs: 0,
            scope_attrs_other: ByteBuf::new(),
        }
    }

    /// A resource row with every field at a harmless default.
    pub(crate) fn resource_row(resource_id: u128) -> FetchedResourceTuple {
        FetchedResourceTuple {
            resource_id,
            attrs: TraceJson::empty(),
            attrs_other: ByteBuf::new(),
            dropped_attrs: 0,
            schema_url: String::new(),
            entity_refs: ByteBuf::new(),
        }
    }

    pub(crate) fn fetched(
        spans: Vec<FetchedSpanTuple>,
        resources: Vec<FetchedResourceTuple>,
    ) -> FetchedTrace {
        FetchedTrace {
            spans,
            resources,
            statements: 1,
            route: FetchRoute::Indexed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fixture::*;
    use super::*;

    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::trace::v1::Span;
    use pulsus_clickhouse::json_column::{
        TraceJson, TraceJsonScalar, TraceJsonValue, escape_json_path,
    };
    use pulsus_read::traces::spans::rows::{FetchedEventTuple, FetchedLinkTuple};
    use serde_bytes::ByteBuf;

    const TRACE_ID: [u8; 16] = [0xab; 16];
    const START: i64 = 1_700_000_000_000_000_000;

    fn assembled(fetched: FetchedTrace) -> AssembledTrace {
        AssembledTrace::from_fetched(&TRACE_ID, fetched).expect("the rebuild succeeds")
    }

    fn spans_of(data: &TracesData) -> Vec<&Span> {
        data.resource_spans
            .iter()
            .flat_map(|rs| &rs.scope_spans)
            .flat_map(|ss| &ss.spans)
            .collect()
    }

    fn keys(attrs: &[KeyValue]) -> Vec<String> {
        attrs.iter().map(|a| a.key.clone()).collect()
    }

    fn value_of(attrs: &[KeyValue], key: &str) -> Option<Value> {
        attrs
            .iter()
            .find(|a| a.key == key)
            .and_then(|a| a.value.clone())
            .and_then(|v| v.value)
    }

    // -----------------------------------------------------------------
    // F-6 — the service name, five sub-cases.
    // -----------------------------------------------------------------

    /// `F-6`: **the service-name rule, row by row.**
    ///
    /// Five sub-cases, one per arm of the rule, and the fifth is the
    /// missing-resource state.
    #[test]
    fn the_service_name_rule_holds_row_by_row() {
        // (1) no service column and no stored key: nothing is
        // synthesised. A resource that is present with zero attributes is
        // not "repaired" into anything.
        let mut span = span_row([0x01; 8], START);
        span.service = String::new();
        let data = assembled(fetched(vec![span], vec![resource_row(1)]));
        assert_eq!(
            keys(
                &data.as_traces_data().resource_spans[0]
                    .resource
                    .as_ref()
                    .expect("a present resource")
                    .attributes
            ),
            Vec::<String>::new(),
            "(1) no service column and no stored key synthesises nothing"
        );

        // (2) a service column and no stored key: reconstructed, and
        // SORTED IN with the rest rather than placed first.
        let mut span = span_row([0x02; 8], START);
        span.service = "checkout".to_string();
        let mut resource = resource_row(1);
        resource.attrs = json(&[
            (
                "z%2Elast",
                TraceJsonValue::Scalar(TraceJsonScalar::Str("zzz".to_string())),
            ),
            (
                "a%2Efirst",
                TraceJsonValue::Scalar(TraceJsonScalar::Str("aaa".to_string())),
            ),
        ]);
        let data = assembled(fetched(vec![span], vec![resource]));
        let attrs = &data.as_traces_data().resource_spans[0]
            .resource
            .as_ref()
            .expect("a present resource")
            .attributes;
        assert_eq!(
            keys(attrs),
            vec!["a.first", "service.name", "z.last"],
            "(2) the reconstructed key is sorted in, not placed first"
        );
        assert_eq!(
            value_of(attrs, "service.name"),
            Some(Value::StringValue("checkout".to_string()))
        );

        // (3) an EMPTY service column with a stored empty-string key: the
        // stored one comes back once, and clause 1 declines as well.
        let mut span = span_row([0x03; 8], START);
        span.service = String::new();
        let mut resource = resource_row(1);
        resource.attrs = json_str(SERVICE_NAME_KEY, "");
        let data = assembled(fetched(vec![span], vec![resource]));
        let attrs = &data.as_traces_data().resource_spans[0]
            .resource
            .as_ref()
            .expect("a present resource")
            .attributes;
        assert_eq!(keys(attrs), vec!["service.name"], "(3) exactly once");
        assert_eq!(
            value_of(attrs, "service.name"),
            Some(Value::StringValue(String::new()))
        );

        // (4) a NON-STRING stored key: the stored typed value comes back
        // and no second entry is added. Without the write side's
        // conditional skip the value is absent from the column and the
        // reader reconstructs the text rendering `"7"` — a changed typed
        // value in a 200.
        let mut span = span_row([0x04; 8], START);
        span.service = "7".to_string();
        let mut resource = resource_row(1);
        // The ESCAPED path, which is what the column stores for a dotted
        // OTLP key: the raw `service.name` would be two stored segments
        // and would come back as the kvlist `service = { name: 7 }`.
        resource.attrs = json(&[(
            &escape_json_path(SERVICE_NAME_KEY),
            TraceJsonValue::Scalar(TraceJsonScalar::Int(7)),
        )]);
        let data = assembled(fetched(vec![span], vec![resource]));
        let attrs = &data.as_traces_data().resource_spans[0]
            .resource
            .as_ref()
            .expect("a present resource")
            .attributes;
        assert_eq!(keys(attrs), vec!["service.name"], "(4) no second entry");
        assert_eq!(value_of(attrs, "service.name"), Some(Value::IntValue(7)));

        // (5) a `resource_id` matching NO entry: the service name alone,
        // an empty schema url, no error — and the counter says so.
        let mut span = span_row([0x05; 8], START);
        span.resource_id = 99;
        span.service = "checkout".to_string();
        let assembled_trace = assembled(fetched(vec![span], vec![resource_row(1)]));
        let group = &assembled_trace.as_traces_data().resource_spans[0];
        let resource = group.resource.as_ref().expect("a present resource");
        assert_eq!(keys(&resource.attributes), vec!["service.name"]);
        assert_eq!(resource.dropped_attributes_count, 0);
        assert_eq!(group.schema_url, "");
        assert_eq!(
            assembled_trace.missing_resources(),
            1,
            "(5) the degraded answer is counted rather than lost silently"
        );
    }

    // -----------------------------------------------------------------
    // F-7 — the root span's parent.
    // -----------------------------------------------------------------

    /// `F-7`: **eight zero bytes come back as an EMPTY `parent_span_id`,
    /// and any other eight come back as stored.**
    #[test]
    fn an_all_zero_parent_span_id_comes_back_empty() {
        let mut root = span_row([0x01; 8], START);
        root.parent_span_id = [0u8; 8];
        let data = assembled(fetched(vec![root], vec![resource_row(1)]));
        assert_eq!(
            spans_of(data.as_traces_data())[0].parent_span_id,
            Vec::<u8>::new(),
            "a stored all-zero parent is an ABSENT parent on the wire"
        );

        let mut child = span_row([0x02; 8], START);
        child.parent_span_id = [0x22; 8];
        let data = assembled(fetched(vec![child], vec![resource_row(1)]));
        assert_eq!(
            spans_of(data.as_traces_data())[0].parent_span_id,
            vec![0x22; 8]
        );
    }

    // -----------------------------------------------------------------
    // F-8 — a nested path set reconstructs its kvlist.
    // -----------------------------------------------------------------

    /// `F-8`: **a stored path set holding two leaves of one nested object
    /// and one escaped dotted key renders TWO attributes** — the kvlist
    /// and the scalar — not three leaves and not one.
    ///
    /// This is the rule a simpler reader gets wrong: unescaping before
    /// splitting turns `a%2Eb` into the two-segment `a.b` and reports a
    /// kvlist the sender never sent.
    #[test]
    fn a_nested_path_set_renders_one_kvlist_and_one_dotted_key() {
        let mut resource = resource_row(1);
        resource.attrs = json(&[
            ("d.e", TraceJsonValue::Scalar(TraceJsonScalar::Int(1))),
            ("d.f", TraceJsonValue::Scalar(TraceJsonScalar::Int(2))),
            (
                "a%2Eb",
                TraceJsonValue::Scalar(TraceJsonScalar::Str("s".to_string())),
            ),
        ]);
        let mut span = span_row([0x01; 8], START);
        span.service = String::new();
        let data = assembled(fetched(vec![span], vec![resource]));
        let attrs = &data.as_traces_data().resource_spans[0]
            .resource
            .as_ref()
            .expect("a present resource")
            .attributes;
        assert_eq!(
            keys(attrs),
            vec!["a.b", "d"],
            "two attributes: the dotted key and the nested object"
        );
        assert_eq!(
            value_of(attrs, "a.b"),
            Some(Value::StringValue("s".to_string())),
            "the escaped segment is ONE key, not two"
        );
        let Some(Value::KvlistValue(list)) = value_of(attrs, "d") else {
            panic!("`d` must be a kvlist, got {:?}", value_of(attrs, "d"));
        };
        assert_eq!(
            keys(&list.values),
            vec!["e", "f"],
            "a kvlist's own leaves come back ascending too"
        );
        assert_eq!(value_of(&list.values, "e"), Some(Value::IntValue(1)));
        assert_eq!(value_of(&list.values, "f"), Some(Value::IntValue(2)));
    }

    // -----------------------------------------------------------------
    // Dedup, order and the field set.
    // -----------------------------------------------------------------

    /// A Zipkin shared span reports one `(trace_id, span_id)` from both
    /// sides of an RPC with different `kind`, and both sides come back.
    #[test]
    fn a_zipkin_shared_span_returns_both_the_server_and_client_sides() {
        let mut server = span_row([0x77; 8], START);
        server.kind = 2;
        let mut client = span_row([0x77; 8], START);
        client.kind = 3;
        let data = assembled(fetched(vec![server, client], vec![resource_row(1)]));
        let got: Vec<([u8; 8], i32)> = spans_of(data.as_traces_data())
            .iter()
            .map(|s| {
                (
                    <[u8; 8]>::try_from(s.span_id.as_slice()).expect("eight bytes"),
                    s.kind,
                )
            })
            .collect();
        assert_eq!(got, vec![([0x77; 8], 2), ([0x77; 8], 3)]);
    }

    /// An at-least-once replay that crossed replicas carries one
    /// `(span_id, kind)` twice, and the reader keeps the row with the
    /// greatest `start_ns`.
    #[test]
    fn a_duplicate_span_id_and_kind_dedups_to_the_latest_start() {
        let mut first = span_row([0x11; 8], START);
        first.name = "earlier".to_string();
        let mut second = span_row([0x11; 8], START + 1);
        second.name = "later".to_string();
        for order in [vec![first.clone(), second.clone()], vec![second, first]] {
            let data = assembled(fetched(order, vec![resource_row(1)]));
            let spans = spans_of(data.as_traces_data());
            assert_eq!(spans.len(), 1, "one survivor per (span_id, kind)");
            assert_eq!(spans[0].name, "later");
        }
    }

    /// The canonical order is `(start_time_unix_nano, span_id, kind)` and
    /// it is independent of the order the engine returned the rows in.
    #[test]
    fn output_order_is_canonical_across_input_permutations() {
        let mut a = span_row([0x33; 8], START);
        a.kind = 2;
        let mut b = span_row([0x77; 8], START);
        b.kind = 2;
        let mut c = span_row([0x77; 8], START);
        c.kind = 3;
        let want = vec![([0x33; 8], 2), ([0x77; 8], 2), ([0x77; 8], 3)];
        for order in [
            vec![a.clone(), b.clone(), c.clone()],
            vec![c.clone(), b.clone(), a.clone()],
            vec![b.clone(), a.clone(), c.clone()],
        ] {
            let data = assembled(fetched(order, vec![resource_row(1)]));
            let got: Vec<([u8; 8], i32)> = spans_of(data.as_traces_data())
                .iter()
                .map(|s| {
                    (
                        <[u8; 8]>::try_from(s.span_id.as_slice()).expect("eight bytes"),
                        s.kind,
                    )
                })
                .collect();
            assert_eq!(got, want);
        }
    }

    /// A ZERO stored start comes back as zero and sorts to the FRONT,
    /// where it used to sort by the receipt time the writer substituted.
    #[test]
    fn a_zero_start_comes_back_as_zero_and_sorts_first() {
        let ordinary = span_row([0x11; 8], START);
        let zero = span_row([0x12; 8], 0);
        let data = assembled(fetched(vec![ordinary, zero], vec![resource_row(1)]));
        let spans = spans_of(data.as_traces_data());
        assert_eq!(
            spans
                .iter()
                .map(|s| s.start_time_unix_nano)
                .collect::<Vec<_>>(),
            vec![0, START as u64],
            "the zero-start span sorts to the front"
        );
        assert_eq!(
            spans[0].span_id,
            vec![0x12; 8],
            "and it is the span that was stored at zero"
        );
    }

    /// One `ResourceSpans` per span, each with one `ScopeSpans` holding
    /// one `Span` — the shape every golden and the byte-frozen reference
    /// capture depend on.
    #[test]
    fn the_response_is_one_resource_spans_per_span() {
        let spans = vec![span_row([0x01; 8], START), span_row([0x02; 8], START + 1)];
        let data = assembled(fetched(spans, vec![resource_row(1)]));
        let groups = &data.as_traces_data().resource_spans;
        assert_eq!(groups.len(), 2, "one group per span");
        for group in groups {
            assert_eq!(group.scope_spans.len(), 1);
            assert_eq!(group.scope_spans[0].spans.len(), 1);
        }
        assert_eq!(
            groups[0].resource, groups[1].resource,
            "the two groups carry the same resource"
        );
    }

    /// Every scalar field of a span, its scope, its events and its links
    /// comes back from its own column — the whole field set, with no
    /// default anywhere, so a reader built from a span-id list cannot
    /// pass.
    #[test]
    fn every_scalar_field_comes_back_from_its_own_column() {
        let mut span = span_row([0x11; 8], START);
        span.end_ns = START as u64 + 500_000_000;
        span.kind = -1;
        span.status_code = 300;
        span.status_message = "boom".to_string();
        span.trace_state = "rojo=00f067aa0ba902b7".to_string();
        span.flags = 1;
        span.name = "every-field".to_string();
        span.scope_name = "live-scope".to_string();
        span.scope_version = "1.2.3".to_string();
        span.scope_schema_url = "https://example.invalid/scope".to_string();
        span.scope_dropped_attrs = 11;
        span.dropped_attrs = 3;
        span.dropped_events = 5;
        span.dropped_links = 9;
        span.events = vec![FetchedEventTuple {
            time_ns: u64::MAX,
            name: "ev".to_string(),
            attrs: TraceJson::empty(),
            attrs_other: ByteBuf::new(),
            dropped_attrs: 2,
        }];
        span.links = vec![FetchedLinkTuple {
            trace_id: ByteBuf::from(vec![0xaa, 0xbb, 0xcc, 0xdd]),
            span_id: ByteBuf::from(vec![0x11, 0x22, 0x33]),
            trace_state: "ls=1".to_string(),
            flags: 2,
            attrs: TraceJson::empty(),
            attrs_other: ByteBuf::new(),
            dropped_attrs: 4,
        }];

        let mut resource = resource_row(1);
        resource.dropped_attrs = 7;
        resource.schema_url = "https://example.invalid/res".to_string();

        let data = assembled(fetched(vec![span], vec![resource]));
        let traces = data.as_traces_data();
        let group = &traces.resource_spans[0];
        assert_eq!(group.schema_url, "https://example.invalid/res");
        assert_eq!(
            group
                .resource
                .as_ref()
                .expect("a present resource")
                .dropped_attributes_count,
            7
        );
        let scope_spans = &group.scope_spans[0];
        assert_eq!(scope_spans.schema_url, "https://example.invalid/scope");
        let scope = scope_spans.scope.as_ref().expect("a present scope");
        assert_eq!(scope.name, "live-scope");
        assert_eq!(scope.version, "1.2.3");
        assert_eq!(scope.dropped_attributes_count, 11);

        let out = &scope_spans.spans[0];
        assert_eq!(
            out.trace_id,
            TRACE_ID.to_vec(),
            "the request's own 16 bytes"
        );
        assert_eq!(out.span_id, vec![0x11; 8]);
        assert_eq!(out.name, "every-field");
        assert_eq!(out.kind, -1, "the protocol's own signed value, verbatim");
        assert_eq!(out.start_time_unix_nano, START as u64);
        assert_eq!(out.end_time_unix_nano, START as u64 + 500_000_000);
        assert_eq!(out.trace_state, "rojo=00f067aa0ba902b7");
        assert_eq!(out.flags, 1);
        assert_eq!(out.dropped_attributes_count, 3);
        assert_eq!(out.dropped_events_count, 5);
        assert_eq!(out.dropped_links_count, 9);
        let status = out.status.as_ref().expect("a present status");
        assert_eq!(
            status.code, 300,
            "the protocol's own signed value, verbatim"
        );
        assert_eq!(status.message, "boom");

        assert_eq!(out.events.len(), 1);
        assert_eq!(
            out.events[0].time_unix_nano,
            u64::MAX,
            "an event time above i64::MAX comes back unsaturated"
        );
        assert_eq!(out.events[0].name, "ev");
        assert_eq!(out.events[0].dropped_attributes_count, 2);

        assert_eq!(out.links.len(), 1);
        assert_eq!(
            out.links[0].trace_id,
            vec![0xaa, 0xbb, 0xcc, 0xdd],
            "a link's ids are emitted as stored, whatever their length"
        );
        assert_eq!(out.links[0].span_id, vec![0x11, 0x22, 0x33]);
        assert_eq!(out.links[0].trace_state, "ls=1");
        assert_eq!(out.links[0].flags, 2);
        assert_eq!(out.links[0].dropped_attributes_count, 4);
    }

    /// Every value kind a `JSON` column can hold renders its own OTLP arm
    /// — an `Int64` is never a `Double`, and an array keeps its element
    /// order.
    #[test]
    fn every_stored_value_kind_renders_its_own_otlp_arm() {
        let mut span = span_row([0x11; 8], START);
        span.attrs = json(&[
            (
                "s%2Estr",
                TraceJsonValue::Scalar(TraceJsonScalar::Str("v".to_string())),
            ),
            ("s%2Eint", TraceJsonValue::Scalar(TraceJsonScalar::Int(42))),
            (
                "s%2Edbl",
                TraceJsonValue::Scalar(TraceJsonScalar::Double(1.5)),
            ),
            (
                "s%2Ebool",
                TraceJsonValue::Scalar(TraceJsonScalar::Bool(true)),
            ),
            (
                "s%2Earr",
                TraceJsonValue::StrArray(vec!["a".to_string(), "b".to_string()]),
            ),
            ("s%2Eempty", TraceJsonValue::EmptyArray),
        ]);
        let data = assembled(fetched(vec![span], vec![resource_row(1)]));
        let attrs = &spans_of(data.as_traces_data())[0].attributes;
        assert_eq!(
            keys(attrs),
            vec!["s.arr", "s.bool", "s.dbl", "s.empty", "s.int", "s.str"],
            "ascending by reconstructed key"
        );
        assert_eq!(
            value_of(attrs, "s.int"),
            Some(Value::IntValue(42)),
            "an Int64 is never a Double"
        );
        assert_eq!(value_of(attrs, "s.dbl"), Some(Value::DoubleValue(1.5)));
        assert_eq!(value_of(attrs, "s.bool"), Some(Value::BoolValue(true)));
        assert_eq!(
            value_of(attrs, "s.str"),
            Some(Value::StringValue("v".to_string()))
        );
        let Some(Value::ArrayValue(arr)) = value_of(attrs, "s.arr") else {
            panic!("`s.arr` must be an array");
        };
        assert_eq!(
            arr.values
                .iter()
                .filter_map(|v| v.value.clone())
                .collect::<Vec<_>>(),
            vec![
                Value::StringValue("a".to_string()),
                Value::StringValue("b".to_string())
            ],
            "in that element order"
        );
        let Some(Value::ArrayValue(empty)) = value_of(attrs, "s.empty") else {
            panic!("`s.empty` must be an array");
        };
        assert!(empty.values.is_empty());
    }

    /// The second carrier's keys are merged into the SAME sort as the
    /// first's, in all five scopes — so a reader cannot emit one carrier
    /// after the other and pass.
    #[test]
    fn the_two_carriers_merge_into_one_sort() {
        let mut span = span_row([0x11; 8], START);
        // `attrs` holds the LATER key and `attrs_other` the EARLIER one,
        // so concatenating the two carriers in either order gives the
        // wrong sequence.
        span.attrs = json_str("z.last", "zzz");
        span.attrs_other = other(&[kv("a.first", Value::BytesValue(vec![0x01, 0x02]))]);
        let data = assembled(fetched(vec![span], vec![resource_row(1)]));
        let attrs = &spans_of(data.as_traces_data())[0].attributes;
        assert_eq!(keys(attrs), vec!["a.first", "z.last"]);
        assert_eq!(
            value_of(attrs, "a.first"),
            Some(Value::BytesValue(vec![0x01, 0x02])),
            "a value no JSON path can hold comes back from the second carrier"
        );
    }

    /// A resource's entity references come back from their own carrier —
    /// a `Resource` message with only that field populated.
    #[test]
    fn a_resources_entity_references_come_back_from_their_carrier() {
        use opentelemetry_proto::tonic::common::v1::EntityRef;

        let refs = vec![
            EntityRef {
                schema_url: "https://example.invalid/ent".to_string(),
                r#type: "service".to_string(),
                id_keys: vec![SERVICE_NAME_KEY.to_string()],
                description_keys: vec!["a.first".to_string()],
            },
            EntityRef {
                r#type: "host".to_string(),
                ..Default::default()
            },
        ];
        let mut resource = resource_row(1);
        resource.entity_refs = ByteBuf::from(
            Resource {
                entity_refs: refs.clone(),
                ..Default::default()
            }
            .encode_to_vec(),
        );
        let data = assembled(fetched(vec![span_row([0x11; 8], START)], vec![resource]));
        assert_eq!(
            data.as_traces_data().resource_spans[0]
                .resource
                .as_ref()
                .expect("a present resource")
                .entity_refs,
            refs
        );
    }

    /// An undecodable attribute carrier is an error naming the column and
    /// the subject, never a silently dropped attribute.
    #[test]
    fn an_undecodable_attribute_carrier_is_an_error_naming_the_column() {
        let mut span = span_row([0x11; 8], START);
        // A length-delimited field whose declared length runs past the
        // end of the buffer.
        span.attrs_other = ByteBuf::from(vec![0x0a, 0x7f, 0x01]);
        let err =
            AssembledTrace::from_fetched(&TRACE_ID, fetched(vec![span], vec![resource_row(1)]))
                .expect_err("an undecodable carrier must not be dropped");
        let message = err.to_string();
        assert!(
            message.contains("attrs_other"),
            "the message must name the column, got {message:?}"
        );
    }

    /// An empty fetch yields an empty `TracesData` — the unit-level
    /// contract; the v1 handler maps an empty fetch to `404` first.
    #[test]
    fn an_empty_fetch_yields_an_empty_traces_data() {
        let data = assembled(fetched(Vec::new(), Vec::new()));
        assert!(data.as_traces_data().resource_spans.is_empty());
        assert_eq!(data.missing_resources(), 0);
    }

    #[test]
    fn an_empty_assembled_trace_encodes_to_no_bytes() {
        assert!(encode_protobuf(&AssembledTrace::empty()).is_empty());
    }

    #[test]
    fn encode_protobuf_round_trips_through_prost() {
        let data = assembled(fetched(
            vec![span_row([0x11; 8], START)],
            vec![resource_row(1)],
        ));
        let bytes = encode_protobuf(&data);
        let back = TracesData::decode(bytes.as_slice()).expect("decodes");
        assert_eq!(&back, data.as_traces_data());
    }

    #[test]
    fn encode_json_is_otlp_canonical_protojson() {
        let data = assembled(fetched(
            vec![span_row([0x11; 8], START)],
            vec![resource_row(1)],
        ));
        let body = encode_json(&data).expect("encodes");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("valid json");
        let span = &json["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
        assert_eq!(span["spanId"], "1111111111111111", "hex ids");
        assert_eq!(
            span["startTimeUnixNano"], "1700000000000000000",
            "u64 as a string"
        );
    }

    /// The materialisation walk's over-reach control: a sender-supplied
    /// non-default status survives untouched.
    #[test]
    fn a_non_default_status_survives_the_materialisation_walk() {
        let mut span = span_row([0x11; 8], START);
        span.status_code = 2;
        span.status_message = "boom".to_string();
        let data = assembled(fetched(vec![span], vec![resource_row(1)]));
        let status = spans_of(data.as_traces_data())[0]
            .status
            .as_ref()
            .expect("a present status");
        assert_eq!(status.code, 2);
        assert_eq!(status.message, "boom");
    }
}
