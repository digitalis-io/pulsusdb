//! Bounded proto3-JSON (OTLP/JSON) building blocks (issue #115, track 6): the
//! hand-written [`serde::de::DeserializeSeed`] wrappers that replace the
//! vendored derive's UNBOUNDED repeated-field decode with the same per-level and
//! cross-request aggregate caps the protobuf wire pre-scan
//! ([`crate::protocols::otlp_prescan`]) enforces — so JSON and protobuf reject a
//! DoS-shaped body at IDENTICAL thresholds.
//!
//! # Why hand-written seeds, not the derive
//!
//! `serde_json::from_slice::<ExportTraceServiceRequest>` runs the vendored
//! camelCase derive, which materializes EVERY repeated field in full before any
//! count could be checked — the very heap amplification track 5 guards on the
//! protobuf path. These seeds bound each repeated/container field DURING
//! deserialization: the offending element is rejected before it (and the array
//! tail behind it) is materialized.
//!
//! # Hybrid map-visitor (the design the task-manager approved)
//!
//! The vendored derive is camelCase-ONLY with no aliases, and it carries every
//! ADR-0004 patch (P1 non-finite doubles, P2 `AnyValue` visitor, P5 string-enum
//! names, P6 non-swallowing oneofs, hex IDs, u64-as-string, base64) that a
//! second-pass re-decode would have to reproduce by hand. So each container
//! wrapper is a HYBRID: it routes only the RECOGNIZED repeated/container keys
//! (accepting BOTH the camelCase and snake_case spelling for the fan-out /
//! container fields, so an alias-split payload cannot evade a counter, per the v6
//! ruling) through bounded seeds, and finishes the remaining scalar/oneof leaves
//! through the vendored derive. Every wrapper BUFFERS its non-intercepted KNOWN
//! scalar keys (order + duplicates preserved) and finishes them through the
//! vendored [`serde::Deserialize`] via [`finish_via_derive`], so ADR-0004
//! behaviour AND the derive's required-field / duplicate-key semantics stay
//! byte-identical (a `serde_json::Map` would silently collapse a duplicate scalar
//! key the non-`serde(default)` derives reject). Genuinely UNKNOWN keys are NOT
//! buffered: they are skipped with [`serde::de::IgnoredAny`]
//! ([`buffer_scalar_or_skip`]) so an attacker-controlled unknown value tree is
//! never materialized — the vendored derives carry no `deny_unknown_fields`, so
//! dropping an ignored key is byte-identical to replaying-and-ignoring it. The
//! `AnyValue` wrapper additionally
//! reproduces the vendored P2 oneof visitor EXACTLY (last recognized key wins;
//! `arrayValue`/`kvlistValue` require their `values`) while bounding container
//! width/depth — the ONLY behavioral divergence from the vendored decode.
//!
//! # Scalar-leaf spellings stay camelCase (v6 ruling 1)
//!
//! Only the fan-out / repeated CONTAINER fields accept both spellings. Scalar
//! leaves (IDs, names, enum fields, `KeyValue.key`, the `AnyValue` value
//! scalars) remain delegated to the camelCase-only derive — accepting a
//! snake_case leaf would be a conformance change beyond the DoS fix. Current
//! conformance behaviour is preserved exactly.
//!
//! # Shared across signals
//!
//! [`JsonAggregates`], [`AnyValueSeed`], [`ResourceSeed`],
//! [`InstrumentationScopeSeed`], [`KeyValueSeed`], [`EntityRefSeed`], and the
//! [`AccumSeq`] combinator are `pub(crate)` for the logs-JSON (6b) and
//! metrics-JSON (6c) sub-tracks to reuse; only the traces roots
//! ([`decode_traces`] and its `*SpansSeed`/`SpanSeed`/…) are traces-specific.

use std::cell::Cell;
use std::fmt;

use serde::Deserializer;
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, EntityRef, InstrumentationScope, KeyValue, KeyValueList,
};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};

use crate::error::LogsIngestError;
use crate::protocols::otlp_prescan::{
    MAX_ANYVALUE_DEPTH, MAX_ANYVALUE_ELEMENTS, MAX_ATTRIBUTES_PER_ELEMENT, MAX_ENTITY_REF_KEYS,
    MAX_ENTITY_REFS, MAX_EVENTS_PER_SPAN, MAX_LINKS_PER_SPAN, MAX_LOG_RECORDS, MAX_RESOURCE_LOGS,
    MAX_RESOURCE_SPANS, MAX_SCOPE_LOGS, MAX_SCOPE_SPANS, MAX_SPANS, MAX_TOTAL_ATTRIBUTES,
    MAX_TOTAL_EVENTS, MAX_TOTAL_LINKS, MAX_TOTAL_LOG_RECORDS, MAX_TOTAL_SPANS,
};

// ---------------------------------------------------------------------------
// Shared aggregate carrier
// ---------------------------------------------------------------------------

/// Running cross-request aggregate element counters, the JSON analog of
/// [`crate::protocols::otlp_prescan`]'s `Aggregates` — `Cell`-based so a shared
/// `&JsonAggregates` can be threaded (immutably) through the whole seed chain
/// while every bounded sequence charges its element into the matching counter.
/// Monotonic / increment-only: never reset, never decremented, so splitting a
/// payload across siblings, aliases, or duplicate keys cannot evade a cap. Holds
/// every [`crate::protocols::otlp_prescan::AggKind`] equivalent so the logs (6b)
/// and metrics (6c) sub-tracks reuse the same carrier.
#[derive(Default)]
pub(crate) struct JsonAggregates {
    pub(crate) spans: Cell<usize>,
    pub(crate) events: Cell<usize>,
    pub(crate) links: Cell<usize>,
    pub(crate) attributes: Cell<usize>,
    pub(crate) anyvalue_elements: Cell<usize>,
    /// Cross-request data-point count (issue #115 track 6c), the JSON analog
    /// of the protobuf pre-scan's `AggKind::DataPoints` — charged once per
    /// accumulated data point across all five `Metric.data` oneof arms,
    /// capped at `MAX_TOTAL_DATA_POINTS`.
    pub(crate) data_points: Cell<usize>,
    /// Cross-request metric-exemplar count (issue #115 track 6c), the JSON
    /// analog of `AggKind::Exemplars` — charged once per accumulated
    /// `Exemplar` across every data-point shape, capped at
    /// `MAX_TOTAL_EXEMPLARS`.
    pub(crate) exemplars: Cell<usize>,
    /// Cross-request log-record count (issue #115 track 6b), the JSON analog
    /// of the protobuf pre-scan's `AggKind::LogRecords` — charged once per
    /// accumulated `LogRecord`, capped at [`MAX_TOTAL_LOG_RECORDS`].
    pub(crate) log_records: Cell<usize>,
}

/// One shared aggregate charge: the counter cell, its cap, and the error label.
#[derive(Clone, Copy)]
pub(crate) struct AggCharge<'a> {
    cell: &'a Cell<usize>,
    cap: usize,
    field: &'static str,
}

// ---------------------------------------------------------------------------
// Buffer-and-delegate: finish scalar/leaf fields through the vendored derive
// ---------------------------------------------------------------------------

/// Finish a message's non-intercepted (scalar / unknown) fields through the
/// vendored [`serde::Deserialize`] derive, so every scalar semantic —
/// required-field rejection, duplicate-key rejection, camelCase spelling, and
/// the ADR-0004 leaves (hex IDs, u64-as-string, base64, P1 non-finite doubles,
/// P5 enum names) — is BYTE-IDENTICAL to the derive (issue #115 ruling 2). The
/// caller has already stripped and separately bounded the repeated / container /
/// `AnyValue`-bearing children, so nothing unbounded reaches the derive.
///
/// `pairs` preserves field ORDER and DUPLICATES: buffering into a
/// `serde_json::Map` would silently collapse a duplicate key (and so accept a
/// body the non-`serde(default)` derives reject), so the pairs are re-emitted as
/// a JSON object that keeps every occurrence and re-parsed — replaying each key
/// to the derive's `Visitor`, which then rejects a duplicate of a KNOWN field and
/// ignores a duplicate unknown field exactly as it would on the original stream.
/// An EMPTY `pairs` yields `{}`, which the derive maps to `T::default()` for a
/// `serde(default)` message and to a missing-required-field error otherwise —
/// again matching the derive per type.
fn finish_via_derive<T, E>(pairs: &[(String, serde_json::Value)]) -> Result<T, E>
where
    T: serde::de::DeserializeOwned,
    E: de::Error,
{
    let mut buf: Vec<u8> = Vec::with_capacity(2 + pairs.len() * 16);
    buf.push(b'{');
    for (i, (key, value)) in pairs.iter().enumerate() {
        if i > 0 {
            buf.push(b',');
        }
        serde_json::to_writer(&mut buf, key).map_err(de::Error::custom)?;
        buf.push(b':');
        serde_json::to_writer(&mut buf, value).map_err(de::Error::custom)?;
    }
    buf.push(b'}');
    serde_json::from_slice::<T>(&buf).map_err(de::Error::custom)
}

/// Buffer a KNOWN scalar field's value for [`finish_via_derive`], or SKIP a
/// genuinely UNKNOWN key's value via [`IgnoredAny`] WITHOUT materializing it
/// (issue #115 finding 2). `known_scalars` is the message's camelCase scalar-leaf
/// field set (scalar leaves stay camelCase-only per the v6 ruling).
///
/// The vendored derives carry NO `deny_unknown_fields` (verified against the
/// vendored source), so they IGNORE unknown fields. An unknown key that is absent
/// from the replayed buffer therefore yields a BYTE-IDENTICAL result to one that
/// is present-and-ignored — so we simply drop it, skipping its value with
/// `IgnoredAny` (depth-bounded by serde_json's own recursion limit) rather than
/// deserializing an attacker-controlled unknown value tree into a
/// `serde_json::Value` first. KNOWN scalar keys are still buffered (their values
/// are bounded by the scalar field's own type) and replayed so the derive keeps
/// enforcing duplicate-known-scalar rejection and every ADR-0004 leaf semantic.
fn buffer_scalar_or_skip<'de, A>(
    key: String,
    known_scalars: &[&str],
    map: &mut A,
    pairs: &mut Vec<(String, serde_json::Value)>,
) -> Result<(), A::Error>
where
    A: MapAccess<'de>,
{
    if known_scalars.contains(&key.as_str()) {
        // KNOWN scalar leaf: buffer its value (bounded by the scalar field's own
        // type) and replay it through the vendored derive, preserving duplicates
        // so duplicate-known-scalar rejection and every ADR-0004 leaf semantic
        // are enforced exactly as the derive would.
        pairs.push((key, map.next_value::<serde_json::Value>()?));
    } else {
        // Genuinely UNKNOWN key: the vendored derives carry no
        // `deny_unknown_fields`, so an absent-and-ignored key is byte-identical
        // to a present-and-ignored one — skip its value with `IgnoredAny`
        // (issue #115 finding 2) rather than materializing an attacker-controlled
        // value tree into a `serde_json::Value` first.
        map.next_value::<IgnoredAny>()?;
    }
    Ok(())
}

/// camelCase scalar-leaf field names each buffer-and-delegate seed hands to the
/// vendored derive (everything NOT intercepted for bounding). Scalar leaves stay
/// camelCase-only (v6 ruling 1); a snake_case spelling is an unknown key the
/// derive ignores, so it is deliberately absent here.
const KEY_VALUE_SCALARS: &[&str] = &["key", "keyStrindex"];
const RESOURCE_SCALARS: &[&str] = &["droppedAttributesCount"];
const SCOPE_SCALARS: &[&str] = &["name", "version", "droppedAttributesCount"];
const ENTITY_REF_SCALARS: &[&str] = &["schemaUrl", "type"];
const SPAN_SCALARS: &[&str] = &[
    "traceId",
    "spanId",
    "traceState",
    "parentSpanId",
    "flags",
    "name",
    "kind",
    "startTimeUnixNano",
    "endTimeUnixNano",
    "droppedAttributesCount",
    "droppedEventsCount",
    "droppedLinksCount",
];
// `Status` is a MESSAGE (not a scalar leaf): `Span.status` is intercepted by
// [`StatusSeed`] so an attacker cannot pad the status object with an unknown key
// carrying a wide value tree (issue #115 track-6a round-3 finding). `code` is an
// enum, `message` a string — both true scalar leaves for the derive.
const STATUS_SCALARS: &[&str] = &["code", "message"];
const EVENT_SCALARS: &[&str] = &["timeUnixNano", "name", "droppedAttributesCount"];
const LINK_SCALARS: &[&str] = &[
    "traceId",
    "spanId",
    "traceState",
    "flags",
    "droppedAttributesCount",
];
const SPANS_ENVELOPE_SCALARS: &[&str] = &["schemaUrl"];
// `ResourceLogs`/`ScopeLogs` carry the same sole scalar leaf (`schemaUrl`) as
// `ResourceSpans`/`ScopeSpans` above — named separately per signal so the
// per-type scalar-list audit (issue #115 track-6a round-4 lesson) stays
// explicit rather than relying on cross-signal reuse of a same-shaped list.
const RESOURCE_LOGS_SCALARS: &[&str] = &["schemaUrl"];
const SCOPE_LOGS_SCALARS: &[&str] = &["schemaUrl"];
// `LogRecord`'s scalar leaves: `attributes` (repeated `KeyValue`) and `body`
// (a MESSAGE `AnyValue`) are intercepted by bounded seeds — neither appears
// here (issue #115 track-6a round-3 lesson: a message in a scalar list is the
// status-DoS class).
const LOG_RECORD_SCALARS: &[&str] = &[
    "timeUnixNano",
    "observedTimeUnixNano",
    "severityNumber",
    "severityText",
    "droppedAttributesCount",
    "flags",
    "traceId",
    "spanId",
    "eventName",
];

// ---------------------------------------------------------------------------
// Bounded-sequence combinator
// ---------------------------------------------------------------------------

/// A [`DeserializeSeed`] over a JSON array that ACCUMULATES its elements into a
/// caller-owned `target` (so repeated occurrences of a field — a duplicate key
/// or a camelCase/snake_case alias split — accumulate into ONE vector and one
/// set of counters), enforcing an optional per-level cap and an optional shared
/// aggregate cap DURING deserialization. `Value = ()`: the elements land in
/// `target`.
///
/// Reject-before-materialize: when a cap is reached the visitor probes for the
/// next element with [`IgnoredAny`] (which walks but never *builds* it) and
/// rejects on the first over-cap element — so neither the offending element nor
/// the array tail behind it is materialized.
pub(crate) struct AccumSeq<'a, T, Mk> {
    target: &'a mut Vec<T>,
    per_level: Option<(usize, &'static str)>,
    agg: Option<AggCharge<'a>>,
    make_seed: Mk,
}

impl<'de, 'a, T, Mk, S> DeserializeSeed<'de> for AccumSeq<'a, T, Mk>
where
    Mk: FnMut() -> S,
    S: DeserializeSeed<'de, Value = T>,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(AccumSeqVisitor {
            target: self.target,
            per_level: self.per_level,
            agg: self.agg,
            make_seed: self.make_seed,
        })
    }
}

struct AccumSeqVisitor<'a, T, Mk> {
    target: &'a mut Vec<T>,
    per_level: Option<(usize, &'static str)>,
    agg: Option<AggCharge<'a>>,
    make_seed: Mk,
}

impl<'de, 'a, T, Mk, S> Visitor<'de> for AccumSeqVisitor<'a, T, Mk>
where
    Mk: FnMut() -> S,
    S: DeserializeSeed<'de, Value = T>,
{
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JSON array")
    }

    fn visit_seq<A>(mut self, mut seq: A) -> Result<(), A::Error>
    where
        A: SeqAccess<'de>,
    {
        loop {
            // Per-level cap: at cap, a further element is over the per-element
            // bound — reject it before it is materialized.
            if let Some((cap, field)) = self.per_level
                && self.target.len() >= cap
            {
                if seq.next_element::<IgnoredAny>()?.is_some() {
                    return Err(de::Error::custom(format!(
                        "{field} exceeds the per-element bound of {cap}"
                    )));
                }
                return Ok(());
            }
            // Shared aggregate cap: at cap, a further element across the whole
            // request is over the aggregate bound — reject before materializing.
            if let Some(a) = self.agg
                && a.cell.get() >= a.cap
            {
                if seq.next_element::<IgnoredAny>()?.is_some() {
                    return Err(de::Error::custom(format!(
                        "{} exceeds the aggregate bound of {}",
                        a.field, a.cap
                    )));
                }
                return Ok(());
            }
            match seq.next_element_seed((self.make_seed)())? {
                Some(elem) => {
                    if let Some(a) = self.agg {
                        a.cell.set(a.cell.get() + 1);
                    }
                    self.target.push(elem);
                }
                None => return Ok(()),
            }
        }
    }
}

/// Convenience: charge a repeated MESSAGE field into `target` through a bounded
/// [`AccumSeq`], via `map.next_value_seed`. The `make_seed` closure produces one
/// element seed per element.
fn accumulate_msgs<'de, A, T, Mk, S>(
    map: &mut A,
    target: &mut Vec<T>,
    per_level: (usize, &'static str),
    agg: Option<AggCharge<'_>>,
    make_seed: Mk,
) -> Result<(), A::Error>
where
    A: MapAccess<'de>,
    Mk: FnMut() -> S,
    S: DeserializeSeed<'de, Value = T>,
{
    map.next_value_seed(AccumSeq {
        target,
        per_level: Some(per_level),
        agg,
        make_seed,
    })
}

// ---------------------------------------------------------------------------
// Option wrapper (proto3-JSON writes an absent singular message as `null`)
// ---------------------------------------------------------------------------

/// Wraps an inner element seed so a JSON `null` singular-message value decodes to
/// `None` (proto3-JSON emits an absent `resource`/`scope`/`value` as `null`; the
/// vendored derive's `Option` handles this, so the hand routes must too).
pub(crate) struct OptionSeed<S>(pub(crate) S);

impl<'de, S> DeserializeSeed<'de> for OptionSeed<S>
where
    S: DeserializeSeed<'de>,
{
    type Value = Option<S::Value>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_option(OptionVisitor(self.0))
    }
}

struct OptionVisitor<S>(S);

impl<'de, S> Visitor<'de> for OptionVisitor<S>
where
    S: DeserializeSeed<'de>,
{
    type Value = Option<S::Value>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("an optional value")
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        self.0.deserialize(deserializer).map(Some)
    }
}

// ---------------------------------------------------------------------------
// AnyValue (bounded width + depth; scalars delegated to the vendored P2 visitor)
// ---------------------------------------------------------------------------

/// Bounded seed for an `AnyValue` object. Caps the `arrayValue`/`kvlistValue`
/// container element WIDTH (shared aggregate [`MAX_ANYVALUE_ELEMENTS`]) and
/// nesting DEPTH ([`MAX_ANYVALUE_DEPTH`], reused from track 1 / the wire
/// pre-scan), accepting BOTH spellings of the two container keys. Every scalar
/// arm (`stringValue`/`boolValue`/`intValue`/`doubleValue`/`bytesValue`/
/// `stringValueStrindex`) is buffered and finished through the vendored derive so
/// ADR-0004 P1 (non-finite doubles), the int64-as-string and base64 leaves
/// decode byte-identically.
pub(crate) struct AnyValueSeed<'a> {
    pub(crate) agg: &'a JsonAggregates,
    /// The nesting level of THIS `AnyValue` (1 for a top-level attribute value;
    /// container level + 1 for a nested element/entry). Over [`MAX_ANYVALUE_DEPTH`]
    /// is rejected before recursing further.
    pub(crate) depth: usize,
}

impl<'de> DeserializeSeed<'de> for AnyValueSeed<'_> {
    type Value = AnyValue;

    fn deserialize<D>(self, deserializer: D) -> Result<AnyValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        if self.depth > MAX_ANYVALUE_DEPTH {
            return Err(de::Error::custom(format!(
                "AnyValue nesting depth exceeds the bound of {MAX_ANYVALUE_DEPTH}"
            )));
        }
        deserializer.deserialize_map(AnyValueVisitor {
            agg: self.agg,
            depth: self.depth,
        })
    }
}

struct AnyValueVisitor<'a> {
    agg: &'a JsonAggregates,
    depth: usize,
}

impl<'de> Visitor<'de> for AnyValueVisitor<'_> {
    type Value = AnyValue;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON AnyValue object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<AnyValue, A::Error>
    where
        A: MapAccess<'de>,
    {
        let child_depth = self.depth + 1;
        let elem_agg = AggCharge {
            cell: &self.agg.anyvalue_elements,
            cap: MAX_ANYVALUE_ELEMENTS,
            field: "AnyValue elements",
        };
        // Reproduce the vendored P2 `deserialize_from_value` oneof visitor EXACTLY
        // (issue #115 finding 1): iterate ALL keys in order, the LAST recognized
        // oneof key wins (no single-member rejection), unknown keys — including
        // the profiling `stringValueStrindex`, which the P2 visitor does not
        // recognize — are skipped, and a map with no recognized key errors. The
        // ONLY divergence from the vendored decode is the width/depth REJECTION
        // the container seeds add; every scalar arm is finished through the
        // vendored `AnyValue` derive so P1 non-finite doubles, int64-as-string and
        // base64 stay byte-identical, and a container that later loses to a scalar
        // is still bounded-decoded exactly as the vendored visitor materializes it.
        let mut value: Option<AnyValueVariant> = None;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "arrayValue" | "array_value" => {
                    let array = map.next_value_seed(BoundedArrayValueSeed {
                        agg: self.agg,
                        elem_depth: child_depth,
                        elem_agg,
                    })?;
                    value = Some(AnyValueVariant::ArrayValue(array));
                }
                "kvlistValue" | "kvlist_value" => {
                    let kvlist = map.next_value_seed(BoundedKvlistValueSeed {
                        agg: self.agg,
                        entry_value_depth: child_depth,
                        elem_agg,
                    })?;
                    value = Some(AnyValueVariant::KvlistValue(kvlist));
                }
                "stringValue" | "boolValue" | "intValue" | "doubleValue" | "bytesValue" => {
                    // Delegate this scalar oneof arm to the vendored `AnyValue`
                    // derive (single-key object) so its exact decode is preserved;
                    // last recognized key wins, matching the P2 visitor.
                    let raw = map.next_value::<serde_json::Value>()?;
                    let mut one = serde_json::Map::new();
                    one.insert(key, raw);
                    let av: AnyValue = serde_json::from_value(serde_json::Value::Object(one))
                        .map_err(de::Error::custom)?;
                    value = av.value;
                }
                _ => {
                    // Unknown key (the P2 visitor `continue`s past these).
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        match value {
            Some(v) => Ok(AnyValue { value: Some(v) }),
            // Byte-identical to the vendored P2 visitor's terminal error.
            None => Err(de::Error::custom(
                "Invalid data for Value, no known keys found",
            )),
        }
    }
}

/// Bounded seed for an `ArrayValue` object (`{"values":[AnyValue, …]}`) returning
/// the assembled [`ArrayValue`]. Its `values` are repeated `AnyValue`s at
/// `elem_depth`, charged against the shared [`MAX_ANYVALUE_ELEMENTS`] aggregate
/// (no per-level cap — depth is the complementary bound). Matches the vendored
/// `ArrayValue` derive: a MISSING `values` and a DUPLICATE `values` are rejected;
/// unknown fields are ignored.
struct BoundedArrayValueSeed<'a> {
    agg: &'a JsonAggregates,
    elem_depth: usize,
    elem_agg: AggCharge<'a>,
}

impl<'de> DeserializeSeed<'de> for BoundedArrayValueSeed<'_> {
    type Value = ArrayValue;

    fn deserialize<D>(self, deserializer: D) -> Result<ArrayValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(BoundedArrayValueVisitor {
            agg: self.agg,
            elem_depth: self.elem_depth,
            elem_agg: self.elem_agg,
        })
    }
}

struct BoundedArrayValueVisitor<'a> {
    agg: &'a JsonAggregates,
    elem_depth: usize,
    elem_agg: AggCharge<'a>,
}

impl<'de> Visitor<'de> for BoundedArrayValueVisitor<'_> {
    type Value = ArrayValue;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON ArrayValue object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<ArrayValue, A::Error>
    where
        A: MapAccess<'de>,
    {
        let agg = self.agg;
        let elem_depth = self.elem_depth;
        let mut values: Vec<AnyValue> = Vec::new();
        let mut seen = false;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "values" => {
                    if seen {
                        return Err(de::Error::duplicate_field("values"));
                    }
                    seen = true;
                    map.next_value_seed(AccumSeq {
                        target: &mut values,
                        per_level: None,
                        agg: Some(self.elem_agg),
                        make_seed: || AnyValueSeed {
                            agg,
                            depth: elem_depth,
                        },
                    })?;
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        if !seen {
            return Err(de::Error::missing_field("values"));
        }
        Ok(ArrayValue { values })
    }
}

/// Bounded seed for a `KeyValueList` object (`{"values":[KeyValue, …]}`) returning
/// the assembled [`KeyValueList`]. Its `values` are repeated `KeyValue`s whose own
/// value `AnyValue` sits at `entry_value_depth`, charged against the shared
/// [`MAX_ANYVALUE_ELEMENTS`] aggregate. Matches the vendored `KeyValueList`
/// derive: MISSING and DUPLICATE `values` are rejected; unknown fields ignored.
struct BoundedKvlistValueSeed<'a> {
    agg: &'a JsonAggregates,
    entry_value_depth: usize,
    elem_agg: AggCharge<'a>,
}

impl<'de> DeserializeSeed<'de> for BoundedKvlistValueSeed<'_> {
    type Value = KeyValueList;

    fn deserialize<D>(self, deserializer: D) -> Result<KeyValueList, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(BoundedKvlistValueVisitor {
            agg: self.agg,
            entry_value_depth: self.entry_value_depth,
            elem_agg: self.elem_agg,
        })
    }
}

struct BoundedKvlistValueVisitor<'a> {
    agg: &'a JsonAggregates,
    entry_value_depth: usize,
    elem_agg: AggCharge<'a>,
}

impl<'de> Visitor<'de> for BoundedKvlistValueVisitor<'_> {
    type Value = KeyValueList;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON KvlistValue object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<KeyValueList, A::Error>
    where
        A: MapAccess<'de>,
    {
        let agg = self.agg;
        let value_depth = self.entry_value_depth;
        let mut values: Vec<KeyValue> = Vec::new();
        let mut seen = false;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "values" => {
                    if seen {
                        return Err(de::Error::duplicate_field("values"));
                    }
                    seen = true;
                    map.next_value_seed(AccumSeq {
                        target: &mut values,
                        per_level: None,
                        agg: Some(self.elem_agg),
                        make_seed: || KeyValueSeed { agg, value_depth },
                    })?;
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        if !seen {
            return Err(de::Error::missing_field("values"));
        }
        Ok(KeyValueList { values })
    }
}

// ---------------------------------------------------------------------------
// KeyValue (built field-by-field; its value AnyValue is depth-bounded)
// ---------------------------------------------------------------------------

/// Bounded seed for a `KeyValue`. `key`/`keyStrindex` are plain scalar leaves
/// (camelCase-only, delegated behaviour); `value` routes through the
/// depth-bounded [`AnyValueSeed`] so a nested attribute tree cannot recurse past
/// [`MAX_ANYVALUE_DEPTH`]. `value_depth` is the level its value `AnyValue` holds.
pub(crate) struct KeyValueSeed<'a> {
    pub(crate) agg: &'a JsonAggregates,
    pub(crate) value_depth: usize,
}

impl<'de> DeserializeSeed<'de> for KeyValueSeed<'_> {
    type Value = KeyValue;

    fn deserialize<D>(self, deserializer: D) -> Result<KeyValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for KeyValueSeed<'_> {
    type Value = KeyValue;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON KeyValue object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<KeyValue, A::Error>
    where
        A: MapAccess<'de>,
    {
        // Finding 2: intercept ONLY the `AnyValue`-bearing `value` child for
        // depth bounding; BUFFER every scalar leaf (`key`, `keyStrindex`, unknown)
        // and finish through the vendored `KeyValue` derive, so the missing
        // required `key` and duplicate scalar keys are rejected — not silently
        // defaulted / last-write-win — exactly as the non-`serde(default)` derive.
        let mut value: Option<AnyValue> = None;
        let mut value_seen = false;
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(k) = map.next_key::<String>()? {
            if k == "value" {
                if value_seen {
                    return Err(de::Error::duplicate_field("value"));
                }
                value_seen = true;
                value = map.next_value_seed(OptionSeed(AnyValueSeed {
                    agg: self.agg,
                    depth: self.value_depth,
                }))?;
            } else {
                buffer_scalar_or_skip(k, KEY_VALUE_SCALARS, &mut map, &mut pairs)?;
            }
        }
        let mut kv: KeyValue = finish_via_derive(&pairs)?;
        kv.value = value;
        Ok(kv)
    }
}

/// Charges the shared attribute aggregate for a `KeyValue` list at
/// [`MAX_ATTRIBUTES_PER_ELEMENT`] per element + [`MAX_TOTAL_ATTRIBUTES`] across
/// the request — the one attribute charge every attribute-bearing message reuses.
fn accumulate_attributes<'de, A>(
    map: &mut A,
    target: &mut Vec<KeyValue>,
    agg: &JsonAggregates,
) -> Result<(), A::Error>
where
    A: MapAccess<'de>,
{
    accumulate_msgs(
        map,
        target,
        (MAX_ATTRIBUTES_PER_ELEMENT, "attributes"),
        Some(AggCharge {
            cell: &agg.attributes,
            cap: MAX_TOTAL_ATTRIBUTES,
            field: "total attributes",
        }),
        || KeyValueSeed {
            agg,
            value_depth: 1,
        },
    )
}

// ---------------------------------------------------------------------------
// Resource / InstrumentationScope / EntityRef (built field-by-field)
// ---------------------------------------------------------------------------

/// Bounded seed for a `Resource`: `attributes` (attr cap) and `entityRefs`
/// (`MAX_ENTITY_REFS`, both spellings). `droppedAttributesCount` is a plain
/// scalar. Built field-by-field — no tricky leaves need the derive.
pub(crate) struct ResourceSeed<'a> {
    pub(crate) agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for ResourceSeed<'_> {
    type Value = Resource;

    fn deserialize<D>(self, deserializer: D) -> Result<Resource, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for ResourceSeed<'_> {
    type Value = Resource;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON Resource object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Resource, A::Error>
    where
        A: MapAccess<'de>,
    {
        // Intercept the repeated `attributes` / `entityRefs` children for bounding;
        // BUFFER the scalar leaf (`droppedAttributesCount`, unknown) and finish
        // through the vendored `Resource` derive so duplicate scalar keys are
        // rejected as the derive does (`Resource` is `serde(default)`, so a MISSING
        // scalar still defaults — an empty buffer yields `Resource::default()`).
        let mut attributes: Vec<KeyValue> = Vec::new();
        let mut entity_refs: Vec<EntityRef> = Vec::new();
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "attributes" => accumulate_attributes(&mut map, &mut attributes, self.agg)?,
                "entityRefs" | "entity_refs" => accumulate_msgs(
                    &mut map,
                    &mut entity_refs,
                    (MAX_ENTITY_REFS, "entityRefs"),
                    None,
                    || EntityRefSeed,
                )?,
                _ => buffer_scalar_or_skip(key, RESOURCE_SCALARS, &mut map, &mut pairs)?,
            }
        }
        let mut resource: Resource = finish_via_derive(&pairs)?;
        resource.attributes = attributes;
        resource.entity_refs = entity_refs;
        Ok(resource)
    }
}

/// Bounded seed for an `EntityRef`: `idKeys`/`descriptionKeys` (repeated strings,
/// `MAX_ENTITY_REF_KEYS`, both spellings). `schemaUrl`/`type` are plain scalars.
pub(crate) struct EntityRefSeed;

impl<'de> DeserializeSeed<'de> for EntityRefSeed {
    type Value = EntityRef;

    fn deserialize<D>(self, deserializer: D) -> Result<EntityRef, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for EntityRefSeed {
    type Value = EntityRef;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON EntityRef object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<EntityRef, A::Error>
    where
        A: MapAccess<'de>,
    {
        // Finding 2: intercept ONLY the repeated `idKeys` / `descriptionKeys`
        // children (both spellings) for bounding; BUFFER the scalar leaves
        // (`schemaUrl`, `type`, unknown) and finish through the vendored
        // `EntityRef` derive, so the missing required `schemaUrl` / `type` and
        // duplicate scalar keys are rejected — `EntityRef` is NOT `serde(default)`,
        // so an empty buffer is a missing-required error there, matching the derive.
        let mut id_keys: Vec<String> = Vec::new();
        let mut description_keys: Vec<String> = Vec::new();
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "idKeys" | "id_keys" => {
                    accumulate_strings(&mut map, &mut id_keys, (MAX_ENTITY_REF_KEYS, "idKeys"))?
                }
                "descriptionKeys" | "description_keys" => accumulate_strings(
                    &mut map,
                    &mut description_keys,
                    (MAX_ENTITY_REF_KEYS, "descriptionKeys"),
                )?,
                _ => buffer_scalar_or_skip(key, ENTITY_REF_SCALARS, &mut map, &mut pairs)?,
            }
        }
        // The vendored `EntityRef` derive (no `serde(default)`) requires ALL fields
        // present, including the repeated `idKeys`/`descriptionKeys` we intercept.
        // Inject empty-array placeholders for the two intercepted repeated fields
        // so the delegate validates ONLY the SCALAR `schemaUrl`/`type` semantics
        // (missing-required + duplicate-key reject) — the repeated fields keep
        // their bounded, optional, accumulate-duplicates behaviour, matching the
        // protobuf path (JSON and protobuf reject identically). The real bounded
        // values are set below.
        pairs.push(("idKeys".to_string(), serde_json::Value::Array(Vec::new())));
        pairs.push((
            "descriptionKeys".to_string(),
            serde_json::Value::Array(Vec::new()),
        ));
        let mut entity_ref: EntityRef = finish_via_derive(&pairs)?;
        entity_ref.id_keys = id_keys;
        entity_ref.description_keys = description_keys;
        Ok(entity_ref)
    }
}

/// Charges a repeated `string` field (no aggregate; per-level only).
fn accumulate_strings<'de, A>(
    map: &mut A,
    target: &mut Vec<String>,
    per_level: (usize, &'static str),
) -> Result<(), A::Error>
where
    A: MapAccess<'de>,
{
    accumulate_msgs(map, target, per_level, None, || {
        std::marker::PhantomData::<String>
    })
}

/// Bounded seed for an `InstrumentationScope`: `attributes` (attr cap).
/// `name`/`version` are plain scalars.
pub(crate) struct InstrumentationScopeSeed<'a> {
    pub(crate) agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for InstrumentationScopeSeed<'_> {
    type Value = InstrumentationScope;

    fn deserialize<D>(self, deserializer: D) -> Result<InstrumentationScope, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for InstrumentationScopeSeed<'_> {
    type Value = InstrumentationScope;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON InstrumentationScope object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<InstrumentationScope, A::Error>
    where
        A: MapAccess<'de>,
    {
        // Intercept the repeated `attributes` child for bounding; BUFFER the
        // scalar leaves (`name`, `version`, `droppedAttributesCount`, unknown) and
        // finish through the vendored `InstrumentationScope` derive so duplicate
        // scalar keys are rejected as the derive does (it is `serde(default)`, so a
        // MISSING scalar still defaults — an empty buffer yields the default).
        let mut attributes: Vec<KeyValue> = Vec::new();
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "attributes" => accumulate_attributes(&mut map, &mut attributes, self.agg)?,
                _ => buffer_scalar_or_skip(key, SCOPE_SCALARS, &mut map, &mut pairs)?,
            }
        }
        let mut scope: InstrumentationScope = finish_via_derive(&pairs)?;
        scope.attributes = attributes;
        Ok(scope)
    }
}

// ---------------------------------------------------------------------------
// Traces roots
// ---------------------------------------------------------------------------

/// Decodes a proto3-JSON `ExportTraceServiceRequest` with every reachable
/// repeated/container field bounded DURING deserialization (issue #115 track 6a).
/// A cap/depth violation is a whole-request `serde` error →
/// [`LogsIngestError::DecodeJson`] (HTTP 400 / `google.rpc.Status.code = 3`),
/// consistent with the protobuf pre-scan's whole-request reject and track 2's
/// Loki JSON bounds.
pub(crate) fn decode_traces(body: &[u8]) -> Result<ExportTraceServiceRequest, LogsIngestError> {
    let agg = JsonAggregates::default();
    let mut de = serde_json::Deserializer::from_slice(body);
    let req = ExportTraceServiceRequestSeed { agg: &agg }.deserialize(&mut de)?;
    // Reject trailing garbage exactly as `serde_json::from_slice` would.
    de.end()?;
    Ok(req)
}

struct ExportTraceServiceRequestSeed<'a> {
    agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for ExportTraceServiceRequestSeed<'_> {
    type Value = ExportTraceServiceRequest;

    fn deserialize<D>(self, deserializer: D) -> Result<ExportTraceServiceRequest, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for ExportTraceServiceRequestSeed<'_> {
    type Value = ExportTraceServiceRequest;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON ExportTraceServiceRequest object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<ExportTraceServiceRequest, A::Error>
    where
        A: MapAccess<'de>,
    {
        let agg = self.agg;
        let mut resource_spans: Vec<ResourceSpans> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "resourceSpans" | "resource_spans" => accumulate_msgs(
                    &mut map,
                    &mut resource_spans,
                    (MAX_RESOURCE_SPANS, "resourceSpans"),
                    None,
                    || ResourceSpansSeed { agg },
                )?,
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(ExportTraceServiceRequest { resource_spans })
    }
}

struct ResourceSpansSeed<'a> {
    agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for ResourceSpansSeed<'_> {
    type Value = ResourceSpans;

    fn deserialize<D>(self, deserializer: D) -> Result<ResourceSpans, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for ResourceSpansSeed<'_> {
    type Value = ResourceSpans;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON ResourceSpans object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<ResourceSpans, A::Error>
    where
        A: MapAccess<'de>,
    {
        // Intercept the singular `resource` (bounded seed) and the repeated
        // `scopeSpans` (bounded) children; BUFFER the scalar leaf (`schemaUrl`) and
        // finish through the vendored `ResourceSpans` derive so a DUPLICATE known
        // scalar rejects exactly as the `serde(default)` derive does — a
        // last-write-win hand-assign silently accepted it (issue #115 finding 1).
        // The intercepted `resource` also guards its own duplicate (the derive
        // would reject a repeated singular message; the bounded seed must too).
        let agg = self.agg;
        let mut resource: Option<Resource> = None;
        let mut resource_seen = false;
        let mut scope_spans: Vec<ScopeSpans> = Vec::new();
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "resource" => {
                    if resource_seen {
                        return Err(de::Error::duplicate_field("resource"));
                    }
                    resource_seen = true;
                    resource = map.next_value_seed(OptionSeed(ResourceSeed { agg }))?;
                }
                "scopeSpans" | "scope_spans" => accumulate_msgs(
                    &mut map,
                    &mut scope_spans,
                    (MAX_SCOPE_SPANS, "scopeSpans"),
                    None,
                    || ScopeSpansSeed { agg },
                )?,
                _ => buffer_scalar_or_skip(key, SPANS_ENVELOPE_SCALARS, &mut map, &mut pairs)?,
            }
        }
        let mut resource_spans: ResourceSpans = finish_via_derive(&pairs)?;
        resource_spans.resource = resource;
        resource_spans.scope_spans = scope_spans;
        Ok(resource_spans)
    }
}

struct ScopeSpansSeed<'a> {
    agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for ScopeSpansSeed<'_> {
    type Value = ScopeSpans;

    fn deserialize<D>(self, deserializer: D) -> Result<ScopeSpans, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for ScopeSpansSeed<'_> {
    type Value = ScopeSpans;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON ScopeSpans object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<ScopeSpans, A::Error>
    where
        A: MapAccess<'de>,
    {
        // Same buffer-and-delegate contract as `ResourceSpansSeed`: intercept the
        // singular `scope` (dup-guarded) and repeated `spans` (bounded); BUFFER the
        // scalar `schemaUrl` and finish through the vendored `ScopeSpans` derive so
        // a duplicate `schemaUrl` rejects exactly as the derive does (issue #115
        // finding 1).
        let agg = self.agg;
        let mut scope: Option<InstrumentationScope> = None;
        let mut scope_seen = false;
        let mut spans: Vec<Span> = Vec::new();
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "scope" => {
                    if scope_seen {
                        return Err(de::Error::duplicate_field("scope"));
                    }
                    scope_seen = true;
                    scope = map.next_value_seed(OptionSeed(InstrumentationScopeSeed { agg }))?
                }
                "spans" => accumulate_msgs(
                    &mut map,
                    &mut spans,
                    (MAX_SPANS, "spans"),
                    Some(AggCharge {
                        cell: &agg.spans,
                        cap: MAX_TOTAL_SPANS,
                        field: "total spans",
                    }),
                    || SpanSeed { agg },
                )?,
                _ => buffer_scalar_or_skip(key, SPANS_ENVELOPE_SCALARS, &mut map, &mut pairs)?,
            }
        }
        let mut scope_spans: ScopeSpans = finish_via_derive(&pairs)?;
        scope_spans.scope = scope;
        scope_spans.spans = spans;
        Ok(scope_spans)
    }
}

/// Bounded seed for a `Span`. Routes `attributes`/`events`/`links` through
/// bounded seeds and BUFFERS every other key (hex `traceId`/`spanId`/
/// `parentSpanId`, u64-as-string timestamps, the `kind` enum, `status` with its
/// enum `code`, `traceState`, `flags`, dropped counts) into a
/// `serde_json::Value::Object`, then finishes those leaves through the vendored
/// derive — so ADR-0004 hex/u64-string/P5-enum decode stays byte-identical.
/// Bounded seed for `Span.status` (a `Status` MESSAGE, not a scalar leaf).
/// `Status` carries no repeated fields and no nested messages, so this buffers
/// its scalar leaves (`code`/`message`) and skips any UNKNOWN nested key via
/// [`IgnoredAny`] (issue #115 track-6a round-3), then finishes through the
/// vendored `Status` derive — matching its exact scalar/enum semantics while
/// never materializing an attacker-controlled unknown value tree.
struct StatusSeed;

impl<'de> DeserializeSeed<'de> for StatusSeed {
    type Value = Status;

    fn deserialize<D>(self, deserializer: D) -> Result<Status, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for StatusSeed {
    type Value = Status;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON Status object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Status, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            buffer_scalar_or_skip(key, STATUS_SCALARS, &mut map, &mut pairs)?;
        }
        finish_via_derive(&pairs)
    }
}

struct SpanSeed<'a> {
    agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for SpanSeed<'_> {
    type Value = Span;

    fn deserialize<D>(self, deserializer: D) -> Result<Span, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for SpanSeed<'_> {
    type Value = Span;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON Span object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Span, A::Error>
    where
        A: MapAccess<'de>,
    {
        let agg = self.agg;
        let mut attributes: Vec<KeyValue> = Vec::new();
        let mut events: Vec<Event> = Vec::new();
        let mut links: Vec<Link> = Vec::new();
        let mut status: Option<Status> = None;
        let mut status_seen = false;
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "attributes" => accumulate_attributes(&mut map, &mut attributes, agg)?,
                "status" => {
                    // A MESSAGE, not a scalar: intercept via the bounded StatusSeed
                    // (dup-guarded like the derive) so unknown nested keys are
                    // IgnoredAny-skipped, never materialized (issue #115).
                    if status_seen {
                        return Err(de::Error::duplicate_field("status"));
                    }
                    status_seen = true;
                    status = map.next_value_seed(OptionSeed(StatusSeed))?;
                }
                "events" => accumulate_msgs(
                    &mut map,
                    &mut events,
                    (MAX_EVENTS_PER_SPAN, "events"),
                    Some(AggCharge {
                        cell: &agg.events,
                        cap: MAX_TOTAL_EVENTS,
                        field: "total span events",
                    }),
                    || EventSeed { agg },
                )?,
                "links" => accumulate_msgs(
                    &mut map,
                    &mut links,
                    (MAX_LINKS_PER_SPAN, "links"),
                    Some(AggCharge {
                        cell: &agg.links,
                        cap: MAX_TOTAL_LINKS,
                        field: "total span links",
                    }),
                    || LinkSeed { agg },
                )?,
                _ => buffer_scalar_or_skip(key, SPAN_SCALARS, &mut map, &mut pairs)?,
            }
        }
        // Empty scalar buffer → `Span::default()` (byte-identical to the
        // `serde(default)` derive on an empty object), skipping a per-span
        // delegate on the common all-repeated-fields shape; a non-empty buffer is
        // finished through the vendored derive so duplicate scalar keys reject.
        let mut span = if pairs.is_empty() {
            Span::default()
        } else {
            finish_via_derive(&pairs)?
        };
        span.attributes = attributes;
        span.events = events;
        span.links = links;
        span.status = status;
        Ok(span)
    }
}

/// Bounded seed for a `Span.Event`: routes `attributes`, buffers `name` and the
/// u64-as-string `timeUnixNano` (finished through the vendored derive).
struct EventSeed<'a> {
    agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for EventSeed<'_> {
    type Value = Event;

    fn deserialize<D>(self, deserializer: D) -> Result<Event, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for EventSeed<'_> {
    type Value = Event;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON Span.Event object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Event, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut attributes: Vec<KeyValue> = Vec::new();
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "attributes" => accumulate_attributes(&mut map, &mut attributes, self.agg)?,
                _ => buffer_scalar_or_skip(key, EVENT_SCALARS, &mut map, &mut pairs)?,
            }
        }
        let mut event = if pairs.is_empty() {
            Event::default()
        } else {
            finish_via_derive(&pairs)?
        };
        event.attributes = attributes;
        Ok(event)
    }
}

/// Bounded seed for a `Span.Link`: routes `attributes`, buffers the hex
/// `traceId`/`spanId` + `traceState`/`flags` (finished through the vendored
/// derive).
struct LinkSeed<'a> {
    agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for LinkSeed<'_> {
    type Value = Link;

    fn deserialize<D>(self, deserializer: D) -> Result<Link, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for LinkSeed<'_> {
    type Value = Link;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON Span.Link object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Link, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut attributes: Vec<KeyValue> = Vec::new();
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "attributes" => accumulate_attributes(&mut map, &mut attributes, self.agg)?,
                _ => buffer_scalar_or_skip(key, LINK_SCALARS, &mut map, &mut pairs)?,
            }
        }
        let mut link = if pairs.is_empty() {
            Link::default()
        } else {
            finish_via_derive(&pairs)?
        };
        link.attributes = attributes;
        Ok(link)
    }
}

// ---------------------------------------------------------------------------
// Logs roots
// ---------------------------------------------------------------------------

/// Decodes a proto3-JSON `ExportLogsServiceRequest` with every reachable
/// repeated/container field bounded DURING deserialization (issue #115 track
/// 6b), mirroring [`decode_traces`] at the SAME thresholds (single-sourced
/// `MAX_*` constants from [`crate::protocols::otlp_prescan`]). A cap/depth
/// violation is a whole-request `serde` error -> [`LogsIngestError::DecodeJson`]
/// (HTTP 400 / `google.rpc.Status.code = 3`).
pub(crate) fn decode_logs(body: &[u8]) -> Result<ExportLogsServiceRequest, LogsIngestError> {
    let agg = JsonAggregates::default();
    let mut de = serde_json::Deserializer::from_slice(body);
    let req = ExportLogsServiceRequestSeed { agg: &agg }.deserialize(&mut de)?;
    // Reject trailing garbage exactly as `serde_json::from_slice` would.
    de.end()?;
    Ok(req)
}

struct ExportLogsServiceRequestSeed<'a> {
    agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for ExportLogsServiceRequestSeed<'_> {
    type Value = ExportLogsServiceRequest;

    fn deserialize<D>(self, deserializer: D) -> Result<ExportLogsServiceRequest, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for ExportLogsServiceRequestSeed<'_> {
    type Value = ExportLogsServiceRequest;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON ExportLogsServiceRequest object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<ExportLogsServiceRequest, A::Error>
    where
        A: MapAccess<'de>,
    {
        let agg = self.agg;
        let mut resource_logs: Vec<ResourceLogs> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "resourceLogs" | "resource_logs" => accumulate_msgs(
                    &mut map,
                    &mut resource_logs,
                    (MAX_RESOURCE_LOGS, "resourceLogs"),
                    None,
                    || ResourceLogsSeed { agg },
                )?,
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(ExportLogsServiceRequest { resource_logs })
    }
}

struct ResourceLogsSeed<'a> {
    agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for ResourceLogsSeed<'_> {
    type Value = ResourceLogs;

    fn deserialize<D>(self, deserializer: D) -> Result<ResourceLogs, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for ResourceLogsSeed<'_> {
    type Value = ResourceLogs;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON ResourceLogs object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<ResourceLogs, A::Error>
    where
        A: MapAccess<'de>,
    {
        // Same buffer-and-delegate contract as `ResourceSpansSeed`: intercept
        // the singular `resource` (dup-guarded) and repeated `scopeLogs`
        // (bounded); BUFFER the scalar `schemaUrl` and finish through the
        // vendored `ResourceLogs` derive so a duplicate `schemaUrl` rejects
        // exactly as the `serde(default)` derive does (issue #115 finding 1).
        let agg = self.agg;
        let mut resource: Option<Resource> = None;
        let mut resource_seen = false;
        let mut scope_logs: Vec<ScopeLogs> = Vec::new();
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "resource" => {
                    if resource_seen {
                        return Err(de::Error::duplicate_field("resource"));
                    }
                    resource_seen = true;
                    resource = map.next_value_seed(OptionSeed(ResourceSeed { agg }))?;
                }
                "scopeLogs" | "scope_logs" => accumulate_msgs(
                    &mut map,
                    &mut scope_logs,
                    (MAX_SCOPE_LOGS, "scopeLogs"),
                    None,
                    || ScopeLogsSeed { agg },
                )?,
                _ => buffer_scalar_or_skip(key, RESOURCE_LOGS_SCALARS, &mut map, &mut pairs)?,
            }
        }
        let mut resource_logs: ResourceLogs = finish_via_derive(&pairs)?;
        resource_logs.resource = resource;
        resource_logs.scope_logs = scope_logs;
        Ok(resource_logs)
    }
}

struct ScopeLogsSeed<'a> {
    agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for ScopeLogsSeed<'_> {
    type Value = ScopeLogs;

    fn deserialize<D>(self, deserializer: D) -> Result<ScopeLogs, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for ScopeLogsSeed<'_> {
    type Value = ScopeLogs;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON ScopeLogs object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<ScopeLogs, A::Error>
    where
        A: MapAccess<'de>,
    {
        // Same buffer-and-delegate contract as `ScopeSpansSeed`: intercept the
        // singular `scope` (dup-guarded) and repeated `logRecords` (bounded,
        // charged into the shared `log_records` aggregate); BUFFER the scalar
        // `schemaUrl` and finish through the vendored `ScopeLogs` derive so a
        // duplicate `schemaUrl` rejects exactly as the derive does.
        let agg = self.agg;
        let mut scope: Option<InstrumentationScope> = None;
        let mut scope_seen = false;
        let mut log_records: Vec<LogRecord> = Vec::new();
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "scope" => {
                    if scope_seen {
                        return Err(de::Error::duplicate_field("scope"));
                    }
                    scope_seen = true;
                    scope = map.next_value_seed(OptionSeed(InstrumentationScopeSeed { agg }))?;
                }
                "logRecords" | "log_records" => accumulate_msgs(
                    &mut map,
                    &mut log_records,
                    (MAX_LOG_RECORDS, "logRecords"),
                    Some(AggCharge {
                        cell: &agg.log_records,
                        cap: MAX_TOTAL_LOG_RECORDS,
                        field: "total log records",
                    }),
                    || LogRecordSeed { agg },
                )?,
                _ => buffer_scalar_or_skip(key, SCOPE_LOGS_SCALARS, &mut map, &mut pairs)?,
            }
        }
        let mut scope_logs: ScopeLogs = finish_via_derive(&pairs)?;
        scope_logs.scope = scope;
        scope_logs.log_records = log_records;
        Ok(scope_logs)
    }
}

/// Bounded seed for a `LogRecord`. Routes `attributes` through the shared
/// attribute accumulator and `body` (a MESSAGE `AnyValue`, never a scalar
/// list entry — issue #115 track-6a round-3 lesson) through the depth-bounded
/// [`AnyValueSeed`]; BUFFERS every other key (hex `traceId`/`spanId`, the
/// u64-as-string timestamps, the `severityNumber` enum, `severityText`,
/// dropped-count, `flags`, `eventName`) and finishes through the vendored
/// derive so ADR-0004 hex/u64-string/P5-enum decode stays byte-identical.
struct LogRecordSeed<'a> {
    agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for LogRecordSeed<'_> {
    type Value = LogRecord;

    fn deserialize<D>(self, deserializer: D) -> Result<LogRecord, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for LogRecordSeed<'_> {
    type Value = LogRecord;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON LogRecord object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<LogRecord, A::Error>
    where
        A: MapAccess<'de>,
    {
        let agg = self.agg;
        let mut attributes: Vec<KeyValue> = Vec::new();
        let mut body: Option<AnyValue> = None;
        let mut body_seen = false;
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "attributes" => accumulate_attributes(&mut map, &mut attributes, agg)?,
                "body" => {
                    // A MESSAGE, not a scalar: intercept via the depth-bounded
                    // `AnyValueSeed` (dup-guarded like the derive) so an
                    // attacker cannot recurse or widen it past the shared
                    // AnyValue bounds (issue #115).
                    if body_seen {
                        return Err(de::Error::duplicate_field("body"));
                    }
                    body_seen = true;
                    body = map.next_value_seed(OptionSeed(AnyValueSeed { agg, depth: 1 }))?;
                }
                _ => buffer_scalar_or_skip(key, LOG_RECORD_SCALARS, &mut map, &mut pairs)?,
            }
        }
        // Empty scalar buffer -> `LogRecord::default()` (byte-identical to the
        // `serde(default)` derive on an empty object), skipping a per-record
        // delegate on the common all-repeated-fields shape; a non-empty buffer
        // is finished through the vendored derive so duplicate scalar keys
        // reject.
        let mut record = if pairs.is_empty() {
            LogRecord::default()
        } else {
            finish_via_derive(&pairs)?
        };
        record.attributes = attributes;
        record.body = body;
        Ok(record)
    }
}

pub(crate) mod metrics;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod logs_tests;
