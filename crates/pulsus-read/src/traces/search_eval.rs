//! Phase-2 exact evaluation (issue #57 plan v3-v7; docs/schemas.md §4.2)
//! — pure, no I/O, unit-tested without a database. Given one hydrated
//! candidate batch (spans deduped by `span_id`) plus its attribute
//! membership / value reads, evaluates the **full** query exactly:
//!
//! - the boolean `FieldExpr` tree per span (physical leaves on hydrated
//!   columns; attribute leaves by membership — the ratified negation
//!   rule: `!=`/`!~` matches a span iff **no** index row for that span
//!   satisfies the positive predicate, so absent-key spans match);
//! - cross-spanset algebra with matched-span membership preserved
//!   (`{A} && {B}` keeps traces matching both, spanset = union of the
//!   operands' matched spans; `||` unions — trace-level, task-manager
//!   adjudication 1);
//! - structural relations (issue #172): `{A} > {B}` (direct child),
//!   `{A} >> {B}` (transitive descendant — a cycle-guarded O(spans)
//!   adjacency-map BFS over `parent_id`; an A-matching span is never
//!   itself yielded, even through a malformed parent cycle),
//!   `{A} ~ {B}` (shared non-zero parent, self excluded) —
//!   evaluated engine-side over the hydrated spans (no structural SQL
//!   exists; Phase 1 is byte-identical to `&&`). The result set is the
//!   RIGHT operand's matching spans only (adjudicated pin 3), so
//!   `matched`, summaries, aggregates, and the sort key all reflect the
//!   RHS — deliberately different from `&&`'s union;
//! - the pipeline (`count`/`sum`/`avg`/`min`/`max` aggregate filters over
//!   the matched spans, then `select()` response projection).
//!
//! Emits **response summaries only** (plan v6 delta 2): the engine's
//! result heap never holds hydrated spans or payloads.
//!
//! ## Allocation-charge audit (code review round 3)
//!
//! Invariant: **no retained or intermediate collection exists
//! uncharged** — every allocation site in this module and its charge:
//!
//! | Allocation site | Charge (always BEFORE the allocation) |
//! |---|---|
//! | per-filter matched set (`eval_filter`) | [`charged_set`] pre-charges `spans.len() × SET_ENTRY_BYTES`; released when empty/merged/after summaries |
//! | `&&`/`||` union set (`union_sets`) | [`charged_set`] pre-charge; both operand sets released after the merge |
//! | aggregate `Vec<f64>` buffers + sorted `&HydratedSpan` ref list | the per-trace `transients` envelope (`matched × (ref + f64 + overhead)`), released after summaries |
//! | `TraceMatch` slot + summaries buffer | base charge (`size_of + overhead + take × size_of::<SpanSummary>`) before `Vec::with_capacity(take)` |
//! | summary name + attributes buffer (full capacity, incl. unused) | `build_summary`'s envelope charge before any clone |
//! | each attribute `(display, value)` clone | per-pair string-length charge immediately before the clone |
//! | scalar renders (`duration`/`status`/`kind`, ≤ ~20 B) | stated residual: transiently rendered to learn the length, charged before entering the buffer |
//! | `out: Vec<TraceMatch>` slots | covered by each match's `size_of::<TraceMatch>` base charge + overhead envelope (growth doubling) |
//! | structural result / participant sets (`rel_children`/`rel_parents`/`rel_descendants`/`rel_ancestors`/`rel_siblings`, plus the Negated complement + Union `union_sets`) | [`charged_set`] pre-charge at the spans upper bound; released when empty/merged/after summaries like any operand set |
//! | descendant adjacency map + BFS queue (`rel_descendants`) | `spans × DESCENDANT_TRANSIENT_BYTES` envelope (map key + `Vec` header + child slot with doubling slack + ≤ 2 queue slots per span; the queue never reallocates by construction) charged before allocation, released after the walk |
//! | descendant/ancestor `reached` set (`rel_descendants`/`rel_ancestors`) | [`charged_set`] pre-charge; released after the walk |
//! | ancestor `span_id → parent_id` map + upward BFS queue (`rel_ancestors`) | `spans × (ANCESTOR_ENTRY_BYTES + 2 queue slots)` charged before allocation, released after the upward walk |
//! | sibling parent map (`rel_siblings`) | `spans × SIBLING_ENTRY_BYTES` charged before allocation, released after the pass |
//! | nested-set index (`compute_nested_set`) | `spans × NESTED_SET_ENTRY_BYTES` charged before allocation; retained for the trace's `eval_spanset`, released right after |
//! | nested-set numbering transients — span-id set + children map (key + `Vec` header + child-`Vec` first-push capacity of 4 slots) + sorted view + Euler stack (`compute_nested_set`) | `spans × NESTED_SET_TRANSIENT_BYTES` envelope charged before allocation, released after numbering |
//!
//! The engine-side (exec.rs) sites are audited in that module's doc;
//! BOTH tables are enforced mechanically by `tests/traces_alloc_audit.rs`
//! (round 4). A failed charge is atomic (no phantom `used`), and a
//! mid-batch breach returns the 422 class with the partial output
//! dropped (error-path release semantics: see `ByteBudget`'s type docs).

use std::collections::{HashMap, HashSet};

use pulsus_traceql::{
    AggregateOp, BoolOp, ComparisonOp, Field, FieldExpr, FieldOp, SpansetExpr, StructuralModifier,
    StructuralOp, UnaryOp, Value,
};

use super::exec::ByteBudget;
use super::filter::{BoolMatch, NestedSetField, SetSide};
use super::search_plan::{
    AggSource, GroupKeyResolver, PhysicalEval, PhysicalSelect, PlannedArith, PlannedBoolTerm,
    PlannedFilter, PlannedGroupKey, PlannedLeafEval, PlannedOperand, ProjectionGroup,
    ProjectionTarget, ProjectionValue, SearchPlan, SpansetStage, TraceCtxEval, WireKey,
};
use crate::logql::error::{ReadError, TooBroadReason};

/// One hydrated span (physical summary columns only — never payloads).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HydratedSpan {
    pub span_id: [u8; 8],
    pub parent_id: [u8; 8],
    pub service: String,
    pub name: String,
    pub timestamp_ns: i64,
    pub duration_ns: i64,
    pub status_code: i8,
    /// The span's OTLP `Status.message` (issue #184's `statusMessage`
    /// intrinsic), byte-capped like `service`/`name`; `""` when absent.
    pub status_message: String,
    pub kind: i8,
    /// The span's OTLP `InstrumentationScope.name`/`version` (issue #192's
    /// `instrumentation:name`/`instrumentation:version` intrinsics),
    /// byte-capped like `service`/`name`; `""` when absent.
    pub scope_name: String,
    pub scope_version: String,
}

/// One candidate trace's hydrated batch slice.
#[derive(Debug, Clone)]
pub struct TraceSpans {
    pub trace_id: [u8; 16],
    pub spans: Vec<HydratedSpan>,
}

/// A `(trace_id, span_id)` pair — the identity every attribute read is
/// keyed on.
pub type SpanKey = ([u8; 16], [u8; 8]);

/// The batch's attribute reads, index-aligned with the plan's
/// `probes` / `agg_fields` / `select_attrs` — plus the issue #184
/// trace-wide co-load results (populated only when the plan's
/// `needs_trace_ctx()`/`needs_child_counts()` flags demand them; empty
/// maps otherwise, so other queries pay nothing).
/// One probe's batch membership result (issue #479).
///
/// A probe whose matched value no projection needs stays a bare key set —
/// the read and the memory are unchanged. A probe a projection reads a
/// value from carries the value fused into the SAME read, so the map
/// answers both questions and no second statement is issued.
#[derive(Debug)]
pub enum ProbeMembership {
    Keys(HashSet<SpanKey>),
    /// The fused value AND the OTLP kind the sender stored it as (issue
    /// #510) — the projection renders the value in that arm rather than
    /// as a string.
    Values(HashMap<SpanKey, (String, StoredType)>),
}

impl ProbeMembership {
    pub fn contains(&self, key: &SpanKey) -> bool {
        match self {
            ProbeMembership::Keys(set) => set.contains(key),
            ProbeMembership::Values(map) => map.contains_key(key),
        }
    }

    /// The matched value and its stored type, when this probe fused one.
    pub fn value(&self, key: &SpanKey) -> Option<(&str, StoredType)> {
        match self {
            ProbeMembership::Keys(_) => None,
            ProbeMembership::Values(map) => map.get(key).map(|(v, t)| (v.as_str(), *t)),
        }
    }
}

/// One stored attribute's OTLP kind, as `trace_attrs_idx.val_type`
/// carries it (issue #510).
///
/// `Unknown` is the empty string a row written before that column existed
/// reads back as: the type cannot be recovered from what is stored (`val`
/// is the text and `val_num` is a parse of that text), so those rows keep
/// the pre-change rendering rule. The four named spellings are the
/// writer's own — `pulsus_write::ingest::traces::AttrValueType::as_str` —
/// and `tests/traces_stored_type_seal.rs` seals them against it with an
/// exhaustive match, so a new writer variant is a compile error rather
/// than an untested path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StoredType {
    String,
    Int,
    Float,
    Bool,
    #[default]
    Unknown,
}

impl StoredType {
    pub fn from_stored(s: &str) -> Self {
        match s {
            "string" => StoredType::String,
            "int" => StoredType::Int,
            "float" => StoredType::Float,
            "bool" => StoredType::Bool,
            _ => StoredType::Unknown,
        }
    }
}

/// THE one place a stored attribute becomes a typed response value
/// (issue #510) — group keys, aggregate arguments and projected span
/// attributes all decide their wire arm here, so an arm can only be
/// wrong in ONE place.
///
/// `text` is the exact stored `val`; `num` is the `val_num` reading,
/// absent for a non-finite value (the numeric read carries
/// `isNotNull(val_num)` and ClickHouse stores `NaN`/`±inf` as NULL there)
/// and for a non-numeric one. An `int` renders from `text`, so a value
/// beyond 2^53 keeps its digits where `val_num` has already rounded them;
/// a `float` with no `num` parses `text` (`"NaN"`, `"inf"`, `"-inf"`,
/// `"-0"` all parse), which is what stops a non-finite value falling to
/// the string arm.
///
/// Returns a BORROWED arm rather than an owned [`GroupValue`] so every
/// caller can charge the exact retained payload before the clone happens
/// — the module's charge-before-allocate contract. The arm DECISION is
/// still this one match; [`AttrArm::into_value`] is a 1:1 mapping with no
/// decision in it.
pub(crate) fn typed_attr_value<'a>(
    t: StoredType,
    num: Option<f64>,
    text: Option<&'a str>,
) -> AttrArm<'a> {
    match (t, text, num) {
        (_, None, None) => AttrArm::Nil,
        (StoredType::Int, Some(text), num) => AttrArm::Int(
            text.parse::<i64>()
                .unwrap_or_else(|_| num.unwrap_or(0.0) as i64),
        ),
        (StoredType::Int, None, Some(num)) => AttrArm::Int(num as i64),
        (StoredType::Float, _, Some(num)) => AttrArm::Double(num.to_bits()),
        (StoredType::Float, Some(text), None) => {
            AttrArm::Double(text.parse::<f64>().unwrap_or(f64::NAN).to_bits())
        }
        (StoredType::Bool, Some(text), _) => AttrArm::Bool(text == "true"),
        (StoredType::Bool, None, Some(num)) => AttrArm::Bool(num != 0.0),
        (StoredType::String, Some(text), _) => AttrArm::Text(text),
        (StoredType::String, None, Some(num)) => AttrArm::Double(num.to_bits()),
        // A row written before `val_type` existed: the type is not
        // recoverable from what is stored, so these rows keep the
        // pre-#510 rule — a numeric reading renders `doubleValue`,
        // otherwise the stored text renders `stringValue`.
        (StoredType::Unknown, _, Some(num)) => AttrArm::Double(num.to_bits()),
        (StoredType::Unknown, Some(text), None) => AttrArm::Text(text),
    }
}

/// One stored attribute's decided wire arm, still borrowing its text
/// (issue #510) — see [`typed_attr_value`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum AttrArm<'a> {
    Text(&'a str),
    Int(i64),
    /// The raw `f64` bit pattern — NOT [`group_double_bits`]: an aggregate
    /// value is not a group key, and a group key's own NaN fold is applied
    /// by the grouping path, not here.
    Double(u64),
    Bool(bool),
    Nil,
}

impl AttrArm<'_> {
    /// The retained payload this arm will own once materialised — the
    /// amount a caller charges BEFORE calling [`Self::into_value`].
    pub(crate) fn payload_bytes(&self) -> usize {
        match self {
            AttrArm::Text(s) => s.len(),
            _ => 0,
        }
    }

    /// The owned response value. A 1:1 mapping — no arm decision.
    pub(crate) fn into_value(self) -> GroupValue {
        match self {
            AttrArm::Text(s) => GroupValue::Str(s.to_string()),
            AttrArm::Int(i) => GroupValue::Int(i),
            AttrArm::Double(bits) => GroupValue::Double(bits),
            AttrArm::Bool(b) => GroupValue::Bool(b),
            AttrArm::Nil => GroupValue::Nil,
        }
    }

    /// The same value with a DOUBLE canonicalised for group IDENTITY
    /// (issue #510): only NaN folds — `-0.0` and `+0.0` stay two groups,
    /// which is what the reference returns.
    pub(crate) fn into_group_value(self) -> GroupValue {
        match self {
            AttrArm::Double(bits) => GroupValue::Double(group_double_bits(f64::from_bits(bits))),
            other => other.into_value(),
        }
    }
}

#[derive(Debug, Default)]
pub struct BatchAttrs {
    pub membership: Vec<ProbeMembership>,
    pub agg_values: Vec<HashMap<SpanKey, f64>>,
    /// The stored OTLP kind of each `agg_values` reading (issue #510),
    /// index-aligned with `agg_values`. A non-finite value has NO entry in
    /// either map — the numeric read filters on `isNotNull(val_num)` —
    /// which is why the by-key path reads the type from `select_types`
    /// first.
    pub agg_types: Vec<HashMap<SpanKey, StoredType>>,
    pub select_values: Vec<HashMap<SpanKey, String>>,
    /// The stored OTLP kind of each `select_values` reading (issue #510),
    /// index-aligned with `select_values`.
    pub select_types: Vec<HashMap<SpanKey, StoredType>>,
    /// Per-span MULTI-VALUED event/link value sets (issue #351),
    /// index-aligned with the plan's `event_sets`
    /// (`search_sql::event_set_sql`). Empty for every query that does not
    /// compare an `event:`/`link:` intrinsic against another field. An
    /// ABSENT span key is the empty set, which is what a span with no
    /// events is.
    pub event_sets: Vec<HashMap<SpanKey, EventValues>>,
    /// Per-trace context (`search_sql::trace_ctx_sql`): the trace-wide
    /// time envelope + the `pick_roots`-equivalent root name/service,
    /// keyed by `trace_id`. Window- and cap-independent (full-trace
    /// exact).
    pub trace_ctx: HashMap<[u8; 16], TraceCtxInfo>,
    /// Direct-child counts (`search_sql::child_count_sql`), keyed by
    /// `(trace_id, parent span_id)`; an absent key means 0 children.
    pub child_counts: HashMap<SpanKey, u64>,
}

/// One span's co-loaded event/link values (issue #351). Two variants
/// because the four intrinsics split exactly as their literal leaves do:
/// `event:timeSinceStart` is read from `val_num`, the other three from
/// `val`.
///
/// **Values, not a deduplicated set** (review of the first cut): the read
/// is one row per value with no server-side aggregate, so a repeated
/// value from an at-least-once replay arrives twice. That is inert under
/// both matching rules — ANY-match is unaffected by a repeat, and
/// ALL-match compares `matchCount == elemCount`, which a repeat
/// increments on both sides — so nothing pays for a `DISTINCT` that
/// could not change an answer.
#[derive(Debug, Clone, PartialEq)]
pub enum EventValues {
    Text(Vec<String>),
    Num(Vec<f64>),
}

/// One trace's context co-load values (issue #184).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceCtxInfo {
    /// `min(timestamp_ns)` over the WHOLE trace.
    pub trace_start_ns: i64,
    /// `max(timestamp_ns + duration_ns)` over the whole trace —
    /// `traceDuration = trace_end_ns - trace_start_ns`.
    pub trace_end_ns: i64,
    /// The root span's byte-capped name (`pick_roots` selection order —
    /// a zero-parent root, else the earliest span).
    pub root_name: String,
    /// The root span's byte-capped service.
    pub root_service: String,
}

/// The per-trace evaluation context for the issue #184 trace-level
/// intrinsics — built once per candidate trace in [`evaluate_batch`]
/// (borrowing straight from [`BatchAttrs`]; no per-trace allocation in
/// the hot loop). `info` is `None` when the plan issued no trace-context
/// co-load (or — defensively — the trace vanished between phases): the
/// dependent leaves then match nothing.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TraceEvalCtx<'a> {
    pub(crate) trace_id: [u8; 16],
    pub(crate) info: Option<&'a TraceCtxInfo>,
    pub(crate) child_counts: &'a HashMap<SpanKey, u64>,
}

/// One trace's complete read-only evaluation environment — the batch
/// attribute reads plus the per-trace #181 nested-set numbering and the
/// #184 trace-level context, bundled so the recursive evaluators carry
/// one context parameter.
struct EvalEnv<'a> {
    attrs: &'a BatchAttrs,
    /// The batch's event/link value sets (issue #351), borrowed straight
    /// from [`BatchAttrs`] — no per-span or per-trace copy.
    event_sets: &'a [HashMap<SpanKey, EventValues>],
    nested_set: Option<&'a NestedSetIndex>,
    ctx: TraceEvalCtx<'a>,
}

/// One projected `attributes` entry (issue #479).
///
/// **Both fields are PRIVATE and there is exactly one constructor**, so no
/// string in this workspace can become a wire attribute key without
/// passing through [`WireKey::new`] — the boundary reaches the renderer,
/// not only the planner, because this is the type the renderer reads.
/// What the compiler does NOT establish is that the `Field` handed here is
/// the query's own field rather than one a caller transformed first: a
/// sole constructor proves a value was BUILT by that function, never that
/// it is the right value. `search_plan`'s
/// `projection_identities_equal_the_parsed_query_fields` is the check for
/// that half.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedAttribute {
    key: WireKey,
    /// The TYPED value (issue #510). It used to be a `String`, which made
    /// the renderer hard-code `stringValue` for every projected attribute
    /// — an int came back `{"stringValue":"3"}` where the reference sends
    /// `{"intValue":"3"}`. Carrying a [`GroupValue`] routes this surface
    /// through the SAME `group_value_json` the group keys and the
    /// aggregates use, so the arm cannot be decided differently here.
    value: GroupValue,
}

impl ProjectedAttribute {
    pub fn new(field: &Field, value: GroupValue) -> Self {
        ProjectedAttribute {
            key: WireKey::new(field),
            value,
        }
    }

    pub fn key(&self) -> &str {
        self.key.as_str()
    }

    pub fn value(&self) -> &GroupValue {
        &self.value
    }

    /// `key.len() + value.payload_bytes()` — the term
    /// [`SpanSummary::heap_payload_bytes`] adds and [`build_summary`]
    /// charges BEFORE the pair is built. A numeric or boolean value owns
    /// no heap payload, so its term is the key alone (issue #510).
    pub fn heap_bytes(&self) -> usize {
        self.key.as_str().len() + self.value.payload_bytes()
    }
}

/// One matched span's response summary (docs/api.md §4.2 `spanSets`
/// entry): summary fields plus the projected attributes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpanSummary {
    pub span_id: [u8; 8],
    /// `Some` iff a name value was COLLECTED for this span (issue #479
    /// rule 1) — the query referenced `name` and the leaf that referenced
    /// it matched, or a `select(name)` stage named it.
    ///
    /// PRIVATE: every read outside this module goes through [`Self::name`],
    /// so a future reader cannot reach the field through a tuple position,
    /// an enum payload, a generic container or an inferred binding — and,
    /// decisively, `Option<String>` in a `json!` value position COMPILES
    /// and renders `null`, which is exactly the wrong answer the reference
    /// never gives. Privacy turns that silent null into a type error.
    /// `None` costs no retained bytes and writes no wire field.
    name: Option<String>,
    pub start_ns: i64,
    pub duration_ns: i64,
    /// The projected attributes, in the plan's projection-group order.
    pub attributes: Vec<ProjectedAttribute>,
}

impl SpanSummary {
    /// The only way to build one outside this module.
    pub fn new(
        span_id: [u8; 8],
        name: Option<String>,
        start_ns: i64,
        duration_ns: i64,
        attributes: Vec<ProjectedAttribute>,
    ) -> Self {
        SpanSummary {
            span_id,
            name,
            start_ns,
            duration_ns,
            attributes,
        }
    }

    /// `None` iff the query collected no name for this span. Callers that
    /// need a `String` must choose their own absent-value behaviour: the
    /// renderer writes no key at all, and the TTL-race fallback writes an
    /// empty root name, as it already does for the other two fallback
    /// fields it refuses to invent.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// The summary's heap payload beyond its own `size_of` slot in the
    /// parent `TraceMatch::spans` buffer (which the parent accounts):
    /// overhead envelope + name bytes (only when one was collected) + the
    /// attributes buffer at its **actual capacity** (code review round 2:
    /// unused preallocated capacity is retained memory too) + the
    /// attribute string bytes. [`evaluate_batch`] charges exactly these
    /// amounts BEFORE each allocation, so a heap-evict release of
    /// [`TraceMatch::retained_bytes`] returns precisely what was charged.
    pub(crate) fn heap_payload_bytes(&self) -> usize {
        super::exec::RETAINED_ENTRY_OVERHEAD
            + self.name.as_ref().map_or(0, String::len)
            + self.attributes.capacity() * std::mem::size_of::<ProjectedAttribute>()
            + self
                .attributes
                .iter()
                .map(ProjectedAttribute::heap_bytes)
                .sum::<usize>()
    }
}

/// The canonical NaN bit pattern all NaN group keys collapse onto (issue
/// #193 R2-F2).
const CANONICAL_NAN_BITS: u64 = 0x7ff8_0000_0000_0000;

/// The WIRE ARM one typed response value renders into, and that value as
/// text (issue #510).
///
/// **One decision, three surfaces, two crates.** The server's
/// `group_value_json` builds its JSON object from this arm, and the live
/// grouping differential — which reads [`GroupValue`]s straight out of
/// the engine, one crate below the encoder — builds its type-tagged
/// comparison token from the same call. Two spellings of "which arm" was
/// the defect issue #510 exists to remove; a differential carrying its
/// own copy would compare text the wire never sends.
///
/// The TEXT is the value's own rendering, not the JSON encoder's: a
/// finite double comes back as `5` where our encoder writes `5.0`. That
/// lexical difference is a ledger row
/// (`traceql-spanset-aggregate-double-lexical-form`), and keeping it out
/// of the token is what lets the differential compare arms and values
/// without failing on it.
///
/// `Nil` is `("stringValue", "nil")`: the reference groups a span
/// carrying no value for a `by()` key under that literal marker, not
/// under a null.
pub fn wire_arm(value: &GroupValue) -> (&'static str, String) {
    match value {
        GroupValue::Str(s) => ("stringValue", s.clone()),
        GroupValue::Int(i) => ("intValue", i.to_string()),
        GroupValue::Double(bits) => {
            let f = f64::from_bits(*bits);
            (
                "doubleValue",
                match non_finite_double_spelling(f) {
                    Some(spelling) => spelling.to_string(),
                    None => f.to_string(),
                },
            )
        }
        GroupValue::Bool(b) => ("boolValue", b.to_string()),
        GroupValue::Nil => ("stringValue", "nil".to_string()),
    }
}

/// The reference's spelling of a NON-FINITE double inside the
/// `doubleValue` arm (issue #510), or `None` when the value is finite.
///
/// protojson renders a `double` field's NaN and infinities as these three
/// literal JSON STRINGS, and the reference sends exactly them —
/// `{"doubleValue":"NaN"}`, `{"doubleValue":"Infinity"}`,
/// `{"doubleValue":"-Infinity"}`. Our own stored spellings are Rust's
/// (`NaN`, `inf`, `-inf`), so the normalisation happens here and `inf`
/// never reaches the wire.
///
/// **In `pulsus-read` rather than beside the renderer** so the response
/// encoder and the live differential — which reads [`GroupValue`]s
/// straight out of the engine, one crate below the encoder — spell a
/// non-finite value the same way. Two spellings would have made the
/// differential compare text the wire never carries.
pub fn non_finite_double_spelling(f: f64) -> Option<&'static str> {
    if f.is_finite() {
        None
    } else if f.is_nan() {
        Some("NaN")
    } else if f.is_sign_positive() {
        Some("Infinity")
    } else {
        Some("-Infinity")
    }
}

/// The bit pattern a double GROUP KEY is identified by (issue #510).
///
/// Group identity is the RAW bit pattern with NaN alone canonicalised.
/// `-0.0` and `+0.0` are DIFFERENT groups, which is what the reference
/// returns: measured on three spans carrying `-0.0`, `+0.0`, `-0.0`, it
/// answers two span sets (`{"doubleValue":0}` over span 02 and
/// `{"doubleValue":-0}` over spans 01 and 03) where the pre-#510 fold gave
/// one span set of three. The NaN fold stays, and it is UNOBSERVABLE end
/// to end — the wire format carries one NaN spelling, so no payload
/// difference can reach a response — which is why it is not claimed as
/// reference parity in either direction.
///
/// Only GROUPING calls this. An aggregate value is not a group key: it
/// keeps its raw bits, so a `-0.0` sum renders `-0` and not `0`.
pub(crate) fn group_double_bits(f: f64) -> u64 {
    if f.is_nan() {
        CANONICAL_NAN_BITS
    } else {
        f.to_bits()
    }
}

/// One typed response value — a `by()` group-key value, an aggregate's
/// own value, or a matched span's projected attribute (issues #193,
/// #510) — rendering to the reference's
/// `value:{stringValue|intValue|doubleValue|boolValue}` arms.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GroupValue {
    Str(String),
    Int(i64),
    /// A double as its `f64` bit pattern; render via `f64::from_bits`.
    /// A GROUP KEY's pattern comes from [`group_double_bits`] (NaN
    /// folded, sign of zero kept); an aggregate's and a projected
    /// attribute's are raw.
    Double(u64),
    Bool(bool),
    /// The span carried no value for this key (grouped into the nil bucket).
    Nil,
}

impl GroupValue {
    /// The owned heap payload beyond the enum's own `size_of` slot: only a
    /// `Str` carries bytes (charged/released via `.len()`, the module-wide
    /// payload convention). Numeric/bool/nil add nothing.
    pub(crate) fn payload_bytes(&self) -> usize {
        match self {
            GroupValue::Str(s) => s.len(),
            _ => 0,
        }
    }
}

/// One response spanSet produced by a `by()` regroup (issue #193): the
/// resolved group-key `attributes` (in `by()` order), the total matched
/// span count IN THIS GROUP (pre-`spss`), and the `spss`-capped per-group
/// summaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpanSetGroup {
    pub attributes: Vec<(String, GroupValue)>,
    pub matched: u32,
    pub spans: Vec<SpanSummary>,
}

impl SpanSetGroup {
    /// This group's retained heap — accounted the SAME way [`TraceMatch`]
    /// accounts its own `spans`, plus the `attributes` (display + value
    /// payload via `.len()`, `.capacity()` for the enclosing `Vec` slot).
    /// [`build_span_set_groups`] charges exactly these amounts before each
    /// allocation, so the heap-evict / coalesce-collapse release is exact.
    pub(crate) fn retained_bytes(&self) -> usize {
        super::exec::RETAINED_ENTRY_OVERHEAD
            + self.attributes.capacity() * std::mem::size_of::<(String, GroupValue)>()
            + self
                .attributes
                .iter()
                .map(|(display, value)| display.len() + value.payload_bytes())
                .sum::<usize>()
            + self.spans.capacity() * std::mem::size_of::<SpanSummary>()
            + self
                .spans
                .iter()
                .map(SpanSummary::heap_payload_bytes)
                .sum::<usize>()
    }
}

/// The retained heap of a `by()`-produced group vector (issue #193): the
/// enclosing `Vec` slot + overhead plus each group's [`SpanSetGroup::retained_bytes`].
/// This is the exact amount [`build_span_set_groups`] charges and the
/// amount a trailing `coalesce()` collapse releases.
pub(crate) fn groups_retained_bytes(groups: &[SpanSetGroup]) -> usize {
    // `len()` == the enclosing `Vec`'s capacity by construction
    // ([`build_span_set_groups`] reserves `Vec::with_capacity(n)` and pushes
    // exactly `n`), so this matches the charge and the release exactly.
    super::exec::RETAINED_ENTRY_OVERHEAD
        + std::mem::size_of_val(groups)
        + groups
            .iter()
            .map(SpanSetGroup::retained_bytes)
            .sum::<usize>()
}

/// One exactly-matched trace, ready for the engine's result heap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceMatch {
    pub trace_id: [u8; 16],
    /// The public sort key: max `timestamp_ns` over the trace's
    /// exactly-matched spans (docs/api.md §4.2 ordering contract).
    pub sort_key: i64,
    /// Total matched spans (pre-`spss` cap) — the response's `matched`.
    pub matched: u32,
    /// `spss`-capped summaries, ascending `(start_ns, span_id)`.
    pub spans: Vec<SpanSummary>,
    /// The `by()`-regrouped spanSets (issue #193): `Some` iff a `by()`
    /// stage is active and not collapsed by a trailing `coalesce()`; the
    /// default (`None`) keeps the flat response byte-identical.
    pub groups: Option<Vec<SpanSetGroup>>,
}

impl TraceMatch {
    /// Capacity-based retained cost — byte-for-byte equal to what
    /// [`evaluate_batch`] charged while building this match (asserted by
    /// the `charges_equal_retained_bytes_exactly` unit test), so the
    /// engine's heap-evict release keeps the budget exact. Includes the
    /// `by()` groups (issue #193) via [`groups_retained_bytes`].
    pub(crate) fn retained_bytes(&self) -> usize {
        std::mem::size_of::<TraceMatch>()
            + super::exec::RETAINED_ENTRY_OVERHEAD
            + self.spans.capacity() * std::mem::size_of::<SpanSummary>()
            + self
                .spans
                .iter()
                .map(SpanSummary::heap_payload_bytes)
                .sum::<usize>()
            + self
                .groups
                .as_deref()
                .map(groups_retained_bytes)
                .unwrap_or(0)
    }
}

/// A `by()` group-key value tuple (issue #193) — one [`GroupValue`] per
/// `by()` key, in query order.
pub(crate) type GroupTuple = Vec<GroupValue>;

/// The fixed per-tuple overhead of the [`GroupCardinalityCounter`]'s
/// `HashSet` — the set slot (the tuple `Vec` header) + the
/// container-overhead envelope. The variable string payload is charged on
/// top per tuple (issue #193 R4-F1: `.len()` for payload, `.capacity()`
/// for slots).
const GROUP_TUPLE_ENTRY_BYTES: usize =
    std::mem::size_of::<GroupTuple>() + super::exec::RETAINED_ENTRY_OVERHEAD;

/// The retained cost of one distinct group-key tuple in the cardinality
/// counter (issue #193 R4-F1): the fixed `HashSet` slot / per-element
/// overhead PLUS the owned `GroupValue::Str` payloads, via the SAME
/// `.len()`-based method [`SpanSetGroup::retained_bytes`] uses for its
/// value strings — so the two accounting sites provably cannot drift.
pub(crate) fn group_tuple_bytes(tuple: &GroupTuple) -> usize {
    GROUP_TUPLE_ENTRY_BYTES
        + tuple.capacity() * std::mem::size_of::<GroupValue>()
        + tuple.iter().map(GroupValue::payload_bytes).sum::<usize>()
}

/// The distinct-group cardinality cap (issue #193 R2-F1): a cross-batch,
/// cross-trace running accumulator threaded into [`evaluate_batch`] and
/// enforced INSIDE [`build_span_set_groups`] at grouping-PRODUCTION time —
/// before any trailing `coalesce()` collapse and before winner eviction,
/// so `by()|coalesce()` and fan-out concentrated in limit-evicted traces
/// are bounded by the SAME static `422 TraceSearchSeriesCap` as bare
/// `by()`. Each distinct tuple's ACTUAL heap ([`group_tuple_bytes`]) is
/// charged BEFORE it is retained; a repeat tuple charges nothing (dedup).
/// The counter persists for the whole request (it must, to keep enforcing
/// across batches) and its charge is released on the success path by
/// [`GroupCardinalityCounter::release`].
pub(crate) struct GroupCardinalityCounter {
    seen: HashSet<GroupTuple>,
    cap: u64,
    charged: usize,
}

impl GroupCardinalityCounter {
    pub(crate) fn new(cap: u64) -> Self {
        GroupCardinalityCounter {
            seen: HashSet::new(),
            cap,
            charged: 0,
        }
    }

    /// Observes one distinct group tuple: charges its actual retained
    /// bytes BEFORE retaining it, then trips the `422` the moment the
    /// distinct count exceeds `cap`. A tuple already seen charges nothing.
    fn observe(&mut self, tuple: &GroupTuple, budget: &mut ByteBudget) -> Result<(), ReadError> {
        if self.seen.contains(tuple) {
            return Ok(());
        }
        let bytes = group_tuple_bytes(tuple);
        budget.charge(bytes)?;
        self.charged += bytes;
        self.seen.insert(tuple.clone());
        if self.seen.len() as u64 > self.cap {
            return Err(ReadError::QueryTooBroad(
                TooBroadReason::TraceSearchSeriesCap {
                    count: self.seen.len() as u64,
                    cap: self.cap,
                },
            ));
        }
        Ok(())
    }

    /// Releases the counter's whole retained charge (success-path only;
    /// on an error the request budget dies whole).
    pub(crate) fn release(&self, budget: &mut ByteBudget) {
        budget.release(self.charged);
    }
}

fn cmp_i64(op: ComparisonOp, lhs: i64, rhs: i64) -> bool {
    match op {
        ComparisonOp::Eq => lhs == rhs,
        ComparisonOp::Neq => lhs != rhs,
        ComparisonOp::Gt => lhs > rhs,
        ComparisonOp::Gte => lhs >= rhs,
        ComparisonOp::Lt => lhs < rhs,
        ComparisonOp::Lte => lhs <= rhs,
        ComparisonOp::Re | ComparisonOp::Nre => false,
    }
}

/// The engine's numeric comparator. `pub(super)` so the metrics filter
/// compiler's `nestedSetParent` lowering (issue #458) decides the root
/// sentinel's truth with the SAME operator table Phase 2 uses — a
/// re-spelling in `metrics_sql` could drift the metrics answer from the
/// search answer with nothing to catch it.
pub(super) fn cmp_f64(op: ComparisonOp, lhs: f64, rhs: f64) -> bool {
    match op {
        ComparisonOp::Eq => lhs == rhs,
        ComparisonOp::Neq => lhs != rhs,
        ComparisonOp::Gt => lhs > rhs,
        ComparisonOp::Gte => lhs >= rhs,
        ComparisonOp::Lt => lhs < rhs,
        ComparisonOp::Lte => lhs <= rhs,
        ComparisonOp::Re | ComparisonOp::Nre => false,
    }
}

const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

/// Renders `bytes` as lowercase hex into the caller's STACK buffer (no
/// heap allocation in the per-span hot loop — query-perf mandate). `buf`
/// must be exactly `2 × bytes.len()`.
fn hex_into<'a>(bytes: &[u8], buf: &'a mut [u8]) -> &'a str {
    debug_assert_eq!(buf.len(), bytes.len() * 2);
    for (i, b) in bytes.iter().enumerate() {
        buf[2 * i] = HEX_DIGITS[(b >> 4) as usize];
        buf[2 * i + 1] = HEX_DIGITS[(b & 0x0f) as usize];
    }
    // Invariant: the buffer holds only ASCII hex digits by construction.
    std::str::from_utf8(buf).expect("hex digits are ASCII")
}

fn eval_physical(p: &PhysicalEval, span: &HydratedSpan) -> bool {
    match p {
        PhysicalEval::Name { op, value } => op.matches(value, &span.name),
        PhysicalEval::Service { op, value } => op.matches(value, &span.service),
        PhysicalEval::Duration { op, nanos } => cmp_i64(*op, span.duration_ns, *nanos),
        PhysicalEval::Status { op, code } => cmp_i64(*op, span.status_code as i64, *code as i64),
        PhysicalEval::Kind { op, code } => cmp_i64(*op, span.kind as i64, *code as i64),
        PhysicalEval::StatusMessage { op, value } => op.matches(value, &span.status_message),
        // The hex comparisons mirror the SQL predicate exactly
        // (`lower(hex(col)) <op> value`): Eq/Neq values arrive
        // pre-lowercased from leaf compilation, regexes run against the
        // lowercase rendering.
        PhysicalEval::SpanIdHex { op, value } => {
            let mut buf = [0u8; 16];
            op.matches(value, hex_into(&span.span_id, &mut buf))
        }
        PhysicalEval::ParentIdHex { op, value } => {
            let mut buf = [0u8; 16];
            op.matches(value, hex_into(&span.parent_id, &mut buf))
        }
        PhysicalEval::InstrumentationName { op, value } => op.matches(value, &span.scope_name),
        PhysicalEval::InstrumentationVersion { op, value } => {
            op.matches(value, &span.scope_version)
        }
    }
}

/// Evaluates one trace-level intrinsic leaf (issue #184) for one span,
/// against the trace-wide co-load context. `traceDuration`/`rootName`/
/// `rootServiceName`/`trace:id` are trace-constant (every span of a
/// matching trace matches); `span:childCount` is per span (its
/// direct-child count, 0 when it parents nothing).
fn eval_trace_ctx(tc: &TraceCtxEval, ctx: &TraceEvalCtx<'_>, span: &HydratedSpan) -> bool {
    match tc {
        TraceCtxEval::ChildCount { op, value } => {
            let n = ctx
                .child_counts
                .get(&(ctx.trace_id, span.span_id))
                .copied()
                .unwrap_or(0);
            cmp_f64(*op, n as f64, *value)
        }
        TraceCtxEval::TraceDurationNs { op, nanos } => ctx
            .info
            .map(|i| cmp_i64(*op, i.trace_end_ns.saturating_sub(i.trace_start_ns), *nanos))
            .unwrap_or(false),
        TraceCtxEval::RootName { op, value } => ctx
            .info
            .map(|i| op.matches(value, &i.root_name))
            .unwrap_or(false),
        TraceCtxEval::RootServiceName { op, value } => ctx
            .info
            .map(|i| op.matches(value, &i.root_service))
            .unwrap_or(false),
        TraceCtxEval::TraceId { op, value } => {
            let mut buf = [0u8; 32];
            op.matches(value, hex_into(&ctx.trace_id, &mut buf))
        }
    }
}

/// One resolved field-vs-field operand value (issue #183). Both fields
/// are borrowed from the hydrated span / attribute reads — no allocation
/// happens in the compare (keeping it out of the per-span hot loop).
struct ResolvedVal<'a> {
    num: Option<f64>,
    /// `Cow` because issue #351's id intrinsics RENDER their value
    /// (lowercase hex of a byte array) rather than borrowing a column.
    text: Option<std::borrow::Cow<'a, str>>,
}

/// Resolves one comparison operand to its typed value for a span, or
/// `None` when an attribute operand's key is absent (absent key ⇒ no
/// match). Physical intrinsics are always present.
fn resolve_operand<'a>(
    operand: &PlannedOperand,
    span: &'a HydratedSpan,
    env: &'a EvalEnv<'a>,
) -> Option<ResolvedVal<'a>> {
    use std::borrow::Cow;
    let trace_id = env.ctx.trace_id;
    let attrs = env.attrs;
    let text = |t: &'a str| {
        Some(ResolvedVal {
            num: None,
            text: Some(Cow::Borrowed(t)),
        })
    };
    let owned = |t: String| {
        Some(ResolvedVal {
            num: None,
            text: Some(Cow::Owned(t)),
        })
    };
    let number = |n: f64| {
        Some(ResolvedVal {
            num: Some(n),
            text: None,
        })
    };
    match operand {
        PlannedOperand::Name => text(&span.name),
        PlannedOperand::Service => text(&span.service),
        // -- issue #351: intrinsics as a field-vs-field operand. Each
        // resolves to ONE scalar per span, from the hydrated span or a
        // co-load `plan_operand` requested. A `None` here means "no
        // value for this span", which the comparison treats as no match —
        // the same rule an absent attribute key follows.
        PlannedOperand::StatusMessage => text(&span.status_message),
        PlannedOperand::ScopeName => text(&span.scope_name),
        PlannedOperand::ScopeVersion => text(&span.scope_version),
        // Ids render as LOWERCASE HEX, matching the id literal path
        // (`lowercase_hex_literal`) so `{ .a = span:id }` compares against
        // the same spelling `{ span:id = "…" }` accepts.
        PlannedOperand::SpanId => owned(hex_lower(&span.span_id)),
        PlannedOperand::ParentId => owned(hex_lower(&span.parent_id)),
        PlannedOperand::TraceId => owned(hex_lower(&trace_id)),
        PlannedOperand::TraceDurationNs => env
            .ctx
            .info
            .and_then(|i| number((i.trace_end_ns - i.trace_start_ns) as f64)),
        PlannedOperand::RootName => env.ctx.info.and_then(|i| text(i.root_name.as_str())),
        PlannedOperand::RootServiceName => env.ctx.info.and_then(|i| text(i.root_service.as_str())),
        // An absent entry is zero children, exactly as the TraceCtx leaf
        // reads it.
        PlannedOperand::ChildCount => number(
            env.ctx
                .child_counts
                .get(&(trace_id, span.span_id))
                .copied()
                .unwrap_or(0) as f64,
        ),
        PlannedOperand::NestedSet(field) => env
            .nested_set
            .and_then(|ix| ix.get(&span.span_id))
            .and_then(|v| number(v.value(*field) as f64)),
        PlannedOperand::Duration => number(span.duration_ns as f64),
        PlannedOperand::Status => number(span.status_code as f64),
        PlannedOperand::Kind => number(span.kind as f64),
        PlannedOperand::Attr { str_idx, num_idx } => {
            let key = (trace_id, span.span_id);
            let t = attrs.select_values[*str_idx].get(&key).map(String::as_str);
            let num = attrs.agg_values[*num_idx].get(&key).copied();
            if t.is_none() && num.is_none() {
                None
            } else {
                Some(ResolvedVal {
                    num,
                    text: t.map(Cow::Borrowed),
                })
            }
        }
    }
}

/// Lowercase hex, the rendering every id intrinsic uses.
fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Resolves an arithmetic operand tree to a number for a span (issue
/// #185). A field operand that is absent or string-typed, or a
/// division/modulo by zero, yields `None` (no match).
fn resolve_arith(node: &PlannedArith, span: &HydratedSpan, env: &EvalEnv<'_>) -> Option<f64> {
    match node {
        PlannedArith::Value(v) => Some(*v),
        PlannedArith::Operand(operand) => resolve_operand(operand, span, env).and_then(|r| r.num),
        PlannedArith::Neg(inner) => resolve_arith(inner, span, env).map(|v| -v),
        PlannedArith::Bin { op, lhs, rhs } => {
            let l = resolve_arith(lhs, span, env)?;
            let r = resolve_arith(rhs, span, env)?;
            super::filter::apply_arith(*op, l, r)
        }
    }
}

/// Lexicographic string comparison for the six ordering/equality
/// operators (Tempo compares string statics byte-lexicographically, which
/// matches Rust's `str` `Ord` — verified against grafana/tempo:3.0.2:
/// `apple < banana`, `"5" <= "5"`).
fn cmp_str(op: ComparisonOp, l: &str, r: &str) -> bool {
    match op {
        ComparisonOp::Eq => l == r,
        ComparisonOp::Neq => l != r,
        ComparisonOp::Gt => l > r,
        ComparisonOp::Gte => l >= r,
        ComparisonOp::Lt => l < r,
        ComparisonOp::Lte => l <= r,
        ComparisonOp::Re | ComparisonOp::Nre => false,
    }
}

/// Evaluates a field-vs-field comparison for one span (issue #183),
/// matching the coercion rule VERIFIED against grafana/tempo:3.0.2
/// (value-parity broadly remains a #185 close condition, but this
/// cross-type rule is Tempo-verified here):
///
/// - **type gate** — the two operands must be the same type; a cross-type
///   pair (one numeric, one string) is **no match for EVERY operator**,
///   even on coincident text (`.a = "5"` string vs `.b = 5` int is NOT a
///   match, and neither is `!=`);
/// - both numeric ⇒ numeric compare (all 6 operators);
/// - both string ⇒ lexicographic string compare (all 6 operators);
/// - an absent attribute key on either side ⇒ no match.
///
/// An operand is numeric-typed iff it resolves a numeric value (`val_num`
/// for an attribute, the physical column for `duration`/`status`/`kind`);
/// otherwise it is string-typed (`name`, `resource.service.name`, a
/// string/bool attribute). The text `val` a numeric attribute row ALSO
/// carries is deliberately NOT used as a fallback — the gate keys on
/// genuine numeric-typedness, so coincident text can never cross the type
/// boundary.
fn eval_field_compare(
    lhs: &PlannedOperand,
    rhs: &PlannedOperand,
    op: ComparisonOp,
    span: &HydratedSpan,
    env: &EvalEnv<'_>,
) -> bool {
    let (Some(l), Some(r)) = (
        resolve_operand(lhs, span, env),
        resolve_operand(rhs, span, env),
    ) else {
        return false; // absent key on either side ⇒ no match
    };
    match (l.num, r.num) {
        // Both numeric-typed ⇒ numeric compare.
        (Some(ln), Some(rn)) => cmp_f64(op, ln, rn),
        // Both string-typed ⇒ lexicographic string compare.
        (None, None) => match (l.text, r.text) {
            (Some(lt), Some(rt)) => cmp_str(op, lt.as_ref(), rt.as_ref()),
            _ => false,
        },
        // Cross-type (numeric vs string) ⇒ no match for every operator.
        _ => false,
    }
}

/// One boolean operand's resolved state (issue #351). Four states, not
/// two: the reference distinguishes "holds `false`" from "holds something
/// that is not a boolean" from "absent", and each leads somewhere
/// different — no match, a whole-query failure under `!`, and no match
/// again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoolVal {
    True,
    False,
    /// Present, but not a boolean — `!` fails the whole query on this
    /// (`pkg/traceql/ast_execute.go:852-858` @ v3.0.2); `=`/`!=` simply
    /// does not match (`:411-417` returns `StaticFalse` for a
    /// non-matching operand pair).
    NotBoolean,
    /// No value for this span — no match, never an error.
    Absent,
}

impl BoolVal {
    fn of(value: bool) -> BoolVal {
        if value { BoolVal::True } else { BoolVal::False }
    }

    fn boolean(self) -> Option<bool> {
        match self {
            BoolVal::True => Some(true),
            BoolVal::False => Some(false),
            BoolVal::NotBoolean | BoolVal::Absent => None,
        }
    }
}

/// Resolves one [`PlannedBoolTerm`] for a span. Booleans are stored as
/// the strings `"true"`/`"false"`, so the resolved text is the
/// discriminator — the same channel `BoolTruth` reads.
fn eval_bool_term(
    term: &PlannedBoolTerm,
    span: &HydratedSpan,
    env: &EvalEnv<'_>,
) -> Result<BoolVal, ReadError> {
    Ok(match term {
        PlannedBoolTerm::Const(v) => BoolVal::of(*v),
        PlannedBoolTerm::Value(operand) => match resolve_operand(operand, span, env) {
            None => BoolVal::Absent,
            Some(v) => match v.text.as_deref() {
                Some("true") => BoolVal::True,
                Some("false") => BoolVal::False,
                _ => BoolVal::NotBoolean,
            },
        },
        PlannedBoolTerm::Not { term, display } => match eval_bool_term(term, span, env)? {
            BoolVal::True => BoolVal::False,
            BoolVal::False => BoolVal::True,
            BoolVal::Absent => BoolVal::Absent,
            // The `!` OPERATOR demands a boolean, and a present
            // non-boolean fails the WHOLE query — the reference's
            // behaviour and ours since issue #335 Stage B.
            BoolVal::NotBoolean => {
                return Err(ReadError::PipelineInvalid {
                    reason: format!("expression (!{display}) expected a boolean"),
                });
            }
        },
        PlannedBoolTerm::Nested(leaf) => BoolVal::of(eval_leaf(leaf, span, env)?),
    })
}

/// Evaluates a boolean-vs-boolean comparison (issue #351).
///
/// BOTH terms are resolved before the operator is applied, even when the
/// first already decides the outcome: the `!` type failure must fire
/// either way, which is what the reference does — measured,
/// `{ .p = .q = !.r }` is a 500 against a string `r` even on spans whose
/// left side is already `false`.
fn eval_bool_compare(
    lhs: &PlannedBoolTerm,
    rhs: &PlannedBoolTerm,
    op: ComparisonOp,
    span: &HydratedSpan,
    env: &EvalEnv<'_>,
) -> Result<bool, ReadError> {
    let l = eval_bool_term(lhs, span, env)?;
    let r = eval_bool_term(rhs, span, env)?;
    let (Some(l), Some(r)) = (l.boolean(), r.boolean()) else {
        return Ok(false);
    };
    Ok(match op {
        ComparisonOp::Eq => l == r,
        ComparisonOp::Neq => l != r,
        // An ordering (or regex) operator over two booleans matches
        // nothing — the operands are still resolved above, so the `!`
        // type demand stays live. Measured: `{ !.ct < !.cu }` is a 200
        // with no matches even where both booleans are present.
        _ => false,
    })
}

/// Consumes the next planned leaf, in pre-order, and evaluates it.
fn eval_planned_leaf(
    filter: &PlannedFilter,
    leaf_idx: &mut usize,
    span: &HydratedSpan,
    env: &EvalEnv<'_>,
) -> Result<bool, ReadError> {
    let leaf = &filter.leaves[*leaf_idx];
    *leaf_idx += 1;
    eval_leaf(leaf, span, env)
}

/// Evaluates one planned leaf against one span. Split from
/// [`eval_planned_leaf`] since issue #351: a leaf can now CONTAIN a leaf
/// (`{ .a = .b = .c }`), and a nested one is not part of the pre-order
/// stream — it is reached through its parent, never by advancing
/// `leaf_idx`.
fn eval_leaf(
    leaf: &PlannedLeafEval,
    span: &HydratedSpan,
    env: &EvalEnv<'_>,
) -> Result<bool, ReadError> {
    Ok(match leaf {
        // **Boolean truthiness** (issue #335 Stage B, D12 capture).
        // Booleans are stored as the strings `"true"`/`"false"`, so the
        // resolved text separates the three cases the reference
        // distinguishes:
        //   absent               -> no match, never an error
        //   present "true"/"false" -> match iff it equals `want`
        //   present, anything else -> the WHOLE QUERY fails, exactly as
        //                             the reference does
        // Returning no-match for the third case would be a quiet wrong
        // answer where the reference is a loud failure.
        PlannedLeafEval::BoolTruth {
            operand,
            want,
            display,
        } => {
            match resolve_operand(operand, span, env) {
                None => false,
                Some(v) => match (v.text.as_deref(), want) {
                    // `want` is the value the OPERAND must hold, so the
                    // negation is already folded in at plan time.
                    (Some("true"), BoolMatch::Is(w)) => *w,
                    (Some("false"), BoolMatch::Is(w)) => !*w,
                    // A boolean operand compared against a NON-boolean
                    // (`{ !.a = 1 }`): resolved and type-checked, matches
                    // nothing.
                    (Some("true" | "false"), BoolMatch::Never) => false,
                    _ => {
                        return Err(ReadError::PipelineInvalid {
                            reason: format!("expression (!{display}) expected a boolean"),
                        });
                    }
                },
            }
        }
        PlannedLeafEval::Physical(p) => eval_physical(p, span),
        PlannedLeafEval::Attr { probe_idx, negated } => {
            let member =
                env.attrs.membership[*probe_idx].contains(&(env.ctx.trace_id, span.span_id));
            member != *negated
        }
        PlannedLeafEval::NestedSet { field, op, value } => env
            .nested_set
            .and_then(|idx| idx.get(&span.span_id))
            .map(|v| cmp_f64(*op, v.value(*field) as f64, *value))
            .unwrap_or(false),
        PlannedLeafEval::FieldCompare { lhs, rhs, op } => {
            eval_field_compare(lhs, rhs, *op, span, env)
        }
        PlannedLeafEval::TraceCtx(tc) => eval_trace_ctx(tc, &env.ctx, span),
        PlannedLeafEval::Arith { lhs, op, rhs } => {
            match (resolve_arith(lhs, span, env), resolve_arith(rhs, span, env)) {
                (Some(l), Some(r)) => cmp_f64(*op, l, r),
                _ => false,
            }
        }
        // Issue #351: a static-vs-static comparison, decided once at plan
        // time. Nothing per span but the read of this bool.
        PlannedLeafEval::Const(v) => *v,
        PlannedLeafEval::BoolCompare { lhs, rhs, op } => {
            eval_bool_compare(lhs, rhs, *op, span, env)?
        }
        PlannedLeafEval::EventSetCompare {
            set_idx,
            scalar,
            op,
            side,
        } => eval_event_set_compare(*set_idx, scalar, *op, *side, span, env),
    })
}

/// Evaluates a multi-valued event/link comparison for one span (issue
/// #351), against the batch's per-span value co-load.
///
/// **ANY-match, `!=` ALL-match** (owner ruling, 2026-08-05) — the
/// reference's own designed rule for a multi-valued operand:
/// `pkg/traceql/ast_execute.go:535-627` @ v3.0.2 sets `matchAll` for
/// `OpNotEqual`/`OpNotRegex` and returns `matchCount == elemCount`,
/// otherwise `matchCount > 0`. Three consequences fall out of that
/// arithmetic and each is deliberate:
///
/// * a span with NO events matches `!=` (`0 == 0`) — the same absent-key
///   rule the literal `{ event:name != "x" }` form already follows
///   (docs/api.md §4.2);
/// * a cross-TYPE element fails its own element comparison, so it makes
///   `!=` false rather than true — the element predicate is the same
///   type gate [`eval_field_compare`] applies to a scalar pair;
/// * the SCALAR side keeps issue #183's rule unchanged — absent ⇒ no
///   match for every operator — so it is resolved first and
///   short-circuits.
///
/// Allocation-free: the co-loaded set is borrowed and compared in place,
/// and an ANY-match returns on its first hit.
fn eval_event_set_compare(
    set_idx: usize,
    scalar: &PlannedOperand,
    op: ComparisonOp,
    side: SetSide,
    span: &HydratedSpan,
    env: &EvalEnv<'_>,
) -> bool {
    let Some(scalar) = resolve_operand(scalar, span, env) else {
        return false; // absent scalar ⇒ no match (issue #183's rule)
    };
    let key = (env.ctx.trace_id, span.span_id);
    let values = env.event_sets.get(set_idx).and_then(|m| m.get(&key));
    // `!=` is the ALL-match operator; every other operator is ANY-match.
    let match_all = matches!(op, ComparisonOp::Neq);
    let mut matched = 0usize;
    let mut total = 0usize;
    match values {
        // No rows for this span: the empty set. ANY-match finds nothing;
        // ALL-match is vacuously satisfied.
        None => {}
        Some(EventValues::Text(items)) => {
            for item in items {
                total += 1;
                // A numeric-typed scalar against a text element is
                // cross-type: no match, for every operator.
                let hit = match (scalar.num, &scalar.text) {
                    (None, Some(text)) => match side {
                        SetSide::Lhs => cmp_str(op, item.as_str(), text.as_ref()),
                        SetSide::Rhs => cmp_str(op, text.as_ref(), item.as_str()),
                    },
                    _ => false,
                };
                if hit {
                    matched += 1;
                    if !match_all {
                        return true;
                    }
                }
            }
        }
        Some(EventValues::Num(items)) => {
            for item in items {
                total += 1;
                let hit = match scalar.num {
                    Some(n) => match side {
                        SetSide::Lhs => cmp_f64(op, *item, n),
                        SetSide::Rhs => cmp_f64(op, n, *item),
                    },
                    None => false,
                };
                if hit {
                    matched += 1;
                    if !match_all {
                        return true;
                    }
                }
            }
        }
    }
    match_all && matched == total
}

/// Evaluates one field expression against one span.
fn eval_expr(
    expr: &FieldExpr,
    filter: &PlannedFilter,
    leaf_idx: &mut usize,
    span: &HydratedSpan,
    env: &EvalEnv<'_>,
) -> Result<bool, ReadError> {
    match expr {
        // **A bare field is TRUTHINESS** (issue #335 Stage B, D12
        // capture) — planned as `.a = true`, so it is leaf-bearing like a
        // written comparison. NOT presence, which is what the
        // pre-collapse grammar produced.
        FieldExpr::Field(_) => eval_planned_leaf(filter, leaf_idx, span, env),
        FieldExpr::Exists { negated: false, .. }
        | FieldExpr::Binary {
            op: FieldOp::Cmp(_),
            ..
        } => eval_planned_leaf(filter, leaf_idx, span, env),
        // `{ .a = nil }` — ABSENCE, PulsusDB's kept semantics (ledger
        // `traceql-eq-nil-uncharacterised`): the key-existence leaf,
        // negated.
        FieldExpr::Exists { negated: true, .. } => {
            Ok(!eval_planned_leaf(filter, leaf_idx, span, env)?)
        }
        FieldExpr::Literal(Value::Bool(b)) => Ok(*b),
        FieldExpr::Literal(_) => Ok(false),
        // **`!` on a bare field is BOOLEAN NOT** (Stage B, D12) — planned
        // as `.a = false`, matching only where the value IS `false`.
        // Absent never matches, which falls out of the leaf.
        //
        // A present NON-boolean operand fails the whole query, as the
        // reference does (`expression (!.a) expected a boolean`): the
        // `PlannedLeafEval::BoolTruth` arm returns that error. An earlier
        // comment here claimed this needed a value-TYPE channel
        // `ResolvedVal` does not carry; that was wrong — booleans are
        // stored as the strings "true"/"false", so the co-loaded value
        // discriminates them from anything else without a new channel.
        FieldExpr::Unary {
            op: UnaryOp::Not,
            expr: inner,
        } if matches!(inner.as_ref(), FieldExpr::Field(_)) => {
            eval_planned_leaf(filter, leaf_idx, span, env)
        }
        FieldExpr::Unary {
            op: UnaryOp::Not,
            expr: inner,
        } => Ok(!eval_expr(inner, filter, leaf_idx, span, env)?),
        FieldExpr::Unary {
            op: UnaryOp::Neg,
            expr: inner,
        } => {
            eval_expr(inner, filter, leaf_idx, span, env)?;
            Ok(false)
        }
        FieldExpr::Binary {
            op: FieldOp::Bool(op),
            lhs,
            rhs,
        } => {
            let l = eval_expr(lhs, filter, leaf_idx, span, env)?;
            let r = eval_expr(rhs, filter, leaf_idx, span, env)?;
            Ok(match op {
                BoolOp::And => l && r,
                BoolOp::Or => l || r,
            })
        }
        FieldExpr::Binary {
            op: FieldOp::Arith(_),
            lhs,
            rhs,
        } => {
            eval_expr(lhs, filter, leaf_idx, span, env)?;
            eval_expr(rhs, filter, leaf_idx, span, env)?;
            Ok(false)
        }
    }
}

/// A matched-span-id set whose storage is charged against the request
/// budget for as long as it lives (code review round 3: spanset
/// intermediates are memory too). The charge is the set's **upper-bound
/// capacity** (every id comes from this trace's spans, so
/// `trace.spans.len()` bounds every set in the tree), paid BEFORE the
/// allocation; [`release_set`] returns it when the set is dropped or
/// merged away. `ByteBudget` is `&mut`-threaded, so release is explicit
/// on every exit path rather than `Drop`-based.
struct ChargedSet {
    set: HashSet<[u8; 8]>,
    charge: usize,
}

/// Per-entry cost of a charged span-id set (id + the container-overhead
/// envelope).
const SET_ENTRY_BYTES: usize =
    std::mem::size_of::<[u8; 8]>() + super::exec::RETAINED_ENTRY_OVERHEAD;

/// Charge-before-allocate constructor for a span-id set of up to
/// `capacity` entries.
fn charged_set(capacity: usize, budget: &mut ByteBudget) -> Result<ChargedSet, ReadError> {
    let charge = capacity * SET_ENTRY_BYTES;
    budget.charge(charge)?;
    Ok(ChargedSet {
        set: HashSet::with_capacity(capacity),
        charge,
    })
}

fn release_set(set: ChargedSet, budget: &mut ByteBudget) {
    budget.release(set.charge);
}

/// Evaluates one `{...}` filter over a trace → its matched span-id set
/// (`None` when nothing matches — the spanset produces no result for
/// this trace). The set is charged before allocation and released here
/// when empty.
fn eval_filter(
    body: Option<&FieldExpr>,
    filter: &PlannedFilter,
    trace: &TraceSpans,
    env: &EvalEnv<'_>,
    budget: &mut ByteBudget,
) -> Result<Option<ChargedSet>, ReadError> {
    let mut matched = charged_set(trace.spans.len(), budget)?;
    for span in &trace.spans {
        let is_match = match body {
            None => true,
            Some(expr) => {
                let mut leaf_idx = 0;
                let matched = eval_expr(expr, filter, &mut leaf_idx, span, env)?;
                // `collect` (filter.rs) and `eval_expr` walk the same AST
                // and pair leaves by pre-order POSITION. Nothing in the
                // types enforces that, and a mismatch is SILENT: every
                // leaf after the offending node is read as a different
                // predicate, so the query returns confidently wrong spans
                // rather than failing. Issue #335 Stage B produced exactly
                // that — `{ .a = nil }` planned no leaf while eval
                // consumed one. Checking it here makes any future
                // divergence fail across the whole existing suite instead
                // of waiting for a test that happens to use the shape.
                debug_assert_eq!(
                    leaf_idx,
                    filter.leaves.len(),
                    "leaf/eval walk desynchronised for {expr}"
                );
                matched
            }
        };
        if is_match {
            matched.set.insert(span.span_id);
        }
    }
    if matched.set.is_empty() {
        release_set(matched, budget);
        Ok(None)
    } else {
        Ok(Some(matched))
    }
}

/// Evaluates the spanset expression tree for one trace, preserving
/// matched-span membership through the cross-spanset algebra. Every set
/// in the tree — per-filter results AND the `&&`/`||` union sets — is
/// budget-charged before allocation; operand sets are released the
/// moment they are merged away, and a mid-evaluation breach propagates
/// the 422 error class (already-made charges die with the failing
/// request's budget — no cross-request state exists).
fn eval_spanset(
    expr: &SpansetExpr,
    plan: &SearchPlan,
    filter_idx: &mut usize,
    trace: &TraceSpans,
    env: &EvalEnv<'_>,
    budget: &mut ByteBudget,
) -> Result<Option<ChargedSet>, ReadError> {
    match expr {
        SpansetExpr::Filter(f) => {
            let filter = &plan.filters[*filter_idx];
            *filter_idx += 1;
            eval_filter(f.body.as_ref(), filter, trace, env, budget)
        }
        SpansetExpr::Binary { op, lhs, rhs } => {
            let l = eval_spanset(lhs, plan, filter_idx, trace, env, budget)?;
            let r = eval_spanset(rhs, plan, filter_idx, trace, env, budget)?;
            match op {
                // Trace-level intersection: the trace qualifies iff both
                // operands matched within it; its spanset is the union of
                // their matched spans (adjudication 1).
                pulsus_traceql::BoolOp::And => match (l, r) {
                    (Some(a), Some(b)) => Ok(Some(union_sets(a, b, trace, budget)?)),
                    (Some(a), None) => {
                        release_set(a, budget);
                        Ok(None)
                    }
                    (None, Some(b)) => {
                        release_set(b, budget);
                        Ok(None)
                    }
                    (None, None) => Ok(None),
                },
                pulsus_traceql::BoolOp::Or => match (l, r) {
                    (Some(a), Some(b)) => Ok(Some(union_sets(a, b, trace, budget)?)),
                    (Some(a), None) => Ok(Some(a)),
                    (None, Some(b)) => Ok(Some(b)),
                    (None, None) => Ok(None),
                },
            }
        }
        // Structural relations (issue #172 + #183): the empty-side
        // handling is modifier-aware, so both operand sets are passed
        // through to `eval_structural` (a Negated relation with an empty
        // LHS returns the whole RHS set — the single most error-prone edge).
        SpansetExpr::Structural {
            op,
            modifier,
            lhs,
            rhs,
        } => {
            let l = eval_spanset(lhs, plan, filter_idx, trace, env, budget)?;
            let r = eval_spanset(rhs, plan, filter_idx, trace, env, budget)?;
            eval_structural(*op, *modifier, l, r, trace, budget)
        }
    }
}

/// The all-zero `parent_id` sentinel: "no recorded parent" (a root).
const ZERO_ID: [u8; 8] = [0u8; 8];

/// Per-span transient cost envelope for the descendant BFS: one
/// adjacency-map contribution (map key + `Vec` header + the child-`Vec`
/// first-push capacity) plus up to two queue slots (an LHS seed and one
/// discovery per span — the queue is sized so it never reallocates),
/// plus the container-overhead envelope.
///
/// Child-`Vec` capacity ceiling — the load-bearing term. A parent's
/// child list is an `entry().or_default()`-created `Vec<[u8; 8]>` filled
/// by `push`. Rust's `Vec` first push jumps to `MIN_NON_ZERO_CAP = 4`
/// (for element sizes in `(1, 1024]`), so it must be charged **4** slots,
/// not 2 — 2 would under-book every single-child parent's real 32-byte
/// allocation by 16 bytes. 4 slots makes the term a genuine AGGREGATE
/// ceiling independent of the other terms' slack: a parent with `c`
/// children allocates `max(4, next_pow2(c)) * 8` bytes, and `max(4,
/// next_pow2(c)) ≤ 4·c` for every `c ≥ 1`, so the total child-`Vec` bytes
/// across all parents is `≤ 8 · Σ 4·c_p = 32 · (children) ≤ 32·spans` —
/// exactly the `spans × 4 × size_of::<[u8; 8]>()` this term books.
///
/// The queue-slot term stays at 2: the BFS queue is pre-sized to
/// `seeds + spans` and never reallocates, so ≤ 2 slots/span is exact
/// (unlike the child `Vec`, it has no `or_default()` first-push jump).
const DESCENDANT_TRANSIENT_BYTES: usize = std::mem::size_of::<[u8; 8]>()
    + std::mem::size_of::<Vec<[u8; 8]>>()
    + 4 * std::mem::size_of::<[u8; 8]>()
    + 2 * std::mem::size_of::<[u8; 8]>()
    + super::exec::RETAINED_ENTRY_OVERHEAD;

/// Per-entry cost of the sibling parent map (`parent_id → (LHS-match
/// count, representative span_id)` + the container-overhead envelope).
const SIBLING_ENTRY_BYTES: usize = std::mem::size_of::<[u8; 8]>()
    + std::mem::size_of::<(u32, [u8; 8])>()
    + super::exec::RETAINED_ENTRY_OVERHEAD;

/// Per-entry cost of the ancestor-walk `span_id → parent_id` map.
const ANCESTOR_ENTRY_BYTES: usize = std::mem::size_of::<[u8; 8]>()
    + std::mem::size_of::<[u8; 8]>()
    + super::exec::RETAINED_ENTRY_OVERHEAD;

/// Evaluates one structural relation (issue #172 + #183) over the trace's
/// hydrated spans — O(spans), bounded by `MAX_SPANS_PER_TRACE`.
///
/// The [`StructuralModifier`] selects which spans are returned:
/// - **Plain** — the RHS spans satisfying the relation (`rhs_participants`);
/// - **Negated** — the RHS spans NOT satisfying it (`rhs.set \ participants`);
///   with an EMPTY LHS but a non-empty RHS the whole RHS set matches
///   (nothing satisfies the relation, so every RHS span is a `!`-match);
/// - **Union** — both participating sides (`rhs_participants ∪ lhs_participants`).
///
/// Consumes (and releases) both operand sets; `None` when the result is
/// empty. Every intermediate is charge-before-allocate; on an error the
/// request's budget dies whole (the standing error-path convention).
fn eval_structural(
    op: StructuralOp,
    modifier: StructuralModifier,
    l: Option<ChargedSet>,
    r: Option<ChargedSet>,
    trace: &TraceSpans,
    budget: &mut ByteBudget,
) -> Result<Option<ChargedSet>, ReadError> {
    match modifier {
        // Plain and Union both require BOTH sides non-empty: the relation
        // needs an LHS and an RHS to participate.
        StructuralModifier::Plain | StructuralModifier::Union => match (l, r) {
            (Some(a), Some(b)) => {
                let result = match modifier {
                    StructuralModifier::Plain => rhs_participants(op, &a, &b, trace, budget)?,
                    _ => {
                        let rp = rhs_participants(op, &a, &b, trace, budget)?;
                        let lp = lhs_participants(op, &a, &b, trace, budget)?;
                        union_sets(rp, lp, trace, budget)?
                    }
                };
                release_set(a, budget);
                release_set(b, budget);
                finish_structural(result, budget)
            }
            (Some(a), None) => {
                release_set(a, budget);
                Ok(None)
            }
            (None, Some(b)) => {
                release_set(b, budget);
                Ok(None)
            }
            (None, None) => Ok(None),
        },
        StructuralModifier::Negated => match (l, r) {
            // Empty RHS: no span to return regardless of the LHS.
            (l_opt, None) => {
                if let Some(a) = l_opt {
                    release_set(a, budget);
                }
                Ok(None)
            }
            // Empty LHS, non-empty RHS: nothing satisfies the relation, so
            // EVERY RHS span is a negated match — return the whole RHS set.
            (None, Some(b)) => Ok(Some(b)),
            (Some(a), Some(b)) => {
                let participants = rhs_participants(op, &a, &b, trace, budget)?;
                let mut result = charged_set(trace.spans.len(), budget)?;
                for id in &b.set {
                    if !participants.set.contains(id) {
                        result.set.insert(*id);
                    }
                }
                release_set(participants, budget);
                release_set(a, budget);
                release_set(b, budget);
                finish_structural(result, budget)
            }
        },
    }
}

/// Releases an empty structural result set (returning `None`) or hands it
/// back charged.
fn finish_structural(
    result: ChargedSet,
    budget: &mut ByteBudget,
) -> Result<Option<ChargedSet>, ReadError> {
    if result.set.is_empty() {
        release_set(result, budget);
        Ok(None)
    } else {
        Ok(Some(result))
    }
}

/// The RHS spans satisfying the relation `{lhs} op {rhs}` — the Plain
/// result set (adjudicated pin 3 for #172's `>`/`>>`/`~`; #183 adds `<`
/// (direct parent) and `<<` (ancestor)).
fn rhs_participants(
    op: StructuralOp,
    lhs: &ChargedSet,
    rhs: &ChargedSet,
    trace: &TraceSpans,
    budget: &mut ByteBudget,
) -> Result<ChargedSet, ReadError> {
    match op {
        StructuralOp::Child => rel_children(lhs, rhs, trace, budget),
        StructuralOp::Parent => rel_parents(lhs, rhs, trace, budget),
        StructuralOp::Descendant => rel_descendants(lhs, rhs, trace, budget),
        StructuralOp::Ancestor => rel_ancestors(lhs, rhs, trace, budget),
        StructuralOp::Sibling => rel_siblings(lhs, rhs, trace, budget),
    }
}

/// The LHS spans participating in the relation (the LHS-side of a Union
/// modifier). It is the mirror of [`rhs_participants`] with the roles of
/// the operands swapped: for `>` (RHS is a child of LHS) the participating
/// LHS spans are the ones that are the PARENT of some RHS span, and so on.
fn lhs_participants(
    op: StructuralOp,
    lhs: &ChargedSet,
    rhs: &ChargedSet,
    trace: &TraceSpans,
    budget: &mut ByteBudget,
) -> Result<ChargedSet, ReadError> {
    match op {
        StructuralOp::Child => rel_parents(rhs, lhs, trace, budget),
        StructuralOp::Parent => rel_children(rhs, lhs, trace, budget),
        StructuralOp::Descendant => rel_ancestors(rhs, lhs, trace, budget),
        StructuralOp::Ancestor => rel_descendants(rhs, lhs, trace, budget),
        StructuralOp::Sibling => rel_siblings(rhs, lhs, trace, budget),
    }
}

/// `cand` spans whose **direct parent** matches `seed`. All-zero
/// `parent_id` spans have no parent and never match; a self-loop edge
/// (`parent_id == span_id`) never makes a span its own child. Orphans
/// (non-zero `parent_id` with no hydrated parent) never match because
/// every seed id is a hydrated span's id. A `cand` span that is ALSO a
/// seed is included when its parent is a *different* seed span (per-pair
/// self-exclusion, not a blanket LHS exclusion — codex review #183).
fn rel_children(
    seed: &ChargedSet,
    cand: &ChargedSet,
    trace: &TraceSpans,
    budget: &mut ByteBudget,
) -> Result<ChargedSet, ReadError> {
    let mut out = charged_set(trace.spans.len(), budget)?;
    for span in &trace.spans {
        if span.parent_id != ZERO_ID
            && span.parent_id != span.span_id
            && cand.set.contains(&span.span_id)
            && seed.set.contains(&span.parent_id)
        {
            out.set.insert(span.span_id);
        }
    }
    Ok(out)
}

/// `cand` spans that are the **direct parent** of some `seed` span (issue
/// #183's `<` in the RHS direction). All-zero parents and self-loops never
/// match.
fn rel_parents(
    seed: &ChargedSet,
    cand: &ChargedSet,
    trace: &TraceSpans,
    budget: &mut ByteBudget,
) -> Result<ChargedSet, ReadError> {
    let mut out = charged_set(trace.spans.len(), budget)?;
    for span in &trace.spans {
        if span.parent_id != ZERO_ID
            && span.parent_id != span.span_id
            && seed.set.contains(&span.span_id)
            && cand.set.contains(&span.parent_id)
        {
            out.set.insert(span.parent_id);
        }
    }
    Ok(out)
}

/// `cand` spans that are a **proper descendant** of *some* `seed` span — a
/// multi-source O(spans) BFS down a `parent_id → children` adjacency map
/// (the documented spike shape, docs/schemas.md §4.2) seeded from `seed`'s
/// matched ids. Only the seed spans themselves are the (distance-0) BFS
/// sources; every node reached across ≥ 1 edge is a proper descendant, so
/// a span that is BOTH a seed and a genuine descendant of a *different*
/// seed IS yielded (per-pair self-exclusion — codex review #183). Self-loop
/// edges are dropped (a span is never its own descendant) and the
/// `reached` set terminates every cycle. An out-of-window (never hydrated)
/// intermediate hop breaks the chain (docs/api.md §4.2).
fn rel_descendants(
    seed: &ChargedSet,
    cand: &ChargedSet,
    trace: &TraceSpans,
    budget: &mut ByteBudget,
) -> Result<ChargedSet, ReadError> {
    let transients = trace.spans.len() * DESCENDANT_TRANSIENT_BYTES;
    budget.charge(transients)?;
    let mut children: HashMap<[u8; 8], Vec<[u8; 8]>> = HashMap::with_capacity(trace.spans.len());
    for span in &trace.spans {
        if span.parent_id != ZERO_ID && span.parent_id != span.span_id {
            children
                .entry(span.parent_id)
                .or_default()
                .push(span.span_id);
        }
    }
    // Seeds are the distance-0 sources; each discovered node is enqueued
    // exactly once, so pushes are bounded by seeds (≤ spans) + one per
    // discovered node (≤ spans) and the reservation is never exceeded.
    let mut queue: Vec<[u8; 8]> = Vec::with_capacity(seed.set.len() + trace.spans.len());
    queue.extend(seed.set.iter().copied());
    let mut reached = charged_set(trace.spans.len(), budget)?;
    let mut out = charged_set(trace.spans.len(), budget)?;
    let mut cursor = 0;
    while cursor < queue.len() {
        let node = queue[cursor];
        cursor += 1;
        if let Some(kids) = children.get(&node) {
            for child in kids {
                // Every child is a PROPER descendant (distance ≥ 1) of a
                // seed source — including a child that is itself a seed.
                if reached.set.insert(*child) {
                    queue.push(*child);
                    if cand.set.contains(child) {
                        out.set.insert(*child);
                    }
                }
            }
        }
    }
    release_set(reached, budget);
    drop(children);
    budget.release(transients);
    Ok(out)
}

/// `cand` spans that are a **proper ancestor** of *some* `seed` span (issue
/// #183's `<<` in the RHS direction) — a multi-source O(spans) BFS UP a
/// `span_id → parent_id` map from the seed sources. Every node reached
/// across ≥ 1 up-edge is a proper ancestor, so a seed span that is also a
/// proper ancestor of a *different* seed IS yielded (per-pair
/// self-exclusion). Self-loop edges are skipped (a span is never its own
/// ancestor), the `reached` set terminates every cycle, and an
/// out-of-window parent breaks the chain.
fn rel_ancestors(
    seed: &ChargedSet,
    cand: &ChargedSet,
    trace: &TraceSpans,
    budget: &mut ByteBudget,
) -> Result<ChargedSet, ReadError> {
    // The `span_id → parent_id` map plus the upward BFS queue (≤ 2 slots
    // per span: seeds + one discovered ancestor each; sized so it never
    // reallocates). The `reached`/`out` sets go through `charged_set`.
    let map_charge =
        trace.spans.len() * (ANCESTOR_ENTRY_BYTES + 2 * std::mem::size_of::<[u8; 8]>());
    budget.charge(map_charge)?;
    let mut parent_of: HashMap<[u8; 8], [u8; 8]> = HashMap::with_capacity(trace.spans.len());
    for span in &trace.spans {
        parent_of.insert(span.span_id, span.parent_id);
    }
    // Seeds are the distance-0 sources; each discovered ancestor is
    // enqueued exactly once (≤ spans distinct parent ids), so pushes stay
    // within the reservation.
    let mut queue: Vec<[u8; 8]> = Vec::with_capacity(seed.set.len() + trace.spans.len());
    queue.extend(seed.set.iter().copied());
    let mut reached = charged_set(trace.spans.len(), budget)?;
    let mut out = charged_set(trace.spans.len(), budget)?;
    let mut cursor = 0;
    while cursor < queue.len() {
        let node = queue[cursor];
        cursor += 1;
        let Some(parent) = parent_of.get(&node).copied() else {
            continue;
        };
        if parent == ZERO_ID || parent == node {
            continue; // no parent / self-loop
        }
        // `parent` is a PROPER ancestor (distance ≥ 1) of a seed source.
        if reached.set.insert(parent) {
            queue.push(parent);
            if cand.set.contains(&parent) {
                out.set.insert(parent);
            }
        }
    }
    release_set(reached, budget);
    drop(parent_of);
    budget.release(map_charge);
    Ok(out)
}

/// `cand` spans sharing a `parent_id` with a **distinct** `seed` span
/// (self excluded). Adjudicated pin 2: all-zero `parent_id` (root) spans
/// have no parent to share and never match. One pass builds
/// `parent_id → (seed count, representative)`; a group of one only matches
/// when its sole seed member is a different span.
fn rel_siblings(
    seed: &ChargedSet,
    cand: &ChargedSet,
    trace: &TraceSpans,
    budget: &mut ByteBudget,
) -> Result<ChargedSet, ReadError> {
    let map_charge = trace.spans.len() * SIBLING_ENTRY_BYTES;
    budget.charge(map_charge)?;
    let mut parents: HashMap<[u8; 8], (u32, [u8; 8])> = HashMap::with_capacity(trace.spans.len());
    for span in &trace.spans {
        if span.parent_id != ZERO_ID && seed.set.contains(&span.span_id) {
            parents
                .entry(span.parent_id)
                .and_modify(|(count, _)| *count += 1)
                .or_insert((1, span.span_id));
        }
    }
    let mut out = charged_set(trace.spans.len(), budget)?;
    for span in &trace.spans {
        if span.parent_id == ZERO_ID || !cand.set.contains(&span.span_id) {
            continue;
        }
        if let Some((count, representative)) = parents.get(&span.parent_id)
            && (*count >= 2 || *representative != span.span_id)
        {
            out.set.insert(span.span_id);
        }
    }
    drop(parents);
    budget.release(map_charge);
    Ok(out)
}

// -- issue #181: nested-set structural intrinsics -----------------------

/// One span's nested-set (modified-preorder) numbering — matched to
/// Tempo v3.0.2's observed scheme (base 1): `left` on Euler-tour enter,
/// `right` on exit (shared counter), and `parent` = the parent span's
/// `left`, or `-1` for a root/orphan.
#[derive(Debug, Clone, Copy)]
pub(crate) struct NestedSetValues {
    left: i64,
    right: i64,
    parent: i64,
}

impl NestedSetValues {
    fn value(&self, field: NestedSetField) -> i64 {
        match field {
            NestedSetField::Parent => self.parent,
            NestedSetField::Left => self.left,
            NestedSetField::Right => self.right,
        }
    }
}

/// The per-trace numbering, keyed by span_id — total over the hydrated
/// forest (every span gets an entry).
type NestedSetIndex = HashMap<[u8; 8], NestedSetValues>;

/// An explicit Euler-tour frame: the numbering is iterative (a
/// 10 000-deep linear chain — `MAX_SPANS_PER_TRACE` — must never recurse).
#[derive(Clone, Copy)]
enum EulerFrame {
    Enter([u8; 8]),
    Exit([u8; 8]),
}

/// The retained nested-set index, charged against the request budget for
/// as long as it lives (mirrors [`ChargedSet`]).
struct ChargedNestedSet {
    index: NestedSetIndex,
    charge: usize,
}

/// Per-entry cost of the retained index (key + values + overhead).
const NESTED_SET_ENTRY_BYTES: usize = std::mem::size_of::<[u8; 8]>()
    + std::mem::size_of::<NestedSetValues>()
    + super::exec::RETAINED_ENTRY_OVERHEAD;

/// Per-span transient cost envelope for the numbering pass: the span-id
/// set (id + overhead), the children adjacency map (key + `Vec` header +
/// the child-`Vec` first-push capacity + overhead), the sorted span view
/// (one reference), up to two Euler-stack frames per span (the stack is
/// sized so it never reallocates), and the promoted-cycle-root set (id +
/// overhead — empty for well-formed data, bounded by spans for a pure
/// cycle).
///
/// Child-`Vec` capacity ceiling — the load-bearing term. A parent's child
/// list is an `or_default()`-created `Vec<[u8; 8]>` filled by `push`.
/// Rust's `Vec` first push jumps to `MIN_NON_ZERO_CAP = 4` (for element
/// sizes in `(1, 1024]`), so it must be charged **4** slots, not 2 — 2
/// would under-book every single-child parent's real 32-byte allocation
/// by 16 bytes. 4 slots makes the term a genuine AGGREGATE ceiling
/// independent of the other terms' slack: a parent with `c` children
/// allocates `max(4, next_pow2(c)) * 8` bytes, and `max(4, next_pow2(c)) ≤
/// 4·c` for every `c ≥ 1`, so the total child-`Vec` bytes across all
/// parents is `≤ 8 · Σ 4·c_p = 32 · (children) ≤ 32·spans` — exactly the
/// `spans × 4 × size_of::<[u8; 8]>()` this term books. (With 2 slots the
/// worst case — a linear chain, `spans − 1` single-child parents each at
/// cap 4 — allocates `≈ 32·spans` against a `16·spans` charge.)
const NESTED_SET_TRANSIENT_BYTES: usize = std::mem::size_of::<[u8; 8]>()
    + super::exec::RETAINED_ENTRY_OVERHEAD
    + std::mem::size_of::<[u8; 8]>()
    + std::mem::size_of::<Vec<[u8; 8]>>()
    + 4 * std::mem::size_of::<[u8; 8]>()
    + super::exec::RETAINED_ENTRY_OVERHEAD
    + std::mem::size_of::<&HydratedSpan>()
    + 2 * std::mem::size_of::<EulerFrame>()
    + std::mem::size_of::<[u8; 8]>()
    + super::exec::RETAINED_ENTRY_OVERHEAD;

fn release_nested_set(charged: ChargedNestedSet, budget: &mut ByteBudget) {
    budget.release(charged.charge);
}

/// Drains the Euler-tour stack: on `Enter(id)` skip an already-numbered
/// span (the cycle guard = visited set is the index itself), else assign
/// `left`, push the matching `Exit`, and push the node's children in
/// reverse so they pop in ascending (sibling) order; on `Exit(id)` assign
/// `right`. The shared `counter` produces the contiguous `1..=2·spans`
/// permutation.
fn euler_drain(
    stack: &mut Vec<EulerFrame>,
    children: &HashMap<[u8; 8], Vec<[u8; 8]>>,
    index: &mut NestedSetIndex,
    counter: &mut i64,
) {
    while let Some(frame) = stack.pop() {
        match frame {
            EulerFrame::Enter(id) => {
                if index.contains_key(&id) {
                    continue;
                }
                index.insert(
                    id,
                    NestedSetValues {
                        left: *counter,
                        right: 0,
                        parent: -1,
                    },
                );
                *counter += 1;
                stack.push(EulerFrame::Exit(id));
                if let Some(kids) = children.get(&id) {
                    for kid in kids.iter().rev() {
                        stack.push(EulerFrame::Enter(*kid));
                    }
                }
            }
            EulerFrame::Exit(id) => {
                if let Some(v) = index.get_mut(&id) {
                    v.right = *counter;
                    *counter += 1;
                }
            }
        }
    }
}

/// Computes one candidate trace's nested-set numbering over the hydrated
/// `parent_id` forest (issue #181) — iterative modified-preorder, base 1,
/// siblings ordered by our deterministic `(timestamp_ns, span_id)` proxy.
/// Every intermediate is charge-before-allocate; the retained index is
/// returned charged and released by the caller after `eval_spanset`.
fn compute_nested_set(
    trace: &TraceSpans,
    budget: &mut ByteBudget,
) -> Result<ChargedNestedSet, ReadError> {
    let n = trace.spans.len();
    let index_charge = n * NESTED_SET_ENTRY_BYTES;
    budget.charge(index_charge)?;
    let transient_charge = n * NESTED_SET_TRANSIENT_BYTES;
    budget.charge(transient_charge)?;

    let mut index: NestedSetIndex = HashMap::with_capacity(n);
    let mut span_ids: HashSet<[u8; 8]> = HashSet::with_capacity(n);
    for span in &trace.spans {
        span_ids.insert(span.span_id);
    }

    // A deterministic ascending view — our sibling/root ordering proxy.
    // Building the children lists and seeding roots from this view keeps
    // every child list and the root seeds in ascending order without a
    // per-list sort.
    let mut ordered: Vec<&HydratedSpan> = trace.spans.iter().collect();
    ordered.sort_by(|a, b| (a.timestamp_ns, a.span_id).cmp(&(b.timestamp_ns, b.span_id)));

    // A span is a child iff its parent is a hydrated span; otherwise
    // (all-zero parent, or an out-of-window/orphan parent) it is a root
    // of the hydrated forest (the #172 windowed-forest precedent).
    let mut children: HashMap<[u8; 8], Vec<[u8; 8]>> = HashMap::with_capacity(n);
    for span in &ordered {
        if span.parent_id != ZERO_ID && span_ids.contains(&span.parent_id) {
            children
                .entry(span.parent_id)
                .or_default()
                .push(span.span_id);
        }
    }

    let mut counter: i64 = 1;
    // Sized so it never reallocates: at most two live frames per span.
    let mut stack: Vec<EulerFrame> = Vec::with_capacity(2 * n);
    // Seed roots in reverse ascending order so they pop ascending.
    for span in ordered.iter().rev() {
        if span.parent_id == ZERO_ID || !span_ids.contains(&span.parent_id) {
            stack.push(EulerFrame::Enter(span.span_id));
        }
    }
    euler_drain(&mut stack, &children, &mut index, &mut counter);
    // Total coverage: any span still unvisited is part of a pure cycle
    // (no forest root) — promote it to a root in ascending order,
    // guaranteeing termination and the full `1..=2·spans` numbering. A
    // promoted span is the root of its (cyclic) component and MUST keep
    // the root sentinel even though its `parent_id` points at another
    // numbered cycle member — otherwise a pure cycle would have no
    // `nestedSetParent < 0` root at all (mirrors #172's cycle handling:
    // a malformed cycle still yields a well-defined result). Empty for
    // well-formed data, so it allocates nothing then.
    let mut promoted_roots: HashSet<[u8; 8]> = HashSet::new();
    for span in &ordered {
        if !index.contains_key(&span.span_id) {
            promoted_roots.insert(span.span_id);
            stack.push(EulerFrame::Enter(span.span_id));
            euler_drain(&mut stack, &children, &mut index, &mut counter);
        }
    }

    // Parent pass: a root/orphan and a promoted cycle-root keep the `-1`
    // sentinel; any other span's `parent` is its parent span's `left`
    // (assigned by construction).
    for span in &trace.spans {
        if span.parent_id == ZERO_ID || promoted_roots.contains(&span.span_id) {
            continue;
        }
        let Some(parent_left) = index.get(&span.parent_id).map(|v| v.left) else {
            continue;
        };
        if let Some(v) = index.get_mut(&span.span_id) {
            v.parent = parent_left;
        }
    }

    drop(span_ids);
    drop(promoted_roots);
    drop(ordered);
    drop(children);
    drop(stack);
    budget.release(transient_charge);
    Ok(ChargedNestedSet {
        index,
        charge: index_charge,
    })
}

/// Merges two charged operand sets into a freshly charged union set —
/// the union is charged BEFORE it is allocated (three sets are briefly
/// live and all three are counted), then both operands are released.
fn union_sets(
    a: ChargedSet,
    b: ChargedSet,
    trace: &TraceSpans,
    budget: &mut ByteBudget,
) -> Result<ChargedSet, ReadError> {
    let mut union = charged_set(trace.spans.len(), budget)?;
    union.set.extend(a.set.iter().copied());
    union.set.extend(b.set.iter().copied());
    release_set(a, budget);
    release_set(b, budget);
    Ok(union)
}

/// One aggregate stage's result for one spanset (issue #510).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AggregateOutcome {
    /// The scalar the threshold comparison uses — the SAME value and the
    /// SAME comparison as before issue #510.
    pub(crate) scalar: f64,
    /// The typed value the response attribute carries.
    pub(crate) wire: GroupValue,
}

/// The aggregate's scalar over ONE spanset's spans, AND the typed value
/// its response attribute carries (issue #492 item 2 for the source,
/// issue #510 for the type).
///
/// The source is the spanset the fold hands it — it used to aggregate the
/// whole trace filtered by the matched-id set, which is what made an
/// aggregate's written position invisible. `Count` is the spanset's
/// member count. On the flat spanset that is the same number as the old
/// matched-id-set size, because `group_hydrated_rows` dedupes hydration
/// rows by `span_id` (`exec.rs:3375, 3401-3403`) and the matched set is
/// drawn from those same spans — pinned by
/// `count_matches_the_deduped_span_set`.
///
/// The wire type follows the aggregate's own type, measured against the
/// reference: `count()` is an `intValue`; the four duration forms are Go
/// `time.Duration.String()` `stringValue`s, and `avg(duration)` is a
/// TRUNCATING integer nanosecond division (6 500 000 000 / 3 renders
/// `2.166666666s`, not `2.1666666666666665s`); `min`/`max(.attr)` take the
/// WINNING contributor's own type; `sum(.attr)` is an `intValue` iff every
/// contributor is stored `int`; `avg(.attr)` is always a `doubleValue`.
///
/// **Where we deliberately differ.** With two spans carrying one key at
/// different stored types the reference's running sum keeps the first
/// contributor's type and silently DROPS every later value of another
/// type, while its mean divides that partial sum by the count of ALL
/// contributors — so the same two values give it four answers depending
/// on arrival order. We sum every numeric contributor. Ledger row
/// `traceql-spanset-aggregate-mixed-type-attribute`, and entry 24 of
/// docs/reference-defects-we-do-not-copy.md.
fn aggregate_value(
    agg: &super::search_plan::PlannedAggregate,
    trace_id: [u8; 16],
    spans: &[&HydratedSpan],
    attrs: &BatchAttrs,
) -> Option<AggregateOutcome> {
    // Each contributor's value and, for an attribute source, the OTLP
    // kind it was stored as. A duration contributor has no stored type;
    // it never reaches the type-driven arms below.
    let values: Vec<(f64, StoredType)> = match &agg.source {
        AggSource::Count => {
            let n = spans.len();
            return Some(AggregateOutcome {
                scalar: n as f64,
                wire: GroupValue::Int(n as i64),
            });
        }
        AggSource::DurationNs => spans
            .iter()
            .map(|s| (s.duration_ns as f64, StoredType::Unknown))
            .collect(),
        AggSource::Attr { field_idx } => spans
            .iter()
            .filter_map(|s| {
                let key = (trace_id, s.span_id);
                let v = attrs.agg_values[*field_idx].get(&key).copied()?;
                let t = attrs.agg_types[*field_idx]
                    .get(&key)
                    .copied()
                    .unwrap_or_default();
                Some((v, t))
            })
            .collect(),
    };
    if values.is_empty() {
        return None;
    }
    // The min/max WINNER — the first contributor at the extreme, so a tie
    // takes the earlier span's type, matching the fold below.
    let winner = |better: fn(f64, f64) -> bool| -> (f64, StoredType) {
        let mut best = values[0];
        for &(v, t) in &values[1..] {
            if better(v, best.0) {
                best = (v, t);
            }
        }
        best
    };
    let (scalar, winner_type) = match agg.op {
        AggregateOp::Count => (values.len() as f64, StoredType::Unknown),
        AggregateOp::Sum => (values.iter().map(|(v, _)| *v).sum(), StoredType::Unknown),
        AggregateOp::Avg => (
            values.iter().map(|(v, _)| *v).sum::<f64>() / values.len() as f64,
            StoredType::Unknown,
        ),
        AggregateOp::Min => winner(|a, b| a < b),
        AggregateOp::Max => winner(|a, b| a > b),
    };
    // An integer SUM needs EVERY contributor to be stored `int` — one
    // double contributor makes the sum a double. A row written before
    // `val_type` existed has an unrecoverable type and counts as not-int.
    let all_int = values.iter().all(|(_, t)| *t == StoredType::Int);
    let wire = match &agg.source {
        // Returned above; this arm is unreachable.
        AggSource::Count => GroupValue::Int(values.len() as i64),
        AggSource::DurationNs => {
            // Integer nanosecond arithmetic, so `avg` TRUNCATES the way the
            // reference's does: it sums `time.Duration`s and divides by the
            // contributor count in whole nanoseconds. 6 500 000 000 / 3
            // renders `2.166666666s`; the `f64` division would render
            // `2.1666666666666665s`.
            let nanos: i64 = match agg.op {
                AggregateOp::Avg => {
                    let sum: i64 = values.iter().map(|(v, _)| *v as i64).sum();
                    sum / values.len() as i64
                }
                _ => scalar as i64,
            };
            GroupValue::Str(go_duration_string(nanos))
        }
        AggSource::Attr { .. } => match agg.op {
            // The WINNING contributor's own type.
            AggregateOp::Min | AggregateOp::Max if winner_type == StoredType::Int => {
                GroupValue::Int(scalar as i64)
            }
            AggregateOp::Sum if all_int => GroupValue::Int(scalar as i64),
            // `avg` is always a double, and a mixed-type or float sum is
            // too. `to_bits()` is RAW — an aggregate value is not a group
            // key, so a `-0.0` sum keeps its sign.
            _ => GroupValue::Double(scalar.to_bits()),
        },
    };
    Some(AggregateOutcome { scalar, wire })
}

/// Renders a stored status code back to its TraceQL keyword (the same
/// closed set `filter::compile_leaf` lowers — OTEL wire codes).
fn status_keyword(code: i8) -> &'static str {
    match code {
        1 => "ok",
        2 => "error",
        _ => "unset",
    }
}

/// Renders a stored kind code back to its TraceQL keyword.
fn kind_keyword(code: i8) -> &'static str {
    match code {
        1 => "internal",
        2 => "server",
        3 => "client",
        4 => "producer",
        5 => "consumer",
        _ => "unspecified",
    }
}

/// Test-only clone observer (code review round 4): counts every
/// selected-attribute value clone actually performed. `record()` sits
/// immediately between the value's budget charge and its clone in
/// [`build_summary`], so "zero recorded clones on a breach path" is an
/// observable proof that the charge preceded — and prevented — the
/// clone, not an inference from counter arithmetic. Thread-local: the
/// test harness runs tests concurrently.
#[cfg(test)]
pub(crate) mod clone_probe {
    use std::cell::Cell;

    thread_local! {
        static VALUE_CLONES: Cell<usize> = const { Cell::new(0) };
    }

    pub(crate) fn reset() {
        VALUE_CLONES.with(|c| c.set(0));
    }

    pub(crate) fn count() -> usize {
        VALUE_CLONES.with(|c| c.get())
    }

    pub(crate) fn record() {
        VALUE_CLONES.with(|c| c.set(c.get() + 1));
    }
}

/// Resolves one projection group's value for one span (issue #479), or
/// `None` when no source's gate passes.
///
/// **The gate is the leaf's own truth value, re-evaluated against the same
/// `EvalEnv`** — which is what makes the rule per SPAN rather than per
/// query. `eval_expr` does not short-circuit (both operands of `&&`/`||`
/// are always evaluated), and `EvalEnv` is unchanged between the matching
/// pass and this one, so a leaf that decided the match decides the
/// projection identically. Measured on the reference:
/// `{name="slow-op" || span.http.method="GET"}` gives the `slow-op` span a
/// `name` and NO `http.method`, though it stores one.
fn resolve_projection<'a>(
    group: &'a ProjectionGroup,
    plan: &SearchPlan,
    trace_id: [u8; 16],
    span: &'a HydratedSpan,
    env: &EvalEnv<'a>,
) -> Result<Option<ProjectedValue<'a>>, ReadError> {
    for source in &group.sources {
        if let Some((filter_idx, leaf_idx)) = source.gate {
            let leaf = &plan.filters[filter_idx].leaves[leaf_idx];
            if !eval_leaf(leaf, span, env)? {
                continue;
            }
        }
        let key = (trace_id, span.span_id);
        let resolved = match &source.value {
            ProjectionValue::Name => Some(ProjectedValue::Borrowed(span.name.as_str())),
            ProjectionValue::Service => Some(ProjectedValue::Borrowed(span.service.as_str())),
            ProjectionValue::StatusMessage => {
                Some(ProjectedValue::Borrowed(span.status_message.as_str()))
            }
            ProjectionValue::ScopeName => Some(ProjectedValue::Borrowed(span.scope_name.as_str())),
            ProjectionValue::ScopeVersion => {
                Some(ProjectedValue::Borrowed(span.scope_version.as_str()))
            }
            ProjectionValue::Status => {
                Some(ProjectedValue::Borrowed(status_keyword(span.status_code)))
            }
            ProjectionValue::Kind => Some(ProjectedValue::Borrowed(kind_keyword(span.kind))),
            // A scalar render, bounded by construction (16 hex bytes).
            ProjectionValue::ParentIdHex => Some(ProjectedValue::Owned(hex_lower(&span.parent_id))),
            // The query's OWN literal — no column is read, so the arm
            // comes from the LITERAL's type. A boolean condition
            // (`{… && .b = true}`) projects `{"boolValue":true}`, which
            // the reference sends and which a `stringValue` rendering got
            // wrong; a string condition projects `{"stringValue":"…"}`.
            //
            // A STRING literal compared against a typed attribute is the
            // one shape where the literal's type and the stored type can
            // disagree, and it has no parity target to get wrong:
            // `{… && .big = "9007199254740993"}` and `{… && .v = "2"}`
            // both return zero traces on the reference while we return
            // the trace, so the accept surfaces have already parted
            // (pre-existing, out of scope here).
            ProjectionValue::ProbeLiteral { text, literal_type } => Some(ProjectedValue::Typed(
                typed_attr_value(*literal_type, None, Some(text.as_str())),
            )),
            // Issue #510: the fused membership read now carries the stored
            // type beside the value, so the projection renders the arm the
            // sender stored rather than always `stringValue`.
            ProjectionValue::ProbeValue { probe_idx } => env
                .attrs
                .membership
                .get(*probe_idx)
                .and_then(|m| m.value(&key))
                .map(|(v, t)| ProjectedValue::Typed(typed_attr_value(t, None, Some(v)))),
            ProjectionValue::SelectValue { field_idx } => env
                .attrs
                .select_values
                .get(*field_idx)
                .and_then(|m| m.get(&key))
                .map(|v| {
                    let t = env
                        .attrs
                        .select_types
                        .get(*field_idx)
                        .and_then(|m| m.get(&key))
                        .copied()
                        .unwrap_or_default();
                    ProjectedValue::Typed(typed_attr_value(t, None, Some(v.as_str())))
                }),
            // A per-trace query-time NUMBER, so it projects in the
            // `intValue` arm — the same arm `by(nestedSetLeft)` already
            // resolved it into, and the arm the reference sends
            // (measured: `{… && nestedSetLeft > 0}` returns
            // `{"key":"nestedSetLeft","value":{"intValue":"1"}}` there).
            // It rendered as a string before issue #510, so one stored
            // number reached the wire two ways from one engine.
            ProjectionValue::NestedSet(field) => env
                .nested_set
                .and_then(|idx| idx.get(&span.span_id))
                .map(|v| ProjectedValue::Typed(AttrArm::Int(v.value(*field)))),
        };
        if let Some(value) = resolved {
            return Ok(Some(value));
        }
    }
    Ok(None)
}

/// A resolved projection value, borrowed from the span / batch reads where
/// it already exists and owned only for the two bounded scalar renders —
/// so an unbounded string is never cloned before its budget charge.
enum ProjectedValue<'a> {
    Borrowed(&'a str),
    Owned(String),
    /// A stored attribute whose wire arm [`typed_attr_value`] has already
    /// decided (issue #510).
    Typed(AttrArm<'a>),
}

impl ProjectedValue<'_> {
    /// The value's own name, for the [`ProjectionTarget::SpanName`]
    /// target. Only the physical sources can fill that target, so a typed
    /// attribute arm never reaches it.
    fn as_str(&self) -> &str {
        match self {
            ProjectedValue::Borrowed(s) => s,
            ProjectedValue::Owned(s) => s.as_str(),
            ProjectedValue::Typed(arm) => match arm {
                AttrArm::Text(s) => s,
                _ => "",
            },
        }
    }

    /// The retained payload the response value will own — charged BEFORE
    /// it is built.
    fn payload_bytes(&self) -> usize {
        match self {
            ProjectedValue::Borrowed(s) => s.len(),
            ProjectedValue::Owned(s) => s.len(),
            ProjectedValue::Typed(arm) => arm.payload_bytes(),
        }
    }

    /// The owned typed response value. The three untyped sources are
    /// strings on the reference too (`select(status)` comes back
    /// `{"stringValue":"ok"}` there and here), so they render `Str`.
    fn into_value(self) -> GroupValue {
        match self {
            ProjectedValue::Borrowed(s) => GroupValue::Str(s.to_string()),
            ProjectedValue::Owned(s) => GroupValue::Str(s),
            ProjectedValue::Typed(arm) => arm.into_value(),
        }
    }
}

/// Builds one span summary, charging the budget **before every retained
/// allocation** (code review round 2): the summary's overhead + the
/// attributes buffer at full capacity are charged before anything is
/// cloned, then each projected value's bytes are charged before that
/// value enters the buffer. Issue #479 makes the NAME charge conditional
/// too — an uncollected name costs nothing and writes no wire field. The
/// one stated residual: scalar renders (`status`/`kind`/`span:parentID`/
/// nested-set numbers — ≤ ~20 bytes by construction) are transiently
/// allocated or borrowed to learn their length, then charged before
/// entering the buffer; unbounded strings (name/service/attr values) are
/// never cloned before their charge.
fn build_summary(
    plan: &SearchPlan,
    trace_id: [u8; 16],
    span: &HydratedSpan,
    env: &EvalEnv<'_>,
    budget: &mut ByteBudget,
) -> Result<SpanSummary, ReadError> {
    let attr_capacity = plan.projected_attr_capacity();
    budget.charge(
        super::exec::RETAINED_ENTRY_OVERHEAD
            + attr_capacity * std::mem::size_of::<ProjectedAttribute>(),
    )?;
    let mut attributes = Vec::with_capacity(attr_capacity);
    let mut name: Option<String> = None;
    for group in &plan.projections {
        let Some(value) = resolve_projection(group, plan, trace_id, span, env)? else {
            continue;
        };
        match &group.target {
            ProjectionTarget::SpanName => {
                budget.charge(value.as_str().len())?;
                // The probe sits between the charge and the clone: a
                // refused charge returns above and this line — and
                // therefore the clone below — never executes (the
                // round-4 observable ordering proof).
                #[cfg(test)]
                clone_probe::record();
                name = Some(value.as_str().to_string());
            }
            ProjectionTarget::Attribute(key) => {
                budget.charge(key.as_str().len() + value.payload_bytes())?;
                #[cfg(test)]
                clone_probe::record();
                attributes.push(ProjectedAttribute::new(&group.field, value.into_value()));
            }
        }
    }
    Ok(SpanSummary::new(
        span.span_id,
        name,
        span.timestamp_ns,
        span.duration_ns,
        attributes,
    ))
}

/// Per-span transient partition cost for [`build_span_set_groups`] (issue
/// #193): the `tuple → bucket` map entry, its distinct-tuple `order` slot,
/// the `members` outer `Vec` slot, and one member-index slot. An
/// upper-bound (every span a distinct group) charged before the partition
/// allocations; the variable string payloads are charged on top and the
/// whole envelope is released when the retained groups are built.
const PER_SPAN_GROUP_TRANSIENT_BYTES: usize = std::mem::size_of::<GroupTuple>()
    + std::mem::size_of::<usize>()
    + super::exec::RETAINED_ENTRY_OVERHEAD
    + std::mem::size_of::<GroupTuple>()
    + std::mem::size_of::<Vec<usize>>()
    + std::mem::size_of::<usize>();

/// Charges `bytes` against `budget` AND records them in the caller's
/// running transient total, so the exact amount charged is released
/// wholesale when the transient partition dies.
fn charge_transient(
    budget: &mut ByteBudget,
    transient: &mut usize,
    bytes: usize,
) -> Result<(), ReadError> {
    budget.charge(bytes)?;
    *transient += bytes;
    Ok(())
}

/// The owned string payload of a group tuple (the `.len()`-based payload
/// convention; numeric/bool/nil values add nothing).
fn tuple_string_payload(tuple: &GroupTuple) -> usize {
    tuple.iter().map(GroupValue::payload_bytes).sum()
}

/// Charges a string's `.len()` into the transient total (before the clone)
/// and returns it as a [`GroupValue::Str`].
fn charged_str(
    s: &str,
    budget: &mut ByteBudget,
    transient: &mut usize,
) -> Result<GroupValue, ReadError> {
    charge_transient(budget, transient, s.len())?;
    Ok(GroupValue::Str(s.to_string()))
}

/// Go's `time.Duration.String()` fractional-part formatter: emits up to
/// `prec` fractional digits (trailing zeros trimmed) as `".xxx"` (empty if
/// all zero) and returns the integer part `u / 10^prec`.
fn go_duration_frac(mut u: u128, prec: u32) -> (String, u128) {
    let mut digits: Vec<u8> = Vec::new();
    let mut printing = false;
    for _ in 0..prec {
        let digit = (u % 10) as u8;
        printing = printing || digit != 0;
        if printing {
            digits.push(b'0' + digit);
        }
        u /= 10;
    }
    let frac = if digits.is_empty() {
        String::new()
    } else {
        digits.reverse();
        let mut s = String::from(".");
        // Safety: `digits` holds only ASCII '0'..='9' by construction.
        s.push_str(std::str::from_utf8(&digits).expect("ascii digits"));
        s
    };
    (frac, u)
}

/// Renders a nanosecond duration as Go's `time.Duration.String()` — the
/// form Tempo v3.0.2 uses for a TraceQL `duration`/`traceDuration` group
/// value's `stringValue` (verified live via the grouping differential):
/// sub-second uses `ns`/`µs`/`ms` with a trimmed fraction, `>= 1s` uses
/// `[h][m]s` with a trimmed fractional-seconds part (`1.5s`, `1m30s`,
/// `1h1m1s`, `500µs`, `0s`).
fn go_duration_string(nanos: i64) -> String {
    if nanos == 0 {
        return "0s".to_string();
    }
    let neg = nanos < 0;
    let u0: u128 = (nanos as i128).unsigned_abs();
    const SECOND: u128 = 1_000_000_000;
    let s = if u0 < SECOND {
        let (unit, prec): (&str, u32) = if u0 < 1_000 {
            ("ns", 0)
        } else if u0 < 1_000_000 {
            ("µs", 3)
        } else {
            ("ms", 6)
        };
        let (frac, int_part) = go_duration_frac(u0, prec);
        format!("{int_part}{frac}{unit}")
    } else {
        let (frac, secs) = go_duration_frac(u0, 9);
        let mut out = format!("{}{frac}s", secs % 60);
        let mins = secs / 60;
        if mins > 0 {
            out = format!("{}m{out}", mins % 60);
            let hours = mins / 60;
            if hours > 0 {
                out = format!("{hours}h{out}");
            }
        }
        out
    };
    if neg { format!("-{s}") } else { s }
}

/// Resolves one `by()` key's typed value for a hydrated span (issue #193),
/// covering EVERY resolvable key form — physical columns, the #181
/// nested-set / #184 trace-level / #192 instrumentation intrinsics, and
/// attributes — from the SAME per-trace evaluation environment the filters
/// read (no new scan). String clones charge their `.len()` into the
/// transient total BEFORE the clone; numeric / absent values allocate
/// nothing.
fn resolve_group_value(
    resolver: &GroupKeyResolver,
    env: &EvalEnv<'_>,
    span: &HydratedSpan,
    budget: &mut ByteBudget,
    transient: &mut usize,
) -> Result<GroupValue, ReadError> {
    let trace_id = env.ctx.trace_id;
    Ok(match resolver {
        GroupKeyResolver::Physical(PhysicalSelect::Service) => {
            charged_str(&span.service, budget, transient)?
        }
        GroupKeyResolver::Physical(PhysicalSelect::Name) => {
            charged_str(&span.name, budget, transient)?
        }
        // `duration`/`status`/`kind` render by their TraceQL TYPE, matching
        // Tempo v3.0.2 (verified live via the grouping differential): a
        // `duration` value is Go's `time.Duration.String()` form, and
        // `status`/`kind` are their lowercase keyword enums — all
        // `stringValue`, NOT numeric.
        GroupKeyResolver::Physical(PhysicalSelect::DurationNs) => {
            charged_str(&go_duration_string(span.duration_ns), budget, transient)?
        }
        GroupKeyResolver::Physical(PhysicalSelect::Status) => {
            charged_str(status_keyword(span.status_code), budget, transient)?
        }
        GroupKeyResolver::Physical(PhysicalSelect::Kind) => {
            charged_str(kind_keyword(span.kind), budget, transient)?
        }
        GroupKeyResolver::StatusMessage => charged_str(&span.status_message, budget, transient)?,
        GroupKeyResolver::InstrumentationName => charged_str(&span.scope_name, budget, transient)?,
        GroupKeyResolver::InstrumentationVersion => {
            charged_str(&span.scope_version, budget, transient)?
        }
        GroupKeyResolver::SpanIdHex => {
            let mut buf = [0u8; 16];
            charged_str(hex_into(&span.span_id, &mut buf), budget, transient)?
        }
        GroupKeyResolver::ParentIdHex => {
            let mut buf = [0u8; 16];
            charged_str(hex_into(&span.parent_id, &mut buf), budget, transient)?
        }
        GroupKeyResolver::TraceIdHex => {
            let mut buf = [0u8; 32];
            charged_str(hex_into(&trace_id, &mut buf), budget, transient)?
        }
        // The numbering covers every hydrated span, so the lookup succeeds
        // whenever the plan forced nested-set computation (index is `Some`);
        // an absent index/entry is `Nil`.
        GroupKeyResolver::NestedSet(field) => env
            .nested_set
            .and_then(|idx| idx.get(&span.span_id))
            .map(|v| GroupValue::Int(v.value(*field)))
            .unwrap_or(GroupValue::Nil),
        // `traceDuration` is a duration type too → the same Go-duration
        // `stringValue` form as `duration`.
        GroupKeyResolver::TraceDuration => match env.ctx.info {
            Some(i) => charged_str(
                &go_duration_string(i.trace_end_ns.saturating_sub(i.trace_start_ns)),
                budget,
                transient,
            )?,
            None => GroupValue::Nil,
        },
        GroupKeyResolver::RootName => match env.ctx.info {
            Some(i) => charged_str(&i.root_name, budget, transient)?,
            None => GroupValue::Nil,
        },
        GroupKeyResolver::RootServiceName => match env.ctx.info {
            Some(i) => charged_str(&i.root_service, budget, transient)?,
            None => GroupValue::Nil,
        },
        GroupKeyResolver::ChildCount => GroupValue::Int(
            env.ctx
                .child_counts
                .get(&(trace_id, span.span_id))
                .copied()
                .unwrap_or(0) as i64,
        ),
        // Issue #510: the arm follows the STORED type, not which of the
        // two reads happened to answer. The string read returns EVERY row
        // for the key, so it is the authority on both the exact text and
        // the type; the numeric read carries `isNotNull(val_num)`, so it
        // is absent for a non-finite value and its type map is only a
        // fallback.
        GroupKeyResolver::Attr { str_idx, num_idx } => {
            let key = (trace_id, span.span_id);
            let text = env.attrs.select_values[*str_idx]
                .get(&key)
                .map(String::as_str);
            let num = env.attrs.agg_values[*num_idx].get(&key).copied();
            let t = env.attrs.select_types[*str_idx]
                .get(&key)
                .copied()
                .or_else(|| env.attrs.agg_types[*num_idx].get(&key).copied())
                .unwrap_or_default();
            let arm = typed_attr_value(t, num, text);
            charge_transient(budget, transient, arm.payload_bytes())?;
            arm.into_group_value()
        }
    })
}

/// Resolves one span's ACCUMULATED GROUPING tuple (issue #193, widened by
/// #492 item 2, narrowed by the #510 merge): the enclosing spanset's
/// already-resolved `by()` values, in pipeline order, followed by this
/// stage's newly resolved ones.
///
/// **`prefix` is the spanset's `key`, never its `attributes`.** The two
/// lists differ the moment an aggregate stage runs: the aggregate
/// contributes to the response attribute list and must stay out of this
/// tuple, because [`GroupCardinalityCounter`] is ONE per query across
/// every batch and every candidate trace and deduplicates on the tuple.
/// With the aggregate's value in it, two traces whose group keys are
/// identical but whose aggregate values differ stop deduplicating, and
/// `{…} | count() > 0 | by(name)` over traces of three and two spans
/// observes four distinct tuples instead of two — a
/// `422 TraceSearchSeriesCap` no query earned. Covered by
/// `an_aggregate_attribute_is_not_part_of_the_cardinality_tuple`.
///
/// The tuple `Vec` slot is charged into the transient total before
/// allocation, and it is reserved at EXACTLY `prefix.len() + keys.len()` —
/// capacity is part of [`group_tuple_bytes`]'s formula, so a first `by()`
/// (empty prefix) charges the counter precisely what it charged before
/// this change.
fn resolve_group_tuple(
    prefix: &[GroupValue],
    keys: &[PlannedGroupKey],
    env: &EvalEnv<'_>,
    span: &HydratedSpan,
    budget: &mut ByteBudget,
    transient: &mut usize,
) -> Result<GroupTuple, ReadError> {
    let len = prefix.len() + keys.len();
    charge_transient(budget, transient, len * std::mem::size_of::<GroupValue>())?;
    let mut tuple = Vec::with_capacity(len);
    for value in prefix {
        charge_transient(budget, transient, value.payload_bytes())?;
        tuple.push(value.clone());
    }
    for key in keys {
        tuple.push(resolve_group_value(
            &key.resolver,
            env,
            span,
            budget,
            transient,
        )?);
    }
    Ok(tuple)
}

/// One spanset inside the pipeline fold (issue #492 item 2, second member
/// added by the #510 merge) and this spanset's member spans in ascending
/// `(timestamp_ns, span_id)`.
///
/// **`attributes` and `key` are two different lists and must stay two.**
/// `attributes` is the RESPONSE list — every contributor in written
/// order, so a `by()` key AND an aggregate's own value. `key` is the
/// accumulated GROUPING tuple — `by()` values only, and the only thing
/// [`GroupCardinalityCounter`] ever sees. An aggregate contributes to the
/// first and must never reach the second: the counter is one per query
/// across every trace, so an aggregate value in the tuple stops two
/// traces' identical group keys from deduplicating and produces a
/// `422` the query never earned (see [`resolve_group_tuple`]).
///
/// Every stage maps a spanset list to a spanset list and keeps it a
/// PARTITION of a subset of the matched spans — `By` sub-divides each
/// input set, `Coalesce` merges them, an aggregate drops whole sets — so
/// the union of the survivors can never hold a span twice and needs no
/// dedupe.
#[derive(Debug)]
struct PipelineSpanset<'a> {
    attributes: Vec<(String, GroupValue)>,
    key: Vec<GroupValue>,
    spans: Vec<&'a HydratedSpan>,
}

/// The bytes one spanset's `attributes` hold — the SAME `.len()`-payload /
/// capacity-slot formula [`SpanSetGroup::retained_bytes`] uses, because
/// materialisation MOVES this exact allocation into the retained group
/// rather than cloning it. [`by_stage`] charges precisely this sum as it
/// builds the attributes (slots first, then each payload before its
/// clone), so the transient-to-retained hand-over is a re-attribution of
/// bytes already charged, never a second charge.
///
/// `capacity() == len()` by construction: every producer reserves with
/// `Vec::with_capacity` and pushes exactly that many.
fn attributes_bytes(attributes: &[(String, GroupValue)]) -> usize {
    std::mem::size_of_val(attributes)
        + attributes
            .iter()
            .map(|(display, value)| display.len() + value.payload_bytes())
            .sum::<usize>()
}

/// The transient charge for a ONE-spanset list holding `spans` span refs:
/// the enclosing `Vec`'s slot + envelope and the member ref slots. Used by
/// the fold's initial state and by `coalesce()`.
fn single_spanset_bytes(spans: usize) -> usize {
    super::exec::RETAINED_ENTRY_OVERHEAD
        + std::mem::size_of::<PipelineSpanset<'_>>()
        + spans * std::mem::size_of::<&HydratedSpan>()
}

/// One `| by(keys)` stage (issue #193's regroup, made positional by #492
/// item 2): sub-divides EACH input spanset independently by the resolved
/// key tuple, first-appearance order preserved, and EXTENDS both of the
/// spanset's lists — this stage's `(display, value)` pairs are appended
/// to the input's response `attributes`, and its values alone to the
/// input's grouping `key`.
///
/// The distinct-group `422` is charged on the ACCUMULATED tuple (the
/// input's values plus the new ones), so `by(.a) | by(.b)` is bounded by
/// the composite cardinality it actually retains rather than by `.b`'s
/// alone — see docs/api.md §4.2 and the
/// `traceql-nested-by-composite-series-cap` ledger row.
///
/// Returns the output list and the exact byte total charged for it, which
/// the fold releases when the next stage supersedes it.
fn by_stage<'a>(
    keys: &[PlannedGroupKey],
    env: &EvalEnv<'_>,
    input: &[PipelineSpanset<'a>],
    counter: &mut GroupCardinalityCounter,
    budget: &mut ByteBudget,
) -> Result<(Vec<PipelineSpanset<'a>>, usize), ReadError> {
    let mut live = 0usize;
    let mut out: Vec<PipelineSpanset<'a>> = Vec::new();
    for set in input {
        // Upper bound (every span its own group), charged before the
        // partition allocations exactly as the pre-#492 grouping did.
        charge_transient(
            budget,
            &mut live,
            set.spans.len() * PER_SPAN_GROUP_TRANSIENT_BYTES,
        )?;
        let mut order: Vec<GroupTuple> = Vec::new();
        let mut members: Vec<Vec<&'a HydratedSpan>> = Vec::new();
        let mut index: HashMap<GroupTuple, usize> = HashMap::new();
        for span in &set.spans {
            let tuple = resolve_group_tuple(&set.key, keys, env, span, budget, &mut live)?;
            if let Some(&bucket) = index.get(&tuple) {
                members[bucket].push(span);
            } else {
                // Charge + cap-check the distinct tuple (persisted) BEFORE
                // it is retained; then the map's owned key copy (transient).
                counter.observe(&tuple, budget)?;
                charge_transient(budget, &mut live, tuple_string_payload(&tuple))?;
                index.insert(tuple.clone(), order.len());
                members.push(vec![span]);
                order.push(tuple);
            }
        }
        drop(index);
        for (tuple, member_spans) in order.into_iter().zip(members) {
            let attr_len = set.attributes.len() + keys.len();
            charge_transient(
                budget,
                &mut live,
                super::exec::RETAINED_ENTRY_OVERHEAD
                    + std::mem::size_of::<PipelineSpanset<'_>>()
                    + attr_len * std::mem::size_of::<(String, GroupValue)>(),
            )?;
            let mut attributes = Vec::with_capacity(attr_len);
            // The input's response attributes carry forward WHOLE — a
            // `by()` key or an earlier aggregate's value alike. They are
            // NOT zipped against the tuple: the tuple holds `by()` values
            // only, so the two lists stop lining up the moment an
            // aggregate has contributed.
            for (display, value) in &set.attributes {
                charge_transient(budget, &mut live, display.len() + value.payload_bytes())?;
                attributes.push((display.clone(), value.clone()));
            }
            // This stage's own keys, paired with the values it just
            // resolved — the TAIL of the accumulated tuple.
            for (key, value) in keys.iter().zip(&tuple[set.key.len()..]) {
                charge_transient(budget, &mut live, key.display.len() + value.payload_bytes())?;
                attributes.push((key.display.clone(), value.clone()));
            }
            // The tuple itself becomes the output's grouping key: it is
            // already charged, already at exact capacity, and moving it
            // costs nothing.
            out.push(PipelineSpanset {
                attributes,
                key: tuple,
                spans: member_spans,
            });
        }
    }
    Ok((out, live))
}

/// One `| coalesce()` stage (issue #193, positional since #492 item 2):
/// merges whatever SURVIVES so far into a single spanset with BOTH lists
/// cleared — no response attributes and no grouping key — re-sorted
/// ascending `(timestamp_ns, span_id)`, so
/// `by(name) | count() > 2 | coalesce()` merges the three spans that
/// passed, not the four that matched. A `by()` written after it starts
/// its tuple afresh, which is why its cardinality is charged when it
/// partitions rather than at the end of the pipeline.
fn coalesce_stage<'a>(
    input: &[PipelineSpanset<'a>],
    budget: &mut ByteBudget,
) -> Result<(Vec<PipelineSpanset<'a>>, usize), ReadError> {
    let total: usize = input.iter().map(|set| set.spans.len()).sum();
    let mut live = 0usize;
    charge_transient(budget, &mut live, single_spanset_bytes(total))?;
    let mut spans: Vec<&'a HydratedSpan> = Vec::with_capacity(total);
    for set in input {
        spans.extend(set.spans.iter().copied());
    }
    spans.sort_by(|a, b| (a.timestamp_ns, a.span_id).cmp(&(b.timestamp_ns, b.span_id)));
    Ok((
        vec![PipelineSpanset {
            attributes: Vec::new(),
            key: Vec::new(),
            spans,
        }],
        live,
    ))
}

/// Folds one trace's matched spans through the query's ordered pipeline
/// (issue #492 item 2) and returns the surviving spansets; an EMPTY vector
/// means the trace does not match and is dropped, which is how
/// `{...} | by(name) | count() > 3` removes a trace whose groups are all
/// too small.
///
/// Every element is a `[]spanset -> []spanset` step applied in WRITTEN
/// order, and the fold returns early the moment the list becomes empty.
/// The previous stage's list is released as soon as its successor is
/// built, so peak transient is bounded by two stages rather than by the
/// pipeline's length; the surviving list's own charge is added to the
/// caller's `transient` total, which the caller releases once the response
/// has been materialised.
fn run_pipeline<'a>(
    plan: &SearchPlan,
    env: &EvalEnv<'_>,
    matched_spans: &[&'a HydratedSpan],
    attrs: &BatchAttrs,
    counter: &mut GroupCardinalityCounter,
    budget: &mut ByteBudget,
    transient: &mut usize,
) -> Result<Vec<PipelineSpanset<'a>>, ReadError> {
    // The initial state: ONE attribute-free spanset holding every
    // selector-matched span, already ascending `(timestamp_ns, span_id)`.
    let mut live = 0usize;
    charge_transient(budget, &mut live, single_spanset_bytes(matched_spans.len()))?;
    let mut sets = vec![PipelineSpanset {
        attributes: Vec::new(),
        key: Vec::new(),
        spans: matched_spans.to_vec(),
    }];
    for stage in plan.post_stages() {
        match stage {
            SpansetStage::By(keys) => {
                let (out, out_live) = by_stage(keys, env, &sets, counter, budget)?;
                sets = out;
                budget.release(live);
                live = out_live;
            }
            SpansetStage::Coalesce => {
                let (out, out_live) = coalesce_stage(&sets, budget)?;
                sets = out;
                budget.release(live);
                live = out_live;
            }
            SpansetStage::Aggregate(agg) => {
                // Filters the list IN PLACE: the survivors keep the charge
                // they already carry and the dropped sets' bytes stay
                // charged (conservatively) until a later stage or the
                // caller releases the list wholesale — charged once,
                // released once.
                //
                // Issue #510: each SURVIVOR also gains this stage's own
                // typed value as a response attribute, at its written
                // position. It joins `attributes` and NOT `key` — see
                // `PipelineSpanset`.
                let trace_id = env.ctx.trace_id;
                let mut write = 0usize;
                for read in 0..sets.len() {
                    let outcome = aggregate_value(agg, trace_id, &sets[read].spans, attrs);
                    let pass = match &outcome {
                        Some(o) => cmp_f64(agg.cmp, o.scalar, agg.threshold),
                        None => false,
                    };
                    if !pass {
                        continue;
                    }
                    let wire = outcome.expect("a passing aggregate has an outcome").wire;
                    // Rebuilt at EXACT capacity rather than pushed onto:
                    // `SpanSetGroup::retained_bytes` accounts the slot Vec
                    // by `capacity()`, and a push past capacity doubles it,
                    // so the charge and the release would stop agreeing.
                    // The old slots stay charged in `live` and are released
                    // with the rest of the fold's transient total.
                    let attr_len = sets[read].attributes.len() + 1;
                    charge_transient(
                        budget,
                        &mut live,
                        attr_len * std::mem::size_of::<(String, GroupValue)>()
                            + agg.display.len()
                            + wire.payload_bytes(),
                    )?;
                    let mut attributes = Vec::with_capacity(attr_len);
                    attributes.append(&mut sets[read].attributes);
                    attributes.push((agg.display.clone(), wire));
                    sets[read].attributes = attributes;
                    sets.swap(write, read);
                    write += 1;
                }
                sets.truncate(write);
            }
        }
        if sets.is_empty() {
            budget.release(live);
            return Ok(Vec::new());
        }
    }
    *transient += live;
    Ok(sets)
}

/// Materialises the fold's surviving spansets into the response's
/// `groups` layer (issue #193's builder, reduced by #492 item 2 to the
/// final step it always was): `spss` is applied PER spanset on its full
/// membership, so `matched` reports the pre-`spss` count. Every retained
/// byte is charged BEFORE allocation so [`groups_retained_bytes`] releases
/// exactly what was charged.
///
/// The spansets are CONSUMED and each one's `attributes` allocation is
/// MOVED into its group — so a wide group key is never held twice — and
/// its already-charged bytes are subtracted from `transient` instead of
/// being charged again, which re-attributes them from the fold's transient
/// total to the group's retained total without moving `budget.used()`.
/// A spanset's `key` is NOT carried into the response: the grouping tuple
/// is dropped here and its charge stays in `transient` for the caller to
/// release.
fn build_span_set_groups(
    plan: &SearchPlan,
    trace_id: [u8; 16],
    sets: Vec<PipelineSpanset<'_>>,
    env: &EvalEnv<'_>,
    budget: &mut ByteBudget,
    transient: &mut usize,
) -> Result<Vec<SpanSetGroup>, ReadError> {
    // Retained groups: charge the enclosing Vec slot before the reservation.
    budget.charge(
        super::exec::RETAINED_ENTRY_OVERHEAD + sets.len() * std::mem::size_of::<SpanSetGroup>(),
    )?;
    let mut groups = Vec::with_capacity(sets.len());
    for set in sets {
        let take = set.spans.len().min(plan.spss as usize);
        // Per-group container: overhead + span slots. The attribute slots
        // and payloads are the fold's allocation, moved in below.
        budget.charge(
            super::exec::RETAINED_ENTRY_OVERHEAD + take * std::mem::size_of::<SpanSummary>(),
        )?;
        *transient -= attributes_bytes(&set.attributes);
        let mut spans = Vec::with_capacity(take);
        for span in set.spans.iter().take(take) {
            spans.push(build_summary(plan, trace_id, span, env, budget)?);
        }
        groups.push(SpanSetGroup {
            attributes: set.attributes,
            matched: set.spans.len() as u32,
            spans,
        });
    }
    Ok(groups)
}

/// Evaluates one hydrated batch → the exactly-matched traces, each as a
/// response summary. Batch inputs are discarded by the caller afterwards
/// (only these summaries survive into the result heap).
///
/// **Budget contract (code review round 2 — the chosen shape is
/// charge-before-allocate):** every retained/returned byte — the
/// `TraceMatch` base, the summaries buffer at capacity, each summary's
/// name/attribute strings — is charged against `budget` BEFORE it is
/// allocated (`build_summary`); per-trace evaluation intermediates (the
/// matched-id set + aggregate value buffers) are charged while live and
/// released when the trace's summaries are done. A breach mid-batch
/// returns the 422 error class immediately — the partially built output
/// is dropped (the request is failing; its counter dies with it) and no
/// returned `Vec` ever contains uncharged bytes.
pub(crate) fn evaluate_batch(
    plan: &SearchPlan,
    traces: &[TraceSpans],
    attrs: &BatchAttrs,
    counter: &mut GroupCardinalityCounter,
    budget: &mut ByteBudget,
) -> Result<Vec<TraceMatch>, ReadError> {
    let mut out = Vec::new();
    'traces: for trace in traces {
        // The query-time nested-set numbering (issue #181) is computed
        // once per candidate trace, only when the plan uses a nested-set
        // intrinsic, and released the moment `eval_spanset` is done (the
        // aggregate/select phases never read it). On an error path the
        // request budget dies whole (standing convention), so no explicit
        // release is required there.
        let nested_set = if plan.nested_set {
            Some(compute_nested_set(trace, budget)?)
        } else {
            None
        };
        // The per-trace read-only environment — the issue #184 context is
        // borrowed straight from the batch's co-load maps (no per-trace
        // allocation).
        let env = EvalEnv {
            attrs,
            event_sets: &attrs.event_sets,
            nested_set: nested_set.as_ref().map(|c| &c.index),
            ctx: TraceEvalCtx {
                trace_id: trace.trace_id,
                info: attrs.trace_ctx.get(&trace.trace_id),
                child_counts: &attrs.child_counts,
            },
        };
        let mut filter_idx = 0;
        let spanset = eval_spanset(&plan.spanset, plan, &mut filter_idx, trace, &env, budget)?;
        // The nested-set numbering (issue #181) now also feeds `by()`
        // grouping over the nested-set intrinsics (issue #193), so it is
        // held until AFTER the pipeline fold and the response
        // materialisation, and released on every exit path — never before
        // grouping can read it.
        let Some(matched) = spanset else {
            if let Some(charged) = nested_set {
                release_nested_set(charged, budget);
            }
            continue;
        };
        // Post-match transients (per-aggregate `Vec<f64>` buffers + the
        // sorted `&HydratedSpan` ref list below): charged while live
        // (round-2: intermediates are memory too), released once this
        // trace's summaries are built. The matched set itself is already
        // charged (`ChargedSet`).
        let transients = matched.set.len()
            * (std::mem::size_of::<&HydratedSpan>()
                + std::mem::size_of::<f64>()
                + super::exec::RETAINED_ENTRY_OVERHEAD);
        if let Err(e) = budget.charge(transients) {
            release_set(matched, budget);
            return Err(e);
        }
        // `select()` never changes which traces match — response shaping
        // only (plan v2).
        let mut matched_spans: Vec<&HydratedSpan> = trace
            .spans
            .iter()
            .filter(|s| matched.set.contains(&s.span_id))
            .collect();
        matched_spans.sort_by(|a, b| (a.timestamp_ns, a.span_id).cmp(&(b.timestamp_ns, b.span_id)));
        // The public inter-trace sort key stays on the SELECTOR-matched
        // spans, not on what the pipeline leaves behind: the reference's
        // `traces[]` order is insensitive to the pipeline (measured on a
        // two-trace corpus for issue #492 item 2), so a filtering stage
        // must not reorder the result.
        let sort_key = matched_spans
            .iter()
            .map(|s| s.timestamp_ns)
            .max()
            .unwrap_or(i64::MIN);
        // Issue #492 item 2: ONE ordered fold over the pipeline's stages —
        // `by()`, `coalesce()` and the aggregate filters at their written
        // positions. An empty result ends the trace here, which is how a
        // grouped aggregate can remove a trace that the flat one keeps.
        let mut pipeline_transient = 0usize;
        let sets = run_pipeline(
            plan,
            &env,
            &matched_spans,
            attrs,
            counter,
            budget,
            &mut pipeline_transient,
        )?;
        if sets.is_empty() {
            budget.release(pipeline_transient);
            budget.release(transients);
            release_set(matched, budget);
            if let Some(charged) = nested_set {
                release_nested_set(charged, budget);
            }
            continue 'traces;
        }
        // The flat view of the response is the UNION of the surviving
        // spansets (a partition, so no dedupe): with no filtering stage it
        // is exactly the matched set, keeping the default response
        // byte-identical. A single survivor is already sorted, so only a
        // genuinely split list pays for a merge.
        let mut merged_transient = 0usize;
        let merged: Option<Vec<&HydratedSpan>> = if sets.len() == 1 {
            None
        } else {
            let total: usize = sets.iter().map(|set| set.spans.len()).sum();
            charge_transient(
                budget,
                &mut merged_transient,
                super::exec::RETAINED_ENTRY_OVERHEAD + total * std::mem::size_of::<&HydratedSpan>(),
            )?;
            let mut union: Vec<&HydratedSpan> = Vec::with_capacity(total);
            for set in &sets {
                union.extend(set.spans.iter().copied());
            }
            union.sort_by(|a, b| (a.timestamp_ns, a.span_id).cmp(&(b.timestamp_ns, b.span_id)));
            Some(union)
        };
        let surviving: &[&HydratedSpan] = merged.as_deref().unwrap_or(&sets[0].spans);
        let take = surviving.len().min(plan.spss as usize);
        // Charge the match base + the summaries buffer (at its exact
        // capacity) BEFORE allocating it.
        budget.charge(
            std::mem::size_of::<TraceMatch>()
                + super::exec::RETAINED_ENTRY_OVERHEAD
                + take * std::mem::size_of::<SpanSummary>(),
        )?;
        let mut summaries = Vec::with_capacity(take);
        for span in surviving.iter().take(take) {
            summaries.push(build_summary(plan, trace.trace_id, span, &env, budget)?);
        }
        let matched_total = surviving.len() as u32;
        // `groups` is `Some` iff a survivor carries `by()` attributes —
        // only `By` adds them and only `Coalesce` collapses the list to
        // one, so a list longer than one always has them and the rule is
        // total. `None` (a flat query, or a trailing `coalesce()`) keeps
        // the flat response byte-identical.
        let groups = if sets.iter().any(|set| !set.attributes.is_empty()) {
            Some(build_span_set_groups(
                plan,
                trace.trace_id,
                sets,
                &env,
                budget,
                &mut pipeline_transient,
            )?)
        } else {
            drop(sets);
            None
        };
        // `env`'s borrow of `nested_set` has ended (its last use was the
        // response materialisation), so the numbering can now be released.
        if let Some(charged) = nested_set {
            release_nested_set(charged, budget);
        }
        drop(merged);
        budget.release(merged_transient);
        budget.release(pipeline_transient);
        drop(matched_spans);
        budget.release(transients);
        release_set(matched, budget);
        out.push(TraceMatch {
            trace_id: trace.trace_id,
            sort_key,
            matched: matched_total,
            spans: summaries,
            groups,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use pulsus_traceql::parse;

    use super::super::filter::SpanFilterCtx;
    use super::super::search_plan::{SearchCtx, SearchParams, plan_search};
    use super::*;

    fn plan(q: &str) -> SearchPlan {
        plan_search(
            &parse(q).expect("parse"),
            &SearchParams {
                start_ns: 0,
                end_ns: 1_000_000,
                limit: 20,
                spss: 3,
            },
            &SearchCtx {
                filter: SpanFilterCtx {
                    spans_table: "trace_spans",
                    attrs_table: "trace_attrs_idx",
                },
                max_candidates: 100,
                max_series: 1_000,
                distributed: false,
            },
        )
        .expect("plan")
    }

    fn plan_cap(q: &str, max_series: u64) -> SearchPlan {
        plan_search(
            &parse(q).expect("parse"),
            &SearchParams {
                start_ns: 0,
                end_ns: 1_000_000,
                limit: 20,
                spss: 3,
            },
            &SearchCtx {
                filter: SpanFilterCtx {
                    spans_table: "trace_spans",
                    attrs_table: "trace_attrs_idx",
                },
                max_candidates: 100,
                max_series,
                distributed: false,
            },
        )
        .expect("plan")
    }

    fn tid(n: u8) -> [u8; 16] {
        let mut id = [0u8; 16];
        id[15] = n;
        id
    }

    fn sid(n: u8) -> [u8; 8] {
        let mut id = [0u8; 8];
        id[7] = n;
        id
    }

    fn span(n: u8, service: &str, name: &str, ts: i64, dur: i64) -> HydratedSpan {
        HydratedSpan {
            span_id: sid(n),
            parent_id: [0u8; 8],
            service: service.to_string(),
            name: name.to_string(),
            timestamp_ns: ts,
            duration_ns: dur,
            status_code: 0,
            status_message: String::new(),
            kind: 1,
            scope_name: String::new(),
            scope_version: String::new(),
        }
    }

    /// Runs the evaluator under a large test budget — round-2 review:
    /// there is deliberately NO uncharged evaluation path, so the pure
    /// semantic tests fund one instead of bypassing the accounting.
    fn eval(plan: &SearchPlan, traces: &[TraceSpans], attrs: &BatchAttrs) -> Vec<TraceMatch> {
        evaluate_batch(
            plan,
            traces,
            attrs,
            &mut GroupCardinalityCounter::new(u64::MAX),
            &mut ByteBudget::new(usize::MAX),
        )
        .expect("within the test budget")
    }

    /// A probe membership vector of `n` empty key sets — the shape every
    /// probe a projection reads no value from keeps (issue #479).
    fn key_sets(n: usize) -> Vec<ProbeMembership> {
        (0..n)
            .map(|_| ProbeMembership::Keys(HashSet::new()))
            .collect()
    }

    /// Inserts one span key into a probe's membership, whichever shape it
    /// carries.
    fn add_member(m: &mut ProbeMembership, key: SpanKey) {
        match m {
            ProbeMembership::Keys(set) => {
                set.insert(key);
            }
            ProbeMembership::Values(map) => {
                map.entry(key).or_default();
            }
        }
    }

    fn membership(plan: &SearchPlan, entries: &[(usize, [u8; 16], [u8; 8])]) -> BatchAttrs {
        let mut attrs = BatchAttrs {
            membership: key_sets(plan.probes.len()),
            agg_values: vec![HashMap::new(); plan.agg_fields.len()],
            agg_types: vec![HashMap::new(); plan.agg_fields.len()],
            select_values: vec![HashMap::new(); plan.select_attrs.len()],
            select_types: vec![HashMap::new(); plan.select_attrs.len()],
            ..BatchAttrs::default()
        };
        for (probe_idx, trace_id, span_id) in entries {
            add_member(&mut attrs.membership[*probe_idx], (*trace_id, *span_id));
        }
        attrs
    }

    /// The per-trace evaluation environment a direct [`build_summary`]
    /// call needs, borrowing the batch reads exactly as `evaluate_batch`
    /// does.
    fn eval_env<'a>(attrs: &'a BatchAttrs, trace_id: [u8; 16]) -> EvalEnv<'a> {
        EvalEnv {
            attrs,
            event_sets: &attrs.event_sets,
            nested_set: None,
            ctx: TraceEvalCtx {
                trace_id,
                info: attrs.trace_ctx.get(&trace_id),
                child_counts: &attrs.child_counts,
            },
        }
    }

    /// The projected `(key, value)` pairs of one summary, for assertions
    /// that predate [`ProjectedAttribute`]. The value is TYPE-TAGGED
    /// (issue #510), so a `stringValue` rendering of an int can never
    /// compare equal to an `intValue` one.
    fn attr_pairs(s: &SpanSummary) -> Vec<(String, String)> {
        s.attributes
            .iter()
            .map(|a| (a.key().to_string(), typed_token(a.value())))
            .collect()
    }

    /// One typed response value as `<arm>=<value>` — the same shape the
    /// live differentials compare, so a hermetic assertion and a live one
    /// fail on the same difference.
    fn typed_token(v: &GroupValue) -> String {
        match v {
            GroupValue::Str(s) => format!("stringValue={s}"),
            GroupValue::Int(i) => format!("intValue={i}"),
            GroupValue::Double(bits) => format!("doubleValue={}", f64::from_bits(*bits)),
            GroupValue::Bool(b) => format!("boolValue={b}"),
            GroupValue::Nil => "nil".to_string(),
        }
    }

    #[test]
    fn mixed_table_or_is_a_real_disjunction_not_an_intersection() {
        // { duration > 2s || span.foo = "x" } — span 1 matches only by
        // duration, span 2 only by attr, span 3 by neither.
        let p = plan(r#"{ duration > 2s || span.foo = "x" }"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![
                span(1, "svc", "slow", 10, 3_000_000_000),
                span(2, "svc", "attr", 20, 1),
                span(3, "svc", "none", 30, 1),
            ],
        };
        let attrs = membership(&p, &[(0, tid(1), sid(2))]);
        let matches = eval(&p, &[trace], &attrs);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].matched, 2);
        let ids: Vec<[u8; 8]> = matches[0].spans.iter().map(|s| s.span_id).collect();
        assert_eq!(ids, vec![sid(1), sid(2)]);
    }

    /// `{ .a = nil && .b = 1 }` — absence beside a second leaf.
    ///
    /// This shape is the one a leaf/eval pairing bug misreads: `= nil`
    /// consumes a leaf in `eval_expr`, so if `collect` plans none, the
    /// `.b = 1` leaf is read as the absence probe and the whole filter
    /// answers with the wrong predicate rather than failing. The
    /// `debug_assert_eq!` in `eval_filter` cannot fire on a shape no test
    /// EVALUATES, and before this test none did — the assertion was
    /// guarding an empty channel.
    #[test]
    fn absence_beside_a_second_leaf_reads_its_own_probe() {
        let p = plan(r#"{ .a = nil && .b = 1 }"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![
                span(1, "svc", "no-a-yes-b", 10, 1),
                span(2, "svc", "yes-a-yes-b", 20, 1),
                span(3, "svc", "no-a-no-b", 30, 1),
            ],
        };
        // Probe 0 is the `.a` key-existence probe (negated by `= nil`),
        // probe 1 is `.b = 1`. Span 1: no `a`, has `b=1` -> matches.
        // Span 2: has `a` -> excluded. Span 3: no `b=1` -> excluded.
        let attrs = membership(
            &p,
            &[
                (0, tid(1), sid(2)),
                (1, tid(1), sid(1)),
                (1, tid(1), sid(2)),
            ],
        );
        let matches = eval(&p, &[trace], &attrs);
        let ids: Vec<[u8; 8]> = matches[0].spans.iter().map(|s| s.span_id).collect();
        assert_eq!(ids, vec![sid(1)]);
    }

    #[test]
    fn negation_matches_absent_and_different_but_not_equal() {
        // Ratified rule: `!=` matches spans lacking the key and spans
        // with a different value; a span whose index rows satisfy the
        // positive predicate does not match.
        let p = plan(r#"{ .env != "prod" }"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![
                span(1, "svc", "absent", 10, 1),
                span(2, "svc", "equal", 20, 1),
                span(3, "svc", "different", 30, 1),
            ],
        };
        // The probe is the positive `env = 'prod'`: span 2 has it; span 3
        // has env=staging (so no row satisfies the positive predicate —
        // not in the membership set); span 1 has no env at all.
        let attrs = membership(&p, &[(0, tid(1), sid(2))]);
        let matches = eval(&p, &[trace], &attrs);
        let ids: Vec<[u8; 8]> = matches[0].spans.iter().map(|s| s.span_id).collect();
        assert_eq!(ids, vec![sid(1), sid(3)]);
    }

    #[test]
    fn dual_scope_membership_satisfies_an_unscoped_negation_correctly() {
        // A span carrying env=prod at EITHER scope is excluded by
        // `{ .env != "prod" }` — the unscoped probe unions both scopes,
        // so one membership entry suffices to reject the span.
        let p = plan(r#"{ .env != "prod" }"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![span(1, "svc", "resource-scoped", 10, 1)],
        };
        let attrs = membership(&p, &[(0, tid(1), sid(1))]);
        assert!(eval(&p, &[trace], &attrs).is_empty());
    }

    #[test]
    fn cross_spanset_and_requires_both_operands_and_unions_membership() {
        let p = plan(r#"{ span.a = "1" } && { span.b = "2" }"#);
        let both = TraceSpans {
            trace_id: tid(1),
            spans: vec![span(1, "s", "a", 10, 1), span(2, "s", "b", 20, 1)],
        };
        let only_a = TraceSpans {
            trace_id: tid(2),
            spans: vec![span(1, "s", "a", 10, 1)],
        };
        let attrs = membership(
            &p,
            &[
                (0, tid(1), sid(1)),
                (1, tid(1), sid(2)),
                (0, tid(2), sid(1)),
            ],
        );
        let matches = eval(&p, &[both, only_a], &attrs);
        assert_eq!(matches.len(), 1, "only the trace matching both operands");
        assert_eq!(matches[0].trace_id, tid(1));
        assert_eq!(matches[0].matched, 2, "spanset is the union of operands");
    }

    #[test]
    fn cross_spanset_or_is_a_union_of_traces() {
        let p = plan(r#"{ span.a = "1" } || { span.b = "2" }"#);
        let only_a = TraceSpans {
            trace_id: tid(1),
            spans: vec![span(1, "s", "a", 10, 1)],
        };
        let only_b = TraceSpans {
            trace_id: tid(2),
            spans: vec![span(1, "s", "b", 10, 1)],
        };
        let attrs = membership(&p, &[(0, tid(1), sid(1)), (1, tid(2), sid(1))]);
        let matches = eval(&p, &[only_a, only_b], &attrs);
        assert_eq!(matches.len(), 2);
    }

    #[test]
    fn count_aggregate_filters_traces_by_matched_span_count() {
        let p = plan(r#"{ name = "hot" } | count() > 1"#);
        let two = TraceSpans {
            trace_id: tid(1),
            spans: vec![span(1, "s", "hot", 10, 1), span(2, "s", "hot", 20, 1)],
        };
        let one = TraceSpans {
            trace_id: tid(2),
            spans: vec![span(1, "s", "hot", 10, 1)],
        };
        let attrs = membership(&p, &[]);
        let matches = eval(&p, &[two, one], &attrs);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].trace_id, tid(1));
    }

    #[test]
    fn span_id_dedup_upstream_means_count_is_not_inflated_by_replays() {
        // The engine dedups by span_id before evaluation; this pins the
        // evaluator's own set semantics — the same span id counted once.
        let p = plan(r#"{ name = "hot" } | count() >= 2"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![span(1, "s", "hot", 10, 1)],
        };
        let attrs = membership(&p, &[]);
        assert!(eval(&p, &[trace], &attrs).is_empty());
    }

    #[test]
    fn avg_duration_aggregate_compares_in_nanoseconds() {
        let p = plan(r#"{ name = "x" } | avg(duration) > 100ms"#);
        let slow = TraceSpans {
            trace_id: tid(1),
            spans: vec![span(1, "s", "x", 10, 200_000_000)],
        };
        let fast = TraceSpans {
            trace_id: tid(2),
            spans: vec![span(1, "s", "x", 10, 50_000_000)],
        };
        let attrs = membership(&p, &[]);
        let matches = eval(&p, &[slow, fast], &attrs);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].trace_id, tid(1));
    }

    #[test]
    fn attr_aggregate_reads_val_num_for_exactly_the_matched_spans() {
        let p = plan(r#"{ name = "x" } | avg(span.retries) > 1"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![span(1, "s", "x", 10, 1), span(2, "s", "y", 20, 1)],
        };
        let mut attrs = membership(&p, &[]);
        attrs.agg_values[0].insert((tid(1), sid(1)), 3.0);
        // span 2 has retries=0 but does NOT match the filter — it must
        // not drag the average down.
        attrs.agg_values[0].insert((tid(1), sid(2)), 0.0);
        let matches = eval(&p, &[trace], &attrs);
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn a_trace_with_no_aggregatable_values_is_rejected_not_defaulted() {
        let p = plan(r#"{ name = "x" } | avg(span.retries) > 0"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![span(1, "s", "x", 10, 1)],
        };
        let attrs = membership(&p, &[]);
        assert!(eval(&p, &[trace], &attrs).is_empty());
    }

    #[test]
    fn select_projects_physical_and_attr_values_into_summaries() {
        let p = plan(r#"{ name = "x" } | select(resource.service.name, span.foo)"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![span(1, "checkout", "x", 10, 1)],
        };
        let mut attrs = membership(&p, &[]);
        attrs.select_values[0].insert((tid(1), sid(1)), "bar".to_string());
        let matches = eval(&p, &[trace], &attrs);
        // Issue #479: the wire key is the BARE field name, never our
        // scope-prefixed spelling.
        assert_eq!(
            attr_pairs(&matches[0].spans[0]),
            vec![
                (
                    "service.name".to_string(),
                    "stringValue=checkout".to_string()
                ),
                ("foo".to_string(), "stringValue=bar".to_string()),
            ]
        );
    }

    #[test]
    fn spss_caps_summaries_but_matched_reports_the_full_count() {
        let p = plan(r#"{ name = "x" }"#); // spss = 3 from the fixture
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: (1..=5).map(|n| span(n, "s", "x", n as i64, 1)).collect(),
        };
        let attrs = membership(&p, &[]);
        let matches = eval(&p, &[trace], &attrs);
        assert_eq!(matches[0].matched, 5);
        assert_eq!(matches[0].spans.len(), 3);
        assert_eq!(matches[0].spans[0].span_id, sid(1), "ascending start_ns");
    }

    #[test]
    fn sort_key_is_the_max_matched_timestamp_not_the_max_span_timestamp() {
        let p = plan(r#"{ name = "x" }"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![span(1, "s", "x", 10, 1), span(2, "s", "other", 99, 1)],
        };
        let attrs = membership(&p, &[]);
        let matches = eval(&p, &[trace], &attrs);
        assert_eq!(matches[0].sort_key, 10);
    }

    #[test]
    fn match_all_spanset_matches_every_span() {
        let p = plan("{}");
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![span(1, "s", "a", 10, 1), span(2, "s", "b", 20, 1)],
        };
        let attrs = membership(&p, &[]);
        let matches = eval(&p, &[trace], &attrs);
        assert_eq!(matches[0].matched, 2);
    }

    #[test]
    fn repeated_key_conjunction_uses_independent_probes() {
        // { span.a = "1" && span.a = "2" } — satisfiable only by a span
        // whose key has BOTH values indexed (arrays render as one value,
        // so ordinarily empty — the semantics must still be per-probe).
        let p = plan(r#"{ span.a = "1" && span.a = "2" }"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![span(1, "s", "x", 10, 1)],
        };
        let attrs = membership(&p, &[(0, tid(1), sid(1))]); // only "1"
        assert!(eval(&p, std::slice::from_ref(&trace), &attrs).is_empty());
        let attrs = membership(&p, &[(0, tid(1), sid(1)), (1, tid(1), sid(1))]);
        assert_eq!(eval(&p, &[trace], &attrs).len(), 1);
    }

    // -- issue #172: structural relations ---------------------------------

    /// `span()` with an explicit parent (`0` = root).
    fn child_span(n: u8, parent: u8, name: &str, ts: i64) -> HydratedSpan {
        let mut s = span(n, "s", name, ts, 1);
        if parent != 0 {
            s.parent_id = sid(parent);
        }
        s
    }

    /// Root A("a", ts 100) → child B("b", ts 10) → grandchild C("b", ts 20).
    fn family_trace() -> TraceSpans {
        TraceSpans {
            trace_id: tid(1),
            spans: vec![
                child_span(1, 0, "a", 100),
                child_span(2, 1, "b", 10),
                child_span(3, 2, "b", 20),
            ],
        }
    }

    #[test]
    fn child_matches_direct_children_only_with_rhs_only_membership() {
        let p = plan(r#"{ name = "a" } > { name = "b" }"#);
        let attrs = membership(&p, &[]);
        let matches = eval(&p, &[family_trace()], &attrs);
        assert_eq!(matches.len(), 1);
        // RHS-only (adjudicated pin 3): only the direct child B — never
        // the grandchild C, never the LHS span A.
        assert_eq!(matches[0].matched, 1);
        let ids: Vec<[u8; 8]> = matches[0].spans.iter().map(|s| s.span_id).collect();
        assert_eq!(ids, vec![sid(2)]);
        // Threshold-termination soundness (edge case 4): the result's
        // sort key (10) sits BELOW the operands' max timestamp (A at
        // 100) — result ⊆ operand union keeps bound_ts an upper bound.
        assert_eq!(matches[0].sort_key, 10);
    }

    #[test]
    fn descendant_matches_the_grandchild_that_child_does_not() {
        let p = plan(r#"{ name = "a" } >> { name = "b" }"#);
        let attrs = membership(&p, &[]);
        let matches = eval(&p, &[family_trace()], &attrs);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].matched, 2, "B (child) and C (grandchild)");
        let mut ids: Vec<[u8; 8]> = matches[0].spans.iter().map(|s| s.span_id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![sid(2), sid(3)]);
    }

    #[test]
    fn descendant_walk_handles_a_wide_fan_out_and_releases_exactly() {
        // A star (one root, 200 children under a single parent) grows the
        // children-adjacency `Vec` well past MIN_NON_ZERO_CAP=4 first push
        // (4 → 8 → … → 256), exercising the child-slot term the transient
        // envelope now books at 4 slots/span. The direct `rel_descendants`
        // call sidesteps the `spss` cap so the full 200-descendant result
        // is asserted, and `used() == 0` after releasing the operand sets
        // confirms the (bumped) transient charge is released in full.
        let mut spans = vec![child_span(1, 0, "root", 0)];
        for i in 2..=201u8 {
            spans.push(child_span(i, 1, "c", i as i64));
        }
        let trace = TraceSpans {
            trace_id: tid(1),
            spans,
        };
        let mut budget = ByteBudget::new(usize::MAX);
        // Seed = the root; candidates = all 200 children.
        let mut seed = charged_set(1, &mut budget).expect("seed");
        seed.set.insert(sid(1));
        let mut cand = charged_set(200, &mut budget).expect("cand");
        for i in 2..=201u8 {
            cand.set.insert(sid(i));
        }
        let out = rel_descendants(&seed, &cand, &trace, &mut budget).expect("in budget");
        assert_eq!(
            out.set.len(),
            200,
            "every child is a proper descendant of the root"
        );
        release_set(out, &mut budget);
        release_set(seed, &mut budget);
        release_set(cand, &mut budget);
        assert_eq!(budget.used(), 0, "all transients + sets released exactly");
    }

    #[test]
    fn a_span_is_never_its_own_descendant() {
        // A("a") also matches the RHS pattern here, but is a seed, not a
        // discovery — `>>` must not return it.
        let p = plan(r#"{ name = "a" } >> { name = "a" }"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![child_span(1, 0, "a", 10)],
        };
        let attrs = membership(&p, &[]);
        assert!(eval(&p, &[trace], &attrs).is_empty());
    }

    #[test]
    fn sibling_matches_a_distinct_shared_parent_span() {
        // B("b") and D("d") share parent A; `{b} ~ {d}` yields D only.
        let p = plan(r#"{ name = "b" } ~ { name = "d" }"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![
                child_span(1, 0, "a", 100),
                child_span(2, 1, "b", 10),
                child_span(3, 1, "d", 20),
            ],
        };
        let attrs = membership(&p, &[]);
        let matches = eval(&p, &[trace], &attrs);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].matched, 1);
        assert_eq!(matches[0].spans[0].span_id, sid(3), "RHS span only");
    }

    #[test]
    fn sibling_excludes_self_when_it_is_the_only_lhs_match() {
        // One child span matching BOTH sides is not its own sibling…
        let p = plan(r#"{ name = "x" } ~ { name = "x" }"#);
        let lone = TraceSpans {
            trace_id: tid(1),
            spans: vec![child_span(1, 0, "a", 100), child_span(2, 1, "x", 10)],
        };
        let attrs = membership(&p, &[]);
        assert!(eval(&p, &[lone], &attrs).is_empty());
        // …but two same-name spans under one parent are siblings of each
        // other (the count ≥ 2 arm of the distinctness rule).
        let pair = TraceSpans {
            trace_id: tid(2),
            spans: vec![
                child_span(1, 0, "a", 100),
                child_span(2, 1, "x", 10),
                child_span(3, 1, "x", 20),
            ],
        };
        let matches = eval(&p, &[pair], &attrs);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].matched, 2);
    }

    #[test]
    fn zero_parent_root_spans_never_match_sibling() {
        // Adjudicated pin 2: two roots (all-zero parent_id) share no
        // parent — `~` never matches them.
        let p = plan(r#"{ name = "r1" } ~ { name = "r2" }"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![child_span(1, 0, "r1", 10), child_span(2, 0, "r2", 20)],
        };
        let attrs = membership(&p, &[]);
        assert!(eval(&p, &[trace], &attrs).is_empty());
    }

    #[test]
    fn structural_composes_into_the_boolean_algebra() {
        // Structural under && (its result unions with the other operand)
        // and under || (trace-level union) — precedence already puts the
        // structural node under the boolean one (parser pin 1).
        let p = plan(r#"{ name = "a" } && { name = "a" } > { name = "b" }"#);
        let attrs = membership(&p, &[]);
        let matches = eval(&p, &[family_trace()], &attrs);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].matched, 2, "union of {{a}} = A and A>B = B");

        let p = plan(r#"{ name = "a" } > { name = "b" } || { name = "zzz" }"#);
        let attrs = membership(&p, &[]);
        let matches = eval(&p, &[family_trace()], &attrs);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].matched, 1, "the || keeps the structural result");
    }

    #[test]
    fn chained_structural_is_evaluated_left_to_right() {
        // ({a} > {b}) > {b}: the inner result {B} is the outer LHS, so
        // only C (child of B) survives.
        let p = plan(r#"{ name = "a" } > { name = "b" } > { name = "b" }"#);
        let attrs = membership(&p, &[]);
        let matches = eval(&p, &[family_trace()], &attrs);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].matched, 1);
        assert_eq!(matches[0].spans[0].span_id, sid(3));
    }

    #[test]
    fn an_empty_operand_side_yields_no_structural_match() {
        for q in [
            r#"{ name = "nomatch" } > { name = "b" }"#,
            r#"{ name = "a" } > { name = "nomatch" }"#,
            r#"{ name = "nomatch" } >> { name = "also-no" }"#,
        ] {
            let p = plan(q);
            let attrs = membership(&p, &[]);
            let mut budget = ByteBudget::new(usize::MAX);
            let matches = evaluate_batch(
                &p,
                &[family_trace()],
                &attrs,
                &mut GroupCardinalityCounter::new(u64::MAX),
                &mut budget,
            )
            .expect("in budget");
            assert!(matches.is_empty(), "{q}");
            assert_eq!(budget.used(), 0, "{q}: all sets released on the miss path");
        }
    }

    #[test]
    fn a_span_is_never_its_own_descendant_through_a_self_loop() {
        // A self-referential edge (parent_id == span_id) must never make a
        // span its own descendant; the traversal must terminate.
        let p = plan(r#"{ name = "p" } >> { name = "p" }"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![child_span(1, 1, "p", 10)],
        };
        let attrs = membership(&p, &[]);
        assert!(
            eval(&p, &[trace], &attrs).is_empty(),
            "a self-loop span is not its own descendant"
        );
    }

    #[test]
    fn a_two_cycle_yields_each_span_via_the_other() {
        // Codex review (issue #183): a malformed 2-cycle where BOTH spans
        // match both operands. Correct per-pair semantics — each span is a
        // descendant of the OTHER (a different span), so BOTH are yielded;
        // the exclusion is per-pair-self, not a blanket LHS exclusion. The
        // traversal must still terminate.
        let p = plan(r#"{ name = "p" } >> { name = "p" }"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![child_span(1, 2, "p", 10), child_span(2, 1, "p", 20)],
        };
        let attrs = membership(&p, &[]);
        let matches = eval(&p, &[trace], &attrs);
        assert_eq!(matched_ids(&matches), vec![1, 2]);
    }

    #[test]
    fn self_relating_transitive_ops_include_other_lhs_matches() {
        // Codex review #183 Finding 1: parent A → child B, BOTH matching
        // `{x}`. `{x} >> {x}` must return B (B is a genuine descendant of a
        // DIFFERENT `{x}`-match, A), and `{x} << {x}` must return A. The
        // negated forms return the complementary set.
        let a_and_b = || TraceSpans {
            trace_id: tid(1),
            // A (id1) root, B (id2) child of A; both carry span.x = "1".
            spans: vec![child_span(1, 0, "a", 10), child_span(2, 1, "b", 20)],
        };
        let cases: &[(&str, &[u8])] = &[
            (r#"{ span.x = "1" } >> { span.x = "1" }"#, &[2]),
            (r#"{ span.x = "1" } << { span.x = "1" }"#, &[1]),
            (r#"{ span.x = "1" } > { span.x = "1" }"#, &[2]),
            (r#"{ span.x = "1" } < { span.x = "1" }"#, &[1]),
            // Negated complements over the RHS = {A, B}.
            (r#"{ span.x = "1" } !>> { span.x = "1" }"#, &[1]),
            (r#"{ span.x = "1" } !<< { span.x = "1" }"#, &[2]),
        ];
        for (q, expected) in cases {
            let p = plan(q);
            // Both sides are the identical `span.x = "1"` probe, deduped to
            // one membership read holding {A, B}; both filters reference it.
            let attrs = membership(&p, &[(0, tid(1), sid(1)), (0, tid(1), sid(2))]);
            let matches = eval(&p, &[a_and_b()], &attrs);
            assert_eq!(&matched_ids(&matches), expected, "{q}");
        }
    }

    #[test]
    fn a_fabricated_parent_cycle_terminates_and_still_matches() {
        // P(id 1, parent 2) ↔ Q(id 2, parent 1): malformed data must not
        // hang; Q is reachable from P through the (cyclic) child edges.
        let p = plan(r#"{ name = "p" } >> { name = "q" }"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![child_span(1, 2, "p", 10), child_span(2, 1, "q", 20)],
        };
        let attrs = membership(&p, &[]);
        let matches = eval(&p, &[trace], &attrs);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].spans[0].span_id, sid(2));
    }

    #[test]
    fn aggregates_and_select_operate_on_the_structural_result_set() {
        // count() sees ONLY the RHS result (1 span, not the 3-span
        // trace); select projects from the result spans.
        let p = plan(r#"{ name = "a" } > { name = "b" } | count() = 1 | select(name)"#);
        let attrs = membership(&p, &[]);
        let matches = eval(&p, &[family_trace()], &attrs);
        assert_eq!(matches.len(), 1);
        // Issue #479: `select(name)` fills the response's own `name`
        // field and never an `attributes` entry — the reference's rule.
        assert_eq!(matches[0].spans[0].name(), Some("b"));
        assert!(attr_pairs(&matches[0].spans[0]).is_empty());
        let p = plan(r#"{ name = "a" } >> { name = "b" } | count() = 1"#);
        let attrs = membership(&p, &[]);
        assert!(
            eval(&p, &[family_trace()], &attrs).is_empty(),
            "the descendant result has 2 spans, so count() = 1 rejects"
        );
    }

    /// AC7 (hermetic half): after a structural batch the budget holds
    /// byte-for-byte the returned matches' retained bytes — every
    /// structural intermediate (operand sets, edge/queue envelope,
    /// visited set, parent map, result set) was released.
    #[test]
    fn structural_charges_equal_retained_bytes_exactly() {
        for q in [
            r#"{ name = "a" } > { name = "b" }"#,
            r#"{ name = "a" } >> { name = "b" }"#,
            r#"{ name = "b" } ~ { name = "b" }"#,
        ] {
            let p = plan(q);
            let trace = TraceSpans {
                trace_id: tid(1),
                spans: vec![
                    child_span(1, 0, "a", 100),
                    child_span(2, 1, "b", 10),
                    child_span(3, 1, "b", 20),
                ],
            };
            let attrs = membership(&p, &[]);
            let mut budget = ByteBudget::new(usize::MAX);
            let matches = evaluate_batch(
                &p,
                &[trace],
                &attrs,
                &mut GroupCardinalityCounter::new(u64::MAX),
                &mut budget,
            )
            .expect("in budget");
            assert_eq!(matches.len(), 1, "{q}");
            let retained: usize = matches.iter().map(TraceMatch::retained_bytes).sum();
            assert_eq!(
                budget.used(),
                retained,
                "{q}: structural intermediates must all be released"
            );
        }
    }

    /// The structural intermediates are charged BEFORE allocation: a
    /// budget below the descendant walk's envelope breaches inside the
    /// relation evaluation with the 422 class.
    #[test]
    fn structural_intermediates_breach_the_budget_before_allocation() {
        let p = plan(r#"{ name = "a" } >> { name = "b" }"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: (0..2_000)
                .map(|n| {
                    let name = if n == 0 { "a" } else { "b" };
                    child_span((n % 250) as u8, if n == 0 { 0 } else { 1 }, name, n as i64)
                })
                .collect(),
        };
        let attrs = membership(&p, &[]);
        // Room for the two operand sets, not for the walk transients.
        let mut budget = ByteBudget::new(2 * 2_000 * SET_ENTRY_BYTES + 1);
        let err = evaluate_batch(
            &p,
            std::slice::from_ref(&trace),
            &attrs,
            &mut GroupCardinalityCounter::new(u64::MAX),
            &mut budget,
        )
        .expect_err("the descendant envelope pre-charge must breach");
        assert!(
            matches!(
                err,
                ReadError::QueryTooBroad(crate::logql::TooBroadReason::ScanBudgetBytes { .. })
            ),
            "got {err:?}"
        );
    }

    // -- issue #183: `<`/`<<`, negated/union modifiers, field compare -----

    /// AC6 fixture T1: A root, B child of A, C child of B, B2 child of A.
    /// Attributes: A{.k=a,.h=hg} B{.k=b,.g=gg,.h=hg} C{.k=c,.g=gg}
    /// B2{.k=b2,.g=gg,.h=hg}. Span ids: A=1, B=2, C=3, B2=4.
    fn ac6_trace() -> TraceSpans {
        TraceSpans {
            trace_id: tid(1),
            spans: vec![
                child_span(1, 0, "a", 100),
                child_span(2, 1, "b", 10),
                child_span(3, 2, "c", 20),
                child_span(4, 1, "b2", 30),
            ],
        }
    }

    /// Builds the membership reads for the AC6 fixture by matching each
    /// registered probe against T1's `(key, val)` attribute rows.
    fn ac6_membership(p: &SearchPlan) -> BatchAttrs {
        use super::super::filter::ValuePred;
        const ROWS: &[(u8, &str, &str)] = &[
            (1, "k", "a"),
            (1, "h", "hg"),
            (2, "k", "b"),
            (2, "g", "gg"),
            (2, "h", "hg"),
            (3, "k", "c"),
            (3, "g", "gg"),
            (4, "k", "b2"),
            (4, "g", "gg"),
            (4, "h", "hg"),
        ];
        let mut attrs = BatchAttrs {
            membership: key_sets(p.probes_len()),
            agg_values: vec![HashMap::new(); p.agg_fields_len()],
            select_values: vec![HashMap::new(); p.select_attrs_len()],
            ..BatchAttrs::default()
        };
        for (i, probe) in p.probes.iter().enumerate() {
            if let ValuePred::StringEq(val) = &probe.pred {
                for (sb, k, v) in ROWS {
                    if probe.key == *k && val == v {
                        add_member(&mut attrs.membership[i], (tid(1), sid(*sb)));
                    }
                }
            }
        }
        attrs
    }

    /// Plans with a large `spss` so the full result span-set survives the
    /// cap (the AC6 union results reach 4 spans).
    fn plan_wide(q: &str) -> SearchPlan {
        plan_search(
            &parse(q).expect("parse"),
            &SearchParams {
                start_ns: 0,
                end_ns: 1_000_000,
                limit: 20,
                spss: 16,
            },
            &SearchCtx {
                filter: SpanFilterCtx {
                    spans_table: "trace_spans",
                    attrs_table: "trace_attrs_idx",
                },
                max_candidates: 100,
                max_series: 1_000,
                distributed: false,
            },
        )
        .expect("plan")
    }

    fn matched_ids(matches: &[TraceMatch]) -> Vec<u8> {
        if matches.is_empty() {
            return vec![];
        }
        let mut ids: Vec<u8> = matches[0].spans.iter().map(|s| s.span_id[7]).collect();
        ids.sort_unstable();
        ids
    }

    #[test]
    fn ac6_complete_structural_matrix_is_correct_hermetically() {
        // The Plan v4 AC6 matrix (all 15 op×modifier + the 2 empty-LHS
        // edges), evaluated hermetically over the byte-frozen T1 fixture —
        // the same expected span-sets the live `traces_search_explain`
        // gate asserts against ClickHouse.
        let cases: &[(&str, &[u8])] = &[
            // Plain
            (r#"{ .k = "a" } > { .g = "gg" }"#, &[2, 4]),
            (r#"{ .k = "a" } >> { .g = "gg" }"#, &[2, 3, 4]),
            (r#"{ .k = "b" } < { .h = "hg" }"#, &[1]),
            (r#"{ .k = "c" } << { .h = "hg" }"#, &[1, 2]),
            (r#"{ .k = "b" } ~ { .g = "gg" }"#, &[4]),
            // Negated (incl. empty-LHS edges)
            (r#"{ .k = "a" } !> { .g = "gg" }"#, &[3]),
            (r#"{ .k = "none" } !> { .g = "gg" }"#, &[2, 3, 4]),
            (r#"{ .k = "b" } !>> { .g = "gg" }"#, &[2, 4]),
            (r#"{ .k = "c" } !< { .h = "hg" }"#, &[1, 4]),
            (r#"{ .k = "c" } !<< { .h = "hg" }"#, &[4]),
            (r#"{ .k = "none" } !<< { .h = "hg" }"#, &[1, 2, 4]),
            (r#"{ .k = "b" } !~ { .g = "gg" }"#, &[2, 3]),
            // Union
            (r#"{ .k = "a" } &> { .g = "gg" }"#, &[1, 2, 4]),
            (r#"{ .k = "a" } &>> { .g = "gg" }"#, &[1, 2, 3, 4]),
            (r#"{ .k = "b" } &< { .h = "hg" }"#, &[1, 2]),
            (r#"{ .k = "c" } &<< { .h = "hg" }"#, &[1, 2, 3]),
            (r#"{ .k = "b" } &~ { .g = "gg" }"#, &[2, 4]),
        ];
        for (q, expected) in cases {
            let p = plan_wide(q);
            let attrs = ac6_membership(&p);
            let matches = eval(&p, &[ac6_trace()], &attrs);
            assert_eq!(&matched_ids(&matches), expected, "{q}");
        }
    }

    #[test]
    fn negated_and_union_structural_release_every_intermediate() {
        // AC7 (hermetic): the negated/union modifiers charge every
        // intermediate before allocation and release all but the result.
        for q in [
            r#"{ .k = "a" } !> { .g = "gg" }"#,
            r#"{ .k = "none" } !> { .g = "gg" }"#,
            r#"{ .k = "a" } &> { .g = "gg" }"#,
            r#"{ .k = "c" } &<< { .h = "hg" }"#,
        ] {
            let p = plan_wide(q);
            let attrs = ac6_membership(&p);
            let mut budget = ByteBudget::new(usize::MAX);
            let matches = evaluate_batch(
                &p,
                &[ac6_trace()],
                &attrs,
                &mut GroupCardinalityCounter::new(u64::MAX),
                &mut budget,
            )
            .expect("in budget");
            let retained: usize = matches.iter().map(TraceMatch::retained_bytes).sum();
            assert_eq!(budget.used(), retained, "{q}: intermediates all released");
        }
    }

    /// The `!`/truthiness ASYMMETRY, measured at the pinned digest over a
    /// store holding a string `a`: `{ .a }` is a 200 matching nothing,
    /// `{ !.a }` is a 500 `expression (!.a) expected a boolean, but got
    /// TypeString`. Equality against a boolean literal does not match a
    /// string; the `!` OPERATOR demands a boolean.
    ///
    /// Before this test the error path had NO eval coverage at all — the
    /// message existed, the allowlist described it, and nothing ran it.
    #[test]
    fn truthiness_tolerates_a_non_boolean_where_negation_fails_the_query() {
        // `{ .a }` plans as `.a = true`: a membership probe, so a span
        // whose `a` is a string is simply not a member.
        let p = plan(r#"{ .a }"#);
        let trace = || TraceSpans {
            trace_id: tid(1),
            spans: vec![span(1, "s", "a-true", 10, 1), span(2, "s", "a-str", 20, 1)],
        };
        let matches = eval(&p, &[trace()], &membership(&p, &[(0, tid(1), sid(1))]));
        assert_eq!(
            matched_ids(&matches),
            vec![1],
            "no error, no match for the string"
        );

        // `{ !.a }` co-loads the value and fails the WHOLE query on a
        // present non-boolean.
        let p = plan(r#"{ !.a }"#);
        let mut attrs = membership(&p, &[]);
        attrs.select_values[0].insert((tid(1), sid(2)), "hello".to_string());
        let err = evaluate_batch(
            &p,
            &[trace()],
            &attrs,
            &mut GroupCardinalityCounter::new(u64::MAX),
            &mut ByteBudget::new(usize::MAX),
        )
        .expect_err("a present non-boolean under `!` must fail the query");
        match err {
            ReadError::PipelineInvalid { reason } => {
                assert!(reason.contains("expected a boolean"), "{reason}");
                assert!(
                    reason.contains("!"),
                    "the message names the negation: {reason}"
                );
            }
            other => panic!("got {other:?}"),
        }
    }

    /// `{ !.c }` and `{ !.c = 1 }` over BOOLEAN values: the first matches
    /// the `false` span, the second matches nothing and does NOT error —
    /// `Never` still resolves the operand, so the type check survives
    /// while no boolean satisfies the comparison. Both measured.
    #[test]
    fn a_negated_operand_matches_by_value_and_never_matches_a_non_boolean_literal() {
        let trace = || TraceSpans {
            trace_id: tid(1),
            spans: vec![
                span(1, "s", "c-true", 10, 1),
                span(2, "s", "c-false", 20, 1),
            ],
        };
        let p = plan(r#"{ !.c }"#);
        let mut attrs = membership(&p, &[]);
        attrs.select_values[0].insert((tid(1), sid(1)), "true".to_string());
        attrs.select_values[0].insert((tid(1), sid(2)), "false".to_string());
        assert_eq!(matched_ids(&eval(&p, &[trace()], &attrs)), vec![2]);

        let p = plan(r#"{ !.c = 1 }"#);
        let mut attrs = membership(&p, &[]);
        attrs.select_values[0].insert((tid(1), sid(1)), "true".to_string());
        attrs.select_values[0].insert((tid(1), sid(2)), "false".to_string());
        assert!(
            eval(&p, &[trace()], &attrs).is_empty(),
            "no boolean satisfies `!c = 1`, and it must not error either"
        );

        // ...but `Never` must still RESOLVE the operand: a present
        // NON-boolean under `!` fails the whole query even though no
        // boolean could have matched. Measured: `{ !.a = 1 }` is a 500
        // against a store holding a string `a`. Skipping the resolve
        // because "nothing can match anyway" would turn that into a
        // silent empty result.
        let mut attrs = membership(&p, &[]);
        attrs.select_values[0].insert((tid(1), sid(1)), "hello".to_string());
        let err = evaluate_batch(
            &p,
            &[trace()],
            &attrs,
            &mut GroupCardinalityCounter::new(u64::MAX),
            &mut ByteBudget::new(usize::MAX),
        )
        .expect_err("`Never` must still type-check the operand");
        assert!(
            matches!(&err, ReadError::PipelineInvalid { reason } if reason.contains("expected a boolean")),
            "got {err:?}"
        );
    }

    #[test]
    fn field_vs_field_string_equality_matches_same_valued_spans() {
        // `{ .a = .b }` — span 1 has equal string values, span 2 unequal,
        // span 3 is missing `.b` (absent key ⇒ no match).
        let p = plan(r#"{ .a = .b }"#);
        assert_eq!(p.select_attrs_len(), 2);
        assert_eq!(p.agg_fields_len(), 2);
        let mut attrs = membership(&p, &[]);
        attrs.select_values[0].insert((tid(1), sid(1)), "x".to_string());
        attrs.select_values[1].insert((tid(1), sid(1)), "x".to_string());
        attrs.select_values[0].insert((tid(1), sid(2)), "x".to_string());
        attrs.select_values[1].insert((tid(1), sid(2)), "y".to_string());
        attrs.select_values[0].insert((tid(1), sid(3)), "x".to_string());
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![
                span(1, "s", "a", 10, 1),
                span(2, "s", "b", 20, 1),
                span(3, "s", "c", 30, 1),
            ],
        };
        let matches = eval(&p, &[trace], &attrs);
        assert_eq!(matched_ids(&matches), vec![1]);
    }

    #[test]
    fn field_vs_field_ordering_is_numeric_or_lexical_by_type() {
        // `{ .a > .b }` — VERIFIED against grafana/tempo:3.0.2: numeric
        // ordering when both are `val_num`; LEXICAL string ordering when
        // both are strings (Tempo matched `apple < banana`); a cross-type
        // pair never matches (even on coincident text).
        let p = plan(r#"{ .a > .b }"#);
        let mut attrs = membership(&p, &[]);
        // span 1: a=5, b=3 (both numeric) → 5 > 3 matches.
        attrs.select_values[0].insert((tid(1), sid(1)), "5".to_string());
        attrs.select_values[1].insert((tid(1), sid(1)), "3".to_string());
        attrs.agg_values[0].insert((tid(1), sid(1)), 5.0);
        attrs.agg_values[1].insert((tid(1), sid(1)), 3.0);
        // span 2: a="z", b="a" (both string, no val_num) → "z" > "a"
        // lexically matches.
        attrs.select_values[0].insert((tid(1), sid(2)), "z".to_string());
        attrs.select_values[1].insert((tid(1), sid(2)), "a".to_string());
        // span 3: a="5" string vs b=5 numeric (coincident text) → cross-type
        // ⇒ no match even though "5" > ... would be false anyway; the point
        // is the type gate blocks any string-vs-numeric ordering.
        attrs.select_values[0].insert((tid(1), sid(3)), "9".to_string());
        attrs.select_values[1].insert((tid(1), sid(3)), "5".to_string());
        attrs.agg_values[1].insert((tid(1), sid(3)), 5.0); // b numeric, a string
        // span 4: a=1, b=9 (both numeric) → 1 > 9 false.
        attrs.select_values[0].insert((tid(1), sid(4)), "1".to_string());
        attrs.select_values[1].insert((tid(1), sid(4)), "9".to_string());
        attrs.agg_values[0].insert((tid(1), sid(4)), 1.0);
        attrs.agg_values[1].insert((tid(1), sid(4)), 9.0);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: (1..=4).map(|n| span(n, "s", "x", n as i64, 1)).collect(),
        };
        let matches = eval(&p, &[trace], &attrs);
        // span1 (numeric 5>3) and span2 (lexical "z">"a"); NOT span3
        // (cross-type) nor span4 (1>9 false).
        assert_eq!(matched_ids(&matches), vec![1, 2]);
    }

    #[test]
    fn field_vs_field_cross_type_coincident_text_never_matches() {
        // Codex #183 round-2 (the demonstrable bug): a string-typed `.a`
        // and a numeric-typed `.b` with COINCIDENT text "5" must NOT match
        // under `=` (a naive text fallback would wrongly match) AND must
        // NOT match under `!=` either — the Tempo type gate blocks
        // cross-type comparison for every operator (verified live:
        // `{ .a = .b }` and `{ .a != .b }` both returned empty).
        for q in [r#"{ .a = .b }"#, r#"{ .a != .b }"#] {
            let p = plan(q);
            let mut attrs = membership(&p, &[]);
            // a: string "5" (text only, no val_num).
            attrs.select_values[0].insert((tid(1), sid(1)), "5".to_string());
            // b: numeric 5 (val_num set AND the text "5" a real numeric row
            // also carries — the exact adversarial shape).
            attrs.select_values[1].insert((tid(1), sid(1)), "5".to_string());
            attrs.agg_values[1].insert((tid(1), sid(1)), 5.0);
            let trace = TraceSpans {
                trace_id: tid(1),
                spans: vec![span(1, "s", "a", 10, 1)],
            };
            assert!(
                eval(&p, &[trace], &attrs).is_empty(),
                "{q}: cross-type coincident text must never match"
            );
        }
    }

    #[test]
    fn field_vs_field_cross_type_and_absent_key_do_not_match() {
        // Authored coercion rule (value-parity-to-#185): a string LHS vs a
        // numeric-only RHS is no match under `=`; an absent key on either
        // side is no match.
        let p = plan(r#"{ .a = .b }"#);
        let mut attrs = membership(&p, &[]);
        // span 1: a is string-only ("x"), b is numeric-only (val_num=5, no
        // string val) → no common comparable type → no match.
        attrs.select_values[0].insert((tid(1), sid(1)), "x".to_string());
        attrs.agg_values[1].insert((tid(1), sid(1)), 5.0);
        // span 2: a present, b absent → no match (absent key).
        attrs.select_values[0].insert((tid(1), sid(2)), "y".to_string());
        attrs.agg_values[0].insert((tid(1), sid(2)), 1.0);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![span(1, "s", "a", 10, 1), span(2, "s", "b", 20, 1)],
        };
        assert!(
            eval(&p, &[trace], &attrs).is_empty(),
            "cross-type and absent-key operands never match"
        );
    }

    /// Every matched span id across ALL matched traces (issue #351's
    /// fixtures put one span in each trace, so `matched_ids`'s
    /// first-trace view would hide most of them — and the per-spanset
    /// `spss` cap would hide the rest).
    fn all_matched_ids(matches: &[TraceMatch]) -> Vec<u8> {
        let mut ids: Vec<u8> = matches
            .iter()
            .flat_map(|m| m.spans.iter().map(|s| s.span_id[7]))
            .collect();
        ids.sort_unstable();
        ids
    }

    /// Issue #351: `{ .p = .q = .r }` — a comparison in operand position,
    /// evaluated LEFT-associatively as `(.p = .q) = .r`.
    ///
    /// **This is the container fixture, span for span.** Ten spans were
    /// pushed to grafana/tempo:3.0.2 with `p`/`q` numeric and `r` of every
    /// type, and the reference matched exactly two: the one where both
    /// sides are `true` and the one where both are `false`. A non-boolean
    /// or absent `r` matches NOTHING, which is why the right operand
    /// cannot be planned as a truthiness leaf — that would fold "`r` is
    /// `false`" together with "`r` is absent" and wrongly match span 9.
    ///
    /// | span | `p`,`q` | `.r` | reference |
    /// |---|---|---|---|
    /// | 1 | 1,1 (true) | `true` | **match** |
    /// | 2 | 1,1 (true) | `false` | no |
    /// | 3 | 1,2 (false) | `true` | no |
    /// | 4 | 1,2 (false) | `false` | **match** |
    /// | 5 | 1,1 (true) | `"hello"` | no |
    /// | 6 | 1,2 (false) | `"hello"` | no |
    /// | 7 | 1,1 (true) | `7` | no |
    /// | 8 | 1,2 (false) | `7` | no |
    /// | 9 | 1,1 (true) | absent | no |
    /// | 10 | 1,2 (false) | absent | no |
    #[test]
    fn a_comparison_in_operand_position_compares_booleans_and_nothing_else() {
        let p = plan(r#"{ .p = .q = .r }"#);
        // Interning order is the plan's traversal: the nested `(p = q)`
        // first, then the `r` term.
        assert_eq!(p.select_attrs_len(), 3);
        let mut attrs = membership(&p, &[]);
        // One span per trace, exactly as the container fixture was
        // pushed — and it keeps every span visible past the per-spanset
        // `spss` cap.
        let mut set_num = |idx: usize, s: u8, v: f64| {
            attrs.select_values[idx].insert((tid(s), sid(s)), v.to_string());
            attrs.agg_values[idx].insert((tid(s), sid(s)), v);
        };
        // `q` equals `p` on spans 1,2,5,7,9 (the left side is `true`) and
        // differs on 3,4,6,8,10 (`false`).
        for (s, q) in [
            (1u8, 1.0),
            (2, 1.0),
            (3, 2.0),
            (4, 2.0),
            (5, 1.0),
            (6, 2.0),
            (7, 1.0),
            (8, 2.0),
            (9, 1.0),
            (10, 2.0),
        ] {
            set_num(0, s, 1.0);
            set_num(1, s, q);
        }
        let mut set_text = |s: u8, v: &str| {
            attrs.select_values[2].insert((tid(s), sid(s)), v.to_string());
        };
        set_text(1, "true");
        set_text(2, "false");
        set_text(3, "true");
        set_text(4, "false");
        set_text(5, "hello");
        set_text(6, "hello");
        set_text(7, "7");
        set_text(8, "7");
        attrs.agg_values[2].insert((tid(7), sid(7)), 7.0);
        attrs.agg_values[2].insert((tid(8), sid(8)), 7.0);
        // spans 9 and 10: `r` absent entirely.
        let traces: Vec<TraceSpans> = (1..=10)
            .map(|n| TraceSpans {
                trace_id: tid(n),
                spans: vec![span(n, "s", "x", n as i64, 1)],
            })
            .collect();
        assert_eq!(all_matched_ids(&eval(&p, &traces, &attrs)), vec![1, 4]);
    }

    /// Issue #351: `{ !.bt = !.bu }` and its mixed spelling. Measured
    /// against the pinned container over four spans carrying both
    /// booleans: `!bt = !bu` matches exactly where `bt == bu`, and
    /// `!bt = bu` exactly where they differ. Absent operands match
    /// nothing (see [`super::filter::BoolTerm::Not`] for why absent is
    /// no-match here rather than the reference's fetch-dependent 500).
    #[test]
    fn a_negation_on_both_sides_compares_the_two_booleans() {
        for (q, expected) in [
            (r#"{ !.bt = !.bu }"#, vec![1u8, 2]),
            (r#"{ !.bt != !.bu }"#, vec![3, 4]),
            (r#"{ !.bt = .bu }"#, vec![3, 4]),
            (r#"{ .bt = !.bu }"#, vec![3, 4]),
            // An ordering operator over two booleans resolves both
            // operands and matches nothing — `{ !.ct < !.cu }` is a
            // reference 200 with no rows.
            (r#"{ !.bt < !.bu }"#, vec![]),
        ] {
            let p = plan(q);
            let mut attrs = membership(&p, &[]);
            let mut set = |idx: usize, s: u8, v: &str| {
                attrs.select_values[idx].insert((tid(1), sid(s)), v.to_string());
            };
            // 1: true/true · 2: false/false · 3: true/false ·
            // 4: false/true · 5: bt only · 6: neither.
            set(0, 1, "true");
            set(1, 1, "true");
            set(0, 2, "false");
            set(1, 2, "false");
            set(0, 3, "true");
            set(1, 3, "false");
            set(0, 4, "false");
            set(1, 4, "true");
            set(0, 5, "true");
            let trace = TraceSpans {
                trace_id: tid(1),
                spans: (1..=6).map(|n| span(n, "s", "x", n as i64, 1)).collect(),
            };
            assert_eq!(matched_ids(&eval(&p, &[trace], &attrs)), expected, "{q}");
        }
    }

    /// Issue #351: the `!` OPERATOR still demands a boolean inside a
    /// boolean comparison — a PRESENT non-boolean fails the whole query,
    /// as it does for the bare `{ !.a }` leaf and as the reference does
    /// (`{ .p = .q = !.r }` against a string `r` is a 500 `expression
    /// (!.r) expected a boolean, but got TypeString`).
    ///
    /// The failing span's LEFT side is already decided, which is the
    /// point: both operands are resolved before the operator is applied,
    /// so the type failure cannot be skipped by a short circuit.
    #[test]
    fn a_present_non_boolean_under_a_negated_operand_fails_the_whole_query() {
        let p = plan(r#"{ .p = .q = !.r }"#);
        let mut attrs = membership(&p, &[]);
        for idx in 0..2 {
            attrs.select_values[idx].insert((tid(1), sid(1)), "1".to_string());
            attrs.agg_values[idx].insert((tid(1), sid(1)), if idx == 0 { 1.0 } else { 2.0 });
        }
        attrs.select_values[2].insert((tid(1), sid(1)), "hello".to_string());
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![span(1, "s", "x", 10, 1)],
        };
        let err = evaluate_batch(
            &p,
            &[trace],
            &attrs,
            &mut GroupCardinalityCounter::new(u64::MAX),
            &mut ByteBudget::new(usize::MAX),
        )
        .expect_err("a present non-boolean under `!` must fail the query");
        match err {
            ReadError::PipelineInvalid { reason } => {
                assert!(reason.contains("(!.r) expected a boolean"), "{reason}");
            }
            other => panic!("expected a pipeline error, got {other:?}"),
        }
    }

    /// Issue #351: a static-vs-static comparison is folded at plan time,
    /// so it matches every span or none — `{ "x" = "x" }` returns the
    /// whole store against the pinned container and `{ "x" = "y" }`
    /// nothing.
    #[test]
    fn a_static_comparison_matches_every_span_or_none() {
        for (q, expected) in [
            (r#"{ "x" = "x" }"#, vec![1u8, 2]),
            (r#"{ "x" = "y" }"#, vec![]),
            (r#"{ "a" < "b" }"#, vec![1, 2]),
            (r#"{ "b" < "a" }"#, vec![]),
            // Duration and number are one numeric family in the
            // reference (`isNumeric` is `Int | Float | Duration`).
            (r#"{ 1s = 1000000000 }"#, vec![1, 2]),
            (r#"{ 1s > 2s }"#, vec![]),
            (r#"{ ok = ok }"#, vec![1, 2]),
            (r#"{ ok != ok }"#, vec![]),
        ] {
            let p = plan(q);
            let attrs = membership(&p, &[]);
            let trace = TraceSpans {
                trace_id: tid(1),
                spans: vec![span(1, "s", "a", 10, 1), span(2, "s", "b", 20, 1)],
            };
            assert_eq!(matched_ids(&eval(&p, &[trace], &attrs)), expected, "{q}");
        }
    }

    /// Issue #351, owner ruling 2026-08-05: `{ .a = event:name }` matches
    /// when ANY of the span's events matches. The fixture is the one the
    /// reference was probed with, and the expected column is OURS — which
    /// is exactly where the deliberate divergence lives (the reference
    /// consults only the FIRST event; ledger row
    /// `traceql-event-link-operand-any-match`).
    ///
    /// | span | events | `.a` | reference | PulsusDB |
    /// |---|---|---|---|---|
    /// | 1 | evX,evY,evZ | `evZ` (last) | no | **match** |
    /// | 2 | evP,evQ,evR | `evP` (first) | match | **match** |
    /// | 3 | ev1,evM,ev2 | `evM` (middle) | no | **match** |
    /// | 4 | ev7,ev8 | `evNope` | no | no |
    /// | 5 | (none) | `evX` | no | no |
    ///
    /// Span 4 is the negative control that keeps "any" from degenerating
    /// into "always"; span 5 is the empty set.
    #[test]
    fn an_event_set_comparison_matches_any_event_not_the_first() {
        let p = plan(r#"{ .a = event:name }"#);
        assert_eq!(p.event_sets_len(), 1);
        let mut attrs = membership(&p, &[]);
        let mut sets = HashMap::new();
        for (s, events) in [
            (1u8, vec!["evX", "evY", "evZ"]),
            (2, vec!["evP", "evQ", "evR"]),
            (3, vec!["ev1", "evM", "ev2"]),
            (4, vec!["ev7", "ev8"]),
        ] {
            sets.insert(
                (tid(s), sid(s)),
                EventValues::Text(events.into_iter().map(str::to_string).collect()),
            );
        }
        attrs.event_sets.push(sets);
        for (s, a) in [
            (1u8, "evZ"),
            (2, "evP"),
            (3, "evM"),
            (4, "evNope"),
            (5, "evX"),
        ] {
            attrs.select_values[0].insert((tid(s), sid(s)), a.to_string());
        }
        let traces: Vec<TraceSpans> = (1..=5)
            .map(|n| TraceSpans {
                trace_id: tid(n),
                spans: vec![span(n, "s", "x", n as i64, 1)],
            })
            .collect();
        assert_eq!(all_matched_ids(&eval(&p, &traces, &attrs)), vec![1, 2, 3]);
    }

    /// Issue #351: `!=` is the ALL-match operator — a span matches only
    /// when EVERY event fails the equality, so a span with NO events
    /// matches (the same absent-key rule the literal
    /// `{ event:name != "x" }` form already follows) and one whose LAST
    /// event matches does not. The reference's own arithmetic:
    /// `matchCount == elemCount` over the negated element predicate
    /// (`ast_execute.go:535-627` @ v3.0.2).
    #[test]
    fn an_event_set_negation_is_all_match_and_an_empty_set_satisfies_it() {
        let p = plan(r#"{ .a != event:name }"#);
        let mut attrs = membership(&p, &[]);
        let mut sets = HashMap::new();
        sets.insert(
            (tid(1), sid(1)),
            EventValues::Text(vec!["evX".into(), "evY".into(), "evZ".into()]),
        );
        sets.insert(
            (tid(2), sid(2)),
            EventValues::Text(vec!["evP".into(), "evQ".into()]),
        );
        // span 3 has NO event rows at all — the empty set.
        attrs.event_sets.push(sets);
        for (s, a) in [(1u8, "evZ"), (2, "nope"), (3, "anything")] {
            attrs.select_values[0].insert((tid(s), sid(s)), a.to_string());
        }
        let traces: Vec<TraceSpans> = (1..=3)
            .map(|n| TraceSpans {
                trace_id: tid(n),
                spans: vec![span(n, "s", "x", n as i64, 1)],
            })
            .collect();
        // span 1 has a matching event → excluded; span 2 none → kept;
        // span 3 no events → kept.
        assert_eq!(all_matched_ids(&eval(&p, &traces, &attrs)), vec![2, 3]);
    }

    /// Issue #351: the ordering operators are NOT symmetric, so the
    /// set's side is carried through the plan.
    /// `event:timeSinceStart` is the numeric member, read from `val_num`.
    ///
    /// One span, events at 1 ms and 5 ms:
    /// `{ .a < event:timeSinceStart }` asks whether some event is LATER
    /// than `.a`; `{ event:timeSinceStart < .a }` whether some event is
    /// EARLIER. At `.a` = 2 ms both hold; at 9 ms only the second; at
    /// 0.5 ms only the first.
    #[test]
    fn an_event_set_ordering_comparison_respects_the_operand_side() {
        for (q, a_ns, expected) in [
            (r#"{ .a < event:timeSinceStart }"#, 2_000_000.0, vec![1u8]),
            (r#"{ .a < event:timeSinceStart }"#, 9_000_000.0, vec![]),
            (r#"{ event:timeSinceStart < .a }"#, 2_000_000.0, vec![1]),
            (r#"{ event:timeSinceStart < .a }"#, 500_000.0, vec![]),
        ] {
            let p = plan(q);
            let mut attrs = membership(&p, &[]);
            let mut sets = HashMap::new();
            sets.insert(
                (tid(1), sid(1)),
                EventValues::Num(vec![1_000_000.0, 5_000_000.0]),
            );
            attrs.event_sets.push(sets);
            attrs.select_values[0].insert((tid(1), sid(1)), a_ns.to_string());
            attrs.agg_values[0].insert((tid(1), sid(1)), a_ns);
            let trace = TraceSpans {
                trace_id: tid(1),
                spans: vec![span(1, "s", "x", 10, 1)],
            };
            assert_eq!(
                all_matched_ids(&eval(&p, &[trace], &attrs)),
                expected,
                "{q} with .a = {a_ns}"
            );
        }
    }

    /// Issue #351: the two rules that are NOT about the set — an absent
    /// SCALAR is no match for every operator (issue #183's rule,
    /// unchanged), and a cross-TYPE element fails its own comparison, so
    /// it makes `!=` false rather than true.
    #[test]
    fn an_event_set_comparison_keeps_the_absent_and_cross_type_rules() {
        // Absent scalar under `!=`: no match, even though ALL-match over
        // an empty set would otherwise say yes.
        let p = plan(r#"{ .a != event:name }"#);
        let mut attrs = membership(&p, &[]);
        attrs.event_sets.push(HashMap::new());
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![span(1, "s", "x", 10, 1)],
        };
        assert!(
            eval(&p, &[trace], &attrs).is_empty(),
            "an absent scalar never matches"
        );
        // Cross-type: a NUMERIC scalar against text event names. `=`
        // matches nothing, and `!=` does not match either — the element
        // comparison fails, so `matchCount < elemCount`.
        for (q, ctx) in [
            (r#"{ .a = event:name }"#, "eq"),
            (r#"{ .a != event:name }"#, "neq"),
        ] {
            let p = plan(q);
            let mut attrs = membership(&p, &[]);
            let mut sets = HashMap::new();
            sets.insert(
                (tid(1), sid(1)),
                EventValues::Text(vec!["5".to_string(), "other".to_string()]),
            );
            attrs.event_sets.push(sets);
            // `.a` is numeric-typed (val_num set) with the coincident
            // text "5" a real numeric row also carries.
            attrs.select_values[0].insert((tid(1), sid(1)), "5".to_string());
            attrs.agg_values[0].insert((tid(1), sid(1)), 5.0);
            let trace = TraceSpans {
                trace_id: tid(1),
                spans: vec![span(1, "s", "x", 10, 1)],
            };
            assert!(
                eval(&p, &[trace], &attrs).is_empty(),
                "{ctx}: a cross-type element must not match"
            );
        }
    }

    #[test]
    fn field_vs_field_intrinsic_operand_reads_the_physical_column() {
        // `{ duration = .b }` — duration is numeric; span matches when the
        // attribute's val_num equals the hydrated duration.
        let p = plan(r#"{ duration = .b }"#);
        let mut attrs = membership(&p, &[]);
        attrs.select_values[0].insert((tid(1), sid(1)), "100".to_string());
        attrs.agg_values[0].insert((tid(1), sid(1)), 100.0);
        attrs.select_values[0].insert((tid(1), sid(2)), "999".to_string());
        attrs.agg_values[0].insert((tid(1), sid(2)), 999.0);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![span(1, "s", "a", 10, 100), span(2, "s", "b", 20, 100)],
        };
        let matches = eval(&p, &[trace], &attrs);
        assert_eq!(matched_ids(&matches), vec![1], "only span1's dur == .b");
    }

    #[test]
    fn logic_not_inverts_the_inner_predicate_per_span() {
        // `{ !(.env = "prod") }` — matches spans WITHOUT env=prod (absent
        // or different), exactly the ratified negation rule.
        let p = plan(r#"{ !(.env = "prod") }"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![
                span(1, "s", "absent", 10, 1),
                span(2, "s", "prod", 20, 1),
                span(3, "s", "staging", 30, 1),
            ],
        };
        // Only span 2 has env=prod.
        let attrs = membership(&p, &[(0, tid(1), sid(2))]);
        let matches = eval(&p, &[trace], &attrs);
        assert_eq!(matched_ids(&matches), vec![1, 3]);
    }

    #[test]
    fn bare_boolean_statics_match_all_or_none() {
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![span(1, "s", "a", 10, 1), span(2, "s", "b", 20, 1)],
        };
        let p_true = plan("{ true }");
        let m = eval(
            &p_true,
            std::slice::from_ref(&trace),
            &membership(&p_true, &[]),
        );
        assert_eq!(m[0].matched, 2, "{{ true }} matches every span");
        let p_false = plan("{ false }");
        assert!(
            eval(&p_false, &[trace], &membership(&p_false, &[])).is_empty(),
            "{{ false }} matches no span"
        );
    }

    // -- round-2 accounting: charge-before-allocate ----------------------

    /// The exact-equality invariant the heap-evict release depends on:
    /// after a batch, the budget holds byte-for-byte the sum of the
    /// returned matches' `retained_bytes` (intermediates released, every
    /// retained byte charged — no formula drift between the charging
    /// path and the cost model).
    #[test]
    fn charges_equal_retained_bytes_exactly() {
        let p = plan(r#"{ name = "x" } | select(resource.service.name, span.foo)"#);
        let traces = vec![
            TraceSpans {
                trace_id: tid(1),
                spans: vec![
                    span(1, "checkout", "x", 10, 1),
                    span(2, "checkout", "x", 20, 1),
                ],
            },
            TraceSpans {
                trace_id: tid(2),
                spans: vec![span(1, "billing", "x", 30, 1)],
            },
        ];
        let mut attrs = membership(&p, &[]);
        attrs.select_values[0].insert((tid(1), sid(1)), "bar-value".to_string());
        let mut budget = ByteBudget::new(usize::MAX);
        let matches = evaluate_batch(
            &p,
            &traces,
            &attrs,
            &mut GroupCardinalityCounter::new(u64::MAX),
            &mut budget,
        )
        .expect("in budget");
        assert_eq!(matches.len(), 2);
        let retained: usize = matches.iter().map(TraceMatch::retained_bytes).sum();
        assert_eq!(
            budget.used(),
            retained,
            "the budget must hold exactly the returned matches' retained bytes"
        );
    }

    // -- issue #193: by()/coalesce() response reshaping ------------------

    /// `by(resource.service.name)` regroups the FULL matched set into one
    /// spanSet per distinct service (first-appearance order); the flat
    /// `matched`/`spans` stay as the ungrouped view.
    #[test]
    fn by_service_groups_matched_spans_by_distinct_service() {
        let p = plan(r#"{ } | by(resource.service.name)"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![
                span(1, "checkout", "a", 10, 1),
                span(2, "billing", "b", 20, 1),
                span(3, "checkout", "c", 30, 1),
            ],
        };
        let matches = eval(&p, &[trace], &membership(&p, &[]));
        assert_eq!(matches[0].matched, 3, "flat ungrouped view unchanged");
        let groups = matches[0].groups.as_ref().expect("by() active");
        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups[0].attributes,
            vec![(
                "by(resource.service.name)".to_string(),
                GroupValue::Str("checkout".to_string())
            )]
        );
        assert_eq!(groups[0].matched, 2);
        let g0_ids: Vec<[u8; 8]> = groups[0].spans.iter().map(|s| s.span_id).collect();
        assert_eq!(g0_ids, vec![sid(1), sid(3)]);
        assert_eq!(groups[1].matched, 1);
        assert_eq!(
            groups[1].attributes[0].1,
            GroupValue::Str("billing".to_string())
        );
    }

    /// `spss` is applied PER GROUP on the pre-`spss` matched set — a group
    /// with more members than `spss` reports the full `matched` but caps
    /// its `spans`.
    #[test]
    fn by_service_applies_spss_per_group() {
        let p = plan(r#"{ } | by(resource.service.name)"#); // spss = 3
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![
                span(1, "checkout", "a", 10, 1),
                span(2, "checkout", "b", 20, 1),
                span(3, "checkout", "c", 30, 1),
                span(4, "checkout", "d", 40, 1),
            ],
        };
        let matches = eval(&p, &[trace], &membership(&p, &[]));
        let groups = matches[0].groups.as_ref().expect("by() active");
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].matched, 4, "full pre-spss group membership");
        assert_eq!(groups[0].spans.len(), 3, "spss cap PER group");
    }

    /// `by()|coalesce()` collapses the groups back to the flat spanSet
    /// (`groups: None`), while `coalesce()|by()` stays grouped.
    #[test]
    fn coalesce_order_relative_to_by_is_honoured() {
        let trace = || TraceSpans {
            trace_id: tid(1),
            spans: vec![
                span(1, "checkout", "a", 10, 1),
                span(2, "billing", "b", 20, 1),
            ],
        };
        let by_then_coalesce = plan(r#"{ } | by(resource.service.name) | coalesce()"#);
        let m = eval(
            &by_then_coalesce,
            &[trace()],
            &membership(&by_then_coalesce, &[]),
        );
        assert!(
            m[0].groups.is_none(),
            "by()|coalesce() collapses to the flat spanSet"
        );

        let coalesce_then_by = plan(r#"{ } | coalesce() | by(resource.service.name)"#);
        let m = eval(
            &coalesce_then_by,
            &[trace()],
            &membership(&coalesce_then_by, &[]),
        );
        assert_eq!(
            m[0].groups
                .as_ref()
                .expect("coalesce()|by() stays grouped")
                .len(),
            2
        );
    }

    /// The distinct-group `422 TraceSearchSeriesCap` fires on the
    /// by()-produced cardinality across the whole batch — BEFORE a trailing
    /// `coalesce()` collapse (so `by()|coalesce()` cannot bypass it) and
    /// counting groups concentrated in ANY trace, not just winners.
    #[test]
    fn distinct_group_cardinality_cap_fires_including_under_trailing_coalesce() {
        let p = plan_cap(r#"{ } | by(resource.service.name) | coalesce()"#, 2);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![
                span(1, "a", "s", 10, 1),
                span(2, "b", "s", 20, 1),
                span(3, "c", "s", 30, 1),
            ],
        };
        let err = evaluate_batch(
            &p,
            &[trace],
            &membership(&p, &[]),
            &mut GroupCardinalityCounter::new(2),
            &mut ByteBudget::new(usize::MAX),
        )
        .expect_err("3 distinct services over a cap of 2 must 422");
        assert!(
            matches!(
                err,
                ReadError::QueryTooBroad(TooBroadReason::TraceSearchSeriesCap { count: 3, cap: 2 })
            ),
            "got {err:?}"
        );
    }

    /// The cap is a CROSS-BATCH running total: distinct tuples accumulate
    /// across `evaluate_batch` calls on the same counter, so fan-out spread
    /// over multiple batches still trips.
    #[test]
    fn distinct_group_cap_accumulates_across_batches() {
        let p = plan_cap(r#"{ } | by(resource.service.name)"#, 2);
        let mut counter = GroupCardinalityCounter::new(2);
        let mut budget = ByteBudget::new(usize::MAX);
        let b1 = TraceSpans {
            trace_id: tid(1),
            spans: vec![span(1, "a", "s", 10, 1), span(2, "b", "s", 20, 1)],
        };
        evaluate_batch(&p, &[b1], &membership(&p, &[]), &mut counter, &mut budget)
            .expect("two distinct groups fit a cap of 2");
        let b2 = TraceSpans {
            trace_id: tid(2),
            spans: vec![span(1, "c", "s", 30, 1)],
        };
        let err = evaluate_batch(&p, &[b2], &membership(&p, &[]), &mut counter, &mut budget)
            .expect_err("the third distinct group across batches trips the cap");
        assert!(matches!(
            err,
            ReadError::QueryTooBroad(TooBroadReason::TraceSearchSeriesCap { count: 3, cap: 2 })
        ));
    }

    // -- issue #492 item 2: the WRITTEN order of the stages --------------

    /// Corpus C-ORD1 — one trace, four spans: three named `a` (durations
    /// 0.5s / 2s / 3s) and one named `b` (0.4s), ascending in time so the
    /// LAST span is the one a `count() > 2` drops. Fixed literal
    /// timestamps: no test input here is derived from the wall clock.
    fn ord1_trace() -> TraceSpans {
        TraceSpans {
            trace_id: tid(1),
            spans: vec![
                span(1, "grp492", "a", 10, 500_000_000),
                span(2, "grp492", "a", 20, 2_000_000_000),
                span(3, "grp492", "a", 30, 3_000_000_000),
                span(4, "grp492", "b", 40, 400_000_000),
            ],
        }
    }

    /// Corpus C-ORD2 — one trace, four spans with CROSS-CUTTING keys:
    /// `(a, server)`, `(a, client)`, `(b, server)`, `(b, server)`. Neither
    /// key alone separates the spans the other separates, which is what
    /// makes a nested `by()` observable.
    fn ord2_trace() -> TraceSpans {
        let with_kind = |n: u8, name: &str, kind: i8, ts: i64| {
            let mut s = span(n, "grp492b", name, ts, 1);
            s.kind = kind;
            s
        };
        TraceSpans {
            trace_id: tid(1),
            spans: vec![
                with_kind(1, "a", 2, 10),
                with_kind(2, "a", 3, 20),
                with_kind(3, "b", 2, 30),
                with_kind(4, "b", 2, 40),
            ],
        }
    }

    /// One group's attribute KEY sequence, in the order the response
    /// carries it.
    fn group_key_sequence(group: &SpanSetGroup) -> Vec<String> {
        group
            .attributes
            .iter()
            .map(|(display, _)| display.clone())
            .collect()
    }

    /// The group map a spanSet array reduces to: the ordered group-VALUE
    /// tuple -> the member span-id low bytes. Compared as a map, never as
    /// a sequence.
    fn group_map(groups: &[SpanSetGroup]) -> BTreeMap<Vec<String>, Vec<u8>> {
        groups
            .iter()
            .map(|g| {
                let key: Vec<String> = g
                    .attributes
                    .iter()
                    .map(|(_, value)| format!("{value:?}"))
                    .collect();
                let members: Vec<u8> = g.spans.iter().map(|s| s.span_id[7]).collect();
                (key, members)
            })
            .collect()
    }

    /// The flat span-id low bytes of the one returned match.
    fn flat_ids(matches: &[TraceMatch]) -> Vec<u8> {
        matches[0].spans.iter().map(|s| s.span_id[7]).collect()
    }

    /// AC1 — the known pair. `by(name) | count() > 2` filters the GROUPS
    /// (one survives, three spans); the SAME aggregate written BEFORE the
    /// `by(name)` filters the whole matched set and leaves both groups.
    /// The two queries send byte-identical SQL, so this difference is the
    /// whole of item 2.
    ///
    /// The attribute VALUES moved when issue #510 merged: an aggregate
    /// stage contributes its own entry, and its value is the spanset's
    /// own count. Written after the `by(name)` that is the surviving
    /// group's THREE spans; written before it, the whole matched set's
    /// FOUR. Those two numbers are what separates the two orderings on
    /// the wire, so they are asserted rather than the shape alone.
    #[test]
    fn by_then_count_and_count_then_by_differ() {
        let p = plan_wide(r#"{ } | by(name) | count() > 2"#);
        let m = eval(&p, &[ord1_trace()], &membership(&p, &[]));
        assert_eq!(
            m.len(),
            1,
            "the `a` group has three spans, so the trace stays"
        );
        let groups = m[0].groups.as_ref().expect("by() active");
        assert_eq!(
            groups.len(),
            1,
            "count() > 2 AFTER by(name) filters the groups"
        );
        assert_eq!(
            groups[0].attributes,
            vec![
                ("by(name)".to_string(), GroupValue::Str("a".to_string())),
                ("count()".to_string(), GroupValue::Int(3)),
            ],
            "the aggregate contributes at its written position, and its value is this \
             GROUP's size"
        );
        assert_eq!(groups[0].matched, 3);
        assert_eq!(
            flat_ids(&m),
            vec![1, 2, 3],
            "the flat view is the survivors"
        );
        assert_eq!(m[0].matched, 3);

        let p = plan_wide(r#"{ } | count() > 2 | by(name)"#);
        let m = eval(&p, &[ord1_trace()], &membership(&p, &[]));
        let groups = m[0].groups.as_ref().expect("by() active");
        assert_eq!(
            groups.len(),
            2,
            "the same aggregate BEFORE by(name) filters the whole matched set"
        );
        for g in groups {
            assert_eq!(
                g.attributes[0],
                ("count()".to_string(), GroupValue::Int(4)),
                "written first, the aggregate sees the whole matched set — FOUR, not the \
                 group's own size, and it leads the list"
            );
            assert_eq!(g.attributes[1].0, "by(name)");
        }
        assert_eq!(groups[0].matched, 3);
        assert_eq!(groups[1].matched, 1);
        assert_eq!(flat_ids(&m), vec![1, 2, 3, 4]);
        assert_eq!(m[0].matched, 4);
    }

    /// AC2 — the narrowest difference: the two queries differ by ONE
    /// character from AC1's, and the grouped one removes the trace
    /// entirely while the flat one keeps it whole. An empty spanset list
    /// ends the trace; it does not fall back to the matched set.
    #[test]
    fn grouped_aggregate_can_empty_the_trace() {
        let p = plan_wide(r#"{ } | by(name) | count() > 3"#);
        let out = eval(&p, &[ord1_trace()], &membership(&p, &[]));
        assert!(
            out.is_empty(),
            "no group holds more than three spans, so nothing survives"
        );

        let p = plan_wide(r#"{ } | count() > 3 | by(name)"#);
        let out = eval(&p, &[ord1_trace()], &membership(&p, &[]));
        assert_eq!(out.len(), 1, "four matched spans pass count() > 3");
        assert_eq!(out[0].groups.as_ref().expect("by() active").len(), 2);
    }

    /// AC3 — a second `by()` SUB-DIVIDES the current spanSets and appends
    /// its attribute; it does not rebuild from the flat matched set. The
    /// attribute sequence is the WRITTEN order, so the two spellings
    /// carry the same partition under different key orders.
    #[test]
    fn nested_by_stages_accumulate_attributes() {
        let p = plan_wide(r#"{ } | by(name) | by(kind)"#);
        let m = eval(&p, &[ord2_trace()], &membership(&p, &[]));
        let groups = m[0].groups.as_ref().expect("by() active");
        assert_eq!(groups.len(), 3, "(a,server) (a,client) (b,server)");
        for g in groups {
            assert_eq!(
                group_key_sequence(g),
                vec!["by(name)".to_string(), "by(kind)".to_string()]
            );
        }
        assert_eq!(
            group_map(groups),
            BTreeMap::from([
                (
                    vec![r#"Str("a")"#.to_string(), r#"Str("server")"#.to_string()],
                    vec![1]
                ),
                (
                    vec![r#"Str("a")"#.to_string(), r#"Str("client")"#.to_string()],
                    vec![2]
                ),
                (
                    vec![r#"Str("b")"#.to_string(), r#"Str("server")"#.to_string()],
                    vec![3, 4]
                ),
            ])
        );

        let p = plan_wide(r#"{ } | by(kind) | by(name)"#);
        let m = eval(&p, &[ord2_trace()], &membership(&p, &[]));
        let groups = m[0].groups.as_ref().expect("by() active");
        assert_eq!(groups.len(), 3);
        for g in groups {
            assert_eq!(
                group_key_sequence(g),
                vec!["by(kind)".to_string(), "by(name)".to_string()]
            );
        }
        assert_eq!(
            group_map(groups),
            BTreeMap::from([
                (
                    vec![r#"Str("server")"#.to_string(), r#"Str("a")"#.to_string()],
                    vec![1]
                ),
                (
                    vec![r#"Str("client")"#.to_string(), r#"Str("a")"#.to_string()],
                    vec![2]
                ),
                (
                    vec![r#"Str("server")"#.to_string(), r#"Str("b")"#.to_string()],
                    vec![3, 4]
                ),
            ])
        );
    }

    /// AC4 — `coalesce()` merges what SURVIVES at its written position,
    /// not what matched: after a filtering stage it merges three spans,
    /// before one it merges four.
    ///
    /// The two queries also differ in what the merged span set CARRIES,
    /// which moved when issue #510 merged. `coalesce()` clears both of a
    /// spanset's lists, so a `coalesce()` written LAST leaves a span set
    /// with no attributes at all — `groups` is `None` and the response is
    /// the byte-identical flat one. An aggregate written AFTER the
    /// `coalesce()` contributes to the cleared list, so the span set
    /// carries `[count()]` and reaches the encoder as a one-entry
    /// `groups`. Both are the reference's, and the second is what
    /// `coalesce_then_count` pins live.
    #[test]
    fn coalesce_after_a_filter_keeps_only_survivors() {
        let p = plan_wide(r#"{ } | by(name) | count() > 2 | coalesce()"#);
        let m = eval(&p, &[ord1_trace()], &membership(&p, &[]));
        assert!(
            m[0].groups.is_none(),
            "coalesce() written LAST clears the list, so the response is flat"
        );
        assert_eq!(m[0].matched, 3);
        assert_eq!(flat_ids(&m), vec![1, 2, 3]);

        let p = plan_wide(r#"{ } | by(name) | coalesce() | count() > 2"#);
        let m = eval(&p, &[ord1_trace()], &membership(&p, &[]));
        let groups = m[0]
            .groups
            .as_ref()
            .expect("the trailing count() contributes");
        assert_eq!(groups.len(), 1, "one merged span set");
        assert_eq!(
            groups[0].attributes,
            vec![("count()".to_string(), GroupValue::Int(4))],
            "the by(name) contributor was cleared by the coalesce(); the aggregate that \
             follows it counts the MERGED set"
        );
        assert_eq!(m[0].matched, 4, "the coalesced set is what count() sees");
        assert_eq!(flat_ids(&m), vec![1, 2, 3, 4]);
    }

    /// AC9 — `count()` now counts a SPANSET's members rather than the
    /// matched-id set. On the flat spanset those are the same number
    /// because the engine's own `group_hydrated_rows` dedupes hydration
    /// rows by `span_id` BEFORE evaluation, so a replayed row cannot
    /// inflate the count. Built through that real function, not a
    /// hand-deduped fixture.
    #[test]
    fn count_matches_the_deduped_span_set() {
        let row = |span_id: u8| super::super::rows::HydrationRow {
            trace_id: tid(1),
            span_id: sid(span_id),
            parent_id: [0u8; 8],
            service: "grp492".to_string(),
            name: "hot".to_string(),
            timestamp_ns: span_id as i64 * 10,
            duration_ns: 1,
            status_code: 0,
            status_message: String::new(),
            kind: 1,
            scope_name: String::new(),
            scope_version: String::new(),
        };
        let verdict = |q: &str, rows: Vec<super::super::rows::HydrationRow>| {
            let p = plan_wide(q);
            let mut budget = ByteBudget::new(usize::MAX);
            let mut charged = 0usize;
            let (traces, _) =
                super::super::exec::group_hydrated_rows(rows, &mut budget, &mut charged)
                    .expect("in budget");
            assert_eq!(traces[0].spans.len(), 3, "the replay is deduped upstream");
            !eval(&p, &traces, &membership(&p, &[])).is_empty()
        };
        let clean = || vec![row(1), row(2), row(3)];
        let replayed = || vec![row(1), row(1), row(2), row(3)];
        // The threshold sits ON the boundary in both directions, so a
        // count that is one too high or one too low flips a verdict: with
        // three distinct spans `> 2` must match and `> 3` must not, and a
        // replayed row must change neither.
        assert!(verdict(r#"{ } | count() > 2"#, clean()));
        assert!(
            verdict(r#"{ } | count() > 2"#, replayed()),
            "the same three spans with one row replayed give the SAME verdict"
        );
        assert!(!verdict(r#"{ } | count() > 3"#, clean()));
        assert!(
            !verdict(r#"{ } | count() > 3"#, replayed()),
            "a replayed row must not inflate the count past the threshold"
        );
    }

    /// AC10 — the distinct-group cap charges the ACCUMULATED key tuple.
    ///
    /// **Why `max_series = 4` and not another number.** On C-ORD2 the
    /// stage-local accounting this change replaces observes FOUR tuples —
    /// `[a]`, `[b]`, `[server]`, `[client]` — and the composite accounting
    /// observes FIVE — `[a]`, `[b]`, `[a,server]`, `[a,client]`,
    /// `[b,server]`. Four is the ONLY cap that separates them: at 4 the
    /// old rule returns `Ok` and the new rule must refuse. Any cap <= 3
    /// refuses under both rules and any cap >= 5 accepts under both, so a
    /// later edit that "simplifies" this number silently stops the test
    /// discriminating. The second assertion pins the discrimination
    /// itself: at 5 the query must still succeed, so the criterion cannot
    /// pass by refusing everything.
    #[test]
    fn nested_by_charges_the_composite_tuple_not_the_stage_local_one() {
        let q = r#"{ } | by(name) | by(kind)"#;
        let p = plan_cap(q, 4);
        let err = evaluate_batch(
            &p,
            &[ord2_trace()],
            &membership(&p, &[]),
            &mut GroupCardinalityCounter::new(4),
            &mut ByteBudget::new(usize::MAX),
        )
        .expect_err("five composite tuples over a cap of 4 must 422");
        assert!(
            matches!(
                err,
                ReadError::QueryTooBroad(TooBroadReason::TraceSearchSeriesCap { count: 5, cap: 4 })
            ),
            "got {err:?}"
        );

        let p = plan_cap(q, 5);
        evaluate_batch(
            &p,
            &[ord2_trace()],
            &membership(&p, &[]),
            &mut GroupCardinalityCounter::new(5),
            &mut ByteBudget::new(usize::MAX),
        )
        .expect("cap 5 is the non-refusal guard: the same query must succeed");
    }

    /// The trace-ordering control: a filtering stage must not move the
    /// public inter-trace `sort_key`. On C-ORD1 the LATEST span is the one
    /// `count() > 2` drops, so a `sort_key` computed over the survivors
    /// would move — measured on the reference, its `traces[]` order is
    /// insensitive to the pipeline.
    #[test]
    fn pipeline_filtering_does_not_move_the_sort_key() {
        let grouped = plan_wide(r#"{ } | by(name)"#);
        let filtered = plan_wide(r#"{ } | by(name) | count() > 2"#);
        let a = eval(&grouped, &[ord1_trace()], &membership(&grouped, &[]));
        let b = eval(&filtered, &[ord1_trace()], &membership(&filtered, &[]));
        assert_eq!(a[0].sort_key, 40, "max timestamp over the MATCHED spans");
        assert_eq!(b[0].sort_key, a[0].sort_key);
    }

    /// Float `by()` grouping (issue #193 R2-F2): `+0.0` and `-0.0` collapse
    /// into one group, and every NaN into one group — the
    /// `canonical_double_bits` mechanism.
    /// **This assertion was INVERTED on issue #510, deliberately.** It
    /// used to assert `-0.0` and `+0.0` fell into ONE group, "matching the
    /// reference". Measured on the reference — three spans carrying
    /// `-0.0`, `+0.0`, `-0.0` — it answers TWO span sets:
    /// `{"doubleValue":0}` over the `+0.0` span and `{"doubleValue":-0}`
    /// over the two `-0.0` spans. The old assertion is left inverted
    /// rather than deleted so the record shows the behaviour was chosen
    /// and then measured to be wrong.
    ///
    /// The NaN half stays. It is NOT a parity claim in either direction:
    /// the wire format carries one NaN spelling, so no end-to-end test can
    /// observe a payload difference, and folding every NaN into one group
    /// is our own choice recorded as unobservable.
    #[test]
    fn signed_zero_splits_and_every_nan_payload_groups_as_one() {
        let p = plan(r#"{ } | by(span.ratio)"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![
                span(1, "s", "a", 10, 1),
                span(2, "s", "b", 20, 1),
                span(3, "s", "c", 30, 1),
                span(4, "s", "d", 40, 1),
            ],
        };
        let mut attrs = membership(&p, &[]);
        // span.ratio interns into agg_fields[0] (numeric) and
        // select_attrs[0] (string); the stored kind rides the string read.
        for (sid_n, v) in [
            (1u8, 0.0f64),
            (2, -0.0),
            (3, f64::NAN),
            // another NaN payload
            (4, f64::from_bits(0x7ff8_0000_0000_0001)),
        ] {
            attrs.agg_values[0].insert((tid(1), sid(sid_n)), v);
            attrs.agg_types[0].insert((tid(1), sid(sid_n)), StoredType::Float);
        }
        let matches = eval(&p, &[trace], &attrs);
        let groups = matches[0].groups.as_ref().expect("by() active");
        assert_eq!(
            groups.len(),
            3,
            "+0.0 and -0.0 are DIFFERENT groups, plus one NaN group"
        );
        let plus_zero = groups
            .iter()
            .find(|g| g.attributes[0].1 == GroupValue::Double(0.0_f64.to_bits()))
            .expect("+0.0 group");
        assert_eq!(plus_zero.matched, 1);
        assert_eq!(plus_zero.spans[0].span_id, sid(1));
        let minus_zero = groups
            .iter()
            .find(|g| g.attributes[0].1 == GroupValue::Double((-0.0_f64).to_bits()))
            .expect("-0.0 group");
        assert_eq!(minus_zero.matched, 1);
        assert_eq!(minus_zero.spans[0].span_id, sid(2));
        let nan_group = groups
            .iter()
            .find(|g| g.attributes[0].1 == GroupValue::Double(CANONICAL_NAN_BITS))
            .expect("nan group");
        assert_eq!(nan_group.matched, 2);
    }

    /// Go `time.Duration.String()` parity (Tempo renders a `duration`
    /// group value via this exact format).
    #[test]
    fn go_duration_string_matches_the_go_runtime_format() {
        assert_eq!(go_duration_string(0), "0s");
        assert_eq!(go_duration_string(500), "500ns");
        assert_eq!(go_duration_string(1_500), "1.5µs");
        assert_eq!(go_duration_string(1_000_000), "1ms");
        assert_eq!(go_duration_string(1_500_000), "1.5ms");
        assert_eq!(go_duration_string(1_000_000_000), "1s");
        assert_eq!(go_duration_string(1_500_000_000), "1.5s");
        assert_eq!(go_duration_string(90_000_000_000), "1m30s");
        assert_eq!(go_duration_string(3_661_000_000_000), "1h1m1s");
        assert_eq!(go_duration_string(-1_500_000_000), "-1.5s");
    }

    /// Finding (flag-5 answer): `by(status)`/`by(kind)`/`by(duration)`
    /// render by their TraceQL TYPE as `stringValue` keyword / duration
    /// forms — matching Tempo v3.0.2 (NOT numeric enums), under the
    /// `by(<expr>)` group-key attribute NAME.
    #[test]
    fn enum_and_duration_by_keys_render_as_typed_keyword_strings() {
        let p = plan("{} | by(kind)");
        let mut s1 = span(1, "s", "a", 10, 1);
        s1.kind = 2; // server
        let mut s2 = span(2, "s", "b", 20, 1);
        s2.kind = 2;
        let mut s3 = span(3, "s", "c", 30, 1);
        s3.kind = 5; // consumer
        let matches = eval(
            &p,
            &[TraceSpans {
                trace_id: tid(1),
                spans: vec![s1, s2, s3],
            }],
            &membership(&p, &[]),
        );
        let groups = matches[0].groups.as_ref().expect("by() active");
        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups[0].attributes[0],
            (
                "by(kind)".to_string(),
                GroupValue::Str("server".to_string())
            )
        );
        assert_eq!(groups[0].matched, 2);
        assert_eq!(
            groups[1].attributes[0].1,
            GroupValue::Str("consumer".to_string())
        );

        let ps = plan("{} | by(status)");
        let mut e1 = span(1, "s", "a", 10, 1);
        e1.status_code = 2; // error
        let mut e2 = span(2, "s", "b", 20, 1);
        e2.status_code = 1; // ok
        let ms = eval(
            &ps,
            &[TraceSpans {
                trace_id: tid(1),
                spans: vec![e1, e2],
            }],
            &membership(&ps, &[]),
        );
        let gs = ms[0].groups.as_ref().expect("by() active");
        assert_eq!(
            gs[0].attributes[0],
            (
                "by(status)".to_string(),
                GroupValue::Str("error".to_string())
            )
        );
        assert_eq!(gs[1].attributes[0].1, GroupValue::Str("ok".to_string()));

        let pd = plan("{} | by(duration)");
        let md = eval(
            &pd,
            &[TraceSpans {
                trace_id: tid(1),
                spans: vec![span(1, "s", "a", 10, 1_500_000_000)],
            }],
            &membership(&pd, &[]),
        );
        assert_eq!(
            md[0].groups.as_ref().expect("by() active")[0].attributes[0],
            (
                "by(duration)".to_string(),
                GroupValue::Str("1.5s".to_string())
            )
        );
    }

    /// Issue #193 (no silent subset): the nested-set intrinsics are
    /// resolvable group keys — `by(nestedSetParent)` groups on the
    /// per-trace numbering (an `intValue`), not a flat fallback.
    #[test]
    fn nested_set_by_key_groups_on_the_numbering() {
        let p = plan("{} | by(nestedSetParent)");
        assert!(p.nested_set, "the by-key must force nested-set numbering");
        // Root span 1; children 2 and 3 both parented to 1 → same
        // nestedSetParent (root's `left`); the root itself is -1.
        let mut child2 = span(2, "s", "b", 20, 1);
        child2.parent_id = sid(1);
        let mut child3 = span(3, "s", "c", 30, 1);
        child3.parent_id = sid(1);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![span(1, "s", "a", 10, 1), child2, child3],
        };
        let matches = eval(&p, &[trace], &membership(&p, &[]));
        let groups = matches[0].groups.as_ref().expect("by() active");
        // Two distinct nestedSetParent values: -1 (root) and the root's
        // left (its two children share it), all rendered as intValue.
        assert_eq!(groups.len(), 2);
        for g in groups {
            assert!(matches!(g.attributes[0].1, GroupValue::Int(_)));
        }
        let root_group = groups
            .iter()
            .find(|g| g.attributes[0].1 == GroupValue::Int(-1))
            .expect("root has nestedSetParent = -1");
        assert_eq!(root_group.matched, 1);
    }

    /// Issue #193 (no silent subset): a trace-level by-key
    /// (`by(rootServiceName)`) forces its co-load and groups on the
    /// trace-wide value (a `stringValue`).
    #[test]
    fn trace_level_by_key_groups_on_the_coload_value() {
        let p = plan("{} | by(rootServiceName)");
        assert!(
            p.trace_ctx,
            "the by-key must force the trace-context co-load"
        );
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![span(1, "s", "a", 10, 1), span(2, "s", "b", 20, 1)],
        };
        let mut attrs = membership(&p, &[]);
        attrs.trace_ctx.insert(
            tid(1),
            TraceCtxInfo {
                trace_start_ns: 0,
                trace_end_ns: 100,
                root_name: "GET /".to_string(),
                root_service: "gateway".to_string(),
            },
        );
        let matches = eval(&p, &[trace], &attrs);
        let groups = matches[0].groups.as_ref().expect("by() active");
        // Trace-constant → one group holding both spans.
        assert_eq!(groups.len(), 1);
        assert_eq!(
            groups[0].attributes[0],
            (
                "by(rootServiceName)".to_string(),
                GroupValue::Str("gateway".to_string())
            )
        );
        assert_eq!(groups[0].matched, 2);
    }

    /// Issue #193 (no silent subset): span-EVENT / span-LINK intrinsics are
    /// collection-valued per span, so grouping by them is a clean plan
    /// error (`400`), NEVER a silent flat 200.
    #[test]
    fn event_and_link_by_keys_are_a_clean_plan_error_not_a_flat_fallback() {
        for q in [
            "{} | by(event:name)",
            "{} | by(link:spanID)",
            "{} | by(link:traceID)",
        ] {
            let err = plan_search(
                &parse(q).expect("parse"),
                &SearchParams {
                    start_ns: 0,
                    end_ns: 1_000_000,
                    limit: 20,
                    spss: 3,
                },
                &SearchCtx {
                    filter: SpanFilterCtx {
                        spans_table: "trace_spans",
                        attrs_table: "trace_attrs_idx",
                    },
                    max_candidates: 100,
                    max_series: 1_000,
                    distributed: false,
                },
            )
            .expect_err(&format!("{q} must be an unsupported-field plan error"));
            assert!(
                matches!(err, super::super::filter::PlanError::UnsupportedField(_)),
                "{q}: expected UnsupportedField (400), got {err:?}"
            );
        }
    }

    /// AC5/AC8: for a grouped query the budget holds EXACTLY the winners'
    /// `retained_bytes` PLUS the counter's per-distinct-tuple
    /// `group_tuple_bytes` — one shared accounting method, no drift.
    #[test]
    fn grouped_charges_equal_retained_plus_counter_exactly() {
        let p = plan(r#"{ } | by(resource.service.name)"#);
        let traces = vec![
            TraceSpans {
                trace_id: tid(1),
                spans: vec![
                    span(1, "checkout", "a", 10, 1),
                    span(2, "billing", "b", 20, 1),
                ],
            },
            TraceSpans {
                trace_id: tid(2),
                spans: vec![span(1, "checkout", "c", 30, 1)],
            },
        ];
        let mut counter = GroupCardinalityCounter::new(u64::MAX);
        let mut budget = ByteBudget::new(usize::MAX);
        let matches = evaluate_batch(&p, &traces, &membership(&p, &[]), &mut counter, &mut budget)
            .expect("in budget");
        let retained: usize = matches.iter().map(TraceMatch::retained_bytes).sum();
        // Distinct tuples across the batch: "checkout" and "billing".
        let expected_counter: usize = [
            vec![GroupValue::Str("checkout".to_string())],
            vec![GroupValue::Str("billing".to_string())],
        ]
        .iter()
        .map(group_tuple_bytes)
        .sum();
        assert_eq!(
            budget.used(),
            retained + expected_counter,
            "budget == winners' retained_bytes + the counter's group_tuple_bytes"
        );
    }

    /// AC9 (issue #193 R4-F2): the cumulative counter is charged BEFORE it
    /// retains a long-string group-key tuple, ACROSS batches. With
    /// `max_series` well above the distinct-tuple count (so the cardinality
    /// `422` never fires), a `ByteBudget` too small for the second batch's
    /// multi-KB long-string tuple returns the byte-budget error at charge
    /// time — never after materialization. The `ByteBudget::charge`
    /// atomicity (no phantom `used`) guarantees the trip precedes the
    /// insert.
    #[test]
    fn multi_batch_long_string_group_keys_trip_the_cumulative_byte_budget() {
        let p = plan(r#"{ } | by(resource.service.name)"#);
        let big_a = "a".repeat(4096);
        let big_b = "b".repeat(4096);
        let batch1 = || TraceSpans {
            trace_id: tid(1),
            spans: vec![span(1, &big_a, "s", 10, 1)],
        };
        let batch2 = || TraceSpans {
            trace_id: tid(2),
            spans: vec![span(1, &big_b, "s", 20, 1)],
        };
        // Learn the exact total cost of both batches (max_series high — the
        // byte budget, not the cap, is the sole trip condition).
        let mut probe_counter = GroupCardinalityCounter::new(1_000);
        let mut probe = ByteBudget::new(usize::MAX);
        evaluate_batch(
            &p,
            &[batch1()],
            &membership(&p, &[]),
            &mut probe_counter,
            &mut probe,
        )
        .expect("batch 1 fits an unbounded budget");
        evaluate_batch(
            &p,
            &[batch2()],
            &membership(&p, &[]),
            &mut probe_counter,
            &mut probe,
        )
        .expect("batch 2 fits an unbounded budget");
        let full = probe.used();

        // One byte short of the two-batch total: batch 1 fits, batch 2 —
        // whose new long-string tuple pushes past the ceiling — trips.
        let mut counter = GroupCardinalityCounter::new(1_000);
        let mut tight = ByteBudget::new(full - 1);
        evaluate_batch(
            &p,
            &[batch1()],
            &membership(&p, &[]),
            &mut counter,
            &mut tight,
        )
        .expect("batch 1 fits the tight budget");
        let err = evaluate_batch(
            &p,
            &[batch2()],
            &membership(&p, &[]),
            &mut counter,
            &mut tight,
        )
        .expect_err("the second long-string tuple trips the cumulative byte budget");
        assert!(
            matches!(
                err,
                ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes { .. })
            ),
            "the byte budget (not the cardinality cap) is the trip: got {err:?}"
        );
    }

    /// Issue #57 re-audit code-review round 2: the per-summary retained
    /// charge floor — `RETAINED_ENTRY_OVERHEAD + name.len()` — pinned
    /// PER ENTRY, at exact equality, for several name lengths including
    /// zero (so the overhead term and the name term are each
    /// independently load-bearing). The AC-A4 integration gate's fixture
    /// deliberately trips on aggregate name bytes alone (its slack over
    /// the budget exceeds the summed overhead term); THIS unit is what
    /// fails if the 64-byte overhead term is silently dropped from the
    /// charge site.
    #[test]
    fn span_summary_charge_is_exactly_overhead_plus_name_len() {
        // Issue #479: the name is retained only when the query COLLECTS
        // one, so the query that anchors this exact-equality unit is the
        // one that collects it and nothing else. `select(name)` fills the
        // response's own `name` field, so the attribute capacity is still
        // 0 and the arithmetic is unchanged.
        let p = plan("{} | select(name)");
        let attrs = membership(&p, &[]);
        for name_len in [0usize, 1, 8_000] {
            let name = "n".repeat(name_len);
            let s = span(1, "svc", &name, 10, 1);
            let mut budget = ByteBudget::new(usize::MAX);
            let env = eval_env(&attrs, tid(1));
            let summary =
                build_summary(&p, tid(1), &s, &env, &mut budget).expect("within the test budget");
            assert_eq!(
                budget.used(),
                super::super::exec::RETAINED_ENTRY_OVERHEAD + name_len,
                "the summary charge must be EXACTLY overhead + name bytes at L={name_len}"
            );
            assert_eq!(
                summary.heap_payload_bytes(),
                super::super::exec::RETAINED_ENTRY_OVERHEAD + name_len,
                "the release-side cost model must equal the charge at L={name_len}"
            );
        }
    }

    // ---- issue #479: the matched-span projection -------------------------

    /// AC7 — the retained-byte contract holds EXACTLY at every projection
    /// value source, on both release paths, and for an absent value.
    ///
    /// `charged == retained` is a relation, so it is asserted as an
    /// equality at each of the four value classes rather than sampled at
    /// one: a class whose charge site was forgotten shows up as a
    /// mismatch, not as an approximate total.
    #[test]
    fn projected_charges_equal_retained_exactly() {
        // (a) all four value classes, each in its own plan so the charge
        // for that class is the whole delta.
        //
        // hydrated column: `{ status = error }` reads `status_keyword`.
        // probe literal:   `{ span.foo = "bar" }` needs no column at all.
        // fused value:     `{ span.foo =~ "b.*" }` reads the probe's own
        //                  fused `v`.
        // nested-set:      `{ nestedSetLeft > 0 }` reads the per-trace
        //                  query-time numbering.
        let mut hydrated_span = span(1, "svc", "op", 10, 1);
        hydrated_span.status_code = 2;

        // hydrated column
        let p = plan(r#"{ status = error }"#);
        let attrs = membership(&p, &[]);
        let mut budget = ByteBudget::new(usize::MAX);
        let env = eval_env(&attrs, tid(1));
        let s = build_summary(&p, tid(1), &hydrated_span, &env, &mut budget).expect("fits");
        assert_eq!(
            attr_pairs(&s),
            vec![("status".to_string(), "stringValue=error".to_string())]
        );
        assert_eq!(budget.used(), s.heap_payload_bytes());

        // probe literal — the value is the query's own literal, so no
        // column is read and the charge is still exact.
        let p = plan(r#"{ span.foo = "bar" }"#);
        let attrs = membership(&p, &[(0, tid(1), sid(1))]);
        let mut budget = ByteBudget::new(usize::MAX);
        let env = eval_env(&attrs, tid(1));
        let s = build_summary(&p, tid(1), &hydrated_span, &env, &mut budget).expect("fits");
        // The literal path carries no stored type, so it stays a string
        // (issue #510) — the reference matches no span at all for the one
        // shape where that could differ.
        assert_eq!(
            attr_pairs(&s),
            vec![("foo".to_string(), "stringValue=bar".to_string())]
        );
        assert_eq!(budget.used(), s.heap_payload_bytes());

        // fused value
        let p = plan(r#"{ span.foo =~ "b.*" }"#);
        assert!(p.probe_fuses_value(0));
        let mut attrs = membership(&p, &[]);
        attrs.membership[0] = ProbeMembership::Values(HashMap::from([(
            (tid(1), sid(1)),
            ("bar-from-the-row".to_string(), StoredType::String),
        )]));
        let mut budget = ByteBudget::new(usize::MAX);
        let env = eval_env(&attrs, tid(1));
        let s = build_summary(&p, tid(1), &hydrated_span, &env, &mut budget).expect("fits");
        assert_eq!(
            attr_pairs(&s),
            vec![(
                "foo".to_string(),
                "stringValue=bar-from-the-row".to_string()
            )],
            "the fused value is the SPAN's stored value, never the query's pattern"
        );
        assert_eq!(budget.used(), s.heap_payload_bytes());

        // nested-set number — the per-trace numbering is what the leaf
        // filtered on, so `evaluate_batch` supplies it.
        let p = plan(r#"{ nestedSetLeft > 0 }"#);
        let attrs = membership(&p, &[]);
        let mut budget = ByteBudget::new(usize::MAX);
        let matches = evaluate_batch(
            &p,
            &[TraceSpans {
                trace_id: tid(1),
                spans: vec![span(1, "svc", "op", 10, 1)],
            }],
            &attrs,
            &mut GroupCardinalityCounter::new(u64::MAX),
            &mut budget,
        )
        .expect("fits");
        assert_eq!(
            attr_pairs(&matches[0].spans[0]),
            vec![("nestedSetLeft".to_string(), "intValue=1".to_string())]
        );

        // (b) an absent value emits no entry and charges nothing for one.
        let p = plan(r#"{ span.foo =~ "b.*" }"#);
        let attrs = membership(&p, &[]); // the probe matched nothing
        let mut budget = ByteBudget::new(usize::MAX);
        let env = eval_env(&attrs, tid(1));
        let s = build_summary(&p, tid(1), &hydrated_span, &env, &mut budget).expect("fits");
        assert!(s.attributes.is_empty());
        assert_eq!(budget.used(), s.heap_payload_bytes());
        assert_eq!(
            budget.used(),
            super::super::exec::RETAINED_ENTRY_OVERHEAD + std::mem::size_of::<ProjectedAttribute>(),
            "the unused-but-allocated capacity slot is charged; the absent value is not"
        );

        // (f) a summary with NO collected name charges nothing for one.
        let p = plan(r#"{ span.foo = "bar" }"#);
        let attrs = membership(&p, &[(0, tid(1), sid(1))]);
        let mut long = span(1, "svc", &"n".repeat(8192), 10, 1);
        long.status_code = 0;
        let mut budget = ByteBudget::new(usize::MAX);
        let env = eval_env(&attrs, tid(1));
        let s = build_summary(&p, tid(1), &long, &env, &mut budget).expect("fits");
        assert_eq!(s.name(), None);
        assert_eq!(
            budget.used(),
            super::super::exec::RETAINED_ENTRY_OVERHEAD
                + std::mem::size_of::<ProjectedAttribute>()
                + "foo".len()
                + "bar".len(),
            "an 8192-byte span name that the query did not collect costs ZERO"
        );

        // (c) charge-before-clone, and (d) the evict-release identity, on
        // a full `evaluate_batch` pass: the budget's total equals the sum
        // of what the release side would return.
        let p = plan(r#"{ span.foo = "bar" } | select(name)"#);
        let attrs = membership(&p, &[(0, tid(1), sid(1))]);
        let mut budget = ByteBudget::new(usize::MAX);
        let matches = evaluate_batch(
            &p,
            &[TraceSpans {
                trace_id: tid(1),
                spans: vec![span(1, "svc", "op", 10, 1)],
            }],
            &attrs,
            &mut GroupCardinalityCounter::new(u64::MAX),
            &mut budget,
        )
        .expect("fits");
        assert_eq!(matches[0].spans[0].name(), Some("op"));
        assert_eq!(
            budget.used(),
            matches
                .iter()
                .map(TraceMatch::retained_bytes)
                .sum::<usize>(),
            "a heap-evict release returns precisely what was charged"
        );
    }

    /// AC13 — an UNSENT name is neither charged nor retained, and the
    /// summary itself does not grow.
    ///
    /// The two summaries differ in exactly one input — whether the plan
    /// collects `name` — so the delta between their charges is the name
    /// and nothing else. A build that retains the name unconditionally
    /// makes that delta zero and fails here.
    #[test]
    fn an_uncollected_name_is_neither_charged_nor_retained() {
        // Niche optimisation: every `take * size_of::<SpanSummary>()`
        // charge in this module depends on the representation not growing.
        assert_eq!(
            std::mem::size_of::<Option<String>>(),
            std::mem::size_of::<String>(),
            "Option<String> must be niche-optimised, or every size_of<SpanSummary> charge moves"
        );

        let name = "n".repeat(8_192);
        let s = span(1, "svc", &name, 10, 1);

        let collecting = plan("{} | select(name)");
        let not_collecting = plan("{}");
        let attrs = membership(&collecting, &[]);

        let mut with_name = ByteBudget::new(usize::MAX);
        let env = eval_env(&attrs, tid(1));
        let a = build_summary(&collecting, tid(1), &s, &env, &mut with_name).expect("fits");

        let mut without_name = ByteBudget::new(usize::MAX);
        let b = build_summary(&not_collecting, tid(1), &s, &env, &mut without_name).expect("fits");

        assert_eq!(a.name(), Some(name.as_str()));
        assert_eq!(b.name(), None);
        assert_eq!(
            with_name.used() - without_name.used(),
            8_192,
            "the charges differ by EXACTLY the name bytes"
        );
        assert_eq!(
            b.heap_payload_bytes(),
            super::super::exec::RETAINED_ENTRY_OVERHEAD,
            "an uncollected name contributes no term to the retained cost model"
        );
        assert_eq!(a.heap_payload_bytes(), with_name.used());
        assert_eq!(b.heap_payload_bytes(), without_name.used());
    }

    /// Round-2 finding: unused preallocated `select()` capacity is
    /// retained memory — it is charged and counted even when no attribute
    /// value materializes (attributes len 0, capacity 1 here).
    #[test]
    fn unused_select_capacity_is_charged_and_counted() {
        let p = plan(r#"{ name = "x" } | select(span.foo)"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![span(1, "s", "x", 10, 1)],
        };
        let attrs = membership(&p, &[]); // no foo value anywhere
        let mut budget = ByteBudget::new(usize::MAX);
        let matches = evaluate_batch(
            &p,
            &[trace],
            &attrs,
            &mut GroupCardinalityCounter::new(u64::MAX),
            &mut budget,
        )
        .expect("in budget");
        let summary = &matches[0].spans[0];
        assert!(summary.attributes.is_empty());
        assert_eq!(
            summary.attributes.capacity(),
            1,
            "with_capacity(projected attribute groups)"
        );
        assert!(
            summary.heap_payload_bytes()
                >= std::mem::size_of::<ProjectedAttribute>()
                    + super::super::exec::RETAINED_ENTRY_OVERHEAD,
            "the empty-but-allocated attributes buffer is still costed"
        );
        assert_eq!(
            budget.used(),
            matches
                .iter()
                .map(TraceMatch::retained_bytes)
                .sum::<usize>()
        );
    }

    /// Round-4 STRICT ordering proof: the clone probe (recorded at the
    /// exact clone site, after the charge) observably shows whether a
    /// selected-value clone ever happened. Two breach points are
    /// exercised: a budget one byte short of the full cost fails at the
    /// LAST charge — the value charge itself, everything before it
    /// succeeded — and a near-zero budget fails at the first fixed
    /// pre-charge. In BOTH cases zero clones are recorded; the success
    /// probe records exactly one. This proves order, it does not infer
    /// it from counter arithmetic.
    #[test]
    fn over_budget_selected_string_errors_before_cloning_into_the_output() {
        // Issue #479: the filter references `duration`, one of the SEVEN
        // envelope fields that never project, so the ONLY projection is
        // the `select()`ed value — which keeps "exactly one clone on the
        // allowed path, zero on either breach path" an observable of the
        // value clone site and not a sum over several.
        let p = plan(r#"{ duration >= 1ns } | select(span.foo)"#);
        let big = "v".repeat(100_000);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![span(1, "s", "x", 10, 1)],
        };
        let mut attrs = membership(&p, &[]);
        attrs.select_values[0].insert((tid(1), sid(1)), big.clone());

        // Success probe: full cost measured; exactly ONE value clone.
        clone_probe::reset();
        let mut probe = ByteBudget::new(usize::MAX);
        let built = evaluate_batch(
            &p,
            std::slice::from_ref(&trace),
            &attrs,
            &mut GroupCardinalityCounter::new(u64::MAX),
            &mut probe,
        )
        .expect("fits");
        assert_eq!(clone_probe::count(), 1, "the allowed path clones once");
        let full_cost = probe.used();
        assert_eq!(full_cost, built[0].retained_bytes());

        // Breach at the FINAL charge — the value charge (deterministic:
        // charges are a fixed sequence and the value charge is last).
        // The charge fails, so the clone site is never reached.
        clone_probe::reset();
        let mut budget = ByteBudget::new(full_cost - 1);
        let err = evaluate_batch(
            &p,
            std::slice::from_ref(&trace),
            &attrs,
            &mut GroupCardinalityCounter::new(u64::MAX),
            &mut budget,
        )
        .expect_err("one byte short must fail at the value charge");
        assert!(
            matches!(
                err,
                ReadError::QueryTooBroad(crate::logql::TooBroadReason::ScanBudgetBytes { .. })
            ),
            "breach propagates the 422 error class, got {err:?}"
        );
        assert_eq!(
            clone_probe::count(),
            0,
            "the 100 KB value was NEVER cloned on the breach path — the charge \
             observably precedes the clone"
        );

        // Breach at the first fixed pre-charge: still zero clones.
        clone_probe::reset();
        let mut tiny = ByteBudget::new(16);
        evaluate_batch(
            &p,
            std::slice::from_ref(&trace),
            &attrs,
            &mut GroupCardinalityCounter::new(u64::MAX),
            &mut tiny,
        )
        .expect_err("a near-zero budget fails before anything is built");
        assert_eq!(clone_probe::count(), 0);
    }

    // -- round-3 accounting: spanset intermediates -----------------------

    /// The cross-spanset intermediates (per-filter sets) are charged
    /// BEFORE allocation: a budget below one filter-set's upper bound
    /// breaches during intermediate evaluation even though the final
    /// result would have been EMPTY (`&&` with a non-matching rhs) — no
    /// uncharged 2,000-entry set ever exists.
    #[test]
    fn spanset_intermediates_breach_even_when_the_final_result_is_empty() {
        let p = plan(r#"{ name = "m" } && { name = "nomatch" }"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: (0..2_000)
                .map(|n| span((n % 250) as u8, "s", "m", n as i64, 1))
                .collect(),
        };
        let attrs = membership(&p, &[]);
        // One filter set's upper-bound pre-charge is spans × entry cost;
        // allow half of it.
        let mut budget = ByteBudget::new(1_000 * SET_ENTRY_BYTES);
        let err = evaluate_batch(
            &p,
            std::slice::from_ref(&trace),
            &attrs,
            &mut GroupCardinalityCounter::new(u64::MAX),
            &mut budget,
        )
        .expect_err("the first filter's set pre-charge must breach");
        assert!(
            matches!(
                err,
                ReadError::QueryTooBroad(crate::logql::TooBroadReason::ScanBudgetBytes { .. })
            ),
            "got {err:?}"
        );
        // And with room the query completes to its (empty) result with
        // every intermediate released.
        let mut roomy = ByteBudget::new(usize::MAX);
        let matches = evaluate_batch(
            &p,
            &[trace],
            &attrs,
            &mut GroupCardinalityCounter::new(u64::MAX),
            &mut roomy,
        )
        .expect("in budget");
        assert!(matches.is_empty());
        assert_eq!(roomy.used(), 0, "all intermediate sets were released");
    }

    /// The `||` union set is charged before it is built — a budget that
    /// fits both operand sets but not the third (union) set breaches at
    /// the union pre-charge; with room, the peak is three live sets and
    /// everything not retained is released.
    #[test]
    fn cross_spanset_union_charges_the_third_set_before_building_it() {
        let p = plan(r#"{ name = "m" } || { name = "m2" }"#);
        let spans: Vec<HydratedSpan> = (0..1_000)
            .map(|n| {
                span(
                    (n % 250) as u8,
                    "s",
                    if n % 2 == 0 { "m" } else { "m2" },
                    n as i64,
                    1,
                )
            })
            .collect();
        let trace = TraceSpans {
            trace_id: tid(1),
            spans,
        };
        let attrs = membership(&p, &[]);
        // Every set (filter results AND the union) pre-charges the
        // 1,000-span upper bound; 2.5 sets of room means the union's
        // pre-charge is the one that breaches.
        let mut budget = ByteBudget::new(2_500 * SET_ENTRY_BYTES);
        let err = evaluate_batch(
            &p,
            std::slice::from_ref(&trace),
            &attrs,
            &mut GroupCardinalityCounter::new(u64::MAX),
            &mut budget,
        )
        .expect_err("the union set's pre-charge must breach");
        assert!(
            matches!(
                err,
                ReadError::QueryTooBroad(crate::logql::TooBroadReason::ScanBudgetBytes { .. })
            ),
            "got {err:?}"
        );
        // No release assertions on the error path — round-4 adjudication:
        // the request-scoped budget is dropped whole on error (see
        // `ByteBudget`'s type docs); error-path releases are not required
        // for soundness.
        // With room: completes, and the budget holds exactly the
        // returned matches (all sets released after the merge).
        let mut roomy = ByteBudget::new(usize::MAX);
        let matches = evaluate_batch(
            &p,
            &[trace],
            &attrs,
            &mut GroupCardinalityCounter::new(u64::MAX),
            &mut roomy,
        )
        .expect("in budget");
        assert_eq!(matches.len(), 1);
        assert_eq!(
            roomy.used(),
            matches
                .iter()
                .map(TraceMatch::retained_bytes)
                .sum::<usize>(),
            "operand and union intermediates were all released"
        );
    }

    // -- issue #181: nested-set structural intrinsics ---------------------

    /// The observed Tempo v3.0.2 aa tree under our `(timestamp_ns,
    /// span_id)` sibling order: root R with children A then B (A sorts
    /// first), B with grandchild C. Expected numbering
    /// `R(1,8,-1) A(2,3,1) B(4,7,1) C(5,6,4)`.
    fn nested_set_aa() -> TraceSpans {
        TraceSpans {
            trace_id: tid(1),
            spans: vec![
                child_span(1, 0, "R", 100),
                child_span(2, 1, "A", 10),
                child_span(3, 1, "B", 20),
                child_span(4, 3, "C", 30),
            ],
        }
    }

    /// A `depth`-span linear chain (span `i+1` is the child of span `i`) —
    /// span ids carry a 4-byte counter so a genuinely deep (10 000) chain
    /// has distinct ids; recursion would overflow the stack here.
    fn deep_chain(depth: usize) -> TraceSpans {
        let mut spans = Vec::with_capacity(depth);
        for i in 0..depth {
            let mut span_id = [0u8; 8];
            span_id[..4].copy_from_slice(&((i as u32) + 1).to_be_bytes());
            let mut parent_id = [0u8; 8];
            if i > 0 {
                parent_id[..4].copy_from_slice(&(i as u32).to_be_bytes());
            }
            spans.push(HydratedSpan {
                span_id,
                parent_id,
                service: "s".to_string(),
                name: "n".to_string(),
                timestamp_ns: i as i64,
                duration_ns: 1,
                status_code: 0,
                status_message: String::new(),
                kind: 1,
                scope_name: String::new(),
                scope_version: String::new(),
            });
        }
        TraceSpans {
            trace_id: tid(1),
            spans,
        }
    }

    /// Total coverage + the contiguous `1..=2·spans` permutation — the
    /// invariants that hold even for a malformed cycle.
    fn assert_contiguous_and_total(trace: &TraceSpans, idx: &NestedSetIndex) {
        let n = trace.spans.len();
        assert_eq!(idx.len(), n, "every span is numbered (total coverage)");
        let mut nums: Vec<i64> = idx.values().flat_map(|v| [v.left, v.right]).collect();
        nums.sort_unstable();
        assert_eq!(
            nums,
            (1..=2 * n as i64).collect::<Vec<_>>(),
            "left ∪ right is the contiguous 1..=2n permutation"
        );
    }

    /// The full nested-set invariants for a well-formed (acyclic) forest.
    fn assert_tree_invariants(trace: &TraceSpans, idx: &NestedSetIndex) {
        assert_contiguous_and_total(trace, idx);
        let span_ids: HashSet<[u8; 8]> = trace.spans.iter().map(|s| s.span_id).collect();
        let has_child: HashSet<[u8; 8]> = trace
            .spans
            .iter()
            .filter(|s| s.parent_id != ZERO_ID && span_ids.contains(&s.parent_id))
            .map(|s| s.parent_id)
            .collect();
        for s in &trace.spans {
            let v = idx[&s.span_id];
            assert!(v.left < v.right, "containment: left < right");
            if s.parent_id == ZERO_ID || !span_ids.contains(&s.parent_id) {
                assert_eq!(v.parent, -1, "root/orphan parent sentinel");
            } else {
                let p = idx[&s.parent_id];
                assert_eq!(v.parent, p.left, "non-root parent == parent.left");
                assert!(
                    p.left < v.left && v.right < p.right,
                    "ancestor strictly contains descendant"
                );
            }
            if !has_child.contains(&s.span_id) {
                assert_eq!(v.right, v.left + 1, "a leaf's right == left + 1");
            }
        }
    }

    #[test]
    fn nested_set_numbering_matches_the_observed_tempo_values() {
        let trace = nested_set_aa();
        let mut budget = ByteBudget::new(usize::MAX);
        let charged = compute_nested_set(&trace, &mut budget).expect("in budget");
        let get = |n: u8| charged.index[&sid(n)];
        let r = get(1);
        assert_eq!((r.left, r.right, r.parent), (1, 8, -1), "R");
        let a = get(2);
        assert_eq!((a.left, a.right, a.parent), (2, 3, 1), "A");
        let b = get(3);
        assert_eq!((b.left, b.right, b.parent), (4, 7, 1), "B");
        let c = get(4);
        assert_eq!((c.left, c.right, c.parent), (5, 6, 4), "C");
        release_nested_set(charged, &mut budget);
        assert_eq!(budget.used(), 0, "index released");
    }

    #[test]
    fn nested_set_invariants_hold_on_multi_child_and_deep_chain_trees() {
        // A 10 000-span chain proves the numbering is iterative (a
        // recursive DFS would overflow the stack).
        for trace in [nested_set_aa(), deep_chain(10_000)] {
            let mut budget = ByteBudget::new(usize::MAX);
            let charged = compute_nested_set(&trace, &mut budget).expect("in budget");
            assert_tree_invariants(&trace, &charged.index);
            release_nested_set(charged, &mut budget);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn nested_set_numbering_handles_a_wide_fan_out_and_releases_exactly() {
        // A star (one root, 200 children) grows the child-adjacency `Vec`
        // well past the MIN_NON_ZERO_CAP=4 first push (4 → 8 → … → 256),
        // exercising the term the transient envelope books at 4 slots/span.
        // The exact post-release `used() == 0` confirms the (bumped)
        // transient charge is released in full.
        let mut spans = vec![child_span(1, 0, "root", 0)];
        for i in 2..=201u8 {
            spans.push(child_span(i, 1, "c", i as i64));
        }
        let trace = TraceSpans {
            trace_id: tid(1),
            spans,
        };
        let mut budget = ByteBudget::new(usize::MAX);
        let charged = compute_nested_set(&trace, &mut budget).expect("in budget");
        assert_tree_invariants(&trace, &charged.index);
        let root = charged.index[&sid(1)];
        assert_eq!(
            (root.left, root.right, root.parent),
            (1, 402, -1),
            "root spans 1..=2·201"
        );
        release_nested_set(charged, &mut budget);
        assert_eq!(budget.used(), 0, "index + all transients released exactly");
    }

    #[test]
    fn nested_set_numbering_terminates_and_covers_a_parent_cycle() {
        // P(id 1, parent 2) ↔ Q(id 2, parent 1): malformed, no root. The
        // promotion-to-root pass numbers both, contiguously, and the walk
        // terminates.
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![child_span(1, 2, "p", 10), child_span(2, 1, "q", 20)],
        };
        let mut budget = ByteBudget::new(usize::MAX);
        let charged = compute_nested_set(&trace, &mut budget).expect("in budget");
        assert_contiguous_and_total(&trace, &charged.index);
        // A pure cycle must still yield a well-defined root: the promoted
        // component root keeps the `-1` sentinel even though its parent_id
        // points at the other (numbered) cycle member (Finding 2). Exactly
        // one root here (the ascending-first span, P), so
        // `{ nestedSetParent < 0 }` is non-empty.
        let roots: Vec<[u8; 8]> = charged
            .index
            .iter()
            .filter(|(_, v)| v.parent < 0)
            .map(|(id, _)| *id)
            .collect();
        assert_eq!(
            roots,
            vec![sid(1)],
            "the promoted cycle-root keeps parent == -1"
        );
        release_nested_set(charged, &mut budget);
    }

    #[test]
    fn nested_set_parent_lt_zero_selects_the_promoted_root_of_a_cycle() {
        // End-to-end through the evaluator: `{ nestedSetParent < 0 }` must
        // select the promoted root of a pure parent cycle (Finding 2).
        let p = plan("{ nestedSetParent < 0 }");
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![child_span(1, 2, "p", 10), child_span(2, 1, "q", 20)],
        };
        let matches = eval(&p, &[trace], &membership(&p, &[]));
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].matched, 1, "exactly one cycle root");
        assert_eq!(matches[0].spans[0].span_id, sid(1));
    }

    #[test]
    fn nested_set_parent_lt_zero_selects_exactly_the_roots() {
        let p = plan("{ nestedSetParent < 0 }");
        assert!(p.nested_set);
        // Single-root aa tree: only R.
        let matches = eval(&p, &[nested_set_aa()], &membership(&p, &[]));
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].matched, 1);
        assert_eq!(matches[0].spans[0].span_id, sid(1), "the root R");
    }

    #[test]
    fn nested_set_parent_lt_zero_selects_every_root_in_a_forest() {
        let p = plan("{ nestedSetParent < 0 }");
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![
                child_span(1, 0, "r1", 10),
                child_span(2, 1, "c", 20),
                child_span(3, 0, "r2", 30),
            ],
        };
        let matches = eval(&p, &[trace], &membership(&p, &[]));
        assert_eq!(matches[0].matched, 2, "both roots R1 and R2");
        let mut ids: Vec<[u8; 8]> = matches[0].spans.iter().map(|s| s.span_id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![sid(1), sid(3)]);
    }

    #[test]
    fn nested_set_left_comparisons_follow_cmp_semantics() {
        // aa lefts: R(sid1)=1, A(sid2)=2, B(sid3)=4, C(sid4)=5.
        let cases: &[(&str, &[u8])] = &[
            ("{ nestedSetLeft = 1 }", &[1]),
            ("{ nestedSetLeft > 3 }", &[3, 4]),
            ("{ nestedSetLeft >= 4 }", &[3, 4]),
            ("{ nestedSetLeft < 4 }", &[1, 2]),
            ("{ nestedSetLeft != 1 }", &[2, 3, 4]),
        ];
        for (q, expected) in cases {
            let p = plan(q);
            let matches = eval(&p, &[nested_set_aa()], &membership(&p, &[]));
            let mut ids: Vec<u8> = matches[0].spans.iter().map(|s| s.span_id[7]).collect();
            ids.sort_unstable();
            assert_eq!(&ids, expected, "{q}");
        }
    }

    #[test]
    fn nested_set_query_releases_the_index_and_all_transients() {
        // AC6: post-batch the budget holds byte-for-byte only the returned
        // matches' retained bytes — the index and every numbering
        // transient are released.
        let p = plan("{ nestedSetParent < 0 }");
        let mut budget = ByteBudget::new(usize::MAX);
        let matches = evaluate_batch(
            &p,
            &[nested_set_aa()],
            &membership(&p, &[]),
            &mut GroupCardinalityCounter::new(u64::MAX),
            &mut budget,
        )
        .expect("fits");
        let retained: usize = matches.iter().map(TraceMatch::retained_bytes).sum();
        assert_eq!(
            budget.used(),
            retained,
            "index + numbering transients all released"
        );
    }

    // -- issue #184: trace-level / colon-scoped intrinsic evaluation ------

    /// A span with an explicit parent + status message (the #184 fixture
    /// shape).
    fn span_with(
        n: u8,
        parent: u8,
        service: &str,
        name: &str,
        ts: i64,
        dur: i64,
        status_message: &str,
    ) -> HydratedSpan {
        let parent_id = if parent == 0 { [0u8; 8] } else { sid(parent) };
        HydratedSpan {
            span_id: sid(n),
            parent_id,
            service: service.to_string(),
            name: name.to_string(),
            timestamp_ns: ts,
            duration_ns: dur,
            status_code: 0,
            status_message: status_message.to_string(),
            kind: 1,
            scope_name: String::new(),
            scope_version: String::new(),
        }
    }

    /// Installs a trace-context co-load result for `trace_id`.
    fn with_trace_ctx(
        attrs: &mut BatchAttrs,
        trace_id: [u8; 16],
        start_ns: i64,
        end_ns: i64,
        root_name: &str,
        root_service: &str,
    ) {
        attrs.trace_ctx.insert(
            trace_id,
            TraceCtxInfo {
                trace_start_ns: start_ns,
                trace_end_ns: end_ns,
                root_name: root_name.to_string(),
                root_service: root_service.to_string(),
            },
        );
    }

    #[test]
    fn status_message_matches_equality_regex_and_the_empty_message() {
        let p = plan(r#"{ statusMessage = "deadline exceeded" }"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![
                span_with(1, 0, "s", "a", 10, 1, "deadline exceeded"),
                span_with(2, 0, "s", "b", 20, 1, "other"),
                span_with(3, 0, "s", "c", 30, 1, ""),
            ],
        };
        let attrs = membership(&p, &[]);
        let matches = eval(&p, std::slice::from_ref(&trace), &attrs);
        let ids: Vec<[u8; 8]> = matches[0].spans.iter().map(|s| s.span_id).collect();
        assert_eq!(ids, vec![sid(1)]);

        // Regex over the message; the empty-message span never matches a
        // non-empty pattern but DOES match `statusMessage = ""`.
        let p = plan(r#"{ statusMessage =~ "deadline.*" }"#);
        let matches = eval(&p, std::slice::from_ref(&trace), &membership(&p, &[]));
        assert_eq!(matches[0].spans[0].span_id, sid(1));
        let p = plan(r#"{ statusMessage = "" }"#);
        let matches = eval(&p, std::slice::from_ref(&trace), &membership(&p, &[]));
        let ids: Vec<[u8; 8]> = matches[0].spans.iter().map(|s| s.span_id).collect();
        assert_eq!(ids, vec![sid(3)], "the empty message is matchable");
    }

    #[test]
    fn instrumentation_name_and_version_match_equality_regex_and_the_empty_value() {
        // Issue #192: the instrumentation intrinsics evaluate on the hydrated
        // `scope_name`/`scope_version` columns, the `statusMessage` shape.
        let with_scope = |n: u8, name: &str, version: &str| HydratedSpan {
            scope_name: name.to_string(),
            scope_version: version.to_string(),
            ..span_with(n, 0, "s", "op", n as i64 * 10, 1, "")
        };
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![
                with_scope(1, "io.otel.http", "1.4.2"),
                with_scope(2, "io.otel.grpc", "2.0.0"),
                with_scope(3, "", ""),
            ],
        };

        let p = plan(r#"{ instrumentation:name = "io.otel.http" }"#);
        let matches = eval(&p, std::slice::from_ref(&trace), &membership(&p, &[]));
        let ids: Vec<[u8; 8]> = matches[0].spans.iter().map(|s| s.span_id).collect();
        assert_eq!(ids, vec![sid(1)]);

        let p = plan(r#"{ instrumentation:name =~ "io.otel.*" }"#);
        let matches = eval(&p, std::slice::from_ref(&trace), &membership(&p, &[]));
        let ids: Vec<[u8; 8]> = matches[0].spans.iter().map(|s| s.span_id).collect();
        assert_eq!(ids, vec![sid(1), sid(2)]);

        let p = plan(r#"{ instrumentation:version = "2.0.0" }"#);
        let matches = eval(&p, std::slice::from_ref(&trace), &membership(&p, &[]));
        let ids: Vec<[u8; 8]> = matches[0].spans.iter().map(|s| s.span_id).collect();
        assert_eq!(ids, vec![sid(2)]);

        // The empty scope span matches only the empty-value equality.
        let p = plan(r#"{ instrumentation:version = "" }"#);
        let matches = eval(&p, std::slice::from_ref(&trace), &membership(&p, &[]));
        let ids: Vec<[u8; 8]> = matches[0].spans.iter().map(|s| s.span_id).collect();
        assert_eq!(ids, vec![sid(3)], "the empty scope version is matchable");
    }

    #[test]
    fn span_id_and_parent_id_match_their_lowercase_hex_case_insensitively() {
        // sid(0xAB) renders as "00000000000000ab".
        let p = plan(r#"{ span:id = "00000000000000AB" }"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![
                span_with(0xAB, 0, "s", "a", 10, 1, ""),
                span_with(2, 0xAB, "s", "b", 20, 1, ""),
            ],
        };
        let attrs = membership(&p, &[]);
        let matches = eval(&p, std::slice::from_ref(&trace), &attrs);
        let ids: Vec<[u8; 8]> = matches[0].spans.iter().map(|s| s.span_id).collect();
        assert_eq!(
            ids,
            vec![sid(0xAB)],
            "uppercase query hex matches (case-insensitive Eq)"
        );

        let p = plan(r#"{ span:parentID = "00000000000000ab" }"#);
        let matches = eval(&p, std::slice::from_ref(&trace), &membership(&p, &[]));
        let ids: Vec<[u8; 8]> = matches[0].spans.iter().map(|s| s.span_id).collect();
        assert_eq!(ids, vec![sid(2)], "only the child of 0xAB matches");

        // A zero parent renders as all-zero hex — the root is addressable.
        let p = plan(r#"{ span:parentID = "0000000000000000" }"#);
        let matches = eval(&p, std::slice::from_ref(&trace), &membership(&p, &[]));
        let ids: Vec<[u8; 8]> = matches[0].spans.iter().map(|s| s.span_id).collect();
        assert_eq!(ids, vec![sid(0xAB)]);
    }

    #[test]
    fn trace_id_matches_every_span_of_the_matching_trace_only() {
        // tid(1) renders as 30 zeros + "01".
        let p = plan(r#"{ trace:id = "00000000000000000000000000000001" }"#);
        let matching = TraceSpans {
            trace_id: tid(1),
            spans: vec![
                span_with(1, 0, "s", "a", 10, 1, ""),
                span_with(2, 1, "s", "b", 20, 1, ""),
            ],
        };
        let other = TraceSpans {
            trace_id: tid(2),
            spans: vec![span_with(1, 0, "s", "a", 10, 1, "")],
        };
        let attrs = membership(&p, &[]);
        let matches = eval(&p, &[matching, other], &attrs);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].trace_id, tid(1));
        assert_eq!(matches[0].matched, 2, "trace-constant: every span matches");
    }

    /// AC-Δ1a (window-independence): the trace-level values come from the
    /// CO-LOAD (full-trace), not the window-bounded hydrated spans — a
    /// trace whose root span and max-end span were NEVER hydrated still
    /// resolves rootName/rootServiceName/traceDuration to the full-trace
    /// values.
    #[test]
    fn trace_level_intrinsics_resolve_from_the_coload_not_the_hydrated_window() {
        // Hydrated view: ONLY the in-window child (ts 500..501). The
        // trace's true envelope (from the co-load) is [10, 2000] with a
        // root outside the window.
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![span_with(7, 9, "child-svc", "child-op", 500, 1, "")],
        };
        for (q, should_match) in [
            (r#"{ rootServiceName = "gw" }"#, true),
            (r#"{ rootName = "GET /checkout" }"#, true),
            ("{ traceDuration > 1500ns }", true),
            ("{ traceDuration >= 1990ns }", true),
            ("{ traceDuration > 3000ns }", false),
            // The window view alone (duration 1 ns) could never satisfy
            // these — passing proves the co-load values are used.
            (r#"{ rootServiceName = "child-svc" }"#, false),
            (r#"{ rootName = "child-op" }"#, false),
        ] {
            let p = plan(q);
            assert!(p.needs_trace_ctx(), "{q} must demand the co-load");
            let mut attrs = membership(&p, &[]);
            with_trace_ctx(&mut attrs, tid(1), 10, 2000, "GET /checkout", "gw");
            let matches = eval(&p, std::slice::from_ref(&trace), &attrs);
            assert_eq!(matches.len(), usize::from(should_match), "{q}");
        }
    }

    /// AC-Δ1a (root-less / missing-context defensiveness): with NO
    /// trace-context entry for the trace (the plan demanded none, or the
    /// trace vanished between phases), the dependent leaves match nothing
    /// — never a panic, never a spurious match.
    #[test]
    fn missing_trace_context_matches_nothing_for_dependent_leaves() {
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![span_with(1, 0, "s", "a", 10, 1, "")],
        };
        for q in [
            r#"{ rootServiceName = "s" }"#,
            r#"{ rootName != "anything" }"#,
            "{ traceDuration >= 0ns }",
        ] {
            let p = plan(q);
            let attrs = membership(&p, &[]); // no trace_ctx entry
            assert!(
                eval(&p, std::slice::from_ref(&trace), &attrs).is_empty(),
                "{q}"
            );
        }
    }

    #[test]
    fn child_count_reads_the_full_trace_coload_and_defaults_to_zero() {
        // Hydrated: parent (1) + one child (2). The co-load knows the
        // FULL trace: span 1 actually has 3 direct children (two outside
        // the window).
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![
                span_with(1, 0, "s", "parent", 10, 1, ""),
                span_with(2, 1, "s", "child", 20, 1, ""),
            ],
        };
        let p = plan("{ span:childCount = 3 }");
        assert!(p.needs_child_counts());
        let mut attrs = membership(&p, &[]);
        attrs.child_counts.insert((tid(1), sid(1)), 3);
        let matches = eval(&p, std::slice::from_ref(&trace), &attrs);
        let ids: Vec<[u8; 8]> = matches[0].spans.iter().map(|s| s.span_id).collect();
        assert_eq!(
            ids,
            vec![sid(1)],
            "the parent's FULL-trace child count (3) matches, not the windowed 1"
        );

        // Absent key ⇒ 0 children: leaf spans satisfy `childCount = 0`.
        let p = plan("{ span:childCount = 0 }");
        let mut attrs = membership(&p, &[]);
        attrs.child_counts.insert((tid(1), sid(1)), 3);
        let matches = eval(&p, std::slice::from_ref(&trace), &attrs);
        let ids: Vec<[u8; 8]> = matches[0].spans.iter().map(|s| s.span_id).collect();
        assert_eq!(ids, vec![sid(2)], "the leaf span has zero children");
    }

    #[test]
    fn trace_level_leaves_compose_with_span_leaves_in_one_filter() {
        // `{ name = "child" && traceDuration > 100ns }`: the span leaf
        // narrows within the trace, the trace leaf gates the whole trace.
        let p = plan(r#"{ name = "child" && traceDuration > 100ns }"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![
                span_with(1, 0, "s", "parent", 10, 1, ""),
                span_with(2, 1, "s", "child", 20, 1, ""),
            ],
        };
        let mut attrs = membership(&p, &[]);
        with_trace_ctx(&mut attrs, tid(1), 10, 2000, "parent", "s");
        let matches = eval(&p, std::slice::from_ref(&trace), &attrs);
        assert_eq!(matches.len(), 1);
        let ids: Vec<[u8; 8]> = matches[0].spans.iter().map(|s| s.span_id).collect();
        assert_eq!(
            ids,
            vec![sid(2)],
            "only the name-matching span is in the spanset"
        );

        // The same trace fails when the trace-level side fails.
        let p = plan(r#"{ name = "child" && traceDuration > 5000ns }"#);
        let mut attrs = membership(&p, &[]);
        with_trace_ctx(&mut attrs, tid(1), 10, 2000, "parent", "s");
        assert!(eval(&p, std::slice::from_ref(&trace), &attrs).is_empty());
    }

    #[test]
    fn nested_set_numbering_breaches_the_budget_before_allocation() {
        // A budget below the numbering envelope breaches with the 422
        // ScanBudgetBytes class at the pre-charge — before the index or
        // any transient is allocated.
        let p = plan("{ nestedSetParent < 0 }");
        let trace = deep_chain(2_000);
        let mut budget = ByteBudget::new(NESTED_SET_ENTRY_BYTES);
        let err = evaluate_batch(
            &p,
            std::slice::from_ref(&trace),
            &membership(&p, &[]),
            &mut GroupCardinalityCounter::new(u64::MAX),
            &mut budget,
        )
        .expect_err("the numbering pre-charge must breach");
        assert!(
            matches!(
                err,
                ReadError::QueryTooBroad(crate::logql::TooBroadReason::ScanBudgetBytes { .. })
            ),
            "got {err:?}"
        );
    }
}
