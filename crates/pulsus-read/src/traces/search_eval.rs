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
//! | structural result set (`child_set`/`descendant_set`/`sibling_set`) | [`charged_set`] pre-charge at the spans upper bound; released when empty/merged/after summaries like any operand set |
//! | descendant adjacency map + BFS queue (`descendant_set`) | `spans × DESCENDANT_TRANSIENT_BYTES` envelope (map key + `Vec` header + child slot with doubling slack + ≤ 2 queue slots per span; the queue never reallocates by construction) charged before allocation, released after the walk |
//! | descendant visited set (`descendant_set`) | [`charged_set`] pre-charge; released after the RHS intersection |
//! | sibling parent map (`sibling_set`) | `spans × SIBLING_ENTRY_BYTES` charged before allocation, released after the pass |
//! | nested-set index (`compute_nested_set`) | `spans × NESTED_SET_ENTRY_BYTES` charged before allocation; retained for the trace's `eval_spanset`, released right after |
//! | nested-set numbering transients — span-id set + children map (key + `Vec` header + child-`Vec` first-push capacity of 4 slots) + sorted view + Euler stack (`compute_nested_set`) | `spans × NESTED_SET_TRANSIENT_BYTES` envelope charged before allocation, released after numbering |
//!
//! The engine-side (exec.rs) sites are audited in that module's doc;
//! BOTH tables are enforced mechanically by `tests/traces_alloc_audit.rs`
//! (round 4). A failed charge is atomic (no phantom `used`), and a
//! mid-batch breach returns the 422 class with the partial output
//! dropped (error-path release semantics: see `ByteBudget`'s type docs).

use std::collections::{HashMap, HashSet};

use pulsus_traceql::{AggregateOp, ComparisonOp, FieldExpr, SpansetExpr, StructuralOp};

use super::exec::ByteBudget;
use super::filter::NestedSetField;
use super::search_plan::{
    AggSource, PhysicalEval, PhysicalSelect, PlannedFilter, PlannedLeafEval, SearchPlan,
    SelectField,
};
use crate::logql::error::ReadError;

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
    pub kind: i8,
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
/// `probes` / `agg_fields` / `select_attrs`.
#[derive(Debug, Default)]
pub struct BatchAttrs {
    pub membership: Vec<HashSet<SpanKey>>,
    pub agg_values: Vec<HashMap<SpanKey, f64>>,
    pub select_values: Vec<HashMap<SpanKey, String>>,
}

/// One matched span's response summary (docs/api.md §4.2 `spanSets`
/// entry): summary fields plus the `select()`-projected attributes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpanSummary {
    pub span_id: [u8; 8],
    pub name: String,
    pub start_ns: i64,
    pub duration_ns: i64,
    /// `select()`-projected `(display, rendered value)` pairs, in the
    /// query's select order.
    pub attributes: Vec<(String, String)>,
}

impl SpanSummary {
    /// The summary's heap payload beyond its own `size_of` slot in the
    /// parent `TraceMatch::spans` buffer (which the parent accounts):
    /// overhead envelope + name bytes + the attributes buffer at its
    /// **actual capacity** (code review round 2: unused preallocated
    /// capacity is retained memory too) + the attribute string bytes.
    /// [`evaluate_batch`] charges exactly these amounts BEFORE each
    /// allocation, so a heap-evict release of
    /// [`TraceMatch::retained_bytes`] returns precisely what was charged.
    pub(crate) fn heap_payload_bytes(&self) -> usize {
        super::exec::RETAINED_ENTRY_OVERHEAD
            + self.name.len()
            + self.attributes.capacity() * std::mem::size_of::<(String, String)>()
            + self
                .attributes
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>()
    }
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
}

impl TraceMatch {
    /// Capacity-based retained cost — byte-for-byte equal to what
    /// [`evaluate_batch`] charged while building this match (asserted by
    /// the `charges_equal_retained_bytes_exactly` unit test), so the
    /// engine's heap-evict release keeps the budget exact.
    pub(crate) fn retained_bytes(&self) -> usize {
        std::mem::size_of::<TraceMatch>()
            + super::exec::RETAINED_ENTRY_OVERHEAD
            + self.spans.capacity() * std::mem::size_of::<SpanSummary>()
            + self
                .spans
                .iter()
                .map(SpanSummary::heap_payload_bytes)
                .sum::<usize>()
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

fn cmp_f64(op: ComparisonOp, lhs: f64, rhs: f64) -> bool {
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

fn eval_physical(p: &PhysicalEval, span: &HydratedSpan) -> bool {
    match p {
        PhysicalEval::Name { op, value } => op.matches(value, &span.name),
        PhysicalEval::Service { op, value } => op.matches(value, &span.service),
        PhysicalEval::Duration { op, nanos } => cmp_i64(*op, span.duration_ns, *nanos),
        PhysicalEval::Status { op, code } => cmp_i64(*op, span.status_code as i64, *code as i64),
        PhysicalEval::Kind { op, code } => cmp_i64(*op, span.kind as i64, *code as i64),
    }
}

/// Evaluates one filter's boolean tree for one span. Deliberately never
/// short-circuits: `leaf_idx` must advance through every comparison so
/// the pre-order leaf registry stays aligned with the AST walk.
fn eval_expr(
    expr: &FieldExpr,
    filter: &PlannedFilter,
    leaf_idx: &mut usize,
    trace_id: [u8; 16],
    span: &HydratedSpan,
    attrs: &BatchAttrs,
    nested_set: Option<&NestedSetIndex>,
) -> bool {
    match expr {
        FieldExpr::Comparison { .. } => {
            let leaf = &filter.leaves[*leaf_idx];
            *leaf_idx += 1;
            match leaf {
                PlannedLeafEval::Physical(p) => eval_physical(p, span),
                PlannedLeafEval::Attr { probe_idx, negated } => {
                    let member = attrs.membership[*probe_idx].contains(&(trace_id, span.span_id));
                    member != *negated
                }
                // The numbering covers every hydrated span, so the lookup
                // succeeds whenever the plan flagged nested-set (index is
                // `Some`); an absent index/entry is a non-match.
                PlannedLeafEval::NestedSet { field, op, value } => nested_set
                    .and_then(|idx| idx.get(&span.span_id))
                    .map(|v| cmp_f64(*op, v.value(*field) as f64, *value))
                    .unwrap_or(false),
            }
        }
        FieldExpr::Binary { op, lhs, rhs } => {
            let l = eval_expr(lhs, filter, leaf_idx, trace_id, span, attrs, nested_set);
            let r = eval_expr(rhs, filter, leaf_idx, trace_id, span, attrs, nested_set);
            match op {
                pulsus_traceql::BoolOp::And => l && r,
                pulsus_traceql::BoolOp::Or => l || r,
            }
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
    attrs: &BatchAttrs,
    nested_set: Option<&NestedSetIndex>,
    budget: &mut ByteBudget,
) -> Result<Option<ChargedSet>, ReadError> {
    let mut matched = charged_set(trace.spans.len(), budget)?;
    for span in &trace.spans {
        let is_match = match body {
            None => true,
            Some(expr) => {
                let mut leaf_idx = 0;
                eval_expr(
                    expr,
                    filter,
                    &mut leaf_idx,
                    trace.trace_id,
                    span,
                    attrs,
                    nested_set,
                )
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
    attrs: &BatchAttrs,
    nested_set: Option<&NestedSetIndex>,
    budget: &mut ByteBudget,
) -> Result<Option<ChargedSet>, ReadError> {
    match expr {
        SpansetExpr::Filter(f) => {
            let filter = &plan.filters[*filter_idx];
            *filter_idx += 1;
            eval_filter(f.body.as_ref(), filter, trace, attrs, nested_set, budget)
        }
        SpansetExpr::Binary { op, lhs, rhs } => {
            let l = eval_spanset(lhs, plan, filter_idx, trace, attrs, nested_set, budget)?;
            let r = eval_spanset(rhs, plan, filter_idx, trace, attrs, nested_set, budget)?;
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
        // Structural relations (issue #172): both operands must match
        // within the trace before the relation is worth computing; an
        // empty side releases the other and yields no result.
        SpansetExpr::Structural { op, lhs, rhs } => {
            let l = eval_spanset(lhs, plan, filter_idx, trace, attrs, nested_set, budget)?;
            let r = eval_spanset(rhs, plan, filter_idx, trace, attrs, nested_set, budget)?;
            match (l, r) {
                (Some(a), Some(b)) => eval_structural(*op, a, b, trace, budget),
                (Some(a), None) => {
                    release_set(a, budget);
                    Ok(None)
                }
                (None, Some(b)) => {
                    release_set(b, budget);
                    Ok(None)
                }
                (None, None) => Ok(None),
            }
        }
    }
}

/// The all-zero `parent_id` sentinel: "no recorded parent" (a root).
const ZERO_ID: [u8; 8] = [0u8; 8];

/// Per-span transient cost envelope for the descendant BFS: one
/// adjacency-map contribution (map key + `Vec` header + a child slot
/// with its growth-doubling slack) plus up to two queue slots (an LHS
/// seed and one discovery per span — the queue is sized so it never
/// reallocates), plus the container-overhead envelope.
const DESCENDANT_TRANSIENT_BYTES: usize = std::mem::size_of::<[u8; 8]>()
    + std::mem::size_of::<Vec<[u8; 8]>>()
    + 2 * std::mem::size_of::<[u8; 8]>()
    + 2 * std::mem::size_of::<[u8; 8]>()
    + super::exec::RETAINED_ENTRY_OVERHEAD;

/// Per-entry cost of the sibling parent map (`parent_id → (LHS-match
/// count, representative span_id)` + the container-overhead envelope).
const SIBLING_ENTRY_BYTES: usize = std::mem::size_of::<[u8; 8]>()
    + std::mem::size_of::<(u32, [u8; 8])>()
    + super::exec::RETAINED_ENTRY_OVERHEAD;

/// Evaluates one structural relation (issue #172) over the trace's
/// hydrated spans — O(spans), bounded by `MAX_SPANS_PER_TRACE`.
/// Consumes (and releases) both operand sets and
/// returns the RHS-only result set (adjudicated pin 3), `None` when it
/// is empty. Every intermediate is charge-before-allocate; on an error
/// the request's budget dies whole (the standing error-path convention).
fn eval_structural(
    op: StructuralOp,
    lhs: ChargedSet,
    rhs: ChargedSet,
    trace: &TraceSpans,
    budget: &mut ByteBudget,
) -> Result<Option<ChargedSet>, ReadError> {
    let result = match op {
        StructuralOp::Child => child_set(&lhs, &rhs, trace, budget)?,
        StructuralOp::Descendant => descendant_set(&lhs, &rhs, trace, budget)?,
        StructuralOp::Sibling => sibling_set(&lhs, &rhs, trace, budget)?,
    };
    release_set(lhs, budget);
    release_set(rhs, budget);
    if result.set.is_empty() {
        release_set(result, budget);
        Ok(None)
    } else {
        Ok(Some(result))
    }
}

/// `{A} > {B}`: spans matching B whose **direct parent** matches A.
/// All-zero `parent_id` spans have no parent and never match; orphans
/// (non-zero `parent_id` with no hydrated parent) never match because
/// every LHS id is a hydrated span's id.
fn child_set(
    lhs: &ChargedSet,
    rhs: &ChargedSet,
    trace: &TraceSpans,
    budget: &mut ByteBudget,
) -> Result<ChargedSet, ReadError> {
    let mut out = charged_set(trace.spans.len(), budget)?;
    for span in &trace.spans {
        if span.parent_id != ZERO_ID
            && rhs.set.contains(&span.span_id)
            && lhs.set.contains(&span.parent_id)
        {
            out.set.insert(span.span_id);
        }
    }
    Ok(out)
}

/// `{A} >> {B}`: spans matching B **strictly below** an A-matching span
/// in the parent chain — an O(spans) BFS over a `parent_id → children`
/// adjacency map (the documented spike shape, docs/schemas.md §4.2)
/// seeded from A's matched ids. The visited set is pre-seeded with the
/// LHS spans: they are traversed *through* but can never be
/// re-discovered, so an LHS-matching span is never yielded as a
/// descendant — including through a malformed/self-referential parent
/// cycle (codex review on issue #172: "a span is never its own
/// descendant") — and the same guard terminates every cycle. An
/// out-of-window (never hydrated) intermediate hop breaks the chain —
/// docs/api.md §4.2.
fn descendant_set(
    lhs: &ChargedSet,
    rhs: &ChargedSet,
    trace: &TraceSpans,
    budget: &mut ByteBudget,
) -> Result<ChargedSet, ReadError> {
    let transients = trace.spans.len() * DESCENDANT_TRANSIENT_BYTES;
    budget.charge(transients)?;
    let mut children: HashMap<[u8; 8], Vec<[u8; 8]>> = HashMap::with_capacity(trace.spans.len());
    for span in &trace.spans {
        if span.parent_id != ZERO_ID {
            children
                .entry(span.parent_id)
                .or_default()
                .push(span.span_id);
        }
    }
    // Pushes are bounded by seeds (≤ spans) + one visited-guarded
    // discovery per non-seed span, so the reservation is never exceeded.
    let mut queue: Vec<[u8; 8]> = Vec::with_capacity(lhs.set.len() + trace.spans.len());
    queue.extend(lhs.set.iter().copied());
    let mut visited = charged_set(trace.spans.len(), budget)?;
    visited.set.extend(lhs.set.iter().copied());
    let mut cursor = 0;
    while cursor < queue.len() {
        let parent = queue[cursor];
        cursor += 1;
        if let Some(kids) = children.get(&parent) {
            for child in kids {
                if visited.set.insert(*child) {
                    queue.push(*child);
                }
            }
        }
    }
    let mut out = charged_set(trace.spans.len(), budget)?;
    for id in &visited.set {
        if !lhs.set.contains(id) && rhs.set.contains(id) {
            out.set.insert(*id);
        }
    }
    release_set(visited, budget);
    drop(children);
    budget.release(transients);
    Ok(out)
}

/// `{A} ~ {B}`: spans matching B sharing a `parent_id` with a
/// **distinct** span matching A (self excluded). Adjudicated pin 2:
/// all-zero `parent_id` (root) spans have no parent to share and never
/// match. One pass builds `parent_id → (LHS count, representative)`;
/// a group of one only matches when its sole LHS member is a different
/// span.
fn sibling_set(
    lhs: &ChargedSet,
    rhs: &ChargedSet,
    trace: &TraceSpans,
    budget: &mut ByteBudget,
) -> Result<ChargedSet, ReadError> {
    let map_charge = trace.spans.len() * SIBLING_ENTRY_BYTES;
    budget.charge(map_charge)?;
    let mut parents: HashMap<[u8; 8], (u32, [u8; 8])> = HashMap::with_capacity(trace.spans.len());
    for span in &trace.spans {
        if span.parent_id != ZERO_ID && lhs.set.contains(&span.span_id) {
            parents
                .entry(span.parent_id)
                .and_modify(|(count, _)| *count += 1)
                .or_insert((1, span.span_id));
        }
    }
    let mut out = charged_set(trace.spans.len(), budget)?;
    for span in &trace.spans {
        if span.parent_id == ZERO_ID || !rhs.set.contains(&span.span_id) {
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

fn aggregate_value(
    agg: &super::search_plan::PlannedAggregate,
    trace: &TraceSpans,
    matched: &HashSet<[u8; 8]>,
    attrs: &BatchAttrs,
) -> Option<f64> {
    let values: Vec<f64> = match &agg.source {
        AggSource::Count => return Some(matched.len() as f64),
        AggSource::DurationNs => trace
            .spans
            .iter()
            .filter(|s| matched.contains(&s.span_id))
            .map(|s| s.duration_ns as f64)
            .collect(),
        AggSource::Attr { field_idx } => trace
            .spans
            .iter()
            .filter(|s| matched.contains(&s.span_id))
            .filter_map(|s| {
                attrs.agg_values[*field_idx]
                    .get(&(trace.trace_id, s.span_id))
                    .copied()
            })
            .collect(),
    };
    if values.is_empty() {
        return None;
    }
    Some(match agg.op {
        AggregateOp::Count => values.len() as f64,
        AggregateOp::Sum => values.iter().sum(),
        AggregateOp::Avg => values.iter().sum::<f64>() / values.len() as f64,
        AggregateOp::Min => values.iter().copied().fold(f64::INFINITY, f64::min),
        AggregateOp::Max => values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
    })
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

/// Builds one span summary, charging the budget **before every retained
/// allocation** (code review round 2): the summary's overhead + name
/// bytes + the attributes buffer at full capacity are charged before
/// anything is cloned, and each attribute's display/value string bytes
/// are charged before that pair is cloned into the buffer. The one
/// stated residual: scalar renders (`duration`/`status`/`kind` — ≤ ~20
/// bytes by construction) are transiently allocated to learn their
/// length, then charged before entering the buffer; unbounded strings
/// (name/service/attr values) are never cloned before their charge.
fn build_summary(
    plan: &SearchPlan,
    trace_id: [u8; 16],
    span: &HydratedSpan,
    attrs: &BatchAttrs,
    budget: &mut ByteBudget,
) -> Result<SpanSummary, ReadError> {
    let attr_capacity = plan.select_fields.len();
    budget.charge(
        super::exec::RETAINED_ENTRY_OVERHEAD
            + span.name.len()
            + attr_capacity * std::mem::size_of::<(String, String)>(),
    )?;
    let mut attributes = Vec::with_capacity(attr_capacity);
    for field in &plan.select_fields {
        match field {
            SelectField::Physical { display, column } => match column {
                PhysicalSelect::Name => {
                    budget.charge(display.len() + span.name.len())?;
                    attributes.push((display.clone(), span.name.clone()));
                }
                PhysicalSelect::Service => {
                    budget.charge(display.len() + span.service.len())?;
                    attributes.push((display.clone(), span.service.clone()));
                }
                PhysicalSelect::DurationNs | PhysicalSelect::Status | PhysicalSelect::Kind => {
                    let value = match column {
                        PhysicalSelect::DurationNs => span.duration_ns.to_string(),
                        PhysicalSelect::Status => status_keyword(span.status_code).to_string(),
                        _ => kind_keyword(span.kind).to_string(),
                    };
                    budget.charge(display.len() + value.len())?;
                    attributes.push((display.clone(), value));
                }
            },
            SelectField::Attr { display, field_idx } => {
                if let Some(value) = attrs.select_values[*field_idx].get(&(trace_id, span.span_id))
                {
                    budget.charge(display.len() + value.len())?;
                    // The probe sits between the charge and the clone: a
                    // refused charge returns above and this line — and
                    // therefore the clone below — never executes (the
                    // round-4 observable ordering proof).
                    #[cfg(test)]
                    clone_probe::record();
                    attributes.push((display.clone(), value.clone()));
                }
            }
        }
    }
    Ok(SpanSummary {
        span_id: span.span_id,
        name: span.name.clone(),
        start_ns: span.timestamp_ns,
        duration_ns: span.duration_ns,
        attributes,
    })
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
        let mut filter_idx = 0;
        let spanset = eval_spanset(
            &plan.spanset,
            plan,
            &mut filter_idx,
            trace,
            attrs,
            nested_set.as_ref().map(|c| &c.index),
            budget,
        )?;
        if let Some(charged) = nested_set {
            release_nested_set(charged, budget);
        }
        let Some(matched) = spanset else {
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
        for agg in &plan.aggregates {
            let pass = match aggregate_value(agg, trace, &matched.set, attrs) {
                Some(value) => cmp_f64(agg.cmp, value, agg.threshold),
                None => false,
            };
            if !pass {
                budget.release(transients);
                release_set(matched, budget);
                continue 'traces;
            }
        }
        // `select()` never changes which traces match — response shaping
        // only (plan v2).
        let mut matched_spans: Vec<&HydratedSpan> = trace
            .spans
            .iter()
            .filter(|s| matched.set.contains(&s.span_id))
            .collect();
        matched_spans.sort_by(|a, b| (a.timestamp_ns, a.span_id).cmp(&(b.timestamp_ns, b.span_id)));
        let sort_key = matched_spans
            .iter()
            .map(|s| s.timestamp_ns)
            .max()
            .unwrap_or(i64::MIN);
        let take = matched_spans.len().min(plan.spss as usize);
        // Charge the match base + the summaries buffer (at its exact
        // capacity) BEFORE allocating it.
        budget.charge(
            std::mem::size_of::<TraceMatch>()
                + super::exec::RETAINED_ENTRY_OVERHEAD
                + take * std::mem::size_of::<SpanSummary>(),
        )?;
        let mut summaries = Vec::with_capacity(take);
        for span in matched_spans.iter().take(take) {
            summaries.push(build_summary(plan, trace.trace_id, span, attrs, budget)?);
        }
        let matched_total = matched_spans.len() as u32;
        drop(matched_spans);
        budget.release(transients);
        release_set(matched, budget);
        out.push(TraceMatch {
            trace_id: trace.trace_id,
            sort_key,
            matched: matched_total,
            spans: summaries,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
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
            kind: 1,
        }
    }

    /// Runs the evaluator under a large test budget — round-2 review:
    /// there is deliberately NO uncharged evaluation path, so the pure
    /// semantic tests fund one instead of bypassing the accounting.
    fn eval(plan: &SearchPlan, traces: &[TraceSpans], attrs: &BatchAttrs) -> Vec<TraceMatch> {
        evaluate_batch(plan, traces, attrs, &mut ByteBudget::new(usize::MAX))
            .expect("within the test budget")
    }

    fn membership(plan: &SearchPlan, entries: &[(usize, [u8; 16], [u8; 8])]) -> BatchAttrs {
        let mut attrs = BatchAttrs {
            membership: vec![HashSet::new(); plan.probes.len()],
            agg_values: vec![HashMap::new(); plan.agg_fields.len()],
            select_values: vec![HashMap::new(); plan.select_attrs.len()],
        };
        for (probe_idx, trace_id, span_id) in entries {
            attrs.membership[*probe_idx].insert((*trace_id, *span_id));
        }
        attrs
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
        assert_eq!(
            matches[0].spans[0].attributes,
            vec![
                ("resource.service.name".to_string(), "checkout".to_string()),
                ("span.foo".to_string(), "bar".to_string()),
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
            let matches =
                evaluate_batch(&p, &[family_trace()], &attrs, &mut budget).expect("in budget");
            assert!(matches.is_empty(), "{q}");
            assert_eq!(budget.used(), 0, "{q}: all sets released on the miss path");
        }
    }

    #[test]
    fn cyclic_seeds_are_never_their_own_descendants() {
        // Codex review (issue #172): a malformed 2-cycle where BOTH
        // spans match both operands. Neither may be yielded — an
        // LHS-matching span is never returned as a descendant, even when
        // the cycle makes it "reachable" from the other seed — and the
        // traversal must terminate.
        let p = plan(r#"{ name = "p" } >> { name = "p" }"#);
        let trace = TraceSpans {
            trace_id: tid(1),
            spans: vec![child_span(1, 2, "p", 10), child_span(2, 1, "p", 20)],
        };
        let attrs = membership(&p, &[]);
        assert!(
            eval(&p, &[trace], &attrs).is_empty(),
            "a span is never its own descendant, including through a parent cycle"
        );
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
        assert_eq!(
            matches[0].spans[0].attributes,
            vec![("name".to_string(), "b".to_string())]
        );
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
            let matches = evaluate_batch(&p, &[trace], &attrs, &mut budget).expect("in budget");
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
        let err = evaluate_batch(&p, std::slice::from_ref(&trace), &attrs, &mut budget)
            .expect_err("the descendant envelope pre-charge must breach");
        assert!(
            matches!(
                err,
                ReadError::QueryTooBroad(crate::logql::TooBroadReason::ScanBudgetBytes { .. })
            ),
            "got {err:?}"
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
        let matches = evaluate_batch(&p, &traces, &attrs, &mut budget).expect("in budget");
        assert_eq!(matches.len(), 2);
        let retained: usize = matches.iter().map(TraceMatch::retained_bytes).sum();
        assert_eq!(
            budget.used(),
            retained,
            "the budget must hold exactly the returned matches' retained bytes"
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
        let p = plan("{}"); // no select() fields: attribute capacity is 0
        let attrs = membership(&p, &[]);
        for name_len in [0usize, 1, 8_000] {
            let name = "n".repeat(name_len);
            let s = span(1, "svc", &name, 10, 1);
            let mut budget = ByteBudget::new(usize::MAX);
            let summary =
                build_summary(&p, tid(1), &s, &attrs, &mut budget).expect("within the test budget");
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
        let matches = evaluate_batch(&p, &[trace], &attrs, &mut budget).expect("in budget");
        let summary = &matches[0].spans[0];
        assert!(summary.attributes.is_empty());
        assert_eq!(
            summary.attributes.capacity(),
            1,
            "with_capacity(select_fields)"
        );
        assert!(
            summary.heap_payload_bytes()
                >= std::mem::size_of::<(String, String)>()
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
        let p = plan(r#"{ name = "x" } | select(span.foo)"#);
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
        let built =
            evaluate_batch(&p, std::slice::from_ref(&trace), &attrs, &mut probe).expect("fits");
        assert_eq!(clone_probe::count(), 1, "the allowed path clones once");
        let full_cost = probe.used();
        assert_eq!(full_cost, built[0].retained_bytes());

        // Breach at the FINAL charge — the value charge (deterministic:
        // charges are a fixed sequence and the value charge is last).
        // The charge fails, so the clone site is never reached.
        clone_probe::reset();
        let mut budget = ByteBudget::new(full_cost - 1);
        let err = evaluate_batch(&p, std::slice::from_ref(&trace), &attrs, &mut budget)
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
        evaluate_batch(&p, std::slice::from_ref(&trace), &attrs, &mut tiny)
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
        let err = evaluate_batch(&p, std::slice::from_ref(&trace), &attrs, &mut budget)
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
        let matches = evaluate_batch(&p, &[trace], &attrs, &mut roomy).expect("in budget");
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
        let err = evaluate_batch(&p, std::slice::from_ref(&trace), &attrs, &mut budget)
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
        let matches = evaluate_batch(&p, &[trace], &attrs, &mut roomy).expect("in budget");
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
                kind: 1,
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
        let matches = evaluate_batch(&p, &[nested_set_aa()], &membership(&p, &[]), &mut budget)
            .expect("fits");
        let retained: usize = matches.iter().map(TraceMatch::retained_bytes).sum();
        assert_eq!(
            budget.used(),
            retained,
            "index + numbering transients all released"
        );
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
