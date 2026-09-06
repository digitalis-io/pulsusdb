//! TraceQL against the shared compile core (issue #492).
//!
//! **Wired, and no TraceQL statement moves.** Since part 3
//! [`super::search_plan::plan_search`] builds a chain here on every
//! search request, folds it and plans it;
//! [`super::exec::TraceEngine`]'s per-batch read dispatch walks that
//! chain instead of six hand-written index loops; and
//! `X-Pulsus-Explain: 1` on the search route returns the plan's shape.
//! Every statement is still rendered by the shipped builders, so all 83
//! frozen SQL goldens are byte-unchanged — which is what makes a moved
//! golden, in whatever part next moves one, a renderer defect rather
//! than an unattributable mix of two changes.
//!
//! What lands here is the chain link set, the [`Lang`] impl, the chain
//! builder ([`chain_of`]), and the two rules the design rests on: `Emit`
//! is `Never` **and served by its own SQL part**, and no regex leaf may
//! claim [`Fidelity::Equivalent`].
//!
//! **Since part 4 one stage does compile into SQL.** A spanset aggregate
//! over an exact single-leaf attribute-equality selector renders a
//! `HAVING` into the phase-1 generator statement
//! ([`aggregate_having_sql`]), so the database discards the traces that
//! do not qualify instead of transporting them. Every other stage still
//! contributes no SQL.
//!
//! **A lowered TraceQL search is four statements, not two, and the
//! aggregate pushdown does not change the count.** The four are the
//! compiled generator, the window-bounded hydration read, the membership
//! read and the winners' root read. The middle two survive lowering
//! because `spanSets[].matched` and `spanSets[].spans[]` are written
//! unconditionally
//! (`pulsus-server/src/traces_api/search_response.rs:428-430`,
//! `:507-512`), and a statement projecting `trace_id, max(timestamp_ns)`
//! produces neither. The fourth is `Emit`'s: the response's root summary
//! is read trace-wide with **no time predicate** — the true root may
//! predate the search window — and `TraceSearchResult.root` is not
//! optional, so every search response needs it. `Never` is the right
//! classification and it does not mean the evaluator does the work: the
//! way the evaluator owns that link is to send a second statement, so the
//! plan builder gives it an SQL part. "Cannot be lowered into THIS
//! statement" and "is not SQL" are different claims, and only the first
//! is made here.
//!
//! The saving is in what each statement carries, not in how many there
//! are: on the corpus part 4 measured, 46 statements and 166,450 result
//! bytes become 4 and 19,710, because every candidate the generator
//! returns already qualifies and the batch loop stops on the first
//! batch.

use pulsus_traceql::{
    AggregateOp, ComparisonOp, Field, FieldExpr, FieldOp, Intrinsic, PipelineStage, Query,
    SpansetExpr, SpansetFilter, UnaryOp,
};

use super::filter::{GenTable, LeafGenerator, PlanError};
use crate::compile::fold::{
    BlockReason, Capability, Col, ColSet, Fidelity, Lang, Lower, LowerCx, Name, NeverReason, Pred,
    Provenance, Relation, Shape, SourceName, SourceTerm,
};
use crate::compile::plan::{HandoffCost, PlanCx, SeedBound, SourceRef};

// ---------------------------------------------------------------------
// Sources
// ---------------------------------------------------------------------

/// The span table, read inside the request's time window.
pub const TRACE_SPANS: SourceRef = SourceRef("trace_spans");
/// The attribute index.
pub const TRACE_ATTRS_IDX: SourceRef = SourceRef("trace_attrs_idx");
/// The winners' root summary: the SAME table, read trace-wide with **no
/// time predicate**, which is why it is a different source and not the
/// same one twice. A window-bounded statement cannot produce that answer:
/// a search whose window begins after the root span starts still reports
/// that root's name, start and duration.
pub const TRACE_SPANS_ROOT: SourceRef = SourceRef("trace_spans:root");
/// The batch hydration read: the span table again, seeded on one batch of
/// candidate trace ids and bounded by the request window.
pub const TRACE_SPANS_HYDRATION: SourceRef = SourceRef("trace_spans:hydration");
/// One attribute membership probe's batch read.
pub const TRACE_ATTRS_MEMBERSHIP: SourceRef = SourceRef("trace_attrs_idx:membership");
/// One attribute VALUE batch read — `val_num` for an aggregate operand,
/// `val` for a `select()` field. Both are the same read shape against the
/// same index, which is why they share a source and the executor names
/// both stages `phase2_attr_values`.
pub const TRACE_ATTRS_VALUES: SourceRef = SourceRef("trace_attrs_idx:values");
/// One MULTI-VALUED event/link set batch read (issue #351).
pub const TRACE_ATTRS_EVENT_SETS: SourceRef = SourceRef("trace_attrs_idx:event_sets");
/// The trace-level context co-load (issue #184): the span table, read
/// trace-wide with no time predicate.
pub const TRACE_SPANS_CTX: SourceRef = SourceRef("trace_spans:trace_ctx");
/// The direct-child-count co-load (issue #184), same table, same
/// trace-wide reach.
pub const TRACE_SPANS_CHILD_COUNT: SourceRef = SourceRef("trace_spans:child_count");

/// The key every TraceQL handoff is seeded on: phase 2 and the winners'
/// root read are all `trace_id IN (…)` primary-key reads.
pub const TRACE_ID: &str = "trace_id";

/// The name of the config field that bounds a TraceQL phase-2 seed. The
/// core carries the number ([`crate::compile::plan::PlanConfig::seed_bound_rows`]);
/// the spelling is ours, because it is our config key.
pub const MAX_CANDIDATES_CONFIG: &str = "reader.traceql_max_candidates";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TqlSource(pub SourceRef);

impl SourceName for TqlSource {
    fn source_ref(&self) -> SourceRef {
        self.0
    }

    fn named(s: SourceRef) -> Self {
        TqlSource(s)
    }
}

/// TraceQL's shapes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TqlShape {
    Spans,
    Traces,
    Groups(Name),
}

impl Shape for TqlShape {}

/// What crosses between two TraceQL parts: the winners' trace ids,
/// bounded by the request `limit`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TqlHandoff(pub Vec<[u8; 16]>);

/// TraceQL's chain link: the selector, the phase-2 reads, the engine work
/// the evaluator does over a hydrated batch, the pipeline stages, and the
/// three synthesised links.
///
/// **The order of the variants is the order the executor issues them**,
/// which is what [`chain_of`] builds and what
/// [`crate::compile::plan::plan_of`] partitions into parts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TqlLink {
    Source {
        expr: Box<SpansetExpr>,
        /// Whether the phase-1 generator set is EXACTLY the selector's
        /// match set rather than a superset of it.
        ///
        /// **Computed since part 4** by
        /// [`super::search_plan::generator_is_exact`], which compares the
        /// membership read's predicate against the generator's for THIS
        /// query rather than arguing about them. It is `false` for almost
        /// every query, and that is the measured truth rather than a
        /// placeholder: `CompiledSpanFilter`'s own contract calls the
        /// generator set "a superset of the filter's matches by
        /// construction", and `search_eval::evaluate_batch` re-evaluates
        /// every leaf against every hydrated span on every search. A
        /// regex-free selector whose phase-1 generator is one conjunct of
        /// three — `{ resource.service.name = "checkout" && span.http.status_code >= 500 && duration > 2s }`
        /// generates on the service predicate alone — is a superset, and
        /// calling it [`Fidelity::Equivalent`] would assert
        /// `orig ⟺ sql` where only `orig ⟹ sql` holds. Since
        /// `BoundaryOutput::Exact`'s contract is *the evaluator MUST NOT
        /// re-filter*, that inversion can DROP rows (issue #492 part 3,
        /// D3).
        ///
        /// An exact generator is the aggregate pushdown's precondition,
        /// which is why the computation landed with it.
        generator_is_exact: bool,
    },
    /// The batch hydration read.
    Hydrate,
    /// One attribute membership probe's batch read; the index is into
    /// `SearchPlan::probes`.
    Membership(usize),
    /// One aggregate operand's `val_num` batch read; the index is into
    /// `SearchPlan::agg_fields`.
    AggValues(usize),
    /// One `select()` field's `val` batch read; the index is into
    /// `SearchPlan::select_attrs`.
    SelectValues(usize),
    /// One event/link value-set batch read; the index is into
    /// `SearchPlan::event_sets`.
    EventSet(usize),
    /// The trace-level context co-load (issue #184).
    TraceCtx,
    /// The direct-child-count co-load (issue #184).
    ChildCount,
    /// A structural relation between two spans of one trace (issue #172).
    Structural,
    /// The per-trace query-time modified-preorder numbering (issue #181).
    NestedSet,
    /// One `!`-operand truthiness leaf (issue #335), whose non-boolean
    /// case fails the WHOLE query.
    BoolTruth,
    Pipe(PipelineStage),
    Order,
    Limit(u32),
    Emit,
}

/// The marker the core is generic over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tql;

// ---------------------------------------------------------------------
// The regex-dialect rule
// ---------------------------------------------------------------------

/// Which string comparison a leaf compiles to — the discriminant of the
/// search planner's own `StrOp`, which is all the fidelity rule reads.
///
/// The `StrOp`-to-`StrOpKind` bridge lives in this module's test rather
/// than here, because nothing in production holds a compiled `StrOp` at
/// the point a fidelity verdict is wanted; it is written there as an
/// exhaustive `match` with no `_` arm, so a new `StrOp` variant fails to
/// build a binary `cargo test --workspace` builds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrOpKind {
    Eq,
    Neq,
    Re,
    Nre,
}

impl StrOpKind {
    /// The AST operator's compiled shape, or `None` for an operator no
    /// string leaf accepts.
    pub fn of_comparison(op: ComparisonOp) -> Option<Self> {
        match op {
            ComparisonOp::Eq => Some(StrOpKind::Eq),
            ComparisonOp::Neq => Some(StrOpKind::Neq),
            ComparisonOp::Re => Some(StrOpKind::Re),
            ComparisonOp::Nre => Some(StrOpKind::Nre),
            ComparisonOp::Gt | ComparisonOp::Gte | ComparisonOp::Lt | ComparisonOp::Lte => None,
        }
    }

    /// **The one place the rule is stated.**
    ///
    /// A regex leaf may never be [`Fidelity::Equivalent`], and the reason
    /// is measured rather than assumed: on this path one pattern gets two
    /// readings. The generator renders the pattern into SQL, where the
    /// database reads it as RE2; our own evaluator re-checks the same
    /// pattern with the raw Rust `regex` crate, with no dialect rewrite
    /// applied anywhere under `traces/`. The two disagree on `\d`, `\w`
    /// and `\s`: over the subject `٤` (U+0664), RE2 does not match and
    /// the Rust crate does.
    ///
    /// **`Wider` is the answer, and it is not a repair.** `Wider` means
    /// `orig ⟹ sql`; on those subjects the SQL reading is the NARROWER
    /// one, so the candidate is discarded in the database and there is
    /// nothing left for the evaluator to re-apply the link to. Measured
    /// end to end through our own write and read paths: two traces, one
    /// span each, named `٤` and `4`; `{ name =~ "\d" }` emits
    /// `match(name, '^(?:\d)$')` and returns ONE trace, the ASCII one,
    /// while the raw Rust crate matches both subjects. The divergence is
    /// a parity defect that ships today and is recorded, not repaired
    /// here; what this rule prevents is a compiler decision built on top
    /// of it.
    pub fn fidelity(self) -> Fidelity {
        match self {
            StrOpKind::Eq | StrOpKind::Neq => Fidelity::Equivalent,
            StrOpKind::Re | StrOpKind::Nre => Fidelity::Wider,
        }
    }
}

/// Whether any leaf of a selector compiles to a regex comparison — the
/// selector-level reading of [`StrOpKind::fidelity`], and not a second
/// statement of the rule: it asks [`StrOpKind::fidelity`] for every leaf
/// it finds.
pub fn selector_fidelity(expr: &SpansetExpr) -> Fidelity {
    fn walk_field(e: &FieldExpr, out: &mut Fidelity) {
        match e {
            FieldExpr::Field(_) | FieldExpr::Literal(_) | FieldExpr::Exists { .. } => {}
            FieldExpr::Unary {
                op: UnaryOp::Not | UnaryOp::Neg,
                expr,
            } => walk_field(expr, out),
            FieldExpr::Binary { op, lhs, rhs } => {
                if let FieldOp::Cmp(c) = op
                    && let Some(kind) = StrOpKind::of_comparison(*c)
                    && kind.fidelity() == Fidelity::Wider
                {
                    *out = Fidelity::Wider;
                }
                walk_field(lhs, out);
                walk_field(rhs, out);
            }
        }
    }
    fn walk_filter(f: &SpansetFilter, out: &mut Fidelity) {
        if let Some(body) = &f.body {
            walk_field(body, out);
        }
    }
    fn walk(e: &SpansetExpr, out: &mut Fidelity) {
        match e {
            SpansetExpr::Filter(f) => walk_filter(f, out),
            SpansetExpr::Binary { lhs, rhs, .. } | SpansetExpr::Structural { lhs, rhs, .. } => {
                walk(lhs, out);
                walk(rhs, out);
            }
        }
    }
    let mut out = Fidelity::Equivalent;
    walk(expr, &mut out);
    out
}

// ---------------------------------------------------------------------
// The spanset aggregate's HAVING
// ---------------------------------------------------------------------

/// The `HAVING` fragment a spanset aggregate compiles to, or `None` when
/// this aggregate may not be pushed (issue #492 part 4).
///
/// **The one renderer.** [`AggregateLower::apply`] records what this
/// returns on the relation and [`super::search_plan::plan_search`] puts
/// the same string into the statement, so the plan's account of the
/// query and the query cannot disagree.
///
/// Three families push, and the rule is duplicate-idempotence rather
/// than taste. `trace_attrs_idx` is a `ReplacingMergeTree` read without
/// `FINAL`, so an at-least-once replay leaves two rows for one span
/// (which is why [`super::search_sql::membership_sql`] uses `SELECT
/// DISTINCT`), and the evaluator deduplicates hydrated spans by
/// `span_id` (`exec::group_hydrated_rows`). Measured on ClickHouse 26.3
/// over one trace with two matched spans of 1.5 s and 0.5 s and one of
/// them replayed: the engine sees `count=2 sum=2.0e9 avg=1.0e9
/// max=1.5e9 min=0.5e9`; the raw index rows give `count()=3 sum=2.5e9
/// avg=833333333.33 max=1.5e9 min=0.5e9`, and `uniqExact(span_id)=2`.
/// `min`, `max` and `uniqExact` agree with the engine; `count()`, `sum`
/// and `avg` do not.
///
/// | stage | fragment |
/// |---|---|
/// | `min(duration) <op> t` | `min(duration_ns) <op> <t as i64>` |
/// | `max(duration) <op> t` | `max(duration_ns) <op> <t as i64>` |
/// | `count() <op> t`       | `uniqExact(span_id) <op> <t as i64>` |
///
/// `uniqExact` and not `count(DISTINCT span_id)`: the latter resolves
/// through the `count_distinct_implementation` session setting, and a
/// read whose meaning depends on a setting we do not send is a read
/// whose meaning we do not know. It also reaches a case a replay does
/// not: one span with two events carrying the same attribute produces
/// two index rows identical in every ordering-key column, collapsed on
/// merge but visible before it, and `count()` would inflate over them.
///
/// `None` for `sum` and `avg` (the measurement above), for an attribute
/// source (`max(.retries)` reads a `val_num` for a different `key` than
/// the generator's, which is a join), and for any threshold whose LEXEME
/// is not an exact integer strictly inside ±2^53
/// ([`super::search_plan::exact_aggregate_threshold`], which carries the
/// argument for the bound). `| count() > 2.5` and
/// `| max(duration) = 9007199254740993` are both accepted queries today
/// and neither pushes.
///
/// **A pushed aggregate reads `duration_ns` from `trace_attrs_idx`; the
/// evaluator reads it from `trace_spans`.** They are two copies of one
/// fact, and they are bound together at one site:
/// `pulsus-write/src/protocols/otlp_traces.rs:404` binds `duration_ns`
/// once per span, and every attribute record for that span and the span
/// record itself take that same binding. `writer/rows.rs`'s
/// `trace_attrs_idx` backfill note already states the invariant in prose
/// — the non-key columns are deterministic functions of the same attr
/// record — which is what makes the versionless `ReplacingMergeTree`
/// collapse safe on a key tuple that does not include `duration_ns`.
/// This is a documented invariant and not a guard: a guard in the read
/// path would have to re-read `trace_spans` to check it, which is the
/// read the pushdown exists to avoid.
pub fn aggregate_having_sql(stage: &PipelineStage) -> Option<String> {
    let PipelineStage::Aggregate {
        op,
        field,
        cmp,
        value,
    } = stage
    else {
        return None;
    };
    // The aggregation source. `sum`/`avg` are refused because a
    // duplicated index row moves them; an attribute source is refused
    // because its `val_num` lives on a different `key` than the
    // generator's rows.
    let agg = match (op, field) {
        (AggregateOp::Count, None) => "uniqExact(span_id)",
        (AggregateOp::Min, Some(FieldExpr::Field(Field::Intrinsic(Intrinsic::Duration)))) => {
            "min(duration_ns)"
        }
        (AggregateOp::Max, Some(FieldExpr::Field(Field::Intrinsic(Intrinsic::Duration)))) => {
            "max(duration_ns)"
        }
        _ => return None,
    };
    let cmp_sql = match cmp {
        ComparisonOp::Eq => "=",
        ComparisonOp::Neq => "!=",
        ComparisonOp::Gt => ">",
        ComparisonOp::Gte => ">=",
        ComparisonOp::Lt => "<",
        ComparisonOp::Lte => "<=",
        // The planner answers 400 for a regex aggregate comparison
        // before a chain is ever built; refusing here as well means this
        // renderer never depends on that having happened.
        ComparisonOp::Re | ComparisonOp::Nre => return None,
    };
    // The threshold is read from the stage's LEXEME, not from the
    // evaluator's `f64`, and the read refuses whenever an `Int64`
    // comparison here and the evaluator's `f64` one could put a span on
    // different sides of it.
    let threshold = super::search_plan::exact_aggregate_threshold(*op, field, value)?;
    Some(format!("{agg} {cmp_sql} {threshold}"))
}

// ---------------------------------------------------------------------
// The dispatchers
// ---------------------------------------------------------------------

macro_rules! dispatchers {
    ($($name:ident),+ $(,)?) => {
        $(
            #[derive(Debug)]
            pub struct $name;
        )+
    };
}

dispatchers!(
    SourceLower,
    Phase2ReadLower,
    TraceLevelLower,
    StructuralLower,
    NestedSetLower,
    BoolTruthLower,
    AggregateLower,
    SelectLower,
    ByLower,
    CoalesceLower,
    NotASearchLinkLower,
    OrderLower,
    LimitLower,
    EmitLower,
);

static SOURCE: SourceLower = SourceLower;
static PHASE2_READ: Phase2ReadLower = Phase2ReadLower;
static TRACE_LEVEL: TraceLevelLower = TraceLevelLower;
static STRUCTURAL: StructuralLower = StructuralLower;
static NESTED_SET: NestedSetLower = NestedSetLower;
static BOOL_TRUTH: BoolTruthLower = BoolTruthLower;
static AGGREGATE: AggregateLower = AggregateLower;
static SELECT: SelectLower = SelectLower;
static BY: ByLower = ByLower;
static COALESCE: CoalesceLower = CoalesceLower;
static NOT_A_SEARCH_LINK: NotASearchLinkLower = NotASearchLinkLower;
static ORDER: OrderLower = OrderLower;
static LIMIT: LimitLower = LimitLower;
static EMIT: EmitLower = EmitLower;

impl Lang for Tql {
    type Stage = TqlLink;
    type Source = TqlSource;
    type ColExpr = String;
    type Shape = TqlShape;
    type Handoff = TqlHandoff;
    type Err = PlanError;

    /// The ONE exhaustive match over the chain-link type. **No `_` arm**:
    /// adding a `PipelineStage` variant fails to compile here.
    fn lower_of(stage: &TqlLink) -> &'static dyn Lower<Tql> {
        match stage {
            TqlLink::Source { .. } => &SOURCE,
            TqlLink::Hydrate
            | TqlLink::Membership(_)
            | TqlLink::AggValues(_)
            | TqlLink::SelectValues(_)
            | TqlLink::EventSet(_) => &PHASE2_READ,
            TqlLink::TraceCtx | TqlLink::ChildCount => &TRACE_LEVEL,
            TqlLink::Structural => &STRUCTURAL,
            TqlLink::NestedSet => &NESTED_SET,
            TqlLink::BoolTruth => &BOOL_TRUTH,
            TqlLink::Pipe(p) => match p {
                PipelineStage::Aggregate { .. } => &AGGREGATE,
                PipelineStage::Select { .. } => &SELECT,
                PipelineStage::By { .. } => &BY,
                PipelineStage::Coalesce => &COALESCE,
                // Not chain links on the search route: the planner
                // answers 400 for all three, so the chain builder never
                // constructs one. They are dispatched rather than
                // wildcarded so the match stays exhaustive.
                PipelineStage::Metric(_)
                | PipelineStage::MetricSecondStage(_)
                | PipelineStage::Compare { .. } => &NOT_A_SEARCH_LINK,
            },
            TqlLink::Order => &ORDER,
            TqlLink::Limit(_) => &LIMIT,
            TqlLink::Emit => &EMIT,
        }
    }

    /// Every phase-2 read and the winners' root read are over a source
    /// the seed statement did not read, which is exactly what the core
    /// recognises as a source handoff. Naming them is what gives each
    /// part its own name on the explain surface.
    fn source_of(stage: &TqlLink, rel: &Relation<Tql>) -> SourceRef {
        match stage {
            TqlLink::Hydrate => TRACE_SPANS_HYDRATION,
            TqlLink::Membership(_) => TRACE_ATTRS_MEMBERSHIP,
            TqlLink::AggValues(_) | TqlLink::SelectValues(_) => TRACE_ATTRS_VALUES,
            TqlLink::EventSet(_) => TRACE_ATTRS_EVENT_SETS,
            TqlLink::TraceCtx => TRACE_SPANS_CTX,
            TqlLink::ChildCount => TRACE_SPANS_CHILD_COUNT,
            TqlLink::Emit => TRACE_SPANS_ROOT,
            TqlLink::Source { .. }
            | TqlLink::Structural
            | TqlLink::NestedSet
            | TqlLink::BoolTruth
            | TqlLink::Pipe(_)
            | TqlLink::Order
            | TqlLink::Limit(_) => rel.source_ref(),
        }
    }

    fn handoff_key(stage: &TqlLink, _rel: &Relation<Tql>) -> Option<Name> {
        match stage {
            TqlLink::Hydrate
            | TqlLink::Membership(_)
            | TqlLink::AggValues(_)
            | TqlLink::SelectValues(_)
            | TqlLink::EventSet(_)
            | TqlLink::TraceCtx
            | TqlLink::ChildCount
            | TqlLink::Emit => Some(Name::from(TRACE_ID)),
            TqlLink::Source { .. }
            | TqlLink::Structural
            | TqlLink::NestedSet
            | TqlLink::BoolTruth
            | TqlLink::Pipe(_)
            | TqlLink::Order
            | TqlLink::Limit(_) => None,
        }
    }

    /// Two bounds, and they are different facts.
    ///
    /// Phase 2 is seeded from the phase-1 candidate list, whose size is
    /// bounded by `reader.traceql_max_candidates` — a config field, and
    /// the number the core reads from
    /// [`crate::compile::plan::PlanConfig::seed_bound_rows`].
    ///
    /// The winners' root read is seeded from the traces that WON, so its
    /// bound is the request's own `limit`. That difference is what makes
    /// the root read the one statement the inexact-limit page loop must
    /// not be attached to (D4).
    fn handoff_bound(stage: &TqlLink, _rel: &Relation<Tql>, cx: &PlanCx<'_>) -> Option<SeedBound> {
        match stage {
            TqlLink::Hydrate
            | TqlLink::Membership(_)
            | TqlLink::AggValues(_)
            | TqlLink::SelectValues(_)
            | TqlLink::EventSet(_)
            | TqlLink::TraceCtx
            | TqlLink::ChildCount => Some(SeedBound::Config {
                name: MAX_CANDIDATES_CONFIG,
                value: cx.config.seed_bound_rows?,
            }),
            TqlLink::Emit => Some(SeedBound::RequestLimit(cx.bounds.limit?)),
            TqlLink::Source { .. }
            | TqlLink::Structural
            | TqlLink::NestedSet
            | TqlLink::BoolTruth
            | TqlLink::Pipe(_)
            | TqlLink::Order
            | TqlLink::Limit(_) => None,
        }
    }

    /// A trace id renders as `unhex('<32 hex>')` inside an `IN (…)` list:
    /// 41 characters plus `", "`, and two AST elements — the call and its
    /// string literal. The 48-byte constant is the
    /// `trace_id IN ()` frame.
    fn handoff_cost(n: u64) -> HandoffCost {
        HandoffCost {
            text_bytes: 48 + n * 43,
            ast_elements: 4 + n * 2,
        }
    }
}

// ---------------------------------------------------------------------
// Per-link rules
// ---------------------------------------------------------------------

impl Lower<Tql> for SourceLower {
    /// Always lowers, possibly partially: an unlowerable leaf contributes
    /// `1` and clears `exact`.
    fn capability(&self, _s: &TqlLink, _rel: &Relation<Tql>) -> Capability {
        Capability::Yes
    }
    fn apply(
        &self,
        _s: &TqlLink,
        rel: Relation<Tql>,
        _cx: &LowerCx<'_, Tql>,
    ) -> Result<Relation<Tql>, PlanError> {
        Ok(rel)
    }
    /// The seed is always applied, so there is no residual case: the
    /// effect is the identity, and this row asserts that rather than
    /// leaving the exemption silent.
    fn residual_effect(&self, _s: &TqlLink, rel: Relation<Tql>) -> Relation<Tql> {
        rel
    }
    /// **Two conditions, and BOTH must hold**, because they rule out
    /// different ways the SQL can mean something other than the selector.
    ///
    /// * No leaf may be a regex — see [`StrOpKind::fidelity`], which this
    ///   delegates to for every leaf. One pattern gets two readings on
    ///   this path and the SQL reading is the narrower one.
    /// * The phase-1 generator set must be exactly the selector's match
    ///   set. It is a documented superset today (see
    ///   [`TqlLink::Source::generator_is_exact`]), so a regex-free
    ///   multi-leaf selector generating on one conjunct would otherwise
    ///   be called `Equivalent` — and `Equivalent` where `Wider` is owed
    ///   inverts `orig ⟹ sql` into `orig ⟺ sql`, which licenses the
    ///   evaluator to skip re-filtering and DROPS rows (issue #492 part
    ///   3, D3).
    fn fidelity(&self, s: &TqlLink, _rel: &Relation<Tql>) -> Fidelity {
        match s {
            TqlLink::Source {
                expr,
                generator_is_exact,
            } => {
                if *generator_is_exact {
                    selector_fidelity(expr)
                } else {
                    Fidelity::Wider
                }
            }
            _ => Fidelity::Wider,
        }
    }
}

impl Lower<Tql> for Phase2ReadLower {
    /// A second statement, not a second clause: the read is over a
    /// different source keyed by this statement's result, which is a
    /// source handoff and never a fold into the seed's `WHERE`. No SQL
    /// form has been written that would put it INTO the seed statement,
    /// which is what `NotYetLowered` says.
    fn capability(&self, _s: &TqlLink, _rel: &Relation<Tql>) -> Capability {
        Capability::No(BlockReason::NotYetLowered)
    }
    fn apply(
        &self,
        _s: &TqlLink,
        rel: Relation<Tql>,
        _cx: &LowerCx<'_, Tql>,
    ) -> Result<Relation<Tql>, PlanError> {
        Ok(rel)
    }
    /// No state effect. The read adds rows the evaluator consults; it
    /// rewrites no column's provenance and narrows no predicate, so the
    /// identity is the whole effect and this row asserts it rather than
    /// leaving the exemption silent.
    fn residual_effect(&self, _s: &TqlLink, rel: Relation<Tql>) -> Relation<Tql> {
        rel
    }
}

impl Lower<Tql> for TraceLevelLower {
    /// `Never`, and the reason is the co-load's REACH, not a missing SQL
    /// form: the trace-context and child-count reads are deliberately
    /// trace-wide and unwindowed, so the trace-level intrinsics evaluate
    /// full-trace-exact regardless of the search window. A
    /// window-bounded statement cannot read those rows, in any state.
    fn capability(&self, _s: &TqlLink, _rel: &Relation<Tql>) -> Capability {
        Capability::Never(NeverReason::TraceLevelIntrinsic)
    }
    fn apply(
        &self,
        _s: &TqlLink,
        rel: Relation<Tql>,
        _cx: &LowerCx<'_, Tql>,
    ) -> Result<Relation<Tql>, PlanError> {
        Ok(rel)
    }
    fn residual_effect(&self, _s: &TqlLink, rel: Relation<Tql>) -> Relation<Tql> {
        rel
    }
}

impl Lower<Tql> for StructuralLower {
    /// `Never`: the relation holds between two spans of one trace, over
    /// a span set our own batching defines. Nothing in the seed
    /// statement's row scope can decide it.
    fn capability(&self, _s: &TqlLink, _rel: &Relation<Tql>) -> Capability {
        Capability::Never(NeverReason::StructuralRelation)
    }
    fn apply(
        &self,
        _s: &TqlLink,
        rel: Relation<Tql>,
        _cx: &LowerCx<'_, Tql>,
    ) -> Result<Relation<Tql>, PlanError> {
        Ok(rel)
    }
    /// Clears `exact`: the generators are the superset union of both
    /// operands' sets and the relation is applied afterwards, so the SQL
    /// means strictly more than the query.
    fn residual_effect(&self, _s: &TqlLink, mut rel: Relation<Tql>) -> Relation<Tql> {
        rel.exact = false;
        rel
    }
}

impl Lower<Tql> for NestedSetLower {
    /// `Never`: a modified-preorder numbering computed per trace at query
    /// time; no stored column carries it.
    fn capability(&self, _s: &TqlLink, _rel: &Relation<Tql>) -> Capability {
        Capability::Never(NeverReason::NestedSetNumbering)
    }
    fn apply(
        &self,
        _s: &TqlLink,
        rel: Relation<Tql>,
        _cx: &LowerCx<'_, Tql>,
    ) -> Result<Relation<Tql>, PlanError> {
        Ok(rel)
    }
    fn residual_effect(&self, _s: &TqlLink, mut rel: Relation<Tql>) -> Relation<Tql> {
        rel.exact = false;
        rel
    }
}

impl Lower<Tql> for BoolTruthLower {
    /// `Never`: one row's type must fail the WHOLE request — a present
    /// non-boolean operand under `!` is an error for the query, not a
    /// non-match for the span — and SQL evaluates row by row.
    fn capability(&self, _s: &TqlLink, _rel: &Relation<Tql>) -> Capability {
        Capability::Never(NeverReason::WholeQueryTypeFailure)
    }
    fn apply(
        &self,
        _s: &TqlLink,
        rel: Relation<Tql>,
        _cx: &LowerCx<'_, Tql>,
    ) -> Result<Relation<Tql>, PlanError> {
        Ok(rel)
    }
    fn residual_effect(&self, _s: &TqlLink, mut rel: Relation<Tql>) -> Relation<Tql> {
        rel.exact = false;
        rel
    }
}

impl Lower<Tql> for AggregateLower {
    /// `exact` **and** `grouping.is_none()` **and** a fragment
    /// [`aggregate_having_sql`] will render.
    ///
    /// An aggregate over a superset is not merely wide, it is wrong:
    /// `max()` can exceed the true maximum and admit a trace that should
    /// not match, `min()` errs the other way, and `count()` inflates.
    /// Measured, the cost of getting this wrong is 333 qualifying traces
    /// becoming 1,000.
    ///
    /// The fourth condition is issue #492 part 4's: the aggregate
    /// families whose value a duplicated index row moves — `sum` and
    /// `avg` — and an attribute source and a non-integral threshold get
    /// `No(NotYetLowered)`, because a SQL form for them exists in
    /// principle and none has been written that is safe over this index.
    fn capability(&self, s: &TqlLink, rel: &Relation<Tql>) -> Capability {
        if rel.shape != TqlShape::Spans {
            return Capability::No(BlockReason::ShapeMismatch);
        }
        if !rel.exact {
            return Capability::No(BlockReason::NotExact);
        }
        if rel.grouping.is_some() {
            return Capability::No(BlockReason::ShapeMismatch);
        }
        let TqlLink::Pipe(stage) = s else {
            return Capability::No(BlockReason::NotYetLowered);
        };
        if aggregate_having_sql(stage).is_none() {
            return Capability::No(BlockReason::NotYetLowered);
        }
        Capability::Yes
    }
    /// Pushes the fragment onto `rel.having`. The relation is what
    /// `plan_search` reads back to render the same text into the
    /// generator statement.
    fn apply(
        &self,
        s: &TqlLink,
        mut rel: Relation<Tql>,
        _cx: &LowerCx<'_, Tql>,
    ) -> Result<Relation<Tql>, PlanError> {
        if let TqlLink::Pipe(stage) = s
            && let Some(frag) = aggregate_having_sql(stage)
        {
            rel.having.push(frag);
        }
        Ok(rel)
    }
    /// [`Fidelity::Equivalent`]: `capability` admits the link only when
    /// the accumulated relation is already `exact` — the generator's
    /// rows are the selector's matched spans — and the fragment
    /// aggregates the same column over the same set the evaluator does.
    /// So `orig ⟺ sql` and not merely `orig ⟹ sql`.
    ///
    /// The evaluator still recomputes the aggregate, because the
    /// response's spanSet `attributes` carries its VALUE (issue #510);
    /// what `Equivalent` says is that no trace the SQL returned will be
    /// dropped by that recomputation, which is what keeps `exact` set
    /// for the links after it.
    fn fidelity(&self, _s: &TqlLink, _rel: &Relation<Tql>) -> Fidelity {
        Fidelity::Equivalent
    }
    /// **Shape unchanged** — whatever the fold has accumulated, not reset
    /// to `Spans`; **clears `exact`**, because the evaluator will drop
    /// traces the SQL returned.
    fn residual_effect(&self, _s: &TqlLink, mut rel: Relation<Tql>) -> Relation<Tql> {
        rel.exact = false;
        rel
    }
}

impl Lower<Tql> for ByLower {
    fn capability(&self, _s: &TqlLink, rel: &Relation<Tql>) -> Capability {
        if rel.shape != TqlShape::Spans {
            return Capability::No(BlockReason::ShapeMismatch);
        }
        if !rel.exact {
            return Capability::No(BlockReason::NotExact);
        }
        if rel.grouping.is_some() {
            return Capability::No(BlockReason::ShapeMismatch);
        }
        Capability::No(BlockReason::NotYetLowered)
    }
    fn apply(
        &self,
        _s: &TqlLink,
        rel: Relation<Tql>,
        _cx: &LowerCx<'_, Tql>,
    ) -> Result<Relation<Tql>, PlanError> {
        Ok(rel)
    }
    /// **Shape unchanged**; clears `exact`; records the key as an
    /// evaluator-owned group consumer, so a later `Aggregate` refuses.
    fn residual_effect(&self, s: &TqlLink, mut rel: Relation<Tql>) -> Relation<Tql> {
        if let TqlLink::Pipe(PipelineStage::By { key }) = s {
            rel.cols
                .set_provenance(&Name::new(key.to_string()), Provenance::EvaluatorOnly);
        }
        rel.exact = false;
        rel
    }
}

impl Lower<Tql> for CoalesceLower {
    /// Two rows in one dispatcher, and one rule rather than two special
    /// cases: with a preceding `By` the grouping slot is occupied and
    /// lowering means WRAPPING; with none it is the identity and costs
    /// nothing.
    fn capability(&self, _s: &TqlLink, rel: &Relation<Tql>) -> Capability {
        if rel.grouping.is_none() {
            // The identity: it contributes no SQL and cannot fail.
            return Capability::Yes;
        }
        Capability::No(BlockReason::NotYetLowered)
    }
    fn apply(
        &self,
        _s: &TqlLink,
        rel: Relation<Tql>,
        _cx: &LowerCx<'_, Tql>,
    ) -> Result<Relation<Tql>, PlanError> {
        Ok(rel)
    }
    /// Grouped: shape unchanged, clears `exact`. Ungrouped: the identity,
    /// and this dispatcher is where that exemption is checked rather than
    /// assumed.
    fn residual_effect(&self, _s: &TqlLink, mut rel: Relation<Tql>) -> Relation<Tql> {
        if rel.grouping.is_some() {
            rel.exact = false;
        }
        rel
    }
    fn fidelity(&self, _s: &TqlLink, _rel: &Relation<Tql>) -> Fidelity {
        Fidelity::Equivalent
    }
}

impl Lower<Tql> for SelectLower {
    /// **No exactness precondition** — projecting a column onto rows the
    /// evaluator will drop is harmless.
    fn capability(&self, s: &TqlLink, rel: &Relation<Tql>) -> Capability {
        if let TqlLink::Pipe(PipelineStage::Select { fields }) = s {
            for f in fields {
                if rel.cols.resolve(&Name::new(f.to_string())).is_none() {
                    return Capability::No(BlockReason::NameNotResolvable);
                }
            }
        }
        Capability::No(BlockReason::NotYetLowered)
    }
    fn apply(
        &self,
        _s: &TqlLink,
        rel: Relation<Tql>,
        _cx: &LowerCx<'_, Tql>,
    ) -> Result<Relation<Tql>, PlanError> {
        Ok(rel)
    }
    /// `cols` unchanged — no existing column moves — and the selected
    /// fields become an evaluator-owned projection.
    fn residual_effect(&self, s: &TqlLink, mut rel: Relation<Tql>) -> Relation<Tql> {
        if let TqlLink::Pipe(PipelineStage::Select { fields }) = s {
            for f in fields {
                rel.cols
                    .set_provenance(&Name::new(f.to_string()), Provenance::EvaluatorOnly);
            }
        }
        rel
    }
}

impl Lower<Tql> for NotASearchLinkLower {
    /// Unreachable by construction: the chain builder never makes one,
    /// because the shipped planner answers `400` for all three metrics
    /// stages on the search route. Classified `Never` so that nobody
    /// later reads it as unfinished work.
    fn capability(&self, _s: &TqlLink, _rel: &Relation<Tql>) -> Capability {
        Capability::Never(NeverReason::NotASearchLink)
    }
    fn apply(
        &self,
        _s: &TqlLink,
        rel: Relation<Tql>,
        _cx: &LowerCx<'_, Tql>,
    ) -> Result<Relation<Tql>, PlanError> {
        Ok(rel)
    }
    fn residual_effect(&self, _s: &TqlLink, rel: Relation<Tql>) -> Relation<Tql> {
        rel
    }
}

impl Lower<Tql> for OrderLower {
    /// `exact` — and this is the part easy to miss. The TraceQL sort key
    /// is `max(matched-span timestamp)`, so over a superset the **order**
    /// is wrong, not just the set. LogQL's `Order` does NOT inherit this
    /// precondition, which is why it is per-link rather than global.
    fn capability(&self, _s: &TqlLink, rel: &Relation<Tql>) -> Capability {
        if !rel.exact {
            return Capability::No(BlockReason::NotExact);
        }
        Capability::No(BlockReason::NotYetLowered)
    }
    fn apply(
        &self,
        _s: &TqlLink,
        rel: Relation<Tql>,
        _cx: &LowerCx<'_, Tql>,
    ) -> Result<Relation<Tql>, PlanError> {
        Ok(rel)
    }
    fn residual_effect(&self, _s: &TqlLink, mut rel: Relation<Tql>) -> Relation<Tql> {
        rel.ordering = None;
        rel
    }
}

impl Lower<Tql> for LimitLower {
    fn capability(&self, _s: &TqlLink, rel: &Relation<Tql>) -> Capability {
        if rel.ordering.is_none() {
            return Capability::No(BlockReason::OrderingNotEstablished);
        }
        Capability::No(BlockReason::NotYetLowered)
    }
    fn apply(
        &self,
        s: &TqlLink,
        mut rel: Relation<Tql>,
        _cx: &LowerCx<'_, Tql>,
    ) -> Result<Relation<Tql>, PlanError> {
        if let TqlLink::Limit(n) = s {
            rel.limit = Some(u64::from(*n));
        }
        Ok(rel)
    }
    fn residual_effect(&self, _s: &TqlLink, mut rel: Relation<Tql>) -> Relation<Tql> {
        rel.limit = None;
        rel
    }
}

impl Lower<Tql> for EmitLower {
    fn capability(&self, _s: &TqlLink, _rel: &Relation<Tql>) -> Capability {
        Capability::Never(NeverReason::NeedsUnwindowedRootRead)
    }
    fn apply(
        &self,
        _s: &TqlLink,
        rel: Relation<Tql>,
        _cx: &LowerCx<'_, Tql>,
    ) -> Result<Relation<Tql>, PlanError> {
        Ok(rel)
    }
    /// Records the winners' root read as the evaluator's — and the way
    /// the evaluator owns it is to send a second statement, which is what
    /// the plan builder turns this into.
    fn residual_effect(&self, _s: &TqlLink, mut rel: Relation<Tql>) -> Relation<Tql> {
        rel.cols
            .set_provenance(&Name::from("root"), Provenance::EvaluatorOnly);
        rel
    }
}

/// The seed relation a TraceQL chain folds from.
///
/// `predicate` is the phase-1 generator set expressed in the core's own
/// lattice — one leaf per generator, tagged with the table that generator
/// reads — so the plan builder can recognise a disjunction whose sides
/// live in different tables and give each its own statement. Building it
/// is [`generator_pred`]'s job.
pub fn seed_relation(source: SourceRef, predicate: Pred) -> Relation<Tql> {
    Relation {
        source: SourceTerm::Base(TqlSource(source)),
        predicate,
        projection: vec![(Name::from(TRACE_ID), TRACE_ID.to_string())],
        cols: ColSet::Closed(vec![
            Col {
                name: Name::from(TRACE_ID),
                provenance: Provenance::Stored,
            },
            Col {
                name: Name::from("name"),
                provenance: Provenance::Stored,
            },
        ]),
        grouping: None,
        ordering: None,
        limit: None,
        shape: TqlShape::Spans,
        exact: true,
        depth: 0,
        having: Vec::new(),
    }
}

// ---------------------------------------------------------------------
// The chain builder
// ---------------------------------------------------------------------

/// The table a phase-1 generator reads, as a [`SourceRef`].
pub fn generator_source(table: GenTable) -> SourceRef {
    match table {
        GenTable::Spans => TRACE_SPANS,
        GenTable::Attrs => TRACE_ATTRS_IDX,
    }
}

/// The phase-1 generator set as one predicate: a disjunction of one leaf
/// per generator, each carrying the table it reads.
///
/// **A disjunction, because that is what the generators mean.** A
/// candidate qualifies if ANY generator returned it — `filter::collect`'s
/// rule for `a || b` is that both sides' sets are needed, and a
/// cross-spanset `{A} && {B}` takes the union of both operands' sets
/// too — so an `OR` is the honest lattice reading and it is what lets
/// `Pred::disjoint_or_branches` find the sides that cannot share one
/// `WHERE`.
///
/// A generator with neither a `PREWHERE` nor a `WHERE` fragment (the
/// time-range superset) contributes the literal `1`. It must contribute a
/// LEAF and not [`Pred::True`], because `Pred::True` carries no source
/// and the conjunctive spine drops it: a branch that vanished could not
/// be keyed on its table, and the partition would silently lose a
/// statement.
pub fn generator_pred(gens: &[(SourceRef, LeafGenerator)]) -> Pred {
    let mut out: Option<Pred> = None;
    for (source, g) in gens {
        let mut frag = String::new();
        if let Some(pw) = &g.prewhere {
            frag.push_str(pw);
        }
        if !g.predicate.is_empty() {
            if !frag.is_empty() {
                frag.push_str(" AND ");
            }
            frag.push_str(&g.predicate);
        }
        if frag.is_empty() {
            frag.push('1');
        }
        let leaf = Pred::leaf(frag, *source);
        out = Some(match out {
            None => leaf,
            Some(acc) => acc.or(leaf),
        });
    }
    out.unwrap_or(Pred::True)
}

/// What the chain builder reads off the half-built plan.
///
/// A struct rather than `&SearchPlan` because the chain is built before
/// the plan value exists — the counters it reads are the vectors
/// `plan_search` is still filling.
#[derive(Debug, Clone, Copy)]
pub struct ChainFacts<'a> {
    /// The DEDUPED phase-1 generators, index-aligned with
    /// `SearchPlan::generator_sqls`. Read by part 4, which owes
    /// [`TqlLink::Source::generator_is_exact`] its computation; part 3
    /// carries them so the chain is built beside the statements rather
    /// than beside a second derivation of them.
    pub generators: &'a [(SourceRef, LeafGenerator)],
    pub probes: usize,
    pub agg_fields: usize,
    pub select_attrs: usize,
    pub event_sets: usize,
    pub trace_ctx: bool,
    pub child_count: bool,
    pub nested_set: bool,
    pub structural: bool,
    pub bool_truth_leaves: usize,
}

/// The chain, in the order the executor issues its statements: the
/// generators, then the per-batch reads, then the engine work
/// `search_eval::evaluate_batch` does over the hydrated batch, then the
/// pipeline fold, then the winners' root read.
///
/// **The `by()` cardinality pre-flight probe is deliberately not a
/// link.** It is an admission check that runs before phase 1 and answers
/// `422` without reading a result row; it produces no candidate and
/// consumes none, so it is not a stage of the query's evaluation.
///
/// **The three metrics `PipelineStage` variants cannot appear here**:
/// `search_plan::plan_pipeline` answers `400` for all three and runs
/// before this, so a chain is only ever built for a query the shipped
/// planner accepts. Nothing here widens what a query may mean.
///
/// `chain.len()` is an identity of the counters above plus three — one
/// `Source`, one `Hydrate`, and `Order`/`Limit`/`Emit` — which is the
/// scale-invariant form of "this adds no per-row work".
///
/// `generator_is_exact` is computed by
/// [`super::search_plan::generator_exactness`] and passed in rather than
/// derived here: the comparison it makes is between two rendered
/// predicates, and only the planner holds both.
pub fn chain_of(
    query: &Query,
    generator_is_exact: bool,
    facts: &ChainFacts<'_>,
    limit: u32,
) -> Vec<TqlLink> {
    let mut chain = Vec::with_capacity(
        3 + 1
            + facts.probes
            + facts.agg_fields
            + facts.select_attrs
            + facts.event_sets
            + usize::from(facts.trace_ctx)
            + usize::from(facts.child_count)
            + usize::from(facts.structural)
            + usize::from(facts.nested_set)
            + facts.bool_truth_leaves
            + query.pipeline.len(),
    );
    chain.push(TqlLink::Source {
        expr: Box::new(query.spanset.clone()),
        generator_is_exact,
    });
    chain.push(TqlLink::Hydrate);
    for i in 0..facts.probes {
        chain.push(TqlLink::Membership(i));
    }
    for i in 0..facts.agg_fields {
        chain.push(TqlLink::AggValues(i));
    }
    for i in 0..facts.select_attrs {
        chain.push(TqlLink::SelectValues(i));
    }
    for i in 0..facts.event_sets {
        chain.push(TqlLink::EventSet(i));
    }
    if facts.trace_ctx {
        chain.push(TqlLink::TraceCtx);
    }
    if facts.child_count {
        chain.push(TqlLink::ChildCount);
    }
    if facts.structural {
        chain.push(TqlLink::Structural);
    }
    if facts.nested_set {
        chain.push(TqlLink::NestedSet);
    }
    for _ in 0..facts.bool_truth_leaves {
        chain.push(TqlLink::BoolTruth);
    }
    for stage in &query.pipeline {
        chain.push(TqlLink::Pipe(stage.clone()));
    }
    chain.push(TqlLink::Order);
    chain.push(TqlLink::Limit(limit));
    chain.push(TqlLink::Emit);
    chain
}

/// The language's own spelling of each chain link, in chain order — the
/// core has no way to name an `L::Stage`, so it takes these as a
/// parameter to `QueryPlan::shape`.
pub fn stage_names(chain: &[TqlLink]) -> Vec<String> {
    chain.iter().map(stage_name).collect()
}

fn stage_name(link: &TqlLink) -> String {
    match link {
        TqlLink::Source { .. } => "Source".to_string(),
        TqlLink::Hydrate => "Hydrate".to_string(),
        TqlLink::Membership(i) => format!("Membership({i})"),
        TqlLink::AggValues(i) => format!("AggValues({i})"),
        TqlLink::SelectValues(i) => format!("SelectValues({i})"),
        TqlLink::EventSet(i) => format!("EventSet({i})"),
        TqlLink::TraceCtx => "TraceCtx".to_string(),
        TqlLink::ChildCount => "ChildCount".to_string(),
        TqlLink::Structural => "Structural".to_string(),
        TqlLink::NestedSet => "NestedSet".to_string(),
        TqlLink::BoolTruth => "BoolTruth".to_string(),
        TqlLink::Pipe(p) => format!("Pipe({})", pipe_name(p)),
        TqlLink::Order => "Order".to_string(),
        TqlLink::Limit(n) => format!("Limit({n})"),
        TqlLink::Emit => "Emit".to_string(),
    }
}

/// The stage's KIND, not its payload: the explain surface names what ran,
/// and a user's own literals are already in the query they sent.
fn pipe_name(stage: &PipelineStage) -> &'static str {
    match stage {
        PipelineStage::Aggregate { .. } => "Aggregate",
        PipelineStage::Select { .. } => "Select",
        PipelineStage::By { .. } => "By",
        PipelineStage::Coalesce => "Coalesce",
        PipelineStage::Metric(_) => "Metric",
        PipelineStage::MetricSecondStage(_) => "MetricSecondStage",
        PipelineStage::Compare { .. } => "Compare",
    }
}

// ---------------------------------------------------------------------
// Gates
// ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::super::search_plan::StrOp;
    use super::*;
    use crate::compile::fold::{Grouping, Ordering, SortDir, SqlExpr};
    use crate::compile::plan::{Cut, Driver, Issue, Part, PlanConfig, plan_of};
    use crate::compile::testkit::{EffectRow, assert_every_residual_state_effect};
    use pulsus_traceql::{AggregateOp, Field, Value};

    fn parse_selector(q: &str) -> SpansetExpr {
        pulsus_traceql::parse(q)
            .unwrap_or_else(|e| panic!("{q}: {e}"))
            .spanset
    }

    fn pipe(q: &str) -> PipelineStage {
        pulsus_traceql::parse(q)
            .unwrap_or_else(|e| panic!("{q}: {e}"))
            .pipeline
            .into_iter()
            .next()
            .unwrap_or_else(|| panic!("{q} has no pipeline stage"))
    }

    /// The search planner's compiled string comparison, classified.
    /// **No `_` arm**: adding a `StrOp` variant fails to build this
    /// binary, which `cargo test --workspace` builds.
    fn kind_of(op: &StrOp) -> StrOpKind {
        match op {
            StrOp::Eq => StrOpKind::Eq,
            StrOp::Neq => StrOpKind::Neq,
            StrOp::Re(_) => StrOpKind::Re,
            StrOp::Nre(_) => StrOpKind::Nre,
        }
    }

    /// Issue #492 acceptance criterion 12, hermetic half: **no regex leaf
    /// claims `Fidelity::Equivalent`.**
    ///
    /// The `match` in [`kind_of`] has no `_` arm, so a new `StrOp`
    /// variant fails to build this test rather than slipping through
    /// unclassified. The two regex arms assert the LITERAL
    /// `Fidelity::Wider` rather than a value the rule under test
    /// produced.
    #[test]
    fn no_regex_leaf_claims_equivalent_fidelity() {
        let re = pulsus_re2::compile_user_regex_anchored(r"\d").expect("test pattern compiles");
        for op in [StrOp::Eq, StrOp::Neq, StrOp::Re(re.clone()), StrOp::Nre(re)] {
            let kind = kind_of(&op);
            match kind {
                StrOpKind::Eq | StrOpKind::Neq => {
                    assert_eq!(kind.fidelity(), Fidelity::Equivalent, "{kind:?}");
                }
                StrOpKind::Re | StrOpKind::Nre => {
                    assert_eq!(
                        kind.fidelity(),
                        Fidelity::Wider,
                        "{kind:?}: a regex leaf is read by TWO engines with different dialects, \
                         and the SQL reading is the NARROWER one — claiming equivalence would \
                         invert the lattice invariant and DROP rows"
                    );
                }
            }
        }

        // The same rule as the selector link sees it: a `=~` anywhere in
        // the selector makes the whole link `Wider`.
        for (q, want) in [
            (
                r#"{ resource.service.name = "checkout" }"#,
                Fidelity::Equivalent,
            ),
            (r#"{ name =~ "\\d" }"#, Fidelity::Wider),
            (r#"{ name !~ "\\d" }"#, Fidelity::Wider),
            (
                r#"{ resource.service.name = "checkout" && name =~ "a.*" }"#,
                Fidelity::Wider,
            ),
            (
                r#"{ resource.service.name = "checkout" || span.foo =~ "b" }"#,
                Fidelity::Wider,
            ),
        ] {
            assert_eq!(selector_fidelity(&parse_selector(q)), want, "{q}");
        }

        // And through the fold, which is where the verdict has its
        // consequence: a `Wider` link clears `exact`, so no later link
        // that needs exactness can lower behind a regex leaf.
        //
        // **Both conditions are varied, because the link takes both**
        // (issue #492 part 3, D3). The regex rows are run with
        // `generator_is_exact: true` as well, so they are not passing on
        // the flag alone: a regex leaf must still clear `exact` when the
        // generator IS exact, which is the only row that can tell the
        // selector rule from the flag. The two `generator_is_exact:
        // false` rows are the value production sets today, and the first
        // of them is the one that used to answer `Equivalent` over a
        // documented superset.
        let bounds = crate::compile::fold::RequestBounds {
            start_ns: 0,
            end_ns: 1,
            step_ns: None,
            limit: Some(20),
        };
        let cx = LowerCx::<Tql>::new(&bounds);
        for (q, generator_is_exact, want_exact) in [
            (r#"{ resource.service.name = "checkout" }"#, true, true),
            (r#"{ resource.service.name = "checkout" }"#, false, false),
            (r#"{ name =~ "\\d" }"#, true, false),
            (r#"{ name =~ "\\d" }"#, false, false),
        ] {
            let chain = vec![TqlLink::Source {
                expr: Box::new(parse_selector(q)),
                generator_is_exact,
            }];
            let lowering = crate::compile::fold::lower_chain::<Tql>(
                &chain,
                seed_relation(TRACE_SPANS, Pred::True),
                &cx,
            )
            .expect("fold");
            assert_eq!(
                lowering.rel.exact, want_exact,
                "{q} with generator_is_exact={generator_is_exact}"
            );
        }
    }

    // --- the residual state effects -----------------------------------

    fn base(shape: TqlShape) -> Relation<Tql> {
        let mut rel = seed_relation(TRACE_SPANS, Pred::True);
        rel.shape = shape;
        rel
    }

    fn grouped(mut rel: Relation<Tql>) -> Relation<Tql> {
        rel.grouping = Some(Grouping {
            keys: vec!["name".to_string()],
        });
        rel
    }

    fn ordered(mut rel: Relation<Tql>, key: &str) -> Relation<Tql> {
        rel.ordering = Some(Ordering {
            keys: vec![(key.to_string(), SortDir::Desc)],
        });
        rel
    }

    fn limited(mut rel: Relation<Tql>, n: u64) -> Relation<Tql> {
        rel.limit = Some(n);
        rel
    }

    fn evaluator_owned(mut rel: Relation<Tql>, name: &str) -> Relation<Tql> {
        rel.cols
            .set_provenance(&Name::new(name), Provenance::EvaluatorOnly);
        rel
    }

    fn not_exact(mut rel: Relation<Tql>) -> Relation<Tql> {
        rel.exact = false;
        rel
    }

    /// Issue #492: every TraceQL link's residual state effect is the one
    /// the design record states, and the two whose stated effect is
    /// *none* assert that the effect IS the identity — so the exemption
    /// is itself a check rather than a silence.
    ///
    /// **Nineteen rows**, one per link kind: ten with a stated effect
    /// (`Aggregate`, `By`, grouped `Coalesce`, `Select`, `Order`,
    /// `Limit`, `Emit`, and issue #492 part 3's `Structural`,
    /// `NestedSet` and `BoolTruth`, which each clear `exact` because the
    /// SQL means strictly more than the query once they are residual)
    /// and nine whose effect is none (`Source`, `Coalesce` with no
    /// preceding `By`, and the seven per-batch reads — five phase-2
    /// statements plus the two trace-wide co-loads — which add rows the
    /// evaluator consults and rewrite no column).
    #[test]
    fn every_residual_state_effect_is_the_one_the_document_states() {
        let sel = parse_selector(r#"{ resource.service.name = "checkout" }"#);
        let agg = pipe(r#"{ .a = "1" } | max(duration) > 1s"#);
        let by = pipe(r#"{ .a = "1" } | by(name)"#);
        let coalesce = pipe(r#"{ .a = "1" } | coalesce()"#);
        let select = pipe(r#"{ .a = "1" } | select(span.http.method)"#);

        // Two seeds per row. They differ in `shape` and in every field
        // the row's effect column names as unchanged or retained; where
        // the effect is only observable on a field with a particular
        // value, BOTH seeds carry that value, so assertion 4 can see the
        // effect on each.
        let rows: Vec<EffectRow<Tql>> = vec![
            EffectRow {
                name: "Source",
                link: TqlLink::Source {
                    expr: Box::new(sel),
                    generator_is_exact: false,
                },
                s1: base(TqlShape::Spans),
                s2: base(TqlShape::Traces),
                e1: base(TqlShape::Spans),
                e2: base(TqlShape::Traces),
                effect_is_constant: false,
                has_effect: false,
            },
            EffectRow {
                name: "Aggregate",
                link: TqlLink::Pipe(agg),
                s1: base(TqlShape::Spans),
                s2: base(TqlShape::Traces),
                e1: not_exact(base(TqlShape::Spans)),
                e2: not_exact(base(TqlShape::Traces)),
                effect_is_constant: false,
                has_effect: true,
            },
            EffectRow {
                name: "By",
                link: TqlLink::Pipe(by),
                s1: base(TqlShape::Spans),
                s2: base(TqlShape::Traces),
                e1: not_exact(evaluator_owned(base(TqlShape::Spans), "name")),
                e2: not_exact(evaluator_owned(base(TqlShape::Traces), "name")),
                effect_is_constant: false,
                has_effect: true,
            },
            EffectRow {
                name: "Coalesce, after a By",
                link: TqlLink::Pipe(coalesce.clone()),
                s1: grouped(base(TqlShape::Spans)),
                s2: grouped(base(TqlShape::Groups(Name::from("name")))),
                e1: not_exact(grouped(base(TqlShape::Spans))),
                e2: not_exact(grouped(base(TqlShape::Groups(Name::from("name"))))),
                effect_is_constant: false,
                has_effect: true,
            },
            EffectRow {
                name: "Coalesce, with no preceding By",
                link: TqlLink::Pipe(coalesce),
                s1: base(TqlShape::Spans),
                s2: base(TqlShape::Traces),
                e1: base(TqlShape::Spans),
                e2: base(TqlShape::Traces),
                effect_is_constant: false,
                has_effect: false,
            },
            EffectRow {
                name: "Select",
                link: TqlLink::Pipe(select),
                s1: base(TqlShape::Spans),
                s2: base(TqlShape::Traces),
                e1: evaluator_owned(base(TqlShape::Spans), "span.http.method"),
                e2: evaluator_owned(base(TqlShape::Traces), "span.http.method"),
                effect_is_constant: false,
                has_effect: true,
            },
            EffectRow {
                name: "Order",
                link: TqlLink::Order,
                s1: ordered(base(TqlShape::Spans), "bound_ts"),
                s2: ordered(base(TqlShape::Traces), "trace_id"),
                e1: base(TqlShape::Spans),
                e2: base(TqlShape::Traces),
                effect_is_constant: false,
                has_effect: true,
            },
            EffectRow {
                name: "Limit",
                link: TqlLink::Limit(20),
                s1: limited(base(TqlShape::Spans), 20),
                s2: limited(base(TqlShape::Traces), 21),
                e1: base(TqlShape::Spans),
                e2: base(TqlShape::Traces),
                effect_is_constant: false,
                has_effect: true,
            },
            EffectRow {
                name: "Emit",
                link: TqlLink::Emit,
                s1: base(TqlShape::Spans),
                s2: base(TqlShape::Traces),
                e1: evaluator_owned(base(TqlShape::Spans), "root"),
                e2: evaluator_owned(base(TqlShape::Traces), "root"),
                effect_is_constant: false,
                has_effect: true,
            },
        ];
        // Issue #492 part 3's ten new links. `Hydrate`, the four indexed
        // phase-2 reads and the two co-loads state NO effect and assert
        // it; the three engine links clear `exact`.
        let mut rows = rows;
        for (name, link) in [
            ("Hydrate", TqlLink::Hydrate),
            ("Membership", TqlLink::Membership(0)),
            ("AggValues", TqlLink::AggValues(0)),
            ("SelectValues", TqlLink::SelectValues(0)),
            ("EventSet", TqlLink::EventSet(0)),
            ("TraceCtx", TqlLink::TraceCtx),
            ("ChildCount", TqlLink::ChildCount),
        ] {
            rows.push(EffectRow {
                name,
                link,
                s1: base(TqlShape::Spans),
                s2: base(TqlShape::Traces),
                e1: base(TqlShape::Spans),
                e2: base(TqlShape::Traces),
                effect_is_constant: false,
                has_effect: false,
            });
        }
        for (name, link) in [
            ("Structural", TqlLink::Structural),
            ("NestedSet", TqlLink::NestedSet),
            ("BoolTruth", TqlLink::BoolTruth),
        ] {
            rows.push(EffectRow {
                name,
                link,
                s1: base(TqlShape::Spans),
                s2: base(TqlShape::Traces),
                e1: not_exact(base(TqlShape::Spans)),
                e2: not_exact(base(TqlShape::Traces)),
                effect_is_constant: false,
                has_effect: true,
            });
        }
        assert_every_residual_state_effect::<Tql>(&rows, 19);
    }

    /// `Emit` is `Never`, AND the plan builder gives it its own SQL part
    /// rather than folding it into the engine part. Those are two
    /// different claims and the second is the one the previous shape of
    /// this design got wrong.
    #[test]
    fn emit_is_never_and_is_served_by_its_own_sql_part() {
        let bounds = crate::compile::fold::RequestBounds {
            start_ns: 0,
            end_ns: 1,
            step_ns: None,
            limit: Some(20),
        };
        let chain = vec![
            TqlLink::Source {
                expr: Box::new(parse_selector(r#"{ resource.service.name = "checkout" }"#)),
                generator_is_exact: false,
            },
            TqlLink::Order,
            TqlLink::Limit(20),
            TqlLink::Emit,
        ];
        let cx = LowerCx::<Tql>::new(&bounds);
        let lowering = crate::compile::fold::lower_chain::<Tql>(
            &chain,
            seed_relation(TRACE_SPANS, Pred::True),
            &cx,
        )
        .expect("fold");
        assert_eq!(
            lowering.how[3],
            crate::compile::fold::Disposition::Residual(
                crate::compile::fold::ResidualReason::Never(NeverReason::NeedsUnwindowedRootRead)
            )
        );

        let config = crate::compile::plan::PlanConfig::default();
        let plan = crate::compile::plan::plan_of::<Tql>(
            &chain,
            lowering,
            &PlanCx {
                bounds: &bounds,
                config: &config,
            },
        )
        .expect("plan");
        let emit_part = plan.links[3].part;
        let crate::compile::plan::Part::Sql(p) = &plan.parts[emit_part] else {
            panic!(
                "Emit must land in an SQL part, not an engine part: {:?}",
                plan.parts
            )
        };
        assert_eq!(
            p.cut,
            Some(crate::compile::plan::Cut::SourceHandoff {
                source: TRACE_SPANS_ROOT,
                key: Name::from(TRACE_ID),
            })
        );
        assert_eq!(
            p.seed.as_ref().map(|s| s.bound),
            Some(SeedBound::RequestLimit(20)),
            "seeded by the winners' trace ids, bounded by the request limit"
        );
        assert_eq!(p.issue, crate::compile::plan::Issue::Once);
        // D1 (issue #492 part 3): the part is NAMED for the source it
        // reads. Before this it rendered the seed's table while its own
        // cut named `trace_spans:root` — the document and the code
        // disagreed and the doc gate could not see it, because that gate
        // compares key SETS and never values.
        assert_eq!(
            p.rel.source_ref(),
            TRACE_SPANS_ROOT,
            "the winners' root read is its own source, not the seed's"
        );
        // D4: and it is issued ONCE. The root read is seeded by the
        // traces that already won, so the request's own limit bounds it
        // and there is no page loop to run.
        assert_eq!(
            p.cut.as_ref().map(crate::compile::plan::Cut::why),
            Some("source_handoff")
        );
    }

    /// Issue #492 part 3, criterion 12: **every [`NeverReason`] variant
    /// has a producing query.**
    ///
    /// Four of the eight had no producer anywhere in the tree until this
    /// part — a variant nothing can construct is documentation shaped
    /// like control flow, and the quiet half of a broken bijection. The
    /// `match` below has **no `_` arm**, so a ninth variant fails to
    /// build this binary rather than joining them.
    ///
    /// **Two of the eight are LogQL's and are witnessed by a LogQL
    /// chain**, not pretended to be TraceQL's: `NoRowToComputeFrom` is
    /// `absent_over_time`'s (the answer is a statement about rows that
    /// are absent) and `ResponseBuild` is that language's `Emit`.
    ///
    /// **`TraceLevelIntrinsic` has TWO producers, and that is
    /// deliberate**: one reason, two trace-wide co-loads. This asserts
    /// coverage of the variant SET, never a bijection with links.
    #[test]
    fn every_never_reason_variant_has_a_producing_query() {
        use crate::compile::fold::{Disposition, NeverReason as N, ResidualReason};

        /// Does any link of this TraceQL query's plan carry the reason?
        fn tql_carries(q: &str, want: N) -> bool {
            let query = pulsus_traceql::parse(q).unwrap_or_else(|e| panic!("{q}: {e}"));
            let params = super::super::search_plan::SearchParams {
                start_ns: 1_700_000_000_000_000_000,
                end_ns: 1_700_010_800_000_000_000,
                limit: 20,
                spss: 3,
            };
            let ctx = super::super::search_plan::SearchCtx {
                filter: super::super::filter::SpanFilterCtx {
                    spans_table: "trace_spans",
                    attrs_table: "trace_attrs_idx",
                },
                max_candidates: 100_000,
                max_series: 1_000,
                distributed: false,
            };
            let plan = super::super::search_plan::plan_search(&query, &params, &ctx)
                .unwrap_or_else(|e| panic!("{q}: {e:?}"));
            plan.compiled()
                .links
                .iter()
                .any(|l| l.how == Disposition::Residual(ResidualReason::Never(want)))
        }

        /// The same question of a LogQL chain.
        fn lql_carries(chain: &[crate::logql::compile::LqlLink], want: N) -> bool {
            let bounds = crate::compile::fold::RequestBounds {
                start_ns: 0,
                end_ns: 1,
                step_ns: None,
                limit: Some(20),
            };
            let cx = LowerCx::<crate::logql::compile::Lql>::new(&bounds);
            let lowering = crate::compile::fold::lower_chain::<crate::logql::compile::Lql>(
                chain,
                crate::logql::compile::seed_relation(),
                &cx,
            )
            .expect("fold");
            lowering
                .how
                .contains(&Disposition::Residual(ResidualReason::Never(want)))
        }

        for variant in [
            N::NeedsUnwindowedRootRead,
            N::StructuralRelation,
            N::NestedSetNumbering,
            N::TraceLevelIntrinsic,
            N::WholeQueryTypeFailure,
            N::NoRowToComputeFrom,
            N::ResponseBuild,
            N::NotASearchLink,
        ] {
            let (witness, carried): (&str, bool) = match variant {
                // Every TraceQL search: the response's root summary is
                // read trace-wide with no time bound.
                N::NeedsUnwindowedRootRead => (
                    r#"{ resource.service.name = "checkout" }"#,
                    tql_carries(r#"{ resource.service.name = "checkout" }"#, variant),
                ),
                N::StructuralRelation => (
                    r#"{ resource.service.name = "checkout" } > { span.foo = "x" }"#,
                    tql_carries(
                        r#"{ resource.service.name = "checkout" } > { span.foo = "x" }"#,
                        variant,
                    ),
                ),
                N::NestedSetNumbering => (
                    "{ nestedSetParent < 0 }",
                    tql_carries("{ nestedSetParent < 0 }", variant),
                ),
                // Two producers, one reason. Both must carry it.
                N::TraceLevelIntrinsic => (
                    "{ traceDuration > 2s } and { span:childCount > 2 }",
                    tql_carries("{ traceDuration > 2s }", variant)
                        && tql_carries("{ span:childCount > 2 }", variant),
                ),
                N::WholeQueryTypeFailure => ("{ !.a }", tql_carries("{ !.a }", variant)),
                N::NoRowToComputeFrom => (
                    r#"absent_over_time({app="a"}[5m])"#,
                    lql_carries(
                        &[crate::logql::compile::LqlLink::RangeAgg {
                            op: pulsus_logql::RangeAggOp::AbsentOverTime,
                            grouping: None,
                            param: None,
                        }],
                        variant,
                    ),
                ),
                N::ResponseBuild => (
                    r#"{app="a"} (every LogQL query)"#,
                    lql_carries(&[crate::logql::compile::LqlLink::Emit], variant),
                ),
                // The one variant no ACCEPTED query can produce, and the
                // arm says so rather than inventing a witness: the
                // shipped planner answers 400 for all three metrics
                // stages on the search route, so `chain_of` never builds
                // the link. Both halves are asserted — the rejection
                // that makes it unreachable, and the classification a
                // hand-built link would get — because either alone would
                // be a claim about the other.
                N::NotASearchLink => {
                    let q = r#"{ } | rate()"#;
                    let query = pulsus_traceql::parse(q).expect("parses");
                    let params = super::super::search_plan::SearchParams {
                        start_ns: 1_700_000_000_000_000_000,
                        end_ns: 1_700_010_800_000_000_000,
                        limit: 20,
                        spss: 3,
                    };
                    let ctx = super::super::search_plan::SearchCtx {
                        filter: super::super::filter::SpanFilterCtx {
                            spans_table: "trace_spans",
                            attrs_table: "trace_attrs_idx",
                        },
                        max_candidates: 100_000,
                        max_series: 1_000,
                        distributed: false,
                    };
                    assert!(
                        super::super::search_plan::plan_search(&query, &params, &ctx).is_err(),
                        "{q}: the search route must refuse a metrics stage, which is what makes \
                         this variant unreachable from a query"
                    );
                    let metric = query.pipeline.first().expect("one stage").clone();
                    let link = TqlLink::Pipe(metric);
                    let carried = matches!(
                        Tql::lower_of(&link).capability(&link, &base(TqlShape::Spans)),
                        Capability::Never(NeverReason::NotASearchLink)
                    );
                    (q, carried)
                }
            };
            assert!(
                carried,
                "{variant:?}: no link in {witness}'s plan carries it"
            );
        }
    }

    /// Issue #492 part 3, criterion 13 (TraceQL's half): **the phase-2
    /// chunk is the batch constant.**
    ///
    /// The hydration read is seeded from the phase-1 candidate list,
    /// which `reader.traceql_max_candidates` bounds at 100,000 here. That
    /// exceeds the database's AST ceiling, so the part is chunked — and
    /// the chunk the executor actually sends is [`super::super::exec::BATCH_TRACES`],
    /// not the largest chunk the ceilings would admit.
    #[test]
    fn the_phase_two_chunk_is_the_batch_constant() {
        let bounds = crate::compile::fold::RequestBounds {
            start_ns: 0,
            end_ns: 1,
            step_ns: None,
            limit: Some(20),
        };
        let chain = vec![
            TqlLink::Source {
                expr: Box::new(parse_selector(r#"{ resource.service.name = "checkout" }"#)),
                generator_is_exact: false,
            },
            TqlLink::Hydrate,
            TqlLink::Order,
            TqlLink::Limit(20),
            TqlLink::Emit,
        ];

        let hydration_issue = |seed_chunk_rows: Option<u32>| -> Issue {
            let cx = LowerCx::<Tql>::new(&bounds);
            let lowering = crate::compile::fold::lower_chain::<Tql>(
                &chain,
                seed_relation(TRACE_SPANS, Pred::True),
                &cx,
            )
            .expect("fold");
            let config = PlanConfig {
                seed_chunk_rows,
                seed_bound_rows: Some(100_000),
                ..PlanConfig::default()
            };
            let plan = plan_of::<Tql>(
                &chain,
                lowering,
                &PlanCx {
                    bounds: &bounds,
                    config: &config,
                },
            )
            .expect("plan");
            // The Hydrate link is chain index 1.
            let Part::Sql(p) = &plan.parts[plan.links[1].part] else {
                panic!("the hydration read must be an SQL part: {:?}", plan.parts)
            };
            assert!(
                matches!(p.cut, Some(Cut::HandoffExceedsBound { .. })),
                "a 100,000-candidate seed does not fit one statement: {:?}",
                p.cut
            );
            p.issue
        };

        assert_eq!(
            hydration_issue(Some(super::super::exec::BATCH_TRACES as u32)),
            Issue::PerSeed(Driver::Chunks {
                bound: 100_000,
                chunk: 32,
            }),
            "the phase-2 chunk is the rendering ceiling (24998), not the batch the executor \
             uses (32)"
        );
        assert_eq!(
            hydration_issue(None),
            Issue::PerSeed(Driver::Chunks {
                bound: 100_000,
                chunk: 24_998,
            }),
            "without the language's own chunk the ceilings decide, and 24998 is what they say"
        );
    }

    /// Issue #492 part 4 criterion 7: **the pushdown refuses every unsafe
    /// shape, and each refusal names the condition that refused it.**
    ///
    /// Three things can stop a spanset aggregate reaching the generator
    /// statement, and the table separates them rather than lumping them
    /// into "not pushed":
    ///
    /// ```text
    ///   Refusal::Renderer   aggregate_having_sql said None — the aggregate
    ///                       family, its source, or its threshold
    ///   Refusal::NotExact   the generator's rows are not the selector's
    ///                       matched spans (which of the six conditions)
    ///   Refusal::FoldState  both of the above are satisfied and the FOLD
    ///                       still refuses: a `by()` before the aggregate
    ///                       has cleared `exact`
    /// ```
    ///
    /// The last row is the one a reader would not predict: `{ .k = "v" }
    /// | by(name) | count() > 2` has an exact generator AND a renderable
    /// fragment, and must still not push, because the aggregate then
    /// filters GROUPS and `uniqExact(span_id)` over the whole trace is a
    /// different question.
    ///
    /// The list is CLOSED at the other end too: over the golden corpus,
    /// `traces_search_sql.rs`'s
    /// `the_generator_having_is_the_fragment_the_plan_recorded` asserts
    /// the set of pushing cases equals a frozen four-name list.
    #[test]
    fn the_pushdown_precondition_refuses_every_unsafe_shape() {
        use super::super::search_plan::NotExact;

        /// What a query's aggregate did, as one value.
        #[derive(Debug, PartialEq, Eq)]
        enum Verdict {
            /// The fragment the generator statement carries.
            Push(String),
            /// `aggregate_having_sql` returned `None`.
            RefusedByRenderer,
            /// The generator is not exactly the selector's match set.
            RefusedNotExact(NotExact),
            /// Renderer and precondition both admit it; the fold does not.
            RefusedByFoldState,
        }

        fn verdict(q: &str) -> Verdict {
            let query = pulsus_traceql::parse(q).unwrap_or_else(|e| panic!("{q}: {e}"));
            let params = super::super::search_plan::SearchParams {
                start_ns: 1_700_000_000_000_000_000,
                end_ns: 1_700_010_800_000_000_000,
                limit: 20,
                spss: 3,
            };
            let ctx = super::super::search_plan::SearchCtx {
                filter: super::super::filter::SpanFilterCtx {
                    spans_table: "trace_spans",
                    attrs_table: "trace_attrs_idx",
                },
                max_candidates: 100_000,
                max_series: 1_000,
                distributed: false,
            };
            let plan = super::super::search_plan::plan_search(&query, &params, &ctx)
                .unwrap_or_else(|e| panic!("{q}: {e:?}"));
            if let Some(frag) = plan.pushed_having() {
                return Verdict::Push(frag.to_string());
            }
            // The renderer alone, read straight off the AST.
            let renders = query
                .pipeline
                .iter()
                .filter(|s| matches!(s, PipelineStage::Aggregate { .. }))
                .all(|s| aggregate_having_sql(s).is_some());
            if !renders {
                return Verdict::RefusedByRenderer;
            }
            match plan.generator_exact() {
                Err(why) => Verdict::RefusedNotExact(why),
                Ok(()) => Verdict::RefusedByFoldState,
            }
        }

        let push = |frag: &str| Verdict::Push(frag.to_string());
        let rows: [(&str, Verdict); 18] = [
            // --- pushed ------------------------------------------------
            (
                r#"{ span.http.method = "GET" } | max(duration) > 1s"#,
                push("max(duration_ns) > 1000000000"),
            ),
            (
                r#"{ span.http.method = "GET" } | min(duration) >= 1s"#,
                push("min(duration_ns) >= 1000000000"),
            ),
            (
                r#"{ span.http.method = "GET" } | count() > 2"#,
                push("uniqExact(span_id) > 2"),
            ),
            // Unscoped: no `scope` term on either side, so the two
            // predicates are byte-equal and condition (5) decides it.
            (
                r#"{ .k = "v" } | max(duration) > 1s"#,
                push("max(duration_ns) > 1000000000"),
            ),
            // A boolean equality renders `val = 'true'`, the same
            // prefix-served form a string equality does.
            (
                r#"{ span.a = true } | max(duration) > 1s"#,
                push("max(duration_ns) > 1000000000"),
            ),
            // --- refused because the generator is not exact ------------
            // An attribute regex compiles to `ValuePred::Regex`, so
            // condition (3) refuses it BEFORE condition (6) is reached.
            // Condition (6) is not thereby redundant — the second half of
            // this test builds the input where it is the one that
            // refuses.
            (
                r#"{ span.http.method =~ "GE.*" } | max(duration) > 1s"#,
                Verdict::RefusedNotExact(NotExact::ValuePredIsNotEquality),
            ),
            // A PHYSICAL regex leaf is refused one condition earlier
            // still: it is not an attribute-membership leaf at all.
            (
                r#"{ name =~ "a.*" } | max(duration) > 1s"#,
                Verdict::RefusedNotExact(NotExact::LeafIsNotAPositiveAttrMatch),
            ),
            (
                r#"{ span.http.method != "GET" } | max(duration) > 1s"#,
                Verdict::RefusedNotExact(NotExact::LeafIsNotAPositiveAttrMatch),
            ),
            (
                "{ span.http.status_code >= 500 } | max(duration) > 1s",
                Verdict::RefusedNotExact(NotExact::ValuePredIsNotEquality),
            ),
            (
                r#"{ resource.service.name = "checkout" } | max(duration) > 1s"#,
                Verdict::RefusedNotExact(NotExact::LeafIsNotAPositiveAttrMatch),
            ),
            (
                "{ duration > 2s } | max(duration) > 1s",
                Verdict::RefusedNotExact(NotExact::LeafIsNotAPositiveAttrMatch),
            ),
            (
                r#"{ .a = "1" && .b = "2" } | max(duration) > 1s"#,
                Verdict::RefusedNotExact(NotExact::NotOneLeaf),
            ),
            (
                r#"{ duration > 2s || span.foo = "x" } | max(duration) > 1s"#,
                Verdict::RefusedNotExact(NotExact::NotOneLeaf),
            ),
            // --- refused because the aggregate does not render ---------
            (
                r#"{ span.http.method = "GET" } | avg(duration) > 1s"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ span.http.method = "GET" } | sum(duration) > 1s"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ span.http.method = "GET" } | max(span.retries) > 1"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ span.http.method = "GET" } | count() > 2.5"#,
                Verdict::RefusedByRenderer,
            ),
            // --- refused by the fold, with both other checks passing ---
            (
                r#"{ span.http.method = "GET" } | by(name) | count() > 2"#,
                Verdict::RefusedByFoldState,
            ),
        ];
        for (q, want) in rows {
            let got = verdict(q);
            assert_eq!(got, want, "{q}: expected {want:?}, got {got:?}");
        }

        // --- condition (6) is live, and no parsed query reaches it -----
        //
        // Every regex the planner can build is refused earlier: an
        // attribute regex by (3), a physical one by (2). So the only way
        // to show (6) is not dead code is to hand it the input where
        // (1)-(5) hold and the SELECTOR still carries a regex. That input
        // is not one `plan_search` produces — it is the input `(3)` would
        // hand it if `(3)` were ever widened to admit a regex probe, and
        // the aggregate over that generator's rows would be wrong,
        // because the SQL reading of a pattern is the NARROWER one and an
        // aggregate over a subset can err in either direction.
        {
            use super::super::filter::{GenClass, GenTable, LeafGenerator, ValuePred};
            use super::super::search_plan::{PlannedFilter, PlannedLeafEval, generator_exactness};

            let probe = super::super::filter::AttrProbe {
                key: "http.method".to_string(),
                scope: Some("span"),
                pred: ValuePred::StringEq("GET".to_string()),
            };
            let predicate = "key = 'http.method' AND val = 'GET' AND scope = 'span'".to_string();
            let filters = vec![PlannedFilter {
                leaves: vec![PlannedLeafEval::Attr {
                    probe_idx: 0,
                    negated: false,
                }],
            }];
            let probes = vec![probe];
            let predicates = vec![predicate.clone()];
            let generators = vec![(
                TRACE_ATTRS_IDX,
                LeafGenerator {
                    class: GenClass::AttrEq,
                    table: GenTable::Attrs,
                    predicate,
                    prewhere: None,
                },
            )];
            // (1)-(5) hold and the selector is regex-free: exact.
            assert_eq!(
                generator_exactness(
                    &filters,
                    &probes,
                    &predicates,
                    &generators,
                    &parse_selector(r#"{ span.http.method = "GET" }"#),
                ),
                Ok(()),
                "the control: with a regex-free selector this input is exact"
            );
            // The ONE thing that moves is the selector's fidelity.
            assert_eq!(
                generator_exactness(
                    &filters,
                    &probes,
                    &predicates,
                    &generators,
                    &parse_selector(r#"{ span.http.method =~ "GE.*" }"#),
                ),
                Err(NotExact::SelectorIsWider),
                "condition (6) must refuse a selector carrying a regex even when the two \
                 predicates are byte-equal"
            );
        }
    }

    /// Issue #492 part 4 round 1: **the pushdown refuses every threshold
    /// the two readings could disagree on, and the boundary is where the
    /// two readings START to differ rather than one number that was
    /// reported.**
    ///
    /// The pushed comparison is `d <op> t` over exact `Int64`s in
    /// ClickHouse; the unpushed one is `f64(d) <op> f64(t)` in the
    /// evaluator. Below 2^53 every integer is exact on both sides, at and
    /// above it they part company, and the four literals around that
    /// point are what this table is built from:
    ///
    /// ```text
    ///   9007199254740991  2^53 - 1  exact f64, and no `d` can straddle it   push
    ///   9007199254740992  2^53      exact f64, but d = 2^53+1 reads `==` in
    ///                               f64 and `!=` in SQL                     refuse
    ///   9007199254740993  2^53 + 1  no f64 holds it; becomes ...992         refuse
    ///   9007199254740994  2^53 + 2  an exact f64, still past the bound      refuse
    /// ```
    ///
    /// The third row is the one the review found. **A build that
    /// special-cased it would pass a test naming only that number and
    /// fail this one**, on rows two and four.
    ///
    /// Every row runs under all six comparison operators, because the
    /// rounding moves the boundary in both directions: on `=` and `<=` a
    /// rounded threshold LOSES a qualifying trace, on `!=`, `>` and `<`
    /// it ADMITS one that does not qualify. Which one it is depends on
    /// the operator, so a per-operator answer would be six answers; the
    /// threshold rule is one.
    ///
    /// The second half asserts the property the doc comment on
    /// `search_plan::aggregate_threshold` now claims: whenever this
    /// pushes, the integer in the fragment and the `f64` the evaluator
    /// compares against are the same number, bit for bit.
    #[test]
    fn the_pushdown_refuses_every_threshold_the_two_readings_could_disagree_on() {
        /// `true` when this literal may be pushed.
        const PUSHES: bool = true;
        const REFUSES: bool = false;

        let rows: [(&str, bool); 14] = [
            // --- plain integers ---------------------------------------
            ("2", PUSHES),
            ("0", PUSHES),
            // An integer spelled with a zero fraction is still an integer.
            ("2.0", PUSHES),
            // ...and one spelled with a NON-zero fraction is not, however
            // far down it sits. `2.0000000000000000001` parses to exactly
            // `2.0` as an `f64`, so the old `fract() != 0.0` rule pushed
            // it; the lexeme rule refuses it. No answer moves — the
            // evaluator then does the same comparison — and the rule is
            // one sentence instead of two.
            ("2.5", REFUSES),
            ("2.0000000000000000001", REFUSES),
            // --- the boundary, in number form -------------------------
            ("9007199254740991", PUSHES),
            ("9007199254740992", REFUSES),
            ("9007199254740993", REFUSES),
            ("9007199254740994", REFUSES),
            ("9007199254740995", REFUSES),
            // --- the same boundary, in duration form ------------------
            ("1s", PUSHES),
            ("9007199254740991ns", PUSHES),
            ("9007199254740993ns", REFUSES),
            // 2600 h is 9,360,000,000,000,000 ns, past 2^53; 2500 h is
            // 9,000,000,000,000,000 ns and inside it. A duration literal
            // is an exact nanosecond count before this function sees it,
            // so only the bound can refuse one.
            ("2600h", REFUSES),
        ];
        let ops = ["=", "!=", ">", ">=", "<", "<="];

        for (literal, wants_push) in rows {
            for op in ops {
                let q = format!(r#"{{ span.http.method = "GET" }} | max(duration) {op} {literal}"#);
                let query = pulsus_traceql::parse(&q).unwrap_or_else(|e| panic!("{q}: {e}"));
                let stage = query
                    .pipeline
                    .iter()
                    .find(|s| matches!(s, PipelineStage::Aggregate { .. }))
                    .unwrap_or_else(|| panic!("{q}: parsed without an aggregate stage"));
                let frag = aggregate_having_sql(stage);
                assert_eq!(
                    frag.is_some(),
                    wants_push,
                    "{q}: expected {}, got {frag:?}",
                    if wants_push { "a fragment" } else { "no fragment" }
                );
                let Some(frag) = frag else { continue };
                // The fragment's integer IS the evaluator's `f64`.
                let PipelineStage::Aggregate { op: agg, field, value, .. } = stage else {
                    unreachable!("filtered above")
                };
                let evaluator = super::super::search_plan::aggregate_threshold(*agg, field, value)
                    .unwrap_or_else(|e| panic!("{q}: {e:?}"));
                let rendered: i64 = frag
                    .rsplit(' ')
                    .next()
                    .unwrap_or_else(|| panic!("{q}: {frag:?} ends in a literal"))
                    .parse()
                    .unwrap_or_else(|e| panic!("{q}: {frag:?} does not end in an integer: {e}"));
                assert_eq!(
                    (rendered as f64).to_bits(),
                    evaluator.to_bits(),
                    "{q}: the statement compares against {rendered} and the evaluator against \
                     {evaluator}"
                );
            }
        }
    }

    /// A negative threshold cannot reach the renderer from a parsed
    /// query, and this is how that was established rather than read: the
    /// aggregate grammar takes a `Number` or `Duration` TOKEN and the
    /// lexer never puts a sign inside either
    /// (`pulsus-traceql/src/lexer.rs::scan_number_or_duration`), so `-1`
    /// arrives as a `Minus` token the aggregate production refuses.
    ///
    /// It matters because `count()` pushes down to `uniqExact(span_id)`,
    /// an unsigned aggregate: a negative literal in that comparison is
    /// the one shape where ClickHouse's own type promotion, rather than
    /// our threshold rule, would decide the answer. The sign handling in
    /// `exact_decimal_integer` therefore covers a form only
    /// `parser::static_value` can build, and this test is what says so.
    #[test]
    fn an_aggregate_threshold_cannot_be_negative() {
        for q in [
            r#"{ span.http.method = "GET" } | count() > -1"#,
            r#"{ span.http.method = "GET" } | max(duration) = -9007199254740993"#,
        ] {
            let err = pulsus_traceql::parse(q).expect_err("a signed threshold must not parse");
            let rendered = err.to_string();
            assert!(
                rendered.contains("a number or a duration"),
                "{q}: expected the aggregate value production to refuse, got {rendered:?}"
            );
        }
    }

    /// A physical `=` leaf still classifies through the same rule, so the
    /// `Equivalent` half of `StrOpKind::fidelity` is exercised by
    /// something other than its own arm list.
    #[test]
    fn a_string_equality_leaf_is_equivalent_and_a_regex_one_is_not() {
        let re = pulsus_re2::compile_user_regex_anchored("a.*").expect("compiles");
        assert_eq!(kind_of(&StrOp::Eq).fidelity(), Fidelity::Equivalent);
        assert_eq!(
            kind_of(&StrOp::Re(re)).fidelity(),
            Fidelity::Wider,
            "the two engines read the pattern differently"
        );
    }

    /// Unused-import guard for the two AST types the row table names.
    #[test]
    fn the_row_table_names_real_ast_shapes() {
        let agg = pipe(r#"{ .a = "1" } | max(duration) > 1s"#);
        let PipelineStage::Aggregate { op, value, .. } = &agg else {
            panic!("{agg:?}")
        };
        assert_eq!(*op, AggregateOp::Max);
        assert!(matches!(value, Value::Duration(_)));
        let sel = pipe(r#"{ .a = "1" } | select(span.http.method)"#);
        let PipelineStage::Select { fields } = &sel else {
            panic!("{sel:?}")
        };
        let f: &Field = &fields[0];
        assert_eq!(f.to_string(), "span.http.method");
        let _ = SqlExpr::new("unused-in-production");
    }
}
