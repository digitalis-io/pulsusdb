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
    BlockReason, Capability, Col, ColSet, Fidelity, Grouping, Lang, Lower, LowerCx, Name,
    NeverReason, Pred, Provenance, Relation, Shape, SourceName, SourceTerm,
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
/// this aggregate may not be pushed (issue #492 parts 4 and 5).
///
/// **The one renderer.** [`AggregateLower::apply`] records what this
/// returns on the relation and [`super::search_plan::plan_search`] puts
/// the same string into the statement, so the plan's account of the
/// query and the query cannot disagree.
///
/// `group_key` is the SQL expression a preceding `by()` lowered to
/// ([`group_key_sql`]), or `None` for the ungrouped form. Both arms pass
/// through the SAME `(aggregate, operator)` rule below, so a per-arm
/// carve-out cannot be written.
///
/// # The containment, and the six cells it admits
///
/// Write `R` for the generator statement's rows and `D` for the spans
/// the evaluator aggregates. Every span in `D` is the survivor of one or
/// more rows of `R` carrying the same `span_id`
/// (`exec::group_hydrated_rows` keeps the FIRST row per `span_id` in the
/// hydration read's `(trace_id, timestamp_ns, span_id)` order), and the
/// selector is then applied to that survivor. So
///
/// ```text
///    generator rows R  ─────────────────────►  one row per delivery
///         │  first-row-per-span_id, then the selector
///         ▼
///    evaluator spans D ─────────────────────►  R ⊇ D,  ALWAYS
/// ```
///
/// `min` over a superset reads **low**; `max` and `uniqExact` over a
/// superset read **high**. A HIGH-reading aggregate under `>` or `>=`
/// admits traces phase 2 then drops; under `<`, `<=`, `=` or `!=` it
/// LOSES a trace, and a lost trace has no second chance
/// (`search_eval.rs:3039` filters the spanset list in place and an empty
/// list ends the trace). A LOW-reading aggregate is the mirror image.
///
/// ```text
///   aggregate      reads   safe operators   ungrouped fragment
///   count()        HIGH    >   >=           uniqExact(span_id) <op> <t>
///   max(duration)  HIGH    >   >=           max(duration_ns)   <op> <t>
///   min(duration)  LOW     <   <=           min(duration_ns)   <op> <t>
/// ```
///
/// **Twelve of the eighteen (aggregate, operator) cells therefore
/// refuse, and part 4 shipped them pushing.** Measured over corpus U —
/// seven traces where a `(trace_id, span_id)` can carry two rows that
/// disagree on `duration_ns` or on the selector — on ClickHouse 26.3,
/// engine against engine (`{ span.http.method = "GET" } | <cell>` with
/// `aggregate_pushed()` asserted `true`, against the same rows planned
/// unpushed):
///
/// ```text
///   cell               engine(unpushed)     engine(pushed)      verdict
///   count()   >  1     0e 0d 03 01          0e 0d 03 01         PUSH
///   count()   <  2     0b 0a 02             0b 0a 02            REFUSE (10 002-span cap, below)
///   min(dur)  <  2s    0b 01                0b 01               PUSH
///   min(dur)  >  2s    0e 0d 0a 03 02       0e 03 02            REFUSE — loses 0d, 0a
///   min(dur)  >= 5s    0a                   (none)              REFUSE — loses 0a
///   max(dur)  >  2s    0e 0d 0a 03 02 01    0e 0d 0a 03 02 01   PUSH
///   max(dur)  <  2s    0b                   (none)              REFUSE — loses 0b
///   max(dur)  != 5s    0e 0b 03 02          0e 03 02            REFUSE — loses 0b
/// ```
///
/// `count()` is not exempt, and the witness is the hydration cap rather
/// than a re-delivery: `search_sql::hydration_sql` carries
/// `LIMIT MAX_SPANS_PER_TRACE + 1 BY trace_id` (10 000), so a trace with
/// 10 002 matched spans is evaluated on 10 000 of them while
/// `uniqExact(span_id)` counts all 10 002. On that corpus
/// `count() < 10001`, `<= 10000`, `= 10000` and `!= 10002` each return
/// one trace unpushed and none pushed.
///
/// The two refusal rules **compose and neither subsumes the other**: a
/// cell must be one of these six AND its threshold must survive
/// [`super::search_plan::exact_aggregate_threshold`] (part 4's ±2^53
/// lexeme rule). That threshold note, at
/// `the_pushdown_refuses_every_threshold_the_two_readings_could_disagree_on`,
/// says `!=` and `>` are the harmless directions — that is true of a
/// ROUNDED THRESHOLD and does not transfer here, where `!=` loses for
/// every aggregate.
///
/// # The grouped arm
///
/// With `V = mapValues(<agg>Map(map(<key>, <arg>)))` the trace-level
/// question is "does any group satisfy the predicate?", and the same six
/// cells are the safe ones for the same reason:
///
/// ```text
///   count()        uniqExactMap  span_id      >  >=   arrayMax(V) <op> <t>
///   max(duration)  maxMap        duration_ns  >  >=   arrayMax(V) <op> <t>
///   min(duration)  minMap        duration_ns  <  <=   arrayMin(V) <op> <t>
/// ```
///
/// `mapValues(…)` is never empty — a trace in the `GROUP BY` has at
/// least one row and every column a key may push to is non-nullable —
/// so `arrayMax`/`arrayMin` are never called on an empty array. Types
/// were printed rather than inferred:
/// `toTypeName(mapValues(uniqExactMap(map('a','x'))))` is
/// `Array(UInt64)` and `toTypeName(mapValues(maxMap(map('a',
/// toInt64(1)))))` is `Array(Int64)`; ClickHouse compares an
/// `Array(UInt64)` element against a negative or 2^53+1 literal
/// accurately, measured on 26.3.
///
/// # What still refuses for part 4's reasons
///
/// `None` for `sum` and `avg` (a duplicated index row moves them:
/// measured on one trace with two matched spans of 1.5 s and 0.5 s and
/// one replayed, the engine sees `avg=1.0e9` and the raw rows
/// `8.33e8`), for an attribute source (`max(.retries)` reads a
/// `val_num` for a different `key` than the generator's, which is a
/// join), and for any threshold whose LEXEME is not an exact integer
/// strictly inside ±2^53.
///
/// `uniqExact` and not `count()`: under the containment above a plain
/// `count()` would also be sound for `>`/`>=` and is cheaper (245 MB
/// against 526 MB on the 2,000,000-row corpus), but it makes the
/// CANDIDATE set move under a byte-identical replay, which
/// `duplicate_index_rows_do_not_move_a_pushed_min_max_or_count` exists
/// to prevent. Considered and not taken.
pub fn aggregate_having_sql(stage: &PipelineStage, group_key: Option<&str>) -> Option<String> {
    let PipelineStage::Aggregate {
        op,
        field,
        cmp,
        value,
    } = stage
    else {
        return None;
    };
    // The (aggregate, operator) rule, and it is ONE match both arms pass
    // through. `arg` is the column the per-group aggregate reads;
    // `scalar` is the ungrouped aggregate; `map_agg` is its `-Map`
    // combinator; `wrapper` is the array reduction that asks the
    // trace-level question. Every other pair returns `None` before the
    // threshold is read.
    let is_duration = matches!(
        field,
        Some(FieldExpr::Field(Field::Intrinsic(Intrinsic::Duration)))
    );
    let (scalar, map_agg, arg, wrapper) = match (op, cmp) {
        (AggregateOp::Count, ComparisonOp::Gt | ComparisonOp::Gte) if field.is_none() => {
            ("uniqExact(span_id)", "uniqExactMap", "span_id", "arrayMax")
        }
        (AggregateOp::Max, ComparisonOp::Gt | ComparisonOp::Gte) if is_duration => {
            ("max(duration_ns)", "maxMap", "duration_ns", "arrayMax")
        }
        (AggregateOp::Min, ComparisonOp::Lt | ComparisonOp::Lte) if is_duration => {
            ("min(duration_ns)", "minMap", "duration_ns", "arrayMin")
        }
        _ => return None,
    };
    let cmp_sql = match cmp {
        ComparisonOp::Gt => ">",
        ComparisonOp::Gte => ">=",
        ComparisonOp::Lt => "<",
        ComparisonOp::Lte => "<=",
        // Unreachable through the match above, which admits only the
        // four ordering operators. Kept as an arm rather than an
        // `unreachable!` so that widening the cell rule cannot render a
        // comparison this function never named.
        ComparisonOp::Eq | ComparisonOp::Neq | ComparisonOp::Re | ComparisonOp::Nre => return None,
    };
    // The threshold is read from the stage's LEXEME, not from the
    // evaluator's `f64`, and the read refuses whenever the integer
    // comparison here — `Int64` for the two duration aggregates,
    // `UInt64` for `uniqExact(span_id)` — and the evaluator's `f64` one
    // could put a span on different sides of it.
    let threshold = super::search_plan::exact_aggregate_threshold(*op, field, value)?;
    Some(match group_key {
        None => format!("{scalar} {cmp_sql} {threshold}"),
        Some(key) => {
            format!("{wrapper}(mapValues({map_agg}(map({key}, {arg})))) {cmp_sql} {threshold}")
        }
    })
}

// ---------------------------------------------------------------------
// The `by()` key's SQL
// ---------------------------------------------------------------------

/// The SQL expression a `by()` key groups on, or `None` when the key may
/// not be pushed (issue #492 part 5). **The one place a `by()` key
/// becomes SQL.**
///
/// A group key is not compared with an operator, it PARTITIONS, so the
/// failure mode is a partition that is finer or coarser on one side, and
/// the direction is not uniform across aggregates: a finer SQL partition
/// LOSES a qualifying trace under `count() > t` and ADMITS an extra one
/// under `min(d) > t`. No "wider is safe" argument covers the set, so
/// the rule is: **the two partitions are identical, or the key does not
/// push.**
///
/// # Keyed on the generator's SOURCE
///
/// Only a `trace_spans` generator accepts. The argument in
/// [`aggregate_having_sql`] needs the map's key and the evaluator's key
/// to come from the SAME row, which holds when the generator reads
/// `trace_spans` — `search_eval::resolve_group_value` reads every
/// pushable key off the hydrated span. It does not hold on
/// `trace_attrs_idx`: there a `by(duration)` key would be
/// `trace_attrs_idx.duration_ns` while the evaluator's comes from
/// `trace_spans.duration_ns`, and those are two rows in two tables with
/// different deduplication rules (`trace_attrs_idx` is a
/// `ReplacingMergeTree` whose ordering key does not contain
/// `duration_ns`, so a merge picks one value arbitrarily, while
/// `trace_spans` is a plain `MergeTree` and keeps both). A span's
/// evaluator key could then be absent from the map entirely, and even
/// `count() > t` could lose a trace.
///
/// # Why every string key renders through [`super::search_sql::byte_cap_expr`]
///
/// The evaluator groups the byte-capped hydrated value
/// (`hydration_sql` projects `if(length(name) <= 8192, name,
/// substringUTF8(name, 1, 2048)) AS name`), so grouping the RAW column
/// would be a different partition. `length(col) <= 8192` is inclusive,
/// so 8192 bytes caps to itself and 8193 is the first that truncates.
/// Measured on ClickHouse 26.3 with three spans, two distinct raw names
/// of 8193 bytes sharing their first 2048 code points:
///
/// ```text
///   arrayMax(mapValues(uniqExactMap(map(name,      span_id))))  -> 2  count() > 2 DROPS the trace
///   arrayMax(mapValues(uniqExactMap(map(cap(name), span_id))))  -> 3  count() > 2 KEEPS it
/// ```
///
/// There is no ingest length cap on `name`/`service`
/// (`pulsus-write/src/protocols/otlp_traces.rs` binds them unbounded),
/// so the case is reachable through our own write path.
///
/// # What refuses, and why each
///
/// ```text
///   by(status)                 status_keyword(i8) has THREE outputs over 256 inputs —
///                              status_code 0 and 3 both read "unset" and SQL splits them
///   by(kind)                   kind_keyword(i8), the same shape with six outputs
///   by(.attr)                  a val/val_num row under a DIFFERENT key than the
///                              generator's: a second source read, and ADR 0008 names
///                              no join clause
///   by(nestedSet*)             computed per trace after the read
///   by(traceDuration)          a trace-wide co-load, not a column of the generator
///   by(rootName)               as above
///   by(rootServiceName)        as above
///   by(span:childCount)        as above
/// ```
///
/// `by(duration)` and the three id keys DO push: the evaluator renders
/// `go_duration_string(span.duration_ns)` and lowercase hex
/// respectively, and both maps are injective, so distinct column values
/// give distinct evaluator keys and the two partitions coincide.
pub(crate) fn group_key_sql(field: &Field, source: SourceRef) -> Option<String> {
    if source != TRACE_SPANS {
        return None;
    }
    let cap = super::search_sql::byte_cap_expr;
    Some(match field {
        Field::Intrinsic(Intrinsic::Name) => cap("name"),
        Field::Intrinsic(Intrinsic::StatusMessage) => cap("status_message"),
        Field::Intrinsic(Intrinsic::InstrumentationName) => cap("scope_name"),
        Field::Intrinsic(Intrinsic::InstrumentationVersion) => cap("scope_version"),
        Field::Intrinsic(Intrinsic::Duration) => "duration_ns".to_string(),
        Field::Intrinsic(Intrinsic::SpanId) => "span_id".to_string(),
        Field::Intrinsic(Intrinsic::ParentId) => "parent_id".to_string(),
        Field::Intrinsic(Intrinsic::TraceId) => "trace_id".to_string(),
        Field::Attribute { scope, key }
            if *scope == pulsus_traceql::AttrScope::Resource && key == "service.name" =>
        {
            cap("service")
        }
        // Every refusal, listed as its own arm so a new `Intrinsic`
        // variant fails to compile here rather than silently joining the
        // accept list.
        Field::Intrinsic(
            Intrinsic::Status
            | Intrinsic::Kind
            | Intrinsic::NestedSetParent
            | Intrinsic::NestedSetLeft
            | Intrinsic::NestedSetRight
            | Intrinsic::ChildCount
            | Intrinsic::TraceDuration
            | Intrinsic::RootName
            | Intrinsic::RootServiceName
            | Intrinsic::EventName
            | Intrinsic::EventTimeSinceStart
            | Intrinsic::LinkSpanId
            | Intrinsic::LinkTraceId,
        )
        | Field::Attribute { .. } => return None,
    })
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

/// The one grouping key a preceding `by()` recorded, as SQL — the
/// argument [`aggregate_having_sql`] takes.
///
/// A single accessor rather than the expression written twice, because
/// [`AggregateLower::capability`] and [`AggregateLower::apply`] must ask
/// the renderer the SAME question; a `capability` that admitted the
/// ungrouped form while `apply` rendered the grouped one would put a
/// fragment in the statement that no precondition checked.
fn grouped_key(rel: &Relation<Tql>) -> Option<&str> {
    rel.grouping.as_ref()?.keys.first().map(String::as_str)
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
    /// `exact`, **no fragment already in this statement's `HAVING`**, and
    /// a fragment [`aggregate_having_sql`] will render for this
    /// (aggregate, operator) pair and this grouping.
    ///
    /// An aggregate over a superset is not merely wide, it is wrong:
    /// `max()` can exceed the true maximum and admit a trace that should
    /// not match, `min()` errs the other way, and `count()` inflates.
    /// Measured, the cost of getting this wrong is 333 qualifying traces
    /// becoming 1,000.
    ///
    /// **`grouping.is_some()` is no longer a refusal** (issue #492 part
    /// 5). A grouping reaches here only when [`ByLower`] found the key
    /// renderable on this generator's source; a key that does not render
    /// clears `exact` exactly as it did before, so the same shapes are
    /// refused and one rule decides it.
    ///
    /// The renderer condition is part 4's, widened by part 5: `sum` and
    /// `avg`, an attribute source, a non-integral threshold, and now the
    /// twelve anti-monotone (aggregate, operator) cells all get
    /// `No(NotYetLowered)`.
    fn capability(&self, s: &TqlLink, rel: &Relation<Tql>) -> Capability {
        if rel.shape != TqlShape::Spans {
            return Capability::No(BlockReason::ShapeMismatch);
        }
        if !rel.exact {
            return Capability::No(BlockReason::NotExact);
        }
        // A statement carries at most one pushed fragment: `plan_search`
        // re-renders only when the fold recorded exactly one.
        //
        // **This line has no witness on its own**, and saying so is the
        // point. [`Self::fidelity`] returns `Wider`, which clears
        // `rel.exact` the moment the first aggregate applies, so the
        // `!rel.exact` check above always refuses a second aggregate
        // first. Measured over 9,425 planned queries: deleting this line
        // alone moves no column of the enumeration freeze. The two
        // conditions are individually redundant and jointly load-bearing
        // — with BOTH removed, 477 of those queries change push status —
        // so this must not be cited as the thing that enforces
        // one-fragment-per-statement. `Fidelity::Wider` is.
        if !rel.having.is_empty() {
            return Capability::No(BlockReason::NotYetLowered);
        }
        let TqlLink::Pipe(stage) = s else {
            return Capability::No(BlockReason::NotYetLowered);
        };
        if aggregate_having_sql(stage, grouped_key(rel)).is_none() {
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
            && let Some(frag) = aggregate_having_sql(stage, grouped_key(&rel))
        {
            rel.having.push(frag);
        }
        Ok(rel)
    }
    /// [`Fidelity::Wider`], for every pushed aggregate, grouped or not
    /// (issue #492 part 5).
    ///
    /// The statement aggregates the generator's ROWS and the evaluator
    /// its DEDUPLICATED, selector-matched spans. Those are not the same
    /// set — `R ⊇ D`, [`aggregate_having_sql`] — so `orig ⟹ sql` and the
    /// evaluator MUST re-apply the link. Part 4 returned
    /// [`Fidelity::Equivalent`] here, which asserts `orig ⟺ sql`; the
    /// counterexamples are measured and pasted in
    /// [`aggregate_having_sql`]'s table, where four cells admit a trace
    /// phase 2 then drops.
    ///
    /// **The consequence is real and small.** `Wider` clears `rel.exact`
    /// at `fold.rs:957`, so a SECOND aggregate in the same pipeline can
    /// no longer lower. That is the behaviour we want: `plan_search`
    /// re-renders only when the fold recorded exactly one fragment, so
    /// before this a two-aggregate pipeline recorded two `Lowered` links
    /// and pushed neither.
    fn fidelity(&self, _s: &TqlLink, _rel: &Relation<Tql>) -> Fidelity {
        Fidelity::Wider
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
    /// **Shape unchanged**; records the key as an evaluator-owned group
    /// consumer in both branches; and then one of two things (issue #492
    /// part 5):
    ///
    /// * the key RENDERS on this generator's source ([`group_key_sql`]),
    ///   the grouping slot is free, and the relation is still exact —
    ///   the grouping is recorded and `exact` is left alone, so a
    ///   following `Aggregate` compiles a per-group fragment;
    /// * otherwise `exact` is cleared, exactly as before part 5, so a
    ///   following `Aggregate` refuses with `NotExact`.
    ///
    /// The link itself still does not lower ([`Self::capability`] is
    /// unchanged): `Capability::Yes` would mean "the evaluator MUST NOT
    /// re-apply this link", and the evaluator is what builds the span
    /// sets.
    fn residual_effect(&self, s: &TqlLink, mut rel: Relation<Tql>) -> Relation<Tql> {
        if let TqlLink::Pipe(PipelineStage::By { key }) = s {
            rel.cols
                .set_provenance(&Name::new(key.to_string()), Provenance::EvaluatorOnly);
            if rel.exact
                && rel.grouping.is_none()
                && let FieldExpr::Field(field) = key
                && let Some(sql) = group_key_sql(field, rel.source_ref())
            {
                rel.grouping = Some(Grouping { keys: vec![sql] });
                return rel;
            }
        }
        rel.exact = false;
        rel
    }
}

impl Lower<Tql> for CoalesceLower {
    /// Two rows in one dispatcher, and one rule rather than two special
    /// cases.
    ///
    /// * No grouping — the identity. It contributes no SQL and cannot
    ///   fail.
    /// * A grouping and no `HAVING` yet — the slot is FREED (issue #492
    ///   part 5), so a following `by()` may fill it again. This is the
    ///   reference's own merge behaviour, read from the pinned build:
    ///   the merge stage concatenates every span set's spans into one
    ///   and carries no group attributes forward, so an unfiltered
    ///   `by()` followed by `coalesce()` is the identity.
    /// * A grouping WITH a `HAVING` — refuse. The aggregate selected
    ///   groups; the spans it selected are not recoverable from a
    ///   statement that has already reduced them.
    fn capability(&self, _s: &TqlLink, rel: &Relation<Tql>) -> Capability {
        if rel.grouping.is_none() || rel.having.is_empty() {
            return Capability::Yes;
        }
        Capability::No(BlockReason::NotYetLowered)
    }
    /// Frees the grouping slot. In the no-grouping arm this is the
    /// identity, so one assignment covers both `Yes` branches.
    fn apply(
        &self,
        _s: &TqlLink,
        mut rel: Relation<Tql>,
        _cx: &LowerCx<'_, Tql>,
    ) -> Result<Relation<Tql>, PlanError> {
        rel.grouping = None;
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

    /// The SQL `by(name)` lowers to on a `trace_spans` generator —
    /// written out rather than called, so the expectation is not
    /// produced by the code under test.
    const NAME_KEY_SQL: &str = "if(length(name) <= 8192, name, substringUTF8(name, 1, 2048))";

    fn grouped_by(mut rel: Relation<Tql>, key: &str) -> Relation<Tql> {
        rel.grouping = Some(Grouping {
            keys: vec![key.to_string()],
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
    /// **Twenty rows**: eleven with a stated effect (`Aggregate`, `By`
    /// in each of its two branches, grouped `Coalesce`, `Select`, `Order`,
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
        let by_status = pipe(r#"{ .a = "1" } | by(status)"#);
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
            // Issue #492 part 5: `By` has TWO branches and both are
            // rows. With a key that renders on this generator's source
            // the grouping is recorded and `exact` is left alone; with
            // one that does not, `exact` is cleared exactly as before
            // part 5. A table carrying only the first would let the
            // renderable set widen with nothing naming it.
            EffectRow {
                name: "By, with a key that renders",
                link: TqlLink::Pipe(by),
                s1: base(TqlShape::Spans),
                s2: base(TqlShape::Traces),
                e1: grouped_by(evaluator_owned(base(TqlShape::Spans), "name"), NAME_KEY_SQL),
                e2: grouped_by(
                    evaluator_owned(base(TqlShape::Traces), "name"),
                    NAME_KEY_SQL,
                ),
                effect_is_constant: false,
                has_effect: true,
            },
            EffectRow {
                name: "By, with a key that does not render",
                link: TqlLink::Pipe(by_status),
                s1: base(TqlShape::Spans),
                s2: base(TqlShape::Traces),
                e1: not_exact(evaluator_owned(base(TqlShape::Spans), "status")),
                e2: not_exact(evaluator_owned(base(TqlShape::Traces), "status")),
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
        assert_every_residual_state_effect::<Tql>(&rows, 20);
    }

    /// Issue #492 part 5 criterion 32, and its assertion names its own
    /// fields.
    ///
    /// The whole-`Relation` row in
    /// [`every_residual_state_effect_is_the_one_the_document_states`]
    /// stays exactly as strong as it is; this is a projection beside it,
    /// not instead of it. The projection exists because the whole-row
    /// comparison reddens for any field — it reddened for a change to
    /// the PROVENANCE NAME during review, which criterion 32's sentence
    /// does not mention — so the row cannot say which channel moved.
    ///
    /// The field list is not written here. It is derived from
    /// `CRITERION_32`'s own backticked tokens, intersected with the
    /// relation's top-level field set, so the only way to change what is
    /// asserted is to change the sentence.
    #[test]
    fn the_by_link_records_the_grouping_and_leaves_exactness_alone_when_the_key_renders() {
        use crate::compile::criterion_fields::assert_named_debug_fields;

        const CRITERION_32: &str = "Criterion 32: `ByLower`'s residual state effect is the one \
            the table states — a `by()` key that renders on this generator's source records the \
            `grouping` and leaves `exact` alone, and one that does not records no `grouping` and \
            clears `exact`.";

        let by_name = TqlLink::Pipe(pipe(r#"{ .a = "1" } | by(name)"#));
        let by_status = TqlLink::Pipe(pipe(r#"{ .a = "1" } | by(status)"#));
        for (link, expected, ctx) in [
            (
                &by_name,
                grouped_by(evaluator_owned(base(TqlShape::Spans), "name"), NAME_KEY_SQL),
                "by(name), which renders on trace_spans",
            ),
            (
                &by_status,
                not_exact(evaluator_owned(base(TqlShape::Spans), "status")),
                "by(status), whose keyword map is not injective",
            ),
        ] {
            let got = Tql::lower_of(link).residual_effect(link, base(TqlShape::Spans));
            assert_named_debug_fields(
                CRITERION_32,
                &format!("{got:#?}"),
                &format!("{expected:#?}"),
                ctx,
            );
        }
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

    /// Issue #492 parts 4 and 5 — **the pushdown refuses every unsafe
    /// shape, and each refusal names the condition that refused it.**
    ///
    /// Three things can stop a spanset aggregate reaching the generator
    /// statement, and the table separates them rather than lumping them
    /// into "not pushed":
    ///
    /// ```text
    ///   Refusal::Renderer   aggregate_having_sql said None — the aggregate
    ///                       family, its (aggregate, operator) cell, or its
    ///                       threshold
    ///   Refusal::NotExact   the generator's rows are not the selector's
    ///                       matched spans (which of the conditions)
    ///   Refusal::FoldState  both of the above are satisfied and the FOLD
    ///                       still refuses: a `by()` whose key does not
    ///                       render has cleared `exact`
    /// ```
    ///
    /// **The refusal is a property of a PAIR, so the table carries every
    /// cell and not only the pushes.** A table listing what pushes cannot
    /// see a missing refusal: drop the operator gate from
    /// [`aggregate_having_sql`] and twelve rows per arm become `Push`
    /// with no test naming them. Hence all eighteen (aggregate, operator)
    /// cells on the attribute-index generator, all eighteen on the
    /// `ServiceEq` one, and all eighteen grouped — six `Push` and twelve
    /// `RefusedByRenderer` each — with the array length in the type.
    ///
    /// The `by()`-key table is the other half of the same shape: a key
    /// that does not render must refuse, and a key that does must push.
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
            // The renderer alone, read straight off the AST. The
            // ungrouped form is the right question to ask it: the
            // six-cell rule is the SAME on both arms, so a cell the
            // ungrouped renderer refuses is one the grouped renderer
            // refuses too.
            let renders = query
                .pipeline
                .iter()
                .filter(|s| matches!(s, PipelineStage::Aggregate { .. }))
                .all(|s| aggregate_having_sql(s, None).is_some());
            if !renders {
                return Verdict::RefusedByRenderer;
            }
            match plan.generator_exact() {
                Err(why) => Verdict::RefusedNotExact(why),
                Ok(()) => Verdict::RefusedByFoldState,
            }
        }

        let push = |frag: &str| Verdict::Push(frag.to_string());

        // --- the two exact leaf families, and everything that is neither
        let shapes: [(&str, Verdict); 15] = [
            // Unscoped: no `scope` term on either side, so the two
            // predicates are byte-equal and the predicate comparison
            // decides it.
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
            // Issue #492 part 5: the SECOND exact family. A bare
            // `resource.service.name` equality is a `PREWHERE`-carrying
            // `trace_spans` read, and until part 5 nothing pushed for it
            // at all.
            (
                r#"{ resource.service.name = "checkout" } | max(duration) > 1s"#,
                push("max(duration_ns) > 1000000000"),
            ),
            // --- refused because the generator is not exact ------------
            // An attribute regex compiles to `ValuePred::Regex`, so the
            // value-predicate condition refuses it before the selector
            // fidelity one is reached. That condition is not thereby
            // redundant — the second half of this test builds the input
            // where it is the one that refuses.
            (
                r#"{ span.http.method =~ "GE.*" } | max(duration) > 1s"#,
                Verdict::RefusedNotExact(NotExact::ValuePredIsNotEquality),
            ),
            // A PHYSICAL regex leaf is refused one condition earlier
            // still: it is in neither exact family.
            (
                r#"{ name =~ "a.*" } | max(duration) > 1s"#,
                Verdict::RefusedNotExact(NotExact::LeafIsNotAnExactLeafFamily),
            ),
            (
                r#"{ span.http.method != "GET" } | max(duration) > 1s"#,
                Verdict::RefusedNotExact(NotExact::LeafIsNotAnExactLeafFamily),
            ),
            (
                "{ span.http.status_code >= 500 } | max(duration) > 1s",
                Verdict::RefusedNotExact(NotExact::ValuePredIsNotEquality),
            ),
            // A service INEQUALITY is not the `ServiceEq` family: absence
            // is not indexable, so its generator is the time-range
            // fallback.
            (
                r#"{ resource.service.name != "checkout" } | max(duration) > 1s"#,
                Verdict::RefusedNotExact(NotExact::LeafIsNotAnExactLeafFamily),
            ),
            // A service REGEX generates through the attribute index and
            // evaluates on the physical column, so it is in neither
            // family either.
            (
                r#"{ resource.service.name =~ "check.*" } | max(duration) > 1s"#,
                Verdict::RefusedNotExact(NotExact::LeafIsNotAnExactLeafFamily),
            ),
            (
                "{ duration > 2s } | max(duration) > 1s",
                Verdict::RefusedNotExact(NotExact::LeafIsNotAnExactLeafFamily),
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
        ];

        // --- all eighteen cells, ungrouped, on the attribute index -----
        //
        // Twelve of these eighteen pushed at `ddb48c96` and returned
        // FEWER traces than the query matches. The `min`/`max` losses
        // need a `(trace_id, span_id)` with two rows disagreeing on
        // `duration_ns`; the four `count()` losses need a trace past the
        // 10 000-span hydration cap, which `uniqExact(span_id)` counts
        // past and the evaluator never sees.
        let ungrouped_attrs: [(&str, Verdict); 18] = [
            (
                r#"{ span.http.method = "GET" } | count() > 1"#,
                push("uniqExact(span_id) > 1"),
            ),
            (
                r#"{ span.http.method = "GET" } | count() >= 2"#,
                push("uniqExact(span_id) >= 2"),
            ),
            (
                r#"{ span.http.method = "GET" } | count() < 2"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ span.http.method = "GET" } | count() <= 1"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ span.http.method = "GET" } | count() = 1"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ span.http.method = "GET" } | count() != 2"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ span.http.method = "GET" } | min(duration) < 2s"#,
                push("min(duration_ns) < 2000000000"),
            ),
            (
                r#"{ span.http.method = "GET" } | min(duration) <= 1s"#,
                push("min(duration_ns) <= 1000000000"),
            ),
            (
                r#"{ span.http.method = "GET" } | min(duration) > 2s"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ span.http.method = "GET" } | min(duration) >= 5s"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ span.http.method = "GET" } | min(duration) = 5s"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ span.http.method = "GET" } | min(duration) != 1s"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ span.http.method = "GET" } | max(duration) > 2s"#,
                push("max(duration_ns) > 2000000000"),
            ),
            (
                r#"{ span.http.method = "GET" } | max(duration) >= 5s"#,
                push("max(duration_ns) >= 5000000000"),
            ),
            (
                r#"{ span.http.method = "GET" } | max(duration) < 2s"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ span.http.method = "GET" } | max(duration) <= 1s"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ span.http.method = "GET" } | max(duration) = 1s"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ span.http.method = "GET" } | max(duration) != 5s"#,
                Verdict::RefusedByRenderer,
            ),
        ];

        // --- all eighteen cells, ungrouped, on the `ServiceEq` read ----
        //
        // `R ⊇ D` holds on this source for one more reason than on the
        // index: the selector reads `service`, a column the evaluator
        // DEDUPLICATES, so a span the generator counts can be one the
        // evaluator has dropped.
        let ungrouped_service: [(&str, Verdict); 18] = [
            (
                r#"{ resource.service.name = "grp" } | count() > 1"#,
                push("uniqExact(span_id) > 1"),
            ),
            (
                r#"{ resource.service.name = "grp" } | count() >= 2"#,
                push("uniqExact(span_id) >= 2"),
            ),
            (
                r#"{ resource.service.name = "grp" } | count() < 2"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ resource.service.name = "grp" } | count() <= 1"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ resource.service.name = "grp" } | count() = 1"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ resource.service.name = "grp" } | count() != 2"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ resource.service.name = "grp" } | min(duration) < 2s"#,
                push("min(duration_ns) < 2000000000"),
            ),
            (
                r#"{ resource.service.name = "grp" } | min(duration) <= 1s"#,
                push("min(duration_ns) <= 1000000000"),
            ),
            (
                r#"{ resource.service.name = "grp" } | min(duration) > 2s"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ resource.service.name = "grp" } | min(duration) >= 5s"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ resource.service.name = "grp" } | min(duration) = 5s"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ resource.service.name = "grp" } | min(duration) != 1s"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ resource.service.name = "grp" } | max(duration) > 2s"#,
                push("max(duration_ns) > 2000000000"),
            ),
            (
                r#"{ resource.service.name = "grp" } | max(duration) >= 5s"#,
                push("max(duration_ns) >= 5000000000"),
            ),
            (
                r#"{ resource.service.name = "grp" } | max(duration) < 2s"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ resource.service.name = "grp" } | max(duration) <= 1s"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ resource.service.name = "grp" } | max(duration) = 1s"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ resource.service.name = "grp" } | max(duration) != 5s"#,
                Verdict::RefusedByRenderer,
            ),
        ];

        // --- all eighteen cells, grouped by a renderable key ----------
        let grouped_by_name: [(&str, Verdict); 18] = [
            (
                r#"{ resource.service.name = "grp" } | by(name) | count() > 1"#,
                push(
                    "arrayMax(mapValues(uniqExactMap(map(if(length(name) <= 8192, name, substringUTF8(name, 1, 2048)), span_id)))) > 1",
                ),
            ),
            (
                r#"{ resource.service.name = "grp" } | by(name) | count() >= 2"#,
                push(
                    "arrayMax(mapValues(uniqExactMap(map(if(length(name) <= 8192, name, substringUTF8(name, 1, 2048)), span_id)))) >= 2",
                ),
            ),
            (
                r#"{ resource.service.name = "grp" } | by(name) | count() < 2"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ resource.service.name = "grp" } | by(name) | count() <= 1"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ resource.service.name = "grp" } | by(name) | count() = 1"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ resource.service.name = "grp" } | by(name) | count() != 2"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ resource.service.name = "grp" } | by(name) | min(duration) < 2s"#,
                push(
                    "arrayMin(mapValues(minMap(map(if(length(name) <= 8192, name, substringUTF8(name, 1, 2048)), duration_ns)))) < 2000000000",
                ),
            ),
            (
                r#"{ resource.service.name = "grp" } | by(name) | min(duration) <= 1s"#,
                push(
                    "arrayMin(mapValues(minMap(map(if(length(name) <= 8192, name, substringUTF8(name, 1, 2048)), duration_ns)))) <= 1000000000",
                ),
            ),
            (
                r#"{ resource.service.name = "grp" } | by(name) | min(duration) > 2s"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ resource.service.name = "grp" } | by(name) | min(duration) >= 5s"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ resource.service.name = "grp" } | by(name) | min(duration) = 5s"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ resource.service.name = "grp" } | by(name) | min(duration) != 1s"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ resource.service.name = "grp" } | by(name) | max(duration) > 2s"#,
                push(
                    "arrayMax(mapValues(maxMap(map(if(length(name) <= 8192, name, substringUTF8(name, 1, 2048)), duration_ns)))) > 2000000000",
                ),
            ),
            (
                r#"{ resource.service.name = "grp" } | by(name) | max(duration) >= 5s"#,
                push(
                    "arrayMax(mapValues(maxMap(map(if(length(name) <= 8192, name, substringUTF8(name, 1, 2048)), duration_ns)))) >= 5000000000",
                ),
            ),
            (
                r#"{ resource.service.name = "grp" } | by(name) | max(duration) < 2s"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ resource.service.name = "grp" } | by(name) | max(duration) <= 1s"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ resource.service.name = "grp" } | by(name) | max(duration) = 1s"#,
                Verdict::RefusedByRenderer,
            ),
            (
                r#"{ resource.service.name = "grp" } | by(name) | max(duration) != 5s"#,
                Verdict::RefusedByRenderer,
            ),
        ];

        // --- the `by()` key's own accept/refuse set --------------------
        //
        // A key that renders fills the grouping slot and leaves `exact`
        // set, so the aggregate compiles a per-group fragment; a key that
        // does not clears `exact`, so the aggregate refuses by fold
        // state. Both directions are here, because a table of pushes
        // alone cannot see a key that started rendering.
        let keys: [(&str, Verdict); 15] = [
            // `by(duration)` is exact because `go_duration_string` is
            // injective over `i64` — a separate test measures that.
            (
                r#"{ resource.service.name = "grp" } | by(duration) | count() > 2"#,
                push("arrayMax(mapValues(uniqExactMap(map(duration_ns, span_id)))) > 2"),
            ),
            (
                r#"{ resource.service.name = "grp" } | by(resource.service.name) | count() > 2"#,
                push(
                    "arrayMax(mapValues(uniqExactMap(map(if(length(service) <= 8192, service, \
                     substringUTF8(service, 1, 2048)), span_id)))) > 2",
                ),
            ),
            (
                r#"{ resource.service.name = "grp" } | by(statusMessage) | count() > 2"#,
                push(
                    "arrayMax(mapValues(uniqExactMap(map(if(length(status_message) <= 8192, \
                     status_message, substringUTF8(status_message, 1, 2048)), span_id)))) > 2",
                ),
            ),
            (
                r#"{ resource.service.name = "grp" } | by(instrumentation:name) | count() > 2"#,
                push(
                    "arrayMax(mapValues(uniqExactMap(map(if(length(scope_name) <= 8192, \
                     scope_name, substringUTF8(scope_name, 1, 2048)), span_id)))) > 2",
                ),
            ),
            (
                r#"{ resource.service.name = "grp" } | by(instrumentation:version) | count() > 2"#,
                push(
                    "arrayMax(mapValues(uniqExactMap(map(if(length(scope_version) <= 8192, \
                     scope_version, substringUTF8(scope_version, 1, 2048)), span_id)))) > 2",
                ),
            ),
            // Hex is injective, so all three id keys induce the same
            // partition as the evaluator's lowercase-hex rendering.
            (
                r#"{ resource.service.name = "grp" } | by(span:id) | count() > 2"#,
                push("arrayMax(mapValues(uniqExactMap(map(span_id, span_id)))) > 2"),
            ),
            (
                r#"{ resource.service.name = "grp" } | by(span:parentID) | count() > 2"#,
                push("arrayMax(mapValues(uniqExactMap(map(parent_id, span_id)))) > 2"),
            ),
            (
                r#"{ resource.service.name = "grp" } | by(trace:id) | count() > 2"#,
                push("arrayMax(mapValues(uniqExactMap(map(trace_id, span_id)))) > 2"),
            ),
            // `status_keyword` has three outputs over 256 inputs —
            // `status_code` 0 and 3 both read "unset" — so SQL splits a
            // group the evaluator merges.
            (
                r#"{ resource.service.name = "grp" } | by(status) | count() > 2"#,
                Verdict::RefusedByFoldState,
            ),
            // `kind_keyword`, six outputs over 256 inputs, same shape.
            (
                r#"{ resource.service.name = "grp" } | by(kind) | count() > 2"#,
                Verdict::RefusedByFoldState,
            ),
            // An attribute key is a `val`/`val_num` row under a DIFFERENT
            // key than the generator's: a second source read, and ADR
            // 0008 names no join clause.
            (
                r#"{ resource.service.name = "grp" } | by(span.foo) | count() > 2"#,
                Verdict::RefusedByFoldState,
            ),
            // The co-load keys are computed per trace after the read and
            // are columns of neither generator source.
            (
                r#"{ resource.service.name = "grp" } | by(traceDuration) | count() > 2"#,
                Verdict::RefusedByFoldState,
            ),
            (
                r#"{ resource.service.name = "grp" } | by(nestedSetLeft) | count() > 2"#,
                Verdict::RefusedByFoldState,
            ),
            (
                r#"{ resource.service.name = "grp" } | by(span:childCount) | count() > 2"#,
                Verdict::RefusedByFoldState,
            ),
            // `name` is not a column of `trace_attrs_idx`, so the SAME
            // key that renders above does not render on this source.
            // That is the source restriction, as a pair.
            (
                r#"{ span.http.method = "GET" } | by(name) | count() > 2"#,
                Verdict::RefusedByFoldState,
            ),
        ];

        // Collected rather than asserted one at a time. Dropping the
        // operator gate turns TWELVE cells per arm into pushes at once,
        // and the whole set is the finding: a row-by-row assertion names
        // the first and says nothing about how many others moved with
        // it, which is exactly the question "did the rule move or did one
        // row?" turns on.
        let rows: Vec<(&str, Verdict)> = shapes
            .into_iter()
            .chain(ungrouped_attrs)
            .chain(ungrouped_service)
            .chain(grouped_by_name)
            .chain(keys)
            .collect();
        let total = rows.len();
        let mut wrong: Vec<String> = Vec::new();
        for (q, want) in rows {
            let got = verdict(q);
            if got != want {
                wrong.push(format!("{q}: expected {want:?}, got {got:?}"));
            }
        }
        assert!(
            wrong.is_empty(),
            "{} of {total} rows disagree:\n{}",
            wrong.len(),
            wrong.join("\n")
        );

        // --- the service literal's cap boundary, as a PAIR -------------
        //
        // The generator's `PREWHERE` compares the RAW `service` column
        // and the evaluator the byte-capped one, so they disagree exactly
        // when the literal is what a capped value can equal — a literal
        // of 2048 code points. 2047 is the last that cannot be, and the
        // two rows differ by one character.
        {
            let under = "a".repeat(2047);
            let at = "a".repeat(2048);
            assert_eq!(
                verdict(&format!(
                    r#"{{ resource.service.name = "{under}" }} | count() > 2"#
                )),
                push("uniqExact(span_id) > 2"),
                "a service literal one code point below the cap must still push"
            );
            assert_eq!(
                verdict(&format!(
                    r#"{{ resource.service.name = "{at}" }} | count() > 2"#
                )),
                Verdict::RefusedNotExact(NotExact::ServiceLiteralAtTheCapBoundary),
                "at the cap a capped stored value can equal the literal and a raw one cannot"
            );
        }

        // --- condition (6) is live, and no parsed query reaches it -----
        //
        // Every regex the planner can build is refused earlier: an
        // attribute regex by the value-predicate condition, a physical
        // one by the leaf-family one. So the only way to show the
        // selector-fidelity condition is not dead code is to hand it the
        // input where every earlier condition holds and the SELECTOR
        // still carries a regex. That input is not one `plan_search`
        // produces — it is the input the value-predicate condition would
        // hand it if that condition were ever widened to admit a regex
        // probe, and the aggregate over that generator's rows would be
        // wrong, because the SQL reading of a pattern is the NARROWER one
        // and an aggregate over a subset can err in either direction.
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
            // Every earlier condition holds and the selector is
            // regex-free: exact.
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
                "the selector-fidelity condition must refuse a selector carrying a regex even \
                 when the two predicates are byte-equal"
            );
        }
    }

    /// Issue #492 part 4: **the boundary the pushdown refuses at is where
    /// the two readings START to differ, not the one number that was
    /// reported.** That the rule leaves no disagreeing threshold at all
    /// is argued at `search_plan::exact_aggregate_threshold`; what this
    /// test pins is where the boundary sits.
    ///
    /// The pushed comparison is `d <op> t` over exact integers in
    /// ClickHouse — `max(duration_ns)` is `Int64` and `uniqExact(span_id)`
    /// `UInt64`, by `toTypeName` on 26.3 — and the unpushed one is
    /// `f64(d) <op> f64(t)` in the evaluator. Below 2^53 every integer is
    /// an exact `f64`, so the two readings cannot part. From 2^53 up a
    /// data value `d` can round ONTO a `t` it is not equal to, and that,
    /// rather than whether `t` itself is exact, is what the bound is
    /// drawn against: rows two and four are both exact `f64`s and are
    /// refused anyway.
    ///
    /// ```text
    ///   9007199254740991  2^53 - 1  exact f64, and no `d` can straddle it   push
    ///   9007199254740992  2^53      exact f64, but d = 2^53+1 reads `==` in
    ///                               f64 and `!=` in SQL                     refuse
    ///   9007199254740993  2^53 + 1  no f64 holds it; becomes ...992         refuse
    ///   9007199254740994  2^53 + 2  an exact f64, still past the bound      refuse
    /// ```
    ///
    /// The third row is the one the review found. **A build that fixed
    /// only the literal's rounding — refusing a lexeme no `f64` holds,
    /// which is exactly that number — would pass a test naming it and
    /// fail this one on rows two and four**, and on `2600h`, the same
    /// case in duration form. Measured by building it: 18 of these 84
    /// cases failed, six operators on each of those three literals.
    ///
    /// Every row runs under all six comparison operators, and the
    /// THRESHOLD rule refuses for all six. Four of them can actually diverge, measured
    /// on the pre-fix build (the bound inclusive, so `t = 2^53` pushes)
    /// against corpus B of `traces_search_pushdown_live.rs`, whose five
    /// traces have `max(duration)` 1s / 2^53-1 / 2^53 / 2^53+1 / 2^53+3
    /// ns. Suffix `03` is the trace `f64` rounds down onto `t`:
    ///
    /// ```text
    ///   op   generator candidates   evaluator (f64)   answer moves?
    ///   =    02                     02,03             yes — 03 lost
    ///   <=   00,01,02               00,01,02,03       yes — 03 lost
    ///   !=   00,01,03,04            00,01,04          no  — 03 admitted
    ///   >    03,04                  04                no  — 03 admitted
    ///   >=   02,03,04               02,03,04          no  — identical
    ///   <    00,01                  00,01             no  — identical
    /// ```
    ///
    /// `>=` and `<` cannot be made to differ: the only splitting case is
    /// `d = 2^53 + 1` against `t = 2^53`, where `>=` is true under both
    /// readings and `<` false under both. Of the four that do, only `=`
    /// and `<=` move an ANSWER — they LOSE a qualifying trace, and a lost
    /// trace has no second chance. `!=` and `>` ADMIT one that does not
    /// qualify, and `search_eval`'s aggregate stage re-applies
    /// `cmp_f64(agg.cmp, …, agg.threshold)` to the hydrated spans, so the
    /// extra candidate costs a transported span set and never reaches the
    /// client. One threshold rule rather than six operator rules, and the
    /// test runs all six so a per-operator carve-out would redden it.
    ///
    /// **Two rules compose here, and neither subsumes the other** (issue
    /// #492 part 5). A `max(duration)` cell renders only under `>` and
    /// `>=` — [`aggregate_having_sql`]'s containment rule — so the four
    /// anti-monotone operators expect NO fragment for every literal,
    /// in-range or not, and the two monotone ones carry this test's own
    /// subject. The two halves are independently visible: dropping the
    /// operator gate reddens 24 of these 84 cases (six literals x four
    /// operators) and dropping the threshold bound reddens the other 18.
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
        // `max(duration)` reads HIGH over the generator's rows, so only
        // `>` and `>=` are safe; the other four LOSE a trace and refuse
        // whatever the literal is.
        let monotone = [">", ">="];
        let anti_monotone = ["=", "!=", "<", "<="];

        // Collected rather than asserted one at a time: on a build with
        // the defect several rows are wrong at once, and the whole set is
        // what says where the boundary moved to.
        let mut wrong: Vec<String> = Vec::new();
        for (literal, in_range) in rows {
            for (op, wants_push) in monotone
                .iter()
                .map(|op| (*op, in_range))
                .chain(anti_monotone.iter().map(|op| (*op, false)))
            {
                let q = format!(r#"{{ span.http.method = "GET" }} | max(duration) {op} {literal}"#);
                let query = pulsus_traceql::parse(&q).unwrap_or_else(|e| panic!("{q}: {e}"));
                let stage = query
                    .pipeline
                    .iter()
                    .find(|s| matches!(s, PipelineStage::Aggregate { .. }))
                    .unwrap_or_else(|| panic!("{q}: parsed without an aggregate stage"));
                let frag = aggregate_having_sql(stage, None);
                if frag.is_some() != wants_push {
                    wrong.push(format!(
                        "{q}: expected {}, got {frag:?}",
                        if wants_push {
                            "a fragment"
                        } else {
                            "no fragment"
                        }
                    ));
                }
                let Some(frag) = frag else { continue };
                // The fragment's integer IS the evaluator's `f64`.
                let PipelineStage::Aggregate {
                    op: agg,
                    field,
                    value,
                    ..
                } = stage
                else {
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
                if (rendered as f64).to_bits() != evaluator.to_bits() {
                    wrong.push(format!(
                        "{q}: the statement compares against {rendered} and the evaluator \
                         against {evaluator}"
                    ));
                }
            }
        }
        assert!(
            wrong.is_empty(),
            "{} of {} cases disagree with the boundary:\n{}",
            wrong.len(),
            rows.len() * (monotone.len() + anti_monotone.len()),
            wrong.join("\n")
        );
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
    /// Issue #492 part 7, criterion 1: **`select()` never lowers, and the
    /// reason it names is decided by the field, not by the source.**
    ///
    /// `SelectLower::capability` has two refusal arms and they are not
    /// interchangeable on the explain surface: `NameNotResolvable`
    /// renders as `name_not_resolvable` (`compile/plan.rs:879`) and is
    /// what a client sees for every attribute spelling, because a
    /// TraceQL seed's `ColSet` is `Closed([trace_id, name])` and no
    /// attribute resolves in it. Only `select(name)` reaches the
    /// `NotYetLowered` arm.
    ///
    /// Both seed sources are asserted because the refusal is a property
    /// of the link, not of the table it is folded against — a capability
    /// that answered `Yes` on one source and `No` on the other would be
    /// a lowering nobody decided.
    ///
    /// `docs/query-lowering.md` §3.1's `Select` row and §9.8 record this
    /// split; this test is what makes those two sentences fail if the
    /// dispatcher stops agreeing with them.
    #[test]
    fn select_refuses_and_names_its_reason_per_field() {
        let want: [(&str, Capability); 6] = [
            (
                r#"{ .a = "1" } | select(name)"#,
                Capability::No(BlockReason::NotYetLowered),
            ),
            (
                r#"{ .a = "1" } | select(span.http.method)"#,
                Capability::No(BlockReason::NameNotResolvable),
            ),
            (
                r#"{ .a = "1" } | select(.foo)"#,
                Capability::No(BlockReason::NameNotResolvable),
            ),
            (
                r#"{ .a = "1" } | select(resource.service.name)"#,
                Capability::No(BlockReason::NameNotResolvable),
            ),
            (
                r#"{ .a = "1" } | select(status)"#,
                Capability::No(BlockReason::NameNotResolvable),
            ),
            (
                r#"{ .a = "1" } | select(name, span.http.method)"#,
                Capability::No(BlockReason::NameNotResolvable),
            ),
        ];
        for (q, expect) in want {
            let link = TqlLink::Pipe(pipe(q));
            for src in [TRACE_SPANS, TRACE_ATTRS_IDX] {
                let rel = seed_relation(src, Pred::True);
                let got = <Tql as Lang>::lower_of(&link).capability(&link, &rel);
                assert_eq!(
                    got, expect,
                    "{q} on {src}: select() must refuse with {expect:?}, got {got:?}"
                );
            }
        }
    }
}
