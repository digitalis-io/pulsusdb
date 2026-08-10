//! Pure `Query + SearchParams + SearchCtx → SearchPlan` planning for the
//! two-phase TraceQL search (issue #57 plan v7; docs/schemas.md §4.2).
//! Deterministic, no I/O: classifies every leaf via
//! [`super::filter::compile_span_filter`], renders the per-generator
//! Phase-1 candidate SQL (deduped, order-preserving), registers the
//! distinct attribute membership probes / aggregate / `select()` value
//! reads Phase 2 needs, and validates the pipeline stages — every
//! rejection here is a caller error ([`PlanError`] → `400 bad_data`).

use pulsus_traceql::{
    AggregateOp, ComparisonOp, Field, FieldExpr, HintValue, Intrinsic, PipelineStage, Query,
    SpansetExpr, SpansetFilter, Value,
};
use regex::Regex;

use crate::logql::escape;
use crate::logql::sql::TimeWindow;

use super::filter::{
    self, ArithNode, AttrProbe, BoolMatch, BoolTerm, CompareOperand, EventSetField, LeafEval,
    NestedSetField, PhysicalPredicate, PlanError, SetSide, SpanFilterCtx, TraceCtxPred,
};
use super::search_sql;

/// The caller-validated request window and response caps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchParams {
    pub start_ns: i64,
    pub end_ns: i64,
    /// Result cap (`limit` param, docs/api.md §4.2).
    pub limit: u32,
    /// Spans-per-spanset cap (`spss` param).
    pub spss: u32,
}

/// Engine-derived planning context.
#[derive(Debug, Clone, Copy)]
pub struct SearchCtx<'a> {
    pub filter: SpanFilterCtx<'a>,
    /// `reader.traceql_max_candidates` — the per-generator top-K depth
    /// (`gen_cap`) *and* the merged-stream consumption ceiling.
    pub max_candidates: u64,
    /// `reader.traceql_max_series` (default 1000) — the shared `by(...)`
    /// cardinality cap (issue #185). A spanset `| by(...)` grouping more
    /// than this many distinct groups is a `422 query_too_broad`, the same
    /// cap and mechanism the metric `by(...)` clause uses.
    pub max_series: u64,
    /// Clustered mode: the engine injects the §7 clustered-reader
    /// settings on every query (co-sharding on `cityHash64(trace_id)`
    /// keeps both phases shard-local — docs/schemas.md §7).
    pub distributed: bool,
}

/// A string comparison's evaluation shape — regexes are compiled once at
/// plan time (full-value anchored, task-manager adjudication 3), so an
/// invalid pattern fails as a `400`, never mid-execution.
#[derive(Debug, Clone)]
pub(crate) enum StrOp {
    Eq,
    Neq,
    Re(Regex),
    Nre(Regex),
}

impl StrOp {
    pub(crate) fn matches(&self, expected: &str, actual: &str) -> bool {
        match self {
            StrOp::Eq => actual == expected,
            StrOp::Neq => actual != expected,
            StrOp::Re(re) => re.is_match(actual),
            StrOp::Nre(re) => !re.is_match(actual),
        }
    }
}

/// One physical leaf, ready for Phase-2 evaluation on hydrated spans.
#[derive(Debug, Clone)]
pub(crate) enum PhysicalEval {
    Name {
        op: StrOp,
        value: String,
    },
    Service {
        op: StrOp,
        value: String,
    },
    Duration {
        op: ComparisonOp,
        nanos: i64,
    },
    Status {
        op: ComparisonOp,
        code: i8,
    },
    Kind {
        op: ComparisonOp,
        code: i8,
    },
    /// `statusMessage` (issue #184) — on the hydrated `status_message`.
    StatusMessage {
        op: StrOp,
        value: String,
    },
    /// `span:id` (issue #184) — against the span id's lowercase-hex
    /// rendering (Eq/Neq values pre-lowercased at leaf compilation).
    SpanIdHex {
        op: StrOp,
        value: String,
    },
    /// `span:parentID` (issue #184) — as [`PhysicalEval::SpanIdHex`] over
    /// `parent_id`.
    ParentIdHex {
        op: StrOp,
        value: String,
    },
    /// `instrumentation:name` (issue #192) — on the hydrated `scope_name`.
    InstrumentationName {
        op: StrOp,
        value: String,
    },
    /// `instrumentation:version` (issue #192) — on the hydrated
    /// `scope_version`.
    InstrumentationVersion {
        op: StrOp,
        value: String,
    },
}

/// One trace-level intrinsic leaf (issue #184), ready for Phase-2
/// evaluation against the per-trace [`super::search_eval::TraceEvalCtx`]
/// (populated from the trace-wide co-loads — window-independent,
/// full-trace-exact).
#[derive(Debug, Clone)]
pub(crate) enum TraceCtxEval {
    /// `span:childCount` — the span's direct-child count from the
    /// child-count co-load (absent parent key ⇒ 0 children).
    ChildCount { op: ComparisonOp, value: f64 },
    /// `traceDuration`/`trace:duration` — `trace_end_ns - trace_start_ns`
    /// from the trace-context co-load.
    TraceDurationNs { op: ComparisonOp, nanos: i64 },
    /// `rootName`/`trace:rootName` — the co-load's byte-capped root name.
    RootName { op: StrOp, value: String },
    /// `rootServiceName`/`trace:rootService` — the co-load's byte-capped
    /// root service.
    RootServiceName { op: StrOp, value: String },
    /// `trace:id` — against the candidate trace id's lowercase-hex
    /// rendering (no co-load needed).
    TraceId { op: StrOp, value: String },
}

/// One resolved operand of a field-vs-field comparison (issue #183). An
/// attribute operand is interned into BOTH `agg_fields` (its `val_num`
/// read) and `select_attrs` (its `val` read) so Phase 2 has a typed value
/// with no new hydration SQL builder.
#[derive(Debug, Clone)]
pub(crate) enum PlannedOperand {
    Name,
    Service,
    Duration,
    Status,
    Kind,
    /// Issue #351 — per-span scalars from the hydrated span or a
    /// per-trace co-load. No batch indices: unlike `Attr`, nothing needs
    /// interning, but several of them REQUIRE a co-load, which
    /// `plan_operand` requests as it maps them.
    StatusMessage,
    SpanId,
    ParentId,
    TraceId,
    TraceDurationNs,
    RootName,
    RootServiceName,
    ChildCount,
    ScopeName,
    ScopeVersion,
    NestedSet(NestedSetField),
    Attr {
        str_idx: usize,
        num_idx: usize,
    },
}

/// Renders a [`CompareOperand`] as the user wrote it, for the `!`
/// type-failure message. The reference names the operand too
/// (`expression (!.a) expected a boolean`), and "the attribute" is no use
/// to anyone whose query mentions several.
fn compare_operand_display(operand: &CompareOperand) -> String {
    match operand {
        CompareOperand::Name => "name".to_string(),
        CompareOperand::Service => "resource.service.name".to_string(),
        CompareOperand::Duration => "duration".to_string(),
        CompareOperand::Status => "status".to_string(),
        CompareOperand::Kind => "kind".to_string(),
        CompareOperand::StatusMessage => "statusMessage".to_string(),
        CompareOperand::SpanId => "span:id".to_string(),
        CompareOperand::ParentId => "span:parentID".to_string(),
        CompareOperand::TraceId => "trace:id".to_string(),
        CompareOperand::TraceDurationNs => "trace:duration".to_string(),
        CompareOperand::RootName => "trace:rootName".to_string(),
        CompareOperand::RootServiceName => "trace:rootService".to_string(),
        CompareOperand::ChildCount => "span:childCount".to_string(),
        CompareOperand::ScopeName => "instrumentation:name".to_string(),
        CompareOperand::ScopeVersion => "instrumentation:version".to_string(),
        CompareOperand::NestedSet(f) => match f {
            NestedSetField::Parent => "nestedSetParent".to_string(),
            NestedSetField::Left => "nestedSetLeft".to_string(),
            NestedSetField::Right => "nestedSetRight".to_string(),
        },
        CompareOperand::Attr { key, scope } => match scope {
            Some(scope) => format!("{scope}.{key}"),
            None => format!(".{key}"),
        },
    }
}

/// One planned leaf — pre-order within its spanset filter, exactly the
/// traversal `search_eval` replays.
#[derive(Debug, Clone)]
pub(crate) enum PlannedLeafEval {
    Physical(PhysicalEval),
    /// Membership in `probes[probe_idx]`'s batch result set; `negated`
    /// applies the ratified `!=`/`!~` absent-key rule.
    Attr {
        probe_idx: usize,
        negated: bool,
    },
    /// A nested-set structural intrinsic comparison (issue #181),
    /// evaluated against the per-trace query-time numbering.
    NestedSet {
        field: NestedSetField,
        op: ComparisonOp,
        value: f64,
    },
    /// The operand of a `!` (issue #335 Stage B) — `{ !.a }`,
    /// `{ !.a = true }`, `{ !.a = 1 }` — evaluated from the operand's
    /// resolved VALUE so absent (no match) and present-non-boolean (whole
    /// query fails) stay distinguishable. `{ .a }` is NOT here: it is the
    /// plain comparison `.a = true`.
    BoolTruth {
        operand: PlannedOperand,
        want: BoolMatch,
        /// The operand as the user wrote it (`.a`, `span.a`, `name`), for
        /// the type-failure message. Rendered HERE because
        /// `PlannedOperand::Attr` keeps only batch indices — the key is
        /// still in scope at plan time and gone by evaluation. Built once
        /// per leaf per query, never per span.
        display: String,
    },
    /// A field-vs-field comparison (issue #183 `comparison.rhs_attribute`),
    /// evaluated per candidate span from both operands' resolved values.
    FieldCompare {
        lhs: PlannedOperand,
        rhs: PlannedOperand,
        op: ComparisonOp,
    },
    /// A trace-level intrinsic comparison (issue #184), evaluated against
    /// the per-trace context co-load.
    TraceCtx(TraceCtxEval),
    /// An arithmetic comparison (issue #185 `arith.*`), evaluated per
    /// candidate span from both resolved operand trees.
    Arith {
        lhs: PlannedArith,
        op: ComparisonOp,
        rhs: PlannedArith,
    },
    /// A static-vs-static comparison, folded at plan time (issue #351) —
    /// `{ "x" = "y" }`. No per-span work at all.
    Const(bool),
    /// A boolean-vs-boolean comparison (issue #351) — `{ .a = .b = .c }`,
    /// `{ !.a = !.b }`.
    BoolCompare {
        lhs: PlannedBoolTerm,
        rhs: PlannedBoolTerm,
        op: ComparisonOp,
    },
    /// A multi-valued event/link comparison (issue #351) — ANY-match,
    /// `!=` ALL-match, evaluated against `event_sets[set_idx]`'s per-span
    /// value set.
    EventSetCompare {
        set_idx: usize,
        scalar: PlannedOperand,
        op: ComparisonOp,
        side: SetSide,
    },
}

/// One planned [`BoolTerm`] (issue #351). Mirrors the compiled term, with
/// attribute operands interned into the batch value reads and the `!`
/// operand's display rendered once per query for the type-failure
/// message — never per span.
#[derive(Debug, Clone)]
pub(crate) enum PlannedBoolTerm {
    Const(bool),
    Value(PlannedOperand),
    Not {
        term: Box<PlannedBoolTerm>,
        /// The negated operand as the user wrote it, for the
        /// `expression (!{display}) expected a boolean` failure — the
        /// same rendering [`PlannedLeafEval::BoolTruth`] carries.
        display: String,
    },
    Nested(Box<PlannedLeafEval>),
}

/// A planned arithmetic operand tree (issue #185): literals are folded;
/// attribute operands are interned into the `val_num` reads Phase 2
/// resolves them from.
#[derive(Debug, Clone)]
pub(crate) enum PlannedArith {
    Value(f64),
    Operand(PlannedOperand),
    Neg(Box<PlannedArith>),
    Bin {
        op: pulsus_traceql::ArithOp,
        lhs: Box<PlannedArith>,
        rhs: Box<PlannedArith>,
    },
}

/// One planned `{...}` spanset filter (pre-order over the spanset
/// expression tree).
#[derive(Debug, Clone)]
pub(crate) struct PlannedFilter {
    pub(crate) leaves: Vec<PlannedLeafEval>,
}

/// One attribute field read for aggregates / `select()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AttrFieldRef {
    pub(crate) key: String,
    pub(crate) scope: Option<&'static str>,
}

/// A validated pipeline aggregate stage.
#[derive(Debug, Clone)]
pub(crate) struct PlannedAggregate {
    pub(crate) op: AggregateOp,
    pub(crate) source: AggSource,
    pub(crate) cmp: ComparisonOp,
    pub(crate) threshold: f64,
}

#[derive(Debug, Clone)]
pub(crate) enum AggSource {
    /// `count()`.
    Count,
    /// `avg|sum|min|max(duration)` over matched spans' `duration_ns`.
    DurationNs,
    /// `avg|sum|min|max(.attr)` over the field's `val_num` read
    /// (`agg_fields[idx]`).
    Attr { field_idx: usize },
}

/// One `select()`-projected response field.
#[derive(Debug, Clone)]
pub(crate) enum SelectField {
    /// Rendered from the hydrated physical columns; `display` is the
    /// TraceQL spelling (`name`, `resource.service.name`, …).
    Physical {
        display: String,
        column: PhysicalSelect,
    },
    /// Rendered from the `select_attrs[idx]` value read.
    Attr { display: String, field_idx: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PhysicalSelect {
    Name,
    Service,
    DurationNs,
    Status,
    Kind,
}

/// One `| by(<keys>)` grouping key (issue #193): the display spelling for
/// the response spanSet `attributes` entry plus the per-span value
/// resolver Phase 2 partitions on. The resolver reuses the SAME interned
/// physical-column / attribute value reads as `select()` and the field
/// operands, so grouping adds no new scan shape (the query-perf mandate).
#[derive(Debug, Clone)]
pub(crate) struct PlannedGroupKey {
    pub(crate) display: String,
    pub(crate) resolver: GroupKeyResolver,
}

/// How one `by()` key's value is resolved for a hydrated span (issue
/// #193). Every by-key form the read-path can resolve to a per-span scalar
/// is covered — physical columns, the trace-level / nested-set / scoped
/// intrinsics #184/#185/#192 already hydrate, and attributes — so no
/// resolvable key silently falls back to the flat response. The only
/// excluded class (multi-valued span-EVENT / span-LINK intrinsics) is a
/// clean `400`, never a flat 200 (see [`plan_group_key`]).
#[derive(Debug, Clone)]
pub(crate) enum GroupKeyResolver {
    /// A physical span column (service/name/duration/status/kind).
    Physical(PhysicalSelect),
    /// `statusMessage` (issue #184) — the hydrated `status_message` string.
    StatusMessage,
    /// `span:id` / `span:parentID` / `trace:id` (issue #184) — the id's
    /// lowercase-hex rendering (`trace:id` is trace-constant).
    SpanIdHex,
    ParentIdHex,
    TraceIdHex,
    /// `instrumentation:name` / `instrumentation:version` (issue #192) —
    /// the hydrated `scope_name` / `scope_version` string.
    InstrumentationName,
    InstrumentationVersion,
    /// `nestedSetParent`/`Left`/`Right` (issue #181) — the per-trace
    /// query-time nested-set numbering (an integer).
    NestedSet(NestedSetField),
    /// `traceDuration` (issue #184) — `trace_end_ns - trace_start_ns` from
    /// the trace-wide context co-load (an integer, nanoseconds).
    TraceDuration,
    /// `rootName` / `rootServiceName` (issue #184) — the co-load's
    /// byte-capped root name / service (a string).
    RootName,
    RootServiceName,
    /// `span:childCount` (issue #184) — the span's direct-child count from
    /// the child-count co-load (an integer).
    ChildCount,
    /// An attribute value: interned into BOTH the numeric (`agg_fields`)
    /// and string (`select_attrs`) reads so the group value is typed —
    /// a `val_num` reading renders `doubleValue`, else the `val` string
    /// renders `stringValue` (the differential pins the exact typing).
    Attr {
        str_idx: usize,
        num_idx: usize,
    },
}

/// One ordered post-filter spanset stage (issue #193): `by()` regroups the
/// matched spans into one spanSet per distinct key-tuple, `coalesce()`
/// merges the current spanSets back into one. Ordered (not flattened
/// flags) so `by()|coalesce()` and `coalesce()|by()` stay distinguishable.
#[derive(Debug, Clone)]
pub(crate) enum SpansetStage {
    By(Vec<PlannedGroupKey>),
    Coalesce,
}

/// The complete, deterministic search plan — everything
/// [`super::exec::TraceEngine::search`] executes, and the golden surface
/// `tests/traces_search_sql.rs` byte-pins (via [`SearchPlan::generator_sqls`]
/// plus the `search_sql` builders it drives per batch).
#[derive(Debug, Clone)]
pub struct SearchPlan {
    pub(crate) window: TimeWindow,
    pub(crate) limit: u32,
    pub(crate) spss: u32,
    pub(crate) max_candidates: u64,
    pub(crate) distributed: bool,
    pub(crate) spans_table: String,
    pub(crate) attrs_table: String,
    /// Deduped Phase-1 generator queries, in first-appearance order.
    pub generator_sqls: Vec<String>,
    /// The spanset expression tree (cloned AST) Phase 2 evaluates.
    pub(crate) spanset: SpansetExpr,
    /// Per-filter leaf evaluations, pre-order over `spanset`.
    pub(crate) filters: Vec<PlannedFilter>,
    /// Whether any planned leaf is a nested-set structural intrinsic
    /// (issue #181) — gates the per-trace query-time numbering in Phase 2
    /// so non-nested-set queries pay nothing.
    pub(crate) nested_set: bool,
    /// Whether any planned leaf reads the trace-level context co-load
    /// (`traceDuration`/`rootName`/`rootServiceName`, issue #184) — gates
    /// the per-batch trace-wide `trace_ctx_sql` read so other queries pay
    /// nothing.
    pub(crate) trace_ctx: bool,
    /// Whether any planned leaf reads the direct-child-count co-load
    /// (`span:childCount`, issue #184) — gates the per-batch trace-wide
    /// `child_count_sql` read.
    pub(crate) child_count: bool,
    /// Distinct attribute membership probes (batch reads).
    pub(crate) probes: Vec<AttrProbe>,
    /// Each probe's pre-escaped positive predicate, index-aligned with
    /// [`Self::probes`] and rendered AT PLAN TIME (issue #282). Rendering
    /// is what validates a probe's regex, so it has to happen where a
    /// rejection is still a `400`; caching the result keeps
    /// [`Self::membership_sql_for`] — a per-batch, mid-execution call —
    /// infallible.
    pub(crate) probe_predicates: Vec<String>,
    /// Distinct attribute aggregate `val_num` reads.
    pub(crate) agg_fields: Vec<AttrFieldRef>,
    /// Distinct attribute `select()` `val` reads.
    pub(crate) select_attrs: Vec<AttrFieldRef>,
    /// Distinct MULTI-VALUED event/link SET reads (issue #351), in
    /// first-appearance order. Empty for every query that does not
    /// compare an `event:`/`link:` intrinsic against another field, so
    /// nothing else pays for the co-load.
    pub(crate) event_sets: Vec<EventSetField>,
    pub(crate) aggregates: Vec<PlannedAggregate>,
    pub(crate) select_fields: Vec<SelectField>,
    /// Spanset-level `| by(fields)` grouping keys (issue #185); empty when
    /// absent. The executor enforces `reader.traceql_max_series` over the
    /// distinct group cardinality (`422 query_too_broad`).
    pub(crate) group_by: Vec<Field>,
    /// Whether a `| coalesce()` stage is present (issue #185).
    pub(crate) coalesce: bool,
    /// The ordered spanset post-stages (issue #193): `by()`/`coalesce()`
    /// in pipeline order, carrying the resolved group keys. Phase 2
    /// reshapes the response from these; empty for a plain (flat) query,
    /// keeping the default response byte-identical.
    pub(crate) post_stages: Vec<SpansetStage>,
    /// The `with(most_recent=true)` search hint (issue #185): the response
    /// keeps its recency ordering (the default), most-recent first.
    pub(crate) most_recent: bool,
    /// The spanset `| by(...)` cardinality pre-flight probe SQL (issue
    /// #185): `Some` when the `by()` keys and spanset shape admit the
    /// distinct-by-key `GROUP BY <keys> LIMIT cap+1` probe (a single
    /// `{...}` filter grouped by `resource.service.name`). The executor
    /// runs it before the main search and flips a `cap+1` result to a
    /// static `422 query_too_broad`.
    pub(crate) by_probe_sql: Option<String>,
}

impl SearchPlan {
    pub fn limit(&self) -> u32 {
        self.limit
    }

    /// The spans-per-spanset cap (issue #57 re-audit v7, visibility-only:
    /// the AC-A4 retained-accumulation gate's Q6 pin reads the PLAN's
    /// cap — the runtime source — mirroring [`Self::limit`]).
    pub fn spss(&self) -> u32 {
        self.spss
    }

    pub fn max_candidates(&self) -> u64 {
        self.max_candidates
    }

    /// The spanset-level `| by(fields)` grouping keys (issue #185); empty
    /// when the query has no `by()` stage. The executor enforces the shared
    /// `reader.traceql_max_series` cardinality cap over these keys.
    pub fn group_by(&self) -> &[Field] {
        &self.group_by
    }

    /// Whether the query carries a `| coalesce()` stage (issue #185).
    pub fn coalesce(&self) -> bool {
        self.coalesce
    }

    /// Whether the query carries a `with(most_recent=true)` hint (issue
    /// #185); the response keeps its default recency ordering.
    pub fn most_recent(&self) -> bool {
        self.most_recent
    }

    /// The ordered spanset post-stages (issue #193) — `by()`/`coalesce()`
    /// in pipeline order. Empty when the query is flat.
    pub(crate) fn post_stages(&self) -> &[SpansetStage] {
        &self.post_stages
    }

    /// The spanset `| by(...)` cardinality pre-flight probe SQL (issue
    /// #185), when one applies — exposed for the executor's cap
    /// enforcement and the golden suite.
    pub fn by_probe_sql(&self) -> Option<&str> {
        self.by_probe_sql.as_deref()
    }

    /// Whether the plan was built against `_dist` tables — the engine's
    /// own config gates the clustered settings; this mirrors it for
    /// callers/tests.
    pub fn distributed(&self) -> bool {
        self.distributed
    }

    /// Number of distinct attribute membership probes (golden suite).
    pub fn probes_len(&self) -> usize {
        self.probes.len()
    }

    /// Number of distinct aggregate `val_num` field reads (golden suite).
    pub fn agg_fields_len(&self) -> usize {
        self.agg_fields.len()
    }

    /// Number of distinct `select()` `val` field reads (golden suite).
    pub fn select_attrs_len(&self) -> usize {
        self.select_attrs.len()
    }

    /// Number of distinct event/link SET reads (issue #351; golden
    /// suite). Zero for every query that compares no `event:`/`link:`
    /// intrinsic against another field.
    pub fn event_sets_len(&self) -> usize {
        self.event_sets.len()
    }

    /// One membership read's SQL for a candidate batch (exposed for the
    /// golden suite; `exec` drives the same builder).
    pub fn membership_sql_for(&self, probe_idx: usize, trace_ids: &[[u8; 16]]) -> String {
        search_sql::membership_sql(
            &self.attrs_table,
            &self.probe_predicates[probe_idx],
            trace_ids,
            self.window,
        )
    }

    /// The batch hydration SQL (exposed for the golden suite).
    pub fn hydration_sql_for(&self, trace_ids: &[[u8; 16]]) -> String {
        search_sql::hydration_sql(
            &self.spans_table,
            trace_ids,
            self.window,
            super::exec::MAX_SPANS_PER_TRACE,
        )
    }

    /// The winners' root-hydration SQL (exposed for the golden suite) —
    /// trace-wide, no time predicate, no row cap.
    pub fn root_sql_for(&self, trace_ids: &[[u8; 16]]) -> String {
        search_sql::root_sql(&self.spans_table, trace_ids)
    }

    /// Whether the plan issues the per-batch trace-level context co-load
    /// (issue #184 — `traceDuration`/`rootName`/`rootServiceName` leaves).
    pub fn needs_trace_ctx(&self) -> bool {
        self.trace_ctx
    }

    /// Whether the plan issues the per-batch direct-child-count co-load
    /// (issue #184 — `span:childCount` leaves).
    pub fn needs_child_counts(&self) -> bool {
        self.child_count
    }

    /// One batch's trace-level context co-load SQL (exposed for the
    /// golden suite; `exec` drives the same builder) — trace-wide, no
    /// time predicate, no row cap (issue #184).
    pub fn trace_ctx_sql_for(&self, trace_ids: &[[u8; 16]]) -> String {
        search_sql::trace_ctx_sql(&self.spans_table, trace_ids)
    }

    /// One batch's direct-child-count co-load SQL (exposed for the golden
    /// suite; `exec` drives the same builder) — trace-wide (issue #184).
    pub fn child_count_sql_for(&self, trace_ids: &[[u8; 16]]) -> String {
        search_sql::child_count_sql(&self.spans_table, trace_ids)
    }

    /// One aggregate field's `val_num` batch read (exposed for the
    /// golden suite; `exec` drives the same builder).
    pub fn agg_values_sql_for(&self, field_idx: usize, trace_ids: &[[u8; 16]]) -> String {
        let field = &self.agg_fields[field_idx];
        search_sql::attr_values_sql(
            &self.attrs_table,
            &escape::ch_string(&field.key),
            field.scope.map(escape::ch_string).as_deref(),
            true,
            trace_ids,
            self.window,
        )
    }

    /// One `select()` field's `val` batch read (exposed for the golden
    /// suite; `exec` drives the same builder).
    pub fn select_values_sql_for(&self, field_idx: usize, trace_ids: &[[u8; 16]]) -> String {
        let field = &self.select_attrs[field_idx];
        search_sql::attr_values_sql(
            &self.attrs_table,
            &escape::ch_string(&field.key),
            field.scope.map(escape::ch_string).as_deref(),
            false,
            trace_ids,
            self.window,
        )
    }

    /// One event/link intrinsic's per-span VALUE SET batch read (issue
    /// #351; exposed for the golden suite, `exec` drives the same
    /// builder).
    pub fn event_set_sql_for(&self, set_idx: usize, trace_ids: &[[u8; 16]]) -> String {
        search_sql::event_set_sql(
            &self.attrs_table,
            self.event_sets[set_idx],
            trace_ids,
            self.window,
        )
    }

    /// Whether this plan reads any event/link value set (issue #351) —
    /// gates the per-batch co-load so every other query pays nothing.
    pub fn needs_event_sets(&self) -> bool {
        !self.event_sets.is_empty()
    }
}

/// The full positive predicate of one membership probe, pre-escaped.
/// Fallible since issue #282 — `value_pred_sql`'s regex arm renders
/// through the checked escaper, so this call IS the probe's regex
/// validation and runs once, at plan time.
fn membership_predicate(probe: &AttrProbe) -> Result<String, PlanError> {
    let mut parts = vec![format!("key = {}", escape::ch_string(&probe.key))];
    parts.push(filter::value_pred_sql(&probe.pred)?);
    if let Some(scope) = probe.scope {
        parts.push(format!("scope = {}", escape::ch_string(scope)));
    }
    Ok(parts.join(" AND "))
}

use eval_compile::planned_str_op;

/// **Leaf module — its entire contents are the Phase-2 regex compile and
/// its one legitimate caller. Nothing else may be added here** (issue
/// #282 review, finding 1).
///
/// Making `compile_anchored` merely private was not a seal: *private is a
/// scope, not a restriction*, so every other `fn` in `search_plan.rs`
/// could still call it and re-create the second plan-time regex validator
/// issue #282 removed — the one that could drift from what
/// `filter.rs` actually emits. Inside this module `compile_anchored` is
/// reachable; outside it, provably not.
///
/// The one thing that survives here is the Phase-2 EVALUATOR's compile:
/// `search_eval` needs a real [`Regex`] object to run `=~`/`!~` against
/// hydrated spans, which is a different job from deciding whether a
/// pattern may reach SQL. Returning `StrOp` rather than `Regex` is what
/// makes the second half hold — no caller can obtain the compiler itself.
///
/// Measured, in this file: a second `fn` outside this module calling
/// `compile_anchored(..)` fails with `E0425` (the name is not in scope);
/// following rustc's own suggested repair — `use eval_compile::…` — then
/// fails with `E0603: function compile_anchored is private`. Moving that
/// same `fn` inside `eval_compile` compiles it, which is exactly what
/// `tests/traces_regex_seal.rs` pins the contents of this module against.
mod eval_compile {
    use regex::Regex;

    use pulsus_traceql::ComparisonOp;

    use super::super::filter::PlanError;
    use super::StrOp;

    /// Compiles the anchored full-value regex a `=~`/`!~` leaf evaluates
    /// engine-side (physical columns only; attribute regexes evaluate in
    /// ClickHouse via `match()`). NOT a validator: `filter.rs` validates
    /// every regex as it renders it (issue #282), and this compile must
    /// never become a second opinion about which patterns are acceptable.
    fn compile_anchored(pat: &str) -> Result<Regex, PlanError> {
        // Issue #291: through the shared compile budget; the message
        // prefix is unchanged, and `RegexCompileError::Display` renders
        // an engine error verbatim.
        pulsus_re2::compile_user_regex_anchored(pat)
            .map_err(|e| PlanError::TypeMismatch(format!("invalid regex {pat:?}: {e}")))
    }

    pub(super) fn planned_str_op(op: ComparisonOp, value: &str) -> Result<StrOp, PlanError> {
        Ok(match op {
            ComparisonOp::Eq => StrOp::Eq,
            ComparisonOp::Neq => StrOp::Neq,
            ComparisonOp::Re => StrOp::Re(compile_anchored(value)?),
            ComparisonOp::Nre => StrOp::Nre(compile_anchored(value)?),
            _ => {
                return Err(PlanError::TypeMismatch(
                    "string fields support only = != =~ !~".to_string(),
                ));
            }
        })
    }
}

fn plan_physical(p: &PhysicalPredicate) -> Result<PhysicalEval, PlanError> {
    Ok(match p {
        PhysicalPredicate::Name { op, value } => PhysicalEval::Name {
            op: planned_str_op(*op, value)?,
            value: value.clone(),
        },
        PhysicalPredicate::Service { op, value } => PhysicalEval::Service {
            op: planned_str_op(*op, value)?,
            value: value.clone(),
        },
        PhysicalPredicate::DurationNs { op, nanos } => PhysicalEval::Duration {
            op: *op,
            nanos: *nanos,
        },
        PhysicalPredicate::Status { op, code } => PhysicalEval::Status {
            op: *op,
            code: *code,
        },
        PhysicalPredicate::Kind { op, code } => PhysicalEval::Kind {
            op: *op,
            code: *code,
        },
        PhysicalPredicate::StatusMessage { op, value } => PhysicalEval::StatusMessage {
            op: planned_str_op(*op, value)?,
            value: value.clone(),
        },
        PhysicalPredicate::SpanIdHex { op, value } => PhysicalEval::SpanIdHex {
            op: planned_str_op(*op, value)?,
            value: value.clone(),
        },
        PhysicalPredicate::ParentIdHex { op, value } => PhysicalEval::ParentIdHex {
            op: planned_str_op(*op, value)?,
            value: value.clone(),
        },
        PhysicalPredicate::InstrumentationName { op, value } => PhysicalEval::InstrumentationName {
            op: planned_str_op(*op, value)?,
            value: value.clone(),
        },
        PhysicalPredicate::InstrumentationVersion { op, value } => {
            PhysicalEval::InstrumentationVersion {
                op: planned_str_op(*op, value)?,
                value: value.clone(),
            }
        }
    })
}

/// Plans one trace-level intrinsic leaf (issue #184): string operators
/// compile their anchored regexes here (a bad pattern is a `400` at plan
/// time, never a mid-execution error) and flag which co-load the plan
/// must issue.
fn plan_trace_ctx(
    pred: &TraceCtxPred,
    trace_ctx: &mut bool,
    child_count: &mut bool,
) -> Result<TraceCtxEval, PlanError> {
    Ok(match pred {
        TraceCtxPred::ChildCount { op, value } => {
            *child_count = true;
            TraceCtxEval::ChildCount {
                op: *op,
                value: *value,
            }
        }
        TraceCtxPred::TraceDurationNs { op, nanos } => {
            *trace_ctx = true;
            TraceCtxEval::TraceDurationNs {
                op: *op,
                nanos: *nanos,
            }
        }
        TraceCtxPred::RootName { op, value } => {
            *trace_ctx = true;
            TraceCtxEval::RootName {
                op: planned_str_op(*op, value)?,
                value: value.clone(),
            }
        }
        TraceCtxPred::RootServiceName { op, value } => {
            *trace_ctx = true;
            TraceCtxEval::RootServiceName {
                op: planned_str_op(*op, value)?,
                value: value.clone(),
            }
        }
        // `trace:id` needs no co-load — the candidate's id is in hand.
        TraceCtxPred::TraceId { op, value } => TraceCtxEval::TraceId {
            op: planned_str_op(*op, value)?,
            value: value.clone(),
        },
    })
}

/// Interns one membership probe and, when it is new, renders its
/// positive predicate — the act that validates its regex (issue #282;
/// this replaces the separate `validate_probe` pre-check, at the same
/// point in the leaf walk, so rejection ordering is unchanged). A bad
/// pattern is a `400` here, never a mid-query server error.
fn intern_probe(
    probe: &AttrProbe,
    probes: &mut Vec<AttrProbe>,
    predicates: &mut Vec<String>,
) -> Result<usize, PlanError> {
    let idx = intern(probes, probe);
    if predicates.len() < probes.len() {
        predicates.push(membership_predicate(probe)?);
    }
    Ok(idx)
}

fn collect_filters<'q>(expr: &'q SpansetExpr, out: &mut Vec<&'q SpansetFilter>) {
    match expr {
        SpansetExpr::Filter(f) => out.push(f),
        // Structural relations (issue #172) plan exactly like `&&`/`||`:
        // lhs-then-rhs pre-order — the same traversal `search_eval`
        // replays — and the superset union of both operands' generators
        // (the relation itself is Phase-2 engine work over hydrated
        // spans, so the emitted SQL is byte-identical to the equivalent
        // `{A} && {B}` plan — the AC4 identity pin).
        SpansetExpr::Binary { lhs, rhs, .. } | SpansetExpr::Structural { lhs, rhs, .. } => {
            collect_filters(lhs, out);
            collect_filters(rhs, out);
        }
    }
}

fn intern<T: PartialEq + Clone>(items: &mut Vec<T>, item: &T) -> usize {
    if let Some(idx) = items.iter().position(|existing| existing == item) {
        idx
    } else {
        items.push(item.clone());
        items.len() - 1
    }
}

fn attr_field_ref(field: &Field) -> Option<AttrFieldRef> {
    match field {
        Field::Attribute { scope, key } => Some(AttrFieldRef {
            key: key.clone(),
            scope: match scope {
                pulsus_traceql::AttrScope::Span => Some("span"),
                pulsus_traceql::AttrScope::Resource => Some("resource"),
                pulsus_traceql::AttrScope::Unscoped => None,
                pulsus_traceql::AttrScope::Instrumentation => Some("instrumentation"),
                pulsus_traceql::AttrScope::Event => Some("event"),
                pulsus_traceql::AttrScope::Link => Some("link"),
            },
        }),
        Field::Intrinsic(_) => None,
    }
}

/// Plans one field-vs-field comparison operand (issue #183): a physical
/// intrinsic resolves from the hydrated columns (no read registered); an
/// attribute is interned into BOTH the `val` (`select_attrs`) and the
/// `val_num` (`agg_fields`) reads so Phase 2 has a typed value.
/// Maps a compiled operand to its planned form, REQUESTING any per-batch
/// co-load it needs on the way (issue #351).
///
/// The co-load flags are the same ones `plan_trace_ctx` sets for the
/// literal-comparison path, so a field-vs-field operand pays for exactly
/// what a literal comparison against the same intrinsic already pays for.
/// Forgetting one would not fail to compile — it would resolve to `None`
/// per span and silently match nothing, so each arm sets its flag beside
/// the mapping rather than in a separate pass.
fn plan_operand(
    operand: &CompareOperand,
    agg_fields: &mut Vec<AttrFieldRef>,
    select_attrs: &mut Vec<AttrFieldRef>,
    nested_set: &mut bool,
    trace_ctx: &mut bool,
    child_count: &mut bool,
) -> PlannedOperand {
    match operand {
        CompareOperand::Name => PlannedOperand::Name,
        CompareOperand::Service => PlannedOperand::Service,
        CompareOperand::Duration => PlannedOperand::Duration,
        CompareOperand::Status => PlannedOperand::Status,
        CompareOperand::Kind => PlannedOperand::Kind,
        CompareOperand::StatusMessage => PlannedOperand::StatusMessage,
        CompareOperand::SpanId => PlannedOperand::SpanId,
        CompareOperand::ParentId => PlannedOperand::ParentId,
        CompareOperand::TraceId => PlannedOperand::TraceId,
        CompareOperand::ScopeName => PlannedOperand::ScopeName,
        CompareOperand::ScopeVersion => PlannedOperand::ScopeVersion,
        CompareOperand::TraceDurationNs => {
            *trace_ctx = true;
            PlannedOperand::TraceDurationNs
        }
        CompareOperand::RootName => {
            *trace_ctx = true;
            PlannedOperand::RootName
        }
        CompareOperand::RootServiceName => {
            *trace_ctx = true;
            PlannedOperand::RootServiceName
        }
        CompareOperand::ChildCount => {
            *child_count = true;
            PlannedOperand::ChildCount
        }
        CompareOperand::NestedSet(f) => {
            *nested_set = true;
            PlannedOperand::NestedSet(*f)
        }
        CompareOperand::Attr { key, scope } => {
            let field_ref = AttrFieldRef {
                key: key.clone(),
                scope: *scope,
            };
            PlannedOperand::Attr {
                str_idx: intern(select_attrs, &field_ref),
                num_idx: intern(agg_fields, &field_ref),
            }
        }
    }
}

/// Plans one arithmetic operand tree (issue #185): folded literals stay
/// scalars; attribute operands intern their `val_num` read (via
/// [`plan_operand`]) so Phase 2 resolves a typed numeric value with no new
/// hydration builder.
fn plan_arith(
    node: &ArithNode,
    agg_fields: &mut Vec<AttrFieldRef>,
    select_attrs: &mut Vec<AttrFieldRef>,
    nested_set: &mut bool,
    trace_ctx: &mut bool,
    child_count: &mut bool,
) -> PlannedArith {
    match node {
        ArithNode::Value(v) => PlannedArith::Value(*v),
        ArithNode::Operand(operand) => PlannedArith::Operand(plan_operand(
            operand,
            agg_fields,
            select_attrs,
            nested_set,
            trace_ctx,
            child_count,
        )),
        ArithNode::Neg(inner) => PlannedArith::Neg(Box::new(plan_arith(
            inner,
            agg_fields,
            select_attrs,
            nested_set,
            trace_ctx,
            child_count,
        ))),
        ArithNode::Bin { op, lhs, rhs } => PlannedArith::Bin {
            op: *op,
            lhs: Box::new(plan_arith(
                lhs,
                agg_fields,
                select_attrs,
                nested_set,
                trace_ctx,
                child_count,
            )),
            rhs: Box::new(plan_arith(
                rhs,
                agg_fields,
                select_attrs,
                nested_set,
                trace_ctx,
                child_count,
            )),
        },
    }
}

fn aggregate_threshold(
    op: AggregateOp,
    field: &Option<FieldExpr>,
    value: &Value,
) -> Result<f64, PlanError> {
    match value {
        Value::Number(raw) => raw
            .parse::<f64>()
            .ok()
            .filter(|n| n.is_finite())
            .ok_or_else(|| PlanError::TypeMismatch(format!("not a finite number: {raw:?}"))),
        // A duration threshold is meaningful only against a duration
        // aggregate (nanosecond scale).
        Value::Duration(d)
            if matches!(
                field,
                Some(FieldExpr::Field(Field::Intrinsic(Intrinsic::Duration)))
            ) && op != AggregateOp::Count =>
        {
            Ok(d.as_nanos() as f64)
        }
        _ => Err(PlanError::TypeMismatch(
            "aggregate comparisons require a numeric (or duration, for duration aggregates) \
             threshold"
                .to_string(),
        )),
    }
}

/// The result of planning a search pipeline: engine-side aggregates,
/// `select()` projections, and the spanset-level `by(...)`/`coalesce()`
/// stages (issue #185).
struct PlannedPipeline {
    aggregates: Vec<PlannedAggregate>,
    select_fields: Vec<SelectField>,
    /// Spanset-level `| by(fields)` grouping keys (issue #185); empty when
    /// absent. Bounded at execution by `reader.traceql_max_series`
    /// (`422 query_too_broad`), the same cap the metric `by(...)` clause
    /// uses.
    group_by: Vec<Field>,
    /// Whether a `| coalesce()` stage is present (issue #185).
    coalesce: bool,
    /// The ordered `by()`/`coalesce()` post-stages with resolved group
    /// keys (issue #193).
    post_stages: Vec<SpansetStage>,
}

fn plan_pipeline(
    query: &Query,
    agg_fields: &mut Vec<AttrFieldRef>,
    select_attrs: &mut Vec<AttrFieldRef>,
    nested_set: &mut bool,
    trace_ctx: &mut bool,
    child_count: &mut bool,
) -> Result<PlannedPipeline, PlanError> {
    let mut aggregates = Vec::new();
    let mut select_fields = Vec::new();
    let mut group_by = Vec::new();
    let mut coalesce = false;
    let mut post_stages: Vec<SpansetStage> = Vec::new();
    for stage in &query.pipeline {
        match stage {
            // Spanset-level grouping / coalesce (issue #185 parse, #193
            // response reshaping): `by(...)` grouping keys feed the #185
            // pre-flight cardinality probe (`group_by`) AND the #193
            // ordered `post_stages` that reshape the response. EVERY
            // resolvable by-key produces a grouped `By` stage; a genuinely
            // un-groupable key form (span-event / span-link intrinsic) is a
            // clean `400` from `plan_group_key`, never a silent flat 200.
            PipelineStage::By { fields } => {
                group_by.extend(fields.iter().cloned());
                let mut keys = Vec::with_capacity(fields.len());
                for field in fields {
                    keys.push(plan_group_key(
                        field,
                        agg_fields,
                        select_attrs,
                        nested_set,
                        trace_ctx,
                        child_count,
                    )?);
                }
                if !keys.is_empty() {
                    post_stages.push(SpansetStage::By(keys));
                }
            }
            PipelineStage::Coalesce => {
                coalesce = true;
                post_stages.push(SpansetStage::Coalesce);
            }
            PipelineStage::Aggregate {
                op,
                field,
                cmp,
                value,
            } => {
                if !matches!(
                    cmp,
                    ComparisonOp::Eq
                        | ComparisonOp::Neq
                        | ComparisonOp::Gt
                        | ComparisonOp::Gte
                        | ComparisonOp::Lt
                        | ComparisonOp::Lte
                ) {
                    return Err(PlanError::TypeMismatch(
                        "aggregate filters do not support regex operators".to_string(),
                    ));
                }
                // The argument is a full `FieldExpr` since issue #335
                // Stage C (D7): the grammar no longer decides which
                // arguments are legal, so the executable subset is
                // decided HERE. A bare `duration` or attribute is
                // planned; every other shape — a composite expression,
                // or an intrinsic with no numeric aggregation path — is
                // a clean 400.
                //
                // **This arm changed and moved no wire verdict**, which
                // is not the same claim and was measured rather than
                // assumed (Stage C review). For the shapes that could
                // reach a planner before Stage C the decision is
                // identical: `{ true } | avg(.a) > 1` renders a
                // `SearchPlan` byte-identical to the one `49cff9a`
                // rendered. The shapes this arm newly REJECTS could not
                // be constructed before — the parser refused them — so
                // no query that used to be answered stops being
                // answered. `{ true } | avg((.a)) > 1` becoming a wire
                // accept is the parser's doing, not this arm's: parens
                // do not survive into the AST, so it arrives here as the
                // already-served `avg(.a)`.
                let source = match (op, field) {
                    (AggregateOp::Count, None) => AggSource::Count,
                    (AggregateOp::Count, Some(_)) => {
                        return Err(PlanError::TypeMismatch(
                            "count() takes no field".to_string(),
                        ));
                    }
                    (_, None) => {
                        return Err(PlanError::TypeMismatch(format!("{op}() requires a field")));
                    }
                    (_, Some(FieldExpr::Field(Field::Intrinsic(Intrinsic::Duration)))) => {
                        AggSource::DurationNs
                    }
                    (_, Some(FieldExpr::Field(Field::Intrinsic(other)))) => {
                        return Err(PlanError::TypeMismatch(format!(
                            "{other} is not numerically aggregatable"
                        )));
                    }
                    (_, Some(FieldExpr::Field(attr @ Field::Attribute { .. }))) => {
                        let field_ref = attr_field_ref(attr)
                            .expect("Field::Attribute always yields a field ref");
                        AggSource::Attr {
                            field_idx: intern(agg_fields, &field_ref),
                        }
                    }
                    (_, Some(expr)) => {
                        return Err(PlanError::TypeMismatch(format!(
                            "{op}({expr}) is not an executable aggregation source: only a bare \
                             duration or attribute can be aggregated"
                        )));
                    }
                };
                aggregates.push(PlannedAggregate {
                    op: *op,
                    source,
                    cmp: *cmp,
                    threshold: aggregate_threshold(*op, field, value)?,
                });
            }
            // Metrics functions are `/api/traces/v1/metrics/*`-only (issue
            // #59): on the search surface a parsed `| rate()` is a caller
            // error (400 bad_data), never silently ignored.
            PipelineStage::Metric(stage) => {
                return Err(PlanError::TypeMismatch(format!(
                    "{} is a metrics function: use /api/traces/v1/metrics/query_range or \
                     /query, not search",
                    stage.func
                )));
            }
            PipelineStage::MetricSecondStage(stage) => {
                return Err(PlanError::TypeMismatch(format!(
                    "{stage} is a metrics second-stage operator: use \
                     /api/traces/v1/metrics/query_range or /query, not search"
                )));
            }
            PipelineStage::Compare { .. } => {
                return Err(PlanError::TypeMismatch(
                    "compare() is a metrics function: use /api/traces/v1/metrics/query_range or \
                     /query, not search"
                        .to_string(),
                ));
            }
            PipelineStage::Select { fields } => {
                for field in fields {
                    let display = field.to_string();
                    let planned = match field {
                        Field::Intrinsic(Intrinsic::Name) => SelectField::Physical {
                            display,
                            column: PhysicalSelect::Name,
                        },
                        Field::Intrinsic(Intrinsic::Duration) => SelectField::Physical {
                            display,
                            column: PhysicalSelect::DurationNs,
                        },
                        Field::Intrinsic(Intrinsic::Status) => SelectField::Physical {
                            display,
                            column: PhysicalSelect::Status,
                        },
                        Field::Intrinsic(Intrinsic::Kind) => SelectField::Physical {
                            display,
                            column: PhysicalSelect::Kind,
                        },
                        // `select(nestedSet*)` is out of scope for #181
                        // (filter-only): a clean 400, tracked as a
                        // follow-up (registry `pipeline.select` stays
                        // generic, owned by #182).
                        Field::Intrinsic(
                            Intrinsic::NestedSetParent
                            | Intrinsic::NestedSetLeft
                            | Intrinsic::NestedSetRight,
                        ) => {
                            return Err(PlanError::TypeMismatch(
                                "select() of a nested-set intrinsic is not supported".to_string(),
                            ));
                        }
                        // Issue #351: `select(span:id)` / `select(trace:id)`
                        // are accepted and project NOTHING — not a
                        // shortcut, the reference's own rule. Its
                        // response builder skips seven intrinsics when
                        // filling a span's attribute list
                        // (`pkg/traceql/engine.go:322-331` @ v3.0.2:
                        // `name`, `duration`, `traceDuration`,
                        // `rootServiceName`, `rootName`, `trace:id`,
                        // `span:id`) because each is already carried in
                        // the response envelope — `spanID`/`traceID`
                        // here, exactly as there. Measured: the body of
                        // `{ .z = "zz" } | select(span:id)` is identical
                        // to the same query with no `select()` at all,
                        // while `select(span:parentID)` DOES add an
                        // attribute.
                        //
                        // The other five skipped intrinsics need no arm:
                        // `name`/`duration` already project (their
                        // physical arms above emit the envelope's own
                        // fields), and the trace-level three are still
                        // rejected below — those are #182's rows, not
                        // this issue's.
                        Field::Intrinsic(Intrinsic::SpanId | Intrinsic::TraceId) => continue,
                        // Issue #184: `select()` projection of the
                        // trace-level/scoped intrinsics is out of scope
                        // (filtering only) — a clean 400, mirroring
                        // nested-set.
                        Field::Intrinsic(
                            Intrinsic::StatusMessage
                            | Intrinsic::ChildCount
                            | Intrinsic::ParentId
                            | Intrinsic::TraceDuration
                            | Intrinsic::RootName
                            | Intrinsic::RootServiceName
                            | Intrinsic::InstrumentationName
                            | Intrinsic::InstrumentationVersion
                            | Intrinsic::EventName
                            | Intrinsic::EventTimeSinceStart
                            | Intrinsic::LinkSpanId
                            | Intrinsic::LinkTraceId,
                        ) => {
                            return Err(PlanError::TypeMismatch(
                                "select() of this intrinsic is not supported".to_string(),
                            ));
                        }
                        Field::Attribute { scope, key }
                            if *scope == pulsus_traceql::AttrScope::Resource
                                && key == "service.name" =>
                        {
                            SelectField::Physical {
                                display,
                                column: PhysicalSelect::Service,
                            }
                        }
                        attr @ Field::Attribute { .. } => {
                            let field_ref = attr_field_ref(attr)
                                .expect("Field::Attribute always yields a field ref");
                            SelectField::Attr {
                                display,
                                field_idx: intern(select_attrs, &field_ref),
                            }
                        }
                    };
                    select_fields.push(planned);
                }
            }
        }
    }
    Ok(PlannedPipeline {
        aggregates,
        select_fields,
        group_by,
        coalesce,
        post_stages,
    })
}

/// Resolves one `by()` key `Field` to a [`PlannedGroupKey`] (issue #193),
/// interning any attribute value reads and forcing the trace-context /
/// child-count / nested-set co-loads a key needs. EVERY by-key the
/// read-path can resolve to a per-span scalar is grouped to parity — the
/// physical columns, the #181 nested-set intrinsics, the #184 trace-level
/// intrinsics, the #192 instrumentation intrinsics, and attributes. The
/// ONLY excluded class is the multi-valued span-EVENT / span-LINK
/// intrinsics (`event:name`/`event:timeSinceStart`/`link:spanID`/
/// `link:traceID`): a span carries a COLLECTION of events/links, so there
/// is no single scalar group value — grouping by them is a clean
/// [`PlanError::UnsupportedField`] (`400`), never a silent flat 200.
fn plan_group_key(
    field: &Field,
    agg_fields: &mut Vec<AttrFieldRef>,
    select_attrs: &mut Vec<AttrFieldRef>,
    nested_set: &mut bool,
    trace_ctx: &mut bool,
    child_count: &mut bool,
) -> Result<PlannedGroupKey, PlanError> {
    // Tempo v3.0.2 names the per-group group-key attribute with the `by()`
    // EXPRESSION, not the bare field (verified live via the e2e grouped
    // signature: PulsusDB `name` vs Tempo `by(name)`). So the response
    // attribute key is `by(<field-spelling>)` — `by(name)`,
    // `by(resource.service.name)`, `by(.foo)`, …
    let display = format!("by({field})");
    let resolver = match field {
        Field::Intrinsic(Intrinsic::Name) => GroupKeyResolver::Physical(PhysicalSelect::Name),
        Field::Intrinsic(Intrinsic::Duration) => {
            GroupKeyResolver::Physical(PhysicalSelect::DurationNs)
        }
        Field::Intrinsic(Intrinsic::Status) => GroupKeyResolver::Physical(PhysicalSelect::Status),
        Field::Intrinsic(Intrinsic::Kind) => GroupKeyResolver::Physical(PhysicalSelect::Kind),
        Field::Intrinsic(Intrinsic::StatusMessage) => GroupKeyResolver::StatusMessage,
        Field::Intrinsic(Intrinsic::SpanId) => GroupKeyResolver::SpanIdHex,
        Field::Intrinsic(Intrinsic::ParentId) => GroupKeyResolver::ParentIdHex,
        Field::Intrinsic(Intrinsic::TraceId) => GroupKeyResolver::TraceIdHex,
        Field::Intrinsic(Intrinsic::InstrumentationName) => GroupKeyResolver::InstrumentationName,
        Field::Intrinsic(Intrinsic::InstrumentationVersion) => {
            GroupKeyResolver::InstrumentationVersion
        }
        Field::Intrinsic(Intrinsic::NestedSetParent) => {
            *nested_set = true;
            GroupKeyResolver::NestedSet(NestedSetField::Parent)
        }
        Field::Intrinsic(Intrinsic::NestedSetLeft) => {
            *nested_set = true;
            GroupKeyResolver::NestedSet(NestedSetField::Left)
        }
        Field::Intrinsic(Intrinsic::NestedSetRight) => {
            *nested_set = true;
            GroupKeyResolver::NestedSet(NestedSetField::Right)
        }
        Field::Intrinsic(Intrinsic::TraceDuration) => {
            *trace_ctx = true;
            GroupKeyResolver::TraceDuration
        }
        Field::Intrinsic(Intrinsic::RootName) => {
            *trace_ctx = true;
            GroupKeyResolver::RootName
        }
        Field::Intrinsic(Intrinsic::RootServiceName) => {
            *trace_ctx = true;
            GroupKeyResolver::RootServiceName
        }
        Field::Intrinsic(Intrinsic::ChildCount) => {
            *child_count = true;
            GroupKeyResolver::ChildCount
        }
        // Span-event / span-link intrinsics are collection-valued per span
        // (a span has many events/links), so there is no single scalar
        // group value — a clean 400, never a silent flat 200.
        Field::Intrinsic(
            Intrinsic::EventName
            | Intrinsic::EventTimeSinceStart
            | Intrinsic::LinkSpanId
            | Intrinsic::LinkTraceId,
        ) => {
            return Err(PlanError::UnsupportedField(format!(
                "by({field}): grouping by a span-event / span-link intrinsic is not supported \
                 (a span carries a collection of events/links, so there is no single group value)"
            )));
        }
        Field::Attribute { scope, key }
            if *scope == pulsus_traceql::AttrScope::Resource && key == "service.name" =>
        {
            GroupKeyResolver::Physical(PhysicalSelect::Service)
        }
        attr @ Field::Attribute { .. } => {
            let field_ref =
                attr_field_ref(attr).expect("Field::Attribute always yields a field ref");
            GroupKeyResolver::Attr {
                str_idx: intern(select_attrs, &field_ref),
                num_idx: intern(agg_fields, &field_ref),
            }
        }
    };
    Ok(PlannedGroupKey { display, resolver })
}

/// The plan-time accumulators a leaf mapping writes into: the interned
/// attribute probes / value reads, and the co-load flags an operand
/// needs. Bundled since issue #351 because the mapping became RECURSIVE
/// (a comparison can be an operand of a comparison), and threading seven
/// `&mut` bindings through a recursive call is how they get mismatched.
struct LeafPlanSink<'a> {
    probes: &'a mut Vec<AttrProbe>,
    probe_predicates: &'a mut Vec<String>,
    agg_fields: &'a mut Vec<AttrFieldRef>,
    select_attrs: &'a mut Vec<AttrFieldRef>,
    /// The distinct event/link SET reads (issue #351).
    event_sets: &'a mut Vec<EventSetField>,
    nested_set: &'a mut bool,
    trace_ctx: &'a mut bool,
    child_count: &'a mut bool,
}

/// Maps one compiled leaf to its planned form, interning every attribute
/// read and raising every co-load flag it needs. Recursive since issue
/// #351: [`LeafEval::BoolCompare`] can hold a nested leaf.
fn plan_leaf_eval(
    eval: &LeafEval,
    sink: &mut LeafPlanSink<'_>,
) -> Result<PlannedLeafEval, PlanError> {
    Ok(match eval {
        LeafEval::Physical(p) => PlannedLeafEval::Physical(plan_physical(p)?),
        LeafEval::Attr { probe, negated } => PlannedLeafEval::Attr {
            probe_idx: intern_probe(probe, sink.probes, sink.probe_predicates)?,
            negated: *negated,
        },
        LeafEval::NestedSet { field, op, value } => {
            *sink.nested_set = true;
            PlannedLeafEval::NestedSet {
                field: *field,
                op: *op,
                value: *value,
            }
        }
        LeafEval::BoolTruth { operand, want } => PlannedLeafEval::BoolTruth {
            display: compare_operand_display(operand),
            operand: plan_operand(
                operand,
                sink.agg_fields,
                sink.select_attrs,
                sink.nested_set,
                sink.trace_ctx,
                sink.child_count,
            ),
            want: *want,
        },
        LeafEval::FieldCompare { lhs, rhs, op } => PlannedLeafEval::FieldCompare {
            lhs: plan_operand(
                lhs,
                sink.agg_fields,
                sink.select_attrs,
                sink.nested_set,
                sink.trace_ctx,
                sink.child_count,
            ),
            rhs: plan_operand(
                rhs,
                sink.agg_fields,
                sink.select_attrs,
                sink.nested_set,
                sink.trace_ctx,
                sink.child_count,
            ),
            op: *op,
        },
        LeafEval::TraceCtx(pred) => {
            PlannedLeafEval::TraceCtx(plan_trace_ctx(pred, sink.trace_ctx, sink.child_count)?)
        }
        LeafEval::Arith { lhs, op, rhs } => PlannedLeafEval::Arith {
            lhs: plan_arith(
                lhs,
                sink.agg_fields,
                sink.select_attrs,
                sink.nested_set,
                sink.trace_ctx,
                sink.child_count,
            ),
            op: *op,
            rhs: plan_arith(
                rhs,
                sink.agg_fields,
                sink.select_attrs,
                sink.nested_set,
                sink.trace_ctx,
                sink.child_count,
            ),
        },
        LeafEval::Const(v) => PlannedLeafEval::Const(*v),
        LeafEval::BoolCompare { lhs, rhs, op } => PlannedLeafEval::BoolCompare {
            lhs: plan_bool_term(lhs, sink)?,
            rhs: plan_bool_term(rhs, sink)?,
            op: *op,
        },
        LeafEval::EventSetCompare {
            set,
            scalar,
            op,
            side,
        } => PlannedLeafEval::EventSetCompare {
            set_idx: intern_event_set(*set, sink.event_sets),
            scalar: plan_operand(
                scalar,
                sink.agg_fields,
                sink.select_attrs,
                sink.nested_set,
                sink.trace_ctx,
                sink.child_count,
            ),
            op: *op,
            side: *side,
        },
    })
}

/// Interns one event/link SET read, returning its batch index. Appends
/// only, so indices stay stable — the same contract the attribute
/// interning follows.
fn intern_event_set(set: EventSetField, sets: &mut Vec<EventSetField>) -> usize {
    if let Some(idx) = sets.iter().position(|s| *s == set) {
        return idx;
    }
    sets.push(set);
    sets.len() - 1
}

/// Maps one compiled [`BoolTerm`] (issue #351), interning its reads.
fn plan_bool_term(
    term: &BoolTerm,
    sink: &mut LeafPlanSink<'_>,
) -> Result<PlannedBoolTerm, PlanError> {
    Ok(match term {
        BoolTerm::Const(v) => PlannedBoolTerm::Const(*v),
        BoolTerm::Value(operand) => PlannedBoolTerm::Value(plan_operand(
            operand,
            sink.agg_fields,
            sink.select_attrs,
            sink.nested_set,
            sink.trace_ctx,
            sink.child_count,
        )),
        BoolTerm::Not(inner) => PlannedBoolTerm::Not {
            display: bool_term_display(inner),
            term: Box::new(plan_bool_term(inner, sink)?),
        },
        BoolTerm::Nested(leaf) => PlannedBoolTerm::Nested(Box::new(plan_leaf_eval(leaf, sink)?)),
    })
}

/// The `!` operand's rendering for the type-failure message. Only the
/// [`BoolTerm::Value`] arm can ever reach that message (a nested leaf
/// always yields a boolean), so the other arms exist to keep this total.
fn bool_term_display(term: &BoolTerm) -> String {
    match term {
        BoolTerm::Value(operand) => compare_operand_display(operand),
        BoolTerm::Const(v) => v.to_string(),
        BoolTerm::Not(inner) => format!("!{}", bool_term_display(inner)),
        BoolTerm::Nested(_) => "expression".to_string(),
    }
}

/// Plans one search request. Pure and deterministic — the same inputs
/// always produce byte-identical SQL (the golden-suite contract).
pub fn plan_search(
    query: &Query,
    params: &SearchParams,
    ctx: &SearchCtx<'_>,
) -> Result<SearchPlan, PlanError> {
    let window = TimeWindow {
        start_ns: params.start_ns,
        end_ns: params.end_ns,
    };

    let mut spanset_filters = Vec::new();
    collect_filters(&query.spanset, &mut spanset_filters);

    let mut probes: Vec<AttrProbe> = Vec::new();
    let mut probe_predicates: Vec<String> = Vec::new();
    let mut filters = Vec::new();
    let mut generator_sqls: Vec<String> = Vec::new();
    let mut nested_set = false;
    let mut trace_ctx = false;
    let mut child_count = false;
    // Attribute value reads (`val_num` / `val`): declared before the
    // filter loop because a field-vs-field comparison leaf interns its
    // attribute operands here (issue #183), then `plan_pipeline` appends
    // the aggregate/`select()` reads — interning only ever appends, so
    // indices stay stable.
    let mut agg_fields: Vec<AttrFieldRef> = Vec::new();
    let mut select_attrs: Vec<AttrFieldRef> = Vec::new();
    // Issue #351: the distinct event/link SET reads, interned by the same
    // append-only rule so indices stay stable.
    let mut event_sets: Vec<EventSetField> = Vec::new();
    for spanset_filter in spanset_filters {
        let compiled = filter::compile_span_filter(spanset_filter)?;
        let mut leaves = Vec::with_capacity(compiled.leaves.len());
        for leaf in &compiled.leaves {
            let mut sink = LeafPlanSink {
                probes: &mut probes,
                probe_predicates: &mut probe_predicates,
                agg_fields: &mut agg_fields,
                select_attrs: &mut select_attrs,
                event_sets: &mut event_sets,
                nested_set: &mut nested_set,
                trace_ctx: &mut trace_ctx,
                child_count: &mut child_count,
            };
            leaves.push(plan_leaf_eval(&leaf.eval, &mut sink)?);
        }
        filters.push(PlannedFilter { leaves });
        // Cross-spanset `{A} op {B}` candidates are the superset union of
        // both operands' generators for BOTH `&&` and `||` (plan v3 —
        // exactness lives in Phase 2, never a lossy trace-id reduction).
        for generator in &compiled.generators {
            let sql = search_sql::generator_sql(
                generator,
                window,
                ctx.filter.spans_table,
                ctx.filter.attrs_table,
                ctx.max_candidates,
            );
            if !generator_sqls.contains(&sql) {
                generator_sqls.push(sql);
            }
        }
    }

    // `plan_pipeline` may force additional co-loads (issue #193): a `by()`
    // key over a nested-set / trace-level / child-count intrinsic needs the
    // same co-load its filter form does, even with no such filter leaf.
    let pipeline = plan_pipeline(
        query,
        &mut agg_fields,
        &mut select_attrs,
        &mut nested_set,
        &mut trace_ctx,
        &mut child_count,
    )?;
    // A trailing `with(most_recent=true)` search hint (issue #185): keeps
    // the response's default recency ordering (most-recent first).
    let most_recent = query
        .hints
        .iter()
        .any(|h| h.key == "most_recent" && matches!(h.value, HintValue::Bool(true)));

    // The spanset `| by(...)` cardinality cap (issue #185): the SAME
    // `reader.traceql_max_series` cap + distinct-by-key pre-flight probe as
    // the metric `by()` cap. The probe is buildable when the `by()` key is
    // `resource.service.name` (the `service` column) and the spanset is a
    // single `{...}` filter (so the "same predicate" is well-defined);
    // other by-key / composite-spanset forms still return 200 (parse-
    // supported, bounded by the search limit / scan budget — the full
    // value/response reshaping is #193).
    let by_probe_sql = by_probe_column(&pipeline.group_by)
        .and_then(|col| single_filter_body(&query.spanset).map(|body| (col, body)))
        .map(|(col, body)| {
            super::metrics_sql::search_by_probe_sql(
                ctx.filter.spans_table,
                ctx.filter.attrs_table,
                body,
                super::metrics_sql::SnappedWindow {
                    start_ns: window.start_ns,
                    end_ns: window.end_ns,
                },
                col,
                ctx.max_series,
            )
        })
        .transpose()?;

    Ok(SearchPlan {
        window,
        limit: params.limit,
        spss: params.spss,
        max_candidates: ctx.max_candidates,
        distributed: ctx.distributed,
        spans_table: ctx.filter.spans_table.to_string(),
        attrs_table: ctx.filter.attrs_table.to_string(),
        generator_sqls,
        spanset: query.spanset.clone(),
        filters,
        nested_set,
        trace_ctx,
        child_count,
        probes,
        probe_predicates,
        agg_fields,
        select_attrs,
        event_sets,
        aggregates: pipeline.aggregates,
        select_fields: pipeline.select_fields,
        group_by: pipeline.group_by,
        coalesce: pipeline.coalesce,
        post_stages: pipeline.post_stages,
        most_recent,
        by_probe_sql,
    })
}

/// The grouping column for a spanset `| by(...)` cap probe, when the keys
/// admit one (issue #185). Currently only `resource.service.name` (the
/// physical `service` column), mirroring the metric `by()` cap; other keys
/// return `None` (no probe — the query still runs, bounded by the search
/// limit / scan budget).
fn by_probe_column(group_by: &[Field]) -> Option<&'static str> {
    match group_by {
        [Field::Attribute { scope, key }]
            if *scope == pulsus_traceql::AttrScope::Resource && key == "service.name" =>
        {
            Some("service")
        }
        _ => None,
    }
}

/// The `{...}` filter body of a single-filter spanset (issue #185): the
/// `by()` cap probe's "same predicate" is only well-defined over a single
/// filter, so composite spansets (`&&`/`||`/structural) yield `None`.
fn single_filter_body(spanset: &SpansetExpr) -> Option<Option<&pulsus_traceql::FieldExpr>> {
    match spanset {
        SpansetExpr::Filter(f) => Some(f.body.as_ref()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use pulsus_traceql::parse;

    use super::*;

    fn ctx<'a>() -> SearchCtx<'a> {
        SearchCtx {
            filter: SpanFilterCtx {
                spans_table: "trace_spans",
                attrs_table: "trace_attrs_idx",
            },
            max_candidates: 100,
            max_series: 1_000,
            distributed: false,
        }
    }

    const PARAMS: SearchParams = SearchParams {
        start_ns: 1_700_000_000_000_000_000,
        end_ns: 1_700_010_800_000_000_000,
        limit: 20,
        spss: 3,
    };

    fn plan(q: &str) -> SearchPlan {
        plan_search(&parse(q).expect("parse"), &PARAMS, &ctx()).expect("plan")
    }

    #[test]
    fn identical_generators_across_spansets_are_deduped() {
        let p = plan(r#"{ .k = "v" } || { .k = "v" }"#);
        assert_eq!(p.generator_sqls.len(), 1);
        assert_eq!(p.probes.len(), 1, "identical probes intern to one read");
        assert_eq!(p.filters.len(), 2, "both filters still evaluate");
    }

    #[test]
    fn repeated_key_conjunction_registers_two_distinct_probes() {
        let p = plan(r#"{ span.a = "1" && span.a = "2" }"#);
        assert_eq!(p.probes.len(), 2);
        assert_eq!(p.filters[0].leaves.len(), 2);
    }

    #[test]
    fn a_negated_leaf_shares_its_positive_probe_and_marks_negation() {
        let p = plan(r#"{ .env != "prod" || .env = "prod" }"#);
        assert_eq!(p.probes.len(), 1);
        let negations: Vec<bool> = p.filters[0]
            .leaves
            .iter()
            .map(|l| match l {
                PlannedLeafEval::Attr { negated, .. } => *negated,
                other => panic!("expected attr leaves, got {other:?}"),
            })
            .collect();
        assert_eq!(negations, vec![true, false]);
    }

    #[test]
    fn count_pipeline_plans_an_engine_side_aggregate() {
        let p = plan(r#"{ .k = "v" } | count() > 2"#);
        assert_eq!(p.aggregates.len(), 1);
        assert!(matches!(p.aggregates[0].source, AggSource::Count));
        assert_eq!(p.aggregates[0].threshold, 2.0);
    }

    #[test]
    fn avg_duration_pipeline_accepts_a_duration_threshold_in_nanos() {
        let p = plan(r#"{ .k = "v" } | avg(duration) > 100ms"#);
        assert!(matches!(p.aggregates[0].source, AggSource::DurationNs));
        assert_eq!(p.aggregates[0].threshold, 100_000_000.0);
    }

    #[test]
    fn attr_aggregate_registers_a_val_num_field_read() {
        let p = plan(r#"{ .k = "v" } | avg(span.retries) > 1"#);
        assert!(matches!(
            p.aggregates[0].source,
            AggSource::Attr { field_idx: 0 }
        ));
        assert_eq!(
            p.agg_fields,
            vec![AttrFieldRef {
                key: "retries".to_string(),
                scope: Some("span"),
            }]
        );
    }

    #[test]
    fn aggregate_on_a_non_numeric_intrinsic_is_rejected() {
        // `avg(name)` parses since issue #335 Stage C and is rejected by
        // `validate`; the planner's own guard covers direct-AST callers,
        // so build the stage by hand.
        let mut query = parse(r#"{ .k = "v" }"#).expect("parse");
        query
            .pipeline
            .push(pulsus_traceql::PipelineStage::Aggregate {
                op: pulsus_traceql::AggregateOp::Avg,
                field: Some(FieldExpr::Field(pulsus_traceql::Field::Intrinsic(
                    Intrinsic::Name,
                ))),
                cmp: ComparisonOp::Gt,
                value: Value::Number("1".to_string()),
            });
        assert!(matches!(
            plan_search(&query, &PARAMS, &ctx()),
            Err(PlanError::TypeMismatch(_))
        ));
    }

    /// Issue #335 Stage C (D7): the argument is a full field expression
    /// now, so the planner — not the grammar — is what refuses a shape
    /// it cannot execute. `avg(span:childCount)` and `avg(trace:duration)`
    /// are reference 200s that parse and validate here and still have no
    /// aggregation path, and `avg(.a + 1)` is a composite source; all
    /// three are clean `400`s, which is what keeps D7 open on the wire
    /// axis while its parse axis closes.
    #[test]
    fn an_aggregate_argument_the_planner_cannot_execute_is_a_clean_400() {
        for q in [
            r#"{ .k = "v" } | avg(span:childCount) > 1"#,
            r#"{ .k = "v" } | avg(trace:duration) > 1s"#,
            r#"{ .k = "v" } | avg(.a + 1) > 1"#,
            r#"{ .k = "v" } | avg(-.a) > 1"#,
        ] {
            let query = parse(q).unwrap_or_else(|e| panic!("{q} must parse: {e}"));
            assert_eq!(pulsus_traceql::validate(&query), Ok(()), "{q}");
            assert!(
                matches!(
                    plan_search(&query, &PARAMS, &ctx()),
                    Err(PlanError::TypeMismatch(_))
                ),
                "{q} must be a planner 400"
            );
        }
    }

    /// The other side of the same arm: a parenthesised attribute is the
    /// SAME AST as the bare one (parentheses group, they do not survive
    /// into the tree), so `avg((.a))` plans exactly like `avg(.a)`.
    /// Recorded as an assertion because it is the one D7 probe whose
    /// wire disposition Stage C moves.
    #[test]
    fn a_parenthesised_aggregate_argument_plans_like_the_bare_one() {
        let bare = parse(r#"{ .k = "v" } | avg(.a) > 1"#).expect("parse");
        let parens = parse(r#"{ .k = "v" } | avg((.a)) > 1"#).expect("parse");
        assert_eq!(bare, parens, "parentheses must not survive into the AST");
        let p = plan_search(&parens, &PARAMS, &ctx()).expect("plans");
        assert!(matches!(
            p.aggregates[0].source,
            AggSource::Attr { field_idx: 0 }
        ));
    }

    #[test]
    fn select_projects_physical_and_attr_fields() {
        let p = plan(r#"{ .k = "v" } | select(name, span.foo, resource.service.name)"#);
        assert_eq!(p.select_fields.len(), 3);
        assert!(matches!(
            p.select_fields[0],
            SelectField::Physical {
                column: PhysicalSelect::Name,
                ..
            }
        ));
        assert!(matches!(p.select_fields[1], SelectField::Attr { .. }));
        assert!(matches!(
            p.select_fields[2],
            SelectField::Physical {
                column: PhysicalSelect::Service,
                ..
            }
        ));
        assert_eq!(p.select_attrs.len(), 1);
    }

    #[test]
    fn a_metric_stage_on_the_search_planner_is_a_type_mismatch() {
        // Issue #59: `| rate()` now PARSES (no longer a positioned
        // NotYetSupported) and must fail search planning as a plain
        // caller error — metrics functions are /metrics-only.
        let query = parse(r#"{ .k = "v" } | rate()"#).expect("parses since issue #59");
        match plan_search(&query, &PARAMS, &ctx()) {
            Err(PlanError::TypeMismatch(msg)) => {
                assert!(msg.contains("metrics"), "{msg}");
            }
            other => panic!("expected TypeMismatch, got {other:?}"),
        }
    }

    #[test]
    fn an_invalid_regex_fails_at_plan_time_not_execution() {
        let query = parse(r#"{ .k =~ "(" }"#).expect("parse");
        assert!(matches!(
            plan_search(&query, &PARAMS, &ctx()),
            Err(PlanError::TypeMismatch(_))
        ));
    }

    /// Issue #282: the two negated regex forms that render NO generator
    /// predicate (both take the time-range branch), so `filter.rs`'s
    /// render-time check cannot see them. `{ .k !~ … }` is caught here
    /// because its POSITIVE predicate — what `membership_sql_for` sends
    /// per Phase-2 batch — is now rendered at plan time; the negated
    /// service regex is caught by the Phase-2 eval compile. Either way
    /// the rejection lands as a `400` before any query is dispatched.
    #[test]
    fn negated_regexes_that_render_no_generator_still_fail_at_plan_time() {
        for q in [r#"{ .k !~ "(" }"#, r#"{ resource.service.name !~ "(" }"#] {
            // The leaf really does compile — this is the case the
            // render-time check in `filter.rs` does not see.
            let f = match parse(q).expect("parse").spanset {
                SpansetExpr::Filter(f) => f,
                other => panic!("expected one filter, got {other:?}"),
            };
            assert!(
                filter::compile_span_filter(&f).is_ok(),
                "{q}: expected a time-range leaf with no rendered predicate"
            );

            match plan_search(&parse(q).expect("parse"), &PARAMS, &ctx()) {
                Err(PlanError::TypeMismatch(msg)) => {
                    assert!(msg.starts_with(r#"invalid regex "(": "#), "{q}: {msg:?}");
                }
                other => panic!("{q}: expected a plan-time rejection, got {other:?}"),
            }
        }
    }

    /// The probe predicates a plan carries are rendered ONCE, at plan
    /// time, and are what `membership_sql_for` emits — so the SQL a
    /// Phase-2 batch sends can never contain a pattern that was not
    /// compiled first.
    #[test]
    fn membership_sql_uses_the_plan_time_rendered_probe_predicate() {
        let p = plan(r#"{ .k =~ "che.*" }"#);
        assert_eq!(p.probe_predicates.len(), p.probes.len());
        assert_eq!(
            p.probe_predicates[0],
            "key = 'k' AND match(val, '^(?:che.*)$')"
        );
        assert!(
            p.membership_sql_for(0, &[[0u8; 16]])
                .contains("match(val, '^(?:che.*)$')")
        );
    }

    /// AC4 (issue #172): a structural plan's Phase-1 SQL is BYTE-IDENTICAL
    /// to the equivalent `{A} && {B}` plan's — no new SQL shape exists, so
    /// the shipped shard-locality/index evidence covers structural plans
    /// verbatim.
    #[test]
    fn structural_generator_sql_is_byte_identical_to_the_and_plan() {
        // All 5 base operators × 3 modifiers (issue #183): every structural
        // form's Phase-1 SQL is byte-identical to the equivalent `{A} && {B}`
        // plan — no new SQL shape exists, so the shipped shard-locality /
        // #57 scan-budget / index evidence covers all 15 verbatim (AC4).
        for op in [
            ">", ">>", "<", "<<", "~", "!>", "!>>", "!<", "!<<", "!~", "&>", "&>>", "&<", "&<<",
            "&~",
        ] {
            let structural = plan(&format!(
                r#"{{ resource.service.name = "checkout" }} {op} {{ span.foo = "x" }}"#
            ));
            let and_plan = plan(r#"{ resource.service.name = "checkout" } && { span.foo = "x" }"#);
            assert_eq!(
                structural.generator_sqls, and_plan.generator_sqls,
                "{op}: generator SQL must be byte-identical to the && plan"
            );
            assert_eq!(
                structural.probes.len(),
                and_plan.probes.len(),
                "{op}: same membership probes"
            );
        }
    }

    #[test]
    fn field_vs_field_attr_compare_prunes_on_the_key_only_scan() {
        // `{ .a = .b }` (issue #183): the Phase-1 generator is the
        // LHS-attribute key-existence `(key)` scan (an index-served
        // superset), NOT a bare time-range fallback.
        let p = plan(r#"{ .a = .b }"#);
        assert_eq!(p.generator_sqls.len(), 1);
        let sql = &p.generator_sqls[0];
        assert!(
            sql.contains("key = 'a'"),
            "must prune on the LHS key: {sql}"
        );
        assert!(
            sql.contains("FROM trace_attrs_idx"),
            "must read the attr index, not the spans table: {sql}"
        );
        // Both operands are interned into val + val_num reads for Phase 2.
        assert_eq!(p.select_attrs.len(), 2, "both operands read `val`");
        assert_eq!(p.agg_fields.len(), 2, "both operands read `val_num`");
    }

    #[test]
    fn structural_registers_both_operands_generators_and_probes() {
        let p = plan(r#"{ span.a = "1" } > { span.b = "2" }"#);
        assert_eq!(
            p.generator_sqls.len(),
            2,
            "superset union of both operands' generators"
        );
        assert_eq!(p.probes.len(), 2);
        assert_eq!(p.filters.len(), 2, "lhs-then-rhs pre-order filters");
    }

    #[test]
    fn nested_set_leaf_sets_the_plan_flag_and_uses_the_time_range_generator() {
        let p = plan("{ nestedSetParent < 0 }");
        assert!(p.nested_set);
        // No column pushdown: the generator is the time-range superset,
        // byte-identical to `{}`.
        let match_all = plan("{}");
        assert_eq!(p.generator_sqls, match_all.generator_sqls);
        assert!(!match_all.nested_set);
        assert!(matches!(
            p.filters[0].leaves[0],
            PlannedLeafEval::NestedSet {
                field: NestedSetField::Parent,
                ..
            }
        ));
    }

    #[test]
    fn select_of_a_nested_set_intrinsic_is_a_type_mismatch() {
        let query = parse(r#"{ .k = "v" } | select(nestedSetLeft)"#).expect("parse");
        match plan_search(&query, &PARAMS, &ctx()) {
            Err(PlanError::TypeMismatch(msg)) => assert!(msg.contains("nested-set"), "{msg}"),
            other => panic!("expected TypeMismatch, got {other:?}"),
        }
    }

    // -- issue #184: trace-level intrinsic planning ----------------------

    #[test]
    fn trace_level_leaves_set_exactly_their_coload_flags() {
        let p = plan("{ traceDuration > 1s }");
        assert!(p.needs_trace_ctx() && !p.needs_child_counts());
        let p = plan(r#"{ rootName = "GET /" }"#);
        assert!(p.needs_trace_ctx() && !p.needs_child_counts());
        let p = plan(r#"{ rootServiceName =~ "gw.*" }"#);
        assert!(p.needs_trace_ctx() && !p.needs_child_counts());
        let p = plan("{ span:childCount > 2 }");
        assert!(!p.needs_trace_ctx() && p.needs_child_counts());
        // `trace:id` needs NO co-load (the candidate's id is in hand), and
        // unrelated queries pay nothing.
        let p = plan(r#"{ trace:id = "00000000000000000000000000000001" }"#);
        assert!(!p.needs_trace_ctx() && !p.needs_child_counts());
        let p = plan(r#"{ resource.service.name = "checkout" }"#);
        assert!(!p.needs_trace_ctx() && !p.needs_child_counts());
    }

    #[test]
    fn coload_sql_builders_render_the_trace_wide_reads() {
        let p = plan(r#"{ rootServiceName = "gw" && span:childCount > 1 }"#);
        assert!(p.needs_trace_ctx() && p.needs_child_counts());
        let ids = [[7u8; 16]];
        let ctx_sql = p.trace_ctx_sql_for(&ids);
        assert!(ctx_sql.contains("FROM trace_spans\n"), "{ctx_sql}");
        assert!(!ctx_sql.contains("timestamp_ns >"), "trace-wide: {ctx_sql}");
        let cc_sql = p.child_count_sql_for(&ids);
        assert!(cc_sql.contains("GROUP BY trace_id, parent_id"), "{cc_sql}");
    }

    #[test]
    fn an_invalid_root_name_regex_fails_at_plan_time() {
        let query = parse(r#"{ rootName =~ "(" }"#).expect("parse");
        assert!(matches!(
            plan_search(&query, &PARAMS, &ctx()),
            Err(PlanError::TypeMismatch(_))
        ));
    }

    #[test]
    fn select_of_a_trace_level_or_scoped_intrinsic_is_a_type_mismatch() {
        for q in [
            r#"{ .k = "v" } | select(statusMessage)"#,
            r#"{ .k = "v" } | select(traceDuration)"#,
            r#"{ .k = "v" } | select(rootName)"#,
            r#"{ .k = "v" } | select(rootServiceName)"#,
            r#"{ .k = "v" } | select(span:childCount)"#,
            r#"{ .k = "v" } | select(span:parentID)"#,
        ] {
            let query = parse(q).expect("parse");
            assert!(
                matches!(
                    plan_search(&query, &PARAMS, &ctx()),
                    Err(PlanError::TypeMismatch(_))
                ),
                "{q}"
            );
        }
    }

    /// Issue #351: `select(span:id)` / `select(trace:id)` are ACCEPTED
    /// and project nothing — the reference skips exactly these (among
    /// seven) when filling a span's response attributes, because both are
    /// already in the envelope (`pkg/traceql/engine.go:322-331` @ v3.0.2).
    /// This test replaces the `trace:id` row of the rejection table
    /// above, which pinned the behaviour the issue exists to change.
    ///
    /// Both halves are asserted: accepted, AND no projection — an accept
    /// that quietly added an attribute would be a different response from
    /// the reference's.
    #[test]
    fn select_of_an_envelope_carried_id_intrinsic_is_accepted_and_projects_nothing() {
        for q in [
            r#"{ .k = "v" } | select(span:id)"#,
            r#"{ .k = "v" } | select(trace:id)"#,
            r#"{ .k = "v" } | select(span:id, trace:id)"#,
        ] {
            let p = plan(q);
            assert!(
                p.select_fields.is_empty(),
                "{q} must project no select() field, got {:?}",
                p.select_fields
            );
        }
        // The control: a projecting `select()` beside them still projects,
        // so "empty" above is the id intrinsics' doing and not a lost
        // stage.
        let p = plan(r#"{ .k = "v" } | select(span:id, .other, trace:id)"#);
        assert_eq!(p.select_fields.len(), 1);
    }

    #[test]
    fn spanset_by_service_builds_the_distinct_by_key_cap_probe() {
        // `{ .a = "1" } | by(resource.service.name)` (issue #185): the plan
        // carries the `GROUP BY g0 LIMIT max_series+1` distinct-by-key
        // probe over the `service` column and the filter predicate.
        let p = plan(r#"{ .a = "1" } | by(resource.service.name)"#);
        let probe = p.by_probe_sql().expect("service by() builds a cap probe");
        assert!(probe.contains("service AS g0"), "{probe}");
        assert!(probe.contains("GROUP BY g0"), "{probe}");
        assert!(
            probe.contains("LIMIT 1001"),
            "cap+1 (max_series=1000): {probe}"
        );
        assert!(probe.contains("count() AS n"), "{probe}");
        assert!(
            probe.contains("key = 'a'"),
            "carries the filter predicate: {probe}"
        );
    }

    #[test]
    fn spanset_by_without_the_service_key_or_over_a_composite_builds_no_probe() {
        // A non-service by-key has no probe column; a composite spanset has
        // no single-filter predicate — both still plan (200), uncapped.
        assert!(
            plan(r#"{ .a = "1" } | by(span.foo)"#)
                .by_probe_sql()
                .is_none()
        );
        assert!(
            plan(r#"{ .a = "1" } && { .b = "2" } | by(resource.service.name)"#)
                .by_probe_sql()
                .is_none()
        );
        // No by() ⇒ no probe.
        assert!(plan(r#"{ .a = "1" }"#).by_probe_sql().is_none());
    }

    #[test]
    fn clustered_ctx_switches_the_table_names_only_via_ctx() {
        let query = parse(r#"{ resource.service.name = "checkout" }"#).expect("parse");
        let clustered = SearchCtx {
            filter: SpanFilterCtx {
                spans_table: "trace_spans_dist",
                attrs_table: "trace_attrs_idx_dist",
            },
            max_candidates: 100,
            max_series: 1_000,
            distributed: true,
        };
        let p = plan_search(&query, &PARAMS, &clustered).expect("plan");
        assert!(p.generator_sqls[0].contains("FROM trace_spans_dist\n"));
        assert!(p.distributed);
    }
}
