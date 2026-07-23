//! Pure `Query + SearchParams + SearchCtx → SearchPlan` planning for the
//! two-phase TraceQL search (issue #57 plan v7; docs/schemas.md §4.2).
//! Deterministic, no I/O: classifies every leaf via
//! [`super::filter::compile_span_filter`], renders the per-generator
//! Phase-1 candidate SQL (deduped, order-preserving), registers the
//! distinct attribute membership probes / aggregate / `select()` value
//! reads Phase 2 needs, and validates the pipeline stages — every
//! rejection here is a caller error ([`PlanError`] → `400 bad_data`).

use pulsus_traceql::{
    AggregateOp, ComparisonOp, Field, Intrinsic, PipelineStage, Query, SpansetExpr, SpansetFilter,
    Value,
};
use regex::Regex;

use crate::logql::escape;
use crate::logql::sql::TimeWindow;

use super::filter::{
    self, AttrProbe, CompareOperand, LeafEval, NestedSetField, PhysicalPredicate, PlanError,
    SpanFilterCtx, ValuePred,
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
    Name { op: StrOp, value: String },
    Service { op: StrOp, value: String },
    Duration { op: ComparisonOp, nanos: i64 },
    Status { op: ComparisonOp, code: i8 },
    Kind { op: ComparisonOp, code: i8 },
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
    Attr { str_idx: usize, num_idx: usize },
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
    /// A field-vs-field comparison (issue #183 `comparison.rhs_attribute`),
    /// evaluated per candidate span from both operands' resolved values.
    FieldCompare {
        lhs: PlannedOperand,
        rhs: PlannedOperand,
        op: ComparisonOp,
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
    /// Distinct attribute membership probes (batch reads).
    pub(crate) probes: Vec<AttrProbe>,
    /// Distinct attribute aggregate `val_num` reads.
    pub(crate) agg_fields: Vec<AttrFieldRef>,
    /// Distinct attribute `select()` `val` reads.
    pub(crate) select_attrs: Vec<AttrFieldRef>,
    pub(crate) aggregates: Vec<PlannedAggregate>,
    pub(crate) select_fields: Vec<SelectField>,
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

    /// One membership read's SQL for a candidate batch (exposed for the
    /// golden suite; `exec` drives the same builder).
    pub fn membership_sql_for(&self, probe_idx: usize, trace_ids: &[[u8; 16]]) -> String {
        let probe = &self.probes[probe_idx];
        search_sql::membership_sql(
            &self.attrs_table,
            &membership_predicate(probe),
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
}

/// The full positive predicate of one membership probe, pre-escaped.
fn membership_predicate(probe: &AttrProbe) -> String {
    let mut parts = vec![format!("key = {}", escape::ch_string(&probe.key))];
    parts.push(filter::value_pred_sql(&probe.pred));
    if let Some(scope) = probe.scope {
        parts.push(format!("scope = {}", escape::ch_string(scope)));
    }
    parts.join(" AND ")
}

/// Compiles the anchored full-value regex a `=~`/`!~` leaf evaluates
/// engine-side (physical columns only; attribute regexes evaluate in
/// ClickHouse via `match()`). `pub(crate)`: the metrics planner reuses it
/// as its plan-time regex validator (a bad pattern must be a `400`, never
/// a mid-query server error — issue #59).
pub(crate) fn compile_anchored(pat: &str) -> Result<Regex, PlanError> {
    Regex::new(&format!("^(?:{pat})$"))
        .map_err(|e| PlanError::TypeMismatch(format!("invalid regex {pat:?}: {e}")))
}

fn planned_str_op(op: ComparisonOp, value: &str) -> Result<StrOp, PlanError> {
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
    })
}

/// Validates a probe's regex at plan time even though ClickHouse
/// evaluates it — a bad pattern must be a `400`, not a mid-query server
/// error.
fn validate_probe(probe: &AttrProbe) -> Result<(), PlanError> {
    if let ValuePred::Regex(pat) = &probe.pred {
        compile_anchored(pat)?;
    }
    Ok(())
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
            },
        }),
        Field::Intrinsic(_) => None,
    }
}

/// Plans one field-vs-field comparison operand (issue #183): a physical
/// intrinsic resolves from the hydrated columns (no read registered); an
/// attribute is interned into BOTH the `val` (`select_attrs`) and the
/// `val_num` (`agg_fields`) reads so Phase 2 has a typed value.
fn plan_operand(
    operand: &CompareOperand,
    agg_fields: &mut Vec<AttrFieldRef>,
    select_attrs: &mut Vec<AttrFieldRef>,
) -> PlannedOperand {
    match operand {
        CompareOperand::Name => PlannedOperand::Name,
        CompareOperand::Service => PlannedOperand::Service,
        CompareOperand::Duration => PlannedOperand::Duration,
        CompareOperand::Status => PlannedOperand::Status,
        CompareOperand::Kind => PlannedOperand::Kind,
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

fn aggregate_threshold(
    op: AggregateOp,
    field: &Option<Field>,
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
            if matches!(field, Some(Field::Intrinsic(Intrinsic::Duration)))
                && op != AggregateOp::Count =>
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

fn plan_pipeline(
    query: &Query,
    agg_fields: &mut Vec<AttrFieldRef>,
    select_attrs: &mut Vec<AttrFieldRef>,
) -> Result<(Vec<PlannedAggregate>, Vec<SelectField>), PlanError> {
    let mut aggregates = Vec::new();
    let mut select_fields = Vec::new();
    for stage in &query.pipeline {
        match stage {
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
                    (_, Some(Field::Intrinsic(Intrinsic::Duration))) => AggSource::DurationNs,
                    (_, Some(Field::Intrinsic(other))) => {
                        return Err(PlanError::TypeMismatch(format!(
                            "{other} is not numerically aggregatable"
                        )));
                    }
                    (_, Some(attr @ Field::Attribute { .. })) => {
                        let field_ref = attr_field_ref(attr)
                            .expect("Field::Attribute always yields a field ref");
                        AggSource::Attr {
                            field_idx: intern(agg_fields, &field_ref),
                        }
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
            PipelineStage::Metric(func) => {
                return Err(PlanError::TypeMismatch(format!(
                    "{func}() is a metrics function: use /api/traces/v1/metrics/query_range or \
                     /query, not search"
                )));
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
    Ok((aggregates, select_fields))
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
    let mut filters = Vec::new();
    let mut generator_sqls: Vec<String> = Vec::new();
    let mut nested_set = false;
    // Attribute value reads (`val_num` / `val`): declared before the
    // filter loop because a field-vs-field comparison leaf interns its
    // attribute operands here (issue #183), then `plan_pipeline` appends
    // the aggregate/`select()` reads — interning only ever appends, so
    // indices stay stable.
    let mut agg_fields: Vec<AttrFieldRef> = Vec::new();
    let mut select_attrs: Vec<AttrFieldRef> = Vec::new();
    for spanset_filter in spanset_filters {
        let compiled = filter::compile_span_filter(spanset_filter)?;
        let mut leaves = Vec::with_capacity(compiled.leaves.len());
        for leaf in &compiled.leaves {
            let planned = match &leaf.eval {
                LeafEval::Physical(p) => PlannedLeafEval::Physical(plan_physical(p)?),
                LeafEval::Attr { probe, negated } => {
                    validate_probe(probe)?;
                    PlannedLeafEval::Attr {
                        probe_idx: intern(&mut probes, probe),
                        negated: *negated,
                    }
                }
                LeafEval::NestedSet { field, op, value } => {
                    nested_set = true;
                    PlannedLeafEval::NestedSet {
                        field: *field,
                        op: *op,
                        value: *value,
                    }
                }
                LeafEval::FieldCompare { lhs, rhs, op } => PlannedLeafEval::FieldCompare {
                    lhs: plan_operand(lhs, &mut agg_fields, &mut select_attrs),
                    rhs: plan_operand(rhs, &mut agg_fields, &mut select_attrs),
                    op: *op,
                },
            };
            leaves.push(planned);
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

    let (aggregates, select_fields) = plan_pipeline(query, &mut agg_fields, &mut select_attrs)?;

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
        probes,
        agg_fields,
        select_attrs,
        aggregates,
        select_fields,
    })
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
        // The parser already rejects `avg(name)`; the planner's own guard
        // covers direct-AST callers, so build the stage by hand.
        let mut query = parse(r#"{ .k = "v" }"#).expect("parse");
        query
            .pipeline
            .push(pulsus_traceql::PipelineStage::Aggregate {
                op: pulsus_traceql::AggregateOp::Avg,
                field: Some(pulsus_traceql::Field::Intrinsic(Intrinsic::Name)),
                cmp: ComparisonOp::Gt,
                value: Value::Number("1".to_string()),
            });
        assert!(matches!(
            plan_search(&query, &PARAMS, &ctx()),
            Err(PlanError::TypeMismatch(_))
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

    #[test]
    fn clustered_ctx_switches_the_table_names_only_via_ctx() {
        let query = parse(r#"{ resource.service.name = "checkout" }"#).expect("parse");
        let clustered = SearchCtx {
            filter: SpanFilterCtx {
                spans_table: "trace_spans_dist",
                attrs_table: "trace_attrs_idx_dist",
            },
            max_candidates: 100,
            distributed: true,
        };
        let p = plan_search(&query, &PARAMS, &clustered).expect("plan");
        assert!(p.generator_sqls[0].contains("FROM trace_spans_dist\n"));
        assert!(p.distributed);
    }
}
