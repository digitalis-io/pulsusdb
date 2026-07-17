//! The pure planner: `Expr → Plan`. Normalizes matchers into the single-pass
//! stage 1 shape (docs/schemas.md §3.2, architect plan amendment §1),
//! decides rollup-vs-raw routing for metric queries, compiles line-filter
//! pushdown, and derives the partition months a range touches. Nothing here
//! talks to ClickHouse — [`plan`] is `Expr + QueryParams + PlanCtx →
//! Result<Plan, ReadError>`, fully deterministic and snapshot-testable.
//!
//! Stage 2 (hydration) and stage 3/metric reads depend on stage 1's
//! *runtime* fingerprint set, so only stage 1's SQL is fully static at plan
//! time; [`Plan`] carries the resolved table names, line filters, and
//! bucket/aggregate expressions [`super::exec::LogQlEngine`] needs to call
//! [`super::sql`]'s stage 2/3/metric builders once fingerprints are known.

use std::collections::HashMap;

use pulsus_logql::{
    BinModifier, BinOp, Expr, Grouping, LineFilter, LineFilterOp, LogExpr, MatchOp, Matcher,
    MetricExpr, RangeAggOp, Stage, StreamSelector, VectorAggOp,
};

use super::error::ReadError;
use super::escape::{ch_regex_anchored, ch_regex_unanchored, ch_string};
use super::params::{Direction, PlanCtx, QueryParams, QuerySpec};

/// A pure fetch plan for either query shape. See the module docs for why
/// stage 2/3 aren't pre-rendered here. `MetricBinary` (issue M6-10) is
/// the plan for a metric expression containing binary operations or
/// scalar literals — a tree whose leaves are ordinary [`MetricPlan`]s;
/// plain single-aggregation metric queries keep planning to
/// [`Plan::Metric`] byte-identically.
#[derive(Debug, Clone, PartialEq)]
pub enum Plan {
    Streams(StreamsPlan),
    Metric(MetricPlan),
    MetricBinary(MetricNode),
}

/// The static (runtime-fingerprint-independent) part of a stream-selector
/// query plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsPlan {
    pub stage1_sql: String,
    pub streams_table: String,
    pub samples_table: String,
    pub start_ns: i64,
    pub end_ns: i64,
    pub direction: Direction,
    /// The stage-3 SQL `LIMIT` — a *scan* bound (issue M6-09 plan v3
    /// delta 4). Equal to [`StreamsPlan::result_limit`] unless the
    /// pipeline contains an unpushed dropping stage (a label filter, or a
    /// line filter after `line_format`), in which case it is
    /// `result_limit × PlanCtx::pipeline_scan_factor` (saturating) so
    /// lightly-filtering pipelines don't under-return. The byte scan
    /// budget still caps the read and aborts first.
    pub scan_limit: u32,
    /// The true response cap — re-applied in-engine to pipeline
    /// survivors, globally across streams. Responses never over-return.
    pub result_limit: u32,
    /// One pre-rendered predicate fragment per **pushed-down** pipeline
    /// `LineFilter` stage — those positioned before the first
    /// `line_format` stage, which reference the original `body` — ANDed
    /// together by [`super::sql::stage3`]. Line filters after a
    /// `line_format` reference the rewritten line and evaluate in-engine
    /// instead ([`super::pipeline::CompiledPipeline`]).
    pub line_filters: Vec<String>,
    /// The full ordered pipeline, compiled per query by
    /// [`super::exec::LogQlEngine`] into the in-engine evaluator.
    pub pipeline: Vec<Stage>,
    pub probes: Vec<ProbePlan>,
}

/// Which physical table a metric read was routed to. See
/// [`RoutingDecision`] for the accompanying (deterministic, plan-derived)
/// reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteChoice {
    Rollup,
    Raw,
}

/// One vector-aggregation layer: `(op, grouping, parsed topk/bottomk k)`
/// — the parsed-parameter shape [`MetricPlan::vector_aggs`] and
/// [`MetricNode::VectorAgg`] carry (issue M6-10).
pub type VectorAggSpec = (VectorAggOp, Option<Grouping>, Option<f64>);

/// The raw-parameter shape straight off the AST (parameter still the
/// raw literal text), before [`parse_vector_agg_params`] validates it.
type RawVectorAggSpec = (VectorAggOp, Option<Grouping>, Option<String>);

/// The rollup-vs-raw routing decision for one metric query, computed once
/// in [`metric_plan`] and carried on both [`MetricPlan`] (for [`super::exec`]
/// to act on) and [`super::explain::PlanExplain`] (for #13's
/// `X-Pulsus-Explain` header to name). `reason` is entirely plan-derived —
/// an enum tag plus numeric nanosecond values, never user-controlled
/// data — so it is safe to surface verbatim in a response header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingDecision {
    pub chosen: RouteChoice,
    pub reason: String,
}

/// The static part of a metric query plan. `table`/`bucket_col`/`agg_expr`
/// encode the rollup-vs-raw routing decision (docs/schemas.md §3.2);
/// `rate_window_ns` is `Some` only for `rate`/`bytes_rate` (the divisor
/// [`super::exec`] applies), never for the `*_over_time` count ops.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricPlan {
    pub stage1_sql: String,
    pub streams_table: String,
    pub table: String,
    pub bucket_col: &'static str,
    pub agg_expr: &'static str,
    pub rollup: bool,
    /// The single routing decision `rollup` is derived from
    /// (`rollup == matches!(routing.chosen, RouteChoice::Rollup)`); kept
    /// alongside the plain bool so callers that only need the SQL shape
    /// (`exec.rs`) don't have to match on the enum.
    pub routing: RoutingDecision,
    /// Line-filter pushdown for the raw fallback (the rollup table has no
    /// `body` column — a metric query with a line filter can never be
    /// rollup-served, see [`metric_plan`]).
    pub extra_predicates: Vec<String>,
    pub start_ns: i64,
    pub end_ns: i64,
    /// `Some(step)` = [`QuerySpec::Range`]'s bucketed shape (`intDiv(_,
    /// step) * step`); `None` = [`QuerySpec::Instant`]'s single-window
    /// aggregate, structurally incapable of emitting a bucket expression.
    pub step_ns: Option<u64>,
    pub rate_window_ns: Option<u64>,
    pub op: RangeAggOp,
    /// Outer-to-inner vector-aggregation chain (`sum by (...) (avg(...))`
    /// nests outer-first); finished in Rust over the per-fingerprint series
    /// (docs/schemas.md §3.2: "the engine ... finishes the `sum by`").
    /// The third element is the parsed `topk`/`bottomk` `k` (issue
    /// M6-10), `None` for every other aggregation.
    pub vector_aggs: Vec<VectorAggSpec>,
    /// `Some` = client-aggregated (issue M6-10): raw-scan + in-engine
    /// pipeline/unwrap/reduce over `metric_raw_samples` (no `LIMIT` —
    /// complete-or-error). `None` = the existing SQL-aggregated
    /// (rollup-or-raw) path, byte-identical to pre-M6-10 plans.
    pub client: Option<ClientAgg>,
    pub probes: Vec<ProbePlan>,
}

/// The client-aggregated execution spec (issue M6-10): what
/// [`super::exec`] runs per surviving line after the full-window raw
/// scan.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientAgg {
    /// The full ordered pipeline (compiled once in exec via
    /// [`super::pipeline::CompiledPipeline`]); the line-filter prefix
    /// before the first `line_format` is ALSO pushed down as
    /// `extra_predicates` (plan v2 D3 — the pushdown-order invariant
    /// lives in [`compile_line_filters`]).
    pub pipeline: Vec<Stage>,
    /// Per-surviving-line sample value source.
    pub value: ClientValue,
    /// The over-time reducer.
    pub range_op: RangeAggOp,
    /// The `quantile_over_time` q, parsed from the AST's raw parameter.
    pub param: Option<f64>,
    /// `absent_over_time` only: the selector's `Eq`-matcher labels — the
    /// synthetic-absence series labels (oracle-probed; plan v2 D2).
    pub absent_labels: Vec<(String, String)>,
}

/// Where a client-aggregated sample's value comes from. `Unwrap` carries
/// no fields (plan v2 D1): the label/conversion live in the compiled
/// unwrap stage inside the pipeline; this is just the marker telling
/// exec to read the pipeline's extracted value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientValue {
    Count,
    Bytes,
    Unwrap,
}

/// One node of a binary-operation metric plan (issue M6-10): leaves are
/// ordinary [`MetricPlan`]s (each planned exactly as if it were the
/// whole query), scalars come from literal operands, and `VectorAgg`
/// covers a vector aggregation over a *binary* operand (`sum(a + b)`) —
/// a thin post-combination layer reusing the same reducer code as
/// [`MetricPlan::vector_aggs`].
#[derive(Debug, Clone, PartialEq)]
pub enum MetricNode {
    Leaf(Box<MetricPlan>),
    Scalar(f64),
    Binary {
        op: BinOp,
        /// The `bool` comparison modifier (0/1 instead of filtering).
        return_bool: bool,
        lhs: Box<MetricNode>,
        rhs: Box<MetricNode>,
    },
    VectorAgg {
        aggs: Vec<VectorAggSpec>,
        inner: Box<MetricNode>,
    },
}

impl MetricNode {
    /// Every [`MetricPlan`] leaf in the tree, left-to-right — the
    /// stage-1 resolution surface (`series`/explain walk these).
    pub fn leaves(&self) -> Vec<&MetricPlan> {
        let mut out = Vec::new();
        self.collect_leaves(&mut out);
        out
    }

    fn collect_leaves<'a>(&'a self, out: &mut Vec<&'a MetricPlan>) {
        match self {
            MetricNode::Leaf(mp) => out.push(mp),
            MetricNode::Scalar(_) => {}
            MetricNode::Binary { lhs, rhs, .. } => {
                lhs.collect_leaves(out);
                rhs.collect_leaves(out);
            }
            MetricNode::VectorAgg { inner, .. } => inner.collect_leaves(out),
        }
    }
}

/// A selectivity `count()` probe over one matcher key's index prefix.
///
/// **Probe SQL is generated and surfaced in `PlanExplain`; probe
/// *execution* (matcher reordering / pre-flight estimate) is deferred**
/// (code-review fix-plan amendment §2, de-scoped rather than implemented).
/// Rationale: with the stage-1 scan itself now budget-capped
/// (`LogQlEngine::resolve_fingerprints`, `budget_settings()` +
/// `map_read_error`), the byte budget on the *actual* index scan already
/// provides the "abort past budget" guarantee docs/schemas.md §3.2
/// attributes to probes. The single grouped scan performs the whole
/// positive/negative intersection in one `GROUP BY ... HAVING` pass
/// (architect plan amendment §1), so OR-branch/matcher ordering inside that
/// one scan is cosmetic — it has no correctness or index-prefix dependency
/// the way, say, a sequential multi-pass plan would. Executing probes to
/// reorder branches or produce a pre-flight estimate is a pure
/// optimization, left for a later milestone; [`ProbePlan`] and its
/// `PlanExplain` wiring stay as-is so the SQL a probe *would* run is still
/// inspectable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbePlan {
    pub key: String,
    pub sql: String,
}

/// Plans `expr` into a [`Plan`]. See the module docs for the two-phase
/// split with stage 2/3 SQL generation.
pub fn plan(expr: &Expr, p: &QueryParams, ctx: &PlanCtx<'_>) -> Result<Plan, ReadError> {
    match expr {
        Expr::Log(log_expr) => Ok(Plan::Streams(streams_plan(log_expr, p, ctx)?)),
        Expr::Metric(metric_expr) => plan_metric_expr(metric_expr, p, ctx),
    }
}

/// Dispatches a metric expression: a (vector-agg-wrapped) range
/// aggregation keeps today's [`Plan::Metric`] shape byte-identically;
/// anything containing a binary operation or scalar literal plans to the
/// [`Plan::MetricBinary`] tree whose leaves are ordinary [`MetricPlan`]s
/// (issue M6-10).
fn plan_metric_expr(
    metric_expr: &MetricExpr,
    p: &QueryParams,
    ctx: &PlanCtx<'_>,
) -> Result<Plan, ReadError> {
    let (base, _) = unwrap_vector_aggs(metric_expr);
    match base {
        MetricExpr::Range { .. } => Ok(Plan::Metric(metric_plan(metric_expr, p, ctx)?)),
        _ => {
            // The zero-step guard normally lives in `metric_plan` (run
            // per leaf); a leaf-less tree (`2 + 2`) must still reject a
            // zero step for request-shape consistency.
            if let QuerySpec::Range { step_ns: 0, .. } = p.spec {
                return Err(ReadError::InvalidStep);
            }
            Ok(Plan::MetricBinary(build_metric_node(metric_expr, p, ctx)?))
        }
    }
}

/// Recursively plans a binary/literal metric expression into a
/// [`MetricNode`] tree. Every (vector-agg-wrapped) range-aggregation
/// operand becomes a [`MetricNode::Leaf`] via the ordinary
/// [`metric_plan`] path, so per-leaf routing/rollup decisions are exactly
/// what the same expression would get standalone.
fn build_metric_node(
    metric_expr: &MetricExpr,
    p: &QueryParams,
    ctx: &PlanCtx<'_>,
) -> Result<MetricNode, ReadError> {
    match metric_expr {
        MetricExpr::Literal(raw) => Ok(MetricNode::Scalar(parse_plan_number(
            raw,
            "scalar literal",
        )?)),
        MetricExpr::Binary {
            op,
            modifier,
            lhs,
            rhs,
        } => Ok(MetricNode::Binary {
            op: *op,
            return_bool: matches!(modifier, Some(BinModifier { return_bool: true })),
            lhs: Box::new(build_metric_node(lhs, p, ctx)?),
            rhs: Box::new(build_metric_node(rhs, p, ctx)?),
        }),
        MetricExpr::Range { .. } => Ok(MetricNode::Leaf(Box::new(metric_plan(
            metric_expr,
            p,
            ctx,
        )?))),
        MetricExpr::Vector { .. } => {
            let (base, raw_aggs) = unwrap_vector_aggs(metric_expr);
            match base {
                MetricExpr::Range { .. } => Ok(MetricNode::Leaf(Box::new(metric_plan(
                    metric_expr,
                    p,
                    ctx,
                )?))),
                MetricExpr::Literal(_) => Err(ReadError::PipelineInvalid {
                    reason: "a vector aggregation cannot aggregate a bare scalar literal"
                        .to_string(),
                }),
                inner => Ok(MetricNode::VectorAgg {
                    aggs: parse_vector_agg_params(&raw_aggs)?,
                    inner: Box::new(build_metric_node(inner, p, ctx)?),
                }),
            }
        }
    }
}

/// Parses a raw AST number the parser guaranteed to be `Number`-token
/// shaped; a non-finite/unparseable value is a named 400, never a NaN
/// smuggled into evaluation.
fn parse_plan_number(raw: &str, what: &str) -> Result<f64, ReadError> {
    match raw.parse::<f64>() {
        Ok(v) if v.is_finite() => Ok(v),
        _ => Err(ReadError::PipelineInvalid {
            reason: format!("invalid {what} {raw:?}"),
        }),
    }
}

/// Validates and parses each vector aggregation's parameter:
/// `topk`/`bottomk` require `k`; the parameterless aggregations must not
/// carry one (the parser already enforces both — planner re-checks for
/// defense in depth on programmatically-built ASTs).
fn parse_vector_agg_params(raw: &[RawVectorAggSpec]) -> Result<Vec<VectorAggSpec>, ReadError> {
    raw.iter()
        .map(|(op, grouping, param)| {
            let parsed = match (op.takes_param(), param) {
                (true, Some(raw)) => Some(parse_plan_number(raw, &format!("{op} parameter"))?),
                (true, None) => {
                    return Err(ReadError::PipelineInvalid {
                        reason: format!("`{op}` requires a k parameter (e.g. {op}(5, ...))"),
                    });
                }
                (false, Some(_)) => {
                    return Err(ReadError::PipelineInvalid {
                        reason: format!("`{op}` takes no parameter"),
                    });
                }
                (false, None) => None,
            };
            Ok((*op, grouping.clone(), parsed))
        })
        .collect()
}

fn streams_plan(
    log_expr: &LogExpr,
    p: &QueryParams,
    ctx: &PlanCtx<'_>,
) -> Result<StreamsPlan, ReadError> {
    let (start_ns, end_ns) = window_bounds_for_streams(&p.spec);
    let normalized = normalize_matchers(&log_expr.selector)?;
    let months = months_overlapping(start_ns, end_ns);
    let stage1_sql = super::sql::stage1(
        ctx.streams_idx,
        &months,
        &normalized.positive_branches,
        &normalized.negative_branches,
    );
    let probes = build_probes(ctx, &months, &normalized.probe_keys);

    // A bare log query cannot evaluate `unwrap` — the unwrapped value
    // only means something inside a range aggregation (plan v3 delta 1).
    if log_expr
        .pipeline
        .iter()
        .any(|s| matches!(s, Stage::Unwrap(_)))
    {
        return Err(ReadError::PipelineInvalid {
            reason: "`unwrap` is only valid inside a range aggregation (e.g. \
                     sum_over_time({...} | unwrap x [5m]))"
                .to_string(),
        });
    }

    let line_filters = compile_line_filters(&log_expr.pipeline);
    let result_limit = p.limit;
    let scan_limit = if has_unpushed_dropping_stage(&log_expr.pipeline) {
        result_limit.saturating_mul(ctx.pipeline_scan_factor)
    } else {
        result_limit
    };

    Ok(StreamsPlan {
        stage1_sql,
        streams_table: ctx.streams.to_string(),
        samples_table: ctx.samples.to_string(),
        start_ns,
        end_ns,
        direction: p.direction,
        scan_limit,
        result_limit,
        line_filters,
        pipeline: log_expr.pipeline.clone(),
        probes,
    })
}

/// Oversample eligibility (plan v3 delta 4): the pipeline contains a
/// stage that drops lines **in-engine** — a label filter, or a line
/// filter positioned after the first `line_format` (which references the
/// rewritten line and cannot push down). Parsers and `label_format` are
/// non-dropping (a parse failure keeps the line with an `__error__`
/// label; fan-out only regroups), so they alone never trigger the
/// oversample and parser-only pipelines keep byte-identical SQL.
fn has_unpushed_dropping_stage(pipeline: &[Stage]) -> bool {
    let mut seen_line_format = false;
    for stage in pipeline {
        match stage {
            Stage::LineFormat(_) => seen_line_format = true,
            Stage::LabelFilter(_) => return true,
            Stage::LineFilter(_) if seen_line_format => return true,
            _ => {}
        }
    }
    false
}

/// The first beyond-line-filter stage in pipeline order. Pre-M6-10 this
/// named the `PipelineUnsupportedInMetric` rejection; since M6-10 its
/// `Some` is the client-aggregation mode trigger — any beyond-line-filter
/// stage means the columnar store cannot express the aggregation and the
/// pipeline evaluates in-engine over the raw scan.
fn metric_pipeline_construct(pipeline: &[Stage]) -> Option<&'static str> {
    use pulsus_logql::ParserStage;
    pipeline.iter().find_map(|stage| match stage {
        Stage::LineFilter(_) => None,
        Stage::Parser(ParserStage::Json { .. }) => Some("json"),
        Stage::Parser(ParserStage::Logfmt { .. }) => Some("logfmt"),
        Stage::Parser(ParserStage::Regexp(_)) => Some("regexp"),
        Stage::Parser(ParserStage::Pattern(_)) => Some("pattern"),
        Stage::LabelFilter(_) => Some("label filter"),
        Stage::LineFormat(_) => Some("line_format"),
        Stage::LabelFormat(_) => Some("label_format"),
        Stage::Unwrap(_) => Some("unwrap"),
    })
}

fn metric_plan(
    metric_expr: &MetricExpr,
    p: &QueryParams,
    ctx: &PlanCtx<'_>,
) -> Result<MetricPlan, ReadError> {
    // `0.is_multiple_of(_)` is trivially `true`, which would otherwise let
    // a zero step reach the routing decision below and pick rollup, then
    // render `intDiv(bucket_ns, 0)` — undefined in ClickHouse. The raw
    // fallback's own `intDiv(timestamp_ns, 0)` bucketing is equally
    // invalid, so this is checked before *any* routing choice is made,
    // making `intDiv(_, 0)` structurally unreachable regardless of what
    // request-level validation #13 later adds (task-manager resolution #4
    // on issue #12: "defense in depth, one cheap branch").
    if let QuerySpec::Range { step_ns: 0, .. } = p.spec {
        return Err(ReadError::InvalidStep);
    }

    let (base, raw_vector_aggs) = unwrap_vector_aggs(metric_expr);
    let MetricExpr::Range { op, range, param } = base else {
        // `plan_metric_expr`/`build_metric_node` route every
        // `Literal`/`Binary`-bottomed expression to the node tree, so the
        // base reaching `metric_plan` is structurally always `Range`.
        unreachable!("metric_plan is only called on Vector-chains bottoming at MetricExpr::Range")
    };
    let vector_aggs = parse_vector_agg_params(&raw_vector_aggs)?;

    // Issue M6-10: metric pipelines now execute in-engine. Classify the
    // query into the SQL-aggregated mode (the four count/bytes ops,
    // un-piped beyond line filters — byte-identical plans, rollup
    // auto-routing preserved) vs the client-aggregated mode (any
    // beyond-line-filter pipeline stage, any unwrap, or any of the new
    // over-time ops — full-window raw scan, complete-or-error).
    let pipeline = &range.selector.pipeline;
    let has_beyond_line_filter = metric_pipeline_construct(pipeline).is_some();
    let has_unwrap = pipeline.iter().any(|s| matches!(s, Stage::Unwrap(_))) ||
        // Defense in depth: the parser only emits the pipeline
        // `Stage::Unwrap` form and always leaves `LogRange::unwrap`
        // `None`.
        range.unwrap.is_some();

    // Unwrap arity — mirrors the oracle's parse errors verbatim
    // ("invalid aggregation X with/without unwrap", probed live).
    let requires_unwrap = matches!(
        op,
        RangeAggOp::SumOverTime
            | RangeAggOp::AvgOverTime
            | RangeAggOp::MinOverTime
            | RangeAggOp::MaxOverTime
            | RangeAggOp::StddevOverTime
            | RangeAggOp::StdvarOverTime
            | RangeAggOp::QuantileOverTime
            | RangeAggOp::FirstOverTime
            | RangeAggOp::LastOverTime
    );
    let forbids_unwrap = matches!(
        op,
        RangeAggOp::CountOverTime | RangeAggOp::BytesRate | RangeAggOp::BytesOverTime
    );
    if requires_unwrap && !has_unwrap {
        return Err(ReadError::PipelineInvalid {
            reason: format!("invalid aggregation {op} without unwrap"),
        });
    }
    if forbids_unwrap && has_unwrap {
        return Err(ReadError::PipelineInvalid {
            reason: format!("invalid aggregation {op} with unwrap"),
        });
    }
    let quantile = match (op, param) {
        (RangeAggOp::QuantileOverTime, Some(raw)) => {
            Some(parse_plan_number(raw, "quantile parameter")?)
        }
        (RangeAggOp::QuantileOverTime, None) => {
            // The parser requires the parameter; re-checked for
            // programmatically-built ASTs.
            return Err(ReadError::PipelineInvalid {
                reason: "quantile_over_time requires a quantile parameter".to_string(),
            });
        }
        _ => None,
    };

    let client_only_op = requires_unwrap || matches!(op, RangeAggOp::AbsentOverTime);
    let client = if has_beyond_line_filter || has_unwrap || client_only_op {
        let value = if has_unwrap {
            ClientValue::Unwrap
        } else if matches!(op, RangeAggOp::BytesRate | RangeAggOp::BytesOverTime) {
            ClientValue::Bytes
        } else {
            ClientValue::Count
        };
        let absent_labels = if matches!(op, RangeAggOp::AbsentOverTime) {
            range
                .selector
                .selector
                .matchers
                .iter()
                .filter(|m| m.op == MatchOp::Eq)
                .map(|m| (m.name.clone(), m.value.clone()))
                .collect()
        } else {
            Vec::new()
        };
        Some(ClientAgg {
            pipeline: pipeline.clone(),
            value,
            range_op: *op,
            param: quantile,
            absent_labels,
        })
    } else {
        None
    };

    let range_ns = range.range.as_nanos();

    let (start_ns, end_ns, step_ns, rate_window_ns) = match p.spec {
        QuerySpec::Instant { at_ns } => {
            let start = at_ns.saturating_sub(range_ns as i64);
            (start, at_ns, None, Some(range_ns))
        }
        QuerySpec::Range {
            start_ns,
            end_ns,
            step_ns,
        } => (start_ns, end_ns, Some(step_ns), Some(step_ns)),
    };

    let normalized = normalize_matchers(&range.selector.selector)?;
    let months = months_overlapping(start_ns, end_ns);
    let stage1_sql = super::sql::stage1(
        ctx.streams_idx,
        &months,
        &normalized.positive_branches,
        &normalized.negative_branches,
    );
    let probes = build_probes(ctx, &months, &normalized.probe_keys);

    let extra_predicates = compile_line_filters(&range.selector.pipeline);
    // A line filter constrains which log lines count; the rollup table
    // (`log_metrics_<res>`) has no `body` column to re-filter, so any
    // pipeline stage forces the raw fallback (docs/schemas.md §3.2: metric
    // reads "never touch samples for count-only rollup shapes" — that
    // guarantee only holds when there is nothing left for `log_samples` to
    // filter).
    //
    // Rollup eligibility is additionally gated on `client.is_none()`
    // (issue M6-10, removing the former guard-comment TODO): every
    // non-count op and every beyond-line-filter pipeline is client-
    // aggregated, always routed raw with its own named reason — the
    // rollup table can neither re-filter bodies nor produce unwrapped
    // values. `client.is_none()` count/bytes ops keep the pre-M6-10
    // routing and reasons byte-identically (the perf regression gate).
    // `Instant` is matched *first*, ahead of the line-filter/resolution
    // checks below: an instant window ([at - range, at]) has no step to
    // test against the resolution regardless of what else is true about
    // the query, so its reason must always be exactly "raw: instant query"
    // — never shadowed by an unrelated raw-fallback reason an instant query
    // also happens to satisfy (code review fix, issue #12: an instant query
    // that also carries a line filter, or runs with `rollup_res_ns == 0`,
    // must still report "raw: instant query", not "raw: line filter
    // present"/"raw: rollup resolution not configured"). schemas.md §3.2
    // ties eligibility strictly to "the query step is a multiple of the
    // resolution", and an unaligned window would silently diverge from raw
    // at bucket edges (task-manager resolution #1 on issue #12).
    let routing = if client.is_some() {
        RoutingDecision {
            chosen: RouteChoice::Raw,
            reason: "raw: client-side pipeline/unwrap aggregation".to_string(),
        }
    } else {
        match p.spec {
            QuerySpec::Instant { .. } => RoutingDecision {
                chosen: RouteChoice::Raw,
                reason: "raw: instant query".to_string(),
            },
            QuerySpec::Range { .. } if !extra_predicates.is_empty() => RoutingDecision {
                chosen: RouteChoice::Raw,
                reason: "raw: line filter present".to_string(),
            },
            QuerySpec::Range { .. } if ctx.rollup_res_ns == 0 => RoutingDecision {
                chosen: RouteChoice::Raw,
                reason: "raw: rollup resolution not configured".to_string(),
            },
            QuerySpec::Range { step_ns, .. } if step_ns.is_multiple_of(ctx.rollup_res_ns) => {
                RoutingDecision {
                    chosen: RouteChoice::Rollup,
                    reason: format!(
                        "rollup: step {step_ns} ns divisible by resolution {} ns",
                        ctx.rollup_res_ns
                    ),
                }
            }
            QuerySpec::Range { step_ns, .. } => RoutingDecision {
                chosen: RouteChoice::Raw,
                reason: format!(
                    "raw: step {step_ns} ns not a multiple of resolution {} ns",
                    ctx.rollup_res_ns
                ),
            },
        }
    };
    let rollup_eligible = matches!(routing.chosen, RouteChoice::Rollup);

    let is_bytes = matches!(op, RangeAggOp::BytesRate | RangeAggOp::BytesOverTime);
    let is_rate = matches!(op, RangeAggOp::Rate | RangeAggOp::BytesRate);

    let (table, bucket_col, agg_expr) = if rollup_eligible {
        (
            ctx.rollup_table.to_string(),
            "bucket_ns",
            if is_bytes { "sum(bytes)" } else { "sum(count)" },
        )
    } else {
        (
            ctx.samples.to_string(),
            "timestamp_ns",
            if is_bytes {
                "sum(length(body))"
            } else {
                "count()"
            },
        )
    };

    Ok(MetricPlan {
        stage1_sql,
        streams_table: ctx.streams.to_string(),
        table,
        bucket_col,
        agg_expr,
        rollup: rollup_eligible,
        routing,
        extra_predicates,
        start_ns,
        end_ns,
        step_ns,
        rate_window_ns: if is_rate { rate_window_ns } else { None },
        op: *op,
        vector_aggs,
        client,
        probes,
    })
}

/// Unwraps every outer `MetricExpr::Vector` layer, returning the
/// innermost non-`Vector` expression and the aggregation chain (with raw
/// parameters) in outer-to-inner order (`sum by (svc) (avg(...))` yields
/// `[(Sum, Some(by(svc)), None)]` first, then deeper wrappers after).
fn unwrap_vector_aggs(expr: &MetricExpr) -> (&MetricExpr, Vec<RawVectorAggSpec>) {
    let mut aggs = Vec::new();
    let mut cur = expr;
    while let MetricExpr::Vector {
        op,
        grouping,
        param,
        inner,
    } = cur
    {
        aggs.push((*op, grouping.clone(), param.clone()));
        cur = inner;
    }
    (cur, aggs)
}

/// Streams (log-selector) queries always evaluate over an explicit
/// `[start, end]` window (`Range`'s bounds); an `Instant` spec has no
/// natural range for a bare selector (that concept only exists for range
/// aggregations, which carry their own `[duration]`) and degenerates to the
/// zero-width instant `[at, at]` — callers needing an instant *log* read
/// with lookback are expected to translate that at the #13 layer before
/// calling `plan` (task-manager resolution #3 is scoped to metric queries).
fn window_bounds_for_streams(spec: &QuerySpec) -> (i64, i64) {
    match *spec {
        QuerySpec::Range {
            start_ns, end_ns, ..
        } => (start_ns, end_ns),
        QuerySpec::Instant { at_ns } => (at_ns, at_ns),
    }
}

fn build_probes(ctx: &PlanCtx<'_>, months: &[String], probe_keys: &[String]) -> Vec<ProbePlan> {
    probe_keys
        .iter()
        .map(|key| ProbePlan {
            key: key.clone(),
            sql: super::sql::probe(ctx.streams_idx, months, &ch_string(key)),
        })
        .collect()
}

/// The result of normalizing a [`StreamSelector`]'s matchers into stage 1's
/// single-pass shape (architect plan amendment §1).
#[derive(Debug)]
struct NormalizedMatchers {
    /// One pre-rendered, parenthesized OR-branch per **distinct positive
    /// key** — the collapse that keeps `HAVING uniqExact(key, val) = n`
    /// (or its `If`-conditional form) valid (architect plan: "Matcher
    /// normalisation").
    positive_branches: Vec<String>,
    /// One pre-rendered, parenthesized OR-branch per negative matcher
    /// (`Neq`/`Nre`) — deliberately *not* collapsed per key: `countIf(...)
    /// = 0` is correct whether one or several negative branches target the
    /// same key.
    negative_branches: Vec<String>,
    /// Distinct label keys carrying a regex matcher (`Re` or `Nre`) — the
    /// only case that warrants a selectivity probe (architect plan:
    /// "Selectivity probes").
    probe_keys: Vec<String>,
}

/// One label key's positive matchers, collapsed to a single condition
/// (architect plan: "Eq+Eq same key/value dedups, Eq+Re same key ANDs
/// both, two different Eq values is `ContradictoryMatchers`").
struct PositiveGroup {
    key: String,
    eq_value: Option<String>,
    re_patterns: Vec<String>,
}

fn push_probe_key(probe_keys: &mut Vec<String>, key: &str) {
    if !probe_keys.iter().any(|k| k == key) {
        probe_keys.push(key.to_string());
    }
}

/// Partitions and collapses a selector's matchers per the architect plan's
/// normalization rules (see [`NormalizedMatchers`] field docs).
fn normalize_matchers(selector: &StreamSelector) -> Result<NormalizedMatchers, ReadError> {
    let mut positive_order: Vec<String> = Vec::new();
    let mut positive_groups: HashMap<String, PositiveGroup> = HashMap::new();
    let mut negative_branches: Vec<String> = Vec::new();
    let mut probe_keys: Vec<String> = Vec::new();

    for Matcher { name, op, value } in &selector.matchers {
        match op {
            MatchOp::Eq => {
                let group = positive_groups.entry(name.clone()).or_insert_with(|| {
                    positive_order.push(name.clone());
                    PositiveGroup {
                        key: name.clone(),
                        eq_value: None,
                        re_patterns: Vec::new(),
                    }
                });
                match &group.eq_value {
                    Some(existing) if existing != value => {
                        return Err(ReadError::ContradictoryMatchers);
                    }
                    _ => group.eq_value = Some(value.clone()),
                }
            }
            MatchOp::Re => {
                push_probe_key(&mut probe_keys, name);
                let group = positive_groups.entry(name.clone()).or_insert_with(|| {
                    positive_order.push(name.clone());
                    PositiveGroup {
                        key: name.clone(),
                        eq_value: None,
                        re_patterns: Vec::new(),
                    }
                });
                if !group.re_patterns.iter().any(|p| p == value) {
                    group.re_patterns.push(value.clone());
                }
            }
            MatchOp::Neq => {
                negative_branches.push(format!(
                    "(key = {} AND val = {})",
                    ch_string(name),
                    ch_string(value)
                ));
            }
            MatchOp::Nre => {
                push_probe_key(&mut probe_keys, name);
                negative_branches.push(format!(
                    "(key = {} AND match(val, {}))",
                    ch_string(name),
                    ch_regex_anchored(value)
                ));
            }
        }
    }

    if positive_order.is_empty() {
        return Err(ReadError::EmptyMatcherSet);
    }

    let positive_branches = positive_order
        .iter()
        .map(|key| {
            let group = &positive_groups[key];
            let mut conds = vec![format!("key = {}", ch_string(&group.key))];
            if let Some(v) = &group.eq_value {
                conds.push(format!("val = {}", ch_string(v)));
            }
            for pat in &group.re_patterns {
                conds.push(format!("match(val, {})", ch_regex_anchored(pat)));
            }
            format!("({})", conds.join(" AND "))
        })
        .collect();

    Ok(NormalizedMatchers {
        positive_branches,
        negative_branches,
        probe_keys,
    })
}

/// Compiles the **pushed-down** pipeline `LineFilter` stages — those
/// positioned before the first `line_format` — into stage-3 predicate
/// fragments, in pipeline order (architect plan amendment: line filters
/// "ALWAYS paired with the exact predicate"). Filters before a parser
/// still push down (parsers read but never rewrite the line — the
/// M6-09 skip-index-preservation gate, `tests/explain_indexes.rs`);
/// filters after a `line_format` reference the rewritten line and are
/// deliberately absent here (evaluated in-engine instead).
pub(crate) fn compile_line_filters(pipeline: &[Stage]) -> Vec<String> {
    let mut out = Vec::new();
    for stage in pipeline {
        match stage {
            Stage::LineFilter(lf) => out.push(compile_line_filter(lf)),
            Stage::LineFormat(_) => break,
            _ => {}
        }
    }
    out
}

/// ClickHouse's `tokenbf_v1` splits on non-alphanumeric ASCII; a `hasToken`
/// prefilter must extract tokens the same way or it misses granules that
/// truly contain the phrase.
fn tokenize(literal: &str) -> Vec<String> {
    literal
        .split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

const REGEX_METACHARS: &[char] = &[
    '.', '^', '$', '*', '+', '?', '(', ')', '[', ']', '{', '}', '|', '\\',
];

/// Conservative, safe-by-construction heuristic: a pattern with zero regex
/// metacharacters is a plain literal, so its tokens can seed a `hasToken`
/// prefilter exactly like a `|=` phrase. Anything else skips the prefilter
/// (never wrong, just less pruning) rather than attempting regex analysis
/// (out of scope — see the AST's own "regex not validated" contract).
fn is_plain_literal(pattern: &str) -> bool {
    !pattern.chars().any(|c| REGEX_METACHARS.contains(&c))
}

/// Compiles one `LineFilter`. Positive ops (`|=`, `|~`) render `hasToken`
/// prefilter(s) ANDed with the exact predicate. Negative ops (`!=`, `!~`)
/// wrap the *same* compound predicate in `NOT (...)` rather than negating
/// only the exact predicate: `hasToken` never has false negatives (a bloom
/// filter can only ever say "maybe present" or "definitely absent"), so
/// `hasToken(...) AND exact(...)` is exactly equivalent to `exact(...)`
/// alone — `NOT (hasToken(...) AND exact(...))` is therefore provably
/// equivalent to `NOT exact(...)`, the correct exclusion semantic, while
/// still surfacing the prefilter for ClickHouse's optimizer to exploit
/// where it can (architect plan: "Prefilter is always paired with the
/// exact predicate").
pub(crate) fn compile_line_filter(lf: &LineFilter) -> String {
    match lf.op {
        LineFilterOp::Contains => contains_predicate(&lf.value),
        LineFilterOp::NotContains => format!("NOT ({})", contains_predicate(&lf.value)),
        LineFilterOp::Regex => regex_predicate(&lf.value),
        LineFilterOp::NotRegex => format!("NOT ({})", regex_predicate(&lf.value)),
    }
}

fn contains_predicate(phrase: &str) -> String {
    let mut parts: Vec<String> = tokenize(phrase)
        .iter()
        .map(|t| format!("hasToken(body, {})", ch_string(t)))
        .collect();
    parts.push(format!("position(body, {}) > 0", ch_string(phrase)));
    parts.join(" AND ")
}

fn regex_predicate(pattern: &str) -> String {
    let mut parts: Vec<String> = Vec::new();
    if is_plain_literal(pattern) {
        parts.extend(
            tokenize(pattern)
                .iter()
                .map(|t| format!("hasToken(body, {})", ch_string(t))),
        );
    }
    parts.push(format!("match(body, {})", ch_regex_unanchored(pattern)));
    parts.join(" AND ")
}

/// Days since the Unix epoch, per nanosecond. Local to this module rather
/// than a `pulsus-model` dependency (out of scope per the architect plan's
/// Cargo.toml deps list) — the civil-calendar conversion below is the same
/// public-domain algorithm `pulsus-model::time::Date` uses.
const NANOS_PER_DAY: i64 = 86_400_000_000_000;

/// Howard Hinnant's public-domain civil-calendar algorithm
/// (<http://howardhinnant.github.io/date_algorithms.html#civil_from_days>),
/// correct for the full `i64` day range.
fn civil_from_days(z: i64) -> (i64, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if m <= 2 { y + 1 } else { y }, m)
}

fn year_month(ts_ns: i64) -> (i64, u32) {
    civil_from_days(ts_ns.div_euclid(NANOS_PER_DAY))
}

/// Every calendar month (UTC) overlapping `[start_ns, end_ns]`, ascending,
/// rendered as quoted ClickHouse `Date` literals (`'YYYY-MM-01'`).
/// `log_streams`/`log_streams_idx` partition monthly (docs/schemas.md
/// §3.1); a range spanning a month boundary must resolve every partition it
/// touches or streams silently vanish (architect plan edge case:
/// "Multi-month ranges").
pub(crate) fn months_overlapping(start_ns: i64, end_ns: i64) -> Vec<String> {
    let (mut y, mut m) = year_month(start_ns);
    let (end_y, end_m) = year_month(end_ns.max(start_ns));
    let mut out = Vec::new();
    loop {
        out.push(format!("'{y:04}-{m:02}-01'"));
        if (y, m) == (end_y, end_m) {
            break;
        }
        if m == 12 {
            y += 1;
            m = 1;
        } else {
            m += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use pulsus_logql::{parse, parse_selector};

    use super::*;

    fn selector(src: &str) -> StreamSelector {
        parse_selector(src).expect("parse selector")
    }

    fn test_ctx() -> PlanCtx<'static> {
        PlanCtx {
            db: "pulsus",
            streams_idx: "log_streams_idx",
            streams: "log_streams",
            samples: "log_samples",
            rollup_table: "log_metrics_5s",
            rollup_res_ns: 5_000_000_000,
            scan_budget_bytes: 1024,
            max_streams: 100_000,
            pipeline_scan_factor: 10,
        }
    }

    fn metric_mp(query: &str, spec: QuerySpec) -> Result<MetricPlan, ReadError> {
        let params = QueryParams {
            spec,
            limit: 100,
            direction: Direction::Backward,
        };
        let expr = parse(query).expect("parse");
        match plan(&expr, &params, &test_ctx())? {
            Plan::Metric(mp) => Ok(mp),
            Plan::Streams(_) | Plan::MetricBinary(_) => panic!("expected a Metric plan"),
        }
    }

    /// Test-gap flagged by the architect-plan review: a zero-step `Range`
    /// query must be rejected before it ever reaches the routing decision
    /// — `0.is_multiple_of(_)` is trivially `true`, so without this guard
    /// it would silently route to rollup and render `intDiv(_, 0)`.
    #[test]
    fn a_zero_step_range_query_is_rejected_as_an_invalid_step() {
        let err = metric_mp(
            r#"rate({env="prod"}[5m])"#,
            QuerySpec::Range {
                start_ns: 0,
                end_ns: 1_000_000_000,
                step_ns: 0,
            },
        )
        .unwrap_err();
        assert!(matches!(err, ReadError::InvalidStep));
    }

    #[test]
    fn a_step_dividing_the_resolution_routes_to_rollup_with_a_named_reason() {
        let mp = metric_mp(
            r#"rate({env="prod"}[5m])"#,
            QuerySpec::Range {
                start_ns: 0,
                end_ns: 1_000_000_000_000,
                step_ns: 60_000_000_000,
            },
        )
        .unwrap();
        assert_eq!(mp.routing.chosen, RouteChoice::Rollup);
        assert!(mp.rollup);
        assert_eq!(
            mp.routing.reason,
            "rollup: step 60000000000 ns divisible by resolution 5000000000 ns"
        );
    }

    #[test]
    fn a_step_not_dividing_the_resolution_routes_to_raw_with_a_named_reason() {
        let mp = metric_mp(
            r#"rate({env="prod"}[5m])"#,
            QuerySpec::Range {
                start_ns: 0,
                end_ns: 1_000_000_000_000,
                step_ns: 3_000_000_000,
            },
        )
        .unwrap();
        assert_eq!(mp.routing.chosen, RouteChoice::Raw);
        assert!(!mp.rollup);
        assert_eq!(
            mp.routing.reason,
            "raw: step 3000000000 ns not a multiple of resolution 5000000000 ns"
        );
    }

    #[test]
    fn a_line_filter_routes_to_raw_with_a_named_reason_even_on_an_eligible_step() {
        let mp = metric_mp(
            r#"count_over_time({env="prod"} |= "err" [5m])"#,
            QuerySpec::Range {
                start_ns: 0,
                end_ns: 1_000_000_000_000,
                step_ns: 60_000_000_000,
            },
        )
        .unwrap();
        assert_eq!(mp.routing.chosen, RouteChoice::Raw);
        assert_eq!(mp.routing.reason, "raw: line filter present");
    }

    #[test]
    fn an_instant_query_routes_to_raw_with_a_named_reason() {
        let mp = metric_mp(
            r#"rate({env="prod"}[5m])"#,
            QuerySpec::Instant {
                at_ns: 1_000_000_000,
            },
        )
        .unwrap();
        assert_eq!(mp.routing.chosen, RouteChoice::Raw);
        assert!(!mp.rollup);
        assert_eq!(mp.routing.reason, "raw: instant query");
    }

    #[test]
    fn an_unconfigured_rollup_resolution_routes_to_raw_with_a_named_reason() {
        let params = QueryParams {
            spec: QuerySpec::Range {
                start_ns: 0,
                end_ns: 1_000_000_000_000,
                step_ns: 60_000_000_000,
            },
            limit: 100,
            direction: Direction::Backward,
        };
        let mut ctx = test_ctx();
        ctx.rollup_res_ns = 0;
        let expr = parse(r#"rate({env="prod"}[5m])"#).expect("parse");
        let mp = match plan(&expr, &params, &ctx).unwrap() {
            Plan::Metric(mp) => mp,
            Plan::Streams(_) | Plan::MetricBinary(_) => panic!("expected a Metric plan"),
        };
        assert_eq!(mp.routing.chosen, RouteChoice::Raw);
        assert_eq!(mp.routing.reason, "raw: rollup resolution not configured");
    }

    /// Precedence lock (code review fix, issue #12): `Instant` must win
    /// over every other raw-fallback reason an instant query also happens
    /// to satisfy — a line filter here would otherwise (wrongly) report
    /// "raw: line filter present" instead of "raw: instant query".
    #[test]
    fn an_instant_query_with_a_line_filter_still_reports_the_instant_reason() {
        let mp = metric_mp(
            r#"rate({env="prod"} |= "err" [5m])"#,
            QuerySpec::Instant {
                at_ns: 1_000_000_000,
            },
        )
        .unwrap();
        assert_eq!(mp.routing.chosen, RouteChoice::Raw);
        assert_eq!(mp.routing.reason, "raw: instant query");
    }

    /// Precedence lock (code review fix, issue #12): an unconfigured
    /// rollup resolution must not shadow the "raw: instant query" reason
    /// either.
    #[test]
    fn an_instant_query_with_an_unconfigured_rollup_resolution_still_reports_the_instant_reason() {
        let params = QueryParams {
            spec: QuerySpec::Instant {
                at_ns: 1_000_000_000,
            },
            limit: 100,
            direction: Direction::Backward,
        };
        let mut ctx = test_ctx();
        ctx.rollup_res_ns = 0;
        let expr = parse(r#"rate({env="prod"}[5m])"#).expect("parse");
        let mp = match plan(&expr, &params, &ctx).unwrap() {
            Plan::Metric(mp) => mp,
            Plan::Streams(_) | Plan::MetricBinary(_) => panic!("expected a Metric plan"),
        };
        assert_eq!(mp.routing.chosen, RouteChoice::Raw);
        assert_eq!(mp.routing.reason, "raw: instant query");
    }

    #[test]
    fn single_positive_matcher_collapses_to_one_branch() {
        let n = normalize_matchers(&selector(r#"{service_name="checkout"}"#)).unwrap();
        assert_eq!(
            n.positive_branches,
            vec!["(key = 'service_name' AND val = 'checkout')"]
        );
        assert!(n.negative_branches.is_empty());
        assert!(n.probe_keys.is_empty());
    }

    #[test]
    fn duplicate_eq_on_the_same_key_and_value_dedups_to_one_branch() {
        let n = normalize_matchers(&selector(
            r#"{service_name="checkout", service_name="checkout"}"#,
        ))
        .unwrap();
        assert_eq!(n.positive_branches.len(), 1);
    }

    #[test]
    fn conflicting_eq_values_on_the_same_key_are_contradictory() {
        let err = normalize_matchers(&selector(
            r#"{service_name="checkout", service_name="billing"}"#,
        ))
        .unwrap_err();
        assert!(matches!(err, ReadError::ContradictoryMatchers));
    }

    #[test]
    fn eq_and_re_on_the_same_key_and_both_conditions_into_one_branch() {
        let n = normalize_matchers(&selector(r#"{env="prod", env=~"prod|staging"}"#)).unwrap();
        assert_eq!(n.positive_branches.len(), 1);
        assert_eq!(
            n.positive_branches[0],
            "(key = 'env' AND val = 'prod' AND match(val, '^(?:prod|staging)$'))"
        );
        assert_eq!(n.probe_keys, vec!["env".to_string()]);
    }

    #[test]
    fn negative_only_selector_is_rejected_as_empty_matcher_set() {
        let err = normalize_matchers(&selector(r#"{env!="prod"}"#)).unwrap_err();
        assert!(matches!(err, ReadError::EmptyMatcherSet));
    }

    #[test]
    fn negative_matchers_are_not_collapsed_per_key() {
        let n = normalize_matchers(&selector(
            r#"{service_name="checkout", team!="qa", team!="staging"}"#,
        ))
        .unwrap();
        assert_eq!(n.negative_branches.len(), 2);
    }

    #[test]
    fn months_overlapping_a_single_month_yields_one_literal() {
        // 2026-07-10T00:00:00Z .. 2026-07-15T00:00:00Z.
        let start = 1_783_641_600_000_000_000;
        let end = 1_784_073_600_000_000_000;
        assert_eq!(
            months_overlapping(start, end),
            vec!["'2026-07-01'".to_string()]
        );
    }

    #[test]
    fn months_overlapping_a_boundary_yields_two_literals() {
        // 2026-07-31T23:00:00Z .. 2026-08-01T01:00:00Z.
        let start = 1_785_538_800_000_000_000;
        let end = 1_785_546_000_000_000_000;
        assert_eq!(
            months_overlapping(start, end),
            vec!["'2026-07-01'".to_string(), "'2026-08-01'".to_string()]
        );
    }

    #[test]
    fn months_overlapping_a_year_boundary_advances_the_year() {
        // 2026-12-15 .. 2027-01-15.
        let start = 1_797_292_800_000_000_000;
        let end = 1_799_971_200_000_000_000;
        assert_eq!(
            months_overlapping(start, end),
            vec!["'2026-12-01'".to_string(), "'2027-01-01'".to_string()]
        );
    }

    fn streams_sp(query: &str) -> StreamsPlan {
        let params = QueryParams {
            spec: QuerySpec::Range {
                start_ns: 0,
                end_ns: 1_000_000_000_000,
                step_ns: 60_000_000_000,
            },
            limit: 100,
            direction: Direction::Backward,
        };
        let expr = parse(query).expect("parse");
        match plan(&expr, &params, &test_ctx()).expect("plan") {
            Plan::Streams(sp) => sp,
            Plan::Metric(_) | Plan::MetricBinary(_) => panic!("expected a Streams plan"),
        }
    }

    // --- AC9(i), issue M6-09: scan_limit oversample eligibility. ---

    #[test]
    fn a_label_filter_pipeline_oversamples_the_scan_limit_by_the_factor() {
        let sp = streams_sp(r#"{env="prod"} | json | status = "500""#);
        assert_eq!(sp.result_limit, 100);
        assert_eq!(sp.scan_limit, 1_000, "scan_limit must be limit * factor");
    }

    #[test]
    fn a_line_filter_after_line_format_oversamples_the_scan_limit() {
        let sp = streams_sp(r#"{env="prod"} | line_format "{{.x}}" |= "err""#);
        assert_eq!(sp.result_limit, 100);
        assert_eq!(sp.scan_limit, 1_000);
        // And the unpushable filter is absent from the stage-3 predicates.
        assert!(sp.line_filters.is_empty());
    }

    #[test]
    fn a_line_filter_only_pipeline_keeps_scan_limit_equal_to_the_limit() {
        let sp = streams_sp(r#"{env="prod"} |= "err" != "debug""#);
        assert_eq!(sp.result_limit, 100);
        assert_eq!(sp.scan_limit, 100, "fast path must stay byte-identical");
        assert_eq!(sp.line_filters.len(), 2);
    }

    #[test]
    fn a_parser_only_pipeline_keeps_scan_limit_equal_to_the_limit() {
        // Parsers are non-dropping (a parse failure keeps the line with
        // an `__error__` label) — no oversample.
        let sp = streams_sp(r#"{env="prod"} |= "err" | json"#);
        assert_eq!(sp.scan_limit, 100);
        assert_eq!(
            sp.line_filters.len(),
            1,
            "the line filter still pushes down"
        );
    }

    #[test]
    fn scan_limit_saturates_instead_of_overflowing() {
        let params = QueryParams {
            spec: QuerySpec::Range {
                start_ns: 0,
                end_ns: 1_000_000_000_000,
                step_ns: 60_000_000_000,
            },
            limit: u32::MAX,
            direction: Direction::Backward,
        };
        let expr = parse(r#"{env="prod"} | json | status = "500""#).expect("parse");
        let Plan::Streams(sp) = plan(&expr, &params, &test_ctx()).expect("plan") else {
            panic!("expected a Streams plan");
        };
        assert_eq!(sp.scan_limit, u32::MAX);
    }

    // --- AC6, issue M6-10: the former M6-09 deferral seam is REMOVED —
    // --- every parseable metric pipeline now plans successfully in
    // --- client mode (the exact query list the M6-09 rejection test
    // --- covered, flipped to success).

    fn range_spec() -> QuerySpec {
        QuerySpec::Range {
            start_ns: 0,
            end_ns: 1_000_000_000_000,
            step_ns: 60_000_000_000,
        }
    }

    #[test]
    fn every_formerly_deferred_metric_pipeline_now_plans_in_client_mode() {
        for query in [
            r#"count_over_time({env="prod"} | json [5m])"#,
            r#"count_over_time({env="prod"} | logfmt [5m])"#,
            r#"count_over_time({env="prod"} | regexp "(?P<x>.*)" [5m])"#,
            r#"count_over_time({env="prod"} | pattern "<x> y" [5m])"#,
            r#"count_over_time({env="prod"} | json | status = "500" [5m])"#,
            r#"rate({env="prod"} | level = "error" [5m])"#,
            r#"rate({env="prod"} | line_format "{{.x}}" [5m])"#,
            r#"rate({env="prod"} | label_format a=b [5m])"#,
            r#"rate({env="prod"} | json | unwrap latency [5m])"#,
            r#"rate({env="prod"} | unwrap latency [5m])"#,
        ] {
            let mp = metric_mp(query, range_spec())
                .unwrap_or_else(|e| panic!("expected {query:?} to plan in client mode, got {e}"));
            let client = mp
                .client
                .as_ref()
                .unwrap_or_else(|| panic!("expected {query:?} to carry a client-aggregation spec"));
            assert!(!mp.rollup, "client mode always routes raw: {query}");
            assert_eq!(
                mp.routing.reason, "raw: client-side pipeline/unwrap aggregation",
                "{query}"
            );
            assert_eq!(
                client.pipeline.len(),
                mp.client.as_ref().unwrap().pipeline.len()
            );
        }
    }

    #[test]
    fn client_value_source_follows_op_and_unwrap_presence() {
        let count = metric_mp(
            r#"count_over_time({env="prod"} | json | status = "500" [5m])"#,
            range_spec(),
        )
        .unwrap();
        assert_eq!(count.client.as_ref().unwrap().value, ClientValue::Count);

        let bytes =
            metric_mp(r#"bytes_over_time({env="prod"} | json [5m])"#, range_spec()).unwrap();
        assert_eq!(bytes.client.as_ref().unwrap().value, ClientValue::Bytes);

        let unwrap = metric_mp(
            r#"sum_over_time({env="prod"} | logfmt | unwrap took [5m])"#,
            range_spec(),
        )
        .unwrap();
        assert_eq!(unwrap.client.as_ref().unwrap().value, ClientValue::Unwrap);
        assert_eq!(
            unwrap.client.as_ref().unwrap().range_op,
            pulsus_logql::RangeAggOp::SumOverTime
        );
    }

    #[test]
    fn every_new_over_time_op_plans_in_client_mode_with_unwrap() {
        for (query, op) in [
            (
                r#"sum_over_time({e="p"} | logfmt | unwrap v [5m])"#,
                pulsus_logql::RangeAggOp::SumOverTime,
            ),
            (
                r#"avg_over_time({e="p"} | logfmt | unwrap v [5m])"#,
                pulsus_logql::RangeAggOp::AvgOverTime,
            ),
            (
                r#"min_over_time({e="p"} | logfmt | unwrap v [5m])"#,
                pulsus_logql::RangeAggOp::MinOverTime,
            ),
            (
                r#"max_over_time({e="p"} | logfmt | unwrap v [5m])"#,
                pulsus_logql::RangeAggOp::MaxOverTime,
            ),
            (
                r#"stddev_over_time({e="p"} | logfmt | unwrap v [5m])"#,
                pulsus_logql::RangeAggOp::StddevOverTime,
            ),
            (
                r#"stdvar_over_time({e="p"} | logfmt | unwrap v [5m])"#,
                pulsus_logql::RangeAggOp::StdvarOverTime,
            ),
            (
                r#"quantile_over_time(0.9, {e="p"} | logfmt | unwrap v [5m])"#,
                pulsus_logql::RangeAggOp::QuantileOverTime,
            ),
            (
                r#"first_over_time({e="p"} | logfmt | unwrap v [5m])"#,
                pulsus_logql::RangeAggOp::FirstOverTime,
            ),
            (
                r#"last_over_time({e="p"} | logfmt | unwrap v [5m])"#,
                pulsus_logql::RangeAggOp::LastOverTime,
            ),
        ] {
            let mp = metric_mp(query, range_spec()).unwrap_or_else(|e| panic!("{query}: {e}"));
            let client = mp.client.as_ref().expect("client mode");
            assert_eq!(client.range_op, op, "{query}");
            assert_eq!(client.value, ClientValue::Unwrap, "{query}");
        }
    }

    #[test]
    fn quantile_over_time_param_is_parsed_onto_the_client_spec() {
        let mp = metric_mp(
            r#"quantile_over_time(0.95, {e="p"} | logfmt | unwrap v [5m])"#,
            range_spec(),
        )
        .unwrap();
        assert_eq!(mp.client.as_ref().unwrap().param, Some(0.95));
    }

    /// AC6: unwrap-required ops without `unwrap` are a NAMED
    /// `PipelineInvalid` (message mirrors the oracle's parse error).
    #[test]
    fn unwrap_required_ops_without_unwrap_are_named_pipeline_invalid() {
        for op in [
            "sum_over_time",
            "avg_over_time",
            "min_over_time",
            "max_over_time",
            "stddev_over_time",
            "stdvar_over_time",
            "first_over_time",
            "last_over_time",
        ] {
            let query = format!(r#"{op}({{e="p"}} | logfmt [5m])"#);
            match metric_mp(&query, range_spec()).unwrap_err() {
                ReadError::PipelineInvalid { reason } => {
                    assert_eq!(reason, format!("invalid aggregation {op} without unwrap"));
                }
                other => panic!("expected {query:?} to be PipelineInvalid, got {other:?}"),
            }
        }
    }

    #[test]
    fn unwrap_forbidding_ops_with_unwrap_are_named_pipeline_invalid() {
        for op in ["count_over_time", "bytes_rate", "bytes_over_time"] {
            let query = format!(r#"{op}({{e="p"}} | logfmt | unwrap v [5m])"#);
            match metric_mp(&query, range_spec()).unwrap_err() {
                ReadError::PipelineInvalid { reason } => {
                    assert_eq!(reason, format!("invalid aggregation {op} with unwrap"));
                }
                other => panic!("expected {query:?} to be PipelineInvalid, got {other:?}"),
            }
        }
    }

    #[test]
    fn absent_over_time_plans_selector_wide_with_eq_matcher_labels() {
        let mp = metric_mp(
            r#"absent_over_time({env="prod", team=~"a|b", region="eu"}[5m])"#,
            range_spec(),
        )
        .unwrap();
        let client = mp.client.as_ref().expect("client mode");
        assert_eq!(client.range_op, pulsus_logql::RangeAggOp::AbsentOverTime);
        assert_eq!(
            client.absent_labels,
            vec![
                ("env".to_string(), "prod".to_string()),
                ("region".to_string(), "eu".to_string()),
            ],
            "only Eq matchers become the synthetic-absence labels"
        );
    }

    /// D3 (plan v2): only the line-filter prefix BEFORE the first
    /// `line_format` pushes to SQL; a post-`line_format` filter evaluates
    /// in-engine (it references the rewritten line).
    #[test]
    fn a_post_line_format_metric_line_filter_is_not_pushed_down() {
        let mp = metric_mp(
            r#"count_over_time({env="prod"} |= "a" | line_format "{{.x}}" |= "b" [5m])"#,
            range_spec(),
        )
        .unwrap();
        assert_eq!(
            mp.extra_predicates.len(),
            1,
            "only the pre-line_format filter pushes down"
        );
        assert!(mp.extra_predicates[0].contains("'a'"));
        assert!(!mp.extra_predicates[0].contains("'b'"));
        // The full ordered pipeline (including the unpushed filter) rides
        // the client spec for in-engine evaluation.
        assert_eq!(mp.client.as_ref().unwrap().pipeline.len(), 3);
    }

    // --- Binary-op planning (issue M6-10). ---

    fn plan_of(query: &str, spec: QuerySpec) -> Result<Plan, ReadError> {
        let params = QueryParams {
            spec,
            limit: 100,
            direction: Direction::Backward,
        };
        let expr = parse(query).expect("parse");
        plan(&expr, &params, &test_ctx())
    }

    #[test]
    fn a_binary_metric_expression_plans_to_a_node_tree_with_ordinary_leaves() {
        let p = plan_of(
            r#"rate({env="prod"}[5m]) + rate({env="staging"}[5m])"#,
            range_spec(),
        )
        .unwrap();
        let Plan::MetricBinary(node) = p else {
            panic!("expected a MetricBinary plan, got {p:?}");
        };
        let MetricNode::Binary {
            op, return_bool, ..
        } = &node
        else {
            panic!("expected a Binary root");
        };
        assert_eq!(*op, BinOp::Add);
        assert!(!return_bool);
        let leaves = node.leaves();
        assert_eq!(leaves.len(), 2);
        // Each leaf routes exactly as it would standalone (rollup here).
        for leaf in leaves {
            assert!(leaf.rollup);
            assert!(leaf.client.is_none());
        }
    }

    #[test]
    fn a_scalar_only_binary_expression_plans_leafless() {
        let p = plan_of("2 ^ 2 ^ 3", range_spec()).unwrap();
        let Plan::MetricBinary(node) = p else {
            panic!("expected a MetricBinary plan");
        };
        assert!(node.leaves().is_empty());
    }

    #[test]
    fn a_zero_step_range_is_rejected_even_for_a_leafless_binary_expression() {
        let err = plan_of(
            "2 + 2",
            QuerySpec::Range {
                start_ns: 0,
                end_ns: 1_000_000_000,
                step_ns: 0,
            },
        )
        .unwrap_err();
        assert!(matches!(err, ReadError::InvalidStep));
    }

    #[test]
    fn a_vector_aggregation_over_a_binary_operand_plans_as_a_vector_agg_node() {
        let p = plan_of(
            r#"sum by (service_name) (rate({a="b"}[5m]) + rate({a="c"}[5m]))"#,
            range_spec(),
        )
        .unwrap();
        let Plan::MetricBinary(node) = p else {
            panic!("expected a MetricBinary plan");
        };
        let MetricNode::VectorAgg { aggs, inner } = &node else {
            panic!("expected a VectorAgg root, got {node:?}");
        };
        assert_eq!(aggs.len(), 1);
        assert_eq!(aggs[0].0, VectorAggOp::Sum);
        assert!(matches!(&**inner, MetricNode::Binary { .. }));
    }

    #[test]
    fn a_vector_aggregation_over_a_bare_scalar_is_rejected() {
        let err = plan_of("sum(2)", range_spec()).unwrap_err();
        assert!(matches!(err, ReadError::PipelineInvalid { .. }));
    }

    #[test]
    fn topk_k_is_parsed_onto_the_vector_agg_chain() {
        let mp = metric_mp(r#"topk(5, rate({env="prod"}[5m]))"#, range_spec()).unwrap();
        assert_eq!(mp.vector_aggs.len(), 1);
        assert_eq!(mp.vector_aggs[0].0, VectorAggOp::Topk);
        assert_eq!(mp.vector_aggs[0].2, Some(5.0));
        // topk/bottomk never disturb the inner query's routing.
        assert!(mp.rollup);
    }

    #[test]
    fn a_metric_query_with_only_line_filters_still_plans() {
        let mp = metric_mp(
            r#"count_over_time({env="prod"} |= "err" [5m])"#,
            QuerySpec::Range {
                start_ns: 0,
                end_ns: 1_000_000_000_000,
                step_ns: 60_000_000_000,
            },
        )
        .expect("line-filter-only metric pipelines are in scope");
        assert_eq!(mp.extra_predicates.len(), 1);
    }

    // --- Bare-query unwrap: the planner-owned rejection (plan v3 D1). ---

    #[test]
    fn a_bare_log_query_with_unwrap_is_rejected_as_pipeline_invalid() {
        let params = QueryParams {
            spec: QuerySpec::Range {
                start_ns: 0,
                end_ns: 1_000_000_000_000,
                step_ns: 60_000_000_000,
            },
            limit: 100,
            direction: Direction::Backward,
        };
        let expr = parse(r#"{env="prod"} | json | unwrap latency"#).expect("parse");
        let err = plan(&expr, &params, &test_ctx()).unwrap_err();
        match err {
            ReadError::PipelineInvalid { reason } => {
                assert!(reason.contains("range aggregation"), "{reason}");
            }
            other => panic!("expected PipelineInvalid, got {other:?}"),
        }
    }

    #[test]
    fn tokenize_splits_on_non_alphanumeric_boundaries() {
        assert_eq!(
            tokenize("connection refused"),
            vec!["connection".to_string(), "refused".to_string()]
        );
    }

    #[test]
    fn is_plain_literal_rejects_regex_metacharacters() {
        assert!(is_plain_literal("connection refused"));
        assert!(!is_plain_literal("test.*"));
    }
}
