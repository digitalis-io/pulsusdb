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
    Expr, Grouping, LineFilter, LineFilterOp, LogExpr, MatchOp, Matcher, MetricExpr, RangeAggOp,
    Stage, StreamSelector, VectorAggOp,
};

use super::error::ReadError;
use super::escape::{ch_regex_anchored, ch_regex_unanchored, ch_string};
use super::params::{Direction, PlanCtx, QueryParams, QuerySpec};

/// A pure fetch plan for either query shape. See the module docs for why
/// stage 2/3 aren't pre-rendered here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    Streams(StreamsPlan),
    Metric(MetricPlan),
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
    pub limit: u32,
    /// One pre-rendered predicate fragment per pipeline `LineFilter` stage,
    /// ANDed together by [`super::sql::stage3`].
    pub line_filters: Vec<String>,
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
    pub vector_aggs: Vec<(VectorAggOp, Option<Grouping>)>,
    pub probes: Vec<ProbePlan>,
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
        Expr::Metric(metric_expr) => Ok(Plan::Metric(metric_plan(metric_expr, p, ctx)?)),
    }
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
    let line_filters = compile_line_filters(&log_expr.pipeline);

    Ok(StreamsPlan {
        stage1_sql,
        streams_table: ctx.streams.to_string(),
        samples_table: ctx.samples.to_string(),
        start_ns,
        end_ns,
        direction: p.direction,
        limit: p.limit,
        line_filters,
        probes,
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

    let (base, vector_aggs) = unwrap_vector_aggs(metric_expr);
    let MetricExpr::Range { op, range, .. } = base else {
        // `unwrap_vector_aggs` strips every `Vector` layer, so the base is
        // structurally always `Range` — the AST has no third variant.
        unreachable!("unwrap_vector_aggs always bottoms out at MetricExpr::Range")
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
    // This routing decision assumes every `RangeAggOp` reaching here is
    // count-only (M1's four: `Rate`/`CountOverTime`/`BytesRate`/
    // `BytesOverTime`, `ast.rs`) — a future non-count op (M6) must gate
    // eligibility on `op` too, not just line-filter/step shape. Likewise,
    // "pipeline-created label dependencies" (a pipeline stage that derives
    // a label the query then groups/filters on) are structurally
    // impossible in M1: the only pipeline stage this crate parses is
    // `LineFilter` (`Stage`, `ast.rs`), which never produces a label. Both
    // are guard comments only — nothing to gate on yet.
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
    let routing = match p.spec {
        QuerySpec::Instant { .. } => RoutingDecision {
            chosen: RouteChoice::Raw,
            reason: "raw: instant query".to_string(),
        },
        QuerySpec::Range { step_ns, .. } if !extra_predicates.is_empty() => RoutingDecision {
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
        probes,
    })
}

/// Unwraps every outer `MetricExpr::Vector` layer, returning the innermost
/// `Range` and the aggregation chain in outer-to-inner order (`sum by
/// (svc) (avg(...))` yields `[(Sum, Some(by(svc)))]` first, then deeper
/// wrappers after).
fn unwrap_vector_aggs(expr: &MetricExpr) -> (&MetricExpr, Vec<(VectorAggOp, Option<Grouping>)>) {
    let mut aggs = Vec::new();
    let mut cur = expr;
    while let MetricExpr::Vector {
        op,
        grouping,
        inner,
    } = cur
    {
        aggs.push((*op, grouping.clone()));
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

/// Compiles every pipeline `LineFilter` stage into a stage-3 predicate
/// fragment, in pipeline order (architect plan amendment: line filters
/// "ALWAYS paired with the exact predicate").
pub(crate) fn compile_line_filters(pipeline: &[Stage]) -> Vec<String> {
    pipeline
        .iter()
        .map(|Stage::LineFilter(lf)| compile_line_filter(lf))
        .collect()
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
            Plan::Streams(_) => panic!("expected a Metric plan"),
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
            Plan::Streams(_) => panic!("expected a Metric plan"),
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
            Plan::Streams(_) => panic!("expected a Metric plan"),
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
