//! `QueryPlan` — the pure planner IR and `plan(expr, PlanParams) ->
//! Result<QueryPlan, PromqlError>`. Walks the parsed `Expr` (via
//! [`crate::parser`]), rejects any out-of-subset node with
//! [`PromqlError::Unsupported`], flattens every `VectorSelector`/
//! `MatrixSelector` into an id-indexed [`SelectorSpec`], and records the
//! typed evaluator IR ([`PlanExpr`]) [`crate::eval::evaluate`] walks.
//!
//! **Metric-scoping is structural** (edge case 9): `__name__` is always
//! extracted into `SelectorSpec::metric_name`, never left in `matchers` —
//! this is docs/schemas.md §2.1's metric-scoped model, load-bearing for
//! both the fetch `PREWHERE metric_name = ...` and issue #30's
//! `SeriesResolver::resolve(metric_name, matchers, window)` signature. A
//! selector with no concrete metric name (a `__name__`-less matcher-only
//! selector, or a regex `__name__` matcher) cannot be resolved through
//! that API and is rejected as [`PromqlError::Unsupported`] — never
//! silently mis-scoped.

use pulsus_model::{LabelMatcher, MatchOp};

use crate::error::PromqlError;
use crate::parser::{
    self, AggregateExpr, BinaryExpr, Call, Expr, LabelModifier, MatrixSelector, Offset,
    PLabelMatchOp, SubqueryExpr, UnaryExpr, VectorMatchCardinality, VectorSelector, token,
};

/// Instant query = `start_ms == end_ms`, `step_ms == 0` (a single-step
/// range — the architect plan's "instant = single-step range").
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PlanParams {
    pub start_ms: i64,
    pub end_ms: i64,
    pub step_ms: i64,
    /// The PromQL staleness lookback, milliseconds — a fixed `300_000`
    /// (5m) default for M2 (task-manager resolution #4 on issue #31);
    /// promote to a per-request/config knob only when a deployment needs
    /// it.
    pub lookback_ms: i64,
}

/// The PromQL default staleness lookback (5 minutes), milliseconds.
pub const DEFAULT_LOOKBACK_MS: i64 = 300_000;

pub type SelectorId = usize;

/// One flattened `VectorSelector`/`MatrixSelector` — the resolver/fetch
/// unit. `matchers` excludes `__name__` (see the module doc).
#[derive(Debug, Clone, PartialEq)]
pub struct SelectorSpec {
    pub id: SelectorId,
    pub metric_name: String,
    pub matchers: Vec<LabelMatcher>,
    /// `Some` for a matrix selector (the range-vector width); `None` for
    /// an instant vector selector.
    pub range_ms: Option<i64>,
    pub offset_ms: i64,
}

impl SelectorSpec {
    /// Fetch bounds for the whole eval span (every step of a range query,
    /// or the single step of an instant query). Left-open right-closed:
    /// `lower_excl = start − range − lookback − offset`, `upper_incl = end
    /// − offset` (architect plan interfaces). The `lookback` term is
    /// always subtracted, even for a matrix selector with its own
    /// `range_ms` — deliberately conservative (over-fetches by up to one
    /// lookback width for range-vector-only queries) rather than
    /// special-casing the two selector kinds' fetch bounds differently;
    /// never wrong, only occasionally fetches a little more than the
    /// evaluator strictly needs.
    pub fn fetch_window(&self, p: &PlanParams) -> (i64, i64) {
        let lower_excl = p.start_ms - self.range_ms.unwrap_or(0) - p.lookback_ms - self.offset_ms;
        let upper_incl = p.end_ms - self.offset_ms;
        (lower_excl, upper_incl)
    }
}

/// Range-vector functions with counter-reset correction + extrapolation
/// (`rate`/`increase`/`delta`) or last-two-samples-only (`irate`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeFn {
    Rate,
    Irate,
    Increase,
    Delta,
}

/// `*_over_time` aggregation functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverTimeFn {
    Avg,
    Min,
    Max,
    Sum,
    Count,
}

/// Aggregation operators. `Group` is **not** in features.md §3's
/// aggregation list (only `sum/avg/min/max/count/topk/bottomk` are) — it
/// is sanctioned *only* for `count`/`group` directly over a bare instant
/// vector selector (code review round 1, finding 4 — architect
/// adjudication AMEND). [`plan_aggregate`] therefore restricts `Group` to
/// that structural shape; `group()` over any computed sub-expression is
/// `Unsupported`. This shape used to double as exactly what
/// `QueryPlan::cache_answerable` recognized for a now-removed
/// zero-ClickHouse fast path (issue #33 architect adjudication: the label
/// cache's activity-bucket granularity cannot reproduce PromQL's exact
/// 5-minute staleness lookback, so `count`/`group` always fetch+evaluate
/// now) — the scope restriction on `Group` itself is independent of that
/// removed optimization and still stands on the features.md §3 grounds
/// alone. `Count` has no such restriction: it *is* in the §3 list and is
/// fully general.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    Group,
    Topk,
    Bottomk,
}

/// `by (...)` / `without (...)` grouping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grouping {
    pub without: bool,
    pub labels: Vec<String>,
}

/// Binary arithmetic/comparison operators. Set operators (`and`/`or`/
/// `unless`) and `atan2` are out of the M2 proof subset (never
/// constructed here — [`plan_binary`] rejects them as
/// [`PromqlError::Unsupported`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl BinOp {
    pub fn is_comparison(self) -> bool {
        matches!(
            self,
            BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge
        )
    }
}

/// Vector-vector matching mode. The default (no `on`/`ignoring` clause)
/// behaves identically to an empty `ignoring()` — match on the full label
/// set — so it is represented uniformly rather than as a separate case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Matching {
    /// `true` = `on (labels)`; `false` = `ignoring (labels)` (or the
    /// default, with `labels` empty).
    pub on: bool,
    pub labels: Vec<String>,
}

impl Matching {
    fn default_ignoring_none() -> Self {
        Matching {
            on: false,
            labels: Vec::new(),
        }
    }
}

/// The typed evaluator IR [`crate::eval::evaluate`] walks. Boxed
/// recursion — a query's AST depth is bounded by the input text length, so
/// stack growth is not a practical concern at PromQL query sizes.
#[derive(Debug, Clone, PartialEq)]
pub enum PlanExpr {
    /// An instant-vector selector — [`crate::eval::staleness`] resolves
    /// one sample per series per step from the selector's fetched samples.
    Selector(SelectorId),
    RangeFn {
        func: RangeFn,
        selector: SelectorId,
    },
    OverTime {
        func: OverTimeFn,
        selector: SelectorId,
    },
    HistogramQuantile {
        quantile: Box<PlanExpr>,
        expr: Box<PlanExpr>,
    },
    Aggregate {
        op: AggOp,
        expr: Box<PlanExpr>,
        /// `topk`/`bottomk`'s `k` parameter.
        param: Option<Box<PlanExpr>>,
        grouping: Option<Grouping>,
    },
    Binary {
        op: BinOp,
        lhs: Box<PlanExpr>,
        rhs: Box<PlanExpr>,
        bool_modifier: bool,
        matching: Matching,
    },
    Scalar(f64),
}

/// A parsed query, planned against `params`.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryPlan {
    pub root: PlanExpr,
    pub selectors: Vec<SelectorSpec>,
    pub params: PlanParams,
}

// Issue #33 architect adjudication (superseding #31's ratified zero-
// ClickHouse `count`/`group` AC and #40's instant-only narrowing of it):
// `QueryPlan::cache_answerable`/`CacheAnswerable` — the structural
// predicate that used to let `pulsus-read`'s `MetricsEngine` answer
// `count`/`group` straight from the label cache — are deleted, not merely
// left unused. The differential proved live that the cache's activity-
// *bucket* granularity (1h) cannot distinguish "had a sample within the
// 5-minute PromQL staleness lookback" from "active somewhere in an
// up-to-24h-old 1-hour bucket" (`count(mem_usage_bytes{service="svc-0"})`:
// this engine returned 69 including 12 series silent for over 5 minutes,
// Prometheus correctly returned 57) — no eligibility/age check on the
// cache itself can close that per-series granularity gap. A predicate that
// can never be lookback-correct is a latent trap for a future caller, not
// a dormant optimization worth keeping around; every `count`/`group` query
// now always resolves -> fetches `metric_samples` -> evaluates, where the
// evaluator applies the real 5-minute lookback per step
// (`pulsus-read`'s `MetricsEngine::query_inner`).

/// Planner state: accumulates flattened selectors while recursively
/// walking the AST. Does not need `params` itself — `PlanParams` only
/// matters for [`SelectorSpec::fetch_window`], computed later by the
/// caller (`pulsus-read`'s fetch layer), not during the walk.
struct Planner {
    selectors: Vec<SelectorSpec>,
}

impl Planner {
    fn push_selector(
        &mut self,
        metric_name: String,
        matchers: Vec<LabelMatcher>,
        range_ms: Option<i64>,
        offset_ms: i64,
    ) -> SelectorId {
        let id = self.selectors.len();
        self.selectors.push(SelectorSpec {
            id,
            metric_name,
            matchers,
            range_ms,
            offset_ms,
        });
        id
    }
}

/// Plans `expr` into a [`QueryPlan`] against `params`.
pub fn plan(expr: &Expr, params: PlanParams) -> Result<QueryPlan, PromqlError> {
    let mut planner = Planner {
        selectors: Vec::new(),
    };
    let root = plan_expr(&mut planner, expr)?;
    Ok(QueryPlan {
        root,
        selectors: planner.selectors,
        params,
    })
}

/// Issue #32 code-review round-1 fix: a `match[]` discovery selector
/// (`/series`, `/labels`, `/label/{name}/values`) is looser than a
/// [`SelectorSpec`] — Prometheus's own `match[]` contract permits a
/// **matcher-only** selector with no concrete metric name at all (e.g.
/// `{job="api"}`), which [`extract_name_and_matchers`]/[`plan`] reject by
/// design (the fetch/resolve path is always metric-scoped). This is the
/// discovery-only counterpart: `expr` must be a bare [`Expr::VectorSelector`]
/// (anything else — an aggregate, a binary expression, a function call —
/// is not a `match[]` selector at all, `PromqlError::Unsupported`).
///
/// - A single `__name__` **`Equal`** matcher (or the bare-name syntax,
///   `up{...}`) -> `Some(name)`, removed from the returned matchers.
/// - No `__name__` matcher at all -> `None` (every matcher, including any
///   ordinary label matchers, is retained) — the standard `{job="api"}`
///   discovery case.
/// - `__name__` matched via `Re`/`NotRe`/`NotEqual` ->
///   `PromqlError::Unsupported` (a documented M2 limitation: `metric_name`
///   is a physical column, not a `labels`-JSON key, so regex/negative
///   metric-name discovery needs its own query shape — deferred to M6
///   parity, matching the existing `extract_name_and_matchers` precedent
///   for the query path).
pub fn series_selector(expr: &Expr) -> Result<(Option<String>, Vec<LabelMatcher>), PromqlError> {
    let Expr::VectorSelector(vs) = expr else {
        return Err(unsupported(
            "match[] selector must be a bare vector selector",
        ));
    };
    if !vs.matchers.or_matchers.is_empty() {
        return Err(unsupported(
            "UTF-8-quoted label-name-or selector syntax (or_matchers)",
        ));
    }

    let mut metric_name: Option<String> = vs.name.clone();
    let mut matchers = Vec::with_capacity(vs.matchers.matchers.len());
    for m in &vs.matchers.matchers {
        if m.name == "__name__" {
            match &m.op {
                PLabelMatchOp::Equal if metric_name.is_none() => {
                    metric_name = Some(m.value.clone());
                }
                PLabelMatchOp::Equal => {
                    return Err(unsupported("selector with a metric name set twice"));
                }
                _ => {
                    return Err(unsupported("__name__ regex/negative in match[]"));
                }
            }
            continue;
        }
        matchers.push(convert_matcher(m)?);
    }

    Ok((metric_name, matchers))
}

fn unsupported(construct: impl Into<String>) -> PromqlError {
    PromqlError::Unsupported {
        construct: construct.into(),
    }
}

fn duration_ms(d: std::time::Duration) -> i64 {
    d.as_millis() as i64
}

fn offset_ms(offset: &Option<Offset>) -> i64 {
    match offset {
        None => 0,
        Some(Offset::Pos(d)) => duration_ms(*d),
        Some(Offset::Neg(d)) => -duration_ms(*d),
    }
}

/// Extracts `(metric_name, matchers-excluding-__name__)` from a
/// [`VectorSelector`], per the module doc's metric-scoping rule.
fn extract_name_and_matchers(
    vs: &VectorSelector,
) -> Result<(String, Vec<LabelMatcher>), PromqlError> {
    if !vs.matchers.or_matchers.is_empty() {
        return Err(unsupported(
            "UTF-8-quoted label-name-or selector syntax (or_matchers)",
        ));
    }

    let mut metric_name: Option<String> = vs.name.clone();
    let mut matchers = Vec::with_capacity(vs.matchers.matchers.len());
    for m in &vs.matchers.matchers {
        if m.name == "__name__" {
            match &m.op {
                PLabelMatchOp::Equal if metric_name.is_none() => {
                    metric_name = Some(m.value.clone());
                }
                PLabelMatchOp::Equal => {
                    // The parser rejects a bare name *and* an explicit
                    // `__name__` matcher together before this is ever
                    // reached, but this branch keeps the extraction total
                    // rather than relying on that upstream invariant.
                    return Err(unsupported("selector with a metric name set twice"));
                }
                _ => {
                    return Err(unsupported(
                        "__name__ matched via regex or negation (no single concrete metric name)",
                    ));
                }
            }
            continue;
        }
        matchers.push(convert_matcher(m)?);
    }

    let metric_name = metric_name.ok_or_else(|| {
        unsupported("selector without a concrete metric name (docs/schemas.md's metric-scoped model requires one)")
    })?;

    Ok((metric_name, matchers))
}

fn convert_matcher(m: &parser::PMatcher) -> Result<LabelMatcher, PromqlError> {
    let op = match &m.op {
        PLabelMatchOp::Equal => MatchOp::Eq,
        PLabelMatchOp::NotEqual => MatchOp::Neq,
        PLabelMatchOp::Re(_) => MatchOp::Re,
        PLabelMatchOp::NotRe(_) => MatchOp::Nre,
    };
    Ok(LabelMatcher {
        key: m.name.clone(),
        op,
        value: m.value.clone(),
    })
}

fn plan_vector_selector(
    planner: &mut Planner,
    vs: &VectorSelector,
) -> Result<PlanExpr, PromqlError> {
    if vs.at.is_some() {
        return Err(unsupported("the @ modifier"));
    }
    let (metric_name, matchers) = extract_name_and_matchers(vs)?;
    let id = planner.push_selector(metric_name, matchers, None, offset_ms(&vs.offset));
    Ok(PlanExpr::Selector(id))
}

/// Plans a matrix selector into a selector-id, for the range/over_time
/// function call sites that expect exactly this shape as their sole
/// argument. Not reachable from generic `plan_expr` (a bare matrix
/// expression outside a range function is rejected there).
fn plan_matrix_selector_id(
    planner: &mut Planner,
    ms: &MatrixSelector,
) -> Result<SelectorId, PromqlError> {
    if ms.vs.at.is_some() {
        return Err(unsupported("the @ modifier"));
    }
    let (metric_name, matchers) = extract_name_and_matchers(&ms.vs)?;
    Ok(planner.push_selector(
        metric_name,
        matchers,
        Some(duration_ms(ms.range)),
        offset_ms(&ms.vs.offset),
    ))
}

fn plan_call(planner: &mut Planner, call: &Call) -> Result<PlanExpr, PromqlError> {
    let name = call.func.name;
    let args = &call.args.args;

    let range_fn = match name {
        "rate" => Some(RangeFn::Rate),
        "irate" => Some(RangeFn::Irate),
        "increase" => Some(RangeFn::Increase),
        "delta" => Some(RangeFn::Delta),
        _ => None,
    };
    if let Some(func) = range_fn {
        let [arg] = args.as_slice() else {
            return Err(unsupported(format!("{name}() with != 1 argument")));
        };
        let Expr::MatrixSelector(ms) = arg.as_ref() else {
            return Err(unsupported(format!(
                "{name}() over an expression other than a bare range-vector selector"
            )));
        };
        let selector = plan_matrix_selector_id(planner, ms)?;
        return Ok(PlanExpr::RangeFn { func, selector });
    }

    let over_time_fn = match name {
        "avg_over_time" => Some(OverTimeFn::Avg),
        "min_over_time" => Some(OverTimeFn::Min),
        "max_over_time" => Some(OverTimeFn::Max),
        "sum_over_time" => Some(OverTimeFn::Sum),
        "count_over_time" => Some(OverTimeFn::Count),
        _ => None,
    };
    if let Some(func) = over_time_fn {
        let [arg] = args.as_slice() else {
            return Err(unsupported(format!("{name}() with != 1 argument")));
        };
        let Expr::MatrixSelector(ms) = arg.as_ref() else {
            return Err(unsupported(format!(
                "{name}() over an expression other than a bare range-vector selector"
            )));
        };
        let selector = plan_matrix_selector_id(planner, ms)?;
        return Ok(PlanExpr::OverTime { func, selector });
    }

    if name == "histogram_quantile" {
        let [quantile_arg, expr_arg] = args.as_slice() else {
            return Err(unsupported("histogram_quantile() with != 2 arguments"));
        };
        let quantile = plan_expr(planner, quantile_arg)?;
        let expr = plan_expr(planner, expr_arg)?;
        return Ok(PlanExpr::HistogramQuantile {
            quantile: Box::new(quantile),
            expr: Box::new(expr),
        });
    }

    Err(unsupported(format!("function {name}()")))
}

fn agg_op(op: token::TokenType) -> Option<AggOp> {
    match op.id() {
        id if id == token::T_SUM => Some(AggOp::Sum),
        id if id == token::T_AVG => Some(AggOp::Avg),
        id if id == token::T_MIN => Some(AggOp::Min),
        id if id == token::T_MAX => Some(AggOp::Max),
        id if id == token::T_COUNT => Some(AggOp::Count),
        id if id == token::T_GROUP => Some(AggOp::Group),
        id if id == token::T_TOPK => Some(AggOp::Topk),
        id if id == token::T_BOTTOMK => Some(AggOp::Bottomk),
        _ => None,
    }
}

/// Strips every layer of `Expr::Paren` wrapping, returning the innermost
/// non-paren expression — mirrors `plan_expr`'s own `Expr::Paren(p) =>
/// plan_expr(planner, &p.expr)` transparent unwrap, so raw-AST structural
/// checks performed *before* planning (e.g. [`plan_aggregate`]'s `group`
/// restriction) agree with what planning itself would see. `((up))`
/// unwraps to `up` in two iterations; a non-paren expression unwraps to
/// itself in zero.
fn unwrap_parens(mut expr: &Expr) -> &Expr {
    while let Expr::Paren(p) = expr {
        expr = &p.expr;
    }
    expr
}

fn plan_aggregate(planner: &mut Planner, agg: &AggregateExpr) -> Result<PlanExpr, PromqlError> {
    let Some(op) = agg_op(agg.op) else {
        return Err(unsupported(format!("aggregation operator {}", agg.op)));
    };
    let grouping = agg.modifier.as_ref().map(|m| match m {
        LabelModifier::Include(ls) => Grouping {
            without: false,
            labels: ls.labels.clone(),
        },
        LabelModifier::Exclude(ls) => Grouping {
            without: true,
            labels: ls.labels.clone(),
        },
    });
    let param = match &agg.param {
        Some(p) => Some(Box::new(plan_expr(planner, p)?)),
        None => None,
    };
    if matches!(op, AggOp::Topk | AggOp::Bottomk) && param.is_none() {
        return Err(unsupported(format!("{} without a k parameter", agg.op)));
    }
    // `group` is restricted to a bare **instant** vector-selector body
    // (code review round 1, finding 4: `group` is not in features.md §3's
    // aggregation list, sanctioned only over a bare selector — see
    // `AggOp`'s own doc comment for the now-removed `cache_answerable` fast
    // path this shape used to double for, issue #33). Checked on the
    // **raw AST** (`agg.expr`), before planning, so every non-selector body
    // (a range vector, a function call, an arithmetic expression, a
    // paren-wrapped range vector...) gets the same named-construct error,
    // rather than `plan_expr`'s generic `Expr::MatrixSelector` arm firing
    // first with an unrelated message for the range-vector case
    // specifically (code review round 2: `group(up[5m])` must name "group"
    // in its `Unsupported` error, not just "range vector used outside
    // ..."). `offset` stays permitted (round 2, the ratified
    // historical-variant sanction): a `VectorSelector` with `offset` set is
    // still `Expr::VectorSelector` structurally, so it passes this check
    // unchanged and always routes through the ordinary resolve+fetch path,
    // which falls back to `metric_series` exactly like any other
    // out-of-cache-window selector. Nested parentheses are unwrapped first
    // (code review round 3 — a regression
    // the round-2 fix introduced: `group((up))` was wrongly rejected,
    // since this check compared `agg.expr` directly against
    // `Expr::VectorSelector` without accounting for `plan_expr`'s own
    // transparent `Expr::Paren` unwrap a few lines below) — so
    // `group((up))`/`group(((up offset 5m)))` are permitted exactly like
    // their unparenthesized forms, while `group((up[5m]))` is still
    // rejected once unwrapping reaches the inner `Expr::MatrixSelector`.
    if op == AggOp::Group && !matches!(unwrap_parens(&agg.expr), Expr::VectorSelector(_)) {
        return Err(unsupported("group (except over a bare instant selector)"));
    }
    let expr = plan_expr(planner, &agg.expr)?;
    // Defense-in-depth, redundant given the raw-AST check above (kept in
    // case that check is ever loosened without updating this one):
    // `PlanExpr::Selector` is only ever produced by
    // [`plan_vector_selector`], which always sets `range_ms: None`, so a
    // range vector can never actually reach this point as `op ==
    // AggOp::Group` with `expr` a bare `Selector` — this `debug_assert!`
    // documents and checks that invariant rather than silently relying on
    // it.
    if op == AggOp::Group
        && let PlanExpr::Selector(id) = &expr
    {
        debug_assert!(
            planner.selectors[*id].range_ms.is_none(),
            "a range-vector selector must never reach plan_aggregate's Group arm as a bare \
             Selector — the raw-AST check above should have already rejected it"
        );
    }
    Ok(PlanExpr::Aggregate {
        op,
        expr: Box::new(expr),
        param,
        grouping,
    })
}

fn bin_op(op: token::TokenType) -> Option<BinOp> {
    match op.id() {
        id if id == token::T_ADD => Some(BinOp::Add),
        id if id == token::T_SUB => Some(BinOp::Sub),
        id if id == token::T_MUL => Some(BinOp::Mul),
        id if id == token::T_DIV => Some(BinOp::Div),
        id if id == token::T_MOD => Some(BinOp::Mod),
        id if id == token::T_POW => Some(BinOp::Pow),
        id if id == token::T_EQLC => Some(BinOp::Eq),
        id if id == token::T_NEQ => Some(BinOp::Ne),
        id if id == token::T_LSS => Some(BinOp::Lt),
        id if id == token::T_LTE => Some(BinOp::Le),
        id if id == token::T_GTR => Some(BinOp::Gt),
        id if id == token::T_GTE => Some(BinOp::Ge),
        _ => None,
    }
}

fn plan_binary(planner: &mut Planner, bin: &BinaryExpr) -> Result<PlanExpr, PromqlError> {
    let Some(op) = bin_op(bin.op) else {
        return Err(unsupported(format!("binary operator {}", bin.op)));
    };

    let (bool_modifier, matching) = match &bin.modifier {
        None => (false, Matching::default_ignoring_none()),
        Some(m) => {
            match &m.card {
                VectorMatchCardinality::OneToOne | VectorMatchCardinality::ManyToMany => {}
                VectorMatchCardinality::ManyToOne(_) | VectorMatchCardinality::OneToMany(_) => {
                    return Err(unsupported(
                        "group_left/group_right (many-to-one vector matching)",
                    ));
                }
            }
            // Issue #81: the vendored parser accepts the experimental
            // `fill`/`fill_left`/`fill_right` binary-operator modifiers
            // (`BinModifier::fill_values`), but the M2 evaluator has no
            // unmatched-side filling — dropping the modifier here would
            // silently return wrong (unfilled) results, the worst failure
            // class for a query engine. Reject by name until M6-07
            // implements the real semantics and removes this. Zero cost
            // for non-fill queries: a single `is_some()` check on the
            // already-parsed modifier struct.
            if m.fill_values.lhs.is_some() || m.fill_values.rhs.is_some() {
                return Err(unsupported(
                    "fill/fill_left/fill_right (binary-operator fill modifier)",
                ));
            }
            let matching = match &m.matching {
                None => Matching::default_ignoring_none(),
                Some(LabelModifier::Include(ls)) => Matching {
                    on: true,
                    labels: ls.labels.clone(),
                },
                Some(LabelModifier::Exclude(ls)) => Matching {
                    on: false,
                    labels: ls.labels.clone(),
                },
            };
            (m.return_bool, matching)
        }
    };

    let lhs = plan_expr(planner, &bin.lhs)?;
    let rhs = plan_expr(planner, &bin.rhs)?;
    Ok(PlanExpr::Binary {
        op,
        lhs: Box::new(lhs),
        rhs: Box::new(rhs),
        bool_modifier,
        matching,
    })
}

fn plan_expr(planner: &mut Planner, expr: &Expr) -> Result<PlanExpr, PromqlError> {
    match expr {
        Expr::VectorSelector(vs) => plan_vector_selector(planner, vs),
        Expr::MatrixSelector(_) => Err(unsupported(
            "range vector used outside rate/irate/increase/delta/*_over_time",
        )),
        Expr::Call(call) => plan_call(planner, call),
        Expr::Aggregate(agg) => plan_aggregate(planner, agg),
        Expr::Binary(bin) => plan_binary(planner, bin),
        Expr::Paren(p) => plan_expr(planner, &p.expr),
        Expr::NumberLiteral(n) => Ok(PlanExpr::Scalar(n.val)),
        Expr::StringLiteral(_) => Err(unsupported("string literal")),
        Expr::Unary(UnaryExpr { .. }) => {
            Err(unsupported("unary negation of a non-scalar expression"))
        }
        Expr::Subquery(SubqueryExpr { .. }) => Err(unsupported("subquery")),
        Expr::Extension(_) => Err(unsupported("extension expression")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse;

    fn params() -> PlanParams {
        PlanParams {
            start_ms: 1_000_000,
            end_ms: 1_000_000,
            step_ms: 0,
            lookback_ms: DEFAULT_LOOKBACK_MS,
        }
    }

    #[test]
    fn plans_a_bare_selector_with_extracted_metric_name() {
        let expr = parse("up").unwrap();
        let p = plan(&expr, params()).unwrap();
        assert_eq!(p.selectors.len(), 1);
        assert_eq!(p.selectors[0].metric_name, "up");
        assert!(p.selectors[0].matchers.is_empty());
        assert_eq!(p.root, PlanExpr::Selector(0));
    }

    #[test]
    fn plans_a_selector_with_matchers_excluding_name() {
        let expr = parse(r#"up{job="api"}"#).unwrap();
        let p = plan(&expr, params()).unwrap();
        assert_eq!(p.selectors[0].metric_name, "up");
        assert_eq!(
            p.selectors[0].matchers,
            vec![LabelMatcher {
                key: "job".to_string(),
                op: MatchOp::Eq,
                value: "api".to_string(),
            }]
        );
    }

    #[test]
    fn plans_an_explicit_name_matcher_form() {
        let expr = parse(r#"{__name__="up",job="api"}"#).unwrap();
        let p = plan(&expr, params()).unwrap();
        assert_eq!(p.selectors[0].metric_name, "up");
        assert_eq!(p.selectors[0].matchers.len(), 1);
        assert_eq!(p.selectors[0].matchers[0].key, "job");
    }

    #[test]
    fn a_selector_without_a_concrete_metric_name_is_unsupported() {
        let expr = parse(r#"{job="api"}"#).unwrap();
        let err = plan(&expr, params()).unwrap_err();
        assert!(matches!(err, PromqlError::Unsupported { .. }));
    }

    #[test]
    fn a_regex_name_matcher_is_unsupported() {
        let expr = parse(r#"{__name__=~"up.*"}"#).unwrap();
        let err = plan(&expr, params()).unwrap_err();
        assert!(matches!(err, PromqlError::Unsupported { .. }));
    }

    /// Issue #37 architect adjudication (code-review finding 1, REJECT —
    /// guard test): the specific multi-metric-alternation shape the
    /// finding named (`{__name__=~"foo|bar"}`) is rejected by `plan()`
    /// exactly like the simpler `up.*` case above — pins the invariant
    /// `eval::eval_step`'s `PlanExpr::Selector` arm's `debug_assert!`
    /// documents: every reachable `SelectorSpec` carries exactly one
    /// concrete metric name, never a multi-metric alternation.
    #[test]
    fn a_name_alternation_regex_matcher_is_unsupported() {
        let expr = parse(r#"{__name__=~"foo|bar"}"#).unwrap();
        let err = plan(&expr, params()).unwrap_err();
        assert!(matches!(err, PromqlError::Unsupported { .. }));
    }

    // --- series_selector (issue #32 code-review round-1 fix) ---

    #[test]
    fn series_selector_extracts_the_bare_metric_name() {
        let expr = parse("up").unwrap();
        let (name, matchers) = series_selector(&expr).unwrap();
        assert_eq!(name, Some("up".to_string()));
        assert!(matchers.is_empty());
    }

    #[test]
    fn series_selector_extracts_the_explicit_name_matcher_form() {
        let expr = parse(r#"{__name__="up",job="api"}"#).unwrap();
        let (name, matchers) = series_selector(&expr).unwrap();
        assert_eq!(name, Some("up".to_string()));
        assert_eq!(matchers.len(), 1);
        assert_eq!(matchers[0].key, "job");
    }

    #[test]
    fn series_selector_permits_a_matcher_only_selector_with_no_metric_name() {
        let expr = parse(r#"{job="api"}"#).unwrap();
        let (name, matchers) = series_selector(&expr).unwrap();
        assert_eq!(name, None);
        assert_eq!(
            matchers,
            vec![LabelMatcher {
                key: "job".to_string(),
                op: MatchOp::Eq,
                value: "api".to_string(),
            }]
        );
    }

    #[test]
    fn series_selector_retains_matchers_alongside_a_named_metric() {
        let expr = parse(r#"up{job="api",env=~"prod.*"}"#).unwrap();
        let (name, matchers) = series_selector(&expr).unwrap();
        assert_eq!(name, Some("up".to_string()));
        assert_eq!(matchers.len(), 2);
    }

    #[test]
    fn series_selector_rejects_a_regex_name_matcher() {
        let expr = parse(r#"{__name__=~"up.*"}"#).unwrap();
        let err = series_selector(&expr).unwrap_err();
        assert!(matches!(err, PromqlError::Unsupported { .. }));
    }

    #[test]
    fn series_selector_rejects_a_negative_name_matcher() {
        // A bare `__name__!=...` matcher is not itself a valid selector
        // (the upstream parser requires at least one non-negated
        // matcher) — pairs it with an ordinary matcher so parsing
        // succeeds and `series_selector`'s own rejection is exercised.
        let expr = parse(r#"{__name__!="up",job="api"}"#).unwrap();
        let err = series_selector(&expr).unwrap_err();
        assert!(matches!(err, PromqlError::Unsupported { .. }));
    }

    #[test]
    fn series_selector_rejects_a_not_regex_name_matcher() {
        let expr = parse(r#"{__name__!~"up.*",job="api"}"#).unwrap();
        let err = series_selector(&expr).unwrap_err();
        assert!(matches!(err, PromqlError::Unsupported { .. }));
    }

    #[test]
    fn series_selector_rejects_a_non_selector_expression() {
        let expr = parse("sum(up)").unwrap();
        let err = series_selector(&expr).unwrap_err();
        assert!(matches!(err, PromqlError::Unsupported { .. }));
    }

    #[test]
    fn plans_offset_into_offset_ms() {
        let expr = parse("up offset 5m").unwrap();
        let p = plan(&expr, params()).unwrap();
        assert_eq!(p.selectors[0].offset_ms, 300_000);
    }

    #[test]
    fn plans_negative_offset() {
        let expr = parse("up offset -5m").unwrap();
        let p = plan(&expr, params()).unwrap();
        assert_eq!(p.selectors[0].offset_ms, -300_000);
    }

    #[test]
    fn the_at_modifier_is_unsupported() {
        let expr = parse("up @ 100").unwrap();
        let err = plan(&expr, params()).unwrap_err();
        assert!(matches!(err, PromqlError::Unsupported { .. }));
    }

    #[test]
    fn plans_a_matrix_selector_inside_rate() {
        let expr = parse("rate(http_requests_total[5m])").unwrap();
        let p = plan(&expr, params()).unwrap();
        assert_eq!(p.selectors[0].range_ms, Some(300_000));
        assert_eq!(
            p.root,
            PlanExpr::RangeFn {
                func: RangeFn::Rate,
                selector: 0
            }
        );
    }

    #[test]
    fn a_bare_matrix_selector_outside_a_range_function_is_unsupported() {
        // The vendored parser's own type checker already rejects every
        // surface-syntax way of placing a matrix-typed expression outside
        // a range-function argument position (e.g. `sum(foo[5m])` fails to
        // *parse* at all: "expected type vector in aggregation
        // expression, got matrix") — so `plan_expr`'s `MatrixSelector`
        // arm is defense-in-depth, never reachable through a query this
        // crate's own `parse()` can produce. Exercised directly here by
        // hand-constructing the AST node, bypassing `parse()`.
        let expr = parser::Expr::MatrixSelector(parser::MatrixSelector {
            vs: parser::VectorSelector::from("foo"),
            range: std::time::Duration::from_secs(60),
        });
        let err = plan(&expr, params()).unwrap_err();
        assert!(matches!(err, PromqlError::Unsupported { .. }));
    }

    #[test]
    fn rate_over_anything_other_than_a_bare_matrix_selector_is_unsupported() {
        let expr = parse("rate(sum(foo)[5m:1m])").unwrap();
        let err = plan(&expr, params());
        assert!(err.is_err());
    }

    #[test]
    fn plans_sum_by_with_grouping() {
        let expr = parse("sum by (job) (up)").unwrap();
        let p = plan(&expr, params()).unwrap();
        match &p.root {
            PlanExpr::Aggregate { op, grouping, .. } => {
                assert_eq!(*op, AggOp::Sum);
                assert_eq!(
                    grouping,
                    &Some(Grouping {
                        without: false,
                        labels: vec!["job".to_string()]
                    })
                );
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn plans_topk_with_its_k_parameter() {
        let expr = parse("topk(5, up)").unwrap();
        let p = plan(&expr, params()).unwrap();
        match &p.root {
            PlanExpr::Aggregate { op, param, .. } => {
                assert_eq!(*op, AggOp::Topk);
                assert_eq!(param.as_deref(), Some(&PlanExpr::Scalar(5.0)));
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn plans_a_vector_scalar_binary_expression() {
        let expr = parse("up * 2").unwrap();
        let p = plan(&expr, params()).unwrap();
        match &p.root {
            PlanExpr::Binary { op, lhs, rhs, .. } => {
                assert_eq!(*op, BinOp::Mul);
                assert_eq!(**lhs, PlanExpr::Selector(0));
                assert_eq!(**rhs, PlanExpr::Scalar(2.0));
            }
            other => panic!("expected Binary, got {other:?}"),
        }
    }

    #[test]
    fn plans_on_matching() {
        let expr = parse("foo * on(job) bar").unwrap();
        let p = plan(&expr, params()).unwrap();
        match &p.root {
            PlanExpr::Binary { matching, .. } => {
                assert!(matching.on);
                assert_eq!(matching.labels, vec!["job".to_string()]);
            }
            other => panic!("expected Binary, got {other:?}"),
        }
    }

    #[test]
    fn plans_bool_comparison() {
        let expr = parse("up == bool 1").unwrap();
        let p = plan(&expr, params()).unwrap();
        match &p.root {
            PlanExpr::Binary {
                op, bool_modifier, ..
            } => {
                assert_eq!(*op, BinOp::Eq);
                assert!(bool_modifier);
            }
            other => panic!("expected Binary, got {other:?}"),
        }
    }

    #[test]
    fn group_left_is_unsupported() {
        let expr = parse("foo * on(job) group_left(x) bar").unwrap();
        let err = plan(&expr, params()).unwrap_err();
        assert!(matches!(err, PromqlError::Unsupported { .. }));
    }

    #[test]
    fn and_is_unsupported() {
        let expr = parse("foo and bar").unwrap();
        let err = plan(&expr, params()).unwrap_err();
        assert!(matches!(err, PromqlError::Unsupported { .. }));
    }

    /// Issue #81: every fill-modifier spelling (`fill`, `fill_left`,
    /// `fill_right`, and both one-sided forms combined) must fail with a
    /// *named* `Unsupported` — before this reject, `plan_binary` silently
    /// dropped `BinModifier::fill_values` and returned wrong (unfilled)
    /// results. M6-07 implements the real semantics and removes the
    /// reject.
    #[test]
    fn every_fill_modifier_spelling_is_a_named_unsupported() {
        for query in [
            "foo + fill(0) bar",
            "foo + fill_left(0) bar",
            "foo + fill_right(0) bar",
            "foo + fill_left(5) fill_right(7) bar",
            "foo == bool fill(30) bar",
            "foo + on(job) fill(0) bar",
        ] {
            let expr = parse(query).unwrap();
            let err = plan(&expr, params()).unwrap_err();
            match err {
                PromqlError::Unsupported { construct } => assert!(
                    construct.contains("fill"),
                    "{query}: error must name the fill construct, got {construct:?}"
                ),
                other => panic!("{query}: expected Unsupported, got {other:?}"),
            }
        }
    }

    /// Issue #81 guard for the non-fill side: a modifier *without* fill
    /// values (plain `on(...)`) keeps planning exactly as before — the
    /// reject is a single `is_some()` check on the already-parsed
    /// modifier, never a new cost or behavior change for non-fill
    /// queries.
    #[test]
    fn a_modifier_without_fill_values_still_plans() {
        let expr = parse("foo * on(job) bar").unwrap();
        assert!(plan(&expr, params()).is_ok());
    }

    #[test]
    fn a_function_outside_the_m2_list_is_unsupported() {
        let expr = parse("abs(up)").unwrap();
        let err = plan(&expr, params()).unwrap_err();
        match err {
            PromqlError::Unsupported { construct } => assert!(construct.contains("abs")),
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn plans_histogram_quantile() {
        let expr = parse("histogram_quantile(0.9, rate(x_bucket[5m]))").unwrap();
        let p = plan(&expr, params()).unwrap();
        match &p.root {
            PlanExpr::HistogramQuantile { quantile, expr } => {
                assert_eq!(**quantile, PlanExpr::Scalar(0.9));
                assert!(matches!(
                    **expr,
                    PlanExpr::RangeFn {
                        func: RangeFn::Rate,
                        ..
                    }
                ));
            }
            other => panic!("expected HistogramQuantile, got {other:?}"),
        }
    }

    #[test]
    fn fetch_window_subtracts_range_lookback_and_offset() {
        let sel = SelectorSpec {
            id: 0,
            metric_name: "up".to_string(),
            matchers: Vec::new(),
            range_ms: Some(300_000),
            offset_ms: 60_000,
        };
        let p = PlanParams {
            start_ms: 10_000_000,
            end_ms: 10_000_000,
            step_ms: 0,
            lookback_ms: DEFAULT_LOOKBACK_MS,
        };
        let (lower_excl, upper_incl) = sel.fetch_window(&p);
        assert_eq!(lower_excl, 10_000_000 - 300_000 - 300_000 - 60_000);
        assert_eq!(upper_incl, 10_000_000 - 60_000);
    }

    // --- `group` restricted to a bare instant selector (code review
    // round 1, finding 4) ---

    #[test]
    fn group_over_a_bare_instant_selector_is_planned() {
        let expr = parse(r#"group(up)"#).unwrap();
        let p = plan(&expr, params()).unwrap();
        match &p.root {
            PlanExpr::Aggregate { op, expr, .. } => {
                assert_eq!(*op, AggOp::Group);
                assert!(matches!(**expr, PlanExpr::Selector(_)));
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn group_by_over_a_bare_instant_selector_is_planned() {
        let expr = parse(r#"group by (job) (up)"#).unwrap();
        let p = plan(&expr, params()).unwrap();
        assert!(matches!(
            p.root,
            PlanExpr::Aggregate {
                op: AggOp::Group,
                ..
            }
        ));
    }

    #[test]
    fn group_over_a_computed_expression_is_unsupported() {
        let expr = parse(r#"group(rate(x[5m]))"#).unwrap();
        let err = plan(&expr, params()).unwrap_err();
        match err {
            PromqlError::Unsupported { construct } => assert!(construct.contains("group")),
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn group_over_vector_scalar_arithmetic_is_unsupported() {
        let expr = parse(r#"group(up * 2)"#).unwrap();
        let err = plan(&expr, params()).unwrap_err();
        assert!(matches!(err, PromqlError::Unsupported { .. }));
    }

    #[test]
    fn count_over_a_computed_expression_is_still_general() {
        // `count` is in features.md §3's list — unlike `group`, it is not
        // restricted to a bare instant selector.
        let expr = parse(r#"count(rate(x[5m]))"#).unwrap();
        assert!(plan(&expr, params()).is_ok());
    }

    // --- `group` over a range vector (code review round 2) ---

    #[test]
    fn group_over_a_range_vector_body_is_rejected_by_the_parser_itself() {
        // `group(up[5m])` never reaches this crate's `plan()` at all: the
        // vendored parser's own type checker rejects a matrix-typed
        // aggregation body ("expected type vector in aggregation
        // expression, got matrix") at `parse()` time — the same upstream
        // behavior `sum(foo[5m])` hits (see
        // `a_bare_matrix_selector_outside_a_range_function_is_unsupported`).
        let err = parse("group(up[5m])").unwrap_err();
        assert!(err.to_string().contains("matrix"));
    }

    #[test]
    fn group_over_a_range_vector_body_is_unsupported_when_hand_constructed() {
        // Defense-in-depth (code review round 2): even though `parse()`
        // itself already rejects this shape (see the test above), directly
        // exercise `plan_aggregate`'s own `range_ms` check by hand-
        // constructing the AST, bypassing `parse()` entirely — mirrors
        // `a_bare_matrix_selector_outside_a_range_function_is_unsupported`'s
        // own bypass technique.
        let matrix = parser::Expr::MatrixSelector(parser::MatrixSelector {
            vs: parser::VectorSelector::from("up"),
            range: std::time::Duration::from_secs(300),
        });
        let group_expr = parser::Expr::Aggregate(parser::AggregateExpr {
            op: token::TokenType::new(token::T_GROUP),
            expr: Box::new(matrix),
            param: None,
            modifier: None,
        });
        let err = plan(&group_expr, params()).unwrap_err();
        match err {
            PromqlError::Unsupported { construct } => assert!(construct.contains("group")),
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn group_over_a_bare_instant_selector_with_offset_is_permitted() {
        // `offset` stays permitted (code review round 2, the ratified
        // historical-variant sanction): `group(up offset 5m)` still plans
        // to `PlanExpr::Aggregate { op: Group, expr: Selector(_), .. }`.
        let expr = parse("group(up offset 5m)").unwrap();
        let p = plan(&expr, params()).unwrap();
        match &p.root {
            PlanExpr::Aggregate { op, expr, .. } => {
                assert_eq!(*op, AggOp::Group);
                let PlanExpr::Selector(id) = expr.as_ref() else {
                    panic!("expected Selector, got {expr:?}")
                };
                assert_eq!(p.selectors[*id].offset_ms, 300_000);
                assert!(p.selectors[*id].range_ms.is_none());
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    // --- code review round 3: nested parens must not break the `group`
    // guard (a regression the round-2 fix introduced) ---

    #[test]
    fn group_over_a_paren_wrapped_bare_instant_selector_is_permitted() {
        let expr = parse("group((up))").unwrap();
        let p = plan(&expr, params()).unwrap();
        match &p.root {
            PlanExpr::Aggregate { op, expr, .. } => {
                assert_eq!(*op, AggOp::Group);
                assert!(matches!(**expr, PlanExpr::Selector(_)));
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn group_over_a_doubly_paren_wrapped_selector_with_offset_is_permitted() {
        let expr = parse("group(((up offset 5m)))").unwrap();
        let p = plan(&expr, params()).unwrap();
        match &p.root {
            PlanExpr::Aggregate { op, expr, .. } => {
                assert_eq!(*op, AggOp::Group);
                let PlanExpr::Selector(id) = expr.as_ref() else {
                    panic!("expected Selector, got {expr:?}")
                };
                assert_eq!(p.selectors[*id].offset_ms, 300_000);
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn group_over_a_paren_wrapped_range_vector_is_still_unsupported() {
        // The vendored parser's own type checker sees through parens too
        // (`group((up[5m]))` fails to parse at all, same as
        // `group(up[5m])`) — asserted here so this stays honest about
        // which layer actually rejects it; the hand-constructed-AST test
        // below exercises `plan_aggregate`'s own paren-unwrapping check
        // directly, bypassing that upstream rejection.
        let err = parse("group((up[5m]))").unwrap_err();
        assert!(err.to_string().contains("matrix"));
    }

    #[test]
    fn group_over_a_paren_wrapped_range_vector_is_unsupported_when_hand_constructed() {
        // Defense-in-depth (mirrors
        // `group_over_a_range_vector_body_is_unsupported_when_hand_constructed`):
        // directly exercises `unwrap_parens` inside `plan_aggregate`'s
        // `group` guard by hand-building `group((up[5m]))`'s AST, bypassing
        // `parse()`'s own (also-correct) rejection.
        let matrix = parser::Expr::MatrixSelector(parser::MatrixSelector {
            vs: parser::VectorSelector::from("up"),
            range: std::time::Duration::from_secs(300),
        });
        let paren_wrapped = parser::Expr::Paren(parser::ParenExpr {
            expr: Box::new(matrix),
        });
        let group_expr = parser::Expr::Aggregate(parser::AggregateExpr {
            op: token::TokenType::new(token::T_GROUP),
            expr: Box::new(paren_wrapped),
            param: None,
            modifier: None,
        });
        let err = plan(&group_expr, params()).unwrap_err();
        match err {
            PromqlError::Unsupported { construct } => assert!(construct.contains("group")),
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }
}
