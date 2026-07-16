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
    /// Mirrors upstream Prometheus's
    /// `--enable-feature=promql-experimental-functions` (issue #65 —
    /// the first consumer of `ReaderConfig::promql_experimental_functions`,
    /// per the #64 Q2 adjudication): when `false`, planning an
    /// experimental function (`max_of`/`min_of`) is rejected by name as
    /// [`PromqlError::Unsupported`]; when `true`, implemented
    /// experimental functions plan normally.
    pub experimental_functions: bool,
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

/// Parameterless range-window functions — every discriminant maps one
/// windowed sample slice to at most one value (`&[Sample] -> Option<f64>`,
/// [`crate::eval::functions::eval_over_time`]). The M2 five are the
/// original `*_over_time` aggregations; issue #67 (M6-04) adds the rest of
/// the range-vector surface, all sharing the exact same fetch/window
/// machinery (zero read-path change — the fetch SQL for `deriv(m[5m])` is
/// byte-identical to `sum_over_time(m[5m])`'s, pinned by
/// `tests::m6_04_range_fns_keep_the_selector_set_byte_identical`).
/// `First`/`Mad`/`TsOf*` are experimental (registry `experimental: true`)
/// — [`plan_call`] rejects them unless
/// [`PlanParams::experimental_functions`] is set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverTimeFn {
    // M2.
    Avg,
    Min,
    Max,
    Sum,
    Count,
    // Issue #67 (M6-04), non-experimental.
    Stddev,
    Stdvar,
    Last,
    Present,
    Idelta,
    Resets,
    Changes,
    Deriv,
    // Issue #67 (M6-04), experimental.
    First,
    Mad,
    TsOfMin,
    TsOfMax,
    TsOfFirst,
    TsOfLast,
}

impl OverTimeFn {
    /// The registry `experimental: true` subset of the range-window
    /// surface (`registry-v3.13.json`) — gated behind
    /// [`PlanParams::experimental_functions`] in [`plan_call`].
    fn is_experimental(self) -> bool {
        matches!(
            self,
            OverTimeFn::First
                | OverTimeFn::Mad
                | OverTimeFn::TsOfMin
                | OverTimeFn::TsOfMax
                | OverTimeFn::TsOfFirst
                | OverTimeFn::TsOfLast
        )
    }
}

/// Range-window functions taking scalar parameter(s) alongside the matrix
/// selector (issue #67, M6-04): `quantile_over_time(φ, m[r])`,
/// `predict_linear(m[r], t)`, `double_exponential_smoothing(m[r], sf, tf)`
/// (the last experimental, gated like [`OverTimeFn::is_experimental`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverTimeParamFn {
    Quantile,
    PredictLinear,
    DoubleExpSmoothing,
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

/// Elementwise vector→vector math/trig functions (issue #65, M6-02):
/// pure post-fetch transforms — every discriminant maps one input sample
/// to one output value, so the wrapped expression's selector set (and
/// therefore its fetch SQL) is byte-identical to the unwrapped one's.
/// The 23 unary discriminants take no scalar arguments; `Clamp` takes
/// two (`min`, `max`), `ClampMin`/`ClampMax` one, and `Round` one
/// (`to_nearest`, defaulted to `1` by the planner when omitted).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MathFn {
    Abs,
    Ceil,
    Floor,
    Sqrt,
    Sgn,
    Deg,
    Rad,
    Exp,
    Ln,
    Log2,
    Log10,
    Sin,
    Cos,
    Tan,
    Asin,
    Acos,
    Atan,
    Sinh,
    Cosh,
    Tanh,
    Asinh,
    Acosh,
    Atanh,
    Clamp,
    ClampMin,
    ClampMax,
    Round,
}

/// Scalar→scalar functions (issue #65, M6-02). `MaxOf`/`MinOf` are
/// experimental (registry `experimental: true`) — [`plan_call`] rejects
/// them unless [`PlanParams::experimental_functions`] is set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFn {
    Pi,
    MaxOf,
    MinOf,
}

/// The eight date/time-field functions (issue #66, M6-03): each maps a
/// unix-seconds instant to one UTC calendar/clock field, computed by the
/// pure integer civil calendar in [`crate::eval::datetime`]. All are
/// pure post-fetch transforms — the optional vector argument's selector
/// set (and therefore its fetch SQL) is byte-identical to the bare
/// expression's, and the no-argument form emits no selector at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DateFn {
    Year,
    Month,
    DayOfMonth,
    DayOfWeek,
    DayOfYear,
    DaysInMonth,
    Hour,
    Minute,
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
    /// Issue #67 (M6-04): a parameterized range-window function. `args`
    /// carries the scalar parameter expression(s) in registry order —
    /// exactly one for `Quantile` (φ) and `PredictLinear` (t seconds),
    /// two for `DoubleExpSmoothing` (sf, tf); planner-enforced arity, the
    /// evaluator re-checks structurally (a descriptive error, never a
    /// panic — the `MathFn` pattern).
    OverTimeParam {
        func: OverTimeParamFn,
        selector: SelectorId,
        args: Vec<Box<PlanExpr>>,
    },
    /// Issue #67 (M6-04): `absent_over_time(m[r])` — emits one synthetic
    /// series (value `1`, labels ported from upstream
    /// `createLabelsForAbsentFunction`, see the evaluator arm) iff every
    /// matched series' window is empty at the step; an empty vector
    /// otherwise.
    AbsentOverTime {
        selector: SelectorId,
    },
    /// Issue #68 (M6-05): `absent(v)` — the instant-vector counterpart of
    /// [`PlanExpr::AbsentOverTime`]. When the (paren-stripped) argument is
    /// a bare vector selector, `selector` carries its id so the evaluator
    /// can synthesize labels from the matchers (the shared
    /// `createLabelsForAbsentFunction` walk); any computed argument plans
    /// normally with `selector: None` (empty synthetic label set).
    Absent {
        arg: Box<PlanExpr>,
        selector: Option<SelectorId>,
    },
    /// Issue #68 (M6-05): `sort(v)`/`sort_desc(v)` — a pure pass-through
    /// reorder (value order, NaN last in both directions). Ordering is
    /// observable for instant queries only.
    Sort {
        descending: bool,
        arg: Box<PlanExpr>,
    },
    /// Issue #68 (M6-05, experimental): `sort_by_label(v, names…)`/
    /// `sort_by_label_desc(v, names…)` — natural (numeric-aware) label
    /// collation in argument order, full-label-set tie-break.
    SortByLabel {
        descending: bool,
        labels: Vec<String>,
        arg: Box<PlanExpr>,
    },
    /// Issue #68 (M6-05): `label_replace(v, dst, replacement, src,
    /// regex)`. `regex` is validated at plan time (compiled with
    /// upstream's exact `^(?s:…)$` dot-all anchoring) and recompiled per
    /// evaluation step; `dst` name validity is checked at plan time too
    /// (both mirror upstream funcLabelReplace's before-the-loop checks,
    /// so they error even over an empty selection).
    LabelReplace {
        arg: Box<PlanExpr>,
        dst: String,
        replacement: String,
        src: String,
        regex: String,
    },
    /// Issue #68 (M6-05): `label_join(v, dst, separator, src…)`. `dst`
    /// and every `src` name are validated at plan time (upstream
    /// funcLabelJoin's own order).
    LabelJoin {
        arg: Box<PlanExpr>,
        dst: String,
        separator: String,
        src_labels: Vec<String>,
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
    /// Issue #65 (M6-02): an elementwise math/trig function over a vector
    /// expression. `scalar_args` carries exactly the argument count
    /// [`MathFn`]'s doc pins per discriminant (planner-enforced arity —
    /// the evaluator re-checks structurally and returns
    /// [`PromqlError::Unsupported`] on the impossible mismatch, never
    /// panics).
    MathFn {
        func: MathFn,
        arg: Box<PlanExpr>,
        scalar_args: Vec<Box<PlanExpr>>,
    },
    /// Issue #65 (M6-02): a scalar→scalar function. `args` is empty for
    /// `Pi`, two scalar expressions for `MaxOf`/`MinOf`.
    ScalarFn {
        func: ScalarFn,
        args: Vec<Box<PlanExpr>>,
    },
    /// Issue #66 (M6-03): `time()` — the evaluation step time as a scalar
    /// (`t_ms / 1000` seconds, varying per step in a range query). Emits
    /// no selector.
    Time,
    /// Issue #66 (M6-03): one of the eight date/time-field functions.
    /// `arg: None` is the upstream no-argument default (the field of the
    /// evaluation step time — `vector(time())` semantics); `Some` applies
    /// the field per element, reading each element's **value** as unix
    /// seconds.
    DateFn {
        func: DateFn,
        arg: Option<Box<PlanExpr>>,
    },
    /// Issue #66 (M6-03): `timestamp(v)`. Prometheus special-cases a
    /// (paren-stripped) **bare vector selector** argument to return each
    /// series' real sample timestamp — `bare_selector` carries that
    /// selector's id, and the evaluator reads
    /// `staleness::instant_value(...).t_ms` directly instead of the step
    /// time. Every computed argument (`timestamp(m+0)`,
    /// `timestamp(abs(m))`, nested `timestamp(timestamp(m))`, ...) takes
    /// the `None` branch: each output element carries the evaluation
    /// step time (upstream `at_modifier.test`'s own contrast).
    Timestamp {
        arg: Box<PlanExpr>,
        bare_selector: Option<SelectorId>,
    },
    /// Issue #66 (M6-03): `scalar(v)` — the single element's value when
    /// the vector has exactly one element, `NaN` otherwise (including
    /// empty).
    ScalarOf {
        arg: Box<PlanExpr>,
    },
    /// Issue #66 (M6-03): `vector(s)` — a one-element instant vector with
    /// the empty label set. Emits no selector of its own.
    VectorOf {
        arg: Box<PlanExpr>,
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
/// walking the AST. Does not need `params` itself beyond the
/// experimental-function gate — `PlanParams` otherwise only matters for
/// [`SelectorSpec::fetch_window`], computed later by the caller
/// (`pulsus-read`'s fetch layer), not during the walk.
struct Planner {
    selectors: Vec<SelectorSpec>,
    /// [`PlanParams::experimental_functions`], carried into
    /// [`plan_call`]'s `max_of`/`min_of` gate.
    experimental: bool,
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
        experimental: params.experimental_functions,
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
        // Issue #67 (M6-04): the rest of the parameterless range-window
        // surface — same shape, same fetch, pure post-fetch computation.
        "stddev_over_time" => Some(OverTimeFn::Stddev),
        "stdvar_over_time" => Some(OverTimeFn::Stdvar),
        "last_over_time" => Some(OverTimeFn::Last),
        "present_over_time" => Some(OverTimeFn::Present),
        "idelta" => Some(OverTimeFn::Idelta),
        "resets" => Some(OverTimeFn::Resets),
        "changes" => Some(OverTimeFn::Changes),
        "deriv" => Some(OverTimeFn::Deriv),
        "first_over_time" => Some(OverTimeFn::First),
        "mad_over_time" => Some(OverTimeFn::Mad),
        "ts_of_min_over_time" => Some(OverTimeFn::TsOfMin),
        "ts_of_max_over_time" => Some(OverTimeFn::TsOfMax),
        "ts_of_first_over_time" => Some(OverTimeFn::TsOfFirst),
        "ts_of_last_over_time" => Some(OverTimeFn::TsOfLast),
        _ => None,
    };
    if let Some(func) = over_time_fn {
        if func.is_experimental() && !planner.experimental {
            return Err(unsupported(format!(
                "experimental function {name}() (requires promql-experimental-functions)"
            )));
        }
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

    // Issue #67 (M6-04): `absent_over_time(m[r])` — the selector's own
    // variant (its output labels come from the *matchers*, not from any
    // fetched series). Upstream also accepts a subquery argument — that
    // form stays `Unsupported` here until M6-08 lands subqueries.
    if name == "absent_over_time" {
        let [arg] = args.as_slice() else {
            return Err(unsupported("absent_over_time() with != 1 argument"));
        };
        let Expr::MatrixSelector(ms) = arg.as_ref() else {
            return Err(unsupported(
                "absent_over_time() over an expression other than a bare range-vector selector",
            ));
        };
        let selector = plan_matrix_selector_id(planner, ms)?;
        return Ok(PlanExpr::AbsentOverTime { selector });
    }

    // Issue #67 (M6-04): parameterized range-window functions. Scalar
    // parameter sub-expressions plan via `plan_expr` (the
    // `histogram_quantile` quantile-arg shape), in source-argument order
    // so any selector a parameter expression contains keeps its id in
    // source order.
    if name == "quantile_over_time" {
        let [phi_arg, matrix_arg] = args.as_slice() else {
            return Err(unsupported("quantile_over_time() with != 2 arguments"));
        };
        let phi = plan_expr(planner, phi_arg)?;
        let Expr::MatrixSelector(ms) = matrix_arg.as_ref() else {
            return Err(unsupported(
                "quantile_over_time() over an expression other than a bare range-vector selector",
            ));
        };
        let selector = plan_matrix_selector_id(planner, ms)?;
        return Ok(PlanExpr::OverTimeParam {
            func: OverTimeParamFn::Quantile,
            selector,
            args: vec![Box::new(phi)],
        });
    }
    if name == "predict_linear" {
        let [matrix_arg, t_arg] = args.as_slice() else {
            return Err(unsupported("predict_linear() with != 2 arguments"));
        };
        let Expr::MatrixSelector(ms) = matrix_arg.as_ref() else {
            return Err(unsupported(
                "predict_linear() over an expression other than a bare range-vector selector",
            ));
        };
        let selector = plan_matrix_selector_id(planner, ms)?;
        let t = plan_expr(planner, t_arg)?;
        return Ok(PlanExpr::OverTimeParam {
            func: OverTimeParamFn::PredictLinear,
            selector,
            args: vec![Box::new(t)],
        });
    }
    if name == "double_exponential_smoothing" {
        if !planner.experimental {
            return Err(unsupported(format!(
                "experimental function {name}() (requires promql-experimental-functions)"
            )));
        }
        let [matrix_arg, sf_arg, tf_arg] = args.as_slice() else {
            return Err(unsupported(
                "double_exponential_smoothing() with != 3 arguments",
            ));
        };
        let Expr::MatrixSelector(ms) = matrix_arg.as_ref() else {
            return Err(unsupported(
                "double_exponential_smoothing() over an expression other than a bare \
                 range-vector selector",
            ));
        };
        let selector = plan_matrix_selector_id(planner, ms)?;
        let sf = plan_expr(planner, sf_arg)?;
        let tf = plan_expr(planner, tf_arg)?;
        return Ok(PlanExpr::OverTimeParam {
            func: OverTimeParamFn::DoubleExpSmoothing,
            selector,
            args: vec![Box::new(sf), Box::new(tf)],
        });
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

    // Issue #65 (M6-02): the 23 unary elementwise math/trig functions —
    // one vector argument, no scalar arguments.
    let unary_fn = match name {
        "abs" => Some(MathFn::Abs),
        "ceil" => Some(MathFn::Ceil),
        "floor" => Some(MathFn::Floor),
        "sqrt" => Some(MathFn::Sqrt),
        "sgn" => Some(MathFn::Sgn),
        "deg" => Some(MathFn::Deg),
        "rad" => Some(MathFn::Rad),
        "exp" => Some(MathFn::Exp),
        "ln" => Some(MathFn::Ln),
        "log2" => Some(MathFn::Log2),
        "log10" => Some(MathFn::Log10),
        "sin" => Some(MathFn::Sin),
        "cos" => Some(MathFn::Cos),
        "tan" => Some(MathFn::Tan),
        "asin" => Some(MathFn::Asin),
        "acos" => Some(MathFn::Acos),
        "atan" => Some(MathFn::Atan),
        "sinh" => Some(MathFn::Sinh),
        "cosh" => Some(MathFn::Cosh),
        "tanh" => Some(MathFn::Tanh),
        "asinh" => Some(MathFn::Asinh),
        "acosh" => Some(MathFn::Acosh),
        "atanh" => Some(MathFn::Atanh),
        _ => None,
    };
    if let Some(func) = unary_fn {
        let [arg] = args.as_slice() else {
            return Err(unsupported(format!("{name}() with != 1 argument")));
        };
        let arg = plan_expr(planner, arg)?;
        return Ok(PlanExpr::MathFn {
            func,
            arg: Box::new(arg),
            scalar_args: Vec::new(),
        });
    }

    // Issue #65 (M6-02): the clamp family — vector plus scalar bound(s).
    // Scalar sub-arguments plan via `plan_expr` (the same shape as
    // `histogram_quantile`'s quantile arg), forward-compatible with
    // `scalar()`/`time()` expressions in those positions.
    if name == "clamp" {
        let [vector_arg, min_arg, max_arg] = args.as_slice() else {
            return Err(unsupported("clamp() with != 3 arguments"));
        };
        let arg = plan_expr(planner, vector_arg)?;
        let min = plan_expr(planner, min_arg)?;
        let max = plan_expr(planner, max_arg)?;
        return Ok(PlanExpr::MathFn {
            func: MathFn::Clamp,
            arg: Box::new(arg),
            scalar_args: vec![Box::new(min), Box::new(max)],
        });
    }
    if let Some(func) = match name {
        "clamp_min" => Some(MathFn::ClampMin),
        "clamp_max" => Some(MathFn::ClampMax),
        _ => None,
    } {
        let [vector_arg, bound_arg] = args.as_slice() else {
            return Err(unsupported(format!("{name}() with != 2 arguments")));
        };
        let arg = plan_expr(planner, vector_arg)?;
        let bound = plan_expr(planner, bound_arg)?;
        return Ok(PlanExpr::MathFn {
            func,
            arg: Box::new(arg),
            scalar_args: vec![Box::new(bound)],
        });
    }

    // Issue #65 (M6-02): `round(v [, to_nearest])` — variadic with an
    // upstream default `to_nearest` of `1`, materialized here at plan
    // time so the evaluator always sees exactly one scalar argument.
    if name == "round" {
        let (vector_arg, to_nearest) = match args.as_slice() {
            [vector_arg] => (vector_arg, PlanExpr::Scalar(1.0)),
            [vector_arg, to_nearest_arg] => (vector_arg, plan_expr(planner, to_nearest_arg)?),
            _ => return Err(unsupported("round() with != 1..2 arguments")),
        };
        let arg = plan_expr(planner, vector_arg)?;
        return Ok(PlanExpr::MathFn {
            func: MathFn::Round,
            arg: Box::new(arg),
            scalar_args: vec![Box::new(to_nearest)],
        });
    }

    // Issue #65 (M6-02): scalar→scalar functions. `pi()` takes no
    // arguments; `max_of`/`min_of` are experimental and gated behind
    // `PlanParams::experimental_functions` (the #64 Q2 adjudication:
    // this is the flag's first consumer).
    if name == "pi" {
        if !args.is_empty() {
            return Err(unsupported("pi() with arguments"));
        }
        return Ok(PlanExpr::ScalarFn {
            func: ScalarFn::Pi,
            args: Vec::new(),
        });
    }
    if let Some(func) = match name {
        "max_of" => Some(ScalarFn::MaxOf),
        "min_of" => Some(ScalarFn::MinOf),
        _ => None,
    } {
        if !planner.experimental {
            return Err(unsupported(format!(
                "experimental function {name}() (requires promql-experimental-functions)"
            )));
        }
        let [a, b] = args.as_slice() else {
            return Err(unsupported(format!("{name}() with != 2 arguments")));
        };
        let a = plan_expr(planner, a)?;
        let b = plan_expr(planner, b)?;
        return Ok(PlanExpr::ScalarFn {
            func,
            args: vec![Box::new(a), Box::new(b)],
        });
    }

    // Issue #66 (M6-03): time/date functions + scalar/vector.
    if name == "time" {
        if !args.is_empty() {
            return Err(unsupported("time() with arguments"));
        }
        return Ok(PlanExpr::Time);
    }
    if let Some(func) = match name {
        "year" => Some(DateFn::Year),
        "month" => Some(DateFn::Month),
        "day_of_month" => Some(DateFn::DayOfMonth),
        "day_of_week" => Some(DateFn::DayOfWeek),
        "day_of_year" => Some(DateFn::DayOfYear),
        "days_in_month" => Some(DateFn::DaysInMonth),
        "hour" => Some(DateFn::Hour),
        "minute" => Some(DateFn::Minute),
        _ => None,
    } {
        // Registry arity: 0 or 1 argument (the upstream default is the
        // evaluation step time).
        let arg = match args.as_slice() {
            [] => None,
            [arg] => Some(Box::new(plan_expr(planner, arg)?)),
            _ => return Err(unsupported(format!("{name}() with > 1 argument"))),
        };
        return Ok(PlanExpr::DateFn { func, arg });
    }
    if name == "timestamp" {
        let [arg] = args.as_slice() else {
            return Err(unsupported("timestamp() with != 1 argument"));
        };
        // Prometheus strips parentheses before deciding whether the
        // argument is a bare selector (real-sample-time branch) — mirror
        // that with the same `unwrap_parens` the `group` guard uses.
        // Routing through `plan_vector_selector` keeps its existing `@`
        // reject (and metric-scoping rules) in force here too.
        return Ok(match unwrap_parens(arg) {
            Expr::VectorSelector(vs) => {
                let planned = plan_vector_selector(planner, vs)?;
                let PlanExpr::Selector(id) = planned else {
                    // `plan_vector_selector` only ever builds a Selector —
                    // kept total (a descriptive error, never a panic).
                    return Err(unsupported(
                        "timestamp() over an unexpected selector plan shape",
                    ));
                };
                PlanExpr::Timestamp {
                    arg: Box::new(PlanExpr::Selector(id)),
                    bare_selector: Some(id),
                }
            }
            other => PlanExpr::Timestamp {
                arg: Box::new(plan_expr(planner, other)?),
                bare_selector: None,
            },
        });
    }
    if name == "scalar" {
        let [arg] = args.as_slice() else {
            return Err(unsupported("scalar() with != 1 argument"));
        };
        let arg = plan_expr(planner, arg)?;
        return Ok(PlanExpr::ScalarOf { arg: Box::new(arg) });
    }
    if name == "vector" {
        let [arg] = args.as_slice() else {
            return Err(unsupported("vector() with != 1 argument"));
        };
        let arg = plan_expr(planner, arg)?;
        return Ok(PlanExpr::VectorOf { arg: Box::new(arg) });
    }

    // Issue #68 (M6-05): sort family. Pure pass-through reorders — no
    // string arguments for the value-sorting pair; the experimental
    // `sort_by_label*` pair takes 0+ label-name string literals (registry
    // variadic `-1`: `sort_by_label(m)` with no names is valid upstream —
    // the full-label-set fallback alone orders it).
    if let Some(descending) = match name {
        "sort" => Some(false),
        "sort_desc" => Some(true),
        _ => None,
    } {
        let [arg] = args.as_slice() else {
            return Err(unsupported(format!("{name}() with != 1 argument")));
        };
        let arg = plan_expr(planner, arg)?;
        return Ok(PlanExpr::Sort {
            descending,
            arg: Box::new(arg),
        });
    }
    if let Some(descending) = match name {
        "sort_by_label" => Some(false),
        "sort_by_label_desc" => Some(true),
        _ => None,
    } {
        if !planner.experimental {
            return Err(unsupported(format!(
                "experimental function {name}() (requires promql-experimental-functions)"
            )));
        }
        let Some((vector_arg, label_args)) = args.split_first() else {
            return Err(unsupported(format!("{name}() with no arguments")));
        };
        let arg = plan_expr(planner, vector_arg)?;
        let labels = label_args
            .iter()
            .map(|a| plan_string_arg(name, a))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(PlanExpr::SortByLabel {
            descending,
            labels,
            arg: Box::new(arg),
        });
    }

    // Issue #68 (M6-05): `label_replace`/`label_join`. String arguments
    // are pulled directly from the (paren-stripped) AST — never via
    // `plan_expr`, which rejects `Expr::StringLiteral` outright — and the
    // regex/label-name validity checks run at plan time, mirroring
    // upstream's before-the-loop checks (they error even over an empty
    // selection).
    if name == "label_replace" {
        let [vector_arg, dst_arg, replacement_arg, src_arg, regex_arg] = args.as_slice() else {
            return Err(unsupported("label_replace() with != 5 arguments"));
        };
        let arg = plan_expr(planner, vector_arg)?;
        let dst = plan_string_arg(name, dst_arg)?;
        let replacement = plan_string_arg(name, replacement_arg)?;
        let src = plan_string_arg(name, src_arg)?;
        let regex = plan_string_arg(name, regex_arg)?;
        // Plan v2 Δ1: upstream's exact `^(?s:regex)$` dot-all anchoring —
        // the same construction the eval arm recompiles per step.
        crate::eval::labels::compile_label_replace_regex(&regex)?;
        if !crate::eval::labels::is_valid_label_name(&dst) {
            return Err(PromqlError::LabelSet {
                detail: format!("invalid destination label name in label_replace(): {dst}"),
            });
        }
        return Ok(PlanExpr::LabelReplace {
            arg: Box::new(arg),
            dst,
            replacement,
            src,
            regex,
        });
    }
    if name == "label_join" {
        // Registry arity: vector, dst, separator, then 0+ src labels
        // (variadic `-1` — `label_join(m, "dst", ", ")` is valid and
        // joins to `""`, deleting dst).
        if args.len() < 3 {
            return Err(unsupported("label_join() with < 3 arguments"));
        }
        let arg = plan_expr(planner, &args[0])?;
        let dst = plan_string_arg(name, &args[1])?;
        let separator = plan_string_arg(name, &args[2])?;
        let mut src_labels = Vec::with_capacity(args.len() - 3);
        for a in &args[3..] {
            src_labels.push(plan_string_arg(name, a)?);
        }
        if !crate::eval::labels::is_valid_label_name(&dst) {
            return Err(PromqlError::LabelSet {
                detail: format!("invalid destination label name in label_join(): {dst}"),
            });
        }
        for src in &src_labels {
            if !crate::eval::labels::is_valid_label_name(src) {
                return Err(PromqlError::LabelSet {
                    detail: format!("invalid source label name in label_join(): {src}"),
                });
            }
        }
        return Ok(PlanExpr::LabelJoin {
            arg: Box::new(arg),
            dst,
            separator,
            src_labels,
        });
    }

    // Issue #68 (M6-05): `absent(v)`. A bare (paren-stripped) vector-
    // selector argument records its selector id so the evaluator can
    // synthesize labels from the matchers (the `timestamp()` special-case
    // shape); every computed argument plans normally with no selector
    // (empty synthetic label set).
    if name == "absent" {
        let [arg] = args.as_slice() else {
            return Err(unsupported("absent() with != 1 argument"));
        };
        return Ok(match unwrap_parens(arg) {
            Expr::VectorSelector(vs) => {
                let planned = plan_vector_selector(planner, vs)?;
                let PlanExpr::Selector(id) = planned else {
                    // `plan_vector_selector` only ever builds a Selector —
                    // kept total (a descriptive error, never a panic).
                    return Err(unsupported(
                        "absent() over an unexpected selector plan shape",
                    ));
                };
                PlanExpr::Absent {
                    arg: Box::new(PlanExpr::Selector(id)),
                    selector: Some(id),
                }
            }
            other => PlanExpr::Absent {
                arg: Box::new(plan_expr(planner, other)?),
                selector: None,
            },
        });
    }

    Err(unsupported(format!("function {name}()")))
}

/// Extracts a string-literal argument for the label/sort functions
/// (issue #68): parentheses are stripped first — the vendored
/// `label_replace((((testmetric))), (("dst")), …)` case requires it —
/// then the literal's value is taken **directly from the AST**
/// (`plan_expr` rejects `Expr::StringLiteral` outright, so string
/// arguments never route through it).
fn plan_string_arg(func: &str, expr: &Expr) -> Result<String, PromqlError> {
    match unwrap_parens(expr) {
        Expr::StringLiteral(s) => Ok(s.val.clone()),
        _ => Err(unsupported(format!(
            "{func}() with a non-string-literal string argument"
        ))),
    }
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
            experimental_functions: false,
        }
    }

    fn params_experimental() -> PlanParams {
        PlanParams {
            experimental_functions: true,
            ..params()
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
    fn a_function_outside_the_implemented_list_is_unsupported() {
        // `histogram_count` is scheduled for the native-histogram issue
        // (#22) — a stand-in for "any function the planner does not yet
        // map" (issue #65 moved the previous stand-in, `abs`, into the
        // implemented set; issue #68 moved its successor, `sort`).
        let expr = parse("histogram_count(up)").unwrap();
        let err = plan(&expr, params()).unwrap_err();
        match err {
            PromqlError::Unsupported { construct } => {
                assert!(construct.contains("histogram_count"));
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    // --- issue #65 (M6-02): elementwise math/trig + scalar functions ---

    #[test]
    fn plans_a_unary_math_fn_over_a_selector() {
        let expr = parse("abs(up)").unwrap();
        let p = plan(&expr, params()).unwrap();
        match &p.root {
            PlanExpr::MathFn {
                func,
                arg,
                scalar_args,
            } => {
                assert_eq!(*func, MathFn::Abs);
                assert_eq!(**arg, PlanExpr::Selector(0));
                assert!(scalar_args.is_empty());
            }
            other => panic!("expected MathFn, got {other:?}"),
        }
    }

    #[test]
    fn a_unary_math_fn_keeps_the_wrapped_selector_set_byte_identical() {
        // Perf Tier-1 gate (issue #65 plan; standing query-performance
        // mandate): `abs(m)`'s selector set — the input the fetch SQL is
        // generated from — is identical to `m`'s, proving the function
        // adds no fetch work, no extra round trip, and no SQL change.
        let wrapped = plan(
            &parse(r#"abs(mem_usage_bytes{service="svc-1"})"#).unwrap(),
            params(),
        )
        .unwrap();
        let bare = plan(
            &parse(r#"mem_usage_bytes{service="svc-1"}"#).unwrap(),
            params(),
        )
        .unwrap();
        assert_eq!(wrapped.selectors, bare.selectors);
    }

    #[test]
    fn plans_clamp_with_two_scalar_args() {
        let expr = parse("clamp(up, 0, 10)").unwrap();
        let p = plan(&expr, params()).unwrap();
        match &p.root {
            PlanExpr::MathFn {
                func,
                arg,
                scalar_args,
            } => {
                assert_eq!(*func, MathFn::Clamp);
                assert_eq!(**arg, PlanExpr::Selector(0));
                assert_eq!(scalar_args.len(), 2);
                assert_eq!(*scalar_args[0], PlanExpr::Scalar(0.0));
                assert_eq!(*scalar_args[1], PlanExpr::Scalar(10.0));
            }
            other => panic!("expected MathFn, got {other:?}"),
        }
    }

    #[test]
    fn plans_clamp_min_and_clamp_max_with_one_scalar_arg() {
        for (query, want) in [
            ("clamp_min(up, 0)", MathFn::ClampMin),
            ("clamp_max(up, 10)", MathFn::ClampMax),
        ] {
            let expr = parse(query).unwrap();
            let p = plan(&expr, params()).unwrap();
            match &p.root {
                PlanExpr::MathFn {
                    func, scalar_args, ..
                } => {
                    assert_eq!(*func, want, "{query}");
                    assert_eq!(scalar_args.len(), 1, "{query}");
                }
                other => panic!("{query}: expected MathFn, got {other:?}"),
            }
        }
    }

    #[test]
    fn round_defaults_its_to_nearest_argument_to_one() {
        let expr = parse("round(up)").unwrap();
        let p = plan(&expr, params()).unwrap();
        match &p.root {
            PlanExpr::MathFn {
                func, scalar_args, ..
            } => {
                assert_eq!(*func, MathFn::Round);
                assert_eq!(scalar_args.len(), 1);
                assert_eq!(*scalar_args[0], PlanExpr::Scalar(1.0));
            }
            other => panic!("expected MathFn, got {other:?}"),
        }
    }

    #[test]
    fn round_plans_an_explicit_to_nearest_argument() {
        let expr = parse("round(up, 0.5)").unwrap();
        let p = plan(&expr, params()).unwrap();
        match &p.root {
            PlanExpr::MathFn { scalar_args, .. } => {
                assert_eq!(*scalar_args[0], PlanExpr::Scalar(0.5));
            }
            other => panic!("expected MathFn, got {other:?}"),
        }
    }

    #[test]
    fn plans_pi_as_a_scalar_fn() {
        let expr = parse("pi()").unwrap();
        let p = plan(&expr, params()).unwrap();
        assert_eq!(
            p.root,
            PlanExpr::ScalarFn {
                func: ScalarFn::Pi,
                args: Vec::new(),
            }
        );
    }

    #[test]
    fn max_of_and_min_of_are_unsupported_without_the_experimental_flag() {
        for query in ["max_of(1, 2)", "min_of(1, 2)"] {
            let expr = parse(query).unwrap();
            let err = plan(&expr, params()).unwrap_err();
            match err {
                PromqlError::Unsupported { construct } => assert!(
                    construct.contains("experimental")
                        && construct.contains("promql-experimental-functions"),
                    "{query}: error must name the gate, got {construct:?}"
                ),
                other => panic!("{query}: expected Unsupported, got {other:?}"),
            }
        }
    }

    #[test]
    fn max_of_and_min_of_plan_with_the_experimental_flag() {
        for (query, want) in [
            ("max_of(1, 2)", ScalarFn::MaxOf),
            ("min_of(1, 2)", ScalarFn::MinOf),
        ] {
            let expr = parse(query).unwrap();
            let p = plan(&expr, params_experimental()).unwrap();
            match &p.root {
                PlanExpr::ScalarFn { func, args } => {
                    assert_eq!(*func, want, "{query}");
                    assert_eq!(args.len(), 2, "{query}");
                    assert_eq!(*args[0], PlanExpr::Scalar(1.0));
                    assert_eq!(*args[1], PlanExpr::Scalar(2.0));
                }
                other => panic!("{query}: expected ScalarFn, got {other:?}"),
            }
        }
    }

    #[test]
    fn non_experimental_math_fns_plan_without_the_experimental_flag() {
        // The gate applies only to the experimental pair — the rest of the
        // M6-02 surface plans regardless of the flag state.
        for query in ["abs(up)", "clamp(up, 0, 1)", "round(up)", "pi()"] {
            let expr = parse(query).unwrap();
            assert!(plan(&expr, params()).is_ok(), "{query}");
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
            experimental_functions: false,
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

    // --- issue #66 (M6-03): time/date functions + scalar/vector ---

    #[test]
    fn plans_time_as_a_zero_selector_scalar_node() {
        let p = plan(&parse("time()").unwrap(), params()).unwrap();
        assert_eq!(p.root, PlanExpr::Time);
        assert!(p.selectors.is_empty(), "time() must emit no selector");
    }

    #[test]
    fn time_with_arguments_is_unsupported() {
        // The vendored parser's arity check already rejects `time(m)`;
        // hand-construct the call to exercise plan_call's own guard.
        let expr = parse("time()").unwrap();
        let Expr::Call(call) = &expr else {
            panic!("expected Call")
        };
        let mut call = call.clone();
        call.args.args.push(Box::new(parse("m").unwrap()));
        let err = plan(&Expr::Call(call), params()).unwrap_err();
        assert!(matches!(err, PromqlError::Unsupported { .. }));
    }

    #[test]
    fn plans_every_date_fn_over_a_vector_argument() {
        for (query, want) in [
            ("year(m)", DateFn::Year),
            ("month(m)", DateFn::Month),
            ("day_of_month(m)", DateFn::DayOfMonth),
            ("day_of_week(m)", DateFn::DayOfWeek),
            ("day_of_year(m)", DateFn::DayOfYear),
            ("days_in_month(m)", DateFn::DaysInMonth),
            ("hour(m)", DateFn::Hour),
            ("minute(m)", DateFn::Minute),
        ] {
            let p = plan(&parse(query).unwrap(), params()).unwrap();
            match &p.root {
                PlanExpr::DateFn { func, arg } => {
                    assert_eq!(*func, want, "{query}");
                    assert_eq!(arg.as_deref(), Some(&PlanExpr::Selector(0)), "{query}");
                }
                other => panic!("{query}: expected DateFn, got {other:?}"),
            }
        }
    }

    #[test]
    fn plans_a_no_argument_date_fn_with_zero_selectors() {
        let p = plan(&parse("month()").unwrap(), params()).unwrap();
        assert_eq!(
            p.root,
            PlanExpr::DateFn {
                func: DateFn::Month,
                arg: None,
            }
        );
        assert!(p.selectors.is_empty(), "month() must emit no selector");
    }

    #[test]
    fn plans_timestamp_over_a_bare_selector_with_the_special_branch() {
        let p = plan(&parse("timestamp(m)").unwrap(), params()).unwrap();
        match &p.root {
            PlanExpr::Timestamp { arg, bare_selector } => {
                assert_eq!(**arg, PlanExpr::Selector(0));
                assert_eq!(*bare_selector, Some(0));
            }
            other => panic!("expected Timestamp, got {other:?}"),
        }
    }

    #[test]
    fn timestamp_strips_parentheses_before_detecting_a_bare_selector() {
        // Prometheus paren-strips the argument, so `timestamp(((m)))`
        // takes the real-sample-time branch exactly like `timestamp(m)`.
        let p = plan(&parse("timestamp(((m)))").unwrap(), params()).unwrap();
        match &p.root {
            PlanExpr::Timestamp { bare_selector, .. } => assert_eq!(*bare_selector, Some(0)),
            other => panic!("expected Timestamp, got {other:?}"),
        }
    }

    #[test]
    fn timestamp_over_a_computed_argument_takes_the_eval_time_branch() {
        for query in [
            "timestamp(m + 0)",
            "timestamp(abs(m))",
            "timestamp(rate(m[1m]))",
        ] {
            let p = plan(&parse(query).unwrap(), params()).unwrap();
            match &p.root {
                PlanExpr::Timestamp { bare_selector, .. } => {
                    assert_eq!(*bare_selector, None, "{query}");
                }
                other => panic!("{query}: expected Timestamp, got {other:?}"),
            }
        }
    }

    #[test]
    fn timestamp_over_a_selector_with_at_modifier_is_unsupported() {
        // The bare-selector branch routes through plan_vector_selector,
        // whose existing `@` reject stays in force.
        let err = plan(&parse("timestamp(m @ 100)").unwrap(), params()).unwrap_err();
        assert!(matches!(err, PromqlError::Unsupported { .. }));
    }

    #[test]
    fn plans_scalar_and_vector_wrappers() {
        let p = plan(&parse("scalar(m)").unwrap(), params()).unwrap();
        match &p.root {
            PlanExpr::ScalarOf { arg } => assert_eq!(**arg, PlanExpr::Selector(0)),
            other => panic!("expected ScalarOf, got {other:?}"),
        }
        let p = plan(&parse("vector(1)").unwrap(), params()).unwrap();
        match &p.root {
            PlanExpr::VectorOf { arg } => assert_eq!(**arg, PlanExpr::Scalar(1.0)),
            other => panic!("expected VectorOf, got {other:?}"),
        }
        assert!(p.selectors.is_empty(), "vector(1) must emit no selector");
    }

    // --- issue #67 (M6-04): range-vector function completion ---

    #[test]
    fn plans_every_new_parameterless_range_fn_to_its_over_time_discriminant() {
        for (query, want) in [
            ("stddev_over_time(m[5m])", OverTimeFn::Stddev),
            ("stdvar_over_time(m[5m])", OverTimeFn::Stdvar),
            ("last_over_time(m[5m])", OverTimeFn::Last),
            ("present_over_time(m[5m])", OverTimeFn::Present),
            ("idelta(m[5m])", OverTimeFn::Idelta),
            ("resets(m[5m])", OverTimeFn::Resets),
            ("changes(m[5m])", OverTimeFn::Changes),
            ("deriv(m[5m])", OverTimeFn::Deriv),
            // Experimental subset — planned under the flag.
            ("first_over_time(m[5m])", OverTimeFn::First),
            ("mad_over_time(m[5m])", OverTimeFn::Mad),
            ("ts_of_min_over_time(m[5m])", OverTimeFn::TsOfMin),
            ("ts_of_max_over_time(m[5m])", OverTimeFn::TsOfMax),
            ("ts_of_first_over_time(m[5m])", OverTimeFn::TsOfFirst),
            ("ts_of_last_over_time(m[5m])", OverTimeFn::TsOfLast),
        ] {
            let p = plan(&parse(query).unwrap(), params_experimental()).unwrap();
            match &p.root {
                PlanExpr::OverTime { func, selector } => {
                    assert_eq!(*func, want, "{query}");
                    assert_eq!(p.selectors[*selector].range_ms, Some(300_000), "{query}");
                }
                other => panic!("{query}: expected OverTime, got {other:?}"),
            }
        }
    }

    /// AC2: each of the 7 experimental names is a named `Unsupported`
    /// without the flag (same gate wording as `max_of`/`min_of`) and
    /// plans with it.
    #[test]
    fn m6_04_experimental_fns_are_gated_behind_the_experimental_flag() {
        for query in [
            "first_over_time(m[5m])",
            "mad_over_time(m[5m])",
            "ts_of_min_over_time(m[5m])",
            "ts_of_max_over_time(m[5m])",
            "ts_of_first_over_time(m[5m])",
            "ts_of_last_over_time(m[5m])",
            "double_exponential_smoothing(m[5m], 0.5, 0.5)",
        ] {
            let err = plan(&parse(query).unwrap(), params()).unwrap_err();
            match err {
                PromqlError::Unsupported { construct } => assert!(
                    construct.contains("experimental")
                        && construct.contains("promql-experimental-functions"),
                    "{query}: error must name the gate, got {construct:?}"
                ),
                other => panic!("{query}: expected Unsupported, got {other:?}"),
            }
            assert!(
                plan(&parse(query).unwrap(), params_experimental()).is_ok(),
                "{query} must plan with the flag on"
            );
        }
    }

    #[test]
    fn m6_04_non_experimental_fns_plan_without_the_experimental_flag() {
        for query in [
            "stddev_over_time(m[5m])",
            "last_over_time(m[5m])",
            "idelta(m[5m])",
            "deriv(m[5m])",
            "absent_over_time(m[5m])",
            "quantile_over_time(0.5, m[5m])",
            "predict_linear(m[5m], 60)",
        ] {
            assert!(plan(&parse(query).unwrap(), params()).is_ok(), "{query}");
        }
    }

    #[test]
    fn plans_absent_over_time_to_its_own_variant() {
        let p = plan(
            &parse(r#"absent_over_time(m{job="api"}[5m])"#).unwrap(),
            params(),
        )
        .unwrap();
        match &p.root {
            PlanExpr::AbsentOverTime { selector } => {
                assert_eq!(p.selectors[*selector].metric_name, "m");
                assert_eq!(p.selectors[*selector].range_ms, Some(300_000));
                assert_eq!(p.selectors[*selector].matchers.len(), 1);
            }
            other => panic!("expected AbsentOverTime, got {other:?}"),
        }
    }

    #[test]
    fn plans_quantile_over_time_with_phi_before_the_selector() {
        let p = plan(&parse("quantile_over_time(0.9, m[5m])").unwrap(), params()).unwrap();
        match &p.root {
            PlanExpr::OverTimeParam {
                func,
                selector,
                args,
            } => {
                assert_eq!(*func, OverTimeParamFn::Quantile);
                assert_eq!(p.selectors[*selector].range_ms, Some(300_000));
                assert_eq!(args.len(), 1);
                assert_eq!(*args[0], PlanExpr::Scalar(0.9));
            }
            other => panic!("expected OverTimeParam, got {other:?}"),
        }
    }

    #[test]
    fn plans_predict_linear_and_double_exponential_smoothing_args() {
        let p = plan(&parse("predict_linear(m[5m], 3600)").unwrap(), params()).unwrap();
        match &p.root {
            PlanExpr::OverTimeParam { func, args, .. } => {
                assert_eq!(*func, OverTimeParamFn::PredictLinear);
                assert_eq!(args.len(), 1);
                assert_eq!(*args[0], PlanExpr::Scalar(3600.0));
            }
            other => panic!("expected OverTimeParam, got {other:?}"),
        }
        let p = plan(
            &parse("double_exponential_smoothing(m[5m], 0.4, 0.2)").unwrap(),
            params_experimental(),
        )
        .unwrap();
        match &p.root {
            PlanExpr::OverTimeParam { func, args, .. } => {
                assert_eq!(*func, OverTimeParamFn::DoubleExpSmoothing);
                assert_eq!(args.len(), 2);
                assert_eq!(*args[0], PlanExpr::Scalar(0.4));
                assert_eq!(*args[1], PlanExpr::Scalar(0.2));
            }
            other => panic!("expected OverTimeParam, got {other:?}"),
        }
    }

    #[test]
    fn m6_04_fns_over_non_bare_range_arguments_are_unsupported() {
        // Subquery arguments (upstream-legal) stay named-Unsupported until
        // M6-08 — the same rule as rate()'s.
        for query in [
            "deriv(sum(foo)[5m:1m])",
            "absent_over_time(rate(foo[5m])[5m:1m])",
            "quantile_over_time(0.5, sum(foo)[5m:1m])",
        ] {
            assert!(plan(&parse(query).unwrap(), params()).is_err(), "{query}");
        }
    }

    /// AC7 (Tier-1 perf gate; standing query-performance mandate): every
    /// M6-04 function is a pure post-fetch computation — its selector set
    /// (the input the fetch SQL is generated from) is **byte-identical**
    /// to `sum_over_time`'s over the same matrix selector, so the fetch
    /// SQL is byte-identical too (same `SelectorSpec` ⇒ same
    /// `fetch_window` ⇒ same query text). Zero new fetch work, zero new
    /// round trips; there is no metrics rollup/downsample read path to
    /// bypass (grep-verified in the plan: `pulsus-read/src/metrics/` has
    /// none). Scalar parameters add no selector.
    #[test]
    fn m6_04_range_fns_keep_the_selector_set_byte_identical() {
        let bare = plan(
            &parse(r#"sum_over_time(mem_usage_bytes{service="svc-1"}[5m])"#).unwrap(),
            params_experimental(),
        )
        .unwrap();
        for query in [
            r#"stddev_over_time(mem_usage_bytes{service="svc-1"}[5m])"#,
            r#"stdvar_over_time(mem_usage_bytes{service="svc-1"}[5m])"#,
            r#"last_over_time(mem_usage_bytes{service="svc-1"}[5m])"#,
            r#"first_over_time(mem_usage_bytes{service="svc-1"}[5m])"#,
            r#"present_over_time(mem_usage_bytes{service="svc-1"}[5m])"#,
            r#"absent_over_time(mem_usage_bytes{service="svc-1"}[5m])"#,
            r#"idelta(mem_usage_bytes{service="svc-1"}[5m])"#,
            r#"resets(mem_usage_bytes{service="svc-1"}[5m])"#,
            r#"changes(mem_usage_bytes{service="svc-1"}[5m])"#,
            r#"deriv(mem_usage_bytes{service="svc-1"}[5m])"#,
            r#"mad_over_time(mem_usage_bytes{service="svc-1"}[5m])"#,
            r#"ts_of_min_over_time(mem_usage_bytes{service="svc-1"}[5m])"#,
            r#"ts_of_max_over_time(mem_usage_bytes{service="svc-1"}[5m])"#,
            r#"ts_of_first_over_time(mem_usage_bytes{service="svc-1"}[5m])"#,
            r#"ts_of_last_over_time(mem_usage_bytes{service="svc-1"}[5m])"#,
            r#"quantile_over_time(0.9, mem_usage_bytes{service="svc-1"}[5m])"#,
            r#"predict_linear(mem_usage_bytes{service="svc-1"}[5m], 3600)"#,
            r#"double_exponential_smoothing(mem_usage_bytes{service="svc-1"}[5m], 0.5, 0.5)"#,
        ] {
            let p = plan(&parse(query).unwrap(), params_experimental()).unwrap();
            assert_eq!(p.selectors, bare.selectors, "{query}");
        }
    }

    /// Perf Tier-1 gate (issue #66 plan; standing query-performance
    /// mandate): the M6-03 wrappers add no fetch work — a wrapped
    /// expression's selector set (the input the fetch SQL is generated
    /// from) is byte-identical to the bare expression's, and the
    /// selector-free shapes emit no selector at all.
    #[test]
    fn m6_03_wrappers_keep_the_selector_set_byte_identical() {
        let bare = plan(
            &parse(r#"mem_usage_bytes{service="svc-1"}"#).unwrap(),
            params(),
        )
        .unwrap();
        for query in [
            r#"timestamp(mem_usage_bytes{service="svc-1"})"#,
            r#"scalar(mem_usage_bytes{service="svc-1"})"#,
            r#"month(mem_usage_bytes{service="svc-1"})"#,
            r#"timestamp(timestamp(mem_usage_bytes{service="svc-1"}))"#,
        ] {
            let wrapped = plan(&parse(query).unwrap(), params()).unwrap();
            assert_eq!(wrapped.selectors, bare.selectors, "{query}");
        }
        for query in ["time()", "month()", "vector(1)", "vector(time())"] {
            let p = plan(&parse(query).unwrap(), params()).unwrap();
            assert!(p.selectors.is_empty(), "{query} must emit no selector");
        }
    }

    // --- issue #68 (M6-05): label, sort & absence functions ---

    /// Perf Tier-1 gate (AC6; standing query-performance mandate): every
    /// M6-05 function is pure post-fetch — the wrapped expression's
    /// selector set (the input the fetch SQL — and therefore the
    /// `X-Pulsus-Explain` `sample_fetch` stage — is generated from) is
    /// byte-identical to the bare expression's, mirroring the
    /// #65/#66/#67 gates above.
    #[test]
    fn m6_05_label_sort_absence_fns_keep_the_selector_set_byte_identical() {
        let bare = plan(
            &parse(r#"mem_usage_bytes{service="svc-1"}"#).unwrap(),
            params_experimental(),
        )
        .unwrap();
        for query in [
            r#"sort(mem_usage_bytes{service="svc-1"})"#,
            r#"sort_desc(mem_usage_bytes{service="svc-1"})"#,
            r#"sort_by_label(mem_usage_bytes{service="svc-1"}, "service")"#,
            r#"sort_by_label_desc(mem_usage_bytes{service="svc-1"}, "service")"#,
            r#"label_replace(mem_usage_bytes{service="svc-1"}, "dst", "$1", "service", "(.*)")"#,
            r#"label_join(mem_usage_bytes{service="svc-1"}, "dst", "-", "service")"#,
            r#"absent(mem_usage_bytes{service="svc-1"})"#,
        ] {
            let wrapped = plan(&parse(query).unwrap(), params_experimental()).unwrap();
            assert_eq!(wrapped.selectors, bare.selectors, "{query}");
        }
    }

    /// `sort_by_label`/`sort_by_label_desc` are registry-experimental —
    /// rejected by name unless the gate is on (the `max_of`/`first_over_
    /// time` pattern), including the zero-label-argument form the
    /// coverage auto-probe uses.
    #[test]
    fn sort_by_label_is_gated_behind_experimental_functions() {
        for query in [
            r#"sort_by_label(up, "job")"#,
            r#"sort_by_label_desc(up, "job")"#,
            "sort_by_label(up)",
        ] {
            let expr = parse(query).unwrap();
            let err = plan(&expr, params()).unwrap_err();
            assert!(matches!(err, PromqlError::Unsupported { .. }), "{query}");
            let p = plan(&expr, params_experimental()).unwrap();
            match &p.root {
                PlanExpr::SortByLabel { .. } => {}
                other => panic!("{query}: expected SortByLabel, got {other:?}"),
            }
        }
    }

    /// String arguments are pulled from the paren-stripped AST — the
    /// vendored `label_replace((((testmetric))), (("dst")), …)` shape.
    #[test]
    fn label_replace_accepts_paren_wrapped_string_arguments() {
        let expr = parse(
            r#"label_replace((((testmetric))), (("dst")), (("value-$1")), (("src")), (("re")))"#,
        )
        .unwrap();
        let p = plan(&expr, params()).unwrap();
        match &p.root {
            PlanExpr::LabelReplace {
                dst,
                replacement,
                src,
                regex,
                ..
            } => {
                assert_eq!(dst, "dst");
                assert_eq!(replacement, "value-$1");
                assert_eq!(src, "src");
                assert_eq!(regex, "re");
            }
            other => panic!("expected LabelReplace, got {other:?}"),
        }
    }

    /// A non-string expression in a string position (and a short
    /// `label_join` argument list) never reaches the planner at all — the
    /// vendored parser's own type/arity check rejects it first
    /// (`plan_string_arg`'s `Unsupported` branch is defense-in-depth for
    /// a hand-built AST, kept total rather than relied upon).
    #[test]
    fn label_fns_non_string_or_short_argument_lists_are_parse_errors() {
        for query in [
            r#"label_replace(up, up, "r", "s", ".*")"#,
            r#"label_join(up, "d", 1)"#,
            r#"sort_by_label(up, up)"#,
            r#"label_join(up, "dst")"#,
        ] {
            let err = parse(query).unwrap_err();
            assert!(matches!(err, PromqlError::Parse(_)), "{query}: {err:?}");
        }
    }

    /// Plan-time validation (mirroring upstream's before-the-loop checks,
    /// so these error even over an empty selection): invalid regex under
    /// the `^(?s:…)$` anchor, and invalid (empty) destination/source
    /// label names, each with the exact upstream message.
    #[test]
    fn label_fns_plan_time_validation_errors_carry_the_upstream_messages() {
        for (query, want) in [
            (
                r#"label_replace(up, "dst", "v", "src", "(.*")"#,
                "invalid regular expression in label_replace(): (.*",
            ),
            (
                r#"label_replace(up, "", "v", "src", "(.*)")"#,
                "invalid destination label name in label_replace(): ",
            ),
            (
                r#"label_join(up, "", ",", "src")"#,
                "invalid destination label name in label_join(): ",
            ),
            (
                r#"label_join(up, "dst", ",", "")"#,
                "invalid source label name in label_join(): ",
            ),
        ] {
            let expr = parse(query).unwrap();
            let err = plan(&expr, params()).unwrap_err();
            assert!(matches!(err, PromqlError::LabelSet { .. }), "{query}");
            assert_eq!(err.to_string(), want, "{query}");
        }
    }

    /// A valid `(?s:…)`-anchored regex plans fine — the dot-all wrapper
    /// itself must not break compilation of ordinary patterns (including
    /// an already-flagged one).
    #[test]
    fn label_replace_accepts_ordinary_and_flagged_regexes_at_plan_time() {
        for query in [
            r#"label_replace(up, "dst", "$1", "src", "source-value-(.*)")"#,
            r#"label_replace(up, "dst", "${x}", "src", "(?P<x>.*)")"#,
            r#"label_replace(up, "dst", "$1", "src", "(?i)(A.B)")"#,
        ] {
            let expr = parse(query).unwrap();
            assert!(plan(&expr, params()).is_ok(), "{query}");
        }
    }

    /// `absent` over a bare (paren-stripped) selector records the
    /// selector id for label synthesis; any computed argument records
    /// `None`.
    #[test]
    fn absent_records_the_bare_selector_and_only_the_bare_selector() {
        for query in ["absent(up)", "absent(((up)))", r#"absent(up{job="x"})"#] {
            let expr = parse(query).unwrap();
            let p = plan(&expr, params()).unwrap();
            match &p.root {
                PlanExpr::Absent {
                    selector: Some(id), ..
                } => assert_eq!(*id, 0, "{query}"),
                other => panic!("{query}: expected Absent with a selector, got {other:?}"),
            }
        }
        for query in ["absent(sum(up))", "absent(up + up)", "absent(rate(up[5m]))"] {
            let expr = parse(query).unwrap();
            let p = plan(&expr, params()).unwrap();
            match &p.root {
                PlanExpr::Absent { selector: None, .. } => {}
                other => panic!("{query}: expected Absent without a selector, got {other:?}"),
            }
        }
    }
}
