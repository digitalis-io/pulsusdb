//! The shell planner of the TraceQL metrics endpoints (issue #59;
//! docs/api.md §4.4): pure `Query + MetricsParams + MetricsCtx →
//! TraceMetricsPlan`, deterministic, no I/O. It validates the metrics
//! shape (one spanset, one metric stage and at most one second stage, or
//! one `compare()`), snaps the window to epoch-aligned step boundaries,
//! builds the range axis, enforces the point cap, and resolves the
//! pipeline — kind, quantiles, ranking, trailing comparison, `compare()`'s
//! `topN` — and the exemplar budget. It reads no table: the metrics
//! statement ([`super::spans::metrics`]) renders the SQL. Every rejection
//! is a [`PlanError`]: `400 bad_data` server-side, except
//! [`PlanError::MetricsPointCap`] — the static pre-execution `422
//! query_too_broad`.

use pulsus_traceql::{MetricFn, PipelineStage, Query, SpansetExpr};

use super::sql_text::PlanError;

/// The hard bucket-count cap (docs/api.md §4.4): a snapped range
/// resolving more buckets is rejected statically with `422
/// query_too_broad` — bounded response, no silent truncation (the
/// adjudicated contract; deliberately 422, not Prometheus's 400).
pub const MAX_METRICS_POINTS: i64 = 11_000;

const NS_PER_S: i64 = 1_000_000_000;
const NS_PER_MS: i64 = 1_000_000;

/// The refusal of a cross-spanset or structural metrics query, the shell
/// planner's and the routed planner's one text.
pub(crate) const STRUCTURAL_METRICS: &str = "cross-spanset and structural expressions ({A} && \
                                              {B}, {A} > {B}) are not supported by metrics queries";

/// The caller-validated request window, step, exemplar budget and
/// retention floor. `step_ms` is whole milliseconds (issue #477 (d)),
/// already derived by the server when the request omitted `step`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsParams {
    pub start_ns: i64,
    pub end_ns: i64,
    /// Bucket width in whole milliseconds; `>= 1`.
    pub step_ms: i64,
    /// The HTTP `exemplars` parameter, normalised by the server:
    /// `None` when absent, empty, unparseable, zero or negative.
    pub exemplars: Option<u32>,
    /// The retention floor: the first nanosecond any statement may read a
    /// span from. `i64::MIN` floors nothing.
    pub floor_ns: i64,
}

/// The emitted bucket grid (issue #477 (a)/(b)): `points` labels at
/// `first_ms + i * step_ms`, label `L` covering the RIGHT-CLOSED instant
/// range `(L - step, L]`.
///
/// `first_ms` is the snapped window's own left edge `aS`, which is the
/// right edge of the extra LEADING bucket `(aS - step, aS]` — the range
/// window reads one whole step before `aS` precisely so that bucket has
/// data (measured against the reference). `points` is `intervals + 1`,
/// so a window of `n` steps emits `n + 1` labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeAxis {
    pub first_ms: i64,
    pub step_ms: i64,
    pub points: usize,
}

impl RangeAxis {
    /// The `i`-th label. Panics in debug on an out-of-range index; the
    /// only caller iterates `0..points`.
    pub fn label_ms(&self, i: usize) -> i64 {
        debug_assert!(
            i < self.points,
            "axis index {i} out of {} points",
            self.points
        );
        self.first_ms + (i as i64) * self.step_ms
    }

    /// The last label — `aE`, the snapped right edge.
    pub fn last_ms(&self) -> i64 {
        self.first_ms + (self.points as i64 - 1) * self.step_ms
    }

    /// The label whose right-closed bucket contains `ts_ms`:
    /// `ceil(ts_ms / step_ms) * step_ms`. An instant landing exactly on a
    /// grid point belongs to THAT point, which is what "right-closed"
    /// means and what the metrics statement's bucket renders.
    pub fn label_for_ms(&self, ts_ms: i64) -> i64 {
        let step = self.step_ms;
        let floor = ts_ms.div_euclid(step) * step;
        if floor == ts_ms { ts_ms } else { floor + step }
    }
}

/// Engine-derived planning context.
#[derive(Debug, Clone, Copy)]
pub struct MetricsCtx {
    /// `reader.traceql_scan_budget_rows` — carried for parity with the
    /// engine's Layer-1 settings (the engine injects it at execution).
    pub scan_budget_rows: u64,
    /// `reader.traceql_max_series` (issue #182) — the series cap the
    /// metrics statement throws past, which the engine turns into a `422`.
    pub max_series: u64,
    /// Clustered mode: the engine injects the §7 clustered-reader
    /// settings.
    pub distributed: bool,
    /// `PULSUS_SKIP_UNAVAILABLE_SHARDS` passthrough for the §7 settings.
    pub skip_unavailable_shards: bool,
}

/// The first-stage value-aggregation functions (`*_over_time`), issue
/// #182. `count`/`rate` are not here — they are [`PlanKind::Count`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFn {
    Sum,
    Min,
    Max,
    Avg,
}

/// A snapped, left-closed/right-open metrics window `[start_ns, end_ns)`
/// — produced by the planner's epoch snapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnappedWindow {
    pub start_ns: i64,
    pub end_ns: i64,
}

/// The read-side metric kind (issue #182): the `uniqExact` count path
/// (rate/count_over_time) or a first-stage value aggregation
/// (sum/min/max/avg over the physical `duration_ns`, scaled ns→seconds at
/// the encode boundary).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanKind {
    /// `rate()` (divides the deduped count by the window width) or
    /// `count_over_time()` (the count itself).
    Count { is_rate: bool },
    /// `sum/min/max/avg_over_time(duration)`.
    Agg(AggFn),
    /// `quantile_over_time(duration, q…)` — one series per quantile
    /// (`p=<q>` label); the quantile list is carried on the plan.
    Quantile,
    /// `histogram_over_time(duration)` — one PLAIN-COUNT series per
    /// power-of-two nanosecond bucket that actually occurred
    /// (`__bucket=<bucket seconds>` label), the reference's
    /// `Log2Bucketize` model (issue #252). There is no ladder and no
    /// cumulation; membership is data-dependent, bounded by the bit
    /// width of `Int64` — 63 buckets are reachable, and the gates use 64
    /// as the static ceiling.
    Histogram,
    /// `compare({selection})` — baseline/selection attribute meta-series
    /// (`__meta_type` + one attribute label). The cross-tab/totals SQL is
    /// carried on the plan.
    Compare,
}

/// The shell plan of one metrics request: its window, axis, kind and
/// pipeline, from which the metrics statement is rendered.
#[derive(Debug, Clone)]
pub struct TraceMetricsPlan {
    kind: PlanKind,
    /// The Tempo `__name__` label for an ungrouped series.
    metric_name: &'static str,
    /// The requested quantiles (`PlanKind::Quantile` only), in request
    /// order — one output series per entry (`p=<q>` label).
    quantiles: Vec<f64>,
    /// The optional second-stage `topk`/`bottomk` reduction, which the
    /// statement applies per timestamp.
    reduce: Option<SeriesReduce>,
    /// The resolved TOTAL exemplar budget for the whole response (issue
    /// #477 (c)): `0` means none. The statement keeps at most this many.
    exemplar_budget: u32,
    /// A trailing `metrics-result comparison` (`… > 5`, issue #182 P6b):
    /// the statement keeps only samples satisfying `<op> <value>`.
    result_filter: Option<(pulsus_traceql::ComparisonOp, f64)>,
    /// `compare()`'s `topN` (issue #460) — the per-key-per-side
    /// rank-and-keep the statement applies to its values. The AST
    /// value is validated `> 0`; a value above `usize::MAX` saturates,
    /// which trims nothing and is exactly what an enormous `topN` means.
    compare_top_n: usize,
    step_ms: i64,
    /// The snapped grid window `[aS, aE)`: its right edge labels the
    /// instant answer and the range axis starts at its left edge.
    window: SnappedWindow,
    /// The emitted bucket grid for the range form.
    range_axis: RangeAxis,
    /// The INSTANT window, exactly as requested: `[start, end)`, one
    /// bucket, not snapped (issue #601 part 2).
    instant_window: SnappedWindow,
    /// The rows the instant statements read: [`Self::instant_window`]
    /// from the retention floor on.
    instant_rows: SnappedWindow,
    /// The rows the range statements read: the grid's
    /// `[aS - step + 1, aE + 1)`, i.e. the instants `(aS - step, aE]`, from
    /// the retention floor on. The grid keeps its buckets; those before the
    /// floor read no span.
    range_rows: SnappedWindow,
    distributed: bool,
}

impl TraceMetricsPlan {
    pub fn kind(&self) -> PlanKind {
        self.kind
    }

    /// The `__name__` label value for an ungrouped series.
    pub fn metric_name(&self) -> &str {
        self.metric_name
    }

    /// The requested quantiles (`PlanKind::Quantile`), in request order.
    pub fn quantiles(&self) -> &[f64] {
        &self.quantiles
    }

    /// The second-stage `topk`/`bottomk` reduction, if any.
    pub fn reduce(&self) -> Option<SeriesReduce> {
        self.reduce
    }

    /// The trailing metrics-result comparison post-filter, if present.
    pub fn result_filter(&self) -> Option<(pulsus_traceql::ComparisonOp, f64)> {
        self.result_filter
    }

    /// `compare()`'s `topN` — how many distinct values the engine keeps
    /// per attribute PER SIDE (issue #460). Meaningless for a non-compare
    /// plan, where it carries the reference's default.
    pub fn compare_top_n(&self) -> usize {
        self.compare_top_n
    }

    /// The resolved TOTAL exemplar budget for the whole response; `0` is
    /// none.
    pub fn exemplar_budget(&self) -> u32 {
        self.exemplar_budget
    }

    /// The emitted bucket grid for the range form.
    pub fn range_axis(&self) -> RangeAxis {
        self.range_axis
    }

    pub fn step_ms(&self) -> i64 {
        self.step_ms
    }

    /// Whether the plan was built against `_dist` tables.
    pub fn distributed(&self) -> bool {
        self.distributed
    }

    /// The rows every INSTANT statement reads, in nanoseconds: the
    /// requested `[start, end)` exactly, from the retention floor on
    /// (issue #601 part 2). The range form reads
    /// [`Self::range_window_ns`].
    pub fn snapped_window_ns(&self) -> (i64, i64) {
        (self.instant_rows.start_ns, self.instant_rows.end_ns)
    }

    /// The rows every RANGE statement reads, in nanoseconds: the grid's
    /// `[aS - step + 1, aE + 1)` — the integer-nanosecond spelling of the
    /// right-closed instant range `(aS - step, aE]` — from the retention
    /// floor on.
    pub fn range_window_ns(&self) -> (i64, i64) {
        (self.range_rows.start_ns, self.range_rows.end_ns)
    }

    /// The instant evaluation timestamp (`E`, the snapped right edge) in
    /// milliseconds — what the server hands the Prometheus vector
    /// encoder as `at_ms` (plan v2 delta 5).
    pub fn snapped_end_ms(&self) -> i64 {
        self.window.end_ns / 1_000_000
    }

    /// The requested INSTANT window's width, `end − start`, in
    /// **fractional seconds** — the instant `rate` denominator.
    ///
    /// `f64`, not whole seconds: a window can be narrower than one second,
    /// and a truncating `i64` form makes the denominator `0`, so `n / 0.0`
    /// encodes as `inf`. Widened through `i128` first: both bounds fit
    /// `i64`, but their difference need not.
    pub(crate) fn window_seconds(&self) -> f64 {
        let width_ns =
            i128::from(self.instant_window.end_ns) - i128::from(self.instant_window.start_ns);
        width_ns as f64 / NS_PER_S as f64
    }

    /// The range `rate` denominator: one bucket's width in fractional
    /// seconds.
    pub(crate) fn step_seconds(&self) -> f64 {
        self.step_ms as f64 / 1_000.0
    }
}

/// Plans one metrics request's shell. Pure and deterministic. It reads
/// neither the filter body nor the `by()` keys nor the target, which the
/// metrics statement compiles.
pub fn plan_trace_metrics(
    query: &Query,
    params: &MetricsParams,
    ctx: &MetricsCtx,
) -> Result<TraceMetricsPlan, PlanError> {
    if params.step_ms < 1 {
        return Err(PlanError::TypeMismatch(
            "step must be a positive whole number of milliseconds".to_string(),
        ));
    }
    if params.end_ns <= params.start_ns {
        return Err(PlanError::TypeMismatch(
            "end must be after start".to_string(),
        ));
    }

    let analysis = analyze_pipeline(query)?;

    // Cross-spanset and structural metrics are out of scope (plan v1
    // edge 4: the compiler is per-SpansetFilter; issue #172's structural
    // relations are two-phase-search-only) — an explicit caller error.
    if !matches!(query.spanset, SpansetExpr::Filter(_)) {
        return Err(PlanError::TypeMismatch(STRUCTURAL_METRICS.to_string()));
    }

    // Epoch-aligned outward snap (plan v2 delta 2): S = ⌊start/step⌋·step,
    // E = ⌈end/step⌉·step — every bucket [b, b+step) is full-width, the
    // window is left-closed/right-open. ALL snap/width arithmetic runs in
    // `i128` (code review round 1, high): any pair of accepted `i64`
    // endpoints — including near-`i64::MIN`/`i64::MAX` extremes whose
    // width does not fit `i64` — must resolve to the same 400/422 classes
    // as ordinary validation, never a panic and never a wrap that sneaks
    // a >cap bucket count past the static check.
    let step_ns = i128::from(params.step_ms) * i128::from(NS_PER_MS);
    let start = i128::from(params.start_ns);
    let end = i128::from(params.end_ns);
    let snapped_start = start.div_euclid(step_ns) * step_ns;
    let snapped_end = match end.rem_euclid(step_ns) {
        0 => end,
        rem => end + (step_ns - rem),
    };
    // end > start was validated above, and the snap only moves the edges
    // outward — a non-positive snapped width is unreachable; keep the
    // guard anyway (defense in depth over the division below).
    if snapped_end <= snapped_start {
        return Err(PlanError::TypeMismatch(
            "end must be after start".to_string(),
        ));
    }

    // The adjudicated bounded-response contract (docs/api.md §4.4):
    // bucket count over the SNAPPED window, statically, before any SQL
    // executes — breach is a 422, never a truncation. Checked FIRST, in
    // exact `i128`, so an over-cap range always 422s even when its
    // snapped bounds would not fit `i64` at all.
    let buckets = (snapped_end - snapped_start) / step_ns;
    if buckets > i128::from(MAX_METRICS_POINTS) {
        return Err(PlanError::MetricsPointCap {
            buckets: i64::try_from(buckets).unwrap_or(i64::MAX),
            cap: MAX_METRICS_POINTS,
        });
    }

    // Under-cap windows whose outward-snapped bounds still escape the
    // storable `i64` nanosecond range (endpoints hugging i64::MIN/MAX, or
    // an enormous step) are plain caller errors — 400, never a wrap.
    let out_of_range = || PlanError::TypeMismatch("start/end is out of range".to_string());
    let window = SnappedWindow {
        start_ns: i64::try_from(snapped_start).map_err(|_| out_of_range())?,
        end_ns: i64::try_from(snapped_end).map_err(|_| out_of_range())?,
    };
    // Issue #477 (a)/(b): the RANGE form reads the right-closed instants
    // `(aS - step, aE]`, which over integer nanoseconds is exactly the
    // left-closed/right-open `[aS - step + 1, aE + 1)` the existing
    // `span_time_clause`/`span_day_clause` render — so neither moves. One
    // whole step wider on the left, because the leading bucket's value is
    // measured to come from that data; and one nanosecond wider on the
    // right, because `aE` itself belongs to the last bucket.
    let range_window = SnappedWindow {
        start_ns: i64::try_from(snapped_start - step_ns + 1).map_err(|_| out_of_range())?,
        end_ns: i64::try_from(snapped_end + 1).map_err(|_| out_of_range())?,
    };
    // `buckets` is `<= MAX_METRICS_POINTS` (checked above), so `+ 1` and
    // the `usize` narrowing are both exact.
    let range_axis = RangeAxis {
        first_ms: window.start_ns / NS_PER_MS,
        step_ms: params.step_ms,
        points: (buckets as usize) + 1,
    };
    // Issue #601 part 2: the instant form is one bucket over exactly the
    // requested window, and every statement reads spans from the
    // retention floor on — the grid above is unchanged, and a floor at or
    // past a window's end leaves it empty.
    let instant_window = SnappedWindow {
        start_ns: params.start_ns,
        end_ns: params.end_ns,
    };
    let floored = |w: SnappedWindow| SnappedWindow {
        start_ns: w.start_ns.max(params.floor_ns).min(w.end_ns),
        end_ns: w.end_ns,
    };
    let instant_rows = floored(instant_window);
    let range_rows = floored(range_window);

    // Issue #477 (c), ONE resolution and not two branches: the `with()`
    // hint wins if present, otherwise the HTTP `exemplars` parameter,
    // otherwise `DEFAULT_EXEMPLARS`. The budget is a TOTAL for the whole
    // response, which the statement keeps.
    let exemplar_budget = match (analysis.exemplar_k, params.exemplars) {
        (Some(k), _) => k.min(MAX_EXEMPLARS),
        (None, Some(p)) => p.min(MAX_EXEMPLARS),
        (None, None) => DEFAULT_EXEMPLARS,
    };

    Ok(TraceMetricsPlan {
        kind: analysis.kind,
        metric_name: analysis.metric_name,
        quantiles: analysis.quantiles,
        reduce: analysis.reduce,
        exemplar_budget,
        result_filter: analysis.result_filter,
        compare_top_n: analysis.compare_top_n,
        step_ms: params.step_ms,
        window,
        range_axis,
        instant_window,
        instant_rows,
        range_rows,
        distributed: ctx.distributed,
    })
}

/// A second-stage series reduction (issue #182 P5): `topk(n)`/`bottomk(n)`
/// applied client-side per timestamp over the (capped) series set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeriesReduce {
    TopK(u64),
    BottomK(u64),
}

/// The well-known attribute keys Tempo v3.0.2 always enumerates in a
/// `compare()` result — appearing as `key=nil` when no span carries them
/// (issue #182 review Fix 3). Captured black-box from the pinned container
/// (query `compare()` over a representative corpus, collect every `key=nil`
/// key) and cross-referenced to the published OTLP semantic conventions
/// (Apache-2.0, freely referenceable) — the same clean-room method used
/// for the response envelope; **no Tempo source list is copied**. Grouped:
/// span intrinsics, instrumentation scope, well-known resource attributes,
/// and the common OTLP HTTP/URL span attributes.
pub const WELL_KNOWN_COMPARE_KEYS: &[&str] = &[
    // Span/trace intrinsics.
    "name",
    "kind",
    "status",
    "statusMessage",
    "rootName",
    "rootServiceName",
    // Instrumentation scope.
    "instrumentation:name",
    "instrumentation:version",
    // Well-known resource attributes (OTLP resource semconv).
    "resource.service.name",
    "resource.cluster",
    "resource.container",
    "resource.namespace",
    "resource.pod",
    "resource.k8s.cluster.name",
    "resource.k8s.container.name",
    "resource.k8s.namespace.name",
    "resource.k8s.pod.name",
    // Common OTLP HTTP/URL span attributes (span semconv).
    "span.http.method",
    "span.http.request.method",
    "span.http.route",
    "span.http.status_code",
    "span.http.url",
    "span.server.address",
    "span.url.path",
    "span.url.route",
];

/// [`pulsus_traceql::COMPARE_DEFAULT_TOP_N`] as a `usize` — the value a
/// non-compare plan carries in its `compare_top_n` field, so that field
/// is never a meaningless zero.
const COMPARE_DEFAULT_TOP_N_USIZE: usize = pulsus_traceql::COMPARE_DEFAULT_TOP_N as usize;

/// The TOTAL exemplar budget for a range response when neither the
/// `with(exemplars=…)` hint nor the HTTP `exemplars` parameter is present
/// (issue #477 (c)): exemplars attach to a plain `{} | rate()` by default,
/// as the reference does.
///
/// **Total, not per bucket** (ruling 1 on issue #477). The unit had to
/// change with the default: per-bucket at the 11 001-point grid cap with a
/// default of 100 is 1.1 million exemplars in one response, and there is
/// no reading of a default of 100 under which that is the intended
/// meaning. `with(exemplars=N)` therefore also means N for the whole
/// response now, where it used to mean N per bucket — a recorded
/// behaviour change (docs/api.md §4.4,
/// `traceql-metrics-exemplars-total-budget` in the divergence ledger).
pub const DEFAULT_EXEMPLARS: u32 = 100;

/// The hard TOTAL exemplar ceiling — both the hint and the parameter are
/// clamped to it, so exemplar collection can never blow the scan/response
/// budget however large a value a client sends.
pub const MAX_EXEMPLARS: u32 = 100;

/// The resolved metrics pipeline: its kind, the `__name__` label for
/// ungrouped output, the optional second-stage reduction, and the optional
/// exemplar budget.
struct PipelineAnalysis {
    kind: PlanKind,
    metric_name: &'static str,
    quantiles: Vec<f64>,
    reduce: Option<SeriesReduce>,
    exemplar_k: Option<u32>,
    /// The trailing metrics-result comparison (`… > 5`), parsed to `f64`.
    result_filter: Option<(pulsus_traceql::ComparisonOp, f64)>,
    /// `compare()`'s `topN` argument (issue #460), saturated into `usize`.
    compare_top_n: usize,
}

/// Analyzes the metrics pipeline: a first-stage metric function (with
/// optional `by(...)`, `with()`, trailing `> value`, and a `topk`/`bottomk`
/// second stage), or a standalone `compare({selection})` stage.
fn analyze_pipeline(query: &Query) -> Result<PipelineAnalysis, PlanError> {
    // compare() is a standalone metrics stage with its own shape; it
    // accepts `with(...)` hints (e.g. exemplars).
    if let [PipelineStage::Compare { top_n, hints, .. }] = query.pipeline.as_slice() {
        return Ok(PipelineAnalysis {
            kind: PlanKind::Compare,
            metric_name: "compare",
            quantiles: Vec::new(),
            reduce: None,
            exemplar_k: resolve_hints(hints)?,
            result_filter: None,
            // `validate` guarantees `> 0`; the saturating cast means an
            // enormous topN trims nothing rather than wrapping to zero
            // and trimming everything.
            compare_top_n: usize::try_from(*top_n).unwrap_or(usize::MAX),
        });
    }
    let (stage, reduce) = match query.pipeline.as_slice() {
        [PipelineStage::Metric(stage)] => (stage, None),
        [
            PipelineStage::Metric(stage),
            PipelineStage::MetricSecondStage(second),
        ] => (stage, Some(resolve_second_stage(second))),
        [] => {
            return Err(PlanError::TypeMismatch(
                "a metrics query requires a metrics function stage (rate, count_over_time, a \
                 *_over_time aggregation, or compare())"
                    .to_string(),
            ));
        }
        _ => {
            return Err(PlanError::TypeMismatch(
                "a metrics query takes one metrics function stage and at most one topk()/bottomk() \
                 second stage; aggregate filters, select(), and { ... } spanset filters are \
                 search-only"
                    .to_string(),
            ));
        }
    };
    let exemplar_k = resolve_hints(&stage.hints)?;
    let (kind, metric_name, quantiles) = resolve_func(&stage.func)?;
    let result_filter = resolve_result_filter(&stage.result_filter)?;
    Ok(PipelineAnalysis {
        kind,
        metric_name,
        quantiles,
        reduce,
        exemplar_k,
        result_filter,
        // Not a compare plan; the reference's default is carried so the
        // field is never a meaningless zero.
        compare_top_n: COMPARE_DEFAULT_TOP_N_USIZE,
    })
}

/// Parses a trailing metrics-result comparison value to `f64` (a duration
/// literal is compared in seconds, matching the value aggregations'
/// ns→seconds encode scaling).
///
/// Issue #237 (settled — do NOT "fix" this like #232): the reference's
/// ns→seconds conversion is the SINGLE-rounding `float64(ns) / 1e9`
/// this function already uses, not #232's two-rounding form (the metrics
/// statement's one rounding is pinned live by `traces_query_v2_live.rs`'s
/// `a_duration_in_seconds_is_rounded_once`). Corroboration for this threshold site specifically,
/// captured from the pinned container 2026-07-26: for a stored
/// 1_118_000_000 ns span, `… | max_over_time(duration) = 1118ms`
/// returns the series, `>= ` the single-rounding f64's shortest decimal
/// matches, `> ` it does not, and `> ` the two-rounding 17-digit
/// neighbour matches — i.e. the reference converts the duration literal
/// to exactly the single-rounding value, as here. (Labelled
/// corroboration, not proof: the load-bearing leg is the raw-wire
/// capture.)
fn resolve_result_filter(
    filter: &Option<(pulsus_traceql::ComparisonOp, pulsus_traceql::Value)>,
) -> Result<Option<(pulsus_traceql::ComparisonOp, f64)>, PlanError> {
    let Some((op, value)) = filter else {
        return Ok(None);
    };
    let v = match value {
        pulsus_traceql::Value::Number(raw) => raw
            .parse::<f64>()
            .map_err(|_| PlanError::TypeMismatch(format!("invalid comparison value {raw:?}")))?,
        pulsus_traceql::Value::Duration(d) => d.as_nanos() as f64 / 1e9,
        other => {
            return Err(PlanError::TypeMismatch(format!(
                "a metrics-result comparison takes a number or duration, got {other}"
            )));
        }
    };
    Ok(Some((*op, v)))
}

/// Maps a parsed second stage to its read-side reduction.
fn resolve_second_stage(second: &pulsus_traceql::SecondStage) -> SeriesReduce {
    match second {
        pulsus_traceql::SecondStage::TopK(n) => SeriesReduce::TopK(*n),
        pulsus_traceql::SecondStage::BottomK(n) => SeriesReduce::BottomK(*n),
    }
}

/// Resolves `with(...)` hints (issue #182 P5). `sample` is accepted and
/// returns the exact (superset) result — value-exact sampling parity
/// routes to #25. Other hints (e.g. `most_recent`) are accepted and
/// ignored (a valid superset), never a `400`.
///
/// `exemplars=<true|false|N>` is the FIRST of the three exemplar inputs
/// (issue #477 (c)): `Some(k)` here always wins over the HTTP parameter
/// and over the default, including `Some(0)`. `false` is therefore
/// `Some(0)` and not `None` — "no exemplars" has to be expressible, and
/// `None` would fall through to the default and turn them back on.
/// The value is a TOTAL budget clamped to [`MAX_EXEMPLARS`].
fn resolve_hints(hints: &[pulsus_traceql::MetricHint]) -> Result<Option<u32>, PlanError> {
    use pulsus_traceql::HintValue;
    let mut exemplar_k: Option<u32> = None;
    for hint in hints {
        if hint.key == "exemplars" {
            let k = match &hint.value {
                HintValue::Bool(true) => DEFAULT_EXEMPLARS,
                HintValue::Bool(false) => 0,
                HintValue::Number(raw) => raw
                    .parse::<f64>()
                    .ok()
                    .filter(|n| *n >= 0.0)
                    .map(|n| (n as u32).min(MAX_EXEMPLARS))
                    .ok_or_else(|| {
                        PlanError::TypeMismatch(format!("invalid exemplars count {raw:?}"))
                    })?,
                _ => {
                    return Err(PlanError::TypeMismatch(
                        "exemplars must be a boolean or a number".to_string(),
                    ));
                }
            };
            exemplar_k = Some(k.min(MAX_EXEMPLARS));
        }
        // `sample` and any other hint: accepted, exact superset returned.
    }
    Ok(exemplar_k)
}

/// Resolves a metric function to its read-side kind, `__name__`, and (for
/// `quantile_over_time`) the parsed quantile list. The target is the
/// metrics statement's.
fn resolve_func(func: &MetricFn) -> Result<(PlanKind, &'static str, Vec<f64>), PlanError> {
    let no_q = Vec::new();
    match func {
        MetricFn::Rate => Ok((PlanKind::Count { is_rate: true }, "rate", no_q)),
        MetricFn::CountOverTime => {
            Ok((PlanKind::Count { is_rate: false }, "count_over_time", no_q))
        }
        MetricFn::SumOverTime(_) => Ok((PlanKind::Agg(AggFn::Sum), "sum_over_time", no_q)),
        MetricFn::MinOverTime(_) => Ok((PlanKind::Agg(AggFn::Min), "min_over_time", no_q)),
        MetricFn::MaxOverTime(_) => Ok((PlanKind::Agg(AggFn::Max), "max_over_time", no_q)),
        MetricFn::AvgOverTime(_) => Ok((PlanKind::Agg(AggFn::Avg), "avg_over_time", no_q)),
        MetricFn::QuantileOverTime { quantiles, .. } => {
            let qs = parse_quantiles(quantiles)?;
            Ok((PlanKind::Quantile, "quantile_over_time", qs))
        }
        MetricFn::HistogramOverTime(_) => Ok((PlanKind::Histogram, "histogram_over_time", no_q)),
    }
}

/// Parses the quantile literals to `f64`, validating each is in `[0, 1]`.
fn parse_quantiles(quantiles: &[pulsus_traceql::Value]) -> Result<Vec<f64>, PlanError> {
    let mut out = Vec::with_capacity(quantiles.len());
    for q in quantiles {
        let pulsus_traceql::Value::Number(raw) = q else {
            return Err(PlanError::TypeMismatch(
                "quantile_over_time quantiles must be numbers".to_string(),
            ));
        };
        let v: f64 = raw
            .parse()
            .map_err(|_| PlanError::TypeMismatch(format!("invalid quantile {raw:?}")))?;
        if !(0.0..=1.0).contains(&v) {
            return Err(PlanError::TypeMismatch(format!(
                "quantile {v} is out of range [0, 1]"
            )));
        }
        out.push(v);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use pulsus_traceql::parse;

    use super::*;

    fn ctx() -> MetricsCtx {
        MetricsCtx {
            scan_budget_rows: 50_000_000,
            max_series: 1_000,
            distributed: false,
            skip_unavailable_shards: false,
        }
    }

    const PARAMS: MetricsParams = MetricsParams {
        start_ns: 1_700_000_000_000_000_000,
        end_ns: 1_700_010_800_000_000_000,
        step_ms: 60_000,
        exemplars: None,
        floor_ns: i64::MIN,
    };

    fn plan(q: &str) -> TraceMetricsPlan {
        plan_trace_metrics(&parse(q).expect("parse"), &PARAMS, &ctx()).expect("plan")
    }

    /// Issue #492 item 9 criterion 15: the metrics-route refusal names the
    /// `{ ... }` spanset filter.
    ///
    /// A `{...}` pipeline stage is search-only. Before item 9 the query
    /// below was a parser `400`; now it parses, reaches this catch-all,
    /// and must be a planner `400` whose message says WHICH stages the
    /// metrics routes take — otherwise a user sent a stage the message
    /// does not mention. The text is asserted in full because the ledger
    /// row `traceql-midpipeline-filter-before-metrics-stage-unsupported`
    /// quotes it verbatim.
    ///
    /// *RED when:* the catch-all keeps its old message, which named only
    /// aggregate filters and `select()`.
    #[test]
    fn the_metrics_refusal_names_the_mid_pipeline_spanset_filter() {
        let q = parse(r#"{ .a = 1 } | { .b = 2 } | rate()"#).expect("it must PARSE");
        // `PipelineAnalysis` carries no `Debug`, so the refusal is
        // destructured rather than unwrapped.
        let Err(err) = analyze_pipeline(&q) else {
            panic!("a `{{...}}` stage must be refused on the metrics routes");
        };
        assert_eq!(
            err.to_string(),
            "type mismatch: a metrics query takes one metrics function stage and at most one \
             topk()/bottomk() second stage; aggregate filters, select(), and { ... } spanset \
             filters are search-only"
        );
    }

    /// AC1 (issue #477 (a)/(b)): the emitted bucket grid, one case per
    /// geometry row measured against the reference. Class CHANGE — no
    /// axis exists at `2f78c53`.
    ///
    /// Every row is `(start_s, end_s, step_ms) -> (points, first_ms,
    /// last_ms)`. `first_ms` is `aS`, the right edge of the extra leading
    /// bucket; `last_ms` is `aE`; `points` is `intervals + 1`.
    #[test]
    fn the_range_axis_matches_the_reference_grid() {
        for (start_s, end_s, step_ms, points, first_ms, last_ms) in [
            (
                1_788_182_400i64,
                1_788_182_640i64,
                30_000i64,
                9usize,
                1_788_182_400_000i64,
                1_788_182_640_000i64,
            ),
            (
                1_788_182_401,
                1_788_182_641,
                30_000,
                10,
                1_788_182_400_000,
                1_788_182_670_000,
            ),
            (
                1_788_182_429,
                1_788_182_669,
                30_000,
                10,
                1_788_182_400_000,
                1_788_182_670_000,
            ),
            (
                1_788_182_521,
                1_788_182_579,
                30_000,
                3,
                1_788_182_520_000,
                1_788_182_580_000,
            ),
            (
                1_788_182_537,
                1_788_182_540,
                500,
                7,
                1_788_182_537_000,
                1_788_182_540_000,
            ),
            (
                1_788_182_535,
                1_788_182_541,
                1_500,
                5,
                1_788_182_535_000,
                1_788_182_541_000,
            ),
            (
                1_788_183_390,
                1_788_183_410,
                20_000,
                3,
                1_788_183_380_000,
                1_788_183_420_000,
            ),
        ] {
            let params = MetricsParams {
                start_ns: start_s * NS_PER_S,
                end_ns: end_s * NS_PER_S,
                step_ms,
                exemplars: None,
                floor_ns: i64::MIN,
            };
            let p = plan_trace_metrics(&parse("{} | rate()").unwrap(), &params, &ctx())
                .unwrap_or_else(|e| panic!("{start_s}..{end_s} step {step_ms}ms: {e}"));
            let axis = p.range_axis();
            assert_eq!(
                (axis.points, axis.first_ms, axis.last_ms()),
                (points, first_ms, last_ms),
                "{start_s}..{end_s} step {step_ms}ms"
            );
            assert_eq!(axis.step_ms, step_ms);
            // The labels really are `first + i*step`, and the last one is
            // the snapped right edge.
            assert_eq!(axis.label_ms(0), first_ms);
            assert_eq!(axis.label_ms(axis.points - 1), last_ms);
        }
    }

    /// AC1, the right-closed lookup: `label_for_ms` is the ceiling, so an
    /// instant landing exactly on a grid point stays on THAT point.
    #[test]
    fn the_axis_label_for_an_instant_is_the_right_closed_ceiling() {
        let axis = RangeAxis {
            first_ms: 1_000_000,
            step_ms: 500,
            points: 5,
        };
        assert_eq!(
            axis.label_for_ms(1_000_000),
            1_000_000,
            "exactly on a grid point goes LEFT"
        );
        assert_eq!(axis.label_for_ms(1_000_001), 1_000_500);
        assert_eq!(axis.label_for_ms(1_000_499), 1_000_500);
        assert_eq!(axis.label_for_ms(1_000_500), 1_000_500);
        // Negative epochs (pre-1970) use the same ceiling, not a truncation.
        let neg = RangeAxis {
            first_ms: -3_600_000,
            step_ms: 60_000,
            points: 2,
        };
        assert_eq!(neg.label_for_ms(-3_600_000), -3_600_000);
        assert_eq!(neg.label_for_ms(-3_599_999), -3_540_000);
        // `-3_660_001` sits inside `(-3_720_000, -3_660_000]`, so its
        // label is that bucket's right edge, not the axis's first one.
        assert_eq!(neg.label_for_ms(-3_660_001), -3_660_000);
    }

    /// AC5(i): the exemplar budget resolves as ONE rule — hint, then the
    /// HTTP parameter, then the default — and the budget is what decides
    /// whether any exemplar SQL is rendered at all.
    #[test]
    fn the_exemplar_budget_resolves_hint_then_parameter_then_default() {
        let plan_ex = |q: &str, exemplars: Option<u32>| {
            let params = MetricsParams {
                exemplars,
                floor_ns: i64::MIN,
                ..PARAMS
            };
            plan_trace_metrics(&parse(q).expect("parse"), &params, &ctx()).expect("plan")
        };
        // No hint, no parameter: exemplars are ON by default.
        let d = plan_ex("{} | rate()", None);
        assert_eq!(d.exemplar_budget(), 100);
        // The parameter alone.
        assert_eq!(plan_ex("{} | rate()", Some(5)).exemplar_budget(), 5);
        // The hint WINS over the parameter, in both directions — this is
        // the pair that discriminates a precedence swap.
        assert_eq!(
            plan_ex("{} | rate() with(exemplars=1)", Some(5)).exemplar_budget(),
            1
        );
        assert_eq!(
            plan_ex("{} | rate() with(exemplars=5)", Some(1)).exemplar_budget(),
            5
        );
        // `false` is expressible and turns them off — it must not fall
        // through to the default.
        let off = plan_ex("{} | rate() with(exemplars=false)", None);
        assert_eq!(off.exemplar_budget(), 0);
        // …including when the parameter asks for some.
        assert_eq!(
            plan_ex("{} | rate() with(exemplars=false)", Some(9)).exemplar_budget(),
            0
        );
        // `true` means the default, not 1.
        assert_eq!(
            plan_ex("{} | rate() with(exemplars=true)", None).exemplar_budget(),
            100
        );
        // Both inputs are clamped to the ceiling.
        assert_eq!(
            plan_ex("{} | rate()", Some(100_000)).exemplar_budget(),
            MAX_EXEMPLARS
        );
        assert_eq!(
            plan_ex("{} | rate() with(exemplars=100000)", None).exemplar_budget(),
            MAX_EXEMPLARS
        );
    }

    /// AC13: the interval cap at the new unit, pinned on both sides.
    #[test]
    fn the_interval_cap_boundary_holds_in_milliseconds() {
        // (step_ms, width_ms, expected)
        for (step_ms, width_ms, want) in [
            (1_000i64, 11_000_000i64, Ok(11_001usize)),
            (1_000, 11_001_000, Err(11_001i64)),
            (1, 11_000, Ok(11_001)),
            (1, 11_001, Err(11_001)),
            (500, 5_500_000, Ok(11_001)),
        ] {
            let params = MetricsParams {
                start_ns: 0,
                end_ns: width_ms * NS_PER_MS,
                step_ms,
                exemplars: None,
                floor_ns: i64::MIN,
            };
            let got = plan_trace_metrics(&parse("{} | rate()").unwrap(), &params, &ctx());
            match (want, got) {
                (Ok(points), Ok(p)) => {
                    assert_eq!(
                        p.range_axis().points,
                        points,
                        "step {step_ms}ms / {width_ms}ms"
                    )
                }
                (Err(buckets), Err(e)) => assert_eq!(
                    e,
                    PlanError::MetricsPointCap {
                        buckets,
                        cap: MAX_METRICS_POINTS,
                    },
                    "step {step_ms}ms / {width_ms}ms"
                ),
                (want, got) => {
                    panic!("step {step_ms}ms / {width_ms}ms: wanted {want:?}, got {got:?}")
                }
            }
        }
    }

    /// The grid snaps outward to step boundaries; the instant window does
    /// not (issue #601 part 2).
    #[test]
    fn the_grid_snaps_outward_to_epoch_aligned_step_boundaries() {
        let p = plan("{} | rate()");
        // 1_700_000_000 is not a multiple of 60 → S floors to
        // 1_699_999_980; 1_700_010_800 → E ceils to 1_700_010_840.
        assert_eq!(p.range_axis().first_ms, 1_699_999_980_000);
        assert_eq!(p.snapped_end_ms(), 1_700_010_840_000);
        assert_eq!(
            p.snapped_window_ns(),
            (1_700_000_000_000_000_000, 1_700_010_800_000_000_000),
            "the instant rows are the requested window"
        );
        assert_eq!(p.window_seconds(), 10_800.0);
    }

    /// Issue #601 part 2: every statement reads from the retention floor
    /// on, the grid unchanged; a floor at or past a window's end leaves it
    /// empty.
    #[test]
    fn the_row_windows_start_at_the_floor() {
        let floor = 1_700_005_000_000_000_000;
        let p = plan_trace_metrics(
            &parse("{} | rate()").unwrap(),
            &MetricsParams {
                floor_ns: floor,
                ..PARAMS
            },
            &ctx(),
        )
        .unwrap();
        assert_eq!(p.range_window_ns(), (floor, 1_700_010_840_000_000_001));
        assert_eq!(p.snapped_window_ns(), (floor, 1_700_010_800_000_000_000));
        assert_eq!(p.range_axis(), plan("{} | rate()").range_axis());
        let past = plan_trace_metrics(
            &parse("{} | rate()").unwrap(),
            &MetricsParams {
                floor_ns: i64::MAX,
                ..PARAMS
            },
            &ctx(),
        )
        .unwrap();
        assert_eq!(
            past.snapped_window_ns(),
            (1_700_010_800_000_000_000, 1_700_010_800_000_000_000)
        );
        assert_eq!(
            past.range_window_ns(),
            (1_700_010_840_000_000_001, 1_700_010_840_000_000_001)
        );
    }

    #[test]
    fn an_aligned_window_snaps_to_itself() {
        let params = MetricsParams {
            start_ns: 1_699_999_980_000_000_000,
            end_ns: 1_700_010_840_000_000_000,
            step_ms: 60_000,
            exemplars: None,
            floor_ns: i64::MIN,
        };
        let p = plan_trace_metrics(&parse("{} | rate()").unwrap(), &params, &ctx()).unwrap();
        assert_eq!(
            p.snapped_window_ns(),
            (params.start_ns, params.end_ns),
            "snap is the identity on aligned windows (AC4 by construction)"
        );
    }

    #[test]
    fn rate_and_count_over_time_map_to_their_kinds() {
        let rate = plan("{} | rate()");
        assert_eq!(rate.kind(), PlanKind::Count { is_rate: true });
        assert_eq!(rate.metric_name(), "rate");
        let count = plan("{} | count_over_time()");
        assert_eq!(count.kind(), PlanKind::Count { is_rate: false });
        assert_eq!(count.metric_name(), "count_over_time");
    }

    #[test]
    fn over_time_aggregations_map_to_agg_kinds() {
        for (q, agg, name) in [
            ("{} | sum_over_time(duration)", AggFn::Sum, "sum_over_time"),
            ("{} | min_over_time(duration)", AggFn::Min, "min_over_time"),
            ("{} | max_over_time(duration)", AggFn::Max, "max_over_time"),
            ("{} | avg_over_time(duration)", AggFn::Avg, "avg_over_time"),
        ] {
            let p = plan(q);
            assert_eq!(p.kind(), PlanKind::Agg(agg), "{q}");
            assert_eq!(p.metric_name(), name, "{q}");
        }
    }

    #[test]
    fn quantile_and_histogram_plan_to_their_kinds() {
        let quant = plan("{} | quantile_over_time(duration, 0.5, 0.9)");
        assert_eq!(quant.kind(), PlanKind::Quantile);
        assert_eq!(quant.quantiles(), &[0.5, 0.9]);
        let hist = plan("{} | histogram_over_time(duration)");
        assert_eq!(hist.kind(), PlanKind::Histogram);
    }

    #[test]
    fn an_out_of_range_quantile_is_a_plan_error() {
        let err = plan_trace_metrics(
            &parse("{} | quantile_over_time(duration, 1.5)").unwrap(),
            &PARAMS,
            &ctx(),
        )
        .expect_err("quantile out of [0,1]");
        assert!(matches!(err, PlanError::TypeMismatch(_)), "{err}");
    }

    #[test]
    fn with_sample_is_accepted_and_leaves_the_exemplar_budget() {
        // `sample` is accepted (exact superset). It says nothing about
        // exemplars, so the two plans resolve the same budget — which
        // since issue #477 (c) is the default, not none.
        let plain = plan("{} | rate()");
        let sampled = plan("{} | rate() with(sample=0.1)");
        assert_eq!(sampled.exemplar_budget(), plain.exemplar_budget());
    }

    #[test]
    fn with_exemplars_sets_the_total_budget() {
        // The hint is a TOTAL of 5 over a 182-point grid (issue #477 (c)).
        let p = plan("{} | rate() with(exemplars=5)");
        assert_eq!(p.exemplar_budget(), 5);
        let wide = plan_trace_metrics(
            &parse("{} | rate() with(exemplars=100)").unwrap(),
            &MetricsParams {
                start_ns: 0,
                end_ns: 10 * NS_PER_S,
                step_ms: 1_000,
                exemplars: None,
                floor_ns: i64::MIN,
            },
            &ctx(),
        )
        .expect("plan");
        assert_eq!(wide.range_axis().points, 11);
        assert_eq!(wide.exemplar_budget(), 100);
    }

    #[test]
    fn topk_and_bottomk_second_stages_set_the_reduction() {
        assert_eq!(
            plan("{} | rate() | topk(3)").reduce(),
            Some(SeriesReduce::TopK(3))
        );
        assert_eq!(
            plan("{} | rate() | bottomk(2)").reduce(),
            Some(SeriesReduce::BottomK(2))
        );
        assert_eq!(plan("{} | rate()").reduce(), None);
    }

    #[test]
    fn compare_plans_to_its_kind() {
        let p = plan(r#"{} | compare({ span.http.status_code = "500" })"#);
        assert_eq!(p.kind(), PlanKind::Compare);
    }

    #[test]
    fn a_metrics_result_comparison_sets_the_post_filter() {
        let p = plan("{} | rate() > 5");
        assert_eq!(
            p.result_filter(),
            Some((pulsus_traceql::ComparisonOp::Gt, 5.0))
        );
        // A duration comparison is normalized to seconds.
        let d = plan("{} | avg_over_time(duration) > 5ms");
        assert_eq!(
            d.result_filter(),
            Some((pulsus_traceql::ComparisonOp::Gt, 0.005))
        );
        assert_eq!(plan("{} | rate()").result_filter(), None);
    }

    /// Issue #237: a duration threshold converts ns→seconds with the
    /// reference's SINGLE rounding, never #232's two-rounding form. The
    /// widths are the ≤16-digit captured group where the two forms
    /// disagree by 1 ULP; the self-consistency leg (threshold bits ==
    /// `ns as f64 / 1e9`, the one rounding the metrics statement applies
    /// to a stored span of that width) is what makes the
    /// reference's observed `= 1118ms` match reproduce.
    #[test]
    fn duration_result_filter_threshold_uses_the_single_rounding_conversion() {
        fn two_rounding_seconds(ns: i64) -> f64 {
            (ns / 1_000_000_000) as f64 + (ns % 1_000_000_000) as f64 / 1e9
        }
        for (ns, lit, want) in [
            (1_118_000_000_i64, "1118ms", 1.118_f64),
            (1_122_000_000, "1122ms", 1.122),
            (1_128_000_000, "1128ms", 1.128),
            (1_235_000_000, "1235ms", 1.235),
            (31_952_000_000, "31952ms", 31.952),
            (1_000_064_438, "1000064438ns", 1.000064438),
        ] {
            let p = plan(&format!("{{}} | max_over_time(duration) > {lit}"));
            let (op, threshold) = p.result_filter().expect("filter present");
            assert_eq!(op, pulsus_traceql::ComparisonOp::Gt, "{lit}");
            assert_eq!(threshold.to_bits(), want.to_bits(), "{lit}");
            assert_ne!(
                threshold.to_bits(),
                two_rounding_seconds(ns).to_bits(),
                "{lit}: the two-rounding form must never be produced"
            );
            assert_eq!(
                threshold.to_bits(),
                (ns as f64 / 1e9).to_bits(),
                "{lit}: threshold == the encode-boundary value for the same span"
            );
        }
    }

    /// Issue #477: the two forms carry DIFFERENT window bounds. The
    /// instant form reads the requested `[start, end)` (issue #601 part 2);
    /// the range form reads `(aS - step, aE]`, spelled over integer
    /// nanoseconds as `[aS - step + 1, aE + 1)`.
    #[test]
    fn each_form_reads_its_own_window() {
        let p = plan("{} | rate()");
        assert_eq!(
            p.range_window_ns(),
            (1_699_999_920_000_000_001, 1_700_010_840_000_000_001)
        );
        assert_eq!(
            p.snapped_window_ns(),
            (1_700_000_000_000_000_000, 1_700_010_800_000_000_000)
        );
    }

    #[test]
    fn a_missing_metric_stage_is_a_plan_error() {
        let err =
            plan_trace_metrics(&parse("{}").unwrap(), &PARAMS, &ctx()).expect_err("must fail");
        assert!(matches!(err, PlanError::TypeMismatch(_)), "{err}");
    }

    #[test]
    fn a_search_pipeline_stage_on_metrics_is_a_plan_error() {
        for q in ["{} | count() > 2", "{} | count() > 2 | rate()"] {
            let err = plan_trace_metrics(&parse(q).unwrap(), &PARAMS, &ctx())
                .expect_err("search stages are not metrics");
            assert!(matches!(err, PlanError::TypeMismatch(_)), "{q}: {err}");
        }
    }

    #[test]
    fn a_cross_spanset_metrics_query_is_a_plan_error() {
        let err = plan_trace_metrics(
            &parse(r#"{ .a = "1" } && { .b = "2" } | rate()"#).unwrap(),
            &PARAMS,
            &ctx(),
        )
        .expect_err("cross-spanset metrics are M4 out of scope");
        assert!(matches!(err, PlanError::TypeMismatch(_)), "{err}");
    }

    /// Issue #172: a structural `q` parses now, but the metrics planner
    /// rejects it as a caller error (→ 400) exactly like cross-spanset.
    #[test]
    fn a_structural_metrics_query_is_a_plan_error() {
        for q in [
            r#"{ .a = "1" } > { .b = "2" } | rate()"#,
            r#"{ .a = "1" } >> { .b = "2" } | count_over_time()"#,
            r#"{ .a = "1" } ~ { .b = "2" } | rate()"#,
            // Issue #183 forms: `<` (parent), negated, and union.
            r#"{ .a = "1" } < { .b = "2" } | rate()"#,
            r#"{ .a = "1" } !> { .b = "2" } | rate()"#,
            r#"{ .a = "1" } &> { .b = "2" } | count_over_time()"#,
        ] {
            let err = plan_trace_metrics(&parse(q).unwrap(), &PARAMS, &ctx())
                .expect_err("structural metrics are out of scope");
            match err {
                PlanError::TypeMismatch(msg) => {
                    assert!(msg.contains("structural"), "{q}: {msg}");
                }
                other => panic!("{q}: expected TypeMismatch, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_non_positive_step_is_a_plan_error() {
        for step_ms in [0, -60] {
            let params = MetricsParams { step_ms, ..PARAMS };
            let err = plan_trace_metrics(&parse("{} | rate()").unwrap(), &params, &ctx())
                .expect_err("non-positive step");
            assert!(matches!(err, PlanError::TypeMismatch(_)), "{err}");
        }
    }

    #[test]
    fn an_inverted_window_is_a_plan_error() {
        let params = MetricsParams {
            start_ns: PARAMS.end_ns,
            end_ns: PARAMS.start_ns,
            step_ms: 60_000,
            exemplars: None,
            floor_ns: i64::MIN,
        };
        let err = plan_trace_metrics(&parse("{} | rate()").unwrap(), &params, &ctx())
            .expect_err("inverted window");
        assert!(matches!(err, PlanError::TypeMismatch(_)), "{err}");
    }

    #[test]
    fn exceeding_the_point_cap_is_the_dedicated_422_variant() {
        // 12,000 one-second buckets > MAX_METRICS_POINTS (11,000).
        let params = MetricsParams {
            start_ns: 0,
            end_ns: 12_000 * 1_000_000_000,
            step_ms: 1_000,
            exemplars: None,
            floor_ns: i64::MIN,
        };
        let err = plan_trace_metrics(&parse("{} | rate()").unwrap(), &params, &ctx())
            .expect_err("over the cap");
        assert_eq!(
            err,
            PlanError::MetricsPointCap {
                buckets: 12_000,
                cap: MAX_METRICS_POINTS,
            }
        );
    }

    // ---- overflow-safety gauntlet (code review round 1, high): every
    // extreme accepted endpoint pair resolves to the ordinary 400/422
    // classes — never a panic, never a wrap past the static cap. -------

    #[test]
    fn near_i64_max_endpoints_are_a_clean_400_not_a_panic() {
        // The outward ceil of `end` would land past i64::MAX: under-cap
        // width, unrepresentable snapped bound → 400.
        let params = MetricsParams {
            start_ns: i64::MAX - 1_000_000_000,
            end_ns: i64::MAX,
            step_ms: 60_000,
            exemplars: None,
            floor_ns: i64::MIN,
        };
        let err = plan_trace_metrics(&parse("{} | rate()").unwrap(), &params, &ctx())
            .expect_err("must reject, not panic");
        assert!(matches!(err, PlanError::TypeMismatch(_)), "{err}");
    }

    #[test]
    fn near_i64_min_endpoints_are_a_clean_400_not_a_panic() {
        // The outward floor of `start` would land below i64::MIN.
        let params = MetricsParams {
            start_ns: i64::MIN,
            end_ns: i64::MIN + 1_000_000_000,
            step_ms: 60_000,
            exemplars: None,
            floor_ns: i64::MIN,
        };
        let err = plan_trace_metrics(&parse("{} | rate()").unwrap(), &params, &ctx())
            .expect_err("must reject, not panic");
        assert!(matches!(err, PlanError::TypeMismatch(_)), "{err}");
    }

    #[test]
    fn a_width_that_wraps_i64_still_hits_the_point_cap_422() {
        // The reviewer's sneak case: end − start overflows i64 (the old
        // subtraction wrapped negative and slid past the `> cap` check).
        // In exact i128 the bucket count is astronomical → the dedicated
        // 422 variant, before any SQL exists.
        let params = MetricsParams {
            start_ns: -9_000_000_000_000_000_000,
            end_ns: 9_000_000_000_000_000_000,
            step_ms: 1_000,
            exemplars: None,
            floor_ns: i64::MIN,
        };
        let err = plan_trace_metrics(&parse("{} | rate()").unwrap(), &params, &ctx())
            .expect_err("must reject, not panic");
        match err {
            PlanError::MetricsPointCap { buckets, cap } => {
                assert_eq!(cap, MAX_METRICS_POINTS);
                assert!(buckets > cap, "exact math: {buckets}");
            }
            other => panic!("expected MetricsPointCap, got {other:?}"),
        }
    }

    #[test]
    fn full_i64_range_endpoints_hit_the_point_cap_422() {
        let params = MetricsParams {
            start_ns: i64::MIN,
            end_ns: i64::MAX,
            step_ms: 1_000,
            exemplars: None,
            floor_ns: i64::MIN,
        };
        let err = plan_trace_metrics(&parse("{} | rate()").unwrap(), &params, &ctx())
            .expect_err("must reject, not panic");
        assert!(matches!(err, PlanError::MetricsPointCap { .. }), "{err}");
    }

    #[test]
    fn a_step_whose_nanos_exceed_i64_is_a_clean_400_not_a_panic() {
        // step_ms = i64::MAX: step_ns only exists in i128; the snapped end
        // (one whole step) cannot fit the storable i64 range → 400.
        let params = MetricsParams {
            step_ms: i64::MAX,
            ..PARAMS
        };
        let err = plan_trace_metrics(&parse("{} | rate()").unwrap(), &params, &ctx())
            .expect_err("must reject, not panic");
        assert!(matches!(err, PlanError::TypeMismatch(_)), "{err}");
    }

    #[test]
    fn an_extreme_under_cap_window_plans_with_an_i128_safe_width() {
        // Both snapped bounds fit i64 but their difference does not: the
        // instant denominator must come out of i128 math, not a wrapping
        // subtraction.
        let params = MetricsParams {
            start_ns: -8_000_000_000_000_000_000,
            end_ns: 8_000_000_000_000_000_000,
            step_ms: 2_000_000_000,
            exemplars: None,
            floor_ns: i64::MIN,
        };
        let p = plan_trace_metrics(&parse("{} | rate()").unwrap(), &params, &ctx())
            .expect("8000 buckets is under the cap");
        assert_eq!(p.window_seconds(), 16_000_000_000.0);
    }

    #[test]
    fn exactly_the_point_cap_plans() {
        let params = MetricsParams {
            start_ns: 0,
            end_ns: MAX_METRICS_POINTS * 1_000_000_000,
            step_ms: 1_000,
            exemplars: None,
            floor_ns: i64::MIN,
        };
        assert!(plan_trace_metrics(&parse("{} | rate()").unwrap(), &params, &ctx()).is_ok());
    }

    #[test]
    fn a_clustered_ctx_sets_the_distributed_flag() {
        let clustered = MetricsCtx {
            distributed: true,
            ..ctx()
        };
        for q in [
            r#"{ span.a = "1" } | rate()"#,
            r#"{ span.a = "1" } | compare({ span.b = "2" })"#,
        ] {
            let p = plan_trace_metrics(&parse(q).unwrap(), &PARAMS, &clustered).unwrap();
            assert!(p.distributed(), "{q}");
        }
    }
}
