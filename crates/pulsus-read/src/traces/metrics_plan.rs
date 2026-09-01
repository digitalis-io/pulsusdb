//! Pure `Query + MetricsParams + MetricsCtx → TraceMetricsPlan` planning
//! for the TraceQL metrics endpoints (issue #59; docs/api.md §4.4).
//! Deterministic, no I/O: validates the M4 metrics shape (single
//! spanset, exactly one metric stage), snaps the window to epoch-aligned
//! step boundaries (plan v2 delta 2), enforces the adjudicated point
//! cap, and renders both byte-frozen SQL forms via
//! [`super::metrics_sql`]. Every rejection is a [`PlanError`]: `400
//! bad_data` server-side, except [`PlanError::MetricsPointCap`] — the
//! adjudicated static pre-execution `422 query_too_broad`.

use pulsus_traceql::{AttrScope, Field, Intrinsic, MetricFn, PipelineStage, Query, SpansetExpr};

use super::filter::{PlanError, SpanFilterCtx};
use super::metrics_sql::{self, AggFn, GroupKeySql, SnappedWindow};

/// The auto-derivation target when `step` is omitted (docs/api.md §4.4,
/// task-manager adjudication 3): the derived step is
/// `max(1, ⌊(end_s − start_s) / DEFAULT_METRICS_POINTS⌋)` whole SECONDS,
/// scaled to the plan's millisecond unit. The derivation itself runs
/// server-side in `parse_metrics_params`; the constant lives here as the
/// committed contract's single source.
pub const DEFAULT_METRICS_POINTS: i64 = 100;

/// The hard bucket-count cap (docs/api.md §4.4): a snapped range
/// resolving more buckets is rejected statically with `422
/// query_too_broad` — bounded response, no silent truncation (the
/// adjudicated contract; deliberately 422, not Prometheus's 400).
pub const MAX_METRICS_POINTS: i64 = 11_000;

const NS_PER_S: i64 = 1_000_000_000;
const NS_PER_MS: i64 = 1_000_000;

/// The caller-validated request window, step and exemplar budget.
/// `step_ms` is whole milliseconds (issue #477 (d)), already defaulted by
/// the server's derivation formula when the request omitted `step`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsParams {
    pub start_ns: i64,
    pub end_ns: i64,
    /// Bucket width in whole milliseconds; `>= 1`.
    pub step_ms: i64,
    /// The HTTP `exemplars` parameter, normalised by the server:
    /// `None` when absent, empty, unparseable, zero or negative.
    pub exemplars: Option<u32>,
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
    /// means and what the SQL's [`super::metrics_sql::range_bucket_expr`]
    /// renders.
    pub fn label_for_ms(&self, ts_ms: i64) -> i64 {
        let step = self.step_ms;
        let floor = ts_ms.div_euclid(step) * step;
        if floor == ts_ms { ts_ms } else { floor + step }
    }
}

/// Engine-derived planning context — mirrors [`super::search_plan::SearchCtx`].
#[derive(Debug, Clone, Copy)]
pub struct MetricsCtx<'a> {
    pub filter: SpanFilterCtx<'a>,
    /// `reader.traceql_scan_budget_rows` — carried for parity with the
    /// engine's Layer-1 settings (the engine injects it at execution).
    pub scan_budget_rows: u64,
    /// `reader.traceql_max_series` (issue #182) — the `by(...)`
    /// distinct-series cap; the plan renders the `LIMIT cap+1` probe with
    /// it, and the engine flips a breach to a static 422.
    pub max_series: u64,
    /// Clustered mode: the engine injects the §7 clustered-reader
    /// settings plus `distributed_product_mode='local'` (the attr
    /// semi-join reads the co-sharded local `trace_attrs_idx` — plan v2
    /// delta 3a).
    pub distributed: bool,
    /// `PULSUS_SKIP_UNAVAILABLE_SHARDS` passthrough for the §7 settings.
    pub skip_unavailable_shards: bool,
}

/// The committed M4 metrics functions ([`pulsus_traceql::MetricFn`]'s
/// read-side twin — the planner owns the value-semantics mapping: `rate`
/// divides the deduped count by the step, in fractional seconds,
/// client-side at the encode boundary; `count_over_time` is the count
/// itself).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricFunc {
    Rate,
    CountOverTime,
}

impl MetricFunc {
    /// The Tempo `__name__` label value for an ungrouped series of this
    /// function (issue #182): the bare function name.
    pub fn name(self) -> &'static str {
        match self {
            MetricFunc::Rate => "rate",
            MetricFunc::CountOverTime => "count_over_time",
        }
    }
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

/// The complete, deterministic metrics plan — both SQL forms are
/// byte-frozen (`tests/traces_metrics_sql.rs`).
#[derive(Debug, Clone)]
pub struct TraceMetricsPlan {
    kind: PlanKind,
    /// The Tempo `__name__` label for an ungrouped series.
    metric_name: &'static str,
    /// The single resolved `by(...)` grouping key, if any (this pass
    /// supports one key: `resource.service.name` → the physical `service`
    /// column). `None` is ungrouped.
    group_label: Option<String>,
    /// The distinct-by-key series-cardinality probe SQL over the RANGE
    /// window, rendered only for a grouped or `compare()` query; the range
    /// path runs it before the main query.
    range_probe_sql: Option<String>,
    /// The same probe over the INSTANT window — byte-identical to the
    /// single probe this plan carried before issue #477, for both the
    /// grouped and the `compare()` shape. The instant path runs this one.
    instant_probe_sql: Option<String>,
    /// The requested quantiles (`PlanKind::Quantile` only), in request
    /// order — one output series per entry (`p=<q>` label).
    quantiles: Vec<f64>,
    /// The optional second-stage `topk`/`bottomk` reduction, applied
    /// client-side per timestamp after the series are framed.
    reduce: Option<SeriesReduce>,
    /// The per-bucket exemplar collection SQL (issue #182 P5), rendered
    /// whenever the resolved TOTAL exemplar budget is non-zero; the engine
    /// runs it and attaches `trace:id` exemplars.
    exemplar_sql: Option<String>,
    /// The resolved TOTAL exemplar budget for the whole response (issue
    /// #477 (c) and ruling 1): `0` means none. The engine thins the
    /// collected per-bucket samples down to this many.
    exemplar_budget: u32,
    /// A trailing `metrics-result comparison` post-filter (`… > 5`, issue
    /// #182 P6b): keeps only samples satisfying `<op> <value>`. Applied
    /// client-side after the series are framed.
    result_filter: Option<(pulsus_traceql::ComparisonOp, f64)>,
    /// `compare()` cross-tab + totals SQL, `(cross_tab, totals)` for the
    /// range and instant forms (`PlanKind::Compare` only).
    compare_range: Option<(String, String)>,
    compare_instant: Option<(String, String)>,
    /// `compare()`'s `topN` (issue #460) — the per-key-per-side
    /// rank-and-keep the engine applies to the cross-tab's rows. The AST
    /// value is validated `> 0`; a value above `usize::MAX` saturates,
    /// which trims nothing and is exactly what an enormous `topN` means.
    compare_top_n: usize,
    step_ms: i64,
    /// The INSTANT evaluation window `[aS, aE)` — unchanged by issue #477.
    window: SnappedWindow,
    /// The emitted bucket grid for the range form.
    range_axis: RangeAxis,
    /// The RANGE evaluation window `[aS - step + 1, aE + 1)`, i.e. the
    /// instants `(aS - step, aE]`.
    range_window: SnappedWindow,
    distributed: bool,
    range_sql: String,
    instant_sql: String,
}

impl TraceMetricsPlan {
    pub fn range_sql(&self) -> &str {
        &self.range_sql
    }

    pub fn instant_sql(&self) -> &str {
        &self.instant_sql
    }

    pub fn kind(&self) -> PlanKind {
        self.kind
    }

    /// The `__name__` label value for an ungrouped series.
    pub fn metric_name(&self) -> &str {
        self.metric_name
    }

    /// The grouping label key, if the query is grouped.
    pub fn group_label(&self) -> Option<&str> {
        self.group_label.as_deref()
    }

    /// The requested quantiles (`PlanKind::Quantile`), in request order.
    pub fn quantiles(&self) -> &[f64] {
        &self.quantiles
    }

    /// The second-stage `topk`/`bottomk` reduction, if any.
    pub fn reduce(&self) -> Option<SeriesReduce> {
        self.reduce
    }

    /// The per-bucket exemplar collection SQL, if `with(exemplars=…)` was
    /// requested on a supported (ungrouped rate/count) query.
    pub fn exemplar_sql(&self) -> Option<&str> {
        self.exemplar_sql.as_deref()
    }

    /// The trailing metrics-result comparison post-filter, if present.
    pub fn result_filter(&self) -> Option<(pulsus_traceql::ComparisonOp, f64)> {
        self.result_filter
    }

    /// The compare() range `(cross_tab, totals)` SQL, if this is a compare
    /// plan.
    pub fn compare_range(&self) -> Option<(&str, &str)> {
        self.compare_range
            .as_ref()
            .map(|(c, t)| (c.as_str(), t.as_str()))
    }

    /// The compare() instant `(cross_tab, totals)` SQL, if this is a
    /// compare plan.
    pub fn compare_instant(&self) -> Option<(&str, &str)> {
        self.compare_instant
            .as_ref()
            .map(|(c, t)| (c.as_str(), t.as_str()))
    }

    /// `compare()`'s `topN` — how many distinct values the engine keeps
    /// per attribute PER SIDE (issue #460). Meaningless for a non-compare
    /// plan, where it carries the reference's default.
    pub fn compare_top_n(&self) -> usize {
        self.compare_top_n
    }

    /// The series-cap probe over the RANGE window — consumed by the range
    /// path only. `None` for an ungrouped, non-`compare()` plan.
    ///
    /// It is a SEPARATE probe from [`Self::instant_probe_sql`] because the
    /// two windows differ: the range answer can contain groups that occur
    /// only in the leading step `(aS - step, aS)` or only at exactly `aE`,
    /// and the instant window `[aS, aE)` excludes both. Counting the range
    /// answer's groups with the instant window would let an unbounded
    /// number of extra series past a static guard (issue #477).
    pub fn range_probe_sql(&self) -> Option<&str> {
        self.range_probe_sql.as_deref()
    }

    /// The series-cap probe over the INSTANT window — byte-identical to
    /// the single probe this plan carried before issue #477, for BOTH the
    /// grouped and the `compare()` shape. Instant path only.
    pub fn instant_probe_sql(&self) -> Option<&str> {
        self.instant_probe_sql.as_deref()
    }

    /// The resolved TOTAL exemplar budget for the whole response; `0` is
    /// none, and then [`Self::exemplar_sql`] is `None`.
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

    /// Whether the plan was built against `_dist` tables (mirrors
    /// [`super::search_plan::SearchPlan::distributed`]).
    pub fn distributed(&self) -> bool {
        self.distributed
    }

    /// The snapped, left-closed INSTANT window `[S, E)` in nanoseconds.
    /// Unchanged by issue #477 — the range form reads
    /// [`Self::range_window_ns`].
    pub fn snapped_window_ns(&self) -> (i64, i64) {
        (self.window.start_ns, self.window.end_ns)
    }

    /// The RANGE window `[aS - step + 1, aE + 1)` in nanoseconds — the
    /// integer-nanosecond spelling of the right-closed instant range
    /// `(aS - step, aE]`.
    pub fn range_window_ns(&self) -> (i64, i64) {
        (self.range_window.start_ns, self.range_window.end_ns)
    }

    /// The instant evaluation timestamp (`E`, the snapped right edge) in
    /// milliseconds — what the server hands the Prometheus vector
    /// encoder as `at_ms` (plan v2 delta 5).
    pub fn snapped_end_ms(&self) -> i64 {
        self.window.end_ns / 1_000_000
    }

    /// The snapped INSTANT window width in **fractional seconds** — the
    /// instant `rate` denominator.
    ///
    /// `f64`, not whole seconds: once the step can be sub-second (issue
    /// #477 (d)) a snapped window can be narrower than one second, and the
    /// old truncating `i64` form made the denominator `0`, so `n / 0.0`
    /// encoded as `inf`. Widened through `i128` first: both snapped bounds
    /// fit `i64`, but their difference need not.
    pub(crate) fn window_seconds(&self) -> f64 {
        let width_ns = i128::from(self.window.end_ns) - i128::from(self.window.start_ns);
        width_ns as f64 / NS_PER_S as f64
    }

    /// The range `rate` denominator: one bucket's width in fractional
    /// seconds.
    pub(crate) fn step_seconds(&self) -> f64 {
        self.step_ms as f64 / 1_000.0
    }
}

/// Plans one metrics request. Pure and deterministic — the same inputs
/// always produce byte-identical SQL (the golden-suite contract).
pub fn plan_trace_metrics(
    query: &Query,
    params: &MetricsParams,
    ctx: &MetricsCtx<'_>,
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
    let SpansetExpr::Filter(spanset_filter) = &query.spanset else {
        return Err(PlanError::TypeMismatch(
            "cross-spanset and structural expressions ({A} && {B}, {A} > {B}) are not supported \
             by metrics queries"
                .to_string(),
        ));
    };

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
    // `time_clause`/`date_clause` render — so neither of those moves. One
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

    let filter_sql = metrics_sql::compile_filter_predicate(
        spanset_filter.body.as_ref(),
        ctx.filter.attrs_table,
        window,
    )?;
    // The attribute semi-joins embed the window's own date/time pruning,
    // so the range form needs its own compilation over the range window.
    let range_filter_sql = metrics_sql::compile_filter_predicate(
        spanset_filter.body.as_ref(),
        ctx.filter.attrs_table,
        range_window,
    )?;
    let spans = ctx.filter.spans_table;
    let keys = analysis.keys;
    let (range_sql, instant_sql) = match analysis.kind {
        PlanKind::Count { .. } => (
            metrics_sql::metrics_count_range_sql(
                spans,
                &range_filter_sql,
                range_window,
                params.step_ms,
                &keys,
            ),
            metrics_sql::metrics_count_instant_sql(spans, &filter_sql, window, &keys),
        ),
        PlanKind::Agg(agg) => (
            metrics_sql::metrics_agg_range_sql(
                spans,
                &range_filter_sql,
                range_window,
                params.step_ms,
                agg,
                &keys,
            ),
            metrics_sql::metrics_agg_instant_sql(spans, &filter_sql, window, agg, &keys),
        ),
        PlanKind::Quantile => (
            metrics_sql::metrics_quantile_range_sql(
                spans,
                &range_filter_sql,
                range_window,
                params.step_ms,
                &analysis.quantiles,
            ),
            metrics_sql::metrics_quantile_instant_sql(
                spans,
                &filter_sql,
                window,
                &analysis.quantiles,
            ),
        ),
        PlanKind::Histogram => (
            metrics_sql::metrics_log2_bucket_range_sql(
                spans,
                &range_filter_sql,
                range_window,
                params.step_ms,
            ),
            metrics_sql::metrics_log2_bucket_instant_sql(spans, &filter_sql, window),
        ),
        // compare() serves from its own cross-tab/totals SQL below.
        PlanKind::Compare => (String::new(), String::new()),
    };

    // compare(): build the cross-tab/totals for the range and instant
    // forms, plus the distinct-(key,value) cap probe (reused by
    // `enforce_series_cap`).
    let (compare_range, compare_instant, compare_probes) = if analysis.kind == PlanKind::Compare {
        let inner_bool = metrics_sql::compile_filter_bool(
            analysis
                .compare_selection
                .as_ref()
                .and_then(|f| f.body.as_ref()),
            ctx.filter.attrs_table,
            window,
        )?;
        // The selection predicate embeds the window's own date/time
        // pruning too (visible as the `trace_attrs_idx … timestamp_ns >= …`
        // clause inside `is_sel`), so the range form needs its own.
        let range_inner_bool = metrics_sql::compile_filter_bool(
            analysis
                .compare_selection
                .as_ref()
                .and_then(|f| f.body.as_ref()),
            ctx.filter.attrs_table,
            range_window,
        )?;
        // The fixed well-known-absent-attribute set contributes 4 series
        // per key on top of the data-driven cross-tab; fold it into the
        // cap so the probe bounds the true materialized output count.
        // Issue #189: three well-known keys (`statusMessage`/`rootName`/
        // `rootServiceName`) are now ALSO data-driven when present, so this
        // fixed term conservatively over-counts by ≤4 per such key (its
        // present rows are counted by the probe AND its 4 slots are
        // reserved here). Safe: over-counting can only reject earlier, never
        // under-cap — do not "tighten" it away.
        let fixed_series = 4 * WELL_KNOWN_COMPARE_KEYS.len() as u64;
        // THREE builds, not two (issue #477). The range pair and its probe
        // come from the range window under the right-closed bucket label;
        // the instant pair keeps today's inputs; and the instant PROBE is
        // rebuilt from today's exact inputs — same window, same filter,
        // same selection predicate, and the frozen left-edge bucket
        // expression — because that is the only construction that
        // guarantees its bytes do not move. `metrics_compare_sql` is a
        // pure function of its input, so reproducing the call reproduces
        // the string. Do NOT extract a probe-only builder to avoid the
        // discarded cross-tab/totals: a second code path that renders the
        // probe is a second place for those bytes to drift.
        let range_bucket = metrics_sql::compare_range_bucket_expr(params.step_ms);
        let r = metrics_sql::metrics_compare_sql(&metrics_sql::CompareSqlInput {
            spans_table: spans,
            attrs_table: ctx.filter.attrs_table,
            outer: &range_filter_sql,
            inner_bool: &range_inner_bool,
            window: range_window,
            bucket_expr: &range_bucket,
            cap: ctx.max_series,
            fixed_series,
            sel_window: analysis.compare_window,
        });
        let instant_bucket = (window.end_ns / NS_PER_MS).to_string();
        let i = metrics_sql::metrics_compare_sql(&metrics_sql::CompareSqlInput {
            spans_table: spans,
            attrs_table: ctx.filter.attrs_table,
            outer: &filter_sql,
            inner_bool: &inner_bool,
            window,
            bucket_expr: &instant_bucket,
            cap: ctx.max_series,
            fixed_series,
            sel_window: analysis.compare_window,
        });
        let instant_probe_bucket = metrics_sql::compare_instant_probe_bucket_expr(params.step_ms);
        let ip = metrics_sql::metrics_compare_sql(&metrics_sql::CompareSqlInput {
            spans_table: spans,
            attrs_table: ctx.filter.attrs_table,
            outer: &filter_sql,
            inner_bool: &inner_bool,
            window,
            bucket_expr: &instant_probe_bucket,
            cap: ctx.max_series,
            fixed_series,
            sel_window: analysis.compare_window,
        });
        (
            Some((r.cross_tab, r.totals)),
            Some((i.cross_tab, i.totals)),
            Some((r.probe, ip.probe)),
        )
    } else {
        (None, None, None)
    };

    let (range_probe_sql, instant_probe_sql) = match compare_probes {
        Some((range_probe, instant_probe)) => (Some(range_probe), Some(instant_probe)),
        None if !keys.is_empty() => (
            Some(metrics_sql::metrics_series_probe_sql(
                spans,
                &range_filter_sql,
                range_window,
                &keys,
                ctx.max_series,
            )),
            Some(metrics_sql::metrics_series_probe_sql(
                spans,
                &filter_sql,
                window,
                &keys,
                ctx.max_series,
            )),
        ),
        None => (None, None),
    };

    // Exemplars are collected for EVERY range shape (issue #182 review
    // Fix 1 — the reference emits exemplars for range rate/count/agg/
    // quantile/histogram/compare, and none for instant): the per-bucket
    // sample is taken over the outer filter and attached to the first
    // series (a range's exemplars are concentrated on one series). The
    // instant path never attaches.
    //
    // Issue #477 (c), ONE resolution and not two branches: the `with()`
    // hint wins if present, otherwise the HTTP `exemplars` parameter,
    // otherwise `DEFAULT_EXEMPLARS`. The budget is a TOTAL for the whole
    // response (ruling 1), so the per-bucket sample size is the budget
    // spread across the grid — at least 1, since `groupArraySample(0, …)`
    // would collect nothing anywhere — and the engine thins the collected
    // list down to the budget afterwards.
    let exemplar_budget = match (analysis.exemplar_k, params.exemplars) {
        (Some(k), _) => k.min(MAX_EXEMPLARS),
        (None, Some(p)) => p.min(MAX_EXEMPLARS),
        (None, None) => DEFAULT_EXEMPLARS,
    };
    let exemplar_sql = (exemplar_budget > 0).then(|| {
        let per_bucket_k = (exemplar_budget / range_axis.points as u32).max(1);
        metrics_sql::metrics_exemplar_range_sql(
            spans,
            &range_filter_sql,
            range_window,
            params.step_ms,
            per_bucket_k,
        )
    });

    Ok(TraceMetricsPlan {
        kind: analysis.kind,
        metric_name: analysis.metric_name,
        group_label: keys.first().map(|k| k.label_key.clone()),
        range_probe_sql,
        instant_probe_sql,
        quantiles: analysis.quantiles,
        reduce: analysis.reduce,
        exemplar_sql,
        exemplar_budget,
        result_filter: analysis.result_filter,
        compare_range,
        compare_instant,
        compare_top_n: analysis.compare_top_n,
        step_ms: params.step_ms,
        window,
        range_axis,
        range_window,
        distributed: ctx.distributed,
        range_sql,
        instant_sql,
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
/// ungrouped output, the resolved `by(...)` grouping keys, the optional
/// second-stage reduction, and the optional exemplar sample size.
struct PipelineAnalysis {
    kind: PlanKind,
    metric_name: &'static str,
    keys: Vec<GroupKeySql>,
    quantiles: Vec<f64>,
    reduce: Option<SeriesReduce>,
    exemplar_k: Option<u32>,
    /// The trailing metrics-result comparison (`… > 5`), parsed to `f64`.
    result_filter: Option<(pulsus_traceql::ComparisonOp, f64)>,
    /// The `compare({selection})` inner filter (cloned), if the pipeline is
    /// a compare stage.
    compare_selection: Option<pulsus_traceql::SpansetFilter>,
    /// `compare()`'s `topN` argument (issue #460), saturated into `usize`.
    compare_top_n: usize,
    /// `compare()`'s `(start, end]` selection window in unix nanoseconds,
    /// `None` when the query carries the `(0, 0)` no-window default.
    compare_window: Option<(i64, i64)>,
}

/// Analyzes the metrics pipeline: a first-stage metric function (with
/// optional `by(...)`, `with()`, trailing `> value`, and a `topk`/`bottomk`
/// second stage), or a standalone `compare({selection})` stage.
fn analyze_pipeline(query: &Query) -> Result<PipelineAnalysis, PlanError> {
    // compare() is a standalone metrics stage with its own shape; it
    // accepts `with(...)` hints (e.g. exemplars).
    if let [
        PipelineStage::Compare {
            selection,
            top_n,
            start_ns,
            end_ns,
            hints,
        },
    ] = query.pipeline.as_slice()
    {
        return Ok(PipelineAnalysis {
            kind: PlanKind::Compare,
            metric_name: "compare",
            keys: Vec::new(),
            quantiles: Vec::new(),
            reduce: None,
            exemplar_k: resolve_hints(hints)?,
            result_filter: None,
            compare_selection: Some((**selection).clone()),
            // `validate` guarantees `> 0`; the saturating cast means an
            // enormous topN trims nothing rather than wrapping to zero
            // and trimming everything.
            compare_top_n: usize::try_from(*top_n).unwrap_or(usize::MAX),
            // `(0, 0)` is the reference's "no selection window", and it
            // is also an explicitly legal spelling — both map to `None`,
            // which renders `is_sel` byte-identically to the pre-#460
            // string. `validate` has already rejected every other
            // non-positive combination.
            compare_window: (*start_ns != 0 || *end_ns != 0).then_some((*start_ns, *end_ns)),
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
                 second stage; aggregate filters and select() are search-only"
                    .to_string(),
            ));
        }
    };
    let exemplar_k = resolve_hints(&stage.hints)?;
    let keys = resolve_by_keys(&stage.by)?;
    let (kind, metric_name, quantiles) = resolve_func(&stage.func)?;
    // Quantile/histogram grouping is a follow-up; keep them ungrouped.
    if matches!(kind, PlanKind::Quantile | PlanKind::Histogram) && !keys.is_empty() {
        return Err(PlanError::TypeMismatch(
            "quantile_over_time/histogram_over_time do not yet support by() grouping (issue #182)"
                .to_string(),
        ));
    }
    let result_filter = resolve_result_filter(&stage.result_filter)?;
    Ok(PipelineAnalysis {
        kind,
        metric_name,
        keys,
        quantiles,
        reduce,
        exemplar_k,
        result_filter,
        compare_selection: None,
        // Not a compare plan; the reference's default is carried so the
        // field is never a meaningless zero.
        compare_top_n: COMPARE_DEFAULT_TOP_N_USIZE,
        compare_window: None,
    })
}

/// Parses a trailing metrics-result comparison value to `f64` (a duration
/// literal is compared in seconds, matching the value aggregations'
/// ns→seconds encode scaling).
///
/// Issue #237 (settled — do NOT "fix" this like #232): the reference's
/// ns→seconds conversion is the SINGLE-rounding `float64(ns) / 1e9`
/// this function already uses, not #232's two-rounding form (see
/// `exec.rs::agg_value` for the 17-significant-digit raw-wire evidence
/// of record). Corroboration for this threshold site specifically,
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

/// Resolves the `by(...)` fields to grouping keys. This pass supports
/// exactly one key, `resource.service.name` (the physical `service`
/// column); attribute by-keys and multi-key grouping route to a
/// follow-up (a clean `400`).
fn resolve_by_keys(by: &[Field]) -> Result<Vec<GroupKeySql>, PlanError> {
    match by {
        [] => Ok(Vec::new()),
        [Field::Attribute { scope, key }]
            if *scope == AttrScope::Resource && key == "service.name" =>
        {
            Ok(vec![GroupKeySql {
                col_expr: "service".to_string(),
                label_key: "resource.service.name".to_string(),
            }])
        }
        [_] => Err(PlanError::TypeMismatch(
            "by() currently supports grouping by resource.service.name only (issue #182); \
             attribute grouping keys route to a follow-up"
                .to_string(),
        )),
        _ => Err(PlanError::TypeMismatch(
            "by() currently supports a single grouping key (issue #182)".to_string(),
        )),
    }
}

/// Resolves a metric function to its read-side kind, `__name__`, and (for
/// `quantile_over_time`) the parsed quantile list. Non-duration
/// aggregation targets route to a follow-up with a precise `400`.
fn resolve_func(func: &MetricFn) -> Result<(PlanKind, &'static str, Vec<f64>), PlanError> {
    let no_q = Vec::new();
    match func {
        MetricFn::Rate => Ok((PlanKind::Count { is_rate: true }, "rate", no_q)),
        MetricFn::CountOverTime => {
            Ok((PlanKind::Count { is_rate: false }, "count_over_time", no_q))
        }
        MetricFn::SumOverTime(f) => {
            require_duration_target(f, "sum_over_time")?;
            Ok((PlanKind::Agg(AggFn::Sum), "sum_over_time", no_q))
        }
        MetricFn::MinOverTime(f) => {
            require_duration_target(f, "min_over_time")?;
            Ok((PlanKind::Agg(AggFn::Min), "min_over_time", no_q))
        }
        MetricFn::MaxOverTime(f) => {
            require_duration_target(f, "max_over_time")?;
            Ok((PlanKind::Agg(AggFn::Max), "max_over_time", no_q))
        }
        MetricFn::AvgOverTime(f) => {
            require_duration_target(f, "avg_over_time")?;
            Ok((PlanKind::Agg(AggFn::Avg), "avg_over_time", no_q))
        }
        MetricFn::QuantileOverTime { field, quantiles } => {
            require_duration_target(field, "quantile_over_time")?;
            let qs = parse_quantiles(quantiles)?;
            Ok((PlanKind::Quantile, "quantile_over_time", qs))
        }
        MetricFn::HistogramOverTime(f) => {
            require_duration_target(f, "histogram_over_time")?;
            Ok((PlanKind::Histogram, "histogram_over_time", no_q))
        }
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

/// The `*_over_time` value target this pass supports is the physical
/// `duration` intrinsic; attribute numeric targets route to a follow-up.
fn require_duration_target(field: &Field, func: &str) -> Result<(), PlanError> {
    if matches!(field, Field::Intrinsic(Intrinsic::Duration)) {
        Ok(())
    } else {
        Err(PlanError::TypeMismatch(format!(
            "{func}() currently supports the duration target only (issue #182); attribute value \
             targets route to a follow-up"
        )))
    }
}

#[cfg(test)]
mod tests {
    use pulsus_traceql::parse;

    use super::*;

    fn ctx<'a>() -> MetricsCtx<'a> {
        MetricsCtx {
            filter: SpanFilterCtx {
                spans_table: "trace_spans",
                attrs_table: "trace_attrs_idx",
            },
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
    };

    fn plan(q: &str) -> TraceMetricsPlan {
        plan_trace_metrics(&parse(q).expect("parse"), &PARAMS, &ctx()).expect("plan")
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
                ..PARAMS
            };
            plan_trace_metrics(&parse(q).expect("parse"), &params, &ctx()).expect("plan")
        };
        // No hint, no parameter: exemplars are ON by default.
        let d = plan_ex("{} | rate()", None);
        assert_eq!(d.exemplar_budget(), 100);
        assert!(
            d.exemplar_sql().is_some(),
            "a plain rate() renders exemplar SQL"
        );
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
        assert!(
            off.exemplar_sql().is_none(),
            "a zero budget renders no exemplar SQL"
        );
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
        // The budget is a TOTAL: the per-bucket sample size the SQL asks
        // for is the budget spread over the grid, floored at 1.
        assert!(
            d.exemplar_sql()
                .expect("rendered")
                .contains("groupArraySample(1, 1)"),
            "182 points and a budget of 100 is one sample per bucket: {:?}",
            d.exemplar_sql()
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

    /// Issue #477: the two probes are built over DIFFERENT windows, and
    /// the instant one is the byte-identical pre-change probe.
    #[test]
    fn the_two_series_cap_probes_are_over_different_windows() {
        for q in [
            "{} | rate() by(resource.service.name)",
            r#"{} | compare({ span.http.status_code = "500" })"#,
        ] {
            let p = plan(q);
            let range = p.range_probe_sql().expect("a range probe");
            let instant = p.instant_probe_sql().expect("an instant probe");
            assert_ne!(range, instant, "{q}: the two probes must not be one probe");
            assert!(
                range.contains(
                    "timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001"
                ),
                "{q}: the range probe is over the range window: {range}"
            );
            assert!(
                instant.contains(
                    "timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000"
                ),
                "{q}: the instant probe is over the instant window: {instant}"
            );
            assert!(
                !instant.contains("timestamp_ns >= 1699999920000000001"),
                "{q}: no range bound may leak into the instant probe: {instant}"
            );
        }
        // An ungrouped, non-compare plan renders neither.
        let p = plan("{} | rate()");
        assert!(p.range_probe_sql().is_none());
        assert!(p.instant_probe_sql().is_none());
    }

    #[test]
    fn the_window_snaps_outward_to_epoch_aligned_step_boundaries() {
        let p = plan("{} | rate()");
        // 1_700_000_000 is not a multiple of 60 → S floors to
        // 1_699_999_980; 1_700_010_800 → E ceils to 1_700_010_840.
        assert_eq!(
            p.snapped_window_ns(),
            (1_699_999_980_000_000_000, 1_700_010_840_000_000_000)
        );
        assert_eq!(p.window_seconds(), 10_860.0);
        assert_eq!(p.snapped_end_ms(), 1_700_010_840_000);
    }

    #[test]
    fn an_aligned_window_snaps_to_itself() {
        let params = MetricsParams {
            start_ns: 1_699_999_980_000_000_000,
            end_ns: 1_700_010_840_000_000_000,
            step_ms: 60_000,
            exemplars: None,
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
    fn by_resource_service_name_sets_the_group_label_and_probe() {
        let p = plan("{} | rate() by(resource.service.name)");
        assert_eq!(p.group_label(), Some("resource.service.name"));
        let probe = p
            .instant_probe_sql()
            .expect("grouped query renders an instant probe");
        assert!(probe.contains("GROUP BY g0"), "{probe}");
        assert!(probe.contains("LIMIT 1001"), "cap+1 sentinel: {probe}");
        assert!(p.range_sql().contains("service AS g0"), "{}", p.range_sql());
    }

    #[test]
    fn an_attribute_by_key_is_a_clean_plan_error_for_now() {
        let err = plan_trace_metrics(
            &parse("{} | rate() by(span.route)").unwrap(),
            &PARAMS,
            &ctx(),
        )
        .expect_err("attribute by-keys route to a follow-up");
        assert!(matches!(err, PlanError::TypeMismatch(_)), "{err}");
    }

    #[test]
    fn quantile_and_histogram_plan_to_their_kinds() {
        let quant = plan("{} | quantile_over_time(duration, 0.5, 0.9)");
        assert_eq!(quant.kind(), PlanKind::Quantile);
        assert_eq!(quant.quantiles(), &[0.5, 0.9]);
        assert!(
            quant
                .range_sql()
                .contains("quantilesTDigest(0.5, 0.9)(val)")
        );

        // Issue #252: the histogram is a log2 tally with NO ladder — a
        // `GROUP BY` on the pushed-down `Log2Bucketize`, guarded by the
        // outer sub-2ns drop, and nothing cumulative anywhere.
        let hist = plan("{} | histogram_over_time(duration)");
        assert_eq!(hist.kind(), PlanKind::Histogram);
        for needle in [
            "toUInt64(roundToExp2(val - 1)) * 2 AS bucket",
            "count() AS n",
            "WHERE val >= 2",
            "GROUP BY t, bucket",
        ] {
            assert!(
                hist.range_sql().contains(needle),
                "{needle:?} missing from\n{}",
                hist.range_sql()
            );
        }
        assert!(
            !hist.range_sql().contains("countIf("),
            "the fixed cumulative `le` ladder is gone:\n{}",
            hist.range_sql()
        );
        assert!(hist.instant_sql().contains("GROUP BY bucket"));
        assert!(hist.instant_sql().contains("WHERE val >= 2"));
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
    fn with_sample_is_accepted_and_returns_the_exact_query() {
        // sample is accepted (exact superset) and does not alter the SQL.
        let plain = plan("{} | rate()");
        let sampled = plan("{} | rate() with(sample=0.1)");
        assert_eq!(plain.range_sql(), sampled.range_sql());
        // `sample` says nothing about exemplars, so the two plans resolve
        // the same budget — which since issue #477 (c) is the default,
        // not none.
        assert_eq!(sampled.exemplar_budget(), plain.exemplar_budget());
        assert_eq!(sampled.exemplar_sql(), plain.exemplar_sql());
    }

    #[test]
    fn with_exemplars_renders_the_groupsample_collection_sql() {
        let p = plan("{} | rate() with(exemplars=5)");
        let ex = p
            .exemplar_sql()
            .expect("exemplars requested → collection SQL");
        // The hint is a TOTAL of 5 over a 182-point grid (issue #477 (c)),
        // so the per-bucket sample size floors at 1 and the engine thins
        // the collected list down to 5.
        assert_eq!(p.exemplar_budget(), 5);
        assert!(
            ex.contains("groupArraySample(1, 1)(tuple(trace_id, timestamp_ns))"),
            "{ex}"
        );
        // A budget larger than the grid asks for more per bucket.
        let wide = plan_trace_metrics(
            &parse("{} | rate() with(exemplars=100)").unwrap(),
            &MetricsParams {
                start_ns: 0,
                end_ns: 10 * NS_PER_S,
                step_ms: 1_000,
                exemplars: None,
            },
            &ctx(),
        )
        .expect("plan");
        assert_eq!(wide.range_axis().points, 11);
        assert!(
            wide.exemplar_sql()
                .expect("rendered")
                .contains("groupArraySample(9, 1)"),
            "100 spread over 11 points is 9 per bucket: {:?}",
            wide.exemplar_sql()
        );
    }

    #[test]
    fn exemplars_are_collected_for_every_range_shape() {
        // Review Fix 1: not just ungrouped rate/count — grouped,
        // aggregation, quantile, histogram all collect exemplars for range.
        for q in [
            "{} | rate() by(resource.service.name) with(exemplars=2)",
            "{} | sum_over_time(duration) with(exemplars=2)",
            "{} | quantile_over_time(duration, 0.9) with(exemplars=2)",
            "{} | histogram_over_time(duration) with(exemplars=2)",
        ] {
            assert!(
                plan(q).exemplar_sql().is_some(),
                "{q}: exemplars must be collected for range shapes"
            );
        }
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
    fn compare_plans_to_a_cross_tab_with_a_selection_predicate_and_probe() {
        let p = plan(r#"{} | compare({ span.http.status_code = "500" })"#);
        assert_eq!(p.kind(), PlanKind::Compare);
        let (cross, totals) = p.compare_range().expect("compare range SQL");
        // The cross-tab enumerates intrinsics + index attrs and counts
        // baseline (count()) and selection (countIf(is_sel)).
        assert!(cross.contains("countIf(is_sel) AS sel_n"), "{cross}");
        assert!(cross.contains("arrayJoin(["), "intrinsic pivot: {cross}");
        assert!(
            cross.contains("concat(a.scope, '.', a.key)"),
            "attr pivot: {cross}"
        );
        // Issue #189/#185: the 3 data-driven well-known keys are emitted;
        // an empty statusMessage is emitted as the DISTINCT `""` value (not
        // folded to nil — Tempo v3.0.2 parity), and the roots are resolved
        // by a WINDOW-FREE per-trace argMin LEFT JOIN (no time predicate
        // inside the roots subquery — trace-wide exactness).
        for tuple in [
            "('statusMessage', i_status_message)",
            "('instrumentation:name', i_scope_name)",
            "('instrumentation:version', i_scope_version)",
            "('rootName', r.root_name)",
            "('rootServiceName', r.root_service)",
        ] {
            assert!(cross.contains(tuple), "missing {tuple}: {cross}");
        }
        assert!(
            !cross.contains("arrayFilter"),
            "empty statusMessage is a distinct value, not filtered to nil: {cross}"
        );
        // The roots read scans `trace_spans` keyed ONLY on the DISTINCT
        // in-window trace_id IN-set (no time predicate on its own scan —
        // trace-wide exactness) and is LEFT JOINed into the intrinsics
        // branch. The byte-exact window-free render is pinned by the golden.
        assert!(
            cross.contains("argMin(if(length(name)")
                && cross.contains("AS root_name")
                && cross.contains("AS root_service"),
            "window-free roots argMin projections: {cross}"
        );
        assert!(
            cross.contains("WHERE trace_id IN (SELECT DISTINCT trace_id FROM")
                && cross.contains("LEFT JOIN"),
            "roots resolved over the DISTINCT trace_id IN-set, LEFT JOINed: {cross}"
        );
        // The selection predicate is the inner filter compiled to a bool.
        assert!(cross.contains("key = 'http.status_code'"), "{cross}");
        assert!(totals.contains("countIf(is_sel) AS sel_total"), "{totals}");
        // The distinct-(key,value) cap probe is reused by the engine.
        let probe = p
            .instant_probe_sql()
            .expect("compare renders an instant cap probe");
        assert!(probe.contains("GROUP BY akey, aval"), "{probe}");
        assert!(probe.contains("LIMIT 1001"), "cap+1: {probe}");
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
    /// `ns as f64 / 1e9`, the exact expression `exec.rs::agg_value`
    /// applies to a stored span of that width) is what makes the
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
    /// instant form keeps `[aS, aE)` byte for byte; the range form reads
    /// `(aS - step, aE]`, spelled over integer nanoseconds as
    /// `[aS - step + 1, aE + 1)`.
    #[test]
    fn the_generated_sql_carries_each_forms_own_window_bounds() {
        let p = plan("{} | rate()");
        assert!(
            p.range_sql().contains(
                "WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001"
            ),
            "{}",
            p.range_sql()
        );
        assert!(
            p.instant_sql().contains(
                "WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000"
            ),
            "{}",
            p.instant_sql()
        );
        assert_eq!(
            p.range_window_ns(),
            (1_699_999_920_000_000_001, 1_700_010_840_000_000_001)
        );
        assert_eq!(
            p.snapped_window_ns(),
            (1_699_999_980_000_000_000, 1_700_010_840_000_000_000)
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
        };
        assert!(plan_trace_metrics(&parse("{} | rate()").unwrap(), &params, &ctx()).is_ok());
    }

    #[test]
    fn an_invalid_regex_fails_at_plan_time_not_execution() {
        let err = plan_trace_metrics(
            &parse(r#"{ .k =~ "(" } | rate()"#).unwrap(),
            &PARAMS,
            &ctx(),
        )
        .expect_err("bad regex");
        assert!(matches!(err, PlanError::TypeMismatch(_)), "{err}");
    }

    #[test]
    fn clustered_ctx_switches_tables_and_the_distributed_flag() {
        let clustered = MetricsCtx {
            filter: SpanFilterCtx {
                spans_table: "trace_spans_dist",
                attrs_table: "trace_attrs_idx_dist",
            },
            scan_budget_rows: 50_000_000,
            max_series: 1_000,
            distributed: true,
            skip_unavailable_shards: false,
        };
        let p = plan_trace_metrics(
            &parse(r#"{ span.a = "1" } | rate()"#).unwrap(),
            &PARAMS,
            &clustered,
        )
        .unwrap();
        assert!(p.range_sql().contains("FROM trace_spans_dist\n"));
        assert!(p.range_sql().contains("FROM trace_attrs_idx_dist WHERE"));
        assert!(p.distributed());
    }
}
