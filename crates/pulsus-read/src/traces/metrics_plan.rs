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
/// task-manager adjudication 3): `step_s = max(1, ⌊(end_s − start_s) /
/// DEFAULT_METRICS_POINTS⌋)`. The derivation itself runs server-side in
/// `parse_metrics_params`; the constant lives here as the committed
/// contract's single source.
pub const DEFAULT_METRICS_POINTS: i64 = 100;

/// The hard bucket-count cap (docs/api.md §4.4): a snapped range
/// resolving more buckets is rejected statically with `422
/// query_too_broad` — bounded response, no silent truncation (the
/// adjudicated contract; deliberately 422, not Prometheus's 400).
pub const MAX_METRICS_POINTS: i64 = 11_000;

const NS_PER_S: i64 = 1_000_000_000;

/// The caller-validated request window and step. `step_s` is whole
/// seconds, already defaulted by the server's derivation formula when
/// the request omitted `step`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsParams {
    pub start_ns: i64,
    pub end_ns: i64,
    pub step_s: i64,
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
/// divides the deduped count by `step_s` client-side at the encode
/// boundary, `count_over_time` is the count itself).
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
    /// `histogram_over_time(duration)` — one cumulative-count series per
    /// exponential `le` bucket (`__bucket=<le seconds>` label).
    Histogram,
    /// `compare({selection})` — baseline/selection attribute meta-series
    /// (`__meta_type` + one attribute label). The cross-tab/totals SQL is
    /// carried on the plan.
    Compare,
}

/// The fixed exponential power-of-two nanosecond `le` boundaries for
/// `histogram_over_time` (issue #182, OQ4). Captured to match the Tempo
/// v3.0.2 `__bucket` convention (power-of-two nanoseconds rendered as
/// float seconds — e.g. `2^30 ns = 1.073741824`); exact
/// boundary/membership value parity vs Tempo is Tier-2 (issue #25). The
/// series count is fixed (bounded), so no cardinality probe applies.
pub const HISTOGRAM_LE_BOUNDS_NS: &[i64] = &[
    1 << 10, // ~1.02µs
    1 << 13,
    1 << 16,
    1 << 19,
    1 << 22, // ~4.19ms
    1 << 25,
    1 << 28,
    1 << 30, // ~1.07s
    1 << 31,
    1 << 32,
    1 << 34,
    1 << 36,
    1 << 38,
    1 << 40, // ~1099s
];

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
    /// The distinct-by-key series-cardinality probe SQL, rendered only for
    /// a grouped query; the engine runs it before the main query.
    probe_sql: Option<String>,
    /// The requested quantiles (`PlanKind::Quantile` only), in request
    /// order — one output series per entry (`p=<q>` label).
    quantiles: Vec<f64>,
    /// The optional second-stage `topk`/`bottomk` reduction, applied
    /// client-side per timestamp after the series are framed.
    reduce: Option<SeriesReduce>,
    /// The per-bucket exemplar collection SQL (issue #182 P5), rendered
    /// when `with(exemplars=…)` is present on an ungrouped rate/count
    /// query; the engine runs it and attaches `trace:id` exemplars.
    exemplar_sql: Option<String>,
    /// A trailing `metrics-result comparison` post-filter (`… > 5`, issue
    /// #182 P6b): keeps only samples satisfying `<op> <value>`. Applied
    /// client-side after the series are framed.
    result_filter: Option<(pulsus_traceql::ComparisonOp, f64)>,
    /// `compare()` cross-tab + totals SQL, `(cross_tab, totals)` for the
    /// range and instant forms (`PlanKind::Compare` only).
    compare_range: Option<(String, String)>,
    compare_instant: Option<(String, String)>,
    step_s: i64,
    window: SnappedWindow,
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

    /// The histogram `le` boundaries in nanoseconds (`PlanKind::Histogram`).
    pub fn histogram_le_bounds_ns(&self) -> &[i64] {
        HISTOGRAM_LE_BOUNDS_NS
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

    /// The distinct-by-key series-cardinality probe SQL (grouped queries
    /// only); the engine runs it before the main query and 422s on a
    /// `cap+1` result.
    pub fn probe_sql(&self) -> Option<&str> {
        self.probe_sql.as_deref()
    }

    pub fn step_s(&self) -> i64 {
        self.step_s
    }

    /// Whether the plan was built against `_dist` tables (mirrors
    /// [`super::search_plan::SearchPlan::distributed`]).
    pub fn distributed(&self) -> bool {
        self.distributed
    }

    /// The snapped, left-closed window `[S, E)` in nanoseconds.
    pub fn snapped_window_ns(&self) -> (i64, i64) {
        (self.window.start_ns, self.window.end_ns)
    }

    /// The instant evaluation timestamp (`E`, the snapped right edge) in
    /// milliseconds — what the server hands the Prometheus vector
    /// encoder as `at_ms` (plan v2 delta 5).
    pub fn snapped_end_ms(&self) -> i64 {
        self.window.end_ns / 1_000_000
    }

    /// The snapped window width in whole seconds — the instant `rate`
    /// denominator. Widened through `i128`: both snapped bounds fit
    /// `i64`, but their *difference* need not (extreme accepted windows
    /// under a large step — code review round 1).
    pub(crate) fn window_s(&self) -> i64 {
        let width_s = (i128::from(self.window.end_ns) - i128::from(self.window.start_ns))
            / i128::from(NS_PER_S);
        i64::try_from(width_s).unwrap_or(i64::MAX)
    }
}

/// Plans one metrics request. Pure and deterministic — the same inputs
/// always produce byte-identical SQL (the golden-suite contract).
pub fn plan_trace_metrics(
    query: &Query,
    params: &MetricsParams,
    ctx: &MetricsCtx<'_>,
) -> Result<TraceMetricsPlan, PlanError> {
    if params.step_s < 1 {
        return Err(PlanError::TypeMismatch(
            "step must be a positive whole number of seconds".to_string(),
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
    let step_ns = i128::from(params.step_s) * i128::from(NS_PER_S);
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

    let filter_sql = metrics_sql::compile_filter_predicate(
        spanset_filter.body.as_ref(),
        ctx.filter.attrs_table,
        window,
    )?;
    let spans = ctx.filter.spans_table;
    let keys = analysis.keys;
    let (range_sql, instant_sql) = match analysis.kind {
        PlanKind::Count { .. } => (
            metrics_sql::metrics_count_range_sql(spans, &filter_sql, window, params.step_s, &keys),
            metrics_sql::metrics_count_instant_sql(spans, &filter_sql, window, &keys),
        ),
        PlanKind::Agg(agg) => (
            metrics_sql::metrics_agg_range_sql(
                spans,
                &filter_sql,
                window,
                params.step_s,
                agg,
                &keys,
            ),
            metrics_sql::metrics_agg_instant_sql(spans, &filter_sql, window, agg, &keys),
        ),
        PlanKind::Quantile => (
            metrics_sql::metrics_quantile_range_sql(
                spans,
                &filter_sql,
                window,
                params.step_s,
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
            metrics_sql::metrics_histogram_range_sql(
                spans,
                &filter_sql,
                window,
                params.step_s,
                HISTOGRAM_LE_BOUNDS_NS,
            ),
            metrics_sql::metrics_histogram_instant_sql(
                spans,
                &filter_sql,
                window,
                HISTOGRAM_LE_BOUNDS_NS,
            ),
        ),
        // compare() serves from its own cross-tab/totals SQL below.
        PlanKind::Compare => (String::new(), String::new()),
    };

    // compare(): build the cross-tab/totals for the range and instant
    // forms, plus the distinct-(key,value) cap probe (reused by
    // `enforce_series_cap`).
    let (compare_range, compare_instant, compare_probe) = if analysis.kind == PlanKind::Compare {
        let inner_bool = metrics_sql::compile_filter_bool(
            analysis
                .compare_selection
                .as_ref()
                .and_then(|f| f.body.as_ref()),
            ctx.filter.attrs_table,
            window,
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
        let range_bucket = metrics_sql::compare_range_bucket_expr(params.step_s);
        let r = metrics_sql::metrics_compare_sql(&metrics_sql::CompareSqlInput {
            spans_table: spans,
            attrs_table: ctx.filter.attrs_table,
            outer: &filter_sql,
            inner_bool: &inner_bool,
            window,
            bucket_expr: &range_bucket,
            cap: ctx.max_series,
            fixed_series,
        });
        let instant_bucket = (window.end_ns / 1_000_000).to_string();
        let i = metrics_sql::metrics_compare_sql(&metrics_sql::CompareSqlInput {
            spans_table: spans,
            attrs_table: ctx.filter.attrs_table,
            outer: &filter_sql,
            inner_bool: &inner_bool,
            window,
            bucket_expr: &instant_bucket,
            cap: ctx.max_series,
            fixed_series,
        });
        (
            Some((r.cross_tab, r.totals)),
            Some((i.cross_tab, i.totals)),
            Some(r.probe),
        )
    } else {
        (None, None, None)
    };

    let probe_sql = compare_probe.or_else(|| {
        keys.first().map(|_| {
            metrics_sql::metrics_series_probe_sql(spans, &filter_sql, window, &keys, ctx.max_series)
        })
    });

    // Exemplars are collected for EVERY range shape (issue #182 review
    // Fix 1 — Tempo emits exemplars for range rate/count/agg/quantile/
    // histogram/compare, and none for instant): the per-bucket sample is
    // taken over the outer filter and attached to the first series (Tempo
    // concentrates a range's exemplars on one series). The instant path
    // never attaches (matching Tempo — verified black-box).
    let exemplar_sql = analysis.exemplar_k.map(|k| {
        metrics_sql::metrics_exemplar_range_sql(spans, &filter_sql, window, params.step_s, k)
    });

    Ok(TraceMetricsPlan {
        kind: analysis.kind,
        metric_name: analysis.metric_name,
        group_label: keys.first().map(|k| k.label_key.clone()),
        probe_sql,
        quantiles: analysis.quantiles,
        reduce: analysis.reduce,
        exemplar_sql,
        result_filter: analysis.result_filter,
        compare_range,
        compare_instant,
        step_s: params.step_s,
        window,
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

/// The default per-bucket exemplar sample size when `with(exemplars=true)`
/// carries no explicit count. Bounded (see [`MAX_EXEMPLARS_PER_BUCKET`]).
pub const DEFAULT_EXEMPLARS_PER_BUCKET: u32 = 1;

/// The hard per-bucket exemplar cap — a `with(exemplars=N)` is clamped to
/// it so exemplar collection can never blow the scan/response budget.
pub const MAX_EXEMPLARS_PER_BUCKET: u32 = 100;

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
}

/// Analyzes the metrics pipeline: a first-stage metric function (with
/// optional `by(...)`, `with()`, trailing `> value`, and a `topk`/`bottomk`
/// second stage), or a standalone `compare({selection})` stage.
fn analyze_pipeline(query: &Query) -> Result<PipelineAnalysis, PlanError> {
    // compare() is a standalone metrics stage with its own shape; it
    // accepts `with(...)` hints (e.g. exemplars).
    if let [PipelineStage::Compare { selection, hints }] = query.pipeline.as_slice() {
        return Ok(PipelineAnalysis {
            kind: PlanKind::Compare,
            metric_name: "compare",
            keys: Vec::new(),
            quantiles: Vec::new(),
            reduce: None,
            exemplar_k: resolve_hints(hints)?,
            result_filter: None,
            compare_selection: Some((**selection).clone()),
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
    })
}

/// Parses a trailing metrics-result comparison value to `f64` (a duration
/// literal is compared in seconds, matching the value aggregations'
/// ns→seconds encode scaling).
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
/// routes to #25. `exemplars=<true|N>` requests per-bucket exemplar
/// collection, clamped to [`MAX_EXEMPLARS_PER_BUCKET`]. Other hints
/// (e.g. `most_recent`) are accepted and ignored (a valid superset), never
/// a `400`.
fn resolve_hints(hints: &[pulsus_traceql::MetricHint]) -> Result<Option<u32>, PlanError> {
    use pulsus_traceql::HintValue;
    let mut exemplar_k: Option<u32> = None;
    for hint in hints {
        if hint.key == "exemplars" {
            let k = match &hint.value {
                HintValue::Bool(true) => DEFAULT_EXEMPLARS_PER_BUCKET,
                HintValue::Bool(false) => continue,
                HintValue::Number(raw) => raw
                    .parse::<f64>()
                    .ok()
                    .filter(|n| *n >= 0.0)
                    .map(|n| (n as u32).clamp(1, MAX_EXEMPLARS_PER_BUCKET))
                    .ok_or_else(|| {
                        PlanError::TypeMismatch(format!("invalid exemplars count {raw:?}"))
                    })?,
                _ => {
                    return Err(PlanError::TypeMismatch(
                        "exemplars must be a boolean or a number".to_string(),
                    ));
                }
            };
            exemplar_k = Some(k.min(MAX_EXEMPLARS_PER_BUCKET));
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
        step_s: 60,
    };

    fn plan(q: &str) -> TraceMetricsPlan {
        plan_trace_metrics(&parse(q).expect("parse"), &PARAMS, &ctx()).expect("plan")
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
        assert_eq!(p.window_s(), 10_860);
        assert_eq!(p.snapped_end_ms(), 1_700_010_840_000);
    }

    #[test]
    fn an_aligned_window_snaps_to_itself() {
        let params = MetricsParams {
            start_ns: 1_699_999_980_000_000_000,
            end_ns: 1_700_010_840_000_000_000,
            step_s: 60,
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
        let probe = p.probe_sql().expect("grouped query renders a probe");
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

        let hist = plan("{} | histogram_over_time(duration)");
        assert_eq!(hist.kind(), PlanKind::Histogram);
        assert!(
            hist.range_sql().contains("countIf(val <= "),
            "{}",
            hist.range_sql()
        );
        assert_eq!(
            hist.histogram_le_bounds_ns().len(),
            HISTOGRAM_LE_BOUNDS_NS.len()
        );
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
        assert!(sampled.exemplar_sql().is_none());
    }

    #[test]
    fn with_exemplars_renders_the_groupsample_collection_sql() {
        let p = plan("{} | rate() with(exemplars=5)");
        let ex = p
            .exemplar_sql()
            .expect("exemplars requested → collection SQL");
        assert!(
            ex.contains("groupArraySample(5, 1)(tuple(trace_id, timestamp_ns))"),
            "{ex}"
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
        assert!(
            cross.contains("arrayJoin(arrayFilter("),
            "intrinsic pivot: {cross}"
        );
        assert!(
            cross.contains("concat(a.scope, '.', a.key)"),
            "attr pivot: {cross}"
        );
        // Issue #189: the 3 data-driven well-known keys are emitted, with an
        // empty statusMessage folded to the nil complement, and the roots
        // resolved by a WINDOW-FREE per-trace argMin LEFT JOIN (no time
        // predicate inside the roots subquery — trace-wide exactness).
        for tuple in [
            "('statusMessage', i_status_message)",
            "('rootName', r.root_name)",
            "('rootServiceName', r.root_service)",
        ] {
            assert!(cross.contains(tuple), "missing {tuple}: {cross}");
        }
        assert!(
            cross.contains("NOT (x.1 = 'statusMessage' AND x.2 = '')"),
            "empty statusMessage → nil complement: {cross}"
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
        let probe = p.probe_sql().expect("compare renders a cap probe");
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

    #[test]
    fn the_generated_sql_carries_the_snapped_left_closed_bounds() {
        let p = plan("{} | rate()");
        assert!(p.range_sql().contains(
            "WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000"
        ));
        assert!(p.instant_sql().contains(
            "WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000"
        ));
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
        for step_s in [0, -60] {
            let params = MetricsParams { step_s, ..PARAMS };
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
            step_s: 60,
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
            step_s: 1,
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
            step_s: 60,
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
            step_s: 60,
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
            step_s: 1,
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
            step_s: 1,
        };
        let err = plan_trace_metrics(&parse("{} | rate()").unwrap(), &params, &ctx())
            .expect_err("must reject, not panic");
        assert!(matches!(err, PlanError::MetricsPointCap { .. }), "{err}");
    }

    #[test]
    fn a_step_whose_nanos_exceed_i64_is_a_clean_400_not_a_panic() {
        // step_s = i64::MAX: step_ns only exists in i128; the snapped end
        // (one whole step) cannot fit the storable i64 range → 400.
        let params = MetricsParams {
            step_s: i64::MAX,
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
            step_s: 2_000_000,
        };
        let p = plan_trace_metrics(&parse("{} | rate()").unwrap(), &params, &ctx())
            .expect("8000 buckets is under the cap");
        assert_eq!(p.window_s(), 16_000_000_000);
    }

    #[test]
    fn exactly_the_point_cap_plans() {
        let params = MetricsParams {
            start_ns: 0,
            end_ns: MAX_METRICS_POINTS * 1_000_000_000,
            step_s: 1,
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
