//! Pure `Query + MetricsParams + MetricsCtx → TraceMetricsPlan` planning
//! for the TraceQL metrics endpoints (issue #59; docs/api.md §4.4).
//! Deterministic, no I/O: validates the M4 metrics shape (single
//! spanset, exactly one metric stage), snaps the window to epoch-aligned
//! step boundaries (plan v2 delta 2), enforces the adjudicated point
//! cap, and renders both byte-frozen SQL forms via
//! [`super::metrics_sql`]. Every rejection is a [`PlanError`]: `400
//! bad_data` server-side, except [`PlanError::MetricsPointCap`] — the
//! adjudicated static pre-execution `422 query_too_broad`.

use pulsus_traceql::{MetricFn, PipelineStage, Query, SpansetExpr};

use super::filter::{PlanError, SpanFilterCtx};
use super::metrics_sql::{self, SnappedWindow};

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

/// The complete, deterministic metrics plan — both SQL forms are
/// byte-frozen (`tests/traces_metrics_sql.rs`).
#[derive(Debug, Clone)]
pub struct TraceMetricsPlan {
    func: MetricFunc,
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

    pub fn func(&self) -> MetricFunc {
        self.func
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

    let func = single_metric_stage(query)?;

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
    let range_sql =
        metrics_sql::metrics_range_sql(ctx.filter.spans_table, &filter_sql, window, params.step_s);
    let instant_sql = metrics_sql::metrics_instant_sql(ctx.filter.spans_table, &filter_sql, window);

    Ok(TraceMetricsPlan {
        func,
        step_s: params.step_s,
        window,
        distributed: ctx.distributed,
        range_sql,
        instant_sql,
    })
}

/// The M4 metrics pipeline shape: exactly one stage, and it is the
/// metric function.
fn single_metric_stage(query: &Query) -> Result<MetricFunc, PlanError> {
    match query.pipeline.as_slice() {
        [PipelineStage::Metric(func)] => Ok(match func {
            MetricFn::Rate => MetricFunc::Rate,
            MetricFn::CountOverTime => MetricFunc::CountOverTime,
        }),
        [] => Err(PlanError::TypeMismatch(
            "a metrics query requires a metrics function stage: rate() or count_over_time()"
                .to_string(),
        )),
        _ => Err(PlanError::TypeMismatch(
            "a metrics query takes exactly one pipeline stage (rate() or count_over_time()); \
             aggregate filters and select() are search-only"
                .to_string(),
        )),
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
    fn rate_and_count_over_time_map_to_their_funcs() {
        assert_eq!(plan("{} | rate()").func(), MetricFunc::Rate);
        assert_eq!(
            plan("{} | count_over_time()").func(),
            MetricFunc::CountOverTime
        );
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
