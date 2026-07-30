//! Issue M6-10 hermetic goldens (AC4/AC4a/AC4c): hand-derived
//! expectations for the client-side over-time reducers, unwrap-error
//! semantics (live-probed against the pinned oracle — the transcript
//! values are inlined next to each pin), `absent_over_time`'s
//! selector-wide cardinality, `topk`/`bottomk` selection + tie-break,
//! and binary operations in BOTH operand orientations. No database:
//! everything drives the same pure functions the engine executes
//! (`run_client_agg_rows` / `apply_vector_aggs` / `combine_binary`).

use std::collections::HashMap;

use pulsus_logql::{BinOp, parse};
use pulsus_read::logql::rows::{MetricScanRow, StreamMetaRow};
use pulsus_read::logql::{
    ClientWindow, CompiledPipeline, Direction, MatrixSeries, MetricNode, MetricPlan, Plan, PlanCtx,
    QueryParams, QueryResult, ReadError, SAMPLE_EXTRACTION_ERROR, TooBroadReason, VectorSample,
    apply_vector_aggs, combine_binary, materialize_vector_lit, plan, run_client_agg_rows,
    run_client_agg_rows_folded, run_variants_rows,
};

/// Issue #236 §4: `apply_vector_aggs` is fallible now — it charges the
/// stage's modelled bytes against `MAX_POST_AGG_BYTES` before it
/// allocates. Every fixture in this suite is orders of magnitude below
/// that cap, so a refusal here means the model or the charge is wrong,
/// not that the fixture is too big; the panic says so.
fn apply_vector_aggs_ok(
    result: QueryResult,
    aggs: &[pulsus_read::logql::plan::VectorAggSpec],
) -> QueryResult {
    apply_vector_aggs(result, aggs).expect("a golden-suite fixture is far below MAX_POST_AGG_BYTES")
}

fn ctx() -> PlanCtx<'static> {
    PlanCtx {
        db: "pulsus",
        streams_idx: "log_streams_idx",
        streams: "log_streams",
        samples: "log_samples",
        rollup_table: "log_metrics_5s",
        rollup_res_ns: 5_000_000_000,
        scan_budget_bytes: 50 * 1024 * 1024 * 1024,
        max_streams: 100_000,
        pipeline_scan_factor: 10,
    }
}

const STEP: i64 = 60_000_000_000; // 60s
const NS: i64 = 1_000_000_000;

fn range_params(start_ns: i64, end_ns: i64) -> QueryParams {
    QueryParams {
        spec: pulsus_read::logql::QuerySpec::Range {
            start_ns,
            end_ns,
            step_ns: STEP as u64,
        },
        limit: 100,
        direction: Direction::Backward,
    }
}

fn instant_params(at_ns: i64) -> QueryParams {
    QueryParams {
        spec: pulsus_read::logql::QuerySpec::Instant { at_ns },
        limit: 100,
        direction: Direction::Backward,
    }
}

fn metric_plan_of(query: &str, params: &QueryParams) -> MetricPlan {
    let expr = parse(query).expect("parse");
    match plan(&expr, params, &ctx()).expect("plan") {
        Plan::Metric(mp) => mp,
        other => panic!("expected a Metric plan, got {other:?}"),
    }
}

fn meta_one() -> HashMap<u64, StreamMetaRow> {
    HashMap::from([(
        1u64,
        StreamMetaRow {
            fingerprint: 1,
            service: "checkout".to_string(),
            labels: r#"{"env":"prod","service_name":"checkout"}"#.to_string(),
        },
    )])
}

fn meta_two() -> HashMap<u64, StreamMetaRow> {
    let mut m = meta_one();
    m.insert(
        2u64,
        StreamMetaRow {
            fingerprint: 2,
            service: "billing".to_string(),
            labels: r#"{"env":"prod","service_name":"billing"}"#.to_string(),
        },
    );
    m
}

fn row(fp: u64, ts_ns: i64, body: &str) -> MetricScanRow {
    MetricScanRow {
        fingerprint: fp,
        timestamp_ns: ts_ns,
        body: body.to_string(),
    }
}

/// One series in the EXACT-comparison rendering: its label set and its
/// points with every value as raw bits.
type BitSeries = (Vec<(String, String)>, Vec<(i64, u64)>);

/// A `QueryResult` rendered for EXACT comparison: series sorted, every
/// value as `f64::to_bits` (no tolerance, and `2` never a prefix of
/// `2.5`). Sorting is over the whole `(labels, points)` tuple so two
/// series sharing a label set still order deterministically.
fn bit_canonical(result: &QueryResult) -> Vec<BitSeries> {
    let mut out: Vec<BitSeries> = match result {
        QueryResult::Matrix(items) => items
            .iter()
            .map(|s| {
                (
                    s.labels.clone(),
                    s.points.iter().map(|(t, v)| (*t, v.to_bits())).collect(),
                )
            })
            .collect(),
        QueryResult::Vector(items) => items
            .iter()
            .map(|s| (s.labels.clone(), vec![(0i64, s.value.to_bits())]))
            .collect(),
        other => panic!("bit_canonical: unsupported result {other:?}"),
    };
    out.sort();
    out
}

/// Runs the full client-aggregated path for `query` over `rows`: plan →
/// compile → aggregate → vector aggs — exactly the engine's post-fetch
/// sequence.
///
/// **Issue #236 AC 7 rides here — on every fixture that goes THROUGH
/// this helper, which is not the same as every fixture in the file.** The
/// engine folds the innermost vector aggregation at the range leaf
/// (`run_client_agg_rows_folded`) instead of materialising the leaf's
/// output and aggregating it afterwards (`run_client_agg_rows` +
/// `apply_vector_aggs`). Those two must agree BIT FOR BIT — the fold
/// changes memory, not values — so this helper runs both and asserts it,
/// then returns the folded one (what the engine actually returns). Any
/// fixture routed here is an equivalence case automatically; there is no
/// list of "the range fixtures" to keep in step.
///
/// **The scope, stated because the earlier wording overstated it**
/// (review round 1 `[low]`): fixtures that drive the engine by another
/// door — `materialize_vector_lit` for `vector(...)`, the direct
/// `apply_vector_aggs` reducer/selection goldens, the `combine_binary`
/// cases and the `run_variants_rows` fan-out — never reach the folded
/// leaf, so there is no second path to compare them against and they
/// execute no bit-equality assertion here.
/// `every_client_routed_fixture_is_an_equivalence_case` counts what does
/// ride, so the claim and the mechanism cannot drift apart.
fn run_client(
    query: &str,
    params: &QueryParams,
    rows: &[MetricScanRow],
    meta: &HashMap<u64, StreamMetaRow>,
) -> Result<QueryResult, ReadError> {
    let mp = metric_plan_of(query, params);
    let client = mp.client.as_ref().expect("client-aggregated plan");
    let compiled = CompiledPipeline::compile(&client.pipeline).expect("compile");
    let window = match mp.step_ns {
        Some(step_ns) => ClientWindow::Range {
            grid_start_ns: mp.grid_start_ns,
            end_ns: mp.end_ns,
            step_ns,
            range_ns: mp.range_ns,
        },
        None => ClientWindow::Instant {
            start_ns: mp.grid_start_ns,
            end_ns: mp.end_ns,
        },
    };
    let materialised =
        run_client_agg_rows(rows, &compiled, meta, client, window, mp.rate_window_ns)
            .map(|r| apply_vector_aggs_ok(r, &mp.vector_aggs));
    let folded = run_client_agg_rows_folded(
        rows,
        &compiled,
        meta,
        client,
        window,
        mp.rate_window_ns,
        &mp.vector_aggs,
    );
    match (&folded, &materialised) {
        (Ok(f), Ok(m)) => assert_eq!(
            bit_canonical(f),
            bit_canonical(m),
            "issue #236 AC 7: the folded leaf and the materialising path \
             must agree bit for bit — {query}"
        ),
        (Err(f), Err(m)) => assert_eq!(
            f.to_string(),
            m.to_string(),
            "issue #236 AC 7: both paths must refuse the same way — {query}"
        ),
        (f, m) => panic!(
            "issue #236 AC 7: the folded leaf and the materialising path \
             disagree on admission — {query}\n  folded: {f:?}\n  materialised: {m:?}"
        ),
    }
    folded
}

/// One series expected: returns its points sorted by step.
fn single_series_points(result: QueryResult) -> Vec<(i64, f64)> {
    let QueryResult::Matrix(mut items) = result else {
        panic!("expected a matrix, got {result:?}");
    };
    assert_eq!(items.len(), 1, "expected exactly one series: {items:?}");
    items.remove(0).points
}

fn single_vector_value(result: QueryResult) -> f64 {
    let QueryResult::Vector(items) = result else {
        panic!("expected a vector, got {result:?}");
    };
    assert_eq!(items.len(), 1, "expected exactly one sample: {items:?}");
    items[0].value
}

// ---------------------------------------------------------------------
// AC4: the over-time reducers over hand-built buckets. Bodies `v=<n>`
// with the SAME label shape collapse (post-unwrap deletion of `v`) into
// one series; bucket 0 holds {1,2}, bucket 60s holds {3,4}.
// ---------------------------------------------------------------------

fn unwrap_rows() -> Vec<MetricScanRow> {
    vec![
        row(1, 10 * NS, "v=1"),
        row(1, 20 * NS, "v=2"),
        row(1, 70 * NS, "v=3"),
        row(1, 80 * NS, "v=4"),
    ]
}

#[test]
fn every_unwrap_reducer_matches_its_hand_derived_buckets() {
    // Issue #227 sliding: grid `{0, 60s, 120s}`, window `(t-60s, t]`. t=0 is
    // empty (gap, no point); t=60s sees the rows at 10s/20s (the old
    // "bucket 0"); t=120s sees 70s/80s (the old "bucket 60"). So the same
    // hand-derived values now emit one grid point LATER.
    let params = range_params(0, 2 * STEP);
    for (op, b0, b60) in [
        ("sum_over_time", 3.0, 7.0),
        ("avg_over_time", 1.5, 3.5),
        ("min_over_time", 1.0, 3.0),
        ("max_over_time", 2.0, 4.0),
        // Population stddev/stdvar (oracle-probed: /n, never /(n-1)).
        ("stddev_over_time", 0.5, 0.5),
        ("stdvar_over_time", 0.25, 0.25),
        // first/last are the endpoints of the canonical window order.
        ("first_over_time", 1.0, 3.0),
        ("last_over_time", 2.0, 4.0),
    ] {
        let query = format!(r#"{op}({{env="prod"}} | logfmt | unwrap v [1m])"#);
        let points =
            single_series_points(run_client(&query, &params, &unwrap_rows(), &meta_one()).unwrap());
        assert_eq!(points, vec![(STEP, b0), (2 * STEP, b60)], "{op}");
    }
}

#[test]
fn rate_over_an_unwrapped_range_is_the_per_second_sum() {
    // Oracle-probed semantic: rate + unwrap = sum(values) / window
    // seconds (bucket 0: (1+2)/60).
    let params = range_params(0, 2 * STEP);
    let points = single_series_points(
        run_client(
            r#"rate({env="prod"} | logfmt | unwrap v [1m])"#,
            &params,
            &unwrap_rows(),
            &meta_one(),
        )
        .unwrap(),
    );
    // Issue #227 sliding: emits at t=60s and t=120s (the empty t=0 is a gap).
    assert_eq!(points, vec![(STEP, 3.0 / 60.0), (2 * STEP, 7.0 / 60.0)]);
}

#[test]
fn quantile_over_time_interpolates_linearly_like_the_oracle() {
    // Oracle transcript: quantile 0.5 over {1,2,3,4} = 2.5; 0.9 = 3.7.
    let rows = vec![
        row(1, 10 * NS, "v=1"),
        row(1, 20 * NS, "v=2"),
        row(1, 30 * NS, "v=3"),
        row(1, 40 * NS, "v=4"),
    ];
    let params = instant_params(60 * NS);
    let v = single_vector_value(
        run_client(
            r#"quantile_over_time(0.5, {env="prod"} | logfmt | unwrap v [1m])"#,
            &params,
            &rows,
            &meta_one(),
        )
        .unwrap(),
    );
    assert_eq!(v, 2.5);
    let v = single_vector_value(
        run_client(
            r#"quantile_over_time(0.9, {env="prod"} | logfmt | unwrap v [1m])"#,
            &params,
            &rows,
            &meta_one(),
        )
        .unwrap(),
    );
    assert!((v - 3.7).abs() < 1e-12, "{v}");
}

// ---------------------------------------------------------------------
// Review round 1 gap (b): first/last boundary, tie, and input-order
// cases.
// ---------------------------------------------------------------------

#[test]
fn first_and_last_are_timestamp_anchored_regardless_of_input_order() {
    // Same rows as `unwrap_rows` but SHUFFLED: reducers must anchor on
    // timestamps, never on arrival order (for distinct timestamps).
    let shuffled = vec![
        row(1, 80 * NS, "v=4"),
        row(1, 10 * NS, "v=1"),
        row(1, 70 * NS, "v=3"),
        row(1, 20 * NS, "v=2"),
    ];
    let params = range_params(0, 2 * STEP);
    for (op, b0, b60) in [("first_over_time", 1.0, 3.0), ("last_over_time", 2.0, 4.0)] {
        let query = format!(r#"{op}({{env="prod"}} | logfmt | unwrap v [1m])"#);
        let points =
            single_series_points(run_client(&query, &params, &shuffled, &meta_one()).unwrap());
        // Issue #227 sliding: values emit at t=60s/120s (empty t=0 gap).
        assert_eq!(
            points,
            vec![(STEP, b0), (2 * STEP, b60)],
            "{op} (shuffled input)"
        );
    }
}

/// Equal timestamps (review round 2, finding 2): the pinned,
/// INPUT-ORDER-INDEPENDENT tie rule — `first` takes the SMALLEST value
/// among samples tied at the minimum timestamp, `last` the LARGEST at
/// the maximum. Both the natural and the fully reversed input ordering
/// must give the one same answer (the SQL scan additionally carries a
/// stable `fingerprint, body` secondary sort, but the reducer does not
/// depend on it).
#[test]
fn first_and_last_tie_break_identically_for_reordered_equal_timestamp_inputs() {
    let natural = vec![
        row(1, 10 * NS, "v=1"),
        row(1, 10 * NS, "v=2"), // ties the min timestamp
        row(1, 30 * NS, "v=3"),
        row(1, 30 * NS, "v=4"), // ties the max timestamp
    ];
    let reversed: Vec<MetricScanRow> = natural.iter().rev().cloned().collect();
    let params = instant_params(60 * NS);
    for rows in [&natural, &reversed] {
        let first = single_vector_value(
            run_client(
                r#"first_over_time({env="prod"} | logfmt | unwrap v [1m])"#,
                &params,
                rows,
                &meta_one(),
            )
            .unwrap(),
        );
        assert_eq!(first, 1.0, "first = smallest value among min-ts ties");
        let last = single_vector_value(
            run_client(
                r#"last_over_time({env="prod"} | logfmt | unwrap v [1m])"#,
                &params,
                rows,
                &meta_one(),
            )
            .unwrap(),
        );
        assert_eq!(last, 4.0, "last = largest value among max-ts ties");
    }
}

/// Issue #227 sliding half-open `(t-range, t]` boundary: a row exactly at
/// `ts == t` is INCLUDED (upper-inclusive), a row at `ts == t-range` is
/// EXCLUDED (lower-exclusive). Rows at ts=1, 60s, 120s with the `[1m]`
/// window on grid `{0, 60s, 120s}`:
///   t=0   : `(-60s, 0]`  empty (ts=1 excluded)     → gap
///   t=60s : `(0, 60s]`   ts∈{1, 60s} → v=1, v=2
///   t=120s: `(60s, 120s]` ts∈{120s}   → v=3  (ts=60s excluded, lower edge)
#[test]
fn sliding_window_boundary_is_half_open_lower_exclusive_upper_inclusive() {
    let rows = vec![
        row(1, 1, "v=1"),
        row(1, STEP, "v=2"),
        row(1, 2 * STEP, "v=3"),
    ];
    let params = range_params(0, 2 * STEP);
    for (op, expected) in [
        ("first_over_time", vec![(STEP, 1.0), (2 * STEP, 3.0)]),
        ("last_over_time", vec![(STEP, 2.0), (2 * STEP, 3.0)]),
    ] {
        let query = format!(r#"{op}({{env="prod"}} | logfmt | unwrap v [1m])"#);
        let points = single_series_points(run_client(&query, &params, &rows, &meta_one()).unwrap());
        assert_eq!(points, expected, "{op}");
    }
}

// ---------------------------------------------------------------------
// Review round 1 finding 2 + finding 1 (quantile bound): the named
// breadth guards.
// ---------------------------------------------------------------------

#[test]
fn an_oversized_bucket_grid_is_a_named_too_broad_error_before_any_allocation() {
    // 1h window at a 1ms step = 3.6M buckets >> the 11k cap. No rows at
    // all — the guard must fire from the request shape alone.
    let params = QueryParams {
        spec: pulsus_read::logql::QuerySpec::Range {
            start_ns: 0,
            end_ns: 3_600 * NS,
            step_ns: 1_000_000, // 1ms
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let err = run_client(
        r#"absent_over_time({env="prod"}[1m])"#,
        &params,
        &[],
        &meta_one(),
    )
    .unwrap_err();
    let ReadError::QueryTooBroad(pulsus_read::logql::TooBroadReason::MetricBuckets {
        buckets,
        cap,
    }) = err
    else {
        panic!("expected QueryTooBroad(MetricBuckets), got {err:?}");
    };
    assert_eq!(cap, pulsus_read::logql::exec::MAX_CLIENT_AGG_BUCKETS);
    assert!(buckets > cap, "{buckets} vs {cap}");
    // The same guard covers every client-aggregated op, not just absent.
    let err = run_client(
        r#"count_over_time({env="prod"} | logfmt [1m])"#,
        &params,
        &[],
        &meta_one(),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ReadError::QueryTooBroad(pulsus_read::logql::TooBroadReason::MetricBuckets { .. })
    ));
}

/// Review round 2, finding 1: extreme window bounds must produce the
/// same NAMED too-broad error — never an integer overflow panic/wrap
/// that slips past the cap.
#[test]
fn extreme_window_bounds_hit_the_bucket_cap_without_overflow() {
    // The full i64 nanosecond range at step 1 ns (~2^64 buckets — would
    // wrap a plain i64 count).
    let params = QueryParams {
        spec: pulsus_read::logql::QuerySpec::Range {
            start_ns: i64::MIN,
            end_ns: i64::MAX,
            step_ns: 1,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let err = run_client(
        r#"absent_over_time({env="prod"}[1m])"#,
        &params,
        &[],
        &meta_one(),
    )
    .unwrap_err();
    assert!(
        matches!(
            err,
            ReadError::QueryTooBroad(pulsus_read::logql::TooBroadReason::MetricBuckets { .. })
        ),
        "{err:?}"
    );
    // A negative-magnitude window (both bounds deep in the past) at a
    // tiny step trips the cap identically.
    let params = QueryParams {
        spec: pulsus_read::logql::QuerySpec::Range {
            start_ns: i64::MIN,
            end_ns: i64::MIN / 2,
            step_ns: 1,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let err = run_client(
        r#"count_over_time({env="prod"} | logfmt [1m])"#,
        &params,
        &[],
        &meta_one(),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ReadError::QueryTooBroad(pulsus_read::logql::TooBroadReason::MetricBuckets { .. })
    ));
    // An inverted (empty) window resolves zero buckets: accepted, empty
    // result — never an underflow.
    let params = QueryParams {
        spec: pulsus_read::logql::QuerySpec::Range {
            start_ns: i64::MAX,
            end_ns: i64::MIN,
            step_ns: 1,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let result = run_client(
        r#"absent_over_time({env="prod"}[1m])"#,
        &params,
        &[],
        &meta_one(),
    )
    .unwrap();
    assert_eq!(result, QueryResult::Matrix(Vec::new()));
    // A large-but-IN-DOMAIN step over the extreme window is a handful of
    // buckets: accepted (no false positive from the widened arithmetic).
    // `MAX_DURATION_NS` is the validated ceiling — since round 10 the
    // reference's full positive int64 (`i64::MAX`), so this is the largest
    // step the reference can represent, over the full timestamp domain
    // (one saturated fence interval); a step ABOVE it is rejected at the
    // planner boundary instead — see
    // `a_hostile_step_is_rejected_end_to_end_by_the_planner`.
    let params = QueryParams {
        spec: pulsus_read::logql::QuerySpec::Range {
            start_ns: i64::MIN,
            end_ns: i64::MAX,
            step_ns: pulsus_read::logql::MAX_DURATION_NS as u64,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    assert!(
        run_client(
            r#"count_over_time({env="prod"} | logfmt [1m])"#,
            &params,
            &[],
            &meta_one(),
        )
        .is_ok()
    );
}

/// Issue #227 review round 10: a 100-year step (`step=3153600000s`, start=0,
/// end=0) fits the reference's positive `time.Duration` and passes its
/// resolution fence, so the reference SERVES it — the retired
/// `i64::MAX / 4` duration cap wrongly 400'd it at the planner boundary.
/// End-to-end through the client-aggregated path: the plan validates, the
/// grid is the single point `t = 0`, and its `(t-1m, t]` window counts the
/// one sample at `ts = 0`.
#[test]
fn a_100_year_step_the_reference_serves_is_served() {
    const HUNDRED_YEARS_NS: u64 = 3_153_600_000_000_000_000;
    let params = QueryParams {
        spec: pulsus_read::logql::QuerySpec::Range {
            start_ns: 0,
            end_ns: 0,
            step_ns: HUNDRED_YEARS_NS,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let points = single_series_points(
        run_client(
            r#"count_over_time({env="prod"}[1m])"#,
            &params,
            &[row(1, 0, "a")],
            &meta_one(),
        )
        .expect("the reference serves a 100-year step; PulsusDB must too"),
    );
    assert_eq!(points, vec![(0, 1.0)]);
}

/// Issue #227: the sliding evaluator must survive extreme timestamps near
/// `i64::MIN` without overflow — the grid point `grid_start + k·step`, the
/// window lower bound `t - range` (a `[3s]` range ≫ the window, so `t-range`
/// underflows i64 and must saturate), and the covering-set math all run in
/// i128 / saturating i64. Grid `{MIN, MIN+3, MIN+6, MIN+9}`, window
/// `(t-3s, t]` (saturates to `(MIN, t]`):
///   t=MIN   : `(MIN, MIN]`   empty (MIN+1 excluded) → gap
///   t=MIN+3 : `(MIN, MIN+3]` → MIN+1 → 1
///   t=MIN+6 : `(MIN, MIN+6]` → MIN+1 → 1
///   t=MIN+9 : `(MIN, MIN+9]` → MIN+1, MIN+7 → 2
#[test]
fn extreme_timestamp_samples_slide_without_overflow() {
    let params = QueryParams {
        spec: pulsus_read::logql::QuerySpec::Range {
            start_ns: i64::MIN,
            end_ns: i64::MIN + 9,
            step_ns: 3,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let rows = vec![row(1, i64::MIN + 1, "a"), row(1, i64::MIN + 7, "b")];
    let points = single_series_points(
        run_client(
            r#"count_over_time({env="prod"} | env = "prod" [3s])"#,
            &params,
            &rows,
            &meta_one(),
        )
        .unwrap(),
    );
    assert_eq!(
        points,
        vec![
            (i64::MIN + 3, 1.0),
            (i64::MIN + 6, 1.0),
            (i64::MIN + 9, 2.0)
        ]
    );

    // absent_over_time at the extreme: the `[3s]` range covers all later
    // grid points once MIN+1 enters, so only t=MIN (whose window excludes
    // MIN+1) reports absence.
    let result = run_client(
        r#"absent_over_time({env="prod"}[3s])"#,
        &params,
        &[row(1, i64::MIN + 1, "a")],
        &meta_one(),
    )
    .unwrap();
    let QueryResult::Matrix(items) = result else {
        panic!("expected a matrix");
    };
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].points,
        vec![(i64::MIN, 1.0)],
        "only the first grid point's window is empty"
    );
}

#[test]
fn a_bucket_grid_at_the_cap_is_accepted() {
    // Exactly 11_000 one-second buckets (0..=10_999s — an end exactly ON
    // a step edge would add the edge bucket and tip over the cap).
    let params = QueryParams {
        spec: pulsus_read::logql::QuerySpec::Range {
            start_ns: 0,
            end_ns: 10_999 * NS,
            step_ns: NS as u64,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    assert!(
        run_client(
            r#"count_over_time({env="prod"} | logfmt [1s])"#,
            &params,
            &[],
            &meta_one(),
        )
        .is_ok()
    );
}

#[test]
fn filtered_count_over_time_counts_only_pipeline_survivors() {
    let rows = vec![
        row(1, 10 * NS, "level=error msg=a"),
        row(1, 20 * NS, "level=info msg=b"), // dropped by the filter
        row(1, 70 * NS, "level=error msg=c"),
    ];
    let params = range_params(0, 2 * STEP);
    let result = run_client(
        r#"count_over_time({env="prod"} | logfmt | level = "error" [1m])"#,
        &params,
        &rows,
        &meta_one(),
    )
    .unwrap();
    let QueryResult::Matrix(items) = result else {
        panic!("expected a matrix");
    };
    // logfmt fans out by final label set; msg differs per line so each
    // surviving line is its own series with count 1 in its bucket.
    assert_eq!(items.len(), 2);
    let total: f64 = items
        .iter()
        .flat_map(|s| s.points.iter().map(|(_, v)| *v))
        .sum();
    assert_eq!(total, 2.0, "the level=info line must not be counted");
}

#[test]
fn bytes_over_time_sums_final_line_byte_lengths() {
    let rows = vec![
        row(1, 10 * NS, "sz=abcd"), // 7 bytes
        row(1, 20 * NS, "sz=ab"),   // 5 bytes
    ];
    let params = instant_params(60 * NS);
    let v = single_vector_value(
        run_client(
            r#"bytes_over_time({env="prod"} | logfmt [1m])"#,
            &params,
            &rows,
            &meta_one(),
        )
        .map(|r| match r {
            QueryResult::Vector(items) => QueryResult::Vector(vec![VectorSample {
                labels: Vec::new(),
                value: items.iter().map(|s| s.value).sum(),
            }]),
            other => other,
        })
        .unwrap(),
    );
    assert_eq!(v, 12.0);
}

// ---------------------------------------------------------------------
// AC4 (oracle-probed unwrap error semantics, adjudication #1):
// - a failed conversion WITHOUT a downstream `__error__` filter FAILS
//   the query with the oracle's exact message shape and error class —
//   both some-lines-fail and all-lines-fail (probed: HTTP 400 both);
// - WITH `| __error__ = ""` the failed line is consumed in stage order
//   and only the good lines aggregate;
// - a MISSING unwrap label silently skips the line (probed: success).
// ---------------------------------------------------------------------

#[test]
fn a_surviving_unwrap_conversion_failure_fails_the_query_with_the_oracle_message() {
    let rows = vec![
        row(1, 10 * NS, "took=250ms x=1"),
        row(1, 20 * NS, "took=abc x=2"), // fails duration conversion
    ];
    let params = instant_params(60 * NS);
    let err = run_client(
        r#"sum_over_time({env="prod"} | logfmt | unwrap duration(took) [1m])"#,
        &params,
        &rows,
        &meta_one(),
    )
    .unwrap_err();
    let ReadError::MetricPipelineError { error_type, series } = &err else {
        panic!("expected MetricPipelineError, got {err:?}");
    };
    assert_eq!(error_type, SAMPLE_EXTRACTION_ERROR);
    assert!(
        series.contains(r#"__error__="SampleExtractionErr""#),
        "{series}"
    );
    assert!(
        series.contains(r#"took="abc""#),
        "the failed line keeps its raw label in the series (oracle shape): {series}"
    );
    // Byte-exact metric-path `__error_details__` (issue #104): a
    // `unwrap duration(took)` conversion failure over `took=abc` renders
    // Go `time.ParseDuration`'s `invalid duration` string, inner-quoted by
    // `render_series_labels` — identical to the label-filter duration
    // family (oracle-confirmed vs grafana/loki:3.4.2).
    assert!(
        series.contains(r#"__error_details__="time: invalid duration \"abc\"""#),
        "metric path must carry the byte-exact SampleExtractionErr detail: {series}"
    );
    // The full oracle template (pinned reference oracle — the compose
    // digest — live-probe transcript, 2026-07-17; HTTP 400):
    //   pipeline error: 'SampleExtractionErr' for series: '{...}'.
    //   Use a label filter to intentionally skip this error. (e.g | __error__!="SampleExtractionErr").
    //   To skip all potential errors you can match empty errors.(e.g __error__="")
    //   The label filter can also be specified after unwrap. (e.g | unwrap latency | __error__="" )
    let msg = err.to_string();
    assert!(
        msg.starts_with("pipeline error: 'SampleExtractionErr' for series: '{"),
        "{msg}"
    );
    assert!(
        msg.ends_with(
            "Use a label filter to intentionally skip this error. \
             (e.g | __error__!=\"SampleExtractionErr\").\n\
             To skip all potential errors you can match empty errors.(e.g __error__=\"\")\n\
             The label filter can also be specified after unwrap. \
             (e.g | unwrap latency | __error__=\"\" )"
        ),
        "{msg}"
    );
}

#[test]
fn an_all_lines_fail_unwrap_also_fails_the_query_never_an_empty_success() {
    // Oracle-probed (all-bad-conversion stream): HTTP 400, same message.
    let rows = vec![
        row(1, 10 * NS, "took=abc x=1"),
        row(1, 20 * NS, "took=zzz x=2"),
    ];
    let params = instant_params(60 * NS);
    let err = run_client(
        r#"sum_over_time({env="prod"} | logfmt | unwrap duration(took) [1m])"#,
        &params,
        &rows,
        &meta_one(),
    )
    .unwrap_err();
    assert!(matches!(err, ReadError::MetricPipelineError { .. }));
}

#[test]
fn a_post_unwrap_error_filter_consumes_failed_lines_and_the_query_succeeds() {
    // Oracle transcript: the same mixed stream with `| __error__ = ""`
    // returns exactly the good line's value (0.25).
    let rows = vec![
        row(1, 10 * NS, "took=250ms x=1"),
        row(1, 20 * NS, "took=abc x=2"),
    ];
    let params = instant_params(60 * NS);
    let v = single_vector_value(
        run_client(
            r#"sum_over_time({env="prod"} | logfmt | unwrap duration(took) | __error__ = "" [1m])"#,
            &params,
            &rows,
            &meta_one(),
        )
        .unwrap(),
    );
    assert_eq!(v, 0.25);
}

#[test]
fn lines_missing_the_unwrap_label_are_silently_skipped_like_the_oracle() {
    // Oracle-probed: a stream whose lines lack the label entirely
    // returns success with an empty result — never an error.
    let rows = vec![row(1, 10 * NS, "x=1"), row(1, 20 * NS, "x=2")];
    let params = instant_params(60 * NS);
    let result = run_client(
        r#"sum_over_time({env="prod"} | logfmt | unwrap duration(took) [1m])"#,
        &params,
        &rows,
        &meta_one(),
    )
    .unwrap();
    assert_eq!(result, QueryResult::Vector(Vec::new()));
}

/// Review round 1 finding 4: hostile parsed label values (quotes,
/// backslashes, control characters) must render escaped in the error
/// series — the same mandatory-set escaping as the canonical labels
/// JSON, never malformed `{k="v"}` text.
#[test]
fn error_series_labels_escape_quotes_backslashes_and_control_chars() {
    // logfmt quoted value with escaped quote + backslash + a tab; the
    // bad `took` makes the line's `__error__` survive.
    let rows = vec![row(1, 10 * NS, r#"took=abc msg="a\"b\\c	d""#)];
    let params = instant_params(60 * NS);
    let err = run_client(
        r#"sum_over_time({env="prod"} | logfmt | unwrap duration(took) [1m])"#,
        &params,
        &rows,
        &meta_one(),
    )
    .unwrap_err();
    let ReadError::MetricPipelineError { series, .. } = &err else {
        panic!("expected MetricPipelineError, got {err:?}");
    };
    assert!(
        series.contains(r#"msg="a\"b\\c\td""#),
        "quote/backslash/control escaping must hold: {series}"
    );
    // The metric-path `__error_details__` (issue #104) itself carries
    // inner double quotes (`time: invalid duration "abc"`), which
    // `render_series_labels` re-escapes byte-exactly — no raw quotes leak.
    assert!(
        series.contains(r#"__error_details__="time: invalid duration \"abc\"""#),
        "detail label must render with escaped inner quotes: {series}"
    );
    // The rendered text stays structurally parseable: every `"` inside
    // a value is escaped, so the quote count is even and no raw control
    // characters leak.
    assert!(!series.contains('\t'), "raw control char leaked: {series}");
}

/// Review round 1 gap (c): the post-`line_format` line filter EXECUTES
/// in the metric evaluator over the REWRITTEN line (the SQL-shape tests
/// prove it is not pushed down; this proves it actually drops).
#[test]
fn a_post_line_format_metric_line_filter_drops_in_engine_on_the_rewritten_line() {
    // All bodies satisfy the pushed `|= "req"` prefix (as the SQL scan
    // would guarantee); survival is decided ONLY by the rewritten line
    // (`{{.status}}`) containing "500".
    let rows = vec![
        row(1, 10 * NS, r#"{"req":"a","status":"500"}"#),
        row(1, 20 * NS, r#"{"req":"b","status":"200"}"#), // rewritten "200" — dropped in-engine
        row(1, 70 * NS, r#"{"req":"c","status":"500"}"#),
    ];
    let params = range_params(0, 2 * STEP);
    let result = run_client(
        r#"count_over_time({env="prod"} |= "req" | json | line_format "{{.status}}" |= "500" [1m])"#,
        &params,
        &rows,
        &meta_one(),
    )
    .unwrap();
    let QueryResult::Matrix(items) = result else {
        panic!("expected a matrix");
    };
    let total: f64 = items
        .iter()
        .flat_map(|s| s.points.iter().map(|(_, v)| *v))
        .sum();
    assert_eq!(total, 2.0, "only rewritten lines containing \"500\" count");
    let buckets: std::collections::BTreeSet<i64> = items
        .iter()
        .flat_map(|s| s.points.iter().map(|(b, _)| *b))
        .collect();
    // Issue #227 sliding: the survivor at 10s lands in the `(0, 60s]` window
    // (grid point 60s), the survivor at 70s in `(60s, 120s]` (grid point
    // 120s) — one grid point later than the old tumbling bucket start.
    assert_eq!(
        buckets.into_iter().collect::<Vec<_>>(),
        vec![STEP, 2 * STEP],
        "one survivor per sliding window"
    );
}

#[test]
fn a_surviving_parser_error_also_fails_a_metric_query() {
    // Oracle-probed generality (JSONParserErr case): ANY surviving
    // nonempty `__error__` fails the metric query, not just unwrap's.
    let rows = vec![
        row(1, 10 * NS, r#"{"status":"500"}"#),
        row(1, 20 * NS, "not json at all"),
    ];
    let params = instant_params(60 * NS);
    let err = run_client(
        r#"count_over_time({env="prod"} | json [1m])"#,
        &params,
        &rows,
        &meta_one(),
    )
    .unwrap_err();
    let ReadError::MetricPipelineError { error_type, series } = &err else {
        panic!("expected MetricPipelineError, got {err:?}");
    };
    assert_eq!(error_type, "JSONParserErr");
    // Byte-exact metric-path `__error_details__` (issue #104): the
    // JSONParserErr detail is the pinned buger/jsonparser message
    // (oracle_probe.txt [1]), rendered as a series label.
    assert!(
        series.contains(
            r#"__error_details__="Value looks like object, but can't find closing '}' symbol""#
        ),
        "metric path must carry the byte-exact JSONParserErr detail: {series}"
    );
}

// ---------------------------------------------------------------------
// Issue #73 (retroactive re-review): the derived-series cap bounds the
// last unbounded axis of client-agg reducer state (`groups x buckets`).
// Both `Vacant`-insert sites — `label_groups` (fan-out/label-mutating)
// and `fp_groups` (non-mutating) — are proven independently: reject at
// cap+1, succeed at exactly the cap.
// ---------------------------------------------------------------------

fn meta_n(n: usize) -> HashMap<u64, StreamMetaRow> {
    (1..=n as u64)
        .map(|fp| {
            (
                fp,
                StreamMetaRow {
                    fingerprint: fp,
                    service: "checkout".to_string(),
                    labels: r#"{"env":"prod","service_name":"checkout"}"#.to_string(),
                },
            )
        })
        .collect()
}

/// Issue #236, rewritten cap golden (AC 16). Before #236 this asserted a
/// mid-scan `MetricSeries` 422 at the 501st fan-out group. That rejection
/// is DELETED: `MAX_QUERY_SERIES` is a final-RESULT cap, so the leaf must
/// now serve every group it scans and let `ensure_result_series` judge the
/// finished result at the engine boundary.
///
/// `logfmt` is a parser -> `mutates_labels` -> fan-out. Every row carries a
/// unique `id=<n>` body on the SAME fingerprint, so each survivor's final
/// label set is distinct -> one `label_groups` entry per row.
///
/// Fails on `590220a` (which 422s at the 501st group).
#[test]
fn label_groups_fan_out_past_the_old_series_cap_is_served_by_the_leaf() {
    let old_cap = 500usize;
    let params = instant_params(60 * NS);
    let query = r#"count_over_time({env="prod"} | logfmt [1m])"#;

    // One PAST the deleted cap, and far past it: both served, with every
    // group present in the output.
    for n in [old_cap + 1, old_cap * 4] {
        let rows: Vec<MetricScanRow> = (0..n)
            .map(|i| row(1, 10 * NS, &format!("id={i}")))
            .collect();
        let result = run_client(query, &params, &rows, &meta_one())
            .unwrap_or_else(|e| panic!("{n} fan-out groups must be served by the leaf, got {e:?}"));
        let QueryResult::Vector(items) = result else {
            panic!("expected a vector");
        };
        assert_eq!(items.len(), n, "every scanned group must reach the result");
    }
}

/// Issue #236, rewritten cap golden (AC 16) — the non-mutating twin of the
/// test above, and AC 14(a)'s premise-fix case.
///
/// `line_format` is beyond-line-filter (so the query IS client-aggregated)
/// but its compile arm sets only `rewrites_line`, never `mutates_labels`
/// (pipeline.rs) — so `metric_mutates_labels() == false` and the query
/// lands on the NON-fan-out `fp_groups` branch, keyed by fingerprint. That
/// branch had NO byte charge before #236; P1 added one, and this pins that
/// the charge admits a large, ordinary label model rather than rejecting
/// it.
///
/// Fails on `590220a` (422 at the 501st fingerprint).
#[test]
fn fp_groups_non_mutating_past_the_old_series_cap_is_served_by_the_leaf() {
    let old_cap = 500usize;
    let params = instant_params(60 * NS);
    let query = r#"count_over_time({env="prod"} | line_format "keep" [1m])"#;

    for n in [old_cap + 1, old_cap * 4] {
        let meta = meta_n(n);
        let rows: Vec<MetricScanRow> = (1..=n as u64).map(|fp| row(fp, 10 * NS, "hello")).collect();
        let result = run_client(query, &params, &rows, &meta).unwrap_or_else(|e| {
            panic!("{n} fingerprint groups must be served by the leaf, got {e:?}")
        });
        let QueryResult::Vector(items) = result else {
            panic!("expected a vector");
        };
        assert_eq!(
            items.len(),
            n,
            "every scanned fingerprint must reach the result"
        );
    }
}

/// Issue #236 AC 11 — **no group-count rejection before the final
/// result**, on the range path where the fold owns the inner aggregation.
///
/// The INNER `sum by (id)` produces 501 groups; the OUTER `sum` collapses
/// them to ONE series, which the reference serves. `MAX_QUERY_SERIES` is a
/// final-RESULT cap, so nothing may reject on the intermediate — not the
/// leaf (Part A deleted that), and not the fold (plan v14 §3 Part B: fold
/// state is bounded by bytes and points and nothing else).
///
/// Fails on `7754844` under any fold that rejects at
/// `groups > MAX_QUERY_SERIES`, and on `590220a` at the 501st scanned
/// group. `run_client`'s AC-7 assertion additionally proves the folded and
/// materialised answers are the same bits at this width.
#[test]
fn a_range_chain_whose_inner_grouping_is_wide_and_result_is_one_series_is_served() {
    let inner_groups = 501usize;
    let params = range_params(0, 2 * STEP);
    let query = r#"sum(sum by (id) (count_over_time({env="prod"} | logfmt [1m])))"#;
    let rows: Vec<MetricScanRow> = (0..inner_groups)
        .map(|i| row(1, 30 * NS, &format!("id={i}")))
        .collect();

    let result = run_client(query, &params, &rows, &meta_one())
        .unwrap_or_else(|e| panic!("{inner_groups} inner groups must be served, got {e:?}"));
    let QueryResult::Matrix(items) = result else {
        panic!("expected a matrix");
    };
    assert_eq!(items.len(), 1, "the FINAL result is one series");
    // Every scanned group contributes exactly 1 to each covering step.
    for (_, v) in &items[0].points {
        assert_eq!(
            v.to_bits(),
            (inner_groups as f64).to_bits(),
            "every inner group must reach the outer sum"
        );
    }

    // The same shape one group narrower is served identically — the
    // boundary is not a boundary at all on an intermediate.
    let rows: Vec<MetricScanRow> = (0..inner_groups - 2)
        .map(|i| row(1, 30 * NS, &format!("id={i}")))
        .collect();
    let result = run_client(query, &params, &rows, &meta_one()).expect("499 inner groups");
    let QueryResult::Matrix(items) = result else {
        panic!("expected a matrix");
    };
    assert_eq!(items.len(), 1);
}

/// Issue #236 Part B — the shape the rest of this file did not have:
/// **several leaf series merging into ONE fold group at the SAME step**.
///
/// Every other range fixture here either has one stream or keeps one
/// output group per leaf group, so the fold's per-slot ACCUMULATION —
/// second and later members reaching a slot that already holds one — was
/// never exercised, and `run_client`'s AC-7 equivalence gate therefore
/// could not see a defect in it. Found by a mutant (last-value-wins in
/// place of `VectorAccum::update`) that this file passed; the gap was in
/// the fixtures, not in the claim, so the fixture is added rather than the
/// claim softened.
///
/// Three streams with DIFFERENT line counts in the same window, so `sum`,
/// `avg`, `min`, `max` and `count` each discriminate a different way, and
/// the AC-7 assertion inside `run_client` proves the folded and
/// materialised answers agree over all five.
#[test]
fn a_range_aggregation_merging_several_streams_into_one_group_accumulates() {
    let meta: HashMap<u64, StreamMetaRow> = (1u64..=3)
        .map(|fp| {
            (
                fp,
                StreamMetaRow {
                    fingerprint: fp,
                    service: format!("svc{fp}"),
                    labels: format!(r#"{{"env":"prod","service_name":"svc{fp}"}}"#),
                },
            )
        })
        .collect();
    // Stream 1 → 1 line, stream 2 → 2 lines, stream 3 → 4 lines, all
    // inside the window `(0, 60s]` so all three land on the SAME grid
    // point with the same group key.
    let mut rows = vec![row(1, 10 * NS, "x")];
    rows.extend((0..2).map(|i| row(2, (10 + i) * NS, "x")));
    rows.extend((0..4).map(|i| row(3, (10 + i) * NS, "x")));
    let params = range_params(0, STEP);

    // (query, the value at t = 60s over members {1, 2, 4})
    let cases: [(&str, f64); 5] = [
        (r#"sum(count_over_time({env="prod"}[1m]))"#, 7.0),
        (r#"count(count_over_time({env="prod"}[1m]))"#, 3.0),
        (r#"min(count_over_time({env="prod"}[1m]))"#, 1.0),
        (r#"max(count_over_time({env="prod"}[1m]))"#, 4.0),
        (r#"avg(count_over_time({env="prod"}[1m]))"#, 7.0f64 / 3.0f64),
    ];
    for (query, want) in cases {
        let result = run_client(query, &params, &rows, &meta).expect(query);
        let QueryResult::Matrix(items) = result else {
            panic!("expected a matrix for {query}");
        };
        assert_eq!(items.len(), 1, "{query}: one collapsed series");
        let at_step = items[0]
            .points
            .iter()
            .find(|(t, _)| *t == STEP)
            .unwrap_or_else(|| panic!("{query}: no point at 60s in {:?}", items[0].points));
        assert_eq!(
            at_step.1.to_bits(),
            want.to_bits(),
            "{query}: every member must reach the slot"
        );
    }
}

/// Issue #236 AC 10 — `topk(0, …)`/`bottomk(0, …)` through the LEAF SEAM
/// over 501 distinct groups, range and instant: `Ok(empty)`, never a
/// group-count rejection.
///
/// `topk(0, …)` is a reference-verbatim 400 at PARSE in both
/// implementations, so this arm is reachable only through the programmatic
/// seam — which is exactly why it is pinned here as well as end-to-end
/// (`pulsus-logql/tests/errors.rs`): the two levels cannot swap without
/// one of them reddening. Fails on `590220a` (422 `MetricSeries` at the
/// 501st group) and under a fold that counts groups before consulting `k`.
#[test]
fn zero_k_over_501_groups_is_empty_not_a_rejection() {
    let n = 501usize;
    let rows: Vec<MetricScanRow> = (0..n)
        .map(|i| row(1, 30 * NS, &format!("id={i}")))
        .collect();

    for op in ["topk", "bottomk"] {
        // Planned with a positive `k` (0 is a parse error), then the spec
        // is rewritten to `k = 0` — the seam the arm is reachable through.
        let inner = r#"count_over_time({env="prod"} | logfmt [1m])"#;
        for (label, params) in [
            ("instant", instant_params(60 * NS)),
            ("range", range_params(0, 2 * STEP)),
        ] {
            let query = format!("{op}(3, {inner})");
            let mp = metric_plan_of(&query, &params);
            let client = mp.client.as_ref().expect("client-aggregated");
            let compiled = CompiledPipeline::compile(&client.pipeline).expect("compile");
            let window = match mp.step_ns {
                Some(step_ns) => ClientWindow::Range {
                    grid_start_ns: mp.grid_start_ns,
                    end_ns: mp.end_ns,
                    step_ns,
                    range_ns: mp.range_ns,
                },
                None => ClientWindow::Instant {
                    start_ns: mp.grid_start_ns,
                    end_ns: mp.end_ns,
                },
            };
            let zero_k: Vec<_> = mp
                .vector_aggs
                .iter()
                .map(|(o, g, _)| (*o, g.clone(), Some(0.0)))
                .collect();
            let result = run_client_agg_rows_folded(
                &rows,
                &compiled,
                &meta_one(),
                client,
                window,
                mp.rate_window_ns,
                &zero_k,
            )
            .unwrap_or_else(|e| panic!("{op}(0) {label} over {n} groups must be Ok, got {e:?}"));
            match result {
                QueryResult::Matrix(items) => {
                    assert!(items.is_empty(), "{op}(0) {label}: {items:?}")
                }
                QueryResult::Vector(items) => {
                    assert!(items.is_empty(), "{op}(0) {label}: {items:?}")
                }
                other => panic!("{op}(0) {label}: unexpected {other:?}"),
            }
        }
    }
}

// ---------------------------------------------------------------------
// AC4a: `absent_over_time` is selector-wide per bucket (plan v2 D2) —
// at most ONE series, absence only for buckets where the WHOLE selector
// produced zero surviving lines, labels = the selector's Eq matchers.
// ---------------------------------------------------------------------

#[test]
fn absent_over_time_emits_at_most_one_selector_wide_series() {
    // Two matched streams; bucket 0 has a line only in stream 1, bucket
    // 60s only in stream 2, bucket 120s is empty on BOTH.
    let rows = vec![row(1, 10 * NS, "a"), row(2, 70 * NS, "b")];
    let params = range_params(0, 3 * STEP);
    let result = run_client(
        r#"absent_over_time({env="prod", team=~"x|y", region="eu"}[1m])"#,
        &params,
        &rows,
        &meta_two(),
    )
    .unwrap();
    let QueryResult::Matrix(items) = result else {
        panic!("expected a matrix");
    };
    assert_eq!(items.len(), 1, "one absence series, never per label set");
    assert_eq!(
        items[0].labels,
        vec![
            ("env".to_string(), "prod".to_string()),
            ("region".to_string(), "eu".to_string()),
        ],
        "Eq-matcher labels only"
    );
    // Issue #227 sliding, grid `{0, 60s, 120s, 180s}`, window `(t-60s, t]`:
    // t=0 has no lookback (empty → absent); t=60s sees the 10s line; t=120s
    // sees the 70s line; t=180s `(120s, 180s]` is empty → absent.
    assert_eq!(
        items[0].points,
        vec![(0, 1.0), (3 * STEP, 1.0)],
        "absence for empty sliding windows only — a window with a line in \
         ANY stream emits nothing"
    );
}

#[test]
fn absent_over_time_instant_emits_one_when_nothing_survives() {
    let params = instant_params(60 * NS);
    let result = run_client(
        r#"absent_over_time({env="prod"}[1m])"#,
        &params,
        &[],
        &meta_one(),
    )
    .unwrap();
    assert_eq!(
        result,
        QueryResult::Vector(vec![VectorSample {
            labels: vec![("env".to_string(), "prod".to_string())],
            value: 1.0,
        }])
    );
    let present = run_client(
        r#"absent_over_time({env="prod"}[1m])"#,
        &params,
        &[row(1, 10 * NS, "a")],
        &meta_one(),
    )
    .unwrap();
    assert_eq!(present, QueryResult::Vector(Vec::new()));
}

// ---------------------------------------------------------------------
// AC4: topk/bottomk selection + deterministic tie-break, stddev/stdvar
// vector aggregations.
// ---------------------------------------------------------------------

fn matrix_fixture() -> QueryResult {
    QueryResult::Matrix(vec![
        MatrixSeries {
            labels: vec![("app".to_string(), "a".to_string())],
            points: vec![(0, 5.0), (STEP, 1.0)],
        },
        MatrixSeries {
            labels: vec![("app".to_string(), "b".to_string())],
            points: vec![(0, 3.0), (STEP, 3.0)],
        },
        MatrixSeries {
            labels: vec![("app".to_string(), "c".to_string())],
            points: vec![(0, 5.0), (STEP, 2.0)],
        },
    ])
}

fn points_by_app(result: QueryResult) -> HashMap<String, Vec<(i64, f64)>> {
    let QueryResult::Matrix(items) = result else {
        panic!("expected a matrix");
    };
    items
        .into_iter()
        .map(|s| (s.labels[0].1.clone(), s.points))
        .collect()
}

#[test]
fn topk_selects_per_step_preserving_original_series_labels() {
    let aggs = vec![(pulsus_logql::VectorAggOp::Topk, None, Some(2.0))];
    let by_app = points_by_app(apply_vector_aggs_ok(matrix_fixture(), &aggs));
    // Step 0: values 5(a), 5(c), 3(b) — the 5.0 tie breaks by label set
    // ascending (a before c), both fit in k=2, b drops.
    // Step 60: 3(b), 2(c) survive; 1(a) drops.
    assert_eq!(by_app["a"], vec![(0, 5.0)]);
    assert_eq!(by_app["b"], vec![(STEP, 3.0)]);
    assert_eq!(by_app["c"], vec![(0, 5.0), (STEP, 2.0)]);
}

#[test]
fn topk_tie_break_is_deterministic_by_label_set() {
    // k=1 forces the tie at step 0 to resolve: labels ascending → app=a.
    let aggs = vec![(pulsus_logql::VectorAggOp::Topk, None, Some(1.0))];
    let by_app = points_by_app(apply_vector_aggs_ok(matrix_fixture(), &aggs));
    assert_eq!(by_app["a"], vec![(0, 5.0)]);
    assert_eq!(by_app["b"], vec![(STEP, 3.0)]);
    assert!(!by_app.contains_key("c"), "{by_app:?}");
}

#[test]
fn bottomk_selects_the_lowest_per_step() {
    let aggs = vec![(pulsus_logql::VectorAggOp::Bottomk, None, Some(1.0))];
    let by_app = points_by_app(apply_vector_aggs_ok(matrix_fixture(), &aggs));
    assert_eq!(by_app["b"], vec![(0, 3.0)]);
    assert_eq!(by_app["a"], vec![(STEP, 1.0)]);
    assert!(!by_app.contains_key("c"));
}

/// Review round 1 finding 3 (oracle-probed): NaN ranks LAST for BOTH
/// `topk` and `bottomk` — `topk(2)` over `{NaN, 5, 1}` selects `{5, 1}`
/// and `bottomk(2)` selects `{1, 5}`; a NaN is never preferred over a
/// finite value.
#[test]
fn topk_and_bottomk_rank_nan_last_in_both_directions() {
    let vector = QueryResult::Vector(vec![
        VectorSample {
            labels: vec![("app".to_string(), "a".to_string())],
            value: f64::NAN,
        },
        VectorSample {
            labels: vec![("app".to_string(), "b".to_string())],
            value: 5.0,
        },
        VectorSample {
            labels: vec![("app".to_string(), "c".to_string())],
            value: 1.0,
        },
    ]);
    let by_app = |r: QueryResult| -> Vec<String> {
        let QueryResult::Vector(items) = r else {
            panic!("expected a vector");
        };
        let mut apps: Vec<String> = items.into_iter().map(|s| s.labels[0].1.clone()).collect();
        apps.sort();
        apps
    };
    let topk2 = vec![(pulsus_logql::VectorAggOp::Topk, None, Some(2.0))];
    assert_eq!(
        by_app(apply_vector_aggs_ok(vector.clone(), &topk2)),
        vec!["b", "c"],
        "topk must not select NaN over finite values"
    );
    let bottomk2 = vec![(pulsus_logql::VectorAggOp::Bottomk, None, Some(2.0))];
    assert_eq!(
        by_app(apply_vector_aggs_ok(vector.clone(), &bottomk2)),
        vec!["b", "c"],
        "bottomk must not select NaN over finite values"
    );
    // NaN is still selectable once every finite value is taken.
    let topk3 = vec![(pulsus_logql::VectorAggOp::Topk, None, Some(3.0))];
    assert_eq!(
        by_app(apply_vector_aggs_ok(vector, &topk3)),
        vec!["a", "b", "c"]
    );
}

/// The same NaN rule on the RANGE (per-step) selection path.
#[test]
fn range_topk_ranks_nan_last_per_step() {
    let matrix = QueryResult::Matrix(vec![
        MatrixSeries {
            labels: vec![("app".to_string(), "a".to_string())],
            points: vec![(0, f64::NAN), (STEP, 2.0)],
        },
        MatrixSeries {
            labels: vec![("app".to_string(), "b".to_string())],
            points: vec![(0, 1.0), (STEP, f64::NAN)],
        },
    ]);
    let topk1 = vec![(pulsus_logql::VectorAggOp::Topk, None, Some(1.0))];
    let by_app = points_by_app(apply_vector_aggs_ok(matrix, &topk1));
    // Step 0: finite 1.0 (b) beats NaN (a); step 60: finite 2.0 (a)
    // beats NaN (b).
    assert_eq!(by_app["a"], vec![(STEP, 2.0)]);
    assert_eq!(by_app["b"], vec![(0, 1.0)]);
}

#[test]
fn stddev_and_stdvar_vector_aggregations_are_population_flavored() {
    let vector = QueryResult::Vector(
        [1.0, 2.0, 3.0, 4.0]
            .iter()
            .enumerate()
            .map(|(i, v)| VectorSample {
                labels: vec![("i".to_string(), i.to_string())],
                value: *v,
            })
            .collect(),
    );
    // Oracle transcript: stddev(1,2,3,4) = 1.118033988749895 (population),
    // stdvar = 1.25.
    let aggs = vec![(pulsus_logql::VectorAggOp::Stddev, None, None)];
    let v = single_vector_value(apply_vector_aggs_ok(vector.clone(), &aggs));
    assert_eq!(v, 1.118033988749895);
    let aggs = vec![(pulsus_logql::VectorAggOp::Stdvar, None, None)];
    let v = single_vector_value(apply_vector_aggs_ok(vector, &aggs));
    assert_eq!(v, 1.25);
}

// ---------------------------------------------------------------------
// Issue #221: approx_topk — count-min-sketch estimates through the real
// `apply_vector_aggs` chain. The collision tokens and expected estimates
// were derived offline (brute-forced cell positions) and VERIFIED by
// executing the reference `pkg/logql`/`pkg/logql/sketch` package at
// v3.7.4 — never hand-computed.
// ---------------------------------------------------------------------

fn lvl_vector(items: &[(&str, f64)]) -> QueryResult {
    QueryResult::Vector(
        items
            .iter()
            .map(|(tok, v)| VectorSample {
                labels: vec![("lvl".to_string(), tok.to_string())],
                value: *v,
            })
            .collect(),
    )
}

fn sorted_pairs(result: QueryResult) -> Vec<(String, f64)> {
    let QueryResult::Vector(items) = result else {
        panic!("expected a vector");
    };
    let mut out: Vec<(String, f64)> = items
        .into_iter()
        .map(|s| (s.labels[0].1.clone(), s.value))
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// AC 8 (two-way): the reference-verified full-7-row collision fixture
/// returns the sketch ESTIMATES (7/7/5) through `apply_vector_aggs`,
/// while `topk` over the SAME input returns the true values (3/4/5) —
/// the proof this is a real sketch, not `topk` in disguise.
#[test]
fn approx_topk_emits_the_sketch_estimate_not_the_true_value() {
    let input = [("v0095169", 3.0), ("v0125949", 4.0), ("ctrl", 5.0)];
    let approx = vec![(pulsus_logql::VectorAggOp::ApproxTopk, None, Some(3.0))];
    assert_eq!(
        sorted_pairs(apply_vector_aggs_ok(lvl_vector(&input), &approx)),
        vec![
            ("ctrl".to_string(), 5.0),
            ("v0095169".to_string(), 7.0),
            ("v0125949".to_string(), 7.0),
        ],
        "reference-verified estimates: the colliding pair reports 7, the clean series 5"
    );
    let topk = vec![(pulsus_logql::VectorAggOp::Topk, None, Some(3.0))];
    assert_eq!(
        sorted_pairs(apply_vector_aggs_ok(lvl_vector(&input), &topk)),
        vec![
            ("ctrl".to_string(), 5.0),
            ("v0095169".to_string(), 3.0),
            ("v0125949".to_string(), 4.0),
        ],
        "topk over the same input keeps the true values"
    );
}

/// When no cells collide, the estimate equals the true value and the
/// selection equals `topk`, value for value.
#[test]
fn approx_topk_is_exact_when_no_cells_collide() {
    let input = [
        ("alpha", 12.0),
        ("beta", 9.0),
        ("gamma", 7.0),
        ("delta", 4.0),
        ("epsilon", 2.0),
    ];
    let approx = vec![(pulsus_logql::VectorAggOp::ApproxTopk, None, Some(3.0))];
    let topk = vec![(pulsus_logql::VectorAggOp::Topk, None, Some(3.0))];
    assert_eq!(
        sorted_pairs(apply_vector_aggs_ok(lvl_vector(&input), &approx)),
        sorted_pairs(apply_vector_aggs_ok(lvl_vector(&input), &topk)),
    );
}

/// Under-`k` returns every series (live-probed reference behaviour:
/// `approx_topk(10, ...)` over 5 series returns all 5).
#[test]
fn approx_topk_under_k_returns_every_series() {
    let input = [("a", 3.0), ("b", 2.0), ("c", 1.0)];
    let approx = vec![(pulsus_logql::VectorAggOp::ApproxTopk, None, Some(10.0))];
    assert_eq!(
        sorted_pairs(apply_vector_aggs_ok(lvl_vector(&input), &approx)).len(),
        3
    );
}

/// AC 9 + AC 18: byte-identical output under three permutations of the
/// input series AND under permutations of each series' own label order
/// — pins the canonical-order + in-place-normalization determinism (the
/// reference's own order is a randomized Go map walk).
#[test]
fn approx_topk_is_insertion_order_independent() {
    let base = [("v0095169", 3.0), ("v0125949", 4.0), ("ctrl", 5.0)];
    let shuffles: [[usize; 3]; 3] = [[0, 1, 2], [2, 0, 1], [1, 2, 0]];
    let approx = vec![(pulsus_logql::VectorAggOp::ApproxTopk, None, Some(2.0))];
    let expect = sorted_pairs(apply_vector_aggs_ok(lvl_vector(&base), &approx));
    for order in shuffles {
        let shuffled: Vec<(&str, f64)> = order.iter().map(|&i| base[i]).collect();
        assert_eq!(
            sorted_pairs(apply_vector_aggs_ok(lvl_vector(&shuffled), &approx)),
            expect,
        );
    }
    // Per-series label order: the same two-label set given in both
    // orders normalizes in place to one canonical key.
    let two_labels = |flip: bool| {
        let mut labels = vec![
            ("app".to_string(), "x".to_string()),
            ("lvl".to_string(), "err".to_string()),
        ];
        if flip {
            labels.reverse();
        }
        QueryResult::Vector(vec![
            VectorSample { labels, value: 6.0 },
            VectorSample {
                labels: vec![("lvl".to_string(), "info".to_string())],
                value: 2.0,
            },
        ])
    };
    let a = apply_vector_aggs_ok(two_labels(false), &approx);
    let b = apply_vector_aggs_ok(two_labels(true), &approx);
    let flatten = |r: QueryResult| {
        let QueryResult::Vector(items) = r else {
            panic!("expected a vector");
        };
        let mut out: Vec<(Vec<(String, String)>, u64)> = items
            .into_iter()
            .map(|s| (s.labels, s.value.to_bits()))
            .collect();
        out.sort();
        out
    };
    assert_eq!(flatten(a), flatten(b));
}

/// B9: retention stops at exactly the reference heap size (10 000) and
/// drops the minimum-estimate entry; an over-cap `k` then returns every
/// RETAINED series.
#[test]
fn approx_topk_retention_cap_is_the_reference_heap_size() {
    let cap = 10_000usize;
    let items: Vec<(String, f64)> = (0..=cap)
        .map(|i| (format!("t{i:05}"), (i + 1) as f64))
        .collect();
    let input: Vec<(&str, f64)> = items.iter().map(|(t, v)| (t.as_str(), *v)).collect();
    let approx = vec![(
        pulsus_logql::VectorAggOp::ApproxTopk,
        None,
        Some((cap + 1) as f64),
    )];
    let out = sorted_pairs(apply_vector_aggs_ok(lvl_vector(&input), &approx));
    assert_eq!(out.len(), cap, "exactly CMS_MAX_LABELS series retained");
    assert!(
        !out.iter().any(|(t, _)| t == "t00000"),
        "the minimum-estimate series (true value 1) is the one dropped"
    );
}

// ---------------------------------------------------------------------
// AC4c: binary operations — both orientations, `^` associativity,
// mixed precedence, `bool`, comparisons, set ops.
// ---------------------------------------------------------------------

/// Hermetic evaluator over LEAFLESS node trees (scalar arithmetic goes
/// through the REAL parser + planner + `combine_binary`).
fn eval_scalar_query(query: &str) -> f64 {
    let expr = parse(query).expect("parse");
    let p = plan(&expr, &instant_params(60 * NS), &ctx()).expect("plan");
    let Plan::MetricBinary(node) = p else {
        panic!("expected a MetricBinary plan for {query}");
    };
    fn eval(node: &MetricNode) -> Result<QueryResult, ReadError> {
        match node {
            MetricNode::Scalar(v) => Ok(QueryResult::Scalar(*v)),
            MetricNode::VectorLit { value, window } => materialize_vector_lit(*value, window),
            MetricNode::Binary {
                op,
                return_bool,
                matching,
                lhs,
                rhs,
            } => combine_binary(*op, *return_bool, matching.as_ref(), eval(lhs)?, eval(rhs)?),
            MetricNode::VectorAgg { aggs, inner } => Ok(apply_vector_aggs_ok(eval(inner)?, aggs)),
            MetricNode::Leaf(_) | MetricNode::Variants { .. } => {
                panic!("scalar-only trees expected")
            }
        }
    }
    match eval(&node).expect("eval") {
        QueryResult::Scalar(v) => v,
        other => panic!("expected a scalar, got {other:?}"),
    }
}

#[test]
fn caret_evaluates_right_associatively() {
    // Oracle transcript: `2 ^ 2 ^ 3` = 256 (2^(2^3)), never 64.
    assert_eq!(eval_scalar_query("2 ^ 2 ^ 3"), 256.0);
}

#[test]
fn mixed_precedence_evaluates_multiplication_first() {
    assert_eq!(eval_scalar_query("1 + 2 * 3"), 7.0);
    assert_eq!(eval_scalar_query("(1 + 2) * 3"), 9.0);
}

#[test]
fn scalar_scalar_comparison_yields_zero_or_one_with_or_without_bool() {
    // Oracle-probed: the reference returns 1/0 for scalar comparisons
    // even without `bool`.
    assert_eq!(eval_scalar_query("2 > 1"), 1.0);
    assert_eq!(eval_scalar_query("2 > bool 1"), 1.0);
    assert_eq!(eval_scalar_query("1 > 2"), 0.0);
}

fn one_sample_vector(v: f64) -> QueryResult {
    QueryResult::Vector(vec![VectorSample {
        labels: vec![("app".to_string(), "x".to_string())],
        value: v,
    }])
}

/// D4: noncommutative operand orientation. `2 - vec(8)` and `vec(8) - 2`
/// must differ; probed live (`20 - sum(10)` = 10 on the oracle).
#[test]
fn scalar_left_and_scalar_right_subtraction_differ() {
    let left = combine_binary(
        BinOp::Sub,
        false,
        None,
        QueryResult::Scalar(2.0),
        one_sample_vector(8.0),
    )
    .unwrap();
    assert_eq!(single_vector_value(left), -6.0);
    let right = combine_binary(
        BinOp::Sub,
        false,
        None,
        one_sample_vector(8.0),
        QueryResult::Scalar(2.0),
    )
    .unwrap();
    assert_eq!(single_vector_value(right), 6.0);
}

#[test]
fn scalar_left_division_and_power_preserve_orientation() {
    let div = combine_binary(
        BinOp::Div,
        false,
        None,
        QueryResult::Scalar(100.0),
        one_sample_vector(4.0),
    )
    .unwrap();
    assert_eq!(single_vector_value(div), 25.0);
    let pow = combine_binary(
        BinOp::Pow,
        false,
        None,
        QueryResult::Scalar(2.0),
        one_sample_vector(3.0),
    )
    .unwrap();
    assert_eq!(single_vector_value(pow), 8.0);
}

#[test]
fn comparison_filters_keep_the_vector_value_in_both_orientations() {
    // Oracle-probed: `5 < vec(10)` keeps the sample with value 10.
    let kept = combine_binary(
        BinOp::Lt,
        false,
        None,
        QueryResult::Scalar(5.0),
        one_sample_vector(10.0),
    )
    .unwrap();
    assert_eq!(single_vector_value(kept), 10.0);
    let dropped = combine_binary(
        BinOp::Gt,
        false,
        None,
        QueryResult::Scalar(5.0),
        one_sample_vector(10.0),
    )
    .unwrap();
    assert_eq!(dropped, QueryResult::Vector(Vec::new()));
    // vector-left: vec(10) > 100 drops; vec(10) > 5 keeps 10.
    let kept = combine_binary(
        BinOp::Gt,
        false,
        None,
        one_sample_vector(10.0),
        QueryResult::Scalar(5.0),
    )
    .unwrap();
    assert_eq!(single_vector_value(kept), 10.0);
    let dropped = combine_binary(
        BinOp::Gt,
        false,
        None,
        one_sample_vector(10.0),
        QueryResult::Scalar(100.0),
    )
    .unwrap();
    assert_eq!(dropped, QueryResult::Vector(Vec::new()));
}

#[test]
fn bool_comparison_returns_zero_or_one_and_never_filters() {
    // Oracle transcript: `vec(10) > bool 5` = 1.
    let hit = combine_binary(
        BinOp::Gt,
        true,
        None,
        one_sample_vector(10.0),
        QueryResult::Scalar(5.0),
    )
    .unwrap();
    assert_eq!(single_vector_value(hit), 1.0);
    let miss = combine_binary(
        BinOp::Gt,
        true,
        None,
        one_sample_vector(10.0),
        QueryResult::Scalar(100.0),
    )
    .unwrap();
    assert_eq!(single_vector_value(miss), 0.0);
}

fn two_sample_vector(a: f64, b: f64) -> QueryResult {
    QueryResult::Vector(vec![
        VectorSample {
            labels: vec![("app".to_string(), "a".to_string())],
            value: a,
        },
        VectorSample {
            labels: vec![("app".to_string(), "b".to_string())],
            value: b,
        },
    ])
}

fn vector_by_app(result: QueryResult) -> HashMap<String, f64> {
    let QueryResult::Vector(items) = result else {
        panic!("expected a vector");
    };
    items
        .into_iter()
        .map(|s| (s.labels[0].1.clone(), s.value))
        .collect()
}

#[test]
fn vector_vector_arithmetic_matches_on_identical_full_label_sets() {
    let lhs = two_sample_vector(10.0, 20.0);
    let rhs = QueryResult::Vector(vec![
        VectorSample {
            labels: vec![("app".to_string(), "a".to_string())],
            value: 4.0,
        },
        // app=c has no lhs partner — dropped; lhs app=b has no rhs
        // partner — dropped.
        VectorSample {
            labels: vec![("app".to_string(), "c".to_string())],
            value: 9.0,
        },
    ]);
    let by_app = vector_by_app(combine_binary(BinOp::Sub, false, None, lhs, rhs).unwrap());
    assert_eq!(by_app.len(), 1);
    assert_eq!(by_app["a"], 6.0);
}

#[test]
fn and_or_unless_are_label_set_operations() {
    let lhs = two_sample_vector(1.0, 2.0); // apps a, b
    let rhs = QueryResult::Vector(vec![
        VectorSample {
            labels: vec![("app".to_string(), "b".to_string())],
            value: 99.0,
        },
        VectorSample {
            labels: vec![("app".to_string(), "c".to_string())],
            value: 100.0,
        },
    ]);
    let and =
        vector_by_app(combine_binary(BinOp::And, false, None, lhs.clone(), rhs.clone()).unwrap());
    assert_eq!(and.len(), 1);
    assert_eq!(and["b"], 2.0, "and keeps LHS values");
    let or =
        vector_by_app(combine_binary(BinOp::Or, false, None, lhs.clone(), rhs.clone()).unwrap());
    assert_eq!(or.len(), 3);
    assert_eq!(or["a"], 1.0);
    assert_eq!(or["b"], 2.0, "or prefers LHS on a label-set collision");
    assert_eq!(or["c"], 100.0);
    let unless = vector_by_app(combine_binary(BinOp::Unless, false, None, lhs, rhs).unwrap());
    assert_eq!(unless.len(), 1);
    assert_eq!(unless["a"], 1.0);
}

#[test]
fn matrix_binary_ops_align_per_shared_step() {
    let lhs = QueryResult::Matrix(vec![MatrixSeries {
        labels: vec![("app".to_string(), "a".to_string())],
        points: vec![(0, 10.0), (STEP, 20.0)],
    }]);
    let rhs = QueryResult::Matrix(vec![MatrixSeries {
        labels: vec![("app".to_string(), "a".to_string())],
        // Only step 0 is shared.
        points: vec![(0, 4.0), (2 * STEP, 1.0)],
    }]);
    let QueryResult::Matrix(items) =
        combine_binary(BinOp::Add, false, None, lhs.clone(), rhs.clone()).unwrap()
    else {
        panic!("expected a matrix");
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].points, vec![(0, 14.0)]);
    // `or` unions per step: lhs points win, rhs fills gaps.
    let QueryResult::Matrix(items) = combine_binary(BinOp::Or, false, None, lhs, rhs).unwrap()
    else {
        panic!("expected a matrix");
    };
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].points,
        vec![(0, 10.0), (STEP, 20.0), (2 * STEP, 1.0)]
    );
}

#[test]
fn set_operations_against_a_scalar_are_a_named_rejection() {
    // Oracle-probed: 400 "unexpected literal for right leg of
    // logical/set binary operation (and)".
    let err = combine_binary(
        BinOp::And,
        false,
        None,
        one_sample_vector(1.0),
        QueryResult::Scalar(2.0),
    )
    .unwrap_err();
    let ReadError::PipelineInvalid { reason } = &err else {
        panic!("expected PipelineInvalid, got {err:?}");
    };
    assert!(
        reason.contains("logical/set binary operation (and)"),
        "{reason}"
    );
}

// ---------------------------------------------------------------------
// Issue #91: vector-matching modifiers (on/ignoring/group_left/
// group_right). Semantics oracle-pinned against grafana/loki:3.4.2.
// ---------------------------------------------------------------------

use pulsus_logql::{MatchGroup, VectorMatching};

fn sample(labels: &[(&str, &str)], value: f64) -> VectorSample {
    VectorSample {
        labels: labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        value,
    }
}

fn on(labels: &[&str], group: Option<MatchGroup>) -> VectorMatching {
    VectorMatching {
        on: true,
        labels: labels.iter().map(|s| s.to_string()).collect(),
        group,
    }
}

fn ignoring(labels: &[&str], group: Option<MatchGroup>) -> VectorMatching {
    VectorMatching {
        on: false,
        labels: labels.iter().map(|s| s.to_string()).collect(),
        group,
    }
}

fn as_vector(result: QueryResult) -> Vec<VectorSample> {
    let QueryResult::Vector(items) = result else {
        panic!("expected a vector, got {result:?}");
    };
    items
}

/// `on(app)` one-to-one: output labels are the REDUCED signature (just
/// `app`), NOT the full LHS label set. Oracle-pinned.
#[test]
fn on_one_to_one_output_is_the_reduced_signature() {
    let lhs = QueryResult::Vector(vec![sample(&[("app", "p"), ("inst", "1")], 10.0)]);
    let rhs = QueryResult::Vector(vec![sample(&[("app", "p"), ("zone", "z")], 2.0)]);
    let out =
        as_vector(combine_binary(BinOp::Div, false, Some(&on(&["app"], None)), lhs, rhs).unwrap());
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].labels, vec![("app".to_string(), "p".to_string())]);
    assert_eq!(out[0].value, 5.0);
}

/// `ignoring(inst)` one-to-one: the signature drops `inst`, so two series
/// differing only in `inst` match; output is the reduced set (`app`).
#[test]
fn ignoring_one_to_one_drops_the_listed_label_from_the_signature() {
    let lhs = QueryResult::Vector(vec![sample(&[("app", "p"), ("inst", "1")], 8.0)]);
    let rhs = QueryResult::Vector(vec![sample(&[("app", "p"), ("inst", "2")], 4.0)]);
    let out = as_vector(
        combine_binary(
            BinOp::Div,
            false,
            Some(&ignoring(&["inst"], None)),
            lhs,
            rhs,
        )
        .unwrap(),
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].labels, vec![("app".to_string(), "p".to_string())]);
    assert_eq!(out[0].value, 2.0);
}

/// `on(app) group_left(extra)`: the MANY (lhs) side passes through whole;
/// the `extra` include label is copied from the ONE (rhs) side.
#[test]
fn group_left_passes_many_side_through_and_copies_include_labels() {
    let lhs = QueryResult::Vector(vec![
        sample(&[("app", "p"), ("inst", "1")], 10.0),
        sample(&[("app", "p"), ("inst", "2")], 20.0),
    ]);
    let rhs = QueryResult::Vector(vec![sample(&[("app", "p"), ("extra", "E")], 2.0)]);
    let out = as_vector(
        combine_binary(
            BinOp::Div,
            false,
            Some(&on(
                &["app"],
                Some(MatchGroup::Left(vec!["extra".to_string()])),
            )),
            lhs,
            rhs,
        )
        .unwrap(),
    );
    assert_eq!(out.len(), 2);
    // Full many-side labels + copied `extra=E`, key-sorted.
    assert_eq!(
        out[0].labels,
        vec![
            ("app".to_string(), "p".to_string()),
            ("extra".to_string(), "E".to_string()),
            ("inst".to_string(), "1".to_string()),
        ]
    );
    assert_eq!(out[0].value, 5.0);
    assert_eq!(out[1].value, 10.0);
    assert_eq!(out[1].labels[2], ("inst".to_string(), "2".to_string()));
}

/// `on(app) group_right`: rhs is the many side; output = full rhs labels,
/// and the value restores source operand order (lhs OP rhs).
#[test]
fn group_right_makes_rhs_the_many_side_and_restores_value_order() {
    let lhs = QueryResult::Vector(vec![sample(&[("app", "p")], 100.0)]);
    let rhs = QueryResult::Vector(vec![
        sample(&[("app", "p"), ("inst", "1")], 10.0),
        sample(&[("app", "p"), ("inst", "2")], 20.0),
    ]);
    let out = as_vector(
        combine_binary(
            BinOp::Div,
            false,
            Some(&on(&["app"], Some(MatchGroup::Right(vec![])))),
            lhs,
            rhs,
        )
        .unwrap(),
    );
    assert_eq!(out.len(), 2);
    // Full many-side (rhs) labels; value = lhs/rhs = 100/10, 100/20.
    assert_eq!(
        out[0].labels,
        vec![
            ("app".to_string(), "p".to_string()),
            ("inst".to_string(), "1".to_string()),
        ]
    );
    assert_eq!(out[0].value, 10.0);
    assert_eq!(out[1].value, 5.0);
}

/// An empty include value drops the label (upstream treats `""` as
/// absent).
#[test]
fn group_left_include_with_empty_value_drops_the_label() {
    let lhs = QueryResult::Vector(vec![sample(&[("app", "p"), ("inst", "1")], 10.0)]);
    let rhs = QueryResult::Vector(vec![sample(&[("app", "p"), ("extra", "")], 2.0)]);
    let out = as_vector(
        combine_binary(
            BinOp::Mul,
            false,
            Some(&on(
                &["app"],
                Some(MatchGroup::Left(vec!["extra".to_string()])),
            )),
            lhs,
            rhs,
        )
        .unwrap(),
    );
    assert_eq!(out.len(), 1);
    // `extra` absent — only the many-side labels survive.
    assert_eq!(
        out[0].labels,
        vec![
            ("app".to_string(), "p".to_string()),
            ("inst".to_string(), "1".to_string()),
        ]
    );
}

/// A second LHS series matching an already-consumed one-to-one signature
/// is the "many-to-one matching must be explicit" error (oracle-pinned).
#[test]
fn one_to_one_second_many_side_match_is_multiple_matches_error() {
    let lhs = QueryResult::Vector(vec![
        sample(&[("app", "p"), ("inst", "1")], 10.0),
        sample(&[("app", "p"), ("inst", "2")], 20.0),
    ]);
    let rhs = QueryResult::Vector(vec![sample(&[("app", "p")], 2.0)]);
    let err = combine_binary(BinOp::Div, false, Some(&on(&["app"], None)), lhs, rhs).unwrap_err();
    let ReadError::PipelineInvalid { reason } = &err else {
        panic!("expected PipelineInvalid, got {err:?}");
    };
    assert!(
        reason.contains("multiple matches for labels: many-to-one matching must be explicit"),
        "{reason}"
    );
}

/// A duplicate ONE-side signature is many-to-many — errors for EVERY
/// cardinality, including a plain one-to-one (the one-side map is built
/// unconditionally). Oracle-pinned wording.
#[test]
fn duplicate_one_side_signature_is_many_to_many_error() {
    let lhs = QueryResult::Vector(vec![sample(&[("app", "p"), ("inst", "1")], 10.0)]);
    // Two rhs series reduce to the same on(app) signature.
    let rhs = QueryResult::Vector(vec![
        sample(&[("app", "p"), ("inst", "1")], 2.0),
        sample(&[("app", "p"), ("inst", "2")], 3.0),
    ]);
    let err = combine_binary(
        BinOp::Div,
        false,
        Some(&on(&["app"], Some(MatchGroup::Left(vec![])))),
        lhs,
        rhs,
    )
    .unwrap_err();
    let ReadError::PipelineInvalid { reason } = &err else {
        panic!("expected PipelineInvalid, got {err:?}");
    };
    assert!(
        reason.contains(
            "many-to-many matching not allowed: matching labels must be unique on one side"
        ),
        "{reason}"
    );
    assert!(
        reason.contains("found duplicate series on the right hand-side"),
        "{reason}"
    );
}

/// A duplicate ONE/RHS-side signature under a PLAIN one-to-one match (no
/// group_left/group_right) is many-to-many — the one-side map is built
/// unconditionally, so it errors even without a group modifier. Distinct
/// from `duplicate_one_side_signature_is_many_to_many_error` (group_left)
/// and from `one_to_one_second_many_side_match_is_multiple_matches_error`
/// (LHS-side). Loki-verbatim wording (grafana/loki:3.4.2).
#[test]
fn plain_one_to_one_duplicate_rhs_signature_is_many_to_many_error() {
    let lhs = QueryResult::Vector(vec![sample(&[("app", "p"), ("inst", "1")], 10.0)]);
    // Two rhs series reduce to the same on(app) signature.
    let rhs = QueryResult::Vector(vec![
        sample(&[("app", "p"), ("inst", "1")], 2.0),
        sample(&[("app", "p"), ("inst", "2")], 3.0),
    ]);
    // NOTE: on(app) with group = None — plain one-to-one, the missing AC3 case.
    let err = combine_binary(BinOp::Div, false, Some(&on(&["app"], None)), lhs, rhs).unwrap_err();
    let ReadError::PipelineInvalid { reason } = &err else {
        panic!("expected PipelineInvalid, got {err:?}");
    };
    // Full byte-exact string (side = "right", not swapped) — anchors the
    // whole message, catching side-label and wording drift.
    assert_eq!(
        reason,
        "found duplicate series on the right hand-side;many-to-many matching \
         not allowed: matching labels must be unique on one side"
    );
}

/// The empty-operand short-circuit is scoped to arithmetic/comparison: a
/// duplicate one-side signature that could never pair (empty other side)
/// must NOT surface a spurious error.
#[test]
fn empty_operand_short_circuits_arithmetic_before_duplicate_detection() {
    let lhs: QueryResult = QueryResult::Vector(vec![]);
    let rhs = QueryResult::Vector(vec![
        sample(&[("app", "p"), ("inst", "1")], 2.0),
        sample(&[("app", "p"), ("inst", "2")], 3.0),
    ]);
    let out = combine_binary(
        BinOp::Div,
        false,
        Some(&on(&["app"], Some(MatchGroup::Left(vec![])))),
        lhs,
        rhs,
    )
    .unwrap();
    assert_eq!(out, QueryResult::Vector(Vec::new()));
}

/// Set ops key on the reduced signature under `on`/`ignoring`, and their
/// empty-operand semantics differ from arithmetic (NO short-circuit):
/// `lhs or ∅`→lhs, `∅ or rhs`→rhs, `lhs and ∅`→∅, `lhs unless ∅`→lhs.
#[test]
fn set_ops_key_on_signature_and_keep_their_own_empty_semantics() {
    let lhs = QueryResult::Vector(vec![sample(&[("app", "p"), ("inst", "1")], 1.0)]);
    let rhs = QueryResult::Vector(vec![sample(&[("app", "p"), ("zone", "z")], 9.0)]);
    // `and on(app)`: signatures match on app -> lhs survives (LHS value).
    let out = as_vector(
        combine_binary(
            BinOp::And,
            false,
            Some(&on(&["app"], None)),
            lhs.clone(),
            rhs.clone(),
        )
        .unwrap(),
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].value, 1.0);
    // `lhs or ∅` -> lhs.
    let empty = QueryResult::Vector(vec![]);
    let out = as_vector(
        combine_binary(
            BinOp::Or,
            false,
            Some(&on(&["app"], None)),
            lhs.clone(),
            empty.clone(),
        )
        .unwrap(),
    );
    assert_eq!(out.len(), 1);
    // `∅ or rhs` -> rhs.
    let out = as_vector(
        combine_binary(
            BinOp::Or,
            false,
            Some(&on(&["app"], None)),
            empty.clone(),
            rhs.clone(),
        )
        .unwrap(),
    );
    assert_eq!(out.len(), 1);
    // `lhs and ∅` -> ∅.
    let out = combine_binary(
        BinOp::And,
        false,
        Some(&on(&["app"], None)),
        lhs.clone(),
        empty.clone(),
    )
    .unwrap();
    assert_eq!(out, QueryResult::Vector(Vec::new()));
    // `lhs unless ∅` -> lhs.
    let out = as_vector(
        combine_binary(
            BinOp::Unless,
            false,
            Some(&on(&["app"], None)),
            lhs.clone(),
            empty,
        )
        .unwrap(),
    );
    assert_eq!(out.len(), 1);
}

/// MATRIX per-step join: two same-(reduced-)signature series whose points
/// never share a step must NOT error; a same-timestamp ambiguity MUST.
#[test]
fn matrix_join_is_per_step_scoped_for_duplicate_detection() {
    // Same on(app) signature, DISJOINT timestamps on the one side -> no
    // per-step ambiguity, no error.
    let lhs = QueryResult::Matrix(vec![MatrixSeries {
        labels: vec![
            ("app".to_string(), "p".to_string()),
            ("inst".to_string(), "1".to_string()),
        ],
        points: vec![(0, 10.0), (STEP, 20.0)],
    }]);
    let rhs = QueryResult::Matrix(vec![
        MatrixSeries {
            labels: vec![
                ("app".to_string(), "p".to_string()),
                ("z".to_string(), "a".to_string()),
            ],
            points: vec![(0, 2.0)],
        },
        MatrixSeries {
            labels: vec![
                ("app".to_string(), "p".to_string()),
                ("z".to_string(), "b".to_string()),
            ],
            points: vec![(STEP, 4.0)],
        },
    ]);
    let QueryResult::Matrix(items) =
        combine_binary(BinOp::Div, false, Some(&on(&["app"], None)), lhs, rhs).unwrap()
    else {
        panic!("expected a matrix");
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].labels, vec![("app".to_string(), "p".to_string())]);
    assert_eq!(items[0].points, vec![(0, 5.0), (STEP, 5.0)]);

    // Same signature COLLIDING at one timestamp -> per-step error.
    let lhs = QueryResult::Matrix(vec![MatrixSeries {
        labels: vec![
            ("app".to_string(), "p".to_string()),
            ("inst".to_string(), "1".to_string()),
        ],
        points: vec![(0, 10.0)],
    }]);
    let rhs = QueryResult::Matrix(vec![
        MatrixSeries {
            labels: vec![
                ("app".to_string(), "p".to_string()),
                ("z".to_string(), "a".to_string()),
            ],
            points: vec![(0, 2.0)],
        },
        MatrixSeries {
            labels: vec![
                ("app".to_string(), "p".to_string()),
                ("z".to_string(), "b".to_string()),
            ],
            points: vec![(0, 4.0)],
        },
    ]);
    let err = combine_binary(
        BinOp::Div,
        false,
        Some(&on(&["app"], Some(MatchGroup::Left(vec![])))),
        lhs,
        rhs,
    )
    .unwrap_err();
    assert!(matches!(err, ReadError::PipelineInvalid { .. }));
}

/// MATRIX set ops with an empty opposite operand on the RANGE path
/// (adjudicated coverage): `or` returns the non-empty side, `unless`
/// keeps lhs, `and` empties — all per step.
#[test]
fn matrix_set_ops_with_empty_operand_per_step() {
    let lhs = QueryResult::Matrix(vec![MatrixSeries {
        labels: vec![("app".to_string(), "p".to_string())],
        points: vec![(0, 10.0), (STEP, 20.0)],
    }]);
    let empty = QueryResult::Matrix(vec![]);
    // `lhs or ∅` -> lhs unchanged.
    let QueryResult::Matrix(items) = combine_binary(
        BinOp::Or,
        false,
        Some(&on(&["app"], None)),
        lhs.clone(),
        empty.clone(),
    )
    .unwrap() else {
        panic!("matrix");
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].points, vec![(0, 10.0), (STEP, 20.0)]);
    // `lhs unless ∅` -> lhs.
    let QueryResult::Matrix(items) = combine_binary(
        BinOp::Unless,
        false,
        Some(&on(&["app"], None)),
        lhs.clone(),
        empty.clone(),
    )
    .unwrap() else {
        panic!("matrix");
    };
    assert_eq!(items[0].points, vec![(0, 10.0), (STEP, 20.0)]);
    // `lhs and ∅` -> ∅.
    let out = combine_binary(BinOp::And, false, Some(&on(&["app"], None)), lhs, empty).unwrap();
    assert_eq!(out, QueryResult::Matrix(Vec::new()));
}

/// MATRIX set ops on the RANGE path with the EMPTY operand on the LEFT
/// (issue #91, review round 2 test-gap 4 — the reversed companions to the
/// `lhs OP ∅` cases above, previously untested): `∅ or rhs` -> rhs and
/// `∅ unless rhs` -> ∅, both at the per-step level. Semantics pinned
/// against `grafana/loki:3.4.2`'s set-op empty-operand handling (`set_op`
/// in binop.rs): `or` yields whichever side is present; `unless` with no
/// lhs has nothing to keep.
#[test]
fn matrix_set_ops_with_empty_left_operand_per_step() {
    let empty = QueryResult::Matrix(vec![]);
    let rhs = QueryResult::Matrix(vec![
        MatrixSeries {
            labels: vec![("app".to_string(), "p".to_string())],
            points: vec![(0, 10.0), (STEP, 20.0)],
        },
        MatrixSeries {
            labels: vec![("app".to_string(), "q".to_string())],
            points: vec![(STEP, 7.0)],
        },
    ]);
    // `∅ or rhs` -> rhs unchanged, per step (every rhs step surfaces).
    let QueryResult::Matrix(items) = combine_binary(
        BinOp::Or,
        false,
        Some(&on(&["app"], None)),
        empty.clone(),
        rhs.clone(),
    )
    .unwrap() else {
        panic!("matrix");
    };
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].labels, vec![("app".to_string(), "p".to_string())]);
    assert_eq!(items[0].points, vec![(0, 10.0), (STEP, 20.0)]);
    assert_eq!(items[1].labels, vec![("app".to_string(), "q".to_string())]);
    assert_eq!(items[1].points, vec![(STEP, 7.0)]);
    // `∅ unless rhs` -> ∅ (no lhs series to keep at any step).
    let out = combine_binary(BinOp::Unless, false, Some(&on(&["app"], None)), empty, rhs).unwrap();
    assert_eq!(out, QueryResult::Matrix(Vec::new()));
}

/// Issue #91, review round 2 test-gap 3: the "grouping labels must ensure
/// unique matches" error path — reachable when `group_left`/`group_right`
/// include-label copying COLLAPSES two distinct many-side output labels
/// into one identity. Here `ignoring(y) group_left(y)` reduces both many
/// series (`y=p`, `y=q`) to the same `on`-signature `{x}`, then copies `y`
/// from a one side that HAS no `y` (so `y` is dropped from the output),
/// making both many series render as the identical `{x}` output — the
/// duplicate grouped identity Prometheus/Loki reject. Oracle-pinned live
/// against `grafana/loki:3.4.2` (HTTP 500, byte-identical body:
/// "multiple matches for labels: grouping labels must ensure unique
/// matches").
#[test]
fn group_left_include_collapsing_distinct_many_labels_is_grouping_unique_error() {
    let lhs = QueryResult::Vector(vec![
        sample(&[("x", "1"), ("y", "p")], 10.0),
        sample(&[("x", "1"), ("y", "q")], 20.0),
    ]);
    // The one side carries no `y`, so copying the `y` include drops it
    // from BOTH many-side outputs -> both collapse to `{x=1}`.
    let rhs = QueryResult::Vector(vec![sample(&[("x", "1")], 2.0)]);
    let err = combine_binary(
        BinOp::Div,
        false,
        Some(&ignoring(
            &["y"],
            Some(MatchGroup::Left(vec!["y".to_string()])),
        )),
        lhs,
        rhs,
    )
    .unwrap_err();
    let ReadError::PipelineInvalid { reason } = &err else {
        panic!("expected PipelineInvalid, got {err:?}");
    };
    assert_eq!(
        reason, "multiple matches for labels: grouping labels must ensure unique matches",
        "byte-identical to the grafana/loki:3.4.2 oracle body"
    );
}

/// Issue #91, review round 2 finding 2: a matching modifier
/// (`on`/`ignoring`/`group_left`/`group_right`) on a binop with a SCALAR
/// operand. Prometheus rejects a non-empty `on`/`ignoring` list here
/// ("vector matching only allowed between instant vectors"), and the
/// review expected Loki to mirror that — but the `grafana/loki:3.4.2`
/// oracle does NOT: it SILENTLY ACCEPTS the modifier and ignores it
/// (probed live — `sum(...) > on(x) 5`, `... + on(x) 5`, `... > on(x)
/// group_left(y) 5`, scalar on either side all return HTTP 200 with the
/// modifier discarded). The engine already mirrors this — the scalar arms
/// of `combine_binary` never consult `matching` — so this test locks the
/// oracle-parity behavior (evaluate, don't reject) against a future
/// "add the Prometheus rejection" regression.
#[test]
fn a_matching_modifier_on_a_scalar_operand_is_ignored_matching_the_loki_oracle() {
    let vector = QueryResult::Vector(vec![
        sample(&[("app", "p")], 10.0),
        sample(&[("app", "q")], 3.0),
    ]);
    let matching = on(&["app"], Some(MatchGroup::Left(vec!["y".to_string()])));
    // vector OP scalar: `> 5` filters on value, modifier ignored.
    let out = as_vector(
        combine_binary(
            BinOp::Gt,
            false,
            Some(&matching),
            vector.clone(),
            QueryResult::Scalar(5.0),
        )
        .unwrap(),
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].labels, vec![("app".to_string(), "p".to_string())]);
    assert_eq!(out[0].value, 10.0);
    // scalar OP vector: arithmetic applies to every sample, modifier
    // ignored (never a "vector matching only allowed..." rejection).
    let out = as_vector(
        combine_binary(
            BinOp::Add,
            false,
            Some(&matching),
            QueryResult::Scalar(100.0),
            vector,
        )
        .unwrap(),
    );
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].value, 110.0);
    assert_eq!(out[1].value, 103.0);
}

// ---------------------------------------------------------------------
// Issue M8-LQ3 / #218: `rate_counter` reset-aware per-second increase and
// `sort`/`sort_desc` value ordering.
//
// ORACLE NOTE: `rate_counter` replays the pinned reference's ns/ms
// `extrapolatedRate(samples, [1m], isCounter=true, isRate=true)`
// (grafana/loki:3.7.3). The result is the reset-aware increase scaled by a
// TINY extrapolation factor `1 + 60000/span_ns`, then divided by the 60s
// window. Two quirks make it diverge from a plain `increase / 60s`: the
// window iterators store timestamps in nanoseconds but `extrapolatedRate`
// divides every span by 1000 (treating ns as ms), and the extrapolation
// window is anchored on the FIRST sample (so `durationToStart == 60`,
// `durationToEnd == 0`). This is NOT Prometheus full-range extrapolation
// (which would push the sparse case toward ~6.0); the ns/ms unit mix keeps
// the factor at ~1.00006. Every value below therefore depends on the
// sample SPAN — the goldens use per-span values, not one uniform factor.
// These are pinned bit-for-bit against grafana/loki:3.7.3 (each e2e-span
// witness was replayed against the live container) and cross-checked by the
// gated e2e vectors (`metric_rate_counter_*`). If the reference diverges
// the e2e gate goes RED and these are corrected in place.
// ---------------------------------------------------------------------

/// Runs an instant `rate_counter` query over a single-series counter body
/// (`c=<n>`, `logfmt | unwrap c`), returning the one per-second value. The
/// `[1m]` window sets `selRange = 60s`; the returned value is the
/// reset-aware increase scaled by the ns/ms extrapolation factor and
/// divided by 60s (see the ORACLE NOTE above).
fn rate_counter_instant(rows: &[MetricScanRow]) -> f64 {
    let params = instant_params(60 * NS);
    single_vector_value(
        run_client(
            r#"rate_counter({env="prod"} | logfmt | unwrap c [1m])"#,
            &params,
            rows,
            &meta_one(),
        )
        .unwrap(),
    )
}

/// Rule 1 — a counter that RESETS mid-window. Values 10,30,5,12 by ts:
/// +20 (10→30), reset +5 (drop to 5, counts from zero), +7 (5→12) = 32.
/// Samples span 10s→40s = 30e9 ns, so the ns/ms extrapolation factor is
/// `(30e6 + 60000/1000) / 30e6 = 1.000002`; `32 * 1.000002 / 60 =
/// 0.5333344`.
#[test]
fn rate_counter_reset_counts_the_post_reset_value_from_zero() {
    let rows = vec![
        row(1, 10 * NS, "c=10"),
        row(1, 20 * NS, "c=30"),
        row(1, 30 * NS, "c=5"), // reset: 5 < 30
        row(1, 40 * NS, "c=12"),
    ];
    assert_eq!(rate_counter_instant(&rows), 0.5333344);
}

/// Rule 3 — duplicate-timestamp samples are processed in DELIVERED scan
/// order (the reducer's stable sort by timestamp preserves the SQL
/// `ORDER BY timestamp_ns, fingerprint, body` order), NOT re-sorted by
/// value. Branch-validated against the pinned reference (v3.7.3): values
/// 5,{10,3}@20s,12 delivered as 5,10,3,12 → +5 (5→10), reset +3 (10→3),
/// +9 (3→12) = 17. Samples span 10s→30s = 20e9 ns, so the ns/ms
/// extrapolation factor is `(20e6 + 60) / 20e6 = 1.000003`; `17 * 1.000003
/// / 60 = 0.2833341833333333`. An ascending-value tie-sort would instead
/// see 5,3,10,12 → increase 12 (a different value), which the reference
/// does NOT produce — so the delivered order is load-bearing and this
/// vector pins it.
#[test]
fn rate_counter_duplicate_timestamp_preserves_delivered_scan_order() {
    // Delivered in the SQL scan order (ts asc, then fingerprint/body): the
    // two ts=20s samples arrive as c=10 then c=3.
    let rows = vec![
        row(1, 10 * NS, "c=5"),
        row(1, 20 * NS, "c=10"),
        row(1, 20 * NS, "c=3"), // same ts as c=10, delivered after it
        row(1, 30 * NS, "c=12"),
    ];
    assert_eq!(rate_counter_instant(&rows), 0.2833341833333333);
    // The stable sort keys only on timestamp, so distinct-ts samples are
    // reordered into ascending time regardless of arrival, but the tied
    // pair keeps whatever relative order it was delivered in.
    let shuffled_distinct = vec![
        row(1, 30 * NS, "c=12"),
        row(1, 10 * NS, "c=5"),
        row(1, 20 * NS, "c=10"),
        row(1, 20 * NS, "c=3"),
    ];
    assert_eq!(rate_counter_instant(&shuffled_distinct), 0.2833341833333333);
}

/// Rule 2 — samples ON the window boundaries both contribute. Monotone
/// 100→160 with no reset ⇒ increase 60. Samples span 1ns→60e9 ns, so the
/// ns/ms extrapolation factor is `(≈60e6 + 60) / ≈60e6 = 1.000001`;
/// `60 * 1.000001 / 60 = 1.000001`. (The SQL half-open `(t-range, t]`
/// window inclusion itself is validated by the gated e2e
/// `metric_rate_counter_boundary` vector; the hermetic reducer sees
/// whatever rows it is handed.)
#[test]
fn rate_counter_boundary_samples_contribute() {
    let rows = vec![row(1, 1, "c=100"), row(1, 60 * NS, "c=160")];
    assert_eq!(rate_counter_instant(&rows), 1.000001);
}

/// No Prometheus full-range extrapolation — sparse interior samples 200→260
/// (span 10s) do NOT inflate toward 6.0 the way boundary-extrapolated
/// Prometheus `rate` would. The reference's ns/ms unit mix keeps the factor
/// at `(10e6 + 60) / 10e6 = 1.000006`, so `increase 60 * 1.000006 / 60 =
/// 1.000006` — just above 1.0, NOT 6.0.
#[test]
fn rate_counter_sparse_window_does_not_full_range_extrapolate() {
    let rows = vec![row(1, 25 * NS, "c=200"), row(1, 35 * NS, "c=260")];
    assert_eq!(rate_counter_instant(&rows), 1.000006);
}

/// A single-sample `rate_counter` group is EMITTED as `0.0`, NOT dropped —
/// pinned against grafana/loki:3.7.3 (a lone `c=42` returns value `"0"`;
/// `extrapolatedRate` returns 0.0 for `<2`-point groups and the reference's
/// range evaluator surfaces it as a 0-valued vector element, it does not
/// filter it out). `single_vector_value` asserts the vector has exactly one
/// element, so a regression to "dropped" fails loudly here.
#[test]
fn rate_counter_single_sample_group_emits_zero() {
    let rows = vec![row(1, 10 * NS, "c=42")];
    assert_eq!(rate_counter_instant(&rows), 0.0);
}

// The four e2e-span exact-f64 goldens (#218): the `rate_counter` witnesses
// in the e2e corpus (`e2e/src/logs_corpus.rs`) sit at `step_ns = 1s`, so
// their spans differ from the per-span goldens above. These pin the exact
// f64 the nightly `oracle_vs_corpus` leg compares against the live
// container — each literal was observed BYTE-FOR-BYTE from grafana/loki:3.7.3
// at the fixture's exact samples/offsets, establishing bit-exact parity
// hermetically (the `1e-9` e2e path is the live regression gate, not the
// source of the bit-exact claim).

/// e2e reset witness (c=10,30,5,12 at offsets 6..9 ⇒ span 3s): increase 32,
/// factor `(3e6 + 60)/3e6 = 1.00002`, / 60s = `0.5333439999999999`
/// (container-observed, byte-for-byte).
#[test]
fn rate_counter_e2e_reset_span_matches_container() {
    let rows = vec![
        row(1, 10 * NS, "c=10"),
        row(1, 11 * NS, "c=30"),
        row(1, 12 * NS, "c=5"), // reset
        row(1, 13 * NS, "c=12"),
    ];
    assert_eq!(rate_counter_instant(&rows), 0.5333439999999999);
}

/// e2e sparse witness (c=200,260 at offsets 10,11 ⇒ span 1s): increase 60,
/// factor `(1e6 + 60)/1e6 = 1.00006`, / 60s = `1.00006`
/// (container-observed).
#[test]
fn rate_counter_e2e_sparse_span_matches_container() {
    let rows = vec![row(1, 10 * NS, "c=200"), row(1, 11 * NS, "c=260")];
    assert_eq!(rate_counter_instant(&rows), 1.00006);
}

/// e2e boundary witness (c=100,160 at offsets 12,13 ⇒ span 1s): increase
/// 60, factor `1.00006`, / 60s = `1.00006` (container-observed).
#[test]
fn rate_counter_e2e_boundary_span_matches_container() {
    let rows = vec![row(1, 10 * NS, "c=100"), row(1, 11 * NS, "c=160")];
    assert_eq!(rate_counter_instant(&rows), 1.00006);
}

/// e2e dup_ts witness (c=5,{10,3}@offset15,12 ⇒ span 2s): delivered order
/// 5,10,3,12 ⇒ increase 17, factor `(2e6 + 60)/2e6 = 1.00003`, / 60s =
/// `0.2833418333333333` (container-observed; a value tie-sort would not).
#[test]
fn rate_counter_e2e_dup_ts_span_matches_container() {
    let rows = vec![
        row(1, 10 * NS, "c=5"),
        row(1, 11 * NS, "c=10"),
        row(1, 11 * NS, "c=3"), // tied ts, delivered after c=10
        row(1, 12 * NS, "c=12"),
    ];
    assert_eq!(rate_counter_instant(&rows), 0.2833418333333333);
}

/// Rule 2 (AC10) — the DISCRIMINATING lower-boundary test: the lookback is
/// half-open `(t-range, t]`, so a sample sitting EXACTLY at `t-range` is
/// EXCLUDED (branch-validated v3.7.3: a lone in-window point past such an
/// excluded start sample yields increase 0). This is enforced by the raw
/// scan's `timestamp_ns > {start_ns}` predicate (strict lower bound), NOT
/// by the reducer — so the discriminator is the generated SQL plus the
/// value the surviving rows reduce to. A closed `[t-range, t]`
/// interpretation would (a) render `>=` here and (b) INCLUDE the `t-range`
/// sample, changing the result — this test fails under that reading.
///
/// Instant `t = 90s`, `[1m]` ⇒ `start_ns = 30s` (`= t-range`),
/// `end_ns = 90s` (`= t`). Samples at 30s (`c=100`, the excluded start),
/// 60s (`c=140`, interior), 90s (`c=200`, the included end). Half-open:
/// only 140→200 survive (span 60s→90s = 30e9 ns) ⇒ increase 60 scaled by
/// `(30e6 + 60)/30e6 = 1.000002`, / 60s = 1.000002. Closed: 100→140→200
/// (span 30s→90s = 60e9 ns) ⇒ increase 100 scaled by `(60e6 + 60)/60e6 =
/// 1.000001`, / 60s ≈ 1.6666683333333332 — a DIFFERENT value, which the
/// assertions below reject.
#[test]
fn rate_counter_excludes_a_sample_at_exactly_t_minus_range() {
    let at_ns = 90 * NS;
    let params = instant_params(at_ns);
    let mp = metric_plan_of(
        r#"rate_counter({env="prod"} | logfmt | unwrap c [1m])"#,
        &params,
    );
    // The plan's window is exactly `(t-range, t]`.
    assert_eq!(mp.start_ns, 30 * NS, "start_ns must be t-range");
    assert_eq!(mp.end_ns, at_ns, "end_ns must be t");

    // The enforcing layer: the raw-scan predicate is STRICT on the lower
    // bound (`>`), so the `t-range` sample is filtered out server-side. A
    // closed interval would render `>=` and this assertion would fail.
    let sql = pulsus_read::logql::sql::metric_raw_samples(
        &mp.table,
        &["checkout".to_string()],
        &[1],
        pulsus_read::logql::sql::TimeWindow {
            start_ns: mp.start_ns,
            end_ns: mp.end_ns,
        },
        mp.scan_lower,
        &mp.extra_predicates,
    );
    assert!(
        sql.contains(&format!(
            "timestamp_ns > {} AND timestamp_ns <= {}",
            30 * NS,
            at_ns
        )),
        "raw-scan window must be half-open `(t-range, t]` (strict `>`): {sql}"
    );

    // The result difference: only the survivors of the half-open window
    // reduce; the `t-range=30s` sample is excluded, the `t=90s` end sample
    // included.
    let all = vec![
        row(1, 30 * NS, "c=100"), // exactly t-range — excluded
        row(1, 60 * NS, "c=140"), // interior
        row(1, at_ns, "c=200"),   // exactly t — included
    ];
    let survivors: Vec<_> = all
        .iter()
        .filter(|r| r.timestamp_ns > mp.start_ns && r.timestamp_ns <= mp.end_ns)
        .cloned()
        .collect();
    let half_open = single_vector_value(
        run_client(
            r#"rate_counter({env="prod"} | logfmt | unwrap c [1m])"#,
            &params,
            &survivors,
            &meta_one(),
        )
        .unwrap(),
    );
    assert_eq!(half_open, 1.000002, "excludes the t-range sample");

    // The closed-interval reading (all three rows) would give a DIFFERENT
    // value — proving the boundary rule is load-bearing, not cosmetic.
    let closed = single_vector_value(
        run_client(
            r#"rate_counter({env="prod"} | logfmt | unwrap c [1m])"#,
            &params,
            &all,
            &meta_one(),
        )
        .unwrap(),
    );
    assert_eq!(closed, 1.6666683333333332);
    assert_ne!(
        half_open, closed,
        "the t-range sample must change the result (half-open ≠ closed)"
    );
}

/// `rate_counter` requires `unwrap` (it reduces unwrapped counter values);
/// without it the planner rejects with the oracle-shaped arity message.
#[test]
fn rate_counter_without_unwrap_is_rejected() {
    let params = instant_params(60 * NS);
    let expr = parse(r#"rate_counter({env="prod"}[1m])"#).expect("parse");
    match plan(&expr, &params, &ctx()) {
        Err(ReadError::PipelineInvalid { reason }) => {
            assert_eq!(reason, "invalid aggregation rate_counter without unwrap");
        }
        other => panic!("expected PipelineInvalid, got {other:?}"),
    }
}

// --- sort / sort_desc value ordering ---------------------------------

fn sort_vector_fixture() -> QueryResult {
    QueryResult::Vector(vec![
        VectorSample {
            labels: vec![("app".to_string(), "a".to_string())],
            value: 5.0,
        },
        VectorSample {
            labels: vec![("app".to_string(), "b".to_string())],
            value: 1.0,
        },
        VectorSample {
            labels: vec![("app".to_string(), "c".to_string())],
            value: 5.0, // ties app=a
        },
        VectorSample {
            labels: vec![("app".to_string(), "d".to_string())],
            value: f64::NAN,
        },
    ])
}

fn ordered_apps(result: QueryResult) -> Vec<String> {
    let QueryResult::Vector(items) = result else {
        panic!("expected a vector, got {result:?}");
    };
    items.into_iter().map(|s| s.labels[0].1.clone()).collect()
}

/// `sort` orders the returned vector ascending by value; equal values
/// break by label set ascending (a before c); NaN ranks LAST.
#[test]
fn sort_orders_the_vector_ascending_with_nan_last() {
    let aggs = vec![(pulsus_logql::VectorAggOp::Sort, None, None)];
    assert_eq!(
        ordered_apps(apply_vector_aggs_ok(sort_vector_fixture(), &aggs)),
        vec!["b", "a", "c", "d"]
    );
}

/// `sort_desc` orders descending; the 5.0 tie still breaks by label set
/// ascending (a before c); NaN ranks LAST in BOTH directions.
#[test]
fn sort_desc_orders_the_vector_descending_with_nan_last() {
    let aggs = vec![(pulsus_logql::VectorAggOp::SortDesc, None, None)];
    assert_eq!(
        ordered_apps(apply_vector_aggs_ok(sort_vector_fixture(), &aggs)),
        vec!["a", "c", "b", "d"]
    );
}

/// A range `sort(...)` yields a matrix with no single sortable value per
/// series, so it is a passthrough — the series set is unchanged.
#[test]
fn range_sort_is_a_passthrough_over_the_matrix() {
    let matrix = QueryResult::Matrix(vec![
        MatrixSeries {
            labels: vec![("app".to_string(), "a".to_string())],
            points: vec![(0, 5.0), (STEP, 1.0)],
        },
        MatrixSeries {
            labels: vec![("app".to_string(), "b".to_string())],
            points: vec![(0, 3.0)],
        },
    ]);
    for op in [
        pulsus_logql::VectorAggOp::Sort,
        pulsus_logql::VectorAggOp::SortDesc,
    ] {
        let out = apply_vector_aggs_ok(matrix.clone(), &[(op, None, None)]);
        let QueryResult::Matrix(items) = out else {
            panic!("expected a matrix");
        };
        let by_app: HashMap<String, Vec<(i64, f64)>> = items
            .into_iter()
            .map(|s| (s.labels[0].1.clone(), s.points))
            .collect();
        assert_eq!(by_app["a"], vec![(0, 5.0), (STEP, 1.0)]);
        assert_eq!(by_app["b"], vec![(0, 3.0)]);
    }
}

// ---------------------------------------------------------------------
// Issue #221: `vector(<scalar>)`.
// ---------------------------------------------------------------------

/// Plans `query` into its `MetricBinary` node tree (the leafless/binary
/// path `vector()` takes).
fn metric_node_of(query: &str, params: &QueryParams) -> MetricNode {
    let expr = parse(query).expect("parse");
    match plan(&expr, params, &ctx()).expect("plan") {
        Plan::MetricBinary(node) => node,
        other => panic!("expected a MetricBinary plan for {query}, got {other:?}"),
    }
}

/// Evaluates a full `MetricNode` tree the engine's way: leaves run the
/// client-aggregation over `rows`, everything else combines in-Rust.
fn eval_node(
    node: &MetricNode,
    rows: &[MetricScanRow],
    meta: &HashMap<u64, StreamMetaRow>,
) -> Result<QueryResult, ReadError> {
    match node {
        MetricNode::Scalar(v) => Ok(QueryResult::Scalar(*v)),
        MetricNode::VectorLit { value, window } => materialize_vector_lit(*value, window),
        MetricNode::Leaf(mp) => {
            let client = mp.client.as_ref().expect("client-aggregated plan");
            let compiled = CompiledPipeline::compile(&client.pipeline).expect("compile");
            let result = run_client_agg_rows(
                rows,
                &compiled,
                meta,
                client,
                match mp.step_ns {
                    Some(step_ns) => ClientWindow::Range {
                        grid_start_ns: mp.grid_start_ns,
                        end_ns: mp.end_ns,
                        step_ns,
                        range_ns: mp.range_ns,
                    },
                    None => ClientWindow::Instant {
                        start_ns: mp.grid_start_ns,
                        end_ns: mp.end_ns,
                    },
                },
                mp.rate_window_ns,
            )?;
            Ok(apply_vector_aggs_ok(result, &mp.vector_aggs))
        }
        MetricNode::Binary {
            op,
            return_bool,
            matching,
            lhs,
            rhs,
        } => combine_binary(
            *op,
            *return_bool,
            matching.as_ref(),
            eval_node(lhs, rows, meta)?,
            eval_node(rhs, rows, meta)?,
        ),
        MetricNode::VectorAgg { aggs, inner } => {
            Ok(apply_vector_aggs_ok(eval_node(inner, rows, meta)?, aggs))
        }
        // `variants(...) of (...)` (issue #221): the pure fan-out twin,
        // over the scan plan's (unwrap-truncated) common pipeline.
        MetricNode::Variants { scan, variants, .. } => {
            let common = scan
                .client
                .as_ref()
                .expect("variants scan is client-aggregated");
            run_variants_rows(rows, meta, &common.pipeline, variants)
        }
    }
}

#[test]
fn instant_vector_lit_is_a_single_empty_label_sample() {
    let node = metric_node_of("vector(5)", &instant_params(60 * NS));
    let out = eval_node(&node, &[], &meta_one()).expect("eval");
    let QueryResult::Vector(items) = out else {
        panic!("expected a vector, got {out:?}");
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].labels, Vec::<(String, String)>::new());
    assert_eq!(items[0].value, 5.0);
}

#[test]
fn instant_vector_lit_binop_combines_as_vectors() {
    // `vector(1) + vector(2)` => `{} 3`.
    let node = metric_node_of("vector(1) + vector(2)", &instant_params(60 * NS));
    assert_eq!(
        single_vector_value(eval_node(&node, &[], &meta_one()).unwrap()),
        3.0
    );
}

#[test]
fn sum_of_a_vector_lit_is_accepted_and_aggregates() {
    // Unlike bare `sum(5)` (rejected), Loki accepts `sum(vector(5))` => `{} 5`.
    let node = metric_node_of("sum(vector(5))", &instant_params(60 * NS));
    assert_eq!(
        single_vector_value(eval_node(&node, &[], &meta_one()).unwrap()),
        5.0
    );
}

#[test]
fn or_vector_zero_fills_an_empty_selection() {
    // The canonical use: `sum(rate({...}[1m])) or vector(0)` on an empty
    // selection yields `{} 0`.
    let node = metric_node_of(
        r#"sum(rate({service_name="checkout"} | logfmt [1m])) or vector(0)"#,
        &instant_params(60 * NS),
    );
    let out = eval_node(&node, &[], &meta_one()).expect("eval");
    let QueryResult::Vector(items) = out else {
        panic!("expected a vector, got {out:?}");
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].labels, Vec::<(String, String)>::new());
    assert_eq!(items[0].value, 0.0);
}

/// Over-cap range `vector(n)` rejects with the SAME `MetricBuckets` 422 a
/// leaf over-cap range query trips — before allocating any grid (the
/// `return` precedes the `Vec`, so a successful assertion proves no
/// allocation). 11_001 step INTERVALS > the 11_000 cap (issue #227 review
/// round 7, finding 1: the fence is the reference's TRUNCATING
/// `(end-start)/step > 11000`, so exactly 11_000 intervals — 11_001
/// inclusive grid points — is served, and this fixture sits one interval
/// past it).
#[test]
fn range_vector_lit_over_the_bucket_cap_rejects_without_allocating() {
    let window = pulsus_read::logql::GridWindow {
        start_ns: 0,
        end_ns: 11_001 * NS,
        step_ns: Some(
            pulsus_read::logql::validate_duration_ns(NS as u64, "step").expect("valid step"),
        ),
    };
    match materialize_vector_lit(0.0, &window) {
        Err(ReadError::QueryTooBroad(TooBroadReason::MetricBuckets { buckets, cap })) => {
            assert_eq!(buckets, 11_001);
            assert_eq!(cap, 11_000);
        }
        other => panic!("expected a MetricBuckets too-broad error, got {other:?}"),
    }
}

/// At exactly the cap — 11_000 intervals, the widest resolution the
/// reference serves — the range `vector(n)` materializes a constant matrix
/// with one empty-label series carrying the value at every one of the
/// 11_001 inclusive grid points (issue #227 review round 7, finding 1:
/// this exact shape was previously a wrong 422).
#[test]
fn range_vector_lit_at_the_bucket_cap_passes_with_exact_point_count() {
    let window = pulsus_read::logql::GridWindow {
        start_ns: 0,
        end_ns: 11_000 * NS,
        step_ns: Some(
            pulsus_read::logql::validate_duration_ns(NS as u64, "step").expect("valid step"),
        ),
    };
    let out = materialize_vector_lit(7.0, &window).expect("at-cap must pass");
    let points = single_series_points(out);
    assert_eq!(points.len(), 11_001);
    assert!(points.iter().all(|(_, v)| *v == 7.0));
}

/// The range grid is byte-identical to `bucket_grid`/`metric_range` even
/// when `start_ns` is not a multiple of the step, and a `data + vector(0)`
/// binop aligns on the data's populated steps.
#[test]
fn range_vector_lit_grid_aligns_under_an_unaligned_start() {
    let step = 10 * NS;
    let window = pulsus_read::logql::GridWindow {
        start_ns: 7 * NS,
        end_ns: 37 * NS,
        step_ns: Some(
            pulsus_read::logql::validate_duration_ns(step as u64, "step").expect("valid step"),
        ),
    };
    let vec_matrix = materialize_vector_lit(0.0, &window).expect("materialize");
    // Issue #227: the grid is START-anchored `{start + k·step ≤ end}`, so an
    // unaligned `start=7NS` yields `7NS, 17NS, 27NS, 37NS` (NOT epoch
    // multiples) — matching the sliding data leaves' grid.
    let start = 7 * NS;
    assert_eq!(
        single_series_points(vec_matrix.clone()),
        vec![
            (start, 0.0),
            (start + step, 0.0),
            (start + 2 * step, 0.0),
            (start + 3 * step, 0.0),
        ],
    );

    // A sparse data series populated only at two of the (start-anchored) grid
    // steps. Empty labels so it one-to-one matches vector(0)'s `{}` series —
    // the test isolates GRID/step alignment, not label matching.
    let data = QueryResult::Matrix(vec![MatrixSeries {
        labels: vec![],
        points: vec![(start + step, 4.0), (start + 3 * step, 9.0)],
    }]);
    let combined = combine_binary(BinOp::Add, false, None, data, vec_matrix).expect("combine");
    assert_eq!(
        single_series_points(combined),
        vec![(start + step, 4.0), (start + 3 * step, 9.0)],
    );
}

/// Round-4 regression: an `i64::MIN` range window must not panic/overflow —
/// `vector(n)` inherits `bucket_grid`'s i128 math, so the start-anchored grid
/// begins exactly at `i64::MIN` (k=0) without wrapping.
#[test]
fn range_vector_lit_is_i64_min_safe() {
    let window = pulsus_read::logql::GridWindow {
        start_ns: i64::MIN,
        end_ns: i64::MIN + 3 * NS,
        step_ns: Some(
            pulsus_read::logql::validate_duration_ns(NS as u64, "step").expect("valid step"),
        ),
    };
    let out = materialize_vector_lit(0.0, &window).expect("i64::MIN window must not panic");
    let points = single_series_points(out);
    assert!(!points.is_empty());
    // Start-anchored: the first grid point is `grid_start` = `i64::MIN` (k=0),
    // computed in i128 and cast back without wrapping.
    assert_eq!(points[0].0, i64::MIN);
}

/// `sort`/`sort_desc` reject a grouping clause — the reference has no
/// `sort by(x)(...)` form, so the planner 400s rather than silently
/// ignoring it.
#[test]
fn sort_with_a_grouping_is_rejected() {
    let params = instant_params(60 * NS);
    for query in [
        r#"sort by(app) (rate({env="prod"}[1m]))"#,
        r#"sort_desc by(app) (rate({env="prod"}[1m]))"#,
    ] {
        let expr = parse(query).expect("parse");
        match plan(&expr, &params, &ctx()) {
            Err(ReadError::PipelineInvalid { .. }) => {}
            other => panic!("expected {query:?} to be PipelineInvalid, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------
// Issue #221: `variants(<metricExpr>, …) of (<logRangeExpr>)` — hermetic
// goldens over the pure fan-out twin (`run_variants_rows` via
// `eval_node`). Reference semantics per the #221 plan §1 (live-probed
// against the pinned reference container): a variant's own selector/
// filters/parsers are dead syntax; its `[range]`, reducer, unwrap tail
// and post-`unwrap` filters are honoured; `__variant__` is the plain
// decimal index injected into an outer aggregation's grouping for BOTH
// `by` and `without`; a `__variant__`-less series is returned at instant
// and dropped at range.
// ---------------------------------------------------------------------

fn meta_env(entries: &[(u64, &str, &str)]) -> HashMap<u64, StreamMetaRow> {
    entries
        .iter()
        .map(|(fp, service, env)| {
            (
                *fp,
                StreamMetaRow {
                    fingerprint: *fp,
                    service: service.to_string(),
                    labels: format!(r#"{{"env":"{env}","service_name":"{service}"}}"#),
                },
            )
        })
        .collect()
}

fn sorted_vector(result: QueryResult) -> Vec<(Vec<(String, String)>, f64)> {
    let QueryResult::Vector(items) = result else {
        panic!("expected a vector, got {result:?}");
    };
    let mut out: Vec<(Vec<(String, String)>, f64)> =
        items.into_iter().map(|s| (s.labels, s.value)).collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn lbl(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// B10/B14 — two extractors over ONE dataset yield DIFFERENT values under
/// decimal `__variant__` indexes (a one-extractor implementation fails).
#[test]
fn variants_two_extractors_yield_different_values_per_index() {
    let node = metric_node_of(
        r#"variants(count_over_time({service_name="checkout"}[5m]), bytes_over_time({service_name="checkout"}[5m])) of ({service_name="checkout"}[5m])"#,
        &instant_params(60 * NS),
    );
    let rows = vec![
        row(1, 10 * NS, "ab"),
        row(1, 20 * NS, "ab"),
        row(1, 30 * NS, "ab"),
    ];
    let out = sorted_vector(eval_node(&node, &rows, &meta_one()).unwrap());
    assert_eq!(
        out,
        vec![
            (
                lbl(&[
                    ("__variant__", "0"),
                    ("env", "prod"),
                    ("service_name", "checkout")
                ]),
                3.0
            ),
            (
                lbl(&[
                    ("__variant__", "1"),
                    ("env", "prod"),
                    ("service_name", "checkout")
                ]),
                6.0
            ),
        ]
    );
}

/// B10 — the variant's OWN selector and line filters are DEAD SYNTAX:
/// only the common range selects data (reference `variantRangeAggExprExtractor`
/// passes nil stages; live-probed).
#[test]
fn variants_ignores_the_variants_own_selector_and_line_filters() {
    let node = metric_node_of(
        r#"variants(count_over_time({service_name="nope"} |= "zzzz" [5m])) of ({service_name="checkout"}[5m])"#,
        &instant_params(60 * NS),
    );
    let rows = vec![row(1, 10 * NS, "hello"), row(1, 20 * NS, "world")];
    let out = sorted_vector(eval_node(&node, &rows, &meta_one()).unwrap());
    assert_eq!(
        out,
        vec![(
            lbl(&[
                ("__variant__", "0"),
                ("env", "prod"),
                ("service_name", "checkout")
            ]),
            2.0
        )]
    );
}

/// B10 — each variant honours its OWN `[range]` on the shared instant
/// window; the scan window stays the common range's (live-probed: 30s vs
/// 5m over one dataset → 1 vs 2 here).
#[test]
fn variants_honour_their_own_range_at_instant() {
    let node = metric_node_of(
        r#"variants(count_over_time({service_name="checkout"}[30s]), count_over_time({service_name="checkout"}[5m])) of ({service_name="checkout"}[5m])"#,
        &instant_params(60 * NS),
    );
    // ts=10s is outside variant 0's (30s, 60s] window but inside 5m.
    let rows = vec![row(1, 10 * NS, "a"), row(1, 40 * NS, "b")];
    let out = sorted_vector(eval_node(&node, &rows, &meta_one()).unwrap());
    assert_eq!(
        out,
        vec![
            (
                lbl(&[
                    ("__variant__", "0"),
                    ("env", "prod"),
                    ("service_name", "checkout")
                ]),
                1.0
            ),
            (
                lbl(&[
                    ("__variant__", "1"),
                    ("env", "prod"),
                    ("service_name", "checkout")
                ]),
                2.0
            ),
        ]
    );
}

/// B14 — `__variant__` is injected into a `by` grouping; a bare `sum`
/// (nil grouping) becomes `by (__variant__)` (the reference's non-nil
/// default `Grouping`), so bare `sum` yields `{__variant__="i"}` only.
#[test]
fn variants_inject_the_index_into_by_and_bare_groupings() {
    let meta = meta_env(&[(1, "svc", "dev"), (2, "svc", "prod")]);
    let rows = vec![
        row(1, 10 * NS, "a"),
        row(1, 20 * NS, "b"),
        row(2, 10 * NS, "c"),
        row(2, 20 * NS, "d"),
        row(2, 30 * NS, "e"),
    ];
    let by_node = metric_node_of(
        r#"variants(sum by (env) (count_over_time({service_name="svc"}[5m]))) of ({service_name="svc"}[5m])"#,
        &instant_params(60 * NS),
    );
    assert_eq!(
        sorted_vector(eval_node(&by_node, &rows, &meta).unwrap()),
        vec![
            (lbl(&[("__variant__", "0"), ("env", "dev")]), 2.0),
            (lbl(&[("__variant__", "0"), ("env", "prod")]), 3.0),
        ]
    );
    let bare_node = metric_node_of(
        r#"variants(sum(count_over_time({service_name="svc"}[5m]))) of ({service_name="svc"}[5m])"#,
        &instant_params(60 * NS),
    );
    assert_eq!(
        sorted_vector(eval_node(&bare_node, &rows, &meta).unwrap()),
        vec![(lbl(&[("__variant__", "0")]), 5.0)]
    );
}

/// B15 (the killer case) — `__variant__` is injected into a `without`
/// grouping too, which STRIPS it; the resulting `__variant__`-less series
/// is PRESENT at instant and ABSENT at range (reference
/// `JoinMultiVariantSampleVector` vs `multiVariantVectorsToSeries` —
/// live-probed). A "just concatenate everything" implementation fails
/// the range half.
#[test]
fn variants_without_strips_the_index_kept_at_instant_dropped_at_range() {
    let meta = meta_env(&[(1, "svc", "dev"), (2, "svc", "prod")]);
    let rows = vec![
        row(1, 10 * NS, "a"),
        row(1, 20 * NS, "b"),
        row(2, 10 * NS, "c"),
        row(2, 20 * NS, "d"),
        row(2, 30 * NS, "e"),
    ];
    let query = r#"variants(sum without (env) (count_over_time({service_name="svc"}[5m]))) of ({service_name="svc"}[5m])"#;
    // Instant: the stripped series is KEPT.
    let node = metric_node_of(query, &instant_params(60 * NS));
    assert_eq!(
        sorted_vector(eval_node(&node, &rows, &meta).unwrap()),
        vec![(lbl(&[("service_name", "svc")]), 5.0)]
    );
    // Range: the same series is DROPPED (no `__variant__`).
    let node = metric_node_of(query, &range_params(0, 60 * NS));
    let out = eval_node(&node, &rows, &meta).unwrap();
    let QueryResult::Matrix(items) = out else {
        panic!("expected a matrix, got {out:?}");
    };
    assert!(
        items.is_empty(),
        "a __variant__-less series must be dropped at range, got {items:?}"
    );
}

/// B14 — an unwrap variant beside a count variant over one common
/// pipeline: each variant gets its OWN unwrap tail; the count variant
/// sees the common (label-mutating) pipeline only.
#[test]
fn variants_unwrap_tail_beside_a_count_variant() {
    let node = metric_node_of(
        r#"variants(sum_over_time({service_name="checkout"} | unwrap v [5m]), count_over_time({service_name="checkout"}[5m])) of ({service_name="checkout"} | logfmt [5m])"#,
        &instant_params(60 * NS),
    );
    let rows = vec![row(1, 10 * NS, "v=1"), row(1, 20 * NS, "v=2")];
    let out = sorted_vector(eval_node(&node, &rows, &meta_one()).unwrap());
    assert_eq!(
        out,
        vec![
            // variant 0: logfmt extracts v, unwrap consumes it (label
            // deleted) => one series summing 1+2.
            (
                lbl(&[
                    ("__variant__", "0"),
                    ("env", "prod"),
                    ("service_name", "checkout")
                ]),
                3.0
            ),
            // variant 1: the common logfmt keeps v as a LABEL => one
            // series per v value.
            (
                lbl(&[
                    ("__variant__", "1"),
                    ("env", "prod"),
                    ("service_name", "checkout"),
                    ("v", "1")
                ]),
                1.0
            ),
            (
                lbl(&[
                    ("__variant__", "1"),
                    ("env", "prod"),
                    ("service_name", "checkout"),
                    ("v", "2")
                ]),
                1.0
            ),
        ]
    );
}

/// B14 — post-`unwrap` label filters in a variant's tail ARE honoured
/// (reference `ReduceAndLabelFilter(PostFilters)`; live-probed 2 vs 3).
#[test]
fn variants_post_unwrap_label_filters_are_honoured() {
    let node = metric_node_of(
        r#"variants(sum_over_time({service_name="checkout"} | unwrap v | v > 1 [5m]), sum_over_time({service_name="checkout"} | unwrap v [5m])) of ({service_name="checkout"} | logfmt [5m])"#,
        &instant_params(60 * NS),
    );
    let rows = vec![row(1, 10 * NS, "v=1"), row(1, 20 * NS, "v=2")];
    let out = sorted_vector(eval_node(&node, &rows, &meta_one()).unwrap());
    assert_eq!(
        out,
        vec![
            (
                lbl(&[
                    ("__variant__", "0"),
                    ("env", "prod"),
                    ("service_name", "checkout")
                ]),
                2.0
            ),
            (
                lbl(&[
                    ("__variant__", "1"),
                    ("env", "prod"),
                    ("service_name", "checkout")
                ]),
                3.0
            ),
        ]
    );
}

/// Δ1 — a COMMON-range `unwrap` (and its post-`unwrap` filters) is dead
/// syntax: the common pipeline is truncated at the first `Stage::Unwrap`,
/// so lines whose unwrap label is missing/unconvertible are NOT dropped
/// and the dead post-filter never applies.
#[test]
fn variants_common_range_unwrap_is_dead_syntax() {
    let rows = vec![
        row(1, 10 * NS, r#"{"d":"x"}"#), // non-numeric d
        row(1, 20 * NS, r#"{"e":1}"#),   // no d at all
        row(1, 30 * NS, r#"{"d":5}"#),
    ];
    for query in [
        r#"variants(count_over_time({service_name="checkout"}[5m])) of ({service_name="checkout"} | json | unwrap d [5m])"#,
        r#"variants(count_over_time({service_name="checkout"}[5m])) of ({service_name="checkout"} | json | unwrap d | d > 1000 [5m])"#,
    ] {
        let node = metric_node_of(query, &instant_params(60 * NS));
        let out = eval_node(&node, &rows, &meta_one()).unwrap();
        let QueryResult::Vector(items) = out else {
            panic!("expected a vector, got {out:?}");
        };
        assert_eq!(
            items.len(),
            3,
            "{query}: one series per distinct d label set"
        );
        let total: f64 = items.iter().map(|s| s.value).sum();
        assert_eq!(
            total, 3.0,
            "{query}: every line counted — the dead unwrap dropped none"
        );
    }
}

/// Δ2 + the adjudicated absent correction — an `absent_over_time`
/// variant's synthetic labels come from the VARIANT's own (otherwise
/// dead) selector (`absentLabels(expr)` reads `expr.Selector() =
/// e.Left.Left`), and the series carries NO `__variant__` (the reference
/// attaches the index per extracted sample; a synthetic series never
/// passes through the extractor — container-captured): index-less and
/// KEPT at instant, DROPPED at range, `{}` under a bare `sum`.
#[test]
fn variants_absent_series_use_the_variants_selector_and_carry_no_index() {
    let query = r#"variants(absent_over_time({service_name="checkout", tier="x"}[5m])) of ({service_name="checkout"}[5m])"#;
    let node = metric_node_of(query, &instant_params(60 * NS));
    let out = sorted_vector(eval_node(&node, &[], &meta_one()).unwrap());
    assert_eq!(
        out,
        vec![(lbl(&[("service_name", "checkout"), ("tier", "x")]), 1.0)]
    );
    // Range: the index-less synthetic series is dropped entirely.
    let node = metric_node_of(query, &range_params(0, 60 * NS));
    let out = eval_node(&node, &[], &meta_one()).unwrap();
    let QueryResult::Matrix(items) = out else {
        panic!("expected a matrix, got {out:?}");
    };
    assert!(
        items.is_empty(),
        "absent series must drop at range: {items:?}"
    );
    // Under a bare `sum` the reference groups the index-less series to
    // `{}` (captured). PulsusDB's `group_key` `by`-grouping currently
    // materializes a MISSING grouped label as `name=""`
    // (`unwrap_or_default`) — a PRE-EXISTING engine semantic affecting
    // EVERY `by` over a missing label, nothing variants-specific.
    // OWNED BY #241, which captured the same divergence independently by
    // a different route and carries the root-cause fix (omit missing
    // labels from the `by` key, reference-exact). OBLIGATION: when #241
    // lands, re-capture and pin the `sum(absent_over_time(...))`
    // sub-case here and in `b13_variants.test` — it is a ready-made
    // acceptance test for that fix (expected `{} 1`). Until then the
    // sub-case stays excluded at both sites.
}

/// B16 — `append_variant_label` OVERRIDES a common-pipeline
/// `__variant__` (the reference appends a duplicate and mis-routes
/// samples — deliberately not reproduced; ledgered).
#[test]
fn variants_index_overrides_a_common_pipeline_variant_label() {
    let node = metric_node_of(
        r#"variants(count_over_time({service_name="checkout"}[5m])) of ({service_name="checkout"} | label_format __variant__="9" [5m])"#,
        &instant_params(60 * NS),
    );
    let rows = vec![row(1, 10 * NS, "a"), row(1, 20 * NS, "b")];
    let out = sorted_vector(eval_node(&node, &rows, &meta_one()).unwrap());
    assert_eq!(
        out,
        vec![(
            lbl(&[
                ("__variant__", "0"),
                ("env", "prod"),
                ("service_name", "checkout")
            ]),
            2.0
        )]
    );
}

/// Corpus case 11's hermetic twin — 11 variants: the index is plain
/// decimal (`"10"`, never zero-padded or capped).
#[test]
fn variants_eleventh_index_is_plain_decimal_ten() {
    let variants = (0..11)
        .map(|_| r#"count_over_time({service_name="checkout"}[5m])"#)
        .collect::<Vec<_>>()
        .join(", ");
    let node = metric_node_of(
        &format!(r#"variants({variants}) of ({{service_name="checkout"}}[5m])"#),
        &instant_params(60 * NS),
    );
    let rows = vec![row(1, 10 * NS, "a")];
    let out = sorted_vector(eval_node(&node, &rows, &meta_one()).unwrap());
    assert_eq!(out.len(), 11);
    let indexes: Vec<&str> = out
        .iter()
        .map(|(labels, _)| {
            labels
                .iter()
                .find(|(k, _)| k == "__variant__")
                .map(|(_, v)| v.as_str())
                .expect("__variant__ present")
        })
        .collect();
    assert!(
        indexes.contains(&"10"),
        "plain decimal index 10: {indexes:?}"
    );
    // Sorted label order puts "10" before "2" (string sort) — the
    // reference's instant label-sorted order.
    assert!(indexes.iter().position(|i| *i == "10") < indexes.iter().position(|i| *i == "2"));
}

/// A2/corpus 12 — `variants(…) of (…) + 1` composes as a binary operand:
/// every variant's value +1, `__variant__` preserved.
#[test]
fn variants_binary_composition_adds_to_every_variant() {
    let node = metric_node_of(
        r#"variants(count_over_time({service_name="checkout"}[5m]), bytes_over_time({service_name="checkout"}[5m])) of ({service_name="checkout"}[5m]) + 1"#,
        &instant_params(60 * NS),
    );
    let rows = vec![row(1, 10 * NS, "ab"), row(1, 20 * NS, "ab")];
    let out = sorted_vector(eval_node(&node, &rows, &meta_one()).unwrap());
    assert_eq!(
        out,
        vec![
            (
                lbl(&[
                    ("__variant__", "0"),
                    ("env", "prod"),
                    ("service_name", "checkout")
                ]),
                3.0
            ),
            (
                lbl(&[
                    ("__variant__", "1"),
                    ("env", "prod"),
                    ("service_name", "checkout")
                ]),
                5.0
            ),
        ]
    );
}

/// B13/B17 — error determinism: chunks are pushed into sub-states in
/// INDEX order, so with two failing-capable variants the raised
/// `MetricPipelineError` is the lowest-indexed variant's (here variant
/// 0's unwrap of the non-numeric `a`, whose failing series carries
/// `a`'s error, not `b`'s).
#[test]
fn variants_error_is_raised_by_the_lowest_indexed_variant() {
    let node = metric_node_of(
        r#"variants(sum_over_time({service_name="checkout"} | unwrap a [5m]), sum_over_time({service_name="checkout"} | unwrap b [5m])) of ({service_name="checkout"} | logfmt [5m])"#,
        &instant_params(60 * NS),
    );
    let rows = vec![row(1, 10 * NS, "a=x b=y")];
    let err = eval_node(&node, &rows, &meta_one()).expect_err("both unwraps fail");
    let ReadError::MetricPipelineError { error_type, series } = &err else {
        panic!("expected MetricPipelineError, got {err:?}");
    };
    assert_eq!(error_type, SAMPLE_EXTRACTION_ERROR);
    // Variant 0 unwraps `a` (value "x"); variant 1 would have failed on
    // `b` (value "y") — the reported detail names `a`'s value, proving
    // index order.
    assert!(
        series.contains(r#"parsing \"x\""#),
        "expected variant 0's failure (parsing \"x\"): {series}"
    );
}

/// B18 — every variant's window shares the SCAN plan's grid
/// (`grid_start_ns`/`end_ns`/`step_ns`); only `range_ns` differs.
#[test]
fn variants_share_the_scan_plans_grid() {
    let node = metric_node_of(
        r#"variants(count_over_time({service_name="checkout"}[30s]), count_over_time({service_name="checkout"}[10m])) of ({service_name="checkout"}[5m])"#,
        &range_params(7 * NS, 127 * NS),
    );
    let MetricNode::Variants { scan, variants, .. } = &node else {
        panic!("expected a Variants node");
    };
    for spec in variants {
        let ClientWindow::Range {
            grid_start_ns,
            end_ns,
            step_ns,
            ..
        } = spec.window()
        else {
            panic!("expected a Range window");
        };
        assert_eq!(grid_start_ns, scan.grid_start_ns);
        assert_eq!(end_ns, scan.end_ns);
        assert_eq!(Some(step_ns), scan.step_ns);
    }
    let windows: Vec<_> = variants
        .iter()
        .map(|s| match s.window() {
            ClientWindow::Range { range_ns, .. } => range_ns.get(),
            ClientWindow::Instant { .. } => unreachable!(),
        })
        .collect();
    assert_eq!(windows, vec![30 * NS, 600 * NS]);
}

/// Range-kind golden — corpus case 13's hermetic twin: matrix envelope,
/// `__variant__` at range, per-variant values on the shared grid.
#[test]
fn variants_range_matrix_carries_the_index_per_series() {
    let node = metric_node_of(
        r#"variants(count_over_time({service_name="checkout"}[1m]), bytes_over_time({service_name="checkout"}[1m])) of ({service_name="checkout"}[1m])"#,
        &range_params(0, 60 * NS),
    );
    let rows = vec![row(1, 10 * NS, "ab"), row(1, 20 * NS, "ab")];
    let out = eval_node(&node, &rows, &meta_one()).unwrap();
    let QueryResult::Matrix(mut items) = out else {
        panic!("expected a matrix, got {out:?}");
    };
    items.sort_by(|a, b| a.labels.cmp(&b.labels));
    assert_eq!(items.len(), 2);
    let idx = |s: &MatrixSeries| {
        s.labels
            .iter()
            .find(|(k, _)| k == "__variant__")
            .map(|(_, v)| v.clone())
            .expect("__variant__ present at range")
    };
    assert_eq!(idx(&items[0]), "0");
    assert_eq!(idx(&items[1]), "1");
    // count at the 60s grid point sees both rows; bytes sees 4 bytes.
    assert_eq!(items[0].points.last(), Some(&(60 * NS, 2.0)));
    assert_eq!(items[1].points.last(), Some(&(60 * NS, 4.0)));
}

/// AC 7's scope, counted rather than claimed (review round 1 `[low]`).
///
/// The bit-equality assertion executes for fixtures routed through
/// `run_client`; fixtures that drive `materialize_vector_lit`,
/// `apply_vector_aggs`, `combine_binary` or `run_variants_rows` directly
/// have no folded twin to compare against. This test reads the suite's
/// own source and pins both counts, so a sentence claiming "every
/// fixture" cannot outlive the mechanism again.
#[test]
fn every_client_routed_fixture_is_an_equivalence_case() {
    let src = include_str!("logql_metric_agg_golden.rs");
    let body = src
        .split_once("fn run_client(")
        .expect("run_client exists")
        .1;
    let routed = body.matches("run_client(").count();
    let direct = src.matches("apply_vector_aggs_ok(").count()
        + src.matches("materialize_vector_lit(").count()
        + src.matches("run_variants_rows(").count();
    assert!(
        routed >= 40,
        "only {routed} fixtures route through run_client — AC 7's equivalence assertion is \
         narrower than it looks"
    );
    assert!(
        direct > 0,
        "if nothing drives the engine directly any more, AC 7's scope caveat is stale and the \
         doc should say `every fixture` again"
    );
    // The claim and the mechanism, side by side: the caveat must exist
    // exactly while the direct fixtures do.
    assert!(
        body.contains("which is not the same as every fixture in the file")
            || src.contains("which is not the same as every fixture in the file"),
        "AC 7's scope caveat has been removed while direct fixtures still exist"
    );
}
