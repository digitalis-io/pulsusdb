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
use pulsus_read::logql::rows::{SampleRow, StreamMetaRow};
use pulsus_read::logql::{
    ClientWindow, CompiledPipeline, Direction, MatrixSeries, MetricNode, MetricPlan, Plan, PlanCtx,
    QueryParams, QueryResult, ReadError, SAMPLE_EXTRACTION_ERROR, VectorSample, apply_vector_aggs,
    combine_binary, plan, run_client_agg_rows,
};

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

fn row(fp: u64, ts_ns: i64, body: &str) -> SampleRow {
    SampleRow {
        fingerprint: fp,
        timestamp_ns: ts_ns,
        body: body.to_string(),
    }
}

/// Runs the full client-aggregated path for `query` over `rows`: plan →
/// compile → aggregate → vector aggs — exactly the engine's post-fetch
/// sequence.
fn run_client(
    query: &str,
    params: &QueryParams,
    rows: &[SampleRow],
    meta: &HashMap<u64, StreamMetaRow>,
) -> Result<QueryResult, ReadError> {
    let mp = metric_plan_of(query, params);
    let client = mp.client.as_ref().expect("client-aggregated plan");
    let compiled = CompiledPipeline::compile(&client.pipeline).expect("compile");
    let result = run_client_agg_rows(
        rows,
        &compiled,
        meta,
        client,
        ClientWindow {
            start_ns: mp.start_ns,
            end_ns: mp.end_ns,
            step_ns: mp.step_ns,
        },
        mp.rate_window_ns,
    )?;
    Ok(apply_vector_aggs(result, &mp.vector_aggs))
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

fn unwrap_rows() -> Vec<SampleRow> {
    vec![
        row(1, 10 * NS, "v=1"),
        row(1, 20 * NS, "v=2"),
        row(1, 70 * NS, "v=3"),
        row(1, 80 * NS, "v=4"),
    ]
}

#[test]
fn every_unwrap_reducer_matches_its_hand_derived_buckets() {
    let params = range_params(0, 2 * STEP);
    for (op, b0, b60) in [
        ("sum_over_time", 3.0, 7.0),
        ("avg_over_time", 1.5, 3.5),
        ("min_over_time", 1.0, 3.0),
        ("max_over_time", 2.0, 4.0),
        // Population stddev/stdvar (oracle-probed: /n, never /(n-1)).
        ("stddev_over_time", 0.5, 0.5),
        ("stdvar_over_time", 0.25, 0.25),
        // first/last are timestamp-anchored within the bucket.
        ("first_over_time", 1.0, 3.0),
        ("last_over_time", 2.0, 4.0),
    ] {
        let query = format!(r#"{op}({{env="prod"}} | logfmt | unwrap v [1m])"#);
        let points =
            single_series_points(run_client(&query, &params, &unwrap_rows(), &meta_one()).unwrap());
        assert_eq!(points, vec![(0, b0), (STEP, b60)], "{op}");
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
    assert_eq!(points, vec![(0, 3.0 / 60.0), (STEP, 7.0 / 60.0)]);
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
        assert_eq!(points, vec![(0, b0), (STEP, b60)], "{op} (shuffled input)");
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
    let reversed: Vec<SampleRow> = natural.iter().rev().cloned().collect();
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

/// Ordinary start/step/end boundaries: a row exactly ON a bucket edge
/// (`ts == k*step`) belongs to bucket `k*step` (the `intDiv` floor —
/// byte-identical to the SQL path's bucketing), including a row at
/// exactly `ts == end` when `end` is step-aligned.
#[test]
fn first_and_last_bucket_edge_rows_land_in_the_edge_bucket() {
    let rows = vec![
        row(1, 1, "v=1"),        // just past start -> bucket 0
        row(1, STEP, "v=2"),     // exactly on the step edge -> bucket STEP
        row(1, 2 * STEP, "v=3"), // exactly at end -> bucket 2*STEP
    ];
    let params = range_params(0, 2 * STEP);
    for (op, expected) in [
        (
            "first_over_time",
            vec![(0, 1.0), (STEP, 2.0), (2 * STEP, 3.0)],
        ),
        (
            "last_over_time",
            vec![(0, 1.0), (STEP, 2.0), (2 * STEP, 3.0)],
        ),
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
    // A huge step over the extreme window is a handful of buckets:
    // accepted (no false positive from the widened arithmetic).
    let params = QueryParams {
        spec: pulsus_read::logql::QuerySpec::Range {
            start_ns: i64::MIN,
            end_ns: i64::MAX,
            step_ns: (i64::MAX / 2) as u64,
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

/// Review round 3: the PER-ROW bucket assignment must survive a genuine
/// extreme-timestamp sample — `i64::MIN + 1` at the non-dividing step 3
/// floors to `i64::MIN - 1` in plain i64 (debug panic / release wrap);
/// widened i128 intermediates clamp that one sub-`i64::MIN` sliver to
/// `i64::MIN` deterministically, and a nearby sample whose bucket DOES
/// fit gets its exact floored start. Full path: plan →
/// `run_client_agg_rows` with surviving rows.
#[test]
fn extreme_timestamp_samples_bucket_without_overflow() {
    // Window (MIN, MIN + 30_000] at step 3 = 10_000 buckets — under the
    // cap, so real bucketing runs.
    let params = QueryParams {
        spec: pulsus_read::logql::QuerySpec::Range {
            start_ns: i64::MIN,
            end_ns: i64::MIN + 30_000,
            step_ns: 3,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    // `| env = "prod"` matches the base label: both rows SURVIVE and
    // must be bucketed (client mode, fingerprint grouping).
    let rows = vec![
        row(1, i64::MIN + 1, "a"), // floors to MIN-1 in i128 → clamps to i64::MIN
        row(1, i64::MIN + 7, "b"), // floors to MIN+5 — representable, exact
    ];
    let points = single_series_points(
        run_client(
            r#"count_over_time({env="prod"} | env = "prod" [3s])"#,
            &params,
            &rows,
            &meta_one(),
        )
        .unwrap(),
    );
    assert_eq!(points, vec![(i64::MIN, 1.0), (i64::MIN + 5, 1.0)]);

    // The absent grid near the extreme clamps IDENTICALLY (its first
    // bucket is the same `i64::MIN` id), so grid and data buckets stay
    // membership-consistent: with the MIN+1 row present, its bucket
    // emits no absence and every other grid bucket does.
    let params = QueryParams {
        spec: pulsus_read::logql::QuerySpec::Range {
            start_ns: i64::MIN,
            end_ns: i64::MIN + 300,
            step_ns: 3,
        },
        limit: 100,
        direction: Direction::Backward,
    };
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
    assert!(
        !items[0].points.iter().any(|(b, _)| *b == i64::MIN),
        "the populated clamped bucket must not report absence: {:?}",
        &items[0].points[..3.min(items[0].points.len())]
    );
    assert_eq!(
        items[0].points.first().map(|(b, _)| *b),
        Some(i64::MIN + 2),
        "absence starts at the first EMPTY grid bucket"
    );
    // Grid k_first..=k_first+100 (the clamped MIN bucket, then MIN+2,
    // MIN+5, ... MIN+299) = 101 buckets, one populated.
    assert_eq!(items[0].points.len(), 100);
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
    assert_eq!(
        buckets.into_iter().collect::<Vec<_>>(),
        vec![0, STEP],
        "one survivor per bucket"
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
    let ReadError::MetricPipelineError { error_type, .. } = &err else {
        panic!("expected MetricPipelineError, got {err:?}");
    };
    assert_eq!(error_type, "JSONParserErr");
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
    // Bucket 120s is genuinely empty; bucket 180s exists because the
    // window end (`ts <= end`, end % step == 0) lands in it under the
    // tumbling `intDiv` grid — the same bucket the SQL path would emit
    // for a row at exactly ts=end — and it too is empty here.
    assert_eq!(
        items[0].points,
        vec![(2 * STEP, 1.0), (3 * STEP, 1.0)],
        "absence for empty buckets only — a bucket with a line in ANY \
         stream emits nothing"
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
    let by_app = points_by_app(apply_vector_aggs(matrix_fixture(), &aggs));
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
    let by_app = points_by_app(apply_vector_aggs(matrix_fixture(), &aggs));
    assert_eq!(by_app["a"], vec![(0, 5.0)]);
    assert_eq!(by_app["b"], vec![(STEP, 3.0)]);
    assert!(!by_app.contains_key("c"), "{by_app:?}");
}

#[test]
fn bottomk_selects_the_lowest_per_step() {
    let aggs = vec![(pulsus_logql::VectorAggOp::Bottomk, None, Some(1.0))];
    let by_app = points_by_app(apply_vector_aggs(matrix_fixture(), &aggs));
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
        by_app(apply_vector_aggs(vector.clone(), &topk2)),
        vec!["b", "c"],
        "topk must not select NaN over finite values"
    );
    let bottomk2 = vec![(pulsus_logql::VectorAggOp::Bottomk, None, Some(2.0))];
    assert_eq!(
        by_app(apply_vector_aggs(vector.clone(), &bottomk2)),
        vec!["b", "c"],
        "bottomk must not select NaN over finite values"
    );
    // NaN is still selectable once every finite value is taken.
    let topk3 = vec![(pulsus_logql::VectorAggOp::Topk, None, Some(3.0))];
    assert_eq!(
        by_app(apply_vector_aggs(vector, &topk3)),
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
    let by_app = points_by_app(apply_vector_aggs(matrix, &topk1));
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
    let v = single_vector_value(apply_vector_aggs(vector.clone(), &aggs));
    assert_eq!(v, 1.118033988749895);
    let aggs = vec![(pulsus_logql::VectorAggOp::Stdvar, None, None)];
    let v = single_vector_value(apply_vector_aggs(vector, &aggs));
    assert_eq!(v, 1.25);
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
            MetricNode::Binary {
                op,
                return_bool,
                matching,
                lhs,
                rhs,
            } => combine_binary(*op, *return_bool, matching.as_ref(), eval(lhs)?, eval(rhs)?),
            MetricNode::VectorAgg { aggs, inner } => Ok(apply_vector_aggs(eval(inner)?, aggs)),
            MetricNode::Leaf(_) => panic!("scalar-only trees expected"),
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
