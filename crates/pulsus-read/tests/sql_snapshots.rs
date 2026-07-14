//! Pure SQL-generation snapshot tests (no database — `assert_eq!` on SQL
//! strings per the crate's lean-deps convention, no `insta`). Covers the
//! canonical shape matrix from the issue #11 review cycle: single/multi
//! equality, regex, negative, mixed matchers; every line-filter op; every
//! range aggregation; every vector aggregation with `by`/`without`;
//! direction/limit variants; and the `Instant`/`Range` `QuerySpec` shapes.

use pulsus_logql::parse;
use pulsus_read::logql::sql::{self, TimeWindow};
use pulsus_read::logql::{Direction, Plan, PlanCtx, QueryParams, QuerySpec, plan};

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
    }
}

// 2026-07-01T12:00:00Z .. 2026-07-01T18:00:00Z — a single-month window so
// stage 1's `month = '2026-07-01'` matches docs/schemas.md §3.2's canonical
// example byte-for-byte.
const START_NS: i64 = 1_782_907_200_000_000_000;
const END_NS: i64 = 1_782_928_800_000_000_000;
const STEP_NS: u64 = 60_000_000_000; // 60s

fn range_params(limit: u32, direction: Direction) -> QueryParams {
    QueryParams {
        spec: QuerySpec::Range {
            start_ns: START_NS,
            end_ns: END_NS,
            step_ns: STEP_NS,
        },
        limit,
        direction,
    }
}

fn streams_plan(query: &str, params: &QueryParams) -> pulsus_read::logql::StreamsPlan {
    let expr = parse(query).expect("parse");
    match plan(&expr, params, &ctx()).expect("plan") {
        Plan::Streams(sp) => sp,
        Plan::Metric(_) => panic!("expected a Streams plan"),
    }
}

fn metric_plan(query: &str, params: &QueryParams) -> pulsus_read::logql::MetricPlan {
    let expr = parse(query).expect("parse");
    match plan(&expr, params, &ctx()).expect("plan") {
        Plan::Metric(mp) => mp,
        Plan::Streams(_) => panic!("expected a Metric plan"),
    }
}

// ---------------------------------------------------------------------
// Stage 1 — matcher normalization shapes.
// ---------------------------------------------------------------------

#[test]
fn pure_positive_selector_is_byte_exact_to_schemas_md_3_2() {
    let sp = streams_plan(
        r#"{service_name="checkout", env="prod"}"#,
        &range_params(100, Direction::Backward),
    );
    assert_eq!(
        sp.stage1_sql,
        "SELECT fingerprint\n\
         FROM log_streams_idx\n\
         WHERE month = '2026-07-01'\n\
         \x20 AND ((key = 'service_name' AND val = 'checkout') OR (key = 'env' AND val = 'prod'))\n\
         GROUP BY fingerprint\n\
         HAVING uniqExact(key, val) = 2"
    );
}

#[test]
fn single_equality_matcher_uses_n_equal_one() {
    let sp = streams_plan(
        r#"{service_name="checkout"}"#,
        &range_params(100, Direction::Backward),
    );
    assert!(sp.stage1_sql.ends_with("HAVING uniqExact(key, val) = 1"));
}

#[test]
fn multi_equality_matchers_count_every_distinct_key() {
    let sp = streams_plan(
        r#"{service_name="checkout", env="prod", team="payments"}"#,
        &range_params(100, Direction::Backward),
    );
    assert!(sp.stage1_sql.ends_with("HAVING uniqExact(key, val) = 3"));
}

#[test]
fn regex_matcher_is_anchored_and_generates_one_selectivity_probe() {
    let sp = streams_plan(
        r#"{env=~"prod|staging"}"#,
        &range_params(100, Direction::Backward),
    );
    assert!(sp.stage1_sql.contains("match(val, '^(?:prod|staging)$')"));
    assert_eq!(sp.probes.len(), 1);
    assert_eq!(sp.probes[0].key, "env");
    assert!(sp.probes[0].sql.contains("SELECT count() AS n"));
    assert!(sp.probes[0].sql.contains("key = 'env'"));
}

#[test]
fn mixed_positive_and_negative_matchers_use_conditional_aggregation() {
    let sp = streams_plan(
        r#"{service_name="checkout", env="prod", team!="qa", app!~"test.*"}"#,
        &range_params(100, Direction::Backward),
    );
    assert_eq!(
        sp.stage1_sql,
        "SELECT fingerprint\n\
         FROM log_streams_idx\n\
         WHERE month = '2026-07-01'\n\
         \x20 AND ((key = 'service_name' AND val = 'checkout') OR (key = 'env' AND val = 'prod') \
         OR (key = 'team' AND val = 'qa') OR (key = 'app' AND match(val, '^(?:test.*)$')))\n\
         GROUP BY fingerprint\n\
         HAVING uniqExactIf((key, val), (key = 'service_name' AND val = 'checkout') OR \
         (key = 'env' AND val = 'prod')) = 2\n\
         \x20  AND countIf((key = 'team' AND val = 'qa') OR \
         (key = 'app' AND match(val, '^(?:test.*)$'))) = 0"
    );
}

#[test]
fn negative_only_selector_is_rejected() {
    let expr = parse(r#"{env!="prod"}"#).expect("parse");
    let err = plan(&expr, &range_params(100, Direction::Backward), &ctx()).unwrap_err();
    assert!(matches!(
        err,
        pulsus_read::logql::ReadError::EmptyMatcherSet
    ));
}

// ---------------------------------------------------------------------
// Stage 3 — line-filter pushdown, singleton/IN service predicate,
// direction/limit.
// ---------------------------------------------------------------------

#[test]
fn line_filter_contains_pushes_down_token_prefilter_and_exact_predicate() {
    let sp = streams_plan(
        r#"{service_name="checkout"} |= "connection refused""#,
        &range_params(100, Direction::Backward),
    );
    assert_eq!(
        sp.line_filters,
        vec![
            "hasToken(body, 'connection') AND hasToken(body, 'refused') AND position(body, 'connection refused') > 0"
                .to_string()
        ]
    );
}

#[test]
fn line_filter_not_contains_negates_the_whole_compound_predicate() {
    let sp = streams_plan(
        r#"{service_name="checkout"} != "connection refused""#,
        &range_params(100, Direction::Backward),
    );
    assert_eq!(
        sp.line_filters,
        vec![
            "NOT (hasToken(body, 'connection') AND hasToken(body, 'refused') AND position(body, 'connection refused') > 0)"
                .to_string()
        ]
    );
}

#[test]
fn line_filter_regex_uses_match_without_a_prefilter_when_not_a_plain_literal() {
    let sp = streams_plan(
        r#"{service_name="checkout"} |~ "err.*""#,
        &range_params(100, Direction::Backward),
    );
    assert_eq!(sp.line_filters, vec!["match(body, 'err.*')".to_string()]);
}

#[test]
fn line_filter_regex_extracts_a_token_prefilter_for_a_plain_literal_pattern() {
    let sp = streams_plan(
        r#"{service_name="checkout"} |~ "connection refused""#,
        &range_params(100, Direction::Backward),
    );
    assert_eq!(
        sp.line_filters,
        vec![
            "hasToken(body, 'connection') AND hasToken(body, 'refused') AND match(body, 'connection refused')"
                .to_string()
        ]
    );
}

#[test]
fn line_filter_not_regex_negates_the_whole_compound_predicate() {
    let sp = streams_plan(
        r#"{service_name="checkout"} !~ "err.*""#,
        &range_params(100, Direction::Backward),
    );
    assert_eq!(
        sp.line_filters,
        vec!["NOT (match(body, 'err.*'))".to_string()]
    );
}

#[test]
fn stage3_renders_the_canonical_shape_with_a_single_service() {
    let sp = streams_plan(
        r#"{service_name="checkout", env="prod"} |= "connection refused""#,
        &range_params(100, Direction::Backward),
    );
    let sql = sql::stage3(
        &sp.samples_table,
        &["'checkout'".to_string()],
        &[18374, 99120],
        TimeWindow {
            start_ns: sp.start_ns,
            end_ns: sp.end_ns,
        },
        &sp.line_filters,
        sp.direction,
        sp.limit,
    );
    assert_eq!(
        sql,
        "SELECT fingerprint, timestamp_ns, body\n\
         FROM log_samples\n\
         PREWHERE service = 'checkout'\n\
         WHERE fingerprint IN (18374, 99120)\n\
         \x20 AND timestamp_ns > 1782907200000000000 AND timestamp_ns <= 1782928800000000000\n\
         \x20 AND hasToken(body, 'connection') AND hasToken(body, 'refused') AND position(body, 'connection refused') > 0\n\
         ORDER BY timestamp_ns DESC\n\
         LIMIT 100"
    );
}

#[test]
fn stage3_uses_in_list_for_more_than_one_service() {
    let sql = sql::stage3(
        "log_samples",
        &["'checkout'".to_string(), "'billing'".to_string()],
        &[1, 2],
        TimeWindow {
            start_ns: START_NS,
            end_ns: END_NS,
        },
        &[],
        Direction::Backward,
        50,
    );
    assert!(sql.contains("PREWHERE service IN ('checkout', 'billing')"));
}

#[test]
fn direction_forward_orders_ascending() {
    let sql = sql::stage3(
        "log_samples",
        &["'checkout'".to_string()],
        &[1],
        TimeWindow {
            start_ns: START_NS,
            end_ns: END_NS,
        },
        &[],
        Direction::Forward,
        25,
    );
    assert!(sql.contains("ORDER BY timestamp_ns ASC"));
    assert!(sql.contains("LIMIT 25"));
}

#[test]
fn direction_backward_orders_descending() {
    let sql = sql::stage3(
        "log_samples",
        &["'checkout'".to_string()],
        &[1],
        TimeWindow {
            start_ns: START_NS,
            end_ns: END_NS,
        },
        &[],
        Direction::Backward,
        25,
    );
    assert!(sql.contains("ORDER BY timestamp_ns DESC"));
}

// ---------------------------------------------------------------------
// Stage 2 — hydration.
// ---------------------------------------------------------------------

#[test]
fn stage2_is_byte_exact_to_schemas_md_3_2() {
    assert_eq!(
        sql::stage2("log_streams", &[18374, 99120]),
        "SELECT fingerprint, service, labels FROM log_streams WHERE fingerprint IN (18374, 99120)"
    );
}

// ---------------------------------------------------------------------
// Metric queries — range aggregation ops, rollup routing, vector aggs.
// ---------------------------------------------------------------------

#[test]
fn rate_is_rollup_served_and_sums_count() {
    let mp = metric_plan(
        r#"rate({env="prod"}[5m])"#,
        &range_params(100, Direction::Backward),
    );
    assert!(mp.rollup);
    assert_eq!(mp.table, "log_metrics_5s");
    assert_eq!(mp.bucket_col, "bucket_ns");
    assert_eq!(mp.agg_expr, "sum(count)");
    assert_eq!(mp.rate_window_ns, Some(STEP_NS));
}

#[test]
fn count_over_time_is_rollup_served_with_no_rate_division() {
    let mp = metric_plan(
        r#"count_over_time({env="prod"}[5m])"#,
        &range_params(100, Direction::Backward),
    );
    assert!(mp.rollup);
    assert_eq!(mp.agg_expr, "sum(count)");
    assert_eq!(mp.rate_window_ns, None);
}

#[test]
fn bytes_rate_sums_bytes_and_divides_by_the_window() {
    let mp = metric_plan(
        r#"bytes_rate({env="prod"}[5m])"#,
        &range_params(100, Direction::Backward),
    );
    assert_eq!(mp.agg_expr, "sum(bytes)");
    assert_eq!(mp.rate_window_ns, Some(STEP_NS));
}

#[test]
fn bytes_over_time_sums_bytes_with_no_rate_division() {
    let mp = metric_plan(
        r#"bytes_over_time({env="prod"}[5m])"#,
        &range_params(100, Direction::Backward),
    );
    assert_eq!(mp.agg_expr, "sum(bytes)");
    assert_eq!(mp.rate_window_ns, None);
}

/// Renders `mp`'s metric SQL the way `LogQlEngine` would once fingerprints
/// and the hydrated service set are known — `services` empty for the
/// rollup path (no `service` column), non-empty for the raw fallback (fix
/// -plan amendment §3: the raw fallback must carry `PREWHERE service ...`
/// to keep `log_samples`'s primary-key prefix engaged).
fn metric_sql(
    mp: &pulsus_read::logql::MetricPlan,
    services: &[String],
    fingerprints: &[u64],
) -> String {
    let source = sql::MetricSource {
        table: &mp.table,
        bucket_col: mp.bucket_col,
        agg_expr: mp.agg_expr,
    };
    let window = TimeWindow {
        start_ns: mp.start_ns,
        end_ns: mp.end_ns,
    };
    match mp.step_ns {
        Some(step_ns) => sql::metric_range(
            source,
            services,
            fingerprints,
            window,
            step_ns,
            &mp.extra_predicates,
        ),
        None => sql::metric_instant(source, services, fingerprints, window, &mp.extra_predicates),
    }
}

#[test]
fn a_line_filter_forces_the_raw_fallback_even_for_count_over_time() {
    let mp = metric_plan(
        r#"count_over_time({env="prod"} |= "err" [5m])"#,
        &range_params(100, Direction::Backward),
    );
    assert!(!mp.rollup);
    assert_eq!(mp.table, "log_samples");
    assert_eq!(mp.bucket_col, "timestamp_ns");
    assert_eq!(mp.agg_expr, "count()");
    assert_eq!(mp.extra_predicates.len(), 1);

    let sql = metric_sql(&mp, &["'checkout'".to_string()], &[101, 205]);
    assert!(
        sql.contains("PREWHERE service = 'checkout'\n"),
        "raw metric fallback must carry PREWHERE service, got:\n{sql}"
    );
}

#[test]
fn bytes_raw_fallback_sums_the_body_length() {
    let mp = metric_plan(
        r#"bytes_over_time({env="prod"} |= "err" [5m])"#,
        &range_params(100, Direction::Backward),
    );
    assert!(!mp.rollup);
    assert_eq!(mp.agg_expr, "sum(length(body))");

    let sql = metric_sql(&mp, &["'checkout'".to_string()], &[101, 205]);
    assert!(
        sql.contains("PREWHERE service = 'checkout'\n"),
        "raw metric fallback must carry PREWHERE service, got:\n{sql}"
    );
}

#[test]
fn a_step_not_dividing_the_rollup_resolution_forces_the_raw_fallback() {
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: START_NS,
            end_ns: END_NS,
            step_ns: 3_000_000_000, // 3s: not a multiple of the 5s rollup resolution
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let mp = metric_plan(r#"rate({env="prod"}[5m])"#, &params);
    assert!(!mp.rollup);
    assert_eq!(mp.table, "log_samples");

    let sql = metric_sql(
        &mp,
        &["'checkout'".to_string(), "'billing'".to_string()],
        &[101, 205],
    );
    assert!(
        sql.contains("PREWHERE service IN ('checkout', 'billing')\n"),
        "raw metric fallback must carry PREWHERE service IN (...) for multiple services, got:\n{sql}"
    );
}

#[test]
fn rollup_served_metric_sql_never_carries_a_service_prewhere() {
    let mp = metric_plan(
        r#"rate({env="prod"}[5m])"#,
        &range_params(100, Direction::Backward),
    );
    assert!(mp.rollup);
    // Even if a caller mistakenly passed a non-empty service set, exercise
    // the documented rollup-path contract: `LogQlEngine` always passes
    // `&[]` for the rollup path since `log_metrics_5s` has no `service`
    // column to filter on.
    let sql = metric_sql(&mp, &[], &[101, 205]);
    assert!(!sql.contains("PREWHERE"));
}

fn rollup_source() -> sql::MetricSource<'static> {
    sql::MetricSource {
        table: "log_metrics_5s",
        bucket_col: "bucket_ns",
        agg_expr: "sum(count)",
    }
}

#[test]
fn metric_range_sql_uses_intdiv_bucketing() {
    let sql = sql::metric_range(
        rollup_source(),
        &[],
        &[101, 205, 990],
        TimeWindow {
            start_ns: START_NS,
            end_ns: END_NS,
        },
        STEP_NS,
        &[],
    );
    assert_eq!(
        sql,
        "SELECT fingerprint, intDiv(bucket_ns, 60000000000) * 60000000000 AS step, sum(count) AS n\n\
         FROM log_metrics_5s\n\
         WHERE fingerprint IN (101, 205, 990) AND bucket_ns > 1782907200000000000 AND bucket_ns <= 1782928800000000000\n\
         GROUP BY fingerprint, step"
    );
}

#[test]
fn metric_instant_sql_has_no_intdiv_bucket_expression() {
    let sql = sql::metric_instant(
        rollup_source(),
        &[],
        &[101, 205],
        TimeWindow {
            start_ns: START_NS,
            end_ns: END_NS,
        },
        &[],
    );
    assert_eq!(
        sql,
        "SELECT fingerprint, sum(count) AS n\n\
         FROM log_metrics_5s\n\
         WHERE fingerprint IN (101, 205) AND bucket_ns > 1782907200000000000 AND bucket_ns <= 1782928800000000000\n\
         GROUP BY fingerprint"
    );
    assert!(!sql.contains("intDiv"));
    assert!(!sql.contains(" step"));
}

// ---------------------------------------------------------------------
// Instant vs Range QuerySpec shapes (task-manager resolution #3).
// ---------------------------------------------------------------------

#[test]
fn instant_metric_spec_has_no_step_and_a_range_derived_window() {
    let params = QueryParams {
        spec: QuerySpec::Instant { at_ns: END_NS },
        limit: 100,
        direction: Direction::Backward,
    };
    let mp = metric_plan(r#"rate({env="prod"}[5m])"#, &params);
    assert_eq!(mp.step_ns, None);
    assert_eq!(mp.end_ns, END_NS);
    // 5m range window: start = at - 5m.
    assert_eq!(mp.start_ns, END_NS - 300_000_000_000);
    assert_eq!(mp.rate_window_ns, Some(300_000_000_000));
}

#[test]
fn range_metric_spec_carries_the_caller_supplied_step() {
    let mp = metric_plan(
        r#"rate({env="prod"}[5m])"#,
        &range_params(100, Direction::Backward),
    );
    assert_eq!(mp.step_ns, Some(STEP_NS));
    assert_eq!(mp.start_ns, START_NS);
    assert_eq!(mp.end_ns, END_NS);
}

#[test]
fn vector_agg_sum_by_captures_the_grouping_labels() {
    let mp = metric_plan(
        r#"sum by (service_name) (rate({env="prod"}[5m]))"#,
        &range_params(100, Direction::Backward),
    );
    assert_eq!(mp.vector_aggs.len(), 1);
    let (op, grouping) = &mp.vector_aggs[0];
    assert_eq!(*op, pulsus_logql::VectorAggOp::Sum);
    let grouping = grouping.as_ref().expect("by grouping");
    assert_eq!(grouping.kind, pulsus_logql::GroupingKind::By);
    assert_eq!(grouping.labels, vec!["service_name".to_string()]);
}

#[test]
fn vector_agg_without_captures_the_excluded_labels() {
    let mp = metric_plan(
        r#"avg without (service_name) (count_over_time({env="prod"}[5m]))"#,
        &range_params(100, Direction::Backward),
    );
    let (op, grouping) = &mp.vector_aggs[0];
    assert_eq!(*op, pulsus_logql::VectorAggOp::Avg);
    assert_eq!(
        grouping.as_ref().unwrap().kind,
        pulsus_logql::GroupingKind::Without
    );
}

#[test]
fn every_vector_agg_op_is_captured_in_the_plan() {
    for (src_op, expected) in [
        ("sum", pulsus_logql::VectorAggOp::Sum),
        ("avg", pulsus_logql::VectorAggOp::Avg),
        ("min", pulsus_logql::VectorAggOp::Min),
        ("max", pulsus_logql::VectorAggOp::Max),
        ("count", pulsus_logql::VectorAggOp::Count),
    ] {
        let query = format!(r#"{src_op} by (service_name) (rate({{env="prod"}}[5m]))"#);
        let mp = metric_plan(&query, &range_params(100, Direction::Backward));
        assert_eq!(mp.vector_aggs[0].0, expected, "op {src_op}");
    }
}

// ---------------------------------------------------------------------
// Multi-month partition pruning.
// ---------------------------------------------------------------------

#[test]
fn a_range_spanning_a_month_boundary_resolves_both_partitions() {
    // 2026-07-31T23:00Z .. 2026-08-01T01:00Z.
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: 1_785_538_800_000_000_000,
            end_ns: 1_785_546_000_000_000_000,
            step_ns: STEP_NS,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let sp = streams_plan(r#"{service_name="checkout"}"#, &params);
    assert!(
        sp.stage1_sql
            .contains("month IN ('2026-07-01', '2026-08-01')")
    );
}
