//! Pure SQL-generation snapshot tests (no database — `assert_eq!` on SQL
//! strings per the crate's lean-deps convention, no `insta`). Covers the
//! canonical shape matrix from the issue #11 review cycle: single/multi
//! equality, regex, negative, mixed matchers; every line-filter op; every
//! range aggregation; every vector aggregation with `by`/`without`;
//! direction/limit variants; and the `Instant`/`Range` `QuerySpec` shapes.

use pulsus_logql::parse;
use pulsus_model::{Fingerprint, FpLiteral};
use pulsus_read::logql::predicate::{CheckedFragment, literal, month_literal};
use pulsus_read::logql::sql::{self, MetricShape, ScanLowerBound, ScanProjection, TimeWindow};
use pulsus_read::logql::{Direction, Plan, PlanCtx, QueryParams, QuerySpec, plan};

/// Issue #286: `StreamsPlan::line_filters` / `MetricPlan::extra_predicates`
/// are `Vec<CheckedFragment>`, so the byte-exact expectations below compare
/// against the rendered text. Accessor only — no expected literal moved.
fn sqls(fs: &[CheckedFragment]) -> Vec<&str> {
    fs.iter().map(CheckedFragment::as_sql).collect()
}

fn ctx() -> PlanCtx<'static> {
    PlanCtx {
        tenant: Box::leak(Box::new(
            pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        )),
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
        Plan::Metric(_) | Plan::MetricBinary(_) => panic!("expected a Streams plan"),
    }
}

fn metric_plan(query: &str, params: &QueryParams) -> pulsus_read::logql::MetricPlan {
    let expr = parse(query).expect("parse");
    match plan(&expr, params, &ctx()).expect("plan") {
        Plan::Metric(mp) => mp,
        Plan::Streams(_) | Plan::MetricBinary(_) => panic!("expected a Metric plan"),
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
         WHERE org_id = '' AND month = '2026-07-01'\n\
         \x20 AND ((key = 'service_name' AND val = 'checkout') OR (key = 'env' AND val = 'prod'))\n\
         GROUP BY fingerprint\n\
         HAVING uniqExact(key, val) = 2"
    );
}

/// **Issue #392 AC 13 — a non-ASCII identifier plans IDENTICALLY to an
/// ASCII one.** Stated as an identity rather than a wall-time claim
/// (Tier 1): the SQL for the non-ASCII spelling is the SQL for the ASCII
/// spelling with `'ex'` rewritten to `'éx'`, byte for byte.
///
/// That is the whole performance argument, made checkable. A label name
/// is always a ClickHouse STRING LITERAL, never an SQL identifier
/// (`crates/pulsus-read/src/logql/escape.rs`), so the same
/// `log_streams_idx` `(month, key, val)` access path serves it, the same
/// `HAVING uniqExact(key, val) = N` prunes it, the same `PREWHERE
/// service` engages the PK prefix, and no coordinator-side work is
/// added. If a future change ever routed a non-ASCII name differently —
/// a client-side filter, an extra probe, a different table — this stops
/// being an identity and reddens.
///
/// Fails on `ff0fb09`: neither non-ASCII query parses there.
#[test]
fn a_non_ascii_label_name_plans_identically_to_an_ascii_one() {
    // (a) A stream-selector matcher — the position whose key literal
    // actually reaches stage 1. This is also the position the reference
    // 400s for a round-trip defect of its own; we serve it, and this is
    // what serving it costs at the plan: nothing.
    let ascii = streams_plan(
        r#"{ex="m", app="x"}"#,
        &range_params(100, Direction::Backward),
    );
    let non_ascii = streams_plan(
        r#"{éx="m", app="x"}"#,
        &range_params(100, Direction::Backward),
    );
    assert!(
        ascii.stage1_sql.contains("key = 'ex'"),
        "non-vacuity: the ASCII twin must really carry the key literal, else the \
         substitution below proves nothing:\n{}",
        ascii.stage1_sql
    );
    assert_eq!(
        ascii.stage1_sql.replace("'ex'", "'éx'"),
        non_ascii.stage1_sql,
        "the non-ASCII selector must plan to the same stage-1 SQL, modulo the key literal"
    );
    assert_eq!(ascii.samples_table, non_ascii.samples_table);
    assert_eq!(ascii.scan_limit, non_ascii.scan_limit);

    // (b) A grouping label. It never reaches SQL — grouping is applied
    // above the scan — so the two plans' SQL is byte-identical with no
    // substitution at all, and the grouping list is the ONLY difference.
    let ascii = metric_plan(
        r#"sum by (ex) (count_over_time({app="x"}[5m]))"#,
        &range_params(100, Direction::Backward),
    );
    let non_ascii = metric_plan(
        r#"sum by (éx) (count_over_time({app="x"}[5m]))"#,
        &range_params(100, Direction::Backward),
    );
    let fps = [18374, 99120].map(|v| Fingerprint::from_raw(v).sql_literal());
    let services = [literal("checkout")];
    assert_eq!(
        sliding_sql(&ascii, &services, &fps),
        sliding_sql(&non_ascii, &services, &fps),
        "a non-ASCII grouping label must not change the scan SQL at all"
    );
    assert_eq!(ascii.table, non_ascii.table);
    assert_eq!(ascii.rollup, non_ascii.rollup);
    assert_eq!(ascii.routing.reason, non_ascii.routing.reason);
    assert_eq!(
        sqls(&ascii.extra_predicates),
        sqls(&non_ascii.extra_predicates)
    );
    let grouping = |mp: &pulsus_read::logql::MetricPlan| {
        mp.vector_aggs[0]
            .1
            .as_ref()
            .expect("by grouping")
            .labels
            .clone()
    };
    assert_eq!(grouping(&ascii), vec!["ex"]);
    assert_eq!(grouping(&non_ascii), vec!["éx"]);
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
         WHERE org_id = '' AND month = '2026-07-01'\n\
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
fn line_filter_contains_pushes_down_an_escaped_like_predicate() {
    let sp = streams_plan(
        r#"{service_name="checkout"} |= "connection refused""#,
        &range_params(100, Direction::Backward),
    );
    assert_eq!(
        sqls(&sp.line_filters),
        vec!["body LIKE '%connection refused%'"]
    );
}

#[test]
fn line_filter_not_contains_negates_the_whole_body_predicate() {
    let sp = streams_plan(
        r#"{service_name="checkout"} != "connection refused""#,
        &range_params(100, Direction::Backward),
    );
    assert_eq!(
        sqls(&sp.line_filters),
        vec!["NOT (body LIKE '%connection refused%')"]
    );
}

#[test]
fn line_filter_regex_uses_a_bare_unanchored_match() {
    let sp = streams_plan(
        r#"{service_name="checkout"} |~ "err.*""#,
        &range_params(100, Direction::Backward),
    );
    assert_eq!(sqls(&sp.line_filters), vec!["match(body, 'err.*')"]);
}

#[test]
fn line_filter_regex_over_a_plain_literal_is_the_same_bare_match() {
    let sp = streams_plan(
        r#"{service_name="checkout"} |~ "connection refused""#,
        &range_params(100, Direction::Backward),
    );
    assert_eq!(
        sqls(&sp.line_filters),
        vec!["match(body, 'connection refused')"]
    );
}

#[test]
fn line_filter_not_regex_negates_the_whole_compound_predicate() {
    let sp = streams_plan(
        r#"{service_name="checkout"} !~ "err.*""#,
        &range_params(100, Direction::Backward),
    );
    assert_eq!(sqls(&sp.line_filters), vec!["NOT (match(body, 'err.*'))"]);
}

#[test]
fn a_word_line_filter_pushes_down_beside_a_literal_filter() {
    // Issue #757: `word(…)` renders `hasToken` over the body's case key,
    // the expression `idx_body_words` indexes.
    let sp = streams_plan(
        r#"{service_name="checkout"} |= word("Connection") |= "x""#,
        &range_params(100, Direction::Backward),
    );
    assert_eq!(
        sqls(&sp.line_filters),
        vec![
            "hasToken(upperUTF8(lowerUTF8(body)), upperUTF8(lowerUTF8('Connection')))",
            "body LIKE '%x%'"
        ]
    );
}

#[test]
fn or_line_filter_of_literals_pushes_down_as_a_parenthesized_disjunction() {
    // M8-LQ2 `linefilter.or`: each disjunct is the same `body LIKE`
    // predicate.
    let sp = streams_plan(
        r#"{service_name="checkout"} |= "foo" or "bar""#,
        &range_params(100, Direction::Backward),
    );
    assert_eq!(
        sqls(&sp.line_filters),
        vec!["((body LIKE '%foo%') OR (body LIKE '%bar%'))"]
    );
}

#[test]
fn or_line_filter_negative_op_wraps_the_disjunction_in_not() {
    let sp = streams_plan(
        r#"{service_name="checkout"} != "foo" or "bar""#,
        &range_params(100, Direction::Backward),
    );
    assert_eq!(
        sqls(&sp.line_filters),
        vec!["NOT ((body LIKE '%foo%') OR (body LIKE '%bar%'))"]
    );
}

#[test]
fn ip_line_filter_is_not_pushed_down_while_a_sibling_literal_filter_is() {
    // An `ip()` line filter renders no `body` predicate, so it is served
    // client-side and must NOT appear in `sp.line_filters`; a sibling literal
    // filter still pushes down (and still prunes granules).
    let sp = streams_plan(
        r#"{service_name="checkout"} |= "boot" |= ip("10.0.0.0/8")"#,
        &range_params(100, Direction::Backward),
    );
    assert_eq!(
        sqls(&sp.line_filters),
        vec!["body LIKE '%boot%'"],
        "only the literal filter pushes down; the ip() filter is client-side"
    );
    assert!(
        !sp.line_filters.iter().any(|f| {
            f.as_sql().contains("10.0.0.0") || f.as_sql().to_ascii_lowercase().contains("ip")
        }),
        "no ip() predicate may leak into the pushed-down SQL: {:?}",
        sp.line_filters
    );
}

#[test]
fn stage3_renders_the_canonical_shape_with_a_single_service() {
    let sp = streams_plan(
        r#"{service_name="checkout", env="prod"} |= "connection refused""#,
        &range_params(100, Direction::Backward),
    );
    let sql = sql::stage3(
        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        &sp.samples_table,
        &[literal("checkout")],
        &[18374, 99120].map(|v| Fingerprint::from_raw(v).sql_literal()),
        TimeWindow {
            start_ns: sp.start_ns,
            end_ns: sp.end_ns,
        },
        &sp.line_filters,
        sp.direction,
        sp.scan_limit,
    );
    assert_eq!(
        sql,
        "SELECT fingerprint, timestamp_ns, body, structured_metadata\n\
         FROM log_samples\n\
         PREWHERE service = 'checkout'\n\
         WHERE org_id = '' AND fingerprint IN ('18374', '99120')\n\
         \x20 AND timestamp_ns > 1782907200000000000 AND timestamp_ns <= 1782928800000000000\n\
         \x20 AND body LIKE '%connection refused%'\n\
         ORDER BY timestamp_ns DESC, fingerprint DESC, cityHash64(body) DESC, body DESC\n\
         LIMIT 100"
    );
}

/// **Issue #406: the stage-3 `ORDER BY` is a TOTAL order, and this is the
/// byte pin on it.** `ORDER BY timestamp_ns` alone left entries sharing a
/// timestamp in whatever order the parts were read in, which ClickHouse
/// does not fix across runs — measured 2026-08-10 over 81 active parts
/// holding 200 rows at one identical `timestamp_ns`, fifteen identical
/// queries returned fifteen different orderings, and at a `LIMIT` cutting
/// through the tie group, fifteen different result SETS. The reference is
/// stable over the same corpus shape.
///
/// The key list is `stage3_keyset`'s, exactly, so the fast path and the
/// fetch-until-limit path break a tie the same way; the assertion below
/// reads that list off `stage3_keyset` rather than restating it, so the two
/// cannot drift apart silently.
#[test]
fn stage3_breaks_timestamp_ties_with_the_same_total_order_as_the_keyset_builder() {
    for (direction, ord) in [(Direction::Backward, "DESC"), (Direction::Forward, "ASC")] {
        let window = TimeWindow {
            start_ns: START_NS,
            end_ns: END_NS,
        };
        let fast = sql::stage3(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            "log_samples",
            &[literal("checkout")],
            &[Fingerprint::from_raw(1).sql_literal()],
            window,
            &[],
            direction,
            25,
        );
        let paged = sql::stage3_keyset(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            "log_samples",
            &[literal("checkout")],
            &[Fingerprint::from_raw(1).sql_literal()],
            window,
            sql::KeysetLower::First,
            direction,
            &[],
            25,
        );
        // The paged builder projects `cityHash64(body) AS body_hash` and
        // sorts on the alias; the fast path has no such projection and
        // sorts on the expression. Same key list either way.
        let want = format!(
            "ORDER BY timestamp_ns {ord}, fingerprint {ord}, cityHash64(body) {ord}, body {ord}"
        );
        assert!(fast.contains(&want), "stage3 {direction:?}:\n{fast}");
        let want_paged =
            format!("ORDER BY timestamp_ns {ord}, fingerprint {ord}, body_hash {ord}, body {ord}");
        assert!(
            paged.contains(&want_paged),
            "stage3_keyset {direction:?} — if this moved, stage3 must move with it:\n{paged}"
        );
    }
}

#[test]
fn stage3_uses_in_list_for_more_than_one_service() {
    let sql = sql::stage3(
        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        "log_samples",
        &[literal("checkout"), literal("billing")],
        &[
            Fingerprint::from_raw(1).sql_literal(),
            Fingerprint::from_raw(2).sql_literal(),
        ],
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
        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        "log_samples",
        &[literal("checkout")],
        &[Fingerprint::from_raw(1).sql_literal()],
        TimeWindow {
            start_ns: START_NS,
            end_ns: END_NS,
        },
        &[],
        Direction::Forward,
        25,
    );
    assert!(sql.contains("ORDER BY timestamp_ns ASC, fingerprint ASC"));
    assert!(sql.contains("LIMIT 25"));
}

#[test]
fn direction_backward_orders_descending() {
    let sql = sql::stage3(
        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        "log_samples",
        &[literal("checkout")],
        &[Fingerprint::from_raw(1).sql_literal()],
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
// Issue M6-09 (AC3): pushdown byte-identity around parser stages. A line
// filter BEFORE a parser keeps the exact `body LIKE` pushdown
// (the parser is pure post-fetch — the named perf gate); a line filter
// AFTER `line_format` references the rewritten line and must be ABSENT
// from the stage-3 SQL.
// ---------------------------------------------------------------------

#[test]
fn a_line_filter_before_a_parser_still_pushes_down_byte_identically() {
    let plain = streams_plan(
        r#"{service_name="checkout"} |= "connection refused""#,
        &range_params(100, Direction::Backward),
    );
    let with_parser = streams_plan(
        r#"{service_name="checkout"} |= "connection refused" | json | status = "500""#,
        &range_params(100, Direction::Backward),
    );
    assert_eq!(
        sqls(&with_parser.line_filters),
        sqls(&plain.line_filters),
        "the parser/label-filter stages must not disturb the pushdown predicate"
    );
    assert_eq!(
        sqls(&with_parser.line_filters),
        vec!["body LIKE '%connection refused%'"]
    );
    // The label filter is an unpushed dropping stage: the scan oversamples
    // while the response cap stays put (AC9's plan-side half).
    assert_eq!(with_parser.result_limit, 100);
    assert_eq!(with_parser.scan_limit, 1_000);

    let sql = sql::stage3(
        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        &with_parser.samples_table,
        &[literal("checkout")],
        &[Fingerprint::from_raw(18374).sql_literal()],
        TimeWindow {
            start_ns: with_parser.start_ns,
            end_ns: with_parser.end_ns,
        },
        &with_parser.line_filters,
        with_parser.direction,
        with_parser.scan_limit,
    );
    assert!(
        sql.contains("AND body LIKE '%connection refused%'"),
        "stage-3 SQL must render the pushdown predicate: {sql}"
    );
    assert!(sql.ends_with("LIMIT 1000"), "{sql}");
}

#[test]
fn a_line_filter_after_line_format_is_absent_from_stage3_sql() {
    let sp = streams_plan(
        r#"{service_name="checkout"} | line_format "{{.x}}" |= "err""#,
        &range_params(100, Direction::Backward),
    );
    assert!(
        sp.line_filters.is_empty(),
        "a post-line_format filter references the rewritten line and cannot push down"
    );
    let sql = sql::stage3(
        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        &sp.samples_table,
        &[literal("checkout")],
        &[Fingerprint::from_raw(18374).sql_literal()],
        TimeWindow {
            start_ns: sp.start_ns,
            end_ns: sp.end_ns,
        },
        &sp.line_filters,
        sp.direction,
        sp.scan_limit,
    );
    // No body PREDICATE may appear in stage-3 SQL (issue #97: `body,` now
    // legitimately appears in the SELECT projection alongside
    // `structured_metadata`, so match the predicate forms directly instead).
    assert!(
        !sql.contains("body LIKE") && !sql.contains("match(body"),
        "no body predicate may appear in stage-3 SQL: {sql}"
    );
    // The unpushed dropping filter triggers the oversample.
    assert!(sql.ends_with("LIMIT 1000"), "{sql}");
}

// ---------------------------------------------------------------------
// Issue #200 (M8-LQ1): pushdown identity around the new stages. A leading
// line filter before `decolorize`/`drop`/`keep`/`unpack`/`logfmt --strict`
// keeps its byte-identical pushdown (they are pure post-fetch transforms
// that add no SQL predicate); a line filter AFTER `decolorize`/`unpack`
// references the rewritten line and must be ABSENT from stage-3 SQL, while
// one after `drop`/`keep` (which do NOT rewrite the line) still pushes down.
// ---------------------------------------------------------------------

#[test]
fn a_leading_line_filter_before_the_new_stages_pushes_down_byte_identically() {
    let plain = streams_plan(
        r#"{service_name="checkout"} |= "connection refused""#,
        &range_params(100, Direction::Backward),
    );
    for query in [
        r#"{service_name="checkout"} |= "connection refused" | decolorize"#,
        r#"{service_name="checkout"} |= "connection refused" | drop level"#,
        r#"{service_name="checkout"} |= "connection refused" | keep app"#,
        r#"{service_name="checkout"} |= "connection refused" | unpack"#,
        r#"{service_name="checkout"} |= "connection refused" | logfmt --strict"#,
    ] {
        let sp = streams_plan(query, &range_params(100, Direction::Backward));
        assert_eq!(
            sqls(&sp.line_filters),
            sqls(&plain.line_filters),
            "the new stage must not disturb the leading pushdown predicate: {query}"
        );
    }
}

#[test]
fn a_line_filter_after_decolorize_or_unpack_is_absent_from_pushdown() {
    // `decolorize`/`unpack` rewrite the line, so a following line filter
    // evaluates in-engine and must not appear in `line_filters`.
    for query in [
        r#"{service_name="checkout"} | decolorize |= "err""#,
        r#"{service_name="checkout"} | unpack |= "err""#,
    ] {
        let sp = streams_plan(query, &range_params(100, Direction::Backward));
        assert!(
            sp.line_filters.is_empty(),
            "a post-rewrite line filter must not push down: {query} => {:?}",
            sp.line_filters
        );
    }
}

#[test]
fn a_line_filter_after_drop_or_keep_still_pushes_down() {
    // `drop`/`keep` mutate labels but do NOT rewrite the line, so a
    // following line filter still references the original body and pushes
    // down byte-identically.
    for query in [
        r#"{service_name="checkout"} | drop level |= "err""#,
        r#"{service_name="checkout"} | keep app |= "err""#,
    ] {
        let sp = streams_plan(query, &range_params(100, Direction::Backward));
        assert_eq!(
            sqls(&sp.line_filters),
            vec!["body LIKE '%err%'"],
            "a line filter after drop/keep must still push down: {query}"
        );
    }
}

// ---------------------------------------------------------------------
// Stage 2 — hydration.
// ---------------------------------------------------------------------

#[test]
fn stage2_is_byte_exact_to_schemas_md_3_2() {
    assert_eq!(
        sql::stage2(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            "log_streams",
            &[18374, 99120].map(|v| Fingerprint::from_raw(v).sql_literal())
        ),
        "SELECT fingerprint, service, labels FROM log_streams WHERE org_id = '' AND fingerprint IN ('18374', '99120')"
    );
}

// ---------------------------------------------------------------------
// Metric queries — range aggregation ops, rollup routing, vector aggs.
// ---------------------------------------------------------------------

/// Review-requested test gap (architect plan review, issue #12): a
/// zero-step `Range` query must be rejected at the planner, before either
/// the rollup or raw SQL builder is ever reached. `0.is_multiple_of(_)` is
/// trivially `true`, so without this guard the routing decision would pick
/// rollup and render `intDiv(bucket_ns, 0)` — undefined in ClickHouse.
#[test]
fn a_zero_step_range_query_is_rejected_before_any_sql_is_generated() {
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: START_NS,
            end_ns: END_NS,
            step_ns: 0,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let expr = parse(r#"rate({env="prod"}[5m])"#).expect("parse");
    let err = plan(&expr, &params, &ctx()).unwrap_err();
    assert!(matches!(err, pulsus_read::logql::ReadError::InvalidStep));
}

/// The reason [`RoutingDecision`] names for a clean counting range query
/// at `STEP_NS` (60s) over a `[5m]` range — shared by every case below
/// (rate/count_over_time/bytes_rate/bytes_over_time differ only in
/// `agg_expr`/`rate_window_ns`). Since issue #624 such a query is counted in
/// the database by the sliding statement.
fn expected_sliding_reason() -> String {
    "raw: sliding range aggregation in the database (issue #624)".to_string()
}

/// The `[5m]` selector range in nanoseconds — issue #227 makes it the
/// `rate`/`bytes_rate` divisor AND the sliding window width (never `step`).
const RANGE_NS: i64 = 300_000_000_000;

/// Issue #624: a clean counting range query is counted in the database at
/// any range and step — the sliding statement when the range is not the
/// step, #507's statement when it is.
#[test]
fn rate_range_is_counted_in_the_database_and_divides_by_the_range() {
    let mp = metric_plan(
        r#"rate({env="prod"}[5m])"#,
        &range_params(100, Direction::Backward),
    );
    assert!(!mp.rollup);
    assert!(mp.client.is_none());
    assert_eq!(mp.table, "log_samples");
    // `rate` divides by the `[range]` (5m), NOT `step` — the
    // `rate([1m]) ≠ rate([10m])` fix.
    assert_eq!(mp.rate_window_ns, Some(RANGE_NS as u64));
    assert_eq!(mp.routing.chosen, pulsus_read::logql::RouteChoice::Raw);
    assert_eq!(
        mp.routing.reason,
        "raw: sliding range aggregation in the database (issue #624)"
    );

    let at_range = QueryParams {
        spec: QuerySpec::Range {
            start_ns: START_NS,
            end_ns: END_NS,
            step_ns: RANGE_NS as u64,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let mp = metric_plan(r#"rate({env="prod"}[5m])"#, &at_range);
    assert!(mp.client.is_none());
    assert_eq!(mp.rate_window_ns, Some(RANGE_NS as u64));
    assert_eq!(
        mp.routing.reason,
        "raw: bucketed range aggregation (issue #507)"
    );
}

#[test]
fn count_over_time_range_slides_raw_with_no_rate_division() {
    let mp = metric_plan(
        r#"count_over_time({env="prod"}[5m])"#,
        &range_params(100, Direction::Backward),
    );
    assert!(!mp.rollup);
    assert!(mp.client.is_none());
    assert_eq!(mp.rate_window_ns, None);
    assert_eq!(mp.routing.reason, expected_sliding_reason());
}

#[test]
fn bytes_rate_range_slides_raw_and_divides_by_the_range() {
    let mp = metric_plan(
        r#"bytes_rate({env="prod"}[5m])"#,
        &range_params(100, Direction::Backward),
    );
    assert!(!mp.rollup);
    assert!(mp.client.is_none());
    assert_eq!(mp.rate_window_ns, Some(RANGE_NS as u64));
    assert_eq!(mp.routing.reason, expected_sliding_reason());
}

#[test]
fn bytes_over_time_range_slides_raw_with_no_rate_division() {
    let mp = metric_plan(
        r#"bytes_over_time({env="prod"}[5m])"#,
        &range_params(100, Direction::Backward),
    );
    assert!(!mp.rollup);
    assert!(mp.client.is_none());
    assert_eq!(mp.rate_window_ns, None);
    assert_eq!(mp.routing.reason, expected_sliding_reason());
}

/// The [`ScanProjection`] `LogQlEngine` would pass for this plan — derived
/// from `mp.op` exactly as `client_metric_read_sql` derives it (issue #249),
/// so a snapshot cannot pin SQL the engine would never issue.
fn projection_of(mp: &pulsus_read::logql::MetricPlan) -> ScanProjection {
    if matches!(mp.op, pulsus_logql::RangeAggOp::AbsentOverTime) {
        ScanProjection::Lean
    } else {
        ScanProjection::WithStructuredMetadata
    }
}

/// Renders the sliding raw scan (`metric_raw_samples_sliding`) the way
/// `LogQlEngine` would for a range query, so the `PREWHERE service` contract
/// (PK-prefix engagement) stays pinned.
fn sliding_sql(
    mp: &pulsus_read::logql::MetricPlan,
    services: &[pulsus_read::logql::predicate::CheckedLiteral],
    fingerprints: &[FpLiteral],
) -> String {
    sql::metric_raw_samples_sliding(
        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        &mp.table,
        services,
        fingerprints,
        TimeWindow {
            start_ns: mp.start_ns,
            end_ns: mp.end_ns,
        },
        mp.scan_lower,
        &mp.extra_predicates,
        projection_of(mp),
    )
}

/// Renders the sliding statement (`metric_range_sliding`) the way
/// `LogQlEngine` issues it for a clean counting range query whose range is
/// not its step (issue #624), so the `PREWHERE service` contract is pinned
/// on the statement that runs.
fn lowered_sql(
    mp: &pulsus_read::logql::MetricPlan,
    services: &[pulsus_read::logql::predicate::CheckedLiteral],
    fingerprints: &[FpLiteral],
) -> String {
    sql::metric_range_sliding(
        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        sql::MetricSource::new(&mp.table, mp.source_shape().expect("a counting plan")),
        services,
        fingerprints,
        sql::SlidingScan {
            window: TimeWindow {
                start_ns: mp.start_ns,
                end_ns: mp.end_ns,
            },
            lower: mp.scan_lower,
            grid_start_ns: mp.grid_start_ns,
            step_ns: mp.step_ns.expect("a range plan").get(),
            range_ns: mp.range_ns.get(),
        },
        &mp.extra_predicates,
    )
    .expect("a renderable sliding statement")
}

/// Issue #249 — **a structured-metadata key in the STREAM SELECTOR is not
/// an index over structured metadata.**
///
/// `{trace="a"}` resolves against `log_streams_idx`, which holds STREAM
/// labels only; nothing about this issue's merge changes that, and the
/// merge happens far downstream of stage 1. Measured on the reference at
/// v3.7.4 over a fixture whose ONLY `trace` is per-entry metadata: the
/// query answers an empty matrix.
///
/// Asserted here rather than in the corpus because `logqltest`'s hermetic
/// store feeds every loaded row to the leaf and never applies the selector
/// (`eval_leaf`), so a "matches nothing" corpus case would pass for the
/// wrong reason. This is the layer where the question is actually decided.
#[test]
fn a_structured_metadata_key_in_the_selector_resolves_against_the_stream_index() {
    let mp = metric_plan(
        r#"count_over_time({trace="a"}[5m])"#,
        &range_params(100, Direction::Backward),
    );
    // The selector became an index lookup on the KEY, in `log_streams_idx`.
    assert!(
        mp.stage1_sql.contains("log_streams_idx"),
        "stage 1 reads the stream index: {}",
        mp.stage1_sql
    );
    assert!(
        mp.stage1_sql.contains("key = 'trace'") && mp.stage1_sql.contains("val = 'a'"),
        "the matcher is a stream-label lookup: {}",
        mp.stage1_sql
    );
    // ...and NOT a structured-metadata predicate anywhere.
    assert!(
        !mp.stage1_sql.contains("structured_metadata"),
        "stage 1 never mentions structured metadata: {}",
        mp.stage1_sql
    );
    let read = sliding_sql(
        &mp,
        &[literal("checkout")],
        &[Fingerprint::from_raw(101).sql_literal()],
    );
    let (select, rest) = read.split_once("\nFROM ").expect("a SELECT list");
    assert!(select.contains("structured_metadata"));
    assert!(
        !rest.contains("structured_metadata") && !rest.contains("trace"),
        "and the scan carries neither the column nor the key below its SELECT: {read}"
    );
}

#[test]
fn a_line_filter_range_slides_raw_and_pushes_the_filter_down() {
    let mp = metric_plan(
        r#"count_over_time({env="prod"} |= "err" [5m])"#,
        &range_params(100, Direction::Backward),
    );
    assert!(!mp.rollup);
    assert!(mp.client.is_none());
    assert_eq!(mp.table, "log_samples");
    assert_eq!(mp.extra_predicates.len(), 1);
    assert_eq!(mp.routing.chosen, pulsus_read::logql::RouteChoice::Raw);
    assert_eq!(mp.routing.reason, expected_sliding_reason());

    let sql = lowered_sql(
        &mp,
        &[literal("checkout")],
        &[
            Fingerprint::from_raw(101).sql_literal(),
            Fingerprint::from_raw(205).sql_literal(),
        ],
    );
    assert!(
        sql.contains("PREWHERE service = 'checkout'\n"),
        "the sliding statement must carry PREWHERE service, got:\n{sql}"
    );
}

#[test]
fn bytes_range_with_a_line_filter_slides_raw() {
    let mp = metric_plan(
        r#"bytes_over_time({env="prod"} |= "err" [5m])"#,
        &range_params(100, Direction::Backward),
    );
    assert!(!mp.rollup);
    assert!(mp.client.is_none());
    assert_eq!(mp.routing.reason, expected_sliding_reason());

    let sql = lowered_sql(
        &mp,
        &[literal("checkout")],
        &[
            Fingerprint::from_raw(101).sql_literal(),
            Fingerprint::from_raw(205).sql_literal(),
        ],
    );
    assert!(
        sql.contains("PREWHERE service = 'checkout'\n"),
        "the sliding statement must carry PREWHERE service, got:\n{sql}"
    );
}

#[test]
fn a_non_dividing_step_still_slides_raw_for_range() {
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: START_NS,
            end_ns: END_NS,
            step_ns: 3_000_000_000, // 3s — irrelevant to routing now
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let mp = metric_plan(r#"rate({env="prod"}[5m])"#, &params);
    assert!(!mp.rollup);
    assert_eq!(mp.table, "log_samples");
    assert_eq!(mp.routing.chosen, pulsus_read::logql::RouteChoice::Raw);
    assert_eq!(mp.routing.reason, expected_sliding_reason());

    let sql = lowered_sql(
        &mp,
        &[literal("checkout"), literal("billing")],
        &[
            Fingerprint::from_raw(101).sql_literal(),
            Fingerprint::from_raw(205).sql_literal(),
        ],
    );
    assert!(
        sql.contains("PREWHERE service IN ('checkout', 'billing')\n"),
        "the sliding statement must carry PREWHERE service IN (...) for multiple services, got:\n{sql}"
    );
}

#[test]
fn a_range_query_never_routes_to_the_rollup() {
    // Issue #227: the rollup fast-path is retired for range reads — even a
    // resolution-dividing step reads raw; since issue #624 a clean counting
    // one is counted in the database by the sliding statement.
    let mp = metric_plan(
        r#"rate({env="prod"}[5m])"#,
        &range_params(100, Direction::Backward),
    );
    assert!(!mp.rollup);
    assert!(mp.client.is_none());
    assert_ne!(mp.table, "log_metrics_5s");
    // The sliding statement DOES carry a service PREWHERE (unlike the old
    // rollup path) to keep the `(service, fingerprint, timestamp_ns)` PK
    // prefix engaged.
    let sql = lowered_sql(
        &mp,
        &[literal("checkout")],
        &[
            Fingerprint::from_raw(101).sql_literal(),
            Fingerprint::from_raw(205).sql_literal(),
        ],
    );
    assert!(sql.contains("PREWHERE service = 'checkout'\n"));
}

fn rollup_source() -> sql::MetricSource<'static> {
    sql::MetricSource::new("log_metrics_5s", MetricShape::RollupCount)
}

#[test]
fn metric_range_sql_uses_intdiv_bucketing() {
    let sql = sql::metric_range(
        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        rollup_source(),
        &[],
        &[
            Fingerprint::from_raw(101).sql_literal(),
            Fingerprint::from_raw(205).sql_literal(),
            Fingerprint::from_raw(990).sql_literal(),
        ],
        TimeWindow {
            start_ns: START_NS,
            end_ns: END_NS,
        },
        ScanLowerBound::Exclusive,
        STEP_NS,
        &[],
    );
    assert_eq!(
        sql,
        "SELECT fingerprint, intDiv(bucket_ns, 60000000000) * 60000000000 AS step, sum(count) AS n\n\
         FROM log_metrics_5s\n\
         WHERE org_id = '' AND fingerprint IN ('101', '205', '990') AND bucket_ns > 1782907200000000000 AND bucket_ns <= 1782928800000000000\n\
         GROUP BY fingerprint, step"
    );
}

#[test]
fn metric_instant_sql_has_no_intdiv_bucket_expression() {
    let sql = sql::metric_instant(
        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        rollup_source(),
        &[],
        &[
            Fingerprint::from_raw(101).sql_literal(),
            Fingerprint::from_raw(205).sql_literal(),
        ],
        TimeWindow {
            start_ns: START_NS,
            end_ns: END_NS,
        },
        ScanLowerBound::Exclusive,
        &[],
        // The ROLLUP source: `log_metrics_5s` has no `structured_metadata`
        // column, so the lean projection is the only renderable one here.
        // This combination is unreachable for a LogQL instant plan (the
        // rollup routing decision requires `QuerySpec::Range`); the
        // parameter is what makes rendering an absent column impossible
        // rather than merely unreached (issue #249).
        ScanProjection::Lean,
    );
    assert_eq!(
        sql,
        "SELECT fingerprint, sum(count) AS n\n\
         FROM log_metrics_5s\n\
         WHERE org_id = '' AND fingerprint IN ('101', '205') AND bucket_ns > 1782907200000000000 AND bucket_ns <= 1782928800000000000\n\
         GROUP BY fingerprint"
    );
    assert!(!sql.contains("intDiv"));
    assert!(!sql.contains(" step"));
}

// ---------------------------------------------------------------------
// /volume rollup aggregation (issue #169, docs/api.md §2.6).
// ---------------------------------------------------------------------

/// Byte-exact `log_volume_rollup` snapshot: the same half-open
/// `(fingerprint IN, bucket_ns > s AND <= e)` predicate family as the
/// rollup metric reads above — rollup-ONLY, no body/`log_samples`
/// reference anywhere (the volume endpoint has no raw fallback).
#[test]
fn log_volume_rollup_is_byte_exact() {
    let sql = sql::log_volume_rollup(
        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        "log_metrics_5s",
        &[
            Fingerprint::from_raw(101).sql_literal(),
            Fingerprint::from_raw(205).sql_literal(),
        ],
        TimeWindow {
            start_ns: START_NS,
            end_ns: END_NS,
        },
    );
    assert_eq!(
        sql,
        "SELECT fingerprint, sum(bytes) AS bytes\n\
         FROM log_metrics_5s\n\
         WHERE org_id = '' AND fingerprint IN ('101', '205') AND bucket_ns > 1782907200000000000 AND bucket_ns <= 1782928800000000000\n\
         GROUP BY fingerprint"
    );
    assert!(!sql.contains("body"), "zero body reads by construction");
}

// ---------------------------------------------------------------------
// /detected_labels stream-index aggregation (issue #170, docs/api.md §2.6).
// ---------------------------------------------------------------------

/// The rendered ClickHouse literal for the four `uuid.Parse` forms
/// (hyphenated, `urn:uuid:` + hyphenated, `{hyphenated}`, bare 32-hex) —
/// `ch_string` escapes the regex's `\{`/`\}` backslashes, hence `\\{`.
const UUID_LITERAL: &str = r"'(?i)^(?:(?:urn:uuid:)?[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|\\{[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\}|[0-9a-f]{32})$'";

/// The `[start, end]` window the label-discovery snapshots below render.
/// 5s-aligned (`1751328000` s is exactly divisible by 5), so
/// `sql::activity_lower_bucket_ns` leaves `start_ns` unchanged and the
/// expected literal below is readable; the flooring behaviour itself is
/// pinned by `sql.rs`'s own unit tests and by
/// `logs_detected_live.rs::detected_labels_keeps_a_sample_in_the_bucket_that_contains_start`.
const DISCOVERY_WINDOW: TimeWindow = TimeWindow {
    start_ns: 1_751_328_000_000_000_000,
    end_ns: 1_751_331_600_000_000_000,
};
const DISCOVERY_RES_NS: u64 = 5_000_000_000;

/// The statement a discovery endpoint issues since issue #399, written out
/// by hand. **Nothing on the expected side of the assertions below is
/// produced by `sql::`**: a defect inside a shared builder must move only the
/// left side, so an expected side that called the builder would move with it
/// and stay green.
///
/// The activity read, unscoped — the `GLOBAL IN` subquery of every
/// unscoped discovery statement, and `/series`' own unmatched statement.
const ACTIVE_UNSCOPED: &str = "SELECT DISTINCT fingerprint FROM log_metrics_5s \
     WHERE org_id = '' AND bucket_ns >= 1751328000000000000 AND bucket_ns <= 1751331600000000000";

/// The activity read with `/series`' resolved list pushed inside it, so it
/// reads primary-key point ranges rather than the whole bucket range.
const ACTIVE_SCOPED_1: &str = "SELECT DISTINCT fingerprint FROM log_metrics_5s \
     WHERE org_id = '' AND fingerprint IN ('7') AND bucket_ns >= 1751328000000000000 \
     AND bucket_ns <= 1751331600000000000";
const ACTIVE_SCOPED_3: &str = "SELECT DISTINCT fingerprint FROM log_metrics_5s \
     WHERE org_id = '' AND fingerprint IN ('101', '205', '4294967296') AND bucket_ns >= 1751328000000000000 \
     AND bucket_ns <= 1751331600000000000";

/// A stage-1 statement, opaque to the discovery builders: they nest it
/// verbatim.
const STAGE1: &str = "SELECT fingerprint\nFROM log_streams_idx\nWHERE org_id = '' AND key = 'app'";

/// The activity read with that stage-1 statement nested inside it — scoped
/// discovery's subquery.
const ACTIVE_NESTED: &str = "SELECT DISTINCT fingerprint FROM log_metrics_5s \
     WHERE org_id = '' AND fingerprint GLOBAL IN (\nSELECT fingerprint\nFROM log_streams_idx\nWHERE org_id = '' AND key = 'app'\n) \
     AND bucket_ns >= 1751328000000000000 AND bucket_ns <= 1751331600000000000";

/// The three whole-statement templates, one per discovery builder.
/// `{MONTH}`, `{ACTIVE}` and `{UUID_LITERAL}` are substituted with
/// [`str::replace`] rather than interpolated, so the text stays byte-identical
/// and no `sql::` call can reach the expected side.
const STMT_LABEL_NAMES: &str = "SELECT DISTINCT key AS name\nFROM log_streams_idx\nWHERE org_id = '' AND {MONTH}\n  AND fingerprint GLOBAL IN ({ACTIVE})\nORDER BY name\nLIMIT 100001";
const STMT_LABEL_VALUES: &str = "SELECT DISTINCT val AS value\nFROM log_streams_idx\nWHERE org_id = '' AND {MONTH} AND key = 'env'\n  AND fingerprint GLOBAL IN ({ACTIVE})\nORDER BY value\nLIMIT 100001";
const STMT_DETECTED_LABELS: &str = "SELECT key, uniqExact(val) AS cardinality, countIf(toFloat64OrNull(val) IS NULL AND NOT match(val, {UUID_LITERAL})) AS non_id_values\nFROM log_streams_idx\nWHERE org_id = '' AND {MONTH}\n  AND fingerprint GLOBAL IN ({ACTIVE})\nGROUP BY key\nORDER BY key\nLIMIT 100001";

/// The row bound plus one, as the engine passes it.
const LIMIT: u64 = 100_001;

fn expected_stmt(template: &str, month: &str, active: &str) -> String {
    template
        .replace("{MONTH}", month)
        .replace("{UUID_LITERAL}", UUID_LITERAL)
        .replace("{ACTIVE}", active)
}

fn active_list() -> Vec<FpLiteral> {
    vec![
        Fingerprint::from_raw(101).sql_literal(),
        Fingerprint::from_raw(205).sql_literal(),
        Fingerprint::from_raw(4_294_967_296).sql_literal(),
    ]
}

fn scope(stage1_sql: Option<&str>) -> sql::DiscoveryScope<'_> {
    sql::DiscoveryScope {
        rollup_table: "log_metrics_5s",
        window: DISCOVERY_WINDOW,
        rollup_res_ns: DISCOVERY_RES_NS,
        stage1_sql,
    }
}

/// **T38.** Each of the three discovery endpoints is ONE statement: the
/// `log_streams_idx` scan carries the activity read as a `GLOBAL IN`
/// subquery, and a scoped request nests its stage-1 statement inside that
/// (issue #399), byte for byte.
#[test]
fn the_discovery_statement_nests_the_activity_read() {
    let months = [month_literal(2026, 7)];
    let t = pulsus_model::Tenant::from_header(None, false).expect("the empty tenant");
    for (stage1, active) in [(None, ACTIVE_UNSCOPED), (Some(STAGE1), ACTIVE_NESTED)] {
        let sc = scope(stage1);
        assert_eq!(
            sql::label_names(&t, "log_streams_idx", &months, &sc, LIMIT),
            expected_stmt(STMT_LABEL_NAMES, "month = '2026-07-01'", active)
        );
        assert_eq!(
            sql::label_values(&t, "log_streams_idx", &months, &literal("env"), &sc, LIMIT),
            expected_stmt(STMT_LABEL_VALUES, "month = '2026-07-01'", active)
        );
        assert_eq!(
            sql::detected_labels(&t, "log_streams_idx", &months, &sc, LIMIT),
            expected_stmt(STMT_DETECTED_LABELS, "month = '2026-07-01'", active)
        );
    }

    // `/series`' activity read stays a statement of its own.
    let fps = active_list();
    assert_eq!(
        sql::active_fingerprints(
            &t,
            "log_metrics_5s",
            sql::ActivityScope::All,
            DISCOVERY_WINDOW,
            DISCOVERY_RES_NS
        ),
        ACTIVE_UNSCOPED
    );
    assert_eq!(
        sql::active_fingerprints(
            &t,
            "log_metrics_5s",
            sql::ActivityScope::Listed(&fps),
            DISCOVERY_WINDOW,
            DISCOVERY_RES_NS
        ),
        ACTIVE_SCOPED_3
    );
    assert_eq!(
        sql::active_fingerprints(
            &t,
            "log_metrics_5s",
            sql::ActivityScope::Listed(&[Fingerprint::from_raw(7).sql_literal()]),
            DISCOVERY_WINDOW,
            DISCOVERY_RES_NS
        ),
        ACTIVE_SCOPED_1
    );
    // A start deliberately OFF the 5s grid still floors to the same lower
    // bound, which is what pins `activity_lower_bucket_ns` inside the scoped
    // form.
    assert_eq!(
        sql::active_fingerprints(
            &t,
            "log_metrics_5s",
            sql::ActivityScope::Listed(&[Fingerprint::from_raw(7).sql_literal()]),
            TimeWindow {
                start_ns: 1_751_328_003_000_000_000,
                end_ns: 1_751_331_600_000_000_000,
            },
            DISCOVERY_RES_NS
        ),
        ACTIVE_SCOPED_1
    );
}

/// Byte-exact `detected_labels` snapshot: ONE aggregation over
/// `log_streams_idx` — month-partition-pruned, one output row per key,
/// `uniqExact` cardinality plus the server-side `containsAllIDTypes`
/// predicate — narrowed to the streams active in the requested window by the
/// activity subquery, and with no `log_samples`/body reference anywhere.
#[test]
fn detected_labels_is_byte_exact() {
    let sql = sql::detected_labels(
        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        "log_streams_idx",
        &[month_literal(2026, 7)],
        &scope(None),
        LIMIT,
    );
    assert_eq!(
        sql,
        expected_stmt(
            STMT_DETECTED_LABELS,
            "month = '2026-07-01'",
            ACTIVE_UNSCOPED
        )
    );
    assert!(!sql.contains("log_samples"), "never touches log_samples");
    assert!(!sql.contains("body"), "zero body reads by construction");

    // Two months, so the month clause is the `IN` form.
    assert_eq!(
        sql::detected_labels(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            "log_streams_idx",
            &[month_literal(2026, 7), month_literal(2026, 8)],
            &scope(None),
            LIMIT,
        ),
        expected_stmt(
            STMT_DETECTED_LABELS,
            "month IN ('2026-07-01', '2026-08-01')",
            ACTIVE_UNSCOPED
        )
    );
}

/// Byte-exact `label_names` snapshot: the `/labels` scan is the same single
/// `DISTINCT key` pass it always was, narrowed by the activity subquery.
#[test]
fn label_names_is_byte_exact() {
    assert_eq!(
        sql::label_names(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            "log_streams_idx",
            &[month_literal(2026, 7)],
            &scope(None),
            LIMIT,
        ),
        expected_stmt(STMT_LABEL_NAMES, "month = '2026-07-01'", ACTIVE_UNSCOPED)
    );
}

/// Byte-exact `label_values` snapshot: the subquery sits beside the existing
/// `AND key = ...`, on its own line, so the key predicate keeps its place in
/// the primary-key prefix.
#[test]
fn label_values_is_byte_exact() {
    assert_eq!(
        sql::label_values(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            "log_streams_idx",
            &[month_literal(2026, 7)],
            &literal("env"),
            &scope(None),
            LIMIT,
        ),
        expected_stmt(STMT_LABEL_VALUES, "month = '2026-07-01'", ACTIVE_UNSCOPED)
    );
}

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
    // Issue #12 behaviour change from #11: an instant window has no step to
    // test against the rollup resolution, so it always routes raw (an
    // unaligned `[at-range, at]` window would silently diverge from raw at
    // bucket edges — task-manager resolution #1).
    assert!(!mp.rollup);
    assert_eq!(mp.routing.chosen, pulsus_read::logql::RouteChoice::Raw);
    assert_eq!(mp.routing.reason, "raw: instant query");
}

#[test]
fn range_metric_spec_carries_the_caller_supplied_step() {
    let mp = metric_plan(
        r#"rate({env="prod"}[5m])"#,
        &range_params(100, Direction::Backward),
    );
    assert_eq!(mp.step_ns.map(|d| d.as_u64()), Some(STEP_NS));
    // Issue #227: the emit grid starts at the caller's `start`; the SCAN
    // lower bound is widened back by the `[5m]` range for the lookback.
    assert_eq!(mp.grid_start_ns, START_NS);
    assert_eq!(mp.start_ns, START_NS - RANGE_NS);
    assert_eq!(mp.range_ns.get(), RANGE_NS);
    assert_eq!(mp.end_ns, END_NS);
}

/// Issue #221 AC 12 (performance invariant): `approx_topk` adds NO SQL,
/// no plan change and no extra round-trip — the plan for
/// `approx_topk(3, X)` is byte-identical to the plan for `topk(3, X)` of
/// the same inner, except the outer op enum. Same stage-1 SQL, same
/// routing, same scan window, same pushdown.
#[test]
fn approx_topk_plans_byte_identically_to_topk() {
    let params = QueryParams {
        spec: QuerySpec::Instant { at_ns: END_NS },
        limit: 100,
        direction: Direction::Backward,
    };
    let mut approx = metric_plan(
        r#"approx_topk(3, sum by (lvl) (count_over_time({app="x"}[5m])))"#,
        &params,
    );
    let topk = metric_plan(
        r#"topk(3, sum by (lvl) (count_over_time({app="x"}[5m])))"#,
        &params,
    );
    assert_eq!(
        approx.vector_aggs[0].0,
        pulsus_logql::VectorAggOp::ApproxTopk
    );
    approx.vector_aggs[0].0 = pulsus_logql::VectorAggOp::Topk;
    assert_eq!(
        approx, topk,
        "the two plans must differ ONLY in the outer vector-agg op"
    );
}

#[test]
fn vector_agg_sum_by_captures_the_grouping_labels() {
    let mp = metric_plan(
        r#"sum by (service_name) (rate({env="prod"}[5m]))"#,
        &range_params(100, Direction::Backward),
    );
    assert_eq!(mp.vector_aggs.len(), 1);
    let (op, grouping, _) = &mp.vector_aggs[0];
    assert_eq!(*op, pulsus_logql::VectorAggOp::Sum);
    let grouping = grouping.as_ref().expect("by grouping");
    assert_eq!(grouping.kind, pulsus_logql::GroupingKind::By);
    assert_eq!(grouping.labels, vec!["service_name"]);
    // Issue #614, part 2: a `sum` on the rollup's grid reads the rollup,
    // where the bare `rate(...)` it wraps is counted from the lines (issue
    // #624). A `sum by` reads it only when every selected stream carries
    // the `by` labels as stream labels, which the executor checks after
    // hydration (`rollup::rollup_or_raw`).
    assert!(mp.rollup && mp.routing.reason.starts_with("rollup: sum over"));
}

#[test]
fn vector_agg_without_captures_the_excluded_labels() {
    let mp = metric_plan(
        r#"avg without (service_name) (count_over_time({env="prod"}[5m]))"#,
        &range_params(100, Direction::Backward),
    );
    let (op, grouping, _) = &mp.vector_aggs[0];
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
// Issue M6-10 (AC2): the client-aggregated fetch. Un-piped metric
// snapshots above are the byte-identity perf gate and must stay
// untouched; a piped/unwrapped metric query renders `metric_raw_samples`
// — no SQL aggregate, and deliberately NO `LIMIT` (complete-or-error).
// ---------------------------------------------------------------------

fn client_metric_sql(mp: &pulsus_read::logql::MetricPlan) -> String {
    assert!(mp.client.is_some(), "expected a client-aggregated plan");
    // Issue #227: a range client query reads via the PK-ordered sliding scan.
    pulsus_read::logql::sql::metric_raw_samples_sliding(
        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        &mp.table,
        &[literal("checkout")],
        &[18374, 99120].map(|v| Fingerprint::from_raw(v).sql_literal()),
        TimeWindow {
            start_ns: mp.start_ns,
            end_ns: mp.end_ns,
        },
        mp.scan_lower,
        &mp.extra_predicates,
        projection_of(mp),
    )
}

#[test]
fn an_unwrapped_sum_over_time_renders_a_sliding_raw_scan_with_no_aggregate_and_no_limit() {
    let mp = metric_plan(
        r#"sum_over_time({service_name="checkout"} | logfmt | unwrap duration(took) [5m])"#,
        &range_params(100, Direction::Backward),
    );
    assert!(!mp.rollup, "client mode never selects the rollup");
    assert_eq!(
        mp.routing.reason,
        "raw: client-side pipeline/unwrap aggregation"
    );
    let sql = client_metric_sql(&mp);
    // Issue #227: PK read order `(service, fingerprint, timestamp_ns)` (no
    // body sort, no global timestamp sort), scan widened by the `[5m]` range
    // (start = START_NS - 300s = 1782906900000000000).
    assert_eq!(
        sql,
        "SELECT fingerprint, timestamp_ns, body, structured_metadata\n\
         FROM log_samples\n\
         PREWHERE service = 'checkout'\n\
         WHERE org_id = '' AND fingerprint IN ('18374', '99120')\n\
         \x20 AND timestamp_ns > 1782906900000000000 AND timestamp_ns <= 1782928800000000000\n\
         ORDER BY service ASC, fingerprint ASC, timestamp_ns ASC"
    );
    assert!(!sql.contains("sum(count)"), "{sql}");
    assert!(!sql.contains("LIMIT"), "aggregations never truncate: {sql}");
    assert!(!sql.contains(" body ASC"), "no body in ORDER BY: {sql}");
}

#[test]
fn a_piped_count_over_time_renders_the_raw_scan_with_the_line_filter_pushed_down() {
    let mp = metric_plan(
        r#"count_over_time({service_name="checkout"} |= "err" | json | status = "500" [5m])"#,
        &range_params(100, Direction::Backward),
    );
    assert!(!mp.rollup);
    let sql = client_metric_sql(&mp);
    assert!(
        sql.contains("body LIKE '%err%'"),
        "the pre-parser line filter still pushes down: {sql}"
    );
    assert!(!sql.contains("count()"), "{sql}");
    assert!(!sql.contains("LIMIT"), "{sql}");
    assert!(!sql.contains("GROUP BY"), "{sql}");
}

/// AC4b (plan v2 D3): the ordered-pipeline pushdown invariant on the
/// metric path — a line filter AFTER `line_format` references the
/// rewritten line and must be ABSENT from the SQL predicate (evaluated
/// in-engine), while the filter BEFORE it still pushes down.
#[test]
fn a_post_line_format_metric_line_filter_is_absent_from_the_raw_scan_sql() {
    let mp = metric_plan(
        r#"count_over_time({service_name="checkout"} |= "a" | line_format "{{.x}}" |= "b" [5m])"#,
        &range_params(100, Direction::Backward),
    );
    let sql = client_metric_sql(&mp);
    assert!(
        sql.contains("body LIKE '%a%'"),
        "the pre-line_format filter pushes down: {sql}"
    );
    assert!(
        !sql.contains("'b'"),
        "the post-line_format filter must NOT reach SQL: {sql}"
    );
}

/// Issue #227: an un-piped range `count_over_time` NO LONGER routes to the
/// rollup (the rollup cannot reproduce Loki's per-event sliding-window
/// boundary) — since issue #624 it is counted in the database over raw lines.
#[test]
fn un_piped_count_over_time_range_slides_raw() {
    let mp = metric_plan(
        r#"count_over_time({service_name="checkout"}[5m])"#,
        &range_params(100, Direction::Backward),
    );
    assert!(!mp.rollup);
    assert!(mp.client.is_none());
    assert_eq!(mp.table, "log_samples");
    assert_eq!(mp.routing.reason, expected_sliding_reason());
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

// ---------------------------------------------------------------------
// Issue #221 — the variants single-scan performance gate (Tier-1).
// ---------------------------------------------------------------------

/// The variants scan plan for `query` (the `MetricNode::Variants` leaf).
fn variants_scan(query: &str, params: &QueryParams) -> pulsus_read::logql::MetricPlan {
    let expr = parse(query).expect("parse");
    // Issue #272: `impl Drop for MetricNode` forbids moving out of a
    // field (E0509), so this re-binds through a reference and clones.
    let planned = plan(&expr, params, &ctx()).expect("plan");
    match &planned {
        Plan::MetricBinary(pulsus_read::logql::MetricNode::Variants { scan, .. }) => {
            (**scan).clone()
        }
        other => panic!("expected a variants plan, got {other:?}"),
    }
}

fn n_variants(n: usize, common: &str) -> String {
    format!(
        "variants({}) of ({common})",
        vec![r#"count_over_time({env="prod"}[5m])"#; n].join(", ")
    )
}

/// AC 11/13 (C1) — ONE scan, independent of N: the `stage1_sql` and the
/// rendered read SQL for `variants(V1) of (LR)`, `variants(V1,V2) of
/// (LR)` and `variants(V1..V5) of (LR)` are byte-identical to each other
/// AND to the client-aggregated `count_over_time(LR)` plan's, at both
/// range and instant — same fingerprint resolution, same `PREWHERE
/// service` PK-prefix engagement, same `body LIKE` line-filter pushdown
/// surface, same single round-trip. A common-range `unwrap` (dead
/// syntax) changes neither string.
///
/// Issue #397 extends it: a WRAPPED variant now runs its own whole
/// pipeline, and that pipeline must add nothing to the scan. A variant's
/// line filter cannot be pushed to ClickHouse — one scan feeds every
/// variant, so a per-variant predicate would drop rows its siblings need
/// — and the reference has the same constraint (it selects once, from
/// `m.logRange.Left`, `pkg/logql/syntax/extractor.go:104 @ v3.7.4`). So
/// the scan for a variant carrying ` |= "z" | logfmt` is byte-identical
/// to the same query with those stages deleted: no extra predicate, no
/// extra probe, no extra round-trip.
#[test]
fn variants_scan_sql_is_byte_identical_to_the_single_extractor_plan() {
    let services = &[literal("checkout")];
    let fps = &[101, 205].map(|v| Fingerprint::from_raw(v).sql_literal());
    for params in [
        range_params(100, Direction::Backward),
        QueryParams {
            spec: QuerySpec::Instant { at_ns: END_NS },
            limit: 100,
            direction: Direction::Backward,
        },
    ] {
        let is_range = matches!(params.spec, QuerySpec::Range { .. });
        let read_sql = |mp: &pulsus_read::logql::MetricPlan| {
            if is_range {
                sliding_sql(mp, services, fps)
            } else {
                sql::metric_raw_samples(
                    &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                    &mp.table,
                    services,
                    fps,
                    TimeWindow {
                        start_ns: mp.start_ns,
                        end_ns: mp.end_ns,
                    },
                    mp.scan_lower,
                    &mp.extra_predicates,
                    projection_of(mp),
                )
            }
        };
        // The single-extractor baseline (AC 13's byte-equality) exists at
        // RANGE: the raw scan rendered from the plain range aggregation's
        // plan. Since issue #624 that plan is counted in the database, and
        // this scan is the one its fallback reads — today's route, which is
        // what a variants scan is compared with. An INSTANT un-piped count
        // is SQL-aggregated (a different read shape by design), so the
        // instant half of the gate is N-independence.
        let (baseline_stage1, baseline_read) = if is_range {
            let single = metric_plan(r#"count_over_time({env="prod"}[5m])"#, &params);
            (single.stage1_sql.clone(), read_sql(&single))
        } else {
            let scan = variants_scan(&n_variants(1, r#"{env="prod"}[5m]"#), &params);
            (scan.stage1_sql.clone(), read_sql(&scan))
        };
        for n in [1usize, 2, 5] {
            let scan = variants_scan(&n_variants(n, r#"{env="prod"}[5m]"#), &params);
            assert_eq!(
                scan.stage1_sql, baseline_stage1,
                "stage1 SQL must be N-independent and single-extractor-identical (n={n})"
            );
            assert_eq!(
                read_sql(&scan),
                baseline_read,
                "read SQL must be N-independent and single-extractor-identical (n={n})"
            );
        }
        // Δ1: a dead common-range unwrap changes neither string.
        let scan = variants_scan(
            r#"variants(count_over_time({env="prod"}[5m])) of ({env="prod"} | unwrap v [5m])"#,
            &params,
        );
        assert_eq!(scan.stage1_sql, baseline_stage1);
        assert_eq!(read_sql(&scan), baseline_read);

        // Issue #397: a WRAPPED variant with a LIVE prefix changes
        // neither string either. Its ` |= "z" | logfmt` runs
        // client-side, on rows the one shared scan already returned.
        let wrapped = variants_scan(
            r#"variants(sum by (env) (count_over_time({env="prod"} |= "z" | logfmt [5m]))) of ({env="prod"}[5m])"#,
            &params,
        );
        let stripped = variants_scan(
            r#"variants(sum by (env) (count_over_time({env="prod"}[5m]))) of ({env="prod"}[5m])"#,
            &params,
        );
        assert_eq!(
            wrapped.stage1_sql, stripped.stage1_sql,
            "a live variant prefix must not reach stage-1 SQL"
        );
        assert_eq!(
            sqls(&wrapped.extra_predicates),
            sqls(&stripped.extra_predicates),
            "a live variant prefix must not become a scan predicate"
        );
        assert_eq!(
            read_sql(&wrapped),
            read_sql(&stripped),
            "a live variant prefix must not change the read SQL"
        );
        assert_eq!(wrapped.stage1_sql, baseline_stage1);
        assert_eq!(read_sql(&wrapped), baseline_read);
    }
}

/// AC 12 (C2, structural half) — a 5-variant tree has exactly ONE scan
/// leaf, so the engine (whose `run_variants` pushes exactly one
/// `stage1_stream_resolution`, one `stage2_hydration` and one
/// `metric_read` per scan) issues exactly one of each; the full
/// explain-stage assertion is live-path territory.
#[test]
fn a_five_variant_tree_has_exactly_one_scan_leaf() {
    let expr = parse(&n_variants(5, r#"{env="prod"}[5m]"#)).expect("parse");
    let params = range_params(100, Direction::Backward);
    let Plan::MetricBinary(node) = plan(&expr, &params, &ctx()).expect("plan") else {
        panic!("expected a MetricBinary plan");
    };
    assert_eq!(node.leaves().len(), 1, "one scan, N-independent");
}

// ---------------------------------------------------------------------
// Issue #343 (boundary fix) — AC 5a/5c: the RENDERED scan predicate when
// the offset shift leaves the representable timestamp axis.
//
// The layer is the point. A planner-field assertion would pass while the
// user still got a domain-wide scan; what a user experiences is the SQL
// text, so that is what these pin. The wire half (no `metric_read` stage
// at all) lives in `pulsus-server/tests/logs_api_live.rs`.
// ---------------------------------------------------------------------

/// One hour below `i64::MAX`. The 5-year caps bound every DURATION in a
/// query, so the only unbounded quantity left is where the request sits on
/// the axis — which is what these two cases vary. Both offsets below are
/// an ordinary one hour.
const OFFSET_AT_NS: i64 = i64::MAX - 3_600_000_000_000 + 1;

/// Renders `sql::metric_instant` from a plan exactly as `LogQlEngine`
/// would for an instant metric query, so the asserted text is the text
/// that would reach ClickHouse.
fn instant_sql(mp: &pulsus_read::logql::MetricPlan, fingerprints: &[FpLiteral]) -> String {
    sql::metric_instant(
        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        sql::MetricSource::new(
            &mp.table,
            mp.source_shape()
                .expect("plan::metric_plan writes both columns out of MetricShape"),
        ),
        &[literal("checkout")],
        fingerprints,
        TimeWindow {
            start_ns: mp.start_ns,
            end_ns: mp.end_ns,
        },
        mp.scan_lower,
        &mp.extra_predicates,
        // Issue #249: an instant plan is always raw over `log_samples`.
        ScanProjection::WithStructuredMetadata,
    )
}

/// **AC 5a.** `offset -1h` at an instant one hour below `i64::MAX` shifts
/// the evaluation domain past the rail. The scan predicate must be the
/// DEGENERATE empty window.
///
/// Against `17e2802` this rendered a saturated window instead — the shift
/// clamped both bounds onto the `i64::MAX` rail, producing a scan over a
/// span the request never named. (The case then read
/// `[2500000h] offset -2500000h` at a 2026 instant, rendering
/// `timestamp_ns > 223372036854775807`, a 1977-01-08 floor; both those
/// durations are now refused at parse by the 5-year cap, so the extreme
/// has moved to the request's position, which nothing bounds.)
///
/// The phrases are asserted whole and delimited: `> 0 AND` must not be
/// able to match as a prefix of a longer number.
#[test]
fn an_offset_past_the_timestamp_axis_renders_the_degenerate_empty_window() {
    let params = QueryParams {
        spec: QuerySpec::Instant {
            at_ns: OFFSET_AT_NS,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let mp = metric_plan(r#"count_over_time({env="prod"}[5m] offset -1h)"#, &params);
    assert!(mp.empty_domain, "the shifted domain left the axis");
    assert_eq!((mp.start_ns, mp.grid_start_ns, mp.end_ns), (0, 0, -1));
    let sql = instant_sql(
        &mp,
        &[
            Fingerprint::from_raw(101).sql_literal(),
            Fingerprint::from_raw(205).sql_literal(),
        ],
    );
    assert!(
        sql.contains("timestamp_ns > 0 AND timestamp_ns <= -1"),
        "expected the degenerate empty window, got:\n{sql}"
    );
    assert!(
        !sql.contains("timestamp_ns <= 9223372036854775807"),
        "the saturated rail must be gone:\n{sql}"
    );
}

/// **AC 5c — the non-vacuity control, and it is required.** The SAME query
/// with the sign flipped stays inside the axis and must render its real
/// window. Deleting the shift, or making every plan degenerate, reddens
/// this while leaving the case above green.
#[test]
fn an_offset_inside_the_timestamp_axis_still_renders_its_real_window() {
    let params = QueryParams {
        spec: QuerySpec::Instant {
            at_ns: OFFSET_AT_NS,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let mp = metric_plan(r#"count_over_time({env="prod"}[5m] offset 1h)"#, &params);
    assert!(!mp.empty_domain, "T - 1h is representable");
    // T - 1h = i64::MAX - 2h + 1, and the scan reaches a further 5m back.
    let end = OFFSET_AT_NS - 3_600_000_000_000;
    let start = end - 300_000_000_000;
    let sql = instant_sql(
        &mp,
        &[
            Fingerprint::from_raw(101).sql_literal(),
            Fingerprint::from_raw(205).sql_literal(),
        ],
    );
    assert!(
        sql.contains(&format!("timestamp_ns > {start} AND timestamp_ns <= {end}")),
        "expected the shifted-but-representable window, got:\n{sql}"
    );
}

// ---------------------------------------------------------------------
// Issue #793 — a matcher that matches the empty string selects streams
// without its label. A stream without label `k` reads as `k=""`: a key is
// required when one of its matchers does not match `""`, and a selector
// needs at least one such matcher.
// ---------------------------------------------------------------------

fn stage1_793(query: &str) -> String {
    streams_plan(query, &range_params(100, Direction::Backward)).stage1_sql
}

/// The plan result of `query`, as text: `Ok` with the stage-1 SQL of a
/// streams plan, or the error.
fn plan_793(query: &str) -> Result<String, String> {
    let expr = parse(query).expect("parse");
    match plan(&expr, &range_params(100, Direction::Backward), &ctx()) {
        Ok(Plan::Streams(sp)) => Ok(sp.stage1_sql),
        Ok(other) => Err(format!("not a streams plan: {other:?}")),
        Err(e) => Err(format!("{e:?}")),
    }
}

const S2_793: &str = "SELECT fingerprint\n\
     FROM log_streams_idx\n\
     WHERE org_id = '' AND month = '2026-07-01'\n\
     \x20 AND ((key = 'host' AND match(val, '^(?:.+)$')) OR (key = 'unit' AND val != ''))\n\
     GROUP BY fingerprint\n\
     HAVING uniqExactIf((key, val), (key = 'host' AND match(val, '^(?:.+)$'))) = 1\n\
     \x20  AND countIf((key = 'unit' AND val != '')) = 0";

/// S1: a `=~` that matches `""` is a failure branch, not a required key.
#[test]
fn s1_793_a_regex_matching_empty_does_not_require_its_key() {
    assert_eq!(
        stage1_793(r#"{host=~".+", unit=~".*"}"#),
        "SELECT fingerprint\n\
         FROM log_streams_idx\n\
         WHERE org_id = '' AND month = '2026-07-01'\n\
         \x20 AND ((key = 'host' AND match(val, '^(?:.+)$')) OR (key = 'unit' AND NOT match(val, '^(?:.*)$')))\n\
         GROUP BY fingerprint\n\
         HAVING uniqExactIf((key, val), (key = 'host' AND match(val, '^(?:.+)$'))) = 1\n\
         \x20  AND countIf((key = 'unit' AND NOT match(val, '^(?:.*)$'))) = 0"
    );
}

/// S2: `unit=""` selects the streams without `unit`.
#[test]
fn s2_793_an_empty_equality_selects_streams_without_the_label() {
    assert_eq!(stage1_793(r#"{host=~".+", unit=""}"#), S2_793);
}

/// S2m: the metric path reaches the same rule.
#[test]
fn s2m_793_the_metric_path_reaches_the_same_rule() {
    let mp = metric_plan(
        r#"sum(count_over_time({host=~".+", unit=""}[5m]))"#,
        &range_params(100, Direction::Backward),
    );
    assert_eq!(mp.stage1_sql, S2_793);
}

/// S3 (control): the issue's negative row stays byte-identical.
#[test]
fn s3_793_a_negative_regex_not_matching_empty_is_unchanged() {
    assert_eq!(
        stage1_793(r#"{host=~".+", unit!~".+"}"#),
        "SELECT fingerprint\n\
         FROM log_streams_idx\n\
         WHERE org_id = '' AND month = '2026-07-01'\n\
         \x20 AND ((key = 'host' AND match(val, '^(?:.+)$')) OR (key = 'unit' AND match(val, '^(?:.+)$')))\n\
         GROUP BY fingerprint\n\
         HAVING uniqExactIf((key, val), (key = 'host' AND match(val, '^(?:.+)$'))) = 1\n\
         \x20  AND countIf((key = 'unit' AND match(val, '^(?:.+)$'))) = 0"
    );
}

/// S4: `unit!=""` requires `unit`.
#[test]
fn s4_793_not_equal_empty_requires_the_label() {
    assert_eq!(
        stage1_793(r#"{host=~".+", unit!=""}"#),
        "SELECT fingerprint\n\
         FROM log_streams_idx\n\
         WHERE org_id = '' AND month = '2026-07-01'\n\
         \x20 AND ((key = 'host' AND match(val, '^(?:.+)$')) OR (key = 'unit') OR (key = 'unit' AND val = ''))\n\
         GROUP BY fingerprint\n\
         HAVING uniqExactIf((key, val), (key = 'host' AND match(val, '^(?:.+)$')) OR (key = 'unit')) = 2\n\
         \x20  AND countIf((key = 'unit' AND val = '')) = 0"
    );
}

/// S5: `{unit!=""}` alone is served.
#[test]
fn s5_793_not_equal_empty_alone_is_served() {
    assert_eq!(
        plan_793(r#"{unit!=""}"#),
        Ok("SELECT fingerprint\n\
            FROM log_streams_idx\n\
            WHERE org_id = '' AND month = '2026-07-01'\n\
            \x20 AND ((key = 'unit') OR (key = 'unit' AND val = ''))\n\
            GROUP BY fingerprint\n\
            HAVING uniqExactIf((key, val), (key = 'unit')) = 1\n\
            \x20  AND countIf((key = 'unit' AND val = '')) = 0"
            .to_string())
    );
}

/// S6: failure-branch order — negatives in matcher order, then the
/// demoted group's.
#[test]
fn s6_793_failure_branches_keep_negatives_first() {
    assert_eq!(
        stage1_793(r#"{service_name="checkout", unit=~".*", unit!="a"}"#),
        "SELECT fingerprint\n\
         FROM log_streams_idx\n\
         WHERE org_id = '' AND month = '2026-07-01'\n\
         \x20 AND ((key = 'service_name' AND val = 'checkout') OR (key = 'unit' AND val = 'a') OR (key = 'unit' AND NOT match(val, '^(?:.*)$')))\n\
         GROUP BY fingerprint\n\
         HAVING uniqExactIf((key, val), (key = 'service_name' AND val = 'checkout')) = 1\n\
         \x20  AND countIf((key = 'unit' AND val = 'a') OR (key = 'unit' AND NOT match(val, '^(?:.*)$'))) = 0"
    );
}

/// S7 (control): a group with one pattern that cannot match `""` stays
/// positive.
#[test]
fn s7_793_a_group_with_one_non_empty_pattern_stays_required() {
    assert_eq!(
        stage1_793(r#"{host=~".+", unit=~".*", unit=~".+"}"#),
        "SELECT fingerprint\n\
         FROM log_streams_idx\n\
         WHERE org_id = '' AND month = '2026-07-01'\n\
         \x20 AND ((key = 'host' AND match(val, '^(?:.+)$')) OR (key = 'unit' AND match(val, '^(?:.*)$') AND match(val, '^(?:.+)$')))\n\
         GROUP BY fingerprint\n\
         HAVING uniqExact(key, val) = 2"
    );
}

/// S8 (control): the verdict on `""` is the database's reading of the
/// pattern, not the as-written one.
#[test]
fn s8_793_the_verdict_on_empty_is_the_databases() {
    assert_eq!(
        stage1_793(r#"{host=~".+", unit=~"[a[b]]?"}"#),
        "SELECT fingerprint\n\
         FROM log_streams_idx\n\
         WHERE org_id = '' AND month = '2026-07-01'\n\
         \x20 AND ((key = 'host' AND match(val, '^(?:.+)$')) OR (key = 'unit' AND match(val, '^(?:[a[b]]?)$')))\n\
         GROUP BY fingerprint\n\
         HAVING uniqExact(key, val) = 2"
    );
}

/// S9: a selector with no matcher that fails to match `""` is refused.
#[test]
fn s9_793_a_selector_every_matcher_of_which_matches_empty_is_refused() {
    for q in [r#"{unit=~".*"}"#, r#"{unit=""}"#, r#"{unit=~".*", foo=""}"#] {
        let expr = parse(q).expect("parse");
        let got = plan(&expr, &range_params(100, Direction::Backward), &ctx());
        assert!(
            matches!(got, Err(pulsus_read::logql::ReadError::EmptyMatcherSet)),
            "{q}: expected EmptyMatcherSet, got {:?}",
            got.map(|_| "a plan")
        );
    }
}

/// S9c (control): `{unit!="x"}` matches `""` and stays refused.
#[test]
fn s9c_793_a_negative_matching_empty_alone_stays_refused() {
    let q = r#"{unit!="x"}"#;
    let expr = parse(q).expect("parse");
    let got = plan(&expr, &range_params(100, Direction::Backward), &ctx());
    assert!(
        matches!(got, Err(pulsus_read::logql::ReadError::EmptyMatcherSet)),
        "{q}: expected EmptyMatcherSet, got {:?}",
        got.map(|_| "a plan")
    );
}

/// S10 (control): a key a negative requires keeps its group positive.
#[test]
fn s10_793_a_key_a_negative_requires_keeps_its_group_positive() {
    assert_eq!(
        stage1_793(r#"{host=~".+", unit=~".*", unit!=""}"#),
        "SELECT fingerprint\n\
         FROM log_streams_idx\n\
         WHERE org_id = '' AND month = '2026-07-01'\n\
         \x20 AND ((key = 'host' AND match(val, '^(?:.+)$')) OR (key = 'unit' AND match(val, '^(?:.*)$')) OR (key = 'unit' AND val = ''))\n\
         GROUP BY fingerprint\n\
         HAVING uniqExactIf((key, val), (key = 'host' AND match(val, '^(?:.+)$')) OR (key = 'unit' AND match(val, '^(?:.*)$'))) = 2\n\
         \x20  AND countIf((key = 'unit' AND val = '')) = 0"
    );
}

/// S11: a `!~` whose regex matches `""` requires the key, and every value
/// fails it: no stream.
#[test]
fn s11_793_a_negative_regex_matching_empty_requires_the_key() {
    assert_eq!(
        stage1_793(r#"{host=~".+", unit!~".*"}"#),
        "SELECT fingerprint\n\
         FROM log_streams_idx\n\
         WHERE org_id = '' AND month = '2026-07-01'\n\
         \x20 AND ((key = 'host' AND match(val, '^(?:.+)$')) OR (key = 'unit') OR (key = 'unit' AND match(val, '^(?:.*)$')))\n\
         GROUP BY fingerprint\n\
         HAVING uniqExactIf((key, val), (key = 'host' AND match(val, '^(?:.+)$')) OR (key = 'unit')) = 2\n\
         \x20  AND countIf((key = 'unit' AND match(val, '^(?:.*)$'))) = 0"
    );
}

/// S12: where the database's reading of the pattern does not compile, the
/// as-written compile decides. The pattern is `[\pL--\d]*`; `ch_string`
/// doubles each backslash in the statement text.
#[test]
fn s12_793_the_as_written_verdict_decides_when_the_rewrite_does_not_compile() {
    assert_eq!(
        plan_793(r#"{host=~".+", unit=~"[\\pL--\\d]*"}"#),
        Ok("SELECT fingerprint\n\
            FROM log_streams_idx\n\
            WHERE org_id = '' AND month = '2026-07-01'\n\
            \x20 AND ((key = 'host' AND match(val, '^(?:.+)$')) OR (key = 'unit' AND NOT match(val, '^(?:[\\\\pL--\\\\d]*)$')))\n\
            GROUP BY fingerprint\n\
            HAVING uniqExactIf((key, val), (key = 'host' AND match(val, '^(?:.+)$'))) = 1\n\
            \x20  AND countIf((key = 'unit' AND NOT match(val, '^(?:[\\\\pL--\\\\d]*)$'))) = 0"
            .to_string())
    );
}
