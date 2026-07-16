//! Issue #59 AC2: the hermetic, byte-frozen golden suite for the TraceQL
//! metrics SQL (docs/schemas.md §4.2, docs/api.md §4.4). Every case
//! renders one deterministic composite — the plan's range SQL and
//! instant SQL — and byte-compares it against a committed file under
//! `tests/golden/traces_metrics/`. **Do not** edit the committed files
//! by hand — run the `#[ignore]` `regenerate_goldens` test and review
//! the diff (the byte-frozen-artifact rule).

use pulsus_read::traces::metrics_plan::{MetricsCtx, MetricsParams, plan_trace_metrics};
use pulsus_read::{SpanFilterCtx, TraceMetricsPlan};

/// Fixed request window: the search suite's 2023-11-14T22:13:20Z .. +3h
/// shape. 1_700_000_000 is deliberately NOT a multiple of 60 — the
/// goldens pin the outward epoch snap (S = 1_699_999_980, E =
/// 1_700_010_840 for step 60).
const PARAMS: MetricsParams = MetricsParams {
    start_ns: 1_700_000_000_000_000_000,
    end_ns: 1_700_010_800_000_000_000,
    step_s: 60,
};

struct Case {
    name: &'static str,
    q: &'static str,
    distributed: bool,
}

const CASES: &[Case] = &[
    Case {
        // The docs/schemas.md §4.2 worked example: the root-AND-spine
        // service equality hoists to PREWHERE (service_time projection);
        // the numeric attr leaf is an index-served semi-join; duration
        // renders inline on the physical column. Counting is the
        // replay-deduped uniqExact — never bare count().
        name: "rate_worked_example",
        q: r#"{ resource.service.name = "checkout" && span.http.status_code >= 500 && duration > 2s } | rate()"#,
        distributed: false,
    },
    Case {
        // Same filter, count_over_time: the SQL body is IDENTICAL to
        // rate (the function only changes the client-side value math at
        // the encode boundary — plan v2 delta 5).
        name: "count_over_time_worked_example",
        q: r#"{ resource.service.name = "checkout" && span.http.status_code >= 500 && duration > 2s } | count_over_time()"#,
        distributed: false,
    },
    Case {
        // `{}` match-all: time-only pushdown, day-pruned then bounded by
        // the Layer-1 budgets.
        name: "match_all_rate",
        q: "{} | rate()",
        distributed: false,
    },
    Case {
        // A lone scoped attr leaf: the whole WHERE is one semi-join.
        name: "attr_semi_join",
        q: "{ span.http.status_code >= 500 } | rate()",
        distributed: false,
    },
    Case {
        // Negated scoped attr: NOT IN around the positive predicate (the
        // ratified absent-key rule).
        name: "negated_attr",
        q: r#"{ span.env != "prod" } | count_over_time()"#,
        distributed: false,
    },
    Case {
        // Unscoped negation: NO scope clause in the subquery — the
        // positive set spans both scopes, so NOT IN counts spans with no
        // positive row in either (plan v2 test-gap closure).
        name: "unscoped_negated_attr",
        q: r#"{ .env != "prod" } | rate()"#,
        distributed: false,
    },
    Case {
        // Nested-OR service equalities: Or is opaque — NO PREWHERE, both
        // service leaves render inline in WHERE (plan v2 delta 4).
        name: "nested_or_service_no_hoist",
        q: r#"{ (resource.service.name = "a" || resource.service.name = "b") && duration > 1s } | rate()"#,
        distributed: false,
    },
    Case {
        // Mixed boolean tree: attr semi-join OR physical leaf, ANDed
        // with a status leaf — pins the deterministic parenthesization.
        name: "mixed_boolean",
        q: r#"{ (span.foo = "x" || duration > 2s) && status = error } | rate()"#,
        distributed: false,
    },
    Case {
        // The clustered worked example: `_dist` tables everywhere; the
        // §7 clustered-reader + set-limit + local-product settings ride
        // as HTTP settings, never SQL text (pinned in traces::exec unit
        // tests).
        name: "clustered_worked_example",
        q: r#"{ resource.service.name = "checkout" && span.http.status_code >= 500 && duration > 2s } | rate()"#,
        distributed: true,
    },
];

fn plan_for(case: &Case) -> TraceMetricsPlan {
    let (spans, attrs) = if case.distributed {
        ("trace_spans_dist", "trace_attrs_idx_dist")
    } else {
        ("trace_spans", "trace_attrs_idx")
    };
    let query = pulsus_traceql::parse(case.q).expect("case query parses");
    plan_trace_metrics(
        &query,
        &PARAMS,
        &MetricsCtx {
            filter: SpanFilterCtx {
                spans_table: spans,
                attrs_table: attrs,
            },
            scan_budget_rows: 50_000_000,
            distributed: case.distributed,
            skip_unavailable_shards: false,
        },
    )
    .expect("case query plans")
}

/// The deterministic composite rendering one golden file freezes: both
/// SQL forms of the plan (range → matrix, instant → vector).
fn composite(case: &Case) -> String {
    let plan = plan_for(case);
    format!(
        "-- case: {}\n-- q: {}\n\n== range (query_range) ==\n{}\n\n== instant (query) ==\n{}\n",
        case.name,
        case.q,
        plan.range_sql(),
        plan.instant_sql()
    )
}

fn golden_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
        .join("traces_metrics")
}

#[test]
fn every_case_matches_its_committed_golden_byte_for_byte() {
    for case in CASES {
        let path = golden_dir().join(format!("{}.sql", case.name));
        let expected = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!(
                "missing golden {path:?} ({e}); run `cargo test -p pulsus-read --test \
                 traces_metrics_sql -- --ignored regenerate_goldens` and commit the diff"
            )
        });
        let actual = composite(case);
        assert_eq!(
            actual, expected,
            "case {:?} drifted from its committed golden {path:?} — if the change is \
             intentional, regenerate and review the diff",
            case.name
        );
    }
}

/// Targeted content assertions on the worked example (the plan's pinned
/// fragments), independent of the composite framing.
#[test]
fn worked_example_pins_the_documented_fragments() {
    let plan = plan_for(&CASES[0]);
    let range = plan.range_sql();
    assert!(range.starts_with(
        "SELECT toUnixTimestamp(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), \
         INTERVAL 60 SECOND)) AS t,\n       uniqExact(trace_id, span_id) AS n\n"
    ));
    assert!(range.contains("PREWHERE service = 'checkout'"));
    // Snapped, left-closed/right-open bounds (plan v2 delta 2) — NOT the
    // raw request window and NOT search's `> start AND <= end`.
    assert!(range.contains(
        "WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000"
    ));
    assert!(range.contains(
        "(trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE \
         date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')"
    ));
    assert!(range.contains("key = 'http.status_code' AND val_num >= 500 AND scope = 'span'"));
    assert!(range.contains("duration_ns > 2000000000"));
    assert!(range.ends_with("GROUP BY t\nORDER BY t ASC"));
    assert!(
        !range.contains("count()"),
        "counting is always the replay-deduped uniqExact"
    );
    assert!(
        !range.contains("/ 60"),
        "the rate division is client-side at the encode boundary, never SQL"
    );
    // The instant form is the same body without bucketing.
    let instant = plan.instant_sql();
    assert!(instant.starts_with("SELECT uniqExact(trace_id, span_id) AS n\n"));
    assert!(!instant.contains("GROUP BY"));
    assert_eq!(plan.snapped_end_ms(), 1_700_010_840_000);
}

#[test]
fn rate_and_count_over_time_share_one_sql_body() {
    // Plan v2 delta 5: the function changes only the encode-boundary
    // value math — byte-identical SQL keeps the AC4 identities exact.
    assert_eq!(
        plan_for(&CASES[0]).range_sql(),
        plan_for(&CASES[1]).range_sql()
    );
    assert_eq!(
        plan_for(&CASES[0]).instant_sql(),
        plan_for(&CASES[1]).instant_sql()
    );
}

#[test]
fn clustered_case_targets_the_dist_tables_everywhere() {
    let plan = plan_for(
        CASES
            .iter()
            .find(|c| c.distributed)
            .expect("clustered case"),
    );
    assert!(plan.range_sql().contains("FROM trace_spans_dist\n"));
    assert!(plan.range_sql().contains("FROM trace_attrs_idx_dist WHERE"));
    assert!(plan.instant_sql().contains("FROM trace_spans_dist\n"));
    assert!(plan.distributed());
}

/// Doc-consistency gate (the search suite's AC8 pattern): every shipped
/// metrics SQL shape and committed constant is documented —
/// docs/schemas.md §4.2 (the pushdown shape, dedup counting, snapping)
/// and docs/api.md §4.4 (function set, step derivation, point cap, 422).
#[test]
fn shipped_metrics_shapes_and_limits_are_documented() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root");
    let schemas = std::fs::read_to_string(root.join("docs/schemas.md")).expect("read schemas.md");
    let api = std::fs::read_to_string(root.join("docs/api.md")).expect("read api.md");

    for needle in [
        "toUnixTimestamp(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), INTERVAL 60 SECOND)) AS t",
        "uniqExact(trace_id, span_id) AS n",
        "PREWHERE service = 'checkout'",
        "GROUP BY t",
        "max_rows_in_set",
        "set_overflow_mode = 'throw'",
        "distributed_product_mode = 'local'",
    ] {
        assert!(
            schemas.contains(needle),
            "docs/schemas.md §4.2 must document {needle:?}"
        );
    }
    for needle in [
        "rate()",
        "count_over_time()",
        "DEFAULT_METRICS_POINTS",
        "MAX_METRICS_POINTS",
        "11000",
        "query_too_broad",
        "left-closed",
    ] {
        assert!(
            api.contains(needle),
            "docs/api.md §4.4 must document {needle:?}"
        );
    }
}

/// Regenerates every committed golden. `#[ignore]`d: run explicitly
/// after an intentional SQL-shape change, review the diff, and say so in
/// the PR (byte-frozen-artifact rule).
#[test]
#[ignore = "regenerates the committed goldens; run explicitly, see doc comment"]
fn regenerate_goldens() {
    let dir = golden_dir();
    std::fs::create_dir_all(&dir).expect("create golden dir");
    for case in CASES {
        let path = dir.join(format!("{}.sql", case.name));
        std::fs::write(&path, composite(case)).unwrap_or_else(|e| panic!("write {path:?}: {e}"));
        eprintln!("wrote {path:?}");
    }
}
