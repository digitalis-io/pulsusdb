//! Issue #596 part 1: the byte-frozen goldens of the metrics statement on
//! `spans` (`crates/pulsus-read/src/traces/spans/metrics.rs`), its range
//! and instant forms, in `tests/golden/traces_spans_metrics/`.
//!
//! **Do not** edit the committed files by hand: run the `#[ignore]`d
//! `regenerate_spans_metrics_goldens` test and review the diff.

use pulsus_read::traces::metrics_plan::{MetricsCtx, MetricsParams};
use pulsus_read::{SpanFilterCtx, StatementTables, plan_trace_metrics_routed};

/// `(stem, query)`: every golden, over `start = 1791400000`, `end =
/// 1791410800`, `step = 60s`, `max_series = 1000` and the default budget.
const CASES: [(&str, &str); 23] = [
    ("rate_match_all", "{} | rate()"),
    ("rate_by_service", "{} | rate() by(resource.service.name)"),
    (
        "rate_by_service_topk",
        "{} | rate() by(resource.service.name) | topk(3)",
    ),
    (
        "count_compared",
        "{} | count_over_time() by(resource.service.name) > 5",
    ),
    (
        "count_no_exemplars",
        "{} | count_over_time() by(resource.service.name) with(exemplars=false)",
    ),
    ("avg_unscoped", "{ .a = 1 } | avg_over_time(duration)"),
    ("sum_errors", "{ status = error } | sum_over_time(duration)"),
    (
        "quantile_three",
        "{} | quantile_over_time(duration, 0.5, 0.9, 0.99)",
    ),
    ("histogram", "{} | histogram_over_time(duration)"),
    (
        "field_vs_field",
        "{ span.http.route = span.url.path } | rate()",
    ),
    (
        "arithmetic",
        "{ duration > span.http.request.body.size * 1000 } | rate()",
    ),
    (
        "explore_root",
        "{nestedSetParent<0 && true && resource.service.name != nil} | rate() \
         by(resource.service.name)",
    ),
    // Issue #596 part 2: any key, typed; attribute targets.
    ("by_route", "{} | rate() by(span.http.route)"),
    (
        "two_keys",
        "{} | rate() by(resource.service.name, span.http.route)",
    ),
    (
        "quantile_by_route",
        "{} | quantile_over_time(duration, 0.5, 0.9, 0.99) by(span.http.route)",
    ),
    (
        "histogram_by_service",
        "{} | histogram_over_time(duration) by(resource.service.name)",
    ),
    (
        "avg_attribute",
        "{} | avg_over_time(span.http.request.body.size)",
    ),
    ("by_root_service", "{} | rate() by(trace:rootService)"),
    ("by_nested_parent", "{} | rate() by(nestedSetParent)"),
    ("by_child_count", "{} | rate() by(span:childCount)"),
    // Issue #594 part 5: the seven in a filter.
    ("filter_trace_duration", "{ trace:duration > 1s } | rate()"),
    ("filter_child_count", "{ span:childCount > 1 } | rate()"),
    (
        "filter_nested_left",
        "{ nestedSetLeft > 0 } | rate() by(resource.service.name)",
    ),
];

fn golden_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
        .join("traces_spans_metrics")
}

/// One golden's text: two comment lines, then the range and the instant
/// statement, each under its `== ` header.
fn golden(stem: &str, query: &str) -> String {
    let parsed =
        pulsus_traceql::parse(query).unwrap_or_else(|e| panic!("{query} must parse: {e:?}"));
    let routed = plan_trace_metrics_routed(
        &parsed,
        &MetricsParams {
            start_ns: 1_791_400_000_000_000_000,
            end_ns: 1_791_410_800_000_000_000,
            step_ms: 60_000,
            exemplars: None,
            floor_ns: i64::MIN,
        },
        &MetricsCtx {
            filter: SpanFilterCtx {
                spans_table: "trace_spans",
                attrs_table: "trace_attrs_idx",
            },
            scan_budget_rows: 50_000_000,
            max_series: 1_000,
            distributed: false,
            skip_unavailable_shards: false,
        },
        &StatementTables {
            spans: "spans",
            error_spans: "error_spans",
            resources: "resources",
            traces: "traces",
            indexed: &[],
        },
    );
    // A refusal is rendered rather than panicked on, so a case that does
    // not plan fails at the comparison below, naming itself.
    match routed.as_ref().map(|r| r.statement()) {
        Ok(Some(s)) => format!(
            "-- case: {stem}\n-- q: {query}\n\n== range ==\n{}\n\n== instant ==\n{}\n",
            s.range_sql(),
            s.instant_sql()
        ),
        Ok(None) => format!("-- case: {stem}\n-- q: {query}\n-- no statement\n"),
        Err(e) => format!("-- case: {stem}\n-- q: {query}\n-- no statement: refused: {e}\n"),
    }
}

/// The twenty-three goldens, byte for byte, collected and asserted once.
#[test]
fn spans_metrics_goldens_match() {
    let mut missing: Vec<String> = Vec::new();
    let mut drifted: Vec<String> = Vec::new();
    for (stem, query) in CASES {
        let path = golden_dir().join(format!("{stem}.sql"));
        match std::fs::read_to_string(&path) {
            Err(e) => missing.push(format!("{stem} ({path:?}: {e})")),
            Ok(expected) => {
                if golden(stem, query) != expected {
                    drifted.push(format!("{stem} ({path:?})"));
                }
            }
        }
    }
    assert!(
        missing.is_empty() && drifted.is_empty(),
        "{} metrics golden(s) missing:\n{}\n{} drifted:\n{}\nif the change is intentional, run \
         `cargo test -p pulsus-read --test traces_spans_metrics_sql -- --ignored \
         regenerate_spans_metrics_goldens` and review the diff",
        missing.len(),
        missing.join("\n"),
        drifted.len(),
        drifted.join("\n")
    );
}

/// Every case is served by the statement.
#[test]
fn every_case_has_a_statement() {
    for (stem, query) in CASES {
        assert!(
            !golden(stem, query).contains("-- no statement"),
            "{stem}: {query} has no statement"
        );
    }
}

/// T3 (issue #601 part 4c): **a resource key reads its label once per
/// resource.** A `by()` on a resource attribute, scoped or unscoped, takes
/// its `(text, type)` label from the lookup, computed there per resource
/// row: neither the range nor the instant statement casts the looked-up
/// text back to a typed value per span.
#[test]
fn a_resource_key_reads_its_label_once_per_resource() {
    for query in [
        "{} | rate() by(resource.k8s.pod.name)",
        "{} | rate() by(.k8s.pod.name)",
    ] {
        let text = golden("t3", query);
        assert!(
            !text.contains("CAST(CAST(tupleElement(rv, 2)"),
            "{query}: the label is rebuilt per span: {text}"
        );
        let parts: Vec<&str> = text.split("\n== ").skip(1).collect();
        assert_eq!(parts.len(), 2, "{query}: a range and an instant statement");
        for (name, part) in parts.iter().map(|p| (&p[..7], p)) {
            for alias in ["AS rk_", "AS ri_"] {
                assert!(
                    part.contains(alias),
                    "{query}: the {name} statement lacks {alias:?}: {part}"
                );
            }
        }
    }
}

/// Regenerates the goldens. `#[ignore]`d: run explicitly after an
/// intentional change to the statement, review the diff, and say so.
#[test]
#[ignore = "regenerates the committed goldens; run explicitly, see doc comment"]
fn regenerate_spans_metrics_goldens() {
    let dir = golden_dir();
    std::fs::create_dir_all(&dir).expect("create golden dir");
    for (stem, query) in CASES {
        let path = dir.join(format!("{stem}.sql"));
        std::fs::write(&path, golden(stem, query))
            .unwrap_or_else(|e| panic!("write {path:?}: {e}"));
    }
}
