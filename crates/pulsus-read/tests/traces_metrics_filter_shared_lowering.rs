//! Issue #559: the metrics route's attribute condition and its aliases.
//! The metrics route and the previous search engine once rendered the
//! condition with one shared column; the checks comparing the two (4a,
//! over the 15 shapes of `CASES`, and 4b, the shared function's two
//! calling files) were deleted with that engine's tests, #602 part
//! 2c-2b-1. What remains is metrics-only.
//!
//! Hermetic — renders SQL and reads the tree, runs no query and needs no
//! container.
//!
//!   * the arithmetic refusal — the metrics route refuses an arithmetic
//!     comparison before any statement exists, with the text it rests on;
//!   * 4c, for the one comparison query it builds — the two filters
//!     declare two distinct aliases, spelled `pi0` and `cpi0`. Measured
//!     when written: the selection prefix hard-coded to `""`, or changed
//!     to `"x"`, each turns it red; a distinct prefix fails its literal
//!     `pi0`/`cpi0` assertion, not only its distinct-alias count;
//!   * no metrics filter statement reads the attribute index, over every
//!     shape the route builds.
//!
//! None of them sends a statement to a server. The route's ANSWERS are
//! pinned by `pulsus-server --test traces_search_live`'s
//! `a_metrics_attribute_condition_tests_the_element_the_span_resolves_to`.

use pulsus_read::traces::metrics_plan::{MetricsCtx, MetricsParams, plan_trace_metrics};
use pulsus_read::{SpanFilterCtx, TraceMetricsPlan};

const START_NS: i64 = 1_700_000_000_000_000_000;
const END_NS: i64 = 1_700_010_800_000_000_000;

fn filter_ctx() -> SpanFilterCtx<'static> {
    SpanFilterCtx {
        spans_table: "trace_spans",
        attrs_table: "trace_attrs_idx",
    }
}

fn metrics_plan(q: &str) -> TraceMetricsPlan {
    let query = pulsus_traceql::parse(q).unwrap_or_else(|e| panic!("{q} parses: {e:?}"));
    plan_trace_metrics(
        &query,
        &MetricsParams {
            start_ns: START_NS,
            end_ns: END_NS,
            step_ms: 60_000,
            exemplars: None,
            floor_ns: i64::MIN,
        },
        &MetricsCtx {
            filter: filter_ctx(),
            scan_budget_rows: 50_000_000,
            max_series: 1_000,
            distributed: false,
            skip_unavailable_shards: false,
        },
    )
    .unwrap_or_else(|e| panic!("{q} plans on the metrics route: {e:?}"))
}

/// The `<expr> AS <alias>` items of a statement's leading `WITH` clause.
///
/// The clause ends at the first line that starts a new statement keyword
/// at the statement's own indentation; every item this code renders is
/// `arrayFirstIndex(…) AS <alias>` on one line, so the items are the
/// lines of the clause.
fn with_items(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut in_clause = false;
    for line in sql.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("WITH ") {
            in_clause = true;
            out.push(rest.trim_end_matches(',').to_string());
            if !trimmed.ends_with(',') {
                break;
            }
            continue;
        }
        if in_clause {
            let more = trimmed.ends_with(',');
            out.push(trimmed.trim_end_matches(',').to_string());
            if !more {
                break;
            }
        }
    }
    out
}

/// The alias each `WITH` item declares — the text after the last ` AS `.
fn with_aliases(sql: &str) -> Vec<String> {
    with_items(sql)
        .iter()
        .map(|item| {
            item.rsplit(" AS ")
                .next()
                .unwrap_or_else(|| panic!("a WITH item must declare an alias: {item}"))
                .to_string()
        })
        .collect()
}

/// The sixth `ValuePred` variant: the metrics route refuses an
/// arithmetic comparison before any statement exists, and this test
/// checks that refusal and its text. (It also checked that the previous
/// search engine rendered the comparison; that half went with the
/// engine's tests, #602 part 2c-2b-1.)
#[test]
fn an_arithmetic_comparison_is_refused_by_metrics_and_is_not_claimed_to_render() {
    let q = "{ span.k * 2 >= 500 }";
    let query = pulsus_traceql::parse(&format!("{q} | rate()"))
        .unwrap_or_else(|e| panic!("{q} parses: {e:?}"));
    let err = plan_trace_metrics(
        &query,
        &MetricsParams {
            start_ns: START_NS,
            end_ns: END_NS,
            step_ms: 60_000,
            exemplars: None,
            floor_ns: i64::MIN,
        },
        &MetricsCtx {
            filter: filter_ctx(),
            scan_budget_rows: 50_000_000,
            max_series: 1_000,
            distributed: false,
            skip_unavailable_shards: false,
        },
    )
    .expect_err("the metrics route refuses an arithmetic comparison");
    assert_eq!(
        format!("{err:?}"),
        "TypeMismatch(\"field-vs-field and arithmetic comparisons are not supported in metrics \
         filters\")",
        "the refusal is the one this exclusion rests on"
    );
}

/// **4c. Two filters in one statement declare DISJOINT aliases.**
///
/// `compare()` is the one shape whose outer filter and selection
/// predicate render into a single `SELECT`, and each numbers its
/// attribute leaves from `0`. The outer filter compiles under the empty
/// prefix — which keeps every single-filter statement's aliases exactly
/// what issue #557 shipped — and the selection under `"c"`.
///
/// The query is chosen so BOTH filters produce aliases: the outer filter
/// is `{ span.env = "prod" }`, an attribute condition, which is not
/// hoistable to `PREWHERE`. With a service equality outside, the outer
/// side declares nothing and a collision cannot arise.
///
/// *RED when:* the two filters share a prefix. Measured by hard-coding
/// the selection prefix to `""`: this failed `left: 1, right: 2` on the
/// distinct-alias count, and the server answered the resulting statement
/// `Code: 179 … MULTIPLE_EXPRESSIONS_FOR_ALIAS` with HTTP 500 rather than
/// picking one silently.
#[test]
fn a_comparisons_two_filters_declare_disjoint_aliases_in_one_statement() {
    let plan =
        metrics_plan(r#"{ span.env = "prod" } | compare({ span.http.status_code = "500" })"#);
    let (cross_tab, _) = plan
        .compare_range()
        .expect("a comparison plan renders a cross-tab");
    let aliases = with_aliases(cross_tab);
    assert_eq!(
        aliases.len(),
        2,
        "one locator per filter, both declared on the statement that reads the span table: \
         {aliases:?}\n{cross_tab}"
    );
    let distinct: std::collections::BTreeSet<&String> = aliases.iter().collect();
    assert_eq!(
        distinct.len(),
        aliases.len(),
        "the two filters' aliases must be disjoint — a shared name is Code: 179 \
         MULTIPLE_EXPRESSIONS_FOR_ALIAS from the server, not a silent pick: {aliases:?}"
    );
    // Which prefix each side takes, pinned: the outer filter keeps the
    // empty one so a single-filter statement's bytes do not move.
    assert!(
        aliases.contains(&"pi0".to_string()) && aliases.contains(&"cpi0".to_string()),
        "the outer filter takes the empty prefix and the selection takes `c`: {aliases:?}"
    );
}

/// **Criterion 3's other half: a metrics statement names the attribute
/// index nowhere.**
///
/// The 13 goldens whose filter carries an attribute condition are frozen
/// byte for byte elsewhere. This says the property those bytes have, over
/// every shape the route builds, so a shape with no golden cannot quietly
/// keep a semi-join.
///
/// `compare()` is the exception and is stated rather than skipped: its
/// attribute ENUMERATION still joins `trace_attrs_idx`, which is how it
/// discovers which attributes the matched spans carry. That join is out
/// of this issue's scope. Its FILTER and its SELECTION are checked here
/// like every other shape.
#[test]
fn no_metrics_filter_statement_reads_the_attribute_index() {
    const SHAPES: [&str; 8] = [
        "{ span.k >= 500 } | rate()",
        "{ span.k >= 500 } | count_over_time()",
        "{ span.k >= 500 } | sum_over_time(duration)",
        "{ span.k >= 500 } | sum_over_time(duration) by(resource.service.name)",
        "{ span.k >= 500 } | quantile_over_time(duration, 0.5, 0.9)",
        "{ span.k >= 500 } | histogram_over_time(duration)",
        "{ span.k >= 500 } | rate() with(exemplars=3)",
        r#"{ .k != "x" } | rate()"#,
    ];
    for q in SHAPES {
        let plan = metrics_plan(q);
        for (what, sql) in [
            ("range", plan.range_sql().to_string()),
            ("instant", plan.instant_sql().to_string()),
        ] {
            assert!(
                !sql.contains("trace_attrs_idx"),
                "{q}: the {what} statement reads the attribute index:\n{sql}"
            );
            assert!(
                !sql.contains("IN (SELECT"),
                "{q}: the {what} statement carries a semi-join:\n{sql}"
            );
        }
        for (what, sql) in [
            ("range probe", plan.range_probe_sql()),
            ("instant probe", plan.instant_probe_sql()),
            ("exemplars", plan.exemplar_sql()),
        ] {
            let Some(sql) = sql else { continue };
            assert!(
                !sql.contains("trace_attrs_idx"),
                "{q}: the {what} statement reads the attribute index:\n{sql}"
            );
        }
    }

    // The comparison shape: its FILTER and SELECTION are on the span row,
    // and the only `trace_attrs_idx` read left in its cross-tab is the
    // attribute enumeration's `INNER JOIN`.
    let plan =
        metrics_plan(r#"{ span.env = "prod" } | compare({ span.http.status_code = "500" })"#);
    let (cross_tab, totals) = plan.compare_range().expect("a comparison plan");
    assert!(
        !totals.contains("trace_attrs_idx"),
        "the comparison totals read the span table alone:\n{totals}"
    );
    assert_eq!(
        cross_tab.matches("trace_attrs_idx").count(),
        1,
        "the ONE remaining read of the attribute index on this route is the attribute \
         enumeration's join, which issue #559 does not touch:\n{cross_tab}"
    );
    assert!(
        cross_tab.contains(
            "INNER JOIN (\n    SELECT DISTINCT trace_id, span_id, scope, key, val \
                            FROM trace_attrs_idx"
        ),
        "and it is that join, not a filter semi-join:\n{cross_tab}"
    );
}
