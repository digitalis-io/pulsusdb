//! Issue #559: the metrics route and the search route answer an
//! attribute condition with **one** rendered column, built by **one**
//! function, and its aliases are namespaced so two filters can share a
//! statement.
//!
//! Hermetic — renders SQL and reads the tree, runs no query and needs no
//! container.
//!
//! Three checks, because each mutation defeats the other two. Measured on
//! this branch, each break run on its own:
//!
//! ```text
//! mutation                                   4a     4b     4c
//! a private any-element copy in metrics_sql  RED    green  green
//! an identical private copy in metrics_sql   green  RED    green
//! the selection prefix hard-coded to ""      green  green  RED
//! ```
//!
//! **What none of the three catches:** a prefix changed to another
//! DISTINCT value — `"x"` where the code says `"c"`. That renames the
//! aliases without colliding them, so the statement is still valid, the
//! server still answers, and nothing here sees it. Said plainly rather
//! than attributed to a check that does not cover it.

use pulsus_read::traces::metrics_plan::{MetricsCtx, MetricsParams, plan_trace_metrics};
use pulsus_read::traces::search_plan::{SearchCtx, SearchParams, SearchPlan, plan_search};
use pulsus_read::{SpanFilterCtx, TraceMetricsPlan};

const START_NS: i64 = 1_700_000_000_000_000_000;
const END_NS: i64 = 1_700_010_800_000_000_000;

fn filter_ctx() -> SpanFilterCtx<'static> {
    SpanFilterCtx {
        spans_table: "trace_spans",
        attrs_table: "trace_attrs_idx",
    }
}

fn plan_for_search(q: &str) -> SearchPlan {
    let query = pulsus_traceql::parse(q).unwrap_or_else(|e| panic!("{q} parses: {e:?}"));
    plan_search(
        &query,
        &SearchParams {
            start_ns: START_NS,
            end_ns: END_NS,
            limit: 20,
            spss: 3,
        },
        &SearchCtx {
            filter: filter_ctx(),
            max_candidates: 100_000,
            max_series: 1_000,
            distributed: false,
        },
    )
    .unwrap_or_else(|e| panic!("{q} plans on the search route: {e:?}"))
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

/// The single element of the search hydration statement's `attr_slot`
/// array, and the statement's `WITH` items.
///
/// Anchored on `] AS attr_slot`, which is the array's own closing
/// bracket, and walked back with the brackets BALANCED. A plain
/// `rfind('[')` finds the wrong one: every element subscripts an array,
/// so `attr_val[pi0] = 'x'` contains a `[` of its own and the naive walk
/// returns the fragment `pi0] = 'x'` — which the metrics statement also
/// contains, so the check passes while testing a suffix instead of the
/// column. Found by the arithmetic case, whose element is
/// `ifNull((attr_num[pi0] * 2) >= 500, 0)`.
///
/// Every query below plans exactly ONE condition, so the array has
/// exactly one element and nothing has to split on commas.
fn search_slot_and_with(q: &str) -> (String, Vec<String>) {
    let sql = plan_for_search(q).hydration_sql_for(&[[7u8; 16]]);
    let end = sql
        .find("] AS attr_slot")
        .unwrap_or_else(|| panic!("{q}: no attr_slot array in the hydration statement:\n{sql}"));
    let bytes = sql.as_bytes();
    let mut depth = 0usize;
    let mut open = None;
    for i in (0..end).rev() {
        match bytes[i] {
            b']' => depth += 1,
            b'[' => {
                if depth == 0 {
                    open = Some(i);
                    break;
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    let open = open.unwrap_or_else(|| panic!("{q}: unbalanced attr_slot array:\n{sql}"));
    let element = sql[open + 1..end].to_string();
    assert!(
        element.matches('[').count() == element.matches(']').count(),
        "{q}: the extracted element must be bracket-balanced, got {element:?}"
    );
    (element, with_items(&sql))
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

/// The 15 route-reachable combinations: three scope ARITIES by the five
/// `ValuePred` variants the metrics route can produce.
///
/// `ValuePred::NumExpr` — the arithmetic comparison — is the sixth
/// variant and is **excluded on purpose**: the metrics route rejects it
/// before any statement exists, with
/// `TypeMismatch("field-vs-field and arithmetic comparisons are not
/// supported in metrics filters")`. It is covered separately by
/// [`an_arithmetic_comparison_is_refused_by_metrics_and_is_not_claimed_to_render`].
const CASES: [(&str, &str); 15] = [
    // single-valued scope
    ("single/StringEq", r#"{ span.k = "x" }"#),
    ("single/BoolEq", "{ span.k = true }"),
    ("single/Regex", r#"{ span.k =~ "a.*" }"#),
    ("single/Num", "{ span.k >= 500 }"),
    ("single/KeyExists", "{ span.k != nil }"),
    // multi-valued scope
    ("multi/StringEq", r#"{ event.k = "x" }"#),
    ("multi/BoolEq", "{ event.k = true }"),
    ("multi/Regex", r#"{ event.k =~ "a.*" }"#),
    ("multi/Num", "{ event.k >= 500 }"),
    ("multi/KeyExists", "{ event.k != nil }"),
    // the unscoped five-scope chain
    ("unscoped/StringEq", r#"{ .k = "x" }"#),
    ("unscoped/BoolEq", "{ .k = true }"),
    ("unscoped/Regex", r#"{ .k =~ "a.*" }"#),
    ("unscoped/Num", "{ .k >= 500 }"),
    ("unscoped/KeyExists", "{ .k != nil }"),
];

/// **4a. The metrics statement carries the search statement's own
/// attribute column, byte for byte.**
///
/// Not "both call the same function" — that is what 4b checks. This one
/// takes the text the SEARCH route renders into its hydration statement's
/// `attr_slot` array and asserts the METRICS statement contains it, so a
/// second implementation that happens to be called from one place still
/// fails here if it renders anything else.
///
/// *RED when:* the metrics route renders a different column. Measured
/// with a private any-element copy in `metrics_sql`: it failed on the
/// first probe, printing the search element
/// `(pi0 != 0) AND attr_val[pi0] = 'x'` against the metrics text
/// `arrayExists((k, s, v) -> k = 'k' AND s = 'span' AND v = 'x', attr_key, attr_scope, attr_val)`.
#[test]
fn the_metrics_statement_carries_the_search_statements_own_attribute_column() {
    for (label, q) in CASES {
        let (element, search_with) = search_slot_and_with(q);
        let metrics = metrics_plan(&format!("{q} | rate()"))
            .range_sql()
            .to_string();
        assert!(
            metrics.contains(&element),
            "{label}: the metrics statement does not carry the search route's attribute \
             column for {q}\n  SEARCH ELEMENT: {element}\n  METRICS: {metrics}"
        );
        assert!(
            !search_with.is_empty(),
            "{label}: the search statement must declare at least one locator for {q}"
        );
        for item in &search_with {
            assert!(
                metrics.contains(item),
                "{label}: the metrics statement does not declare the search route's locator \
                 for {q}\n  SEARCH WITH: {item}\n  METRICS: {metrics}"
            );
        }
        assert_eq!(
            with_items(&metrics),
            search_with,
            "{label}: the two routes must declare the SAME locators, in the same order, for {q}"
        );
    }
}

/// The sixth `ValuePred` variant, covered where it actually lives.
///
/// The metrics route refuses an arithmetic comparison before any
/// statement exists, so there is no metrics statement to compare against
/// and this test does not pretend there is. What it checks is that the
/// SHARED helper renders it — so the variant is not silently unreachable
/// in the one place both routes go through.
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
    // The search route does render it, through the same helper the
    // metrics route would have used.
    let (element, with) = search_slot_and_with(q);
    assert!(
        element.contains("attr_num[pi0]"),
        "the arithmetic comparison reads the located element's number: {element}"
    );
    assert_eq!(with.len(), 1, "one locator: {with:?}");
}

/// **4b. There is ONE implementation.**
///
/// `filter::probe_column` has exactly two non-test callers — the search
/// route's slot assembly and the metrics route's leaf sink. A second copy
/// of the rendering, even a byte-identical one, is a second place for the
/// two routes to drift, and 4a cannot see it while the copy agrees.
///
/// The search runs from the REPOSITORY ROOT over the tracked file list,
/// refuses an empty list and reads standard input from `/dev/null`, so it
/// cannot silently answer over a subdirectory or hang.
///
/// *RED when:* a caller is added or removed. Measured with an identical
/// private copy in `metrics_sql`: 4a stayed green and this failed
/// `left: 1, right: 2`.
#[test]
fn the_span_row_attribute_column_has_exactly_two_non_test_callers() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root");
    let tracked = std::process::Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["ls-files", "--", "*.rs"])
        .stdin(std::process::Stdio::null())
        .output()
        .expect("git ls-files");
    assert!(tracked.status.success(), "git ls-files failed");
    let files: Vec<&str> = std::str::from_utf8(&tracked.stdout)
        .expect("utf-8 file list")
        .lines()
        .filter(|l| !l.is_empty())
        .collect();
    assert!(
        files.len() > 100,
        "the tracked-file list is {} entries, which is not this repository — a search over an \
         empty or partial list reports green and means nothing",
        files.len()
    );

    let mut callers: Vec<String> = Vec::new();
    for rel in &files {
        let text = std::fs::read_to_string(root.join(rel)).unwrap_or_else(|e| {
            panic!("read {rel}: {e}");
        });
        if text.contains("filter::probe_column(") {
            callers.push((*rel).to_string());
        }
    }
    callers.sort();
    assert_eq!(
        callers,
        vec![
            "crates/pulsus-read/src/traces/metrics_sql.rs".to_string(),
            "crates/pulsus-read/src/traces/search_plan.rs".to_string(),
        ],
        "filter::probe_column must have exactly these two calling files"
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
