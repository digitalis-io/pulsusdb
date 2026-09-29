//! Issue #583 (R9), case `T-B5`: **one window rule across the five reads
//! a client can ask for**, asserted on the emitted statement text.
//!
//! Hermetic — compiling only, no ClickHouse. One window is compiled five
//! ways: a search, a trace fetch, a tag-value read, a metrics range query
//! and a service-graph request. What the case asserts:
//!
//! * every `timestamp_ns` bound anywhere in the five reads uses the
//!   half-open operators, `>=` on the low side and `<` on the high side —
//!   against the **shipped** column name at this point; task 8 re-asserts
//!   it against `start_ns`;
//! * the two reads whose bound IS the request's window — the search's
//!   phase-2 hydration and both halves of the service graph — carry the
//!   request's own literals, `timestamp_ns >= <s> AND timestamp_ns < <e>`;
//! * the recency candidate bound is `ts_max >= <s> AND ts_min <= <e - 1>`,
//!   derived from the same convention rather than written out;
//! * the trace fetch and the winners' root hydration carry **no**
//!   `timestamp_ns` bound at all — both are `trace_id` point reads
//!   (`traces/sql.rs`, `search_sql::root_sql`);
//! * the tag-value read carries **no** `timestamp_ns` bound either, and
//!   its day clause is the day span the request touches. That is deferred
//!   work owned by issue #598, not an oversight: the read is day-granular
//!   today and #598 gives it a row bound against the statement its cost is
//!   measured on;
//! * the graph and metrics halves hold **before** the change and must hold
//!   after — the regression half. Both already use these operators
//!   (`graph_sql.rs`, `metrics_sql.rs`), so the case fails if either is
//!   "tidied" into the other convention.
//!
//! **Two windows are deliberately outside this case.** The per-step range
//! selector inside a metrics query keeps the right-closed instants the
//! query language defines for it, rendered `[aS − step + 1, aE + 1)`
//! (`metrics_plan.rs`) — so the metrics statement's bound is checked for
//! its operators and NOT for the request's literals, which are not what it
//! selects. And `compare()`'s `start`/`end` arguments keep the reference's
//! `(start, end]`. Neither is a request window. The metrics query compiled
//! here is a plain `rate()`, so no selection window is rendered at all and
//! the second exclusion is structural rather than a carve-out.

use pulsus_read::traces::metrics_plan::{MetricsCtx, MetricsParams, plan_trace_metrics};
use pulsus_read::traces::search_plan::{SearchCtx, SearchParams, plan_search};
use pulsus_read::traces::tags_sql::{DaySpan, span_name_values_sql};
use pulsus_read::traces::{GraphWindow, SERVICE_GRAPH_MAX_EDGES, service_graph_sql};
use pulsus_read::{SpanFilterCtx, TAG_VALUES_MAX};

/// The one request window every read below is compiled over:
/// 2023-11-14T22:13:20Z to +3h, the shape the search and metrics golden
/// suites already use. It crosses a UTC midnight, which is what gives the
/// tag read's day clause two days to name.
const START_NS: i64 = 1_700_000_000_000_000_000;
const END_NS: i64 = 1_700_010_800_000_000_000;

/// 400 s divides both bounds exactly, so the metrics planner's outward
/// epoch snap is the identity here and the emitted literals are the
/// request's own.
const STEP_MS: i64 = 400_000;

const SPANS_TABLE: &str = "trace_spans";
const ATTRS_TABLE: &str = "trace_attrs_idx";

const BATCH: [[u8; 16]; 1] = [[
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
]];

fn filter_ctx() -> SpanFilterCtx<'static> {
    SpanFilterCtx {
        spans_table: SPANS_TABLE,
        attrs_table: ATTRS_TABLE,
    }
}

/// The three statements the empty-selector search issues over the window,
/// in a fixed order: `0` the phase-1 generator (the recency read), `1` the
/// phase-2 hydration, `2` the winners' root hydration.
fn search_statements() -> Vec<(&'static str, String)> {
    let query = pulsus_traceql::parse("{}").expect("the empty selector parses");
    let plan = plan_search(
        &query,
        &SearchParams {
            start_ns: START_NS,
            end_ns: END_NS,
            limit: 20,
            spss: 3,
        },
        &SearchCtx {
            filter: filter_ctx(),
            recent_table: "trace_recent",
            errors_table: "trace_error_spans",
            max_candidates: 100_000,
            max_series: 1_000,
            distributed: false,
        },
    )
    .expect("the empty selector plans");
    let mut out = vec![("search phase-1 generator", plan.generator_sqls[0].clone())];
    out.push(("search phase-2 hydration", plan.hydration_sql_for(&BATCH)));
    out.push(("search root hydration", plan.root_sql_for(&BATCH)));
    out
}

fn metrics_statement() -> String {
    let query = pulsus_traceql::parse("{} | rate()").expect("the metrics query parses");
    let plan = plan_trace_metrics(
        &query,
        &MetricsParams {
            start_ns: START_NS,
            end_ns: END_NS,
            step_ms: STEP_MS,
            exemplars: None,
        },
        &MetricsCtx {
            filter: filter_ctx(),
            scan_budget_rows: 1_000_000_000,
            max_series: 1_000,
            distributed: false,
            skip_unavailable_shards: false,
        },
    )
    .expect("the metrics query plans");
    plan.range_sql().to_string()
}

fn graph_statement() -> String {
    service_graph_sql(
        GraphWindow {
            start_ns: START_NS,
            end_ns: END_NS,
        },
        "trace_edges",
        SERVICE_GRAPH_MAX_EDGES,
    )
}

fn fetch_statement() -> String {
    pulsus_read::traces::sql::point_read_sql(SPANS_TABLE, "000102030405060708090a0b0c0d0e0f")
}

fn tag_values_statement() -> String {
    span_name_values_sql(
        filter_ctx(),
        DaySpan::from_window(START_NS, END_NS),
        &[],
        TAG_VALUES_MAX + 1,
    )
}

/// Every `timestamp_ns` comparison in `sql`, as the operator that follows
/// the column name — so a bound written with an unexpected operator is
/// reported rather than merely missing from a `contains` check.
fn timestamp_operators(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let needle = "timestamp_ns ";
    let mut rest = sql;
    while let Some(at) = rest.find(needle) {
        let after = &rest[at + needle.len()..];
        let op: String = after
            .chars()
            .take_while(|c| matches!(c, '<' | '>' | '=' | '!'))
            .collect();
        if !op.is_empty() {
            out.push(op);
        }
        rest = after;
    }
    out
}

/// `T-B5`: the five reads, one window, one rule.
#[test]
fn every_request_window_renders_the_one_half_open_bound() {
    let row_bound = format!("timestamp_ns >= {START_NS} AND timestamp_ns < {END_NS}");
    let recency_bound = format!("ts_max >= {START_NS} AND ts_min <= {}", END_NS - 1);
    let mut wrong: Vec<String> = Vec::new();

    let search = search_statements();
    let metrics = ("metrics range query", metrics_statement());
    let graph = ("service graph", graph_statement());

    // -- every read: the operators, wherever a `timestamp_ns` bound
    //    appears. The metrics statement is here and not below because its
    //    literals are the range selector's instants, which the query
    //    language owns (module doc).
    for (label, sql) in search.iter().chain([&metrics, &graph]) {
        let unexpected: Vec<String> = timestamp_operators(sql)
            .into_iter()
            .filter(|op| op != ">=" && op != "<")
            .collect();
        if !unexpected.is_empty() {
            wrong.push(format!(
                "{label}: every timestamp_ns bound must be >= or <, found {unexpected:?}:\n{sql}"
            ));
        }
    }

    // -- the two reads whose bound IS the request's window: its literals.
    for (label, sql) in [&search[1], &graph] {
        if !sql.contains(&row_bound) {
            wrong.push(format!("{label}: expected `{row_bound}`:\n{sql}"));
        }
    }

    // -- the recency candidate bound, derived from the same convention:
    //    the first included nanosecond and the last.
    let (generator_label, generator) = &search[0];
    if !generator.contains(&recency_bound) {
        wrong.push(format!(
            "{generator_label}: expected `{recency_bound}` — the recency row bound is rendered \
             from the window's first and last included nanosecond, not written out with its \
             own operators:\n{generator}"
        ));
    }

    // -- the two point reads: no window at all, and neither gains one.
    for (label, sql) in [("trace fetch", fetch_statement()), search[2].clone()] {
        if sql.contains("timestamp_ns >") || sql.contains("timestamp_ns <") {
            wrong.push(format!(
                "{label}: a `trace_id` point read carries no time bound and gains none:\n{sql}"
            ));
        }
    }

    // -- the tag-value read: no row bound, and the day span the request
    //    touches. Deferred to issue #598, which gives it the row bound
    //    against the statement whose cost is measured.
    let tag_values = tag_values_statement();
    if tag_values.contains("timestamp_ns >") || tag_values.contains("timestamp_ns <") {
        wrong.push(format!(
            "tag-value read: the store-backed read is day-granular until issue #598; a row \
             bound here is that task's, and it moves this case rather than editing it:\n\
             {tag_values}"
        ));
    }
    let day_span = "toDate(fromUnixTimestamp64Nano(timestamp_ns)) >= toDate('2023-11-14') \
                    AND toDate(fromUnixTimestamp64Nano(timestamp_ns)) <= toDate('2023-11-15')";
    if !tag_values.contains(day_span) {
        wrong.push(format!(
            "tag-value read: expected the day span the window touches, `{day_span}`:\n\
             {tag_values}"
        ));
    }

    assert!(
        wrong.is_empty(),
        "{} of the five reads render a window this rule does not allow:\n{}",
        wrong.len(),
        wrong.join("\n\n")
    );
}
