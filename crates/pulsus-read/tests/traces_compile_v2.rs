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
    pulsus_read::traces::spans::fetch::indexed_fetch_sql(
        "traces",
        "spans",
        "resources",
        "000102030405060708090a0b0c0d0e0f",
    )
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

// =====================================================================
// Issue #588 — the span-scope predicate compiler, text only
// =====================================================================
//
// Hermetic. What a leaf and a membership statement RENDER; the answers
// those renderings give live in `tests/traces_query_v2_live.rs`, and the
// two halves meet at `T-C4`, which freezes the exact statement that suite
// issues.
//
// `T-C7` is not a case here: it is the existing `golden_sql_freeze` suite,
// run unchanged. `PINNED_SQL_CORPUS` must not move — this change adds a
// `WindowSql` method and edits no existing method body, so `search_sql`
// and `metrics_sql` cannot render different text.

use pulsus_read::traces::PlanError;
use pulsus_read::traces::spans::predicate::{
    PredicateCtx, compile_span_leaf, compile_span_leaf_in, compile_span_predicate,
    compile_span_predicate_in, span_membership_sql,
};
use pulsus_read::traces::window_sql::WindowSql;
use pulsus_traceql::{
    AttrScope, ComparisonOp, Field, FieldExpr, Intrinsic, SpansetExpr, SpansetFilter, Value,
};

/// The `T-B1` window: `[start, end)` one nanosecond wide, so all three
/// clauses are rendered from one instant and the day and bucket bounds are
/// each a single value.
const T_B1_START: i64 = 1_790_094_846_486_853_636;
const T_B1_END: i64 = 1_790_094_846_486_853_637;

fn t_b1_window() -> WindowSql {
    WindowSql::start_closed_end_open(T_B1_START, T_B1_END)
}

/// The filter body of a one-spanset query.
fn filter_body(query: &str) -> FieldExpr {
    let parsed =
        pulsus_traceql::parse(query).unwrap_or_else(|e| panic!("{query} must parse: {e:?}"));
    match parsed.spanset {
        SpansetExpr::Filter(SpansetFilter { body: Some(b) }) => b,
        other => panic!("{query}: expected one filter with a body, got {other}"),
    }
}

/// The predicate text `query` compiles to.
fn rendered(query: &str) -> String {
    compile_span_predicate(&filter_body(query))
        .unwrap_or_else(|e| panic!("{query} must compile: {e}"))
        .sql()
        .to_string()
}

/// The error `query` is refused with.
fn refusal(query: &str) -> PlanError {
    match compile_span_predicate(&filter_body(query)) {
        Err(e) => e,
        Ok(p) => panic!("{query} must be refused, it compiled to `{}`", p.sql()),
    }
}

fn attr(key: &str) -> Field {
    Field::Attribute {
        scope: AttrScope::Span,
        key: key.to_string(),
    }
}

/// A `Value::Duration`, taken from a parsed query — `Duration::from_nanos`
/// is crate-private, so the parser is the only constructor a test has.
fn duration_value(query: &str) -> Value {
    match filter_body(query) {
        FieldExpr::Binary { rhs, .. } => match *rhs {
            FieldExpr::Literal(v @ Value::Duration(_)) => v,
            other => panic!("{query}: expected a duration literal, got {other}"),
        },
        other => panic!("{query}: expected a comparison, got {other}"),
    }
}

fn leaf_text(field: &Field, op: ComparisonOp, value: &Value) -> String {
    compile_span_leaf(field, op, value)
        .unwrap_or_else(|e| panic!("{field} {op} {value} must compile: {e}"))
        .sql()
        .to_string()
}

// ---------------------------------------------------------------------
// the operator set, for the two generated matrices
// ---------------------------------------------------------------------

/// Every `ComparisonOp` the language has. **The compile gate is
/// [`op_is_ordered`]**, whose `match` has no wildcard arm: a ninth variant
/// fails to compile until it is classified. This array is the iteration
/// order and `the_four_ordered_operators_are_a_complement` checks the two
/// classes partition it.
const ALL_OPS: [ComparisonOp; 8] = [
    ComparisonOp::Eq,
    ComparisonOp::Neq,
    ComparisonOp::Gt,
    ComparisonOp::Gte,
    ComparisonOp::Lt,
    ComparisonOp::Lte,
    ComparisonOp::Re,
    ComparisonOp::Nre,
];

/// **No wildcard arm**: a ninth `ComparisonOp` variant fails to compile
/// here rather than being skipped by `T-C6b`.
fn op_is_ordered(op: ComparisonOp) -> bool {
    match op {
        ComparisonOp::Gt | ComparisonOp::Gte | ComparisonOp::Lt | ComparisonOp::Lte => true,
        ComparisonOp::Eq | ComparisonOp::Neq | ComparisonOp::Re | ComparisonOp::Nre => false,
    }
}

/// The four ordered operators, as the COMPLEMENT of the four that are not
/// — `pulsus_traceql` offers no constant for it, and a list written out
/// here is how an earlier draft of this case used `>` twice and `<` not at
/// all.
fn ordered_ops() -> Vec<ComparisonOp> {
    ALL_OPS
        .into_iter()
        .filter(|op| op_is_ordered(*op))
        .collect()
}

#[test]
fn the_four_ordered_operators_are_a_complement() {
    for (i, a) in ALL_OPS.iter().enumerate() {
        for b in &ALL_OPS[i + 1..] {
            assert_ne!(a, b, "ALL_OPS lists an operator twice");
        }
    }
    let not_ordered: Vec<ComparisonOp> = ALL_OPS
        .into_iter()
        .filter(|op| !op_is_ordered(*op))
        .collect();
    assert_eq!(
        not_ordered,
        vec![
            ComparisonOp::Eq,
            ComparisonOp::Neq,
            ComparisonOp::Re,
            ComparisonOp::Nre
        ]
    );
    assert_eq!(ordered_ops().len() + not_ordered.len(), ALL_OPS.len());
    assert_eq!(ordered_ops().len(), 4);
}

// ---------------------------------------------------------------------
// T-C1 — the static keywords' digits, which do not go through `f64`
// ---------------------------------------------------------------------

/// `T-C1`: `maxInt` and `minInt` arrive as ordinary decimal literals and
/// must render as their own digits. `render_num(parse_num("9223372036854775807"))`
/// is `9223372036854776000` — one more than `2^63 - 1`, which ClickHouse
/// types `UInt64` — so a rendering that goes through `f64` is visible in
/// the text even where no answer distinguishes it (`minInt`'s case).
#[test]
fn t_c1_the_static_integer_keywords_render_their_own_digits() {
    let max = rendered(r#"{ span.a < maxInt }"#);
    assert_eq!(
        max,
        "(coalesce(attrs.`a`.:Int64 < 9223372036854775807, false) OR \
         coalesce(attrs.`a`.:Float64 < 9223372036854775807, false))"
    );
    assert_eq!(
        max.matches("9223372036854775807").count(),
        2,
        "the digit string appears once per typed variant: {max}"
    );
    let min = rendered(r#"{ span.a = minInt }"#);
    assert_eq!(
        min,
        "(coalesce(attrs.`a`.:Int64 = -9223372036854775808, false) OR \
         coalesce(attrs.`a`.:Float64 = -9223372036854775808, false))"
    );
    assert_eq!(
        min.matches("-9223372036854775808").count(),
        2,
        "the digit string appears once per typed variant: {min}"
    );
}

// ---------------------------------------------------------------------
// T-C2 — the attribute path, twice escaped
// ---------------------------------------------------------------------

/// `T-C2`: the writer's own JSON-path escape, then the identifier quote.
/// Neither is optional and the order is not free — escaping `%` first is
/// what keeps the key spelled `a%2Eb` off the key `a.b`'s stored path.
#[test]
fn t_c2_the_attribute_path_carries_both_escapes() {
    let dotted = leaf_text(
        &attr("http.response.status_code"),
        ComparisonOp::Eq,
        &Value::Number("200".to_string()),
    );
    assert!(
        dotted.contains("attrs.`http%2Eresponse%2Estatus_code`"),
        "{dotted}"
    );

    // A client-chosen OTLP key becomes a SQL IDENTIFIER here for the first
    // time. Unescaped, `attrs.`a`b`` is `Code: 62 ... Back quoted string is
    // not closed`.
    let backtick = leaf_text(
        &attr("a`b"),
        ComparisonOp::Eq,
        &Value::Number("1".to_string()),
    );
    assert!(backtick.contains("attrs.`a\\`b`"), "{backtick}");

    let escaped_percent = leaf_text(
        &attr("a%2Eb"),
        ComparisonOp::Eq,
        &Value::Number("1".to_string()),
    );
    let dot_key = leaf_text(
        &attr("a.b"),
        ComparisonOp::Eq,
        &Value::Number("1".to_string()),
    );
    assert!(
        escaped_percent.contains("attrs.`a%252Eb`"),
        "{escaped_percent}"
    );
    assert!(dot_key.contains("attrs.`a%2Eb`"), "{dot_key}");
    assert_ne!(
        escaped_percent, dot_key,
        "the key `a%2Eb` and the key `a.b` must not share a stored path"
    );
}

// ---------------------------------------------------------------------
// T-C3 / T-C4 — the window, and the one place it composes with a predicate
// ---------------------------------------------------------------------

/// `T-C3`: the three clauses the span table's read carries, each rendered
/// from one `WindowSql`.
///
/// Two of them differ in shape from the issue's own text, and both
/// differences are decisions already taken: the bucket bound divides
/// **server-side** so the reader never reproduces `intDiv`'s rounding, and
/// the day clause carries the explicit `'UTC'` that makes it
/// byte-identical to `spans`' own `PARTITION BY` — without it the
/// expression is a different one and the partition prune is lost.
#[test]
fn t_c3_the_span_tables_three_window_clauses() {
    let w = t_b1_window();
    assert_eq!(
        w.span_time_clause(),
        "start_ns >= 1790094846486853636 AND start_ns < 1790094846486853637"
    );
    assert_eq!(
        w.span_bucket_clause(),
        "intDiv(start_ns, 300000000000) BETWEEN intDiv(1790094846486853636, 300000000000) AND \
         intDiv(1790094846486853636, 300000000000)"
    );
    assert_eq!(
        w.span_day_clause(),
        "toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2026-09-22') AND \
         toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2026-09-22')"
    );
}

/// `T-C4`: the whole membership statement, byte for byte — the ONE place a
/// window and a predicate compose, so the text frozen here is the text the
/// live suite ran.
#[test]
fn t_c4_the_membership_statement_is_frozen_whole() {
    let predicate =
        compile_span_predicate(&filter_body(r#"{ span.http.response.status_code != 200 }"#))
            .expect("T-A1 compiles");
    assert_eq!(
        span_membership_sql("spans", t_b1_window(), &predicate),
        "SELECT lower(hex(span_id)) AS id\n\
         FROM spans\n\
         WHERE start_ns >= 1790094846486853636 AND start_ns < 1790094846486853637\n\
         \x20 AND intDiv(start_ns, 300000000000) BETWEEN intDiv(1790094846486853636, 300000000000) \
         AND intDiv(1790094846486853636, 300000000000)\n\
         \x20 AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2026-09-22') AND \
         toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2026-09-22')\n\
         \x20 AND (NOT (coalesce(attrs.`http%2Eresponse%2Estatus_code`.:Int64 = 200, false) OR \
         coalesce(attrs.`http%2Eresponse%2Estatus_code`.:Float64 = 200, false)))\n\
         ORDER BY id"
    );
}

// ---------------------------------------------------------------------
// T-C5 — every out-of-scope construct refuses, naming itself
// ---------------------------------------------------------------------

/// The intrinsics part 1 serves. Every other `Intrinsic` variant is
/// #589's or later and must REFUSE rather than compile something wrong.
const IN_SCOPE_INTRINSICS: [Intrinsic; 10] = [
    Intrinsic::Name,
    Intrinsic::Duration,
    Intrinsic::Status,
    Intrinsic::Kind,
    Intrinsic::StatusMessage,
    Intrinsic::SpanId,
    Intrinsic::ParentId,
    Intrinsic::TraceId,
    Intrinsic::InstrumentationName,
    Intrinsic::InstrumentationVersion,
];

/// The issue each out-of-scope intrinsic is refused with, and `None` for
/// the ten this compiler serves. **No wildcard arm**: a new `Intrinsic`
/// variant fails to compile here until it is classified.
fn intrinsic_target(intrinsic: Intrinsic) -> Option<&'static str> {
    match intrinsic {
        Intrinsic::Name
        | Intrinsic::Duration
        | Intrinsic::Status
        | Intrinsic::Kind
        | Intrinsic::StatusMessage
        | Intrinsic::SpanId
        | Intrinsic::ParentId
        | Intrinsic::TraceId
        | Intrinsic::InstrumentationName
        | Intrinsic::InstrumentationVersion => None,
        Intrinsic::EventName
        | Intrinsic::EventTimeSinceStart
        | Intrinsic::LinkSpanId
        | Intrinsic::LinkTraceId => Some("#589 part 2"),
        Intrinsic::NestedSetParent
        | Intrinsic::NestedSetLeft
        | Intrinsic::NestedSetRight
        | Intrinsic::ChildCount
        | Intrinsic::TraceDuration
        | Intrinsic::RootName
        | Intrinsic::RootServiceName => Some("#594"),
    }
}

/// Section 2's refusal: a `resource.` field compiled with no context.
const RESOURCE_NEEDS_WINDOW: &str = "the \"resource.\" attribute scope needs the request window: \
                                     compile it with compile_span_predicate_in (issue #589)";

/// `T-C5`: driven from `Intrinsic::ALL` and `AttrScope::ALL`, which the
/// `enum_with_all!` macro generates from the same token list as the
/// variants — so a new variant fails this test rather than being skipped.
///
/// Each refusal's issue is asserted as the whole `(issue …)` suffix: a
/// bare `#589` would also match the wrong part.
#[test]
fn t_c5_every_out_of_scope_field_refuses_and_names_itself() {
    let mut out_of_scope = 0usize;
    for intrinsic in Intrinsic::ALL.iter().copied() {
        let got = compile_span_leaf(
            &Field::Intrinsic(intrinsic),
            ComparisonOp::Eq,
            &Value::String("x".to_string()),
        );
        let Some(target) = intrinsic_target(intrinsic) else {
            assert!(
                !matches!(got, Err(PlanError::UnsupportedField(_))),
                "{intrinsic} is served but refused as unsupported: {got:?}"
            );
            continue;
        };
        out_of_scope += 1;
        match got {
            Err(PlanError::UnsupportedField(msg)) => assert!(
                msg.contains(&intrinsic.to_string()) && msg.ends_with(&format!("(issue {target})")),
                "{intrinsic}: the refusal must name the construct and end `(issue {target})`, \
                 got {msg:?}"
            ),
            other => panic!("{intrinsic} must be UnsupportedField, got {other:?}"),
        }
    }
    assert_eq!(
        out_of_scope + IN_SCOPE_INTRINSICS.len(),
        Intrinsic::ALL.len(),
        "every intrinsic is either in scope or refused — a new variant belongs in one list"
    );
    for intrinsic in IN_SCOPE_INTRINSICS {
        assert_eq!(intrinsic_target(intrinsic), None, "{intrinsic}");
    }

    let mut refused_scopes = 0usize;
    for scope in AttrScope::ALL.iter().copied() {
        let got = compile_span_leaf(
            &Field::Attribute {
                scope,
                key: "k".to_string(),
            },
            ComparisonOp::Eq,
            &Value::String("x".to_string()),
        );
        match scope {
            AttrScope::Span | AttrScope::Instrumentation => {
                assert!(got.is_ok(), "{scope} compiles with no context: {got:?}");
            }
            AttrScope::Resource => {
                assert_eq!(
                    got.map(|p| p.sql().to_string()),
                    Err(PlanError::UnsupportedField(
                        RESOURCE_NEEDS_WINDOW.to_string()
                    )),
                    "resource. with no context"
                );
            }
            AttrScope::Unscoped | AttrScope::Event | AttrScope::Link => {
                refused_scopes += 1;
                match got {
                    Err(PlanError::UnsupportedField(msg)) => assert!(
                        msg.contains(&format!("\"{scope}\""))
                            && msg.ends_with("(issue #589 part 2)"),
                        "{scope}: the refusal must name the scope and `(issue #589 part 2)`, \
                         got {msg:?}"
                    ),
                    other => panic!("the {scope} scope must be UnsupportedField, got {other:?}"),
                }
            }
        }
    }
    assert_eq!(refused_scopes + 3, AttrScope::ALL.len());

    // The expression-level constructs section 7 defers to part 3, each
    // naming itself.
    for (query, token) in [
        (r#"{ span.a + 1 = 2 }"#, "arithmetic"),
        (r#"{ span.a = span.b }"#, "field-against-field"),
        (r#"{ 1 = 1 }"#, "two literals"),
        (r#"{ (span.a = 1) = true }"#, "boolean-valued"),
        (r#"{ !span.a = span.b }"#, "boolean-valued"),
    ] {
        match refusal(query) {
            PlanError::UnsupportedField(msg) => assert!(
                msg.contains(token) && msg.ends_with("(issue #589 part 3)"),
                "{query}: the refusal must name {token:?} and `(issue #589 part 3)`, got {msg:?}"
            ),
            other => panic!("{query} must be UnsupportedField, got {other:?}"),
        }
    }
    assert_eq!(
        compile_span_predicate(&FieldExpr::Literal(Value::Number("5".to_string())))
            .map(|p| p.sql().to_string()),
        Err(PlanError::TypeMismatch(
            "a non-boolean literal is not a predicate".to_string()
        )),
        "{{ 5 }}, which the validator rejects first and nothing will serve"
    );
}

// ---------------------------------------------------------------------
// T-C6 / T-C6b — the refusals that are kept
// ---------------------------------------------------------------------

/// `T-C6`: the fixed refusals, each message compared byte for byte against
/// `filter.rs`'s own text — so `{ span.k = error }` is the same `400` on
/// both compilers.
///
/// `{ duration =~ "x" }` takes the OPERAND message, not the operator one:
/// `filter.rs` checks the value before the operator and this compiler
/// keeps that order, which is what makes the two byte-identical. The
/// operator message is reachable only with a duration operand, asserted
/// below through the leaf.
#[test]
fn t_c6_the_fixed_refusals_keep_their_messages() {
    for (query, message) in [
        (
            r#"{ duration > 100 }"#,
            "duration requires a duration literal",
        ),
        (
            r#"{ duration =~ "x" }"#,
            "duration requires a duration literal",
        ),
        (r#"{ name = 5 }"#, "name requires a string value"),
        (r#"{ status > ok }"#, "status supports only = and !="),
        (r#"{ kind > internal }"#, "kind supports only = and !="),
        (
            r#"{ span.k > true }"#,
            "attribute \"k\" does not support operator > on this value type",
        ),
        (
            r#"{ span.k = error }"#,
            "attribute \"k\" does not support operator = on this value type",
        ),
    ] {
        assert_eq!(
            refusal(query),
            PlanError::TypeMismatch(message.to_string()),
            "{query}"
        );
    }
    assert_eq!(
        compile_span_leaf(
            &Field::Intrinsic(Intrinsic::Duration),
            ComparisonOp::Re,
            &duration_value(r#"{ duration > 1ns }"#),
        )
        .expect_err("a regex operator on duration is refused"),
        PlanError::TypeMismatch("duration does not support regex operators".to_string())
    );
}

/// `T-C6b`: the two carved-out fields' refusal matrix, as a CROSS PRODUCT
/// rather than a list of queries — `docs/api.md:1039` says of them that
/// "any ordered comparison at those fields stay `400`", and a list is how
/// an earlier draft used `>` twice and `<` not at all.
#[test]
fn t_c6b_the_carved_out_fields_refuse_every_ordered_comparison() {
    const CARVED_OUT: [Intrinsic; 2] = [
        Intrinsic::InstrumentationName,
        Intrinsic::InstrumentationVersion,
    ];
    let operands = [
        Value::String("j".to_string()),
        Value::Number("5".to_string()),
    ];
    let mut seen = 0usize;
    for field in CARVED_OUT {
        for op in ordered_ops() {
            for value in &operands {
                let err = compile_span_leaf(&Field::Intrinsic(field), op, value)
                    .expect_err("an ordered comparison at a carved-out field is a 400");
                assert_eq!(
                    err,
                    PlanError::TypeMismatch(format!("{field} supports only = != =~ !~")),
                    "{field} {op} {value:?}"
                );
                seen += 1;
            }
        }
    }
    assert_eq!(
        seen,
        CARVED_OUT.len() * ordered_ops().len() * operands.len()
    );
}

// ---------------------------------------------------------------------
// T-C8 — the shapes at those same fields that are NOT refusals
// ---------------------------------------------------------------------

/// `T-C8`: a cross-type `=`/`!=` at a carved-out field folds to the
/// literal `false`, and an ordered comparison on a STRING renders — it is
/// a reference `200` (`crates/pulsus-traceql/src/validate.rs:767-770`) and
/// our own validator accepts it, so the shipped compiler's `400` there was
/// strictness this pass must not inherit.
#[test]
fn t_c8_the_shapes_that_render_rather_than_refuse() {
    assert_eq!(rendered(r#"{ instrumentation:name = 5 }"#), "false");
    assert_eq!(rendered(r#"{ instrumentation:version != 5 }"#), "false");
    assert_eq!(rendered(r#"{ name > "a" }"#), "name > 'a'");
    assert_eq!(
        rendered(r#"{ span.k >= "a" }"#),
        "coalesce(attrs.`k`.:String >= 'a', false)"
    );
}

// ---------------------------------------------------------------------
// T-C9 / T-C10 — the three id columns
// ---------------------------------------------------------------------

/// `T-C9`: the literal is lowercased for `=`/`!=` and for nothing else,
/// and the raw-byte fast path carries the OPERATOR — an earlier draft
/// admitted `!=` to that path and then rendered `=`, which inverts all
/// three id columns' answers.
#[test]
fn t_c9_the_id_columns_fast_path_keeps_its_operator_and_lowercases_only_equality() {
    assert_eq!(
        rendered(r#"{ span:id = "0A1B2C3D4E5F60A1" }"#),
        "span_id = unhex('0a1b2c3d4e5f60a1')"
    );
    assert_eq!(
        rendered(r#"{ span:id != "0A1B2C3D4E5F60A1" }"#),
        "span_id != unhex('0a1b2c3d4e5f60a1')"
    );
    assert_eq!(
        rendered(r#"{ span:parentID != "0A1B2C3D4E5F60A1" }"#),
        "parent_span_id != unhex('0a1b2c3d4e5f60a1')"
    );
    assert_eq!(
        rendered(r#"{ trace:id != "0A1B2C3D4E5F60710A1B2C3D4E5F6071" }"#),
        "trace_id != toFixedString(unhex('0a1b2c3d4e5f60710a1b2c3d4e5f6071'), 16)"
    );
    // A regex may be deliberately case-sensitive, so the pattern's case
    // survives and the key read is NOT taken.
    assert_eq!(
        rendered(r#"{ span:id =~ "0A1B.*" }"#),
        "match(lower(hex(span_id)), '^(?:0A1B.*)$')"
    );
    // The width guard: `unhex('')` is zero bytes and a `FixedString`
    // right-pads, so dropping it makes `= ""` equal every root span.
    let empty = rendered(r#"{ span:parentID = "" }"#);
    assert_eq!(empty, "lower(hex(parent_span_id)) = ''");
    assert!(!empty.contains("unhex"), "{empty}");
}

/// `T-C10`: an ordered operator never takes the key read. No answer on
/// either fixture distinguishes `lower(hex(col)) <op> S` from a
/// `FixedString` comparison — hex encoding preserves order — so this text
/// is the only instrument.
#[test]
fn t_c10_an_ordered_id_comparison_is_lexicographic_on_hex_text() {
    let text = rendered(r#"{ span:id < "0a1b2c3d4e5f60a4" }"#);
    assert_eq!(text, "lower(hex(span_id)) < '0a1b2c3d4e5f60a4'");
    assert!(!text.contains("unhex"), "{text}");
    assert!(!text.contains("FixedString"), "{text}");
}

// ---------------------------------------------------------------------
// T-C11 — the literal on the left
// ---------------------------------------------------------------------

/// `T-C11`: a literal on the left mirrors the operator, so the two
/// spellings are one rendering.
#[test]
fn t_c11_a_literal_on_the_left_mirrors_the_operator() {
    assert_eq!(
        rendered(r#"{ 500 <= span.http.response.status_code }"#),
        rendered(r#"{ span.http.response.status_code >= 500 }"#)
    );
    assert_eq!(
        rendered(r#"{ 200 != span.http.response.status_code }"#),
        rendered(r#"{ span.http.response.status_code != 200 }"#)
    );
}

// =====================================================================
// Issue #589 part 1 — resource and instrumentation conditions, and the
// boolean operators, text only
// =====================================================================

/// `T-B1`'s window and the unqualified resource table: what every
/// resource leaf below is compiled against.
fn ctx() -> PredicateCtx<'static> {
    PredicateCtx {
        window: t_b1_window(),
        resources_table: "resources",
    }
}

/// `R(p)` — section 3.1's resource subquery, from `W`'s own day bound.
fn r_of(p: &str) -> String {
    format!(
        "resource_id IN (SELECT resource_id FROM resources WHERE {} AND ({p}))",
        t_b1_window().resources_day_clause()
    )
}

/// The predicate text `query` compiles to with [`ctx`].
fn rendered_in(query: &str) -> String {
    compile_span_predicate_in(&filter_body(query), &ctx())
        .unwrap_or_else(|e| panic!("{query} must compile in a context: {e}"))
        .sql()
        .to_string()
}

fn scoped(scope: AttrScope, key: &str) -> Field {
    Field::Attribute {
        scope,
        key: key.to_string(),
    }
}

/// The four operand kinds the cross products run every operator against.
fn cross_operands() -> [Value; 4] {
    [
        Value::String("v".to_string()),
        Value::Number("3".to_string()),
        duration_value(r#"{ duration > 1s }"#),
        Value::Bool(true),
    ]
}

/// One cross-product cell: the span leaf, and the same leaf at `scope`.
/// `None` is the truthiness cell, `{ <scope>k }`.
fn cell(
    scope: AttrScope,
    cell: Option<(ComparisonOp, &Value)>,
    with_ctx: bool,
) -> (Result<String, PlanError>, Result<String, PlanError>) {
    let text = |r: Result<pulsus_read::traces::spans::predicate::SpanPredicate, PlanError>| {
        r.map(|p| p.sql().to_string())
    };
    match cell {
        Some((op, value)) => {
            let span = text(compile_span_leaf(&attr("k"), op, value));
            let other = if with_ctx {
                text(compile_span_leaf_in(&scoped(scope, "k"), op, value, &ctx()))
            } else {
                text(compile_span_leaf(&scoped(scope, "k"), op, value))
            };
            (span, other)
        }
        None => {
            let span = text(compile_span_predicate(&FieldExpr::Field(attr("k"))));
            let expr = FieldExpr::Field(scoped(scope, "k"));
            let other = if with_ctx {
                text(compile_span_predicate_in(&expr, &ctx()))
            } else {
                text(compile_span_predicate(&expr))
            };
            (span, other)
        }
    }
}

/// Every cell of the cross product: `ALL_OPS` × [`cross_operands`], then
/// truthiness.
fn cross_cells() -> Vec<Option<(ComparisonOp, Value)>> {
    let mut out: Vec<Option<(ComparisonOp, Value)>> = Vec::new();
    for op in ALL_OPS {
        for value in cross_operands() {
            out.push(Some((op, value)));
        }
    }
    out.push(None);
    out
}

// ---------------------------------------------------------------------
// T-C12 — the resource cross product
// ---------------------------------------------------------------------

/// `T-C12`: where `span.k` renders `L`, `resource.k` renders `R(pos(L))`,
/// or `NOT (R(pos(L)))` when `L` is negated — the negation is OUTSIDE the
/// subquery. Where `span.k` is refused, `resource.k` is refused with the
/// same error.
#[test]
fn t_c12_a_resource_leaf_is_the_span_leaf_inside_the_subquery() {
    let mut seen = 0usize;
    for c in cross_cells() {
        let (span, resource) = cell(
            AttrScope::Resource,
            c.as_ref().map(|(op, v)| (*op, v)),
            true,
        );
        seen += 1;
        let want = match span {
            Ok(l) => Ok(match l.strip_prefix("NOT ") {
                Some(pos) => format!("NOT ({})", r_of(pos)),
                None => r_of(&l),
            }),
            Err(e) => Err(e),
        };
        assert_eq!(resource, want, "{c:?}");
    }
    assert_eq!(seen, ALL_OPS.len() * 4 + 1);

    assert_eq!(
        rendered_in(r#"{ resource.k != nil }"#),
        r_of("dynamicType(attrs.`k`) != 'None'")
    );
    assert_eq!(
        rendered_in(r#"{ resource.k = nil }"#),
        format!("NOT ({})", r_of("dynamicType(attrs.`k`) != 'None'"))
    );
}

// ---------------------------------------------------------------------
// T-C13 — the instrumentation cross product
// ---------------------------------------------------------------------

/// `T-C13`: `instrumentation.k` is `span.k` with the root `scope_attrs.`
/// in place of `attrs.`, for every cell. It needs no context.
#[test]
fn t_c13_an_instrumentation_leaf_is_the_span_leaf_on_scope_attrs() {
    let mut seen = 0usize;
    for c in cross_cells() {
        let (span, scope) = cell(
            AttrScope::Instrumentation,
            c.as_ref().map(|(op, v)| (*op, v)),
            false,
        );
        seen += 1;
        let want = span.map(|l| l.replace("attrs.`", "scope_attrs.`"));
        assert_eq!(scope, want, "{c:?}");
    }
    assert_eq!(seen, ALL_OPS.len() * 4 + 1);

    assert_eq!(
        rendered(r#"{ instrumentation.k != nil }"#),
        "dynamicType(scope_attrs.`k`) != 'None'"
    );
    assert_eq!(
        rendered(r#"{ instrumentation.k = nil }"#),
        "dynamicType(scope_attrs.`k`) = 'None'"
    );

    // `T-T9`'s compile half.
    let build = rendered(r#"{ instrumentation.otel.scope.build = "release" }"#);
    assert_eq!(
        build,
        "(coalesce(scope_attrs.`otel%2Escope%2Ebuild`.:String = 'release', false) OR \
         has(scope_attrs.`otel%2Escope%2Ebuild`.:`Array(Nullable(String))`, 'release'))"
    );
    assert_eq!(
        build.matches("attrs.`").count(),
        build.matches("scope_attrs.`").count(),
        "every attribute path is under scope_attrs: {build}"
    );
}

// ---------------------------------------------------------------------
// T-C14 — `resource.service.name`
// ---------------------------------------------------------------------

/// `T-C14`: section 3.3's table, byte for byte. Every string and regex
/// comparison is gated on the value having been stored as a string, and
/// a negation goes outside the gate.
#[test]
fn t_c14_the_service_name_reads_the_span_row_gated_on_its_type() {
    let svc = scoped(AttrScope::Resource, "service.name");
    let leaf = |op: ComparisonOp, v: Value| {
        compile_span_leaf_in(&svc, op, &v, &ctx()).map(|p| p.sql().to_string())
    };
    let s = |t: &str| Value::String(t.to_string());
    let n = |t: &str| Value::Number(t.to_string());

    assert_eq!(
        leaf(ComparisonOp::Eq, s("svc")),
        Ok("(service_type = 'string' AND service = 'svc')".to_string())
    );
    assert_eq!(
        leaf(ComparisonOp::Neq, s("svc")),
        Ok("NOT (service_type = 'string' AND service = 'svc')".to_string())
    );
    assert_eq!(
        leaf(ComparisonOp::Re, s("sv.*")),
        Ok("(service_type = 'string' AND match(service, '^(?:sv.*)$'))".to_string())
    );
    assert_eq!(
        leaf(ComparisonOp::Nre, s("sv.*")),
        Ok("NOT (service_type = 'string' AND match(service, '^(?:sv.*)$'))".to_string())
    );
    for v in [n("5"), Value::Bool(true)] {
        assert_eq!(leaf(ComparisonOp::Eq, v.clone()), Ok("false".to_string()));
        assert_eq!(leaf(ComparisonOp::Neq, v), Ok("false".to_string()));
    }
    for op in ordered_ops() {
        for v in [s("a"), n("5")] {
            assert_eq!(
                leaf(op, v),
                Err(PlanError::TypeMismatch(
                    "resource.service.name supports only = != =~ !~".to_string()
                )),
                "{op}"
            );
        }
    }
    for op in [ComparisonOp::Re, ComparisonOp::Nre] {
        assert_eq!(
            leaf(op, n("5")),
            Err(PlanError::TypeMismatch(
                "resource.service.name requires a string value".to_string()
            )),
            "{op}"
        );
    }
    // The two refusals again, from the query text.
    assert_eq!(
        refusal_in(r#"{ resource.service.name > "a" }"#),
        PlanError::TypeMismatch("resource.service.name supports only = != =~ !~".to_string())
    );
    assert_eq!(
        refusal_in(r#"{ resource.service.name =~ 5 }"#),
        PlanError::TypeMismatch("resource.service.name requires a string value".to_string())
    );

    // Truthiness matches no span; presence reads the type, never the text.
    assert_eq!(rendered_in(r#"{ resource.service.name }"#), "false");
    assert_eq!(
        rendered_in(r#"{ resource.service.name != nil }"#),
        "service_type != ''"
    );
    assert_eq!(
        rendered_in(r#"{ resource.service.name = nil }"#),
        "NOT (service_type != '')"
    );
}

/// The error `query` is refused with in [`ctx`].
fn refusal_in(query: &str) -> PlanError {
    match compile_span_predicate_in(&filter_body(query), &ctx()) {
        Err(e) => e,
        Ok(p) => panic!("{query} must be refused, it compiled to `{}`", p.sql()),
    }
}

// ---------------------------------------------------------------------
// T-C15 — the subquery, the context and the window
// ---------------------------------------------------------------------

/// `T-C15`: the subquery is inline text carrying `W`'s own day bound;
/// with no context a resource field is refused; and a predicate compiled
/// for one window cannot be composed with another.
#[test]
fn t_c15_a_resource_leaf_carries_its_window_inline() {
    assert_eq!(
        rendered_in(r#"{ resource.k8s.pod.name = "payment-a" }"#),
        "resource_id IN (SELECT resource_id FROM resources WHERE \
         day >= toDate(fromUnixTimestamp64Nano(1790094846486853636), 'UTC') \
         AND day <= toDate(fromUnixTimestamp64Nano(1790094846486853636), 'UTC') AND \
         ((coalesce(attrs.`k8s%2Epod%2Ename`.:String = 'payment-a', false) OR \
         has(attrs.`k8s%2Epod%2Ename`.:`Array(Nullable(String))`, 'payment-a'))))"
    );
    assert_eq!(
        refusal(r#"{ resource.k8s.pod.name = "payment-a" }"#),
        PlanError::UnsupportedField(RESOURCE_NEEDS_WINDOW.to_string())
    );
    // The same window composes.
    let p = compile_span_predicate_in(
        &filter_body(r#"{ resource.k8s.pod.name = "payment-a" }"#),
        &ctx(),
    )
    .expect("compiles");
    assert!(span_membership_sql("spans", t_b1_window(), &p).contains("resource_id IN"));
}

#[test]
#[should_panic(expected = "different window")]
fn t_c15_a_resource_predicate_refuses_another_window() {
    let p = compile_span_predicate_in(
        &filter_body(r#"{ resource.k8s.pod.name = "payment-a" }"#),
        &ctx(),
    )
    .expect("compiles");
    let other = WindowSql::start_closed_end_open(T_B1_START, T_B1_END + 1);
    let _ = span_membership_sql("spans", other, &p);
}

// ---------------------------------------------------------------------
// T-C16 / T-C17 — the operators
// ---------------------------------------------------------------------

/// `T-C16`: every operand parenthesised once, so the SQL grouping is the
/// parse tree's and SQL's own `AND`-before-`OR` never applies.
#[test]
fn t_c16_the_operators_keep_the_parse_trees_grouping() {
    let a = rendered(r#"{ span.a = 1 }"#);
    let b = rendered(r#"{ span.b = 2 }"#);
    let c = rendered(r#"{ span.c = 3 }"#);
    assert_eq!(
        rendered(r#"{ span.a = 1 && span.b = 2 }"#),
        format!("({a}) AND ({b})")
    );
    assert_eq!(
        rendered(r#"{ span.a = 1 || span.b = 2 }"#),
        format!("({a}) OR ({b})")
    );
    assert_eq!(
        rendered(r#"{ span.a = 1 || span.b = 2 && span.c = 3 }"#),
        format!("(({a}) OR ({b})) AND ({c})")
    );
    assert_eq!(rendered(r#"{ !(span.a = 1) }"#), format!("NOT ({a})"));
}

/// `T-C17`: `!=` at resource scope is the complement of `=`, and the same
/// text as `!(… = …)`.
#[test]
fn t_c17_a_resource_inequality_is_the_negated_equality() {
    assert_eq!(
        rendered_in(r#"{ resource.k != "v" }"#),
        rendered_in(r#"{ !(resource.k = "v") }"#)
    );
}

// ---------------------------------------------------------------------
// T-C18 — `!` over a bare field
// ---------------------------------------------------------------------

/// The demand condition and message for `!<root>.<key>` at span or
/// instrumentation scope.
fn demand(path: &str, field: &str) -> String {
    format!(
        "throwIf(dynamicType({path}) != 'None' AND dynamicType({path}) != 'Bool', \
         'expression (!{field}) expected a boolean')"
    )
}

/// `T-C18`: `{ !f }` matches only `false`, and a present non-boolean
/// fails the statement. Every demand is lifted out of the expression into
/// one `plus`, which does not short-circuit.
#[test]
fn t_c18_not_over_a_bare_field_demands_a_boolean() {
    let k = "attrs.`k`";
    let dk = demand(k, "span.k");
    let t_false = format!("coalesce({k}.:Bool = false, false)");
    let t_true = format!("coalesce({k}.:Bool = true, false)");

    assert_eq!(
        rendered(r#"{ !span.k }"#),
        format!("({dk} + toUInt8({t_false})) = 1")
    );
    assert_eq!(
        rendered(r#"{ !instrumentation.k }"#),
        format!(
            "({} + toUInt8(coalesce(scope_attrs.`k`.:Bool = false, false))) = 1",
            demand("scope_attrs.`k`", "instrumentation.k")
        )
    );
    assert_eq!(
        rendered_in(r#"{ !resource.k }"#),
        format!(
            "(throwIf({}, 'expression (!resource.k) expected a boolean') + toUInt8({})) = 1",
            r_of("dynamicType(attrs.`k`) != 'None' AND dynamicType(attrs.`k`) != 'Bool'"),
            r_of("coalesce(attrs.`k`.:Bool = false, false)")
        )
    );
    assert_eq!(
        rendered_in(r#"{ !resource.service.name }"#),
        "(throwIf(service_type != '' AND service_type != 'bool', \
         'expression (!resource.service.name) expected a boolean') + \
         toUInt8((service_type = 'bool' AND service = 'false'))) = 1"
    );
    // `(!f) op lit`: `= b` wants `!b`, `!= b` wants `b`, anything else
    // matches nothing — and the demand stays.
    assert_eq!(
        rendered(r#"{ !span.k = true }"#),
        format!("({dk} + toUInt8({t_false})) = 1")
    );
    assert_eq!(
        rendered(r#"{ !span.k != true }"#),
        format!("({dk} + toUInt8({t_true})) = 1")
    );
    assert_eq!(
        rendered(r#"{ !span.k = 1 }"#),
        format!("({dk} + toUInt8(false)) = 1")
    );
    assert_eq!(
        rendered(r#"{ true = !span.k }"#),
        format!("({dk} + toUInt8({t_false})) = 1")
    );
    // Lifted out of a short-circuiting operator.
    assert_eq!(
        rendered(r#"{ true || !span.k }"#),
        format!("({dk} + toUInt8((true) OR ({t_false}))) = 1")
    );
    // One `throwIf` per distinct demand, in pre-order.
    let (a, b) = ("attrs.`a`", "attrs.`b`");
    assert_eq!(
        rendered(r#"{ !span.a && !span.b }"#),
        format!(
            "({} + {} + toUInt8((coalesce({a}.:Bool = false, false)) AND \
             (coalesce({b}.:Bool = false, false)))) = 1",
            demand(a, "span.a"),
            demand(b, "span.b")
        )
    );
    assert_eq!(
        rendered(r#"{ !span.k && !span.k }"#),
        format!("({dk} + toUInt8(({t_false}) AND ({t_false}))) = 1")
    );
    let both = compile_span_predicate(&filter_body(r#"{ !span.a && !span.b }"#)).expect("compiles");
    assert_eq!(
        both.demand_messages(),
        [
            "expression (!span.a) expected a boolean".to_string(),
            "expression (!span.b) expected a boolean".to_string(),
        ]
    );

    // No demand, no wrapper.
    let plain = rendered(r#"{ span.a = 1 }"#);
    assert!(
        !plain.contains("throwIf") && !plain.contains("toUInt8"),
        "{plain}"
    );
    assert!(
        compile_span_predicate(&filter_body(r#"{ span.a = 1 }"#))
            .expect("compiles")
            .demand_messages()
            .is_empty()
    );

    assert_eq!(
        refusal(r#"{ !instrumentation:name }"#),
        PlanError::TypeMismatch(
            "expression (!instrumentation:name) expected a boolean".to_string()
        )
    );

    for query in [
        r#"{ !span.k }"#,
        r#"{ !instrumentation.k }"#,
        r#"{ !span.a && !span.b }"#,
        r#"{ true || !span.k }"#,
    ] {
        assert!(!rendered(query).contains("NOT IN ('None'"), "{query}");
    }
    for query in [r#"{ !resource.k }"#, r#"{ !resource.service.name }"#] {
        assert!(!rendered_in(query).contains("NOT IN ('None'"), "{query}");
    }
}
