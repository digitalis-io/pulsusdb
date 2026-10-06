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
    AttrScope, ComparisonOp, Field, FieldExpr, FieldOp, Intrinsic, SpansetExpr, SpansetFilter,
    Value,
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

/// The intrinsics this compiler serves. Every other `Intrinsic` variant is
/// #594's and must REFUSE rather than compile something wrong.
const IN_SCOPE_INTRINSICS: [Intrinsic; 14] = [
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
    Intrinsic::EventName,
    Intrinsic::EventTimeSinceStart,
    Intrinsic::LinkSpanId,
    Intrinsic::LinkTraceId,
];

/// The issue each out-of-scope intrinsic is refused with, and `None` for
/// the fourteen this compiler serves. **No wildcard arm**: a new
/// `Intrinsic` variant fails to compile here until it is classified.
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
        | Intrinsic::InstrumentationVersion
        | Intrinsic::EventName
        | Intrinsic::EventTimeSinceStart
        | Intrinsic::LinkSpanId
        | Intrinsic::LinkTraceId => None,
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

/// Part 2 section 5.1's refusal: a `.` field compiled with no context.
const UNSCOPED_NEEDS_WINDOW: &str = "the \".\" attribute scope needs the request window: \
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

    let mut visited_scopes = 0usize;
    for scope in AttrScope::ALL.iter().copied() {
        let got = compile_span_leaf(
            &Field::Attribute {
                scope,
                key: "k".to_string(),
            },
            ComparisonOp::Eq,
            &Value::String("x".to_string()),
        );
        visited_scopes += 1;
        match scope {
            AttrScope::Span | AttrScope::Instrumentation | AttrScope::Event | AttrScope::Link => {
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
            AttrScope::Unscoped => {
                assert_eq!(
                    got.map(|p| p.sql().to_string()),
                    Err(PlanError::UnsupportedField(
                        UNSCOPED_NEEDS_WINDOW.to_string()
                    )),
                    ". with no context"
                );
            }
        }
    }
    assert_eq!(visited_scopes, AttrScope::ALL.len());

    // The expression-level constructs part 3c serves now compile.
    for query in [
        r#"{ span.a + 1 = 2 }"#,
        r#"{ 1 = 1 }"#,
        r#"{ (span.a = 1) = true }"#,
        r#"{ !span.a = span.b }"#,
    ] {
        let got = compile_span_predicate(&filter_body(query)).map(|p| p.sql().to_string());
        assert!(got.is_ok(), "{query} must compile: {got:?}");
    }

    // The expression-level constructs deferred to part 3d, each naming
    // itself and its own part.
    for (query, token, suffix) in [
        (
            r#"{ event.a = span.b }"#,
            "field-against-field",
            "(issue #589 part 3d)",
        ),
        (r#"{ event.a + 1 = 2 }"#, "event.", "(issue #589 part 3d)"),
        (r#"{ !event.a = span.b }"#, "event.", "(issue #589 part 3d)"),
        (r#"{ .a + 1 = 2 }"#, ".", "(issue #589 part 3d)"),
    ] {
        match refusal(query) {
            PlanError::UnsupportedField(msg) => assert!(
                msg.contains(token) && msg.ends_with(suffix),
                "{query}: the refusal must name {token:?} and end `{suffix}`, got {msg:?}"
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
/// rather than a list of queries — `docs/api.md:1042` says of them that
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

// =====================================================================
// Issue #589 part 2 — event and link conditions, the four event and link
// intrinsics, and the unscoped `.k` chain, text only
// =====================================================================

/// The five typed reads a span-row attribute text carries, each with the
/// lambda variable an element condition binds it to (section 3.1).
const READS: [(&str, &str); 5] = [
    (".:String", "s"),
    (".:Int64", "i"),
    (".:Float64", "f"),
    (".:Bool", "b"),
    (".:`Array(Nullable(String))`", "sa"),
];

/// This test's own rewrite of the span leaf `span_text` (key `k`) into the
/// element condition over `e` (`events` or `links`): strip a leading
/// `NOT `, replace each ``attrs.`k`<suffix>`` with its variable, list the
/// variables in first-use order, wrap in `arrayExists`, and put the `NOT`
/// back OUTSIDE it.
fn element_of(e: &str, span_text: &str) -> String {
    let (negated, positive) = match span_text.strip_prefix("NOT ") {
        Some(p) => (true, p),
        None => (false, span_text),
    };
    let needle = "attrs.`k`";
    let mut body = String::new();
    let mut vars: Vec<(&str, &str)> = Vec::new();
    let mut rest = positive;
    while let Some(at) = rest.find(needle) {
        body.push_str(&rest[..at]);
        let after = &rest[at + needle.len()..];
        let (suffix, var) = READS
            .iter()
            .copied()
            .find(|(suffix, _)| after.starts_with(suffix))
            .unwrap_or_else(|| panic!("an untyped read in {span_text}"));
        body.push_str(var);
        if !vars.iter().any(|(_, v)| *v == var) {
            vars.push((suffix, var));
        }
        rest = &after[suffix.len()..];
    }
    body.push_str(rest);
    assert!(!vars.is_empty(), "no attribute read in {span_text}");
    let names: Vec<&str> = vars.iter().map(|(_, v)| *v).collect();
    let args = if names.len() > 1 {
        format!("({})", names.join(", "))
    } else {
        names[0].to_string()
    };
    let arrays: Vec<String> = vars
        .iter()
        .map(|(suffix, _)| format!("{e}.attrs.`k`{suffix}"))
        .collect();
    let inner = format!("arrayExists({args} -> {body}, {})", arrays.join(", "));
    if negated {
        format!("NOT {inner}")
    } else {
        inner
    }
}

// ---------------------------------------------------------------------
// T-C20 — the element cross product
// ---------------------------------------------------------------------

/// `T-C20`: where `span.k` renders `L`, `event.k` and `link.k` render the
/// test's own rewrite of `L` ([`element_of`]); where `span.k` is refused,
/// they are refused with the same error. The negation is OUTSIDE
/// `arrayExists`, never inside the lambda.
#[test]
fn t_c20_an_event_or_link_leaf_is_any_match_over_the_typed_array() {
    let mut seen = 0usize;
    for (scope, e) in [(AttrScope::Event, "events"), (AttrScope::Link, "links")] {
        for c in cross_cells() {
            let (span, element) = cell(scope, c.as_ref().map(|(op, v)| (*op, v)), false);
            seen += 1;
            assert_eq!(element, span.map(|l| element_of(e, &l)), "{scope} {c:?}");
        }
    }
    assert_eq!(seen, 2 * (ALL_OPS.len() * 4 + 1));

    // Section 3.2's four texts.
    assert_eq!(
        rendered(r#"{ event.k = "v" }"#),
        "arrayExists((s, sa) -> (coalesce(s = 'v', false) OR has(sa, 'v')), \
         events.attrs.`k`.:String, events.attrs.`k`.:`Array(Nullable(String))`)"
    );
    assert_eq!(
        rendered(r#"{ event.k != 3 }"#),
        "NOT arrayExists((i, f) -> (coalesce(i = 3, false) OR coalesce(f = 3, false)), \
         events.attrs.`k`.:Int64, events.attrs.`k`.:Float64)"
    );
    assert_eq!(
        rendered(r#"{ link.k =~ "v" }"#),
        "arrayExists(s -> coalesce(match(s, '^(?:v)$'), false), links.attrs.`k`.:String)"
    );
    assert_eq!(
        rendered(r#"{ event.k }"#),
        "arrayExists(b -> coalesce(b = true, false), events.attrs.`k`.:Bool)"
    );
    // Section 3.3's two.
    assert_eq!(
        rendered(r#"{ event.k != nil }"#),
        "arrayExists(d -> dynamicType(d) != 'None', events.attrs.`k`)"
    );
    assert_eq!(
        rendered(r#"{ event.k = nil }"#),
        "NOT arrayExists(d -> dynamicType(d) != 'None', events.attrs.`k`)"
    );
}

// ---------------------------------------------------------------------
// T-C21 — the unscoped chain, generated
// ---------------------------------------------------------------------

/// The chain's scopes, in its order (section 5.1).
const CHAIN: [AttrScope; 5] = [
    AttrScope::Span,
    AttrScope::Resource,
    AttrScope::Event,
    AttrScope::Link,
    AttrScope::Instrumentation,
];

/// What `field` compiles to in [`ctx`]: the cell's leaf, or `{ field }`.
fn chain_cell_text(
    field: Field,
    cell: &Option<(ComparisonOp, Value)>,
) -> Result<String, PlanError> {
    match cell {
        Some((op, v)) => compile_span_leaf_in(&field, *op, v, &ctx()),
        None => compile_span_predicate_in(&FieldExpr::Field(field), &ctx()),
    }
    .map(|p| p.sql().to_string())
}

/// `{ <scope>key != nil }` in [`ctx`].
fn presence_text(scope: AttrScope, key: &str) -> String {
    compile_span_predicate_in(
        &FieldExpr::Exists {
            field: scoped(scope, key),
            negated: false,
        },
        &ctx(),
    )
    .unwrap_or_else(|e| panic!("{scope}{key} != nil must compile: {e}"))
    .sql()
    .to_string()
}

/// Section 5.2's `P_resource` at `service.name`.
fn service_present_resource() -> String {
    format!(
        "(service_type = 'string' OR {})",
        r_of("dynamicType(attrs.`service%2Ename`) != 'None'")
    )
}

/// The symbol of `pos(op)` (section 4): `!=` to `=`, the others unchanged.
fn pos_symbol(op: ComparisonOp) -> &'static str {
    match op {
        ComparisonOp::Eq | ComparisonOp::Neq => "=",
        ComparisonOp::Gt => ">",
        ComparisonOp::Gte => ">=",
        ComparisonOp::Lt => "<",
        ComparisonOp::Lte => "<=",
        ComparisonOp::Re | ComparisonOp::Nre => "=~",
    }
}

/// Section 5.2's `X_resource` at `service.name`, built from `p` (the span
/// leaf's positive text), `R` and `G`.
fn service_x_resource(span_text: &str, cell: &Option<(ComparisonOp, Value)>) -> String {
    let (negated, p) = match span_text.strip_prefix("NOT ") {
        Some(p) => (true, p),
        None => (false, span_text),
    };
    let r = r_of(p);
    match cell {
        Some((op, Value::String(s))) => {
            let g = match op {
                ComparisonOp::Re | ComparisonOp::Nre => {
                    format!("match(service, '^(?:{s})$')")
                }
                other => format!("service {} '{s}'", pos_symbol(*other)),
            };
            let x = format!("((service_type = 'string' AND {g}) OR {r})");
            if negated { format!("NOT {x}") } else { x }
        }
        _ => {
            if negated {
                format!("NOT ({r})")
            } else {
                r
            }
        }
    }
}

/// `T-C21`: `.key <op> v` is section 5.1's `multiIf` over the five scopes'
/// own compiled texts, in the chain's order, with `D` from `X_span`'s
/// polarity; at `service.name` the resource branch is section 5.2's.
#[test]
fn t_c21_the_unscoped_chain_is_built_from_each_scopes_own_text() {
    let mut seen = 0usize;
    for key in ["k", "service.name"] {
        for c in cross_cells() {
            seen += 1;
            let got = chain_cell_text(scoped(AttrScope::Unscoped, key), &c);
            let x_span = chain_cell_text(scoped(AttrScope::Span, key), &c);
            let want = x_span.map(|x_span| {
                let mut args: Vec<String> = Vec::new();
                for scope in CHAIN {
                    let (p, x) = if scope == AttrScope::Resource && key == "service.name" {
                        (service_present_resource(), service_x_resource(&x_span, &c))
                    } else {
                        let x = chain_cell_text(scoped(scope, key), &c).unwrap_or_else(|e| {
                            panic!("{scope}{key} refuses where span.{key} compiles: {e}")
                        });
                        (presence_text(scope, key), x)
                    };
                    args.push(p);
                    args.push(x);
                }
                let d = if x_span.starts_with("NOT ") {
                    "true"
                } else {
                    "false"
                };
                format!("multiIf({}, {d})", args.join(", "))
            });
            assert_eq!(got, want, ".{key} {c:?}");
        }
    }
    assert_eq!(seen, 2 * (ALL_OPS.len() * 4 + 1));

    let d = t_b1_window().resources_day_clause();
    // Section 5.3's text.
    assert_eq!(
        rendered_in(r#"{ .k != "v" }"#),
        format!(
            "multiIf(dynamicType(attrs.`k`) != 'None', NOT (coalesce(attrs.`k`.:String = 'v', false) \
             OR has(attrs.`k`.:`Array(Nullable(String))`, 'v')), \
             resource_id IN (SELECT resource_id FROM resources WHERE {d} AND \
             (dynamicType(attrs.`k`) != 'None')), \
             NOT (resource_id IN (SELECT resource_id FROM resources WHERE {d} AND \
             ((coalesce(attrs.`k`.:String = 'v', false) OR \
             has(attrs.`k`.:`Array(Nullable(String))`, 'v'))))), \
             arrayExists(d -> dynamicType(d) != 'None', events.attrs.`k`), \
             NOT arrayExists((s, sa) -> (coalesce(s = 'v', false) OR has(sa, 'v')), \
             events.attrs.`k`.:String, events.attrs.`k`.:`Array(Nullable(String))`), \
             arrayExists(d -> dynamicType(d) != 'None', links.attrs.`k`), \
             NOT arrayExists((s, sa) -> (coalesce(s = 'v', false) OR has(sa, 'v')), \
             links.attrs.`k`.:String, links.attrs.`k`.:`Array(Nullable(String))`), \
             dynamicType(scope_attrs.`k`) != 'None', \
             NOT (coalesce(scope_attrs.`k`.:String = 'v', false) OR \
             has(scope_attrs.`k`.:`Array(Nullable(String))`, 'v')), \
             true)"
        )
    );

    // Section 5.2's resource branches and `P_resource`, byte for byte, each
    // the second and first argument pair of its chain.
    let svc_x = format!(
        "((service_type = 'string' AND service = 'x') OR resource_id IN (SELECT resource_id FROM \
         resources WHERE {d} AND ((coalesce(attrs.`service%2Ename`.:String = 'x', false) OR \
         has(attrs.`service%2Ename`.:`Array(Nullable(String))`, 'x')))))"
    );
    let svc_n = format!(
        "resource_id IN (SELECT resource_id FROM resources WHERE {d} AND \
         ((coalesce(attrs.`service%2Ename`.:Int64 = 12345, false) OR \
         coalesce(attrs.`service%2Ename`.:Float64 = 12345, false))))"
    );
    let svc_p = format!(
        "(service_type = 'string' OR resource_id IN (SELECT resource_id FROM resources WHERE {d} \
         AND (dynamicType(attrs.`service%2Ename`) != 'None')))"
    );
    for (query, x_resource) in [
        (r#"{ .service.name = "x" }"#, &svc_x),
        (r#"{ .service.name = 12345 }"#, &svc_n),
    ] {
        let got = rendered_in(query);
        let span_pair = format!(
            "multiIf({}, {}, ",
            presence_text(AttrScope::Span, "service.name"),
            chain_cell_text(
                scoped(AttrScope::Span, "service.name"),
                &match filter_body(query) {
                    FieldExpr::Binary { rhs, .. } => match *rhs {
                        FieldExpr::Literal(v) => Some((ComparisonOp::Eq, v)),
                        other => panic!("{other}"),
                    },
                    other => panic!("{other}"),
                }
            )
            .expect("span.service.name compiles")
        );
        let prefix = format!("{span_pair}{svc_p}, {x_resource}, ");
        assert!(
            got.starts_with(&prefix),
            "{query}:\n{got}\nmust start\n{prefix}"
        );
    }

    // Presence and absence: every scope's presence, ORed in the chain's
    // order, never ANDed.
    let presence = format!(
        "(dynamicType(attrs.`k`) != 'None') OR ({}) OR \
         (arrayExists(d -> dynamicType(d) != 'None', events.attrs.`k`)) OR \
         (arrayExists(d -> dynamicType(d) != 'None', links.attrs.`k`)) OR \
         (dynamicType(scope_attrs.`k`) != 'None')",
        r_of("dynamicType(attrs.`k`) != 'None'")
    );
    assert_eq!(rendered_in(r#"{ .k != nil }"#), presence);
    assert_eq!(rendered_in(r#"{ .k = nil }"#), format!("NOT ({presence})"));
}

// ---------------------------------------------------------------------
// T-C22 — the four intrinsics
// ---------------------------------------------------------------------

/// `T-C22`: section 4's texts and refusals, byte for byte.
#[test]
fn t_c22_the_event_and_link_intrinsics_are_any_match_over_their_arrays() {
    assert_eq!(
        rendered(r#"{ event:name !~ "e.*" }"#),
        "NOT arrayExists(n -> match(n, '^(?:e.*)$'), events.name)"
    );
    assert_eq!(
        rendered(r#"{ event:timeSinceStart < 3ms }"#),
        "arrayExists(t -> toInt128(t) - start_ns < 3000000, events.time_ns)"
    );
    assert_eq!(
        rendered(r#"{ event:timeSinceStart != 2ms }"#),
        "NOT arrayExists(t -> toInt128(t) - start_ns = 2000000, events.time_ns)"
    );
    assert_eq!(
        rendered(r#"{ link:traceID != "AB" }"#),
        "NOT arrayExists(h -> lower(hex(h)) = 'ab', links.trace_id)"
    );
    assert_eq!(
        rendered(r#"{ link:spanID =~ "0A.*" }"#),
        "arrayExists(h -> match(lower(hex(h)), '^(?:0A.*)$'), links.span_id)"
    );
    // A bare number is nanoseconds.
    assert_eq!(
        rendered(r#"{ event:timeSinceStart > 5 }"#),
        "arrayExists(t -> toInt128(t) - start_ns > 5, events.time_ns)"
    );
    // An ordered string comparison renders.
    assert_eq!(
        rendered(r#"{ event:name > "a" }"#),
        "arrayExists(n -> n > 'a', events.name)"
    );

    for (query, message) in [
        (
            r#"{ event:name = 5 }"#,
            "event:name requires a string value",
        ),
        (
            r#"{ event:timeSinceStart = "x" }"#,
            "event:timeSinceStart requires a duration or a number",
        ),
        (
            r#"{ event:timeSinceStart =~ "x" }"#,
            "event:timeSinceStart requires a duration or a number",
        ),
        (
            r#"{ link:spanID = 5 }"#,
            "link:spanID requires a string value",
        ),
        (
            r#"{ link:traceID = 5 }"#,
            "link:traceID requires a string value",
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
            &Field::Intrinsic(Intrinsic::EventTimeSinceStart),
            ComparisonOp::Re,
            &duration_value(r#"{ duration > 1ms }"#),
        )
        .map(|p| p.sql().to_string()),
        Err(PlanError::TypeMismatch(
            "event:timeSinceStart does not support regex operators".to_string()
        ))
    );
    // `!` over each keeps part 1's plan-time refusal.
    for intrinsic in [
        "event:name",
        "event:timeSinceStart",
        "link:spanID",
        "link:traceID",
    ] {
        assert_eq!(
            refusal(&format!("{{ !{intrinsic} }}")),
            PlanError::TypeMismatch(format!("expression (!{intrinsic}) expected a boolean"))
        );
    }
}

// ---------------------------------------------------------------------
// T-C23 — `!` over the new scopes
// ---------------------------------------------------------------------

/// `T-C23`: section 6. `T` and the demand condition `c` are any-match for
/// `event.`/`link.`, and a `multiIf` over the scopes for `.`.
#[test]
fn t_c23_not_over_an_event_link_or_unscoped_field_demands_a_boolean() {
    let elem = |e: &str| {
        (
            format!("arrayExists(b -> coalesce(b = false, false), {e}.attrs.`k`.:Bool)"),
            format!(
                "arrayExists(d -> dynamicType(d) != 'None' AND dynamicType(d) != 'Bool', \
                 {e}.attrs.`k`)"
            ),
        )
    };
    let (t_ev, c_ev) = elem("events");
    let (t_lk, c_lk) = elem("links");
    assert_eq!(
        rendered(r#"{ !event.k }"#),
        format!(
            "(throwIf({c_ev}, 'expression (!event.k) expected a boolean') + toUInt8({t_ev})) = 1"
        )
    );
    assert_eq!(
        rendered(r#"{ !link.k = true }"#),
        format!(
            "(throwIf({c_lk}, 'expression (!link.k) expected a boolean') + toUInt8({t_lk})) = 1"
        )
    );

    let c_span = "dynamicType(attrs.`k`) != 'None' AND dynamicType(attrs.`k`) != 'Bool'";
    let t_span = "coalesce(attrs.`k`.:Bool = false, false)";
    let c_in = "dynamicType(scope_attrs.`k`) != 'None' AND dynamicType(scope_attrs.`k`) != 'Bool'";
    let t_in = "coalesce(scope_attrs.`k`.:Bool = false, false)";
    let p = |scope: AttrScope| presence_text(scope, "k");
    let t = format!(
        "multiIf({}, {t_span}, {}, {}, {}, {t_ev}, {}, {t_lk}, {}, {t_in}, false)",
        p(AttrScope::Span),
        p(AttrScope::Resource),
        r_of(t_span),
        p(AttrScope::Event),
        p(AttrScope::Link),
        p(AttrScope::Instrumentation),
    );
    let c = format!(
        "multiIf({}, {c_span}, {}, {}, {}, {c_ev}, {}, {c_lk}, {}, {c_in}, false)",
        p(AttrScope::Span),
        p(AttrScope::Resource),
        r_of(c_span),
        p(AttrScope::Event),
        p(AttrScope::Link),
        p(AttrScope::Instrumentation),
    );
    assert_eq!(
        rendered_in(r#"{ !.k }"#),
        format!("(throwIf({c}, 'expression (!.k) expected a boolean') + toUInt8({t})) = 1")
    );

    // At `service.name`: section 5.2's `P_resource`, part 1's `T` and `c`.
    let ps = |scope: AttrScope| presence_text(scope, "service.name");
    let span_path = "attrs.`service%2Ename`";
    let in_path = "scope_attrs.`service%2Ename`";
    let elem_s = |e: &str| {
        (
            format!(
                "arrayExists(b -> coalesce(b = false, false), {e}.attrs.`service%2Ename`.:Bool)"
            ),
            format!(
                "arrayExists(d -> dynamicType(d) != 'None' AND dynamicType(d) != 'Bool', \
                 {e}.attrs.`service%2Ename`)"
            ),
        )
    };
    let (ts_ev, cs_ev) = elem_s("events");
    let (ts_lk, cs_lk) = elem_s("links");
    let ts = format!(
        "multiIf({}, coalesce({span_path}.:Bool = false, false), {}, \
         (service_type = 'bool' AND service = 'false'), {}, {ts_ev}, {}, {ts_lk}, {}, \
         coalesce({in_path}.:Bool = false, false), false)",
        ps(AttrScope::Span),
        service_present_resource(),
        ps(AttrScope::Event),
        ps(AttrScope::Link),
        ps(AttrScope::Instrumentation),
    );
    let cs = format!(
        "multiIf({}, dynamicType({span_path}) != 'None' AND dynamicType({span_path}) != 'Bool', \
         {}, service_type != '' AND service_type != 'bool', {}, {cs_ev}, {}, {cs_lk}, {}, \
         dynamicType({in_path}) != 'None' AND dynamicType({in_path}) != 'Bool', false)",
        ps(AttrScope::Span),
        service_present_resource(),
        ps(AttrScope::Event),
        ps(AttrScope::Link),
        ps(AttrScope::Instrumentation),
    );
    assert_eq!(
        rendered_in(r#"{ !.service.name }"#),
        format!(
            "(throwIf({cs}, 'expression (!.service.name) expected a boolean') + toUInt8({ts})) = 1"
        )
    );

    // Lifted out of a short-circuiting operator, once.
    let lifted = rendered(r#"{ true || !event.k }"#);
    assert_eq!(
        lifted,
        format!(
            "(throwIf({c_ev}, 'expression (!event.k) expected a boolean') + \
             toUInt8((true) OR ({t_ev}))) = 1"
        )
    );
    assert_eq!(lifted.matches("throwIf(").count(), 1, "{lifted}");
    assert_eq!(
        compile_span_predicate(&filter_body(r#"{ true || !event.k }"#))
            .expect("compiles")
            .demand_messages(),
        ["expression (!event.k) expected a boolean".to_string()]
    );
}

// ---------------------------------------------------------------------
// T-C24 — the chain's truthiness, presence and context
// ---------------------------------------------------------------------

/// `T-C24`: `{ .k }` is the chain of `{ sc.k }` with `D = false`;
/// `{ .k = nil }` and `{ .k != nil }` are the ORed presences; the chain
/// needs the window, and `event.` does not.
#[test]
fn t_c24_the_chain_needs_the_window_and_the_element_scopes_do_not() {
    let d = t_b1_window().resources_day_clause();
    let r_present = format!(
        "resource_id IN (SELECT resource_id FROM resources WHERE {d} AND \
         (dynamicType(attrs.`k`) != 'None'))"
    );
    let r_true = format!(
        "resource_id IN (SELECT resource_id FROM resources WHERE {d} AND \
         (coalesce(attrs.`k`.:Bool = true, false)))"
    );
    assert_eq!(
        rendered_in(r#"{ .k }"#),
        format!(
            "multiIf(dynamicType(attrs.`k`) != 'None', coalesce(attrs.`k`.:Bool = true, false), \
             {r_present}, {r_true}, \
             arrayExists(d -> dynamicType(d) != 'None', events.attrs.`k`), \
             arrayExists(b -> coalesce(b = true, false), events.attrs.`k`.:Bool), \
             arrayExists(d -> dynamicType(d) != 'None', links.attrs.`k`), \
             arrayExists(b -> coalesce(b = true, false), links.attrs.`k`.:Bool), \
             dynamicType(scope_attrs.`k`) != 'None', coalesce(scope_attrs.`k`.:Bool = true, false), \
             false)"
        )
    );
    let presence = format!(
        "(dynamicType(attrs.`k`) != 'None') OR ({r_present}) OR \
         (arrayExists(d -> dynamicType(d) != 'None', events.attrs.`k`)) OR \
         (arrayExists(d -> dynamicType(d) != 'None', links.attrs.`k`)) OR \
         (dynamicType(scope_attrs.`k`) != 'None')"
    );
    assert_eq!(rendered_in(r#"{ .k != nil }"#), presence);
    assert_eq!(rendered_in(r#"{ .k = nil }"#), format!("NOT ({presence})"));

    assert_eq!(
        refusal(r#"{ .k = 1 }"#),
        PlanError::UnsupportedField(UNSCOPED_NEEDS_WINDOW.to_string())
    );
    assert_eq!(
        rendered(r#"{ event.k = 1 }"#),
        "arrayExists((i, f) -> (coalesce(i = 1, false) OR coalesce(f = 1, false)), \
         events.attrs.`k`.:Int64, events.attrs.`k`.:Float64)"
    );
    let p = compile_span_predicate(&filter_body(r#"{ event.k = 1 }"#)).expect("compiles");
    // No resource subquery, so no window is carried: another window composes.
    let other = WindowSql::start_closed_end_open(T_B1_START, T_B1_END + 1);
    assert!(span_membership_sql("spans", other, &p).contains("arrayExists"));
}

// =====================================================================
// Issue #589 part 3a — field against field on the span row, text only
// =====================================================================

/// `L op R` with both sides fields, built directly so a pair the
/// validator would refuse first still reaches the compiler.
fn field_compare_expr(lhs: &Field, op: ComparisonOp, rhs: &Field) -> FieldExpr {
    FieldExpr::Binary {
        op: FieldOp::Cmp(op),
        lhs: Box::new(FieldExpr::Field(lhs.clone())),
        rhs: Box::new(FieldExpr::Field(rhs.clone())),
    }
}

/// The six operators a field-against-field comparison serves.
const FF_OPS: [ComparisonOp; 6] = [
    ComparisonOp::Eq,
    ComparisonOp::Neq,
    ComparisonOp::Lt,
    ComparisonOp::Lte,
    ComparisonOp::Gt,
    ComparisonOp::Gte,
];

fn ff_symbol(op: ComparisonOp) -> &'static str {
    match op {
        ComparisonOp::Eq => "=",
        ComparisonOp::Neq => "!=",
        ComparisonOp::Gt => ">",
        ComparisonOp::Gte => ">=",
        ComparisonOp::Lt => "<",
        ComparisonOp::Lte => "<=",
        ComparisonOp::Re | ComparisonOp::Nre => panic!("no field-against-field regex"),
    }
}

/// This test's own copy of the integer-against-float rule: the integer
/// side is compared exactly, as written.
fn ff_mixed_int(e: &str) -> String {
    e.to_string()
}

/// One operand of the part-3a design's section 3.1, with the part-3b
/// design's section 5.2 resource operands: the field, its arms by class,
/// whether it can be `NULL`, and whether it needs the context.
///
/// A resource-row arm reads the lambda variable, written `{v}` here and
/// replaced by `r1` on the left and `r2` on the right. `types` is the text
/// of the operand's type set, `tupleElement(Q(k), 3)`; `s_on_span_row`
/// leaves the scalar string arm ungated; `bind` says the operand is bound
/// to `V(k)` (section 5.3).
struct FfOperand {
    field: Field,
    arms: Vec<(&'static str, String)>,
    nullable: bool,
    needs_ctx: bool,
    types: Option<String>,
    s_on_span_row: bool,
    bind: bool,
}

impl FfOperand {
    fn arm(&self, class: &str, var: &str) -> Option<String> {
        self.arms
            .iter()
            .find(|(c, _)| *c == class)
            .map(|(_, e)| e.replace("{v}", var))
    }

    /// The gate of the scalar arm of `class`, or of the array arm whose
    /// elements are `class` — section 5.4, from this file's own type names.
    fn gate(&self, class: &str, array: bool) -> Option<String> {
        let types = self.types.as_deref()?;
        let name = ff_type_name(class)?;
        if array {
            return Some(format!("has({types}, 'Array(Nullable({name}))')"));
        }
        if class == "s" && self.s_on_span_row {
            return None;
        }
        Some(format!("has({types}, '{name}')"))
    }

    /// `V(k)` for a bound operand.
    fn bound_value(&self) -> String {
        match &self.field {
            Field::Attribute {
                scope: AttrScope::Resource,
                key,
            } => v_of(&ff_resource_path(key)),
            other => panic!("{other} is not bound"),
        }
    }
}

/// Section 5.2's type names, scalar by class.
fn ff_type_name(class: &str) -> Option<&'static str> {
    match class {
        "s" => Some("String"),
        "i" => Some("Int64"),
        "f" => Some("Float64"),
        "b" => Some("Bool"),
        _ => None,
    }
}

/// `P(k)` for the keys these cases use: the writer's escape of `.` is
/// `%2E`, and none of them carries a `%` or a backtick.
fn ff_resource_path(key: &str) -> String {
    format!("attrs.`{}`", key.replace('.', "%2E"))
}

/// `Q(k)` — section 5.1, from [`ctx`]'s window and table.
fn q_of(path: &str) -> String {
    format!(
        "(SELECT (groupArray(resource_id), groupArray({path}), \
         groupUniqArray(dynamicType({path}))) FROM resources WHERE {} AND \
         dynamicType({path}) != 'None')",
        t_b1_window().resources_day_clause()
    )
}

/// `V(k)` — section 5.1.
fn v_of(path: &str) -> String {
    let q = q_of(path);
    format!(
        "arrayElement(tupleElement({q}, 2), transform(resource_id, tupleElement({q}, 1), \
         arrayEnumerate(tupleElement({q}, 1)), 0))"
    )
}

/// `G(k, 'T')` — section 5.1.
fn g_of(path: &str, type_name: &str) -> String {
    format!("has(tupleElement({}, 3), '{type_name}')", q_of(path))
}

/// Section 5.2's `resource.k` row: every class read from the bound value,
/// each gated on the key's type set.
fn ff_resource(key: &str) -> FfOperand {
    let typed = |t: &str| format!("dynamicElement({{v}}, '{t}')");
    FfOperand {
        field: scoped(AttrScope::Resource, key),
        arms: vec![
            ("s", typed("String")),
            ("i", typed("Int64")),
            ("f", typed("Float64")),
            ("b", typed("Bool")),
            ("sa", typed("Array(Nullable(String))")),
            ("ia", typed("Array(Nullable(Int64))")),
            ("fa", typed("Array(Nullable(Float64))")),
            ("ba", typed("Array(Nullable(Bool))")),
        ],
        nullable: true,
        needs_ctx: true,
        types: Some(format!("tupleElement({}, 3)", q_of(&ff_resource_path(key)))),
        s_on_span_row: false,
        bind: true,
    }
}

/// An attribute operand's eight typed reads at `root`.
fn ff_attr(scope: AttrScope, root: &str, key: &str) -> FfOperand {
    let p = format!("{root}.`{key}`");
    FfOperand {
        field: scoped(scope, key),
        arms: vec![
            ("s", format!("{p}.:String")),
            ("i", format!("{p}.:Int64")),
            ("f", format!("{p}.:Float64")),
            ("b", format!("{p}.:Bool")),
            ("sa", format!("{p}.:`Array(Nullable(String))`")),
            ("ia", format!("{p}.:`Array(Nullable(Int64))`")),
            ("fa", format!("{p}.:`Array(Nullable(Float64))`")),
            ("ba", format!("{p}.:`Array(Nullable(Bool))`")),
        ],
        nullable: true,
        needs_ctx: false,
        types: None,
        s_on_span_row: false,
        bind: false,
    }
}

fn ff_intrinsic(intrinsic: Intrinsic, class: &'static str, expr: &str) -> FfOperand {
    FfOperand {
        field: Field::Intrinsic(intrinsic),
        arms: vec![(class, expr.to_string())],
        nullable: false,
        needs_ctx: false,
        types: None,
        s_on_span_row: false,
        bind: false,
    }
}

/// Section 3.1's operands, with the part-3b design's two resource operands.
fn ff_operands() -> Vec<FfOperand> {
    vec![
        ff_attr(AttrScope::Span, "attrs", "a"),
        ff_attr(AttrScope::Instrumentation, "scope_attrs", "a"),
        ff_intrinsic(Intrinsic::Name, "s", "name"),
        ff_intrinsic(Intrinsic::StatusMessage, "s", "status_message"),
        ff_intrinsic(Intrinsic::SpanId, "s", "lower(hex(span_id))"),
        ff_intrinsic(Intrinsic::ParentId, "s", "lower(hex(parent_span_id))"),
        ff_intrinsic(Intrinsic::TraceId, "s", "lower(hex(trace_id))"),
        ff_intrinsic(Intrinsic::InstrumentationName, "s", "scope_name"),
        ff_intrinsic(Intrinsic::InstrumentationVersion, "s", "scope_version"),
        ff_intrinsic(Intrinsic::Duration, "i", "duration_ns"),
        ff_intrinsic(Intrinsic::Status, "st", "status_code"),
        ff_intrinsic(Intrinsic::Kind, "kd", "kind"),
        ff_resource("a"),
        {
            let mut service = ff_resource("service.name");
            service.arms[0].1 = "if(service_type = 'string', service, NULL)".to_string();
            service.s_on_span_row = true;
            service
        },
    ]
}

/// Section 3.2's scalar pairs in order, each with whether an ordered
/// operator has a term.
const FF_SCALAR_PAIRS: [(&str, &str, bool); 8] = [
    ("s", "s", true),
    ("i", "i", true),
    ("i", "f", true),
    ("f", "i", true),
    ("f", "f", true),
    ("b", "b", false),
    ("st", "st", false),
    ("kd", "kd", false),
];

/// Section 3.2's `(scalar class, element class)` array pairs in order.
const FF_ARRAY_PAIRS: [(&str, &str, bool); 6] = [
    ("s", "s", true),
    ("i", "i", true),
    ("i", "f", true),
    ("f", "i", true),
    ("f", "f", true),
    ("b", "b", false),
];

/// The two sides of one pair with the integer side of a mixed pair
/// passed through [`ff_mixed_int`].
fn ff_mixed(lc: &str, rc: &str, l: &str, r: &str) -> (String, String) {
    match (lc, rc) {
        ("i", "f") => (ff_mixed_int(l), r.to_string()),
        ("f", "i") => (l.to_string(), ff_mixed_int(r)),
        _ => (l.to_string(), r.to_string()),
    }
}

fn ff_array_term(op: ComparisonOp, l: &str, r: &str, array: &str) -> String {
    if op == ComparisonOp::Neq {
        format!("(notEmpty({array}) AND arrayAll(x -> coalesce({l} != {r}, false), {array}))")
    } else {
        format!(
            "arrayExists(x -> coalesce({l} {} {r}, false), {array})",
            ff_symbol(op)
        )
    }
}

/// Section 5.4's wrapper: a term with a gate on either side, or both,
/// left first, is `if(<gates>, <term>, false)`.
fn ff_gated(term: String, gl: Option<String>, gr: Option<String>) -> String {
    let gates: Vec<String> = gl.into_iter().chain(gr).collect();
    if gates.is_empty() {
        term
    } else {
        format!("if({}, {term}, false)", gates.join(" AND "))
    }
}

/// The text section 3.2 gives `L op R`, built from this file's own copy of
/// the rules, with the part-3b design's gates (5.4) and binding (5.3): the
/// left operand reads `r1`, the right `r2`.
fn ff_expected(l: &FfOperand, op: ComparisonOp, r: &FfOperand) -> String {
    let sym = ff_symbol(op);
    let ordered = op_is_ordered(op);
    let coalesced = l.nullable || r.nullable;
    let (lv, rv) = ("r1", "r2");
    let mut terms = Vec::new();
    for (lc, rc, ordered_ok) in FF_SCALAR_PAIRS {
        if ordered && !ordered_ok {
            continue;
        }
        let (Some(a), Some(b)) = (l.arm(lc, lv), r.arm(rc, rv)) else {
            continue;
        };
        let (a, b) = ff_mixed(lc, rc, &a, &b);
        let c = format!("{a} {sym} {b}");
        let term = if coalesced {
            format!("coalesce({c}, false)")
        } else {
            c
        };
        terms.push(ff_gated(term, l.gate(lc, false), r.gate(rc, false)));
    }
    // The array on the right: the left's scalar against each element.
    for (c, e, ordered_ok) in FF_ARRAY_PAIRS {
        if ordered && !ordered_ok {
            continue;
        }
        let (Some(scalar), Some(array)) = (l.arm(c, lv), r.arm(&format!("{e}a"), rv)) else {
            continue;
        };
        let (a, b) = ff_mixed(c, e, &scalar, "x");
        terms.push(ff_gated(
            ff_array_term(op, &a, &b, &array),
            l.gate(c, false),
            r.gate(e, true),
        ));
    }
    // The array on the left: each element against the right's scalar.
    for (c, e, ordered_ok) in FF_ARRAY_PAIRS {
        if ordered && !ordered_ok {
            continue;
        }
        let (Some(array), Some(scalar)) = (l.arm(&format!("{e}a"), lv), r.arm(c, rv)) else {
            continue;
        };
        let (a, b) = ff_mixed(e, c, "x", &scalar);
        terms.push(ff_gated(
            ff_array_term(op, &a, &b, &array),
            l.gate(e, true),
            r.gate(c, false),
        ));
    }
    if terms.is_empty() {
        return "false".to_string();
    }
    let body = format!("({})", terms.join(" OR "));
    match (l.bind, r.bind) {
        (false, false) => body,
        (true, false) => format!("arrayExists({lv} -> {body}, [{}])", l.bound_value()),
        (false, true) => format!("arrayExists({rv} -> {body}, [{}])", r.bound_value()),
        (true, true) => format!(
            "arrayExists(({lv}, {rv}) -> {body}, [{}], [{}])",
            l.bound_value(),
            r.bound_value()
        ),
    }
}

/// `T-C25`: every pair of section 3.1's operands, with the part-3b design's
/// two resource operands, under each of the six operators — 1,176 cells —
/// compiles to the text this file builds from its own copy of the rules,
/// with no demand. With no context a cell naming either resource operand
/// is refused and every other cell is the same text.
#[test]
fn t_c25_field_against_field_is_the_generated_cross_product() {
    let operands = ff_operands();
    assert_eq!(operands.len(), 14);
    let mut cells = 0usize;
    for l in &operands {
        for r in &operands {
            for op in FF_OPS {
                cells += 1;
                let expr = field_compare_expr(&l.field, op, &r.field);
                let want = ff_expected(l, op, r);
                let label = format!("{} {op} {}", l.field, r.field);
                let got = compile_span_predicate_in(&expr, &ctx())
                    .unwrap_or_else(|e| panic!("{label} must compile in a context: {e}"));
                assert_eq!(got.sql(), want, "{label}");
                assert!(got.demand_messages().is_empty(), "{label}: no demand");
                let bare = compile_span_predicate(&expr).map(|p| p.sql().to_string());
                if l.needs_ctx || r.needs_ctx {
                    assert_eq!(
                        bare,
                        Err(PlanError::UnsupportedField(
                            RESOURCE_NEEDS_WINDOW.to_string()
                        )),
                        "{label} with no context"
                    );
                } else {
                    assert_eq!(bare, Ok(want), "{label} with no context");
                }
            }
        }
    }
    assert_eq!(cells, 1_176);
}

/// `T-C26`: the part-3a design's section 3.5 texts, byte for byte.
#[test]
fn t_c26_the_measured_field_against_field_texts() {
    assert_eq!(
        rendered(r#"{ name != span.k }"#),
        "(coalesce(name != attrs.`k`.:String, false) OR \
         (notEmpty(attrs.`k`.:`Array(Nullable(String))`) AND \
         arrayAll(x -> coalesce(name != x, false), attrs.`k`.:`Array(Nullable(String))`)))"
    );
    assert_eq!(rendered(r#"{ status = span.k }"#), "false");
    assert_eq!(
        rendered(r#"{ status = status }"#),
        "(status_code = status_code)"
    );
    // The part-3b design's section 5.7, with `Q`, `V` and `G` spelled out
    // from `ctx()`.
    let k = "attrs.`k`";
    let sn = "attrs.`service%2Ename`";
    assert_eq!(
        rendered_in(r#"{ resource.service.name < instrumentation:version }"#),
        format!(
            "arrayExists(r1 -> (coalesce(if(service_type = 'string', service, NULL) < \
             scope_version, false) OR if({}, arrayExists(x -> coalesce(x < scope_version, \
             false), dynamicElement(r1, 'Array(Nullable(String))')), false)), [{}])",
            g_of(sn, "Array(Nullable(String))"),
            v_of(sn)
        )
    );
    assert_eq!(
        rendered_in(r#"{ resource.k < name }"#),
        format!(
            "arrayExists(r1 -> (if({}, coalesce(dynamicElement(r1, 'String') < name, false), \
             false) OR if({}, arrayExists(x -> coalesce(x < name, false), dynamicElement(r1, \
             'Array(Nullable(String))')), false)), [{}])",
            g_of(k, "String"),
            g_of(k, "Array(Nullable(String))"),
            v_of(k)
        )
    );
    assert_eq!(
        rendered_in(r#"{ duration = resource.service.name }"#),
        format!(
            "arrayExists(r2 -> (if({}, coalesce(duration_ns = dynamicElement(r2, 'Int64'), \
             false), false) OR if({}, coalesce(duration_ns = dynamicElement(r2, 'Float64'), \
             false), false) OR if({}, arrayExists(x -> coalesce(duration_ns = x, false), \
             dynamicElement(r2, 'Array(Nullable(Int64))')), false) OR if({}, arrayExists(x -> \
             coalesce(duration_ns = x, false), dynamicElement(r2, \
             'Array(Nullable(Float64))')), false)), [{}])",
            g_of(sn, "Int64"),
            g_of(sn, "Float64"),
            g_of(sn, "Array(Nullable(Int64))"),
            g_of(sn, "Array(Nullable(Float64))"),
            v_of(sn)
        )
    );
    assert_eq!(
        rendered_in(r#"{ resource.service.name = name }"#),
        format!(
            "arrayExists(r1 -> (coalesce(if(service_type = 'string', service, NULL) = name, \
             false) OR if({}, arrayExists(x -> coalesce(x = name, false), dynamicElement(r1, \
             'Array(Nullable(String))')), false)), [{}])",
            g_of(sn, "Array(Nullable(String))"),
            v_of(sn)
        )
    );
    assert_eq!(rendered_in(r#"{ resource.k = status }"#), "false");
    assert_eq!(
        rendered(r#"{ duration > span.k }"#),
        "(coalesce(duration_ns > attrs.`k`.:Int64, false) OR \
         coalesce(duration_ns > attrs.`k`.:Float64, false) OR \
         arrayExists(x -> coalesce(duration_ns > x, false), attrs.`k`.:`Array(Nullable(Int64))`) OR \
         arrayExists(x -> coalesce(duration_ns > x, false), attrs.`k`.:`Array(Nullable(Float64))`))"
    );
    assert_eq!(
        rendered(r#"{ span.k = span.j }"#),
        "(coalesce(attrs.`k`.:String = attrs.`j`.:String, false) OR \
         coalesce(attrs.`k`.:Int64 = attrs.`j`.:Int64, false) OR \
         coalesce(attrs.`k`.:Int64 = attrs.`j`.:Float64, false) OR \
         coalesce(attrs.`k`.:Float64 = attrs.`j`.:Int64, false) OR \
         coalesce(attrs.`k`.:Float64 = attrs.`j`.:Float64, false) OR \
         coalesce(attrs.`k`.:Bool = attrs.`j`.:Bool, false) OR \
         arrayExists(x -> coalesce(attrs.`k`.:String = x, false), attrs.`j`.:`Array(Nullable(String))`) OR \
         arrayExists(x -> coalesce(attrs.`k`.:Int64 = x, false), attrs.`j`.:`Array(Nullable(Int64))`) OR \
         arrayExists(x -> coalesce(attrs.`k`.:Int64 = x, false), attrs.`j`.:`Array(Nullable(Float64))`) OR \
         arrayExists(x -> coalesce(attrs.`k`.:Float64 = x, false), attrs.`j`.:`Array(Nullable(Int64))`) OR \
         arrayExists(x -> coalesce(attrs.`k`.:Float64 = x, false), attrs.`j`.:`Array(Nullable(Float64))`) OR \
         arrayExists(x -> coalesce(attrs.`k`.:Bool = x, false), attrs.`j`.:`Array(Nullable(Bool))`) OR \
         arrayExists(x -> coalesce(x = attrs.`j`.:String, false), attrs.`k`.:`Array(Nullable(String))`) OR \
         arrayExists(x -> coalesce(x = attrs.`j`.:Int64, false), attrs.`k`.:`Array(Nullable(Int64))`) OR \
         arrayExists(x -> coalesce(x = attrs.`j`.:Int64, false), attrs.`k`.:`Array(Nullable(Float64))`) OR \
         arrayExists(x -> coalesce(x = attrs.`j`.:Float64, false), attrs.`k`.:`Array(Nullable(Int64))`) OR \
         arrayExists(x -> coalesce(x = attrs.`j`.:Float64, false), attrs.`k`.:`Array(Nullable(Float64))`) OR \
         arrayExists(x -> coalesce(x = attrs.`j`.:Bool, false), attrs.`k`.:`Array(Nullable(Bool))`))"
    );
}

/// `T-C27`: the part-3a design's section 3.6 refusals, whole strings, in
/// their order — the regex operator first, then the left operand, then the
/// right.
#[test]
fn t_c27_the_field_against_field_refusals() {
    let span_a = scoped(AttrScope::Span, "a");
    let refused_bare = |expr: &FieldExpr| match compile_span_predicate(expr) {
        Err(e) => e,
        Ok(p) => panic!("{expr} must be refused, it compiled to `{}`", p.sql()),
    };
    let refused_in = |expr: &FieldExpr| match compile_span_predicate_in(expr, &ctx()) {
        Err(e) => e,
        Ok(p) => panic!("{expr} must be refused, it compiled to `{}`", p.sql()),
    };
    let check = |expr: FieldExpr, want: PlanError| {
        assert_eq!(refused_bare(&expr), want, "{expr} with no context");
        assert_eq!(refused_in(&expr), want, "{expr} in a context");
    };

    for (scope, key) in [
        (AttrScope::Event, "k"),
        (AttrScope::Link, "k"),
        (AttrScope::Unscoped, "k"),
    ] {
        let want = PlanError::UnsupportedField(format!(
            "a field-against-field comparison with a \"{scope}\" operand is not supported by \
             the span-scope predicate compiler yet (issue #589 part 3d)"
        ));
        let f = scoped(scope, key);
        check(
            field_compare_expr(&f, ComparisonOp::Eq, &span_a),
            want.clone(),
        );
        check(field_compare_expr(&span_a, ComparisonOp::Eq, &f), want);
    }

    for intrinsic in [
        Intrinsic::EventName,
        Intrinsic::EventTimeSinceStart,
        Intrinsic::LinkSpanId,
        Intrinsic::LinkTraceId,
    ] {
        let want = PlanError::UnsupportedField(format!(
            "a field-against-field comparison with {intrinsic} is not supported by the \
             span-scope predicate compiler yet (issue #589 part 3d)"
        ));
        let f = Field::Intrinsic(intrinsic);
        check(
            field_compare_expr(&f, ComparisonOp::Eq, &span_a),
            want.clone(),
        );
        check(field_compare_expr(&span_a, ComparisonOp::Eq, &f), want);
    }

    let mut nested_and_trace = 0usize;
    for intrinsic in Intrinsic::ALL.iter().copied() {
        if intrinsic_target(intrinsic) != Some("#594") {
            continue;
        }
        nested_and_trace += 1;
        let want = PlanError::UnsupportedField(format!(
            "{intrinsic} is not supported by the span-scope predicate compiler yet (issue #594)"
        ));
        let f = Field::Intrinsic(intrinsic);
        check(
            field_compare_expr(&f, ComparisonOp::Eq, &span_a),
            want.clone(),
        );
        check(field_compare_expr(&span_a, ComparisonOp::Eq, &f), want);
    }
    assert_eq!(nested_and_trace, 7);

    let event_k = scoped(AttrScope::Event, "k");
    let nested_left = Field::Intrinsic(Intrinsic::NestedSetLeft);
    check(
        field_compare_expr(&event_k, ComparisonOp::Eq, &nested_left),
        PlanError::UnsupportedField(
            "a field-against-field comparison with a \"event.\" operand is not supported by \
             the span-scope predicate compiler yet (issue #589 part 3d)"
                .to_string(),
        ),
    );
    check(
        field_compare_expr(&nested_left, ComparisonOp::Eq, &event_k),
        PlanError::UnsupportedField(
            "nestedSetLeft is not supported by the span-scope predicate compiler yet (issue #594)"
                .to_string(),
        ),
    );

    let span_b = scoped(AttrScope::Span, "b");
    for op in [ComparisonOp::Re, ComparisonOp::Nre] {
        let regex = PlanError::TypeMismatch(
            "a field-against-field comparison does not support regex operators".to_string(),
        );
        check(field_compare_expr(&span_a, op, &span_b), regex.clone());
        // The operator is checked before either operand.
        check(field_compare_expr(&event_k, op, &nested_left), regex);
    }
}

// =====================================================================
// Issue #589 part 3b — field against field with a resource operand
// =====================================================================

/// `T-C28`: a comparison reading a resource operand carries its window,
/// so composing it with another window panics, as `T-C15`'s literal leaf
/// does. `resource.service.name` is one: its non-string arms read the
/// resource row.
#[test]
#[should_panic(expected = "different window")]
fn t_c28_a_service_name_operand_refuses_another_window() {
    let p = compile_span_predicate_in(
        &filter_body(r#"{ span.b = resource.service.name }"#),
        &ctx(),
    )
    .expect("compiles");
    let other = WindowSql::start_closed_end_open(T_B1_START, T_B1_END + 1);
    let _ = span_membership_sql("spans", other, &p);
}

#[test]
#[should_panic(expected = "different window")]
fn t_c28_a_resource_operand_refuses_another_window() {
    let p = compile_span_predicate_in(&filter_body(r#"{ resource.a = span.b }"#), &ctx())
        .expect("compiles");
    let other = WindowSql::start_closed_end_open(T_B1_START, T_B1_END + 1);
    let _ = span_membership_sql("spans", other, &p);
}

// =====================================================================
// Issue #589 part 3c — sides that are expressions
// =====================================================================

/// The design's section 5.3 `NI` and `NF`.
const AX_NI: &str = "CAST(NULL, 'Nullable(Int256)')";
const AX_NF: &str = "CAST(NULL, 'Nullable(Float64)')";

/// One arithmetic value as this file builds it from its own copy of
/// section 5.3's tables: the tuple text, and whether a duration is in it.
#[derive(Clone)]
struct Ax {
    text: String,
    dur: bool,
}

/// A span or instrumentation attribute leaf at `root`.
fn ax_attr(root: &str, key: &str) -> Ax {
    let p = format!("{root}.`{key}`");
    Ax {
        text: format!("tuple(toInt256({p}.:Int64), {p}.:Float64)"),
        dur: false,
    }
}

fn ax_duration() -> Ax {
    Ax {
        text: format!("tuple(toInt256(duration_ns), {AX_NF})"),
        dur: true,
    }
}

/// An integer constant: `digits` is `I(v)`.
fn ax_int(digits: &str, dur: bool) -> Ax {
    Ax {
        text: format!("tuple(toInt256({digits}), {AX_NF})"),
        dur,
    }
}

/// A float constant: `rendered` is `render_num(x)`.
fn ax_float(rendered: &str) -> Ax {
    Ax {
        text: format!("tuple({AX_NI}, toFloat64('{rendered}'))"),
        dur: false,
    }
}

/// A resource attribute leaf, its value `V(k)` spelled out from [`ctx`].
fn ax_resource(key: &str) -> Ax {
    Ax {
        text: format!(
            "arrayElement(arrayMap(pv -> tuple(toInt256(dynamicElement(pv, 'Int64')), \
             dynamicElement(pv, 'Float64')), [{}]), 1)",
            v_of(&ff_resource_path(key))
        ),
        dur: false,
    }
}

/// `NEG(x)`.
fn ax_neg_int(x: &str) -> String {
    format!("if({x} != 0 AND negate({x}) = {x}, NULL, negate({x}))")
}

/// A binary node, `op` one of `+ - * / % ^`.
fn ax_bin(op: &str, l: &Ax, r: &Ax) -> Ax {
    let (l1, l2) = ("tupleElement(pl, 1)", "tupleElement(pl, 2)");
    let (r1, r2) = ("tupleElement(pr, 1)", "tupleElement(pr, 2)");
    let fl = format!("coalesce({l2}, toFloat64({l1}))");
    let fr = format!("coalesce({r2}, toFloat64({r1}))");
    let h = format!("{l2} IS NULL AND {r2} IS NULL");
    let dur = l.dur || r.dur;
    let (i, f) = match op {
        "+" => (
            format!(
                "if(({l1} > 0 AND {r1} > 0 AND {l1} + {r1} <= 0) OR ({l1} < 0 AND {r1} < 0 AND \
                 {l1} + {r1} >= 0), NULL, {l1} + {r1})"
            ),
            format!("if({h}, NULL, {fl} + {fr})"),
        ),
        "-" => (
            format!(
                "if(({l1} >= 0 AND {r1} < 0 AND {l1} - {r1} < 0) OR ({l1} < 0 AND {r1} > 0 AND \
                 {l1} - {r1} >= 0), NULL, {l1} - {r1})"
            ),
            format!("if({h}, NULL, {fl} - {fr})"),
        ),
        "*" => (
            format!(
                "if(abs(toFloat64({l1} * {r1}) - toFloat64({l1}) * toFloat64({r1})) <= 1e-6 * \
                 abs(toFloat64({l1}) * toFloat64({r1})), {l1} * {r1}, NULL)"
            ),
            format!("if({h}, NULL, {fl} * {fr})"),
        ),
        "/" if dur => (AX_NI.to_string(), format!("{fl} / {fr}")),
        "/" => (
            format!(
                "multiIf({r1} = 0, NULL, {r1} = -1, {}, intDiv({l1}, if({r1} = 0 OR {r1} = -1, \
                 1, {r1})))",
                ax_neg_int(l1)
            ),
            format!("if({h}, NULL, {fl} / {fr})"),
        ),
        "%" => (
            format!(
                "multiIf({r1} = 0, NULL, {r1} = -1, {l1} * 0, {l1} % if({r1} = 0 OR {r1} = -1, \
                 1, {r1}))"
            ),
            format!("if({h}, NULL, {fl} % {fr})"),
        ),
        "^" => {
            let pf = format!("pow(toFloat64({l1}), toFloat64({r1}))");
            let fold = format!(
                "tupleElement(arrayFold((pa, pk) -> (if(bitTest(toUInt8(assumeNotNull({r1})), \
                 pk), tupleElement(pa, 1) * tupleElement(pa, 2), tupleElement(pa, 1)), \
                 tupleElement(pa, 2) * tupleElement(pa, 2)), range(8), (toInt256(1), \
                 assumeNotNull({l1}))), 1)"
            );
            (
                format!(
                    "if({l1} IS NULL OR {r1} IS NULL OR {r1} < 0, NULL, if({r1} >= 256, \
                     multiIf({l1} = 0, 0, {l1} = 1, 1, {l1} = -1, if({r1} % 2 = 0, 1, -1), \
                     NULL), arrayElement(arrayMap(pq -> if(isFinite({pf}) AND \
                     abs(toFloat64(pq) - {pf}) <= 1e-6 * abs({pf}), pq, NULL), [{fold}]), 1)))"
                ),
                format!("if({h} AND coalesce({r1} >= 0, false), NULL, pow({fl}, {fr}))"),
            )
        }
        other => panic!("not an arithmetic operator: {other}"),
    };
    Ax {
        text: format!(
            "arrayElement(arrayMap((pl, pr) -> tuple({i}, {f}), [{}], [{}]), 1)",
            l.text, r.text
        ),
        dur,
    }
}

/// `<node> <sym> <literal>` with the node the left side, bound to `a1`, and
/// the literal an integer constant: section 5.2's arms through part 3a's
/// terms.
fn ax_compare_left(node: &Ax, sym: &str, literal: &str) -> String {
    format!(
        "arrayExists(a1 -> (coalesce(tupleElement(a1, 1) {sym} {literal}, false) OR \
         coalesce(tupleElement(a1, 2) {sym} {literal}, false)), [{}])",
        node.text
    )
}

const AX_OPS: [&str; 6] = ["+", "-", "*", "/", "%", "^"];

/// `T-C29`: every ordered pair of section 9's five operands with at least
/// one field, under each arithmetic operator, in `{ <L> <op> <R> > 3 }` —
/// 126 cells — compiles to the text this file builds from its own copy of
/// section 5.3, with no demand.
#[test]
fn t_c29_arithmetic_is_the_generated_cross_product() {
    let operands: [(&str, Ax, bool); 5] = [
        ("span.a", ax_attr("attrs", "a"), true),
        ("instrumentation.a", ax_attr("scope_attrs", "a"), true),
        ("duration", ax_duration(), true),
        ("2", ax_int("2", false), false),
        ("0.5", ax_float("0.5"), false),
    ];
    let mut cells = 0usize;
    for (lq, la, lf) in &operands {
        for (rq, ra, rf) in &operands {
            if !lf && !rf {
                continue;
            }
            for op in AX_OPS {
                cells += 1;
                let query = format!("{{ {lq} {op} {rq} > 3 }}");
                let p = compile_span_predicate(&filter_body(&query))
                    .unwrap_or_else(|e| panic!("{query} must compile: {e}"));
                assert_eq!(
                    p.sql(),
                    ax_compare_left(&ax_bin(op, la, ra), ">", "3"),
                    "{query}"
                );
                assert!(p.demand_messages().is_empty(), "{query}: no demand");
            }
        }
    }
    assert_eq!(cells, 126);
}

/// `T-C30`: section 9's fixed texts, byte for byte.
#[test]
fn t_c30_the_measured_arithmetic_texts() {
    assert_eq!(
        rendered(r#"{ -span.a > 0 }"#),
        "arrayExists(a1 -> (coalesce(tupleElement(a1, 1) > 0, false) OR \
         coalesce(tupleElement(a1, 2) > 0, false)), [arrayElement(arrayMap(pl -> \
         tuple(if(tupleElement(pl, 1) != 0 AND negate(tupleElement(pl, 1)) = tupleElement(pl, 1), \
         NULL, negate(tupleElement(pl, 1))), negate(tupleElement(pl, 2))), \
         [tuple(toInt256(attrs.`a`.:Int64), attrs.`a`.:Float64)]), 1)])"
    );
    assert_eq!(
        rendered(r#"{ duration / 1ms > 1 }"#),
        "arrayExists(a1 -> (coalesce(tupleElement(a1, 1) > 1, false) OR \
         coalesce(tupleElement(a1, 2) > 1, false)), [arrayElement(arrayMap((pl, pr) -> \
         tuple(CAST(NULL, 'Nullable(Int256)'), coalesce(tupleElement(pl, 2), \
         toFloat64(tupleElement(pl, 1))) / coalesce(tupleElement(pr, 2), \
         toFloat64(tupleElement(pr, 1)))), [tuple(toInt256(duration_ns), CAST(NULL, \
         'Nullable(Float64)'))], [tuple(toInt256(1000000), CAST(NULL, \
         'Nullable(Float64)'))]), 1)])"
    );
    let (a, b) = (ax_attr("attrs", "a"), ax_attr("attrs", "b"));
    assert_eq!(
        rendered(r#"{ span.a / span.b = -3 }"#),
        ax_compare_left(&ax_bin("/", &a, &b), "=", "-3")
    );
    assert_eq!(
        rendered(r#"{ span.a ^ 2 > 8 }"#),
        ax_compare_left(&ax_bin("^", &a, &ax_int("2", false)), ">", "8")
    );
    assert_eq!(
        rendered_in(r#"{ span.a * resource.r > 7 }"#),
        ax_compare_left(&ax_bin("*", &a, &ax_resource("r")), ">", "7")
    );
    assert_eq!(
        rendered(r#"{ !span.f = span.g }"#),
        "(throwIf(dynamicType(attrs.`f`) != 'None' AND dynamicType(attrs.`f`) != 'Bool', \
         'expression (!span.f) expected a boolean') + toUInt8((coalesce((NOT attrs.`f`.:Bool) = \
         attrs.`g`.:Bool, false) OR arrayExists(x -> coalesce((NOT attrs.`f`.:Bool) = x, false), \
         attrs.`g`.:`Array(Nullable(Bool))`)))) = 1"
    );
}

/// `T-C31`: a literal-only side folds to the constant it computes, and a
/// folded integer division or modulo by zero is `false`.
#[test]
fn t_c31_a_literal_only_side_folds() {
    for (folded, plain) in [
        (r#"{ span.a = 2 - 1 }"#, r#"{ span.a = 1 }"#),
        (r#"{ span.a = -7 / 2 }"#, r#"{ span.a = -3 }"#),
        (r#"{ span.a = -7 % 2 }"#, r#"{ span.a = -1 }"#),
        (r#"{ span.a = 5.0 / 2 }"#, r#"{ span.a = 2.5 }"#),
        (r#"{ span.a > 1ms + 1ms }"#, r#"{ span.a > 2000000 }"#),
    ] {
        assert_eq!(rendered(folded), rendered(plain), "{folded}");
    }
    assert_eq!(
        rendered_in(r#"{ .a = 2 ^ 3 }"#),
        rendered_in(r#"{ .a = 8 }"#)
    );
    for query in [
        r#"{ span.a = 1 / 0 }"#,
        r#"{ span.a = 1 % 0 }"#,
        r#"{ span.a + 1 / 0 > 0 }"#,
    ] {
        assert_eq!(rendered(query), "false", "{query}");
    }
    let duration_by_zero = rendered(r#"{ span.a < 1ms / 0 }"#);
    assert_ne!(duration_by_zero, "false");
    assert!(
        duration_by_zero.contains("tupleElement(a2, 2)"),
        "a duration's zero divisor is the run-time infinity: {duration_by_zero}"
    );
}

/// The cap's refusal, section 5.6.
fn cap_refusal() -> PlanError {
    PlanError::UnsupportedField(
        "an arithmetic expression of more than 64 operations is not supported".to_string(),
    )
}

/// `{ span.a + span.a + … }` with `nodes` operators.
fn span_chain(nodes: usize) -> String {
    vec!["span.a"; nodes + 1].join(" + ")
}

/// `T-C32`: a deferred operand is refused before a zero divisor folds to
/// `false`; folding comes before the refusal for a lone field; the cap
/// counts across the predicate and not a folded subtree.
#[test]
fn t_c32_the_order_of_the_rules_and_the_cap() {
    match refusal(r#"{ 1 / 0 = event.a }"#) {
        PlanError::UnsupportedField(msg) => assert!(
            msg.contains("event.") && msg.ends_with("(issue #589 part 3d)"),
            "the event operand's refusal, got {msg:?}"
        ),
        other => panic!("must be UnsupportedField, got {other:?}"),
    }
    assert_eq!(
        rendered(r#"{ event.a = 1 + 1 }"#),
        rendered(r#"{ event.a = 2 }"#)
    );
    let q64 = format!("{{ {} > 0 }}", span_chain(64));
    assert!(
        compile_span_predicate(&filter_body(&q64)).is_ok(),
        "64 operations compile"
    );
    let q65 = format!("{{ {} > 0 }}", span_chain(65));
    assert_eq!(refusal(&q65), cap_refusal(), "65 operations");
    let split = format!("{{ {} > 0 && {} > 0 }}", span_chain(33), span_chain(32));
    assert_eq!(refusal(&split), cap_refusal(), "33 and 32 operations");
    let ones = vec!["1"; 200].join(" + ");
    assert_eq!(
        rendered(&format!("{{ span.a = {ones} }}")),
        rendered(r#"{ span.a = 200 }"#),
        "a folded chain is not counted"
    );
}

/// Part 1's numeric pair over `n`.
fn numeric_pair_text(op: &str, n: &str) -> String {
    format!(
        "(coalesce(attrs.`a`.:Int64 {op} {n}, false) OR coalesce(attrs.`a`.:Float64 {op} {n}, \
         false))"
    )
}

/// `T-C33`: the literal pairs, and the literals at the 64- and 128-bit
/// edges.
#[test]
fn t_c33_literal_pairs_and_the_integer_boundaries() {
    assert_eq!(
        rendered(r#"{ "abc" =~ "a.*" }"#),
        "match('abc', '^(?:a.*)$')"
    );
    assert_eq!(
        rendered(r#"{ "abc" !~ "a.*" }"#),
        "NOT match('abc', '^(?:a.*)$')"
    );
    assert_eq!(
        rendered(r#"{ "a\nb" =~ "a.b" }"#),
        "match('a\\nb', '^(?:a.b)$')"
    );
    for pattern in ["(", "(?=a)"] {
        let got = refusal(&format!(r#"{{ "abc" =~ "{pattern}" }}"#));
        let leaf = refusal(&format!(r#"{{ name =~ "{pattern}" }}"#));
        assert_eq!(got, leaf, "{pattern}: the leaf's own error");
        assert!(
            matches!(&got, PlanError::TypeMismatch(m) if m.starts_with("invalid regex")),
            "{pattern}: {got:?}"
        );
    }
    for (query, text) in [
        (r#"{ "b" < "a" }"#, "('b' < 'a')"),
        (r#"{ 1s = 1000000000 }"#, "(1000000000 = 1000000000)"),
        (
            r#"{ 9007199254740993 = 9007199254740992.0 }"#,
            "(9007199254740993 = toFloat64('9007199254740992'))",
        ),
        (r#"{ ok != error }"#, "(1 != 2)"),
        (
            r#"{ 18446744073709551615ns = 18446744073709551615ns }"#,
            "(18446744073709551615 = 18446744073709551615)",
        ),
        (
            r#"{ 18446744073709551615ns + 1ns > 18446744073709551615ns }"#,
            "(toInt256('18446744073709551616') > 18446744073709551615)",
        ),
        (
            r#"{ 170141183460469231731687303715884105727 = 170141183460469231731687303715884105727 }"#,
            "(toInt256('170141183460469231731687303715884105727') = \
             toInt256('170141183460469231731687303715884105727'))",
        ),
        (
            r#"{ -170141183460469231731687303715884105728 < -170141183460469231731687303715884105727 }"#,
            "(toInt256('-170141183460469231731687303715884105728') < \
             toInt256('-170141183460469231731687303715884105727'))",
        ),
        (
            r#"{ 340282366920938463463374607431768211455 > 340282366920938463463374607431768211454 }"#,
            "(toInt256('340282366920938463463374607431768211455') > \
             toInt256('340282366920938463463374607431768211454'))",
        ),
        (
            r#"{ duration < 18446744073709551615ns }"#,
            "duration_ns < 18446744073709551615",
        ),
        (
            r#"{ event:timeSinceStart < 18446744073709551615ns }"#,
            "arrayExists(t -> toInt128(t) - start_ns < 18446744073709551615, events.time_ns)",
        ),
        (
            r#"{ event:timeSinceStart = 18446744073709551615 }"#,
            "arrayExists(t -> toInt128(t) - start_ns = 18446744073709551615, events.time_ns)",
        ),
    ] {
        assert_eq!(rendered(query), text, "{query}");
    }
    for (query, op, n) in [
        (
            r#"{ span.a = 340282366920938463463374607431768211455 }"#,
            "=",
            "toInt256('340282366920938463463374607431768211455')",
        ),
        (
            r#"{ span.a = -170141183460469231731687303715884105728 }"#,
            "=",
            "toInt256('-170141183460469231731687303715884105728')",
        ),
        (
            r#"{ span.a = 170141183460469231731687303715884105727 }"#,
            "=",
            "toInt256('170141183460469231731687303715884105727')",
        ),
        (
            r#"{ span.a = -(170141183460469231731687303715884105728) }"#,
            "=",
            "toInt256('-170141183460469231731687303715884105728')",
        ),
        (
            r#"{ span.a < 18446744073709551615ns }"#,
            "<",
            "18446744073709551615",
        ),
        (r#"{ span.a = -1 }"#, "=", "-1"),
    ] {
        assert_eq!(rendered(query), numeric_pair_text(op, n), "{query}");
    }
    for (query, literal) in [
        (
            r#"{ 340282366920938463463374607431768211456 = 1 }"#,
            "340282366920938463463374607431768211456",
        ),
        (
            r#"{ -170141183460469231731687303715884105729 = 1 }"#,
            "-170141183460469231731687303715884105729",
        ),
        (
            r#"{ span.a = 340282366920938463463374607431768211456 }"#,
            "340282366920938463463374607431768211456",
        ),
        (
            r#"{ span.a < -170141183460469231731687303715884105729 }"#,
            "-170141183460469231731687303715884105729",
        ),
        (
            r#"{ span.a + 340282366920938463463374607431768211456 > 0 }"#,
            "340282366920938463463374607431768211456",
        ),
        (
            r#"{ span.a + -170141183460469231731687303715884105729 > 0 }"#,
            "-170141183460469231731687303715884105729",
        ),
    ] {
        assert_eq!(
            refusal(query),
            PlanError::TypeMismatch(format!("integer literal out of range: {literal}")),
            "{query}"
        );
    }
    // The sign positions: a negative operand, a nested negation and a
    // parenthesised literal fold as the signed value.
    for (folded, plain) in [
        (r#"{ span.a = 3 - -1 }"#, r#"{ span.a = 4 }"#),
        (r#"{ span.a = - -1 }"#, r#"{ span.a = 1 }"#),
        (r#"{ span.a = -(1) }"#, r#"{ span.a = -1 }"#),
    ] {
        assert_eq!(rendered(folded), rendered(plain), "{folded}");
    }
    let negative_operand = rendered(r#"{ span.a - -1 > 0 }"#);
    assert_eq!(
        negative_operand,
        ax_compare_left(
            &ax_bin("-", &ax_attr("attrs", "a"), &ax_int("-1", false)),
            ">",
            "0"
        )
    );
    let negative_zero = rendered(r#"{ span.a > 1 / -0.0 }"#);
    assert!(
        negative_zero.contains("toFloat64('-0')"),
        "the sign of zero is kept: {negative_zero}"
    );
}
