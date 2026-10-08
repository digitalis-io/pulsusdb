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
use pulsus_read::traces::spans::projection::Projection;
use pulsus_read::traces::spans::search::{SearchFilter, compile_search, search_sql};
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

    // The expression-level constructs part 3e serves now compile; `.a`
    // needs the window, so every row is compiled in a context.
    for query in [
        r#"{ event.a = span.b + 1 }"#,
        r#"{ event.a + 1 = 2 }"#,
        r#"{ !event.a = span.b }"#,
        r#"{ .a + 1 = 2 }"#,
    ] {
        let got =
            compile_span_predicate_in(&filter_body(query), &ctx()).map(|p| p.sql().to_string());
        assert!(got.is_ok(), "{query} must compile: {got:?}");
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

/// A scope's presence of `key` as the chain tests it, in [`ctx`] — written
/// out, not compiled, so a change to the compiled presence moves only the
/// compiled side of an assertion. `T-C24` freezes the same five texts.
fn presence_fixed(scope: AttrScope, key: &str) -> String {
    let k = key.replace('.', "%2E");
    match scope {
        AttrScope::Span => format!("dynamicType(attrs.`{k}`) != 'None'"),
        AttrScope::Resource => r_of(&format!("dynamicType(attrs.`{k}`) != 'None'")),
        AttrScope::Event => {
            format!("arrayExists(d -> dynamicType(d) != 'None', events.attrs.`{k}`)")
        }
        AttrScope::Link => format!("arrayExists(d -> dynamicType(d) != 'None', links.attrs.`{k}`)"),
        AttrScope::Instrumentation => format!("dynamicType(scope_attrs.`{k}`) != 'None'"),
        AttrScope::Unscoped => panic!("the chain has no presence of its own"),
    }
}

/// `!<e>.key`'s match, `want` false: part 2's any element, or with `all`
/// part 3d's decision 7 — the key held and no element `true`.
fn not_element_match(e: &str, key: &str, all: bool) -> String {
    let path = format!("{e}.attrs.`{}`", key.replace('.', "%2E"));
    if all {
        format!(
            "(arrayExists(d -> dynamicType(d) != 'None', {path}) AND NOT arrayExists(b -> \
             coalesce(b = true, false), {path}.:Bool))"
        )
    } else {
        format!("arrayExists(b -> coalesce(b = false, false), {path}.:Bool)")
    }
}

/// `!<e>.key`'s demand condition: some element holds a non-boolean.
fn not_element_demand(e: &str, key: &str) -> String {
    format!(
        "arrayExists(d -> dynamicType(d) != 'None' AND dynamicType(d) != 'Bool', {e}.attrs.`{}`)",
        key.replace('.', "%2E")
    )
}

/// `{ !<e>.key }` in full, `want` false, `all` as [`not_element_match`].
fn not_element_text(e: &str, scope: AttrScope, key: &str, all: bool) -> String {
    format!(
        "(throwIf({}, 'expression (!{scope}{key}) expected a boolean') + toUInt8({})) = 1",
        not_element_demand(e, key),
        not_element_match(e, key, all)
    )
}

/// `{ !.key }` in full in [`ctx`], `want` false: section 6 of part 2's
/// `multiIf` over each scope's match and over each scope's demand, the
/// event and link matches every element with `all` (part 3d's decision 7).
/// `key` carries no `.`, so the resource scope is not `service.name`'s.
fn not_chain_text(key: &str, all: bool) -> String {
    assert!(!key.contains('.'), "service.name's chain is T-C23's own");
    let t_span = format!("coalesce(attrs.`{key}`.:Bool = false, false)");
    let t_in = format!("coalesce(scope_attrs.`{key}`.:Bool = false, false)");
    let p = |scope: AttrScope| presence_fixed(scope, key);
    let t = format!(
        "multiIf({}, {t_span}, {}, {}, {}, {}, {}, {}, {}, {t_in}, false)",
        p(AttrScope::Span),
        p(AttrScope::Resource),
        r_of(&t_span),
        p(AttrScope::Event),
        not_element_match("events", key, all),
        p(AttrScope::Link),
        not_element_match("links", key, all),
        p(AttrScope::Instrumentation),
    );
    let c = not_chain_demand(key);
    format!("(throwIf({c}, 'expression (!.{key}) expected a boolean') + toUInt8({t})) = 1")
}

/// `!.key`'s demand condition in [`ctx`]: each scope's, chosen by the
/// chain. `key` carries no `.`.
fn not_chain_demand(key: &str) -> String {
    let c_span =
        format!("dynamicType(attrs.`{key}`) != 'None' AND dynamicType(attrs.`{key}`) != 'Bool'");
    let c_in = format!(
        "dynamicType(scope_attrs.`{key}`) != 'None' AND dynamicType(scope_attrs.`{key}`) != 'Bool'"
    );
    let p = |scope: AttrScope| presence_fixed(scope, key);
    format!(
        "multiIf({}, {c_span}, {}, {}, {}, {}, {}, {}, {}, {c_in}, false)",
        p(AttrScope::Span),
        p(AttrScope::Resource),
        r_of(&c_span),
        p(AttrScope::Event),
        not_element_demand("events", key),
        p(AttrScope::Link),
        not_element_demand("links", key),
        p(AttrScope::Instrumentation),
    )
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

    assert_eq!(rendered_in(r#"{ !.k }"#), not_chain_text("k", false));

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
    let Some(body) = ff_body(l, op, r) else {
        return "false".to_string();
    };
    match (l.bind, r.bind) {
        (false, false) => body,
        (true, false) => format!("arrayExists(r1 -> {body}, [{}])", l.bound_value()),
        (false, true) => format!("arrayExists(r2 -> {body}, [{}])", r.bound_value()),
        (true, true) => format!(
            "arrayExists((r1, r2) -> {body}, [{}], [{}])",
            l.bound_value(),
            r.bound_value()
        ),
    }
}

/// [`ff_expected`]'s terms, ORed and unbound; `None` when there is no
/// term. Part 3d's section 5.2 reuses it for an element against a scalar.
fn ff_body(l: &FfOperand, op: ComparisonOp, r: &FfOperand) -> Option<String> {
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
        return None;
    }
    Some(format!("({})", terms.join(" OR ")))
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

    // An event set opposite a #594 intrinsic: the intrinsic's own refusal,
    // whichever side it is on (part 3d's section 5.6).
    let event_k = scoped(AttrScope::Event, "k");
    let nested_left = Field::Intrinsic(Intrinsic::NestedSetLeft);
    let nested_refusal = PlanError::UnsupportedField(
        "nestedSetLeft is not supported by the span-scope predicate compiler yet (issue #594)"
            .to_string(),
    );
    check(
        field_compare_expr(&event_k, ComparisonOp::Eq, &nested_left),
        nested_refusal.clone(),
    );
    check(
        field_compare_expr(&nested_left, ComparisonOp::Eq, &event_k),
        nested_refusal,
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

/// `T-C32`: a zero divisor folds the comparison to `false`, a set operand
/// beside it included; folding comes before anything else for a lone
/// field; the cap counts across the predicate and not a folded subtree.
#[test]
fn t_c32_the_order_of_the_rules_and_the_cap() {
    // A lone set opposite a side folding to no value is `false` (part 3d's
    // decision 8), and so is a set inside arithmetic beside one, under
    // every operator: the comparison is `false` before any set loop is
    // built, so an empty set's `!=` does not override it (part 3e's
    // decision 15).
    for query in [
        r#"{ 1 / 0 = event.a }"#,
        r#"{ 1 / 0 = event.a + 1 }"#,
        r#"{ event.a + 1 / 0 = 2 }"#,
        r#"{ event.a + 1 / 0 != 2 }"#,
        r#"{ 1 / 0 != event.a + 1 }"#,
    ] {
        assert_eq!(rendered(query), "false", "{query}");
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

// =====================================================================
// Issue #589 part 3d — field against field with event, link and unscoped
// operands
// =====================================================================

/// One of part 3d's seven set operands (section 5.1).
#[derive(Clone, Copy)]
enum SetKind {
    /// `event.k` or `link.k`: the `JSON` array's root, and the key.
    Attr(&'static str, &'static str),
    EventName,
    TimeSinceStart,
    LinkSpanId,
    LinkTraceId,
    /// `.k`.
    Chain(&'static str),
}

struct SetSpec {
    field: Field,
    kind: SetKind,
}

/// Every class an attribute element can hold: this file's class name and
/// the stored type it is read as (part 3b's `T_c`).
const ELEMENT_TYPES: [(&str, &str); 8] = [
    ("s", "String"),
    ("i", "Int64"),
    ("f", "Float64"),
    ("b", "Bool"),
    ("sa", "Array(Nullable(String))"),
    ("ia", "Array(Nullable(Int64))"),
    ("fa", "Array(Nullable(Float64))"),
    ("ba", "Array(Nullable(Bool))"),
];

/// The seven operands `T-C34` pairs with 3c's fourteen.
fn set_operands() -> Vec<SetSpec> {
    vec![
        SetSpec {
            field: scoped(AttrScope::Event, "a"),
            kind: SetKind::Attr("events.attrs", "a"),
        },
        SetSpec {
            field: scoped(AttrScope::Link, "a"),
            kind: SetKind::Attr("links.attrs", "a"),
        },
        SetSpec {
            field: Field::Intrinsic(Intrinsic::EventName),
            kind: SetKind::EventName,
        },
        SetSpec {
            field: Field::Intrinsic(Intrinsic::EventTimeSinceStart),
            kind: SetKind::TimeSinceStart,
        },
        SetSpec {
            field: Field::Intrinsic(Intrinsic::LinkSpanId),
            kind: SetKind::LinkSpanId,
        },
        SetSpec {
            field: Field::Intrinsic(Intrinsic::LinkTraceId),
            kind: SetKind::LinkTraceId,
        },
        SetSpec {
            field: scoped(AttrScope::Unscoped, "a"),
            kind: SetKind::Chain("a"),
        },
    ]
}

/// `C(k)`, section 5.4, for the chain at position `n`: every scope's value
/// tagged with its scope, the elements of the first scope holding `k` kept.
fn chain_c(key: &str, n: usize) -> String {
    chain_c_over(key, &format!("r{n}"))
}

/// [`chain_c`] reading its resource value from the variable `rv`: part
/// 3e binds a chain occurrence's value to `cr<k>`.
fn chain_c_over(key: &str, rv: &str) -> String {
    let k = key.replace('.', "%2E");
    let (span, ev, lk, sc) = (
        format!("attrs.`{k}`"),
        format!("events.attrs.`{k}`"),
        format!("links.attrs.`{k}`"),
        format!("scope_attrs.`{k}`"),
    );
    let (pr, values, rt) = if key == "service.name" {
        (
            format!("(service_type = 'string' OR dynamicType({rv}) != 'None')"),
            format!("[CAST(CAST(service, 'String'), 'Dynamic')], [{rv}]"),
            "[if(service_type = 'string', 2, 9)], [if(service_type = 'string', 9, 2)]".to_string(),
        )
    } else {
        (
            format!("dynamicType({rv}) != 'None'"),
            format!("[{rv}]"),
            "[2]".to_string(),
        )
    };
    let tag = format!(
        "multiIf(dynamicType({span}) != 'None', 1, {pr}, 2, arrayExists(d -> dynamicType(d) != \
         'None', {ev}), 3, arrayExists(d -> dynamicType(d) != 'None', {lk}), 4, \
         dynamicType({sc}) != 'None', 5, 0)"
    );
    format!(
        "arrayFilter((d, g) -> g = {tag} AND dynamicType(d) != 'None', arrayConcat([{span}], {values}, \
         {ev}, {lk}, [{sc}]), arrayConcat([1], {rt}, arrayMap(d -> 3, {ev}), arrayMap(d -> 4, \
         {lk}), [5]))"
    )
}

/// The per-class lists of section 5.3 over a `Dynamic` array `src`.
fn dynamic_lists(src: &str) -> Vec<(&'static str, String)> {
    ELEMENT_TYPES
        .iter()
        .map(|(class, t)| {
            (
                *class,
                format!("arrayMap(d -> dynamicElement(d, '{t}'), arrayFilter(d -> dynamicType(d) = '{t}', {src}))"),
            )
        })
        .collect()
}

impl SetSpec {
    fn needs_ctx(&self) -> bool {
        matches!(self.kind, SetKind::Chain(_))
    }

    /// `S`, section 5.1, at position `n`.
    fn array(&self, n: usize) -> String {
        match self.kind {
            SetKind::Attr(root, key) => {
                format!("arrayFilter(d -> dynamicType(d) != 'None', {root}.`{key}`)")
            }
            SetKind::EventName => "events.name".to_string(),
            SetKind::TimeSinceStart => {
                "arrayMap(t -> toInt128(t) - start_ns, events.time_ns)".to_string()
            }
            SetKind::LinkSpanId => "arrayMap(h -> lower(hex(h)), links.span_id)".to_string(),
            SetKind::LinkTraceId => "arrayMap(h -> lower(hex(h)), links.trace_id)".to_string(),
            SetKind::Chain(_) => format!("c{n}"),
        }
    }

    fn nullable(&self) -> bool {
        matches!(self.kind, SetKind::Attr(..) | SetKind::Chain(_))
    }

    /// The element `e<n>` as a 3c operand: its arms by class.
    fn element(&self, n: usize) -> FfOperand {
        let e = format!("e{n}");
        let arms = match self.kind {
            SetKind::Attr(..) | SetKind::Chain(_) => ELEMENT_TYPES
                .iter()
                .map(|(class, t)| (*class, format!("dynamicElement({e}, '{t}')")))
                .collect(),
            SetKind::EventName | SetKind::LinkSpanId | SetKind::LinkTraceId => vec![("s", e)],
            SetKind::TimeSinceStart => vec![("i", e)],
        };
        FfOperand {
            field: self.field.clone(),
            arms,
            nullable: self.nullable(),
            needs_ctx: self.needs_ctx(),
            types: None,
            s_on_span_row: false,
            bind: false,
        }
    }

    /// `|S|`, section 5.3.
    fn count(&self, n: usize) -> String {
        match self.kind {
            SetKind::Attr(root, key) => {
                format!("arrayCount(d -> dynamicType(d) != 'None', {root}.`{key}`)")
            }
            SetKind::EventName => "length(events.name)".to_string(),
            SetKind::TimeSinceStart => "length(events.time_ns)".to_string(),
            SetKind::LinkSpanId => "length(links.span_id)".to_string(),
            SetKind::LinkTraceId => "length(links.trace_id)".to_string(),
            SetKind::Chain(_) => format!("length(c{n})"),
        }
    }

    /// `S_c` for every class the set has, section 5.3.
    fn lists(&self, n: usize) -> Vec<(&'static str, String)> {
        match self.kind {
            SetKind::Attr(root, key) => dynamic_lists(&format!("{root}.`{key}`")),
            SetKind::Chain(_) => dynamic_lists(&format!("c{n}")),
            SetKind::EventName | SetKind::LinkSpanId | SetKind::LinkTraceId => {
                vec![("s", self.array(n))]
            }
            SetKind::TimeSinceStart => vec![("i", self.array(n))],
        }
    }

    /// A chain's presence; an event, link or intrinsic set has none.
    fn presence(&self, n: usize) -> Option<String> {
        match self.kind {
            SetKind::Chain(_) => Some(format!("notEmpty(c{n})")),
            _ => None,
        }
    }

    /// The chain's own variable and `C(k)`.
    fn chain(&self, n: usize) -> Option<(String, String)> {
        match self.kind {
            SetKind::Chain(key) => Some((format!("c{n}"), chain_c(key, n))),
            _ => None,
        }
    }

    /// The resource value the chain reads, bound to `r<n>`.
    fn bind(&self, n: usize) -> Option<(String, String)> {
        match self.kind {
            SetKind::Chain(key) => Some((format!("r{n}"), v_of(&ff_resource_path(key)))),
            _ => None,
        }
    }
}

/// A scalar's presence, section 5.1, at position `n`.
fn ff_presence(s: &FfOperand, n: usize) -> Option<String> {
    match &s.field {
        Field::Attribute {
            scope: AttrScope::Span,
            key,
        } => Some(format!("dynamicType(attrs.`{key}`) != 'None'")),
        Field::Attribute {
            scope: AttrScope::Instrumentation,
            key,
        } => Some(format!("dynamicType(scope_attrs.`{key}`) != 'None'")),
        Field::Attribute {
            scope: AttrScope::Resource,
            key,
        } if key == "service.name" => Some(format!(
            "(service_type = 'string' OR dynamicType(r{n}) != 'None')"
        )),
        Field::Attribute {
            scope: AttrScope::Resource,
            ..
        } => Some(format!("dynamicType(r{n}) != 'None'")),
        Field::Intrinsic(_) => None,
        other => panic!("{other} is not one of 3c's scalars"),
    }
}

/// One side of a part-3d comparison.
#[derive(Clone, Copy)]
enum Side<'a> {
    Scalar(&'a FfOperand),
    Set(&'a SetSpec),
}

impl Side<'_> {
    fn field(&self) -> &Field {
        match self {
            Side::Scalar(s) => &s.field,
            Side::Set(s) => &s.field,
        }
    }

    fn is_set(&self) -> bool {
        matches!(self, Side::Set(_))
    }

    /// The refusal the side gives with no context, if it needs one.
    fn needs_window(&self) -> Option<PlanError> {
        match self {
            Side::Scalar(s) if s.needs_ctx => Some(PlanError::UnsupportedField(
                RESOURCE_NEEDS_WINDOW.to_string(),
            )),
            Side::Set(s) if s.needs_ctx() => Some(PlanError::UnsupportedField(
                UNSCOPED_NEEDS_WINDOW.to_string(),
            )),
            _ => None,
        }
    }

    fn bind(&self, n: usize) -> Option<(String, String)> {
        match self {
            Side::Scalar(s) if s.bind => Some((format!("r{n}"), s.bound_value())),
            Side::Scalar(_) => None,
            Side::Set(s) => s.bind(n),
        }
    }
}

/// Section 5.2: one set against a scalar, the set at position `n`.
fn element_expected(set: &SetSpec, n: usize, op: ComparisonOp, scalar: &FfOperand) -> String {
    let element = set.element(n);
    let body = if n == 1 {
        ff_body(&element, op, scalar)
    } else {
        ff_body(scalar, op, &element)
    };
    // Decision 9: with no term, `= < <= > >=` match nothing and `!=` keeps
    // its every-element rule over a body of `false`.
    let body = match body {
        Some(body) => body,
        None if op == ComparisonOp::Neq => "false".to_string(),
        None => return "false".to_string(),
    };
    let (e, array) = (format!("e{n}"), set.array(n));
    if op != ComparisonOp::Neq {
        return format!("arrayExists({e} -> {body}, {array})");
    }
    let scalar_presence = ff_presence(scalar, 3 - n);
    let presences: Vec<String> = if n == 1 {
        [set.presence(n), scalar_presence]
    } else {
        [scalar_presence, set.presence(n)]
    }
    .into_iter()
    .flatten()
    .collect();
    let all = format!("arrayAll({e} -> {body}, {array})");
    if presences.is_empty() {
        all
    } else {
        format!("({} AND {all})", presences.join(" AND "))
    }
}

/// Section 5.3: two sets, by their class lists.
fn class_lists_expected(l: &SetSpec, op: ComparisonOp, r: &SetSpec) -> String {
    let (ll, rl) = (l.lists(1), r.lists(2));
    let get = |lists: &[(&str, String)], class: &str| {
        lists
            .iter()
            .find(|(c, _)| *c == class)
            .map(|(_, t)| t.clone())
    };
    let nullable = l.nullable() || r.nullable();
    let sym = ff_symbol(op);
    let cmp = |lc: &str, rc: &str, a: &str, b: &str| {
        let (a, b) = ff_mixed(lc, rc, a, b);
        if nullable {
            format!("coalesce({a} {sym} {b}, false)")
        } else {
            format!("({a} {sym} {b})")
        }
    };
    let neq = op == ComparisonOp::Neq;
    let ordered = op_is_ordered(op);
    let mut terms = Vec::new();
    for (lc, rc, ordered_ok) in FF_SCALAR_PAIRS {
        if ordered && !ordered_ok {
            continue;
        }
        let (Some(lt), Some(rt)) = (get(&ll, lc), get(&rl, rc)) else {
            continue;
        };
        terms.push(if neq {
            format!(
                "arraySum(p -> arrayCount(q -> {}, {rt}), {lt})",
                cmp(lc, rc, "p", "q")
            )
        } else {
            format!(
                "arrayExists(x -> arrayExists(y -> {}, {rt}), {lt})",
                cmp(lc, rc, "x", "y")
            )
        });
    }
    // The array on the right.
    for (c, e, ordered_ok) in FF_ARRAY_PAIRS {
        if ordered && !ordered_ok {
            continue;
        }
        let (Some(lt), Some(rt)) = (get(&ll, c), get(&rl, &format!("{e}a"))) else {
            continue;
        };
        terms.push(if neq {
            format!(
                "arraySum(p -> arrayCount(q -> (notEmpty(q) AND arrayAll(x -> {}, q)), {rt}), \
                 {lt})",
                cmp(c, e, "p", "x")
            )
        } else {
            format!(
                "arrayExists(x -> arrayExists(w -> arrayExists(y -> {}, w), {rt}), {lt})",
                cmp(c, e, "x", "y")
            )
        });
    }
    // The array on the left.
    for (c, e, ordered_ok) in FF_ARRAY_PAIRS {
        if ordered && !ordered_ok {
            continue;
        }
        let (Some(lt), Some(rt)) = (get(&ll, &format!("{e}a")), get(&rl, c)) else {
            continue;
        };
        terms.push(if neq {
            format!(
                "arraySum(p -> arrayCount(q -> (notEmpty(p) AND arrayAll(x -> {}, p)), {rt}), \
                 {lt})",
                cmp(e, c, "x", "q")
            )
        } else {
            format!(
                "arrayExists(u -> arrayExists(x -> arrayExists(y -> {}, {rt}), u), {lt})",
                cmp(e, c, "x", "y")
            )
        });
    }
    // Decision 9: with no term, `!=` counts no pair, so it holds when either
    // set is empty.
    if terms.is_empty() {
        if !neq {
            return "false".to_string();
        }
        terms.push("0".to_string());
    }
    if !neq {
        return format!("({})", terms.join(" OR "));
    }
    let pred = format!(
        "(({}) = {} * {})",
        terms.join(" + "),
        l.count(1),
        r.count(2)
    );
    let presences: Vec<String> = [l.presence(1), r.presence(2)]
        .into_iter()
        .flatten()
        .collect();
    if presences.is_empty() {
        pred
    } else {
        format!("({} AND {pred})", presences.join(" AND "))
    }
}

/// The text part 3d gives `L op R`, at least one side a set: section 5.2
/// or 5.3, then 5.5's binding — the chains, left outermost, inside the
/// resource values.
fn set_pair_expected(l: Side<'_>, op: ComparisonOp, r: Side<'_>) -> String {
    let pred = match (l, r) {
        (Side::Set(a), Side::Set(b)) => class_lists_expected(a, op, b),
        (Side::Set(a), Side::Scalar(s)) => element_expected(a, 1, op, s),
        (Side::Scalar(s), Side::Set(b)) => element_expected(b, 2, op, s),
        (Side::Scalar(_), Side::Scalar(_)) => panic!("no set: 3c's ff_expected"),
    };
    if pred == "false" {
        return pred;
    }
    let mut text = pred;
    for (side, n) in [(r, 2), (l, 1)] {
        if let Side::Set(s) = side
            && let Some((c, chain)) = s.chain(n)
        {
            text = format!("arrayExists({c} -> {text}, [{chain}])");
        }
    }
    let binds: Vec<(String, String)> = [l.bind(1), r.bind(2)].into_iter().flatten().collect();
    match binds.as_slice() {
        [] => text,
        [(v, value)] => format!("arrayExists({v} -> {text}, [{value}])"),
        [(v1, x1), (v2, x2)] => format!("arrayExists(({v1}, {v2}) -> {text}, [{x1}], [{x2}])"),
        many => panic!("{} binds", many.len()),
    }
}

/// `T-C34`: every ordered pair of 3c's fourteen operands and part 3d's
/// seven sets with at least one set, under each of the six operators —
/// 1,470 cells — compiles to the text this file builds from its own copy
/// of section 5, with no demand; a pair sharing no type pair under `!=`
/// expects decision 9's text. With no context the first operand needing
/// it, left before right, gives its own refusal; every other cell is the
/// same text.
#[test]
fn t_c34_set_operands_are_the_generated_cross_product() {
    let scalars = ff_operands();
    let sets = set_operands();
    assert_eq!((scalars.len(), sets.len()), (14, 7));
    let sides: Vec<Side<'_>> = scalars
        .iter()
        .map(Side::Scalar)
        .chain(sets.iter().map(Side::Set))
        .collect();
    let mut cells = 0usize;
    for l in &sides {
        for r in &sides {
            if !l.is_set() && !r.is_set() {
                continue;
            }
            for op in FF_OPS {
                cells += 1;
                let expr = field_compare_expr(l.field(), op, r.field());
                let want = set_pair_expected(*l, op, *r);
                let label = format!("{} {op} {}", l.field(), r.field());
                let got = compile_span_predicate_in(&expr, &ctx())
                    .unwrap_or_else(|e| panic!("{label} must compile in a context: {e}"));
                assert_eq!(got.sql(), want, "{label}");
                assert!(got.demand_messages().is_empty(), "{label}: no demand");
                let bare = compile_span_predicate(&expr).map(|p| p.sql().to_string());
                match l.needs_window().or_else(|| r.needs_window()) {
                    Some(refusal) => assert_eq!(bare, Err(refusal), "{label} with no context"),
                    None => assert_eq!(bare, Ok(want), "{label} with no context"),
                }
            }
        }
    }
    assert_eq!(cells, 1_470);
}

/// `T-C35`: section 5.7's texts, byte for byte, `V(a)` and `C(a)` spelled
/// out from [`ctx`].
#[test]
fn t_c35_the_set_comparison_texts() {
    assert_eq!(
        rendered(r#"{ event.a = name }"#),
        "arrayExists(e1 -> (coalesce(dynamicElement(e1, 'String') = name, false) OR \
         arrayExists(x -> coalesce(x = name, false), dynamicElement(e1, \
         'Array(Nullable(String))'))), arrayFilter(d -> dynamicType(d) != 'None', \
         events.attrs.`a`))"
    );
    assert_eq!(
        rendered(r#"{ event.a != name }"#),
        "arrayAll(e1 -> (coalesce(dynamicElement(e1, 'String') != name, false) OR \
         (notEmpty(dynamicElement(e1, 'Array(Nullable(String))')) AND arrayAll(x -> \
         coalesce(x != name, false), dynamicElement(e1, 'Array(Nullable(String))')))), \
         arrayFilter(d -> dynamicType(d) != 'None', events.attrs.`a`))"
    );
    assert_eq!(
        rendered(r#"{ event:name != span.s }"#),
        "(dynamicType(attrs.`s`) != 'None' AND arrayAll(e1 -> (coalesce(e1 != \
         attrs.`s`.:String, false) OR (notEmpty(attrs.`s`.:`Array(Nullable(String))`) AND \
         arrayAll(x -> coalesce(e1 != x, false), attrs.`s`.:`Array(Nullable(String))`))), \
         events.name))"
    );
    assert_eq!(
        rendered(r#"{ event:timeSinceStart < duration }"#),
        "arrayExists(e1 -> (e1 < duration_ns), arrayMap(t -> toInt128(t) - start_ns, \
         events.time_ns))"
    );
    assert_eq!(
        rendered(r#"{ link:spanID != span:id }"#),
        "arrayAll(e1 -> (e1 != lower(hex(span_id))), arrayMap(h -> lower(hex(h)), \
         links.span_id))"
    );
    assert_eq!(
        rendered(r#"{ event:name = link:spanID }"#),
        "(arrayExists(x -> arrayExists(y -> (x = y), arrayMap(h -> lower(hex(h)), \
         links.span_id)), events.name))"
    );
    assert_eq!(
        rendered(r#"{ event:name != link:spanID }"#),
        "((arraySum(p -> arrayCount(q -> (p != q), arrayMap(h -> lower(hex(h)), \
         links.span_id)), events.name)) = length(events.name) * length(links.span_id))"
    );
    let c_a = "arrayFilter((d, g) -> g = multiIf(dynamicType(attrs.`a`) != 'None', 1, \
               dynamicType(r1) != 'None', 2, arrayExists(d -> dynamicType(d) != 'None', \
               events.attrs.`a`), 3, arrayExists(d -> dynamicType(d) != 'None', \
               links.attrs.`a`), 4, dynamicType(scope_attrs.`a`) != 'None', 5, 0) AND \
               dynamicType(d) != 'None', arrayConcat([attrs.`a`], [r1], events.attrs.`a`, \
               links.attrs.`a`, [scope_attrs.`a`]), arrayConcat([1], [2], arrayMap(d -> 3, \
               events.attrs.`a`), arrayMap(d -> 4, links.attrs.`a`), [5]))";
    assert_eq!(
        rendered_in(r#"{ .a != name }"#),
        format!(
            "arrayExists(r1 -> arrayExists(c1 -> (notEmpty(c1) AND arrayAll(e1 -> \
             (coalesce(dynamicElement(e1, 'String') != name, false) OR \
             (notEmpty(dynamicElement(e1, 'Array(Nullable(String))')) AND arrayAll(x -> \
             coalesce(x != name, false), dynamicElement(e1, 'Array(Nullable(String))')))), \
             c1)), [{c_a}]), [{}])",
            v_of("attrs.`a`")
        )
    );
    assert_eq!(rendered(r#"{ event.a = status }"#), "false");
    // Decision 9: `!=` with no term holds exactly when the set is empty.
    assert_eq!(
        rendered(r#"{ event:name != duration }"#),
        "arrayAll(e1 -> false, events.name)"
    );
    assert_eq!(
        rendered(r#"{ event:name != event:timeSinceStart }"#),
        "((0) = length(events.name) * length(events.time_ns))"
    );
}

/// `T-C36`: the chain needs its window and an event set does not; the
/// regex operators keep part 3a's refusal.
#[test]
fn t_c36_the_set_operand_refusals() {
    assert_eq!(
        refusal(r#"{ .a = span.b }"#),
        PlanError::UnsupportedField(UNSCOPED_NEEDS_WINDOW.to_string())
    );
    assert!(
        compile_span_predicate(&filter_body(r#"{ event.a = span.b }"#)).is_ok(),
        "an event set needs no context"
    );
    assert_eq!(
        refusal(r#"{ event.a =~ link.a }"#),
        PlanError::TypeMismatch(
            "a field-against-field comparison does not support regex operators".to_string()
        )
    );
}

/// `T-C37`: a chain operand reads the resource row, so its predicate
/// carries its window and composing it with another panics, as `T-C28`'s
/// do.
#[test]
#[should_panic(expected = "different window")]
fn t_c37_a_chain_operand_refuses_another_window() {
    let p =
        compile_span_predicate_in(&filter_body(r#"{ .a = span.b }"#), &ctx()).expect("compiles");
    let other = WindowSql::start_closed_end_open(T_B1_START, T_B1_END + 1);
    let _ = span_membership_sql("spans", other, &p);
}

/// `T-C38`'s leaf cells: what `compile_span_leaf_in` answered for each of
/// the seven operands at the branch point, generated once and checked in,
/// so a change to the leaf moves only the compiled side of a cell. One
/// line per cell: operand, the leaf's operator and value, then `ok` and
/// the text or `err` and the error.
const T_C38_LEAVES: &str = include_str!("fixtures/traces_compile_v2_t_c38_leaf.tsv");

/// One outcome as `T_C38_LEAVES` writes it.
fn outcome(got: Result<String, PlanError>) -> String {
    match got {
        Ok(sql) => format!("ok\t{sql}"),
        Err(e) => format!("err\t{e:?}"),
    }
}

fn t_c38_leaf(operand: &str, leaf: &str) -> String {
    let mut found = T_C38_LEAVES.lines().filter_map(|line| {
        let mut cols = line.splitn(3, '\t');
        let (o, l, rest) = (cols.next()?, cols.next()?, cols.next()?);
        (o == operand && l == leaf).then(|| rest.to_string())
    });
    let one = found
        .next()
        .unwrap_or_else(|| panic!("no leaf cell for {operand} {leaf}"));
    assert!(
        found.next().is_none(),
        "two leaf cells for {operand} {leaf}"
    );
    one
}

/// What one `T-C38` row expects of every operand.
enum Shape {
    Leaf(&'static str),
    Not {
        all: bool,
    },
    False,
    /// Part 3e's build, from [`ex_expected`].
    Expr(ExShape),
    Exists {
        negated: bool,
    },
    Pair,
    Regex,
}

/// `T-C38`: section 9.1's table — the seven operands under 32 shapes, 224
/// cells, each against its exact outcome.
#[test]
fn t_c38_every_compile_path_for_every_set_operand() {
    // (query with `{F}` for the operand, the row's shape)
    let rows: [(&str, Shape); 32] = [
        ("{ {F} }", Shape::Leaf("= true")),
        ("{ !{F} }", Shape::Not { all: false }),
        ("{ !{F} = true }", Shape::Not { all: false }),
        ("{ true = !{F} }", Shape::Not { all: false }),
        ("{ !{F} != false }", Shape::Not { all: true }),
        ("{ false != !{F} }", Shape::Not { all: true }),
        ("{ {F} = 2 }", Shape::Leaf("= 2")),
        (r#"{ {F} = "x" }"#, Shape::Leaf("= \"x\"")),
        ("{ {F} < 1ms }", Shape::Leaf("< 1ms")),
        (r#"{ {F} =~ "x" }"#, Shape::Leaf("=~ \"x\"")),
        ("{ {F} = 1 + 1 }", Shape::Leaf("= 2")),
        ("{ 1ms + 1ms < {F} }", Shape::Leaf("> 2000000")),
        ("{ {F} > 1000 * 3 }", Shape::Leaf("> 3000")),
        ("{ {F} = -1 }", Shape::Leaf("= -1")),
        ("{ {F} = maxInt + 1 }", Shape::Leaf("= 9223372036854775808")),
        ("{ {F} = 1 / 0 }", Shape::False),
        ("{ 1 / 0 = {F} }", Shape::False),
        ("{ {F} < 1 % 0 }", Shape::False),
        ("{ 1 % 0 < {F} }", Shape::False),
        ("{ 1 / 0 = {F} + 1 }", Shape::False),
        ("{ {F} = 2.0 ^ 0.5 }", Shape::Expr(ExShape::VsPow)),
        ("{ {F} != nil }", Shape::Exists { negated: false }),
        ("{ {F} = nil }", Shape::Exists { negated: true }),
        ("{ {F} = span.a }", Shape::Pair),
        ("{ span.a = {F} }", Shape::Pair),
        ("{ {F} = {G} }", Shape::Pair),
        ("{ {F} =~ span.b }", Shape::Regex),
        ("{ {F} + 1 = 2 }", Shape::Expr(ExShape::PlusOneVsTwo)),
        ("{ {F} = span.b + 1 }", Shape::Expr(ExShape::VsSpanBPlusOne)),
        ("{ !{F} = span.b }", Shape::Expr(ExShape::NotVsSpanB)),
        ("{ span.b = !{F} }", Shape::Expr(ExShape::SpanBVsNot)),
        ("{ {F} = (span.b = 1) }", Shape::Expr(ExShape::VsBool)),
    ];
    let sets = set_operands();
    let span_a = ff_attr(AttrScope::Span, "attrs", "a");
    let mut cells = 0usize;
    for set in &sets {
        let f = set.field.to_string();
        // Section 9.1's `G`: `event:name`, or `link:spanID` opposite it.
        let g = if matches!(set.kind, SetKind::EventName) {
            &sets[4]
        } else {
            &sets[2]
        };
        for (template, shape) in &rows {
            cells += 1;
            let query = template
                .replace("{F}", &f)
                .replace("{G}", &g.field.to_string());
            let got = outcome(
                compile_span_predicate_in(&filter_body(&query), &ctx())
                    .map(|p| p.sql().to_string()),
            );
            let bool_err = || {
                Err(PlanError::TypeMismatch(format!(
                    "expression (!{f}) expected a boolean"
                )))
            };
            let want = match shape {
                Shape::Leaf(leaf) => t_c38_leaf(&f, leaf),
                Shape::Not { all } => outcome(match (set.kind, &set.field) {
                    (SetKind::Attr(root, key), Field::Attribute { scope, .. }) => {
                        let e = root.trim_end_matches(".attrs");
                        Ok(not_element_text(e, *scope, key, *all))
                    }
                    (SetKind::Chain(key), _) => Ok(not_chain_text(key, *all)),
                    _ => bool_err(),
                }),
                Shape::False => outcome(Ok("false".to_string())),
                Shape::Expr(shape) => outcome(ex_expected(*shape, set, g, ComparisonOp::Eq)),
                Shape::Exists { negated } => outcome(match set.kind {
                    SetKind::Attr(root, key) => {
                        let present =
                            format!("arrayExists(d -> dynamicType(d) != 'None', {root}.`{key}`)");
                        Ok(if *negated {
                            format!("NOT {present}")
                        } else {
                            present
                        })
                    }
                    SetKind::Chain(key) => {
                        let any = CHAIN
                            .iter()
                            .map(|scope| format!("({})", presence_fixed(*scope, key)))
                            .collect::<Vec<_>>()
                            .join(" OR ");
                        Ok(if *negated {
                            format!("NOT ({any})")
                        } else {
                            any
                        })
                    }
                    _ => Err(PlanError::TypeMismatch(
                        "existence checks are only supported on attributes".to_string(),
                    )),
                }),
                Shape::Pair => {
                    let me = Side::Set(set);
                    let text = if template.starts_with("{ span.a") {
                        set_pair_expected(Side::Scalar(&span_a), ComparisonOp::Eq, me)
                    } else if template.contains("{G}") {
                        set_pair_expected(me, ComparisonOp::Eq, Side::Set(g))
                    } else {
                        set_pair_expected(me, ComparisonOp::Eq, Side::Scalar(&span_a))
                    };
                    outcome(Ok(text))
                }
                Shape::Regex => outcome(Err(PlanError::TypeMismatch(
                    "a field-against-field comparison does not support regex operators".to_string(),
                ))),
            };
            assert_eq!(got, want, "{query}");
        }
    }
    assert_eq!(cells, 224);
}

/// `T-C39`: decision 7's `!=` over every element, the literal on either
/// side; part 2's `=` unchanged; decision 8's fold over the four
/// intrinsics and to no value.
#[test]
fn t_c39_not_over_a_set_and_the_folded_side() {
    assert_eq!(
        rendered(r#"{ !event.f != false }"#),
        "(throwIf(arrayExists(d -> dynamicType(d) != 'None' AND dynamicType(d) != 'Bool', \
         events.attrs.`f`), 'expression (!event.f) expected a boolean') + \
         toUInt8((arrayExists(d -> dynamicType(d) != 'None', events.attrs.`f`) AND NOT \
         arrayExists(b -> coalesce(b = true, false), events.attrs.`f`.:Bool)))) = 1"
    );
    let any = "(throwIf(arrayExists(d -> dynamicType(d) != 'None' AND dynamicType(d) != 'Bool', \
               events.attrs.`f`), 'expression (!event.f) expected a boolean') + \
               toUInt8(arrayExists(b -> coalesce(b = false, false), events.attrs.`f`.:Bool))) = 1";
    assert_eq!(rendered(r#"{ !event.f = true }"#), any);
    assert_eq!(rendered(r#"{ true = !event.f }"#), any);
    let link_all = "(throwIf(arrayExists(d -> dynamicType(d) != 'None' AND dynamicType(d) != \
                    'Bool', links.attrs.`lf`), 'expression (!link.lf) expected a boolean') + \
                    toUInt8((arrayExists(d -> dynamicType(d) != 'None', links.attrs.`lf`) AND \
                    NOT arrayExists(b -> coalesce(b = false, false), links.attrs.`lf`.:Bool)))) \
                    = 1";
    assert_eq!(rendered(r#"{ !link.lf != true }"#), link_all);
    assert_eq!(rendered(r#"{ true != !link.lf }"#), link_all);
    assert_eq!(
        rendered(r#"{ event:timeSinceStart > 5ms + 4ms }"#),
        "arrayExists(t -> toInt128(t) - start_ns > 9000000, events.time_ns)"
    );
    assert_eq!(rendered(r#"{ event:name = 1 / 0 }"#), "false");
    assert_eq!(rendered_in(r#"{ .a < 1 % 0 }"#), "false");
}

// =====================================================================
// Issue #589 part 3e — event, link and unscoped operands in expressions
// =====================================================================

/// One occurrence of a set operand (part 3e's section 5.1): its element
/// variable `e<k>`, its set, a chain's variable `c<k>`, `C(k)` over `cr<k>`
/// and `V(k)`, and whether it is a `!F` side.
struct ExOcc {
    var: String,
    array: String,
    chain: Option<(String, String, String)>,
    negated: bool,
}

/// What the operands of one comparison contribute to section 5.4's build,
/// in pre-order, left side first.
#[derive(Default)]
struct ExFrame {
    occurrences: Vec<ExOcc>,
    /// Decision 11's presences of the scalar field leaves.
    scalars: Vec<String>,
    /// The arithmetic sides' `a<n>` and their tuples.
    a_binds: Vec<(String, String)>,
    /// `!F`'s demands, `(condition, message)`.
    demands: Vec<(String, String)>,
}

/// The part-3c typed `NULL` tuple of a leaf with no number.
fn ax_no_number() -> Ax {
    Ax {
        text: format!("tuple({AX_NI}, {AX_NF})"),
        dur: false,
    }
}

/// Unary minus over `x`: part 3c's node.
fn ax_neg(x: &Ax) -> Ax {
    Ax {
        text: format!(
            "arrayElement(arrayMap(pl -> tuple({}, negate(tupleElement(pl, 2))), [{}]), 1)",
            ax_neg_int("tupleElement(pl, 1)"),
            x.text
        ),
        dur: x.dur,
    }
}

impl ExFrame {
    /// Registers `set` as the next occurrence and returns its number.
    fn occurrence(&mut self, set: &SetSpec, negated: bool) -> usize {
        let k = self.occurrences.len() + 1;
        let chain = match set.kind {
            SetKind::Chain(key) => Some((
                format!("c{k}"),
                chain_c_over(key, &format!("cr{k}")),
                v_of(&ff_resource_path(key)),
            )),
            _ => None,
        };
        self.occurrences.push(ExOcc {
            var: format!("e{k}"),
            array: set.array(k),
            chain,
            negated,
        });
        k
    }

    /// A lone set field side: part 3d's element arms over `e<k>`.
    fn set_side(&mut self, set: &SetSpec) -> FfOperand {
        let k = self.occurrence(set, false);
        set.element(k)
    }

    /// `!F` as a side (section 5.3), `F` an attribute or chain set.
    fn not_side(&mut self, set: &SetSpec) -> FfOperand {
        let k = self.occurrence(set, true);
        let (condition, label) = match (set.kind, &set.field) {
            (SetKind::Attr(root, key), Field::Attribute { scope, .. }) => (
                not_element_demand(root.trim_end_matches(".attrs"), key),
                format!("{scope}{key}"),
            ),
            (SetKind::Chain(key), _) => (not_chain_demand(key), format!(".{key}")),
            _ => panic!("an intrinsic `!F` is refused"),
        };
        self.demands.push((
            condition,
            format!("expression (!{label}) expected a boolean"),
        ));
        FfOperand {
            field: set.field.clone(),
            arms: vec![("b", format!("(NOT dynamicElement(e{k}, 'Bool'))"))],
            nullable: true,
            needs_ctx: false,
            types: None,
            s_on_span_row: false,
            bind: false,
        }
    }

    /// A set leaf inside arithmetic (section 5.2).
    fn set_leaf(&mut self, set: &SetSpec) -> Ax {
        let k = self.occurrence(set, false);
        match set.kind {
            SetKind::Attr(..) | SetKind::Chain(_) => Ax {
                text: format!(
                    "tuple(toInt256(dynamicElement(e{k}, 'Int64')), dynamicElement(e{k}, \
                     'Float64'))"
                ),
                dur: false,
            },
            SetKind::TimeSinceStart => Ax {
                text: format!("tuple(toInt256(e{k}), {AX_NF})"),
                dur: true,
            },
            SetKind::EventName | SetKind::LinkSpanId | SetKind::LinkTraceId => ax_no_number(),
        }
    }

    /// A span attribute, lone or in arithmetic: decision 11's presence.
    fn span_attr(&mut self, key: &str) {
        self.scalars
            .push(format!("dynamicType(attrs.`{key}`) != 'None'"));
    }

    /// An arithmetic side `n`: its tuple bound to `a<n>`.
    fn arith_side(&mut self, n: usize, ax: Ax) -> FfOperand {
        let a = format!("a{n}");
        self.a_binds.push((a.clone(), ax.text));
        FfOperand {
            field: attr("unused"),
            arms: vec![
                ("i", format!("tupleElement({a}, 1)")),
                ("f", format!("tupleElement({a}, 2)")),
            ],
            nullable: true,
            needs_ctx: false,
            types: None,
            s_on_span_row: false,
            bind: false,
        }
    }

    /// Section 5.4's build over the two sides' arms.
    fn build(&self, l: &FfOperand, op: ComparisonOp, r: &FfOperand) -> String {
        let neq = op == ComparisonOp::Neq;
        let text = match ff_body(l, op, r) {
            Some(body) => Some(body),
            None if neq => Some("false".to_string()),
            None => None,
        };
        let text = match text {
            None => "false".to_string(),
            Some(body) => self.loops(body, neq),
        };
        if self.demands.is_empty() {
            return text;
        }
        let mut sql = String::from("(");
        for (condition, message) in &self.demands {
            sql.push_str(&format!("throwIf({condition}, '{message}') + "));
        }
        format!("{sql}toUInt8({text})) = 1")
    }

    fn loops(&self, body: String, neq: bool) -> String {
        // 1. the arithmetic sides' bindings.
        let mut text = ex_bind(&self.a_binds, body);
        // 2. each occurrence, last first.
        for occ in self.occurrences.iter().rev() {
            let f = if neq { "arrayAll" } else { "arrayExists" };
            text = format!("{f}({} -> {text}, {})", occ.var, occ.array);
        }
        // 3. `!=`: the presences — the scalar leaves, then the chains, then
        //    the `!F` sets — each text once.
        if neq {
            let mut presences: Vec<String> = Vec::new();
            let chains = self
                .occurrences
                .iter()
                .filter_map(|o| o.chain.as_ref().map(|(c, _, _)| format!("notEmpty({c})")));
            let negated = self
                .occurrences
                .iter()
                .filter(|o| o.negated)
                .map(|o| format!("notEmpty({})", o.array));
            for p in self.scalars.iter().cloned().chain(chains).chain(negated) {
                if !presences.contains(&p) {
                    presences.push(p);
                }
            }
            if !presences.is_empty() {
                text = format!("({} AND {text})", presences.join(" AND "));
            }
        }
        // 4. each chain, last first.
        for occ in self.occurrences.iter().rev() {
            if let Some((c, chain, _)) = &occ.chain {
                text = format!("arrayExists({c} -> {text}, [{chain}])");
            }
        }
        // 5. the chains' resource values (no lone resource field here).
        let cr: Vec<(String, String)> = self
            .occurrences
            .iter()
            .enumerate()
            .filter_map(|(i, o)| {
                o.chain
                    .as_ref()
                    .map(|(_, _, v)| (format!("cr{}", i + 1), v.clone()))
            })
            .collect();
        ex_bind(&cr, text)
    }
}

/// Part 3c's binding of values to variables, one lambda over one-element
/// arrays.
fn ex_bind(binds: &[(String, String)], body: String) -> String {
    match binds {
        [] => body,
        [(v, x)] => format!("arrayExists({v} -> {body}, [{x}])"),
        many => {
            let vars: Vec<&str> = many.iter().map(|(v, _)| v.as_str()).collect();
            let values: Vec<String> = many.iter().map(|(_, x)| format!("[{x}]")).collect();
            format!(
                "arrayExists(({}) -> {body}, {})",
                vars.join(", "),
                values.join(", ")
            )
        }
    }
}

/// The shapes part 3e serves, each with `F` a set operand and `G` the
/// second one of `T-C38`.
#[derive(Clone, Copy, Debug)]
enum ExShape {
    /// `{ F + 1 op span.a }`
    PlusOneVsSpanA,
    /// `{ span.a op F * 2 }`
    SpanAVsTimesTwo,
    /// `{ -F op 0 }`
    NegVsZero,
    /// `{ F op (span.b = 1) }`
    VsBool,
    /// `{ !F op span.a }`
    NotVsSpanA,
    /// `{ F + G op 1 }`
    PlusG,
    /// `{ F op 2.0 ^ 0.5 }`
    VsPow,
    /// `{ F + 1 op 2 }`
    PlusOneVsTwo,
    /// `{ F op span.b + 1 }`
    VsSpanBPlusOne,
    /// `{ !F op span.b }`
    NotVsSpanB,
    /// `{ span.b op !F }`
    SpanBVsNot,
}

impl ExShape {
    fn query(self, f: &str, g: &str, op: ComparisonOp) -> String {
        let op = ff_symbol(op);
        match self {
            ExShape::PlusOneVsSpanA => format!("{{ {f} + 1 {op} span.a }}"),
            ExShape::SpanAVsTimesTwo => format!("{{ span.a {op} {f} * 2 }}"),
            ExShape::NegVsZero => format!("{{ -{f} {op} 0 }}"),
            ExShape::VsBool => format!("{{ {f} {op} (span.b = 1) }}"),
            ExShape::NotVsSpanA => format!("{{ !{f} {op} span.a }}"),
            ExShape::PlusG => format!("{{ {f} + {g} {op} 1 }}"),
            ExShape::VsPow => format!("{{ {f} {op} 2.0 ^ 0.5 }}"),
            ExShape::PlusOneVsTwo => format!("{{ {f} + 1 {op} 2 }}"),
            ExShape::VsSpanBPlusOne => format!("{{ {f} {op} span.b + 1 }}"),
            ExShape::NotVsSpanB => format!("{{ !{f} {op} span.b }}"),
            ExShape::SpanBVsNot => format!("{{ span.b {op} !{f} }}"),
        }
    }
}

/// A literal's arms.
fn ex_literal(digits: &str) -> FfOperand {
    ff_intrinsic(Intrinsic::Duration, "i", digits)
}

/// What part 3e compiles `shape` to in [`ctx`], from this file's own copy
/// of section 5: the build of 5.4 over part 3c's node rules and part 3a's
/// terms, or 5.3's refusal of an intrinsic `!F`.
fn ex_expected(
    shape: ExShape,
    set: &SetSpec,
    g: &SetSpec,
    op: ComparisonOp,
) -> Result<String, PlanError> {
    let mut fr = ExFrame::default();
    let intrinsic_not = || {
        Err(PlanError::TypeMismatch(format!(
            "expression (!{}) expected a boolean",
            set.field
        )))
    };
    let is_attr_or_chain = matches!(set.kind, SetKind::Attr(..) | SetKind::Chain(_));
    let (l, r) = match shape {
        ExShape::PlusOneVsSpanA => {
            let leaf = fr.set_leaf(set);
            let l = fr.arith_side(1, ax_bin("+", &leaf, &ax_int("1", false)));
            fr.span_attr("a");
            (l, ff_attr(AttrScope::Span, "attrs", "a"))
        }
        ExShape::SpanAVsTimesTwo => {
            fr.span_attr("a");
            let leaf = fr.set_leaf(set);
            let r = fr.arith_side(2, ax_bin("*", &leaf, &ax_int("2", false)));
            (ff_attr(AttrScope::Span, "attrs", "a"), r)
        }
        ExShape::NegVsZero => {
            let leaf = fr.set_leaf(set);
            (fr.arith_side(1, ax_neg(&leaf)), ex_literal("0"))
        }
        ExShape::VsBool => {
            let l = fr.set_side(set);
            let b = "((coalesce(attrs.`b`.:Int64 = 1, false) OR coalesce(attrs.`b`.:Float64 = 1, \
                     false)))";
            (l, ff_intrinsic(Intrinsic::Name, "b", b))
        }
        ExShape::NotVsSpanA => {
            if !is_attr_or_chain {
                return intrinsic_not();
            }
            let l = fr.not_side(set);
            fr.span_attr("a");
            (l, ff_attr(AttrScope::Span, "attrs", "a"))
        }
        ExShape::PlusG => {
            let a = fr.set_leaf(set);
            let b = fr.set_leaf(g);
            (fr.arith_side(1, ax_bin("+", &a, &b)), ex_literal("1"))
        }
        ExShape::VsPow => {
            let l = fr.set_side(set);
            let r = fr.arith_side(2, ax_bin("^", &ax_float("2"), &ax_float("0.5")));
            (l, r)
        }
        ExShape::PlusOneVsTwo => {
            let leaf = fr.set_leaf(set);
            (
                fr.arith_side(1, ax_bin("+", &leaf, &ax_int("1", false))),
                ex_literal("2"),
            )
        }
        ExShape::VsSpanBPlusOne => {
            let l = fr.set_side(set);
            fr.span_attr("b");
            let r = fr.arith_side(2, ax_bin("+", &ax_attr("attrs", "b"), &ax_int("1", false)));
            (l, r)
        }
        ExShape::NotVsSpanB => {
            if !is_attr_or_chain {
                return intrinsic_not();
            }
            let l = fr.not_side(set);
            fr.span_attr("b");
            (l, ff_attr(AttrScope::Span, "attrs", "b"))
        }
        ExShape::SpanBVsNot => {
            if !is_attr_or_chain {
                return intrinsic_not();
            }
            fr.span_attr("b");
            let r = fr.not_side(set);
            (ff_attr(AttrScope::Span, "attrs", "b"), r)
        }
    };
    Ok(fr.build(&l, op, &r))
}

/// `T-C40`'s six shapes.
const T_C40_SHAPES: [ExShape; 6] = [
    ExShape::PlusOneVsSpanA,
    ExShape::SpanAVsTimesTwo,
    ExShape::NegVsZero,
    ExShape::VsBool,
    ExShape::NotVsSpanA,
    ExShape::PlusG,
];

/// `T-C38`'s `G` for `set`: `event:name`, or `link:spanID` opposite it.
fn second_set<'a>(sets: &'a [SetSpec], set: &SetSpec) -> &'a SetSpec {
    if matches!(set.kind, SetKind::EventName) {
        &sets[4]
    } else {
        &sets[2]
    }
}

/// `T-C40`: the seven set operands × six shapes × the six operators — 252
/// cells — each the exact text this file builds from its own copy of
/// section 5, or 5.3's exact refusal.
#[test]
fn t_c40_set_operands_in_expressions_are_the_generated_cross_product() {
    let sets = set_operands();
    let mut cells = 0usize;
    for set in &sets {
        let g = second_set(&sets, set);
        for shape in T_C40_SHAPES {
            for op in FF_OPS {
                cells += 1;
                let query = shape.query(&set.field.to_string(), &g.field.to_string(), op);
                let got = compile_span_predicate_in(&filter_body(&query), &ctx())
                    .map(|p| p.sql().to_string());
                assert_eq!(got, ex_expected(shape, set, g, op), "{query}");
            }
        }
    }
    assert_eq!(cells, 252);
}

/// `T-C41`: section 5.5's texts, byte for byte, `V(a)` and `C(a)` spelled
/// out from [`ctx`].
#[test]
fn t_c41_the_set_expression_texts() {
    assert_eq!(
        rendered(r#"{ -event.a > 0 }"#),
        "arrayExists(e1 -> arrayExists(a1 -> (coalesce(tupleElement(a1, 1) > 0, false) OR \
         coalesce(tupleElement(a1, 2) > 0, false)), [arrayElement(arrayMap(pl -> \
         tuple(if(tupleElement(pl, 1) != 0 AND negate(tupleElement(pl, 1)) = tupleElement(pl, \
         1), NULL, negate(tupleElement(pl, 1))), negate(tupleElement(pl, 2))), \
         [tuple(toInt256(dynamicElement(e1, 'Int64')), dynamicElement(e1, 'Float64'))]), 1)]), \
         arrayFilter(d -> dynamicType(d) != 'None', events.attrs.`a`))"
    );
    assert_eq!(
        rendered(r#"{ event:timeSinceStart / 1ms > 9.5 }"#),
        "arrayExists(e1 -> arrayExists(a1 -> (coalesce(tupleElement(a1, 1) > toFloat64('9.5'), \
         false) OR coalesce(tupleElement(a1, 2) > toFloat64('9.5'), false)), \
         [arrayElement(arrayMap((pl, pr) -> tuple(CAST(NULL, 'Nullable(Int256)'), \
         coalesce(tupleElement(pl, 2), toFloat64(tupleElement(pl, 1))) / \
         coalesce(tupleElement(pr, 2), toFloat64(tupleElement(pr, 1)))), [tuple(toInt256(e1), \
         CAST(NULL, 'Nullable(Float64)'))], [tuple(toInt256(1000000), CAST(NULL, \
         'Nullable(Float64)'))]), 1)]), arrayMap(t -> toInt128(t) - start_ns, events.time_ns))"
    );
    assert_eq!(
        rendered(r#"{ !event.f = span.g }"#),
        "(throwIf(arrayExists(d -> dynamicType(d) != 'None' AND dynamicType(d) != 'Bool', \
         events.attrs.`f`), 'expression (!event.f) expected a boolean') + \
         toUInt8(arrayExists(e1 -> (coalesce((NOT dynamicElement(e1, 'Bool')) = \
         attrs.`g`.:Bool, false) OR arrayExists(x -> coalesce((NOT dynamicElement(e1, 'Bool')) = \
         x, false), attrs.`g`.:`Array(Nullable(Bool))`)), arrayFilter(d -> dynamicType(d) != \
         'None', events.attrs.`f`)))) = 1"
    );
    // A duration divided by a plain number divides as a float: the
    // element's duration flag, not the divisor's, decides it.
    let tss = Ax {
        text: format!("tuple(toInt256(e1), {AX_NF})"),
        dur: true,
    };
    assert_eq!(
        rendered(r#"{ event:timeSinceStart / 7 > 1392857.1 }"#),
        format!(
            "arrayExists(e1 -> arrayExists(a1 -> (coalesce(tupleElement(a1, 1) > \
             toFloat64('1392857.1'), false) OR coalesce(tupleElement(a1, 2) > \
             toFloat64('1392857.1'), false)), [{}]), arrayMap(t -> toInt128(t) - start_ns, \
             events.time_ns))",
            ax_bin("/", &tss, &ax_int("7", false)).text
        )
    );
    let plus = ax_bin("+", &ax_no_number(), &ax_int("1", false));
    assert_eq!(
        rendered(r#"{ event:name + 1 != 2 }"#),
        format!(
            "arrayAll(e1 -> arrayExists(a1 -> (coalesce(tupleElement(a1, 1) != 2, false) OR \
             coalesce(tupleElement(a1, 2) != 2, false)), [{}]), events.name)",
            plus.text
        )
    );
    let c_a = "arrayFilter((d, g) -> g = multiIf(dynamicType(attrs.`a`) != 'None', 1, \
               dynamicType(cr1) != 'None', 2, arrayExists(d -> dynamicType(d) != 'None', \
               events.attrs.`a`), 3, arrayExists(d -> dynamicType(d) != 'None', \
               links.attrs.`a`), 4, dynamicType(scope_attrs.`a`) != 'None', 5, 0) AND \
               dynamicType(d) != 'None', arrayConcat([attrs.`a`], [cr1], events.attrs.`a`, \
               links.attrs.`a`, [scope_attrs.`a`]), arrayConcat([1], [2], arrayMap(d -> 3, \
               events.attrs.`a`), arrayMap(d -> 4, links.attrs.`a`), [5]))";
    let a1 = FfOperand {
        field: attr("unused"),
        arms: vec![
            ("i", "tupleElement(a1, 1)".to_string()),
            ("f", "tupleElement(a1, 2)".to_string()),
        ],
        nullable: true,
        needs_ctx: false,
        types: None,
        s_on_span_row: false,
        bind: false,
    };
    let terms = ff_body(
        &a1,
        ComparisonOp::Neq,
        &ff_attr(AttrScope::Span, "attrs", "c"),
    )
    .expect("terms");
    let node = ax_bin(
        "+",
        &Ax {
            text: "tuple(toInt256(dynamicElement(e1, 'Int64')), dynamicElement(e1, 'Float64'))"
                .to_string(),
            dur: false,
        },
        &ax_int("1", false),
    );
    assert_eq!(
        rendered_in(r#"{ .a + 1 != span.c }"#),
        format!(
            "arrayExists(cr1 -> arrayExists(c1 -> (dynamicType(attrs.`c`) != 'None' AND \
             notEmpty(c1) AND arrayAll(e1 -> arrayExists(a1 -> {terms}, [{}]), c1)), [{c_a}]), \
             [{}])",
            node.text,
            v_of("attrs.`a`")
        )
    );
}

/// The refusal of a third set operand (decision 13).
fn too_many_sets() -> PlanError {
    PlanError::UnsupportedField(
        "a comparison with more than 2 event, link or unscoped operands is not supported"
            .to_string(),
    )
}

/// `T-C42`: decision 13's cap, counted per occurrence across both sides;
/// no outcome of `T-C38`'s or `T-C40`'s cells names a part of #589; the
/// refusals parts 3a–3d keep are unchanged.
#[test]
fn t_c42_the_limits_and_the_refusals_that_remain() {
    for query in [
        r#"{ event.a + link.a + event.b > 1 }"#,
        r#"{ event.a + event.b = link.a }"#,
        r#"{ event.a - event.a = event.a }"#,
        // The cap is applied before a side folding to no value makes the
        // comparison `false`, in either order.
        r#"{ event.a + event.a + event.a + 1 / 0 != 2 }"#,
        r#"{ 2 != event.a + event.a + event.a + 1 / 0 }"#,
    ] {
        assert_eq!(
            compile_span_predicate_in(&filter_body(query), &ctx()).map(|p| p.sql().to_string()),
            Err(too_many_sets()),
            "{query}"
        );
    }
    for query in [
        r#"{ event.a + link.a > 1 }"#,
        r#"{ event.a - event.a = 1 }"#,
    ] {
        assert!(
            compile_span_predicate_in(&filter_body(query), &ctx()).is_ok(),
            "{query} compiles"
        );
    }

    // No outcome names a part of #589.
    let sets = set_operands();
    let mut queries: Vec<String> = Vec::new();
    for set in &sets {
        let f = set.field.to_string();
        let g = second_set(&sets, set).field.to_string();
        for shape in T_C40_SHAPES {
            for op in FF_OPS {
                queries.push(shape.query(&f, &g, op));
            }
        }
        for template in [
            "{ 1 / 0 = {F} + 1 }",
            "{ {F} = 2.0 ^ 0.5 }",
            "{ {F} + 1 = 2 }",
            "{ {F} = span.b + 1 }",
            "{ !{F} = span.b }",
            "{ span.b = !{F} }",
            "{ {F} = (span.b = 1) }",
        ] {
            queries.push(template.replace("{F}", &f));
        }
    }
    assert_eq!(queries.len(), 7 * (36 + 7));
    for query in &queries {
        let got = outcome(
            compile_span_predicate_in(&filter_body(query), &ctx()).map(|p| p.sql().to_string()),
        );
        assert!(!got.contains("#589 part"), "{query}: {got}");
    }

    // The refusals parts 3a–3d keep.
    assert_eq!(
        refusal(r#"{ event.a =~ span.b }"#),
        PlanError::TypeMismatch(
            "a field-against-field comparison does not support regex operators".to_string()
        )
    );
    assert_eq!(
        refusal(r#"{ event.a + nestedSetLeft > 1 }"#),
        PlanError::UnsupportedField(
            "nestedSetLeft is not supported by the span-scope predicate compiler yet (issue #594)"
                .to_string()
        )
    );
    assert_eq!(
        refusal(r#"{ .a + 1 = 2 }"#),
        PlanError::UnsupportedField(UNSCOPED_NEEDS_WINDOW.to_string())
    );
    assert_eq!(
        refusal(r#"{ event.a + resource.r > 1 }"#),
        PlanError::UnsupportedField(RESOURCE_NEEDS_WINDOW.to_string())
    );
}

// =====================================================================
// Issue #590 — the search statement
// =====================================================================

/// The predicate a search body compiles to in `ctx`: `{}` has no body and
/// is `{ true }` (the predicate compiler's own reading).
fn search_predicate(
    query: &str,
    ctx: &PredicateCtx<'_>,
) -> pulsus_read::traces::spans::predicate::SpanPredicate {
    let parsed =
        pulsus_traceql::parse(query).unwrap_or_else(|e| panic!("{query} must parse: {e:?}"));
    let body = match parsed.spanset {
        SpansetExpr::Filter(SpansetFilter { body: Some(b) }) => b,
        SpansetExpr::Filter(SpansetFilter { body: None }) => FieldExpr::Literal(Value::Bool(true)),
        other => panic!("{query}: expected one filter, got {other}"),
    };
    compile_span_predicate_in(&body, ctx).unwrap_or_else(|e| panic!("{query} must compile: {e}"))
}

/// `T-B4`: the statement bounds both span reads by the sort key's leading
/// column, rendered from `start` and from `end - 1` and divided
/// server-side.
#[test]
fn t_b4_the_search_statement_carries_the_bucket_bound() {
    let p = search_predicate("{}", &ctx());
    let sql = search_sql(
        "spans",
        "traces",
        t_b1_window(),
        &SearchFilter::One(p),
        &Projection::none(),
        20,
        3,
    );
    let bound = "intDiv(start_ns, 300000000000) BETWEEN intDiv(1790094846486853636, 300000000000) \
                 AND intDiv(1790094846486853636, 300000000000)";
    assert_eq!(sql.matches(bound).count(), 2, "{sql}");
    assert!(!sql.contains("1790094846486853637, 300000000000)"), "{sql}");
    assert_eq!(1_790_094_846_486_853_636_i64 / 300_000_000_000, 5_966_982);
}

/// The window the search goldens are rendered for: g1's three hours,
/// `[1790084801, 1790095601)` s.
fn g1_window() -> WindowSql {
    WindowSql::start_closed_end_open(1_790_084_801_000_000_000, 1_790_095_601_000_000_000)
}

/// The search goldens: file stem, query, `limit` and `spss`. Every one
/// renders through `compile_search`, its projection included: issue #591
/// part 2 serves the off-row projections `resource_attribute` and
/// `event_intrinsic` waited for, and adds the four after `instrumentation`.
const SEARCH_GOLDENS: [(&str, &str, u32, u32); 17] = [
    ("match_all", "{}", 20, 3),
    (
        "service",
        r#"{ resource.service.name = "checkout" }"#,
        20,
        3,
    ),
    (
        "span_attribute",
        r#"{ span.http.response.status_code >= 500 }"#,
        20,
        3,
    ),
    (
        "resource_attribute",
        r#"{ resource.k8s.pod.name =~ "checkout.*" }"#,
        20,
        3,
    ),
    ("event_intrinsic", r#"{ event:name = "exception" }"#, 20, 3),
    ("demand", r#"{ !span.app.cache.hit }"#, 20, 3),
    ("limits", "{}", 5, 10),
    (
        "spanset_and",
        r#"{ resource.service.name = "checkout" } && { status = error }"#,
        20,
        3,
    ),
    (
        "spanset_or",
        r#"{ resource.service.name = "frontend" } || { span.http.response.status_code >= 500 }"#,
        20,
        3,
    ),
    (
        "spanset_nested",
        r#"{ span.a = 1 } || { span.b = 2 } && { span.c = 3 }"#,
        20,
        3,
    ),
    (
        "name_and_kind",
        r#"{ name =~ "GET.*" && kind = client }"#,
        20,
        3,
    ),
    (
        "literal_and_stored",
        r#"{ span.app.user.id = "u-10013" || span.app.discount.ratio > 0.45 }"#,
        20,
        3,
    ),
    (
        "instrumentation",
        r#"{ instrumentation.otel.scope.build = "release" && instrumentation:name = "otel" }"#,
        20,
        3,
    ),
    (
        "unscoped_span",
        r#"{ .http.response.status_code >= 500 }"#,
        20,
        3,
    ),
    (
        "unscoped_resource",
        r#"{ .k8s.pod.name =~ "checkout.*" }"#,
        20,
        3,
    ),
    ("event_element", r#"{ event.retry.count > 1 }"#, 20, 3),
    (
        "link_element",
        r#"{ link.messaging.kafka.offset > 0 }"#,
        20,
        3,
    ),
];

fn search_golden_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
        .join("traces_spans_search")
}

/// One golden's text: two comment lines, then the one statement.
fn search_golden(stem: &str, query: &str, limit: u32, spss: u32) -> String {
    let w = g1_window();
    let ctx = PredicateCtx {
        window: w,
        resources_table: "resources",
    };
    let parsed =
        pulsus_traceql::parse(query).unwrap_or_else(|e| panic!("{query} must parse: {e:?}"));
    let statement = compile_search(&parsed, &ctx, "spans", "traces", limit, spss)
        .unwrap_or_else(|e| panic!("{query} must compile: {e}"));
    format!(
        "-- case: {stem}\n-- q: {query} limit={limit} spss={spss}\n{}\n",
        statement.sql()
    )
}

/// The seventeen goldens of `golden/traces_spans_search/`, byte for byte,
/// collected and asserted once.
#[test]
fn search_statement_goldens_match() {
    let mut missing: Vec<String> = Vec::new();
    let mut drifted: Vec<String> = Vec::new();
    for (stem, query, limit, spss) in SEARCH_GOLDENS {
        let path = search_golden_dir().join(format!("{stem}.sql"));
        match std::fs::read_to_string(&path) {
            Err(e) => missing.push(format!("{stem} ({path:?}: {e})")),
            Ok(expected) => {
                if search_golden(stem, query, limit, spss) != expected {
                    drifted.push(format!("{stem} ({path:?})"));
                }
            }
        }
    }
    assert!(
        missing.is_empty() && drifted.is_empty(),
        "{} search golden(s) missing:\n{}\n{} drifted:\n{}\nif the change is intentional, run \
         `cargo test -p pulsus-read --test traces_compile_v2 -- --ignored \
         regenerate_search_statement_goldens` and review the diff",
        missing.len(),
        missing.join("\n"),
        drifted.len(),
        drifted.join("\n")
    );
}

/// Regenerates the search goldens. `#[ignore]`d: run explicitly after an
/// intentional change to the statement, review the diff, and say so.
#[test]
#[ignore = "regenerates the committed goldens; run explicitly, see doc comment"]
fn regenerate_search_statement_goldens() {
    let dir = search_golden_dir();
    std::fs::create_dir_all(&dir).expect("create golden dir");
    for (stem, query, limit, spss) in SEARCH_GOLDENS {
        let path = dir.join(format!("{stem}.sql"));
        std::fs::write(&path, search_golden(stem, query, limit, spss))
            .unwrap_or_else(|e| panic!("write {path:?}: {e}"));
    }
}

/// A predicate compiled for one window, composed into a search over
/// another, panics, as `span_membership_sql` does.
#[test]
#[should_panic(expected = "different window")]
fn search_sql_rejects_another_windows_predicate() {
    // `resource.k` reads the resource table, so the predicate carries its
    // window; `resource.service.name = "x"` reads the span row and would not.
    let p = search_predicate(r#"{ resource.k = "x" }"#, &ctx());
    let other = WindowSql::start_closed_end_open(T_B1_START, T_B1_END + 1);
    let _ = search_sql(
        "spans",
        "traces",
        other,
        &SearchFilter::One(p),
        &Projection::none(),
        20,
        3,
    );
}

// =====================================================================
// Issue #591 part 1 — the search statement answers a plain query whole
// =====================================================================

/// `compile_search` over `query` in [`ctx`], with the statement's usual
/// `limit` and `spss`.
fn compile_search_of(
    query: &str,
) -> Result<pulsus_read::traces::spans::search::SearchStatement, PlanError> {
    let parsed =
        pulsus_traceql::parse(query).unwrap_or_else(|e| panic!("{query} must parse: {e:?}"));
    compile_search(&parsed, &ctx(), "spans", "traces", 20, 3)
}

/// Section 8.2: what part 3 and issues #592–#594 serve is refused, each
/// naming its target, and part 2's one residual (its section 3.6).
#[test]
fn compile_search_refuses_what_parts_two_and_three_serve() {
    for (query, target) in [
        // Issue #591 part 2's section 3.6: one set field twice in a
        // comparison holding arithmetic has no single element to project.
        (r#"{ event.k * event.k > 5 }"#, "two occurrences"),
        (r#"{ .k + 1 > .k }"#, "two occurrences"),
        // The same under `!=`, which projects nothing but must still be
        // refused: the predicate matches any pair, today's engine each
        // element against itself (code review round 1 of part 2).
        (r#"{ event.k * event.k != 5 }"#, "two occurrences"),
        (r#"{ .k + 1 != .k }"#, "two occurrences"),
        (r#"{ link.lk - link.lk != 0 }"#, "two occurrences"),
        (r#"{ .a = 1 } | count() > 1"#, "#592 part 3"),
        (r#"{ .a = 1 } | by(name)"#, "#592 part 2"),
        (r#"{ .a = 1 } | coalesce()"#, "#592 part 2"),
        (r#"{ .a = 1 } > { .b = 2 }"#, "#593"),
        (r#"({ .a = 1 } > { .b = 2 }) && { .c = 3 }"#, "#593"),
        (r#"{ nestedSetLeft > 0 }"#, "#594"),
    ] {
        match compile_search_of(query) {
            Err(PlanError::UnsupportedField(msg)) => assert!(
                msg.contains(target),
                "{query}: the refusal must name {target}, got {msg:?}"
            ),
            Err(other) => {
                panic!("{query}: expected UnsupportedField naming {target}, got {other:?}")
            }
            Ok(s) => panic!(
                "{query}: expected a refusal naming {target}, compiled to {}",
                s.sql()
            ),
        }
    }
}

/// Section 8.2: a query whose off-row fields project nothing compiles,
/// and issue #591 part 2's section 7.1: so does one that projects an
/// off-row value.
#[test]
fn compile_search_serves_off_row_projections() {
    for query in [
        r#"{ .env != "prod" }"#,
        r#"{ resource.k !~ "x" }"#,
        r#"{ .a = nil }"#,
        r#"{ .a = .b }"#,
        r#"{ !(.a = 1) }"#,
        r#"{ .a = 1 }"#,
        r#"{ resource.k = "x" }"#,
        r#"{ event.k = "x" }"#,
        r#"{ link.k = "x" }"#,
        r#"{ event:name = "x" }"#,
        r#"{ resource.k * 2 > 5 }"#,
        r#"{ event.k * 2 > 5 }"#,
        r#"{ .k * 2 > 5 }"#,
        r#"{ event.k = event.k }"#,
        r#"{ event:timeSinceStart * 2 > 3ms }"#,
        r#"{ event:name = event:name }"#,
    ] {
        if let Err(e) = compile_search_of(query) {
            panic!("{query} must compile: {e}");
        }
    }
}

/// Section 8.2: a spanset tree reads the window once — one `FROM spans`
/// before `top` closes, and no `trace_id IN` set built from a read.
#[test]
fn a_tree_reads_the_window_once() {
    let statement =
        compile_search_of(r#"{ resource.service.name = "checkout" } && { status = error }"#)
            .unwrap_or_else(|e| panic!("spanset_and must compile: {e}"));
    let sql = statement.sql();
    let top = sql
        .find(") AS top")
        .unwrap_or_else(|| panic!("no top: {sql}"));
    assert_eq!(sql[..top].matches("FROM spans").count(), 1, "{sql}");
    assert!(!sql.contains("trace_id IN (SELECT trace_id"), "{sql}");
}

/// Section 8.2: more than 255 projection groups is refused rather than
/// wrapping the `UInt8` group index.
#[test]
fn the_projection_is_bounded() {
    // Built as a tree, not parsed: the parser's 64-level nesting limit
    // refuses a filter of 256 conditions before it reaches the compiler.
    let mut body: Option<FieldExpr> = None;
    for i in 1..=256 {
        let cond = FieldExpr::Binary {
            op: FieldOp::Cmp(ComparisonOp::Eq),
            lhs: Box::new(FieldExpr::Field(scoped(AttrScope::Span, &format!("a{i}")))),
            rhs: Box::new(FieldExpr::Literal(Value::Number(i.to_string()))),
        };
        body = Some(match body {
            None => cond,
            Some(prev) => FieldExpr::Binary {
                op: FieldOp::Bool(pulsus_traceql::BoolOp::And),
                lhs: Box::new(prev),
                rhs: Box::new(cond),
            },
        });
    }
    let query = pulsus_traceql::Query {
        spanset: SpansetExpr::Filter(SpansetFilter { body }),
        pipeline: Vec::new(),
        hints: Vec::new(),
    };
    match compile_search(&query, &ctx(), "spans", "traces", 20, 3) {
        Err(PlanError::UnsupportedField(msg)) => assert!(
            msg.contains("255"),
            "the refusal names the limit of 255 fields, got {msg:?}"
        ),
        Err(other) => panic!("expected UnsupportedField, got {other:?}"),
        Ok(_) => panic!("256 projection groups must be refused"),
    }
}

// =====================================================================
// Issue #591 part 3 — the fork routes by the plan
// =====================================================================

/// The plan today's planner makes for `query` over [`g1_window`], with the
/// search defaults.
fn fork_plan(query: &str) -> pulsus_read::SearchPlan {
    use pulsus_read::SpanFilterCtx;
    use pulsus_read::traces::search_plan::{SearchCtx, SearchParams, plan_search};
    let parsed =
        pulsus_traceql::parse(query).unwrap_or_else(|e| panic!("{query} must parse: {e:?}"));
    plan_search(
        &parsed,
        &SearchParams {
            start_ns: 1_790_084_801_000_000_000,
            end_ns: 1_790_095_601_000_000_000,
            limit: 20,
            spss: 3,
        },
        &SearchCtx {
            filter: SpanFilterCtx {
                spans_table: "trace_spans",
                attrs_table: "trace_attrs_idx",
            },
            recent_table: "trace_recent",
            errors_table: "trace_error_spans",
            max_candidates: 100_000,
            max_series: 1_000,
            distributed: false,
        },
    )
    .unwrap_or_else(|e| panic!("{query} must plan: {e:?}"))
}

/// Section 6.1: `plan_statement` refuses every shape of section 3.1's
/// table that reaches it — a `|` stage, a structural operator, a
/// nested-set or trace-level intrinsic, part 2's residual set shapes — and
/// gives a statement for a covered search.
#[test]
fn the_fork_routes_by_the_plan() {
    use pulsus_read::traces::spans::search::plan_statement;
    let mut wrong = Vec::new();
    for query in [
        r#"{ .a = 1 } | count() > 1"#,
        r#"{ .a = 1 } | coalesce()"#,
        r#"{ .a = 1 } | by(name)"#,
        r#"{ .a = 1 } | { name = "b" } | coalesce()"#,
        r#"{ .a = 1 } && { .b = 2 } | { .c = 3 }"#,
        r#"{ .a = 1 } > { .b = 2 }"#,
        r#"{ nestedSetLeft > 0 }"#,
        r#"{ nestedSetParent < 0 }"#,
        r#"{ traceDuration > 1s }"#,
        r#"{ span:childCount > 2 }"#,
        r#"{ event.k * event.k > 5 }"#,
        r#"{ .k + 1 > .k }"#,
        r#"{ link.lk - link.lk != 0 }"#,
        r#"{ (event.k = 1) = true }"#,
        r#"{ !event.k = false }"#,
    ] {
        if plan_statement(&fork_plan(query), "spans", "traces", "resources").is_some() {
            wrong.push(format!(
                "{query}: served by the statement, must be today's engine's"
            ));
        }
    }
    for query in [
        r#"{ event.k * 2 > 5 }"#,
        r#"{ .k = .k }"#,
        r#"{ span.k = "x" }"#,
        "{}",
        // Issue #592 part 1: later `{…}` filters and `select()` after a
        // single filter.
        r#"{ .a = 1 } | { }"#,
        r#"{ .a = 1 } | ({ name = "b" })"#,
        r#"{ } | { .a = 1 } | { name = "b" }"#,
        r#"{ .a = 1 } | select(name)"#,
        r#"{ status = error } | select(span.http.status_code, .foo, resource.service.name)"#,
    ] {
        if plan_statement(&fork_plan(query), "spans", "traces", "resources").is_none() {
            wrong.push(format!(
                "{query}: today's engine's, must be the statement's"
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
