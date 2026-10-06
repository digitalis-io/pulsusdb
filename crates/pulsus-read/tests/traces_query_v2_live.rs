//! Issue #588: what the span-scope predicate compiler ANSWERS, against a
//! real ClickHouse.
//!
//! Every case here compiles one TraceQL filter body with
//! [`compile_span_predicate`], issues the one statement
//! [`span_membership_sql`] renders over a request window, and compares the
//! span ids it returns with a written-out list. Nothing is asserted about
//! the statement's text — `tests/traces_compile_v2.rs` holds the text
//! assertions, and the two halves meet at `T-C4`, which freezes the exact
//! statement this suite issues.
//!
//! **Seven fixtures, each test function in its own database.**
//!
//! * The worked fixture of `docs/TraceQL/functional-requirements.md` §6.1 —
//!   three traces, nine spans, every attribute type, one event, one link,
//!   one retried push. Its answers for `F3`–`F20` carry a committed
//!   reference-checked value (`docs/TraceQL/measure/results/fixture-answers.tsv`);
//!   every other answer below pins this product's own documented rule.
//! * Fixture T — seven spans, for the three things §6.1 cannot express: a
//!   boolean `true`, an integer at and just past `f64`'s exactness limit,
//!   and an id containing a hex letter. §6.1's ids are digits only, so no
//!   uppercase literal can discriminate there.
//! * Fixture R — four spans, one resource each: resource and
//!   instrumentation values of every type, and a resource row deleted.
//! * Fixture S — ten spans, one stored form of `service.name` each.
//! * Fixture C — the catalogue fixture, 34 spans transcribed from
//!   `docs/TraceQL/measure/fixture/make_catalogue_fixture.py`, whose answers
//!   are `docs/TraceQL/query-catalogue-accepted.md`'s.
//! * Fixture V — eight spans whose events, links and scopes tell
//!   any-match from first-element and all-match, and the unscoped chain's
//!   order from any other.
//! * Fixture W — twenty-three spans, two fields each, for comparing one
//!   field with another: every stored type against every other, integers
//!   past `f64`'s exactness limit, arrays on either side, and the
//!   intrinsics against attributes.
//!
//! Each is seeded by building the OTLP request bodies and handing them to
//! `pulsus_write::parse_trace_landing`, then inserting the rows it
//! produces into `trace_landing`: the fixture's bytes go through the
//! shipped encoder, so a case cannot pass against an `attrs` value the
//! writer would never produce. `spans_mv` populates `spans`.
//!
//! **The base instant is `now`, not a committed one.** `spans` carries a
//! TTL with `ttl_only_drop_parts = 1`, so a fixed historical base would
//! make a whole part eligible for deletion at insert time. Every span id
//! and every answer below is base-independent; the request window is
//! `[base, base + 10s)`.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:26.3
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test traces_query_v2_live
//! podman rm -f pulsus-ch-test
//! ```

use std::time::Duration as StdDuration;

use futures::StreamExt;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList, any_value,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span};
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_read::traces::spans::predicate::{
    PredicateCtx, compile_span_leaf_in, compile_span_predicate, compile_span_predicate_in,
    span_membership_sql,
};
use pulsus_read::traces::window_sql::WindowSql;
use pulsus_schema::{RenderCtx, SchemaParams};
use pulsus_schema_testkit::run_init;
use pulsus_traceql::{
    AttrScope, ComparisonOp, Field, FieldExpr, SpansetExpr, SpansetFilter, Value,
};
use pulsus_write::{TraceLandingRow, parse_trace_landing};

// ---------------------------------------------------------------------
// the live gate
// ---------------------------------------------------------------------

/// `true` when this suite should run. Skips cleanly on a developer machine
/// with no container; **panics** rather than skipping when the gate is
/// absent inside a live CI job, so a lost `env:` block reddens the build
/// instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-read/tests/traces_query_v2_live.rs for setup)"
            );
            return;
        }
    };
}

fn base_config() -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: "default".to_string(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: StdDuration::from_secs(60),
        ..ChConnConfig::default()
    }
}

/// The ONE `CREATE DATABASE` call site in this file (issue #616): both
/// fixtures reach the server through here, so the per-checkout prefix
/// cannot be lost on one of them.
async fn fresh_db(db: &str) -> ChClient {
    let admin = ChClient::new(base_config()).await.expect("connect admin");
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the test database");
    let ctx: SchemaParams = RenderCtx::for_tests(db);
    run_init(&admin, &ctx).await.expect("run_init");
    ChClient::new(ChConnConfig {
        database: db.to_string(),
        ..base_config()
    })
    .await
    .expect("connect to the test database")
}

/// Drops `db` by its exact name — never by a pattern.
async fn drop_db(db: &str) {
    let admin = ChClient::new(base_config()).await.expect("connect admin");
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the test database");
}

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("a clock after the epoch")
            .as_nanos(),
    )
    .expect("a representable instant")
}

/// The window every statement below carries: ten seconds from a whole
/// second, half-open, the one request-window rule.
const WINDOW_NS: i64 = 10_000_000_000;

/// The table every statement below reads. The suite owns its database, so
/// the unqualified name resolves through the client's own `database`.
const SPANS_TABLE: &str = "spans";

// ---------------------------------------------------------------------
// reading
// ---------------------------------------------------------------------

/// The membership statement's one column. It is aliased `id` and NOT
/// `span_id`: a select-list alias shadows the column of the same name for
/// the whole statement, so `AS span_id` would make every `span:id`
/// predicate compare the hex of the hex text. `span_membership_sql`'s own
/// doc carries the measurement.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct IdRow {
    id: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CountRow {
    n: u64,
}

/// `final = 1` on every read, as every read on these tables is
/// (`docs/TraceQL/measure/fixture/fixture_answers.py`): `spans` is a
/// `ReplacingMergeTree` and a retried push collapses on the sorting key
/// only after a merge.
fn read_settings() -> QuerySettings {
    QuerySettings::new().set("final", 1)
}

/// The driver's own `?` placeholder is doubled before the text is sent, as
/// every other live suite in this crate does: an anchored regex carries
/// `(?:` and a single `?` is a bind argument the statement has none of.
/// The RENDERED text is untouched — `T-C4` freezes that, and the doubling
/// is the transport's concern rather than the compiler's.
async fn ids_of(client: &ChClient, sql: &str) -> Vec<String> {
    let mut stream = client
        .query_stream::<IdRow>(&sql.replace('?', "??"), &read_settings())
        .await
        .unwrap_or_else(|e| panic!("the membership read failed: {e}\nSQL:\n{sql}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(
            row.unwrap_or_else(|e| panic!("decode failed: {e}\nSQL:\n{sql}"))
                .id,
        );
    }
    out
}

async fn count(client: &ChClient, sql: &str) -> u64 {
    let mut stream = client
        .query_stream::<CountRow>(sql, &read_settings())
        .await
        .unwrap_or_else(|e| panic!("the count read failed: {e}\nSQL:\n{sql}"));
    stream.next().await.expect("one row").expect("decode").n
}

/// The filter body of a one-spanset query. A query this suite writes that
/// is not one filter with a body is a mistake in the case, not an
/// outcome, so it panics.
fn body(query: &str) -> FieldExpr {
    let parsed =
        pulsus_traceql::parse(query).unwrap_or_else(|e| panic!("{query} must parse: {e:?}"));
    match parsed.spanset {
        SpansetExpr::Filter(SpansetFilter { body: Some(b) }) => b,
        other => panic!("{query}: expected one filter with a body, got {other}"),
    }
}

/// Compiles `query` and returns the span ids the one membership statement
/// answers with.
async fn answer(client: &ChClient, w: WindowSql, query: &str) -> Vec<String> {
    let predicate = compile_span_predicate(&body(query))
        .unwrap_or_else(|e| panic!("{query} must compile: {e}"));
    let sql = span_membership_sql(SPANS_TABLE, w, &predicate);
    ids_of(client, &sql).await
}

/// The ids a predicate text written out BY THIS SUITE answers with — used
/// only to issue the rendering a case's rule forbids, so a cell that would
/// answer the same either way is reported rather than counted. It is
/// deliberately a plainer statement than [`span_membership_sql`]'s: the
/// row bound alone selects the same fixture rows, and nothing here is
/// asserting a statement shape.
async fn raw_answer(client: &ChClient, w: WindowSql, predicate: &str) -> Vec<String> {
    let sql = format!(
        "SELECT lower(hex(span_id)) AS id FROM {SPANS_TABLE} WHERE {} AND ({predicate}) \
         ORDER BY id",
        w.span_time_clause()
    );
    ids_of(client, &sql).await
}

// ---------------------------------------------------------------------
// the operator set
// ---------------------------------------------------------------------

/// Every `ComparisonOp` the language has.
///
/// **The compile gate is [`op_class`], not this array**: that `match` has
/// no wildcard arm, so a ninth variant fails to compile until it is
/// classified. This array is the iteration order, and
/// `the_ordered_operators_are_the_complement_of_the_four_that_are_not`
/// checks the two classes partition it with no member in both.
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

/// Whether `op` is one of the four ordered comparisons. **No wildcard
/// arm**: a ninth `ComparisonOp` variant fails to compile here rather
/// than being skipped by the matrices below.
fn op_class_is_ordered(op: ComparisonOp) -> bool {
    match op {
        ComparisonOp::Gt | ComparisonOp::Gte | ComparisonOp::Lt | ComparisonOp::Lte => true,
        ComparisonOp::Eq | ComparisonOp::Neq | ComparisonOp::Re | ComparisonOp::Nre => false,
    }
}

/// The ordered operators, as the COMPLEMENT of the four that are not —
/// never a list, so `UC6`'s cell count is computed from the language's own
/// operator set rather than transcribed. `pulsus_traceql` offers no
/// constant for it.
fn ordered_ops() -> Vec<ComparisonOp> {
    ALL_OPS
        .into_iter()
        .filter(|op| op_class_is_ordered(*op))
        .collect()
}

fn op_symbol(op: ComparisonOp) -> &'static str {
    match op {
        ComparisonOp::Gt => ">",
        ComparisonOp::Gte => ">=",
        ComparisonOp::Lt => "<",
        ComparisonOp::Lte => "<=",
        ComparisonOp::Eq => "=",
        ComparisonOp::Neq => "!=",
        ComparisonOp::Re => "=~",
        ComparisonOp::Nre => "!~",
    }
}

#[test]
fn the_ordered_operators_are_the_complement_of_the_four_that_are_not() {
    for (i, a) in ALL_OPS.iter().enumerate() {
        for b in &ALL_OPS[i + 1..] {
            assert_ne!(a, b, "ALL_OPS lists an operator twice");
        }
    }
    let ordered = ordered_ops();
    let not_ordered: Vec<ComparisonOp> = ALL_OPS
        .into_iter()
        .filter(|op| !op_class_is_ordered(*op))
        .collect();
    assert_eq!(
        not_ordered,
        vec![
            ComparisonOp::Eq,
            ComparisonOp::Neq,
            ComparisonOp::Re,
            ComparisonOp::Nre
        ],
        "the ordered operators must be exactly the complement of = != =~ !~"
    );
    assert_eq!(
        ordered.len() + not_ordered.len(),
        ALL_OPS.len(),
        "every operator belongs to exactly one class"
    );
    assert_eq!(ordered.len(), 4);
}

// ---------------------------------------------------------------------
// OTLP fixture builders
// ---------------------------------------------------------------------

fn str_value(s: &str) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::StringValue(s.to_string())),
    }
}

fn int_value(i: i64) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::IntValue(i)),
    }
}

fn double_value(d: f64) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::DoubleValue(d)),
    }
}

fn bool_value(b: bool) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::BoolValue(b)),
    }
}

fn str_array_value(values: &[&str]) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::ArrayValue(ArrayValue {
            values: values.iter().map(|v| str_value(v)).collect(),
        })),
    }
}

fn int_array_value(values: &[i64]) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::ArrayValue(ArrayValue {
            values: values.iter().map(|v| int_value(*v)).collect(),
        })),
    }
}

fn kv(key: &str, value: AnyValue) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(value),
        key_strindex: 0,
    }
}

/// The scope all sixteen fixture spans share — `io.opentelemetry.http` /
/// `2.9.0`, which is what `IN1` reads.
fn scope() -> InstrumentationScope {
    InstrumentationScope {
        name: "io.opentelemetry.http".to_string(),
        version: "2.9.0".to_string(),
        attributes: vec![kv("otel.scope.build", str_value("release"))],
        dropped_attributes_count: 0,
    }
}

/// One request: one resource, one scope, the spans given.
fn request(service: &str, pod: &str, spans: Vec<Span>) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    kv("service.name", str_value(service)),
                    kv("deployment.environment.name", str_value("prod")),
                    kv("k8s.pod.name", str_value(pod)),
                ],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(scope()),
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// One span. `status_code` 0 leaves the `status` field absent, as the
/// fixture generator does.
#[allow(clippy::too_many_arguments)]
fn span_of(
    trace_id: Vec<u8>,
    span_id: Vec<u8>,
    parent_span_id: Vec<u8>,
    name: &str,
    kind: i32,
    start_ns: i64,
    duration_ns: i64,
    attributes: Vec<KeyValue>,
    status_code: i32,
    events: Vec<span::Event>,
    links: Vec<span::Link>,
) -> Span {
    Span {
        trace_id,
        span_id,
        parent_span_id,
        trace_state: String::new(),
        flags: 0,
        name: name.to_string(),
        kind,
        start_time_unix_nano: start_ns as u64,
        end_time_unix_nano: (start_ns + duration_ns) as u64,
        attributes,
        dropped_attributes_count: 0,
        events,
        dropped_events_count: 0,
        links,
        dropped_links_count: 0,
        status: if status_code == 0 {
            None
        } else {
            Some(Status {
                message: "boom".to_string(),
                code: status_code,
            })
        },
    }
}

/// Inserts every landing row one request produces, in one block — one push
/// is one insert. `token` is the insert's own deduplication token, so the
/// §6.1 fixture's RETRIED push lands a second copy rather than being
/// suppressed at the landing table: collapsing it is `final = 1`'s job and
/// this suite exercises it rather than assuming it.
async fn land(client: &ChClient, req: &ExportTraceServiceRequest, token: &str) {
    let parsed = parse_trace_landing(req, now_ns()).expect("the landing decode");
    let received_ms = now_ns() / 1_000_000;
    let mut rows: Vec<TraceLandingRow> = Vec::with_capacity(parsed.total_rows() as usize);
    rows.extend(
        parsed
            .spans
            .iter()
            .cloned()
            .map(|s| TraceLandingRow::span(received_ms, s)),
    );
    rows.extend(
        parsed
            .resources
            .iter()
            .cloned()
            .map(|r| TraceLandingRow::resource(received_ms, r)),
    );
    rows.extend(
        parsed
            .tag_names
            .iter()
            .cloned()
            .map(|t| TraceLandingRow::tag_name(received_ms, t)),
    );
    rows.extend(
        parsed
            .tag_values
            .iter()
            .cloned()
            .map(|t| TraceLandingRow::tag_value(received_ms, t)),
    );
    client
        .insert_block_with(
            "trace_landing",
            &rows,
            &QuerySettings::trace_landing_insert(token, 1_048_576),
        )
        .await
        .expect("the landing insert");
}

// ---------------------------------------------------------------------
// the §6.1 fixture
// ---------------------------------------------------------------------

/// `…0001` to `…0009`, by their last four hex digits.
fn id61(short: &str) -> String {
    format!("{short:0>16}")
}

fn span_id_bytes(n: u8) -> Vec<u8> {
    let mut out = vec![0u8; 8];
    out[7] = n;
    out
}

/// The six §6.1 request bodies, transcribed from
/// `docs/TraceQL/measure/fixture/make_fixture.py` — five distinct ones and
/// the retried copy of the first.
fn fixture_61_bodies(base_ns: i64) -> Vec<ExportTraceServiceRequest> {
    let t1 = vec![0x11u8; 16];
    let t2 = vec![0x22u8; 16];
    let t3 = vec![0x33u8; 16];
    let no_parent: Vec<u8> = Vec::new();

    let body0 = request(
        "frontend",
        "frontend-a",
        vec![
            span_of(
                t1.clone(),
                span_id_bytes(1),
                no_parent.clone(),
                "GET /cart",
                2,
                base_ns,
                500_000_000,
                vec![
                    kv("http.request.method", str_value("GET")),
                    kv("http.route", str_value("/cart")),
                    kv("http.response.status_code", int_value(500)),
                    kv("app.user.id", str_value("u-1")),
                    kv("app.cache.hit", bool_value(false)),
                ],
                2,
                Vec::new(),
                Vec::new(),
            ),
            span_of(
                t1.clone(),
                span_id_bytes(2),
                span_id_bytes(1),
                "checkout.Create",
                3,
                base_ns + 10_000_000,
                400_000_000,
                vec![kv("rpc.system", str_value("grpc"))],
                0,
                Vec::new(),
                Vec::new(),
            ),
        ],
    );

    let body1 = request(
        "checkout",
        "checkout-a",
        vec![
            span_of(
                t1.clone(),
                span_id_bytes(3),
                span_id_bytes(2),
                "checkout.Create",
                2,
                base_ns + 20_000_000,
                380_000_000,
                vec![
                    kv("rpc.system", str_value("grpc")),
                    kv("app.items.count", int_value(3)),
                    kv("app.discount.ratio", double_value(0.25)),
                    kv("app.tags", str_array_value(&["gold", "eu"])),
                ],
                0,
                Vec::new(),
                Vec::new(),
            ),
            span_of(
                t1.clone(),
                span_id_bytes(4),
                span_id_bytes(3),
                "payment.Charge",
                3,
                base_ns + 30_000_000,
                300_000_000,
                vec![kv("rpc.system", str_value("grpc"))],
                0,
                Vec::new(),
                Vec::new(),
            ),
        ],
    );

    let body2 = request(
        "payment",
        "payment-a",
        vec![
            span_of(
                t1.clone(),
                span_id_bytes(5),
                span_id_bytes(4),
                "payment.Charge",
                2,
                base_ns + 40_000_000,
                280_000_000,
                vec![
                    kv("rpc.system", str_value("grpc")),
                    kv("payment.amount", double_value(12.5)),
                    kv("payment.currency", str_value("EUR")),
                ],
                2,
                vec![span::Event {
                    time_unix_nano: (base_ns + 200_000_000) as u64,
                    name: "exception".to_string(),
                    attributes: vec![
                        kv(
                            "exception.type",
                            str_value("java.lang.IllegalStateException"),
                        ),
                        kv("exception.message", str_value("no funds")),
                    ],
                    dropped_attributes_count: 0,
                }],
                Vec::new(),
            ),
            span_of(
                t1.clone(),
                span_id_bytes(6),
                span_id_bytes(5),
                "SELECT ledger",
                3,
                base_ns + 60_000_000,
                120_000_000,
                vec![
                    kv("db.system.name", str_value("postgresql")),
                    kv("db.query.text", str_value("SELECT 1")),
                    kv("http.response.status_code", str_value("200")),
                ],
                0,
                Vec::new(),
                Vec::new(),
            ),
        ],
    );

    let body3 = request(
        "frontend",
        "frontend-b",
        vec![span_of(
            t2,
            span_id_bytes(7),
            no_parent.clone(),
            "GET /health",
            2,
            base_ns + 1_000_000_000,
            5_000_000,
            vec![
                kv("http.request.method", str_value("GET")),
                kv("http.route", str_value("/health")),
                kv("http.response.status_code", int_value(200)),
            ],
            0,
            Vec::new(),
            Vec::new(),
        )],
    );

    let body4 = request(
        "accounting",
        "accounting-a",
        vec![
            span_of(
                t3.clone(),
                span_id_bytes(8),
                no_parent,
                "orders process",
                5,
                base_ns + 1_999_000_000,
                2_000_000_000,
                vec![
                    kv("messaging.system", str_value("kafka")),
                    kv("messaging.destination.name", str_value("orders")),
                ],
                0,
                Vec::new(),
                vec![span::Link {
                    trace_id: t1,
                    span_id: span_id_bytes(5),
                    trace_state: String::new(),
                    attributes: vec![kv("link.kind", str_value("producer"))],
                    dropped_attributes_count: 0,
                    flags: 0,
                }],
            ),
            span_of(
                t3,
                span_id_bytes(9),
                span_id_bytes(8),
                "SELECT ledger",
                3,
                base_ns + 2_100_000_000,
                50_000_000,
                vec![
                    kv("db.system.name", str_value("postgresql")),
                    kv("db.query.text", str_value("SELECT 2")),
                ],
                0,
                Vec::new(),
                Vec::new(),
            ),
        ],
    );

    // The retried push is the FIRST body again, byte for byte.
    let retry = body0.clone();
    vec![body0, retry, body1, body2, body3, body4]
}

// ---------------------------------------------------------------------
// fixture T
// ---------------------------------------------------------------------

/// `a1` to `a7`, by their last two hex digits.
fn idt(short: &str) -> String {
    format!("0a1b2c3d4e5f60{short}")
}

fn t_span_id(last: u8) -> Vec<u8> {
    vec![0x0a, 0x1b, 0x2c, 0x3d, 0x4e, 0x5f, 0x60, last]
}

const T_TRACE_HEX: &str = "0a1b2c3d4e5f60710a1b2c3d4e5f6071";

fn t_trace_id() -> Vec<u8> {
    vec![
        0x0a, 0x1b, 0x2c, 0x3d, 0x4e, 0x5f, 0x60, 0x71, 0x0a, 0x1b, 0x2c, 0x3d, 0x4e, 0x5f, 0x60,
        0x71,
    ]
}

/// The one fixture-T body. All seven spans share one `start_ns`, one
/// `name` and `kind = 1`: nothing here tests an intrinsic except the three
/// id columns, and a shared value is one fewer thing to get wrong. All
/// three large integers ride OTLP's `intValue`, which is an `int64` on the
/// wire, so no part of the path is special-cased.
fn fixture_t_body(base_ns: i64) -> ExportTraceServiceRequest {
    let start = base_ns + 1_000_000;
    let root = t_span_id(0xa1);
    let mut spans = Vec::new();
    let attrs: [(u8, KeyValue); 7] = [
        (0xa1, kv("app.cache.hit", bool_value(true))),
        (0xa2, kv("app.cache.hit", bool_value(false))),
        (0xa3, kv("app.cache.hit", str_value("true"))),
        (0xa4, kv("app.items.count", int_value(i64::MAX))),
        (0xa5, kv("app.items.count", int_value(3))),
        (0xa6, kv("app.seq", int_value(9_007_199_254_740_992))),
        (0xa7, kv("app.seq", int_value(9_007_199_254_740_993))),
    ];
    for (last, attr) in attrs {
        spans.push(span_of(
            t_trace_id(),
            t_span_id(last),
            if last == 0xa1 {
                Vec::new()
            } else {
                root.clone()
            },
            "span",
            1,
            start,
            1_000_000,
            vec![attr],
            0,
            Vec::new(),
            Vec::new(),
        ));
    }
    request("tfixture", "tfixture-a", spans)
}

// ---------------------------------------------------------------------
// the case table
// ---------------------------------------------------------------------

/// One case: its name, the query, and the span ids it must answer with, in
/// the ascending hex order the statement's `ORDER BY span_id` returns.
struct Case {
    name: &'static str,
    query: &'static str,
    expect: &'static [&'static str],
}

const ALL_61: &[&str] = &[
    "0001", "0002", "0003", "0004", "0005", "0006", "0007", "0008", "0009",
];
const ALL_T: &[&str] = &["a1", "a2", "a3", "a4", "a5", "a6", "a7"];

async fn run_cases(
    client: &ChClient,
    w: WindowSql,
    cases: &[Case],
    expand: fn(&str) -> String,
) -> Vec<String> {
    let mut wrong = Vec::new();
    for case in cases {
        let want: Vec<String> = case.expect.iter().copied().map(expand).collect();
        let got = answer(client, w, case.query).await;
        if got != want {
            let predicate = compile_span_predicate(&body(case.query))
                .map(|p| p.sql().to_string())
                .unwrap_or_else(|e| format!("<refused: {e}>"));
            wrong.push(format!(
                "{}  {}\n   expected {:?}\n   answered {:?}\n   predicate: {predicate}",
                case.name, case.query, want, got
            ));
        }
    }
    wrong
}

// ---------------------------------------------------------------------
// the §6.1 cases
// ---------------------------------------------------------------------

const CASES_61: &[Case] = &[
    Case {
        name: "T-A1",
        query: r#"{ span.http.response.status_code != 200 }"#,
        expect: &[
            "0001", "0002", "0003", "0004", "0005", "0006", "0008", "0009",
        ],
    },
    Case {
        name: "T-A2a (F4)",
        query: r#"{ span.http.response.status_code >= 500 }"#,
        expect: &["0001"],
    },
    Case {
        name: "T-A2b (F5)",
        query: r#"{ span.http.response.status_code = "200" }"#,
        expect: &["0006"],
    },
    Case {
        name: "T-A2c",
        query: r#"{ span.http.response.status_code = 200 }"#,
        expect: &["0007"],
    },
    Case {
        name: "T-A2d",
        query: r#"{ span.http.response.status_code != 500 }"#,
        expect: &[
            "0002", "0003", "0004", "0005", "0006", "0007", "0008", "0009",
        ],
    },
    Case {
        name: "F6",
        query: r#"{ span.app.discount.ratio > 0.2 }"#,
        expect: &["0003"],
    },
    Case {
        name: "F7",
        query: r#"{ span.app.tags = "gold" }"#,
        expect: &["0003"],
    },
    Case {
        name: "F7b",
        query: r#"{ span.app.tags = "silver" }"#,
        expect: &[],
    },
    Case {
        name: "F8",
        query: r#"{ span.app.cache.hit = false }"#,
        expect: &["0001"],
    },
    Case {
        name: "T-A16",
        query: r#"{ span.app.cache.hit }"#,
        expect: &[],
    },
    Case {
        name: "T-A17",
        query: r#"{ span.app.tags != nil }"#,
        expect: &["0003"],
    },
    Case {
        name: "T-A17b",
        query: r#"{ span.http.response.status_code != nil }"#,
        expect: &["0001", "0006", "0007"],
    },
    Case {
        name: "T-A18",
        query: r#"{ span.app.tags = nil }"#,
        expect: &[
            "0001", "0002", "0004", "0005", "0006", "0007", "0008", "0009",
        ],
    },
    Case {
        name: "T-A18b",
        query: r#"{ span.http.response.status_code = nil }"#,
        expect: &["0002", "0003", "0004", "0005", "0008", "0009"],
    },
    Case {
        name: "F3",
        query: r#"{ status = error }"#,
        expect: &["0001", "0005"],
    },
    Case {
        name: "F20",
        query: r#"{ kind = consumer }"#,
        expect: &["0008"],
    },
    Case {
        name: "F9",
        query: r#"{ duration > 1s }"#,
        expect: &["0008"],
    },
    Case {
        name: "F9b >=",
        query: r#"{ duration >= 2s }"#,
        expect: &["0008"],
    },
    Case {
        name: "F9b >",
        query: r#"{ duration > 2s }"#,
        expect: &[],
    },
    Case {
        name: "DUR1 1ns",
        query: r#"{ span.app.items.count > 1ns }"#,
        expect: &["0003"],
    },
    Case {
        name: "DUR1 1us",
        query: r#"{ span.app.items.count > 1us }"#,
        expect: &[],
    },
    Case {
        name: "F14",
        query: r#"{ name = "SELECT ledger" }"#,
        expect: &["0006", "0009"],
    },
    Case {
        name: "SM1",
        query: r#"{ statusMessage = "boom" }"#,
        expect: &["0001", "0005"],
    },
    Case {
        name: "ORD1",
        query: r#"{ name > "" }"#,
        expect: ALL_61,
    },
    Case {
        name: "ORD2 >",
        query: r#"{ name > "SELECT ledger" }"#,
        expect: &["0002", "0003", "0004", "0005", "0008"],
    },
    Case {
        name: "ORD2 <=",
        query: r#"{ name <= "SELECT ledger" }"#,
        expect: &["0001", "0006", "0007", "0009"],
    },
    Case {
        name: "ORD3 >",
        query: r#"{ span.http.route > "/cart" }"#,
        expect: &["0007"],
    },
    Case {
        name: "ORD3 >=",
        query: r#"{ span.http.route >= "/cart" }"#,
        expect: &["0001", "0007"],
    },
    Case {
        name: "ORD3 <",
        query: r#"{ span.http.route < "/health" }"#,
        expect: &["0001"],
    },
    Case {
        name: "LOL1",
        query: r#"{ 500 <= span.http.response.status_code }"#,
        expect: &["0001"],
    },
    Case {
        name: "LOL2",
        query: r#"{ 200 != span.http.response.status_code }"#,
        expect: &[
            "0001", "0002", "0003", "0004", "0005", "0006", "0008", "0009",
        ],
    },
    Case {
        name: "RE1",
        query: r#"{ span.http.route =~ "/ca.*" }"#,
        expect: &["0001"],
    },
    Case {
        name: "RE2",
        query: r#"{ span.http.route =~ "ca" }"#,
        expect: &[],
    },
    Case {
        name: "RE3",
        query: r#"{ span.http.route !~ "ca" }"#,
        expect: ALL_61,
    },
    Case {
        name: "RE4 anchored",
        query: r#"{ span:id =~ "0{15}1" }"#,
        expect: &["0001"],
    },
    Case {
        name: "RE4 unanchored",
        query: r#"{ span:id =~ "0001" }"#,
        expect: &[],
    },
    Case {
        name: "RE5",
        query: r#"{ span:parentID !~ "0000" }"#,
        expect: ALL_61,
    },
    Case {
        name: "SK1",
        query: r#"{ span.app.items.count < maxInt }"#,
        expect: &["0003"],
    },
    Case {
        name: "SK2",
        query: r#"{ span.app.items.count = minInt }"#,
        expect: &[],
    },
    Case {
        name: "ID1",
        query: r#"{ span:id = "0000000000000001" }"#,
        expect: &["0001"],
    },
    Case {
        name: "ID3",
        query: r#"{ span:parentID = "0000000000000008" }"#,
        expect: &["0009"],
    },
    Case {
        name: "ID4",
        query: r#"{ span:parentID = "" }"#,
        expect: &[],
    },
    Case {
        name: "ID5",
        query: r#"{ span:parentID = "0000000000000000" }"#,
        expect: &["0001", "0007", "0008"],
    },
    Case {
        name: "IN1 name",
        query: r#"{ instrumentation:name = "io.opentelemetry.http" }"#,
        expect: ALL_61,
    },
    Case {
        name: "IN1 version",
        query: r#"{ instrumentation:version = "2.9.0" }"#,
        expect: ALL_61,
    },
    Case {
        name: "IN1 miss",
        query: r#"{ instrumentation:name = "nope" }"#,
        expect: &[],
    },
    Case {
        name: "BL1 true",
        query: r#"{ true }"#,
        expect: ALL_61,
    },
    Case {
        name: "BL1 false",
        query: r#"{ false }"#,
        expect: &[],
    },
];

/// The §6.1 fixture's answers — 47 cases over nine spans.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_predicate_compiler_answers_the_worked_fixture() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_read_it_t588_fixture61");
    let client = fresh_db(&db).await;
    let base_ns = (now_ns() / 1_000_000_000) * 1_000_000_000;
    for (i, req) in fixture_61_bodies(base_ns).into_iter().enumerate() {
        land(&client, &req, &format!("t588-61-{i}-{}", now_ns())).await;
    }
    let w = WindowSql::start_closed_end_open(base_ns, base_ns + WINDOW_NS);

    // Section 12: before any predicate runs, the seeding itself. A missing
    // `spans_mv` would leave the insert succeeding and every case
    // answering nothing, which reads as a wrong predicate.
    let seeded = count(&client, &format!("SELECT count() AS n FROM {SPANS_TABLE}")).await;
    assert_eq!(
        seeded, 9,
        "the §6.1 fixture seeds nine spans through spans_mv (the retried push collapses under \
         final = 1); nothing below can be read as a predicate result until this holds"
    );

    let wrong = run_cases(&client, w, CASES_61, id61).await;
    drop_db(&db).await;
    assert!(
        wrong.is_empty(),
        "{} of {} §6.1 cases answer something else:\n\n{}",
        wrong.len(),
        CASES_61.len(),
        wrong.join("\n\n")
    );
}

// ---------------------------------------------------------------------
// the fixture-T cases
// ---------------------------------------------------------------------

const CASES_T: &[Case] = &[
    Case {
        name: "T-A16b",
        query: r#"{ span.app.cache.hit }"#,
        expect: &["a1"],
    },
    Case {
        name: "T-A16c",
        query: r#"{ span.app.cache.hit = false }"#,
        expect: &["a2"],
    },
    Case {
        name: "T-A16d",
        query: r#"{ span.app.cache.hit != true }"#,
        expect: &["a2", "a3", "a4", "a5", "a6", "a7"],
    },
    Case {
        name: "T-A16e",
        query: r#"{ span.app.cache.hit != nil }"#,
        expect: &["a1", "a2", "a3"],
    },
    Case {
        name: "T-A16f",
        query: r#"{ span.app.cache.hit = "true" }"#,
        expect: &["a3"],
    },
    Case {
        name: "SK3",
        query: r#"{ span.app.items.count < maxInt }"#,
        expect: &["a5"],
    },
    Case {
        name: "SK4",
        query: r#"{ span.app.items.count = maxInt }"#,
        expect: &["a4"],
    },
    // `SK5` — the six operators against `2^53 + 1`. Every one of the six
    // answers differently from the same literal taken through `f64`, which
    // is why the decimal literal does not go through one.
    Case {
        name: "SK5 !=",
        query: r#"{ span.app.seq != 9007199254740993 }"#,
        expect: &["a1", "a2", "a3", "a4", "a5", "a6"],
    },
    Case {
        name: "SK5 <",
        query: r#"{ span.app.seq < 9007199254740993 }"#,
        expect: &["a6"],
    },
    Case {
        name: "SK5 <=",
        query: r#"{ span.app.seq <= 9007199254740993 }"#,
        expect: &["a6", "a7"],
    },
    Case {
        name: "SK5 =",
        query: r#"{ span.app.seq = 9007199254740993 }"#,
        expect: &["a7"],
    },
    Case {
        name: "SK5 >",
        query: r#"{ span.app.seq > 9007199254740993 }"#,
        expect: &[],
    },
    Case {
        name: "SK5 >=",
        query: r#"{ span.app.seq >= 9007199254740993 }"#,
        expect: &["a7"],
    },
    Case {
        name: "UC1",
        query: r#"{ span:id = "0A1B2C3D4E5F60A1" }"#,
        expect: &["a1"],
    },
    Case {
        name: "UC2",
        query: r#"{ trace:id = "0A1B2C3D4E5F60710A1B2C3D4E5F6071" }"#,
        expect: ALL_T,
    },
    Case {
        name: "UC3",
        query: r#"{ span:parentID = "0A1B2C3D4E5F60A1" }"#,
        expect: &["a2", "a3", "a4", "a5", "a6", "a7"],
    },
    Case {
        name: "UC4",
        query: r#"{ span:id =~ "0A1B.*" }"#,
        expect: &[],
    },
    Case {
        name: "ORD5 <",
        query: r#"{ span:id < "0a1b2c3d4e5f60a4" }"#,
        expect: &["a1", "a2", "a3"],
    },
    Case {
        name: "ORD5 >=",
        query: r#"{ span:id >= "0a1b2c3d4e5f60a4" }"#,
        expect: &["a4", "a5", "a6", "a7"],
    },
    // `UC5` — the complement, on all three id columns. An earlier draft
    // admitted `!=` to the raw-byte fast path and then rendered `=` there,
    // which inverts every one of these three answers.
    Case {
        name: "UC5 span:id",
        query: r#"{ span:id != "0a1b2c3d4e5f60a1" }"#,
        expect: &["a2", "a3", "a4", "a5", "a6", "a7"],
    },
    Case {
        name: "UC5 span:parentID",
        query: r#"{ span:parentID != "0a1b2c3d4e5f60a1" }"#,
        expect: &["a1"],
    },
    Case {
        name: "UC5 trace:id",
        query: r#"{ trace:id != "0a1b2c3d4e5f60710a1b2c3d4e5f6071" }"#,
        expect: &[],
    },
];

/// Fixture T's answers — the boolean `true`, the `f64` exactness limit and
/// the letter-bearing ids §6.1 cannot express.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_predicate_compiler_answers_the_id_boolean_and_integer_fixture() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_read_it_t588_fixturet");
    let client = fresh_db(&db).await;
    let base_ns = (now_ns() / 1_000_000_000) * 1_000_000_000;
    land(
        &client,
        &fixture_t_body(base_ns),
        &format!("t588-t-{}", now_ns()),
    )
    .await;
    let w = WindowSql::start_closed_end_open(base_ns, base_ns + WINDOW_NS);

    let seeded = count(&client, &format!("SELECT count() AS n FROM {SPANS_TABLE}")).await;
    assert_eq!(
        seeded, 7,
        "fixture T seeds seven spans through spans_mv; nothing below can be read as a predicate \
         result until this holds"
    );

    let mut wrong = run_cases(&client, w, CASES_T, idt).await;
    wrong.extend(uc6_cells(&client, w).await);
    drop_db(&db).await;
    assert!(
        wrong.is_empty(),
        "{} of {} fixture-T checks answer something else:\n\n{}",
        wrong.len(),
        CASES_T.len() + 2 * UC6_COLUMNS.len() * ordered_ops().len(),
        wrong.join("\n\n")
    );
}

// ---------------------------------------------------------------------
// `UC6` — the full-width uppercase ordered matrix
// ---------------------------------------------------------------------

/// The three id columns and the uppercase full-width literal each is
/// compared against. Each literal is **unequal** to every stored value, so
/// the strict and non-strict forms can differ; `span:id`'s sits INSIDE the
/// stored range, so that column is the one separating `<` from `<=` and
/// `>` from `>=`.
const UC6_COLUMNS: [(&str, &str, &str); 3] = [
    ("span:id", "span_id", "0A1B2C3D4E5F60A4"),
    ("span:parentID", "parent_span_id", "0A1B2C3D4E5F60A2"),
    ("trace:id", "trace_id", "0A1B2C3D4E5F60710A1B2C3D4E5F6072"),
];

/// Every cell's two answers: the one the rule gives (the raw literal) and
/// the one the rendering it forbids gives (the lowercased literal). All
/// twelve differ, which is what makes none of them decoration.
fn uc6_expected(
    field: &str,
    op: ComparisonOp,
) -> (&'static [&'static str], &'static [&'static str]) {
    const A123: &[&str] = &["a1", "a2", "a3"];
    const A1234: &[&str] = &["a1", "a2", "a3", "a4"];
    const A567: &[&str] = &["a5", "a6", "a7"];
    const A4567: &[&str] = &["a4", "a5", "a6", "a7"];
    const A1: &[&str] = &["a1"];
    const A2_7: &[&str] = &["a2", "a3", "a4", "a5", "a6", "a7"];
    const NONE: &[&str] = &[];
    match (field, op) {
        ("span:id", ComparisonOp::Lt) => (NONE, A123),
        ("span:id", ComparisonOp::Lte) => (NONE, A1234),
        ("span:id", ComparisonOp::Gt) => (ALL_T, A567),
        ("span:id", ComparisonOp::Gte) => (ALL_T, A4567),
        ("span:parentID", ComparisonOp::Lt) => (A1, ALL_T),
        ("span:parentID", ComparisonOp::Lte) => (A1, ALL_T),
        ("span:parentID", ComparisonOp::Gt) => (A2_7, NONE),
        ("span:parentID", ComparisonOp::Gte) => (A2_7, NONE),
        ("trace:id", ComparisonOp::Lt) => (NONE, ALL_T),
        ("trace:id", ComparisonOp::Lte) => (NONE, ALL_T),
        ("trace:id", ComparisonOp::Gt) => (ALL_T, NONE),
        ("trace:id", ComparisonOp::Gte) => (ALL_T, NONE),
        other => panic!("UC6 has no cell for {other:?}"),
    }
}

/// `UC6`: four ordered operators x three id columns, the cell count
/// COMPUTED from the two array lengths rather than listed — a dropped
/// operator is a length mismatch, not a missing row.
async fn uc6_cells(client: &ChClient, w: WindowSql) -> Vec<String> {
    let mut wrong = Vec::new();
    let mut seen = 0usize;
    for (field, column, literal) in UC6_COLUMNS {
        for op in ordered_ops() {
            seen += 1;
            let (want_raw, want_lower) = uc6_expected(field, op);
            let want_raw: Vec<String> = want_raw.iter().copied().map(idt).collect();
            let want_lower: Vec<String> = want_lower.iter().copied().map(idt).collect();
            let query = format!("{{ {field} {} \"{literal}\" }}", op_symbol(op));
            let got = answer(client, w, &query).await;
            if got != want_raw {
                wrong.push(format!(
                    "UC6 {query}\n   expected {want_raw:?}\n   answered {got:?}"
                ));
            }
            // The rendering section 6.1 forbids: the literal lowercased for
            // an ORDERED operator too. Issued as text, so a cell that would
            // answer the same either way is reported rather than counted.
            let forbidden = format!(
                "lower(hex({column})) {} '{}'",
                op_symbol(op),
                literal.to_lowercase()
            );
            let got_lower = raw_answer(client, w, &forbidden).await;
            if got_lower != want_lower {
                wrong.push(format!(
                    "UC6 {query}: the forbidden rendering `{forbidden}`\n   expected \
                     {want_lower:?}\n   answered {got_lower:?}"
                ));
            }
            if want_raw == want_lower {
                wrong.push(format!(
                    "UC6 {query}: this cell does not discriminate — the rule and the rendering it \
                     forbids both answer {want_raw:?}"
                ));
            }
        }
    }
    assert_eq!(
        seen,
        UC6_COLUMNS.len() * ordered_ops().len(),
        "UC6's cell count is computed from the column and operator lists"
    );
    wrong
}

/// The trace id fixture T's spans carry, so `UC2`/`UC5`'s literals cannot
/// drift from the seeded value without this failing.
#[test]
fn fixture_t_trace_id_is_the_literal_the_cases_write() {
    let hex: String = t_trace_id().iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(hex, T_TRACE_HEX);
    assert_eq!(idt("a1"), "0a1b2c3d4e5f60a1");
    assert_eq!(id61("0009"), "0000000000000009");
}

// =====================================================================
// Issue #589 part 1 — resource and instrumentation conditions, and the
// boolean operators
// =====================================================================
//
// Every statement below is compiled with `compile_span_predicate_in`, so a
// resource condition carries its subquery over the suite's own
// `resources` table, bounded by the request window's days.

/// The resource table every statement below reads, unqualified for
/// [`SPANS_TABLE`]'s reason.
const RESOURCES_TABLE: &str = "resources";

fn ctx_of(w: WindowSql) -> PredicateCtx<'static> {
    PredicateCtx {
        window: w,
        resources_table: RESOURCES_TABLE,
    }
}

/// The ids a statement answers with, or the driver's error text. The error
/// can arrive before the first row or in the middle of the stream, so both
/// are read.
async fn try_ids_of(client: &ChClient, sql: &str) -> Result<Vec<String>, String> {
    let mut stream = client
        .query_stream::<IdRow>(&sql.replace('?', "??"), &read_settings())
        .await
        .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.map_err(|e| e.to_string())?.id);
    }
    Ok(out)
}

/// Compiles `query` in the window's context and issues the membership
/// statement. A refusal is an `Err` naming it.
async fn answer_in(client: &ChClient, w: WindowSql, query: &str) -> Result<Vec<String>, String> {
    let predicate = compile_span_predicate_in(&body(query), &ctx_of(w))
        .map_err(|e| format!("<refused: {e}>"))?;
    try_ids_of(client, &span_membership_sql(SPANS_TABLE, w, &predicate)).await
}

/// A statement this suite expects to FAIL: `Ok(text)` is the driver's
/// error, `Err(ids)` is the answer it gave instead.
async fn answer_err(client: &ChClient, w: WindowSql, query: &str) -> Result<String, Vec<String>> {
    match answer_in(client, w, query).await {
        Ok(ids) => Err(ids),
        Err(text) => Ok(text),
    }
}

/// What one case must do: answer exactly these ids, or fail with an error
/// containing this message.
enum Want {
    Ids(&'static [&'static str]),
    Fails(&'static str),
}

struct CaseIn {
    name: &'static str,
    query: &'static str,
    want: Want,
}

async fn run_cases_in(
    client: &ChClient,
    w: WindowSql,
    cases: &[CaseIn],
    expand: fn(&str) -> String,
) -> Vec<String> {
    let mut wrong = Vec::new();
    for case in cases {
        let predicate = compile_span_predicate_in(&body(case.query), &ctx_of(w))
            .map(|p| p.sql().to_string())
            .unwrap_or_else(|e| format!("<refused: {e}>"));
        match &case.want {
            Want::Ids(ids) => {
                let want: Vec<String> = ids.iter().copied().map(expand).collect();
                let got = answer_in(client, w, case.query).await;
                if got.as_ref() != Ok(&want) {
                    wrong.push(format!(
                        "{}  {}\n   expected {want:?}\n   answered {got:?}\n   predicate: \
                         {predicate}",
                        case.name, case.query
                    ));
                }
            }
            Want::Fails(message) => match answer_err(client, w, case.query).await {
                Ok(text) if text.contains(message) && !text.starts_with("<refused") => {}
                other => wrong.push(format!(
                    "{}  {}\n   expected the statement to fail with {message:?}\n   got \
                     {other:?}\n   predicate: {predicate}",
                    case.name, case.query
                )),
            },
        }
    }
    wrong
}

// ---------------------------------------------------------------------
// 10.1 — the §6.1 fixture
// ---------------------------------------------------------------------

const ALL_BUT_PAYMENT_A: &[&str] = &["0001", "0002", "0003", "0004", "0007", "0008", "0009"];

const CASES_61_RESOURCES: &[CaseIn] = &[
    CaseIn {
        name: "F2",
        query: r#"{ resource.service.name = "payment" }"#,
        want: Want::Ids(&["0005", "0006"]),
    },
    CaseIn {
        name: "F18",
        query: r#"{ resource.k8s.pod.name = "payment-a" }"#,
        want: Want::Ids(&["0005", "0006"]),
    },
    CaseIn {
        name: "RS1",
        query: r#"{ resource.k8s.pod.name != "payment-a" }"#,
        want: Want::Ids(ALL_BUT_PAYMENT_A),
    },
    CaseIn {
        name: "RS2 front.*",
        query: r#"{ resource.k8s.pod.name =~ "front.*" }"#,
        want: Want::Ids(&["0001", "0002", "0007"]),
    },
    CaseIn {
        name: "RS2 a",
        query: r#"{ resource.k8s.pod.name =~ "a" }"#,
        want: Want::Ids(&[]),
    },
    CaseIn {
        name: "RS3 =~",
        query: r#"{ resource.service.name =~ "pay.*" }"#,
        want: Want::Ids(&["0005", "0006"]),
    },
    CaseIn {
        name: "RS3 !=",
        query: r#"{ resource.service.name != "payment" }"#,
        want: Want::Ids(ALL_BUT_PAYMENT_A),
    },
    CaseIn {
        name: "BO1",
        query: r#"{ span.rpc.system = "grpc" && status = error }"#,
        want: Want::Ids(&["0005"]),
    },
    CaseIn {
        name: "BO2",
        query: r#"{ name = "SELECT ledger" || kind = consumer }"#,
        want: Want::Ids(&["0006", "0008", "0009"]),
    },
    CaseIn {
        name: "BO3",
        query: r#"{ name = "SELECT ledger" || status = error && duration > 300ms }"#,
        want: Want::Ids(&["0001"]),
    },
    CaseIn {
        name: "BO4 ||",
        query: r#"{ true || !span.app.cache.hit }"#,
        want: Want::Ids(ALL_61),
    },
    CaseIn {
        name: "BO4 &&",
        query: r#"{ false && !span.app.cache.hit }"#,
        want: Want::Ids(&[]),
    },
    CaseIn {
        name: "BN1",
        query: r#"{ !span.app.cache.hit }"#,
        want: Want::Ids(&["0001"]),
    },
    CaseIn {
        name: "BN2",
        query: r#"{ !(span.app.cache.hit = true) }"#,
        want: Want::Ids(ALL_61),
    },
    CaseIn {
        name: "BN3",
        query: r#"{ !(span.http.response.status_code = 500 && span.app.cache.hit = false) }"#,
        want: Want::Ids(&[
            "0002", "0003", "0004", "0005", "0006", "0007", "0008", "0009",
        ]),
    },
    CaseIn {
        name: "BN4",
        query: r#"{ !(span.app.tags = "gold") }"#,
        want: Want::Ids(&[
            "0001", "0002", "0004", "0005", "0006", "0007", "0008", "0009",
        ]),
    },
    CaseIn {
        name: "BN5 = true",
        query: r#"{ !span.app.cache.hit = true }"#,
        want: Want::Ids(&["0001"]),
    },
    CaseIn {
        name: "BN5 != true",
        query: r#"{ !span.app.cache.hit != true }"#,
        want: Want::Ids(&[]),
    },
    CaseIn {
        name: "BN6",
        query: r#"{ !resource.service.name }"#,
        want: Want::Fails("expression (!resource.service.name) expected a boolean"),
    },
];

/// Section 10.1: resource conditions and the operators on the §6.1
/// fixture.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_predicate_compiler_answers_resources_and_operators_on_the_worked_fixture() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_read_it_t589_fixture61");
    let client = fresh_db(&db).await;
    let base_ns = (now_ns() / 1_000_000_000) * 1_000_000_000;
    for (i, req) in fixture_61_bodies(base_ns).into_iter().enumerate() {
        land(&client, &req, &format!("t589-61-{i}-{}", now_ns())).await;
    }
    let w = WindowSql::start_closed_end_open(base_ns, base_ns + WINDOW_NS);

    let seeded = count(&client, &format!("SELECT count() AS n FROM {SPANS_TABLE}")).await;
    assert_eq!(
        seeded, 9,
        "the §6.1 fixture seeds nine spans; nothing below can be read as a predicate result \
         until this holds"
    );

    let wrong = run_cases_in(&client, w, CASES_61_RESOURCES, id61).await;
    drop_db(&db).await;
    assert!(
        wrong.is_empty(),
        "{} of {} §6.1 cases answer something else:\n\n{}",
        wrong.len(),
        CASES_61_RESOURCES.len(),
        wrong.join("\n\n")
    );
}

// ---------------------------------------------------------------------
// 10.2 — fixture R
// ---------------------------------------------------------------------

/// `r1` to `r4`: span ids `00000000000000a1` to `…a4`.
fn idr(short: &str) -> String {
    format!("00000000000000a{}", short.trim_start_matches('r'))
}

/// One request: one resource, one scope, one span — the shape fixtures R
/// and S both use.
fn one_span_request(
    resource_attrs: Vec<KeyValue>,
    scope: InstrumentationScope,
    span: Span,
) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: resource_attrs,
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(scope),
                spans: vec![span],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Fixture R, section 9.1: four requests, one span each.
fn fixture_r_bodies(base_ns: i64) -> Vec<ExportTraceServiceRequest> {
    let r_scope = |attributes: Vec<KeyValue>| InstrumentationScope {
        name: "io.pulsus.r".to_string(),
        version: "1.0".to_string(),
        attributes,
        dropped_attributes_count: 0,
    };
    let r_span = |n: u8, attributes: Vec<KeyValue>| {
        span_of(
            vec![0xee; 16],
            vec![0, 0, 0, 0, 0, 0, 0, 0xa0 + n],
            Vec::new(),
            "span",
            1,
            base_ns + i64::from(0xa0 + n) * 1_000_000,
            1_000_000,
            attributes,
            0,
            Vec::new(),
            Vec::new(),
        )
    };
    vec![
        one_span_request(
            vec![
                kv("service.name", str_value("svc-r")),
                kv("tier", int_value(3)),
                kv("canary", bool_value(true)),
                kv("region", str_value("eu")),
            ],
            r_scope(vec![
                kv("otel.scope.build", str_value("release")),
                kv("lib.flag", bool_value(true)),
            ]),
            r_span(1, vec![kv("app.flag", bool_value(false))]),
        ),
        one_span_request(
            vec![
                kv("service.name", int_value(12345)),
                kv("tier", double_value(3.5)),
                kv("canary", bool_value(false)),
            ],
            r_scope(vec![kv("otel.scope.build", str_value("debug"))]),
            r_span(2, vec![kv("app.flag", bool_value(true))]),
        ),
        one_span_request(
            vec![kv("service.name", str_value(""))],
            r_scope(Vec::new()),
            r_span(3, vec![kv("otel.scope.build", str_value("release"))]),
        ),
        one_span_request(
            vec![kv("region", str_value("us"))],
            r_scope(vec![kv("lib.n", int_value(7))]),
            r_span(4, vec![kv("app.flag", str_value("false"))]),
        ),
    ]
}

const CASES_R: &[CaseIn] = &[
    CaseIn {
        name: "RR1 = 3",
        query: r#"{ resource.tier = 3 }"#,
        want: Want::Ids(&["r1"]),
    },
    CaseIn {
        name: "RR1 > 3",
        query: r#"{ resource.tier > 3 }"#,
        want: Want::Ids(&["r2"]),
    },
    CaseIn {
        name: "RR2 = false",
        query: r#"{ resource.canary = false }"#,
        want: Want::Ids(&["r2"]),
    },
    CaseIn {
        name: "RR2 truthiness",
        query: r#"{ resource.canary }"#,
        want: Want::Ids(&["r1"]),
    },
    CaseIn {
        name: "RR3 != nil",
        query: r#"{ resource.canary != nil }"#,
        want: Want::Ids(&["r1", "r2"]),
    },
    CaseIn {
        name: "RR3 = nil",
        query: r#"{ resource.region = nil }"#,
        want: Want::Ids(&["r2", "r3"]),
    },
    CaseIn {
        name: "RR4",
        query: r#"{ resource.tier != 3 }"#,
        want: Want::Ids(&["r2", "r3", "r4"]),
    },
    CaseIn {
        name: "RR5",
        query: r#"{ resource.service.name = 12345 }"#,
        want: Want::Ids(&[]),
    },
    CaseIn {
        name: "RR7",
        query: r#"{ resource.service.name != nil }"#,
        want: Want::Ids(&["r1", "r2", "r3"]),
    },
    CaseIn {
        name: "RR8",
        query: r#"{ resource.service.name = nil }"#,
        want: Want::Ids(&["r4"]),
    },
    CaseIn {
        name: "IS1",
        query: r#"{ instrumentation.otel.scope.build = "release" }"#,
        want: Want::Ids(&["r1"]),
    },
    CaseIn {
        name: "IS2",
        query: r#"{ instrumentation.otel.scope.build != "release" }"#,
        want: Want::Ids(&["r2", "r3", "r4"]),
    },
    CaseIn {
        name: "IS3 truthiness",
        query: r#"{ instrumentation.lib.flag }"#,
        want: Want::Ids(&["r1"]),
    },
    CaseIn {
        name: "IS3 > 5",
        query: r#"{ instrumentation.lib.n > 5 }"#,
        want: Want::Ids(&["r4"]),
    },
    CaseIn {
        name: "IS3 != nil",
        query: r#"{ instrumentation.lib.n != nil }"#,
        want: Want::Ids(&["r4"]),
    },
    CaseIn {
        name: "BO5 ||",
        query: r#"{ true || !span.app.flag }"#,
        want: Want::Fails("expression (!span.app.flag) expected a boolean"),
    },
    CaseIn {
        name: "BO5 &&",
        query: r#"{ false && !span.app.flag }"#,
        want: Want::Fails("expression (!span.app.flag) expected a boolean"),
    },
    CaseIn {
        name: "BN7",
        query: r#"{ !resource.canary }"#,
        want: Want::Ids(&["r2"]),
    },
    CaseIn {
        name: "BN8",
        query: r#"{ !instrumentation.lib.flag }"#,
        want: Want::Ids(&[]),
    },
    CaseIn {
        name: "BN9 bare",
        query: r#"{ !span.app.flag }"#,
        want: Want::Fails("expression (!span.app.flag) expected a boolean"),
    },
    CaseIn {
        name: "BN9 = 1",
        query: r#"{ !span.app.flag = 1 }"#,
        want: Want::Fails("expression (!span.app.flag) expected a boolean"),
    },
    CaseIn {
        name: "BN10",
        query: r#"{ !resource.tier }"#,
        want: Want::Fails("expression (!resource.tier) expected a boolean"),
    },
];

/// `RR-MISS`'s three queries, run after `r4`'s resource row is deleted.
const CASES_R_MISS: &[CaseIn] = &[
    CaseIn {
        name: "RR-MISS !=",
        query: r#"{ resource.region != "eu" }"#,
        want: Want::Ids(&["r2", "r3", "r4"]),
    },
    CaseIn {
        name: "RR-MISS =",
        query: r#"{ resource.region = "us" }"#,
        want: Want::Ids(&[]),
    },
    CaseIn {
        name: "RR-MISS = nil",
        query: r#"{ resource.region = nil }"#,
        want: Want::Ids(&["r2", "r3", "r4"]),
    },
];

/// Deletes the resource row of the span `span_hex`, synchronously.
async fn delete_resource_row_of(client: &ChClient, span_hex: &str) {
    client
        .execute(
            &format!(
                "ALTER TABLE {RESOURCES_TABLE} DELETE WHERE resource_id IN \
                 (SELECT resource_id FROM {SPANS_TABLE} WHERE span_id = unhex('{span_hex}')) \
                 SETTINGS mutations_sync = 2"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("delete the resource row");
}

/// Section 10.2: fixture R. `RR-MISS` runs last: it deletes a row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_predicate_compiler_answers_resource_and_scope_values() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_read_it_t589_fixturer");
    let client = fresh_db(&db).await;
    let base_ns = (now_ns() / 1_000_000_000) * 1_000_000_000;
    for (i, req) in fixture_r_bodies(base_ns).into_iter().enumerate() {
        land(&client, &req, &format!("t589-r-{i}-{}", now_ns())).await;
    }
    let w = WindowSql::start_closed_end_open(base_ns, base_ns + WINDOW_NS);

    let seeded = count(&client, &format!("SELECT count() AS n FROM {SPANS_TABLE}")).await;
    assert_eq!(
        seeded, 4,
        "fixture R seeds four spans; nothing below can be read as a predicate result until \
         this holds"
    );

    let mut wrong = run_cases_in(&client, w, CASES_R, idr).await;
    delete_resource_row_of(&client, &idr("r4")).await;
    wrong.extend(run_cases_in(&client, w, CASES_R_MISS, idr).await);
    drop_db(&db).await;
    assert!(
        wrong.is_empty(),
        "{} of {} fixture-R cases answer something else:\n\n{}",
        wrong.len(),
        CASES_R.len() + CASES_R_MISS.len(),
        wrong.join("\n\n")
    );
}

// ---------------------------------------------------------------------
// 10.4 — fixture S, the service-name cross product
// ---------------------------------------------------------------------

/// `s1` to `s10`: span ids `00000000000000b1` to `…b9`, then `…ba`.
fn ids_n(n: u8) -> String {
    format!("00000000000000{:02x}", 0xb0 + n)
}

fn bytes_value(bytes: &[u8]) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::BytesValue(bytes.to_vec())),
    }
}

fn kvlist_value(pairs: Vec<KeyValue>) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::KvlistValue(KeyValueList {
            values: pairs,
        })),
    }
}

/// Section 3.4's stored forms of `service.name`, `s1`–`s9` in order, each
/// with whether it is a `StringValue`. `s10` carries no `service.name`.
fn fixture_s_forms() -> Vec<(AnyValue, bool)> {
    vec![
        (str_value("svc"), true),
        (str_value(""), true),
        (bool_value(true), false),
        (int_value(12345), false),
        (double_value(1.5), false),
        (str_array_value(&["svc"]), false),
        (kvlist_value(vec![kv("child", str_value("x"))]), false),
        (bytes_value(&[0xde, 0xad, 0xbe]), false),
        (kvlist_value(Vec::new()), false),
    ]
}

/// Fixture S, section 9.2: ten requests, one span each.
fn fixture_s_bodies(base_ns: i64) -> Vec<ExportTraceServiceRequest> {
    let s_span = |n: u8| {
        span_of(
            vec![0xdd; 16],
            vec![0, 0, 0, 0, 0, 0, 0, 0xb0 + n],
            Vec::new(),
            "span",
            1,
            base_ns + i64::from(0xb0 + n) * 1_000_000,
            1_000_000,
            Vec::new(),
            0,
            Vec::new(),
            Vec::new(),
        )
    };
    let mut out: Vec<ExportTraceServiceRequest> = fixture_s_forms()
        .into_iter()
        .zip(1u8..)
        .map(|((value, _), n)| {
            one_span_request(vec![kv("service.name", value)], scope(), s_span(n))
        })
        .collect();
    out.push(one_span_request(
        vec![kv("region", str_value("x"))],
        scope(),
        s_span(10),
    ));
    out
}

/// An RE2 pattern matching exactly `text`.
fn re2_literal(text: &str) -> String {
    let mut out = String::new();
    for c in text.chars() {
        if "\\.+*?()|[]{}^$".contains(c) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ServiceRow {
    id: String,
    service: String,
}

/// Section 10.4: every stored form of `service.name` × `= != =~ !~`, the
/// literal being the form's own `service` text, with the expected answer
/// computed by the loop. Then `SX-MISS`, which deletes a row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_service_name_is_compared_only_as_a_string() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_read_it_t589_fixtures");
    let client = fresh_db(&db).await;
    let base_ns = (now_ns() / 1_000_000_000) * 1_000_000_000;
    for (i, req) in fixture_s_bodies(base_ns).into_iter().enumerate() {
        land(&client, &req, &format!("t589-s-{i}-{}", now_ns())).await;
    }
    let w = WindowSql::start_closed_end_open(base_ns, base_ns + WINDOW_NS);

    let seeded = count(&client, &format!("SELECT count() AS n FROM {SPANS_TABLE}")).await;
    assert_eq!(
        seeded, 10,
        "fixture S seeds ten spans; nothing below can be read as a predicate result until \
         this holds"
    );

    // Each form's own `service` text, as the writer stored it.
    let mut texts: Vec<ServiceRow> = Vec::new();
    let sql = format!(
        "SELECT lower(hex(span_id)) AS id, toString(service) AS service FROM {SPANS_TABLE} \
         ORDER BY id"
    );
    let mut stream = client
        .query_stream::<ServiceRow>(&sql, &read_settings())
        .await
        .expect("the service read");
    while let Some(row) = stream.next().await {
        texts.push(row.expect("decode"));
    }
    let all: Vec<String> = (1..=10).map(ids_n).collect();
    assert_eq!(
        texts.iter().map(|r| r.id.clone()).collect::<Vec<_>>(),
        all,
        "one row per form"
    );

    let svc = Field::Attribute {
        scope: AttrScope::Resource,
        key: "service.name".to_string(),
    };
    let ctx = ctx_of(w);
    let ops = [
        ComparisonOp::Eq,
        ComparisonOp::Neq,
        ComparisonOp::Re,
        ComparisonOp::Nre,
    ];
    let mut wrong = Vec::new();
    let mut cells = 0usize;
    for ((_, is_string), (n, row)) in fixture_s_forms().into_iter().zip((1u8..).zip(&texts)) {
        let this = ids_n(n);
        assert_eq!(row.id, this);
        for op in ops {
            cells += 1;
            let lit = match op {
                ComparisonOp::Re | ComparisonOp::Nre => re2_literal(&row.service),
                _ => row.service.clone(),
            };
            let positive = matches!(op, ComparisonOp::Eq | ComparisonOp::Re);
            let want: Vec<String> = match (is_string, positive) {
                (true, true) => vec![this.clone()],
                (true, false) => all.iter().filter(|i| **i != this).cloned().collect(),
                (false, true) => Vec::new(),
                (false, false) => all.clone(),
            };
            let got = match compile_span_leaf_in(&svc, op, &Value::String(lit.clone()), &ctx) {
                Ok(p) => try_ids_of(&client, &span_membership_sql(SPANS_TABLE, w, &p)).await,
                Err(e) => Err(format!("<refused: {e}>")),
            };
            if got.as_ref() != Ok(&want) {
                wrong.push(format!(
                    "SX s{n} resource.service.name {} {lit:?}\n   expected {want:?}\n   \
                     answered {got:?}",
                    op_symbol(op)
                ));
            }
        }
    }
    assert_eq!(cells, 9 * 4, "SX runs every form against every operator");

    // `SX-MISS`: with `s1`'s resource row gone, no form reads it.
    delete_resource_row_of(&client, &ids_n(1)).await;
    let s1 = ids_n(1);
    let s10 = ids_n(10);
    let but = |x: &String| -> Vec<String> { all.iter().filter(|i| *i != x).cloned().collect() };
    for (query, want) in [
        (r#"{ resource.service.name = "svc" }"#, vec![s1.clone()]),
        (r#"{ resource.service.name != "svc" }"#, but(&s1)),
        (r#"{ resource.service.name =~ "svc" }"#, vec![s1.clone()]),
        (r#"{ resource.service.name !~ "svc" }"#, but(&s1)),
        (r#"{ resource.service.name != nil }"#, but(&s10)),
    ] {
        let got = answer_in(&client, w, query).await;
        if got.as_ref() != Ok(&want) {
            wrong.push(format!(
                "SX-MISS {query}\n   expected {want:?}\n   answered {got:?}"
            ));
        }
    }
    drop_db(&db).await;
    assert!(
        wrong.is_empty(),
        "{} of {} fixture-S checks answer something else:\n\n{}",
        wrong.len(),
        cells + 5,
        wrong.join("\n\n")
    );
}

// =====================================================================
// Issue #589 part 2 — event and link conditions, the four event and link
// intrinsics, and the unscoped `.k` chain
// =====================================================================

/// One span with its own status message — `span_of` writes `"boom"` for
/// every non-zero status, and the catalogue fixture carries `''` on all
/// but one.
#[allow(clippy::too_many_arguments)]
fn span_with_message(
    trace_id: Vec<u8>,
    span_id: Vec<u8>,
    parent_span_id: Vec<u8>,
    name: &str,
    kind: i32,
    start_ns: i64,
    duration_ns: i64,
    attributes: Vec<KeyValue>,
    status: Option<(i32, &str)>,
    events: Vec<span::Event>,
    links: Vec<span::Link>,
) -> Span {
    Span {
        status: status.map(|(code, message)| Status {
            message: message.to_string(),
            code,
        }),
        ..span_of(
            trace_id,
            span_id,
            parent_span_id,
            name,
            kind,
            start_ns,
            duration_ns,
            attributes,
            0,
            events,
            links,
        )
    }
}

/// One request: one resource, one scope, the spans given.
fn group_request(
    resource_attrs: Vec<KeyValue>,
    scope: InstrumentationScope,
    spans: Vec<Span>,
) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: resource_attrs,
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(scope),
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn scope_named(name: &str, version: &str, attributes: Vec<KeyValue>) -> InstrumentationScope {
    InstrumentationScope {
        name: name.to_string(),
        version: version.to_string(),
        attributes,
        dropped_attributes_count: 0,
    }
}

fn event_of(time_ns: i64, name: &str, attributes: Vec<KeyValue>) -> span::Event {
    span::Event {
        time_unix_nano: time_ns as u64,
        name: name.to_string(),
        attributes,
        dropped_attributes_count: 0,
    }
}

fn link_of(trace_id: Vec<u8>, span_id: Vec<u8>, attributes: Vec<KeyValue>) -> span::Link {
    span::Link {
        trace_id,
        span_id,
        trace_state: String::new(),
        attributes,
        dropped_attributes_count: 0,
        flags: 0,
    }
}

fn hex_bytes(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex"))
        .collect()
}

// ---------------------------------------------------------------------
// 10.1 — the §6.1 fixture
// ---------------------------------------------------------------------

const CASES_61_PART2: &[CaseIn] = &[
    CaseIn {
        name: "F10",
        query: r#"{ event.exception.type = "java.lang.IllegalStateException" }"#,
        want: Want::Ids(&["0005"]),
    },
    CaseIn {
        name: "F11",
        query: r#"{ link:traceID = "11111111111111111111111111111111" }"#,
        want: Want::Ids(&["0008"]),
    },
    CaseIn {
        name: "F17",
        query: r#"{ .app.user.id = "u-1" }"#,
        want: Want::Ids(&["0001"]),
    },
    CaseIn {
        name: "EN61",
        query: r#"{ event:name = "exception" }"#,
        want: Want::Ids(&["0005"]),
    },
    CaseIn {
        name: "LS61",
        query: r#"{ link:spanID = "0000000000000005" }"#,
        want: Want::Ids(&["0008"]),
    },
];

/// Section 10.1: events, links and the chain on the §6.1 fixture.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_predicate_compiler_answers_events_links_and_the_chain_on_the_worked_fixture() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_read_it_t589p2_fixture61");
    let client = fresh_db(&db).await;
    let base_ns = (now_ns() / 1_000_000_000) * 1_000_000_000;
    for (i, req) in fixture_61_bodies(base_ns).into_iter().enumerate() {
        land(&client, &req, &format!("t589p2-61-{i}-{}", now_ns())).await;
    }
    let w = WindowSql::start_closed_end_open(base_ns, base_ns + WINDOW_NS);

    let seeded = count(&client, &format!("SELECT count() AS n FROM {SPANS_TABLE}")).await;
    assert_eq!(
        seeded, 9,
        "the §6.1 fixture seeds nine spans; nothing below can be read as a predicate result \
         until this holds"
    );

    let wrong = run_cases_in(&client, w, CASES_61_PART2, id61).await;
    drop_db(&db).await;
    assert!(
        wrong.is_empty(),
        "{} of {} §6.1 cases answer something else:\n\n{}",
        wrong.len(),
        CASES_61_PART2.len(),
        wrong.join("\n\n")
    );
}

// ---------------------------------------------------------------------
// 10.2 — fixture C, the catalogue fixture
// ---------------------------------------------------------------------

/// The catalogue fixture's window: `[base, base + 60 s)`.
const CATALOGUE_WINDOW_NS: i64 = 60_000_000_000;

/// The generator's `sid(n)`.
fn sid(n: u64) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}

/// Fixture C, section 9.1: the generator's seven requests, in its
/// first-appearance order, every span field the generator's.
fn fixture_catalogue_bodies(base_ns: i64) -> Vec<ExportTraceServiceRequest> {
    const NS: i64 = 1_000_000_000;
    const MS: i64 = 1_000_000;
    let t = |b: u8| vec![b; 16];
    let res = |service: &str, env: &str, pod: &str| {
        vec![
            kv("service.name", str_value(service)),
            kv("deployment.environment", str_value(env)),
            kv("k8s.pod.name", str_value(pod)),
        ]
    };
    let http = || scope_named("io.opentelemetry.http", "2.9.0", Vec::new());
    // The generator's `span(...)`, with no event, no link, and the status
    // given.
    #[allow(clippy::too_many_arguments)]
    fn s(
        base_ns: i64,
        trace: Vec<u8>,
        n: u64,
        parent: u64,
        name: &str,
        kind: i32,
        t_off: i64,
        dur: i64,
        attrs: Vec<KeyValue>,
        status: Option<(i32, &str)>,
    ) -> Span {
        span_with_message(
            trace,
            sid(n),
            if parent == 0 { Vec::new() } else { sid(parent) },
            name,
            kind,
            base_ns + t_off,
            dur,
            attrs,
            status,
            Vec::new(),
            Vec::new(),
        )
    }
    let b = base_ns;
    let i = |k: &str, v: i64| kv(k, int_value(v));
    let st = |k: &str, v: &str| kv(k, str_value(v));

    let req0 = group_request(
        res("checkout", "prod", "checkout-a"),
        http(),
        vec![
            s(
                b,
                t(0x11),
                1,
                0,
                "GET /api/orders",
                2,
                0,
                3 * NS,
                vec![
                    i("a", 1),
                    i("b", 2),
                    st("foo", "f"),
                    st("env", "prod"),
                    kv("success", bool_value(true)),
                    i("retries", 1),
                    kv("retried", bool_value(true)),
                    i("http.status_code", 500),
                    st("http.method", "POST"),
                    st("http.url", "/api/orders/17"),
                    st("http.route", "/api/v2/list"),
                    i("bytes", 1500),
                    i("client.timeout", 5 * NS),
                    i("attr with spaces", 1),
                    st("foo bar", "x"),
                ],
                Some((2, "boom")),
            ),
            s(
                b,
                t(0x11),
                2,
                1,
                "b",
                3,
                100 * MS,
                2 * NS,
                vec![i("b", 2), i("c", 3), i("bytes", 900), i("retries", 3)],
                None,
            ),
            s(
                b,
                t(0x11),
                3,
                2,
                "c",
                1,
                200 * MS,
                NS,
                vec![i("c", 3), i("d", 4), kv("a", double_value(1.0))],
                None,
            ),
        ],
    );

    let link_trace = hex_bytes("000102030405060708090a0b0c0d0e0f");
    let link_span = hex_bytes("0a1b2c3d4e5f6071");
    let mut span4 = s(
        b,
        t(0x22),
        4,
        0,
        "GET /",
        2,
        10 * NS,
        3 * NS,
        vec![
            i("a", 1),
            i("d", 4),
            i("http.status_code", 200),
            st("env", "dev"),
        ],
        None,
    );
    span4.events = vec![event_of(
        b + 10 * NS + 2 * MS,
        "exception",
        vec![
            st("exception.type", "IOError"),
            st("exception.message", "io"),
        ],
    )];
    span4.links = vec![link_of(
        link_trace,
        link_span,
        vec![st("relation", "child_of")],
    )];
    let req1 = group_request(
        res("gw", "dev", "gw-a"),
        scope_named(
            "otel",
            "1.0",
            vec![st("name", "otel"), st("otel.scope.build", "release")],
        ),
        vec![span4],
    );
    let req2 = group_request(
        res("gw", "dev", "gw-a"),
        scope_named("otel", "1.0", Vec::new()),
        vec![
            s(
                b,
                t(0x22),
                5,
                4,
                "GET /api/orders",
                3,
                10 * NS + 5 * MS,
                NS,
                vec![
                    i("a", 1),
                    i("b", 2),
                    st("http.method", "GET"),
                    st("http.url", "/api/v3/list"),
                ],
                None,
            ),
            s(
                b,
                t(0x22),
                6,
                5,
                "child",
                5,
                10 * NS + 10 * MS,
                500 * MS,
                vec![i("c", 3), kv("success", bool_value(false))],
                None,
            ),
        ],
    );

    let t6 = t(0x66);
    let req3 = group_request(
        res("plain", "staging", "plain-a"),
        http(),
        vec![
            s(
                b,
                t(0x33),
                7,
                0,
                "bare",
                1,
                20 * NS,
                100 * MS,
                Vec::new(),
                None,
            ),
            s(
                b,
                t(0x33),
                8,
                7,
                "bare child",
                4,
                20 * NS + 10 * MS,
                50 * MS,
                vec![kv("a", double_value(2.5)), i("retries", 5)],
                Some((1, "")),
            ),
            s(
                b,
                t6.clone(),
                40,
                0,
                "checkout",
                0,
                50 * NS,
                1,
                vec![i("a", 3)],
                None,
            ),
            s(
                b,
                t6.clone(),
                41,
                40,
                "Az",
                1,
                50 * NS + MS,
                NS,
                vec![i("a", 8)],
                Some((2, "")),
            ),
            s(
                b,
                t6.clone(),
                42,
                40,
                "A\n",
                1,
                50 * NS + 2 * MS,
                NS,
                vec![i("a", 2), i("b", 2)],
                Some((2, "")),
            ),
            s(
                b,
                t6.clone(),
                43,
                40,
                "\u{e9}\u{65e5}\u{1F600}",
                1,
                50 * NS + 3 * MS,
                NS,
                vec![i("a", 6)],
                Some((2, "")),
            ),
            s(
                b,
                t6.clone(),
                44,
                40,
                "col1\tcol2\n\"quoted\" \\ bell\x07 vt\x0b bs\x08 ff\x0c cr\r",
                1,
                50 * NS + 4 * MS,
                NS,
                vec![i("a", -1)],
                Some((2, "")),
            ),
            s(
                b,
                t6.clone(),
                45,
                40,
                "b",
                1,
                50 * NS + 5 * MS,
                1_073_741_824,
                vec![i("a", 1), kv("foo", bool_value(true))],
                None,
            ),
            s(
                b,
                t6.clone(),
                49,
                45,
                "b-child-1",
                1,
                50 * NS + 8 * MS,
                NS,
                Vec::new(),
                None,
            ),
            s(
                b,
                t6.clone(),
                50,
                45,
                "b-child-2",
                1,
                50 * NS + 9 * MS,
                NS,
                Vec::new(),
                None,
            ),
            s(
                b,
                t6.clone(),
                46,
                40,
                "min",
                1,
                50 * NS + 6 * MS,
                NS,
                vec![i("a", i64::MIN)],
                None,
            ),
        ],
    );

    let t4 = t(0x44);
    let t5 = t(0x55);
    let edge = |n: u64, parent: u64, name: &str, kind: i32, t_off: i64, dur: i64, attrs| {
        s(
            b,
            if n == 30 || n == 31 {
                t5.clone()
            } else {
                t4.clone()
            },
            n,
            parent,
            name,
            kind,
            t_off,
            dur,
            attrs,
            None,
        )
    };
    let req4 = group_request(
        res("edge", "prod", "edge-a"),
        http(),
        vec![
            edge(20, 0, "P", 2, 30 * NS, 5 * NS, Vec::new()),
            edge(21, 20, "A2", 1, 30 * NS + MS, NS, vec![i("a", 1)]),
            edge(22, 21, "B1", 1, 30 * NS + 2 * MS, NS, vec![i("b", 2)]),
            edge(23, 22, "A1", 1, 30 * NS + 3 * MS, NS, vec![i("a", 1)]),
            edge(24, 20, "P2", 1, 30 * NS + 4 * MS, NS, Vec::new()),
            edge(25, 24, "A3", 1, 30 * NS + 5 * MS, NS, vec![i("a", 1)]),
            edge(26, 24, "B2", 1, 30 * NS + 6 * MS, NS, vec![i("b", 2)]),
            edge(27, 999, "orphan", 1, 30 * NS + 7 * MS, NS, vec![i("b", 2)]),
            edge(36, 24, "B3", 1, 30 * NS + 8 * MS, NS, vec![i("b", 2)]),
            edge(32, 20, "A4", 1, 30 * NS + 9 * MS, NS, vec![i("a", 1)]),
            edge(37, 32, "N1", 1, 30 * NS + 10 * MS, NS, Vec::new()),
            edge(38, 37, "N2", 1, 30 * NS + 11 * MS, NS, Vec::new()),
            edge(33, 38, "B4", 1, 30 * NS + 12 * MS, NS, vec![i("b", 2)]),
            edge(30, 31, "cyc-a", 1, 40 * NS, NS, vec![i("a", 1)]),
            edge(31, 30, "cyc-b", 1, 40 * NS + MS, NS, vec![i("b", 2)]),
        ],
    );

    let mut shadow = res("shadow", "prod", "shadow-a");
    shadow.push(i("http.status_code", 200));
    let req5 = group_request(
        shadow,
        http(),
        vec![s(
            b,
            t6.clone(),
            47,
            40,
            "shadow",
            1,
            50 * NS + 7 * MS,
            NS,
            vec![i("http.status_code", 500)],
            None,
        )],
    );
    let req6 = group_request(
        res("regex", "development", "regex-a"),
        http(),
        vec![s(
            b,
            t6,
            48,
            40,
            "xGETy",
            1,
            50 * NS + 10 * MS,
            NS,
            vec![st("http.url", "x/api/z"), st("http.route", "z/api/v2/x")],
            None,
        )],
    );
    vec![req0, req1, req2, req3, req4, req5, req6]
}

/// The generator's 34 span ids, by their last four hex digits.
const ALL_C: &[&str] = &[
    "0001", "0002", "0003", "0004", "0005", "0006", "0007", "0008", "0014", "0015", "0016", "0017",
    "0018", "0019", "001a", "001b", "001e", "001f", "0020", "0021", "0024", "0025", "0026", "0028",
    "0029", "002a", "002b", "002c", "002d", "002e", "002f", "0030", "0031", "0032",
];

const ALL_C_BUT_0001: &[&str] = &[
    "0002", "0003", "0004", "0005", "0006", "0007", "0008", "0014", "0015", "0016", "0017", "0018",
    "0019", "001a", "001b", "001e", "001f", "0020", "0021", "0024", "0025", "0026", "0028", "0029",
    "002a", "002b", "002c", "002d", "002e", "002f", "0030", "0031", "0032",
];

const ALL_C_BUT_0004: &[&str] = &[
    "0001", "0002", "0003", "0005", "0006", "0007", "0008", "0014", "0015", "0016", "0017", "0018",
    "0019", "001a", "001b", "001e", "001f", "0020", "0021", "0024", "0025", "0026", "0028", "0029",
    "002a", "002b", "002c", "002d", "002e", "002f", "0030", "0031", "0032",
];

const CASES_C: &[CaseIn] = &[
    CaseIn {
        name: "CAT18",
        query: r#"{ .http.status_code = 200 }"#,
        want: Want::Ids(&["0004"]),
    },
    CaseIn {
        name: "UC1",
        query: r#"{ .http.status_code = 500 }"#,
        want: Want::Ids(&["0001", "002f"]),
    },
    CaseIn {
        name: "CAT11",
        query: r#"{ .env != "prod" }"#,
        want: Want::Ids(ALL_C_BUT_0001),
    },
    CaseIn {
        name: "CAT19",
        query: r#"{ .foo }"#,
        want: Want::Ids(&["002d"]),
    },
    CaseIn {
        name: "CAT40",
        query: r#"{ .a = nil }"#,
        want: Want::Ids(&[
            "0002", "0006", "0007", "0014", "0016", "0018", "001a", "001b", "001f", "0021", "0024",
            "0025", "0026", "002f", "0030", "0031", "0032",
        ]),
    },
    CaseIn {
        name: "CAT41",
        query: r#"{ .a != nil }"#,
        want: Want::Ids(&[
            "0001", "0003", "0004", "0005", "0008", "0015", "0017", "0019", "001e", "0020", "0028",
            "0029", "002a", "002b", "002c", "002d", "002e",
        ]),
    },
    CaseIn {
        name: "CAT48",
        query: r#"{ .a = 1 }"#,
        want: Want::Ids(&[
            "0001", "0003", "0004", "0005", "0015", "0017", "0019", "001e", "0020", "002d",
        ]),
    },
    CaseIn {
        name: "CAT101",
        query: r#"{ !(.a = 1) }"#,
        want: Want::Ids(&[
            "0002", "0006", "0007", "0008", "0014", "0016", "0018", "001a", "001b", "001f", "0021",
            "0024", "0025", "0026", "0028", "0029", "002a", "002b", "002c", "002e", "002f", "0030",
            "0031", "0032",
        ]),
    },
    CaseIn {
        name: "UC-SVC",
        query: r#"{ .service.name = "gw" }"#,
        want: Want::Ids(&["0004", "0005", "0006"]),
    },
    CaseIn {
        name: "UC-NIL service.name",
        query: r#"{ .service.name != nil }"#,
        want: Want::Ids(ALL_C),
    },
    CaseIn {
        name: "UC-NIL deployment.environment",
        query: r#"{ .deployment.environment != nil }"#,
        want: Want::Ids(ALL_C),
    },
    CaseIn {
        name: "UC-NIL exception.type",
        query: r#"{ .exception.type = nil }"#,
        want: Want::Ids(ALL_C_BUT_0004),
    },
    CaseIn {
        name: "UC-RES",
        query: r#"{ .deployment.environment = "dev" }"#,
        want: Want::Ids(&["0004", "0005", "0006"]),
    },
    CaseIn {
        name: "UC-EV",
        query: r#"{ .exception.type = "IOError" }"#,
        want: Want::Ids(&["0004"]),
    },
    CaseIn {
        name: "UC-LK",
        query: r#"{ .relation = "child_of" }"#,
        want: Want::Ids(&["0004"]),
    },
    CaseIn {
        name: "UC-IN",
        query: r#"{ .otel.scope.build = "release" }"#,
        want: Want::Ids(&["0004"]),
    },
    CaseIn {
        name: "CAT51",
        query: r#"{ event:name = "exception" }"#,
        want: Want::Ids(&["0004"]),
    },
    CaseIn {
        name: "TSS > 1ms (CAT52)",
        query: r#"{ event:timeSinceStart > 1ms }"#,
        want: Want::Ids(&["0004"]),
    },
    CaseIn {
        name: "TSS < 3ms",
        query: r#"{ event:timeSinceStart < 3ms }"#,
        want: Want::Ids(&["0004"]),
    },
    CaseIn {
        name: "CAT57",
        query: r#"{ link:spanID = "0a1b2c3d4e5f6071" }"#,
        want: Want::Ids(&["0004"]),
    },
    CaseIn {
        name: "CAT58",
        query: r#"{ link:traceID = "000102030405060708090a0b0c0d0e0f" }"#,
        want: Want::Ids(&["0004"]),
    },
    CaseIn {
        name: "LK-UP",
        query: r#"{ link:traceID = "000102030405060708090A0B0C0D0E0F" }"#,
        want: Want::Ids(&["0004"]),
    },
    CaseIn {
        name: "CAT115",
        query: r#"{ event.exception.type = "IOError" }"#,
        want: Want::Ids(&["0004"]),
    },
    CaseIn {
        name: "CAT117",
        query: r#"{ link.relation = "child_of" }"#,
        want: Want::Ids(&["0004"]),
    },
];

/// Section 10.2: the catalogue fixture.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_predicate_compiler_answers_events_links_and_the_chain_on_the_catalogue_fixture() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_read_it_t589p2_catalogue");
    let client = fresh_db(&db).await;
    let base_ns = (now_ns() / 1_000_000_000) * 1_000_000_000;
    for (i, req) in fixture_catalogue_bodies(base_ns).into_iter().enumerate() {
        land(&client, &req, &format!("t589p2-c-{i}-{}", now_ns())).await;
    }
    let w = WindowSql::start_closed_end_open(base_ns, base_ns + CATALOGUE_WINDOW_NS);

    let seeded = count(&client, &format!("SELECT count() AS n FROM {SPANS_TABLE}")).await;
    assert_eq!(
        seeded, 34,
        "the catalogue fixture seeds 34 spans; nothing below can be read as a predicate result \
         until this holds"
    );
    let ids = ids_of(
        &client,
        &format!("SELECT lower(hex(span_id)) AS id FROM {SPANS_TABLE} ORDER BY id"),
    )
    .await;
    let want: Vec<String> = ALL_C.iter().copied().map(id61).collect();
    assert_eq!(ids, want, "the 34 span ids are the generator's");

    let wrong = run_cases_in(&client, w, CASES_C, id61).await;
    drop_db(&db).await;
    assert!(
        wrong.is_empty(),
        "{} of {} catalogue-fixture cases answer something else:\n\n{}",
        wrong.len(),
        CASES_C.len(),
        wrong.join("\n\n")
    );
}

// ---------------------------------------------------------------------
// 10.3 — fixture V, any element and the chain order
// ---------------------------------------------------------------------

/// `v1` to `v8`: span ids `00000000000000c1` to `…c8`.
fn idv(short: &str) -> String {
    format!("00000000000000c{}", short.trim_start_matches('v'))
}

/// Fixture V, section 9.2: eight requests, one span each.
fn fixture_v_bodies(base_ns: i64) -> Vec<ExportTraceServiceRequest> {
    const MS: i64 = 1_000_000;
    let st = |k: &str, v: &str| kv(k, str_value(v));
    let start = |n: u8| base_ns + i64::from(n) * MS;
    let v_span = |n: u8,
                  attrs: Vec<KeyValue>,
                  events: Vec<(i64, &str, Vec<KeyValue>)>,
                  links: Vec<span::Link>| {
        span_with_message(
            vec![0xcc; 16],
            vec![0, 0, 0, 0, 0, 0, 0, 0xc0 + n],
            Vec::new(),
            "span",
            1,
            start(n),
            MS,
            attrs,
            None,
            events
                .into_iter()
                .map(|(offset, name, a)| event_of(start(n) + offset, name, a))
                .collect(),
            links,
        )
    };
    let resource = |n: u8, extra: Vec<KeyValue>| {
        let service = match n {
            3 => double_value(1.461413291919332),
            6 => int_value(12345),
            8 => str_array_value(&["vsvc", "x"]),
            _ => str_value("vsvc"),
        };
        let mut attrs = vec![kv("service.name", service)];
        attrs.extend(extra);
        attrs
    };
    let v_scope = |attrs: Vec<KeyValue>| scope_named("io.pulsus.v", "1.0", attrs);
    let dd_link = || link_of(vec![0xdd; 16], vec![0xdd; 8], vec![st("k", "l")]);
    let e1 = || vec![(2 * MS, "e1", vec![st("k", "e")])];

    vec![
        one_span_request(
            resource(1, vec![st("k", "r")]),
            v_scope(vec![st("k", "i")]),
            v_span(1, vec![st("k", "s")], e1(), vec![dd_link()]),
        ),
        one_span_request(
            resource(2, vec![st("k", "r")]),
            v_scope(vec![st("k", "i")]),
            v_span(2, Vec::new(), e1(), vec![dd_link()]),
        ),
        one_span_request(
            resource(3, Vec::new()),
            v_scope(vec![st("k", "i")]),
            v_span(3, Vec::new(), e1(), vec![dd_link()]),
        ),
        one_span_request(
            resource(4, Vec::new()),
            v_scope(vec![st("k", "i")]),
            v_span(4, Vec::new(), Vec::new(), vec![dd_link()]),
        ),
        one_span_request(
            resource(5, Vec::new()),
            v_scope(vec![st("k", "i")]),
            v_span(
                5,
                Vec::new(),
                vec![(-MS, "evN", vec![kv("tags", str_array_value(&["a", "b"]))])],
                Vec::new(),
            ),
        ),
        one_span_request(
            resource(6, Vec::new()),
            v_scope(Vec::new()),
            v_span(6, Vec::new(), Vec::new(), Vec::new()),
        ),
        one_span_request(
            resource(7, Vec::new()),
            v_scope(Vec::new()),
            v_span(
                7,
                Vec::new(),
                vec![
                    (MS, "evA", vec![st("code", "c1")]),
                    (2 * MS, "evB", vec![st("code", "c2"), st("flag", "yes")]),
                    (5 * MS, "evC", vec![kv("code", int_value(3))]),
                ],
                vec![
                    link_of(
                        vec![0x01; 16],
                        hex_bytes("0a0a0a0a0a0a0a01"),
                        vec![st("lk", "l1")],
                    ),
                    link_of(
                        vec![0x02; 16],
                        hex_bytes("0b0b0b0b0b0b0b02"),
                        vec![st("lk", "l2")],
                    ),
                ],
            ),
        ),
        one_span_request(
            resource(8, Vec::new()),
            v_scope(Vec::new()),
            v_span(
                8,
                Vec::new(),
                vec![(2 * MS, "evX", vec![kv("flag", bool_value(false))])],
                vec![link_of(
                    vec![0x03; 16],
                    hex_bytes("0c0c0c0c0c0c0c03"),
                    vec![kv("lflag", bool_value(false))],
                )],
            ),
        ),
    ]
}

const ALL_V: &[&str] = &["v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8"];
const ALL_V_BUT_V5: &[&str] = &["v1", "v2", "v3", "v4", "v6", "v7", "v8"];
const ALL_V_BUT_V6: &[&str] = &["v1", "v2", "v3", "v4", "v5", "v7", "v8"];
const ALL_V_BUT_V7: &[&str] = &["v1", "v2", "v3", "v4", "v5", "v6", "v8"];
const ALL_V_BUT_V8: &[&str] = &["v1", "v2", "v3", "v4", "v5", "v6", "v7"];
const V_SVC_STRING: &[&str] = &["v1", "v2", "v4", "v5", "v7"];

const CASES_V: &[CaseIn] = &[
    CaseIn {
        name: "CH s",
        query: r#"{ .k = "s" }"#,
        want: Want::Ids(&["v1"]),
    },
    CaseIn {
        name: "CH r",
        query: r#"{ .k = "r" }"#,
        want: Want::Ids(&["v2"]),
    },
    CaseIn {
        name: "CH e",
        query: r#"{ .k = "e" }"#,
        want: Want::Ids(&["v3"]),
    },
    CaseIn {
        name: "CH l",
        query: r#"{ .k = "l" }"#,
        want: Want::Ids(&["v4"]),
    },
    CaseIn {
        name: "CH i",
        query: r#"{ .k = "i" }"#,
        want: Want::Ids(&["v5"]),
    },
    CaseIn {
        name: "CH6",
        query: r#"{ .k != "e" }"#,
        want: Want::Ids(&["v1", "v2", "v4", "v5", "v6", "v7", "v8"]),
    },
    CaseIn {
        name: "CH7 != nil",
        query: r#"{ .k != nil }"#,
        want: Want::Ids(&["v1", "v2", "v3", "v4", "v5"]),
    },
    CaseIn {
        name: "CH7 = nil",
        query: r#"{ .k = nil }"#,
        want: Want::Ids(&["v6", "v7", "v8"]),
    },
    CaseIn {
        name: "SV1 >",
        query: r#"{ .service.name > "u" }"#,
        want: Want::Ids(V_SVC_STRING),
    },
    CaseIn {
        name: "SV1 <",
        query: r#"{ .service.name < "u" }"#,
        want: Want::Ids(&[]),
    },
    CaseIn {
        name: "SV2 =",
        query: r#"{ .service.name = 12345 }"#,
        want: Want::Ids(&["v6"]),
    },
    CaseIn {
        name: "SV2 !=",
        query: r#"{ .service.name != 12345 }"#,
        want: Want::Ids(ALL_V_BUT_V6),
    },
    CaseIn {
        name: "SVD =",
        query: r#"{ .service.name = 1.461413291919332 }"#,
        want: Want::Ids(&["v3"]),
    },
    CaseIn {
        name: "SVD >",
        query: r#"{ .service.name > 1.4 }"#,
        want: Want::Ids(&["v3", "v6"]),
    },
    CaseIn {
        name: "SV3 =",
        query: r#"{ .service.name = "12345" }"#,
        want: Want::Ids(&[]),
    },
    CaseIn {
        name: "SV3 =~",
        query: r#"{ .service.name =~ "1.*" }"#,
        want: Want::Ids(&[]),
    },
    CaseIn {
        name: "SV4 =",
        query: r#"{ .service.name = "x" }"#,
        want: Want::Ids(&["v8"]),
    },
    CaseIn {
        name: "SV4 !=",
        query: r#"{ .service.name != "x" }"#,
        want: Want::Ids(ALL_V_BUT_V8),
    },
    CaseIn {
        name: "EV1",
        query: r#"{ event.code = "c2" }"#,
        want: Want::Ids(&["v7"]),
    },
    CaseIn {
        name: "EV2",
        query: r#"{ event.code != "c2" }"#,
        want: Want::Ids(ALL_V_BUT_V7),
    },
    CaseIn {
        name: "EV3 = 3",
        query: r#"{ event.code = 3 }"#,
        want: Want::Ids(&["v7"]),
    },
    CaseIn {
        name: "EV3 > 2",
        query: r#"{ event.code > 2 }"#,
        want: Want::Ids(&["v7"]),
    },
    CaseIn {
        name: "EV4",
        query: r#"{ event.code !~ "c.*" }"#,
        want: Want::Ids(ALL_V_BUT_V7),
    },
    CaseIn {
        name: "EV5 != nil",
        query: r#"{ event.code != nil }"#,
        want: Want::Ids(&["v7"]),
    },
    CaseIn {
        name: "EV5 = nil",
        query: r#"{ event.code = nil }"#,
        want: Want::Ids(ALL_V_BUT_V7),
    },
    CaseIn {
        name: "EV6 event.tags =",
        query: r#"{ event.tags = "b" }"#,
        want: Want::Ids(&["v5"]),
    },
    CaseIn {
        name: "EV6 .tags",
        query: r#"{ .tags = "a" }"#,
        want: Want::Ids(&["v5"]),
    },
    CaseIn {
        name: "EV6 event.tags !=",
        query: r#"{ event.tags != "b" }"#,
        want: Want::Ids(ALL_V_BUT_V5),
    },
    CaseIn {
        name: "LK1",
        query: r#"{ link.lk = "l2" }"#,
        want: Want::Ids(&["v7"]),
    },
    CaseIn {
        name: "LK2",
        query: r#"{ link.lk != "l1" }"#,
        want: Want::Ids(ALL_V_BUT_V7),
    },
    CaseIn {
        name: "EN1 =",
        query: r#"{ event:name = "evB" }"#,
        want: Want::Ids(&["v7"]),
    },
    CaseIn {
        name: "EN1 !=",
        query: r#"{ event:name != "evB" }"#,
        want: Want::Ids(ALL_V_BUT_V7),
    },
    CaseIn {
        name: "EN2 ev[A-C]",
        query: r#"{ event:name =~ "ev[A-C]" }"#,
        want: Want::Ids(&["v7"]),
    },
    CaseIn {
        name: "EN2 ev",
        query: r#"{ event:name =~ "ev" }"#,
        want: Want::Ids(&[]),
    },
    CaseIn {
        name: "TS1",
        query: r#"{ event:timeSinceStart > 4ms }"#,
        want: Want::Ids(&["v7"]),
    },
    CaseIn {
        name: "TS2",
        query: r#"{ event:timeSinceStart != 2ms }"#,
        want: Want::Ids(&["v4", "v5", "v6"]),
    },
    CaseIn {
        name: "TS3 < 0",
        query: r#"{ event:timeSinceStart < 0 }"#,
        want: Want::Ids(&["v5"]),
    },
    CaseIn {
        name: "TS3 < 1ms",
        query: r#"{ event:timeSinceStart < 1ms }"#,
        want: Want::Ids(&["v5"]),
    },
    CaseIn {
        name: "LI1",
        query: r#"{ link:spanID = "0b0b0b0b0b0b0b02" }"#,
        want: Want::Ids(&["v7"]),
    },
    CaseIn {
        name: "LI2",
        query: r#"{ link:traceID != "02020202020202020202020202020202" }"#,
        want: Want::Ids(ALL_V_BUT_V7),
    },
    CaseIn {
        name: "NE1",
        query: r#"{ !link.lflag }"#,
        want: Want::Ids(&["v8"]),
    },
    CaseIn {
        name: "NE2",
        query: r#"{ !event.flag }"#,
        want: Want::Fails("expression (!event.flag) expected a boolean"),
    },
    CaseIn {
        name: "NE3",
        query: r#"{ !.lflag }"#,
        want: Want::Ids(&["v8"]),
    },
    CaseIn {
        name: "NE4",
        query: r#"{ !.flag }"#,
        want: Want::Fails("expression (!.flag) expected a boolean"),
    },
    CaseIn {
        name: "NE5",
        query: r#"{ false && !event.flag }"#,
        want: Want::Fails("expression (!event.flag) expected a boolean"),
    },
];

/// `UC-MISS`: run after `v2`'s resource row (shared with `v1`) is deleted.
const CASES_V_UC_MISS: &[CaseIn] = &[
    CaseIn {
        name: "UC-MISS r",
        query: r#"{ .k = "r" }"#,
        want: Want::Ids(&[]),
    },
    CaseIn {
        name: "UC-MISS e",
        query: r#"{ .k = "e" }"#,
        want: Want::Ids(&["v2", "v3"]),
    },
    CaseIn {
        name: "UC-MISS != r",
        query: r#"{ .k != "r" }"#,
        want: Want::Ids(ALL_V),
    },
];

/// `SV-MISS`: run after `UC-MISS` and the deletion of `v3`'s, `v6`'s,
/// `v7`'s and `v8`'s resource rows — every resource row is then gone.
const CASES_V_SV_MISS: &[CaseIn] = &[
    CaseIn {
        name: "SV-MISS = vsvc",
        query: r#"{ .service.name = "vsvc" }"#,
        want: Want::Ids(V_SVC_STRING),
    },
    CaseIn {
        name: "SV-MISS > u",
        query: r#"{ .service.name > "u" }"#,
        want: Want::Ids(V_SVC_STRING),
    },
    CaseIn {
        name: "SV-MISS = 12345",
        query: r#"{ .service.name = 12345 }"#,
        want: Want::Ids(&[]),
    },
    CaseIn {
        name: "SV-MISS = 1.461413291919332",
        query: r#"{ .service.name = 1.461413291919332 }"#,
        want: Want::Ids(&[]),
    },
    CaseIn {
        name: "SV-MISS = x",
        query: r#"{ .service.name = "x" }"#,
        want: Want::Ids(&[]),
    },
    CaseIn {
        name: "SV-MISS = nil",
        query: r#"{ .service.name = nil }"#,
        want: Want::Ids(&["v3", "v6", "v8"]),
    },
    CaseIn {
        name: "SV-MISS != nil",
        query: r#"{ .service.name != nil }"#,
        want: Want::Ids(V_SVC_STRING),
    },
];

/// Section 10.3: fixture V. `UC-MISS` and then `SV-MISS` run last: each
/// deletes resource rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_predicate_compiler_answers_any_element_and_the_chain_order() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_read_it_t589p2_fixturev");
    let client = fresh_db(&db).await;
    let base_ns = (now_ns() / 1_000_000_000) * 1_000_000_000;
    for (i, req) in fixture_v_bodies(base_ns).into_iter().enumerate() {
        land(&client, &req, &format!("t589p2-v-{i}-{}", now_ns())).await;
    }
    let w = WindowSql::start_closed_end_open(base_ns, base_ns + WINDOW_NS);

    let seeded = count(&client, &format!("SELECT count() AS n FROM {SPANS_TABLE}")).await;
    assert_eq!(
        seeded, 8,
        "fixture V seeds eight spans; nothing below can be read as a predicate result until \
         this holds"
    );

    let mut wrong = run_cases_in(&client, w, CASES_V, idv).await;
    delete_resource_row_of(&client, &idv("v2")).await;
    wrong.extend(run_cases_in(&client, w, CASES_V_UC_MISS, idv).await);
    for v in ["v3", "v6", "v7", "v8"] {
        delete_resource_row_of(&client, &idv(v)).await;
    }
    wrong.extend(run_cases_in(&client, w, CASES_V_SV_MISS, idv).await);
    drop_db(&db).await;
    assert!(
        wrong.is_empty(),
        "{} of {} fixture-V cases answer something else:\n\n{}",
        wrong.len(),
        CASES_V.len() + CASES_V_UC_MISS.len() + CASES_V_SV_MISS.len(),
        wrong.join("\n\n")
    );
}

// ---------------------------------------------------------------------
// #589 part 3a — fixture W, field against field
// ---------------------------------------------------------------------

/// `w01` to `w23`: span ids `000000000000e001` to `…e023`, the span's
/// number written as decimal digits in the id's last byte.
fn idw(short: &str) -> String {
    format!("000000000000e0{}", short.trim_start_matches('w'))
}

/// One fixture-W span: its number, span attributes, scope attributes,
/// resource `service.name`, name, kind and status code.
type WSpan<'a> = (
    u8,
    Vec<KeyValue>,
    Vec<KeyValue>,
    AnyValue,
    &'a str,
    i32,
    i32,
);

/// Fixture W, the part-3a design's section 5.1: twenty-three requests,
/// one span each.
fn fixture_w_bodies(base_ns: i64) -> Vec<ExportTraceServiceRequest> {
    const MS: i64 = 1_000_000;
    let st = |k: &str, v: &str| kv(k, str_value(v));
    let it = |k: &str, v: i64| kv(k, int_value(v));
    let db = |k: &str, v: f64| kv(k, double_value(v));
    let bl = |k: &str, v: bool| kv(k, bool_value(v));
    let ar = |k: &str, v: &[&str]| kv(k, str_array_value(v));
    let mut spans: Vec<WSpan<'_>> = Vec::new();
    let wsvc = || str_value("wsvc");
    let plain = |n: u8, attrs: Vec<KeyValue>| (n, attrs, Vec::new(), wsvc(), "op", 1, 0);
    spans.push(plain(1, vec![st("a", "5"), it("b", 5)]));
    spans.push(plain(2, vec![it("a", 5), it("b", 5)]));
    spans.push(plain(
        3,
        vec![
            it("a", 9_007_199_254_740_993),
            it("b", 9_007_199_254_740_992),
        ],
    ));
    spans.push(plain(
        4,
        vec![
            it("a", 9_007_199_254_740_992),
            it("b", 9_007_199_254_740_993),
        ],
    ));
    spans.push(plain(5, vec![st("a", "apple"), st("b", "banana")]));
    spans.push(plain(6, vec![it("a", 3), db("b", 0.25)]));
    spans.push(plain(
        7,
        vec![
            it("a", 9_007_199_254_740_993),
            db("b", 9_007_199_254_740_992.0),
        ],
    ));
    spans.push(plain(8, vec![bl("a", true), bl("b", true)]));
    spans.push(plain(9, vec![st("a", "x")]));
    spans.push(plain(10, vec![ar("a", &["gold", "eu"]), st("b", "eu")]));
    spans.push(plain(11, vec![ar("a", &["x", "y"]), st("b", "z")]));
    spans.push(plain(
        12,
        vec![kv("a", int_array_value(&[1, 2])), it("b", 2)],
    ));
    spans.push(plain(13, vec![st("a", "5"), st("b", "5")]));
    spans.push(plain(14, vec![bl("a", false), bl("b", true)]));
    spans.push(plain(15, vec![ar("a", &[]), st("b", "q")]));
    spans.push((
        16,
        vec![
            st("nm", "op-x"),
            it("st", 2),
            it("kd", 2),
            it("dn", 1_000_000),
            st("sid", "000000000000e016"),
        ],
        Vec::new(),
        wsvc(),
        "op-x",
        2,
        2,
    ));
    spans.push((
        17,
        vec![
            st("nm", "op-y"),
            db("dfl", 1_000_000.5),
            st("sid", "000000000000E017"),
        ],
        Vec::new(),
        wsvc(),
        "op-x",
        1,
        0,
    ));
    spans.push((18, vec![it("c", 5)], vec![st("c", "5")], wsvc(), "op", 1, 0));
    spans.push((
        19,
        vec![it("c", 7), st("iname", "io.pulsus.w")],
        vec![it("c", 7)],
        wsvc(),
        "op",
        1,
        0,
    ));
    spans.push((20, vec![db("c", 7.5)], vec![it("c", 7)], wsvc(), "op", 1, 0));
    spans.push(plain(21, vec![st("svc", "wsvc")]));
    spans.push((
        22,
        vec![st("svc", "7")],
        Vec::new(),
        int_value(7),
        "op",
        1,
        0,
    ));
    spans.push((23, vec![it("svc", 8)], Vec::new(), int_value(8), "op", 1, 0));

    spans
        .into_iter()
        .map(|(n, attrs, scope_attrs, service, name, kind, status)| {
            let last = u8::from_str_radix(&format!("{n:02}"), 16).expect("two decimal digits");
            one_span_request(
                vec![kv("service.name", service)],
                scope_named("io.pulsus.w", "1.0", scope_attrs),
                span_with_message(
                    vec![0xee; 16],
                    vec![0, 0, 0, 0, 0, 0, 0xe0, last],
                    Vec::new(),
                    name,
                    kind,
                    base_ns + i64::from(n) * MS,
                    MS,
                    attrs,
                    (status != 0).then_some((status, "boom")),
                    Vec::new(),
                    Vec::new(),
                ),
            )
        })
        .collect()
}

const ALL_W: &[&str] = &[
    "w01", "w02", "w03", "w04", "w05", "w06", "w07", "w08", "w09", "w10", "w11", "w12", "w13",
    "w14", "w15", "w16", "w17", "w18", "w19", "w20", "w21", "w22", "w23",
];

/// The part-3a design's section 6.1.
const CASES_W: &[CaseIn] = &[
    CaseIn {
        name: "FF-EQ",
        query: r#"{ span.a = span.b }"#,
        want: Want::Ids(&["w02", "w08", "w10", "w12", "w13"]),
    },
    CaseIn {
        name: "FF-NE",
        query: r#"{ span.a != span.b }"#,
        want: Want::Ids(&["w03", "w04", "w05", "w06", "w07", "w11", "w14"]),
    },
    CaseIn {
        name: "FF-LT",
        query: r#"{ span.a < span.b }"#,
        want: Want::Ids(&["w04", "w05", "w11", "w12"]),
    },
    CaseIn {
        name: "FF-LE",
        query: r#"{ span.a <= span.b }"#,
        want: Want::Ids(&["w02", "w04", "w05", "w10", "w11", "w12", "w13"]),
    },
    CaseIn {
        name: "FF-GT",
        query: r#"{ span.a > span.b }"#,
        want: Want::Ids(&["w03", "w06", "w07", "w10"]),
    },
    CaseIn {
        name: "FF-GE",
        query: r#"{ span.a >= span.b }"#,
        want: Want::Ids(&["w02", "w03", "w06", "w07", "w10", "w12", "w13"]),
    },
    CaseIn {
        name: "FF-MIR >",
        query: r#"{ span.b > span.a }"#,
        want: Want::Ids(&["w04", "w05", "w11", "w12"]),
    },
    CaseIn {
        name: "FF-MIR <=",
        query: r#"{ span.b <= span.a }"#,
        want: Want::Ids(&["w02", "w03", "w06", "w07", "w10", "w12", "w13"]),
    },
    CaseIn {
        name: "IC1",
        query: r#"{ instrumentation.c = span.c }"#,
        want: Want::Ids(&["w19"]),
    },
    CaseIn {
        name: "IC2",
        query: r#"{ instrumentation.c < span.c }"#,
        want: Want::Ids(&["w20"]),
    },
    CaseIn {
        name: "IC3",
        query: r#"{ instrumentation.c != span.c }"#,
        want: Want::Ids(&["w20"]),
    },
    CaseIn {
        name: "NM =",
        query: r#"{ name = span.nm }"#,
        want: Want::Ids(&["w16"]),
    },
    CaseIn {
        name: "NM <",
        query: r#"{ name < span.nm }"#,
        want: Want::Ids(&["w17"]),
    },
    CaseIn {
        name: "DU =",
        query: r#"{ duration = span.dn }"#,
        want: Want::Ids(&["w16"]),
    },
    CaseIn {
        name: "DU <",
        query: r#"{ duration < span.dfl }"#,
        want: Want::Ids(&["w17"]),
    },
    CaseIn {
        name: "SK status",
        query: r#"{ status = span.st }"#,
        want: Want::Ids(&[]),
    },
    CaseIn {
        name: "SK kind",
        query: r#"{ kind = span.kd }"#,
        want: Want::Ids(&[]),
    },
    CaseIn {
        name: "SS status =",
        query: r#"{ status = status }"#,
        want: Want::Ids(ALL_W),
    },
    CaseIn {
        name: "SS status !=",
        query: r#"{ status != status }"#,
        want: Want::Ids(&[]),
    },
    CaseIn {
        name: "SS kind =",
        query: r#"{ kind = kind }"#,
        want: Want::Ids(ALL_W),
    },
    CaseIn {
        name: "ID",
        query: r#"{ span:id = span.sid }"#,
        want: Want::Ids(&["w16"]),
    },
    CaseIn {
        name: "SVC",
        query: r#"{ resource.service.name = span.svc }"#,
        want: Want::Ids(&["w21"]),
    },
    CaseIn {
        name: "IN",
        query: r#"{ instrumentation:name = span.iname }"#,
        want: Want::Ids(&["w19"]),
    },
];

/// Section 6.1 of the part-3a design: fixture W.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_predicate_compiler_compares_two_span_row_fields() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_read_it_t589p3a_fixturew");
    let client = fresh_db(&db).await;
    let base_ns = (now_ns() / 1_000_000_000) * 1_000_000_000;
    for (i, req) in fixture_w_bodies(base_ns).into_iter().enumerate() {
        land(&client, &req, &format!("t589p3a-w-{i}-{}", now_ns())).await;
    }
    let w = WindowSql::start_closed_end_open(base_ns, base_ns + WINDOW_NS);

    let seeded = count(&client, &format!("SELECT count() AS n FROM {SPANS_TABLE}")).await;
    assert_eq!(
        seeded, 23,
        "fixture W seeds twenty-three spans; nothing below can be read as a predicate result \
         until this holds"
    );

    let wrong = run_cases_in(&client, w, CASES_W, idw).await;
    drop_db(&db).await;
    assert!(
        wrong.is_empty(),
        "{} of {} fixture-W cases answer something else:\n\n{}",
        wrong.len(),
        CASES_W.len(),
        wrong.join("\n\n")
    );
}

/// Section 6.2 of the part-3a design: the field half of `T-A7`.
const CASES_61_FIELDS: &[CaseIn] = &[CaseIn {
    name: "T-A7 (field half)",
    query: r#"{ span.app.items.count > span.app.discount.ratio }"#,
    want: Want::Ids(&["0003"]),
}];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_predicate_compiler_compares_fields_on_the_worked_fixture() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_read_it_t589p3a_fixture61");
    let client = fresh_db(&db).await;
    let base_ns = (now_ns() / 1_000_000_000) * 1_000_000_000;
    for (i, req) in fixture_61_bodies(base_ns).into_iter().enumerate() {
        land(&client, &req, &format!("t589p3a-61-{i}-{}", now_ns())).await;
    }
    let w = WindowSql::start_closed_end_open(base_ns, base_ns + WINDOW_NS);

    let seeded = count(&client, &format!("SELECT count() AS n FROM {SPANS_TABLE}")).await;
    assert_eq!(
        seeded, 9,
        "the §6.1 fixture seeds nine spans; nothing below can be read as a predicate result \
         until this holds"
    );

    let wrong = run_cases_in(&client, w, CASES_61_FIELDS, id61).await;
    drop_db(&db).await;
    assert!(
        wrong.is_empty(),
        "{} of {} §6.1 field cases answer something else:\n\n{}",
        wrong.len(),
        CASES_61_FIELDS.len(),
        wrong.join("\n\n")
    );
}
