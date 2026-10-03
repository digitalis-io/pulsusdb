//! The span row encoder and the `JSON` column, round-tripped against a
//! running ClickHouse server (issue #585).
//!
//! **The test seam, because there is no compiler and no route on the new
//! tables yet.** Every case here decodes an OTLP request with the landing
//! decoder, inserts the rows it produces into `trace_landing` with the
//! production client, and reads back through the five views with a literal
//! `SELECT` written out in the case. None of them goes through a `q=`
//! parameter or an HTTP route; those arrive with the read-path tasks, and
//! each re-asserts the same behaviour through the route it adds.
//!
//! **What had to be fixed before any of this could run.** The trace landing
//! table declares its `events` and `links` columns as
//! `Array(Tuple(time_ns Int64, name LowCardinality(String), attrs JSON,
//! dropped_attrs UInt32))` — named tuple elements, transcribed from
//! `docs/TraceQL/measure/schema.sql` — and the type parser the driver uses
//! for insert-side schema validation could not parse a **named** tuple at
//! any depth. Measured against `clickhouse-types` 0.1.2, the release the
//! lockfile pins, and 0.1.3, the latest published:
//!
//! ```text
//! Array(Tuple(time_ns Int64, name LowCardinality(String), attrs JSON, dropped_attrs UInt32))
//!   -> type parsing error: Unknown data type: time_ns Int64
//! Tuple(a Int64, b String)
//!   -> type parsing error: Unknown data type: a Int64
//! Array(Tuple(Int64, LowCardinality(String), JSON, UInt32))
//!   -> OK
//! ```
//!
//! so every insert into `trace_landing` failed before a byte was sent:
//!
//! ```text
//! Decode("error while parsing columns header from the response: \
//!   type parsing error: Unknown data type: \n    time_ns Int64")
//! ```
//!
//! Two routes were measured and neither was taken: dropping the element
//! names makes a read say `events.2` rather than `events.name` and moves the
//! approved DDL, and `Client::with_validation(false)` removes insert-side
//! validation for every insert of every signal. The route taken instead is
//! `vendor/clickhouse-types`, vendored at 0.1.2 and patched — see its
//! `PATCHES.md` and `crates/pulsus-clickhouse/tests/named_tuple_types.rs`,
//! which is the parser's own case set. The schema did not move.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:26.3
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-write --test trace_rows_v2
//! podman rm -f pulsus-ch-test
//! ```

use std::time::Duration;

use futures::StreamExt;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList, any_value,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span};
use prost::Message as _;

// Two readings this file had to measure rather than assume, both on
// ClickHouse 26.3.29.7:
//
//   * a `JSON` subcolumn read is **Nullable** — `attrs.`k`.:String` is
//     `Nullable(String)` — so a non-nullable row field cannot take one and
//     every such read goes through `assumeNotNull`;
//   * a `JSON` nested inside an `Array(Tuple(…))` element takes **no**
//     `.:Type` cast at all (`Code: 62` at the colon), so an event's or a
//     link's attribute is read with `toString` and its stored type with
//     `dynamicType`, which is the stronger read anyway.
//
// And one expected value: the server names a mixed array's stored type
// `Array(Dynamic)`, without the `max_types` parameter the wire form carries.

use pulsus_clickhouse::{
    ChClient, ChConnConfig, ChError, ChProto, Idempotency, QuerySettings, Row,
};
use pulsus_schema::{RenderCtx, SchemaParams, run_init};
use pulsus_write::{ParsedTraceLanding, TraceLandingRow, parse_trace_landing, resource_identity};

/// An instant inside the span retention window. `ttl_only_drop_parts = 1`
/// makes a whole already-expired part eligible for deletion right after the
/// insert, so a fixture at a fixed past instant would leave a case reading
/// an empty table.
fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("a clock after the epoch")
            .as_nanos(),
    )
    .expect("a representable instant")
}

fn now_ms() -> i64 {
    now_ns() / 1_000_000
}

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-write/tests/trace_rows_v2.rs for setup)"
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
        database: std::env::var("PULSUS_TEST_CH_DATABASE")
            .unwrap_or_else(|_| "default".to_string()),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(20),
        ..ChConnConfig::default()
    }
}

/// A fresh database with the whole schema in it, and a client bound to it.
///
/// `db` is already composed by `pulsus_testkit::test_db`, at the call site,
/// so the prefix reaches every name: two checkouts sharing one ClickHouse
/// would otherwise both use it and drop each other's data.
async fn fresh_db(db: String) -> (ChClient, String) {
    let admin = ChClient::new(base_config()).await.expect("connect");
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the test database");
    let ctx: SchemaParams = RenderCtx::for_tests(&db);
    run_init(&admin, &ctx).await.expect("run_init");

    let client = ChClient::new(ChConnConfig {
        database: db.clone(),
        ..base_config()
    })
    .await
    .expect("connect to the test database");
    (client, db)
}

async fn drop_db(db: &str) {
    let admin = ChClient::new(base_config()).await.expect("connect");
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the test database");
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CountRow {
    n: u64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct TextRow {
    s: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct BytesRow {
    #[serde(with = "serde_bytes")]
    b: Vec<u8>,
}

async fn count(client: &ChClient, sql: &str) -> u64 {
    let mut stream = client
        .query_stream::<CountRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("count failed: {e}\nSQL:\n{sql}"));
    stream.next().await.expect("one row").expect("decode").n
}

async fn scalar(client: &ChClient, sql: &str) -> String {
    let mut stream = client
        .query_stream::<TextRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("scalar failed: {e}\nSQL:\n{sql}"));
    stream.next().await.expect("one row").expect("decode").s
}

async fn texts(client: &ChClient, sql: &str) -> Vec<String> {
    let mut stream = client
        .query_stream::<TextRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("text query failed: {e}\nSQL:\n{sql}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode").s);
    }
    out
}

async fn blob(client: &ChClient, sql: &str) -> Vec<u8> {
    let mut stream = client
        .query_stream::<BytesRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("blob query failed: {e}\nSQL:\n{sql}"));
    stream.next().await.expect("one row").expect("decode").b
}

// --- fixture builders -----------------------------------------------------

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

fn array_value(values: Vec<AnyValue>) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::ArrayValue(ArrayValue { values })),
    }
}

fn kvlist_value(pairs: Vec<(&str, AnyValue)>) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::KvlistValue(KeyValueList {
            values: pairs
                .into_iter()
                .map(|(k, v)| KeyValue {
                    key: k.to_string(),
                    value: Some(v),
                    key_strindex: 0,
                })
                .collect(),
        })),
    }
}

fn bytes_value(bytes: &[u8]) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::BytesValue(bytes.to_vec())),
    }
}

fn kv(key: &str, value: AnyValue) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(value),
        key_strindex: 0,
    }
}

/// One span, with the ids and attributes given. `start_ns` is taken from the
/// clock so the fixture sits inside the retention window.
fn span_with(trace: u8, id: u8, attrs: Vec<KeyValue>) -> Span {
    Span {
        trace_id: vec![trace; 16],
        span_id: vec![id; 8],
        parent_span_id: Vec::new(),
        trace_state: String::new(),
        flags: 0,
        name: "GET /api".to_string(),
        kind: 2,
        start_time_unix_nano: now_ns() as u64,
        end_time_unix_nano: now_ns() as u64 + 1_000_000,
        attributes: attrs,
        dropped_attributes_count: 0,
        events: Vec::new(),
        dropped_events_count: 0,
        links: Vec::new(),
        dropped_links_count: 0,
        status: Some(Status {
            message: String::new(),
            code: 1,
        }),
    }
}

/// One request carrying `spans` under a resource whose `service.name` is
/// `service`, and no scope attributes.
fn request(service: &str, spans: Vec<Span>) -> ExportTraceServiceRequest {
    request_with(service, Vec::new(), spans, None, "")
}

fn request_with(
    service: &str,
    resource_attrs: Vec<KeyValue>,
    spans: Vec<Span>,
    scope: Option<InstrumentationScope>,
    schema_url: &str,
) -> ExportTraceServiceRequest {
    let mut attributes = vec![kv("service.name", str_value(service))];
    attributes.extend(resource_attrs);
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes,
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_spans: vec![ScopeSpans {
                scope,
                spans,
                schema_url: String::new(),
            }],
            schema_url: schema_url.to_string(),
        }],
    }
}

/// The settings every landing insert this file makes carries — the writer's
/// own constructor, so a case's insert is pinned the way a push's is. The
/// token is minted per insert from the clock, which is all the landing
/// table's own deduplication window needs of it here.
fn landing_settings() -> QuerySettings {
    QuerySettings::trace_landing_insert(&format!("it-{}", now_ns()), 1_048_576)
}

/// [`land`] without the panic, for the two cases whose own assertion is that
/// the insert succeeded. A case that unwraps here reports neither the input
/// nor the expectation.
async fn try_land(
    client: &ChClient,
    req: &ExportTraceServiceRequest,
) -> Result<ParsedTraceLanding, ChError> {
    let parsed = parse_trace_landing(req, now_ns()).expect("the landing decode");
    let received_ms = now_ms();
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
    // **Through the landing insert's own pinned settings**, which is what a
    // push carries: `materialized_views_ignore_errors = 0` among them, which
    // is what makes a throwing view fail the insert rather than be ignored.
    client
        .insert_block_with("trace_landing", &rows, &landing_settings())
        .await?;
    Ok(parsed)
}

/// Decodes `req` and inserts every row it produces into `trace_landing`
/// through the production client, in **one** block — one push is one insert.
async fn land(client: &ChClient, req: &ExportTraceServiceRequest) -> ParsedTraceLanding {
    try_land(client, req).await.expect("the landing insert")
}

/// [`land`] at a `received_ms` the caller chooses, so a case can place a
/// push inside or outside a replay window it names (issue #586).
async fn land_at(
    client: &ChClient,
    req: &ExportTraceServiceRequest,
    received_ms: i64,
) -> ParsedTraceLanding {
    let parsed = parse_trace_landing(req, now_ns()).expect("the landing decode");
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
        .insert_block_with("trace_landing", &rows, &landing_settings())
        .await
        .expect("the landing insert");
    parsed
}

// --- the cases ------------------------------------------------------------

/// **T-A3.** A flat key spelled with a dot and a nested object are distinct
/// stored paths, and neither predicate counts the other's span.
///
/// Measured on ClickHouse 26.3.29.7: the two predicates answer `1, 0` on the
/// flat key's span and `0, 1` on the nested one's.
#[tokio::test]
async fn t_a3_the_dotted_key_and_the_nested_object_are_distinct_paths() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_rows_it_a3")).await;

    land(
        &client,
        &request(
            "checkout",
            vec![
                span_with(0xa1, 0x11, vec![kv("a.b", int_value(1))]),
                span_with(
                    0xa2,
                    0x22,
                    vec![kv("a", kvlist_value(vec![("b", int_value(1))]))],
                ),
            ],
        ),
    )
    .await;

    let flat = scalar(
        &client,
        "SELECT hex(span_id) AS s FROM spans \
         WHERE coalesce(attrs.`a%2Eb`.:Int64 = 1, false)",
    )
    .await;
    let nested = scalar(
        &client,
        "SELECT hex(span_id) AS s FROM spans WHERE coalesce(attrs.a.b.:Int64 = 1, false)",
    )
    .await;
    assert_eq!(
        flat, "1111111111111111",
        "the escaped path belongs to the flat key's span"
    );
    assert_eq!(
        nested, "2222222222222222",
        "the dotted leaf path belongs to the nested object's span"
    );
    assert_ne!(flat, nested, "neither predicate may count the other's span");
    assert_eq!(
        count(
            &client,
            "SELECT count() AS n FROM spans WHERE coalesce(attrs.`a%2Eb`.:Int64 = 1, false)"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &client,
            "SELECT count() AS n FROM spans WHERE coalesce(attrs.a.b.:Int64 = 1, false)"
        )
        .await,
        1
    );

    drop_db(&db).await;
}

/// **T-A4.** A key literally spelled `a%2Eb` stores the path `a%252Eb`, so it
/// does not collide with the key `a.b`, and the unescape expression recovers
/// the original key.
#[tokio::test]
async fn t_a4_a_literally_percent_named_key_double_escapes() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_rows_it_a4")).await;

    land(
        &client,
        &request(
            "checkout",
            vec![span_with(0xb1, 0x33, vec![kv("a%2Eb", int_value(1))])],
        ),
    )
    .await;

    assert_eq!(
        count(
            &client,
            "SELECT count() AS n FROM spans WHERE dynamicType(attrs.`a%252Eb`) != 'None'"
        )
        .await,
        1,
        "the stored path is the double-escaped one"
    );
    assert_eq!(
        count(
            &client,
            "SELECT count() AS n FROM spans WHERE dynamicType(attrs.`a%2Eb`) != 'None'"
        )
        .await,
        0,
        "and it is not the path the key `a.b` would store"
    );
    assert_eq!(
        scalar(
            &client,
            "SELECT replaceAll(replaceAll('a%252Eb', '%2E', '.'), '%25', '%') AS s"
        )
        .await,
        "a%2Eb",
        "the unescape expression recovers the original OTLP key"
    );

    drop_db(&db).await;
}

/// **T-A5.** A span carrying one key twice stores the first value.
///
/// Without the writer's own deduplication the server refuses the block:
/// `type_json_skip_duplicated_paths` is `0`, so a repeated path in one value
/// is an exception rather than a silent drop.
#[tokio::test]
async fn t_a5_a_duplicate_key_in_one_span_keeps_the_first_value() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_rows_it_a5")).await;

    let req = request(
        "checkout",
        vec![span_with(
            0xc1,
            0x44,
            vec![kv("k", str_value("first")), kv("k", str_value("second"))],
        )],
    );
    land(&client, &req).await;

    assert_eq!(
        scalar(
            &client,
            "SELECT assumeNotNull(attrs.`k`.:String) AS s FROM spans"
        )
        .await,
        "first",
        "the first value of a repeated key wins"
    );

    drop_db(&db).await;
}

/// **T-A6.** The three non-finite doubles survive the binary form.
///
/// **This case is what establishes the claim; nothing in the tree did.** Text
/// JSON refuses all three on this engine — `{"k":NaN}`, `{"k":Inf}`,
/// `{"k":Infinity}` and `{"k":1e400}` each answer `Code: 117` — and two
/// non-text routes were tried and both **dropped** the value:
/// `SELECT CAST(map('k', toFloat64('inf')) AS JSON)` and
/// `SELECT CAST(CAST(tuple(toFloat64('inf')), 'Tuple(k Float64)') AS JSON)`
/// each answer `{}`. So it needs the insert.
#[tokio::test]
async fn t_a6_the_non_finite_doubles_survive_the_binary_form() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_rows_it_a6")).await;

    land(
        &client,
        &request(
            "checkout",
            vec![
                span_with(0xd1, 0x51, vec![kv("k", double_value(f64::INFINITY))]),
                span_with(0xd2, 0x52, vec![kv("k", double_value(f64::NEG_INFINITY))]),
                span_with(0xd3, 0x53, vec![kv("k", double_value(f64::NAN))]),
            ],
        ),
    )
    .await;

    assert_eq!(
        count(
            &client,
            "SELECT count() AS n FROM spans WHERE coalesce(attrs.`k`.:Float64 > 500, false)"
        )
        .await,
        1,
        "+Inf reads above every finite bound"
    );
    assert_eq!(
        count(
            &client,
            "SELECT count() AS n FROM spans WHERE coalesce(attrs.`k`.:Float64 < -500, false)"
        )
        .await,
        1,
        "-Inf reads below every finite bound"
    );
    assert_eq!(
        count(
            &client,
            "SELECT count() AS n FROM spans \
             WHERE coalesce(isNaN(attrs.`k`.:Float64), false)"
        )
        .await,
        1,
        "and NaN reads as NaN"
    );
    let types = texts(
        &client,
        "SELECT DISTINCT toTypeName(attrs.`k`.:Float64) AS s FROM spans",
    )
    .await;
    assert_eq!(
        types,
        vec!["Nullable(Float64)".to_string()],
        "every one reads through the same subcolumn type"
    );

    drop_db(&db).await;
}

/// **Every OTLP value kind round-trips**, one span per row of the stored-type
/// table, each read back with the statement given.
///
/// The expected values were measured on ClickHouse 26.3.29.7. **The kvlist
/// row is the one whose expected value #585 states wrongly**: a nested object
/// has no value at its parent path, so `dynamicType(attrs.`k`)` is `None`
/// there and not "not `None`" — measured,
/// `WITH CAST('{"k":{"a":{"b":1}}}' AS JSON) AS attrs SELECT
/// dynamicType(attrs.`k`)` answers `None` while `JSONAllPathsWithTypes(attrs)`
/// answers `{'k.a.b':'Int64'}`.
#[tokio::test]
async fn every_otlp_value_kind_round_trips() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_rows_it_kinds")).await;

    // One span per value kind, each under its own span id so a read can
    // name it.
    let spans = vec![
        span_with(0xe0, 0x01, vec![kv("k", str_value("s"))]),
        span_with(0xe0, 0x02, vec![kv("k", bool_value(true))]),
        span_with(0xe0, 0x03, vec![kv("k", int_value(7))]),
        span_with(0xe0, 0x04, vec![kv("k", double_value(1.5))]),
        span_with(
            0xe0,
            0x05,
            vec![kv("k", array_value(vec![str_value("a"), str_value("b")]))],
        ),
        span_with(
            0xe0,
            0x06,
            vec![kv("k", array_value(vec![int_value(1), int_value(2)]))],
        ),
        span_with(
            0xe0,
            0x07,
            vec![kv("k", array_value(vec![str_value("a"), int_value(1)]))],
        ),
        span_with(
            0xe0,
            0x08,
            vec![kv("k", array_value(vec![bytes_value(&[1, 2, 3])]))],
        ),
        span_with(
            0xe0,
            0x09,
            vec![kv(
                "k",
                kvlist_value(vec![("a", kvlist_value(vec![("b", int_value(1))]))]),
            )],
        ),
        span_with(0xe0, 0x0a, vec![kv("k", kvlist_value(vec![]))]),
        span_with(0xe0, 0x0b, vec![kv("k", bytes_value(&[9, 9]))]),
        span_with(
            0xe0,
            0x0c,
            vec![KeyValue {
                key: "k".to_string(),
                value: Some(AnyValue { value: None }),
                key_strindex: 0,
            }],
        ),
    ];
    land(&client, &request("checkout", spans)).await;

    let at = |id: u8| format!("span_id = unhex('{:016x}')", u64::from_be_bytes([id; 8]));
    // `span_with` fills all eight bytes with the same value, so the hex of
    // the id is that byte repeated sixteen times.
    let id_hex = |id: u8| format!("{:02x}", id).repeat(8);
    let where_id = |id: u8| format!("span_id = unhex('{}')", id_hex(id));
    let _ = at;

    // The four scalars.
    assert_eq!(
        scalar(
            &client,
            &format!(
                "SELECT assumeNotNull(attrs.`k`.:String) AS s FROM spans WHERE {}",
                where_id(0x01)
            )
        )
        .await,
        "s"
    );
    assert_eq!(
        scalar(
            &client,
            &format!(
                "SELECT toString(assumeNotNull(attrs.`k`.:Bool)) AS s FROM spans WHERE {}",
                where_id(0x02)
            )
        )
        .await,
        "true"
    );
    assert_eq!(
        scalar(
            &client,
            &format!(
                "SELECT toString(assumeNotNull(attrs.`k`.:Int64)) AS s FROM spans WHERE {}",
                where_id(0x03)
            )
        )
        .await,
        "7"
    );
    assert_eq!(
        scalar(
            &client,
            &format!(
                "SELECT toString(assumeNotNull(attrs.`k`.:Float64)) AS s FROM spans WHERE {}",
                where_id(0x04)
            )
        )
        .await,
        "1.5"
    );

    // The two typed arrays keep their element type.
    assert_eq!(
        count(
            &client,
            &format!(
                "SELECT count() AS n FROM spans \
                 WHERE {} AND has(attrs.`k`.:`Array(Nullable(String))`, 'b')",
                where_id(0x05)
            )
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &client,
            &format!(
                "SELECT count() AS n FROM spans \
                 WHERE {} AND has(attrs.`k`.:`Array(Nullable(Int64))`, 2)",
                where_id(0x06)
            )
        )
        .await,
        1
    );

    // A mixed array falls to `Array(Dynamic)`.
    assert_eq!(
        scalar(
            &client,
            &format!(
                "SELECT dynamicType(attrs.`k`) AS s FROM spans WHERE {}",
                where_id(0x07)
            )
        )
        .await,
        "Array(Dynamic)",
        "the server names the stored type without the `max_types` the wire \
         form carries"
    );
    assert_eq!(
        scalar(
            &client,
            &format!(
                "SELECT toString(assumeNotNull(attrs.`k`.:`Array(Dynamic)`)) \
                 AS s FROM spans WHERE {}",
                where_id(0x07)
            )
        )
        .await,
        "['a',1]"
    );

    // The four shapes with no JSON representation: the path is absent and
    // `attrs_other` carries the key under its original OTLP spelling.
    for (label, id) in [
        ("an array holding a bytes element", 0x08u8),
        ("an empty kvlist", 0x0a),
        ("a bytes value", 0x0b),
        ("an AnyValue with no arm set", 0x0c),
    ] {
        assert_eq!(
            scalar(
                &client,
                &format!(
                    "SELECT dynamicType(attrs.`k`) AS s FROM spans WHERE {}",
                    where_id(id)
                )
            )
            .await,
            "None",
            "{label}: no path is stored"
        );
        let other = blob(
            &client,
            &format!("SELECT attrs_other AS b FROM spans WHERE {}", where_id(id)),
        )
        .await;
        let decoded = KeyValueList::decode(other.as_slice())
            .unwrap_or_else(|e| panic!("{label}: attrs_other must decode as a KeyValueList: {e}"));
        assert_eq!(
            decoded
                .values
                .iter()
                .map(|kv| kv.key.as_str())
                .collect::<Vec<_>>(),
            vec!["k"],
            "{label}: the key is carried under its original OTLP spelling"
        );
    }

    // The nested kvlist: dotted leaf paths and **nothing at the parent**.
    assert_eq!(
        scalar(
            &client,
            &format!(
                "SELECT toString(assumeNotNull(attrs.`k`.a.b.:Int64)) AS s FROM spans WHERE {}",
                where_id(0x09)
            )
        )
        .await,
        "1"
    );
    assert_eq!(
        scalar(
            &client,
            &format!(
                "SELECT dynamicType(attrs.`k`) AS s FROM spans WHERE {}",
                where_id(0x09)
            )
        )
        .await,
        "None",
        "a nested object has no value at its parent path — this is the \
         expected value issue #585 states wrongly"
    );

    drop_db(&db).await;
}

/// An empty array is stored as `Array(Nullable(String))` with no element.
///
/// It is a decision this encoder makes rather than something the wire says:
/// an empty array carries no element type, and the stored column needs one.
#[tokio::test]
async fn an_empty_array_is_stored_as_a_string_array() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_rows_it_emptyarr")).await;

    land(
        &client,
        &request(
            "checkout",
            vec![span_with(0xf1, 0x61, vec![kv("k", array_value(vec![]))])],
        ),
    )
    .await;

    assert_eq!(
        scalar(&client, "SELECT dynamicType(attrs.`k`) AS s FROM spans").await,
        "Array(Nullable(String))"
    );
    assert_eq!(
        scalar(
            &client,
            "SELECT toString(length(assumeNotNull(attrs.`k`.:`Array(Nullable(String))`))) \
             AS s FROM spans"
        )
        .await,
        "0"
    );

    drop_db(&db).await;
}

/// **The resource identity is 128 bits and stable.** Eight pairs, each one
/// property.
///
/// The eighth is the one a previous revision of this case could not see,
/// because none of its pairs varied the service: an implementation that
/// strips `service.name` before hashing passes the first seven and fails it.
/// It asserts a **read on one id** rather than a count over `resources`,
/// because that table's key is `(service, resource_id)` and a shared id
/// still produces two rows.
#[tokio::test]
async fn the_resource_id_is_128_bits_and_stable() {
    skip_unless_live!();

    let resource = |pairs: Vec<KeyValue>| Resource {
        attributes: pairs,
        dropped_attributes_count: 0,
        entity_refs: Vec::new(),
    };
    let id = |pairs: Vec<KeyValue>, schema_url: &str| {
        resource_identity(Some(&resource(pairs)), schema_url)
    };

    let base = || {
        vec![
            kv("service.name", str_value("checkout")),
            kv("host.name", str_value("node-a")),
            kv("k", int_value(1)),
        ]
    };

    // 1. Byte-identical resources give the same id.
    assert_eq!(id(base(), ""), id(base(), ""), "identical resources");

    // 2. The same pairs in a different order give the same id.
    let reordered = vec![
        kv("k", int_value(1)),
        kv("service.name", str_value("checkout")),
        kv("host.name", str_value("node-a")),
    ];
    assert_eq!(
        id(base(), ""),
        id(reordered, ""),
        "the identity does not depend on arrival order"
    );

    // 3. One changed value gives a different id.
    let mut changed_value = base();
    changed_value[1] = kv("host.name", str_value("node-b"));
    assert_ne!(id(base(), ""), id(changed_value, ""), "a changed value");

    // 4. One changed key gives a different id.
    let mut changed_key = base();
    changed_key[1] = kv("host.id", str_value("node-a"));
    assert_ne!(id(base(), ""), id(changed_key, ""), "a changed key");

    // 5. A different schema url gives a different id.
    assert_ne!(
        id(base(), ""),
        id(base(), "https://example.invalid/v1"),
        "the schema url is part of the identity"
    );

    // 6. An integer `1` and the string `"1"` are different resources.
    let mut as_text = base();
    as_text[2] = kv("k", str_value("1"));
    assert_ne!(
        id(base(), ""),
        id(as_text, ""),
        "the value is type-tagged, so 1 and \"1\" differ"
    );

    // 7. A key present with an empty value differs from an absent key.
    let mut empty_value = base();
    empty_value[2] = kv("k", str_value(""));
    let absent = vec![
        kv("service.name", str_value("checkout")),
        kv("host.name", str_value("node-a")),
    ];
    assert_ne!(
        id(empty_value, ""),
        id(absent, ""),
        "a present key with an empty value is not an absent key"
    );

    // 8. Two resources differing only in `service.name` give different ids,
    // and the read on one id answers one row.
    let mut other_service = base();
    other_service[0] = kv("service.name", str_value("pay"));
    let first = id(base(), "");
    assert_ne!(
        first,
        id(other_service.clone(), ""),
        "service.name is in the hash"
    );

    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_rows_it_resid")).await;
    let req = ExportTraceServiceRequest {
        resource_spans: vec![
            ResourceSpans {
                resource: Some(resource(base())),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![span_with(0x71, 0x71, Vec::new())],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            },
            ResourceSpans {
                resource: Some(resource(other_service)),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![span_with(0x72, 0x72, Vec::new())],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            },
        ],
    };
    land(&client, &req).await;

    assert_eq!(
        count(
            &client,
            &format!(
                "SELECT count() AS n FROM resources FINAL WHERE resource_id = {}",
                first.sql_literal()
            )
        )
        .await,
        1,
        "a read on one id answers one row: a shared id would answer two, \
         because this table's key is (service, resource_id)"
    );
    assert_eq!(
        count(&client, "SELECT count() AS n FROM resources FINAL").await,
        2,
        "two services, two resource rows — which is 2 either way, and is why \
         the assertion above reads one id rather than counting the table"
    );

    // The stored column type is the one the design names.
    assert_eq!(
        scalar(
            &client,
            &format!(
                "SELECT type AS s FROM system.columns \
                 WHERE database = '{db}' AND table = 'spans' AND name = 'resource_id'"
            )
        )
        .await,
        "UInt128"
    );

    drop_db(&db).await;
}

/// **Two resources differing only in a nested value's type stay two rows.**
///
/// `resources` is a `ReplacingMergeTree` keyed on `(service, resource_id)`,
/// so this is the read that sees a shared identity: both resources carry one
/// service, and an identity that tags only the outermost value gives them one
/// id, the key collapses them under `FINAL`, and one resource's attributes
/// are gone while the spans that carried its id join to the survivor.
///
/// The pair is a bytes value against the string of its own base64 text,
/// nested two deep — `base64("x")` is `eA==`. One of the two stores the path
/// `k.n` and the other carries its value in `attrs_other`, so the collapse
/// also loses a stored attribute rather than a duplicate of one.
///
/// Every assertion here is a read: the identities themselves are held by
/// `nested_values_are_type_tagged_at_every_depth` in `otlp_traces`, and an
/// identity assertion in front of these would fire first and leave the reads
/// unexercised.
#[tokio::test]
async fn two_resources_differing_in_a_nested_type_stay_two_rows() {
    skip_unless_live!();

    let resource = |value: AnyValue| Resource {
        attributes: vec![kv("service.name", str_value("checkout")), kv("k", value)],
        dropped_attributes_count: 0,
        entity_refs: Vec::new(),
    };
    let nested = |leaf: AnyValue| kvlist_value(vec![("n", array_value(vec![leaf]))]);
    let opaque = resource(nested(bytes_value(b"x")));
    let text = resource(nested(str_value("eA==")));

    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_rows_it_nested_id")).await;
    let req = ExportTraceServiceRequest {
        resource_spans: vec![
            ResourceSpans {
                resource: Some(opaque),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![span_with(0x73, 0x73, Vec::new())],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            },
            ResourceSpans {
                resource: Some(text),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![span_with(0x74, 0x74, Vec::new())],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            },
        ],
    };
    land(&client, &req).await;

    assert_eq!(
        count(&client, "SELECT count() AS n FROM resources FINAL").await,
        2,
        "one service and two identities: a shared identity is one row here, \
         because this table's key is (service, resource_id)"
    );
    assert_eq!(
        count(
            &client,
            "SELECT count() AS n FROM resources FINAL \
             WHERE dynamicType(attrs.k.n) != 'None'"
        )
        .await,
        1,
        "the string-valued resource stores the path"
    );
    assert_eq!(
        count(
            &client,
            "SELECT count() AS n FROM resources FINAL WHERE attrs_other != ''"
        )
        .await,
        1,
        "and the bytes-valued one carries its value in the side channel"
    );

    drop_db(&db).await;
}

/// **T-S2.** The service name is a column, not a stored resource attribute.
#[tokio::test]
async fn t_s2_the_service_name_is_not_in_the_resource_attributes() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_rows_it_s2")).await;

    land(
        &client,
        &request("checkout", vec![span_with(0x81, 0x81, Vec::new())]),
    )
    .await;

    assert_eq!(
        count(
            &client,
            "SELECT count() AS n FROM resources \
             WHERE dynamicType(attrs.`service%2Ename`) != 'None'"
        )
        .await,
        0,
        "the stored resource attributes omit service.name"
    );
    assert_eq!(
        scalar(&client, "SELECT toString(service) AS s FROM spans").await,
        "checkout",
        "the span row carries it as a column"
    );

    drop_db(&db).await;
}

/// **T-T9.** The fifth attribute scope — the instrumentation scope's own
/// attributes — reaches both catalogs.
#[tokio::test]
async fn t_t9_the_fifth_scope_reaches_both_catalogs() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_rows_it_t9")).await;

    land(
        &client,
        &request_with(
            "checkout",
            Vec::new(),
            vec![span_with(0x91, 0x91, Vec::new())],
            Some(InstrumentationScope {
                name: "io.otel.http".to_string(),
                version: "1.4.2".to_string(),
                attributes: vec![kv("otel.scope.build", str_value("release"))],
                dropped_attributes_count: 0,
            }),
            "",
        ),
    )
    .await;

    assert_eq!(
        count(
            &client,
            "SELECT count() AS n FROM tag_names FINAL \
             WHERE scope = 'instrumentation' AND key = 'otel.scope.build'"
        )
        .await,
        1,
        "the name reaches tag_names under the instrumentation scope"
    );
    assert_eq!(
        count(
            &client,
            "SELECT count() AS n FROM tag_values FINAL \
             WHERE scope = 'instrumentation' AND key = 'otel.scope.build' \
               AND value = 'release' AND val_type = 'string'"
        )
        .await,
        1,
        "and the value reaches tag_values with its declared type"
    );
    assert_eq!(
        scalar(
            &client,
            "SELECT assumeNotNull(scope_attrs.`otel%2Escope%2Ebuild`.:String) AS s FROM spans"
        )
        .await,
        "release",
        "and the span row stores it under its escaped path"
    );

    drop_db(&db).await;
}

/// **T-W7, the column half.** A span carrying every field, one event, one
/// link and every value type reads back equal as values.
///
/// **Its precondition is the fetch, which is a later task**, so this case
/// reads the columns with a literal `SELECT` and compares the decoded values
/// rather than an HTTP response.
#[tokio::test]
async fn t_w7_a_span_with_every_field_reads_back_equal() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_rows_it_w7")).await;

    let start = now_ns();
    let span = Span {
        trace_id: vec![0xab; 16],
        span_id: vec![0xcd; 8],
        parent_span_id: vec![0xef; 8],
        trace_state: "rojo=00f067aa0ba902b7".to_string(),
        flags: 0x301,
        name: "GET /api".to_string(),
        kind: 3,
        start_time_unix_nano: start as u64,
        end_time_unix_nano: start as u64 + 4_000_000,
        attributes: vec![
            kv("s", str_value("text")),
            kv("b", bool_value(false)),
            kv("i", int_value(-7)),
            kv("f", double_value(2.5)),
            kv("arr", array_value(vec![int_value(3), int_value(4)])),
        ],
        dropped_attributes_count: 2,
        events: vec![span::Event {
            time_unix_nano: start as u64 + 1_000_000,
            name: "exception".to_string(),
            attributes: vec![kv("exception.type", str_value("IOError"))],
            dropped_attributes_count: 1,
        }],
        dropped_events_count: 3,
        links: vec![span::Link {
            trace_id: vec![0x11; 16],
            span_id: vec![0x22; 8],
            trace_state: "congo=t61rcWkgMzE".to_string(),
            attributes: vec![kv("link.kind", str_value("follows"))],
            dropped_attributes_count: 4,
            flags: 0x100,
        }],
        dropped_links_count: 5,
        status: Some(Status {
            message: "it broke".to_string(),
            code: 2,
        }),
    };
    // **W1.** The insert's own `Result` is the first assertion, because this
    // is the only case here that exercises the **approved** column types
    // against a server — the table comes from the catalogue through
    // `run_init`, so this is where `Display`'s compact form is checked against
    // the server's own `getName()` byte for byte. Nothing unwraps.
    let r = try_land(&client, &request("checkout", vec![span])).await;
    assert!(r.is_ok(), "the landing insert returned {:?}", r.err());

    // The scalar columns, as one tab-separated row so one statement covers
    // them and the comparison names what differed.
    let got = scalar(
        &client,
        "SELECT concat(\
            hex(trace_id), '|', hex(span_id), '|', hex(parent_span_id), '|', \
            toString(duration_ns), '|', toString(service), '|', toString(name), '|', \
            toString(kind), '|', toString(status_code), '|', status_message, '|', \
            trace_state, '|', toString(flags), '|', toString(scope_name), '|', \
            toString(scope_version), '|', toString(dropped_attrs), '|', \
            toString(dropped_events), '|', toString(dropped_links)\
         ) AS s FROM spans",
    )
    .await;
    assert_eq!(
        got,
        format!(
            "{}|{}|{}|4000000|checkout|GET /api|3|2|it broke|rojo=00f067aa0ba902b7|769|||2|3|5",
            "AB".repeat(16),
            "CD".repeat(8),
            "EF".repeat(8)
        ),
        "every scalar column reads back what the span carried"
    );

    // The event and the link tuples.
    assert_eq!(
        scalar(
            &client,
            "SELECT concat(\
                toString(events[1].2), '|', toString(events[1].4), '|', \
                toString(events[1].3.`exception%2Etype`), '|', \
                dynamicType(events[1].3.`exception%2Etype`)\
             ) AS s FROM spans"
        )
        .await,
        "exception|1|IOError|String",
        "the event's name, its dropped count and its own attribute"
    );
    assert_eq!(
        scalar(
            &client,
            "SELECT concat(\
                hex(links[1].1), '|', hex(links[1].2), '|', links[1].3, '|', \
                toString(links[1].4), '|', toString(links[1].6), '|', \
                toString(links[1].5.`link%2Ekind`), '|', \
                dynamicType(links[1].5.`link%2Ekind`)\
             ) AS s FROM spans"
        )
        .await,
        format!(
            "{}|{}|congo=t61rcWkgMzE|256|4|follows|String",
            "11".repeat(16),
            "22".repeat(8)
        ),
        "the link's ids, its trace state, its flags, its dropped count and \
         its own attribute"
    );
    assert_eq!(
        scalar(
            &client,
            "SELECT toString(events[1].1 = toInt64(start_ns + 1000000)) AS s FROM spans"
        )
        .await,
        "1",
        "the event's own time"
    );

    // The five attribute values.
    assert_eq!(
        scalar(
            &client,
            "SELECT concat(\
                assumeNotNull(attrs.`s`.:String), '|', \
                toString(assumeNotNull(attrs.`b`.:Bool)), '|', \
                toString(assumeNotNull(attrs.`i`.:Int64)), '|', \
                toString(assumeNotNull(attrs.`f`.:Float64)), '|', \
                toString(assumeNotNull(attrs.`arr`.:`Array(Nullable(Int64))`))\
             ) AS s FROM spans"
        )
        .await,
        "text|false|-7|2.5|[3,4]"
    );

    drop_db(&db).await;
}

/// **W2.** One push, one span carrying **two** events and **two** links: both
/// of each land, in declaration order.
///
/// Its mechanism is the validator's tuple cursor. `InnerDataTypeValidatorKind
/// ::Tuple` is a slice consumed by `split_first`, so the **second** array
/// element must be given a fresh one; a shared cursor would validate the
/// second event's first field against `LowCardinality(String)` and the server
/// would never see the block. One array element cannot see that.
#[tokio::test]
async fn two_events_and_two_links_in_one_span_all_land() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_rows_it_w2")).await;

    let start = now_ns();
    let span = Span {
        trace_id: vec![0x5a; 16],
        span_id: vec![0x6b; 8],
        parent_span_id: Vec::new(),
        trace_state: String::new(),
        flags: 0,
        name: "two of each".to_string(),
        kind: 2,
        start_time_unix_nano: start as u64,
        end_time_unix_nano: start as u64 + 2_000_000,
        attributes: Vec::new(),
        dropped_attributes_count: 0,
        events: vec![
            span::Event {
                time_unix_nano: start as u64 + 100_000,
                name: "first".to_string(),
                attributes: vec![kv("e", str_value("one"))],
                dropped_attributes_count: 1,
            },
            span::Event {
                time_unix_nano: start as u64 + 200_000,
                name: "second".to_string(),
                attributes: vec![kv("e", str_value("two"))],
                dropped_attributes_count: 2,
            },
        ],
        dropped_events_count: 0,
        links: vec![
            span::Link {
                trace_id: vec![0x11; 16],
                span_id: vec![0x21; 8],
                trace_state: "a=1".to_string(),
                attributes: vec![kv("l", str_value("one"))],
                dropped_attributes_count: 3,
                flags: 1,
            },
            span::Link {
                trace_id: vec![0x12; 16],
                span_id: vec![0x22; 8],
                trace_state: "a=2".to_string(),
                attributes: vec![kv("l", str_value("two"))],
                dropped_attributes_count: 4,
                flags: 2,
            },
        ],
        dropped_links_count: 0,
        status: None,
    };

    let r = try_land(&client, &request("checkout", vec![span])).await;
    assert!(r.is_ok(), "the landing insert returned {:?}", r.err());

    assert_eq!(
        scalar(
            &client,
            "SELECT concat(\
                toString(length(events)), '|', \
                toString(events[1].2), '|', toString(events[2].2), '|', \
                toString(events[1].4), '|', toString(events[2].4), '|', \
                toString(events[1].3.`e`), '|', toString(events[2].3.`e`)\
             ) AS s FROM spans"
        )
        .await,
        "2|first|second|1|2|one|two",
        "both events land, in declaration order, with their own attributes"
    );
    assert_eq!(
        scalar(
            &client,
            "SELECT concat(\
                toString(length(links)), '|', \
                hex(links[1].1), '|', hex(links[2].1), '|', \
                links[1].3, '|', links[2].3, '|', \
                toString(links[1].6), '|', toString(links[2].6), '|', \
                toString(links[1].5.`l`), '|', toString(links[2].5.`l`)\
             ) AS s FROM spans"
        )
        .await,
        format!(
            "2|{}|{}|a=1|a=2|3|4|one|two",
            "11".repeat(16),
            "12".repeat(16)
        ),
        "both links land, keyed by their own trace ids, with their own \
         attributes"
    );

    drop_db(&db).await;
}

/// **The insert omits `event_id`, so the server fills it.** Thirty-one
/// columns in the insert's column list and `event_id` is not one of them;
/// live, every landed row's own identity is non-zero and distinct.
#[tokio::test]
async fn the_insert_omits_event_id_so_the_server_fills_it() {
    skip_unless_live!();
    // The hermetic half first, so the live half is not the only thing
    // holding the column list.
    let names = <TraceLandingRow as clickhouse::Row>::COLUMN_NAMES;
    assert_eq!(
        names.len(),
        31,
        "the table has 32 columns and the row type declares the other 31: {names:?}"
    );
    assert!(
        !names.contains(&"event_id"),
        "the landed event's own identity is the server's to fill: {names:?}"
    );

    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_rows_it_eventid")).await;
    land(
        &client,
        &request(
            "checkout",
            vec![
                span_with(0x61, 0x11, vec![kv("k", int_value(1))]),
                span_with(0x62, 0x12, vec![kv("k", int_value(2))]),
            ],
        ),
    )
    .await;

    let landed = count(&client, "SELECT count() AS n FROM trace_landing").await;
    assert!(landed >= 2, "the push landed its rows: {landed}");
    assert_eq!(
        count(
            &client,
            "SELECT count() AS n FROM trace_landing \
             WHERE event_id = toUUID('00000000-0000-0000-0000-000000000000')"
        )
        .await,
        0,
        "no landed row carries the zero identity"
    );
    assert_eq!(
        count(
            &client,
            "SELECT uniqExact(event_id) AS n FROM trace_landing"
        )
        .await,
        landed,
        "every landed row's identity is its own"
    );

    drop_db(&db).await;
}

/// A key whose value no JSON path can hold is listed in `tag_names` and has
/// no value in `tag_values`, and a composite key's leaf paths each get their
/// own name row.
///
/// That is the disagreement `docs/api.md` §4.3's two halves carry for a
/// composite key: the name is listed, the value is not.
#[tokio::test]
async fn a_composite_key_is_named_and_its_value_is_not() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_rows_it_composite")).await;

    land(
        &client,
        &request(
            "checkout",
            vec![span_with(
                0x51,
                0x51,
                vec![
                    kv(
                        "composite",
                        kvlist_value(vec![("a", kvlist_value(vec![("b", int_value(1))]))]),
                    ),
                    kv("opaque", bytes_value(&[1, 2])),
                    kv("plain", str_value("v")),
                ],
            )],
        ),
    )
    .await;

    let named = texts(
        &client,
        "SELECT key AS s FROM tag_names FINAL WHERE scope = 'span' ORDER BY key",
    )
    .await;
    assert_eq!(
        named,
        vec![
            "composite".to_string(),
            "composite.a.b".to_string(),
            "opaque".to_string(),
            "plain".to_string(),
        ],
        "every key the push carried is named, the composite's leaf included"
    );

    let valued = texts(
        &client,
        "SELECT key AS s FROM tag_values FINAL WHERE scope = 'span' ORDER BY key",
    )
    .await;
    assert_eq!(
        valued,
        vec!["plain".to_string()],
        "only the scalar has a value row: a kvlist has no scalar text and no \
         declared type, and neither does a value in attrs_other"
    );

    drop_db(&db).await;
}

/// A scalar array contributes one `tag_values` row **per element**, each with
/// the element's own declared type; a mixed array contributes none.
#[tokio::test]
async fn a_scalar_array_contributes_one_value_row_per_element() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_rows_it_arrvalues")).await;

    land(
        &client,
        &request(
            "checkout",
            vec![span_with(
                0x41,
                0x41,
                vec![
                    kv("tags", array_value(vec![str_value("x"), str_value("y")])),
                    kv("mixed", array_value(vec![str_value("x"), int_value(1)])),
                ],
            )],
        ),
    )
    .await;

    let rows = texts(
        &client,
        "SELECT concat(key, '=', value, ':', val_type) AS s FROM tag_values FINAL \
         WHERE scope = 'span' ORDER BY key, value",
    )
    .await;
    assert_eq!(
        rows,
        vec!["tags=x:string".to_string(), "tags=y:string".to_string()],
        "one row per element of the scalar array, and none for the mixed one"
    );

    drop_db(&db).await;
}

// === The write path on the five targets (issue #586) ======================

/// A span of `trace`/`id` at `start_ns` with `duration_ns`, in `service`,
/// optionally rooted.
#[allow(clippy::too_many_arguments)]
fn span_full(
    trace: &[u8; 16],
    id: u8,
    parent: Option<u8>,
    name: &str,
    kind: i32,
    start_ns: i64,
    duration_ns: i64,
    attrs: Vec<KeyValue>,
) -> Span {
    Span {
        trace_id: trace.to_vec(),
        span_id: vec![id; 8],
        parent_span_id: parent.map(|p| vec![p; 8]).unwrap_or_default(),
        trace_state: String::new(),
        flags: 0,
        name: name.to_string(),
        kind,
        start_time_unix_nano: u64::try_from(start_ns).expect("a post-epoch fixture"),
        end_time_unix_nano: u64::try_from(start_ns + duration_ns).expect("a post-epoch fixture"),
        attributes: attrs,
        dropped_attributes_count: 0,
        events: Vec::new(),
        dropped_events_count: 0,
        links: Vec::new(),
        dropped_links_count: 0,
        status: None,
    }
}

fn trace_id_of(seed: u8) -> [u8; 16] {
    [seed; 16]
}

/// One request carrying several resources, so a case that needs more than
/// one service in **one** push — and therefore one `received_ms` — can build
/// it.
fn request_of_services(groups: Vec<(&str, Vec<Span>)>) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: groups
            .into_iter()
            .map(|(service, spans)| ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", str_value(service))],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            })
            .collect(),
    }
}

/// Detaches or re-attaches the five views, so a case can land a row into the
/// landing table **without** the targets being written — which is the only
/// way to observe a row a replay has not yet carried over, since a committed
/// landing insert otherwise fans out as part of its own processing.
async fn set_views_attached(client: &ChClient, attached: bool) {
    let verb = if attached { "ATTACH" } else { "DETACH" };
    for mv in [
        "spans_mv",
        "resources_mv",
        "traces_mv",
        "tag_names_mv",
        "tag_values_mv",
    ] {
        client
            .execute(
                &format!("{verb} TABLE {mv}"),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .unwrap_or_else(|e| panic!("{verb} {mv}: {e}"));
    }
}

/// **`T-W1`.** The fixture's six bodies with one sent twice, in **one**
/// push: the target's `ReplacingMergeTree` key collapses the repeat, and
/// the landing table holds both copies — which is what shows the collapse is
/// the target's doing and not a writer filter.
#[tokio::test]
async fn t_w1_a_repeated_body_in_one_block_collapses_on_the_target_key() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_landing_it_w1")).await;

    let t0 = now_ns();
    // Six bodies, nine spans: three carry two spans and three carry one.
    let mut spans = Vec::new();
    for (i, n) in [2u8, 2, 2, 1, 1, 1].into_iter().enumerate() {
        let trace = trace_id_of(0xb0 + i as u8);
        for j in 0..n {
            spans.push(span_full(
                &trace,
                0x10 + j,
                None,
                "GET /api",
                2,
                t0 + (i as i64) * 1_000_000 + i64::from(j),
                1_000,
                vec![kv("http.route", str_value("/api"))],
            ));
        }
    }
    // The first body again, in the same push.
    let repeated = trace_id_of(0xb0);
    for j in 0..2u8 {
        spans.push(span_full(
            &repeated,
            0x10 + j,
            None,
            "GET /api",
            2,
            t0 + i64::from(j),
            1_000,
            vec![kv("http.route", str_value("/api"))],
        ));
    }
    assert_eq!(spans.len(), 11, "nine spans plus one body's two again");
    land(&client, &request("checkout", spans)).await;

    assert_eq!(
        count(&client, "SELECT count() AS n FROM spans FINAL").await,
        9,
        "the repeated body's two spans collapse on the ReplacingMergeTree key"
    );
    assert_eq!(
        count(
            &client,
            "SELECT count() AS n FROM trace_landing WHERE row_kind = 0"
        )
        .await,
        11,
        "and the landing table holds both copies: the collapse is the \
         target's key, not a writer filter"
    );
    // Recorded, not asserted: it is 9 only while `optimize_on_insert` is 1.
    let unmerged = count(&client, "SELECT count() AS n FROM spans").await;
    eprintln!("T-W1: SELECT count() FROM spans (recorded) = {unmerged}");

    drop_db(&db).await;
}

/// **`T-W2`.** The same body in two separate pushes, outside any
/// suppression window — the landing inserts here carry no claim at all —
/// collapses to nine spans before any merge.
#[tokio::test]
async fn t_w2_the_same_body_in_two_pushes_collapses_before_any_merge() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_landing_it_w2")).await;

    let t0 = now_ns();
    let mut spans = Vec::new();
    for (i, n) in [2u8, 2, 2, 1, 1, 1].into_iter().enumerate() {
        let trace = trace_id_of(0xc0 + i as u8);
        for j in 0..n {
            spans.push(span_full(
                &trace,
                0x20 + j,
                None,
                "GET /api",
                2,
                t0 + (i as i64) * 1_000_000 + i64::from(j),
                1_000,
                vec![kv("http.route", str_value("/api"))],
            ));
        }
    }
    let body = request("checkout", spans);
    land(&client, &body).await;
    land(&client, &body).await;

    assert_eq!(
        count(&client, "SELECT count() AS n FROM spans FINAL").await,
        9,
        "two pushes of the same nine spans are nine spans"
    );

    drop_db(&db).await;
}

/// **`T-W3`.** One span id arriving with two OTLP kinds — a shared span,
/// both RPC halves under one id — is two rows after `FINAL`, because `kind`
/// is in the sorting key.
#[tokio::test]
async fn t_w3_one_span_id_with_two_kinds_is_two_rows() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_landing_it_w3")).await;

    let t0 = now_ns();
    let trace = trace_id_of(0xd0);
    land(
        &client,
        &request(
            "checkout",
            vec![
                span_full(&trace, 0x31, None, "GET /api", 2, t0, 1_000, vec![]),
                span_full(&trace, 0x31, None, "GET /api", 3, t0, 1_000, vec![]),
            ],
        ),
    )
    .await;

    assert_eq!(
        count(&client, "SELECT count() AS n FROM spans FINAL").await,
        2,
        "a shared span keeps both halves: `kind` is in the sorting key"
    );

    drop_db(&db).await;
}

/// **`T-W5`.** A thousand spans of one resource in one day land **one**
/// kind-1 row, not a thousand collapsed by the target. The second half is
/// the load-bearing one: a writer emitting one resource row per span passes
/// the first and fails this.
#[tokio::test]
async fn t_w5_a_thousand_spans_of_one_resource_land_one_resource_row() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_landing_it_w5")).await;

    let t0 = (now_ns() / 86_400_000_000_000) * 86_400_000_000_000 + 3_600_000_000_000;
    let trace = trace_id_of(0xe0);
    let spans: Vec<Span> = (0..1_000i64)
        .map(|i| {
            span_full(
                &trace,
                (i % 251) as u8 + 1,
                None,
                "GET /api",
                2,
                t0 + i,
                1_000,
                vec![],
            )
        })
        .collect();
    land(&client, &request("checkout", spans)).await;

    assert_eq!(
        count(&client, "SELECT count() AS n FROM resources FINAL").await,
        1
    );
    assert_eq!(
        count(
            &client,
            "SELECT count() AS n FROM trace_landing WHERE row_kind = 1"
        )
        .await,
        1,
        "the writer emits one resource row per distinct (resource, day) in \
         the push, and does not lean on the target to collapse a thousand"
    );

    drop_db(&db).await;
}

/// **A second push re-emits the same resource and catalog rows**, and this
/// is the case a cross-push cache fails: any per-process set of
/// already-written keys makes the second push emit none of them, and then a
/// fan-out failure on the first push leaves the entries missing for good.
#[tokio::test]
async fn a_second_push_re_emits_the_same_resource_and_catalog_rows() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_landing_it_reemit")).await;

    let t0 = now_ns();
    let trace = trace_id_of(0xf0);
    let body = request(
        "checkout",
        vec![span_full(
            &trace,
            0x41,
            None,
            "GET /api",
            2,
            t0,
            1_000,
            vec![kv("http.route", str_value("/api"))],
        )],
    );
    let parsed = land(&client, &body).await;
    let single = count(
        &client,
        "SELECT count() AS n FROM trace_landing WHERE row_kind IN (1, 2, 3)",
    )
    .await;
    assert!(single > 0, "the push lands catalog and resource rows");
    let names = count(&client, "SELECT count() AS n FROM tag_names FINAL").await;
    let values = count(&client, "SELECT count() AS n FROM tag_values FINAL").await;
    let resources = count(&client, "SELECT count() AS n FROM resources FINAL").await;

    land(&client, &body).await;
    assert_eq!(
        count(
            &client,
            "SELECT count() AS n FROM trace_landing WHERE row_kind IN (1, 2, 3)"
        )
        .await,
        single * 2,
        "the second push re-emits every kind-1, kind-2 and kind-3 row: there \
         is no cache of anything already written anywhere on this path"
    );
    assert_eq!(
        count(&client, "SELECT count() AS n FROM tag_names FINAL").await,
        names,
        "and the targets collapse the repeats on their own keys"
    );
    assert_eq!(
        count(&client, "SELECT count() AS n FROM tag_values FINAL").await,
        values
    );
    assert_eq!(
        count(&client, "SELECT count() AS n FROM resources FINAL").await,
        resources
    );
    assert_eq!(parsed.resources.len(), 1);

    drop_db(&db).await;
}

/// **`T-W6`.** A view pointed at a table with an incompatible column fails
/// the insert, and the throwing view's own target holds nothing.
///
/// **Three assertions and no fourth.** The healthy siblings' counts are
/// recorded and asserted against nothing: a measurement over 300 trials of
/// one throwing view and three healthy siblings on one source table found
/// the siblings committing in 28, 25 and 32 of them, so a fourth assertion
/// that `spans` is empty would fail about a tenth of the time — and it would
/// state a guarantee this design does not make.
#[tokio::test]
async fn t_w6_a_throwing_view_fails_the_insert_and_its_target_holds_nothing() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_landing_it_w6")).await;

    // Point `traces_mv` at a table whose `trace_id` cannot take the
    // projection's `FixedString(16)`.
    for sql in [
        "DROP VIEW IF EXISTS traces_mv",
        "CREATE TABLE traces_broken (day Date, trace_id Int8, start_ns Int64, end_ns Int64, \
         root_service String, root_name String, services Array(String)) \
         ENGINE = MergeTree ORDER BY trace_id",
        "CREATE MATERIALIZED VIEW traces_mv TO traces_broken AS \
         SELECT toDate(fromUnixTimestamp64Nano(s)) AS day, trace_id, s AS start_ns, e AS end_ns, \
                rs AS root_service, rn AS root_name, sv AS services \
         FROM (SELECT trace_id, min(start_ns) AS s, max(start_ns + duration_ns) AS e, \
                      maxIf(service, parent_span_id = toFixedString('', 8)) AS rs, \
                      maxIf(name, parent_span_id = toFixedString('', 8)) AS rn, \
                      groupUniqArray(toString(service)) AS sv \
               FROM trace_landing WHERE row_kind = 0 GROUP BY trace_id)",
    ] {
        client
            .execute(sql, &QuerySettings::new(), Idempotency::NonIdempotent)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    let t0 = now_ns();
    let trace = trace_id_of(0x5a);
    let err = try_land(
        &client,
        &request(
            "checkout",
            vec![span_full(
                &trace,
                0x51,
                None,
                "GET /api",
                2,
                t0,
                1_000,
                vec![kv("http.route", str_value("/api"))],
            )],
        ),
    )
    .await
    .expect_err("a view's exception fails the insert");
    eprintln!("T-W6: the insert failed with {err}");

    assert_eq!(
        count(&client, "SELECT count() AS n FROM traces").await,
        0,
        "the throwing view's own target holds nothing"
    );
    for table in ["spans", "resources", "tag_names", "tag_values"] {
        let n = count(&client, &format!("SELECT count() AS n FROM {table}")).await;
        eprintln!("T-W6: {table} held {n} rows (recorded, asserted against nothing)");
    }

    drop_db(&db).await;
}

/// **A profiling string reference lands nowhere** (the live half): not in
/// either attribute column, not in either catalog, in any of the five
/// scopes.
#[tokio::test]
async fn a_profiling_string_reference_lands_nowhere() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_landing_it_strindex")).await;

    let strindex = AnyValue {
        value: Some(any_value::Value::StringValueStrindex(7)),
    };
    let scope = InstrumentationScope {
        name: "io.otel.http".to_string(),
        version: String::new(),
        attributes: vec![kv("s1", strindex.clone())],
        dropped_attributes_count: 0,
    };
    let span = span_with(
        0x6a,
        0x61,
        vec![
            kv("k1", strindex.clone()),
            kv(
                "k2",
                array_value(vec![str_value("a"), strindex.clone(), str_value("b")]),
            ),
            kv("k3", array_value(vec![strindex.clone()])),
            kv("k4", bytes_value(b"x")),
        ],
    );
    land(
        &client,
        &request_with("checkout", Vec::new(), vec![span], Some(scope), ""),
    )
    .await;

    assert_eq!(
        scalar(&client, "SELECT dynamicType(attrs.`k1`) AS s FROM spans").await,
        "None",
        "no path is stored for the profiling reference"
    );
    assert_eq!(
        count(
            &client,
            "SELECT count() AS n FROM tag_names FINAL WHERE key IN ('k1', 's1')"
        )
        .await,
        0,
        "and neither catalog lists the key, in either scope"
    );
    assert_eq!(
        count(
            &client,
            "SELECT toUInt64(length(attrs.`k2`.:`Array(Nullable(String))`)) AS n FROM spans"
        )
        .await,
        2,
        "the element of that arm is dropped and the rest land"
    );
    assert_eq!(
        count(
            &client,
            "SELECT toUInt64(has(attrs.`k2`.:`Array(Nullable(String))`, 'b')) AS n FROM spans"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &client,
            "SELECT toUInt64(length(attrs.`k3`.:`Array(Nullable(String))`)) AS n FROM spans"
        )
        .await,
        0,
        "an array whose every element is that arm lands as the empty array"
    );

    let other = blob(&client, "SELECT attrs_other AS b FROM spans").await;
    let decoded = KeyValueList::decode(other.as_slice()).expect("a KeyValueList");
    let keys: Vec<&str> = decoded.values.iter().map(|kv| kv.key.as_str()).collect();
    assert_eq!(
        keys,
        vec!["k4"],
        "the bytes value is still carried and the index is not"
    );

    drop_db(&db).await;
}

// === `rebuild-traces`: replaying a landing window (issue #586) ============
//
// Every case here drives `pulsus_schema::replay_trace_window`, which is the
// engine `pulsusdb rebuild-traces` wraps: the command parses its two RFC3339
// arguments into the epoch milliseconds this function takes, and does nothing
// else. The window is half-open — `received_ms >= from AND received_ms < to`
// — so a one-millisecond window selects one stamp.

/// The replay's own row ceiling, the value both pinned row limits carry on
/// every statement it sends.
const REPLAY_MAX_ROWS: u64 = 1_048_576;

async fn replay(
    client: &ChClient,
    db: &str,
    from_ms: i64,
    to_ms: i64,
) -> pulsus_schema::ReplayReport {
    let ctx: SchemaParams = RenderCtx::for_tests(db);
    pulsus_schema::replay_trace_window(client, &ctx, from_ms, to_ms, REPLAY_MAX_ROWS)
        .await
        .unwrap_or_else(|e| panic!("the replay failed: {e}"))
}

async fn received_ms_bounds(client: &ChClient) -> (i64, i64) {
    let text = scalar(
        client,
        "SELECT concat(toString(min(received_ms)), ':', toString(max(received_ms))) AS s \
         FROM trace_landing",
    )
    .await;
    let (lo, hi) = text.split_once(':').expect("two bounds");
    (lo.parse().expect("a stamp"), hi.parse().expect("a stamp"))
}

async fn truncate_targets(client: &ChClient) {
    for table in ["spans", "traces", "resources", "tag_names", "tag_values"] {
        client
            .execute(
                &format!("TRUNCATE TABLE {table}"),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .unwrap_or_else(|e| panic!("truncating {table}: {e}"));
    }
}

/// **The repair replays one target partition per statement.** The shape
/// assertion is the whole case: a repair emitting one statement for the
/// window, or one spanning two partitions, fails on the count and on the
/// predicate.
///
/// There is deliberately **no hostile `max_partitions_per_insert_block`** in
/// this setup: every repair statement carries it at the plan's constant
/// through the shared constructor, and a per-query value overrides a
/// profile, so a lowered partition-count limit could not discriminate
/// anything here. That is a statement about that one setting and not about
/// limits — two cases below do give the repair a hostile profile.
#[tokio::test]
async fn the_repair_replays_one_partition_per_statement() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db(
        "pulsus_trace_landing_it_partitions",
    ))
    .await;

    // Five distinct UTC dates, one trace per date so `traces` has five
    // partitions too, and one resource so `resources` has one row per day.
    let day = 86_400_000_000_000i64;
    let today = (now_ns() / day) * day + 3_600_000_000_000;
    let mut spans = Vec::new();
    for d in 0..5i64 {
        let trace = trace_id_of(0x70 + d as u8);
        for j in 0..2u8 {
            spans.push(span_full(
                &trace,
                0x81 + j,
                None,
                "GET /api",
                2,
                today - d * day + i64::from(j),
                1_000,
                vec![kv("http.route", str_value("/api"))],
            ));
        }
    }
    land(&client, &request("checkout", spans)).await;
    let before = (
        count(&client, "SELECT count() AS n FROM spans FINAL").await,
        count(&client, "SELECT count() AS n FROM traces FINAL").await,
        count(
            &client,
            "SELECT toUInt64(uniqExact(day)) AS n FROM resources",
        )
        .await,
    );
    // **`resources` is read by its day count and without `FINAL`**: its
    // sorting key is `(service, resource_id)` and `day` is only its
    // partition key, so `FINAL` collapses one resource's five day-rows into
    // one.
    assert_eq!(before, (10, 5, 5), "the fixture spans five UTC dates");
    truncate_targets(&client).await;

    let (lo, hi) = received_ms_bounds(&client).await;
    let report = replay(&client, &db, lo, hi + 1).await;
    let inserts = report.inserts();

    let per_target = |target: &str| {
        inserts
            .iter()
            .filter(|s| s.contains(&format!("INSERT INTO {db}.{target} ")))
            .count()
    };
    assert_eq!(
        per_target("spans"),
        5,
        "one statement per span partition: {inserts:#?}"
    );
    assert_eq!(per_target("traces"), 5, "{inserts:#?}");
    assert_eq!(per_target("resources"), 5, "{inserts:#?}");
    assert_eq!(per_target("tag_names"), 1, "unpartitioned: one statement");
    assert_eq!(per_target("tag_values"), 1);
    assert_eq!(inserts.len(), 17, "{inserts:#?}");
    for target in ["spans", "traces", "resources"] {
        let mut predicates: Vec<&str> = inserts
            .iter()
            .filter(|s| s.contains(&format!("INSERT INTO {db}.{target} ")))
            .map(|s| {
                s.rsplit_once(" WHERE ")
                    .unwrap_or_else(|| panic!("{target}: no partition predicate in {s}"))
                    .1
            })
            .collect();
        predicates.sort_unstable();
        predicates.dedup();
        assert_eq!(
            predicates.len(),
            5,
            "{target}: five statements, five distinct single-partition \
             predicates: {predicates:?}"
        );
        for predicate in &predicates {
            assert!(
                predicate.contains(" = '")
                    && !predicate.contains(" OR ")
                    && !predicate.contains(" IN "),
                "{target}: each statement restricts the partition expression \
                 to a single value: {predicate}"
            );
        }
    }

    assert_eq!(
        (
            count(&client, "SELECT count() AS n FROM spans FINAL").await,
            count(&client, "SELECT count() AS n FROM traces FINAL").await,
            count(
                &client,
                "SELECT toUInt64(uniqExact(day)) AS n FROM resources"
            )
            .await,
        ),
        before,
        "the replay completes and each day-partitioned target holds all five days"
    );

    drop_db(&db).await;
}

/// **The repair converges after a partition is left part-written.**
///
/// The part-written partition is made by a run that **completes over half
/// the window**, not by a run that fails: the first run's `--to` is push B's
/// own stamp and the bound is exclusive, so it replays push A alone. All
/// five of the phase-one values differ from the whole-window ones, which is
/// what makes this a part-written partition rather than a smaller correct
/// one.
///
/// Every `start_ns` is on one UTC day and that is load-bearing: `traces` is
/// partitioned by `day` over the block's own `min(start_ns)`, so a partial
/// block and a whole one land in the same partition — and therefore merge on
/// `trace_id` — only while the trace sits inside one day.
#[tokio::test]
async fn the_repair_converges_after_a_partition_is_left_part_written() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_landing_it_converge")).await;

    // One instant inside the span retention, and one UTC day for every
    // span: a `start_ns` of a few hundred nanoseconds is 1970 and past
    // `spans`' own delete-TTL.
    let day = 86_400_000_000_000i64;
    let t0 = (now_ns() / day) * day + 3_600_000_000_000;
    let t = trace_id_of(0x91);
    let u = trace_id_of(0x92);

    // Push A: two children of T in `cart`.
    land(
        &client,
        &request(
            "cart",
            vec![
                span_full(&t, 0xa1, Some(0xa0), "child-1", 2, t0 + 300, 100, vec![]),
                span_full(&t, 0xa2, Some(0xa0), "child-2", 2, t0 + 400, 100, vec![]),
            ],
        ),
    )
    .await;
    // Push B, once the first push's millisecond has passed — **one** landing
    // insert carrying all three of its spans under three resources, so the
    // window below holds exactly two stamps.
    tokio::time::sleep(Duration::from_millis(3)).await;
    land(
        &client,
        &request_of_services(vec![
            (
                "checkout",
                vec![span_full(&t, 0xa0, None, "/api", 2, t0 + 100, 800, vec![])],
            ),
            (
                "cart",
                vec![span_full(
                    &t,
                    0xa3,
                    Some(0xa0),
                    "child-3",
                    2,
                    t0 + 500,
                    100,
                    vec![],
                )],
            ),
            (
                "pay",
                vec![span_full(
                    &u,
                    0xb1,
                    None,
                    "charge",
                    2,
                    t0 + 200,
                    100,
                    vec![],
                )],
            ),
        ]),
    )
    .await;

    // **Exactly two stamps**, and the case fails here rather than in the
    // phases below if the two pushes shared one.
    let stamps = count(
        &client,
        "SELECT uniqExact(received_ms) AS n FROM trace_landing",
    )
    .await;
    assert_eq!(
        stamps, 2,
        "the window has to hold exactly two received_ms stamps, so the \
         half-window run replays push A alone"
    );

    let ingest_names = count(&client, "SELECT count() AS n FROM tag_names FINAL").await;
    let ingest_values = count(&client, "SELECT count() AS n FROM tag_values FINAL").await;
    truncate_targets(&client).await;

    let (lo, hi) = received_ms_bounds(&client).await;
    assert!(hi > lo, "the window has two ends");

    // Phase one: the half-window run, which replays push A alone.
    replay(&client, &db, lo, hi).await;
    assert_eq!(
        count(&client, "SELECT count() AS n FROM spans FINAL").await,
        2,
        "the half-window run replayed push A alone"
    );
    assert_eq!(
        traces_row(&client, &t).await,
        format!("{}|{}|||['cart']", t0 + 300, t0 + 500),
        "a part-written partition: no root, one service, the children's own \
         bounds"
    );
    assert_eq!(
        count(
            &client,
            &format!(
                "SELECT count() AS n FROM traces WHERE trace_id = unhex('{}')",
                hex_of(&u)
            )
        )
        .await,
        0,
        "and nothing of the second trace at all"
    );

    // Phase two: the whole window. A re-run completes what was left, and
    // the duplicates collapse on the key each target collapses on.
    replay(&client, &db, lo, hi + 1).await;
    assert_eq!(
        count(&client, "SELECT count() AS n FROM spans FINAL").await,
        5
    );
    assert_eq!(
        traces_row(&client, &t).await,
        format!(
            "{}|{}|checkout|/api|['cart','checkout']",
            t0 + 100,
            t0 + 900
        ),
        "the re-run converges"
    );
    assert_eq!(
        count(
            &client,
            &format!(
                "SELECT count() AS n FROM traces FINAL WHERE trace_id = unhex('{}')",
                hex_of(&u)
            )
        )
        .await,
        1,
        "and the second trace has its own row"
    );
    assert_eq!(
        count(&client, "SELECT count() AS n FROM resources FINAL").await,
        3,
        "three distinct resources, one per service name"
    );
    assert_eq!(
        count(&client, "SELECT count() AS n FROM tag_names FINAL").await,
        ingest_names,
        "the replay reaches the same catalog rows the ingest did"
    );
    assert_eq!(
        count(&client, "SELECT count() AS n FROM tag_values FINAL").await,
        ingest_values
    );

    // Recorded, not asserted: the second is 2 until a merge combines the
    // partial row with the complete one, and the read above answers
    // correctly whether or not it has.
    let unmerged_spans = count(&client, "SELECT count() AS n FROM spans").await;
    let unmerged_traces = count(
        &client,
        &format!(
            "SELECT count() AS n FROM traces WHERE trace_id = unhex('{}')",
            hex_of(&t)
        ),
    )
    .await;
    eprintln!(
        "converge: SELECT count() FROM spans = {unmerged_spans}, \
         traces rows for T = {unmerged_traces} (both recorded)"
    );

    drop_db(&db).await;
}

/// The `traces` read of `docs/TraceQL/sql-schema.md` §5.2's own shape, as one
/// string: `min(start_ns)|max(end_ns)|root_service|root_name|services`.
async fn traces_row(client: &ChClient, trace: &[u8; 16]) -> String {
    scalar(
        client,
        &format!(
            "SELECT concat(toString(min(start_ns)), '|', toString(max(end_ns)), '|', \
                    max(root_service), '|', max(root_name), '|', \
                    toString(arraySort(groupUniqArrayArray(services)))) AS s \
             FROM traces WHERE trace_id = unhex('{}') GROUP BY trace_id",
            hex_of(trace)
        ),
    )
    .await
}

fn hex_of(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// **A row arriving behind the repair is recovered by the next run.**
///
/// The first run leaves that row's span **absent** — asserted, not merely
/// allowed, because it is what the repair discloses — and a second run with
/// no further arrivals leaves every landed row present, the late one
/// included, with nothing doubled.
///
/// The late row's stamp is **inside** the first run's window and in a
/// partition that run had already processed; what the run missed is a row
/// that was not committed when it read. The seam is after the run rather
/// than between two of its statements, which the engine's fixed window makes
/// the only expressible one.
#[tokio::test]
async fn a_row_arriving_behind_the_repair_is_recovered_by_the_next_run() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_landing_it_behind")).await;

    let day = 86_400_000_000_000i64;
    let t0 = (now_ns() / day) * day + 3_600_000_000_000;
    let early = now_ms();
    let late_window_end = early + 10;

    // A window over two UTC days.
    land_at(
        &client,
        &request(
            "checkout",
            vec![
                span_full(
                    &trace_id_of(0xc1),
                    0xd1,
                    None,
                    "day-1",
                    2,
                    t0,
                    1_000,
                    vec![],
                ),
                span_full(
                    &trace_id_of(0xc2),
                    0xd2,
                    None,
                    "day-2",
                    2,
                    t0 - day,
                    1_000,
                    vec![],
                ),
            ],
        ),
        early,
    )
    .await;
    truncate_targets(&client).await;

    replay(&client, &db, early, late_window_end).await;
    assert_eq!(
        count(&client, "SELECT count() AS n FROM spans FINAL").await,
        2,
        "the first run replays the window it read"
    );

    // A row for an already-processed day, stamped inside the same window,
    // committed behind the run. The five views are detached while it lands:
    // a committed landing insert otherwise fans out as part of its own
    // processing, which is the whole point of the design and would put the
    // row in `spans` with no replay involved.
    set_views_attached(&client, false).await;
    land_at(
        &client,
        &request(
            "checkout",
            vec![span_full(
                &trace_id_of(0xc3),
                0xd3,
                None,
                "late",
                2,
                t0 + 5_000,
                1_000,
                vec![],
            )],
        ),
        early,
    )
    .await;
    set_views_attached(&client, true).await;
    assert_eq!(
        count(
            &client,
            "SELECT count() AS n FROM spans FINAL WHERE name = 'late'"
        )
        .await,
        0,
        "the first run left the late row's span absent, which is what the \
         repair discloses rather than prevents"
    );

    replay(&client, &db, early, late_window_end).await;
    assert_eq!(
        count(&client, "SELECT count() AS n FROM spans FINAL").await,
        3,
        "a second run with no further arrivals recovers it"
    );
    assert_eq!(
        count(
            &client,
            "SELECT count() AS n FROM spans FINAL WHERE name = 'late'"
        )
        .await,
        1,
        "and nothing is doubled"
    );

    drop_db(&db).await;
}

/// **A replay of a landing window changes no answer.** It is the case the
/// repair's "no partition drop" rests on: metrics refuses a replay into its
/// append-only targets for exactly the reason this fixture would expose if
/// any trace target summed.
#[tokio::test]
async fn a_replay_of_a_landing_window_changes_no_answer() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_landing_it_replay")).await;

    let day = 86_400_000_000_000i64;
    let t0 = (now_ns() / day) * day + 3_600_000_000_000;
    let services = ["checkout", "cart", "pay"];
    for (s, service) in services.iter().enumerate() {
        let spans: Vec<Span> = (0..34i64)
            .filter(|i| (s as i64) * 34 + i < 100)
            .map(|i| {
                let n = (s as i64) * 34 + i;
                span_full(
                    &trace_id_of(0xe1 + s as u8),
                    (n % 251) as u8 + 1,
                    None,
                    "GET /api",
                    2,
                    t0 + n,
                    1_000,
                    vec![
                        kv("http.route", str_value("/api")),
                        kv(&format!("app.k{}", n % 12), str_value("v")),
                    ],
                )
            })
            .collect();
        land(&client, &request(service, spans)).await;
    }
    assert_eq!(
        count(&client, "SELECT count() AS n FROM spans FINAL").await,
        100,
        "the fixture is a hundred spans over three resources"
    );
    let single = (
        count(&client, "SELECT count() AS n FROM resources FINAL").await,
        count(&client, "SELECT count() AS n FROM tag_names FINAL").await,
        count(&client, "SELECT count() AS n FROM tag_values FINAL").await,
    );
    assert_eq!(single.0, 3);
    let traces_before = all_traces_rows(&client).await;

    let (lo, hi) = received_ms_bounds(&client).await;
    for pass in 1..=2 {
        let report = replay(&client, &db, lo, hi + 1).await;
        // **The one thing that tells a replay which changed no answer from a
        // replay that never ran.** Every other assertion below is satisfied
        // by a command that did nothing at all. All hundred spans are on one
        // UTC day and all three traces' own minima with them, so each
        // day-partitioned target has one partition and the two catalogs one
        // statement each.
        assert_eq!(
            report.inserts().len(),
            5,
            "pass {pass}: one statement per target partition: {:#?}",
            report.inserts()
        );
        assert_eq!(
            count(&client, "SELECT count() AS n FROM spans FINAL").await,
            100,
            "pass {pass}: no partition was dropped and nothing doubled"
        );
        assert_eq!(
            (
                count(&client, "SELECT count() AS n FROM resources FINAL").await,
                count(&client, "SELECT count() AS n FROM tag_names FINAL").await,
                count(&client, "SELECT count() AS n FROM tag_values FINAL").await,
            ),
            single,
            "pass {pass}"
        );
        assert_eq!(
            all_traces_rows(&client).await,
            traces_before,
            "pass {pass}: every column of every `traces` row equals the \
             single-push value"
        );
    }

    // Allowed to be 300: the collapse is the target's own key.
    let unmerged = count(&client, "SELECT count() AS n FROM spans").await;
    eprintln!("replay: SELECT count() FROM spans (recorded) = {unmerged}");

    drop_db(&db).await;
}

/// Every `traces` row, reduced the way §5.2 reads that table.
async fn all_traces_rows(client: &ChClient) -> Vec<String> {
    texts(
        client,
        "SELECT concat(hex(trace_id), '|', toString(min(start_ns)), '|', \
                toString(max(end_ns)), '|', max(root_service), '|', max(root_name), '|', \
                toString(arraySort(groupUniqArrayArray(services)))) AS s \
         FROM traces GROUP BY trace_id ORDER BY s",
    )
    .await
}

/// **A read cap cannot turn a repair run into a short success.**
///
/// The profile is reproduced as a per-user setting, which is how a
/// deployment's profile is reached without touching the shared server's
/// configuration. Those two settings are the whole profile: this case bounds
/// nothing about how the rows arrive in chunks, because each part of the
/// outcome holds for every arrangement the engine can produce.
///
/// The raise is certain because the window's 400 rows are more than the cap
/// of 200 and `SizeLimits::check` raises on `rows > max_rows`. The two zeros
/// are certain because the squash threshold is the pinned row ceiling and
/// the window's 400 rows never reach it, so nothing reaches a sink before
/// the pipeline finishes and the raise aborts it.
#[tokio::test]
async fn a_read_cap_cannot_turn_a_repair_run_into_a_short_success() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_landing_it_readcap")).await;

    let day = 86_400_000_000_000i64;
    let t0 = (now_ns() / day) * day + 3_600_000_000_000;
    let spans: Vec<Span> = (0..400i64)
        .map(|i| {
            span_full(
                &trace_id_of(0xf1),
                (i % 251) as u8 + 1,
                None,
                "GET /api",
                2,
                t0 + i,
                1_000,
                vec![],
            )
        })
        .collect();
    land(&client, &request("checkout", spans)).await;
    truncate_targets(&client).await;

    let user = format!("{db}_capped");
    let capped = capped_client(
        &client,
        &db,
        &user,
        "max_rows_to_read = 200, read_overflow_mode = 'break'",
    )
    .await;

    let (lo, hi) = received_ms_bounds(&client).await;
    let ctx: SchemaParams = RenderCtx::for_tests(&db);
    let err = pulsus_schema::replay_trace_window(&capped, &ctx, lo, hi + 1, REPLAY_MAX_ROWS)
        .await
        .expect_err("the run must fail: the pinned mode makes the cap loud");
    let text = err.to_string();
    assert!(
        text.contains("158"),
        "the error carries TOO_MANY_ROWS: {text}"
    );
    assert!(
        text.contains("max_rows_to_read"),
        "the error names the cap the deployment set: {text}"
    );
    // **The text is the server's own, read off 26.3.29.7**: `Limit for rows
    // (controlled by 'max_rows_to_read' setting) exceeded, max rows: 200.00,
    // current rows: 401.00`. It is not the shorter `Limit for rows or bytes
    // to read exceeded` the plan quoted — the engine produces that one for
    // the combined row-or-byte check and this one for the row cap alone.
    assert_eq!(count(&client, "SELECT count() AS n FROM spans").await, 0);
    assert_eq!(count(&client, "SELECT count() AS n FROM traces").await, 0);

    drop_user(&client, &user).await;
    drop_db(&db).await;
}

/// **A group-by cap cannot turn the per-trace replay into a short success.**
/// `traces_mv` is the one view whose projection groups, and at `break` the
/// aggregation would stop consuming its input and the statement would
/// succeed having written at least 3 of the 10 traces and at most all 10 —
/// which is why **no count is the expected value** and the run's outcome is.
#[tokio::test]
async fn a_group_by_cap_cannot_turn_the_traces_replay_into_a_short_success() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_landing_it_groupcap")).await;

    let day = 86_400_000_000_000i64;
    let t0 = (now_ns() / day) * day + 3_600_000_000_000;
    let mut spans = Vec::new();
    for t in 0..10u8 {
        for i in 0..40i64 {
            spans.push(span_full(
                &trace_id_of(0x11 + t),
                (i % 251) as u8 + 1,
                None,
                "GET /api",
                2,
                t0 + i64::from(t) * 1_000 + i,
                1_000,
                vec![],
            ));
        }
    }
    land(&client, &request("checkout", spans)).await;
    truncate_targets(&client).await;

    let user = format!("{db}_grouped");
    let capped = capped_client(
        &client,
        &db,
        &user,
        "max_rows_to_group_by = 2, group_by_overflow_mode = 'break'",
    )
    .await;

    let (lo, hi) = received_ms_bounds(&client).await;
    let ctx: SchemaParams = RenderCtx::for_tests(&db);
    let err = pulsus_schema::replay_trace_window(&capped, &ctx, lo, hi + 1, REPLAY_MAX_ROWS)
        .await
        .expect_err("the run must fail: the pinned mode makes the cap loud");
    let text = err.to_string();
    assert!(text.contains("158"), "{text}");
    assert!(
        text.contains("Limit for rows to GROUP BY exceeded"),
        "{text}"
    );
    assert_eq!(
        count(&client, "SELECT count() AS n FROM traces FINAL").await,
        0
    );
    // Recorded, not asserted: the `spans` statement has no `GROUP BY` and
    // may have run and committed before the per-trace one raised.
    let spans_rows = count(&client, "SELECT count() AS n FROM spans FINAL").await;
    eprintln!("group-by cap: spans FINAL = {spans_rows} (recorded)");

    drop_user(&client, &user).await;
    drop_db(&db).await;
}

/// Creates a user carrying `settings`, grants it the test database, and
/// returns a client bound to it. Dropped by exact name by [`drop_user`].
async fn capped_client(admin: &ChClient, db: &str, user: &str, settings: &str) -> ChClient {
    for sql in [
        format!("DROP USER IF EXISTS {user}"),
        format!("CREATE USER {user} IDENTIFIED WITH no_password SETTINGS {settings}"),
        format!("GRANT ALL ON {db}.* TO {user}"),
    ] {
        admin
            .execute(&sql, &QuerySettings::new(), Idempotency::NonIdempotent)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    ChClient::new(ChConnConfig {
        database: db.to_string(),
        user: user.to_string(),
        password: String::new(),
        ..base_config()
    })
    .await
    .expect("connect as the capped user")
}

async fn drop_user(admin: &ChClient, user: &str) {
    admin
        .execute(
            &format!("DROP USER IF EXISTS {user}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the capped user");
}

// === Issue #587: the values #586's definitions could not hold ==============

/// One request whose single `ScopeSpans` carries its own `schema_url` and
/// whose scope carries its own dropped count, which
/// [`request_with`] cannot express.
fn request_with_scope_spans(
    service: &str,
    spans: Vec<Span>,
    scope: Option<InstrumentationScope>,
    scope_schema_url: &str,
) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv("service.name", str_value(service))],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_spans: vec![ScopeSpans {
                scope,
                spans,
                schema_url: scope_schema_url.to_string(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// **W-1 (issue #587 rows 1, 2 and 3).** A scope's schema url, its own
/// dropped-attribute count and an attribute of its whose value no JSON path
/// can hold are all stored.
///
/// Each is its own column and its own assertion. Without
/// `#[serde(with = "serde_bytes")]` on `scope_attrs_other` the **insert**
/// fails rather than the comparison — serde's default `Vec<u8>` path
/// targets `Array(UInt8)` — so this case cannot be green with a sequence
/// encoding either.
#[tokio::test]
async fn w1_the_scope_schema_url_dropped_count_and_unstorable_attribute_are_stored() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_rows_it_w1")).await;

    let r = try_land(
        &client,
        &request_with_scope_spans(
            "checkout",
            vec![span_with(0xb1, 0x11, Vec::new())],
            Some(InstrumentationScope {
                name: "io.otel.http".to_string(),
                version: "1.4.2".to_string(),
                attributes: vec![kv("scope.blob", bytes_value(&[0xde, 0xad, 0xbe, 0xef]))],
                dropped_attributes_count: 11,
            }),
            "https://example.invalid/scope",
        ),
    )
    .await;
    assert!(r.is_ok(), "the landing insert returned {:?}", r.err());

    assert_eq!(
        scalar(
            &client,
            "SELECT concat(scope_schema_url, '|', toString(scope_dropped_attrs)) AS s FROM spans"
        )
        .await,
        "https://example.invalid/scope|11",
        "the scope's own schema url and dropped count"
    );

    // The scope's unstorable attribute, under its ORIGINAL OTLP key, in the
    // same `KeyValueList` carrier the span's own `attrs_other` uses.
    let carrier = blob(&client, "SELECT scope_attrs_other AS b FROM spans").await;
    let decoded = KeyValueList::decode(carrier.as_slice()).expect("a KeyValueList");
    assert_eq!(decoded.values.len(), 1, "one entry: {decoded:?}");
    assert_eq!(decoded.values[0].key, "scope.blob");
    assert_eq!(
        decoded.values[0].value,
        Some(bytes_value(&[0xde, 0xad, 0xbe, 0xef])),
        "the bytes the sender sent"
    );

    // And the storable scope attributes are untouched beside it.
    assert_eq!(
        count(
            &client,
            "SELECT count() AS n FROM tag_names \
             WHERE scope = 'instrumentation' AND key = 'scope.blob'"
        )
        .await,
        1,
        "a key whose value went to `attrs_other` is still catalogued"
    );

    drop_db(&db).await;
}

/// **W-2 (issue #587 row 4, and half of row 7).** An event's and a link's
/// own unstorable attribute are stored, and a link's ids are the bytes the
/// sender sent whatever their length.
///
/// Both tuples gain an element, so a `serialize_tuple` left at its old
/// arity sends a short tuple and the insert answers
/// `Code: 32 … Attempt to read after eof`; and each new element has to
/// reach `serialize_bytes` rather than serde's sequence path.
#[tokio::test]
async fn w2_each_event_and_link_carries_its_own_unstorable_attribute_and_the_ids_sent() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_rows_it_w2")).await;

    let start = now_ns();
    let span = Span {
        trace_id: vec![0xb2; 16],
        span_id: vec![0x22; 8],
        parent_span_id: Vec::new(),
        trace_state: String::new(),
        flags: 0,
        name: "GET /api".to_string(),
        kind: 2,
        start_time_unix_nano: start as u64,
        end_time_unix_nano: start as u64 + 1_000_000,
        attributes: Vec::new(),
        dropped_attributes_count: 0,
        events: vec![span::Event {
            time_unix_nano: start as u64 + 1,
            name: "exception".to_string(),
            attributes: vec![kv("event.blob", bytes_value(&[0x01, 0x02]))],
            dropped_attributes_count: 7,
        }],
        dropped_events_count: 0,
        // **Four bytes of trace id and three of span id**, which the
        // reference copies with no length check and returns verbatim.
        links: vec![span::Link {
            trace_id: vec![0xaa, 0xbb, 0xcc, 0xdd],
            span_id: vec![0x01, 0x02, 0x03],
            trace_state: String::new(),
            attributes: vec![kv("link.blob", bytes_value(&[0x03, 0x04, 0x05]))],
            dropped_attributes_count: 9,
            flags: 0,
        }],
        dropped_links_count: 0,
        status: None,
    };

    let r = try_land(&client, &request("checkout", vec![span])).await;
    assert!(r.is_ok(), "the landing insert returned {:?}", r.err());

    // The two ids, as the bytes sent.
    assert_eq!(
        scalar(
            &client,
            "SELECT concat(hex(links[1].trace_id), '|', hex(links[1].span_id)) AS s FROM spans"
        )
        .await,
        "AABBCCDD|010203",
        "a link's ids are the bytes the sender put on the wire"
    );

    // Each element's own carrier, decoded as the `KeyValueList` it is.
    for (what, sql, key, value) in [
        (
            "the event",
            "SELECT events[1].attrs_other AS b FROM spans",
            "event.blob",
            vec![0x01u8, 0x02],
        ),
        (
            "the link",
            "SELECT links[1].attrs_other AS b FROM spans",
            "link.blob",
            vec![0x03u8, 0x04, 0x05],
        ),
    ] {
        let decoded =
            KeyValueList::decode(blob(&client, sql).await.as_slice()).expect("a KeyValueList");
        assert_eq!(decoded.values.len(), 1, "{what}: one entry: {decoded:?}");
        assert_eq!(decoded.values[0].key, key, "{what}");
        assert_eq!(
            decoded.values[0].value,
            Some(bytes_value(&value)),
            "{what}: the bytes the sender sent"
        );
    }

    // The elements' other members are undisturbed by the new one.
    assert_eq!(
        scalar(
            &client,
            "SELECT concat(toString(events[1].dropped_attrs), '|', \
                           toString(links[1].dropped_attrs)) AS s FROM spans"
        )
        .await,
        "7|9",
        "each element's dropped count still reads back"
    );

    drop_db(&db).await;
}

/// **W-3 (issue #587 row 5, and §3.2's any-depth clause).** A kvlist with a
/// leaf no JSON path can hold moves the **whole** attribute to
/// `attrs_other`, under its own key, at **any** depth.
///
/// The unstorable leaf is a **grandchild**, so an implementation that
/// checks its direct children and ignores a deeper verdict writes `k.a.ok`
/// and `k.b` as paths and leaves only the grandchild out — which is why the
/// **absence** of `k.b` is what discriminates depth-two propagation from
/// depth-one.
///
/// The second, separate attribute keyed literally `k.a.deep` is what pins
/// the carrier choice: under a design that put the leaf into
/// `attrs_other` under its own dotted key the two would collide, and a
/// reader could tell neither apart nor put the nested one back together.
#[tokio::test]
async fn w3_a_kvlist_with_an_unstorable_leaf_at_any_depth_moves_whole() {
    skip_unless_live!();
    let (client, db) = fresh_db(pulsus_testkit::test_db("pulsus_trace_rows_it_w3")).await;

    let nested = kvlist_value(vec![
        (
            "a",
            kvlist_value(vec![
                ("deep", bytes_value(&[0xfe, 0xed])),
                ("ok", int_value(2)),
            ]),
        ),
        ("b", int_value(1)),
    ]);
    let r = try_land(
        &client,
        &request(
            "checkout",
            vec![span_with(
                0xb3,
                0x33,
                vec![kv("k", nested.clone()), kv("k.a.deep", str_value("flat"))],
            )],
        ),
    )
    .await;
    assert!(r.is_ok(), "the landing insert returned {:?}", r.err());

    // `attrs` holds the flat attribute's ESCAPED path and no path of the
    // kvlist's — not the grandchild, not its sibling, not the uncle.
    assert_eq!(
        texts(
            &client,
            "SELECT arrayJoin(arraySort(JSONAllPaths(attrs))) AS s FROM spans"
        )
        .await,
        vec!["k%2Ea%2Edeep".to_string()],
        "the only stored path is the separate flat attribute's"
    );

    // `attrs_other` holds ONE entry, keyed `k`, whose value is the whole
    // two-level kvlist the sender sent.
    let decoded = KeyValueList::decode(
        blob(&client, "SELECT attrs_other AS b FROM spans")
            .await
            .as_slice(),
    )
    .expect("a KeyValueList");
    assert_eq!(decoded.values.len(), 1, "one entry: {decoded:?}");
    assert_eq!(decoded.values[0].key, "k", "under the attribute's own key");
    assert_eq!(
        decoded.values[0].value,
        Some(nested),
        "the whole attribute, whole"
    );

    // The catalog lists the attribute's own key and the separate flat key,
    // and no leaf of the kvlist.
    assert_eq!(
        texts(
            &client,
            "SELECT key AS s FROM tag_names WHERE scope = 'span' ORDER BY key"
        )
        .await,
        vec!["k".to_string(), "k.a.deep".to_string()],
        "two keys: the attribute's own and the separate flat one"
    );

    drop_db(&db).await;
}
