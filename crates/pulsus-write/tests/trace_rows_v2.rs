//! The span row encoder and the `JSON` column, round-tripped against a real
//! ClickHouse server (issue #585).
//!
//! **The test seam, because there is no compiler and no route on the new
//! tables yet.** Every case here decodes an OTLP request with the landing
//! decoder, inserts the rows it produces into `trace_landing` with the
//! production client, and reads back through the five views with a literal
//! `SELECT` written out in the case. None of them goes through a `q=`
//! parameter or an HTTP route; those arrive with the read-path tasks, and
//! each re-asserts the same behaviour through the route it adds.
//!
//! **This suite does not run yet, and the reason is a finding against the
//! design rather than against the code below.** The trace landing table
//! declares its `events` and `links` columns as
//! `Array(Tuple(time_ns Int64, name LowCardinality(String), attrs JSON,
//! dropped_attrs UInt32))` — named tuple elements, transcribed from
//! `docs/TraceQL/measure/schema.sql` — and the type parser the driver uses
//! for insert-side schema validation cannot parse a **named** tuple at any
//! depth. Measured against `clickhouse-types` 0.1.2 and 0.1.3, which is the
//! latest published:
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
//! So every insert into `trace_landing` fails before a byte is sent:
//!
//! ```text
//! Decode("error while parsing columns header from the response: \
//!   type parsing error: Unknown data type: \n    time_ns Int64")
//! ```
//!
//! Two routes were measured, each unblocking this suite to the same point —
//! nine of these cases passing, the other four being this file's own
//! read-back SQL rather than the writer:
//!
//! * drop the element names from the two `Array(Tuple(…))` columns in
//!   `spans` and `trace_landing`. Validation stays on and the driver patch
//!   stays load-bearing; the cost is that a read says `events.2` rather
//!   than `events.name`, and both this design's DDL and
//!   `docs/TraceQL/measure/schema.sql` move.
//! * build the client with `Client::with_validation(false)`. The names stay;
//!   the cost is that insert-side schema validation disappears for every
//!   insert of every signal, and the driver patch has nothing left to do.
//!
//! Which one is taken is a design decision, not an implementation one, so it
//! is not taken here. **No CI step runs this file** until it is.
//!
//! **The suite itself is complete and was run to green under the first
//! route**: thirteen of thirteen against 26.3.29.7 with the element names
//! dropped and nothing else changed. Three of its cases were then
//! deliberately broken and each failed — the path escape leaving `%` alone,
//! an empty array landing as a mixed one, and a non-finite double being
//! dropped rather than encoded. So what is outstanding is the decision, not
//! this file.
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

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
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

/// Decodes `req` and inserts every row it produces into `trace_landing`
/// through the production client, in **one** block — one push is one insert.
async fn land(client: &ChClient, req: &ExportTraceServiceRequest) -> ParsedTraceLanding {
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
    client
        .insert_block("trace_landing", &rows)
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
    land(&client, &request("checkout", vec![span])).await;

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
