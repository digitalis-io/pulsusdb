//! OTLP traces ingest round-trip (issue #54 AC5, plan v2 delta 3): the
//! committed fixture POSTs through the real product path — `POST
//! /v1/traces` -> `otlp_traces::parse` -> `TraceWriter` (sync mode) ->
//! ClickHouse — against a fresh, isolated database (`DROP DATABASE` +
//! `run_init`, so the T1 tables exist), then asserts **exact** physical
//! `count()`s on BOTH writer-written tables: `trace_spans` = #spans and
//! `trace_attrs_idx` = #attr rows. Exact (not `>=`, the round-1 review
//! fix): both tables are writer-written (not MV-derived) with distinct
//! `ORDER BY` keys per row in this batch, so `count()` is exact and
//! merge-stable — a duplicate write cannot hide. No poll needed: the sync
//! `FlushWait` resolves only after both inserts are durable.
//!
//! Also proves the load-bearing wire encodings the row shapes pin
//! (`[u8; N]` -> `FixedString(N)`, `serde_bytes` `Vec<u8>` -> `String`):
//! the stored payload reads back byte-identical and still decodes as the
//! self-contained `TracesData` (the pinned T2/T3 contract, live).
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, same harness as
//! `ingest_fidelity.rs`:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:26.3
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-write --test trace_ingest_roundtrip
//! podman rm -f pulsus-ch-test
//! ```

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::routing::post;
use futures::StreamExt;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::trace::v1::TracesData;
use prost::Message;
use tower::ServiceExt;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_config::WriterConfig;
use pulsus_schema::{RenderCtx, SchemaParams, run_init};
use pulsus_write::ingest::http::traces;
use pulsus_write::{TraceWriter, TraceWriterTables};

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping when
/// the gate is absent in a live CI job, so a lost `env:` block reddens the
/// build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-write/tests/trace_ingest_roundtrip.rs for setup)"
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

fn db_config(db: &str) -> ChConnConfig {
    ChConnConfig {
        database: db.to_string(),
        ..base_config()
    }
}

fn schema_params(db: &str) -> SchemaParams {
    RenderCtx {
        db: db.to_string(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
    }
}

/// Prepares a fresh, isolated database (`DROP DATABASE IF EXISTS` +
/// `run_init`) and returns a client bound to it.
async fn fresh_db(db: &str) -> ChClient {
    let admin = ChClient::new(base_config()).await.expect("connect admin");
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
    run_init(&admin, &schema_params(db))
        .await
        .expect("run_init");
    ChClient::new(db_config(db)).await.expect("connect db")
}

/// Loads the committed fixture and rebases every span's timestamps onto
/// "now" (preserving each span's offset/duration): `trace_spans`/
/// `trace_attrs_idx` carry `ttl_only_drop_parts = 1` delete-TTLs, so the
/// fixture's fixed 2023 literals would land in an already-expired part —
/// the exact hazard `ingest_fidelity.rs::now_ns`'s doc comment records.
fn fixture_request_rebased_to_now() -> (ExportTraceServiceRequest, usize, usize) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/otlp-traces/two_spans_dual_scope.bin");
    let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("read fixture {path:?}: {e}"));
    let mut req = ExportTraceServiceRequest::decode(bytes.as_slice()).expect("fixture decodes");

    let now_ns = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits u64");

    let mut span_count = 0usize;
    let mut attr_count = 0usize;
    for rs in &mut req.resource_spans {
        let resource_attrs = rs
            .resource
            .as_ref()
            .map(|r| r.attributes.len())
            .unwrap_or(0);
        for ss in &mut rs.scope_spans {
            // Issue #192: instrumentation-scope attributes are indexed under
            // `scope='instrumentation'`, emitted once PER SPAN in this scope
            // (exactly as resource attrs re-emit per span).
            let scope_attrs = ss.scope.as_ref().map(|s| s.attributes.len()).unwrap_or(0);
            let base = ss
                .spans
                .iter()
                .map(|s| s.start_time_unix_nano)
                .min()
                .expect("fixture has spans");
            for span in &mut ss.spans {
                let offset = span.start_time_unix_nano - base;
                let duration = span.end_time_unix_nano - span.start_time_unix_nano;
                // Each EVENT moves with its span (issue #556). The helper
                // used to leave `time_unix_nano` at the fixture's 2023
                // value, so the `timeSinceStart` intrinsic came out as a
                // large negative delta (measured: -89628571868717907). No
                // assertion looked at it before; the array test asserts the
                // element, so it has to be the fixture's own +3 ms.
                for event in &mut span.events {
                    let event_offset = event.time_unix_nano - span.start_time_unix_nano;
                    event.time_unix_nano = now_ns + offset + event_offset;
                }
                span.start_time_unix_nano = now_ns + offset;
                span.end_time_unix_nano = now_ns + offset + duration;
                span_count += 1;
                // Issue #192 PR-B: each span event fans out into two
                // `event:intrinsic` rows (name/timeSinceStart) plus one
                // `event`-scoped row per event attribute. (The attr rows carry
                // the SPAN's rebased timestamp/date, so they land in a live
                // partition; the event's own time only feeds timeSinceStart.)
                let event_rows: usize = span.events.iter().map(|e| 2 + e.attributes.len()).sum();
                // Issue #192 PR-C: each span link fans out identically — two
                // `link:intrinsic` rows (spanID/traceID) plus one `link`-scoped
                // row per link attribute.
                let link_rows: usize = span.links.iter().map(|l| 2 + l.attributes.len()).sum();
                attr_count +=
                    resource_attrs + span.attributes.len() + scope_attrs + event_rows + link_rows;
            }
        }
    }
    (req, span_count, attr_count)
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CountRow {
    n: u64,
}

async fn count(client: &ChClient, sql: &str) -> u64 {
    let mut stream = client
        .query_stream::<CountRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("count query failed: {e}\nSQL:\n{sql}"));
    stream.next().await.expect("one row").expect("decode").n
}

/// AC5 (issue #54, plan v2 delta 3): sync POST through the full product
/// path, then exact `count()`s on both tables + a live payload read-back.
#[tokio::test]
async fn sync_post_round_trips_exact_counts_on_both_trace_tables() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_write_it_trace_roundtrip");
    let client = fresh_db(db).await;

    let (req, span_count, attr_count) = fixture_request_rebased_to_now();
    // The committed fixture's known shape: 2 spans; 2 resource attrs x 2
    // spans + 3 span attrs on span A + 1 instrumentation-scope attr x 2
    // spans (issue #192) + span A's one event: 2 `event:intrinsic` rows
    // (name/timeSinceStart) + 1 `event` attr row (issue #192 PR-B) + span A's
    // one link: 2 `link:intrinsic` rows (spanID/traceID) + 1 `link` attr row
    // (issue #192 PR-C) = 15 attr rows. Recounted from the decoded fixture
    // above so a fixture edit fails here loudly rather than silently
    // weakening the exact-count assertion.
    assert_eq!(span_count, 2);
    assert_eq!(attr_count, 15);

    let writer = Arc::new(TraceWriter::new_with_tables(
        Arc::new(ChClient::new(db_config(db)).await.expect("connect writer")),
        &WriterConfig::default(),
        TraceWriterTables::traces_default(),
    ));
    let router: Router = Router::new()
        .route("/v1/traces", post(traces::<TraceWriter>))
        .with_state(writer);

    let request = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .body(Body::from(req.encode_to_vec()))
        .expect("build request");
    // No X-Pulsus-Async header: sync mode — the 200 means both
    // generations (spans + attrs) are durable, so the counts below need
    // no settle poll.
    let response = router.oneshot(request).await.expect("router call");
    assert_eq!(
        response.status(),
        axum::http::StatusCode::OK,
        "product ingest path must accept the fixture"
    );

    let spans = count(
        &client,
        &format!("SELECT count() AS n FROM {db}.trace_spans"),
    )
    .await;
    assert_eq!(
        spans, span_count as u64,
        "trace_spans must hold exactly one row per fixture span"
    );
    let attrs = count(
        &client,
        &format!("SELECT count() AS n FROM {db}.trace_attrs_idx"),
    )
    .await;
    assert_eq!(
        attrs, attr_count as u64,
        "trace_attrs_idx must hold exactly one row per indexed resource/span attribute"
    );

    // Load-bearing wire encodings, proven live: the `serde_bytes` payload
    // round-trips byte-identical out of the `String` column and still
    // decodes as the self-contained single-ResourceSpans TracesData; the
    // `[u8; N]` ids round-trip through FixedString(N).
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct PayloadRow {
        trace_id: [u8; 16],
        span_id: [u8; 8],
        name: String,
        #[serde(with = "serde_bytes")]
        payload: Vec<u8>,
    }
    let mut stream = client
        .query_stream::<PayloadRow>(
            &format!("SELECT trace_id, span_id, name, payload FROM {db}.trace_spans ORDER BY name"),
            &QuerySettings::new(),
        )
        .await
        .expect("select payloads");
    let mut rows = Vec::new();
    while let Some(row) = stream.next().await {
        rows.push(row.expect("decode payload row"));
    }
    assert_eq!(rows.len(), 2);
    for row in &rows {
        let payload =
            TracesData::decode(row.payload.as_slice()).expect("stored payload decodes live");
        assert_eq!(payload.resource_spans.len(), 1);
        let ss = &payload.resource_spans[0].scope_spans[0];
        assert_eq!(ss.spans.len(), 1);
        assert_eq!(ss.spans[0].trace_id, row.trace_id.to_vec());
        assert_eq!(ss.spans[0].span_id, row.span_id.to_vec());
        assert_eq!(ss.spans[0].name, row.name);
    }

    // The scope discriminator landed physically (amended migration 17):
    // the dual-scope key holds exactly one 'resource' + one 'span' row.
    let dual_scopes = count(
        &client,
        &format!(
            "SELECT countDistinct(scope) AS n FROM {db}.trace_attrs_idx \
             WHERE key = 'deployment.environment' AND val = 'prod'"
        ),
    )
    .await;
    assert_eq!(
        dual_scopes, 2,
        "the same verbatim (key, val) at both scopes must land as two scoped rows"
    );
}

/// Issue #184 AC5 (ingest side): a non-empty OTLP `Status.message`
/// round-trips through the REAL wire path (`POST /v1/traces` →
/// `otlp_traces::parse` → `TraceWriter`) into the migration-35
/// `trace_spans.status_message` column, verbatim; spans without a Status
/// land `''`. The committed fixture is untouched — the decoded request is
/// mutated in memory.
#[tokio::test]
async fn status_message_round_trips_through_the_product_ingest_path() {
    skip_unless_live!();
    use opentelemetry_proto::tonic::trace::v1::Status;
    let db = &pulsus_testkit::test_db("pulsus_write_it_trace_status_msg");
    let client = fresh_db(db).await;

    let (mut req, span_count, _) = fixture_request_rebased_to_now();
    assert_eq!(span_count, 2);
    // First span carries a message; the second carries NO Status at all
    // (fixture-layout-independent: walk every scope_spans list in order).
    let mut idx = 0usize;
    for rs in &mut req.resource_spans {
        for ss in &mut rs.scope_spans {
            for span in &mut ss.spans {
                span.status = if idx == 0 {
                    Some(Status {
                        message: "deadline exceeded: ingest-184".to_string(),
                        code: 2,
                    })
                } else {
                    None
                };
                idx += 1;
            }
        }
    }
    assert_eq!(idx, 2);

    let writer = Arc::new(TraceWriter::new_with_tables(
        Arc::new(ChClient::new(db_config(db)).await.expect("connect writer")),
        &WriterConfig::default(),
        TraceWriterTables::traces_default(),
    ));
    let router: Router = Router::new()
        .route("/v1/traces", post(traces::<TraceWriter>))
        .with_state(writer);
    let request = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .body(Body::from(req.encode_to_vec()))
        .expect("build request");
    let response = router.oneshot(request).await.expect("router call");
    assert_eq!(response.status(), axum::http::StatusCode::OK);

    let with_message = count(
        &client,
        &format!(
            "SELECT count() AS n FROM {db}.trace_spans \
             WHERE status_message = 'deadline exceeded: ingest-184'"
        ),
    )
    .await;
    assert_eq!(
        with_message, 1,
        "the message lands verbatim on exactly its span"
    );
    let empty = count(
        &client,
        &format!("SELECT count() AS n FROM {db}.trace_spans WHERE status_message = ''"),
    )
    .await;
    assert_eq!(empty, 1, "a span without a Status stores the '' default");
}

// ---------------------------------------------------------------------
// Issue #556: the span row carries its own attributes.
// ---------------------------------------------------------------------

/// The five arrays plus `attr_num` re-read as BITS. `-0.0 == 0.0` is
/// `true`, so only a bit comparison can see a stored sign bit; and the
/// `arrayMap` keeps each NULL at its own position rather than dropping it.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct ArrayRow {
    name: String,
    attr_key: Vec<String>,
    attr_scope: Vec<String>,
    attr_val: Vec<String>,
    attr_type: Vec<String>,
    attr_num_bits: Vec<Option<u64>>,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct AlignmentRow {
    spans: u64,
    elements: u64,
    aligned: u64,
}

/// Issue #556 criterion 1. One ingested span round-trips EVERY element at
/// its own position, NULLs included, read back with a literal `SELECT` of
/// the five arrays through the product ingest path.
///
/// The fixture carries all four sources: 2 resource attrs, 3 span attrs, 1
/// instrumentation attr, one event (2 intrinsics + 1 attr) and one link (2
/// intrinsics + 1 attr) — 12 elements on span A and 3 on span B, which is
/// the same 15 rows `trace_attrs_idx` holds.
///
/// `attr_num` is compared as BITS, not as values.
#[tokio::test]
async fn attribute_arrays_round_trip_every_element_at_its_own_position() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_write_it_trace_attr_arrays");
    let client = fresh_db(db).await;

    let (req, span_count, attr_count) = fixture_request_rebased_to_now();
    assert_eq!(span_count, 2);
    assert_eq!(attr_count, 15);

    let writer = Arc::new(TraceWriter::new_with_tables(
        Arc::new(ChClient::new(db_config(db)).await.expect("connect writer")),
        &WriterConfig::default(),
        TraceWriterTables::traces_default(),
    ));
    let router: Router = Router::new()
        .route("/v1/traces", post(traces::<TraceWriter>))
        .with_state(writer);
    let request = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .body(Body::from(req.encode_to_vec()))
        .expect("build request");
    let response = router.oneshot(request).await.expect("router call");
    assert_eq!(response.status(), axum::http::StatusCode::OK);

    let sql = format!(
        "SELECT name, attr_key, attr_scope, attr_val, attr_type, \
         arrayMap(x -> if(isNull(x), NULL, reinterpretAsUInt64(assumeNotNull(x))), attr_num) \
         AS attr_num_bits \
         FROM {db}.trace_spans ORDER BY name"
    );
    let mut stream = client
        .query_stream::<ArrayRow>(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("select arrays failed: {e}\nSQL:\n{sql}"));
    let mut rows = Vec::new();
    while let Some(row) = stream.next().await {
        rows.push(row.expect("decode array row"));
    }
    assert_eq!(rows.len(), 2);

    let s = |xs: &[&str]| xs.iter().map(|x| x.to_string()).collect::<Vec<String>>();
    assert_eq!(
        rows[0],
        ArrayRow {
            name: "GET /checkout".to_string(),
            attr_key: s(&[
                "service.name",
                "deployment.environment",
                "http.status_code",
                "http.method",
                "deployment.environment",
                "scope.only.attr",
                "name",
                "timeSinceStart",
                "exception.type",
                "spanID",
                "traceID",
                "link.relation",
            ]),
            attr_scope: s(&[
                "resource",
                "resource",
                "span",
                "span",
                "span",
                "instrumentation",
                "event:intrinsic",
                "event:intrinsic",
                "event",
                "link:intrinsic",
                "link:intrinsic",
                "link",
            ]),
            attr_val: s(&[
                "checkout",
                "prod",
                "500",
                "GET",
                "prod",
                "payload-only",
                "exception",
                "3000000",
                "IOError",
                "0123456789abcdef",
                "aabbccddeeff00112233445566778899",
                "child_of",
            ]),
            attr_type: s(&[
                "string", "string", "int", "string", "string", "string", "string", "int", "string",
                "string", "string", "string",
            ]),
            attr_num_bits: vec![
                None,
                None,
                Some(500.0f64.to_bits()),
                None,
                None,
                None,
                None,
                Some(3_000_000.0f64.to_bits()),
                None,
                None,
                None,
                None,
            ],
        }
    );
    assert_eq!(
        rows[1],
        ArrayRow {
            name: "charge-card".to_string(),
            attr_key: s(&["service.name", "deployment.environment", "scope.only.attr"]),
            attr_scope: s(&["resource", "resource", "instrumentation"]),
            attr_val: s(&["checkout", "prod", "payload-only"]),
            attr_type: s(&["string", "string", "string"]),
            attr_num_bits: vec![None, None, None],
        }
    );

    // All five lengths equal on every row, and the elements total the index
    // rows: the arrays and `trace_attrs_idx` hold the same 15 attributes.
    let sql = format!(
        "SELECT count() AS spans, sum(length(attr_key)) AS elements, \
         countIf(length(attr_key) = length(attr_scope) AND length(attr_key) = length(attr_val) \
         AND length(attr_key) = length(attr_type) AND length(attr_key) = length(attr_num)) \
         AS aligned FROM {db}.trace_spans"
    );
    let mut stream = client
        .query_stream::<AlignmentRow>(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("alignment query failed: {e}\nSQL:\n{sql}"));
    let agg = stream.next().await.expect("one row").expect("decode");
    assert_eq!(agg.spans, 2);
    assert_eq!(agg.elements, 15, "one element per indexed attribute");
    assert_eq!(agg.aligned, 2, "every row's five lengths are equal");
    let idx = count(
        &client,
        &format!("SELECT count() AS n FROM {db}.trace_attrs_idx"),
    )
    .await;
    assert_eq!(idx, 15, "the index still holds the rows it always did");
}

/// Issue #556 criterion 2. A misaligned direct `INSERT` — five arrays of
/// lengths 2, 2, 2, 2, **1** — is refused by migration 59's constraint.
///
/// Asserted on the CODE and the CONSTRAINT NAME only. The server appends
/// the table's UUID after its name and a wait-phase clause
/// (`: While executing WaitForAsyncInsert.`) that depends on the insert
/// path, and our own client redacts the version; a test written against
/// the message as printed fails on its first run.
#[tokio::test]
async fn a_misaligned_direct_insert_is_rejected_with_code_469() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_write_it_trace_attr_misaligned");
    let client = fresh_db(db).await;

    let now_ns = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64");
    let sql = format!(
        "INSERT INTO {db}.trace_spans (trace_id, span_id, parent_id, name, service, \
         timestamp_ns, duration_ns, status_code, kind, payload_type, payload, \
         attr_key, attr_scope, attr_val, attr_type, attr_num) \
         VALUES (unhex('0102030405060708090a0b0c0d0e0f10'), unhex('0102030405060708'), \
         unhex('0000000000000000'), 'n', 's', {now_ns}, 1, 0, 2, 1, '', \
         ['a','b'], ['span','span'], ['1','2'], ['int','int'], [1])"
    );
    let err = client
        .execute(&sql, &QuerySettings::new(), Idempotency::NonIdempotent)
        .await
        .expect_err("a misaligned row must be refused");
    match err {
        pulsus_clickhouse::ChError::Server { code, ref message } => {
            assert_eq!(code, 469, "VIOLATED_CONSTRAINT; got: {err}");
            assert!(
                message.contains("attr_arrays_aligned"),
                "the refusal must name the constraint; got: {message}"
            );
        }
        other => panic!("expected a server error, got: {other}"),
    }
    let stored = count(
        &client,
        &format!("SELECT count() AS n FROM {db}.trace_spans"),
    )
    .await;
    assert_eq!(stored, 0, "nothing is stored when the constraint refuses");

    // And the insert every existing suite sends — naming no array column at
    // all — is still accepted: the constraint is 0 = 0 = 0 = 0 = 0.
    let sql = format!(
        "INSERT INTO {db}.trace_spans (trace_id, span_id, parent_id, name, service, \
         timestamp_ns, duration_ns, status_code, kind, payload_type, payload) \
         VALUES (unhex('0102030405060708090a0b0c0d0e0f10'), unhex('0102030405060708'), \
         unhex('0000000000000000'), 'n', 's', {now_ns}, 1, 0, 2, 1, '')"
    );
    client
        .execute(&sql, &QuerySettings::new(), Idempotency::NonIdempotent)
        .await
        .expect("a row naming no array column is accepted");
    let stored = count(
        &client,
        &format!(
            "SELECT count() AS n FROM {db}.trace_spans \
             WHERE length(attr_key) = 0 AND length(attr_num) = 0"
        ),
    )
    .await;
    assert_eq!(stored, 1, "it reads back with five empty arrays");
}
