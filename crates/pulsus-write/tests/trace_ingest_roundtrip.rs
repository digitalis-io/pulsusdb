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
//!     clickhouse/clickhouse-server:24.8
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

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
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
            let base = ss
                .spans
                .iter()
                .map(|s| s.start_time_unix_nano)
                .min()
                .expect("fixture has spans");
            for span in &mut ss.spans {
                let offset = span.start_time_unix_nano - base;
                let duration = span.end_time_unix_nano - span.start_time_unix_nano;
                span.start_time_unix_nano = now_ns + offset;
                span.end_time_unix_nano = now_ns + offset + duration;
                span_count += 1;
                attr_count += resource_attrs + span.attributes.len();
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
    let db = "pulsus_write_it_trace_roundtrip";
    let client = fresh_db(db).await;

    let (req, span_count, attr_count) = fixture_request_rebased_to_now();
    // The committed fixture's known shape: 2 spans; 2 resource attrs x 2
    // spans + 3 span attrs on span A = 7 attr rows. Recounted from the
    // decoded fixture above so a fixture edit fails here loudly rather
    // than silently weakening the exact-count assertion.
    assert_eq!(span_count, 2);
    assert_eq!(attr_count, 7);

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
    let db = "pulsus_write_it_trace_status_msg";
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
