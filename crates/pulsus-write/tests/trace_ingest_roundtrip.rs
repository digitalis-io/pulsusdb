//! OTLP traces ingest round-trip (issue #54 AC5, re-pointed by issue
//! #602): the committed fixture POSTs through the product path — `POST
//! /v1/traces` -> the landing decode -> `TraceWriter` (sync mode) ->
//! `trace_landing` and its views — against a fresh, isolated database, then
//! asserts **exact** counts on the tables the views fill. No poll is needed:
//! the sync `FlushWait` resolves only after the landing insert, and with it
//! every view's insert, has committed.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, same harness as
//! `ingest_fidelity.rs`:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:26.8.21.10
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
use prost::Message;
use tower::ServiceExt;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_config::WriterConfig;
use pulsus_schema::{RenderCtx, SchemaParams};
use pulsus_schema_testkit::run_init;
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
    RenderCtx::for_tests(db)
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

/// Loads the committed fixture and rebases every span's and event's
/// timestamps onto "now", keeping each offset and duration: the span table
/// carries a `ttl_only_drop_parts = 1` delete-TTL, so the fixture's fixed
/// 2023 literals would land in an already-expired part.
fn fixture_request_rebased_to_now() -> (ExportTraceServiceRequest, usize) {
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
    for rs in &mut req.resource_spans {
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
                for event in &mut span.events {
                    let event_offset = event.time_unix_nano - span.start_time_unix_nano;
                    event.time_unix_nano = now_ns + offset + event_offset;
                }
                span.start_time_unix_nano = now_ns + offset;
                span.end_time_unix_nano = now_ns + offset + duration;
                span_count += 1;
            }
        }
    }
    (req, span_count)
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

/// AC5 (issue #54, re-pointed by issue #602): sync POST through the full
/// product path, then exact counts on every table the views fill from one
/// landing block.
#[tokio::test]
async fn sync_post_round_trips_exact_counts_on_the_span_tables() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_write_it_trace_roundtrip");
    let client = fresh_db(db).await;

    let (req, span_count) = fixture_request_rebased_to_now();
    assert_eq!(span_count, 2);

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
    // No X-Pulsus-Async header: sync mode — the 200 means the landing
    // insert and every view's insert have committed.
    let response = router.oneshot(request).await.expect("router call");
    assert_eq!(
        response.status(),
        axum::http::StatusCode::OK,
        "product ingest path must accept the fixture"
    );

    // Hand-derived from the fixture: two spans of one resource; eight
    // distinct (scope, key) pairs and eight distinct (scope, key, value,
    // type) rows (`trace_ingest_fidelity.rs` lists them).
    for (table, want) in [
        ("trace_landing", 2 + 1 + 8 + 8),
        ("spans", 2),
        ("resources", 1),
        ("tag_names", 8),
        ("tag_values", 8),
        ("error_spans", 1),
    ] {
        let got = count(&client, &format!("SELECT count() AS n FROM {db}.{table}")).await;
        assert_eq!(got, want, "{table} after one sync push of the fixture");
    }

    // The ids round-trip through `FixedString(N)` and the parent link holds.
    let linked = count(
        &client,
        &format!(
            "SELECT count() AS n FROM {db}.spans AS c INNER JOIN {db}.spans AS p \
             ON c.parent_span_id = p.span_id AND c.trace_id = p.trace_id \
             WHERE hex(c.trace_id) = '4BF92F3577B34DA6A3CE929D0E0E4736' \
               AND hex(c.span_id) = '3022840F1D1E9A7D' AND hex(p.span_id) = '00F067AA0BA902B7'"
        ),
    )
    .await;
    assert_eq!(linked, 1, "span B's stored parent is span A");

    // The same verbatim key at two scopes is two catalog rows.
    let dual_scopes = count(
        &client,
        &format!(
            "SELECT countDistinct(scope) AS n FROM {db}.tag_values \
             WHERE key = 'deployment.environment' AND value = 'prod'"
        ),
    )
    .await;
    assert_eq!(
        dual_scopes, 2,
        "the same verbatim (key, value) at both scopes must land as two scoped rows"
    );
}

/// Issue #184 AC5 (ingest side): a non-empty OTLP `Status.message`
/// round-trips through the wire path (`POST /v1/traces` → the landing
/// decode → `TraceWriter`) into `spans.status_message`, verbatim; spans
/// without a Status land `''`. The committed fixture is untouched — the decoded request is
/// mutated in memory.
#[tokio::test]
async fn status_message_round_trips_through_the_product_ingest_path() {
    skip_unless_live!();
    use opentelemetry_proto::tonic::trace::v1::Status;
    let db = &pulsus_testkit::test_db("pulsus_write_it_trace_status_msg");
    let client = fresh_db(db).await;

    let (mut req, span_count) = fixture_request_rebased_to_now();
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
            "SELECT count() AS n FROM {db}.spans \
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
        &format!("SELECT count() AS n FROM {db}.spans WHERE status_message = ''"),
    )
    .await;
    assert_eq!(empty, 1, "a span without a Status stores the '' default");
}
