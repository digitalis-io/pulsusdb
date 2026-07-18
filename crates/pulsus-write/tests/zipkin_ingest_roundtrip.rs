//! Zipkin v2 JSON ingest round-trip (issue #75): the two Tier-1 gates for
//! the Zipkin receiver, both against a fresh, isolated database (`DROP
//! DATABASE` + `run_init`, so the T1 tables exist), driven through the real
//! product path — `pulsus_write::ingest_zipkin` -> `zipkin::to_otlp` ->
//! `otlp_traces::parse` -> `TraceWriter` (sync mode) -> ClickHouse.
//!
//! 1. **Fingerprint identity** (`zipkin_stored_spans_are_byte_identical_to_
//!    the_equivalent_otlp_ingest`): a Zipkin span and the *equivalent* OTLP
//!    span, ingested into separate databases, store byte-identical
//!    `trace_spans` summary columns (trace_id, span_id, parent_id, name,
//!    service, timestamp_ns, duration_ns, status_code, kind, payload_type)
//!    — the 64-bit trace-id left-pad and micros→nanos are the load-bearing
//!    pieces. Proves a trace is queryable identically whether it arrives via
//!    Zipkin or OTLP.
//!
//! 2. **Shared-span storage** (`a_zipkin_shared_span_stores_both_the_server_
//!    and_client_sides`): a Zipkin shared RPC span (same traceId+id, kind
//!    SERVER vs CLIENT) stores as TWO `trace_spans` rows distinguished only
//!    by `kind` — the write-side half of the trace-by-ID shared-span fix
//!    (the read-side "trace-by-ID returns both" is proven end-to-end in
//!    `pulsus-server`'s `traces_api_live.rs` and hermetically in
//!    `assemble.rs`).
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, same harness as
//! `trace_ingest_roundtrip.rs`:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-write --test zipkin_ingest_roundtrip
//! podman rm -f pulsus-ch-test
//! ```

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{HeaderMap, StatusCode};
use futures::StreamExt;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::span::SpanKind;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, TracesData};
use prost::Message;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_config::WriterConfig;
use pulsus_schema::{RenderCtx, SchemaParams, run_init};
use pulsus_write::{TraceWriter, TraceWriterTables};

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-write/tests/zipkin_ingest_roundtrip.rs for setup)"
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

async fn trace_writer(db: &str) -> Arc<TraceWriter> {
    Arc::new(TraceWriter::new_with_tables(
        Arc::new(ChClient::new(db_config(db)).await.expect("connect writer")),
        &WriterConfig::default(),
        TraceWriterTables::traces_default(),
    ))
}

/// Wall-clock now in microseconds — the fixtures use recent timestamps so
/// the 7-day delete-TTL can never drop the part underfoot (same rationale
/// as `trace_ingest_roundtrip.rs`).
fn now_micros() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_micros(),
    )
    .expect("fits i64")
}

/// The `trace_spans` summary columns — everything but the payload blob.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
struct SummaryRow {
    trace_id: [u8; 16],
    span_id: [u8; 8],
    parent_id: [u8; 8],
    name: String,
    service: String,
    timestamp_ns: i64,
    duration_ns: i64,
    status_code: i8,
    kind: i8,
    payload_type: i8,
}

async fn summary_rows(client: &ChClient, db: &str) -> Vec<SummaryRow> {
    let sql = format!(
        "SELECT trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
         status_code, kind, payload_type FROM {db}.trace_spans ORDER BY span_id"
    );
    let mut stream = client
        .query_stream::<SummaryRow>(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("summary query failed: {e}\nSQL:\n{sql}"));
    let mut rows = Vec::new();
    while let Some(row) = stream.next().await {
        rows.push(row.expect("decode summary row"));
    }
    rows
}

fn str_kv(key: &str, val: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(val.to_string())),
        }),
        key_strindex: 0,
    }
}

/// AC1 (fingerprint identity): the same logical trace, ingested once as
/// Zipkin v2 JSON and once as the equivalent OTLP, stores byte-identical
/// `trace_spans` summary columns.
#[tokio::test]
async fn zipkin_stored_spans_are_byte_identical_to_the_equivalent_otlp_ingest() {
    skip_unless_live!();

    let ts = now_micros();
    let dur = 1_500i64;

    // -- Zipkin side -----------------------------------------------------
    let zdb = "pulsus_write_it_zipkin_identity_zipkin";
    let zclient = fresh_db(zdb).await;
    let zwriter = trace_writer(zdb).await;
    let zipkin_body = format!(
        r#"[{{"traceId":"0000000000000001","id":"0000000000000002","parentId":"0000000000000003",
             "name":"op","kind":"CLIENT","timestamp":{ts},"duration":{dur},
             "localEndpoint":{{"serviceName":"svc"}}}}]"#
    );
    let res = pulsus_write::ingest_zipkin(
        zwriter.as_ref(),
        HeaderMap::new(),
        Body::from(zipkin_body.into_bytes()),
    )
    .await;
    assert_eq!(
        res.status(),
        StatusCode::ACCEPTED,
        "Zipkin ingest must return 202 Accepted"
    );

    // -- OTLP side: the hand-built equivalent ----------------------------
    let odb = "pulsus_write_it_zipkin_identity_otlp";
    let oclient = fresh_db(odb).await;
    let owriter = trace_writer(odb).await;
    let otlp = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![str_kv("service.name", "svc")],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    // 64-bit Zipkin trace id, left-padded to 16 bytes.
                    trace_id: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
                    span_id: vec![0, 0, 0, 0, 0, 0, 0, 2],
                    parent_span_id: vec![0, 0, 0, 0, 0, 0, 0, 3],
                    name: "op".to_string(),
                    kind: SpanKind::Client as i32,
                    start_time_unix_nano: (ts * 1000) as u64,
                    end_time_unix_nano: ((ts + dur) * 1000) as u64,
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/x-protobuf".parse().unwrap());
    let res =
        pulsus_write::ingest_traces(owriter.as_ref(), headers, Body::from(otlp.encode_to_vec()))
            .await;
    assert_eq!(res.status(), StatusCode::OK, "OTLP ingest must return 200");

    // -- Compare summary columns -----------------------------------------
    let zrows = summary_rows(&zclient, zdb).await;
    let orows = summary_rows(&oclient, odb).await;
    assert_eq!(zrows.len(), 1, "one Zipkin span stored");
    assert_eq!(orows.len(), 1, "one OTLP span stored");
    assert_eq!(
        zrows, orows,
        "Zipkin-ingested and OTLP-ingested summary columns must be byte-identical"
    );
    // Explicit pins on the load-bearing conversions.
    assert_eq!(
        zrows[0].trace_id,
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
        "64-bit trace id left-padded to 16 bytes"
    );
    assert_eq!(zrows[0].timestamp_ns, ts * 1000, "micros → nanos");
    assert_eq!(zrows[0].duration_ns, dur * 1000, "micros → nanos");
    assert_eq!(zrows[0].kind, SpanKind::Client as i8, "CLIENT → kind 3");
    assert_eq!(zrows[0].payload_type, 1, "adapts to OTLP payload_type 1");
}

/// AC2 (shared-span storage): a Zipkin shared RPC span (same traceId+id,
/// kind SERVER vs CLIENT) stores as TWO `trace_spans` rows separated only
/// by `kind` — neither side is dropped on the write path.
#[tokio::test]
async fn a_zipkin_shared_span_stores_both_the_server_and_client_sides() {
    skip_unless_live!();

    let ts = now_micros();
    let db = "pulsus_write_it_zipkin_shared_span";
    let client = fresh_db(db).await;
    let writer = trace_writer(db).await;

    // A single logical RPC span reported from both ends: identical
    // traceId+id, different kind; the SERVER copy carries shared:true.
    let body = format!(
        r#"[
          {{"traceId":"00000000000000010000000000000001","id":"0000000000000002",
            "name":"rpc","kind":"CLIENT","timestamp":{ts},"duration":2000,
            "localEndpoint":{{"serviceName":"frontend"}}}},
          {{"traceId":"00000000000000010000000000000001","id":"0000000000000002",
            "name":"rpc","kind":"SERVER","timestamp":{ts},"duration":1800,"shared":true,
            "localEndpoint":{{"serviceName":"backend"}}}}
        ]"#
    );
    let res = pulsus_write::ingest_zipkin(
        writer.as_ref(),
        HeaderMap::new(),
        Body::from(body.into_bytes()),
    )
    .await;
    assert_eq!(res.status(), StatusCode::ACCEPTED, "shared pair accepted");

    let rows = summary_rows(&client, db).await;
    assert_eq!(rows.len(), 2, "both shared-span sides stored");
    // Same trace_id + span_id, distinct kind (SERVER=2, CLIENT=3).
    assert_eq!(rows[0].span_id, rows[1].span_id, "same span_id");
    assert_eq!(rows[0].trace_id, rows[1].trace_id, "same trace_id");
    let mut kinds = vec![rows[0].kind, rows[1].kind];
    kinds.sort();
    assert_eq!(
        kinds,
        vec![SpanKind::Server as i8, SpanKind::Client as i8],
        "the two sides are SERVER and CLIENT"
    );

    // Each stored payload independently decodes and carries its own kind.
    #[derive(Row, serde::Serialize, serde::Deserialize)]
    struct PayloadRow {
        kind: i8,
        #[serde(with = "serde_bytes")]
        payload: Vec<u8>,
    }
    let mut stream = client
        .query_stream::<PayloadRow>(
            &format!("SELECT kind, payload FROM {db}.trace_spans"),
            &QuerySettings::new(),
        )
        .await
        .expect("select payloads");
    let mut seen = Vec::new();
    while let Some(row) = stream.next().await {
        let row = row.expect("decode payload row");
        let data = TracesData::decode(row.payload.as_slice()).expect("payload decodes");
        let span = &data.resource_spans[0].scope_spans[0].spans[0];
        assert_eq!(
            span.kind as i8, row.kind,
            "the decoded payload's kind matches the stored column"
        );
        seen.push(row.kind);
    }
    seen.sort();
    assert_eq!(seen, vec![SpanKind::Server as i8, SpanKind::Client as i8]);
}
