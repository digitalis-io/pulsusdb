//! Log-pattern ingest fidelity gate (M7-C3, issue #171). Proves that the SAME
//! logical log lines produce byte-identical `log_patterns`
//! `(fingerprint, bucket_ns, pattern, count)` rows whether they arrive via
//! **OTLP** (`otlp_logs::parse`) or the **Loki push** transport
//! (`loki_push::parse_json`) — both asserted against a third, independent
//! **hand-derived golden**, not `A == B` (which could mask a shared bug). The
//! `fingerprint` alone is derived from an independent oracle — ClickHouse's own
//! `cityHash64` over the documented buffer layout — since a 64-bit hash is
//! infeasible to hand-compute (the `ingest_fidelity.rs` convention).
//!
//! Also proves the replay semantics (AC 2b iii): an explicit re-admit of an
//! identical batch (a client re-send simulation) SUMS the pattern counts, and
//! the `log_metrics_<res>` rollup inflates by the same factor on the same event
//! — pinning the documented `log_metrics`-parity best-effort-approximate
//! semantics.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`.
//!
//! Run locally:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-write --test log_patterns_fidelity
//! podman rm -f pulsus-ch-test
//! ```

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_config::WriterConfig;
use pulsus_schema::{RenderCtx, SchemaParams, run_init};
use pulsus_write::{LogWriter, ParsedLogs, WriterTables};

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
}

fn test_config() -> ChConnConfig {
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
        query_timeout: Duration::from_secs(30),
        ..ChConnConfig::default()
    }
}

fn test_ctx(db: &str) -> SchemaParams {
    RenderCtx {
        db: db.to_string(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
    }
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-write/tests/log_patterns_fidelity.rs for setup)"
            );
            return;
        }
    };
}

async fn drop_database(client: &ChClient, db: &str) {
    client
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
}

async fn fresh_db(db: &str) -> Arc<ChClient> {
    let client = Arc::new(ChClient::new(test_config()).await.expect("connect"));
    drop_database(&client, db).await;
    run_init(&client, &test_ctx(db)).await.expect("schema init");
    client
}

/// A ClickHouse client scoped to `db` (`?database=db`) — the writer's
/// `insert_block` prepends the CONNECTION database to its bare table names
/// (and the `clickhouse` crate `DESCRIBE`s the table on that connection
/// first), so the writer must run against a db-scoped connection, exactly as
/// the production server's writer targets the configured database.
async fn db_client(db: &str) -> Arc<ChClient> {
    Arc::new(
        ChClient::new(ChConnConfig {
            database: db.to_string(),
            ..test_config()
        })
        .await
        .expect("connect (db-scoped)"),
    )
}

/// A wall-clock-recent nanosecond timestamp (the `log_patterns` TTL is 7 days;
/// the reads below happen immediately, but a recent timestamp also keeps the
/// fixture inside any near-now retention window).
fn recent_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64")
        - 60_000_000_000 // one minute ago
}

const SERVICE: &str = "checkout";
// Three lines: two share a template (digit id varies), one is distinct.
const LINE_A1: &str = "user 12 login ok";
const LINE_A2: &str = "user 34 login ok";
const LINE_B: &str = "cache miss for widgets";

// The hand-derived templates (D1 rules): a digit-bearing token → `<_>`.
const TEMPLATE_A: &str = "user <_> login ok";
const TEMPLATE_B: &str = "cache miss for widgets";

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
        key_strindex: 0,
    }
}

fn otlp_request(ts_ns: i64) -> ExportLogsServiceRequest {
    let record = |body: &str| LogRecord {
        time_unix_nano: ts_ns as u64,
        observed_time_unix_nano: ts_ns as u64,
        severity_number: 9,
        body: Some(AnyValue {
            value: Some(Value::StringValue(body.to_string())),
        }),
        ..Default::default()
    };
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![kv("service.name", SERVICE)],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![record(LINE_A1), record(LINE_A2), record(LINE_B)],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn loki_push_json(ts_ns: i64) -> Vec<u8> {
    let val = |line: &str| format!(r#"["{ts_ns}","{line}"]"#);
    format!(
        r#"{{"streams":[{{"stream":{{"service_name":"{SERVICE}"}},"values":[{},{},{}]}}]}}"#,
        val(LINE_A1),
        val(LINE_A2),
        val(LINE_B)
    )
    .into_bytes()
}

async fn admit_and_drain(db: &str, batch: ParsedLogs) {
    let writer = LogWriter::new_with_tables(
        db_client(db).await,
        &WriterConfig::default(),
        WriterTables::logs_default(),
    );
    pulsus_write::LogSink::admit(&writer, batch).expect("queue has room");
    writer.shutdown(Duration::from_secs(10)).await;
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
struct PatternRow {
    fingerprint: u64,
    bucket_ns: i64,
    pattern: String,
    count: u64,
}

async fn read_patterns(client: &ChClient, db: &str) -> Vec<PatternRow> {
    let sql = format!(
        "SELECT fingerprint, bucket_ns, pattern, sum(count) AS count FROM {db}.log_patterns \
         GROUP BY fingerprint, bucket_ns, pattern ORDER BY pattern"
    );
    let mut stream = client
        .query_stream::<PatternRow>(&sql, &QuerySettings::new())
        .await
        .expect("read log_patterns");
    let mut out = Vec::new();
    while let Some(r) = stream.next().await {
        out.push(r.expect("decode pattern row"));
    }
    out
}

/// The stream fingerprint via a live `cityHash64` oracle over the documented
/// buffer layout (`key ++ 0xFF ++ value ++ 0xFF`) — NOT
/// `pulsus_model::stream_fingerprint` (the `ingest_fidelity.rs` non-tautology
/// rule). Only one label here (`service_name=checkout`).
async fn ch_fingerprint(client: &ChClient) -> u64 {
    #[derive(Row, serde::Serialize, serde::Deserialize)]
    struct FpRow {
        fp: u64,
    }
    let sql = format!(
        "SELECT cityHash64(concat('service_name', unhex('FF'), '{SERVICE}', unhex('FF'))) AS fp"
    );
    let mut stream = client
        .query_stream::<FpRow>(&sql, &QuerySettings::new())
        .await
        .expect("cityHash64 oracle");
    stream.next().await.expect("one row").expect("decode").fp
}

#[tokio::test]
async fn otlp_and_loki_push_land_identical_hand_derived_pattern_rows() {
    skip_unless_live!();
    let ts_ns = recent_ns();
    let bucket_ns = (ts_ns / 10_000_000_000) * 10_000_000_000;

    // -- OTLP transport --------------------------------------------------
    let otlp_db = format!("pulsus_patterns_fid_otlp_it_{}", std::process::id());
    let client = fresh_db(&otlp_db).await;
    let fp = ch_fingerprint(&client).await;
    let otlp_batch = pulsus_write::parse(&otlp_request(ts_ns), ts_ns).expect("otlp parse");
    admit_and_drain(&otlp_db, otlp_batch).await;
    let otlp_rows = read_patterns(&client, &otlp_db).await;

    // -- Loki push transport ---------------------------------------------
    let loki_db = format!("pulsus_patterns_fid_loki_it_{}", std::process::id());
    let loki_client = fresh_db(&loki_db).await;
    let loki_batch =
        pulsus_write::parse_loki_json(&loki_push_json(ts_ns), ts_ns).expect("loki parse");
    admit_and_drain(&loki_db, loki_batch).await;
    let loki_rows = read_patterns(&loki_client, &loki_db).await;

    // -- Hand-derived golden ---------------------------------------------
    let golden = vec![
        PatternRow {
            fingerprint: fp,
            bucket_ns,
            pattern: TEMPLATE_B.to_string(),
            count: 1,
        },
        PatternRow {
            fingerprint: fp,
            bucket_ns,
            pattern: TEMPLATE_A.to_string(),
            count: 2, // LINE_A1 + LINE_A2 share a template
        },
    ];

    assert_eq!(
        otlp_rows, golden,
        "OTLP-ingested patterns must match the golden"
    );
    assert_eq!(
        loki_rows, golden,
        "Loki-push-ingested patterns must match the golden byte-for-byte"
    );

    drop_database(&client, &otlp_db).await;
    drop_database(&loki_client, &loki_db).await;
}

#[tokio::test]
async fn re_admitting_an_identical_batch_sums_counts_at_log_metrics_parity() {
    skip_unless_live!();
    let ts_ns = recent_ns();
    let db = format!("pulsus_patterns_replay_it_{}", std::process::id());
    let client = fresh_db(&db).await;

    // Client re-send simulation: admit the SAME OTLP batch twice.
    for _ in 0..2 {
        let batch = pulsus_write::parse(&otlp_request(ts_ns), ts_ns).expect("otlp parse");
        admit_and_drain(&db, batch).await;
    }
    // Force the rollup MV's parts to merge so the sum is observable.
    client
        .execute(
            &format!("OPTIMIZE TABLE {db}.log_patterns FINAL"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("optimize patterns");

    let rows = read_patterns(&client, &db).await;
    let pattern_total: u64 = rows.iter().map(|r| r.count).sum();
    // 3 lines × 2 admits = 6 total pattern-count units.
    assert_eq!(
        pattern_total, 6,
        "a re-admitted batch sums pattern counts (best-effort-approximate under re-send)"
    );

    // Cross-check: the log_metrics rollup inflates by the SAME factor on the
    // same event (the documented parity). 3 lines × 2 admits = 6 log lines.
    #[derive(Row, serde::Serialize, serde::Deserialize)]
    struct CountRow {
        c: u64,
    }
    let mv_sql = format!("SELECT sum(count) AS c FROM {db}.log_metrics_5s");
    let mut stream = client
        .query_stream::<CountRow>(&mv_sql, &QuerySettings::new())
        .await
        .expect("read log_metrics_5s");
    let mv_total = stream.next().await.expect("one row").expect("decode").c;
    assert_eq!(
        mv_total, pattern_total,
        "log_metrics rollup must inflate by the same factor as log_patterns on a client re-send"
    );

    drop_database(&client, &db).await;
}
