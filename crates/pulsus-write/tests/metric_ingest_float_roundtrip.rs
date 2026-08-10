//! Live OTLP/JSON metrics ingest: the stored value's BITS (issue #270).
//!
//! `otlp_json_float_roundtrip.rs` pins the decode; this suite closes the loop
//! by pushing the same discriminating literals through the real product path —
//! `POST /v1/metrics` with `Content-Type: application/json` ->
//! `otlp_metrics::decode_json` -> `otlp_metrics::parse` -> `MetricWriter`
//! (sync) -> ClickHouse — and reading each sample back as
//! `reinterpretAsUInt64(value)`.
//!
//! Reading the bits **in SQL** rather than as an `f64` keeps the assertion
//! independent of every client-side float rendering and parse: ClickHouse
//! hands back a `UInt64` whose value is the `Float64` column's storage bits,
//! so nothing between the wire literal and the assertion can round.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, same harness as
//! `trace_ingest_roundtrip.rs`:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-write --test metric_ingest_float_roundtrip
//! podman rm -f pulsus-ch-test
//! ```

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::routing::post;
use futures::StreamExt;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_config::WriterConfig;
use pulsus_model::DEFAULT_ACTIVITY_BUCKET_MS;
use pulsus_schema::{RenderCtx, SchemaParams, run_init};
use pulsus_write::ingest::http::metrics;
use pulsus_write::{MetricWriter, MetricWriterTables};

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
                 (see crates/pulsus-write/tests/metric_ingest_float_roundtrip.rs for setup)"
            );
            return;
        }
    };
}

/// Decimal literals on which the correctly-rounded parser and `serde_json`'s
/// default (non-`float_roundtrip`) parser return different `f64`s, paired
/// with the nearest-representable bits. Same provenance and the same reason
/// for existing as the table in `otlp_json_float_roundtrip.rs`: a literal both
/// parsers agree on would make this whole suite vacuous.
///
/// A subset is enough here — this leg proves the decoded value reaches
/// storage unaltered, and the decode itself is enumerated field-by-field in
/// the hermetic suite.
const VECTORS: &[(&str, u64)] = &[
    ("0.0018322491389592419", 0x3f5e_0502_8851_2b04),
    ("1.9816883557688978", 0x3fff_b4fe_d96e_434b),
    ("-1774.1730603736187", 0xc09b_b8b1_36bd_13b4),
    ("1066074736.6241531", 0x41cf_c581_384f_e440),
];

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

async fn drop_database(db: &str) {
    let admin = ChClient::new(base_config()).await.expect("connect admin");
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
}

/// The `metric_samples` `value` column read back as its raw storage bits.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SampleBitsRow {
    unix_milli: i64,
    bits: u64,
}

/// One gauge data point per vector, one millisecond apart, based at "now" so
/// the rows land in a live partition — `metric_samples` carries a
/// `ttl_only_drop_parts = 1` delete-TTL, so fixed past literals would be
/// written into an already-expired part.
fn gauge_body(base_ms: i64) -> Vec<u8> {
    let points: Vec<String> = VECTORS
        .iter()
        .enumerate()
        .map(|(i, (lex, _))| {
            let ns = (base_ms + i as i64) * 1_000_000;
            format!(r#"{{"timeUnixNano":"{ns}","asDouble":{lex}}}"#)
        })
        .collect();
    format!(
        r#"{{"resourceMetrics":[{{"resource":{{"attributes":[
             {{"key":"service.name","value":{{"stringValue":"float-roundtrip"}}}}]}},
           "scopeMetrics":[{{"metrics":[
             {{"name":"pulsus_ulp_gauge_probe","gauge":{{"dataPoints":[{}]}}}}
           ]}}]}}]}}"#,
        points.join(",")
    )
    .into_bytes()
}

#[tokio::test]
async fn otlp_json_metrics_store_the_nearest_representable_f64_bits() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_write_it_metric_float_roundtrip");
    let client = fresh_db(db).await;

    let base_ms = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("fits i64");

    let writer = Arc::new(MetricWriter::new_with_tables(
        Arc::new(ChClient::new(db_config(db)).await.expect("connect writer")),
        &WriterConfig::default(),
        DEFAULT_ACTIVITY_BUCKET_MS,
        MetricWriterTables::metrics_default(),
    ));
    let router: Router = Router::new()
        .route("/v1/metrics", post(metrics::<MetricWriter>))
        .with_state(writer.clone());

    let request = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header("content-type", "application/json")
        .body(Body::from(gauge_body(base_ms)))
        .expect("build request");
    // No X-Pulsus-Async header: sync mode, so the 200 means the samples are
    // durable and the read-back below needs no settle poll.
    let response = tower::ServiceExt::oneshot(router, request)
        .await
        .expect("router call");
    assert_eq!(
        response.status(),
        axum::http::StatusCode::OK,
        "the OTLP/JSON metrics route must accept the body"
    );

    let sql = format!(
        "SELECT unix_milli, reinterpretAsUInt64(value) AS bits \
         FROM {db}.metric_samples \
         WHERE metric_name = 'pulsus_ulp_gauge_probe' ORDER BY unix_milli"
    );
    let mut rows: Vec<SampleBitsRow> = Vec::new();
    {
        let mut stream = client
            .query_stream::<SampleBitsRow>(&sql, &QuerySettings::new())
            .await
            .unwrap_or_else(|e| panic!("read back metric_samples failed: {e}\nSQL:\n{sql}"));
        while let Some(row) = stream.next().await {
            rows.push(row.expect("decode SampleBitsRow"));
        }
    }

    assert_eq!(
        rows.len(),
        VECTORS.len(),
        "every posted data point must have produced exactly one sample row"
    );
    for (i, ((lex, want), row)) in VECTORS.iter().zip(&rows).enumerate() {
        assert_eq!(
            row.unix_milli,
            base_ms + i as i64,
            "row {i} landed on an unexpected timestamp, so the pairing below would be wrong"
        );
        assert_eq!(
            row.bits, *want,
            "literal {lex} stored as 0x{:016x}, want the nearest representable 0x{want:016x}",
            row.bits
        );
    }

    writer.shutdown(Duration::from_secs(5)).await;
    drop_database(db).await;
}
