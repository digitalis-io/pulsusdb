//! Live end-to-end test for issue #140's headline AC: a Prometheus
//! remote-write native histogram carrying a **GAUGE reset hint** lands
//! `metric_hist_samples.counter_reset_hint = 3` through the real ingest
//! stack (`ingest_remote_write` → parse → [`MetricWriter`] → ClickHouse),
//! and an UNKNOWN-hint sibling lands `0`. Also proves the stored row
//! decodes back through `HistogramColumns → NativeHistogram → to_float()`
//! with `CounterResetHint::Gauge`, and that the histograms-only series
//! registered a `metric_series` `value_type = 1` row.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, harness mirrors
//! `tests/live_metric_hist_writer.rs`:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-write --test live_remote_write_hist
//! podman rm -f pulsus-ch-test
//! ```

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue};
use futures::StreamExt;
use prost::Message;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_config::WriterConfig;
use pulsus_model::{CounterResetHint, DEFAULT_ACTIVITY_BUCKET_MS, HistogramColumns};
use pulsus_schema::{RenderCtx, run_init};
use pulsus_write::protocols::remote_write::{
    BucketSpan, Histogram, HistogramCount, Label, TimeSeries, WriteRequest,
};
use pulsus_write::{MetricHistSampleRow, MetricWriter, MetricWriterTables, ingest_remote_write};

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
}

fn test_config(database: &str) -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: database.to_string(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(20),
        ..ChConnConfig::default()
    }
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-write/tests/live_remote_write_hist.rs for setup)"
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

async fn init_db(bootstrap: &ChClient, db: &str) -> Arc<ChClient> {
    drop_database(bootstrap, db).await;
    let params = RenderCtx {
        db: db.to_string(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
    };
    run_init(bootstrap, &params).await.expect("run_init");
    Arc::new(
        ChClient::new(test_config(db))
            .await
            .expect("connect (target db)"),
    )
}

/// A near-now millisecond timestamp: within the 7-day retention TTL so a
/// background merge never drops the row before the test reads it back.
fn recent_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_millis(),
    )
    .expect("now fits i64")
}

/// A valid integer wire histogram: positive absolute counts [1, 2, 1]
/// (deltas [1, 1, -1]), count 4, sum 5.
fn wire_hist(reset_hint: i32, timestamp: i64) -> Histogram {
    Histogram {
        count: Some(HistogramCount::Int(4)),
        sum: 5.0,
        schema: 0,
        zero_count: Some(HistogramCount::Int(0)),
        positive_spans: vec![BucketSpan {
            offset: 0,
            length: 3,
        }],
        positive_deltas: vec![1, 1, -1],
        reset_hint,
        timestamp,
        ..Default::default()
    }
}

fn series(name: &str, hint_label: &str, histograms: Vec<Histogram>) -> TimeSeries {
    TimeSeries {
        labels: vec![
            Label {
                name: "__name__".to_string(),
                value: name.to_string(),
            },
            Label {
                name: "hint".to_string(),
                value: hint_label.to_string(),
            },
        ],
        samples: vec![],
        histograms,
    }
}

async fn select_hist_rows(client: &ChClient, db: &str, name: &str) -> Vec<MetricHistSampleRow> {
    let sql = format!(
        "SELECT metric_name, fingerprint, unix_milli, schema, zero_threshold, zero_count, count, \
         sum, pos_span_offsets, pos_span_lengths, pos_bucket_deltas, neg_span_offsets, \
         neg_span_lengths, neg_bucket_deltas, custom_values, counter_reset_hint \
         FROM {db}.metric_hist_samples WHERE metric_name = '{name}' ORDER BY fingerprint"
    );
    let mut stream = client
        .query_stream::<MetricHistSampleRow>(&sql, &QuerySettings::new())
        .await
        .expect("query metric_hist_samples");
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode MetricHistSampleRow"));
    }
    out
}

/// Issue #140 headline AC: a gauge-hint wire native histogram lands
/// `counter_reset_hint = 3` end-to-end; the unknown-hint sibling lands `0`;
/// the stored row decodes to a `Gauge` `FloatHistogram`; and the series
/// registered a `value_type = 1` `metric_series` row.
#[tokio::test]
async fn gauge_hint_native_histogram_lands_counter_reset_hint_3_end_to_end() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_write_it_rw_hist_gauge";
    let client = init_db(&bootstrap, db).await;
    let writer = MetricWriter::new_with_tables(
        client.clone(),
        &WriterConfig::default(),
        DEFAULT_ACTIVITY_BUCKET_MS,
        MetricWriterTables::metrics_default(),
    );

    // One gauge-hint and one unknown-hint integer native histogram, distinct
    // series of the same metric, POSTed as a real snappy prompb body through
    // the real remote-write handler in sync mode (no X-Pulsus-Async header ⇒
    // the response confirms the flush).
    let ts = recent_ms();
    let req = WriteRequest {
        timeseries: vec![
            series("rw_gauge_probe", "gauge", vec![wire_hist(3, ts)]),
            series("rw_gauge_probe", "unknown", vec![wire_hist(0, ts)]),
        ],
        metadata: vec![],
    };
    let compressed = snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy compress");
    let mut headers = HeaderMap::new();
    headers.insert("content-encoding", HeaderValue::from_static("snappy"));
    let response = ingest_remote_write(&writer, headers, Body::from(compressed)).await;
    assert_eq!(
        response.status(),
        axum::http::StatusCode::NO_CONTENT,
        "sync-mode remote write with native histograms must return 204"
    );
    writer.shutdown(Duration::from_secs(5)).await;

    // Both rows landed, with the pinned counter_reset_hint bytes.
    let rows = select_hist_rows(&client, db, "rw_gauge_probe").await;
    assert_eq!(rows.len(), 2, "both histogram rows round-trip");
    let hints: Vec<u8> = rows.iter().map(|r| r.counter_reset_hint).collect();
    assert!(
        hints.contains(&3),
        "the gauge-hint sample must store counter_reset_hint = 3, got {hints:?}"
    );
    assert!(
        hints.contains(&0),
        "the unknown-hint sample must store counter_reset_hint = 0, got {hints:?}"
    );

    // The stored gauge row decodes back through the model round-trip to a
    // Gauge FloatHistogram (the read path issue #125 threads).
    let gauge_row = rows
        .iter()
        .find(|r| r.counter_reset_hint == 3)
        .expect("gauge row present");
    let decoded = pulsus_model::NativeHistogram::from_columns(&HistogramColumns {
        schema: gauge_row.schema,
        zero_threshold: gauge_row.zero_threshold,
        zero_count: gauge_row.zero_count,
        count: gauge_row.count,
        sum: gauge_row.sum,
        pos_span_offsets: gauge_row.pos_span_offsets.clone(),
        pos_span_lengths: gauge_row.pos_span_lengths.clone(),
        pos_bucket_deltas: gauge_row.pos_bucket_deltas.clone(),
        neg_span_offsets: gauge_row.neg_span_offsets.clone(),
        neg_span_lengths: gauge_row.neg_span_lengths.clone(),
        neg_bucket_deltas: gauge_row.neg_bucket_deltas.clone(),
        custom_values: gauge_row.custom_values.clone(),
        counter_reset_hint: gauge_row.counter_reset_hint,
    })
    .expect("decode stored columns");
    assert_eq!(
        decoded.to_float().counter_reset_hint,
        CounterResetHint::Gauge,
        "stored 3 decodes to Gauge on the query-time FloatHistogram"
    );
    assert_eq!(gauge_row.count, 4);
    assert_eq!(gauge_row.pos_bucket_deltas, vec![1, 1, -1]);
    assert_eq!(gauge_row.sum.to_bits(), 5.0f64.to_bits());

    // The histograms-only series registered metric_series rows with the
    // histogram value_type discriminator (= 1).
    #[derive(pulsus_clickhouse::Row, serde::Serialize, serde::Deserialize, Debug)]
    struct CountRow {
        n: u64,
    }
    let sql = format!(
        "SELECT count() AS n FROM {db}.metric_series \
         WHERE metric_name = 'rw_gauge_probe' AND value_type = 1"
    );
    let mut stream = client
        .query_stream::<CountRow>(&sql, &QuerySettings::new())
        .await
        .expect("value_type query");
    let n = stream.next().await.expect("one row").expect("decode").n;
    assert_eq!(
        n, 2,
        "both remote-write histogram series must register value_type = 1 metric_series rows"
    );

    drop_database(&bootstrap, db).await;
}
