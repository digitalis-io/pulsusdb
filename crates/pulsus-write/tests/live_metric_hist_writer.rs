//! Live end-to-end test: the M7-A4 native-histogram write path
//! ([`MetricWriter`] → `metric_hist_samples`, issue #120) against a real
//! ClickHouse. Three binding proofs:
//!
//! 1. **ClickHouse-NaN gate (deciding, mandatory):** inserts three
//!    `metric_hist_samples` rows whose `sum` is finite / `STALE_NAN_BITS` /
//!    canonical `f64::NAN`, reads them back through the typed `Row`
//!    (RowBinary), and asserts each `sum.to_bits()` survives EXACTLY and the
//!    stale/absent NaN payloads stay DISTINCT. This is also the first live
//!    proof of the landed float-stale assumption (`staleness.rs:38` compares
//!    `to_bits() == STALE_NAN_BITS`). If ClickHouse canonicalized NaN, both
//!    the absent-sum design AND the shipped float-stale path would break —
//!    that is a cross-cutting escalation, not an A4-local fallback.
//! 2. **Native round-trip:** `otlp_metrics::parse(mode=Native)` → writer
//!    (sync flush) → SELECT reconstructs the absolute per-bucket counts
//!    (including an internal zero) bit-for-bit.
//! 3. **value_type discriminator:** a cross-request float-then-histogram at
//!    the same series lands a row in BOTH co-sharded tables, and
//!    `metric_series` carries both `value_type` rows
//!    (`groupBitOr(bitShiftLeft(1, value_type)) == 3`).
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, mirroring
//! `tests/live_metric_writer.rs`.
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-write --test live_metric_hist_writer
//! podman rm -f pulsus-ch-test
//! ```

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::exponential_histogram_data_point::Buckets;
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, ExponentialHistogram, ExponentialHistogramDataPoint, Metric,
    ResourceMetrics, ScopeMetrics, metric,
};
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_config::{ExpHistogramMode, WriterConfig};
use pulsus_model::{
    DEFAULT_ACTIVITY_BUCKET_MS, LabelSet, NativeHistogram, STALE_NAN_BITS, Span, metric_fingerprint,
};
use pulsus_schema::{RenderCtx, run_init};
use pulsus_write::protocols::otlp_metrics;
use pulsus_write::{
    HistogramPoint, MetricHistSampleRow, MetricPoint, MetricSink, MetricWriter, MetricWriterTables,
    ParsedMetrics, SeriesRef,
};

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
                 (see crates/pulsus-write/tests/live_metric_hist_writer.rs for setup)"
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
/// background merge never drops the row before the test reads it back (an
/// epoch/2023 timestamp is past its `unix_milli + 7 DAY` TTL and can be
/// TTL-dropped mid-test).
fn recent_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_millis(),
    )
    .expect("now fits i64")
}

fn writer(client: Arc<ChClient>) -> MetricWriter {
    MetricWriter::new_with_tables(
        client,
        &WriterConfig::default(),
        DEFAULT_ACTIVITY_BUCKET_MS,
        MetricWriterTables::metrics_default(),
    )
}

async fn flush(writer: &MetricWriter, batch: ParsedMetrics) {
    let wait = writer.admit_flush(batch).expect("queue has room");
    tokio::time::timeout(Duration::from_secs(10), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect("flush succeeds");
}

/// A single-histogram fixture: schema 0, positive absolute counts [1, 0, 2]
/// (an internal zero) delta-encoded to [1, -1, 2], count 3.
fn hist_with_internal_zero(sum: f64) -> NativeHistogram {
    NativeHistogram {
        counter_reset_hint: pulsus_model::CounterResetHint::Unknown,
        schema: 0,
        zero_threshold: 0.0,
        zero_count: 0,
        count: 3,
        sum,
        positive_spans: vec![Span {
            offset: 1,
            length: 3,
        }],
        negative_spans: vec![],
        positive_buckets: vec![1, -1, 2],
        negative_buckets: vec![],
        custom_values: vec![],
    }
}

fn hist_point(name: &str, fp: u64, unix_milli: i64, histogram: NativeHistogram) -> HistogramPoint {
    HistogramPoint {
        metric_name: Arc::from(name),
        fingerprint: fp,
        unix_milli,
        histogram,
    }
}

fn series_ref(name: &str, fp: u64, labels: LabelSet) -> SeriesRef {
    SeriesRef {
        metric_name: Arc::from(name),
        fingerprint: fp,
        labels,
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

/// PROOF 1 — the deciding ClickHouse-NaN gate: three sum forms (finite,
/// stale-NaN, absent-NaN) survive a real RowBinary insert→SELECT bit-for-bit
/// and stay distinct.
#[tokio::test]
async fn hist_sum_nan_payloads_survive_clickhouse_bit_for_bit_and_stay_distinct() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_write_it_hist_nan_gate";
    let client = init_db(&bootstrap, db).await;
    let writer = writer(client.clone());

    // Three series (distinct fingerprints), one per sum form. count = 3 for
    // all: the finite-sum branch needs bucket total == count, the NaN
    // branches need bucket total <= count.
    let forms: [(u64, f64); 3] = [(1, 5.0), (2, f64::from_bits(STALE_NAN_BITS)), (3, f64::NAN)];
    let ts = recent_ms();
    let mut batch = ParsedMetrics::default();
    for (fp, sum) in forms {
        let (labels, _) = LabelSet::from_normalized([("form".to_string(), fp.to_string())]);
        batch.hist_samples.push(hist_point(
            "nan_probe",
            fp,
            ts,
            hist_with_internal_zero(sum),
        ));
        batch.series.push(series_ref("nan_probe", fp, labels));
    }
    flush(&writer, batch).await;
    writer.shutdown(Duration::from_secs(5)).await;

    let rows = select_hist_rows(&client, db, "nan_probe").await;
    assert_eq!(rows.len(), 3, "three native histogram rows round-trip");
    let bits: Vec<u64> = rows.iter().map(|r| r.sum.to_bits()).collect();
    assert_eq!(bits[0], 5.0f64.to_bits(), "finite sum survives exactly");
    assert_eq!(
        bits[1], STALE_NAN_BITS,
        "stale-NaN payload survives ClickHouse Float64 storage EXACTLY (the landed \
         float-stale path depends on this)"
    );
    assert_eq!(
        bits[2],
        f64::NAN.to_bits(),
        "absent-sum quiet-NaN payload survives exactly"
    );
    assert_ne!(
        bits[1], bits[2],
        "stale-NaN and absent-NaN must remain DISTINCT through ClickHouse — if they \
         collapsed, ClickHouse canonicalizes NaN (escalate, do not silently fall back)"
    );

    drop_database(&bootstrap, db).await;
}

/// PROOF 2 — a native exp histogram round-trips OTLP → parse(Native) →
/// writer → SELECT, reconstructing the absolute per-bucket counts (incl. the
/// internal zero) exactly.
#[tokio::test]
async fn native_exp_histogram_round_trips_absolute_counts_through_clickhouse() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_write_it_hist_roundtrip";
    let client = init_db(&bootstrap, db).await;
    let writer = writer(client.clone());

    // scale 0, positive [1, 0, 2] at offset 0 (internal zero), count 3.
    let dp = ExponentialHistogramDataPoint {
        time_unix_nano: recent_ms() as u64 * 1_000_000,
        count: 3,
        sum: Some(7.5),
        positive: Some(Buckets {
            offset: 0,
            bucket_counts: vec![1, 0, 2],
        }),
        ..Default::default()
    };
    let req = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "op_seconds".to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: vec![],
                    data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                        data_points: vec![dp],
                        aggregation_temporality: AggregationTemporality::Cumulative as i32,
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    let parsed = otlp_metrics::parse(&req, 0, ExpHistogramMode::Native).expect("within the budget");
    flush(&writer, parsed).await;
    writer.shutdown(Duration::from_secs(5)).await;

    let rows = select_hist_rows(&client, db, "op_seconds").await;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.schema, 0);
    assert_eq!(row.count, 3);
    assert_eq!(row.sum.to_bits(), 7.5f64.to_bits());
    assert_eq!(row.pos_span_offsets, vec![1]);
    assert_eq!(row.pos_span_lengths, vec![3]);
    // Reconstruct absolute per-bucket counts from the stored deltas.
    let mut running = 0i64;
    let abs: Vec<i64> = row
        .pos_bucket_deltas
        .iter()
        .map(|&d| {
            running += d;
            running
        })
        .collect();
    assert_eq!(
        abs,
        vec![1, 0, 2],
        "the internal zero bucket round-trips through ClickHouse"
    );
    // Issue #125: today's OTLP ingest always writes counter_reset_hint 0
    // (Unknown; no monotonicity signal exists on the wire), and the value
    // decodes back to `CounterResetHint::Unknown` through the model's
    // column round-trip.
    assert_eq!(
        row.counter_reset_hint, 0,
        "OTLP-ingested native histograms always store hint 0 (Unknown)"
    );
    let decoded = pulsus_model::NativeHistogram::from_columns(&pulsus_model::HistogramColumns {
        schema: row.schema,
        zero_threshold: row.zero_threshold,
        zero_count: row.zero_count,
        count: row.count,
        sum: row.sum,
        pos_span_offsets: row.pos_span_offsets.clone(),
        pos_span_lengths: row.pos_span_lengths.clone(),
        pos_bucket_deltas: row.pos_bucket_deltas.clone(),
        neg_span_offsets: row.neg_span_offsets.clone(),
        neg_span_lengths: row.neg_span_lengths.clone(),
        neg_bucket_deltas: row.neg_bucket_deltas.clone(),
        custom_values: row.custom_values.clone(),
        counter_reset_hint: row.counter_reset_hint,
    })
    .expect("decode stored columns");
    assert_eq!(
        decoded.to_float().counter_reset_hint,
        pulsus_model::CounterResetHint::Unknown,
        "stored 0 decodes to Unknown on the query-time FloatHistogram"
    );

    drop_database(&bootstrap, db).await;
}

/// PROOF 3 — cross-request float-then-histogram at the same series: a row
/// lands in BOTH co-sharded tables, and `metric_series` carries both
/// `value_type` discriminator rows (mask == 3).
#[tokio::test]
async fn cross_request_float_and_histogram_register_both_value_type_rows() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_write_it_hist_value_type";
    let client = init_db(&bootstrap, db).await;
    let writer = writer(client.clone());

    let (labels, _) = LabelSet::from_normalized([("job".to_string(), "checkout".to_string())]);
    let fp = metric_fingerprint(&labels);
    let ts = recent_ms();

    // Request 1: a float sample for `svc`.
    let float_batch = ParsedMetrics {
        samples: vec![MetricPoint {
            metric_name: Arc::from("svc"),
            fingerprint: fp,
            unix_milli: ts,
            value: 1.0,
        }],
        series: vec![series_ref("svc", fp, labels.clone())],
        ..Default::default()
    };
    flush(&writer, float_batch).await;

    // Request 2: a native histogram for the SAME series+timestamp.
    let hist_batch = ParsedMetrics {
        hist_samples: vec![hist_point("svc", fp, ts, hist_with_internal_zero(5.0))],
        series: vec![series_ref("svc", fp, labels)],
        ..Default::default()
    };
    flush(&writer, hist_batch).await;
    writer.shutdown(Duration::from_secs(5)).await;

    #[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
    struct CountRow {
        n: u64,
    }
    async fn count(client: &ChClient, sql: &str) -> u64 {
        let mut stream = client
            .query_stream::<CountRow>(sql, &QuerySettings::new())
            .await
            .expect("count query");
        stream.next().await.expect("one row").expect("decode").n
    }

    // Both co-sharded tables hold their row.
    let floats = count(
        &client,
        &format!("SELECT count() AS n FROM {db}.metric_samples WHERE metric_name = 'svc'"),
    )
    .await;
    assert_eq!(floats, 1, "the float row coexists");
    let hists = count(
        &client,
        &format!("SELECT count() AS n FROM {db}.metric_hist_samples WHERE metric_name = 'svc'"),
    )
    .await;
    assert_eq!(hists, 1, "the histogram row coexists");

    // metric_series carries both value_type discriminator rows — surfaced as
    // the mixed mask (1<<0 | 1<<1 == 3). This asserts A4's write-side
    // discriminator registration only; the read path does not consult it.
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
    struct MaskRow {
        mask: u8,
    }
    let sql = format!(
        "SELECT groupBitOr(bitShiftLeft(toUInt8(1), value_type)) AS mask \
         FROM {db}.metric_series WHERE metric_name = 'svc'"
    );
    let mut stream = client
        .query_stream::<MaskRow>(&sql, &QuerySettings::new())
        .await
        .expect("mask query");
    let mask = stream.next().await.expect("one row").expect("decode").mask;
    assert_eq!(
        mask, 3,
        "A4 registered both a value_type=0 (float) and value_type=1 (histogram) metric_series row"
    );

    drop_database(&bootstrap, db).await;
}
