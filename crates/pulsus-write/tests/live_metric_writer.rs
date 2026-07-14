//! Live end-to-end test: [`MetricWriter`] against a real ClickHouse (issue
//! #26), proving `metric_metadata`'s `ReplacingMergeTree(updated_ns)`
//! collapses an A→B→A descriptor history down to a single, latest-`updated_ns`
//! row on a `FINAL` read — the fix `metric_metadata` needed (architect plan
//! amendment 1, finding 3) for its bounded last-value cache (finding 2) to
//! have a deterministic "latest" to collapse to. Gated behind
//! `PULSUS_TEST_CLICKHOUSE=1`, mirroring `pulsus-schema`'s `live_schema.rs`.
//!
//! To run these:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-write --test live_metric_writer
//! podman rm -f pulsus-ch-test
//! ```

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_config::WriterConfig;
use pulsus_model::{DEFAULT_ACTIVITY_BUCKET_MS, LabelSet};
use pulsus_schema::{RenderCtx, run_init};
use pulsus_write::{
    MetricMetadata, MetricPoint, MetricSink, MetricWriter, MetricWriterTables, ParsedMetrics,
    SeriesRef,
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
                 (see crates/pulsus-write/tests/live_metric_writer.rs for setup)"
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

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct MetricMetadataRow {
    metric_name: String,
    metric_type: String,
    help: String,
    unit: String,
    updated_ns: i64,
}

async fn metadata_rows_final(
    client: &ChClient,
    db: &str,
    metric_name: &str,
) -> Vec<MetricMetadataRow> {
    let sql = format!(
        "SELECT metric_name, metric_type, help, unit, updated_ns \
         FROM {db}.metric_metadata FINAL WHERE metric_name = '{metric_name}' ORDER BY metric_name"
    );
    let mut stream = client
        .query_stream::<MetricMetadataRow>(&sql, &QuerySettings::new())
        .await
        .expect("query metric_metadata");
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode MetricMetadataRow"));
    }
    out
}

fn metadata(metric_name: &str, metric_type: &str, updated_ns: i64) -> ParsedMetrics {
    ParsedMetrics {
        metadata: vec![MetricMetadata {
            metric_name: Arc::from(metric_name),
            metric_type: metric_type.to_string(),
            help: "help text".to_string(),
            unit: "".to_string(),
            updated_ns,
        }],
        ..Default::default()
    }
}

/// A→B→A: three sync `admit_flush` calls (type=counter, gauge, counter),
/// each with a strictly increasing `updated_ns`. `ReplacingMergeTree
/// (updated_ns)` must collapse the three physical rows down to exactly one
/// on a `FINAL` read, and that one row must carry the *last* admitted
/// value (`counter`, from the third call) — not an arbitrary/undefined
/// merge outcome (the review-cycle finding this schema fix closes).
#[tokio::test]
async fn metric_metadata_a_to_b_to_a_collapses_to_the_latest_value_on_final_read() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_write_it_metric_metadata";
    drop_database(&bootstrap, db).await;

    let params = RenderCtx {
        db: db.to_string(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
    };
    run_init(&bootstrap, &params).await.expect("run_init");

    let client = Arc::new(
        ChClient::new(test_config(db))
            .await
            .expect("connect (target db)"),
    );
    let writer = MetricWriter::new_with_tables(
        client.clone(),
        &WriterConfig::default(),
        DEFAULT_ACTIVITY_BUCKET_MS,
        MetricWriterTables::metrics_default(),
    );

    let metric_name = "http_requests_total";
    for (metric_type, updated_ns) in [("counter", 1), ("gauge", 2), ("counter", 3)] {
        let wait = writer
            .admit_flush(metadata(metric_name, metric_type, updated_ns))
            .expect("queue has room");
        tokio::time::timeout(Duration::from_secs(10), wait)
            .await
            .expect("flush settles within the test timeout")
            .expect("metadata flush succeeds");
    }

    writer.shutdown(Duration::from_secs(5)).await;

    let rows = metadata_rows_final(&client, db, metric_name).await;
    assert_eq!(
        rows.len(),
        1,
        "ReplacingMergeTree(updated_ns) must collapse the 3-row A/B/A history to 1 row on FINAL"
    );
    assert_eq!(rows[0].metric_type, "counter");
    assert_eq!(rows[0].updated_ns, 3, "the latest updated_ns must win");

    drop_database(&bootstrap, db).await;
}

/// A metadata descriptor identical to the last one durably flushed must not
/// be re-emitted (idempotence half of the architect plan's A→B→A fix): two
/// admissions of the exact same tuple leave exactly one physical row.
#[tokio::test]
async fn metric_metadata_repeated_identical_descriptor_is_idempotent() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_write_it_metric_metadata_idempotent";
    drop_database(&bootstrap, db).await;

    let params = RenderCtx {
        db: db.to_string(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
    };
    run_init(&bootstrap, &params).await.expect("run_init");

    let client = Arc::new(
        ChClient::new(test_config(db))
            .await
            .expect("connect (target db)"),
    );
    let writer = MetricWriter::new_with_tables(
        client.clone(),
        &WriterConfig::default(),
        DEFAULT_ACTIVITY_BUCKET_MS,
        MetricWriterTables::metrics_default(),
    );

    let metric_name = "up";
    let wait = writer
        .admit_flush(metadata(metric_name, "gauge", 1))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(10), wait)
        .await
        .expect("flush settles")
        .expect("first flush succeeds");

    // Second admission: identical tuple. The success-only `MetadataCache`
    // now holds the first flush's value, so this must be suppressed at
    // admission — never even reach a `metric_metadata` insert.
    writer
        .admit(metadata(metric_name, "gauge", 2))
        .expect("queue has room");

    writer.shutdown(Duration::from_secs(5)).await;

    let rows = metadata_rows_final(&client, db, metric_name).await;
    assert_eq!(
        rows.len(),
        1,
        "a repeated identical descriptor must not create a second physical row"
    );
    assert_eq!(
        rows[0].updated_ns, 1,
        "the suppressed duplicate never flushed"
    );

    drop_database(&bootstrap, db).await;
}

/// Sanity check that the writer's own registration gate agrees with
/// `metric_series`' schema-documented dedup key end to end: two samples in
/// the same activity bucket for one series register exactly one
/// `metric_series` row against a live server (the mock-based coverage in
/// `tests/metric_writer.rs` proves the LRU logic in isolation; this proves
/// the whole path, including RowBinary encoding of the canonical label
/// JSON, against real ClickHouse).
#[tokio::test]
async fn metric_series_same_bucket_samples_register_exactly_one_row() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_write_it_metric_series";
    drop_database(&bootstrap, db).await;

    let params = RenderCtx {
        db: db.to_string(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
    };
    run_init(&bootstrap, &params).await.expect("run_init");

    let client = Arc::new(
        ChClient::new(test_config(db))
            .await
            .expect("connect (target db)"),
    );
    let writer = MetricWriter::new_with_tables(
        client.clone(),
        &WriterConfig::default(),
        DEFAULT_ACTIVITY_BUCKET_MS,
        MetricWriterTables::metrics_default(),
    );

    let (labels, _) = LabelSet::from_normalized([("job".to_string(), "checkout".to_string())]);
    let metric_name: Arc<str> = Arc::from("http_requests_total");
    let series = SeriesRef {
        metric_name: metric_name.clone(),
        fingerprint: 42,
        labels,
    };
    let batch = ParsedMetrics {
        samples: vec![
            MetricPoint {
                metric_name: metric_name.clone(),
                fingerprint: 42,
                unix_milli: 0,
                value: 1.0,
            },
            MetricPoint {
                metric_name: metric_name.clone(),
                fingerprint: 42,
                unix_milli: 60_000, // same 1h bucket as unix_milli=0
                value: 2.0,
            },
        ],
        series: vec![series],
        ..Default::default()
    };

    let wait = writer.admit_flush(batch).expect("queue has room");
    tokio::time::timeout(Duration::from_secs(10), wait)
        .await
        .expect("flush settles")
        .expect("flush succeeds");

    writer.shutdown(Duration::from_secs(5)).await;

    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct CountRow {
        n: u64,
    }
    let sql =
        format!("SELECT count() AS n FROM {db}.metric_series WHERE metric_name = '{metric_name}'");
    let mut stream = client
        .query_stream::<CountRow>(&sql, &QuerySettings::new())
        .await
        .expect("query metric_series");
    let count = stream.next().await.expect("one row").expect("decode").n;
    assert_eq!(
        count, 1,
        "two same-bucket samples for one series must register exactly one metric_series row"
    );

    drop_database(&bootstrap, db).await;
}

/// Belt-and-suspenders guard test (issue #31 code review round 1, finding
/// 2 — architect adjudication REJECT with a guard test required): a
/// fingerprint's `metric_series.labels` cannot change across rows by
/// construction — `metric_fingerprint` is `hash(canonical label set)`
/// (docs/schemas.md §2.1) and the writer renders `labels` as deterministic
/// canonical JSON (sorted keys, issue #4/#26 canonicalization), so a label
/// change *is* a different fingerprint, never a new row for the same one.
/// This is what makes issue #31's `series_labels_by_fingerprint` (a plain
/// `DESC LIMIT 1 BY` hydration with no window bound) safe: whichever row
/// it picks for a fingerprint carries the same `labels` any other row for
/// that fingerprint would. Proven here against the real product write
/// path (`MetricWriter`, not a direct `insert_block`): two samples for the
/// same series in two *different* activity buckets (so two distinct
/// `metric_series` rows are registered — same fingerprint, different
/// `unix_milli`) must carry byte-identical `labels` text.
#[tokio::test]
async fn metric_series_rows_for_the_same_fingerprint_carry_byte_identical_labels() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_write_it_metric_series_label_immutability";
    drop_database(&bootstrap, db).await;

    let params = RenderCtx {
        db: db.to_string(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
    };
    run_init(&bootstrap, &params).await.expect("run_init");

    let client = Arc::new(
        ChClient::new(test_config(db))
            .await
            .expect("connect (target db)"),
    );
    let writer = MetricWriter::new_with_tables(
        client.clone(),
        &WriterConfig::default(),
        DEFAULT_ACTIVITY_BUCKET_MS,
        MetricWriterTables::metrics_default(),
    );

    let (labels, _) = LabelSet::from_normalized([("job".to_string(), "checkout".to_string())]);
    let metric_name: Arc<str> = Arc::from("http_requests_total");
    let series = SeriesRef {
        metric_name: metric_name.clone(),
        fingerprint: 4242,
        labels,
    };
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let batch = ParsedMetrics {
        samples: vec![
            MetricPoint {
                metric_name: metric_name.clone(),
                fingerprint: 4242,
                unix_milli: 0,
                value: 1.0,
            },
            MetricPoint {
                metric_name: metric_name.clone(),
                fingerprint: 4242,
                unix_milli: bucket * 5, // a distinct activity bucket
                value: 2.0,
            },
        ],
        series: vec![series],
        ..Default::default()
    };

    let wait = writer.admit_flush(batch).expect("queue has room");
    tokio::time::timeout(Duration::from_secs(10), wait)
        .await
        .expect("flush settles")
        .expect("flush succeeds");

    writer.shutdown(Duration::from_secs(5)).await;

    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct LabelsRow {
        unix_milli: i64,
        labels: String,
    }
    let sql = format!(
        "SELECT unix_milli, labels FROM {db}.metric_series \
         WHERE metric_name = '{metric_name}' AND fingerprint = 4242 ORDER BY unix_milli"
    );
    let mut stream = client
        .query_stream::<LabelsRow>(&sql, &QuerySettings::new())
        .await
        .expect("query metric_series");
    let mut rows = Vec::new();
    while let Some(row) = stream.next().await {
        rows.push(row.expect("decode LabelsRow"));
    }

    assert_eq!(
        rows.len(),
        2,
        "two distinct activity buckets must register two metric_series rows"
    );
    assert_eq!(
        rows[0].labels, rows[1].labels,
        "the same fingerprint's labels must be byte-identical across every metric_series row \
         (labels are immutable by construction — a change would be a different fingerprint)"
    );

    drop_database(&bootstrap, db).await;
}
