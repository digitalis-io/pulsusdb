//! Issue #498 criterion 6: **every `Row` struct that carries a fingerprint
//! is exercised against the `UInt128` column it reads.**
//!
//! Widening the column is a runtime contract, not a compile-time one. The
//! vendored client validates a row type against the result set's declared
//! types, and it does so **per statement**: a struct left at `u64` fails
//! on the first row of its own statement and on no other, with
//!
//! ```text
//!   Decode("schema mismatch: … attempting to (de)serialize ClickHouse
//!           type UInt128 as u64 which is not compatible")
//! ```
//!
//! So one row per TABLE proves nothing about the other structs that read
//! the same table: this suite drives one statement per STRUCT.
//!
//! **The four boundary values** are the fingerprints, at every struct:
//!
//! ```text
//!   18446744073709551615   2^64 - 1   the last value a bare literal reads exactly
//!   18446744073709551616   2^64       the first Float64
//!   18446744073709551617   2^64 + 1   the value a bare literal loses
//!   18446744073709551618   2^64 + 2   its neighbour
//! ```
//!
//! **The inventory.** Twenty-three production `Row` structs carry a
//! `fingerprint` field — ten in `logql/rows.rs`, four in
//! `metrics/sample_rows.rs`, one in `metrics/rows.rs`, two in
//! `metrics/exec.rs` and six in `pulsus-write`'s `writer/rows.rs`. Two of
//! the twenty-three were private, which is why a `pub struct` search
//! returns twenty-one; they are `pub` now with that reason recorded on
//! them. A twenty-fourth, `MetricRangeUnwrappedRow`, carries no
//! `fingerprint` field but reads the `class` column, which is the
//! fingerprint itself when the group-key plan groups per fingerprint — so
//! it is exercised here too.
//!
//! ```text
//!   podman: the shared instance, HTTP 18123
//!   PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=18123 \
//!     PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!     cargo test -p pulsus-read --test live_fingerprint_rowbinary
//! ```

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, ChRow, QuerySettings};
use pulsus_model::Fingerprint;
use pulsus_read::logql::rows as logql_rows;
use pulsus_read::metrics::exec as metrics_exec;
use pulsus_read::metrics::{rows as metrics_rows, sample_rows};
use pulsus_schema::{RenderCtx, run_init};
use pulsus_write::writer::{
    LogPatternRow, LogSampleRow, LogStreamRow, MetricHistSampleRow, MetricSampleRow,
    MetricSeriesRow,
};

/// `2^64-1`, `2^64`, `2^64+1`, `2^64+2`.
const BOUNDARY: [u128; 4] = [
    18_446_744_073_709_551_615,
    18_446_744_073_709_551_616,
    18_446_744_073_709_551_617,
    18_446_744_073_709_551_618,
];

/// The sample tables carry a delete-TTL, so the rows must be recent: a
/// fixed past instant would be dropped by the TTL before the read and the
/// suite would report an empty result rather than a width problem. Only
/// the FINGERPRINTS are pinned here; the instant is the wall clock.
fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64")
}

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test"
            );
            return;
        }
    };
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

fn fingerprints() -> Vec<Fingerprint> {
    BOUNDARY
        .iter()
        .copied()
        .map(Fingerprint::from_raw)
        .collect()
}

fn fp_in_list() -> String {
    BOUNDARY
        .iter()
        .map(|v| format!("toUInt128('{v}')"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Drains `sql` through `R`, so a struct whose fingerprint field is narrow
/// fails here rather than anywhere else.
async fn read_all<R: ChRow>(client: &ChClient, what: &str, sql: &str) -> Vec<R> {
    let mut stream = client
        .query_stream::<R>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{what}: dispatch failed: {e}\nSQL:\n{sql}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.unwrap_or_else(|e| panic!("{what}: row decode failed: {e}\nSQL:\n{sql}")));
    }
    out
}

/// The four boundary values came back, in ascending order, exactly once
/// each.
fn assert_the_four_boundary_values(what: &str, got: Vec<Fingerprint>) {
    assert_eq!(
        got,
        fingerprints(),
        "{what}: the fingerprints read back are not the four boundary values"
    );
}

async fn drop_database(client: &ChClient, db: &str) {
    client
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            pulsus_clickhouse::Idempotency::Idempotent,
        )
        .await
        .expect("drop database");
}

/// Creates the schema and writes one row per boundary value into every
/// table that carries a fingerprint, **through the writer's own `Row`
/// structs** — so the serialize direction is exercised by the same six
/// structs production inserts with.
async fn seed(client: &ChClient) {
    let ts_ns = now_ns();
    let ts_ms = ts_ns / 1_000_000;
    let streams: Vec<LogStreamRow> = fingerprints()
        .into_iter()
        .map(|fingerprint| LogStreamRow {
            month: pulsus_model::Date::start_of_month_utc(ts_ns)
                .expect("a month for now")
                .days_since_epoch(),
            fingerprint,
            service: "checkout".to_string(),
            labels: r#"{"service_name":"checkout"}"#.to_string(),
            updated_ns: ts_ns,
        })
        .collect();
    client
        .insert_block("log_streams", &streams)
        .await
        .expect("insert log_streams through LogStreamRow");

    let samples: Vec<LogSampleRow> = fingerprints()
        .into_iter()
        .map(|fingerprint| LogSampleRow {
            service: "checkout".to_string(),
            fingerprint,
            timestamp_ns: ts_ns,
            severity: 0,
            body: r#"{"latency":7}"#.to_string(),
            structured_metadata: r#"{"trace_id":"abc"}"#.to_string(),
        })
        .collect();
    client
        .insert_block("log_samples", &samples)
        .await
        .expect("insert log_samples through LogSampleRow");

    let patterns: Vec<LogPatternRow> = fingerprints()
        .into_iter()
        .map(|fingerprint| LogPatternRow {
            fingerprint,
            bucket_ns: ts_ns,
            pattern: "user <_> login".to_string(),
            count: 1,
        })
        .collect();
    client
        .insert_block("log_patterns", &patterns)
        .await
        .expect("insert log_patterns through LogPatternRow");

    let series: Vec<MetricSeriesRow> = fingerprints()
        .into_iter()
        .map(|fingerprint| MetricSeriesRow {
            metric_name: "pulsus_probe".to_string(),
            fingerprint,
            unix_milli: ts_ms,
            labels: r#"{"service_name":"checkout"}"#.to_string(),
            value_type: 0,
        })
        .collect();
    client
        .insert_block("metric_series", &series)
        .await
        .expect("insert metric_series through MetricSeriesRow");

    let metric_samples: Vec<MetricSampleRow> = fingerprints()
        .into_iter()
        .map(|fingerprint| MetricSampleRow {
            metric_name: "pulsus_probe".to_string(),
            fingerprint,
            unix_milli: ts_ms,
            value: 1.0,
        })
        .collect();
    client
        .insert_block("metric_samples", &metric_samples)
        .await
        .expect("insert metric_samples through MetricSampleRow");

    let hist: Vec<MetricHistSampleRow> = fingerprints()
        .into_iter()
        .map(|fingerprint| MetricHistSampleRow {
            metric_name: "pulsus_probe_hist".to_string(),
            fingerprint,
            unix_milli: ts_ms,
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 1,
            sum: 1.0,
            pos_span_offsets: vec![0],
            pos_span_lengths: vec![1],
            pos_bucket_deltas: vec![1],
            neg_span_offsets: Vec::new(),
            neg_span_lengths: Vec::new(),
            neg_bucket_deltas: Vec::new(),
            custom_values: Vec::new(),
            counter_reset_hint: 0,
        })
        .collect();
    client
        .insert_block("metric_hist_samples", &hist)
        .await
        .expect("insert metric_hist_samples through MetricHistSampleRow");
}

/// **The control: the client refuses a narrow field, with the message the
/// criterion names.**
///
/// Every production struct's `fingerprint` is a `Fingerprint`, and the
/// type reaches far enough that narrowing one back to `u64` is a **build**
/// failure rather than a runtime one — measured on all three tried
/// (`HistSampleRow`, `FingerprintOnlyRow`, `LogPatternRow`), each stopped
/// by `cargo` before any test ran. That is a stronger guard than this
/// suite, and it is also why the suite's own mechanism cannot be shown by
/// narrowing production code.
///
/// So the mechanism is shown on a struct this file owns: the same column,
/// the same client, a `u64` field. If this test ever stops failing, the
/// per-statement validation the whole suite rests on has gone.
#[tokio::test]
async fn a_narrow_row_field_is_refused_by_the_client() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_read_it_fp_narrow");
    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect");
    drop_database(&bootstrap, &db).await;
    run_init(
        &bootstrap,
        &RenderCtx {
            db: db.clone(),
            cluster: None,
            dist_suffix: "_dist".to_string(),
            storage_policy: None,
            retention_days: 7,
            log_rollup: Duration::from_secs(5),
            metrics_landing_retention_hours: 6,
            metrics_dedup_window: 10_000,
        },
    )
    .await
    .expect("run_init");
    let client = ChClient::new(test_config(&db))
        .await
        .expect("connect to db");
    seed(&client).await;

    #[derive(Debug, pulsus_clickhouse::Row, serde::Serialize, serde::Deserialize)]
    struct NarrowRow {
        fingerprint: u64,
    }

    let sql = format!(
        "SELECT fingerprint FROM log_streams WHERE fingerprint IN ({}) ORDER BY fingerprint",
        fp_in_list()
    );
    let mut stream = client
        .query_stream::<NarrowRow>(&sql, &QuerySettings::new())
        .await
        .expect("dispatch succeeds; the refusal is on the first row");
    let first = stream.next().await.expect("one row");
    let err = first.expect_err("a u64 field must be refused against a UInt128 column");
    let text = format!("{err}");
    // Measured verbatim on ClickHouse 26.3.29.7:
    //   decode: schema mismatch: While processing column NarrowRow.fingerprint:
    //   attempting to (de)serialize ClickHouse type UInt128 as u64 which is
    //   not compatible
    assert!(
        text.contains("attempting to (de)serialize ClickHouse type UInt128 as u64"),
        "the refusal does not name the two types: {text}"
    );
    drop(stream);

    drop_database(&bootstrap, &db).await;
}

/// One statement per struct, every struct in the inventory.
#[tokio::test]
async fn every_fingerprint_row_struct_round_trips_the_uint128_column() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_read_it_fp_rowbinary");
    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect");
    drop_database(&bootstrap, &db).await;
    run_init(
        &bootstrap,
        &RenderCtx {
            db: db.clone(),
            cluster: None,
            dist_suffix: "_dist".to_string(),
            storage_policy: None,
            retention_days: 7,
            log_rollup: Duration::from_secs(5),
            metrics_landing_retention_hours: 6,
            metrics_dedup_window: 10_000,
        },
    )
    .await
    .expect("run_init");
    let client = ChClient::new(test_config(&db))
        .await
        .expect("connect to db");

    seed(&client).await;
    let fps = fp_in_list();

    // --- crates/pulsus-read/src/logql/rows.rs (10) --------------------
    let rows: Vec<logql_rows::StreamRow> = read_all(
        &client,
        "StreamRow",
        &format!(
            "SELECT fingerprint FROM log_streams WHERE fingerprint IN ({fps}) ORDER BY fingerprint"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "StreamRow",
        rows.into_iter().map(|r| r.fingerprint).collect(),
    );

    let rows: Vec<logql_rows::StreamMetaRow> = read_all(
        &client,
        "StreamMetaRow",
        &format!(
            "SELECT fingerprint, service, labels FROM log_streams \
             WHERE fingerprint IN ({fps}) ORDER BY fingerprint"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "StreamMetaRow",
        rows.into_iter().map(|r| r.fingerprint).collect(),
    );

    let rows: Vec<logql_rows::SampleRow> = read_all(
        &client,
        "SampleRow",
        &format!(
            "SELECT fingerprint, timestamp_ns, body, structured_metadata FROM log_samples \
             WHERE fingerprint IN ({fps}) ORDER BY fingerprint"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "logql::rows::SampleRow",
        rows.into_iter().map(|r| r.fingerprint).collect(),
    );

    let rows: Vec<logql_rows::TailSampleRow> = read_all(
        &client,
        "TailSampleRow",
        &format!(
            "SELECT fingerprint, timestamp_ns, body, cityHash64(body) AS body_hash, \
             structured_metadata FROM log_samples WHERE fingerprint IN ({fps}) \
             ORDER BY fingerprint"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "TailSampleRow",
        rows.into_iter().map(|r| r.fingerprint).collect(),
    );

    let rows: Vec<logql_rows::MetricScanRow> = read_all(
        &client,
        "MetricScanRow",
        &format!(
            "SELECT fingerprint, timestamp_ns, body, structured_metadata FROM log_samples \
             WHERE fingerprint IN ({fps}) ORDER BY fingerprint"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "MetricScanRow",
        rows.into_iter().map(|r| r.fingerprint).collect(),
    );

    let rows: Vec<logql_rows::VolumeRow> = read_all(
        &client,
        "VolumeRow",
        &format!(
            "SELECT fingerprint, sum(length(body)) AS bytes FROM log_samples \
             WHERE fingerprint IN ({fps}) GROUP BY fingerprint ORDER BY fingerprint"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "VolumeRow",
        rows.into_iter().map(|r| r.fingerprint).collect(),
    );

    let rows: Vec<logql_rows::MetricBucketRow> = read_all(
        &client,
        "MetricBucketRow",
        &format!(
            "SELECT fingerprint, intDiv(timestamp_ns, 60000000000) * 60000000000 AS step, \
             count() AS n FROM log_samples WHERE fingerprint IN ({fps}) \
             GROUP BY fingerprint, step ORDER BY fingerprint"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "MetricBucketRow",
        rows.into_iter().map(|r| r.fingerprint).collect(),
    );

    let rows: Vec<logql_rows::MetricInstantRow> = read_all(
        &client,
        "MetricInstantRow",
        &format!(
            "SELECT fingerprint, count() AS n, any(structured_metadata) AS structured_metadata \
             FROM log_samples WHERE fingerprint IN ({fps}) GROUP BY fingerprint \
             ORDER BY fingerprint"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "MetricInstantRow",
        rows.into_iter().map(|r| r.fingerprint).collect(),
    );

    let rows: Vec<logql_rows::MetricRangeBucketRow> = read_all(
        &client,
        "MetricRangeBucketRow",
        &format!(
            "SELECT fingerprint, timestamp_ns AS bucket_ns, count() AS n, \
             any(structured_metadata) AS structured_metadata FROM log_samples \
             WHERE fingerprint IN ({fps}) GROUP BY fingerprint, bucket_ns ORDER BY fingerprint"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "MetricRangeBucketRow",
        rows.into_iter().map(|r| r.fingerprint).collect(),
    );

    // `UnwrappedLaneRow` carries BOTH a `class` and a `fingerprint`, and
    // `class` is the fingerprint itself on the per-fingerprint plan — the
    // shape `unwrapped_class_expr` renders when there is no grouping.
    let rows: Vec<logql_rows::UnwrappedLaneRow> = read_all(
        &client,
        "UnwrappedLaneRow",
        &format!(
            "SELECT fingerprint AS class, timestamp_ns AS bucket_ns, toUInt8(1) AS decided, \
             CAST([], 'Array(Tuple(UInt8, String))') AS keys, toFloat64(7) AS v, \
             '' AS body, fingerprint, '' AS sm_text, \
             CAST([], 'Array(Tuple(String, String))') AS sm_kept \
             FROM log_samples WHERE fingerprint IN ({fps}) ORDER BY fingerprint"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "UnwrappedLaneRow (fingerprint)",
        rows.iter().map(|r| r.fingerprint).collect(),
    );
    assert_the_four_boundary_values(
        "UnwrappedLaneRow (class)",
        rows.into_iter().map(|r| r.class).collect(),
    );

    // The twenty-fourth: no `fingerprint` field, but its `class` column is
    // the fingerprint on the per-fingerprint plan.
    let rows: Vec<logql_rows::MetricRangeUnwrappedRow> = read_all(
        &client,
        "MetricRangeUnwrappedRow",
        &format!(
            "SELECT fingerprint AS class, timestamp_ns AS bucket_ns, \
             CAST([], 'Array(Tuple(UInt8, String))') AS keys, toFloat64(7) AS v, \
             toUInt64(1) AS n_value, toUInt64(0) AS n_missing, toUInt64(0) AS n_undecided, \
             '' AS sm_text, CAST([], 'Array(Tuple(String, String))') AS sm_kept \
             FROM log_samples WHERE fingerprint IN ({fps}) ORDER BY class"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "MetricRangeUnwrappedRow (class)",
        rows.into_iter().map(|r| r.class).collect(),
    );

    // --- crates/pulsus-read/src/metrics/sample_rows.rs (4) ------------
    let rows: Vec<sample_rows::SampleRow> = read_all(
        &client,
        "metrics::SampleRow",
        &format!(
            "SELECT fingerprint, unix_milli, value FROM metric_samples \
             WHERE fingerprint IN ({fps}) ORDER BY fingerprint"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "metrics::sample_rows::SampleRow",
        rows.into_iter().map(|r| r.fingerprint).collect(),
    );

    let rows: Vec<sample_rows::MultiSampleRow> = read_all(
        &client,
        "MultiSampleRow",
        &format!(
            "SELECT metric_name, fingerprint, unix_milli, value FROM metric_samples \
             WHERE fingerprint IN ({fps}) ORDER BY fingerprint"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "MultiSampleRow",
        rows.into_iter().map(|r| r.fingerprint).collect(),
    );

    const HIST_COLUMNS: &str = "schema, zero_threshold, zero_count, count, sum, \
                                pos_span_offsets, pos_span_lengths, pos_bucket_deltas, \
                                neg_span_offsets, neg_span_lengths, neg_bucket_deltas, \
                                custom_values, counter_reset_hint";
    let rows: Vec<sample_rows::HistSampleRow> = read_all(
        &client,
        "HistSampleRow",
        &format!(
            "SELECT fingerprint, unix_milli, {HIST_COLUMNS} FROM metric_hist_samples \
             WHERE fingerprint IN ({fps}) ORDER BY fingerprint"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "HistSampleRow",
        rows.into_iter().map(|r| r.fingerprint).collect(),
    );

    let rows: Vec<sample_rows::MultiHistSampleRow> = read_all(
        &client,
        "MultiHistSampleRow",
        &format!(
            "SELECT metric_name, fingerprint, unix_milli, {HIST_COLUMNS} \
             FROM metric_hist_samples WHERE fingerprint IN ({fps}) ORDER BY fingerprint"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "MultiHistSampleRow",
        rows.into_iter().map(|r| r.fingerprint).collect(),
    );

    // --- crates/pulsus-read/src/metrics/rows.rs (1) -------------------
    let rows: Vec<metrics_rows::SeriesRow> = read_all(
        &client,
        "SeriesRow",
        &format!(
            "SELECT fingerprint, metric_name, labels FROM metric_series \
             WHERE fingerprint IN ({fps}) ORDER BY fingerprint"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "SeriesRow",
        rows.into_iter().map(|r| r.fingerprint).collect(),
    );

    // --- crates/pulsus-read/src/metrics/exec.rs (2, the two that were
    // --- private) ----------------------------------------------------
    let rows: Vec<metrics_exec::HydratedLabelsRow> = read_all(
        &client,
        "HydratedLabelsRow",
        &format!(
            "SELECT fingerprint, labels FROM metric_series WHERE fingerprint IN ({fps}) \
             ORDER BY fingerprint"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "HydratedLabelsRow",
        rows.into_iter().map(|r| r.fingerprint).collect(),
    );

    let rows: Vec<metrics_exec::FingerprintOnlyRow> = read_all(
        &client,
        "FingerprintOnlyRow",
        &format!(
            "SELECT fingerprint FROM metric_series WHERE fingerprint IN ({fps}) \
             ORDER BY fingerprint"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "FingerprintOnlyRow",
        rows.into_iter().map(|r| r.fingerprint).collect(),
    );

    // --- crates/pulsus-write/src/writer/rows.rs (6) -------------------
    // `seed` already wrote through all six; reading back through the same
    // six closes the round trip in both directions.
    let rows: Vec<LogStreamRow> = read_all(
        &client,
        "LogStreamRow",
        &format!(
            "SELECT month, fingerprint, service, labels, updated_ns FROM log_streams \
             WHERE fingerprint IN ({fps}) ORDER BY fingerprint"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "LogStreamRow",
        rows.into_iter().map(|r| r.fingerprint).collect(),
    );

    let rows: Vec<LogSampleRow> = read_all(
        &client,
        "LogSampleRow",
        &format!(
            "SELECT service, fingerprint, timestamp_ns, severity, body, structured_metadata \
             FROM log_samples WHERE fingerprint IN ({fps}) ORDER BY fingerprint"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "LogSampleRow",
        rows.into_iter().map(|r| r.fingerprint).collect(),
    );

    let rows: Vec<LogPatternRow> = read_all(
        &client,
        "LogPatternRow",
        &format!(
            "SELECT fingerprint, bucket_ns, pattern, count FROM log_patterns \
             WHERE fingerprint IN ({fps}) ORDER BY fingerprint"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "LogPatternRow",
        rows.into_iter().map(|r| r.fingerprint).collect(),
    );

    let rows: Vec<MetricSeriesRow> = read_all(
        &client,
        "MetricSeriesRow",
        &format!(
            "SELECT metric_name, fingerprint, unix_milli, labels, value_type FROM metric_series \
             WHERE fingerprint IN ({fps}) ORDER BY fingerprint"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "MetricSeriesRow",
        rows.into_iter().map(|r| r.fingerprint).collect(),
    );

    let rows: Vec<MetricSampleRow> = read_all(
        &client,
        "MetricSampleRow",
        &format!(
            "SELECT metric_name, fingerprint, unix_milli, value FROM metric_samples \
             WHERE fingerprint IN ({fps}) ORDER BY fingerprint"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "MetricSampleRow",
        rows.into_iter().map(|r| r.fingerprint).collect(),
    );

    let rows: Vec<MetricHistSampleRow> = read_all(
        &client,
        "MetricHistSampleRow",
        &format!(
            "SELECT metric_name, fingerprint, unix_milli, schema, zero_threshold, zero_count, \
             count, sum, pos_span_offsets, pos_span_lengths, pos_bucket_deltas, \
             neg_span_offsets, neg_span_lengths, neg_bucket_deltas, custom_values, \
             counter_reset_hint FROM metric_hist_samples WHERE fingerprint IN ({fps}) \
             ORDER BY fingerprint"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "MetricHistSampleRow",
        rows.into_iter().map(|r| r.fingerprint).collect(),
    );

    // The rollup table's own column, written by `log_metrics_5s_mv` from
    // the `log_samples` rows above rather than by a `Row` struct: the
    // projection must carry the width through the view.
    let rows: Vec<logql_rows::StreamRow> = read_all(
        &client,
        "log_metrics_5s (through the materialized view)",
        &format!(
            "SELECT fingerprint FROM log_metrics_5s WHERE fingerprint IN ({fps}) \
             GROUP BY fingerprint ORDER BY fingerprint"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "log_metrics_5s.fingerprint",
        rows.into_iter().map(|r| r.fingerprint).collect(),
    );

    // And the label index's, written by `log_streams_idx_mv`.
    let rows: Vec<logql_rows::StreamRow> = read_all(
        &client,
        "log_streams_idx (through the materialized view)",
        &format!(
            "SELECT fingerprint FROM log_streams_idx WHERE fingerprint IN ({fps}) \
             GROUP BY fingerprint ORDER BY fingerprint"
        ),
    )
    .await;
    assert_the_four_boundary_values(
        "log_streams_idx.fingerprint",
        rows.into_iter().map(|r| r.fingerprint).collect(),
    );

    drop_database(&bootstrap, &db).await;
}
