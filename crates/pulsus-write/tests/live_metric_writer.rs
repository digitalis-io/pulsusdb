//! Live end-to-end test: [`MetricWriter`] against a real ClickHouse — one
//! push is one insert of one block into `metric_landing` (issue #603), and
//! the block the server stored is what this file reads back.
//!
//! **Nothing here reads a target table.** `metric_samples`,
//! `metric_series`, `metric_metadata` and `metric_hist_samples` are
//! maintained from the landing table by materialized view; what the writer
//! claims when it answers a push is that the landing block committed, and
//! that is what these cases check. Gated behind `PULSUS_TEST_CLICKHOUSE=1`,
//! mirroring `pulsus-schema`'s `live_schema.rs`.
//!
//! To run these:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:26.3
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-write --test live_metric_writer
//! podman rm -f pulsus-ch-test
//! ```

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_config::WriterConfig;
use pulsus_model::{DEFAULT_ACTIVITY_BUCKET_MS, Fingerprint, LabelSet};
use pulsus_schema::{RenderCtx, run_init};
use pulsus_write::{
    MetricMetadata, MetricPoint, MetricSink, MetricWriter, MetricWriterTables, ParsedMetrics,
    PushHeaders, SeriesRef,
};

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping when
/// the gate is absent in a live CI job, so a lost `env:` block reddens the
/// build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
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

fn params_for(db: &str) -> RenderCtx {
    RenderCtx {
        db: db.to_string(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
        metrics_landing_retention_hours: 6,
        metrics_dedup_window: 10_000,
    }
}

/// A bootstrap client, a freshly initialised database, and a writer over it.
///
/// `db` is the composed name, not a bare one: every live-test object name is
/// written as `pulsus_testkit::test_db("…")` at its own call site, so two
/// checkouts sharing one ClickHouse cannot drop each other's data (issue
/// #320's naming gate reads the call site, not this helper).
async fn live_writer(db: String) -> (ChClient, String, Arc<ChClient>, MetricWriter) {
    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    drop_database(&bootstrap, &db).await;
    run_init(&bootstrap, &params_for(&db))
        .await
        .expect("run_init");

    let client = Arc::new(
        ChClient::new(test_config(&db))
            .await
            .expect("connect (target db)"),
    );
    let writer = MetricWriter::new_with_tables(
        client.clone(),
        &WriterConfig::default(),
        DEFAULT_ACTIVITY_BUCKET_MS,
        MetricWriterTables::metrics_default(),
    );
    (bootstrap, db, client, writer)
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CountRow {
    n: u64,
}

async fn count(client: &ChClient, sql: &str) -> u64 {
    let mut stream = client
        .query_stream::<CountRow>(sql, &QuerySettings::new())
        .await
        .expect("query the landing table");
    stream
        .next()
        .await
        .expect("one row")
        .expect("decode CountRow")
        .n
}

/// One push of every kind is one block in the landing table, with the rows
/// the writer built and one `received_ms` shared by all of them.
#[tokio::test]
async fn a_push_lands_one_block_carrying_every_kind() {
    skip_unless_live!();
    let (bootstrap, db, client, writer) =
        live_writer(pulsus_testkit::test_db("pulsus_write_it_landing_block")).await;

    let (labels, _) = LabelSet::from_normalized([("job".to_string(), "checkout".to_string())]);
    let metric_name: Arc<str> = Arc::from("http_requests_total");
    let batch = ParsedMetrics {
        samples: vec![MetricPoint {
            metric_name: metric_name.clone(),
            fingerprint: Fingerprint::from_raw(42),
            unix_milli: 1_000,
            value: 1.5,
        }],
        series: vec![SeriesRef {
            metric_name: metric_name.clone(),
            fingerprint: Fingerprint::from_raw(42),
            labels,
        }],
        metadata: vec![MetricMetadata {
            metric_name: metric_name.clone(),
            metric_type: "counter".to_string(),
            help: "help text".to_string(),
            unit: String::new(),
            updated_ns: 1,
        }],
        ..Default::default()
    };

    let wait = writer
        .admit_flush(batch, PushHeaders::default())
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(10), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect("the landing block commits");
    writer.shutdown(Duration::from_secs(5)).await;

    for (kind, want) in [(0u8, 1u64), (1, 0), (2, 1), (3, 1)] {
        let got = count(
            &client,
            &format!("SELECT count() AS n FROM {db}.metric_landing WHERE kind = {kind}"),
        )
        .await;
        assert_eq!(got, want, "kind {kind} rows in the landing block");
    }
    let distinct_stamps = count(
        &client,
        &format!("SELECT uniqExact(received_ms) AS n FROM {db}.metric_landing"),
    )
    .await;
    assert_eq!(
        distinct_stamps, 1,
        "every row of one push carries the same stamp, so the block lies in one partition"
    );

    drop_database(&bootstrap, &db).await;
}

/// The writer never sets `event_id`, so the server fills it from the
/// column's own `DEFAULT generateUUIDv7()`: two rows of one push carry two
/// different, non-nil, version-7 values.
///
/// A row type carrying the column would store whatever the writer put
/// there — the nil UUID on every row, for an explicit zero.
#[tokio::test]
async fn the_insert_omits_event_id_so_the_server_fills_it() {
    skip_unless_live!();
    let (bootstrap, db, client, writer) =
        live_writer(pulsus_testkit::test_db("pulsus_write_it_landing_event_id")).await;

    let metric_name: Arc<str> = Arc::from("http_requests_total");
    let batch = ParsedMetrics {
        samples: vec![
            MetricPoint {
                metric_name: metric_name.clone(),
                fingerprint: Fingerprint::from_raw(42),
                unix_milli: 1_000,
                value: 1.0,
            },
            MetricPoint {
                metric_name: metric_name.clone(),
                fingerprint: Fingerprint::from_raw(42),
                unix_milli: 1_001,
                value: 2.0,
            },
        ],
        ..Default::default()
    };
    let wait = writer
        .admit_flush(batch, PushHeaders::default())
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(10), wait)
        .await
        .expect("flush settles")
        .expect("commits");
    writer.shutdown(Duration::from_secs(5)).await;

    let rows = count(
        &client,
        &format!("SELECT count() AS n FROM {db}.metric_landing WHERE kind = 0"),
    )
    .await;
    assert_eq!(rows, 2);
    let distinct = count(
        &client,
        &format!("SELECT uniqExact(event_id) AS n FROM {db}.metric_landing WHERE kind = 0"),
    )
    .await;
    assert_eq!(distinct, 2, "each landed event has its own identity");
    let nil = count(
        &client,
        &format!(
            "SELECT count() AS n FROM {db}.metric_landing \
             WHERE event_id = toUUID('00000000-0000-0000-0000-000000000000')"
        ),
    )
    .await;
    assert_eq!(nil, 0, "no row carries the nil UUID");
    let version_7 = count(
        &client,
        &format!(
            "SELECT count() AS n FROM {db}.metric_landing \
             WHERE substring(toString(event_id), 15, 1) = '7'"
        ),
    )
    .await;
    assert_eq!(version_7, 2, "the server's own time-ordered UUID version");

    drop_database(&bootstrap, &db).await;
}

/// The writer's registration gate against a live server: two samples in the
/// same activity bucket for one series land exactly one kind-2 row, through
/// the whole path including the RowBinary encoding of the canonical label
/// JSON.
#[tokio::test]
async fn same_bucket_samples_land_exactly_one_registration_row() {
    skip_unless_live!();
    let (bootstrap, db, client, writer) =
        live_writer(pulsus_testkit::test_db("pulsus_write_it_landing_series")).await;

    let (labels, _) = LabelSet::from_normalized([("job".to_string(), "checkout".to_string())]);
    let metric_name: Arc<str> = Arc::from("http_requests_total");
    let batch = ParsedMetrics {
        samples: vec![
            MetricPoint {
                metric_name: metric_name.clone(),
                fingerprint: Fingerprint::from_raw(42),
                unix_milli: 0,
                value: 1.0,
            },
            MetricPoint {
                metric_name: metric_name.clone(),
                fingerprint: Fingerprint::from_raw(42),
                unix_milli: 60_000, // the same 1h bucket as unix_milli = 0
                value: 2.0,
            },
        ],
        series: vec![SeriesRef {
            metric_name: metric_name.clone(),
            fingerprint: Fingerprint::from_raw(42),
            labels,
        }],
        ..Default::default()
    };

    let wait = writer
        .admit_flush(batch, PushHeaders::default())
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(10), wait)
        .await
        .expect("flush settles")
        .expect("commits");
    writer.shutdown(Duration::from_secs(5)).await;

    let registrations = count(
        &client,
        &format!(
            "SELECT count() AS n FROM {db}.metric_landing \
             WHERE kind = 2 AND metric_name = '{metric_name}'"
        ),
    )
    .await;
    assert_eq!(
        registrations, 1,
        "two same-bucket samples for one series register exactly one row"
    );

    drop_database(&bootstrap, &db).await;
}

/// A fingerprint's labels cannot change across rows by construction —
/// `metric_fingerprint` is `hash(canonical label set)` and the writer
/// renders `labels` as deterministic canonical JSON, so a label change *is*
/// a different fingerprint. Proven through the product write path: two
/// samples for one series in two different activity buckets register two
/// rows, and the two carry byte-identical `labels` text.
#[tokio::test]
async fn registration_rows_for_one_fingerprint_carry_byte_identical_labels() {
    skip_unless_live!();
    let (bootstrap, db, client, writer) =
        live_writer(pulsus_testkit::test_db("pulsus_write_it_landing_labels")).await;

    let (labels, _) = LabelSet::from_normalized([("job".to_string(), "checkout".to_string())]);
    let metric_name: Arc<str> = Arc::from("http_requests_total");
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let batch = ParsedMetrics {
        samples: vec![
            MetricPoint {
                metric_name: metric_name.clone(),
                fingerprint: Fingerprint::from_raw(4242),
                unix_milli: 0,
                value: 1.0,
            },
            MetricPoint {
                metric_name: metric_name.clone(),
                fingerprint: Fingerprint::from_raw(4242),
                unix_milli: bucket * 5, // a distinct activity bucket
                value: 2.0,
            },
        ],
        series: vec![SeriesRef {
            metric_name: metric_name.clone(),
            fingerprint: Fingerprint::from_raw(4242),
            labels,
        }],
        ..Default::default()
    };

    let wait = writer
        .admit_flush(batch, PushHeaders::default())
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(10), wait)
        .await
        .expect("flush settles")
        .expect("commits");
    writer.shutdown(Duration::from_secs(5)).await;

    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct LabelsRow {
        unix_milli: i64,
        labels: String,
    }
    let sql = format!(
        "SELECT unix_milli, labels FROM {db}.metric_landing \
         WHERE kind = 2 AND metric_name = '{metric_name}' AND fingerprint = 4242 \
         ORDER BY unix_milli"
    );
    let mut stream = client
        .query_stream::<LabelsRow>(&sql, &QuerySettings::new())
        .await
        .expect("query the landing table");
    let mut rows = Vec::new();
    while let Some(row) = stream.next().await {
        rows.push(row.expect("decode LabelsRow"));
    }

    assert_eq!(
        rows.len(),
        2,
        "two distinct activity buckets register two rows"
    );
    assert_eq!(
        rows[0].labels, rows[1].labels,
        "the same fingerprint's labels must be byte-identical across every registration \
         row (labels are immutable by construction — a change would be a different \
         fingerprint)"
    );

    drop_database(&bootstrap, &db).await;
}
