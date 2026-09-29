//! Live end-to-end test: [`LogWriter`] against a real ClickHouse — one push
//! is one insert of one block into `log_landing` (issue #603), and the five
//! tables the views maintain are what this file reads back.
//!
//! **This suite is the one that shows the fan-out.** Everything else about the
//! logs write path is asserted over the block the writer hands to a mock
//! (`crates/pulsus-write/tests/log_landing.rs`); what only a server can show is
//! that five materialized views fire off that one insert, and that nothing in
//! the catalogue is second-level. Gated behind `PULSUS_TEST_CLICKHOUSE=1`,
//! mirroring `pulsus-schema`'s `live_schema.rs`.
//!
//! To run these:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:26.3
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-write --test live_log_writer
//! podman rm -f pulsus-ch-test
//! ```

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_config::WriterConfig;
use pulsus_model::{Date, Fingerprint, LabelSet, UnixNano};
use pulsus_schema::{RenderCtx, run_init};
use pulsus_write::writer::{LogWriter, WriterTables};
use pulsus_write::{LogRow, LogSink, ParsedLogs, PushHeaders, StreamRow};

const SERVICE: &str = "checkout-api";

/// The fixture's timestamp: **now**, so the landed lines are inside
/// `PULSUS_RETENTION_DAYS`.
///
/// `log_samples` and `log_patterns` carry a row-granular delete-TTL, and
/// ClickHouse applies a part's TTL as the part is written — a fixed historical
/// stamp would land the rows and then have the server drop them, and every
/// count below would read zero for a reason that has nothing to do with the
/// views. Both lines take the same stamp, so they share one rollup bucket and
/// one pattern bucket.
fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is past the epoch")
            .as_nanos(),
    )
    .expect("a representable nanosecond stamp")
}

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
                 (see crates/pulsus-write/tests/live_log_writer.rs for setup)"
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

/// A bootstrap client, a freshly initialised database, and a writer over it.
///
/// `db` is the composed name, not a bare one: every live-test object name is
/// written as `pulsus_testkit::test_db("…")` at its own call site, so two
/// checkouts sharing one ClickHouse cannot drop each other's data.
async fn live_writer(db: String) -> (ChClient, String, Arc<ChClient>, LogWriter) {
    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    drop_database(&bootstrap, &db).await;
    run_init(&bootstrap, &RenderCtx::for_tests(&db))
        .await
        .expect("run_init");

    let client = Arc::new(
        ChClient::new(test_config(&db))
            .await
            .expect("connect (target db)"),
    );
    let writer = LogWriter::new_with_tables(
        client.clone(),
        &WriterConfig::default(),
        WriterTables::logs_default(),
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
        .expect("query");
    stream
        .next()
        .await
        .expect("one row")
        .expect("decode CountRow")
        .n
}

/// **The fixture, chosen so every count below is decidable.** Two lines that
/// differ only in a digit run, so extraction yields ONE template and one
/// kind-2 row; one unregistered stream carrying exactly two labels, so
/// `log_streams_idx` takes exactly two rows; both lines in one 10 s window, so
/// the rollup takes one bucket.
fn fixture() -> (ParsedLogs, u64) {
    let ts = now_ns();
    let bodies = ["request 1 completed", "request 2 completed"];
    let body_bytes: u64 = bodies.iter().map(|b| b.len() as u64).sum();
    let (labels, _) = LabelSet::from_normalized([
        ("service_name".to_string(), SERVICE.to_string()),
        ("env".to_string(), "production".to_string()),
    ]);
    let batch = ParsedLogs {
        rows: bodies
            .iter()
            .map(|body| LogRow {
                service: SERVICE.to_string(),
                fingerprint: Fingerprint::from_raw(42),
                timestamp_ns: UnixNano(ts),
                severity: 0,
                body: (*body).to_string(),
                structured_metadata: String::new(),
            })
            .collect(),
        streams: vec![StreamRow {
            month: Date::start_of_month_utc(ts).expect("a representable month"),
            fingerprint: Fingerprint::from_raw(42),
            service: SERVICE.to_string(),
            labels,
            updated_ns: ts,
        }],
        ..Default::default()
    };
    (batch, body_bytes)
}

/// **T48.** One push lands one block carrying every kind, and **five
/// materialized views fire off that one insert**.
///
/// The counts are what shows nothing is second-level: `log_streams_idx` is an
/// `ARRAY JOIN` over the same landed kind-1 row `log_streams` takes, and
/// `log_metrics_<res>` aggregates the same landed kind-0 rows `log_samples`
/// takes. A view still reading a target table would see nothing at all here,
/// because the writer no longer inserts into one.
#[tokio::test]
async fn a_push_lands_one_block_carrying_every_kind() {
    skip_unless_live!();
    let (bootstrap, db, client, writer) =
        live_writer(pulsus_testkit::test_db("pulsus_write_it_log_landing_block")).await;

    let (batch, body_bytes) = fixture();
    writer
        .admit_flush(batch, PushHeaders::default())
        .expect("the queue has room")
        .await
        .expect("the landing block commits");

    // The landing table holds the whole push: two lines, one registration and
    // one pattern aggregate.
    assert_eq!(
        count(
            &client,
            &format!("SELECT count() AS n FROM {db}.log_landing")
        )
        .await,
        4,
        "two kind-0, one kind-1 and one kind-2 row"
    );
    for (kind, want) in [(0u8, 2u64), (1, 1), (2, 1)] {
        assert_eq!(
            count(
                &client,
                &format!("SELECT count() AS n FROM {db}.log_landing WHERE kind = {kind}")
            )
            .await,
            want,
            "kind {kind}"
        );
    }

    // The five tables the views maintain, off that one insert.
    assert_eq!(
        count(
            &client,
            &format!("SELECT count() AS n FROM {db}.log_samples")
        )
        .await,
        2,
        "log_samples_mv copies the kind-0 rows through"
    );
    assert_eq!(
        count(
            &client,
            &format!("SELECT count() AS n FROM {db}.log_streams")
        )
        .await,
        1,
        "log_streams_mv copies the kind-1 row through"
    );
    assert_eq!(
        count(
            &client,
            &format!("SELECT count() AS n FROM {db}.log_streams_idx")
        )
        .await,
        2,
        "log_streams_idx_mv ARRAY JOINs the same kind-1 row: one index row per \
         label pair, and none from a log line, whose labels are empty"
    );

    #[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
    struct RollupRow {
        count: u64,
        bytes: u64,
    }
    let mut stream = client
        .query_stream::<RollupRow>(
            &format!("SELECT sum(count) AS count, sum(bytes) AS bytes FROM {db}.log_metrics_5s"),
            &QuerySettings::new(),
        )
        .await
        .expect("read the rollup");
    let rollup = stream
        .next()
        .await
        .expect("one row")
        .expect("decode RollupRow");
    drop(stream);
    assert_eq!(rollup.count, 2, "the rollup counted both landed lines");
    assert_eq!(rollup.bytes, body_bytes, "and summed their bodies' lengths");
    assert_eq!(
        count(
            &client,
            &format!("SELECT count() AS n FROM {db}.log_metrics_5s")
        )
        .await,
        1,
        "one partial row per (fingerprint, bucket_ns)"
    );

    #[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
    struct PatternRow {
        count: u64,
    }
    let mut stream = client
        .query_stream::<PatternRow>(
            &format!("SELECT sum(count) AS count FROM {db}.log_patterns"),
            &QuerySettings::new(),
        )
        .await
        .expect("read the patterns");
    let pattern = stream
        .next()
        .await
        .expect("one row")
        .expect("decode PatternRow");
    drop(stream);
    assert_eq!(
        pattern.count, 2,
        "log_patterns_mv aliases pattern_count back to count: the aggregate \
         over both lines"
    );
    assert_eq!(
        count(
            &client,
            &format!("SELECT count() AS n FROM {db}.log_patterns")
        )
        .await,
        1,
        "the two lines share one template"
    );

    writer.shutdown(Duration::from_secs(5)).await;
    drop_database(&bootstrap, &db).await;
}

/// **T49.** The insert omits `event_id`, so the server fills it from its own
/// `DEFAULT generateUUIDv7()`: every landed row carries a DISTINCT version-7
/// UUID. A row type carrying the column would store whatever the writer put
/// there — the nil UUID on every row, for an explicit zero — and every count
/// above would still pass.
#[tokio::test]
async fn the_log_landing_insert_omits_event_id_so_the_server_fills_it() {
    skip_unless_live!();
    let (bootstrap, db, client, writer) =
        live_writer(pulsus_testkit::test_db("pulsus_write_it_log_event_id")).await;

    let (batch, _) = fixture();
    writer
        .admit_flush(batch, PushHeaders::default())
        .expect("the queue has room")
        .await
        .expect("the landing block commits");

    #[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
    struct EventIdRow {
        id: String,
        /// The version nibble of the canonical hyphenated form — its
        /// fifteenth character.
        version: String,
    }
    let mut stream = client
        .query_stream::<EventIdRow>(
            &format!(
                "SELECT toString(event_id) AS id, substring(toString(event_id), 15, 1) \
                 AS version FROM {db}.log_landing ORDER BY id"
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("read the identities back");
    let mut ids: Vec<String> = Vec::new();
    while let Some(row) = stream.next().await {
        let row = row.expect("decode EventIdRow");
        assert_eq!(
            row.version, "7",
            "the server's own default is a version-7 UUID: {}",
            row.id
        );
        assert_ne!(
            row.id, "00000000-0000-0000-0000-000000000000",
            "a writer-set identity would be the nil UUID on every row"
        );
        ids.push(row.id);
    }
    drop(stream);

    assert_eq!(ids.len(), 4, "one identity per landed row");
    let mut distinct = ids.clone();
    distinct.sort();
    distinct.dedup();
    assert_eq!(
        distinct.len(),
        ids.len(),
        "every identity is distinct: {ids:?}"
    );

    writer.shutdown(Duration::from_secs(5)).await;
    drop_database(&bootstrap, &db).await;
}
