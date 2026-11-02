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
//!     clickhouse/clickhouse-server:26.8.21.10
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-write --test live_log_writer
//! podman rm -f pulsus-ch-test
//! ```

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_config::WriterConfig;
use pulsus_model::{Date, Fingerprint, LabelSet, UnixNano};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;
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
    run_init(&bootstrap, &RenderCtx::for_tests(&db, 7))
        .await
        .expect("run_init");

    let client = Arc::new(
        ChClient::new(test_config(&db))
            .await
            .expect("connect (target db)"),
    );
    let writer = LogWriter::new_with_tables(
        client.clone(),
        &unbatched_config(),
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
        .admit_flush(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            batch,
            PushHeaders::default(),
        )
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
        .admit_flush(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            batch,
            PushHeaders::default(),
        )
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

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, Copy)]
struct LogCounts {
    ended: u64,
    pending: u64,
}

/// Waits until every statement `filter` selects in `client`'s
/// `system.query_log` has its ending row, and at least `at_least` have ended.
///
/// One `SYSTEM FLUSH LOGS` is not a barrier: a statement's `QueryFinish` row
/// can be queued after its client has the answer. Its `QueryStart` row is
/// queued before it runs, so once flushed it names every statement sent; an
/// execution is a `query_id` with its `query_start_time_microseconds`, which
/// both rows share. `filter` must not name `type`. Fails after 60 s.
async fn settle_query_log(client: &ChClient, filter: &str, at_least: u64) {
    let sql = format!(
        "SELECT toUInt64(countIf(e)) AS ended, toUInt64(countIf(s AND NOT e)) AS pending \
         FROM (SELECT query_id, max(type = 'QueryStart') AS s, \
                 max(type IN ('QueryFinish', 'ExceptionWhileProcessing', \
                              'ExceptionBeforeStart')) AS e \
               FROM system.query_log \
               WHERE ({filter}) AND query NOT LIKE '%system.query_log%' \
               GROUP BY query_id, query_start_time_microseconds)"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        client
            .execute(
                "SYSTEM FLUSH LOGS",
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("SYSTEM FLUSH LOGS");
        let mut stream = client
            .query_stream::<LogCounts>(&sql, &QuerySettings::new())
            .await
            .unwrap_or_else(|e| panic!("query failed: {e}\nSQL:\n{sql}"));
        let counts = stream
            .next()
            .await
            .expect("one row")
            .expect("decode LogCounts");
        drop(stream);
        if counts.ended >= at_least && counts.pending == 0 {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "system.query_log did not settle within 60 s for {filter}: {counts:?}, \
             at least {at_least} expected"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// **2-L1 (issue #494 part 2).** Eight sync pushes of 50 distinct lines,
/// admitted together through a writer with the 1000 ms window, are one
/// landing insert on the server, and every line is stored once.
#[tokio::test]
async fn eight_log_pushes_admitted_together_are_one_landing_insert() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_write_it_log_batch");
    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    drop_database(&bootstrap, &db).await;
    run_init(&bootstrap, &RenderCtx::for_tests(&db, 7))
        .await
        .expect("run_init");
    let client = Arc::new(
        ChClient::new(test_config(&db))
            .await
            .expect("connect (target db)"),
    );
    let cfg = WriterConfig {
        landing_batch_ms: 1_000,
        ..WriterConfig::default()
    };
    let writer = LogWriter::new_with_tables(client.clone(), &cfg, WriterTables::logs_default());
    // This run's statements: the database is recreated each run, but the
    // log keeps earlier runs' rows under the same name.
    let start = count(
        &client,
        "SELECT toUInt64(toUnixTimestamp64Micro(now64(6))) AS n",
    )
    .await;

    let ts = now_ns();
    let tenant = pulsus_model::Tenant::from_header(None, false).expect("the empty tenant");
    let waits: Vec<_> = (0..8u128)
        .map(|push| {
            let (labels, _) = LabelSet::from_normalized([
                ("service_name".to_string(), SERVICE.to_string()),
                ("push".to_string(), push.to_string()),
            ]);
            let batch = ParsedLogs {
                rows: (0..50)
                    .map(|line| LogRow {
                        service: SERVICE.to_string(),
                        fingerprint: Fingerprint::from_raw(100 + push),
                        timestamp_ns: UnixNano(ts + line),
                        severity: 0,
                        body: format!("push {push} line {line}"),
                        structured_metadata: String::new(),
                    })
                    .collect(),
                streams: vec![StreamRow {
                    month: Date::start_of_month_utc(ts).expect("a representable month"),
                    fingerprint: Fingerprint::from_raw(100 + push),
                    service: SERVICE.to_string(),
                    labels,
                    updated_ns: ts,
                }],
                ..Default::default()
            };
            writer
                .admit_flush(&tenant, batch, PushHeaders::default())
                .expect("admitted")
        })
        .collect();
    for wait in waits {
        wait.await.expect("every push is answered Ok");
    }

    let filter = format!(
        "current_database = '{db}' AND query_kind = 'Insert' \
         AND startsWith(query, 'INSERT INTO `log_landing`') \
         AND query_start_time_microseconds >= fromUnixTimestamp64Micro(toInt64({start}))"
    );
    settle_query_log(&client, &filter, 1).await;
    assert_eq!(
        count(
            &client,
            &format!(
                "SELECT count() AS n FROM system.query_log WHERE type = 'QueryFinish' AND {filter}"
            )
        )
        .await,
        1,
        "one landing insert"
    );
    assert_eq!(
        count(&client, "SELECT count() AS n FROM log_samples").await,
        400
    );
    assert_eq!(
        count(&client, "SELECT uniqExact(body) AS n FROM log_samples").await,
        400
    );

    writer.shutdown(Duration::from_secs(5)).await;
    drop_database(&bootstrap, &db).await;
}

/// `WriterConfig::default()` with log and trace batching off (issue #494
/// part 2), so each case here keeps meaning one push per insert.
#[allow(dead_code)]
fn unbatched_config() -> pulsus_config::WriterConfig {
    pulsus_config::WriterConfig {
        landing_batch_ms: 0,
        ..pulsus_config::WriterConfig::default()
    }
}
