//! Live failure-injection e2e for the `log_streams` registration backfill
//! (issue #134): a Poisoned registration flush whose samples committed
//! must self-heal — the stream resolves in the sample's month (L1) — and
//! a false-Poisoned re-insert (rows actually landed) must collapse to one
//! row on a `FINAL` read (L2: the duplicate insert provably occurred
//! before the collapse is asserted). Gated behind
//! `PULSUS_TEST_CLICKHOUSE=1`, mirroring `live_metric_writer.rs`.
//!
//! To run these:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-write --test live_log_stream_backfill
//! podman rm -f pulsus-ch-test
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{
    ChClient, ChConnConfig, ChError, ChProto, Idempotency, QuerySettings, Row,
};
use pulsus_config::WriterConfig;
use pulsus_model::{Date, LabelSet, UnixNano};
use pulsus_schema::{RenderCtx, run_init};
use pulsus_write::writer::{BlockInserter, ChBlockInserter, LogStreamRow, LogWriter};
use pulsus_write::{LogRow, LogSink, ParsedLogs, StreamRow, WriterTables};

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
                 (see crates/pulsus-write/tests/live_log_stream_backfill.rs for setup)"
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

async fn init_db(bootstrap: &ChClient, db: &str) {
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
}

/// How the wrapper treats the FIRST `log_streams` insert (every later
/// call forwards normally).
#[derive(Clone, Copy, Debug)]
enum InjectMode {
    /// L1 "lost registration": swallow (never forwards) and return a
    /// deterministic error — classified Poisoned; the rows genuinely
    /// never landed.
    Swallow,
    /// L2 "false Poisoned": forward (the rows genuinely land) and THEN
    /// return a deterministic error — the backfill re-insert is a real
    /// duplicate.
    ForwardThenFail,
}

/// Failure-injecting wrapper over the real streams inserter.
struct InjectingInserter {
    inner: ChBlockInserter,
    mode: InjectMode,
    calls: AtomicUsize,
    forwarded: AtomicUsize,
}

impl InjectingInserter {
    fn new(client: Arc<ChClient>, mode: InjectMode) -> Arc<Self> {
        Arc::new(InjectingInserter {
            inner: ChBlockInserter::new(client),
            mode,
            calls: AtomicUsize::new(0),
            forwarded: AtomicUsize::new(0),
        })
    }

    fn forwarded(&self) -> usize {
        self.forwarded.load(Ordering::SeqCst)
    }
}

impl BlockInserter<LogStreamRow> for InjectingInserter {
    fn insert<'a>(
        &'a self,
        table: &'a str,
        rows: &'a [LogStreamRow],
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if call == 0 {
                match self.mode {
                    InjectMode::Swallow => {
                        return Err(ChError::Decode(
                            "injected: registration insert swallowed".to_string(),
                        ));
                    }
                    InjectMode::ForwardThenFail => {
                        self.inner
                            .insert(table, rows)
                            .await
                            .expect("the forwarded first insert must genuinely land");
                        self.forwarded.fetch_add(1, Ordering::SeqCst);
                        return Err(ChError::Decode(
                            "injected: false-Poisoned after a committed insert".to_string(),
                        ));
                    }
                }
            }
            let result = self.inner.insert(table, rows).await;
            if result.is_ok() {
                self.forwarded.fetch_add(1, Ordering::SeqCst);
            }
            result
        })
    }
}

const TS_NS: i64 = 1_700_000_000_000_000_000; // 2023-11-14 UTC -> month 2023-11-01

fn batch_for(fingerprint: u64, service: &str) -> ParsedLogs {
    let (labels, _) =
        LabelSet::from_normalized([("service_name".to_string(), service.to_string())]);
    ParsedLogs {
        rows: vec![LogRow {
            service: service.to_string(),
            fingerprint,
            timestamp_ns: UnixNano(TS_NS),
            severity: 0,
            body: "hello".to_string(),
            structured_metadata: String::new(),
        }],
        streams: vec![StreamRow {
            month: Date::start_of_month_utc(TS_NS).unwrap(),
            fingerprint,
            service: service.to_string(),
            labels,
            updated_ns: TS_NS,
        }],
        ..Default::default()
    }
}

fn month_days() -> u16 {
    Date::start_of_month_utc(TS_NS).unwrap().days_since_epoch()
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct FingerprintRow {
    fingerprint: u64,
}

/// The stage-1-shaped resolution query (docs/schemas.md §3.2): does the
/// `(key, val)` pair resolve `fingerprint`s in the sample's month?
async fn stage1_fingerprints(client: &ChClient, db: &str, service: &str) -> Vec<u64> {
    let sql = format!(
        "SELECT fingerprint FROM {db}.log_streams_idx \
         WHERE month = toDate({days}) AND key = 'service_name' AND val = '{service}' \
         GROUP BY fingerprint HAVING uniqExact(key, val) = 1 \
         ORDER BY fingerprint",
        days = month_days(),
    );
    let mut stream = client
        .query_stream::<FingerprintRow>(&sql, &QuerySettings::new())
        .await
        .expect("query log_streams_idx");
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode FingerprintRow").fingerprint);
    }
    out
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CountRow {
    n: u64,
}

async fn streams_final_count(client: &ChClient, db: &str, fingerprint: u64) -> u64 {
    let sql = format!(
        "SELECT count() AS n FROM {db}.log_streams FINAL \
         WHERE fingerprint = {fingerprint} AND month = toDate({days})",
        days = month_days(),
    );
    let mut stream = client
        .query_stream::<CountRow>(&sql, &QuerySettings::new())
        .await
        .expect("query log_streams FINAL");
    stream.next().await.expect("one row").expect("decode").n
}

/// Bounded real-time poll (architecture.md's bounded-poll convention —
/// no fixed sleeps): up to ~30s for the 5s backfill tick plus insert
/// latency.
async fn poll_until(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..150 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("condition not reached within the 30s poll budget: {what}");
}

/// Issue #134 AC9 — L1 "lost registration" (the issue's mandated
/// failure-injection test): the first `log_streams` insert is swallowed
/// with a deterministic error (Poisoned; samples insert real), so
/// without the backfill the registration never lands and the stream is
/// permanently unresolvable in the sample's month. With the backfill,
/// the re-insert heals it and the stage-1-shaped query resolves the
/// fingerprint.
#[tokio::test]
async fn l1_lost_registration_backfill_resolves_the_stream_in_the_samples_month() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_write_it_stream_backfill_l1";
    init_db(&bootstrap, db).await;

    let client = Arc::new(
        ChClient::new(test_config(db))
            .await
            .expect("connect (target db)"),
    );
    let streams = InjectingInserter::new(client.clone(), InjectMode::Swallow);
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1; // flush on the very next append

    let writer = LogWriter::with_inserters_with_tables(
        Arc::new(ChBlockInserter::new(client.clone())),
        streams.clone(),
        &cfg,
        WriterTables::logs_default(),
    );

    let fingerprint = 77u64;
    let service = "backfill-l1-svc";
    let wait = writer
        .admit_flush(batch_for(fingerprint, service))
        .expect("queue has room");
    let result = tokio::time::timeout(Duration::from_secs(10), wait)
        .await
        .expect("flush settles within the test timeout");
    assert!(
        result.is_err(),
        "the injected streams poison must resolve the sync waiter Err, got {result:?}"
    );

    // The registration is genuinely lost until the backfill heals it.
    assert_eq!(streams.forwarded(), 0);

    poll_until("the backfill re-insert heals the lost registration", || {
        streams.forwarded() >= 1 && writer.metrics().backfill_healed_total == 1
    })
    .await;

    let fingerprints = stage1_fingerprints(&client, db, service).await;
    assert_eq!(
        fingerprints,
        vec![fingerprint],
        "the stream must resolve in the sample's month after the heal"
    );

    writer.shutdown(Duration::from_secs(5)).await;
    drop_database(&bootstrap, db).await;
}

/// Issue #134 AC10 — L2 "false Poisoned" no-inflation pin: the first
/// insert FORWARDS (rows genuinely land) and then reports a
/// deterministic error, so the backfill re-insert is a real duplicate.
/// Asserted in order: (a) the wrapper observed exactly 2 forwarded
/// streams inserts AND the heal was counted — the duplicate provably
/// occurred — then (b) `FINAL` collapses to exactly 1 row, and (c)
/// stage-1 resolves exactly one fingerprint row. Safety does not depend
/// on the Poisoned classification being truthful.
#[tokio::test]
async fn l2_false_poisoned_duplicate_reinsert_collapses_on_final_read() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_write_it_stream_backfill_l2";
    init_db(&bootstrap, db).await;

    let client = Arc::new(
        ChClient::new(test_config(db))
            .await
            .expect("connect (target db)"),
    );
    let streams = InjectingInserter::new(client.clone(), InjectMode::ForwardThenFail);
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    let writer = LogWriter::with_inserters_with_tables(
        Arc::new(ChBlockInserter::new(client.clone())),
        streams.clone(),
        &cfg,
        WriterTables::logs_default(),
    );

    let fingerprint = 88u64;
    let service = "backfill-l2-svc";
    let wait = writer
        .admit_flush(batch_for(fingerprint, service))
        .expect("queue has room");
    let result = tokio::time::timeout(Duration::from_secs(10), wait)
        .await
        .expect("flush settles within the test timeout");
    assert!(
        result.is_err(),
        "the injected false poison must resolve the sync waiter Err, got {result:?}"
    );

    // (a) The duplicate re-insert provably occurred: two forwarded
    // inserts (the committed original + the backfill's duplicate) AND a
    // counted heal — asserted BEFORE any collapse query.
    poll_until("the backfill duplicates the committed registration", || {
        streams.forwarded() == 2 && writer.metrics().backfill_healed_total == 1
    })
    .await;
    assert_eq!(streams.forwarded(), 2);
    assert_eq!(writer.metrics().backfill_healed_total, 1);

    // (b) The genuine duplicate collapses on a FINAL read — no inflation.
    let count = streams_final_count(&client, db, fingerprint).await;
    assert_eq!(
        count, 1,
        "ReplacingMergeTree(updated_ns) must collapse the duplicate registration to 1 row"
    );

    // (c) Stage-1 resolves exactly one fingerprint row.
    let fingerprints = stage1_fingerprints(&client, db, service).await;
    assert_eq!(
        fingerprints,
        vec![fingerprint],
        "stage-1 must resolve exactly one fingerprint row"
    );

    writer.shutdown(Duration::from_secs(5)).await;
    drop_database(&bootstrap, db).await;
}
