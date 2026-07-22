//! Live failure-injection e2e for the metric registration backfill
//! (issue #139): a Poisoned `metric_series` registration flush whose
//! samples committed must self-heal — the orphan is observed pre-heal and
//! the read-side `LIMIT 1 BY` lookup resolves post-heal, byte-identically
//! stable across a forced duplicate re-insert (L-M1) — and a
//! `metric_metadata` heal must collapse to exactly one `FINAL` row whose
//! survivor is the max-`updated_ns` descriptor even after a genuine
//! duplicate plus a stale re-insert (L-M2). Gated behind
//! `PULSUS_TEST_CLICKHOUSE=1`, mirroring `live_log_stream_backfill.rs`.
//!
//! To run these:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-write --test live_metric_registration_backfill
//! podman rm -f pulsus-ch-test
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{
    ChClient, ChConnConfig, ChError, ChProto, ChRow, Idempotency, QuerySettings, Row,
};
use pulsus_config::WriterConfig;
use pulsus_model::{DEFAULT_ACTIVITY_BUCKET_MS, LabelSet, floor_to_activity_bucket};
use pulsus_schema::{RenderCtx, run_init};
use pulsus_write::writer::{BlockInserter, ChBlockInserter, MetricWriter};
use pulsus_write::{
    MetricMetadata, MetricMetadataRow, MetricPoint, MetricSeriesRow, MetricSink, ParsedMetrics,
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
                 (see crates/pulsus-write/tests/live_metric_registration_backfill.rs for setup)"
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

/// How the wrapper treats the FIRST insert (every later call forwards
/// normally) — mirrors `live_log_stream_backfill.rs`'s inject modes.
#[derive(Clone, Copy, Debug)]
enum InjectMode {
    /// "Lost registration": swallow (never forwards) and return a
    /// deterministic error — classified Poisoned; the rows genuinely
    /// never landed.
    Swallow,
    /// "False Poisoned": forward (the rows genuinely land) and THEN
    /// return a deterministic error — the backfill re-insert is a real
    /// duplicate.
    ForwardThenFail,
}

/// Failure-injecting wrapper over the real inserter, generic over the
/// row shape (issue #139: reused for `metric_series` and
/// `metric_metadata`).
///
/// **Deterministic orphan window** (code-review fix): every call after
/// the first — i.e. every backfill re-insert — parks on `heal_gate`
/// until the test calls [`Self::release_heals`]. The 5s backfill tick
/// may fire, but its insert attempt cannot forward (and `forwarded`
/// cannot flip) while the gate is closed, so the orphan-state assertions
/// are observed with ZERO possibility of a concurrent heal — no race
/// against the wall-clock tick.
struct InjectingInserter {
    inner: ChBlockInserter,
    mode: InjectMode,
    calls: AtomicUsize,
    forwarded: AtomicUsize,
    /// Closed (0 permits) at construction; opened by [`Self::release_heals`].
    heal_gate: tokio::sync::Semaphore,
}

impl InjectingInserter {
    fn new(client: Arc<ChClient>, mode: InjectMode) -> Arc<Self> {
        Arc::new(InjectingInserter {
            inner: ChBlockInserter::new(client),
            mode,
            calls: AtomicUsize::new(0),
            forwarded: AtomicUsize::new(0),
            heal_gate: tokio::sync::Semaphore::new(0),
        })
    }

    fn forwarded(&self) -> usize {
        self.forwarded.load(Ordering::SeqCst)
    }

    /// Opens the heal gate: every parked and future backfill re-insert
    /// proceeds. Enough permits that the gate never closes again.
    fn release_heals(&self) {
        self.heal_gate.add_permits(1_000_000);
    }
}

impl<R: ChRow + Send + Sync> BlockInserter<R> for InjectingInserter {
    fn insert<'a>(
        &'a self,
        table: &'a str,
        rows: &'a [R],
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
                        BlockInserter::<R>::insert(&self.inner, table, rows)
                            .await
                            .expect("the forwarded first insert must genuinely land");
                        self.forwarded.fetch_add(1, Ordering::SeqCst);
                        return Err(ChError::Decode(
                            "injected: false-Poisoned after a committed insert".to_string(),
                        ));
                    }
                }
            }
            // A backfill re-insert: park until the test releases the
            // gate — the deterministic-orphan-window seam.
            let permit = self
                .heal_gate
                .acquire()
                .await
                .expect("heal gate never closed");
            permit.forget();
            let result = BlockInserter::<R>::insert(&self.inner, table, rows).await;
            if result.is_ok() {
                self.forwarded.fetch_add(1, Ordering::SeqCst);
            }
            result
        })
    }
}

const BUCKET_MS: i64 = DEFAULT_ACTIVITY_BUCKET_MS;

/// "Now" in milliseconds — `metric_samples` carries a delete-TTL
/// (retention 7d here), so a fixed historical timestamp would land in an
/// expired part and could be dropped before the test observes it (the
/// hazard `trace_ingest_roundtrip.rs`'s rebasing fixture documents).
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as i64
}

fn labels() -> LabelSet {
    let (labels, _) = LabelSet::from_normalized([("job".to_string(), "checkout".to_string())]);
    labels
}

/// One float sample plus its `SeriesRef` — a series the writer has never
/// registered.
fn series_batch(metric_name: &str, fingerprint: u64, unix_milli: i64) -> ParsedMetrics {
    ParsedMetrics {
        samples: vec![MetricPoint {
            metric_name: Arc::from(metric_name),
            fingerprint,
            unix_milli,
            value: 1.5,
        }],
        series: vec![SeriesRef {
            metric_name: Arc::from(metric_name),
            fingerprint,
            labels: labels(),
        }],
        ..Default::default()
    }
}

fn metadata_batch(metric_name: &str, metric_type: &str, updated_ns: i64) -> ParsedMetrics {
    ParsedMetrics {
        metadata: vec![MetricMetadata {
            metric_name: Arc::from(metric_name),
            metric_type: metric_type.to_string(),
            help: "help".to_string(),
            unit: String::new(),
            updated_ns,
        }],
        ..Default::default()
    }
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct SeriesLookupRow {
    fingerprint: u64,
    labels: String,
}

/// The read-side dedup shape (docs/schemas.md §2.1,
/// pulsus-read/src/metrics/sql.rs): `LIMIT 1 BY metric_name, fingerprint`.
async fn series_limit1_lookup(
    client: &ChClient,
    db: &str,
    metric_name: &str,
) -> Vec<SeriesLookupRow> {
    let sql = format!(
        "SELECT fingerprint, labels FROM {db}.metric_series \
         WHERE metric_name = '{metric_name}' \
         ORDER BY unix_milli DESC \
         LIMIT 1 BY metric_name, fingerprint"
    );
    let mut stream = client
        .query_stream::<SeriesLookupRow>(&sql, &QuerySettings::new())
        .await
        .expect("query metric_series");
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode SeriesLookupRow"));
    }
    out
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CountRow {
    n: u64,
}

async fn count_query(client: &ChClient, sql: &str) -> u64 {
    let mut stream = client
        .query_stream::<CountRow>(sql, &QuerySettings::new())
        .await
        .expect("count query");
    stream.next().await.expect("one row").expect("decode").n
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct MetadataFinalRow {
    metric_type: String,
    updated_ns: i64,
}

/// Bounded real-time poll (the bounded-poll convention — no fixed
/// sleeps): up to ~30s for the 5s backfill tick plus insert latency.
async fn poll_until(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..150 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("condition not reached within the 30s poll budget: {what}");
}

fn writer_with_series_inject(
    client: Arc<ChClient>,
    series: Arc<InjectingInserter>,
) -> MetricWriter {
    let real: Arc<ChBlockInserter> = Arc::new(ChBlockInserter::new(client));
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1; // flush on the very next append
    MetricWriter::with_inserters(real.clone(), series, real.clone(), real, &cfg, BUCKET_MS)
}

fn writer_with_metadata_inject(
    client: Arc<ChClient>,
    metadata: Arc<InjectingInserter>,
) -> MetricWriter {
    let real: Arc<ChBlockInserter> = Arc::new(ChBlockInserter::new(client));
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;
    MetricWriter::with_inserters(real.clone(), real.clone(), metadata, real, &cfg, BUCKET_MS)
}

/// Issue #139 L-M1 — series orphan heal + `LIMIT 1 BY` result stability:
/// the first `metric_series` insert is swallowed with a deterministic
/// error (Poisoned; the samples insert is real), so the sample is visible
/// while the series registration is absent — the orphan. The backfill
/// re-insert heals it within a bounded poll; the `LIMIT 1 BY
/// metric_name, fingerprint` lookup then returns exactly one
/// `(fingerprint, labels)` row, and its result is byte-identical after a
/// forced duplicate re-insert (raw `count()` may be 2 — documented
/// bounded duplication; `metric_series` is duplicate-tolerant by design).
#[tokio::test]
async fn l_m1_series_orphan_heals_and_limit1_lookup_is_duplicate_stable() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_write_it_metric_backfill_lm1";
    init_db(&bootstrap, db).await;

    let client = Arc::new(
        ChClient::new(test_config(db))
            .await
            .expect("connect (target db)"),
    );
    let series = InjectingInserter::new(client.clone(), InjectMode::Swallow);
    let writer = writer_with_series_inject(client.clone(), series.clone());

    let metric_name = "backfill_lm1_total";
    let fingerprint = 77u64;
    let ts_ms = now_ms();
    let wait = writer
        .admit_flush(series_batch(metric_name, fingerprint, ts_ms))
        .expect("queue has room");
    let result = tokio::time::timeout(Duration::from_secs(10), wait)
        .await
        .expect("flush settles within the test timeout");
    assert!(
        result.is_err(),
        "the injected series poison must resolve the sync waiter Err, got {result:?}"
    );

    // The orphan, observed DETERMINISTICALLY: the heal gate is still
    // closed, so no backfill re-insert can forward regardless of how
    // many 5s ticks fire during this phase — the observations below
    // cannot race a heal. (a) nothing forwarded, (b) the registration
    // lookup is empty, (c) the sample becomes durable (the samples
    // generation settles in parallel — the sync waiter short-circuited
    // on the series Err, so poll for its commit), (d) the lookup is
    // STILL empty with the sample durable: the orphan.
    assert_eq!(series.forwarded(), 0);
    assert_eq!(writer.metrics().series_backfill.healed_total, 0);
    assert_eq!(
        series_limit1_lookup(&client, db, metric_name).await,
        Vec::<SeriesLookupRow>::new(),
        "the series registration is absent pre-heal"
    );
    let samples_count_sql = format!(
        "SELECT count() AS n FROM {db}.metric_samples \
         WHERE metric_name = '{metric_name}' AND fingerprint = {fingerprint}"
    );
    let mut sample_committed = false;
    for _ in 0..150 {
        if count_query(&client, &samples_count_sql).await == 1 {
            sample_committed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(sample_committed, "the sample committed — the orphan exists");
    assert_eq!(
        series_limit1_lookup(&client, db, metric_name).await,
        Vec::<SeriesLookupRow>::new(),
        "sample durable, registration absent — the orphan, observed under a \
         closed heal gate (no concurrent heal possible)"
    );
    assert_eq!(series.forwarded(), 0, "the gate held: nothing forwarded");

    // Release the gate: the (possibly already parked) backfill
    // re-insert proceeds and heals the registration.
    series.release_heals();
    poll_until("the backfill re-insert heals the lost registration", || {
        series.forwarded() >= 1 && writer.metrics().series_backfill.healed_total == 1
    })
    .await;

    let healed = series_limit1_lookup(&client, db, metric_name).await;
    assert_eq!(healed.len(), 1, "exactly one (fingerprint, labels) row");
    assert_eq!(healed[0].fingerprint, fingerprint);
    assert_eq!(healed[0].labels, r#"{"job":"checkout"}"#);

    // Forced duplicate re-insert: physically insert the same logical row
    // again — the LIMIT 1 BY result must be byte-identical (raw count may
    // be 2, the documented bounded duplication).
    let duplicate = MetricSeriesRow {
        metric_name: metric_name.to_string(),
        fingerprint,
        unix_milli: floor_to_activity_bucket(ts_ms, BUCKET_MS),
        labels: r#"{"job":"checkout"}"#.to_string(),
        value_type: 0,
    };
    client
        .insert_block("metric_series", &[duplicate])
        .await
        .expect("forced duplicate insert");

    let raw_count = count_query(
        &client,
        &format!(
            "SELECT count() AS n FROM {db}.metric_series \
             WHERE metric_name = '{metric_name}' AND fingerprint = {fingerprint}"
        ),
    )
    .await;
    assert_eq!(
        raw_count, 2,
        "the duplicate provably landed (bounded physical duplication)"
    );
    let after_duplicate = series_limit1_lookup(&client, db, metric_name).await;
    assert_eq!(
        after_duplicate, healed,
        "the LIMIT 1 BY result is byte-identical across the duplicate re-insert"
    );

    writer.shutdown(Duration::from_secs(5)).await;
    drop_database(&bootstrap, db).await;
}

/// Issue #139 L-M2 — metadata heal + `FINAL` no-inflation with a
/// max-`updated_ns` survivor: the first `metric_metadata` insert
/// genuinely lands and then reports a deterministic error ("false
/// Poisoned"), so the backfill re-insert is a real duplicate; after the
/// heal, `FINAL` collapses to exactly one row. A further STALE re-insert
/// (smaller `updated_ns`, different descriptor) still collapses to one
/// row, and the survivor is the max-`updated_ns` descriptor —
/// `ReplacingMergeTree(updated_ns)`'s winner.
#[tokio::test]
async fn l_m2_metadata_duplicate_and_stale_reinserts_collapse_to_the_max_updated_ns_row() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_write_it_metric_backfill_lm2";
    init_db(&bootstrap, db).await;

    let client = Arc::new(
        ChClient::new(test_config(db))
            .await
            .expect("connect (target db)"),
    );
    let metadata = InjectingInserter::new(client.clone(), InjectMode::ForwardThenFail);
    let writer = writer_with_metadata_inject(client.clone(), metadata.clone());

    let metric_name = "backfill_lm2_total";
    let updated_ns = 1_700_000_000_000_000_000i64;
    let wait = writer
        .admit_flush(metadata_batch(metric_name, "counter", updated_ns))
        .expect("queue has room");
    let result = tokio::time::timeout(Duration::from_secs(10), wait)
        .await
        .expect("flush settles within the test timeout");
    assert!(
        result.is_err(),
        "the injected false poison must resolve the sync waiter Err, got {result:?}"
    );

    // Deterministic checkpoint under the closed heal gate: only the
    // ORIGINAL (forwarded-then-failed) insert has happened — the
    // backfill's duplicate provably has NOT (no race against the 5s
    // tick; a fired tick parks on the gate).
    assert_eq!(metadata.forwarded(), 1, "only the original insert landed");
    assert_eq!(writer.metrics().metadata_backfill.healed_total, 0);
    assert_eq!(
        count_query(
            &client,
            &format!(
                "SELECT count() AS n FROM {db}.metric_metadata \
             WHERE metric_name = '{metric_name}'"
            )
        )
        .await,
        1,
        "exactly the original physical row exists pre-duplicate"
    );

    // Release the gate, then: the duplicate re-insert provably occurred
    // — two forwarded inserts (the committed original + the backfill's
    // duplicate) AND a counted heal — asserted BEFORE any collapse
    // query.
    metadata.release_heals();
    poll_until("the backfill duplicates the committed metadata row", || {
        metadata.forwarded() == 2 && writer.metrics().metadata_backfill.healed_total == 1
    })
    .await;

    let final_count_sql = format!(
        "SELECT count() AS n FROM {db}.metric_metadata FINAL \
         WHERE metric_name = '{metric_name}'"
    );
    assert_eq!(
        count_query(&client, &final_count_sql).await,
        1,
        "ReplacingMergeTree(updated_ns) must collapse the duplicate to 1 row"
    );

    // A stale re-insert (what an abandoned/late backfill attempt would
    // amount to): smaller updated_ns, different descriptor — must lose
    // deterministically to the newer row.
    let stale = MetricMetadataRow {
        metric_name: metric_name.to_string(),
        metric_type: "stale".to_string(),
        help: "stale".to_string(),
        unit: String::new(),
        updated_ns: updated_ns - 1_000,
    };
    client
        .insert_block("metric_metadata", &[stale])
        .await
        .expect("stale duplicate insert");

    assert_eq!(
        count_query(&client, &final_count_sql).await,
        1,
        "FINAL still collapses to 1 row after the stale re-insert"
    );
    let sql = format!(
        "SELECT metric_type, updated_ns FROM {db}.metric_metadata FINAL \
         WHERE metric_name = '{metric_name}'"
    );
    let mut stream = client
        .query_stream::<MetadataFinalRow>(&sql, &QuerySettings::new())
        .await
        .expect("query metric_metadata FINAL");
    let survivor = stream
        .next()
        .await
        .expect("one row")
        .expect("decode MetadataFinalRow");
    assert_eq!(
        survivor.updated_ns, updated_ns,
        "the survivor is the max-updated_ns row"
    );
    assert_eq!(
        survivor.metric_type, "counter",
        "the stale descriptor lost the merge"
    );

    writer.shutdown(Duration::from_secs(5)).await;
    drop_database(&bootstrap, db).await;
}
