//! Live failure-injection e2e for the `trace_attrs_idx` registration
//! backfill (issue #139, L-T1): the attrs insert fails while the span
//! commits — the span is fetchable by `trace_id` but the index probe
//! returns nothing (the orphan; fails without the fix) — then the
//! backfill heals it: the healed `trace_attrs_idx` row is durable and
//! retrievable via its primary-key access path (the index's own read
//! shape), the co-committed span is fetchable by the returned `trace_id`,
//! and a forced duplicate re-insert collapses to a `FINAL` count of 1.
//!
//! Scope note (plan v3 delta 3): the direct primary-key probe is the
//! storage-layer shape the TraceQL search executes against this index —
//! the full attribute-scoped TraceQL-**executor** end-to-end ("search
//! finds the healed span") is deliberately out of scope here
//! (pulsus-write cannot dev-depend on pulsus-read's executor — crate
//! layering) and lives in follow-up issue #167 against pulsus-read's
//! live suite.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, mirroring
//! `live_log_stream_backfill.rs`. To run:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-write --test live_trace_attr_backfill
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
use pulsus_schema::{RenderCtx, run_init};
use pulsus_write::writer::{BlockInserter, ChBlockInserter, TraceWriter, TraceWriterTables};
use pulsus_write::{AttrRecord, ParsedTraces, SpanRecord, TraceAttrRow, TraceSink};

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
                 (see crates/pulsus-write/tests/live_trace_attr_backfill.rs for setup)"
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

/// Failure-injecting wrapper over the real attrs inserter: swallows the
/// FIRST insert (never forwards) and returns a deterministic error —
/// classified Poisoned; the rows genuinely never landed.
///
/// **Deterministic orphan window** (code-review fix): every later call —
/// i.e. every backfill re-insert — parks on `heal_gate` until the test
/// calls [`Self::release_heals`]. The 5s backfill tick may fire, but its
/// insert attempt cannot forward while the gate is closed, so the
/// orphan-state assertions are observed with ZERO possibility of a
/// concurrent heal — no race against the wall-clock tick.
struct SwallowFirstInserter {
    inner: ChBlockInserter,
    calls: AtomicUsize,
    forwarded: AtomicUsize,
    /// Closed (0 permits) at construction; opened by [`Self::release_heals`].
    heal_gate: tokio::sync::Semaphore,
}

impl SwallowFirstInserter {
    fn new(client: Arc<ChClient>) -> Arc<Self> {
        Arc::new(SwallowFirstInserter {
            inner: ChBlockInserter::new(client),
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

impl BlockInserter<TraceAttrRow> for SwallowFirstInserter {
    fn insert<'a>(
        &'a self,
        table: &'a str,
        rows: &'a [TraceAttrRow],
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if call == 0 {
                return Err(ChError::Decode(
                    "injected: attr-index insert swallowed".to_string(),
                ));
            }
            // A backfill re-insert: park until the test releases the
            // gate — the deterministic-orphan-window seam.
            let permit = self
                .heal_gate
                .acquire()
                .await
                .expect("heal gate never closed");
            permit.forget();
            let result = self.inner.insert(table, rows).await;
            if result.is_ok() {
                self.forwarded.fetch_add(1, Ordering::SeqCst);
            }
            result
        })
    }
}

const TRACE_ID: [u8; 16] = [0xC1; 16];
const TRACE_ID_HEX: &str = "c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1";
const SPAN_ID: [u8; 8] = [0x0A; 8];
const ATTR_KEY: &str = "http.status_code";
const ATTR_VAL: &str = "500";
const ATTR_SCOPE: &str = "span";

/// "Now" in nanoseconds — the trace tables carry `ttl_only_drop_parts=1`
/// delete-TTLs (retention 7d here), so a fixed historical timestamp
/// would land in an already-expired part and be dropped before the test
/// could observe it (the hazard `trace_ingest_roundtrip.rs`'s rebasing
/// fixture documents).
fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos() as i64
}

fn batch(ts_ns: i64, date: u16) -> ParsedTraces {
    ParsedTraces {
        spans: vec![SpanRecord {
            trace_id: TRACE_ID,
            span_id: SPAN_ID,
            parent_id: [0; 8],
            name: "op-a".to_string(),
            service: "checkout".to_string(),
            timestamp_ns: ts_ns,
            duration_ns: 1_000_000_000,
            status_code: 2,
            kind: 3,
            payload: vec![0xDE, 0xAD, 0xBE, 0xEF],
        }],
        attrs: vec![AttrRecord {
            date,
            key: ATTR_KEY.to_string(),
            scope: ATTR_SCOPE.to_string(),
            val: ATTR_VAL.to_string(),
            val_num: Some(500.0),
            timestamp_ns: ts_ns,
            trace_id: TRACE_ID,
            span_id: SPAN_ID,
            duration_ns: 1_000_000_000,
        }],
        ..Default::default()
    }
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct TraceHexRow {
    trace_hex: String,
}

/// The index's own primary-key access path (the storage shape the
/// TraceQL search planner emits against `ORDER BY (key, val, scope,
/// timestamp_ns, trace_id, span_id)` — the executor proof itself is
/// #167).
async fn index_probe(client: &ChClient, db: &str) -> Vec<String> {
    let sql = format!(
        "SELECT lower(hex(trace_id)) AS trace_hex FROM {db}.trace_attrs_idx \
         WHERE key = '{ATTR_KEY}' AND val = '{ATTR_VAL}' AND scope = '{ATTR_SCOPE}' \
         ORDER BY timestamp_ns"
    );
    let mut stream = client
        .query_stream::<TraceHexRow>(&sql, &QuerySettings::new())
        .await
        .expect("query trace_attrs_idx");
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode TraceHexRow").trace_hex);
    }
    out
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SpanFetchRow {
    name: String,
    service: String,
}

async fn fetch_span_by_trace_id(client: &ChClient, db: &str, trace_hex: &str) -> Vec<SpanFetchRow> {
    let sql = format!(
        "SELECT name, service FROM {db}.trace_spans \
         WHERE trace_id = unhex('{trace_hex}')"
    );
    let mut stream = client
        .query_stream::<SpanFetchRow>(&sql, &QuerySettings::new())
        .await
        .expect("query trace_spans");
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode SpanFetchRow"));
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

/// Bounded real-time poll: up to ~30s for the 5s backfill tick plus
/// insert latency.
async fn poll_until(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..150 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("condition not reached within the 30s poll budget: {what}");
}

/// Issue #139 L-T1 (AC wording per plan v2 delta 2 / v3 delta 3): the
/// attrs inserter fails first while the span commits → the span is
/// fetchable by `trace_id` while the index probe returns nothing (the
/// orphan — this assertion fails without the backfill); after the heal
/// the probe finds the `trace_id`, the span row fetched by it matches,
/// and a forced duplicate re-insert collapses to `FINAL` count == 1. The
/// TraceQL-executor end-to-end proof is follow-up #167.
#[tokio::test]
async fn l_t1_lost_attr_index_heals_and_the_primary_key_probe_finds_the_span() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_write_it_trace_attr_backfill_lt1";
    init_db(&bootstrap, db).await;

    let client = Arc::new(
        ChClient::new(test_config(db))
            .await
            .expect("connect (target db)"),
    );
    let attrs = SwallowFirstInserter::new(client.clone());
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1; // flush on the very next append

    let writer = TraceWriter::with_inserters_with_tables(
        Arc::new(ChBlockInserter::new(client.clone())),
        attrs.clone(),
        &cfg,
        TraceWriterTables::traces_default(),
    );

    let ts_ns = now_ns();
    let date = (ts_ns / 86_400_000_000_000) as u16; // UTC day since epoch
    let wait = writer
        .admit_flush(batch(ts_ns, date))
        .expect("queue has room");
    let result = tokio::time::timeout(Duration::from_secs(10), wait)
        .await
        .expect("flush settles within the test timeout");
    assert!(
        result.is_err(),
        "the injected attrs poison must resolve the sync waiter Err, got {result:?}"
    );

    // The orphan, observed DETERMINISTICALLY: the heal gate is still
    // closed, so no backfill re-insert can forward regardless of how
    // many 5s ticks fire during this phase — the observations below
    // cannot race a heal. (a) nothing forwarded, (b) the index probe
    // finds nothing, (c) the span becomes fetchable by trace_id (the
    // spans generation settles in parallel — the sync waiter
    // short-circuited on the attrs Err, so poll for its commit), (d) the
    // probe is STILL empty with the span durable: the orphan — this is
    // the assertion that fails without the fix.
    assert_eq!(attrs.forwarded(), 0);
    assert_eq!(writer.metrics().attrs_backfill.healed_total, 0);
    assert_eq!(
        index_probe(&client, db).await,
        Vec::<String>::new(),
        "the attr-index row is absent pre-heal — the span is invisible to the index"
    );
    let mut span_committed = Vec::new();
    for _ in 0..150 {
        span_committed = fetch_span_by_trace_id(&client, db, TRACE_ID_HEX).await;
        if !span_committed.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(span_committed.len(), 1, "the span committed");
    assert_eq!(span_committed[0].name, "op-a");
    assert_eq!(
        index_probe(&client, db).await,
        Vec::<String>::new(),
        "span fetchable, index row absent — the orphan, observed under a closed \
         heal gate (no concurrent heal possible)"
    );
    assert_eq!(attrs.forwarded(), 0, "the gate held: nothing forwarded");

    // Release the gate: the (possibly already parked) backfill
    // re-insert proceeds and heals the index row.
    attrs.release_heals();
    poll_until("the backfill re-insert heals the attr-index row", || {
        attrs.forwarded() >= 1 && writer.metrics().attrs_backfill.healed_total == 1
    })
    .await;

    // The healed registration is retrievable via the index's primary-key
    // access path, and the returned trace_id fetches the matching span.
    let hits = index_probe(&client, db).await;
    assert_eq!(hits, vec![TRACE_ID_HEX.to_string()]);
    let spans = fetch_span_by_trace_id(&client, db, &hits[0]).await;
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].name, "op-a");
    assert_eq!(spans[0].service, "checkout");

    // Forced duplicate re-insert of the same logical attr row: the
    // ReplacingMergeTree collapses it — FINAL count == 1, no inflation.
    let duplicate = TraceAttrRow {
        date,
        key: ATTR_KEY.to_string(),
        val: ATTR_VAL.to_string(),
        scope: ATTR_SCOPE.to_string(),
        val_num: Some(500.0),
        timestamp_ns: ts_ns,
        trace_id: TRACE_ID,
        span_id: SPAN_ID,
        duration_ns: 1_000_000_000,
    };
    client
        .insert_block("trace_attrs_idx", &[duplicate])
        .await
        .expect("forced duplicate insert");

    let final_count = count_query(
        &client,
        &format!(
            "SELECT count() AS n FROM {db}.trace_attrs_idx FINAL \
             WHERE key = '{ATTR_KEY}' AND val = '{ATTR_VAL}' AND scope = '{ATTR_SCOPE}'"
        ),
    )
    .await;
    assert_eq!(
        final_count, 1,
        "ReplacingMergeTree must collapse the duplicate attr row to 1 on FINAL"
    );

    writer.shutdown(Duration::from_secs(5)).await;
    drop_database(&bootstrap, db).await;
}
