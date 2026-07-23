//! Issue #167: the end-to-end attribute-search proof deferred from #139
//! (L-T1). Replays #139's deterministic failure injection — the
//! `trace_attrs_idx` registration insert is swallowed while the span
//! commits, then the registration backfill heals it — and proves the
//! criterion #139 could not (crate layering): pre-heal the span is
//! durable (fetchable by trace id through the real point-read path) yet
//! INVISIBLE to an attribute-scoped TraceQL search; post-heal the
//! IDENTICAL `SearchPlan`, executed by the real
//! `parse → plan_search → TraceEngine::search` pipeline, finds it. No
//! raw SQL touches `trace_attrs_idx` anywhere — every observation goes
//! through the executor.
//!
//! The orphan window is deterministic, not a wall-clock race: the local
//! `SwallowFirstInserter` copy (the #139 seam, `pulsus-write/tests/
//! live_trace_attr_backfill.rs`) parks every backfill re-insert on a
//! CLOSED `tokio::sync::Semaphore` until the test opens it, so the 5s
//! backfill tick can fire without ever forwarding a heal.
//!
//! `pulsus-write` is a DEV-ONLY dependency here (production graph
//! unchanged and acyclic — see `Cargo.toml`).
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, mirroring the sibling live
//! suites. To run:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test traces_search_heal_live
//! podman rm -f pulsus-ch-test
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChError, ChProto, Idempotency, QuerySettings};
use pulsus_config::WriterConfig;
use pulsus_read::traces::search_plan::{SearchParams, plan_search};
use pulsus_read::{SearchPlan, TraceEngine, TraceReadConfig};
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
                 (see crates/pulsus-read/tests/traces_search_heal_live.rs for setup)"
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

/// Failure-injecting wrapper over the real attrs inserter (local copy of
/// the #139 seam — deliberately NOT exported from pulsus-write): swallows
/// the FIRST insert (never forwards) and returns a deterministic error —
/// classified Poisoned; the rows genuinely never landed.
///
/// **Deterministic orphan window**: every later call — i.e. every
/// backfill re-insert — parks on `heal_gate` until the test calls
/// [`Self::release_heals`]. The 5s backfill tick may fire, but its insert
/// attempt cannot forward while the gate is closed, so the orphan-state
/// assertions are observed with ZERO possibility of a concurrent heal —
/// no race against the wall-clock tick.
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
/// could observe it (the #139 / `trace_ingest_roundtrip.rs` hazard).
fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos() as i64
}

/// #139's L-T1 fixture, exactly: one span whose attr registration is
/// poisoned on the first insert.
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
            shared: 0,
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

fn engine_config() -> TraceReadConfig {
    TraceReadConfig {
        spans_table: "trace_spans".to_string(),
        attrs_table: "trace_attrs_idx".to_string(),
        catalog_table: "trace_tag_catalog".to_string(),
        edges_table: "trace_edges".to_string(),
        max_candidates: 100_000,
        scan_budget_rows: 50_000_000,
        generator_max_memory_bytes: 536_870_912,
        distributed: false,
        skip_unavailable_shards: false,
    }
}

/// Bounded real-time poll: up to ~30s for the 5s backfill tick plus
/// insert latency. Never a wall-time assert — only a budget.
async fn poll_until(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..150 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("condition not reached within the 30s poll budget: {what}");
}

/// Issue #167 (the #139 L-T1 deferred acceptance criterion): the attrs
/// inserter fails first while the span commits → under a CLOSED heal
/// gate the span is fetchable by trace id (real point-read path) while
/// the attribute-scoped TraceQL search returns NOTHING (the orphan,
/// observed race-free); after `release_heals` + the backfill tick, the
/// IDENTICAL plan — built by `pulsus_traceql::parse` → `plan_search` and
/// executed by `TraceEngine::search` — finds the healed trace.
#[tokio::test]
async fn healed_attr_registration_is_found_by_attribute_scoped_traceql_search() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_trace_search_heal";
    init_db(&bootstrap, db).await;

    let client = Arc::new(
        ChClient::new(test_config(db))
            .await
            .expect("connect (writer db)"),
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

    let engine = TraceEngine::new(
        ChClient::new(test_config(db))
            .await
            .expect("connect (engine)"),
        engine_config(),
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

    // The one attribute-scoped plan, built ONCE through the real
    // pipeline (parse → plan_search) and reused verbatim pre- and
    // post-heal: same window, same generator SQL, same evaluation —
    // the only variable across the two executions is the heal.
    let query =
        pulsus_traceql::parse(r#"{ span.http.status_code = "500" }"#).expect("query parses");
    let plan: SearchPlan = plan_search(
        &query,
        &SearchParams {
            start_ns: ts_ns - 3_600_000_000_000,
            end_ns: ts_ns + 3_600_000_000_000,
            limit: 20,
            spss: 10,
        },
        &engine.search_ctx(),
    )
    .expect("query plans");

    // Planner-regression guard (standing query-performance directive):
    // the AttrEq leaf must compile to exactly one phase-1 generator
    // against the `(key, val, scope)` primary-key prefix of
    // `trace_attrs_idx` — a silent fallback to the time-range span scan
    // would still "find" the trace post-heal while abandoning the
    // EXPLAIN-gated index path.
    assert_eq!(
        plan.generator_sqls.len(),
        1,
        "one AttrEq leaf plans exactly one candidate generator"
    );
    assert!(
        plan.generator_sqls[0].contains("trace_attrs_idx"),
        "the attr-equality generator must target trace_attrs_idx, got:\n{}",
        plan.generator_sqls[0]
    );

    // The orphan, observed DETERMINISTICALLY: the heal gate is still
    // closed, so no backfill re-insert can forward regardless of how
    // many 5s ticks fire during this phase — the observations below
    // cannot race a heal. (a) nothing forwarded, (b) the span becomes
    // durable — fetchable through the real point-read path (the sync
    // waiter short-circuited on the attrs Err, so poll for the spans
    // generation's commit), (c) with the span durable, the
    // attribute-scoped search still returns NOTHING: the orphan the
    // #139 backfill exists to heal, now stated through the executor.
    assert_eq!(attrs.forwarded(), 0);
    assert_eq!(writer.metrics().attrs_backfill.healed_total, 0);
    let mut committed_spans = Vec::new();
    for _ in 0..150 {
        committed_spans = engine
            .fetch_by_id(TRACE_ID_HEX)
            .await
            .expect("point read executes");
        if !committed_spans.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(committed_spans.len(), 1, "the span committed");
    assert_eq!(committed_spans[0].span_id, SPAN_ID);
    let pre_heal = engine
        .search(&plan)
        .await
        .expect("pre-heal search executes");
    assert!(
        pre_heal.traces.is_empty(),
        "span durable, registration absent — the attribute-scoped search must \
         return nothing under the closed heal gate (no concurrent heal possible), \
         got {} trace(s)",
        pre_heal.traces.len()
    );
    assert_eq!(pre_heal.returned, 0);
    assert_eq!(attrs.forwarded(), 0, "the gate held: nothing forwarded");

    // Release the gate: the (possibly already parked) backfill
    // re-insert proceeds and heals the registration row.
    attrs.release_heals();
    poll_until("the backfill re-insert heals the attr-index row", || {
        attrs.forwarded() >= 1 && writer.metrics().attrs_backfill.healed_total == 1
    })
    .await;

    // The deferred #139 criterion: the IDENTICAL plan, executed by the
    // real executor, now finds the healed trace.
    let post_heal = engine
        .search(&plan)
        .await
        .expect("post-heal search executes");
    assert!(!post_heal.partial, "a one-span search is never partial");
    assert_eq!(
        post_heal.returned, 1,
        "the healed trace is found by the attribute-scoped search"
    );
    assert_eq!(post_heal.traces.len(), 1);
    let trace = &post_heal.traces[0];
    assert_eq!(trace.trace_id, TRACE_ID);
    assert_eq!(trace.root.service, "checkout");
    assert_eq!(trace.root.name, "op-a");
    assert_eq!(trace.matched, 1, "exactly the one matched span");
    assert_eq!(trace.spans.len(), 1);
    assert_eq!(trace.spans[0].name, "op-a");

    writer.shutdown(Duration::from_secs(5)).await;
    drop_database(&bootstrap, db).await;
}
