//! `TraceWriter` hermetic backfill tests (issue #139): the
//! `trace_attrs_idx` Poisoned-only registration backfill — heal (T1), the
//! #9 pins (T2/T3), and the structural append-only exclusion for
//! `trace_spans` (T4). All against a mock `BlockInserter` — no real
//! ClickHouse (see `tests/live_trace_attr_backfill.rs` for the
//! `PULSUS_TEST_CLICKHOUSE=1`-gated live counterpart). Mirrors
//! `tests/writer.rs`'s harness; the mock is duplicated because each
//! `tests/*.rs` file compiles as its own crate.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pulsus_clickhouse::{ChError, ChRow};
use pulsus_config::WriterConfig;
use pulsus_write::writer::{
    BackfillMetricsSnapshot, BlockInserter, TraceWriter, TraceWriterTables,
};
use pulsus_write::{AttrRecord, ParsedTraces, SpanRecord, TraceSink};

#[derive(Clone, Copy, Debug)]
enum MockBehavior {
    Ok,
    Poison,
    Uncertain,
    PoisonThenOk,
    PoisonThenUncertain,
}

struct MockInserter {
    behavior: Mutex<MockBehavior>,
    calls: AtomicUsize,
    last_row_count: Mutex<usize>,
    /// The most recent call's rows, JSON-serialized, for content asserts.
    last_rows_json: Mutex<String>,
    /// Only consulted under the `*Then*` behaviors: the number of
    /// remaining calls that must fail before the second phase begins.
    fail_remaining: AtomicUsize,
}

impl MockInserter {
    fn new(behavior: MockBehavior) -> Arc<Self> {
        Self::new_with_fail_budget(behavior, 0)
    }

    fn new_with_fail_budget(behavior: MockBehavior, n: usize) -> Arc<Self> {
        Arc::new(MockInserter {
            behavior: Mutex::new(behavior),
            calls: AtomicUsize::new(0),
            last_row_count: Mutex::new(0),
            last_rows_json: Mutex::new(String::new()),
            fail_remaining: AtomicUsize::new(n),
        })
    }

    fn consume_fail_budget(&self) -> bool {
        self.fail_remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn last_row_count(&self) -> usize {
        *self.last_row_count.lock().expect("mock mutex poisoned")
    }

    fn last_rows_json(&self) -> String {
        self.last_rows_json
            .lock()
            .expect("mock mutex poisoned")
            .clone()
    }
}

impl<R: ChRow> BlockInserter<R> for MockInserter {
    fn insert<'a>(
        &'a self,
        _table: &'a str,
        rows: &'a [R],
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        *self.last_row_count.lock().expect("mock mutex poisoned") = rows.len();
        *self.last_rows_json.lock().expect("mock mutex poisoned") =
            serde_json::to_string(rows).unwrap_or_default();
        let behavior = *self.behavior.lock().expect("mock mutex poisoned");
        Box::pin(async move {
            match behavior {
                MockBehavior::Ok => Ok(()),
                MockBehavior::Poison => Err(ChError::Decode("mock poison".to_string())),
                MockBehavior::Uncertain => {
                    Err(ChError::InsertUncertain("mock uncertain".to_string()))
                }
                MockBehavior::PoisonThenOk => {
                    if self.consume_fail_budget() {
                        Err(ChError::Decode("mock poison".to_string()))
                    } else {
                        Ok(())
                    }
                }
                MockBehavior::PoisonThenUncertain => {
                    if self.consume_fail_budget() {
                        Err(ChError::Decode("mock poison".to_string()))
                    } else {
                        Err(ChError::InsertUncertain("mock uncertain".to_string()))
                    }
                }
            }
        })
    }
}

fn writer_with(
    cfg: WriterConfig,
    spans: Arc<MockInserter>,
    attrs: Arc<MockInserter>,
) -> TraceWriter {
    TraceWriter::with_inserters_with_tables(spans, attrs, &cfg, TraceWriterTables::traces_default())
}

const TS_NS: i64 = 1_700_000_000_000_000_000;

fn span_record(trace_seed: u8) -> SpanRecord {
    SpanRecord {
        trace_id: [trace_seed; 16],
        span_id: [0x01; 8],
        parent_id: [0; 8],
        name: "op-a".to_string(),
        service: "checkout".to_string(),
        timestamp_ns: TS_NS,
        duration_ns: 1_000_000_000,
        status_code: 2,
        kind: 3,
        shared: 0,
        payload: vec![0xDE, 0xAD],
    }
}

fn attr_record(trace_seed: u8, key: &str, val: &str) -> AttrRecord {
    AttrRecord {
        date: 19_675,
        key: key.to_string(),
        scope: "span".to_string(),
        val: val.to_string(),
        val_num: None,
        timestamp_ns: TS_NS,
        trace_id: [trace_seed; 16],
        span_id: [0x01; 8],
        duration_ns: 1_000_000_000,
    }
}

/// One span with two indexed attrs — a real request's smallest
/// interesting shape (the attrs generation carries >1 row, so T1 can
/// assert the re-insert's exact row count).
fn batch_for(trace_seed: u8) -> ParsedTraces {
    ParsedTraces {
        spans: vec![span_record(trace_seed)],
        attrs: vec![
            attr_record(trace_seed, "http.status_code", "500"),
            attr_record(trace_seed, "peer.service", "db"),
        ],
        ..Default::default()
    }
}

/// Paused-time poll (mirrors `tests/writer.rs`'s helper).
async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..600 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("condition not reached within the paused-time budget: {what}");
}

/// Issue #139 T1 (hermetic heal — fails with `trace_attrs_idx`'s
/// `on_flush_poisoned` reverted to `None`): a Poisoned attrs flush with a
/// committed spans flush resolves the sync waiter `Err` (the span is
/// fetchable by ID but invisible to attribute search — the orphan), then
/// the backfill task re-inserts exactly the failed attr rows on its 5s
/// tick and confirms the heal. The only spool write is the original
/// generation failure.
#[tokio::test(start_paused = true)]
async fn attrs_backfill_reinserts_a_failed_attr_index_generation_until_durable() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1; // flush on the very next append

    let spans = MockInserter::new(MockBehavior::Ok);
    let attrs = MockInserter::new_with_fail_budget(MockBehavior::PoisonThenOk, 1);
    let writer = writer_with(cfg, spans.clone(), attrs.clone());

    let wait = writer.admit_flush(batch_for(0x61)).expect("queue has room");
    let result = tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout");
    assert!(
        result.is_err(),
        "the poisoned attrs generation must still resolve the sync waiter Err"
    );
    assert_eq!(spans.call_count(), 1, "the spans generation committed");
    assert_eq!(attrs.call_count(), 1);
    assert_eq!(writer.metrics().attrs_backfill.enqueued_total, 2);

    wait_until("the backfill re-insert heals the attr index rows", || {
        writer.metrics().attrs_backfill.healed_total == 2
    })
    .await;

    assert_eq!(attrs.call_count(), 2, "exactly one re-insert");
    assert_eq!(
        attrs.last_row_count(),
        2,
        "the re-insert carries exactly the two failed attr rows"
    );
    let json = attrs.last_rows_json();
    assert!(
        json.contains("http.status_code") && json.contains("peer.service"),
        "the re-inserted rows are the failed attr rows, got {json}"
    );
    let metrics = writer.metrics();
    assert_eq!(metrics.attrs_backfill.pending, 0);
    assert_eq!(
        metrics.spool_poison_total, 1,
        "the only spool write is the original generation failure"
    );
    assert_eq!(
        metrics.spool_uncertain_total, 0,
        "the backfill path never spools"
    );
}

/// Issue #139 T2 (#9 pin, generation path): an Uncertain attrs generation
/// failure is NEVER enqueued or replayed.
#[tokio::test(start_paused = true)]
async fn attrs_uncertain_generation_failure_is_never_enqueued_or_replayed() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    let spans = MockInserter::new(MockBehavior::Ok);
    let attrs = MockInserter::new(MockBehavior::Uncertain);
    let writer = writer_with(cfg, spans, attrs.clone());

    let wait = writer.admit_flush(batch_for(0x62)).expect("queue has room");
    tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect_err("an insert-uncertain attrs flush resolves Err");

    // Advance well past three backfill tick intervals (5s each).
    tokio::time::sleep(Duration::from_secs(16)).await;

    let metrics = writer.metrics();
    assert_eq!(
        attrs.call_count(),
        1,
        "an uncertain generation failure must never be re-inserted (#9)"
    );
    assert_eq!(metrics.attrs_backfill.enqueued_total, 0);
    assert_eq!(metrics.attrs_backfill.pending, 0, "the backlog stays empty");
}

/// Issue #139 T3 (no poison spin; the tick's own `InsertUncertain` is
/// covered by the identical generic path pinned in M4/`tests/writer.rs` —
/// this pins the deterministic-abandon arm for the attrs backlog): a
/// deterministic re-insert failure abandons in exactly one attempt.
#[tokio::test(start_paused = true)]
async fn attrs_deterministic_backfill_failure_abandons_without_spinning() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    let spans = MockInserter::new(MockBehavior::Ok);
    let attrs = MockInserter::new(MockBehavior::Poison);
    let writer = writer_with(cfg, spans, attrs.clone());

    let wait = writer.admit_flush(batch_for(0x63)).expect("queue has room");
    tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect_err("the poisoned attrs generation resolves Err");

    wait_until("the deterministic re-insert failure is abandoned", || {
        writer.metrics().attrs_backfill.abandoned_total == 2
    })
    .await;
    assert_eq!(writer.metrics().attrs_backfill.pending, 0);

    tokio::time::sleep(Duration::from_secs(16)).await;
    let metrics = writer.metrics();
    assert_eq!(
        attrs.call_count(),
        2,
        "one generation insert plus exactly one abandoned re-insert — no spin"
    );
    assert_eq!(metrics.attrs_backfill.abandoned_total, 2);
    assert_eq!(
        metrics.spool_poison_total, 1,
        "the backfill abandonment never double-spools"
    );
    assert_eq!(metrics.spool_uncertain_total, 0);
}

/// Issue #139 T3 companion (#9 pin, tick path): an attrs backfill
/// re-insert whose OWN outcome is `InsertUncertain` is terminal —
/// abandoned, never retried.
#[tokio::test(start_paused = true)]
async fn attrs_uncertain_backfill_outcome_is_terminally_abandoned_never_retried() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    let spans = MockInserter::new(MockBehavior::Ok);
    let attrs = MockInserter::new_with_fail_budget(MockBehavior::PoisonThenUncertain, 1);
    let writer = writer_with(cfg, spans, attrs.clone());

    let wait = writer.admit_flush(batch_for(0x64)).expect("queue has room");
    tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect_err("the poisoned attrs generation resolves Err");

    wait_until("the uncertain re-insert outcome is abandoned", || {
        writer.metrics().attrs_backfill.abandoned_total == 2
    })
    .await;
    assert_eq!(attrs.call_count(), 2);

    tokio::time::sleep(Duration::from_secs(16)).await;
    assert_eq!(
        attrs.call_count(),
        2,
        "an uncertain backfill outcome must never be retried (#9)"
    );
    assert_eq!(writer.metrics().attrs_backfill.abandoned_total, 2);
}

/// Issue #139 T4 (structural append-only exclusion, #9 in full): a
/// Poisoned `trace_spans` flush leaves the attrs backfill counter set at
/// zero — the span table has no `on_flush_poisoned` hook at all.
#[tokio::test(start_paused = true)]
async fn poisoned_span_flush_never_touches_the_attrs_backfill_backlog() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    let spans = MockInserter::new(MockBehavior::Poison);
    let attrs = MockInserter::new(MockBehavior::Ok);
    let writer = writer_with(cfg, spans.clone(), attrs);

    let wait = writer.admit_flush(batch_for(0x65)).expect("queue has room");
    tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect_err("the poisoned spans generation resolves Err");

    // Advance well past three backfill tick intervals.
    tokio::time::sleep(Duration::from_secs(16)).await;

    assert_eq!(
        writer.metrics().attrs_backfill,
        BackfillMetricsSnapshot::default(),
        "a poisoned trace_spans flush must never touch the attrs backlog \
         (structural #9 exclusion)"
    );
    assert_eq!(spans.call_count(), 1, "no span re-insert, ever");
    assert_eq!(writer.metrics().spool_poison_total, 1);
}
