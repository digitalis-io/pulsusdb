//! Writer-core tests (issue #9 architect plan + review cycles' required
//! test gaps): concurrent-admit backpressure bound, sync wait-join across
//! both tables, the duplicate stream-registration race, and shutdown
//! settlement of already-admitted waiters. All against a mock
//! [`BlockInserter`] — no real ClickHouse (architect plan: "no real
//! ClickHouse in unit tests").

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pulsus_clickhouse::{ChError, ChRow};
use pulsus_config::WriterConfig;
use pulsus_model::{Date, LabelSet, UnixNano};
use pulsus_write::writer::{BlockInserter, LogSampleRow, LogWriter};
use pulsus_write::{Backpressure, LogRow, LogSink, ParsedLogs, StreamRow};

/// Scriptable mock [`BlockInserter`]: every call is recorded (row count),
/// and its outcome is whatever [`MockInserter::new`]/[`MockInserter::
/// new_failing_n_times_then_ok`] configured — `Ok`, a classified
/// poison/uncertain failure, `Hang` (never resolves, for exercising
/// shutdown's forced-settlement/timeout path), or `RetryThenOk` (fails
/// with a retryable pre-send error a fixed number of times, then
/// succeeds, for exercising the retry/whole-batch-resend path).
#[derive(Clone, Copy, Debug)]
enum MockBehavior {
    Ok,
    Poison,
    Uncertain,
    Hang,
    RetryThenOk,
    /// Issue #134 backfill tests: fail `fail_remaining` calls with a
    /// deterministic poison error, then succeed (`PoisonThenOk`), return
    /// `InsertUncertain` (`PoisonThenUncertain`), or hang forever
    /// (`PoisonThenHang`).
    PoisonThenOk,
    PoisonThenUncertain,
    PoisonThenHang,
}

struct MockInserter {
    behavior: Mutex<MockBehavior>,
    calls: AtomicUsize,
    last_row_count: Mutex<usize>,
    /// The row count of *every* `insert` call, in order — unlike
    /// `last_row_count`, this lets a test assert that every retry attempt
    /// resent the whole batch (no partial/shrinking visibility across
    /// attempts).
    row_counts: Mutex<Vec<usize>>,
    /// Only consulted under `MockBehavior::RetryThenOk`: the number of
    /// remaining calls that must fail with a retryable error before the
    /// mock starts returning `Ok`.
    fail_remaining: AtomicUsize,
}

impl MockInserter {
    fn new(behavior: MockBehavior) -> Arc<Self> {
        Arc::new(MockInserter {
            behavior: Mutex::new(behavior),
            calls: AtomicUsize::new(0),
            last_row_count: Mutex::new(0),
            row_counts: Mutex::new(Vec::new()),
            fail_remaining: AtomicUsize::new(0),
        })
    }

    /// A mock that fails its first `n` calls with a retryable pre-send
    /// error ([`ChError::Timeout`]) and succeeds from the `n + 1`th call
    /// onward.
    fn new_failing_n_times_then_ok(n: usize) -> Arc<Self> {
        Self::new_with_fail_budget(MockBehavior::RetryThenOk, n)
    }

    /// A mock whose `behavior` consults a budget of `n` initial failures
    /// (the `*Then*` variants' first phase).
    fn new_with_fail_budget(behavior: MockBehavior, n: usize) -> Arc<Self> {
        Arc::new(MockInserter {
            behavior: Mutex::new(behavior),
            calls: AtomicUsize::new(0),
            last_row_count: Mutex::new(0),
            row_counts: Mutex::new(Vec::new()),
            fail_remaining: AtomicUsize::new(n),
        })
    }

    /// Consumes one unit of the remaining-failures budget; `true` while
    /// budget remains.
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

    /// The row count recorded on every call, in call order.
    fn row_counts(&self) -> Vec<usize> {
        self.row_counts.lock().expect("mock mutex poisoned").clone()
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
        self.row_counts
            .lock()
            .expect("mock mutex poisoned")
            .push(rows.len());
        let behavior = *self.behavior.lock().expect("mock mutex poisoned");
        Box::pin(async move {
            match behavior {
                MockBehavior::Ok => Ok(()),
                MockBehavior::Poison => Err(ChError::Decode("mock poison".to_string())),
                MockBehavior::Uncertain => {
                    Err(ChError::InsertUncertain("mock uncertain".to_string()))
                }
                MockBehavior::Hang => std::future::pending::<Result<(), ChError>>().await,
                MockBehavior::RetryThenOk => {
                    // Consume one unit of the remaining-failures budget
                    // per call; once it hits zero, every further call
                    // succeeds.
                    if self.consume_fail_budget() {
                        Err(ChError::Timeout("mock retryable timeout".to_string()))
                    } else {
                        Ok(())
                    }
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
                MockBehavior::PoisonThenHang => {
                    if self.consume_fail_budget() {
                        Err(ChError::Decode("mock poison".to_string()))
                    } else {
                        std::future::pending::<Result<(), ChError>>().await
                    }
                }
            }
        })
    }
}

fn labels_with_service(service: &str) -> LabelSet {
    let (labels, _) =
        LabelSet::from_normalized([("service_name".to_string(), service.to_string())]);
    labels
}

/// One `log_samples` row plus, if `new_stream` is set, its `StreamRow`
/// (fresh `(fingerprint, month)` — a real request's first record for a
/// stream `parse()` has never seen before).
fn batch_for(fingerprint: u64, service: &str, timestamp_ns: i64, new_stream: bool) -> ParsedLogs {
    let mut out = ParsedLogs {
        rows: vec![LogRow {
            service: service.to_string(),
            fingerprint,
            timestamp_ns: UnixNano(timestamp_ns),
            severity: 0,
            body: "hello".to_string(),
            structured_metadata: String::new(),
        }],
        ..Default::default()
    };
    if new_stream {
        out.streams.push(StreamRow {
            month: Date::start_of_month_utc(timestamp_ns).unwrap(),
            fingerprint,
            service: service.to_string(),
            labels: labels_with_service(service),
            updated_ns: timestamp_ns,
        });
    }
    out
}

fn writer_with(
    cfg: WriterConfig,
    samples: Arc<MockInserter>,
    streams: Arc<MockInserter>,
) -> LogWriter {
    LogWriter::with_inserters(samples, streams, &cfg)
}

/// Required test (review cycle, "concurrent-admit bound"): many
/// concurrent `admit` calls against a small `PULSUS_INGEST_QUEUE_BYTES`
/// must never let `queued_bytes` exceed the limit, and the calls that
/// lose the race get `Backpressure`.
#[tokio::test]
async fn concurrent_admit_never_exceeds_the_queue_bytes_limit() {
    let one_row_bytes = LogSampleRow {
        service: "svc".to_string(),
        fingerprint: 0,
        timestamp_ns: 0,
        severity: 0,
        body: "hello".to_string(),
        structured_metadata: String::new(),
    }
    .est_bytes();

    let admits = 40u64;
    let limit = one_row_bytes * 10; // room for exactly 10 successful admits
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = u64::MAX; // never auto-flush: bytes stay reserved
    cfg.ingest_queue_bytes.0 = limit;

    let samples = MockInserter::new(MockBehavior::Hang);
    let streams = MockInserter::new(MockBehavior::Hang);
    let writer = Arc::new(writer_with(cfg, samples, streams));

    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..admits {
        let writer = writer.clone();
        tasks.spawn(async move { writer.admit(batch_for(i, "svc", 0, false)) });
    }
    let results: Vec<Result<(), Backpressure>> = tasks.join_all().await;

    let ok_count = results.iter().filter(|r| r.is_ok()).count() as u64;
    let backpressure_count = results.iter().filter(|r| r.is_err()).count() as u64;

    assert_eq!(ok_count + backpressure_count, admits);
    assert!(
        backpressure_count > 0,
        "expected at least one admit to be rejected under a deliberately tight limit"
    );
    assert!(ok_count > 0, "expected at least one admit to succeed");

    let queue_bytes = writer.metrics().queue_bytes;
    assert!(
        queue_bytes <= limit,
        "queued_bytes ({queue_bytes}) must never exceed the configured limit ({limit})"
    );
    assert_eq!(queue_bytes, ok_count * one_row_bytes);
}

/// Required test (review cycle, "sync streams-fail/samples-succeed"): a
/// sync request's `FlushWait` must resolve `Err`, not `Ok`, when its
/// joined `log_streams` generation fails even though `log_samples`
/// succeeds — closing the "sync success reported before stream
/// registration is durable" gap the review flagged.
#[tokio::test]
async fn sync_admit_flush_resolves_err_when_streams_flush_fails_even_though_samples_succeed() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1; // flush on the very next append

    let samples = MockInserter::new(MockBehavior::Ok);
    let streams = MockInserter::new(MockBehavior::Poison);
    let writer = writer_with(cfg, samples.clone(), streams.clone());

    let wait = writer
        .admit_flush(batch_for(1, "svc", 1_700_000_000_000_000_000, true))
        .expect("queue has room");
    let result = tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("flush settles within the test timeout");

    assert!(
        result.is_err(),
        "expected Err because the streams generation was poisoned, got {result:?}"
    );
    assert_eq!(samples.call_count(), 1);
    assert_eq!(streams.call_count(), 1);
}

/// `ChError::InsertUncertain` is spooled and reported to the sync waiter
/// exactly like a poison failure, but classified distinctly (never
/// retried, never conflated with a deterministic poison failure) — see
/// `writer::error::WriteError::Uncertain`'s doc comment for why this
/// distinction is the one hard invariant this crate enforces.
#[tokio::test]
async fn sync_admit_flush_resolves_err_and_spools_uncertain_on_insert_uncertain() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    let samples = MockInserter::new(MockBehavior::Ok);
    let streams = MockInserter::new(MockBehavior::Uncertain);
    let writer = writer_with(cfg, samples, streams);

    let wait = writer
        .admit_flush(batch_for(2, "svc", 1_700_000_000_000_000_000, true))
        .expect("queue has room");
    let result = tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("flush settles within the test timeout");

    let err = result.expect_err("an insert-uncertain streams flush must resolve Err");
    assert!(err.to_string().to_lowercase().contains("audit"));
    assert_eq!(writer.metrics().spool_uncertain_total, 1);
}

/// Required test (review cycle, "duplicate-admit race"): two admits for
/// the same brand-new `(fingerprint, month)` key, both landing before the
/// first registration flush settles (guaranteed here by never `.await`ing
/// between them on the single-threaded test runtime), must both emit
/// their `StreamRow` — the LRU is populated only on flush success, so
/// neither admit can see the other's not-yet-durable write — and this
/// must be harmless: no panic, no orphaned sample.
#[tokio::test]
async fn duplicate_admit_race_before_the_first_stream_flush_settles_is_harmless() {
    let cfg = WriterConfig::default(); // large batch_bytes/batch_ms: nothing auto-flushes yet

    let samples = MockInserter::new(MockBehavior::Ok);
    let streams = MockInserter::new(MockBehavior::Ok);
    let writer = writer_with(cfg, samples.clone(), streams.clone());

    // Two synchronous (non-`.await`ed) admits for the identical new
    // stream key: neither yields control to the executor in between, so
    // the flush task cannot have run — both land in the same still-open
    // generation, exactly the "before the first flush settles" race.
    writer
        .admit(batch_for(7, "svc", 1_700_000_000_000_000_000, true))
        .expect("queue has room");
    writer
        .admit(batch_for(7, "svc", 1_700_000_000_000_000_000, true))
        .expect("queue has room");

    assert_eq!(
        writer.metrics().stream_registrations_total,
        2,
        "both admits' StreamRows must be counted — neither was suppressed"
    );

    // Force the still-open generation to flush and settle.
    tokio::time::timeout(
        Duration::from_secs(5),
        writer.shutdown(Duration::from_secs(2)),
    )
    .await
    .expect("shutdown completes within the test timeout");

    assert_eq!(streams.call_count(), 1, "one flush carried both duplicates");
    assert_eq!(
        streams.last_row_count(),
        2,
        "the duplicate StreamRow was not deduplicated pre-flush (harmless, per architect plan)"
    );
    assert_eq!(samples.call_count(), 1);
}

/// Required test (review cycle, "shutdown_settles_inflight_waiters"): a
/// sync request joined to both `log_samples` and `log_streams`, then
/// `shutdown` before either flush completes, must resolve every waiter
/// with a shutdown error, release the reservation back to zero, and have
/// both flush tasks exit within the drain deadline.
#[tokio::test]
async fn shutdown_settles_inflight_waiters() {
    let cfg = WriterConfig::default();

    // `Hang` guarantees the drain's flush attempt cannot complete before
    // the deadline, forcing the forced-settlement path (not a lucky
    // same-tick success).
    let samples = MockInserter::new(MockBehavior::Hang);
    let streams = MockInserter::new(MockBehavior::Hang);
    let writer = writer_with(cfg, samples, streams);

    let wait = writer
        .admit_flush(batch_for(9, "svc", 1_700_000_000_000_000_000, true))
        .expect("queue has room");

    // Shutdown before anything has flushed: the flush tasks have not even
    // been polled yet (no `.await` happened between `admit_flush` and
    // here on this single-threaded test runtime).
    tokio::time::timeout(
        Duration::from_secs(5),
        writer.shutdown(Duration::from_millis(50)),
    )
    .await
    .expect("both flush tasks exit within the drain deadline");

    let result = wait.await;
    let err = result.expect_err("a shutdown-settled generation must resolve Err, not Ok");
    let message = err.to_string().to_lowercase();
    assert!(
        message.contains("shut"),
        "expected a shutdown-flavoured error, got: {message}"
    );

    assert_eq!(
        writer.metrics().queue_bytes,
        0,
        "reserved bytes must be released back to zero exactly once"
    );
}

/// `admit`/`admit_flush` reject outright once shutdown has begun (phase
/// 1 of the architect plan amendment 2: "stop admitting").
#[tokio::test]
async fn admission_is_rejected_once_shutdown_has_begun() {
    let cfg = WriterConfig::default();
    let samples = MockInserter::new(MockBehavior::Ok);
    let streams = MockInserter::new(MockBehavior::Ok);
    let writer = writer_with(cfg, samples, streams);

    writer.shutdown(Duration::from_secs(1)).await;

    let result = writer.admit(batch_for(1, "svc", 0, false));
    assert_eq!(result, Err(Backpressure));
}

/// Required test (architect plan amendment 3, code-review FAIL finding
/// 1): a size-triggered *normal-path* flush already inside its (hanging)
/// insert call, when shutdown fires, must be bounded by the drain
/// deadline and force-settle with `WriteError::ShuttingDown` — not the
/// `shutdown_settles_inflight_waiters` drain-path case above, whose
/// shutdown fires before either flush task has even been polled. This
/// exercises `writer::table::settle_generation`'s shutdown-aware
/// mid-flight bounding (the bug: the old normal path awaited its insert
/// with no budget at all, so it never observed shutdown).
#[tokio::test]
async fn shutdown_force_settles_a_normal_path_flush_already_in_progress() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1; // flush the very next append, for both tables

    let samples = MockInserter::new(MockBehavior::Hang);
    let streams = MockInserter::new(MockBehavior::Hang);
    let writer = writer_with(cfg, samples.clone(), streams.clone());

    let wait = writer
        .admit_flush(batch_for(11, "svc", 1_700_000_000_000_000_000, true))
        .expect("queue has room");

    // Wait until the normal-path flush task has actually entered the
    // (hanging) insert call for both tables -- not merely been notified
    // -- so shutdown below races an in-progress insert, per finding 1.
    for _ in 0..1_000 {
        if samples.call_count() > 0 && streams.call_count() > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        samples.call_count(),
        1,
        "the samples normal-path flush must have started before shutdown"
    );
    assert_eq!(
        streams.call_count(),
        1,
        "the streams normal-path flush must have started before shutdown"
    );

    tokio::time::timeout(
        Duration::from_secs(5),
        writer.shutdown(Duration::from_millis(50)),
    )
    .await
    .expect("both flush tasks exit within the drain deadline even mid-flush");

    let result = wait.await;
    let err =
        result.expect_err("a normal-path flush force-settled by shutdown must resolve Err, not Ok");
    let message = err.to_string().to_lowercase();
    assert!(
        message.contains("shut"),
        "expected a shutdown-flavoured error, got: {message}"
    );

    assert_eq!(
        writer.metrics().queue_bytes,
        0,
        "reserved bytes must be released back to zero exactly once"
    );
}

/// Required test (architect plan amendment 3, code-review test gap): a
/// retryable pre-send failure (`ChError::Timeout`/`Connect`/`Io`) must
/// retry the *whole* batch — no partial visibility — and eventually
/// succeed once the retry budget is not exhausted, incrementing
/// `retries_total` along the way.
#[tokio::test]
async fn retryable_pre_send_failure_resends_the_whole_batch_before_succeeding() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1; // flush the very next append

    let samples = MockInserter::new_failing_n_times_then_ok(2);
    let streams = MockInserter::new(MockBehavior::Ok);
    let writer = writer_with(cfg, samples.clone(), streams);

    let wait = writer
        .admit_flush(batch_for(42, "svc", 0, false))
        .expect("queue has room");
    let result = tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("flush eventually settles within the test timeout");

    assert!(
        result.is_ok(),
        "expected the batch to succeed once the retryable failures are exhausted, got {result:?}"
    );
    assert_eq!(
        samples.call_count(),
        3,
        "2 retryable failures plus 1 successful resend"
    );
    assert_eq!(
        samples.last_row_count(),
        1,
        "the whole batch — not a partial subset — is resent on every attempt"
    );
    assert_eq!(writer.metrics().samples.retries_total, 2);
}

/// Required test (architect plan amendment 3, code-review test gap): rows
/// below the size threshold on both tables must still flush once
/// `PULSUS_BATCH_MS` elapses, driven by the flush task's interval tick
/// alone (no size push, no shutdown).
#[tokio::test]
async fn age_trigger_flushes_both_tables_after_batch_ms_even_below_the_size_threshold() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = u64::MAX; // never size-trigger
    cfg.batch_ms = 20;

    let samples = MockInserter::new(MockBehavior::Ok);
    let streams = MockInserter::new(MockBehavior::Ok);
    let writer = writer_with(cfg, samples.clone(), streams.clone());

    writer
        .admit(batch_for(5, "svc", 1_700_000_000_000_000_000, true))
        .expect("queue has room");

    for _ in 0..200 {
        if samples.call_count() > 0 && streams.call_count() > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(
        samples.call_count(),
        1,
        "the log_samples table must flush on the age tick alone"
    );
    assert_eq!(
        streams.call_count(),
        1,
        "the log_streams table must flush on the age tick alone"
    );
}

/// Required test (round-3 review follow-up, architect-confirmed
/// test-only gap): a *multi-row* batch's retryable pre-send failures must
/// resend the whole batch, unchanged, on every attempt — not just the
/// single-row case `retryable_pre_send_failure_resends_the_whole_batch_
/// before_succeeding` above already covers.
#[tokio::test]
async fn retryable_pre_send_failure_resends_the_whole_multi_row_batch_before_succeeding() {
    let one_row_bytes = LogSampleRow {
        service: "svc".to_string(),
        fingerprint: 0,
        timestamp_ns: 0,
        severity: 0,
        body: "hello".to_string(),
        structured_metadata: String::new(),
    }
    .est_bytes();

    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = one_row_bytes * 3; // flush once all 3 rows have landed

    let samples = MockInserter::new_failing_n_times_then_ok(2);
    let streams = MockInserter::new(MockBehavior::Ok);
    let writer = writer_with(cfg, samples.clone(), streams);

    // Three synchronous (non-`.await`ed) admits land in the same
    // still-open generation (single-threaded test runtime: no yield
    // point between them) — the third crosses `batch_bytes` and notifies
    // the flush task. The last call uses `admit_flush` so the test gets a
    // waiter for the generation carrying all three rows.
    writer
        .admit(batch_for(101, "svc", 0, false))
        .expect("queue has room");
    writer
        .admit(batch_for(102, "svc", 0, false))
        .expect("queue has room");
    let wait = writer
        .admit_flush(batch_for(103, "svc", 0, false))
        .expect("queue has room");

    let result = tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("flush eventually settles within the test timeout");

    assert!(
        result.is_ok(),
        "expected the batch to succeed once the retryable failures are exhausted, got {result:?}"
    );
    assert_eq!(
        samples.call_count(),
        3,
        "2 retryable failures plus 1 successful resend"
    );
    assert_eq!(
        samples.row_counts(),
        vec![3, 3, 3],
        "every attempt resent the whole 3-row batch — no partial visibility across retries"
    );
    assert_eq!(writer.metrics().samples.retries_total, 2);
}

/// Required test (round-3 review follow-up, architect-confirmed
/// test-only gap): once a stream's `(fingerprint, month)` has been
/// durably registered by a successful `log_streams` flush, a later admit
/// for the identical key must be suppressed by the success-only `StreamLru`
/// — counted as a hit, never appended/inserted again — closing the loop
/// on `duplicate_admit_race_before_the_first_stream_flush_settles_is_
/// harmless` above, which only covers the *pre*-flush race.
#[tokio::test]
async fn post_flush_admit_of_the_same_stream_key_is_suppressed_by_the_lru() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1; // flush on the very next append

    let samples = MockInserter::new(MockBehavior::Ok);
    let streams = MockInserter::new(MockBehavior::Ok);
    let writer = writer_with(cfg, samples.clone(), streams.clone());

    // First admit: a brand-new stream key, flushed to durability — the
    // success-only LRU promotion hook runs (inside `finish_generation`)
    // strictly before this wait resolves.
    let wait = writer
        .admit_flush(batch_for(21, "svc", 1_700_000_000_000_000_000, true))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect("the first registration must flush Ok");

    assert_eq!(streams.call_count(), 1);
    assert_eq!(
        writer.metrics().stream_registrations_total,
        1,
        "one StreamRow registered so far"
    );

    // Second admit: the identical `(fingerprint, month)` key, now
    // durably known via the confirmed-flush LRU promotion.
    writer
        .admit(batch_for(21, "svc", 1_700_000_000_000_000_000, true))
        .expect("queue has room");

    let metrics = writer.metrics();
    assert_eq!(
        metrics.lru_hits_total, 1,
        "the second admit's identical key must hit the LRU"
    );
    assert_eq!(
        metrics.stream_registrations_total, 1,
        "the suppressed duplicate must not be counted as a new registration"
    );

    // Drain whatever is currently buffered (just the second admit's
    // LogRow — its StreamRow was suppressed before ever reaching a
    // buffer) and confirm no second `log_streams` insert ever happened.
    tokio::time::timeout(
        Duration::from_secs(5),
        writer.shutdown(Duration::from_secs(2)),
    )
    .await
    .expect("shutdown completes within the test timeout");

    assert_eq!(
        streams.call_count(),
        1,
        "no second log_streams insert happened for the suppressed duplicate"
    );
}

/// Paused-time poll: yields to the scheduler (auto-advancing the paused
/// clock) until `cond` holds — bounded so a regression fails loudly
/// instead of hanging.
async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..600 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("condition not reached within the paused-time budget: {what}");
}

/// Issue #134 AC1 (hermetic heal — fails without the backfill): a
/// Poisoned `log_streams` flush resolves the sync waiter `Err`, then the
/// backfill task re-inserts the registration on its 5s tick and confirms
/// the heal. The only spool write is the original generation failure —
/// the backfill itself never spools.
#[tokio::test(start_paused = true)]
async fn backfill_reinserts_a_failed_stream_registration_until_durable() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1; // flush on the very next append

    let samples = MockInserter::new(MockBehavior::Ok);
    let streams = MockInserter::new_with_fail_budget(MockBehavior::PoisonThenOk, 1);
    let writer = writer_with(cfg, samples, streams.clone());

    let wait = writer
        .admit_flush(batch_for(31, "svc", 1_700_000_000_000_000_000, true))
        .expect("queue has room");
    let result = tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout");
    assert!(
        result.is_err(),
        "the poisoned streams generation must still resolve the sync waiter Err"
    );
    assert_eq!(streams.call_count(), 1);
    assert_eq!(writer.metrics().backfill_enqueued_total, 1);

    // The 5s backfill tick re-inserts exactly the one pending row —
    // without the fix there is no second streams insert, ever.
    wait_until("the backfill re-insert heals the registration", || {
        writer.metrics().backfill_healed_total == 1
    })
    .await;

    assert_eq!(streams.call_count(), 2, "exactly one re-insert");
    assert_eq!(
        streams.last_row_count(),
        1,
        "the re-insert carries exactly the one backlogged row"
    );
    let metrics = writer.metrics();
    assert_eq!(metrics.backfill_enqueued_total, 1);
    assert_eq!(metrics.backfill_healed_total, 1);
    assert_eq!(metrics.backfill_pending, 0);
    assert_eq!(
        metrics.spool_poison_total, 1,
        "the only spool write is the original generation failure"
    );
    assert_eq!(
        metrics.spool_uncertain_total, 0,
        "the backfill path never spools"
    );
}

/// Issue #134 AC2 (LRU promotion on heal): a confirmed backfill re-insert
/// promotes the `(fingerprint, month)` into the success-only `StreamLru`,
/// so a later admit of the same key is an LRU hit, never a re-emitted
/// `StreamRow`.
#[tokio::test(start_paused = true)]
async fn backfill_heal_promotes_the_stream_lru() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    let samples = MockInserter::new(MockBehavior::Ok);
    let streams = MockInserter::new_with_fail_budget(MockBehavior::PoisonThenOk, 1);
    let writer = writer_with(cfg, samples, streams.clone());

    let wait = writer
        .admit_flush(batch_for(32, "svc", 1_700_000_000_000_000_000, true))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect_err("the poisoned streams generation resolves Err");

    wait_until("the backfill re-insert heals the registration", || {
        writer.metrics().backfill_healed_total == 1
    })
    .await;
    assert_eq!(writer.metrics().stream_registrations_total, 1);

    // Re-admit the identical `(fingerprint, month)`: promoted by the
    // confirmed heal, it must hit the LRU — no new StreamRow.
    writer
        .admit(batch_for(32, "svc", 1_700_000_000_000_000_000, true))
        .expect("queue has room");

    let metrics = writer.metrics();
    assert_eq!(
        metrics.lru_hits_total, 1,
        "the healed key must be a confirmed-flush LRU hit"
    );
    assert_eq!(
        metrics.stream_registrations_total, 1,
        "no re-emitted StreamRow after the heal"
    );
}

/// Issue #134 AC3 (#9 pin, generation path): an Uncertain `log_streams`
/// generation failure is NEVER enqueued or replayed — the hook exists
/// only in the Poisoned arm, so the backfill task has nothing to do.
#[tokio::test(start_paused = true)]
async fn uncertain_generation_failure_is_never_enqueued_or_replayed() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    let samples = MockInserter::new(MockBehavior::Ok);
    let streams = MockInserter::new(MockBehavior::Uncertain);
    let writer = writer_with(cfg, samples, streams.clone());

    let wait = writer
        .admit_flush(batch_for(33, "svc", 1_700_000_000_000_000_000, true))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect_err("an insert-uncertain streams flush resolves Err");

    // Advance well past three backfill tick intervals (5s each).
    tokio::time::sleep(Duration::from_secs(16)).await;

    let metrics = writer.metrics();
    assert_eq!(
        streams.call_count(),
        1,
        "an uncertain generation failure must never be re-inserted (#9)"
    );
    assert_eq!(metrics.backfill_enqueued_total, 0);
    assert_eq!(metrics.backfill_pending, 0, "the backlog stays empty");
}

/// Issue #134 AC4 (#9 pin, tick path): a backfill re-insert whose OWN
/// outcome is `InsertUncertain` is terminal — abandoned, never retried.
/// Spool counters are unchanged across the abandonment (no double-spool).
#[tokio::test(start_paused = true)]
async fn uncertain_backfill_outcome_is_terminally_abandoned_never_retried() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    let samples = MockInserter::new(MockBehavior::Ok);
    let streams = MockInserter::new_with_fail_budget(MockBehavior::PoisonThenUncertain, 1);
    let writer = writer_with(cfg, samples, streams.clone());

    let wait = writer
        .admit_flush(batch_for(34, "svc", 1_700_000_000_000_000_000, true))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect_err("the poisoned streams generation resolves Err");

    let before = writer.metrics();
    assert_eq!(before.spool_poison_total, 1);
    assert_eq!(before.spool_uncertain_total, 0);

    wait_until("the uncertain re-insert outcome is abandoned", || {
        writer.metrics().backfill_abandoned_total == 1
    })
    .await;
    assert_eq!(writer.metrics().backfill_pending, 0);
    assert_eq!(streams.call_count(), 2);

    // Further paused-time advance: no retry of the uncertain outcome.
    tokio::time::sleep(Duration::from_secs(16)).await;
    let metrics = writer.metrics();
    assert_eq!(
        streams.call_count(),
        2,
        "an uncertain backfill outcome must never be retried (#9)"
    );
    assert_eq!(metrics.backfill_abandoned_total, 1);
    assert_eq!(
        metrics.spool_poison_total, 1,
        "spool counters unchanged across the abandonment"
    );
    assert_eq!(metrics.spool_uncertain_total, 0);
}

/// Issue #134 AC5 (no poison spin): a deterministic backfill re-insert
/// failure abandons the pending batch — the tick never spins on a
/// poisoned backlog.
#[tokio::test(start_paused = true)]
async fn deterministic_backfill_failure_abandons_without_spinning() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    let samples = MockInserter::new(MockBehavior::Ok);
    let streams = MockInserter::new(MockBehavior::Poison);
    let writer = writer_with(cfg, samples, streams.clone());

    let wait = writer
        .admit_flush(batch_for(35, "svc", 1_700_000_000_000_000_000, true))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect_err("the poisoned streams generation resolves Err");

    let before = writer.metrics();
    assert_eq!(before.spool_poison_total, 1);
    assert_eq!(before.spool_uncertain_total, 0);

    wait_until("the deterministic re-insert failure is abandoned", || {
        writer.metrics().backfill_abandoned_total == 1
    })
    .await;
    assert_eq!(writer.metrics().backfill_pending, 0);

    tokio::time::sleep(Duration::from_secs(16)).await;
    let metrics = writer.metrics();
    assert_eq!(
        streams.call_count(),
        2,
        "one generation insert plus exactly one abandoned re-insert — no spin"
    );
    assert_eq!(metrics.backfill_abandoned_total, 1);
    assert_eq!(
        metrics.spool_poison_total, 1,
        "the backfill abandonment never double-spools"
    );
    assert_eq!(metrics.spool_uncertain_total, 0);
}

/// One scripted call outcome for [`ScriptedInserter`] — consumed in call
/// order (issue #134 code-review race regression: the mid-flight steps
/// need a gate the test releases explicitly).
#[derive(Clone, Copy, Debug)]
enum ScriptStep {
    Poison,
    Succeed,
    /// Blocks on the gate semaphore until the test calls
    /// [`ScriptedInserter::release_gate`], then resolves as named.
    BlockThenSucceed,
    BlockThenPoison,
}

/// A per-call scripted [`BlockInserter`]: unlike [`MockInserter`]'s
/// single behavior + budget, each call pops the next [`ScriptStep`], and
/// the `BlockThen*` steps park in flight until the test releases the
/// gate — the seam the in-flight-race regressions below need.
struct ScriptedInserter {
    steps: Mutex<std::collections::VecDeque<ScriptStep>>,
    gate: tokio::sync::Semaphore,
    calls: AtomicUsize,
}

impl ScriptedInserter {
    fn new(steps: impl IntoIterator<Item = ScriptStep>) -> Arc<Self> {
        Arc::new(ScriptedInserter {
            steps: Mutex::new(steps.into_iter().collect()),
            gate: tokio::sync::Semaphore::new(0),
            calls: AtomicUsize::new(0),
        })
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn release_gate(&self) {
        self.gate.add_permits(1);
    }
}

impl<R: ChRow> BlockInserter<R> for ScriptedInserter {
    fn insert<'a>(
        &'a self,
        _table: &'a str,
        _rows: &'a [R],
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let step = self
            .steps
            .lock()
            .expect("script mutex poisoned")
            .pop_front()
            .expect("scripted inserter called more times than scripted");
        Box::pin(async move {
            match step {
                ScriptStep::Poison => Err(ChError::Decode("scripted poison".to_string())),
                ScriptStep::Succeed => Ok(()),
                ScriptStep::BlockThenSucceed => {
                    let permit = self.gate.acquire().await.expect("gate never closed");
                    permit.forget();
                    Ok(())
                }
                ScriptStep::BlockThenPoison => {
                    let permit = self.gate.acquire().await.expect("gate never closed");
                    permit.forget();
                    Err(ChError::Decode("scripted poison".to_string()))
                }
            }
        })
    }
}

/// Issue #134 code-review race regression (success resolve): a NEWER
/// Poisoned flush for the same `(fingerprint, month)` enqueued WHILE a
/// stale backfill re-insert is in flight must survive the stale
/// attempt's success — not be falsely marked healed/LRU-promoted (the
/// stale attempt inserted the OLD row, not the newer one). The
/// version-checked removal leaves it for the next tick, which retries
/// and heals it. Fails under an unconditional key-only remove: the
/// fourth insert never happens.
#[tokio::test(start_paused = true)]
async fn newer_entry_enqueued_during_inflight_backfill_survives_a_stale_success() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    // Calls in order: generation 1 (Poison, enqueues T1), backfill
    // re-insert of T1 (blocked in flight), generation 2 (Poison,
    // enqueues newer T2 replacing T1), next-tick re-insert of T2 (Ok).
    let samples = MockInserter::new(MockBehavior::Ok);
    let streams = ScriptedInserter::new([
        ScriptStep::Poison,
        ScriptStep::BlockThenSucceed,
        ScriptStep::Poison,
        ScriptStep::Succeed,
    ]);
    let writer = LogWriter::with_inserters(samples, streams.clone(), &cfg);

    let t1 = 1_700_000_000_000_000_000i64;
    let wait = writer
        .admit_flush(batch_for(41, "svc", t1, true))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect_err("the poisoned streams generation resolves Err");

    // Reach the state where the T1 re-insert is in flight and parked on
    // the gate.
    wait_until("the stale backfill re-insert is in flight", || {
        streams.call_count() == 2
    })
    .await;

    // While it is in flight: a newer Poisoned flush for the SAME key
    // (same fingerprint/month, larger updated_ns) replaces the entry.
    let wait = writer
        .admit_flush(batch_for(41, "svc", t1 + 1, true))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect_err("the second poisoned streams generation resolves Err");
    let metrics = writer.metrics();
    assert_eq!(
        metrics.backfill_enqueued_total, 2,
        "the replacement was accepted"
    );
    assert_eq!(metrics.backfill_pending, 1);
    assert_eq!(metrics.backfill_healed_total, 0);

    // Resolve the stale attempt as a SUCCESS: only the T1 row it carried
    // was inserted — the newer T2 entry must survive and be retried.
    streams.release_gate();
    wait_until(
        "the surviving newer entry is retried on the next tick",
        || streams.call_count() == 4,
    )
    .await;
    wait_until("the newer entry's own retry heals it", || {
        writer.metrics().backfill_healed_total == 1
    })
    .await;

    let metrics = writer.metrics();
    assert_eq!(
        metrics.backfill_healed_total, 1,
        "exactly one heal — the newer row's own confirmed re-insert, never the stale attempt"
    );
    assert_eq!(metrics.backfill_pending, 0);
    assert_eq!(metrics.backfill_abandoned_total, 0);
}

/// Issue #134 code-review race regression (failure resolve): the same
/// mid-flight replacement, but the stale attempt resolves with a
/// deterministic FAILURE — the newer entry must not be silently
/// abandoned with it; it survives and heals on the next tick. Fails
/// under an unconditional key-only remove (abandoned, fourth insert
/// never happens).
#[tokio::test(start_paused = true)]
async fn newer_entry_enqueued_during_inflight_backfill_survives_a_stale_failure() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    let samples = MockInserter::new(MockBehavior::Ok);
    let streams = ScriptedInserter::new([
        ScriptStep::Poison,
        ScriptStep::BlockThenPoison,
        ScriptStep::Poison,
        ScriptStep::Succeed,
    ]);
    let writer = LogWriter::with_inserters(samples, streams.clone(), &cfg);

    let t1 = 1_700_000_000_000_000_000i64;
    let wait = writer
        .admit_flush(batch_for(42, "svc", t1, true))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect_err("the poisoned streams generation resolves Err");

    wait_until("the stale backfill re-insert is in flight", || {
        streams.call_count() == 2
    })
    .await;

    let wait = writer
        .admit_flush(batch_for(42, "svc", t1 + 1, true))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect_err("the second poisoned streams generation resolves Err");
    assert_eq!(writer.metrics().backfill_pending, 1);

    // Resolve the stale attempt as a deterministic FAILURE: the newer
    // entry must survive (not be abandoned along with the stale one).
    streams.release_gate();
    wait_until(
        "the surviving newer entry is retried on the next tick",
        || streams.call_count() == 4,
    )
    .await;
    wait_until("the newer entry's own retry heals it", || {
        writer.metrics().backfill_healed_total == 1
    })
    .await;

    let metrics = writer.metrics();
    assert_eq!(metrics.backfill_healed_total, 1);
    assert_eq!(metrics.backfill_pending, 0);
    assert_eq!(
        metrics.backfill_abandoned_total, 0,
        "the newer entry was never abandoned — the stale attempt's failure removed nothing"
    );
}

/// Issue #134 AC8 (bounded shutdown, plan v4 §A): with a non-empty
/// backlog and a backfill re-insert hanging in flight, `shutdown` must
/// still complete — the in-flight attempt is deadline-bounded and dropped
/// on elapse. Fails against an unbounded tick (the shutdown join would
/// hang on the backfill task).
#[tokio::test(start_paused = true)]
async fn shutdown_completes_with_a_hanging_backfill_insert_in_flight() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    let samples = MockInserter::new(MockBehavior::Ok);
    let streams = MockInserter::new_with_fail_budget(MockBehavior::PoisonThenHang, 1);
    let writer = writer_with(cfg, samples, streams.clone());

    let wait = writer
        .admit_flush(batch_for(36, "svc", 1_700_000_000_000_000_000, true))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect_err("the poisoned streams generation resolves Err");

    // Reach the state where the backfill's re-insert is in flight and
    // hanging (the second streams call never resolves).
    wait_until("the hanging backfill re-insert is in flight", || {
        streams.call_count() == 2
    })
    .await;

    tokio::time::timeout(
        Duration::from_secs(10),
        writer.shutdown(Duration::from_secs(2)),
    )
    .await
    .expect("shutdown must complete despite the hanging in-flight backfill insert");
}
