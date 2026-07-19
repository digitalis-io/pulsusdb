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
        Arc::new(MockInserter {
            behavior: Mutex::new(MockBehavior::RetryThenOk),
            calls: AtomicUsize::new(0),
            last_row_count: Mutex::new(0),
            row_counts: Mutex::new(Vec::new()),
            fail_remaining: AtomicUsize::new(n),
        })
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
                    let should_fail = self
                        .fail_remaining
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                        .is_ok();
                    if should_fail {
                        Err(ChError::Timeout("mock retryable timeout".to_string()))
                    } else {
                        Ok(())
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
