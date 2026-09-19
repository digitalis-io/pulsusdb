//! Issue #494 at the writer seam: a push a writer has already accepted, and
//! still remembers, is not stored twice by that writer.
//!
//! Against mock inserters, no ClickHouse. The live legs — the queries and
//! the answers they must give — are in
//! `crates/pulsus-server/tests/push_dedup_live.rs`.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pulsus_clickhouse::{ChError, ChRow};
use pulsus_config::WriterConfig;
use pulsus_model::{Date, Fingerprint, LabelSet, UnixNano};
use pulsus_write::MetricSink;
use pulsus_write::ingest::metrics::{MetricMetadata, MetricPoint, ParsedMetrics, SeriesRef};
use pulsus_write::writer::{BlockInserter, LogWriter, MetricWriter};
use pulsus_write::{AdmitRefusal, LogRow, LogSink, ParsedLogs, PushHeaders, StreamRow, WaitMode};

// ---------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Behavior {
    Ok,
    /// Never resolves — the generation stays in flight until the drain
    /// deadline force-settles it.
    Hang,
    /// A non-retryable failure: provably not committed.
    Poison,
}

struct MockInserter {
    behavior: Behavior,
    calls: AtomicUsize,
    rows: Mutex<Vec<usize>>,
}

impl MockInserter {
    fn new(behavior: Behavior) -> Arc<Self> {
        Arc::new(MockInserter {
            behavior,
            calls: AtomicUsize::new(0),
            rows: Mutex::new(Vec::new()),
        })
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    /// Every row this table has been handed, summed over every insert.
    fn rows_inserted(&self) -> usize {
        self.rows.lock().expect("mock mutex").iter().sum()
    }
}

impl<R: ChRow> BlockInserter<R> for MockInserter {
    fn insert<'a>(
        &'a self,
        _table: &'a str,
        rows: &'a [R],
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.rows.lock().expect("mock mutex").push(rows.len());
        let behavior = self.behavior;
        Box::pin(async move {
            match behavior {
                Behavior::Ok => Ok(()),
                Behavior::Poison => Err(ChError::Decode("mock poison".to_string())),
                Behavior::Hang => std::future::pending::<Result<(), ChError>>().await,
            }
        })
    }
}

fn labels_with_service(service: &str) -> LabelSet {
    let (labels, _) =
        LabelSet::from_normalized([("service_name".to_string(), service.to_string())]);
    labels
}

/// One `log_samples` row, plus its `StreamRow` when `new_stream`.
fn batch(fingerprint: u128, body: &str, timestamp_ns: i64, new_stream: bool) -> ParsedLogs {
    let mut out = ParsedLogs {
        rows: vec![LogRow {
            service: "svc".to_string(),
            fingerprint: Fingerprint::from_raw(fingerprint),
            timestamp_ns: UnixNano(timestamp_ns),
            severity: 0,
            body: body.to_string(),
            structured_metadata: String::new(),
        }],
        ..Default::default()
    };
    if new_stream {
        out.streams.push(StreamRow {
            month: Date::start_of_month_utc(timestamp_ns).expect("in-range month"),
            fingerprint: Fingerprint::from_raw(fingerprint),
            service: "svc".to_string(),
            labels: labels_with_service("svc"),
            // Receiver-injected receive time: excluded from the identity,
            // so a retry carrying a different one is still the same push.
            updated_ns: timestamp_ns,
        });
    }
    out
}

const T: i64 = 1_700_000_000_000_000_000;

fn writer_with(
    cfg: WriterConfig,
    samples: Arc<MockInserter>,
    streams: Arc<MockInserter>,
) -> LogWriter {
    let cfg = WriterConfig {
        log_patterns: false,
        ..cfg
    };
    LogWriter::with_inserters(samples, streams, MockInserter::new(Behavior::Ok), &cfg)
}

fn metric_writer_with(cfg: WriterConfig, metadata: Arc<MockInserter>) -> MetricWriter {
    metric_writer_with_samples(cfg, MockInserter::new(Behavior::Ok), metadata)
}

fn metric_writer_with_samples(
    cfg: WriterConfig,
    samples: Arc<MockInserter>,
    metadata: Arc<MockInserter>,
) -> MetricWriter {
    MetricWriter::with_inserters(
        samples,
        MockInserter::new(Behavior::Ok),
        metadata,
        MockInserter::new(Behavior::Ok),
        &cfg,
        pulsus_model::DEFAULT_ACTIVITY_BUCKET_MS,
    )
}

fn metadata_batch(name: &str, metric_type: &str, updated_ns: i64) -> ParsedMetrics {
    ParsedMetrics {
        metadata: vec![MetricMetadata {
            metric_name: Arc::from(name),
            metric_type: metric_type.to_string(),
            help: String::new(),
            unit: String::new(),
            // Receiver-injected in production (`now_ns`); named here so the
            // A/B/A sequence is legible.
            updated_ns,
        }],
        ..Default::default()
    }
}

/// Flush on the very next append, so a sync admit settles promptly.
fn eager() -> WriterConfig {
    WriterConfig {
        batch_bytes: pulsus_config::ByteSize(1),
        ..Default::default()
    }
}

/// Nothing auto-flushes: the generation stays open for the whole test.
fn never_flushes() -> WriterConfig {
    WriterConfig {
        batch_bytes: pulsus_config::ByteSize(u64::MAX),
        batch_ms: pulsus_config::BATCH_MS_CEILING,
        ..Default::default()
    }
}

// ---------------------------------------------------------------------
// Criterion 3 — the declared limit, same writer against a second writer
// ---------------------------------------------------------------------

/// The same body twice through ONE writer stores one push's rows; through
/// two writer processes it stores both, which is the declared limit; and
/// with `PULSUS_INGEST_DEDUP=false` one writer stores both again.
#[tokio::test]
async fn the_declared_limit_has_three_legs() {
    // (1) one writer, twice.
    let samples = MockInserter::new(Behavior::Ok);
    let writer = writer_with(eager(), samples.clone(), MockInserter::new(Behavior::Ok));
    for _ in 0..2 {
        let wait = writer
            .admit_flush(batch(1, "checkout failed", T, true), PushHeaders::default())
            .expect("queue has room");
        wait.await.expect("the flush settles");
    }
    writer.shutdown(Duration::from_secs(2)).await;
    assert_eq!(
        samples.rows_inserted(),
        1,
        "the same body twice through one writer is one push"
    );

    // (2) two writers, once each.
    let a_samples = MockInserter::new(Behavior::Ok);
    let b_samples = MockInserter::new(Behavior::Ok);
    let a = writer_with(eager(), a_samples.clone(), MockInserter::new(Behavior::Ok));
    let b = writer_with(eager(), b_samples.clone(), MockInserter::new(Behavior::Ok));
    for writer in [&a, &b] {
        let wait = writer
            .admit_flush(batch(1, "checkout failed", T, true), PushHeaders::default())
            .expect("queue has room");
        wait.await.expect("the flush settles");
    }
    a.shutdown(Duration::from_secs(2)).await;
    b.shutdown(Duration::from_secs(2)).await;
    assert_eq!(
        a_samples.rows_inserted() + b_samples.rows_inserted(),
        2,
        "the cross-writer case is the declared limit: both are stored"
    );

    // (3) one writer, twice, with the mechanism off.
    let off_samples = MockInserter::new(Behavior::Ok);
    let off = writer_with(
        WriterConfig {
            ingest_dedup: false,
            ..eager()
        },
        off_samples.clone(),
        MockInserter::new(Behavior::Ok),
    );
    assert!(off.dedup().is_none(), "the index is not built at all");
    for _ in 0..2 {
        let wait = off
            .admit_flush(batch(1, "checkout failed", T, true), PushHeaders::default())
            .expect("queue has room");
        wait.await.expect("the flush settles");
    }
    off.shutdown(Duration::from_secs(2)).await;
    assert_eq!(
        off_samples.rows_inserted(),
        2,
        "with the mechanism off the retry is stored, which is the defect"
    );
}

// ---------------------------------------------------------------------
// Criterion 4 — concurrent identical requests on one writer
// ---------------------------------------------------------------------

/// Eight identical pushes fired at once: exactly one admits and seven are
/// counted suppressed. The lookup and the claim are one critical section,
/// so no two can both find the key absent.
///
/// A multi-threaded runtime and a barrier, so the eight really do overlap:
/// `admit` is synchronous, so on a current-thread runtime they would run
/// one after another and the race would not be exercised at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn eight_concurrent_identical_pushes_admit_exactly_one() {
    let samples = MockInserter::new(Behavior::Ok);
    let writer = Arc::new(writer_with(
        never_flushes(),
        samples.clone(),
        MockInserter::new(Behavior::Ok),
    ));
    let gate = Arc::new(tokio::sync::Barrier::new(8));

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let writer = writer.clone();
        let gate = gate.clone();
        tasks.spawn(async move {
            gate.wait().await;
            writer.admit(batch(2, "concurrent", T, true), PushHeaders::default())
        });
    }
    let results: Vec<Result<(), AdmitRefusal>> = tasks.join_all().await;
    assert!(results.iter().all(Result::is_ok), "no push is refused");

    let snap = writer.metrics().dedup;
    assert_eq!(
        snap.duplicate_pushes_total, 7,
        "seven of the eight are suppressed"
    );
    assert_eq!(
        snap.duplicate_rows_suppressed_total, 7,
        "one row each, seven times"
    );

    writer.shutdown(Duration::from_secs(2)).await;
    assert_eq!(
        samples.rows_inserted(),
        1,
        "exactly one push's rows reached the table"
    );
}

// ---------------------------------------------------------------------
// Criterion 5 — per-target completion, and the two outcomes are separate
// ---------------------------------------------------------------------

/// Every recorded target settles provably not-committed, so the rows are
/// not there and the next identical push is admitted.
#[tokio::test]
async fn a_push_whose_every_target_failed_definitely_is_re_admitted() {
    let samples = MockInserter::new(Behavior::Poison);
    let writer = writer_with(
        eager(),
        samples.clone(),
        MockInserter::new(Behavior::Poison),
    );

    let wait = writer
        .admit_flush(batch(3, "poisoned", T, false), PushHeaders::default())
        .expect("queue has room");
    wait.await.expect_err("a poisoned flush resolves Err");

    let wait = writer
        .admit_flush(batch(3, "poisoned", T, false), PushHeaders::default())
        .expect("queue has room");
    wait.await.expect_err("and so does the re-admitted one");

    writer.shutdown(Duration::from_secs(2)).await;
    assert_eq!(
        samples.call_count(),
        2,
        "provably-not-committed rows are re-admitted, not suppressed"
    );
    assert_eq!(writer.metrics().dedup.duplicate_pushes_total, 0);
}

/// The pattern table is a claim target but never joins the durability
/// acknowledgement, so a pattern-only failure reproduces the original
/// caller's success for a suppressed caller — and the disagreement between
/// the targets is counted.
#[tokio::test]
async fn a_pattern_only_failure_is_counted_but_is_not_the_suppressed_callers_answer() {
    let samples = MockInserter::new(Behavior::Ok);
    let streams = MockInserter::new(Behavior::Ok);
    let patterns = MockInserter::new(Behavior::Poison);
    let cfg = WriterConfig {
        log_patterns: true,
        ..eager()
    };
    let writer = LogWriter::with_inserters(samples, streams, patterns.clone(), &cfg);

    let wait = writer
        .admit_flush(batch(4, "mixed outcome", T, true), PushHeaders::default())
        .expect("queue has room");
    wait.await
        .expect("a pattern failure never fails the original caller");

    // The pattern generation settles asynchronously; wait for it.
    for _ in 0..200 {
        if writer.metrics().dedup.mixed_outcome_total == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        writer.metrics().dedup.mixed_outcome_total,
        1,
        "targets that disagreed are counted"
    );
    assert!(patterns.call_count() >= 1);

    let wait = writer
        .admit_flush(batch(4, "mixed outcome", T, true), PushHeaders::default())
        .expect("queue has room");
    wait.await
        .expect("the suppressed caller is told the ORIGINAL's success, not the pattern failure");
    assert_eq!(writer.metrics().dedup.duplicate_pushes_total, 1);
    writer.shutdown(Duration::from_secs(2)).await;
}

// ---------------------------------------------------------------------
// Criterion 6 — rollback on a rejected reservation
// ---------------------------------------------------------------------

/// A push refused by the queue-bytes gate leaves no claim behind: the same
/// body, once there is room, is stored.
#[tokio::test]
async fn a_push_refused_by_backpressure_leaves_no_claim() {
    // The two bodies are the same length, so one reservation is the same
    // size as the other. The queue limit is measured rather than guessed:
    // a probe writer with room to spare reports what one of them reserves.
    let first = batch(5, "the first body", T, true);
    let second = batch(6, "the second body", T, true);
    let reserved = {
        let probe = writer_with(
            never_flushes(),
            MockInserter::new(Behavior::Ok),
            MockInserter::new(Behavior::Ok),
        );
        probe
            .admit(first.clone(), PushHeaders::default())
            .expect("the probe has room");
        let bytes = probe.metrics().queue_bytes;
        probe.shutdown(Duration::from_secs(2)).await;
        bytes
    };
    assert!(reserved > 0, "a push reserves bytes");

    // A queue that fits one batch and not two, and an age flush that
    // releases the first batch's bytes shortly afterwards.
    let cfg = WriterConfig {
        batch_ms: 20,
        batch_bytes: pulsus_config::ByteSize(u64::MAX),
        ingest_queue_bytes: pulsus_config::ByteSize(reserved + reserved / 2),
        ..Default::default()
    };
    let samples = MockInserter::new(Behavior::Ok);
    let writer = writer_with(cfg, samples.clone(), MockInserter::new(Behavior::Ok));

    writer
        .admit(first, PushHeaders::default())
        .expect("the first push fits");
    let refusal = writer
        .admit(second.clone(), PushHeaders::default())
        .expect_err("the second push does not fit");
    assert_eq!(refusal, AdmitRefusal::Backpressure);
    assert_eq!(
        writer.metrics().dedup.rollbacks_total,
        1,
        "the claim taken before the reservation was rolled back"
    );

    // Wait for the age flush to release the first push's bytes.
    for _ in 0..200 {
        if writer.metrics().queue_bytes == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(writer.metrics().queue_bytes, 0, "the queue drained");

    writer
        .admit(second, PushHeaders::default())
        .expect("with room, the same body is stored");
    assert_eq!(
        writer.metrics().dedup.duplicate_pushes_total,
        0,
        "a rolled-back push is not a duplicate of itself"
    );
    writer.shutdown(Duration::from_secs(2)).await;
    assert_eq!(
        samples.rows_inserted(),
        2,
        "both bodies are stored, the refused one on its second attempt"
    );
}

/// Criterion 17, second leg: after that rollback, a request carrying only
/// `Retry-Attempt: 1` and no earlier claim is **stored**. The header is
/// never a suppression key — there is nothing stored to match it against.
#[tokio::test]
async fn a_lone_retry_attempt_header_is_stored() {
    let declared = PushHeaders {
        idempotency_key: None,
        declared_retry: true,
    };
    let samples = MockInserter::new(Behavior::Ok);
    let writer = writer_with(eager(), samples.clone(), MockInserter::new(Behavior::Ok));

    let wait = writer
        .admit_flush(batch(7, "a retry of nothing", T, true), declared)
        .expect("queue has room");
    wait.await.expect("the flush settles");

    writer.shutdown(Duration::from_secs(2)).await;
    assert_eq!(
        samples.rows_inserted(),
        1,
        "the lone retry marker is stored"
    );
    let snap = writer.metrics().dedup;
    assert_eq!(
        (
            snap.duplicate_pushes_total,
            snap.duplicate_pushes_declared_total
        ),
        (0, 0),
        "nothing was suppressed, so neither counter moves"
    );
}

/// Two DIFFERENT bodies, both carrying `Retry-Attempt: 1`, are two pushes.
/// The header matches nothing, so it cannot make one body stand for
/// another — which is what treating it as a key would do.
#[tokio::test]
async fn two_different_bodies_declaring_a_retry_are_two_pushes() {
    let declared = || PushHeaders {
        idempotency_key: None,
        declared_retry: true,
    };
    let samples = MockInserter::new(Behavior::Ok);
    let writer = writer_with(eager(), samples.clone(), MockInserter::new(Behavior::Ok));

    for body in ["first line", "second line"] {
        let wait = writer
            .admit_flush(batch(70, body, T, true), declared())
            .expect("queue has room");
        wait.await.expect("the flush settles");
    }
    writer.shutdown(Duration::from_secs(2)).await;
    assert_eq!(
        samples.rows_inserted(),
        2,
        "the retry marker is never a suppression key"
    );
    assert_eq!(writer.metrics().dedup.duplicate_pushes_declared_total, 0);
}

/// And when a suppression really happens, the `declared` dimension is the
/// one that moves.
#[tokio::test]
async fn a_declared_retry_moves_the_declared_dimension_only() {
    let samples = MockInserter::new(Behavior::Ok);
    let writer = writer_with(eager(), samples.clone(), MockInserter::new(Behavior::Ok));

    let wait = writer
        .admit_flush(batch(8, "declared", T, true), PushHeaders::default())
        .expect("queue has room");
    wait.await.expect("the flush settles");

    let wait = writer
        .admit_flush(
            batch(8, "declared", T, true),
            PushHeaders {
                idempotency_key: None,
                declared_retry: true,
            },
        )
        .expect("queue has room");
    wait.await
        .expect("the suppressed caller gets the original's success");

    let snap = writer.metrics().dedup;
    assert_eq!(
        (
            snap.duplicate_pushes_total,
            snap.duplicate_pushes_declared_total
        ),
        (0, 1)
    );
    writer.shutdown(Duration::from_secs(2)).await;
    assert_eq!(samples.rows_inserted(), 1);
}

// ---------------------------------------------------------------------
// Criterion 11 — the key
// ---------------------------------------------------------------------

/// Two requests with different `Idempotency-Key` values are two pushes
/// whatever they carry; the same key with the same content is one; the same
/// key with different content is a client error and stores nothing.
#[tokio::test]
async fn the_idempotency_key_namespaces_the_identity() {
    let key = |k: &str| PushHeaders {
        idempotency_key: Some(k.to_string()),
        declared_retry: false,
    };
    let samples = MockInserter::new(Behavior::Ok);
    let writer = writer_with(eager(), samples.clone(), MockInserter::new(Behavior::Ok));

    for k in ["a", "b"] {
        let wait = writer
            .admit_flush(batch(9, "same body", T, true), key(k))
            .expect("queue has room");
        wait.await.expect("the flush settles");
    }
    // The same key, the same content: one push.
    let wait = writer
        .admit_flush(batch(9, "same body", T, true), key("a"))
        .expect("queue has room");
    wait.await
        .expect("the suppressed caller gets the original's answer");

    // The same key, different content: a client error, and nothing stored.
    let refusal = writer
        .admit_flush(batch(9, "a different body", T, true), key("a"))
        .expect_err("a reused key carrying different content is refused");
    assert_eq!(refusal, AdmitRefusal::KeyReused);
    assert_eq!(writer.metrics().dedup.key_reused_total, 1);

    writer.shutdown(Duration::from_secs(2)).await;
    assert_eq!(
        samples.rows_inserted(),
        2,
        "two distinct keys stored two pushes; the repeat and the refusal stored none"
    );
}

// ---------------------------------------------------------------------
// Criterion 7 — membership is attached under the lock, and sealed
// ---------------------------------------------------------------------

/// A generation swapped out and settled while the admitting call is still
/// between its append and its `seal` must still settle its claim: the claim
/// key travels with the generation, attached under the buffer's own lock,
/// and completion is evaluated at BOTH events.
///
/// Driven through the index directly, because `admit_batch` appends and
/// seals inside one synchronous call and nothing can be interleaved there.
#[test]
fn a_target_that_settles_before_seal_is_evaluated_at_seal() {
    use pulsus_write::{Admission, PushDedup, PushDigest, PushIdentity, TargetOutcome};

    let index = PushDedup::new(
        16 * 1024 * 1024,
        Duration::from_secs(300),
        Duration::from_secs(120),
    );
    let id = PushIdentity {
        key: PushDigest::from_raw(1),
        content: PushDigest::from_raw(1),
        declared_retry: false,
    };
    let Admission::Admit(mut guard) = index.admit(id, WaitMode::None) else {
        panic!("the first push must admit");
    };
    guard.note_target(true);
    // The generation is swapped out and settles HERE, before `seal`.
    index.settle_target(id.key, TargetOutcome::Committed, true);
    assert!(
        matches!(
            index.admit(id, WaitMode::None),
            Admission::SuppressedSettled(_)
        ),
        "the claim is still open, so the retry is suppressed"
    );
    guard.seal();

    assert!(
        matches!(
            index.admit(id, WaitMode::None),
            Admission::SuppressedSettled(pulsus_write::ClaimOutcome::Ok)
        ),
        "completion is evaluated at seal, and the answer is the original's"
    );
}

// ---------------------------------------------------------------------
// Criterion 8 — forced shutdown reports, and `tick` has a caller
// ---------------------------------------------------------------------

/// A generation abandoned at the drain deadline reports an unknown fate,
/// through the forced-settle path that bypasses the normal reporting path.
/// The claim is therefore terminal rather than open for ever, and rows that
/// may have committed are never re-admitted.
#[tokio::test]
async fn a_forced_shutdown_reports_its_generations_claims() {
    let body = batch(10, "in flight at shutdown", T, false);
    let id = pulsus_write::log_identity(&body, &PushHeaders::default());

    let samples = MockInserter::new(Behavior::Hang);
    let writer = writer_with(eager(), samples, MockInserter::new(Behavior::Ok));
    let index = writer.dedup().expect("the index is on").clone();

    writer
        .admit(body, PushHeaders::default())
        .expect("queue has room");
    assert!(
        matches!(
            index.admit(id, WaitMode::Register),
            Admission::SuppressedPending { .. }
        ),
        "while the insert is in flight the claim is open"
    );

    // The insert hangs, so the drain deadline is what settles it.
    writer.shutdown(Duration::from_millis(50)).await;

    match index.admit(id, WaitMode::Register) {
        Admission::SuppressedSettled(outcome) => assert_eq!(
            outcome,
            pulsus_write::ClaimOutcome::Failed,
            "an abandoned generation's fate is not a success"
        ),
        other => panic!(
            "the forced settle must have reported: a caller now gets {other:?} \
             and would wait for a wakeup that never comes"
        ),
    }
    assert_eq!(
        index.snapshot().unknown_total,
        0,
        "the claim settled at the deadline rather than ageing out"
    );
}

use pulsus_write::Admission;

/// `tick` has a production caller: the per-table flush task drives it once
/// per cycle, and that is what ages a claim whose targets never settle into
/// a tombstone. Remove the call and the claim stays open for ever.
#[tokio::test(start_paused = true)]
async fn the_flush_task_drives_the_index_tick() {
    // `log_samples` hangs, so its own flush task parks inside the insert;
    // `log_streams` has nothing to flush and keeps looping, which is the
    // caller that ages this claim.
    let cfg = WriterConfig {
        batch_bytes: pulsus_config::ByteSize(1),
        ..Default::default()
    };
    let writer = writer_with(
        cfg,
        MockInserter::new(Behavior::Hang),
        MockInserter::new(Behavior::Ok),
    );
    writer
        .admit(batch(11, "never settles", T, false), PushHeaders::default())
        .expect("queue has room");

    // Past the claim deadline: `PULSUS_BATCH_MS` plus the insert bound.
    tokio::time::sleep(Duration::from_secs(130)).await;
    tokio::task::yield_now().await;

    assert_eq!(
        writer.metrics().dedup.unknown_total,
        1,
        "a claim past its deadline becomes a tombstone"
    );
    let index = writer.dedup().expect("the index is on").clone();
    drop(writer);
    // The tombstone is retained: a later identical push is still
    // suppressed, because those rows may have committed.
    assert_eq!(index.snapshot().unknown_total, 1);
}

/// **The window is a real one, through the writer.** A body pushed twice
/// inside `PULSUS_INGEST_DEDUP_WINDOW` stores one copy; the same body
/// pushed again after the window has elapsed stores a second, because it
/// is a new push and not a retry of anything the writer still remembers.
///
/// ```text
///   t = 0s    push, push        1 copy stored — the second is suppressed
///   t = 61s   push              2 copies stored — the window elapsed
/// ```
///
/// The clock is the runtime's, advanced rather than waited out: the
/// shipped default window is five minutes and the smallest accepted one is
/// a second, and neither is something a test should sit through. The
/// window here is sixty seconds, far enough above the flush task's 200 ms
/// cadence that the auto-advance between the two early pushes cannot
/// cross it.
#[tokio::test(start_paused = true)]
async fn a_push_after_the_window_elapses_is_stored_again() {
    let window = Duration::from_secs(60);
    let cfg = WriterConfig {
        ingest_dedup_window: pulsus_config::HumanDuration(window),
        ..eager()
    };
    let samples = MockInserter::new(Behavior::Ok);
    let writer = writer_with(cfg, samples.clone(), MockInserter::new(Behavior::Ok));

    for _ in 0..2 {
        let wait = writer
            .admit_flush(
                batch(80, "inside the window", T, true),
                PushHeaders::default(),
            )
            .expect("queue has room");
        wait.await.expect("the flush settles");
    }
    assert_eq!(
        samples.rows_inserted(),
        1,
        "inside the window the retry stores nothing"
    );
    assert_eq!(writer.metrics().dedup.duplicate_pushes_total, 1);

    tokio::time::sleep(window + Duration::from_secs(1)).await;
    // The flush task drives `tick`, which is what drops the expired claim.
    tokio::task::yield_now().await;

    let wait = writer
        .admit_flush(
            batch(80, "inside the window", T, true),
            PushHeaders::default(),
        )
        .expect("queue has room");
    wait.await.expect("the flush settles");
    assert_eq!(
        samples.rows_inserted(),
        2,
        "past the window the same body is a new push and is stored"
    );
    assert_eq!(
        writer.metrics().dedup.duplicate_pushes_total,
        1,
        "and nothing was suppressed the second time"
    );
    writer.shutdown(Duration::from_secs(2)).await;
}

// ---------------------------------------------------------------------
// A descriptor that comes back is not a retry
// ---------------------------------------------------------------------

/// `metric_metadata` rows carry a receiver-injected `updated_ns` and the
/// table is a `ReplacingMergeTree` versioned by it, so the field is
/// outside the push identity — it changes on every retry, and a retry that
/// matched nothing would defeat the whole mechanism.
///
/// That leaves one sequence the content digest cannot read correctly:
///
/// ```text
///   counter at t1    emitted, wins
///   gauge   at t2    emitted, wins
///   counter at t3    byte-identical content to t1
/// ```
///
/// Suppressing the third leaves `gauge` as the stored type of a metric
/// that is a counter — a wrong answer, not a lost duplicate. So a push
/// that emits a descriptor is never suppressed, and this is that rule.
#[tokio::test]
async fn a_descriptor_that_comes_back_is_stored_again() {
    let metadata = MockInserter::new(Behavior::Ok);
    let writer = metric_writer_with(eager(), metadata.clone());

    for (metric_type, updated_ns) in [("counter", 1), ("gauge", 2), ("counter", 3)] {
        let wait = writer
            .admit_flush(
                metadata_batch("http_requests_total", metric_type, updated_ns),
                PushHeaders::default(),
            )
            .expect("queue has room");
        wait.await.expect("the flush settles");
    }
    writer.shutdown(Duration::from_secs(2)).await;

    assert_eq!(
        metadata.rows_inserted(),
        3,
        "all three descriptors reach the table: the third is the current \
         value of the metric, and the version column is what decides the \
         winner"
    );
    assert_eq!(
        writer.metrics().dedup.duplicate_pushes_total,
        1,
        "the third push IS suppressed — it repeats the first — and the \
         descriptor is written anyway, which is the whole point: \
         suppression drops the rows a repeat would duplicate, not the one \
         a repeat would correct"
    );
}

/// The case an earlier revision reopened, committed so it cannot be
/// reopened again: **two concurrent identical pushes carrying a descriptor
/// AND samples store one copy of the samples.**
///
/// The revision before this one excepted descriptor-bearing pushes from
/// suppression altogether, and two such requests raced each other into the
/// table: both returned success and the samples were stored twice. One
/// claim per content is what fixes it — the exception was never the
/// mechanism.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_identical_descriptor_bearing_pushes_store_one_copy() {
    let samples = MockInserter::new(Behavior::Ok);
    let metadata = MockInserter::new(Behavior::Ok);
    let writer = Arc::new(metric_writer_with_samples(
        never_flushes(),
        samples.clone(),
        metadata.clone(),
    ));
    let gate = Arc::new(tokio::sync::Barrier::new(4));

    let body = || {
        let mut batch = metadata_batch("http_requests_total", "counter", 1);
        batch.samples.push(MetricPoint {
            metric_name: "http_requests_total".into(),
            fingerprint: Fingerprint::from_raw(11),
            unix_milli: 1_700_000_000_000,
            value: 1.0,
        });
        batch.series.push(SeriesRef {
            metric_name: "http_requests_total".into(),
            fingerprint: Fingerprint::from_raw(11),
            labels: labels_with_service("svc"),
        });
        batch
    };

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..4 {
        let writer = writer.clone();
        let gate = gate.clone();
        tasks.spawn(async move {
            gate.wait().await;
            MetricSink::admit(writer.as_ref(), body(), PushHeaders::default())
        });
    }
    let results: Vec<Result<(), AdmitRefusal>> = tasks.join_all().await;
    assert!(results.iter().all(Result::is_ok), "no push is refused");
    assert_eq!(
        writer.metrics().dedup.duplicate_pushes_total,
        3,
        "three of the four are suppressed"
    );

    writer.shutdown(Duration::from_secs(2)).await;
    assert_eq!(
        samples.rows_inserted(),
        1,
        "exactly one push's samples reached the table"
    );
    assert_eq!(
        metadata.rows_inserted(),
        4,
        "every one of the four enqueues its descriptor: suppression drops the sample, \
         series and histogram rows and still offers the descriptor to the cache gate, and \
         the gate emits a row unless the descriptor equals the one last CONFIRMED-flushed. \
         Under this barrier none of the four has confirmed anything yet, so all four emit. \
         **This is a figure of THIS fixture, and the live path gives one.** \
         `a_concurrent_descriptor_race_leaves_one_visible_row` in \
         `crates/pulsus-server/tests/push_dedup_live.rs` sends four content-identical \
         descriptor-bearing writes over HTTP at once and measures ONE row in \
         `metric_metadata`, with and without `FINAL`, from the first read — so nothing is \
         collapsed there, because nothing beyond one row is written. Four HTTP requests are \
         not released together the way a barrier releases four admissions in one process. \
         Which of them confirms its flush first is not measured; the row counts are. Both \
         figures are asserted, each on its own path, because round 4 of this issue's code \
         review found the notes claiming both and the tests establishing neither."
    );
}

/// The rule costs nothing on a genuine retry: the descriptor cache emits a
/// row only when it differs from the one last confirmed-flushed, so the
/// retry carries none, stays claimable, and is suppressed.
#[tokio::test]
async fn a_retried_push_carrying_the_same_descriptor_is_still_suppressed() {
    let samples = MockInserter::new(Behavior::Ok);
    let metadata = MockInserter::new(Behavior::Ok);
    let writer = metric_writer_with_samples(eager(), samples.clone(), metadata.clone());

    let body = || {
        let mut batch = metadata_batch("http_requests_total", "counter", 1);
        batch.samples.push(MetricPoint {
            metric_name: "http_requests_total".into(),
            fingerprint: Fingerprint::from_raw(9),
            unix_milli: 1_700_000_000_000,
            value: 1.0,
        });
        batch.series.push(SeriesRef {
            metric_name: "http_requests_total".into(),
            fingerprint: Fingerprint::from_raw(9),
            labels: labels_with_service("svc"),
        });
        batch
    };

    for _ in 0..2 {
        let wait = writer
            .admit_flush(body(), PushHeaders::default())
            .expect("queue has room");
        wait.await.expect("the flush settles");
    }
    writer.shutdown(Duration::from_secs(2)).await;

    assert_eq!(
        metadata.rows_inserted(),
        1,
        "the descriptor is emitted once; the retry's is already cached"
    );
    assert_eq!(
        samples.rows_inserted(),
        1,
        "and the retry's sample is suppressed, which is the point"
    );
    assert_eq!(writer.metrics().dedup.duplicate_pushes_total, 1);
}

// ---------------------------------------------------------------------
// Criterion 18 — the keyed wait has no check-then-wait window
// ---------------------------------------------------------------------

/// A suppressed sync caller that starts waiting at the exact moment the
/// claim settles must observe the outcome, never hang. The terminal check
/// and the registration happen inside one acquisition of the lock the
/// settle also takes, so there is no gap for the settle to land in.
#[tokio::test]
async fn a_settle_racing_the_wait_never_strands_the_caller() {
    use pulsus_write::{PushDedup, PushDigest, PushIdentity, TargetOutcome};

    for round in 0..200u128 {
        let index = PushDedup::new(
            16 * 1024 * 1024,
            Duration::from_secs(300),
            Duration::from_secs(120),
        );
        let id = PushIdentity {
            key: PushDigest::from_raw(round),
            content: PushDigest::from_raw(round),
            declared_retry: false,
        };
        let Admission::Admit(mut guard) = index.admit(id, WaitMode::None) else {
            panic!("must admit");
        };
        guard.note_target(true);
        guard.seal();

        let settler = index.clone();
        let handle = tokio::task::spawn_blocking(move || {
            settler.settle_target(id.key, TargetOutcome::Committed, true);
        });

        let answer = match index.admit(id, WaitMode::Register) {
            Admission::SuppressedSettled(outcome) => outcome,
            Admission::SuppressedPending { guard, rx } => {
                let outcome = tokio::time::timeout(Duration::from_secs(5), rx)
                    .await
                    .expect("the caller must never hang")
                    .expect("the sender is never dropped without a send");
                drop(guard);
                outcome
            }
            other => panic!("round {round}: unexpected {other:?}"),
        };
        assert_eq!(answer, pulsus_write::ClaimOutcome::Ok);
        handle.await.expect("the settling task");
    }
}

/// A waiter on one key is not woken by a settle of another, and a settle of
/// its own key wakes every waiter on it.
#[tokio::test]
async fn a_settle_wakes_its_own_keys_waiters_and_no_others() {
    use pulsus_write::{PushDedup, PushDigest, PushIdentity, TargetOutcome};

    let index = PushDedup::new(
        16 * 1024 * 1024,
        Duration::from_secs(300),
        Duration::from_secs(120),
    );
    let id = |n: u128| PushIdentity {
        key: PushDigest::from_raw(n),
        content: PushDigest::from_raw(n),
        declared_retry: false,
    };
    for n in [1u128, 2] {
        let Admission::Admit(mut guard) = index.admit(id(n), WaitMode::None) else {
            panic!("must admit");
        };
        guard.note_target(true);
        guard.seal();
    }
    let mut on_a = Vec::new();
    for _ in 0..3 {
        match index.admit(id(1), WaitMode::Register) {
            Admission::SuppressedPending { guard, rx } => on_a.push((guard, rx)),
            other => panic!("unexpected {other:?}"),
        }
    }
    let (b_guard, mut b_rx) = match index.admit(id(2), WaitMode::Register) {
        Admission::SuppressedPending { guard, rx } => (guard, rx),
        other => panic!("unexpected {other:?}"),
    };

    index.settle_target(id(1).key, TargetOutcome::Committed, true);
    for (guard, rx) in on_a {
        assert_eq!(
            rx.await.expect("every waiter on the settled key is woken"),
            pulsus_write::ClaimOutcome::Ok
        );
        drop(guard);
    }
    assert!(
        b_rx.try_recv().is_err(),
        "a settle of one key must not wake another key's waiter"
    );
    drop((b_guard, b_rx));
}
