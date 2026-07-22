//! `MetricWriter` tests (issue #26 architect plan + review-cycle required
//! tests): three-table sync wait-join / batch-atomicity parity with
//! `LogWriter` (issue #9), activity-bucket series-registration LRU
//! suppression/emission, the crash/partial-failure guarantee (amendment 2:
//! sync `admit_flush` never resolves a false success), and the cross-crate
//! bucket-floor constant identity. All against a mock `BlockInserter` — no
//! real ClickHouse (see `tests/live_metric_writer.rs` for the
//! `PULSUS_TEST_CLICKHOUSE=1`-gated live counterparts, incl. the
//! `ReplacingMergeTree(updated_ns)` collapse-on-read proof).

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pulsus_clickhouse::{ChError, ChRow};
use pulsus_config::{Config, WriterConfig};
use pulsus_model::{DEFAULT_ACTIVITY_BUCKET_MS, LabelSet, NativeHistogram, Span};
use pulsus_write::writer::{BlockInserter, MetricWriter};
use pulsus_write::{
    HistogramPoint, MetricMetadata, MetricPoint, MetricSink, ParsedMetrics, SeriesRef,
};

const BUCKET_MS: i64 = DEFAULT_ACTIVITY_BUCKET_MS;

/// Scriptable mock [`BlockInserter`] — see `tests/writer.rs`'s identical
/// mock for the full rationale; duplicated here (rather than shared)
/// because each `tests/*.rs` file compiles as its own crate. The
/// `*Then*` variants (issue #139, ported from #134's harness) fail
/// `fail_remaining` calls with a deterministic poison error first.
#[derive(Clone, Copy, Debug)]
enum MockBehavior {
    Ok,
    Poison,
    Uncertain,
    Hang,
    PoisonThenOk,
    PoisonThenUncertain,
}

struct MockInserter {
    behavior: Mutex<MockBehavior>,
    calls: AtomicUsize,
    last_row_count: Mutex<usize>,
    /// The most recent call's rows, JSON-serialized — lets a test inspect
    /// row *contents* (e.g. a bucket-floored `unix_milli`) without needing
    /// a row-shape-specific mock per test.
    last_rows_json: Mutex<String>,
    /// Only consulted under the `*Then*` behaviors: the number of
    /// remaining calls that must fail before the second phase begins.
    fail_remaining: AtomicUsize,
}

impl MockInserter {
    fn new(behavior: MockBehavior) -> Arc<Self> {
        Self::new_with_fail_budget(behavior, 0)
    }

    /// A mock whose `behavior` consults a budget of `n` initial failures
    /// (the `*Then*` variants' first phase).
    fn new_with_fail_budget(behavior: MockBehavior, n: usize) -> Arc<Self> {
        Arc::new(MockInserter {
            behavior: Mutex::new(behavior),
            calls: AtomicUsize::new(0),
            last_row_count: Mutex::new(0),
            last_rows_json: Mutex::new(String::new()),
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
                MockBehavior::Hang => std::future::pending::<Result<(), ChError>>().await,
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
    samples: Arc<MockInserter>,
    series: Arc<MockInserter>,
    metadata: Arc<MockInserter>,
) -> MetricWriter {
    // Issue #120: the fourth (`metric_hist_samples`) inserter is unexercised
    // by these float-only tests — a always-`Ok` mock keeps their behavior
    // unchanged; `hist_writer_with` below scripts it for the native path.
    let hist_samples = MockInserter::new(MockBehavior::Ok);
    MetricWriter::with_inserters(samples, series, metadata, hist_samples, &cfg, BUCKET_MS)
}

/// Constructor for the native-histogram tests (issue #120): exposes the
/// `metric_hist_samples` inserter so a test can assert what was written to it
/// (and stamp `value_type` on `metric_series`).
fn hist_writer_with(
    cfg: WriterConfig,
    samples: Arc<MockInserter>,
    series: Arc<MockInserter>,
    hist_samples: Arc<MockInserter>,
) -> MetricWriter {
    let metadata = MockInserter::new(MockBehavior::Ok);
    MetricWriter::with_inserters(samples, series, metadata, hist_samples, &cfg, BUCKET_MS)
}

fn series_ref(metric_name: &str, fingerprint: u64) -> SeriesRef {
    let (labels, _) = LabelSet::from_normalized([("job".to_string(), "checkout".to_string())]);
    SeriesRef {
        metric_name: Arc::from(metric_name),
        fingerprint,
        labels,
    }
}

/// One sample plus, if `new_series` is set, its `SeriesRef` — a real
/// request's first point for a series the writer has never registered.
fn batch_for(
    metric_name: &str,
    fingerprint: u64,
    unix_milli: i64,
    new_series: bool,
) -> ParsedMetrics {
    let mut out = ParsedMetrics {
        samples: vec![MetricPoint {
            metric_name: Arc::from(metric_name),
            fingerprint,
            unix_milli,
            value: 1.0,
        }],
        ..Default::default()
    };
    if new_series {
        out.series.push(series_ref(metric_name, fingerprint));
    }
    out
}

/// The AC's cross-crate bucket-floor identity (architect plan edge case 3):
/// the default `metric_series` activity bucket
/// (`pulsus_config::ReaderConfig::series_activity_bucket`) must resolve to
/// exactly `pulsus_model::DEFAULT_ACTIVITY_BUCKET_MS`, and the writer's
/// admission-time flooring must be the same function the reader (issue
/// #30) renders into its historical-bound SQL — proven here by construction
/// (`MetricWriter` only ever calls `floor_to_activity_bucket`), not by
/// convention.
#[test]
fn default_series_activity_bucket_matches_the_shared_floor_constant() {
    let cfg = Config::default();
    assert_eq!(
        cfg.reader.series_activity_bucket.0.as_millis() as i64,
        DEFAULT_ACTIVITY_BUCKET_MS
    );
}

/// Batch-atomicity parity with `LogWriter` (issue #9's core, inherited):
/// a sync request's `FlushWait` must resolve `Err` when `metric_series`
/// fails even though `metric_samples` succeeds.
#[tokio::test]
async fn sync_admit_flush_resolves_err_when_series_flush_fails_even_though_samples_succeed() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1; // flush on the very next append

    let samples = MockInserter::new(MockBehavior::Ok);
    let series = MockInserter::new(MockBehavior::Poison);
    let metadata = MockInserter::new(MockBehavior::Ok);
    let writer = writer_with(cfg, samples.clone(), series.clone(), metadata);

    let wait = writer
        .admit_flush(batch_for("http_requests_total", 1, 0, true))
        .expect("queue has room");
    let result = tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("flush settles within the test timeout");

    assert!(
        result.is_err(),
        "expected Err because the series generation was poisoned, got {result:?}"
    );
    assert_eq!(samples.call_count(), 1);
    assert_eq!(series.call_count(), 1);
}

/// Required test (architect plan amendment 1/2, closing the review-cycle
/// crash/partial-failure gap): `metric_samples` settles `Ok` while
/// `metric_series` returns `InsertUncertain` —
/// (a) sync `admit_flush` resolves `Err`, never a silent success;
/// (b) the series LRU key is NOT promoted, so a re-admit re-emits;
/// (c) the samples generation settled exactly once (no auto-replay: a
///     second flush never happens for the same admission).
///
/// The transient "samples durable, series not (yet/never)" state this
/// leaves behind is legal (amendment 2, "Finding 1 wording precision") —
/// this test does not assert cross-reader visibility (no real ClickHouse
/// here to observe it against), only the three binding guarantees above.
#[tokio::test]
async fn crash_partial_failure_series_uncertain_never_reports_a_false_success() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    let samples = MockInserter::new(MockBehavior::Ok);
    let series = MockInserter::new(MockBehavior::Uncertain);
    let metadata = MockInserter::new(MockBehavior::Ok);
    let writer = writer_with(cfg, samples.clone(), series.clone(), metadata);

    let wait = writer
        .admit_flush(batch_for("http_requests_total", 7, 0, true))
        .expect("queue has room");
    let result = tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("flush settles within the test timeout");

    // (a) never a false success.
    let err = result.expect_err("an insert-uncertain series flush must resolve Err, not Ok");
    assert!(err.to_string().to_lowercase().contains("audit"));
    assert_eq!(writer.metrics().spool_uncertain_total, 1);

    // (c) samples settled exactly once — no auto-replay of the batch.
    assert_eq!(samples.call_count(), 1);
    assert_eq!(series.call_count(), 1);

    // (b) the LRU was not promoted: a fresh admission for the identical
    // series key must be treated as a miss again (re-emitted), not
    // suppressed.
    writer
        .admit(batch_for("http_requests_total", 7, 0, true))
        .expect("queue has room");
    assert_eq!(
        writer.metrics().series_lru_misses_total,
        2,
        "the uncertain flush must not have promoted the key: the re-admit is a miss again"
    );
    assert_eq!(
        writer.metrics().series_lru_hits_total,
        0,
        "an unpromoted key can never be hit"
    );
}

/// Required test (architect plan "Data flow" + docs/schemas.md §2.1): a
/// series' second sample landing in the SAME activity bucket must not
/// re-register — the LRU suppresses it before it ever reaches a buffer.
#[tokio::test]
async fn same_bucket_second_sample_is_suppressed_by_the_series_lru() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    let samples = MockInserter::new(MockBehavior::Ok);
    let series = MockInserter::new(MockBehavior::Ok);
    let metadata = MockInserter::new(MockBehavior::Ok);
    let writer = writer_with(cfg, samples.clone(), series.clone(), metadata);

    // First sample: brand-new series, registered and flushed to
    // durability — the success-only LRU promotion hook runs strictly
    // before this wait resolves.
    let wait = writer
        .admit_flush(batch_for("http_requests_total", 1, 0, true))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("flush settles")
        .expect("the first registration must flush Ok");
    assert_eq!(series.call_count(), 1);
    assert_eq!(writer.metrics().series_registrations_total, 1);

    // Second sample, same series, same activity bucket (unix_milli=60_000
    // floors to the same bucket as 0 under the default 1h bucket_ms): must
    // be an LRU hit, no new registration.
    writer
        .admit(batch_for("http_requests_total", 1, 60_000, false))
        .expect("queue has room");

    let metrics = writer.metrics();
    assert_eq!(metrics.series_lru_hits_total, 1);
    assert_eq!(
        metrics.series_registrations_total, 1,
        "the same-bucket sample must not register a second metric_series row"
    );

    writer.shutdown(Duration::from_secs(2)).await;
    assert_eq!(
        series.call_count(),
        1,
        "no second metric_series insert happened for the suppressed same-bucket sample"
    );
}

/// A sample landing in a NEW activity bucket for an already-registered
/// series must register again — the LRU key is `(metric_name, fingerprint,
/// bucket)`, so crossing a bucket boundary is a fresh miss (docs/schemas.md
/// §2.1's per-bucket registration rule).
#[tokio::test]
async fn new_bucket_for_an_already_registered_series_emits_a_new_registration() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    let samples = MockInserter::new(MockBehavior::Ok);
    let series = MockInserter::new(MockBehavior::Ok);
    let metadata = MockInserter::new(MockBehavior::Ok);
    let writer = writer_with(cfg, samples.clone(), series.clone(), metadata);

    let wait = writer
        .admit_flush(batch_for("http_requests_total", 1, 0, true))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("flush settles")
        .expect("the first registration must flush Ok");

    // A sample one whole bucket later: needs a fresh SeriesRef, exactly as
    // a real receiver would supply on any request touching a series (the
    // seam does not require the caller to omit already-known series).
    let next_bucket = batch_for("http_requests_total", 1, BUCKET_MS, true);

    let wait = writer.admit_flush(next_bucket).expect("queue has room");
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("flush settles")
        .expect("the new-bucket registration must flush Ok");

    assert_eq!(
        writer.metrics().series_registrations_total,
        2,
        "crossing a bucket boundary must register a new metric_series row"
    );
    assert_eq!(series.call_count(), 2);
}

/// A `metric_series` row's `unix_milli` must be the ACTIVITY-BUCKET floor,
/// not the raw sample timestamp — proven by inspecting the row the mock
/// inserter actually received (not just the registration count above).
#[tokio::test]
async fn registered_series_row_carries_the_bucket_floored_timestamp_not_the_raw_sample() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    let samples = MockInserter::new(MockBehavior::Ok);
    let series = MockInserter::new(MockBehavior::Ok);
    let metadata = MockInserter::new(MockBehavior::Ok);
    let writer = writer_with(cfg, samples, series.clone(), metadata);

    let raw_unix_milli = BUCKET_MS + 12_345; // mid-bucket, not on a boundary
    let wait = writer
        .admit_flush(batch_for("http_requests_total", 1, raw_unix_milli, true))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("flush settles")
        .expect("flush succeeds");

    assert_eq!(series.last_row_count(), 1);
    let json = series.last_rows_json();
    let bucket_field = format!("\"unix_milli\":{BUCKET_MS}");
    let raw_field = format!("\"unix_milli\":{raw_unix_milli}");
    assert!(
        json.contains(&bucket_field),
        "expected the bucket-floored unix_milli {BUCKET_MS} in {json}"
    );
    assert!(
        !json.contains(&raw_field),
        "the raw sample timestamp must never appear in a metric_series row: {json}"
    );
}

/// Shutdown settlement parity with `LogWriter` (issue #9's core, inherited,
/// generalized to three tasks): a sync request joined to all three tables,
/// then `shutdown` before any flush completes, must resolve the waiter with
/// a shutdown error, release the reservation back to zero, and have every
/// flush task exit within the drain deadline.
#[tokio::test]
async fn shutdown_settles_inflight_waiters_across_all_three_tables() {
    let cfg = WriterConfig::default();

    let samples = MockInserter::new(MockBehavior::Hang);
    let series = MockInserter::new(MockBehavior::Hang);
    let metadata = MockInserter::new(MockBehavior::Hang);
    let writer = writer_with(cfg, samples, series, metadata);

    let batch = ParsedMetrics {
        samples: vec![MetricPoint {
            metric_name: Arc::from("http_requests_total"),
            fingerprint: 1,
            unix_milli: 0,
            value: 1.0,
        }],
        series: vec![series_ref("http_requests_total", 1)],
        metadata: vec![MetricMetadata {
            metric_name: Arc::from("http_requests_total"),
            metric_type: "counter".to_string(),
            help: "".to_string(),
            unit: "".to_string(),
            updated_ns: 1,
        }],
        ..Default::default()
    };

    let wait = writer.admit_flush(batch).expect("queue has room");

    tokio::time::timeout(
        Duration::from_secs(5),
        writer.shutdown(Duration::from_millis(50)),
    )
    .await
    .expect("every flush task exits within the drain deadline");

    let result = wait.await;
    let err = result.expect_err("a shutdown-settled generation must resolve Err, not Ok");
    assert!(err.to_string().to_lowercase().contains("shut"));

    assert_eq!(
        writer.metrics().queue_bytes,
        0,
        "reserved bytes must be released back to zero exactly once"
    );
}

/// Metadata idempotence at the admission layer (mirrors the live A→B→A
/// test in `tests/live_metric_writer.rs`, but exercised purely through the
/// mock — no ClickHouse needed to prove the `MetadataCache` gate logic
/// itself): repeated identical descriptors flush once; a changed
/// descriptor after that flushes again.
#[tokio::test]
async fn metadata_repeated_identical_descriptor_flushes_once_then_a_change_flushes_again() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    let samples = MockInserter::new(MockBehavior::Ok);
    let series = MockInserter::new(MockBehavior::Ok);
    let metadata = MockInserter::new(MockBehavior::Ok);
    let writer = writer_with(cfg, samples, series, metadata.clone());

    let meta = |metric_type: &str| ParsedMetrics {
        metadata: vec![MetricMetadata {
            metric_name: Arc::from("up"),
            metric_type: metric_type.to_string(),
            help: "".to_string(),
            unit: "".to_string(),
            updated_ns: 1,
        }],
        ..Default::default()
    };

    let wait = writer.admit_flush(meta("gauge")).expect("queue has room");
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("flush settles")
        .expect("first flush succeeds");
    assert_eq!(metadata.call_count(), 1);
    assert_eq!(writer.metrics().metadata_upserts_total, 1);

    // Identical descriptor again: must be suppressed at admission (no
    // buffered row at all), not merely deduplicated at flush time.
    writer.admit(meta("gauge")).expect("queue has room");
    writer.shutdown(Duration::from_secs(2)).await;
    assert_eq!(
        metadata.call_count(),
        1,
        "a repeated identical descriptor must never re-flush"
    );
    assert_eq!(writer.metrics().metadata_upserts_total, 1);
}

/// Paused-time poll: yields to the scheduler (auto-advancing the paused
/// clock) until `cond` holds — bounded so a regression fails loudly
/// instead of hanging. Mirrors `tests/writer.rs`'s helper.
async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..600 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("condition not reached within the paused-time budget: {what}");
}

/// A metadata-only batch: one descriptor for `name`.
fn metadata_batch(name: &str, metric_type: &str, help: &str, updated_ns: i64) -> ParsedMetrics {
    ParsedMetrics {
        metadata: vec![MetricMetadata {
            metric_name: Arc::from(name),
            metric_type: metric_type.to_string(),
            help: help.to_string(),
            unit: String::new(),
            updated_ns,
        }],
        ..Default::default()
    }
}

// -- metric registration backfill (issue #139) ------------------------

/// Issue #139 M1 (hermetic heal — fails with `metric_series`'s
/// `on_flush_poisoned` reverted to `None`): a Poisoned `metric_series`
/// flush resolves the sync waiter `Err` (samples committed — the orphan),
/// then the backfill task re-inserts the registration on its 5s tick and
/// confirms the heal. The only spool write is the original generation
/// failure — the backfill itself never spools.
#[tokio::test(start_paused = true)]
async fn series_backfill_reinserts_a_failed_series_registration_until_durable() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1; // flush on the very next append

    let samples = MockInserter::new(MockBehavior::Ok);
    let series = MockInserter::new_with_fail_budget(MockBehavior::PoisonThenOk, 1);
    let metadata = MockInserter::new(MockBehavior::Ok);
    let writer = writer_with(cfg, samples.clone(), series.clone(), metadata);

    let wait = writer
        .admit_flush(batch_for("http_requests_total", 51, 0, true))
        .expect("queue has room");
    let result = tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout");
    assert!(
        result.is_err(),
        "the poisoned series generation must still resolve the sync waiter Err"
    );
    assert_eq!(samples.call_count(), 1, "the samples generation committed");
    assert_eq!(series.call_count(), 1);
    assert_eq!(writer.metrics().series_backfill.enqueued_total, 1);

    // The 5s backfill tick re-inserts exactly the one pending row —
    // without the fix there is no second series insert, ever.
    wait_until(
        "the backfill re-insert heals the series registration",
        || writer.metrics().series_backfill.healed_total == 1,
    )
    .await;

    assert_eq!(series.call_count(), 2, "exactly one re-insert");
    assert_eq!(
        series.last_row_count(),
        1,
        "the re-insert carries exactly the one backlogged row"
    );
    let metrics = writer.metrics();
    assert_eq!(metrics.series_backfill.enqueued_total, 1);
    assert_eq!(metrics.series_backfill.healed_total, 1);
    assert_eq!(metrics.series_backfill.pending, 0);
    assert_eq!(
        metrics.spool_poison_total, 1,
        "the only spool write is the original generation failure"
    );
    assert_eq!(
        metrics.spool_uncertain_total, 0,
        "the backfill path never spools"
    );
}

/// Issue #139 M2 (`SeriesLru` promotion on heal): a confirmed backfill
/// re-insert promotes the `(name, fingerprint, bucket, value_type)` into
/// the success-only `SeriesLru`, so a later admit of the same key is an
/// LRU hit, never a re-emitted series row.
#[tokio::test(start_paused = true)]
async fn series_backfill_heal_promotes_the_series_lru() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    let samples = MockInserter::new(MockBehavior::Ok);
    let series = MockInserter::new_with_fail_budget(MockBehavior::PoisonThenOk, 1);
    let metadata = MockInserter::new(MockBehavior::Ok);
    let writer = writer_with(cfg, samples, series.clone(), metadata);

    let wait = writer
        .admit_flush(batch_for("http_requests_total", 52, 0, true))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect_err("the poisoned series generation resolves Err");

    wait_until(
        "the backfill re-insert heals the series registration",
        || writer.metrics().series_backfill.healed_total == 1,
    )
    .await;
    assert_eq!(writer.metrics().series_registrations_total, 1);

    // Re-admit the identical `(name, fp, bucket, value_type)`: promoted
    // by the confirmed heal, it must hit the LRU — no new series row.
    writer
        .admit(batch_for("http_requests_total", 52, 0, true))
        .expect("queue has room");

    let metrics = writer.metrics();
    assert_eq!(
        metrics.series_lru_hits_total, 1,
        "the healed key must be a confirmed-flush LRU hit"
    );
    assert_eq!(
        metrics.series_registrations_total, 1,
        "no re-emitted series row after the heal"
    );
    assert_eq!(series.call_count(), 2, "generation + heal, nothing more");
}

/// Issue #139 M3 (#9 pin, generation path): an Uncertain `metric_series`
/// generation failure is NEVER enqueued or replayed — the hook exists
/// only in the Poisoned arm, so the backfill task has nothing to do.
#[tokio::test(start_paused = true)]
async fn series_uncertain_generation_failure_is_never_enqueued_or_replayed() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    let samples = MockInserter::new(MockBehavior::Ok);
    let series = MockInserter::new(MockBehavior::Uncertain);
    let metadata = MockInserter::new(MockBehavior::Ok);
    let writer = writer_with(cfg, samples, series.clone(), metadata);

    let wait = writer
        .admit_flush(batch_for("http_requests_total", 53, 0, true))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect_err("an insert-uncertain series flush resolves Err");

    // Advance well past three backfill tick intervals (5s each).
    tokio::time::sleep(Duration::from_secs(16)).await;

    let metrics = writer.metrics();
    assert_eq!(
        series.call_count(),
        1,
        "an uncertain generation failure must never be re-inserted (#9)"
    );
    assert_eq!(metrics.series_backfill.enqueued_total, 0);
    assert_eq!(
        metrics.series_backfill.pending, 0,
        "the backlog stays empty"
    );
}

/// Issue #139 M4 (#9 pin, tick path): a series backfill re-insert whose
/// OWN outcome is `InsertUncertain` is terminal — abandoned, never
/// retried. Spool counters are unchanged across the abandonment.
#[tokio::test(start_paused = true)]
async fn series_uncertain_backfill_outcome_is_terminally_abandoned_never_retried() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    let samples = MockInserter::new(MockBehavior::Ok);
    let series = MockInserter::new_with_fail_budget(MockBehavior::PoisonThenUncertain, 1);
    let metadata = MockInserter::new(MockBehavior::Ok);
    let writer = writer_with(cfg, samples, series.clone(), metadata);

    let wait = writer
        .admit_flush(batch_for("http_requests_total", 54, 0, true))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect_err("the poisoned series generation resolves Err");

    let before = writer.metrics();
    assert_eq!(before.spool_poison_total, 1);
    assert_eq!(before.spool_uncertain_total, 0);

    wait_until("the uncertain re-insert outcome is abandoned", || {
        writer.metrics().series_backfill.abandoned_total == 1
    })
    .await;
    assert_eq!(writer.metrics().series_backfill.pending, 0);
    assert_eq!(series.call_count(), 2);

    // Further paused-time advance: no retry of the uncertain outcome.
    tokio::time::sleep(Duration::from_secs(16)).await;
    let metrics = writer.metrics();
    assert_eq!(
        series.call_count(),
        2,
        "an uncertain backfill outcome must never be retried (#9)"
    );
    assert_eq!(metrics.series_backfill.abandoned_total, 1);
    assert_eq!(
        metrics.spool_poison_total, 1,
        "spool counters unchanged across the abandonment"
    );
    assert_eq!(metrics.spool_uncertain_total, 0);
}

/// Issue #139 M5 (no poison spin): a deterministic series backfill
/// re-insert failure abandons the pending batch — the tick never spins
/// on a poisoned backlog, and never double-spools.
#[tokio::test(start_paused = true)]
async fn series_deterministic_backfill_failure_abandons_without_spinning() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    let samples = MockInserter::new(MockBehavior::Ok);
    let series = MockInserter::new(MockBehavior::Poison);
    let metadata = MockInserter::new(MockBehavior::Ok);
    let writer = writer_with(cfg, samples, series.clone(), metadata);

    let wait = writer
        .admit_flush(batch_for("http_requests_total", 55, 0, true))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect_err("the poisoned series generation resolves Err");

    wait_until("the deterministic re-insert failure is abandoned", || {
        writer.metrics().series_backfill.abandoned_total == 1
    })
    .await;
    assert_eq!(writer.metrics().series_backfill.pending, 0);

    tokio::time::sleep(Duration::from_secs(16)).await;
    let metrics = writer.metrics();
    assert_eq!(
        series.call_count(),
        2,
        "one generation insert plus exactly one abandoned re-insert — no spin"
    );
    assert_eq!(metrics.series_backfill.abandoned_total, 1);
    assert_eq!(
        metrics.spool_poison_total, 1,
        "the backfill abandonment never double-spools"
    );
    assert_eq!(metrics.spool_uncertain_total, 0);
}

/// Issue #139 M6 (REWRITTEN, plan v3 delta 1 — the v1 heal-promotes-cache
/// assertion is retired as contradicting invalidate-on-heal): a Poisoned
/// `metric_metadata` flush heals on the backfill tick (row durability:
/// the second insert carries exactly the failed row), and post-heal the
/// cache holds NO entry for the name — so a subsequent admission of the
/// SAME descriptor RE-EMITS one redundant row (the safe direction,
/// collapsed by `ReplacingMergeTree(updated_ns)`).
#[tokio::test(start_paused = true)]
async fn metadata_heal_invalidates_the_cache_so_an_identical_readmission_reemits() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    let samples = MockInserter::new(MockBehavior::Ok);
    let series = MockInserter::new(MockBehavior::Ok);
    let metadata = MockInserter::new_with_fail_budget(MockBehavior::PoisonThenOk, 1);
    let writer = writer_with(cfg, samples, series, metadata.clone());

    let wait = writer
        .admit_flush(metadata_batch("up", "gauge", "help", 1))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect_err("the poisoned metadata generation resolves Err");
    assert_eq!(metadata.call_count(), 1);
    assert_eq!(writer.metrics().metadata_backfill.enqueued_total, 1);

    wait_until("the backfill re-insert heals the metadata row", || {
        writer.metrics().metadata_backfill.healed_total == 1
    })
    .await;
    assert_eq!(metadata.call_count(), 2, "exactly one re-insert");
    assert_eq!(
        metadata.last_row_count(),
        1,
        "the re-insert carries exactly the failed row"
    );
    assert_eq!(writer.metrics().metadata_backfill.pending, 0);

    // Post-heal the cache asserts NOTHING for the name (invalidated, not
    // promoted): admitting the IDENTICAL descriptor must RE-EMIT a
    // redundant, RMT-collapsed row — never be suppressed.
    let wait = writer
        .admit_flush(metadata_batch("up", "gauge", "help", 2))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect("the re-emitted row flushes Ok");

    assert_eq!(
        writer.metrics().metadata_upserts_total,
        2,
        "the identical descriptor re-emits after the heal (cache was invalidated)"
    );
    assert_eq!(
        metadata.call_count(),
        3,
        "generation + heal + the re-emitted row's flush"
    );
}

/// Issue #139 M7b (plan v3 delta 2 — the resident interleaving, full
/// production pipeline): stale A@1 metadata generation Poisoned (real
/// `on_flush_poisoned` → backlog) → newer B@2 confirmed (cache holds B)
/// → the backfill tick heals stale A through the writer-installed hook →
/// a subsequent admission of descriptor A must EMIT a metadata row (the
/// cache was invalidated, not repopulated with A).
///
/// Fails under a reverted upsert-like hook: the heal would install A →
/// the subsequent A admission is suppressed as an equal descriptor → the
/// awaited re-emit insert (4th mock call) never arrives. (The
/// evicted-interleaving counterpart — which also kills v1's version-gated
/// upsert — is M7a, a unit test against the real hook body in
/// `writer::metric`.)
#[tokio::test(start_paused = true)]
async fn m7b_stale_metadata_heal_never_installs_over_a_resident_newer_descriptor() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    let samples = MockInserter::new(MockBehavior::Ok);
    let series = MockInserter::new(MockBehavior::Ok);
    let metadata = MockInserter::new_with_fail_budget(MockBehavior::PoisonThenOk, 1);
    let writer = writer_with(cfg, samples, series, metadata.clone());

    // A@1 poisons: the stale row enters the backlog through the real
    // production hook.
    let wait = writer
        .admit_flush(metadata_batch("up", "counter", "A", 1))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect_err("the poisoned A@1 metadata generation resolves Err");
    assert_eq!(writer.metrics().metadata_backfill.enqueued_total, 1);

    // B@2 confirms while stale A is still pending: the cache now holds B
    // (flush-success upsert).
    let wait = writer
        .admit_flush(metadata_batch("up", "gauge", "B", 2))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect("B@2 flushes Ok");
    assert_eq!(metadata.call_count(), 2);
    assert_eq!(writer.metrics().metadata_upserts_total, 2);

    // The backfill tick heals stale A@1 through the writer-installed
    // hook — which must ONLY invalidate, never install A.
    wait_until("the stale A@1 re-insert heals", || {
        writer.metrics().metadata_backfill.healed_total == 1
    })
    .await;
    assert_eq!(metadata.call_count(), 3);

    // Admit descriptor A again (A@3): with the cache invalidated it MUST
    // emit — under an upsert-like hook A would be resident and the
    // admission suppressed, so this 4th insert would never arrive.
    let wait = writer
        .admit_flush(metadata_batch("up", "counter", "A", 3))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect("the A@3 re-emit flushes Ok");
    wait_until("the A@3 re-emit insert arrives", || {
        metadata.call_count() == 4
    })
    .await;

    assert_eq!(
        writer.metrics().metadata_upserts_total,
        3,
        "the post-heal A admission re-emits (cache asserted nothing for the name)"
    );
    let json = metadata.last_rows_json();
    assert!(
        json.contains("\"metric_type\":\"counter\"") && json.contains("\"help\":\"A\""),
        "the re-emitted row is descriptor A, got {json}"
    );
}

/// Issue #139 M8 (structural append-only exclusion, #9 in full):
/// Poisoned `metric_samples` AND `metric_hist_samples` flushes leave BOTH
/// backfill counter sets at zero — the sample tables have no
/// `on_flush_poisoned` hook at all.
#[tokio::test(start_paused = true)]
async fn poisoned_sample_and_hist_flushes_never_touch_any_backfill_backlog() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    let samples = MockInserter::new(MockBehavior::Poison);
    let series = MockInserter::new(MockBehavior::Ok);
    let hist = MockInserter::new(MockBehavior::Poison);
    let writer = hist_writer_with(cfg, samples.clone(), series, hist.clone());

    // One float sample + one histogram sample (both tables poison).
    let mut batch = batch_for("http_request_duration_seconds", 58, 0, true);
    batch.hist_samples = hist_batch_for("http_request_duration_seconds", 58, 0, false).hist_samples;

    let wait = writer.admit_flush(batch).expect("queue has room");
    tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("flush settles within the test timeout")
        .expect_err("the poisoned sample generations resolve Err");

    // Advance well past three backfill tick intervals.
    tokio::time::sleep(Duration::from_secs(16)).await;

    let metrics = writer.metrics();
    let zero = pulsus_write::writer::BackfillMetricsSnapshot::default();
    assert_eq!(
        metrics.series_backfill, zero,
        "a poisoned metric_samples/metric_hist_samples flush must never touch the \
         series backlog (structural #9 exclusion)"
    );
    assert_eq!(metrics.metadata_backfill, zero, "…nor the metadata backlog");
    assert_eq!(samples.call_count(), 1, "no sample re-insert, ever");
    assert_eq!(hist.call_count(), 1, "no hist-sample re-insert, ever");
    assert_eq!(
        metrics.spool_poison_total, 2,
        "both generations spooled once"
    );
}

// -- native histogram write path (M7-A4, issue #120) -----------------

/// A single-histogram fixture (schema 0, absolute buckets [1,2,1] ->
/// deltas [1,1,-1], count 4).
fn native_hist(metric_name: &str, fingerprint: u64, unix_milli: i64, sum: f64) -> NativeHistogram {
    NativeHistogram {
        counter_reset_hint: pulsus_model::CounterResetHint::Unknown,
        schema: 0,
        zero_threshold: 0.0,
        zero_count: 0,
        count: 4,
        sum,
        positive_spans: vec![Span {
            offset: 1,
            length: 3,
        }],
        negative_spans: vec![],
        positive_buckets: vec![1, 1, -1],
        negative_buckets: vec![],
        custom_values: vec![],
    }
    .also_ident(metric_name, fingerprint, unix_milli)
}

// Tiny helper trait so the fixture reads top-to-bottom (the ident fields
// live on `HistogramPoint`, not `NativeHistogram`).
trait AlsoIdent {
    fn also_ident(self, _n: &str, _f: u64, _u: i64) -> NativeHistogram;
}
impl AlsoIdent for NativeHistogram {
    fn also_ident(self, _n: &str, _f: u64, _u: i64) -> NativeHistogram {
        self
    }
}

fn hist_batch_for(
    metric_name: &str,
    fingerprint: u64,
    unix_milli: i64,
    new_series: bool,
) -> ParsedMetrics {
    let mut out = ParsedMetrics {
        hist_samples: vec![HistogramPoint {
            metric_name: Arc::from(metric_name),
            fingerprint,
            unix_milli,
            histogram: native_hist(metric_name, fingerprint, unix_milli, 5.0),
        }],
        ..Default::default()
    };
    if new_series {
        out.series.push(series_ref(metric_name, fingerprint));
    }
    out
}

/// A native-histogram batch lands one `metric_hist_samples` row and one
/// `metric_series` row stamped `value_type = 1`.
#[tokio::test]
async fn native_histogram_batch_writes_hist_row_and_registers_value_type_one() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1; // flush on the next append

    let samples = MockInserter::new(MockBehavior::Ok);
    let series = MockInserter::new(MockBehavior::Ok);
    let hist = MockInserter::new(MockBehavior::Ok);
    let writer = hist_writer_with(cfg, samples.clone(), series.clone(), hist.clone());

    let wait = writer
        .admit_flush(hist_batch_for("http_request_duration_seconds", 7, 0, true))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("flush settles")
        .expect("flush succeeds");
    writer.shutdown(Duration::from_secs(2)).await;

    assert_eq!(hist.call_count(), 1, "one metric_hist_samples flush");
    assert_eq!(hist.last_row_count(), 1);
    assert_eq!(series.call_count(), 1, "one metric_series registration");
    assert!(
        series.last_rows_json().contains("\"value_type\":1"),
        "the histogram series must register value_type=1, got {}",
        series.last_rows_json()
    );
    // No float samples in this batch.
    assert_eq!(samples.call_count(), 0);
    assert_eq!(writer.metrics().hist_samples.rows_total, 1);
}

/// AC4 (hermetic): a transition bucket — a float sample then a histogram
/// sample at the SAME `(metric_name, fingerprint, bucket)` — registers BOTH
/// `metric_series` rows (`value_type` 0 and 1). If `value_type` were absent
/// from the LRU key, the second registration would be a false hit and the
/// series would flush only once.
#[tokio::test]
async fn transition_bucket_registers_both_float_and_histogram_series_rows() {
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;

    let samples = MockInserter::new(MockBehavior::Ok);
    let series = MockInserter::new(MockBehavior::Ok);
    let hist = MockInserter::new(MockBehavior::Ok);
    let writer = hist_writer_with(cfg, samples.clone(), series.clone(), hist.clone());

    // Float sample first: registers metric_series value_type=0, LRU-promoted
    // on flush.
    let wait = writer
        .admit_flush(batch_for("m", 1, 0, true))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("flush settles")
        .expect("float flush succeeds");
    assert_eq!(series.call_count(), 1);
    assert!(series.last_rows_json().contains("\"value_type\":0"));

    // Histogram sample at the SAME (name, fp, bucket): a distinct value_type
    // key, so it must register a SECOND metric_series row (value_type=1).
    let wait = writer
        .admit_flush(hist_batch_for("m", 1, 0, true))
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("flush settles")
        .expect("histogram flush succeeds");
    writer.shutdown(Duration::from_secs(2)).await;

    assert_eq!(
        series.call_count(),
        2,
        "the histogram registration must NOT be suppressed by the float's LRU entry \
         (value_type is part of the key)"
    );
    assert!(
        series.last_rows_json().contains("\"value_type\":1"),
        "the second registration is the histogram series (value_type=1), got {}",
        series.last_rows_json()
    );
}
