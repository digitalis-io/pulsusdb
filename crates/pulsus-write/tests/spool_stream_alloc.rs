//! What spooling a failed metrics landing block costs in **live** memory
//! beyond committing the same block (issue #603 code review round 7, finding
//! 3).
//!
//! A block that cannot be committed is written to disk as one JSON document,
//! because that copy is the push's only one. The queue reservation covers the
//! block's rows and is held until that write returns — but it covers the rows
//! only. An encoder that materialises the whole document in memory first
//! holds, on top of those rows, one value tree for the push and then the
//! push's whole serialised body, and charges for neither: enough concurrently
//! failing blocks then exceed `PULSUS_INGEST_QUEUE_BYTES` by a multiple of the
//! admitted data, which is exactly what that bound exists to stop.
//!
//! So what is measured here is the **peak live bytes** of spooling one block
//! minus the peak live bytes of committing the identical block — everything
//! the failure path holds that the success path does not — at one size and at
//! four times that size. It must stay under a fixed ceiling and must not grow
//! with the push.
//!
//! Live bytes, not bytes requested: an encoder that writes the document in
//! bounded pieces still *requests* bytes in proportion to the push, and only
//! a high-water mark of what is held at one instant tells the two apart.
//!
//! Everything runs in the single `#[test]` below, on a current-thread runtime,
//! so no parallel test thread contributes to the process-wide high-water mark
//! this measures.

use std::alloc::{GlobalAlloc, Layout, System};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use pulsus_clickhouse::{ChError, ChRow, QuerySettings};
use pulsus_config::WriterConfig;
use pulsus_model::{DEFAULT_ACTIVITY_BUCKET_MS, Fingerprint, LabelSet};
use pulsus_write::writer::{BlockInserter, MetricWriter, MetricWriterTables, WriterRuntime};
use pulsus_write::{MetricPoint, MetricSink, ParsedMetrics, PushHeaders, SeriesRef};

// -- the allocator --------------------------------------------------------

/// Bytes held right now, process-wide, and the high-water mark since it was
/// last armed. Process-wide rather than per-thread because `tokio::fs` runs
/// the file write on a blocking thread: a per-thread tally would miss what
/// that thread holds and would see a buffer allocated here and freed there as
/// memory never released.
static LIVE: AtomicI64 = AtomicI64::new(0);
static PEAK: AtomicI64 = AtomicI64::new(0);

/// Records `delta` against the live total and raises the high-water mark.
/// Atomics only — nothing here allocates, so the allocator cannot re-enter
/// itself.
fn charge(delta: i64) {
    let live = LIVE.fetch_add(delta, Ordering::Relaxed) + delta;
    if delta > 0 {
        PEAK.fetch_max(live, Ordering::Relaxed);
    }
}

struct PeakAlloc;

// SAFETY: every method delegates verbatim to the system allocator; the only
// side effect is a pair of relaxed atomic updates.
unsafe impl GlobalAlloc for PeakAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        charge(layout.size() as i64);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        charge(-(layout.size() as i64));
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        charge(new_size as i64 - layout.size() as i64);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: PeakAlloc = PeakAlloc;

/// Arms the high-water mark at what is live now, runs `body`, and answers how
/// far above that mark the peak rose.
async fn peak_bytes_of<F: Future<Output = ()>>(body: F) -> u64 {
    let base = LIVE.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    body.await;
    (PEAK.load(Ordering::Relaxed) - base).max(0) as u64
}

// -- the inserter ---------------------------------------------------------

/// Answers every insert the same way: `Ok`, so the block commits and is never
/// spooled, or a non-retryable error, so it is spooled whole.
struct FixedInserter {
    poison: bool,
}

impl<R: ChRow> BlockInserter<R> for FixedInserter {
    fn insert<'a>(
        &'a self,
        _table: &'a str,
        _rows: &'a [R],
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
        let poison = self.poison;
        Box::pin(async move {
            if poison {
                Err(ChError::Decode("spooled on purpose".to_string()))
            } else {
                Ok(())
            }
        })
    }

    fn insert_with<'a>(
        &'a self,
        table: &'a str,
        rows: &'a [R],
        _extra: &'a QuerySettings,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
        self.insert(table, rows)
    }
}

// -- the fixture ----------------------------------------------------------

fn spool_root(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pulsus-spool-stream-alloc-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create the spool root");
    dir
}

/// `rows` float samples on one series, one millisecond apart, so the push is
/// `rows + 1` landing rows and every one of them carries text.
fn push_of(rows: usize) -> ParsedMetrics {
    let (labels, _) = LabelSet::from_normalized([(
        "instance".to_string(),
        "checkout-7.eu-west-1.internal:9100".to_string(),
    )]);
    ParsedMetrics {
        samples: (0..rows)
            .map(|i| MetricPoint {
                metric_name: Arc::from("http_request_duration_seconds_bucket"),
                fingerprint: Fingerprint::from_raw(7),
                unix_milli: 1_000 + i as i64,
                value: 1.5,
            })
            .collect(),
        series: vec![SeriesRef {
            metric_name: Arc::from("http_request_duration_seconds_bucket"),
            fingerprint: Fingerprint::from_raw(7),
            labels,
        }],
        ..Default::default()
    }
}

/// Admits one push of `rows` samples and waits for it to settle, against an
/// inserter that either commits it or poisons it. Answers the peak live bytes
/// over the whole push, measured from the instant before admission — the rows
/// are materialised inside that window in both cases, so the difference
/// between the two is what the failure path holds and the success path does
/// not.
async fn peak_over_one_push(rows: usize, poison: bool) -> u64 {
    let root = spool_root(if poison { "poison" } else { "commit" });
    let mut runtime = WriterRuntime::from_config(&WriterConfig {
        metrics_landing_inserters: 1,
        metrics_landing_retries: 0,
        ..Default::default()
    });
    runtime.spool_dir = root.clone();
    let writer = MetricWriter::with_landing_inserter_and_runtime(
        Arc::new(FixedInserter { poison }),
        runtime,
        DEFAULT_ACTIVITY_BUCKET_MS,
        MetricWriterTables::metrics_default(),
    );
    let push = push_of(rows);

    let peak = peak_bytes_of(async {
        let wait = writer
            .admit_flush(push, PushHeaders::default())
            .expect("the queue has room");
        let answer = tokio::time::timeout(Duration::from_secs(60), wait)
            .await
            .expect("the block settles");
        assert_eq!(
            answer.is_err(),
            poison,
            "the poisoned block fails and the other commits"
        );
    })
    .await;

    assert_eq!(
        spooled(&root),
        usize::from(poison),
        "the poisoned block is on disk and the committed one is not"
    );
    writer.shutdown(Duration::from_secs(5)).await;
    std::fs::remove_dir_all(&root).ok();
    peak
}

fn spooled(root: &Path) -> usize {
    let dir = root.join("poison").join("metric_landing");
    std::fs::read_dir(&dir)
        .map(|entries| {
            entries
                .flatten()
                .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("json"))
                .count()
        })
        .unwrap_or(0)
}

/// The two push sizes. Small enough to stay quick, large enough that a
/// document held whole is megabytes: one landing row's JSON object is several
/// hundred bytes of keys, text and numbers before the serialised form of it.
const ROWS: usize = 5_000;
const ROWS_4X: usize = ROWS * 4;

/// What spooling one block may hold beyond committing it. A bounded encoder
/// holds one row's value, one row's text and a fixed output buffer, all of
/// which are well inside this; a whole-document encoder holds a value tree and
/// a serialised body for the entire push, which is megabytes at `ROWS` and
/// four times that at `ROWS_4X`. The slack between the two is what makes the
/// ceiling robust against incidental allocation inside the window.
const SPOOL_CEILING_BYTES: u64 = 1024 * 1024;

#[test]
fn spooling_a_block_holds_no_copy_of_the_push() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("build a current-thread runtime");

    runtime.block_on(async {
        // Warm-up, so no measured window pays a one-time cost.
        let _ = peak_over_one_push(64, true).await;
        let _ = peak_over_one_push(64, false).await;

        for (label, rows) in [("N", ROWS), ("4N", ROWS_4X)] {
            let commit = peak_over_one_push(rows, false).await;
            let spool = peak_over_one_push(rows, true).await;
            let extra = spool.saturating_sub(commit);
            assert!(
                extra <= SPOOL_CEILING_BYTES,
                "spooling {rows} rows ({label}) held {extra} bytes more than \
                 committing the same block, over the ceiling \
                 {SPOOL_CEILING_BYTES}: the encoder holds a copy of the push \
                 that nothing charges the queue for (commit peak {commit}, \
                 spool peak {spool})"
            );
        }
    });
}
