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
//! **Three shapes of push, because one row can be as large as many** (issue
//! #603 code review rounds 8 and 9): many narrow float samples; one accepted
//! native-histogram row carrying up to 65,536 custom bucket bounds; and one row
//! whose single string field is megabytes — a kind-2 `labels`, or a kind-3
//! `help` and `unit`. A per-row value tree is invisible in the first and is the
//! whole cost in the second; a serialized string is invisible in both and is
//! the whole cost in the third.
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
use pulsus_model::{
    CUSTOM_BUCKETS_SCHEMA, CounterResetHint, DEFAULT_ACTIVITY_BUCKET_MS, Fingerprint, LabelSet,
    NativeHistogram, Span,
};
use pulsus_write::writer::{
    BlockInserter, MetricWriter, MetricWriterTables, SPOOL_CHUNK_BYTES, WriterRuntime,
};
use pulsus_write::{
    HistogramPoint, MetricMetadata, MetricPoint, MetricSink, ParsedMetrics, PushHeaders, SeriesRef,
};

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

/// One accepted native-histogram sample whose single row carries `bounds`
/// custom bucket bounds, plus its series — **two landing rows, one of them as
/// wide as the ingest seam admits**.
///
/// `MAX_BUCKETS_PER_HISTOGRAM_SIDE` is 65,536
/// (`crates/pulsus-write/src/protocols/remote_write.rs`), so a decoded
/// histogram may carry that many `custom_values`, and `custom_values` is
/// spooled twice — once as numbers and once as the exact bit patterns. The
/// histogram is built the way the ingest seam accepts it (schema −53, no
/// negative side, no zero bucket, `count` the cumulative bucket total), and
/// the case below asserts `validate()` agrees before it is pushed: a row the
/// seam would refuse proves nothing about what an accepted one costs.
fn wide_hist_push(bounds: usize) -> ParsedMetrics {
    let (labels, _) = LabelSet::from_normalized([(
        "instance".to_string(),
        "checkout-7.eu-west-1.internal:9100".to_string(),
    )]);
    ParsedMetrics {
        hist_samples: vec![HistogramPoint {
            metric_name: Arc::from("http_request_duration_seconds"),
            fingerprint: Fingerprint::from_raw(11),
            unix_milli: 1_000,
            histogram: wide_histogram(bounds),
        }],
        series: vec![SeriesRef {
            metric_name: Arc::from("http_request_duration_seconds"),
            fingerprint: Fingerprint::from_raw(11),
            labels,
        }],
        ..Default::default()
    }
}

/// One sample and its series, whose **one label value is `bytes` long** — a
/// kind-2 landing row whose `labels` column is a single string many spool
/// chunks long (issue #603 code review round 9, finding 1).
///
/// A row can be as large as many rows through its text as well as through its
/// arrays, and nothing at the ingest seam bounds one label's value below the
/// per-push byte ceiling. The value is plain ASCII with an escape every few
/// bytes, so the encoded form is longer than the value and the escaping is
/// part of what is measured.
fn long_label_push(bytes: usize) -> ParsedMetrics {
    let (labels, _) = LabelSet::from_normalized([("instance".to_string(), long_text(bytes))]);
    ParsedMetrics {
        samples: vec![MetricPoint {
            metric_name: Arc::from("http_request_duration_seconds_bucket"),
            fingerprint: Fingerprint::from_raw(7),
            unix_milli: 1_000,
            value: 1.5,
        }],
        series: vec![SeriesRef {
            metric_name: Arc::from("http_request_duration_seconds_bucket"),
            fingerprint: Fingerprint::from_raw(7),
            labels,
        }],
        ..Default::default()
    }
}

/// One descriptor whose `help` is `bytes` long and whose `unit` is an eighth
/// of that — a kind-3 landing row carrying **two** strings past the chunk
/// boundary, so a bound that happened to hold for the first string only would
/// not hold here. A descriptor is a push of its own: it needs no sample, and
/// every push emits its descriptors.
fn long_metadata_push(bytes: usize) -> ParsedMetrics {
    ParsedMetrics {
        metadata: vec![MetricMetadata {
            metric_name: Arc::from("http_request_duration_seconds"),
            metric_type: "histogram".to_string(),
            help: long_text(bytes),
            unit: long_text(bytes / 8),
            updated_ns: 1_700_000_000_000_000_000,
        }],
        ..Default::default()
    }
}

/// `bytes` of text with a character needing a JSON escape every few bytes, so
/// a streamed escaper's fragments fall inside escapes as well as between them.
fn long_text(bytes: usize) -> String {
    "a\"b\nc\\d\u{0}e".repeat(bytes / 9)
}

/// The histogram [`wide_hist_push`] carries: `bounds` custom bucket bounds,
/// each a float whose decimal form is long enough that the spooled text is not
/// a single digit.
fn wide_histogram(bounds: usize) -> NativeHistogram {
    NativeHistogram {
        counter_reset_hint: CounterResetHint::Unknown,
        schema: CUSTOM_BUCKETS_SCHEMA,
        zero_threshold: 0.0,
        zero_count: 0,
        count: 3,
        sum: 6.25,
        positive_spans: vec![Span {
            offset: 0,
            length: 2,
        }],
        negative_spans: Vec::new(),
        positive_buckets: vec![1, 1],
        negative_buckets: Vec::new(),
        custom_values: (0..bounds).map(|i| 0.5 + i as f64 / 1024.0).collect(),
    }
}

/// Admits one push of `rows` samples and waits for it to settle, against an
/// inserter that either commits it or poisons it. Answers the peak live bytes
/// over the whole push, measured from the instant before admission — the rows
/// are materialised inside that window in both cases, so the difference
/// between the two is what the failure path holds and the success path does
/// not.
async fn peak_over_one_push(rows: usize, poison: bool) -> u64 {
    peak_over_push(push_of(rows), poison).await.peak
}

/// What one measured push answers: the peak live bytes, and how long the
/// document it spooled is. The second is how a case states that its fields
/// crossed the chunk boundary rather than assuming they did.
struct Measured {
    peak: u64,
    spooled_bytes: u64,
}

/// [`peak_over_one_push`] over any push, so the wide-row case measures the
/// same window the many-rows case does.
async fn peak_over_push(push: ParsedMetrics, poison: bool) -> Measured {
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

    let documents = spooled(&root);
    assert_eq!(
        documents.len(),
        usize::from(poison),
        "the poisoned block is on disk and the committed one is not"
    );
    let spooled_bytes = documents.iter().sum();
    writer.shutdown(Duration::from_secs(5)).await;
    std::fs::remove_dir_all(&root).ok();
    Measured {
        peak,
        spooled_bytes,
    }
}

/// The byte length of every spooled document under `root`.
fn spooled(root: &Path) -> Vec<u64> {
    let dir = root.join("poison").join("metric_landing");
    std::fs::read_dir(&dir)
        .map(|entries| {
            entries
                .flatten()
                .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("json"))
                .filter_map(|e| e.metadata().ok().map(|m| m.len()))
                .collect()
        })
        .unwrap_or_default()
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

/// The two widths of **one** row: a quarter of the ingest seam's per-side
/// bucket cap, and the cap itself.
const BOUNDS: usize = 16_384;
const BOUNDS_4X: usize = BOUNDS * 4;

/// The two lengths of **one string field**: both many
/// [`SPOOL_CHUNK_BYTES`] chunks, and the larger four times the smaller. The
/// smaller alone is twice [`SPOOL_CEILING_BYTES`], so an encoder holding one
/// serialized copy of it is over the ceiling at either length and the two
/// lengths differ by more than [`WIDTH_SLACK_BYTES`].
///
/// Both fit inside `PULSUS_BATCH_BYTES` (`WriterConfig::batch_bytes`, 16 MiB
/// by default): the kind-3 case's two strings come to nine eighths of the
/// larger figure, and admission refuses a push above the ceiling before
/// anything is queued.
const STRING_BYTES: usize = 2 * 1024 * 1024;
const STRING_BYTES_4X: usize = STRING_BYTES * 4;

/// How far apart the two widths' overheads may be, in either direction. It is
/// what makes "fixed" an assertion rather than a hope: a four-fold array must
/// cost the encoder nothing, because it writes elements one at a time into a
/// buffer that spills as it fills. A whole-document encoder pays for every
/// element twice, in a value tree and again in the serialised body, and the two
/// widths then differ by megabytes.
///
/// The fixed part the encoder may add is the 64 KiB chunk and the file
/// writer's own copy of it; this leaves room for those at either width and no
/// room for an array of 16,384 floats.
const WIDTH_SLACK_BYTES: u64 = 256 * 1024;

/// **Both halves are in one `#[test]`.** The measurement is a process-wide
/// high-water mark, so a second test function running on another thread would
/// land inside this one's window (this file's own doc comment).
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

        // **One wide row, not many narrow ones** (issue #603 code review round
        // 8, finding 2). The bound above holds for a push of many small rows
        // whatever the encoder does per row; what a per-row value tree costs is
        // only visible when the row itself is wide, and an accepted histogram
        // may carry 65,536 custom bucket bounds.
        let mut wide_extra = Vec::new();
        for (label, bounds) in [("W", BOUNDS), ("4W", BOUNDS_4X)] {
            wide_histogram(bounds)
                .validate()
                .expect("the ingest seam accepts this histogram");
            let commit = peak_over_push(wide_hist_push(bounds), false).await.peak;
            let spool = peak_over_push(wide_hist_push(bounds), true).await.peak;
            let extra = spool.saturating_sub(commit);
            assert!(
                extra <= SPOOL_CEILING_BYTES,
                "spooling one row of {bounds} custom bucket bounds ({label}) \
                 held {extra} bytes more than committing it, over the ceiling \
                 {SPOOL_CEILING_BYTES}: the encoder holds a copy of the row's \
                 arrays that nothing charges the queue for (commit peak \
                 {commit}, spool peak {spool})"
            );
            wide_extra.push(extra);
        }
        let (narrow, wide) = (wide_extra[0], wide_extra[1]);
        assert!(
            wide.abs_diff(narrow) <= WIDTH_SLACK_BYTES,
            "four times the array moved the spool path's overhead by {} bytes \
             ({narrow} at {BOUNDS} bounds, {wide} at {BOUNDS_4X}): it must stay \
             fixed as the array grows, or the bound is one push's size and not \
             a constant",
            wide.abs_diff(narrow)
        );

        // **One long string, not one long array** (issue #603 code review
        // round 9, finding 1). A row is as large as the push chooses through
        // its text as well: a kind-2 row's `labels` and a kind-3 row's `help`
        // and `unit` are each one string, of any length admission accepts. An
        // encoder that serialises a whole string before it spills holds a copy
        // of that string on top of the row the reservation covers, which is the
        // same defect the array case pins one shape further down.
        for (name, push) in [
            (
                "kind-2 labels",
                long_label_push as fn(usize) -> ParsedMetrics,
            ),
            ("kind-3 help and unit", long_metadata_push),
        ] {
            let mut string_extra = Vec::new();
            for (label, bytes) in [("S", STRING_BYTES), ("4S", STRING_BYTES_4X)] {
                let commit = peak_over_push(push(bytes), false).await.peak;
                let spooled = peak_over_push(push(bytes), true).await;
                let extra = spooled.peak.saturating_sub(commit);
                // The boundary is crossed, not assumed: the document on disk
                // is longer than the chunk several times over, and one field of
                // it is longer than the chunk on its own.
                assert!(
                    bytes / 8 > SPOOL_CHUNK_BYTES
                        && spooled.spooled_bytes > 4 * SPOOL_CHUNK_BYTES as u64,
                    "the {name} case at {label} must cross the {SPOOL_CHUNK_BYTES} \
                     byte chunk boundary inside one field: its shortest long \
                     string is {} bytes and its document is {} bytes",
                    bytes / 8,
                    spooled.spooled_bytes
                );
                assert!(
                    extra <= SPOOL_CEILING_BYTES,
                    "spooling one row whose {name} is {bytes} bytes ({label}) \
                     held {extra} bytes more than committing it, over the \
                     ceiling {SPOOL_CEILING_BYTES}: the encoder holds a copy of \
                     the row's text that nothing charges the queue for (commit \
                     peak {commit}, spool peak {})",
                    spooled.peak
                );
                string_extra.push(extra);
            }
            let (short, long) = (string_extra[0], string_extra[1]);
            assert!(
                long.abs_diff(short) <= WIDTH_SLACK_BYTES,
                "four times the {name} text moved the spool path's overhead by \
                 {} bytes ({short} at {STRING_BYTES} bytes, {long} at \
                 {STRING_BYTES_4X}): it must stay fixed as the string grows, or \
                 the bound is one push's size and not a constant",
                long.abs_diff(short)
            );
        }
    });
}
