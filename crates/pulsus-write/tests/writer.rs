//! Writer-core tests that are not about one landing block: the
//! concurrent-admit backpressure bound, the pattern kill switch, and the
//! charge a stream registration carries.
//!
//! **Most of this suite went with the per-table flush loops** (issue #603).
//! One push is now one insert of one block into `log_landing`, so the cases
//! that drove two or three flush generations independently — the cross-table
//! wait-join, the age trigger, the per-table retry and spool classification,
//! the duplicate stream-registration race, the forced settlement of a
//! generation, and the whole `log_streams` registration backfill — describe a
//! path that no longer exists. What replaces them is
//! `crates/pulsus-write/tests/log_landing.rs`, which drives the same endings
//! over one block.
//!
//! All against a mock [`BlockInserter`] — no real ClickHouse.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use pulsus_clickhouse::{ChError, ChRow};
use pulsus_config::WriterConfig;
use pulsus_model::{Date, Fingerprint, LabelSet, UnixNano};
use pulsus_write::writer::{
    BlockInserter, LogLandingRow, LogSampleRow, LogStreamRow, LogWriter, landing_charge,
};
use pulsus_write::{AdmitRefusal, LogRow, LogSink, ParsedLogs, PushHeaders, StreamRow};

/// A mock [`BlockInserter`]: every call is counted, and every call answers the
/// same way — `Ok`, or `Hang`, which never resolves so an admitted block's
/// bytes stay reserved for the whole of a case.
#[derive(Clone, Copy, Debug)]
enum MockBehavior {
    Ok,
    Hang,
}

struct MockInserter {
    behavior: MockBehavior,
    calls: AtomicUsize,
    kinds: std::sync::Mutex<Vec<Vec<u64>>>,
}

impl MockInserter {
    fn new(behavior: MockBehavior) -> Arc<Self> {
        Arc::new(MockInserter {
            behavior,
            calls: AtomicUsize::new(0),
            kinds: std::sync::Mutex::new(Vec::new()),
        })
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn kinds_of(&self, call: usize) -> Vec<u64> {
        self.kinds
            .lock()
            .expect("mock mutex poisoned")
            .get(call)
            .cloned()
            .unwrap_or_else(|| panic!("no insert call at index {call}"))
    }
}

impl<R: ChRow> BlockInserter<R> for MockInserter {
    fn insert<'a>(
        &'a self,
        _table: &'a str,
        rows: &'a [R],
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.kinds.lock().expect("mock mutex poisoned").push(
            rows.iter()
                .map(|r| {
                    serde_json::to_value(r)
                        .ok()
                        .and_then(|v| v["kind"].as_u64())
                        .unwrap_or(u64::MAX)
                })
                .collect(),
        );
        let behavior = self.behavior;
        Box::pin(async move {
            match behavior {
                MockBehavior::Ok => Ok(()),
                MockBehavior::Hang => std::future::pending::<Result<(), ChError>>().await,
            }
        })
    }
}

const T: i64 = 1_700_000_000_000_000_000;

fn labels_with_service(service: &str) -> LabelSet {
    LabelSet::from_normalized([("service_name".to_string(), service.to_string())]).0
}

fn batch_for(fingerprint: u128, service: &str, timestamp_ns: i64, new_stream: bool) -> ParsedLogs {
    let mut out = ParsedLogs {
        rows: vec![LogRow {
            service: service.to_string(),
            fingerprint: Fingerprint::from_raw(fingerprint),
            timestamp_ns: UnixNano(timestamp_ns),
            severity: 0,
            body: "hello".to_string(),
            structured_metadata: String::new(),
        }],
        ..Default::default()
    };
    if new_stream {
        out.streams.push(StreamRow {
            month: Date::start_of_month_utc(timestamp_ns).expect("a representable month"),
            fingerprint: Fingerprint::from_raw(fingerprint),
            service: service.to_string(),
            labels: labels_with_service(service),
            updated_ns: timestamp_ns,
        });
    }
    out
}

fn writer_with(cfg: WriterConfig, landing: Arc<MockInserter>) -> LogWriter {
    LogWriter::with_landing_inserter(landing, &cfg)
}

/// Many concurrent `admit` calls against a small `PULSUS_INGEST_QUEUE_BYTES`
/// must never let `queued_bytes` exceed the limit, and the calls that lose the
/// race get `Backpressure`.
///
/// **The charge is one whole block per push** (issue #603), not one row's
/// estimate: the queue holds the block, its settings and its claim beside the
/// rows, so the limit here is a multiple of `landing_charge`.
#[tokio::test]
async fn concurrent_admit_never_exceeds_the_queue_bytes_limit() {
    let one_push = batch_for(0, "svc", T, false);
    let push_bytes = landing_charge::<LogLandingRow>(
        one_push
            .rows
            .iter()
            .map(|r| LogLandingRow::est_landing_bytes(LogSampleRow::est_source_bytes(r)))
            .sum(),
    );

    let admits = 40u64;
    let limit = push_bytes * 10; // room for exactly 10 successful admits
    let cfg = WriterConfig {
        log_patterns: false,
        ingest_dedup: false,
        ingest_queue_bytes: pulsus_config::ByteSize(limit),
        ..Default::default()
    };

    // Hanging, so an admitted push's bytes stay reserved for the whole case.
    let landing = MockInserter::new(MockBehavior::Hang);
    let writer = Arc::new(writer_with(cfg, landing));

    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..admits {
        let writer = writer.clone();
        tasks.spawn(async move {
            writer.admit(
                batch_for(u128::from(i), "svc", T, false),
                PushHeaders::default(),
            )
        });
    }
    let results: Vec<Result<(), AdmitRefusal>> = tasks.join_all().await;

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
    assert_eq!(queue_bytes, ok_count * push_bytes);

    writer.shutdown(Duration::from_millis(10)).await;
}

/// `PULSUS_LOG_PATTERNS=false` is zero extraction work and zero pattern rows:
/// the block carries the lines and the registration and nothing else.
#[tokio::test]
async fn kill_switch_off_lands_no_pattern_rows() {
    let landing = MockInserter::new(MockBehavior::Ok);
    let writer = writer_with(
        WriterConfig {
            log_patterns: false,
            ingest_dedup: false,
            ..Default::default()
        },
        landing.clone(),
    );

    writer
        .admit_flush(two_line_batch(), PushHeaders::default())
        .expect("queue has room")
        .await
        .expect("the block commits");

    assert_eq!(landing.call_count(), 1);
    assert_eq!(
        landing.kinds_of(0),
        vec![0, 0, 1],
        "two lines and one registration, and no pattern aggregate"
    );

    writer.shutdown(Duration::from_secs(2)).await;
}

/// The kill switch ON lands the aggregate over the identical fixture, so the
/// case above asserts an absence this push otherwise produces rather than one
/// the fixture could never have shown.
#[tokio::test]
async fn kill_switch_on_lands_the_pattern_aggregate() {
    let landing = MockInserter::new(MockBehavior::Ok);
    let writer = writer_with(
        WriterConfig {
            log_patterns: true,
            ingest_dedup: false,
            ..Default::default()
        },
        landing.clone(),
    );

    writer
        .admit_flush(two_line_batch(), PushHeaders::default())
        .expect("queue has room")
        .await
        .expect("the block commits");

    let kinds = landing.kinds_of(0);
    assert!(
        kinds.contains(&2),
        "extraction lands at least one kind-2 row: {kinds:?}"
    );

    writer.shutdown(Duration::from_secs(2)).await;
}

/// The fixture both kill-switch cases push: two lines whose bodies differ,
/// plus one unregistered stream.
fn two_line_batch() -> ParsedLogs {
    let mut batch = batch_for(1, "svc", T, true);
    batch.rows.push(LogRow {
        service: "svc".to_string(),
        fingerprint: Fingerprint::from_raw(1),
        timestamp_ns: UnixNano(T),
        severity: 0,
        body: "request 7 completed".to_string(),
        structured_metadata: String::new(),
    });
    batch
}

/// The queue charge covers the canonical label JSON a kind-1 row owns, which
/// the source `StreamRow` holds as a `LabelSet` and never as text. A charge
/// read off the label set's raw key and value lengths would understate the
/// escaped string the landed row then holds.
#[test]
fn the_stream_charge_covers_the_canonical_json_the_row_owns() {
    let stream = StreamRow {
        month: Date::start_of_month_utc(T).expect("a representable month"),
        fingerprint: Fingerprint::from_raw(1),
        service: "svc".to_string(),
        labels: LabelSet::from_normalized([
            ("a".to_string(), "\"quoted\"\n".to_string()),
            ("b".to_string(), "plain".to_string()),
        ])
        .0,
        updated_ns: T,
    };
    let charged = LogLandingRow::est_landing_bytes(LogStreamRow::est_source_bytes(&stream));
    let row = LogLandingRow::stream(0, &stream);
    // The row's own text, plus the fixed-width columns `LogStreamRow::estimate`
    // prices: month, fingerprint and updated_ns.
    let held = LogLandingRow::est_landing_bytes(
        (row.service.len() + row.labels.len()) as u64 + 2 + 16 + 8,
    );
    assert!(
        charged >= held,
        "the charge ({charged}) must cover the {held} bytes the landed row \
         holds, escaping included"
    );
}
