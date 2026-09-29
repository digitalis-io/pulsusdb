//! `LogWriter` tests: one push is one insert of one block into `log_landing`
//! (issue #603), the block's contents and its per-kind columns, the two
//! per-push ceilings, the insert loop's endings, and the success-only stream
//! registration. All against a mock `BlockInserter` — no real ClickHouse (see
//! `tests/live_log_writer.rs` for the `PULSUS_TEST_CLICKHOUSE=1`-gated live
//! counterparts).
//!
//! **Nothing here reads a target table.** The five derived logs tables are
//! maintained by materialized view; what this file pins is the block the
//! writer hands over.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pulsus_clickhouse::{ChError, ChRow, QuerySettings};
use pulsus_config::{ByteSize, WriterConfig};
use pulsus_model::{Date, Fingerprint, LabelSet, UnixNano};
use pulsus_write::writer::{
    BlockInserter, LOG_LANDING_ROW_SLOT_BYTES, LogLandingRow, LogSampleRow, LogStreamRow,
    LogWriter, WriterRuntime, WriterTables, landing_block_overhead_bytes,
};
use pulsus_write::{
    AdmitRefusal, LogRow, LogSink, ParsedLogs, PushHeaders, StreamRow, push_too_large_message,
};

const LANDING: &str = "log_landing";
const SERVICE: &str = "checkout-api";
/// A timestamp inside a representable month, fixed so every case's rows land
/// in one partition and one pattern bucket.
const TS: i64 = 1_700_000_000_000_000_000;

// -- the mock inserter ------------------------------------------------

/// What one insert call does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Act {
    Ok,
    /// A non-retryable error: provably not committed.
    Poison,
    /// A pre-send retryable error.
    Retryable,
    /// `ChError::InsertUncertain`: the fate is unknown.
    Uncertain,
    /// Never returns.
    Hang,
}

/// One scripted call: how long it takes before it acts, then what it does.
#[derive(Clone, Copy, Debug)]
struct Step {
    delay: Duration,
    act: Act,
}

impl Step {
    fn now(act: Act) -> Self {
        Step {
            delay: Duration::ZERO,
            act,
        }
    }
}

/// A scriptable mock [`BlockInserter`]: the script is consumed front to back
/// and its **last step repeats** for every later call, so a one-step script is
/// a constant behaviour. Records every call's table, rows and settings.
struct MockInserter {
    script: Vec<Step>,
    /// Whether a call's table, rows and settings are kept. A case pushing
    /// millions of rows past this mock would otherwise hold a JSON copy of
    /// every one; it still counts its calls.
    record: bool,
    calls: AtomicUsize,
    /// One permit per call, added as the insert begins — a case that has to
    /// know a block is provably in flight waits on this.
    entered: tokio::sync::Semaphore,
    rows: Mutex<Vec<Vec<serde_json::Value>>>,
    settings: Mutex<Vec<Vec<(String, String)>>>,
    tables: Mutex<Vec<String>>,
    empty: QuerySettings,
}

impl MockInserter {
    fn new(script: Vec<Step>) -> Arc<Self> {
        Self::built(script, true)
    }

    fn always(act: Act) -> Arc<Self> {
        Self::new(vec![Step::now(act)])
    }

    /// [`Self::always`], keeping no copy of what it was handed.
    fn always_unrecorded(act: Act) -> Arc<Self> {
        Self::built(vec![Step::now(act)], false)
    }

    fn built(script: Vec<Step>, record: bool) -> Arc<Self> {
        Arc::new(MockInserter {
            script,
            record,
            calls: AtomicUsize::new(0),
            entered: tokio::sync::Semaphore::new(0),
            rows: Mutex::new(Vec::new()),
            settings: Mutex::new(Vec::new()),
            tables: Mutex::new(Vec::new()),
            empty: QuerySettings::new(),
        })
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    /// One call's rows, by call index.
    fn rows_of(&self, call: usize) -> Vec<serde_json::Value> {
        self.rows
            .lock()
            .expect("mock mutex poisoned")
            .get(call)
            .cloned()
            .unwrap_or_else(|| panic!("no insert call at index {call}"))
    }

    fn settings_of(&self, call: usize) -> Vec<(String, String)> {
        self.settings
            .lock()
            .expect("mock mutex poisoned")
            .get(call)
            .cloned()
            .unwrap_or_else(|| panic!("no insert call at index {call}"))
    }

    fn tables(&self) -> Vec<String> {
        self.tables.lock().expect("mock mutex poisoned").clone()
    }
}

impl<R: ChRow> BlockInserter<R> for MockInserter {
    fn insert<'a>(
        &'a self,
        table: &'a str,
        rows: &'a [R],
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
        self.insert_with(table, rows, &self.empty)
    }

    fn insert_with<'a>(
        &'a self,
        table: &'a str,
        rows: &'a [R],
        extra: &'a QuerySettings,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.record {
            self.tables
                .lock()
                .expect("mock mutex poisoned")
                .push(table.to_string());
            self.rows.lock().expect("mock mutex poisoned").push(
                rows.iter()
                    .map(|r| serde_json::to_value(r).unwrap_or(serde_json::Value::Null))
                    .collect(),
            );
            self.settings.lock().expect("mock mutex poisoned").push(
                extra
                    .entries()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            );
        }
        let step = *self
            .script
            .get(call)
            .or_else(|| self.script.last())
            .expect("a script has at least one step");
        self.entered.add_permits(1);
        Box::pin(async move {
            if !step.delay.is_zero() {
                tokio::time::sleep(step.delay).await;
            }
            match step.act {
                Act::Ok => Ok(()),
                Act::Poison => Err(ChError::Decode("mock poison".to_string())),
                Act::Retryable => Err(ChError::Timeout("mock pre-send retryable".to_string())),
                Act::Uncertain => Err(ChError::InsertUncertain("mock uncertain".to_string())),
                Act::Hang => std::future::pending::<Result<(), ChError>>().await,
            }
        })
    }
}

// -- writer construction ----------------------------------------------

/// A spool root of this test's own, so a spool assertion sees only this test's
/// files and nothing lands in the process working directory.
fn spool_root(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pulsus-log-landing-{name}-{}-{}",
        std::process::id(),
        unique()
    ));
    std::fs::create_dir_all(&dir).expect("create the spool root");
    dir
}

fn unique() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

fn runtime_at(cfg: &WriterConfig, spool: &Path) -> WriterRuntime {
    let mut runtime = WriterRuntime::from_config(cfg);
    runtime.spool_dir = spool.to_path_buf();
    runtime
}

fn writer_at(runtime: WriterRuntime, inserter: Arc<MockInserter>) -> LogWriter {
    LogWriter::with_landing_inserter_and_runtime(inserter, runtime, WriterTables::logs_default())
}

/// A writer over `cfg` whose spool is `spool`.
fn writer_with(cfg: &WriterConfig, spool: &Path, inserter: Arc<MockInserter>) -> LogWriter {
    writer_at(runtime_at(cfg, spool), inserter)
}

/// Every spool record this run wrote under `kind`, parsed.
fn spool_records(root: &Path, kind: &str) -> Vec<serde_json::Value> {
    let dir = root.join(kind).join(LANDING);
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let text = std::fs::read_to_string(&path).expect("read a spool file");
        out.push(serde_json::from_str(&text).expect("a spool file is JSON"));
    }
    out
}

/// Yields until `done` holds, so a test can let the insert workers make
/// progress. It yields rather than sleeping: every case that calls it runs on
/// a paused or a single-threaded clock, where a sleep would either advance the
/// paused clock or wait in real time for a worker that only runs when this
/// task yields.
async fn settle_until(mut done: impl FnMut() -> bool) {
    for _ in 0..1024 {
        if done() {
            return;
        }
        tokio::task::yield_now().await;
    }
}

// -- fixtures ---------------------------------------------------------

fn labels_of(service: &str) -> LabelSet {
    LabelSet::from_normalized([
        ("service_name".to_string(), service.to_string()),
        ("env".to_string(), "production".to_string()),
    ])
    .0
}

fn log_row(fingerprint: u128, body: &str) -> LogRow {
    LogRow {
        service: SERVICE.to_string(),
        fingerprint: Fingerprint::from_raw(fingerprint),
        timestamp_ns: UnixNano(TS),
        severity: 0,
        body: body.to_string(),
        structured_metadata: String::new(),
    }
}

fn stream_row(fingerprint: u128) -> StreamRow {
    StreamRow {
        month: Date::start_of_month_utc(TS).expect("a representable month"),
        fingerprint: Fingerprint::from_raw(fingerprint),
        service: SERVICE.to_string(),
        labels: labels_of(SERVICE),
        updated_ns: TS,
    }
}

/// **The fixture every row-count assertion below is decidable against.** Two
/// lines that differ only in a digit run, so extraction yields ONE template
/// and one kind-2 row; one unregistered stream, so one kind-1 row. Four rows,
/// kinds `[0, 0, 1, 2]`.
fn two_lines_one_stream(fingerprint: u128) -> ParsedLogs {
    ParsedLogs {
        rows: vec![
            log_row(fingerprint, "request 1 completed"),
            log_row(fingerprint, "request 2 completed"),
        ],
        streams: vec![stream_row(fingerprint)],
        ..Default::default()
    }
}

/// The `kind` of each row of one recorded insert call, in the block's order.
fn kinds_of(rows: &[serde_json::Value]) -> Vec<u64> {
    rows.iter()
        .map(|r| r["kind"].as_u64().expect("every landed row carries a kind"))
        .collect()
}

fn rows_of_kind(rows: &[serde_json::Value], kind: u64) -> Vec<&serde_json::Value> {
    rows.iter()
        .filter(|r| r["kind"].as_u64() == Some(kind))
        .collect()
}

// -- T1/T2: one push, one insert --------------------------------------

/// **T1.** One push is one `insert_with` call, into `log_landing`, carrying
/// every kind the push produced. The fixture is chosen so the row count is
/// decidable: two lines differing only in a digit run share one template, so
/// extraction yields exactly one kind-2 row.
#[tokio::test]
async fn one_logs_push_is_one_insert_into_the_landing_table() {
    let root = spool_root("one-insert");
    let inserter = MockInserter::always(Act::Ok);
    let writer = writer_with(&WriterConfig::default(), &root, inserter.clone());

    let wait = writer
        .admit_flush(two_lines_one_stream(1), PushHeaders::default())
        .expect("queue has room");
    wait.await.expect("the block commits");

    assert_eq!(inserter.call_count(), 1, "one push is one insert");
    assert_eq!(inserter.tables(), vec![LANDING.to_string()]);

    let rows = inserter.rows_of(0);
    assert_eq!(
        kinds_of(&rows),
        vec![0, 0, 1, 2],
        "two lines, one stream registration, one pattern aggregate: {rows:#?}"
    );
    let patterns = rows_of_kind(&rows, 2);
    assert_eq!(patterns.len(), 1, "the two lines share one template");
    assert_eq!(
        patterns[0]["pattern_count"].as_u64(),
        Some(2),
        "the aggregate counts both lines"
    );

    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **T2.** Two pushes are two inserts, neither carrying the other's rows.
#[tokio::test]
async fn two_logs_pushes_are_two_inserts() {
    let root = spool_root("two-inserts");
    let inserter = MockInserter::always(Act::Ok);
    // Suppression off: the two bodies differ only by fingerprint below, and
    // this case is about the block count rather than the index.
    let cfg = WriterConfig {
        ingest_dedup: false,
        ..Default::default()
    };
    let writer = writer_with(&cfg, &root, inserter.clone());

    for fp in [1u128, 2] {
        writer
            .admit_flush(two_lines_one_stream(fp), PushHeaders::default())
            .expect("queue has room")
            .await
            .expect("the block commits");
    }

    assert_eq!(inserter.call_count(), 2, "two pushes are two inserts");
    for (call, fp) in [(0usize, 1u128), (1, 2)] {
        let rows = inserter.rows_of(call);
        let want =
            serde_json::to_value(Fingerprint::from_raw(fp)).expect("a fingerprint serialises");
        let mut fingerprints: Vec<String> =
            rows.iter().map(|r| r["fingerprint"].to_string()).collect();
        fingerprints.sort();
        fingerprints.dedup();
        assert_eq!(
            fingerprints,
            vec![want.to_string()],
            "call {call} carries its own push's rows and no other's"
        );
    }

    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **T3.** Every landing column carries the value its kind was built from,
/// and every other column is the type's default.
///
/// **`labels == ""` on kinds 0 and 2 is load-bearing**, not incidental:
/// `log_streams_idx_mv` is an `ARRAY JOIN` over
/// `JSONExtractKeysAndValues(labels, 'String')`, which is the empty array for
/// `''`, so a kind-0 or kind-2 row emits no index row whichever side of the
/// join the server applies `WHERE kind = 1` on. Setting `labels` in `line()`
/// would give `log_streams_idx` a row per log line, and this case reddens.
#[test]
fn every_landing_column_is_the_value_its_kind_was_built_from() {
    let line = log_row(7, "request 1 completed");
    let stream = stream_row(7);
    let pattern = pulsus_write::writer::LogPatternRow {
        fingerprint: Fingerprint::from_raw(9),
        bucket_ns: TS - 3,
        pattern: "request <_> completed".to_string(),
        count: 4,
    };

    let k0 = LogLandingRow::line(5, &line);
    assert_eq!(k0.received_ms, 5);
    assert_eq!(k0.kind, LogLandingRow::KIND_LINE);
    assert_eq!(k0.service, line.service);
    assert_eq!(k0.fingerprint, line.fingerprint);
    assert_eq!(k0.timestamp_ns, line.timestamp_ns.0);
    assert_eq!(k0.severity, line.severity);
    assert_eq!(k0.body, line.body);
    assert_eq!(k0.structured_metadata, line.structured_metadata);
    assert_eq!(k0.month, 0);
    assert_eq!(k0.labels, "", "a log line must land no label blob");
    assert_eq!(k0.updated_ns, 0);
    assert_eq!(k0.pattern, "");
    assert_eq!(k0.pattern_count, 0);

    let k1 = LogLandingRow::stream(5, &stream);
    assert_eq!(k1.received_ms, 5);
    assert_eq!(k1.kind, LogLandingRow::KIND_STREAM);
    assert_eq!(k1.service, stream.service);
    assert_eq!(k1.fingerprint, stream.fingerprint);
    assert_eq!(k1.timestamp_ns, 0);
    assert_eq!(k1.severity, 0);
    assert_eq!(k1.body, "", "a stream registration must land no body");
    assert_eq!(k1.structured_metadata, "");
    assert_eq!(k1.month, stream.month.days_since_epoch());
    assert_eq!(k1.labels, stream.labels.to_canonical_json());
    assert_eq!(k1.updated_ns, stream.updated_ns);
    assert_eq!(k1.pattern, "");
    assert_eq!(k1.pattern_count, 0);

    let k2 = LogLandingRow::pattern(5, pattern.clone());
    assert_eq!(k2.received_ms, 5);
    assert_eq!(k2.kind, LogLandingRow::KIND_PATTERN);
    assert_eq!(k2.service, "");
    assert_eq!(k2.fingerprint, pattern.fingerprint);
    assert_eq!(
        k2.timestamp_ns, pattern.bucket_ns,
        "a kind-2 row's timestamp_ns is the aggregate's bucket floor"
    );
    assert_eq!(k2.severity, 0);
    assert_eq!(k2.body, "", "a pattern aggregate must land no body");
    assert_eq!(k2.structured_metadata, "");
    assert_eq!(k2.month, 0);
    assert_eq!(k2.labels, "", "a pattern aggregate must land no label blob");
    assert_eq!(k2.updated_ns, 0);
    assert_eq!(k2.pattern, pattern.pattern);
    assert_eq!(k2.pattern_count, pattern.count);
}

/// **T7.** The pattern bound never undercharges the landed pattern rows: for
/// a batch whose extraction yields `k` distinct templates, the bound is at
/// least the sum of `est_landing_bytes` over the landed kind-2 rows. Dropping
/// the `LOG_LANDING_ROW_SLOT_BYTES` term reddens it.
#[test]
fn the_pattern_bound_never_undercharges_the_landed_pattern_rows() {
    use pulsus_write::patterns::{
        AGG_BASE_OVERHEAD, MAX_DISTINCT_PATTERNS_PER_BATCH, PATTERN_ROW_OVERHEAD,
        aggregate_patterns, est_template_bound,
    };

    for (name, rows) in [
        (
            "one template",
            vec![
                log_row(1, "request 1 completed"),
                log_row(1, "request 2 completed"),
            ],
        ),
        (
            "many templates",
            (0..64)
                .map(|i| log_row(1, &format!("stage {i} alpha beta gamma delta")))
                .collect(),
        ),
        (
            "long bodies",
            (0..8)
                .map(|i| log_row(1, &format!("{} {i}", "word ".repeat(200))))
                .collect(),
        ),
    ] {
        let distinct_cap = rows.len().min(MAX_DISTINCT_PATTERNS_PER_BATCH) as u64;
        let bound: u64 = rows.iter().map(est_template_bound).sum::<u64>()
            + AGG_BASE_OVERHEAD
            + distinct_cap * (PATTERN_ROW_OVERHEAD + LOG_LANDING_ROW_SLOT_BYTES);

        let landed: u64 = aggregate_patterns(&rows)
            .rows
            .into_iter()
            .map(|r| {
                let row = LogLandingRow::pattern(0, r);
                LogLandingRow::est_landing_bytes(row.pattern.len() as u64)
            })
            .sum();

        assert!(
            bound >= landed,
            "{name}: the bound ({bound}) must cover the {landed} bytes the \
             landed kind-2 rows hold"
        );
    }
}

// -- the ceilings and the queue allowance -----------------------------

/// **T12.** A push at either ceiling is refused whole, and both sides of both
/// ceilings are computed from the constants rather than written down.
///
/// Rows: `log_landing_max_rows - 1` landed rows is admitted,
/// `log_landing_max_rows` is `PushTooLarge` — the comparison is `>=`, because
/// a push AT the count is not strictly under the count both pinned row limits
/// carry. Bytes: the case derives one push's own charge, sets the ceiling to
/// exactly that and asserts it is admitted, then to that less one and asserts
/// the refusal — the comparison is `>`.
///
/// On every refusal: no insert, `queue_bytes == 0`, and the same body sent
/// again is stored rather than suppressed, because the un-sealed guard was
/// dropped with the claim.
#[tokio::test]
async fn a_logs_push_at_a_ceiling_is_refused_whole() {
    // -- the row ceiling, both sides.
    //
    // Patterns off, so the row bound is exactly `lines + new_streams` and the
    // case can sit one row either side of the limit. A push at the limit would
    // otherwise need its extraction counted too.
    const ROW_LIMIT: u64 = 1_000;
    let cfg = WriterConfig {
        log_landing_max_rows: ROW_LIMIT,
        log_patterns: false,
        ingest_dedup: false,
        // Large enough that the byte ceiling never decides these two.
        batch_bytes: ByteSize(1 << 30),
        ingest_queue_bytes: ByteSize(1 << 30),
        ..Default::default()
    };

    for (name, lines, admitted) in [
        ("one under the limit", ROW_LIMIT - 1, true),
        ("at the limit", ROW_LIMIT, false),
    ] {
        let root = spool_root(&format!("rows-{}", lines));
        let inserter = MockInserter::always_unrecorded(Act::Ok);
        let writer = writer_with(&cfg, &root, inserter.clone());

        let batch = ParsedLogs {
            rows: (0..lines)
                .map(|i| log_row(1, &format!("line {i}")))
                .collect(),
            ..Default::default()
        };
        let outcome = writer.admit_flush(batch, PushHeaders::default());
        if admitted {
            outcome
                .expect("{name}: one row under the limit is admitted")
                .await
                .ok();
        } else {
            let err = outcome.expect_err("{name}: a push at the limit is refused");
            let AdmitRefusal::PushTooLarge {
                rows, row_limit, ..
            } = err
            else {
                panic!("{name}: expected PushTooLarge, got {err:?}");
            };
            assert_eq!(
                rows, ROW_LIMIT,
                "{name}: the bound the decision was made on"
            );
            assert_eq!(row_limit, ROW_LIMIT);
            assert_eq!(inserter.call_count(), 0, "{name}: nothing is stored");
            assert_eq!(
                writer.metrics().queue_bytes,
                0,
                "{name}: nothing is reserved"
            );
        }

        writer.shutdown(Duration::from_secs(2)).await;
        std::fs::remove_dir_all(&root).ok();
    }

    // -- the byte ceiling, both sides, against one push's own derived charge.
    let batch = two_lines_one_stream(1);
    let line_bytes: u64 = batch
        .rows
        .iter()
        .map(|r| LogLandingRow::est_landing_bytes(LogSampleRow::est_source_bytes(r)))
        .sum();
    let stream_bytes: u64 = batch
        .streams
        .iter()
        .map(|s| LogLandingRow::est_landing_bytes(LogStreamRow::est_source_bytes(s)))
        .sum();
    let charge = line_bytes + stream_bytes + landing_block_overhead_bytes::<LogLandingRow>();

    for (name, limit, admitted) in [
        ("at the charge", charge, true),
        ("one byte under the charge", charge - 1, false),
    ] {
        let cfg = WriterConfig {
            log_patterns: false,
            ingest_dedup: false,
            batch_bytes: ByteSize(limit),
            ingest_queue_bytes: ByteSize(1 << 30),
            ..Default::default()
        };
        let root = spool_root(&format!("bytes-{limit}"));
        let inserter = MockInserter::always(Act::Ok);
        let writer = writer_with(&cfg, &root, inserter.clone());

        let outcome = writer.admit_flush(two_lines_one_stream(1), PushHeaders::default());
        if admitted {
            outcome
                .unwrap_or_else(|e| {
                    panic!("{name}: a charge equal to the ceiling is admitted: {e:?}")
                })
                .await
                .ok();
            assert_eq!(inserter.call_count(), 1, "{name}");
        } else {
            let err = outcome.expect_err("a charge over the ceiling is refused");
            let AdmitRefusal::PushTooLarge {
                bytes, byte_limit, ..
            } = err
            else {
                panic!("{name}: expected PushTooLarge, got {err:?}");
            };
            assert_eq!(bytes, charge, "{name}: the charge the decision was made on");
            assert_eq!(byte_limit, limit);
            assert_eq!(inserter.call_count(), 0, "{name}: nothing is stored");
            assert_eq!(
                writer.metrics().queue_bytes,
                0,
                "{name}: nothing is reserved"
            );

            // The claim was never sealed, so the same body sent again is
            // stored rather than suppressed — a client's retry of an unstored
            // push must not be answered the refusal's outcome.
            let roomy = WriterConfig {
                log_patterns: false,
                batch_bytes: ByteSize(1 << 30),
                ingest_queue_bytes: ByteSize(1 << 30),
                ..Default::default()
            };
            let root2 = spool_root("bytes-retry");
            let inserter2 = MockInserter::always(Act::Ok);
            let writer2 = writer_with(&roomy, &root2, inserter2.clone());
            writer2
                .admit_flush(two_lines_one_stream(1), PushHeaders::default())
                .expect("the retry of an unstored push is admitted")
                .await
                .expect("and it commits");
            assert_eq!(inserter2.call_count(), 1);
            writer2.shutdown(Duration::from_secs(2)).await;
            std::fs::remove_dir_all(&root2).ok();
        }

        writer.shutdown(Duration::from_secs(2)).await;
        std::fs::remove_dir_all(&root).ok();
    }
}

/// **T13.** The queue byte allowance is aggregate across pushes: a second push
/// is refused `429` where the first fits and the sum does not, the refusal is
/// counted, and the reservation is rolled back.
#[tokio::test]
async fn the_queue_byte_allowance_is_aggregate_across_log_pushes() {
    let batch = two_lines_one_stream(1);
    let line_bytes: u64 = batch
        .rows
        .iter()
        .map(|r| LogLandingRow::est_landing_bytes(LogSampleRow::est_source_bytes(r)))
        .sum();
    let stream_bytes: u64 = batch
        .streams
        .iter()
        .map(|s| LogLandingRow::est_landing_bytes(LogStreamRow::est_source_bytes(s)))
        .sum();
    let charge = line_bytes + stream_bytes + landing_block_overhead_bytes::<LogLandingRow>();

    let cfg = WriterConfig {
        log_patterns: false,
        ingest_dedup: false,
        batch_bytes: ByteSize(1 << 30),
        // Room for one push and not two.
        ingest_queue_bytes: ByteSize(charge + charge / 2),
        ..Default::default()
    };
    let root = spool_root("aggregate-allowance");
    // Hanging, so the first push's charge stays live while the second is
    // admitted.
    let inserter = MockInserter::always(Act::Hang);
    let writer = writer_with(&cfg, &root, inserter.clone());

    let _first = writer
        .admit_flush(two_lines_one_stream(1), PushHeaders::default())
        .expect("the first push fits");
    settle_until(|| writer.metrics().queue_bytes == charge).await;
    assert_eq!(writer.metrics().queue_bytes, charge);

    let err = writer
        .admit_flush(two_lines_one_stream(2), PushHeaders::default())
        .expect_err("the sum does not fit");
    assert!(
        matches!(err, AdmitRefusal::Backpressure),
        "the second push is refused backpressure, got {err:?}"
    );
    let snap = writer.metrics();
    assert_eq!(snap.backpressure_total, 1, "the refusal is counted");
    assert_eq!(
        snap.queue_bytes, charge,
        "the refused push's reservation is rolled back, leaving the first's"
    );

    writer.shutdown(Duration::from_millis(10)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **T14.** The refusal's message carries the push's own bounded size and both
/// limits, so a client reading a `413` body can tell which ceiling it met.
#[tokio::test]
async fn the_push_too_large_message_names_the_size_and_both_limits() {
    let cfg = WriterConfig {
        log_landing_max_rows: 1_000,
        log_patterns: false,
        batch_bytes: ByteSize(1 << 30),
        ..Default::default()
    };
    let root = spool_root("message");
    let inserter = MockInserter::always_unrecorded(Act::Ok);
    let writer = writer_with(&cfg, &root, inserter);

    let batch = ParsedLogs {
        rows: (0..1_000)
            .map(|i| log_row(1, &format!("line {i}")))
            .collect(),
        ..Default::default()
    };
    let err = writer
        .admit_flush(batch, PushHeaders::default())
        .expect_err("a push at the row limit is refused");
    let AdmitRefusal::PushTooLarge {
        rows,
        row_limit,
        bytes,
        byte_limit,
    } = err
    else {
        panic!("expected PushTooLarge, got {err:?}");
    };
    let message = push_too_large_message(rows, row_limit, bytes, byte_limit);
    for needle in [
        rows.to_string(),
        row_limit.to_string(),
        bytes.to_string(),
        byte_limit.to_string(),
    ] {
        assert!(
            message.contains(&needle),
            "the message must name {needle}: {message}"
        );
    }

    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **T16.** A valid push with no rows and no streams is a success, makes no
/// block and no insert, and is charged for nothing — one block's fixed
/// overhead exceeds the smallest accepted value of either byte limit, so
/// charging an empty push for a block that is never created would refuse it.
#[tokio::test]
async fn an_empty_logs_push_is_a_success_and_is_charged_for_nothing() {
    // The case only bites while one block's own overhead exceeds the smallest
    // accepted limit.
    assert!(landing_block_overhead_bytes::<LogLandingRow>() > 1);
    for (name, cfg) in [
        (
            "one-byte per-push ceiling",
            WriterConfig {
                batch_bytes: ByteSize(1),
                ..Default::default()
            },
        ),
        (
            "one-byte queue allowance",
            WriterConfig {
                ingest_queue_bytes: ByteSize(1),
                ..Default::default()
            },
        ),
    ] {
        let root = spool_root("empty");
        let inserter = MockInserter::always(Act::Ok);
        let writer = writer_with(&cfg, &root, inserter.clone());

        writer
            .admit_flush(ParsedLogs::default(), PushHeaders::default())
            .unwrap_or_else(|e| panic!("{name}: an empty push is a success: {e:?}"))
            .await
            .unwrap_or_else(|e| panic!("{name}: and completes at admission: {e:?}"));

        assert_eq!(inserter.call_count(), 0, "{name}: no block, no insert");
        assert_eq!(
            writer.metrics().queue_bytes,
            0,
            "{name}: charged for nothing"
        );

        writer.shutdown(Duration::from_secs(2)).await;
        std::fs::remove_dir_all(&root).ok();
    }
}

/// **T17.** A suppressed push stores nothing and makes no block.
///
/// **Counting the calls, not the stored rows, is the observation point**: a
/// suppression branch that queued a block anyway would send an insert the
/// server then drops as a repeat, leaving every row count right and the call
/// count wrong.
#[tokio::test]
async fn a_suppressed_logs_push_stores_nothing_and_makes_no_block() {
    let root = spool_root("suppressed");
    let inserter = MockInserter::always(Act::Ok);
    let writer = writer_with(&WriterConfig::default(), &root, inserter.clone());

    writer
        .admit_flush(two_lines_one_stream(1), PushHeaders::default())
        .expect("the first push is admitted")
        .await
        .expect("and commits");
    assert_eq!(inserter.call_count(), 1);

    writer
        .admit_flush(two_lines_one_stream(1), PushHeaders::default())
        .expect("the repeat is answered")
        .await
        .expect("with the original push's outcome");
    assert_eq!(
        inserter.call_count(),
        1,
        "a suppressed push makes zero inserts of its own"
    );

    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

// -- registration -----------------------------------------------------

/// **T19.** The `StreamLru` is promoted only when the block commits: while the
/// insert is parked the key is absent, so a second push still lands its
/// registration row; after `Ok` it is present and the next push does not.
#[tokio::test]
async fn the_stream_lru_is_promoted_only_when_the_block_commits() {
    let cfg = WriterConfig {
        log_landing_inserters: 1,
        log_patterns: false,
        ingest_dedup: false,
        ..Default::default()
    };
    let root = spool_root("lru-commit");
    // One hanging call, then always Ok.
    let inserter = MockInserter::new(vec![Step::now(Act::Hang), Step::now(Act::Ok)]);
    let writer = writer_with(&cfg, &root, inserter.clone());

    let _parked = writer
        .admit_flush(two_lines_one_stream(1), PushHeaders::default())
        .expect("queue has room");
    settle_until(|| inserter.call_count() == 1).await;
    assert_eq!(inserter.call_count(), 1, "the first block is in flight");

    // The LRU cannot have been promoted: the block has not committed.
    assert_eq!(
        writer.metrics().lru_misses_total,
        1,
        "the first push's stream was a miss"
    );
    assert_eq!(
        writer.metrics().lru_hits_total,
        0,
        "and nothing has been promoted while the insert is parked"
    );

    writer.shutdown(Duration::from_millis(10)).await;
    std::fs::remove_dir_all(&root).ok();

    // A committing writer promotes, so the second push's stream is a hit.
    let root = spool_root("lru-commit-ok");
    let inserter = MockInserter::always(Act::Ok);
    let writer = writer_with(&cfg, &root, inserter.clone());
    writer
        .admit_flush(two_lines_one_stream(1), PushHeaders::default())
        .expect("queue has room")
        .await
        .expect("commits");
    writer
        .admit_flush(two_lines_one_stream(1), PushHeaders::default())
        .expect("queue has room")
        .await
        .expect("commits");
    assert_eq!(
        writer.metrics().lru_hits_total,
        1,
        "the committed block's registration was promoted"
    );
    let second = inserter.rows_of(1);
    assert!(
        rows_of_kind(&second, 1).is_empty(),
        "so the second push lands no registration row: {second:#?}"
    );

    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **T20.** A failed block leaves the `StreamLru` empty, so the next push
/// re-registers — the registration backfill's replacement. Break it by
/// promoting at admission and the second push lands no kind-1 row.
#[tokio::test(start_paused = true)]
async fn a_failed_log_block_leaves_the_stream_lru_empty_so_the_next_push_re_registers() {
    let cfg = WriterConfig {
        log_landing_retries: 0,
        log_patterns: false,
        ingest_dedup: false,
        ..Default::default()
    };
    let root = spool_root("failed-lru");
    let inserter = MockInserter::always(Act::Poison);
    let writer = writer_with(&cfg, &root, inserter.clone());

    let answer = tokio::time::timeout(
        Duration::from_secs(600),
        writer
            .admit_flush(two_lines_one_stream(1), PushHeaders::default())
            .expect("queue has room"),
    )
    .await
    .expect("the loop settles");
    answer.expect_err("the insert failed");

    let second = tokio::time::timeout(
        Duration::from_secs(600),
        writer
            .admit_flush(two_lines_one_stream(1), PushHeaders::default())
            .expect("queue has room"),
    )
    .await
    .expect("the loop settles");
    second.expect_err("the insert failed again");

    let rows = inserter.rows_of(1);
    assert_eq!(
        rows_of_kind(&rows, 1).len(),
        1,
        "a push whose block never committed emits its registration row again: \
         {rows:#?}"
    );

    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

// -- the token, the endings and the shutdown --------------------------

/// **T21.** A token is minted per sealed block and repeated byte-identically
/// on resend. Two halves, each reddening a different wrong body.
///
/// **First, with suppression off**, two byte-identical pushes carry two
/// DIFFERENT tokens. Suppression has to be off for this half to exist at all —
/// at the default the second push is suppressed and makes no block and no
/// token — and byte-identical bodies are the point: a token derived from the
/// block's content would give both the same token, the server would drop the
/// second, and a deployment with suppression off would lose a push.
///
/// **Second**, one push against an inserter that fails retryably once and then
/// succeeds: both attempts carried the IDENTICAL token. A loop that rebuilt
/// the block's settings between attempts reddens.
#[tokio::test(start_paused = true)]
async fn a_token_is_minted_per_sealed_log_block_and_repeated_on_resend() {
    fn token_of(settings: &[(String, String)]) -> String {
        settings
            .iter()
            .find(|(k, _)| k == "insert_deduplication_token")
            .map(|(_, v)| v.clone())
            .expect("every landing insert pins a deduplication token")
    }

    let cfg = WriterConfig {
        ingest_dedup: false,
        log_patterns: false,
        ..Default::default()
    };
    let root = spool_root("token-distinct");
    let inserter = MockInserter::always(Act::Ok);
    let writer = writer_with(&cfg, &root, inserter.clone());
    for _ in 0..2 {
        tokio::time::timeout(
            Duration::from_secs(600),
            writer
                .admit_flush(two_lines_one_stream(1), PushHeaders::default())
                .expect("queue has room"),
        )
        .await
        .expect("commits")
        .expect("commits");
    }
    assert_eq!(inserter.call_count(), 2);
    assert_ne!(
        token_of(&inserter.settings_of(0)),
        token_of(&inserter.settings_of(1)),
        "two byte-identical pushes are two genuine pushes and must not share \
         a token"
    );
    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();

    let retrying = WriterConfig {
        log_landing_retries: 2,
        log_patterns: false,
        ..Default::default()
    };
    let root = spool_root("token-resend");
    let inserter = MockInserter::new(vec![Step::now(Act::Retryable), Step::now(Act::Ok)]);
    let writer = writer_with(&retrying, &root, inserter.clone());
    tokio::time::timeout(
        Duration::from_secs(600),
        writer
            .admit_flush(two_lines_one_stream(1), PushHeaders::default())
            .expect("queue has room"),
    )
    .await
    .expect("settles")
    .expect("the resend commits");
    assert_eq!(inserter.call_count(), 2, "one resend");
    assert_eq!(
        token_of(&inserter.settings_of(0)),
        token_of(&inserter.settings_of(1)),
        "a resend of the same block carries the identical token, or the \
         server cannot recognise it as a repeat"
    );

    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **T22.** The endings of the insert loop, one run each: how many attempts,
/// which spool directory holds the record, what the waiting caller is told.
/// A second attempt, a wrong retry class, or a commit where a settlement
/// belongs changes one of them.
///
/// **T8 rides the same table**: after each ending `queue_bytes` is `0` **and**
/// a following push of the same size is admitted rather than refused `429`.
/// The gauge alone cannot tell a double subtract from a correct one — a gauge
/// that wrapped also reads low — so the second admission is the other half of
/// the observation.
#[tokio::test(start_paused = true)]
async fn a_logs_block_settles_at_each_ending_the_loop_names() {
    struct Run {
        name: &'static str,
        retries: u32,
        script: Vec<Step>,
        attempts: usize,
        uncertain: usize,
        poison: usize,
        ok: bool,
    }

    let runs = vec![
        Run {
            name: "a-no-retries-uncertain",
            retries: 0,
            script: vec![Step::now(Act::Uncertain)],
            attempts: 1,
            uncertain: 1,
            poison: 0,
            ok: false,
        },
        Run {
            name: "b-uncertain-every-time",
            retries: 2,
            script: vec![Step::now(Act::Uncertain)],
            attempts: 3,
            uncertain: 1,
            poison: 0,
            ok: false,
        },
        Run {
            name: "c-retryable-then-ok",
            retries: 2,
            script: vec![Step::now(Act::Retryable), Step::now(Act::Ok)],
            attempts: 2,
            uncertain: 0,
            poison: 0,
            ok: true,
        },
        Run {
            name: "d-non-retryable",
            retries: 2,
            script: vec![Step::now(Act::Poison)],
            attempts: 1,
            uncertain: 0,
            poison: 1,
            ok: false,
        },
        Run {
            name: "e-retryable-every-time",
            retries: 2,
            script: vec![Step::now(Act::Retryable)],
            attempts: 3,
            uncertain: 0,
            poison: 1,
            ok: false,
        },
        Run {
            name: "f-uncertain-then-ok",
            retries: 2,
            script: vec![Step::now(Act::Uncertain), Step::now(Act::Ok)],
            attempts: 2,
            uncertain: 0,
            poison: 0,
            ok: true,
        },
    ];

    for run in runs {
        let cfg = WriterConfig {
            log_landing_retries: run.retries,
            log_patterns: false,
            ingest_dedup: false,
            ..Default::default()
        };
        let root = spool_root(run.name);
        let inserter = MockInserter::new(run.script.clone());
        let writer = writer_with(&cfg, &root, inserter.clone());

        let wait = writer
            .admit_flush(two_lines_one_stream(1), PushHeaders::default())
            .expect("queue has room");
        let answer = tokio::time::timeout(Duration::from_secs(600), wait)
            .await
            .unwrap_or_else(|_| panic!("{}: the loop settles", run.name));

        assert_eq!(
            inserter.call_count(),
            run.attempts,
            "{}: attempts",
            run.name
        );
        assert_eq!(
            spool_records(&root, "uncertain").len(),
            run.uncertain,
            "{}: uncertain records",
            run.name
        );
        assert_eq!(
            spool_records(&root, "poison").len(),
            run.poison,
            "{}: poison records",
            run.name
        );
        assert_eq!(answer.is_ok(), run.ok, "{}: the caller's answer", run.name);
        let snap = writer.metrics();
        assert_eq!(
            snap.spool_uncertain_total, run.uncertain as u64,
            "{}",
            run.name
        );
        assert_eq!(snap.spool_poison_total, run.poison as u64, "{}", run.name);

        // T8: the charge is released exactly once at every ending. The gauge
        // reads zero, AND the allowance is genuinely back — a double subtract
        // wraps the gauge low, which reading it alone cannot distinguish from
        // a correct release.
        assert_eq!(
            snap.queue_bytes, 0,
            "{}: every ending releases the push's bytes",
            run.name
        );
        let again = writer.admit_flush(two_lines_one_stream(2), PushHeaders::default());
        assert!(
            again.is_ok(),
            "{}: a following push of the same size must be admitted, not \
             refused 429 — the allowance came back",
            run.name
        );
        let _ = tokio::time::timeout(
            Duration::from_secs(600),
            again.expect("admitted just above"),
        )
        .await;

        writer.shutdown(Duration::from_secs(2)).await;
        std::fs::remove_dir_all(&root).ok();
    }
}

/// **T23.** A block's fate never walks back from uncertain. An inserter
/// returning `InsertUncertain` and then a retryable pre-send error must still
/// file `uncertain/` with nothing under `poison/`, and tell the caller unknown
/// — a loop that classified from the LAST outcome would file as
/// provably-not-stored a block that may have committed.
#[tokio::test(start_paused = true)]
async fn a_logs_blocks_fate_never_walks_back_from_uncertain() {
    let cfg = WriterConfig {
        log_landing_retries: 2,
        log_patterns: false,
        ingest_dedup: false,
        ..Default::default()
    };
    let root = spool_root("fate-uncertain");
    let inserter = MockInserter::new(vec![Step::now(Act::Uncertain), Step::now(Act::Retryable)]);
    let writer = writer_with(&cfg, &root, inserter.clone());

    let wait = writer
        .admit_flush(two_lines_one_stream(1), PushHeaders::default())
        .expect("queue has room");
    let err = tokio::time::timeout(Duration::from_secs(600), wait)
        .await
        .expect("settles")
        .expect_err("never a success")
        .to_string();

    assert_eq!(
        inserter.call_count(),
        3,
        "two resends after the first attempt"
    );
    assert_eq!(
        spool_records(&root, "uncertain").len(),
        1,
        "the block is filed uncertain, because an attempt may have committed"
    );
    assert!(
        spool_records(&root, "poison").is_empty(),
        "nothing may claim this block was provably not stored"
    );
    assert!(
        err.contains("commit fate is unknown"),
        "the caller is told unknown, never not-stored: {err}"
    );

    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// The fixture **T24 and T25 share**, and constructing it is half of each
/// case. One inserter and a hanging call: admit the first push and wait until
/// the inserter reports it has been entered, so that block is provably in
/// flight; only then admit the second, which the single worker cannot take, so
/// it is provably still in the queue.
///
/// Admitting "several pushes" instead guarantees neither state — with the
/// queue possibly empty at the deadline, a drain that mishandles only queued
/// blocks passes.
async fn two_blocks_one_inflight_one_queued(
    root: &Path,
) -> (
    Arc<LogWriter>,
    Arc<MockInserter>,
    pulsus_write::FlushWait,
    pulsus_write::FlushWait,
) {
    let cfg = WriterConfig {
        log_landing_retries: 0,
        log_landing_inserters: 1,
        log_patterns: false,
        ingest_dedup: false,
        ..Default::default()
    };
    let inserter = MockInserter::always(Act::Hang);
    let writer = Arc::new(writer_with(&cfg, root, inserter.clone()));

    let a = writer
        .admit_flush(two_lines_one_stream(1), PushHeaders::default())
        .expect("queue has room");
    inserter
        .entered
        .acquire()
        .await
        .expect("the entered semaphore is never closed")
        .forget();
    assert_eq!(inserter.call_count(), 1, "A is provably in flight");
    let b = writer
        .admit_flush(two_lines_one_stream(2), PushHeaders::default())
        .expect("queue has room");
    (writer, inserter, a, b)
}

/// **T24.** The drain accounts for every push it admitted: each caller's wait
/// resolves rather than hanging, and `queue_bytes` is `0` once `shutdown`
/// returns. A drain that returned before settling the queued block leaves that
/// waiter unresolved and its charge never comes back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_drain_accounts_for_every_log_push_it_admitted() {
    let root = spool_root("drain-accounts");
    let (writer, _inserter, a, b) = two_blocks_one_inflight_one_queued(&root).await;

    let shutdown = {
        let writer = writer.clone();
        tokio::spawn(async move { writer.shutdown(Duration::from_millis(10)).await })
    };
    for (name, wait) in [("A", a), ("B", b)] {
        tokio::time::timeout(Duration::from_secs(30), wait)
            .await
            .unwrap_or_else(|_| panic!("{name}'s wait never resolved"))
            .expect_err("both are answered the shutdown error");
    }
    shutdown.await.expect("shutdown completes");

    assert_eq!(
        writer.metrics().queue_bytes,
        0,
        "every admitted push's charge came back"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// **T25.** Shutdown files an in-flight block and a queued block differently:
/// the in-flight one leaves one record under `uncertain/` and reports
/// uncertain, the queued one leaves one under `poison/` and reports
/// not-committed, and neither directory holds the other's record.
///
/// Both callers are told `ShuttingDown`, which is why the directory rather
/// than the error is what separates them. Filing the in-flight one as poison
/// would claim a block that may have committed was provably not stored — the
/// one direction the fate rule forbids.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_files_an_inflight_log_block_and_a_queued_block_differently() {
    let root = spool_root("shutdown-two");
    let (writer, inserter, a, b) = two_blocks_one_inflight_one_queued(&root).await;

    let shutdown = {
        let writer = writer.clone();
        tokio::spawn(async move { writer.shutdown(Duration::from_millis(10)).await })
    };
    let a_answer = tokio::time::timeout(Duration::from_secs(30), a)
        .await
        .expect("A's wait resolves");
    let b_answer = tokio::time::timeout(Duration::from_secs(30), b)
        .await
        .expect("B's wait resolves");
    shutdown.await.expect("shutdown completes");

    assert_eq!(inserter.call_count(), 1, "B is never sent");
    let uncertain = spool_records(&root, "uncertain");
    let poison = spool_records(&root, "poison");
    assert_eq!(uncertain.len(), 1, "the in-flight block may have committed");
    assert_eq!(
        uncertain[0]["error"].as_str(),
        Some("the writer shut down with an attempt in flight")
    );
    assert_eq!(poison.len(), 1, "the queued block provably did not");
    assert_eq!(
        poison[0]["error"].as_str(),
        Some("the writer shut down before the block was sent")
    );
    for (name, answer) in [("A", a_answer), ("B", b_answer)] {
        let err = answer.expect_err("never a success").to_string();
        assert!(
            err.contains("shutting down"),
            "{name} is answered the shutdown error: {err}"
        );
    }
    assert_eq!(writer.metrics().queue_bytes, 0);
    std::fs::remove_dir_all(&root).ok();
}
