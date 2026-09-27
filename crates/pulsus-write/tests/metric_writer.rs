//! `MetricWriter` tests: one push is one insert of one block into
//! `metric_landing` (issue #603), the block's contents and its per-kind
//! columns, the two per-push ceilings, the insert loop's endings and the
//! wall-clock budget that bounds it, and the activity-bucket registration
//! LRU. All against a mock `BlockInserter` — no real ClickHouse (see
//! `tests/live_metric_writer.rs` for the `PULSUS_TEST_CLICKHOUSE=1`-gated
//! live counterparts).
//!
//! **Nothing here reads a target table.** The four derived metric tables are
//! maintained by materialized view; what this file pins is the block the
//! writer hands over.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pulsus_clickhouse::{ChError, ChRow, QuerySettings};
use pulsus_config::{ByteSize, Config, WriterConfig};
use pulsus_model::{DEFAULT_ACTIVITY_BUCKET_MS, Fingerprint, LabelSet, NativeHistogram, Span};
use pulsus_write::writer::{BlockInserter, MetricWriter, MetricWriterTables, WriterRuntime};
use pulsus_write::{
    AdmitRefusal, HistogramPoint, MetricMetadata, MetricPoint, MetricSink, ParsedMetrics,
    PushHeaders, SeriesRef, push_too_large_message,
};
use tokio::time::Instant;

const BUCKET_MS: i64 = DEFAULT_ACTIVITY_BUCKET_MS;
const LANDING: &str = "metric_landing";

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
    /// Parks until the test releases one permit, then returns `Ok`.
    Gate,
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

    fn after(delay: Duration, act: Act) -> Self {
        Step { delay, act }
    }
}

/// A scriptable mock [`BlockInserter`]: the script is consumed front to back
/// and its **last step repeats** for every later call, so a one-step script
/// is a constant behaviour. Records every call's table, rows and settings.
struct MockInserter {
    script: Vec<Step>,
    calls: AtomicUsize,
    rows: Mutex<Vec<Vec<serde_json::Value>>>,
    settings: Mutex<Vec<Vec<(String, String)>>>,
    tables: Mutex<Vec<String>>,
    gate: Arc<tokio::sync::Semaphore>,
    /// An owned empty settings set, so [`BlockInserter::insert`] can
    /// delegate to `insert_with` without handing it a temporary.
    empty: QuerySettings,
}

impl MockInserter {
    fn new(script: Vec<Step>) -> Arc<Self> {
        Arc::new(MockInserter {
            script,
            calls: AtomicUsize::new(0),
            rows: Mutex::new(Vec::new()),
            settings: Mutex::new(Vec::new()),
            tables: Mutex::new(Vec::new()),
            gate: Arc::new(tokio::sync::Semaphore::new(0)),
            empty: QuerySettings::new(),
        })
    }

    fn always(act: Act) -> Arc<Self> {
        Self::new(vec![Step::now(act)])
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

    fn release_one(&self) {
        self.gate.add_permits(1);
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
        let step = *self
            .script
            .get(call)
            .or_else(|| self.script.last())
            .expect("a script has at least one step");
        let gate = self.gate.clone();
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
                Act::Gate => {
                    gate.acquire()
                        .await
                        .expect("the gate semaphore is never closed")
                        .forget();
                    Ok(())
                }
            }
        })
    }
}

// -- writer construction ----------------------------------------------

/// A spool root of this test's own, so a spool assertion sees only this
/// test's files and nothing lands in the process working directory.
fn spool_root(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pulsus-metric-writer-{name}-{}-{}",
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

fn writer_at(runtime: WriterRuntime, inserter: Arc<MockInserter>) -> MetricWriter {
    MetricWriter::with_landing_inserter_and_runtime(
        inserter,
        runtime,
        BUCKET_MS,
        MetricWriterTables::metrics_default(),
    )
}

/// A writer over `cfg` whose spool is `spool`.
fn writer_with(cfg: &WriterConfig, spool: &Path, inserter: Arc<MockInserter>) -> MetricWriter {
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

/// Waits until `done` holds, up to a bounded number of turns, so a test can
/// let the insert workers make progress.
///
/// It yields rather than sleeping: every case that calls it runs on a paused
/// or a single-threaded clock, where a sleep would either advance the paused
/// clock — changing what the case measures — or wait in real time for a
/// worker that only runs when this task yields.
async fn settle_until(mut done: impl FnMut() -> bool) {
    for _ in 0..1024 {
        if done() {
            return;
        }
        tokio::task::yield_now().await;
    }
}

// -- fixtures ---------------------------------------------------------

/// A series whose only wire label was `__name__`, which the parsers remove,
/// so its label set is empty and its canonical JSON is `{}`.
fn bare_series(metric_name: &str, fingerprint: u128) -> SeriesRef {
    let (labels, _) = LabelSet::from_normalized(Vec::<(String, String)>::new());
    SeriesRef {
        metric_name: Arc::from(metric_name),
        fingerprint: Fingerprint::from_raw(fingerprint),
        labels,
    }
}

fn series_ref(metric_name: &str, fingerprint: u128) -> SeriesRef {
    let (labels, _) = LabelSet::from_normalized([("job".to_string(), "checkout".to_string())]);
    SeriesRef {
        metric_name: Arc::from(metric_name),
        fingerprint: Fingerprint::from_raw(fingerprint),
        labels,
    }
}

/// One float sample plus, if `new_series` is set, its `SeriesRef` — a real
/// request's first point for a series the writer has never registered.
fn batch_for(
    metric_name: &str,
    fingerprint: u128,
    unix_milli: i64,
    new_series: bool,
) -> ParsedMetrics {
    let mut out = ParsedMetrics {
        samples: vec![MetricPoint {
            metric_name: Arc::from(metric_name),
            fingerprint: Fingerprint::from_raw(fingerprint),
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

/// A one-span, one-bucket histogram with no custom bounds: the smallest
/// valid native histogram, and the one the byte figures below are computed
/// from.
fn one_bucket_hist(sum: f64) -> NativeHistogram {
    NativeHistogram {
        counter_reset_hint: pulsus_model::CounterResetHint::Unknown,
        schema: 0,
        zero_threshold: 0.0,
        zero_count: 0,
        count: 1,
        sum,
        positive_spans: vec![Span {
            offset: 1,
            length: 1,
        }],
        negative_spans: vec![],
        positive_buckets: vec![1],
        negative_buckets: vec![],
        custom_values: vec![],
    }
}

/// **Five landing rows and 178 estimated bytes.** The mixed push every byte
/// figure in this file is derived from: 1 float sample and 1 one-bucket
/// histogram sample on one unregistered series `m` whose label set is empty,
/// both in one activity bucket, plus a `gauge` descriptor with empty help and
/// unit. Each landing row is estimated by the target row it becomes:
///
/// | kind | rows | bytes each |
/// |---|---|---|
/// | 0 | 1 | 33 = 1 name + 16 fingerprint + 8 unix_milli + 8 value |
/// | 1 | 1 | 75 = 1 + 16 + 8 + 1 schema + 8 zero_threshold + 8 zero_count + 8 count + 8 sum + 1 hint + 1×(4+4) span + 1×8 delta |
/// | 2 | 2 | 28 = 1 + 2 labels (`{}`) + 16 + 8 + 1 value_type |
/// | 3 | 1 | 14 = 1 name + 5 `gauge` + 0 help + 0 unit + 8 updated_ns |
///
/// 33 + 75 + 28 + 28 + 14 = 178.
const MIXED_PUSH_ROWS: u64 = 5;
const MIXED_PUSH_BYTES: u64 = 178;

fn mixed_push(unix_milli: i64, updated_ns: i64, with_descriptor: bool) -> ParsedMetrics {
    let mut out = ParsedMetrics {
        samples: vec![MetricPoint {
            metric_name: Arc::from("m"),
            fingerprint: Fingerprint::from_raw(1),
            unix_milli,
            value: 1.0,
        }],
        hist_samples: vec![HistogramPoint {
            metric_name: Arc::from("m"),
            fingerprint: Fingerprint::from_raw(1),
            unix_milli,
            histogram: one_bucket_hist(5.0),
        }],
        series: vec![bare_series("m", 1)],
        ..Default::default()
    };
    if with_descriptor {
        out.metadata.push(MetricMetadata {
            metric_name: Arc::from("m"),
            metric_type: "gauge".to_string(),
            help: String::new(),
            unit: String::new(),
            updated_ns,
        });
    }
    out
}

fn kinds(rows: &[serde_json::Value]) -> Vec<u64> {
    let mut out: Vec<u64> = rows
        .iter()
        .map(|r| r["kind"].as_u64().expect("every row carries a kind"))
        .collect();
    out.sort_unstable();
    out
}

fn rows_of_kind(rows: &[serde_json::Value], kind: u64) -> Vec<&serde_json::Value> {
    rows.iter()
        .filter(|r| r["kind"].as_u64() == Some(kind))
        .collect()
}

fn token_of(settings: &[(String, String)]) -> String {
    settings
        .iter()
        .find(|(k, _)| k == "insert_deduplication_token")
        .map(|(_, v)| v.clone())
        .unwrap_or_default()
}

fn epoch_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

// -- the cross-crate bucket-floor identity ----------------------------

/// The default `metric_series` activity bucket
/// (`pulsus_config::ReaderConfig::series_activity_bucket`) must resolve to
/// exactly `pulsus_model::DEFAULT_ACTIVITY_BUCKET_MS`, and the writer's
/// admission-time flooring must be the same function the reader renders into
/// its historical-bound SQL — proven by construction (`MetricWriter` only
/// ever calls `floor_to_activity_bucket`), not by convention.
#[test]
fn default_series_activity_bucket_matches_the_shared_floor_constant() {
    let cfg = Config::default();
    assert_eq!(
        cfg.reader.series_activity_bucket.0.as_millis() as i64,
        DEFAULT_ACTIVITY_BUCKET_MS
    );
}

// -- one push, one block ----------------------------------------------

/// A push of 2 float samples and 1 histogram sample on one unregistered
/// series in one activity bucket, plus 1 new descriptor, is **exactly one**
/// insert of 6 rows into `metric_landing`: 2 kind-0, 1 kind-1, 2 kind-2 (the
/// series registers at both value types) and 1 kind-3, every row carrying one
/// `received_ms` taken inside the push.
///
/// Then an empty push makes no block and no insert at all and is answered a
/// success.
#[tokio::test]
async fn one_push_is_one_insert_into_the_landing_table() {
    let cfg = WriterConfig::default();
    let root = spool_root("one-push");
    let inserter = MockInserter::always(Act::Ok);
    let writer = writer_with(&cfg, &root, inserter.clone());

    let batch = ParsedMetrics {
        samples: vec![
            MetricPoint {
                metric_name: Arc::from("m"),
                fingerprint: Fingerprint::from_raw(1),
                unix_milli: 1_000,
                value: 1.0,
            },
            MetricPoint {
                metric_name: Arc::from("m"),
                fingerprint: Fingerprint::from_raw(1),
                unix_milli: 1_001,
                value: 2.0,
            },
        ],
        hist_samples: vec![HistogramPoint {
            metric_name: Arc::from("m"),
            fingerprint: Fingerprint::from_raw(1),
            unix_milli: 1_002,
            histogram: one_bucket_hist(5.0),
        }],
        series: vec![series_ref("m", 1)],
        metadata: vec![MetricMetadata {
            metric_name: Arc::from("m"),
            metric_type: "counter".to_string(),
            help: "h".to_string(),
            unit: "s".to_string(),
            updated_ns: 7,
        }],
        ..Default::default()
    };

    let t0 = epoch_millis();
    let wait = writer
        .admit_flush(batch, PushHeaders::default())
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("the landing insert settles")
        .expect("it commits");
    let t1 = epoch_millis();

    assert_eq!(inserter.call_count(), 1, "one push is one insert");
    assert_eq!(inserter.tables(), vec![LANDING.to_string()]);
    let rows = inserter.rows_of(0);
    assert_eq!(rows.len(), 6, "2 kind-0 + 1 kind-1 + 2 kind-2 + 1 kind-3");
    assert_eq!(kinds(&rows), vec![0, 0, 1, 2, 2, 3]);
    let stamps: Vec<i64> = rows
        .iter()
        .map(|r| {
            r["received_ms"]
                .as_i64()
                .expect("an epoch-millisecond stamp")
        })
        .collect();
    assert!(
        stamps.windows(2).all(|w| w[0] == w[1]),
        "every row of a push carries the SAME stamp, so the block lies in one \
         partition: {stamps:?}"
    );
    assert!(
        stamps[0] >= t0 && stamps[0] <= t1,
        "the stamp is epoch milliseconds taken inside the push: {} not in [{t0}, {t1}]",
        stamps[0]
    );

    // A valid push with no rows of any kind: no block, no insert, a success.
    let before = writer.metrics().dedup.rollbacks_total;
    let wait = writer
        .admit_flush(ParsedMetrics::default(), PushHeaders::default())
        .expect("an empty push is admitted");
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("it settles at admission")
        .expect("an empty push is a success");
    assert_eq!(inserter.call_count(), 1, "an empty push inserts nothing");
    assert_eq!(writer.metrics().dedup.rollbacks_total, before);
    assert_eq!(writer.metrics().queue_bytes, 0);

    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// An insert never carries two pushes: two pushes differing in `unix_milli`
/// give two calls, each carrying its own push's rows and its own minted
/// token.
#[tokio::test]
async fn two_pushes_are_two_inserts() {
    let cfg = WriterConfig {
        metrics_landing_inserters: 2,
        ..Default::default()
    };
    let root = spool_root("two-pushes");
    let inserter = MockInserter::always(Act::Ok);
    let writer = writer_with(&cfg, &root, inserter.clone());

    for unix_milli in [1_000, 1_001] {
        let wait = writer
            .admit_flush(batch_for("m", 1, unix_milli, true), PushHeaders::default())
            .expect("queue has room");
        tokio::time::timeout(Duration::from_secs(5), wait)
            .await
            .expect("settles")
            .expect("commits");
    }

    assert_eq!(inserter.call_count(), 2);
    for (call, unix_milli) in [(0usize, 1_000i64), (1, 1_001)] {
        let rows = inserter.rows_of(call);
        let samples = rows_of_kind(&rows, 0);
        assert_eq!(samples.len(), 1, "one push's rows only, call {call}");
        assert_eq!(samples[0]["unix_milli"].as_i64(), Some(unix_milli));
    }
    assert_ne!(
        token_of(&inserter.settings_of(0)),
        token_of(&inserter.settings_of(1)),
        "two sealed blocks never share a deduplication token: the second would be \
         dropped as a resend of the first"
    );

    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// The token is minted per sealed block and repeated byte-identically on
/// every resend, with the rows and the whole settings set identical too —
/// that is what makes a resend of a block the server may already have
/// accepted store nothing twice.
#[tokio::test]
async fn a_token_is_minted_per_sealed_block_and_repeated_on_resend() {
    let cfg = WriterConfig {
        metrics_landing_retries: 2,
        ..Default::default()
    };
    let root = spool_root("token-resend");
    let inserter = MockInserter::always(Act::Uncertain);
    let writer = writer_with(&cfg, &root, inserter.clone());

    let wait = writer
        .admit_flush(batch_for("m", 1, 1_000, true), PushHeaders::default())
        .expect("queue has room");
    let answer = tokio::time::timeout(Duration::from_secs(30), wait)
        .await
        .expect("settles");
    assert!(answer.is_err(), "an uncertain block never answers success");

    assert_eq!(inserter.call_count(), 3, "one attempt plus two resends");
    let first = inserter.settings_of(0);
    for call in 1..3 {
        assert_eq!(
            inserter.settings_of(call),
            first,
            "every resend carries the identical settings, token included"
        );
        assert_eq!(
            inserter.rows_of(call),
            inserter.rows_of(0),
            "every resend carries the identical rows"
        );
    }
    assert!(
        !token_of(&first).is_empty(),
        "a landing insert carries a deduplication token: {first:?}"
    );
    assert_eq!(
        first
            .iter()
            .find(|(k, _)| k == "max_insert_block_size")
            .map(|(_, v)| v.as_str()),
        Some("1048576"),
        "the block-size ceiling admission refused against is pinned on the insert: {first:?}"
    );

    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// A fate classified from the **last** outcome rather than carried would file
/// both of these as provably-not-committed although an earlier attempt may
/// have committed. Each run's first attempt is uncertain, so both must end in
/// `uncertain/` whatever came after.
#[tokio::test(start_paused = true)]
async fn the_fate_never_walks_back_from_uncertain() {
    for (name, script, attempts) in [
        (
            "then-poison",
            vec![Step::now(Act::Uncertain), Step::now(Act::Poison)],
            2usize,
        ),
        (
            "then-retryable",
            vec![Step::now(Act::Uncertain), Step::now(Act::Retryable)],
            3,
        ),
    ] {
        let cfg = WriterConfig {
            metrics_landing_retries: 2,
            ..Default::default()
        };
        let root = spool_root(name);
        let inserter = MockInserter::new(script);
        let writer = writer_with(&cfg, &root, inserter.clone());

        let wait = writer
            .admit_flush(batch_for("m", 1, 1_000, true), PushHeaders::default())
            .expect("queue has room");
        let answer = tokio::time::timeout(Duration::from_secs(600), wait)
            .await
            .expect("settles");
        let err = answer.expect_err("never a success").to_string();

        assert_eq!(inserter.call_count(), attempts, "{name}: attempts");
        assert_eq!(
            spool_records(&root, "uncertain").len(),
            1,
            "{name}: the block is filed uncertain, because an attempt may have committed"
        );
        assert!(
            spool_records(&root, "poison").is_empty(),
            "{name}: nothing may claim this block was provably not stored"
        );
        assert!(
            err.contains("commit fate is unknown"),
            "{name}: the caller is told unknown, never not-stored: {err}"
        );

        writer.shutdown(Duration::from_secs(2)).await;
        std::fs::remove_dir_all(&root).ok();
    }
}

/// The endings of the insert loop, one run each: how many attempts, which
/// spool directory, what the caller is told, and whether the series LRU was
/// promoted. A second loop or a wrong retry class changes one of them.
#[tokio::test(start_paused = true)]
async fn the_insert_loop_endings() {
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
            name: "j-uncertain-then-ok",
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
            metrics_landing_retries: run.retries,
            ..Default::default()
        };
        let root = spool_root(run.name);
        let inserter = MockInserter::new(run.script.clone());
        let writer = writer_with(&cfg, &root, inserter.clone());

        let wait = writer
            .admit_flush(batch_for("m", 1, 1_000, true), PushHeaders::default())
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
        assert_eq!(
            snap.queue_bytes, 0,
            "{}: every ending releases the push's bytes",
            run.name
        );

        // The series LRU is promoted only by a commit, so an uncommitted
        // block's series key emits its kind-2 row again next push.
        let before = inserter.call_count();
        let wait = writer
            .admit_flush(batch_for("m", 1, 1_500, true), PushHeaders::default())
            .expect("queue has room");
        let _ = tokio::time::timeout(Duration::from_secs(600), wait)
            .await
            .unwrap_or_else(|_| panic!("{}: the second push settles", run.name));
        let registrations = rows_of_kind(&inserter.rows_of(before), 2).len();
        let expected = usize::from(!run.ok);
        assert_eq!(
            registrations, expected,
            "{}: the LRU is promoted iff the block committed",
            run.name
        );

        writer.shutdown(Duration::from_secs(2)).await;
        std::fs::remove_dir_all(&root).ok();
    }
}

/// The landing budget bounds the whole loop — the queue wait, every attempt
/// and every sleep — and it is measured from the push's admission.
///
/// A budget measured per attempt fails this: the fourth attempt would get a
/// full 30 s, a fifth would start, and the answer would come after 120 s.
#[tokio::test(start_paused = true)]
async fn the_landing_budget_bounds_the_loop() {
    let cfg = WriterConfig {
        metrics_landing_retries: 10,
        ..Default::default()
    };
    let root = spool_root("budget-bounds");
    // A retryable pre-send error returned 30 s into every attempt: three
    // attempts and their sleeps reach 90 s plus at most 0.7 s of jitter (the
    // shipped backoff caps the first three resends at 100, 200 and 400 ms),
    // so the fourth attempt's own timeout is what ends the block.
    let inserter = MockInserter::new(vec![Step::after(Duration::from_secs(30), Act::Retryable)]);
    let writer = writer_with(&cfg, &root, inserter.clone());

    let started = Instant::now();
    let wait = writer
        .admit_flush(batch_for("m", 1, 1_000, true), PushHeaders::default())
        .expect("queue has room");
    let answer = wait.await;
    let elapsed = started.elapsed();

    assert_eq!(
        inserter.call_count(),
        4,
        "exactly four attempts fit inside the budget"
    );
    assert_eq!(
        elapsed,
        Duration::from_secs(120),
        "the waiter resolves at the budget, not after it"
    );
    assert_eq!(spool_records(&root, "uncertain").len(), 1);
    assert!(spool_records(&root, "poison").is_empty());
    let err = answer.expect_err("never a success").to_string();
    assert!(err.contains("commit fate is unknown"), "{err}");

    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// A retryable failure arriving just before expiry must not sleep past the
/// budget: the remainder is recomputed **after** the attempt, so the sleep is
/// the lesser of the backoff and what is left.
///
/// The budget is 1 s, the attempt returns a retryable failure 990 ms in, and
/// the backoff is drawn from `[0, 10 s]` — the shipped full-jitter policy, so
/// the draw itself is not a figure a test can fix. **What is fixed is where
/// the block settles: exactly at the budget, for every draw.** With the
/// remainder recomputed, either the sleep is capped at the 10 ms left and the
/// check at the top of the loop settles the block at 1.000 s, or the sleep is
/// shorter and the next attempt's own timeout is the remainder and ends it at
/// 1.000 s. A loop that sleeps its backoff instead settles at 990 ms plus the
/// draw, which is 1.000 s for one value out of 10,001.
#[tokio::test(start_paused = true)]
async fn a_retry_sleep_never_carries_a_block_past_the_budget() {
    let cfg = WriterConfig {
        metrics_landing_retries: 5,
        ..Default::default()
    };
    let root = spool_root("retry-sleep");
    let mut runtime = runtime_at(&cfg, &root);
    runtime.landing_budget = Duration::from_secs(1);
    runtime.retry_base_delay = Duration::from_secs(10);
    runtime.retry_max_delay = Duration::from_secs(10);
    let inserter = MockInserter::new(vec![Step::after(
        Duration::from_millis(990),
        Act::Retryable,
    )]);
    let writer = writer_at(runtime, inserter.clone());

    let started = Instant::now();
    let wait = writer
        .admit_flush(batch_for("m", 1, 1_000, true), PushHeaders::default())
        .expect("queue has room");
    let answer = wait.await;
    let elapsed = started.elapsed();

    assert_eq!(
        elapsed,
        Duration::from_secs(1),
        "the block settles AT the budget, whatever the jitter draw was"
    );
    assert!(
        inserter.call_count() <= 2,
        "a 1 s budget admits at most two 990 ms attempts, got {}",
        inserter.call_count()
    );
    assert_eq!(
        spool_records(&root, "poison").len() + spool_records(&root, "uncertain").len(),
        1,
        "one block, one spool record"
    );
    assert!(answer.is_err());

    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// The budget expiring inside an attempt ends the block without a further
/// attempt, and reports the fate as unknown: the timeout encloses the
/// connection checkout and the health ping as well as the send, so it cannot
/// tell which of the three it interrupted, and over-reporting is the
/// direction a claim has to err in.
#[tokio::test(start_paused = true)]
async fn the_budget_expiring_inside_an_attempt_reports_an_unknown_fate() {
    let cfg = WriterConfig {
        metrics_landing_retries: 9,
        ..Default::default()
    };
    let root = spool_root("budget-in-attempt");
    let inserter = MockInserter::new(vec![Step::now(Act::Uncertain), Step::now(Act::Hang)]);
    let writer = writer_with(&cfg, &root, inserter.clone());

    let wait = writer
        .admit_flush(batch_for("m", 1, 1_000, true), PushHeaders::default())
        .expect("queue has room");
    let answer = wait.await;

    assert_eq!(
        inserter.call_count(),
        2,
        "no attempt starts once the budget is spent"
    );
    assert_eq!(spool_records(&root, "uncertain").len(), 1);
    assert!(spool_records(&root, "poison").is_empty());
    assert!(answer.is_err());

    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// The two shutdown endings are separate fates: a block abandoned mid-attempt
/// may have committed and is filed `uncertain/`; a block that was never sent
/// is filed `poison/`. Both callers are told the writer is shutting down.
///
/// Spooling nothing at the shutdown deadline — the shipped flush task's
/// behaviour — would lose the push's only copy.
#[tokio::test(start_paused = true)]
async fn shutdown_files_an_inflight_block_and_a_queued_block_differently() {
    let cfg = WriterConfig {
        metrics_landing_retries: 0,
        metrics_landing_inserters: 1,
        ..Default::default()
    };
    let root = spool_root("shutdown-two");
    let inserter = MockInserter::always(Act::Hang);
    let writer = Arc::new(writer_with(&cfg, &root, inserter.clone()));

    let a = writer
        .admit_flush(batch_for("a", 1, 1_000, true), PushHeaders::default())
        .expect("queue has room");
    settle_until(|| inserter.call_count() == 1).await;
    assert_eq!(inserter.call_count(), 1, "A is in flight");
    let b = writer
        .admit_flush(batch_for("b", 2, 1_000, true), PushHeaders::default())
        .expect("queue has room");

    let shutdown = {
        let writer = writer.clone();
        tokio::spawn(async move { writer.shutdown(Duration::from_millis(10)).await })
    };
    let a_answer = a.await;
    let b_answer = b.await;
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

/// A push that passed the shutting-down check and then found the queue closed
/// is settled by the admitting task, through the same ending a still-queued
/// block gets: its bytes are released, its claim is reported
/// provably-not-committed, the block is spooled, and its caller is answered
/// the shutdown error rather than left waiting for a worker that has gone.
#[tokio::test]
async fn a_block_sent_after_the_queue_closed_is_settled_by_the_admitting_task() {
    let cfg = WriterConfig {
        metrics_landing_inserters: 1,
        ..Default::default()
    };
    let root = spool_root("send-after-close");
    let inserter = MockInserter::always(Act::Ok);
    let writer = writer_with(&cfg, &root, inserter.clone());

    // Drive the drain to completion: every worker has closed the queue and
    // returned, so nothing can take a block off it any more. Clearing the
    // flag is what puts admission back past its own refusal, which is the
    // only way into this race from outside.
    writer.shutdown(Duration::from_millis(10)).await;
    writer.reopen_admission_for_test();

    let wait = writer
        .admit_flush(batch_for("m", 1, 1_000, true), PushHeaders::default())
        .expect("admission accepts: the flag is clear");
    let answer = tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("the admitting task settles it rather than leaving the caller waiting");
    let err = answer.expect_err("never a success").to_string();
    assert!(err.contains("shutting down"), "{err}");
    assert_eq!(inserter.call_count(), 0, "nothing was ever sent");
    let poison = spool_records(&root, "poison");
    assert_eq!(poison.len(), 1);
    assert_eq!(
        poison[0]["error"].as_str(),
        Some("the writer shut down before the block was sent")
    );
    assert_eq!(writer.metrics().queue_bytes, 0, "the bytes were released");
    assert_eq!(writer.metrics().spool_poison_total, 1);

    // The claim was reported provably-not-committed, so it was released: the
    // identical body sent again is admitted rather than suppressed.
    let wait = writer
        .admit_flush(batch_for("m", 1, 1_000, true), PushHeaders::default())
        .expect("queue has room");
    let _ = tokio::time::timeout(Duration::from_secs(5), wait).await;
    assert_eq!(
        writer.metrics().dedup.duplicate_pushes_total,
        0,
        "a settled-not-committed claim never suppresses the retry of an unstored push"
    );

    std::fs::remove_dir_all(&root).ok();
}

// -- the two per-push ceilings ----------------------------------------

/// A push that does not fit one block is refused **whole**, naming its own
/// size and both limits, with nothing stored, no bytes reserved and the claim
/// rolled back — so the client's retry of an unstored push is stored rather
/// than answered with the refused push's outcome.
#[tokio::test]
async fn a_push_at_a_ceiling_is_refused_whole() {
    let root = spool_root("ceiling");

    // The row ceiling. A row count over kind-0 only would be 1, under any
    // ceiling, and the push would be admitted.
    let cfg = WriterConfig {
        metrics_landing_max_rows: MIXED_PUSH_ROWS,
        ..Default::default()
    };
    let inserter = MockInserter::always(Act::Ok);
    let writer = writer_with(&cfg, &root, inserter.clone());
    let before = writer.metrics().dedup.rollbacks_total;
    let err = writer
        .admit_flush(mixed_push(1_000, 1, true), PushHeaders::default())
        .expect_err("a push at the row ceiling is refused");
    assert_eq!(
        err,
        AdmitRefusal::PushTooLarge {
            rows: MIXED_PUSH_ROWS,
            row_limit: MIXED_PUSH_ROWS,
            bytes: MIXED_PUSH_BYTES,
            byte_limit: 16 * 1024 * 1024,
        }
    );
    assert_eq!(inserter.call_count(), 0, "a refusal stores nothing");
    assert_eq!(
        writer.metrics().dedup.rollbacks_total,
        before + 1,
        "the claim is rolled back, not left standing"
    );
    assert_eq!(writer.metrics().queue_bytes, 0, "no bytes stay reserved");

    // An immediately following identical push is refused the same way rather
    // than suppressed as a repeat.
    let again = writer
        .admit_flush(mixed_push(1_000, 1, true), PushHeaders::default())
        .expect_err("still refused");
    assert_eq!(again, err);
    assert_eq!(writer.metrics().dedup.duplicate_pushes_total, 0);

    // The same push without its descriptor is 4 rows, under the ceiling, and
    // is inserted once.
    let wait = writer
        .admit_flush(mixed_push(1_000, 1, false), PushHeaders::default())
        .expect("4 rows fit");
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("settles")
        .expect("commits");
    assert_eq!(inserter.call_count(), 1);
    writer.shutdown(Duration::from_secs(2)).await;

    // The byte ceiling, at the push's own estimate and one below it. A byte
    // total omitting any one of the four estimators lands under 177 and is
    // admitted where it must be refused.
    for (limit, refused) in [(MIXED_PUSH_BYTES, false), (MIXED_PUSH_BYTES - 1, true)] {
        let cfg = WriterConfig {
            batch_bytes: ByteSize(limit),
            ..Default::default()
        };
        let inserter = MockInserter::always(Act::Ok);
        let writer = writer_with(&cfg, &root, inserter.clone());
        let before = writer.metrics().dedup.rollbacks_total;
        let result = writer.admit_flush(mixed_push(1_000, 1, true), PushHeaders::default());
        if refused {
            let err = result.expect_err("refused at one byte under the estimate");
            assert_eq!(
                err,
                AdmitRefusal::PushTooLarge {
                    rows: MIXED_PUSH_ROWS,
                    row_limit: 1_048_576,
                    bytes: MIXED_PUSH_BYTES,
                    byte_limit: limit,
                }
            );
            assert_eq!(writer.metrics().dedup.rollbacks_total, before + 1);
            assert_eq!(writer.metrics().queue_bytes, 0);
            assert_eq!(inserter.call_count(), 0);
        } else {
            let wait = result.expect("byte equality is admitted, not refused");
            tokio::time::timeout(Duration::from_secs(5), wait)
                .await
                .expect("settles")
                .expect("commits");
            assert_eq!(inserter.call_count(), 1);
        }
        writer.shutdown(Duration::from_secs(2)).await;
    }

    std::fs::remove_dir_all(&root).ok();
}

/// The refusal's message is rendered from the push's own four numbers.
#[test]
fn the_push_too_large_message_names_the_size_and_both_limits() {
    assert_eq!(
        push_too_large_message(5, 5, 178, 16_777_216),
        "push does not fit one block: 5 rows (limit 5), 178 estimated bytes (limit 16777216)"
    );
}

// -- descriptors ------------------------------------------------------

/// Every push emits its descriptors: there is no cache gate left, so three
/// pushes carrying the same descriptor land three kind-3 rows, the third
/// carrying the largest `updated_ns`. A push carrying samples and no metadata
/// emits none.
#[tokio::test]
async fn every_push_emits_its_descriptors() {
    let cfg = WriterConfig::default();
    let root = spool_root("descriptors");
    let inserter = MockInserter::always(Act::Ok);
    let writer = writer_with(&cfg, &root, inserter.clone());

    for call in 0usize..3 {
        let wait = writer
            .admit_flush(
                mixed_push(1_000 + call as i64, (call as i64) + 1, true),
                PushHeaders::default(),
            )
            .expect("queue has room");
        tokio::time::timeout(Duration::from_secs(5), wait)
            .await
            .expect("settles")
            .expect("commits");
        let rows = inserter.rows_of(call);
        let descriptors = rows_of_kind(&rows, 3);
        assert_eq!(
            descriptors.len(),
            1,
            "push {call} lands its own descriptor: any suppression drops it"
        );
        assert_eq!(
            descriptors[0]["updated_ns"].as_i64(),
            Some((call as i64) + 1)
        );
        assert_eq!(descriptors[0]["metric_type"].as_str(), Some("gauge"));
    }

    // A changed descriptor for the same name lands too.
    let mut changed = mixed_push(1_003, 4, true);
    changed.metadata[0].metric_type = "counter".to_string();
    let wait = writer
        .admit_flush(changed, PushHeaders::default())
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("settles")
        .expect("commits");
    let rows = inserter.rows_of(3);
    let descriptors = rows_of_kind(&rows, 3);
    assert_eq!(descriptors.len(), 1);
    assert_eq!(descriptors[0]["metric_type"].as_str(), Some("counter"));

    // Samples with no metadata emit no kind-3 row.
    let wait = writer
        .admit_flush(batch_for("other", 9, 1_000, true), PushHeaders::default())
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("settles")
        .expect("commits");
    assert!(rows_of_kind(&inserter.rows_of(4), 3).is_empty());

    assert_eq!(writer.metrics().metadata_upserts_total, 4);
    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

// -- the block's columns ----------------------------------------------

/// Every landing column carries the value its kind was built from, in the
/// column the schema names for it. A row count or a key-set assertion cannot
/// see two columns swapped between kinds or within a kind.
#[tokio::test]
async fn every_landing_column_is_the_value_its_kind_was_built_from() {
    let cfg = WriterConfig::default();
    let root = spool_root("columns");
    let inserter = MockInserter::always(Act::Ok);
    let writer = writer_with(&cfg, &root, inserter.clone());

    let (labels, _) = LabelSet::from_normalized([("a".to_string(), "b".to_string())]);
    let batch = ParsedMetrics {
        samples: vec![MetricPoint {
            metric_name: Arc::from("m0"),
            fingerprint: Fingerprint::from_raw(10),
            unix_milli: 1_000,
            value: 1.5,
        }],
        hist_samples: vec![HistogramPoint {
            metric_name: Arc::from("m1"),
            fingerprint: Fingerprint::from_raw(11),
            unix_milli: 1_001,
            histogram: NativeHistogram {
                counter_reset_hint: pulsus_model::CounterResetHint::CounterReset,
                schema: 2,
                zero_threshold: 0.25,
                zero_count: 3,
                count: 7,
                sum: 9.5,
                positive_spans: vec![Span {
                    offset: 1,
                    length: 2,
                }],
                negative_spans: vec![],
                positive_buckets: vec![3, -1],
                negative_buckets: vec![],
                custom_values: vec![],
            },
        }],
        series: vec![
            SeriesRef {
                metric_name: Arc::from("m0"),
                fingerprint: Fingerprint::from_raw(10),
                labels: labels.clone(),
            },
            SeriesRef {
                metric_name: Arc::from("m1"),
                fingerprint: Fingerprint::from_raw(11),
                labels: labels.clone(),
            },
        ],
        metadata: vec![MetricMetadata {
            metric_name: Arc::from("m3"),
            metric_type: "counter".to_string(),
            help: "h".to_string(),
            unit: "s".to_string(),
            updated_ns: 4_000,
        }],
        ..Default::default()
    };

    let wait = writer
        .admit_flush(batch, PushHeaders::default())
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("settles")
        .expect("commits");
    let rows = inserter.rows_of(0);
    assert_eq!(kinds(&rows), vec![0, 1, 2, 2, 3]);

    let float = rows_of_kind(&rows, 0)[0];
    assert_eq!(float["metric_name"].as_str(), Some("m0"));
    assert_eq!(float["unix_milli"].as_i64(), Some(1_000));
    assert_eq!(float["value"].as_f64(), Some(1.5));
    assert_eq!(float["labels"].as_str(), Some(""), "not a kind-2 column");
    assert_eq!(float["updated_ns"].as_i64(), Some(0));

    let hist = rows_of_kind(&rows, 1)[0];
    assert_eq!(hist["metric_name"].as_str(), Some("m1"));
    assert_eq!(hist["unix_milli"].as_i64(), Some(1_001));
    assert_eq!(hist["hist_schema"].as_i64(), Some(2));
    assert_eq!(hist["hist_zero_threshold"].as_f64(), Some(0.25));
    assert_eq!(hist["hist_zero_count"].as_u64(), Some(3));
    assert_eq!(hist["hist_count"].as_u64(), Some(7));
    assert_eq!(hist["hist_sum"].as_f64(), Some(9.5));
    assert_eq!(
        hist["hist_pos_span_offsets"],
        serde_json::json!([1]),
        "a positive array must never land in a negative column"
    );
    assert_eq!(hist["hist_pos_span_lengths"], serde_json::json!([2]));
    assert_eq!(hist["hist_pos_bucket_deltas"], serde_json::json!([3, -1]));
    assert_eq!(hist["hist_neg_span_offsets"], serde_json::json!([]));
    assert_eq!(hist["hist_neg_span_lengths"], serde_json::json!([]));
    assert_eq!(hist["hist_neg_bucket_deltas"], serde_json::json!([]));
    assert_eq!(hist["hist_custom_values"], serde_json::json!([]));
    assert_eq!(hist["hist_counter_reset_hint"].as_u64(), Some(1));
    assert_eq!(hist["value"].as_f64(), Some(0.0), "not a kind-0 column");

    let registrations = rows_of_kind(&rows, 2);
    let mut value_types: Vec<u64> = registrations
        .iter()
        .map(|r| r["value_type"].as_u64().expect("a value type"))
        .collect();
    value_types.sort_unstable();
    assert_eq!(value_types, vec![0, 1], "one row per value type");
    for row in &registrations {
        assert_eq!(row["labels"].as_str(), Some(r#"{"a":"b"}"#));
        assert_eq!(
            row["unix_milli"].as_i64(),
            Some(0),
            "a kind-2 row's unix_milli is the activity-bucket floor, not a sample time"
        );
        assert_eq!(row["value"].as_f64(), Some(0.0));
    }

    let descriptor = rows_of_kind(&rows, 3)[0];
    assert_eq!(descriptor["metric_name"].as_str(), Some("m3"));
    assert_eq!(descriptor["metric_type"].as_str(), Some("counter"));
    assert_eq!(descriptor["help"].as_str(), Some("h"), "help is not unit");
    assert_eq!(descriptor["unit"].as_str(), Some("s"));
    assert_eq!(descriptor["updated_ns"].as_i64(), Some(4_000));
    assert_eq!(descriptor["fingerprint"].as_u64(), Some(0));

    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

// -- concurrency and the queue's byte allowance -----------------------

/// The inserter count bounds how many landing inserts overlap: with two
/// workers and both held, a third push waits in the queue.
#[tokio::test]
async fn the_inserter_count_bounds_concurrent_inserts() {
    let cfg = WriterConfig {
        metrics_landing_inserters: 2,
        ..Default::default()
    };
    let root = spool_root("inserters");
    let inserter = MockInserter::always(Act::Gate);
    let writer = writer_with(&cfg, &root, inserter.clone());

    for unix_milli in [1_000, 1_001, 1_002] {
        writer
            .admit(batch_for("m", 1, unix_milli, true), PushHeaders::default())
            .expect("queue has room");
    }
    settle_until(|| inserter.call_count() >= 2).await;
    settle_until(|| false).await;
    assert_eq!(
        inserter.call_count(),
        2,
        "two workers, so two inserts in flight and the third queued"
    );

    inserter.release_one();
    settle_until(|| inserter.call_count() >= 3).await;
    assert_eq!(inserter.call_count(), 3, "releasing one starts the third");

    inserter.release_one();
    inserter.release_one();
    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// The queue's byte allowance is aggregate across pushes, not per push: with
/// room for exactly three of the 178-byte push, a fourth is refused and
/// nothing of it stays reserved. Releasing the held inserts returns exactly
/// what was reserved.
#[tokio::test]
async fn the_queue_byte_allowance_is_aggregate_across_pushes() {
    let cfg = WriterConfig {
        metrics_landing_inserters: 1,
        // Exactly three of the 178-byte push.
        ingest_queue_bytes: ByteSize(MIXED_PUSH_BYTES * 3),
        ..Default::default()
    };
    let root = spool_root("aggregate-bytes");
    let inserter = MockInserter::always(Act::Gate);
    let writer = writer_with(&cfg, &root, inserter.clone());

    // Each push is 178 bytes: the held insert never promotes the series LRU,
    // so all three emit their two kind-2 rows.
    for unix_milli in [1_000, 1_001, 1_002] {
        writer
            .admit(mixed_push(unix_milli, 1, true), PushHeaders::default())
            .expect("queue has room for three");
    }
    settle_until(|| inserter.call_count() >= 1).await;
    assert_eq!(writer.metrics().queue_bytes, MIXED_PUSH_BYTES * 3);

    let before = writer.metrics();
    let err = writer
        .admit(mixed_push(1_003, 1, true), PushHeaders::default())
        .expect_err("the fourth push does not fit the aggregate allowance");
    assert_eq!(err, AdmitRefusal::Backpressure);
    let after = writer.metrics();
    assert_eq!(after.backpressure_total, before.backpressure_total + 1);
    assert_eq!(
        after.dedup.rollbacks_total,
        before.dedup.rollbacks_total + 1,
        "the refused push's claim is rolled back"
    );
    assert_eq!(
        after.queue_bytes,
        MIXED_PUSH_BYTES * 3,
        "the refusal reserved nothing: a reading of four pushes' worth would be the \
         roll-back not happening"
    );

    for _ in 0..3 {
        inserter.release_one();
    }
    settle_until(|| writer.metrics().queue_bytes == 0).await;
    assert_eq!(
        writer.metrics().queue_bytes,
        0,
        "exactly what was reserved is released"
    );
    assert_eq!(inserter.call_count(), 3);

    // The fourth body again: admitted, and the fourth insert call.
    writer
        .admit(mixed_push(1_003, 1, true), PushHeaders::default())
        .expect("queue has room now");
    inserter.release_one();
    settle_until(|| inserter.call_count() >= 4).await;
    assert_eq!(
        inserter.call_count(),
        4,
        "a refusal that sealed the claim would make this a suppressed duplicate"
    );
    assert_eq!(writer.metrics().dedup.duplicate_pushes_total, 0);

    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// A block whose budget expired while it waited in the queue starts no insert
/// at all, and is filed as provably not sent — never `uncertain/`, which
/// would claim a block nobody sent may have committed.
///
/// The same 121 s on the same clock ends the two blocks differently: A's own
/// attempt was interrupted by the budget, which is the unknown fate; B was
/// still queued, which is not.
#[tokio::test(start_paused = true)]
async fn a_block_whose_budget_expired_while_queued_never_starts_an_insert() {
    let cfg = WriterConfig {
        metrics_landing_inserters: 1,
        metrics_landing_retries: 0,
        batch_ms: 5_000,
        ..Default::default()
    };
    let root = spool_root("queued-budget");
    // The first call never returns; a later one would return Ok, so a loop
    // that starts an attempt for every block whatever the elapsed budget
    // settles B `Ok` and makes the call count 2.
    let inserter = MockInserter::new(vec![Step::now(Act::Hang), Step::now(Act::Ok)]);
    let writer = writer_with(&cfg, &root, inserter.clone());

    writer
        .admit(batch_for("a", 1, 1_000, true), PushHeaders::default())
        .expect("queue has room");
    settle_until(|| inserter.call_count() == 1).await;
    assert_eq!(inserter.call_count(), 1, "A is in flight");
    let b = writer
        .admit_flush(batch_for("b", 2, 1_000, true), PushHeaders::default())
        .expect("queue has room");

    let answer = tokio::time::timeout(Duration::from_secs(600), b)
        .await
        .expect("B settles");

    assert_eq!(
        inserter.call_count(),
        1,
        "the worker found B's budget spent and started no insert for it"
    );
    let poison = spool_records(&root, "poison");
    assert_eq!(poison.len(), 1, "B is filed provably not sent");
    assert_eq!(
        poison[0]["error"].as_str(),
        Some("the landing budget was spent before the block was sent")
    );
    let err = answer.expect_err("never a success").to_string();
    assert!(
        err.contains("spooled to poison"),
        "B's caller is told the block was not stored: {err}"
    );
    assert_eq!(
        spool_records(&root, "uncertain").len(),
        1,
        "A's own attempt was interrupted by the budget, which is the unknown fate"
    );
    let snap = writer.metrics();
    assert_eq!(snap.spool_poison_total, 1);
    assert_eq!(snap.spool_uncertain_total, 1);
    assert_eq!(snap.queue_bytes, 0);

    // The budget settled both claims strictly inside the claim deadline
    // (`batch_ms` + 120 s = 125 s), so nothing aged into a tombstone.
    assert_eq!(
        writer.dedup().expect("an index").snapshot().unknown_total,
        0
    );
    tokio::time::advance(Duration::from_secs(10)).await;
    settle_until(|| false).await;
    assert_eq!(
        writer.dedup().expect("an index").snapshot().unknown_total,
        0,
        "past the claim deadline both claims are already terminal"
    );

    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

// -- counters ---------------------------------------------------------

/// The landing loop counts what it did: rows, bytes, one flush per push, its
/// latency, its resends, the in-flight gauge and spool-write failures.
#[tokio::test]
async fn the_landing_loop_counts_what_it_did() {
    let cfg = WriterConfig::default();
    let root = spool_root("counters");
    let inserter = MockInserter::new(vec![Step::after(Duration::from_millis(5), Act::Ok)]);
    let writer = writer_with(&cfg, &root, inserter.clone());

    let wait = writer
        .admit_flush(mixed_push(1_000, 1, true), PushHeaders::default())
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("settles")
        .expect("commits");

    let landing = writer.metrics().landing;
    assert_eq!(landing.rows_total, MIXED_PUSH_ROWS);
    assert_eq!(landing.bytes_total, MIXED_PUSH_BYTES);
    assert_eq!(
        landing.flushes_total, 1,
        "one insert per push, not one per row"
    );
    assert_eq!(landing.flush_latency_count, 1);
    assert!(
        landing.flush_latency_sum_ns >= 5_000_000,
        "the latency covers the insert: {}",
        landing.flush_latency_sum_ns
    );
    assert_eq!(landing.retries_total, 0);
    assert_eq!(landing.inflight, 0);
    assert_eq!(landing.spool_write_failures_total, 0);
    writer.shutdown(Duration::from_secs(2)).await;

    // The in-flight gauge is 1 while the insert is held.
    let held = MockInserter::always(Act::Gate);
    let writer = writer_with(&cfg, &root, held.clone());
    writer
        .admit(mixed_push(1_000, 1, true), PushHeaders::default())
        .expect("queue has room");
    settle_until(|| held.call_count() == 1).await;
    assert_eq!(writer.metrics().landing.inflight, 1);
    held.release_one();
    writer.shutdown(Duration::from_secs(2)).await;

    // One retryable pre-send error then Ok counts one resend.
    let retried = MockInserter::new(vec![Step::now(Act::Retryable), Step::now(Act::Ok)]);
    let writer = writer_with(&cfg, &root, retried.clone());
    let wait = writer
        .admit_flush(mixed_push(1_000, 1, true), PushHeaders::default())
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(30), wait)
        .await
        .expect("settles")
        .expect("commits");
    assert_eq!(writer.metrics().landing.retries_total, 1);
    writer.shutdown(Duration::from_secs(2)).await;

    std::fs::remove_dir_all(&root).ok();
}

/// A spool write that itself fails is counted and changes no outcome: the
/// claim is still reported from the fate, the caller is still answered, and
/// the reservation is still released.
#[tokio::test]
async fn a_failed_spool_write_is_counted_and_changes_no_outcome() {
    let cfg = WriterConfig::default();
    // A spool root that is a plain FILE, so `create_dir_all` fails
    // deterministically — the shipped `plain_file_spool_root` setup.
    let root = std::env::temp_dir().join(format!(
        "pulsus-metric-writer-plain-file-{}-{}",
        std::process::id(),
        unique()
    ));
    std::fs::write(&root, b"not a directory").expect("create the plain-file spool root");

    let inserter = MockInserter::always(Act::Poison);
    let writer = writer_at(runtime_at(&cfg, &root), inserter.clone());

    let wait = writer
        .admit_flush(mixed_push(1_000, 1, true), PushHeaders::default())
        .expect("queue has room");
    let answer = tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("settles");
    let err = answer.expect_err("never a success").to_string();
    assert!(
        err.contains("spooled to poison"),
        "the outcome comes from the fate, not from the spool write: {err}"
    );
    assert_eq!(writer.metrics().landing.spool_write_failures_total, 1);
    assert_eq!(
        writer.metrics().queue_bytes,
        0,
        "an ending that returned from the spool error without releasing would leave 178"
    );

    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_file(&root).ok();
}

// -- the registration LRU (unchanged rules, new shape) ----------------

/// A series' second sample in the SAME activity bucket must not re-register:
/// the LRU suppresses it before it reaches a block, so the second push
/// inserts a block carrying no kind-2 row rather than not inserting at all.
#[tokio::test]
async fn same_bucket_second_sample_is_suppressed_by_the_series_lru() {
    let cfg = WriterConfig::default();
    let root = spool_root("lru-same-bucket");
    let inserter = MockInserter::always(Act::Ok);
    let writer = writer_with(&cfg, &root, inserter.clone());

    let wait = writer
        .admit_flush(
            batch_for("http_requests_total", 1, 0, true),
            PushHeaders::default(),
        )
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("settles")
        .expect("the first registration commits");
    assert_eq!(inserter.call_count(), 1);
    assert_eq!(writer.metrics().series_registrations_total, 1);
    assert_eq!(rows_of_kind(&inserter.rows_of(0), 2).len(), 1);

    // Same series, same activity bucket (60_000 floors to the same bucket as
    // 0 under the default 1h bucket).
    let wait = writer
        .admit_flush(
            batch_for("http_requests_total", 1, 60_000, false),
            PushHeaders::default(),
        )
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("settles")
        .expect("commits");

    let metrics = writer.metrics();
    assert_eq!(metrics.series_lru_hits_total, 1);
    assert_eq!(
        metrics.series_registrations_total, 1,
        "the same-bucket sample must not register a second row"
    );
    assert_eq!(inserter.call_count(), 2, "the samples still land");
    assert!(
        rows_of_kind(&inserter.rows_of(1), 2).is_empty(),
        "the second block carries no registration row"
    );

    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// A sample in a NEW activity bucket for an already-registered series
/// registers again: the LRU key is `(metric_name, fingerprint, bucket,
/// value_type)`, so crossing a bucket boundary is a fresh miss.
#[tokio::test]
async fn new_bucket_for_an_already_registered_series_emits_a_new_registration() {
    let cfg = WriterConfig::default();
    let root = spool_root("lru-new-bucket");
    let inserter = MockInserter::always(Act::Ok);
    let writer = writer_with(&cfg, &root, inserter.clone());

    for unix_milli in [0, BUCKET_MS] {
        let wait = writer
            .admit_flush(
                batch_for("http_requests_total", 1, unix_milli, true),
                PushHeaders::default(),
            )
            .expect("queue has room");
        tokio::time::timeout(Duration::from_secs(5), wait)
            .await
            .expect("settles")
            .expect("commits");
    }

    assert_eq!(
        writer.metrics().series_registrations_total,
        2,
        "crossing a bucket boundary registers a new row"
    );
    assert_eq!(inserter.call_count(), 2);
    assert_eq!(
        rows_of_kind(&inserter.rows_of(1), 2).len(),
        1,
        "the second block carries the new bucket's registration"
    );

    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// A kind-2 row's `unix_milli` is the activity-bucket floor, not the raw
/// sample timestamp — read off the block the inserter received.
#[tokio::test]
async fn registered_series_row_carries_the_bucket_floored_timestamp_not_the_raw_sample() {
    let cfg = WriterConfig::default();
    let root = spool_root("lru-floor");
    let inserter = MockInserter::always(Act::Ok);
    let writer = writer_with(&cfg, &root, inserter.clone());

    let raw_unix_milli = BUCKET_MS + 12_345; // mid-bucket, not on a boundary
    let wait = writer
        .admit_flush(
            batch_for("http_requests_total", 1, raw_unix_milli, true),
            PushHeaders::default(),
        )
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("settles")
        .expect("commits");

    let rows = inserter.rows_of(0);
    let registration = rows_of_kind(&rows, 2);
    assert_eq!(registration.len(), 1);
    assert_eq!(registration[0]["unix_milli"].as_i64(), Some(BUCKET_MS));
    assert_eq!(
        rows_of_kind(&rows, 0)[0]["unix_milli"].as_i64(),
        Some(raw_unix_milli),
        "the sample keeps its own timestamp"
    );

    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// A native-histogram batch lands one kind-1 row and one kind-2 row stamped
/// `value_type = 1`, in one insert.
#[tokio::test]
async fn native_histogram_batch_writes_hist_row_and_registers_value_type_one() {
    let cfg = WriterConfig::default();
    let root = spool_root("hist-one");
    let inserter = MockInserter::always(Act::Ok);
    let writer = writer_with(&cfg, &root, inserter.clone());

    let batch = ParsedMetrics {
        hist_samples: vec![HistogramPoint {
            metric_name: Arc::from("http_request_duration_seconds"),
            fingerprint: Fingerprint::from_raw(7),
            unix_milli: 0,
            histogram: one_bucket_hist(5.0),
        }],
        series: vec![series_ref("http_request_duration_seconds", 7)],
        ..Default::default()
    };
    let wait = writer
        .admit_flush(batch, PushHeaders::default())
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("settles")
        .expect("commits");

    assert_eq!(inserter.call_count(), 1, "one insert");
    let rows = inserter.rows_of(0);
    assert_eq!(kinds(&rows), vec![1, 2]);
    assert_eq!(rows_of_kind(&rows, 2)[0]["value_type"].as_u64(), Some(1));
    assert_eq!(writer.metrics().landing.rows_total, 2);

    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// A transition bucket — a float sample then a histogram sample at the SAME
/// `(metric_name, fingerprint, bucket)` — registers BOTH rows. If
/// `value_type` were absent from the LRU key the second would be a false hit
/// and the second block would carry no registration.
#[tokio::test]
async fn transition_bucket_registers_both_float_and_histogram_series_rows() {
    let cfg = WriterConfig::default();
    let root = spool_root("hist-transition");
    let inserter = MockInserter::always(Act::Ok);
    let writer = writer_with(&cfg, &root, inserter.clone());

    let wait = writer
        .admit_flush(batch_for("m", 1, 0, true), PushHeaders::default())
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("settles")
        .expect("commits");
    let first = inserter.rows_of(0);
    assert_eq!(rows_of_kind(&first, 2)[0]["value_type"].as_u64(), Some(0));

    let batch = ParsedMetrics {
        hist_samples: vec![HistogramPoint {
            metric_name: Arc::from("m"),
            fingerprint: Fingerprint::from_raw(1),
            unix_milli: 0,
            histogram: one_bucket_hist(5.0),
        }],
        series: vec![series_ref("m", 1)],
        ..Default::default()
    };
    let wait = writer
        .admit_flush(batch, PushHeaders::default())
        .expect("queue has room");
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("settles")
        .expect("commits");

    assert_eq!(inserter.call_count(), 2);
    let second_call = inserter.rows_of(1);
    let second = rows_of_kind(&second_call, 2);
    assert_eq!(
        second.len(),
        1,
        "the histogram registration is not suppressed by the float's LRU entry"
    );
    assert_eq!(second[0]["value_type"].as_u64(), Some(1));

    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}
