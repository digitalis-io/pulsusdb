//! What the **production** inserter puts on the wire, and how the production
//! insert path classifies what comes back (issue #603).
//!
//! Every case here drives `ChBlockInserter` — the adapter `MetricWriter::new`
//! builds — and `ChClient::insert_block_with` underneath it, against the
//! hermetic mock in `tests/common/mock_ch_insert.rs`. Nothing here uses a
//! `BlockInserter` mock: a test double that implements `insert_with` itself
//! proves only that the double does.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChError, QuerySettings};
use pulsus_config::WriterConfig;
use pulsus_model::{DEFAULT_ACTIVITY_BUCKET_MS, Fingerprint, LabelSet};
use pulsus_write::writer::{
    BlockInserter, ChBlockInserter, MetricWriter, MetricWriterTables, WriterRuntime,
};
use pulsus_write::{MetricPoint, MetricSink, ParsedMetrics, PushHeaders, SeriesRef, TraceSink};

#[path = "common/mock_ch_insert.rs"]
mod mock_ch_insert;
use mock_ch_insert::{DescribeAnswer, InsertAnswer, MockChInsert, OneCol};

/// The settings one landing insert carries, as the writer builds them.
fn landing_settings() -> QuerySettings {
    QuerySettings::landing_insert("tok-1", 1_048_576)
}

/// **The per-block settings must reach the server.** The writer hands
/// `QuerySettings::landing_insert` to `BlockInserter::insert_with`; if the
/// production adapter does not override that method it inherits a default
/// that drops `extra` and calls `insert`, so the deduplication token, the two
/// deduplication pins and the seven block limits are never sent — no retry
/// safety at all, and a push above the server's own default block size split
/// into several blocks.
#[tokio::test]
async fn the_production_inserter_sends_the_landing_settings_on_the_wire() {
    let mock = MockChInsert::start(DescribeAnswer::Ok, InsertAnswer::Ok);
    let client = Arc::new(
        ChClient::new(mock.conn_config())
            .await
            .expect("connect to the mock"),
    );
    let inserter = ChBlockInserter::new(client);
    let rows = vec![OneCol { v: 7 }];

    BlockInserter::<OneCol>::insert_with(&inserter, "t", &rows, &landing_settings())
        .await
        .expect("the mock answers the insert with a 200");

    let insert = mock.insert_request();
    assert_eq!(
        insert.param("insert_deduplication_token").as_deref(),
        Some("tok-1"),
        "the minted token is what makes a resend safe: {}",
        insert.target
    );
    assert_eq!(
        insert.param("max_insert_block_size").as_deref(),
        Some("1048576"),
        "the block-size pin is what stops one push becoming two blocks: {}",
        insert.target
    );
    assert_eq!(
        insert.param("min_insert_block_size_rows").as_deref(),
        Some("1048576"),
        "the minimum pair ends a block too, and its row half follows the same \
         ceiling: {}",
        insert.target
    );
    // Five settings end a block by a measure that is not a row count: three
    // byte limits, one elapsed time, and the connection handling the wait
    // time needs (issue #603 code review rounds 7 and 8, finding 1). Each
    // must reach the server pinned to its "does not participate" value, or a
    // push inside the row ceiling still becomes several blocks under a
    // server profile that set one of them — and the last of them disables
    // deduplication outright, which defeats the token.
    for key in [
        "max_insert_block_size_bytes",
        "input_format_max_block_size_bytes",
        "min_insert_block_size_bytes",
        "input_format_max_block_wait_ms",
        "input_format_connection_handling",
    ] {
        assert_eq!(
            insert.param(key).as_deref(),
            Some("0"),
            "{key} must be pinned off on the wire, or the row ceiling is not \
             the only thing that forms a block: {}",
            insert.target
        );
    }
    assert_eq!(
        insert.param("deduplicate_insert").as_deref(),
        Some("enable"),
        "{}",
        insert.target
    );
    assert_eq!(
        insert
            .param("deduplicate_blocks_in_dependent_materialized_views")
            .as_deref(),
        Some("1"),
        "{}",
        insert.target
    );
    assert_eq!(
        insert.param("async_insert").as_deref(),
        Some("0"),
        "the shipped pin is still there: {}",
        insert.target
    );
}

/// The adapter's own settings and the call's are both sent, and where the two
/// name the same setting the **call's** value is the one on the wire: the
/// per-block token is minted per push and an inserter-wide value can never
/// stand in for it.
#[tokio::test]
async fn the_calls_settings_win_over_the_inserters_own() {
    let mock = MockChInsert::start(DescribeAnswer::Ok, InsertAnswer::Ok);
    let client = Arc::new(
        ChClient::new(mock.conn_config())
            .await
            .expect("connect to the mock"),
    );
    let inserter = ChBlockInserter::with_settings(
        client,
        QuerySettings::new()
            .set("insert_deduplication_token", "inserter-wide")
            .set("log_comment", "from the inserter"),
    );
    let rows = vec![OneCol { v: 7 }];

    BlockInserter::<OneCol>::insert_with(&inserter, "t", &rows, &landing_settings())
        .await
        .expect("the mock answers the insert with a 200");

    let insert = mock.insert_request();
    assert_eq!(
        insert.param("insert_deduplication_token").as_deref(),
        Some("tok-1"),
        "the call's token, not the inserter's: {}",
        insert.target
    );
    assert_eq!(
        insert.param("log_comment").as_deref(),
        Some("from the inserter"),
        "a setting only the inserter names is still sent: {}",
        insert.target
    );
}

/// **A non-retryable exception that arrives after the block was transmitted
/// has UNKNOWN commit fate, not a proven one.** The source block may already
/// be committed when a materialized view on it throws, and every metric table
/// is now maintained by one, so a server exception from the end of an insert
/// must reach the caller as `InsertUncertain` — the writer files that
/// `uncertain/` and reports the claim `Uncertain`. Surfaced as a plain
/// `ChError::Server` it is classified pre-send and the block is filed as
/// provably **not** stored, which is the one direction that must never
/// happen.
#[tokio::test]
async fn a_server_exception_after_the_block_was_sent_is_uncertain() {
    let mock = MockChInsert::start(
        DescribeAnswer::Ok,
        // 395 is FUNCTION_THROW_IF_VALUE_IS_NON_ZERO: the shape a view that
        // throws synchronously takes, and not a retryable code.
        InsertAnswer::Exception {
            code: 395,
            text: "a dependent view threw while processing the block",
        },
    );
    let client = ChClient::new(mock.conn_config())
        .await
        .expect("connect to the mock");
    let rows = vec![OneCol { v: 7 }];

    let err = client
        .insert_block_with("t", &rows, &landing_settings())
        .await
        .expect_err("the mock answers the insert with an exception");

    match err {
        ChError::InsertUncertain(msg) => {
            assert!(
                msg.contains("395"),
                "the uncertain error keeps the exception it came from: {msg}"
            );
        }
        other => panic!(
            "an exception returned after the block was transmitted must be \
             InsertUncertain, got {other:?}"
        ),
    }
    assert!(
        !mock.requests().is_empty(),
        "the mock served the requests it recorded"
    );
}

/// **A RETRYABLE failure proven to precede transmission keeps its own
/// class.** The vendored client fetches the target's column metadata before
/// it opens the insert request, so an exception there is answered with none
/// of the block sent: it is genuine pre-commit poison and must stay the
/// precise error, or every bad statement would be reported as "may have
/// committed" and the claim would never be released for the client's retry.
///
/// **The injected code is retryable on purpose.** `SOCKET_TIMEOUT` (209) is
/// in `ChError::is_retryable`'s table, and the phase rule is the only thing
/// that keeps it out of `InsertUncertain`: the classification this replaced
/// downgraded every retryable failure whatever phase it came from, so a
/// non-retryable code — `UNKNOWN_TABLE` (60), for one — takes the identical
/// branch before and after and could not tell the two apart.
#[tokio::test]
async fn a_retryable_failure_before_the_block_was_sent_keeps_its_own_class() {
    let mock = MockChInsert::start(
        DescribeAnswer::Exception {
            code: 209,
            text: "Timeout exceeded while reading from socket",
        },
        InsertAnswer::Ok,
    );
    let client = ChClient::new(mock.conn_config())
        .await
        .expect("connect to the mock");
    let rows = vec![OneCol { v: 7 }];

    let err = client
        .insert_block_with("t", &rows, &landing_settings())
        .await
        .expect_err("the mock answers the metadata read with an exception");

    match &err {
        ChError::Server { code, .. } => assert_eq!(*code, 209, "the server's own code"),
        other => panic!("a pre-send failure must not be downgraded: {other:?}"),
    }
    assert!(
        err.is_retryable(),
        "a pre-send failure keeps its retryable class, which is what lets the \
         landing loop resend the block: {err:?}"
    );
    assert!(
        mock.requests()
            .iter()
            .all(|r| !r.body.starts_with("INSERT INTO")),
        "no insert was ever opened: {:?}",
        mock.requests()
    );
}

/// **The client's own deadline firing while the metadata read is still
/// pending is a pre-send failure, not uncertainty.** One deadline covers the
/// whole create/write/end sequence, so the arm that handles it has to say
/// which phase it cut: a deadline during metadata acquisition leaves the
/// insert request never opened, and reporting it `InsertUncertain` tells the
/// caller that a block which demonstrably never left the process may have
/// committed — which costs it the pre-send resend it was entitled to.
///
/// The mock reads the `DESCRIBE` and answers nothing, so the metadata read is
/// what the deadline interrupts; `query_timeout` is that client-side insert
/// deadline, short here so the case does not wait out the default.
#[tokio::test]
async fn a_client_deadline_during_the_metadata_read_is_a_pre_send_failure() {
    let mock = MockChInsert::start(DescribeAnswer::Stall, InsertAnswer::Ok);
    let client = ChClient::new(mock.conn_config_with_timeout(Duration::from_millis(400)))
        .await
        .expect("connect to the mock");
    let rows = vec![OneCol { v: 7 }];

    let err = client
        .insert_block_with("t", &rows, &landing_settings())
        .await
        .expect_err("the mock never answers the metadata read");

    match &err {
        ChError::Timeout(msg) => assert!(
            msg.contains("insert_block exceeded"),
            "the deadline's own error: {msg}"
        ),
        other => panic!(
            "a deadline that fired before the insert request was opened must \
             keep its own class, not be downgraded to uncertainty: {other:?}"
        ),
    }
    assert!(
        err.is_retryable(),
        "a deadline proven to precede transmission keeps its retryable class, \
         which is what lets the landing loop resend the block: {err:?}"
    );
    assert!(
        mock.requests()
            .iter()
            .all(|r| !r.body.starts_with("INSERT INTO")),
        "no insert was ever opened: {:?}",
        mock.requests()
    );
}

/// **The same deadline firing once the insert request is open IS
/// uncertainty.** The pair with the case above is what pins the phase split:
/// one deadline, two classes, decided by how far the attempt had got. From
/// the first `write` the vendored client may already have flushed part of the
/// block, and a source block can be committed before a view on it answers, so
/// the commit fate is unknown and no caller may auto-retry it.
#[tokio::test]
async fn a_client_deadline_after_the_insert_was_opened_is_uncertain() {
    let mock = MockChInsert::start(DescribeAnswer::Ok, InsertAnswer::Stall);
    let client = ChClient::new(mock.conn_config_with_timeout(Duration::from_millis(400)))
        .await
        .expect("connect to the mock");
    let rows = vec![OneCol { v: 7 }];

    let err = client
        .insert_block_with("t", &rows, &landing_settings())
        .await
        .expect_err("the mock never answers the insert");

    match &err {
        ChError::InsertUncertain(msg) => assert!(
            msg.contains("insert_block exceeded"),
            "the uncertain error keeps the deadline it came from: {msg}"
        ),
        other => panic!(
            "a deadline that fired with the insert request open has unknown \
             commit fate: {other:?}"
        ),
    }
    assert!(
        !err.is_retryable(),
        "an uncertain block is never auto-retried: {err:?}"
    );
    // The insert request reached the server, so the phase this case names is
    // the phase the deadline cut. The mock records a request before it stalls,
    // so the record is there as soon as it has read it; the bounded wait is for
    // that record and never for the outcome asserted above.
    let mut opened = false;
    for _ in 0..200 {
        if mock
            .requests()
            .iter()
            .any(|r| r.body.starts_with("INSERT INTO"))
        {
            opened = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        opened,
        "the insert request reached the mock: {:?}",
        mock.requests()
    );
}

/// A spool root of this case's own, so the writer writes nothing into the
/// process working directory.
fn spool_root(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pulsus-landing-settings-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create the spool root");
    dir
}

/// How many files this run spooled under `kind`.
fn spooled(root: &Path, kind: &str) -> usize {
    let dir = root.join(kind).join("metric_landing");
    std::fs::read_dir(&dir)
        .map(|entries| {
            entries
                .flatten()
                .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("json"))
                .count()
        })
        .unwrap_or(0)
}

/// One float sample and its series, which is the smallest push that makes a
/// landing block.
fn one_sample_push() -> ParsedMetrics {
    let (labels, _) = LabelSet::from_normalized([("job".to_string(), "checkout".to_string())]);
    ParsedMetrics {
        samples: vec![MetricPoint {
            metric_name: Arc::from("up"),
            fingerprint: Fingerprint::from_raw(7),
            unix_milli: 1_000,
            value: 1.0,
        }],
        series: vec![SeriesRef {
            metric_name: Arc::from("up"),
            fingerprint: Fingerprint::from_raw(7),
            labels,
        }],
        ..Default::default()
    }
}

/// **A retryable pre-send failure reaches the resend loop, and the block it
/// ends still settles provably NOT committed.** The writer drives the
/// production `ChBlockInserter` here, so the failure is the one the vendored
/// client raises from its own metadata read — not a class a mock inserter
/// chose to return.
///
/// Two things are asserted because the loop resends on either class: the
/// number of attempts (one resend at `metrics_landing_retries = 1`, so two
/// metadata reads on the wire), and the fate the block ends with. A pre-send
/// failure reported as `InsertUncertain` — what the classification this
/// replaced did to every retryable error — resends just the same, and files
/// the push as "may have been stored", which never releases the claim for
/// the client's own retry.
#[tokio::test]
async fn a_retryable_pre_send_failure_is_resent_and_settles_not_committed() {
    let mock = MockChInsert::start(
        DescribeAnswer::Exception {
            code: 209,
            text: "Timeout exceeded while reading from socket",
        },
        InsertAnswer::Ok,
    );
    let client = Arc::new(
        ChClient::new(mock.conn_config())
            .await
            .expect("connect to the mock"),
    );
    let root = spool_root("retryable-pre-send");
    let mut runtime = WriterRuntime::from_config(&WriterConfig {
        metrics_landing_retries: 1,
        metrics_landing_inserters: 1,
        ..Default::default()
    });
    runtime.spool_dir = root.clone();
    let writer = MetricWriter::with_landing_inserter_and_runtime(
        Arc::new(ChBlockInserter::new(client)),
        runtime,
        DEFAULT_ACTIVITY_BUCKET_MS,
        MetricWriterTables::metrics_default(),
    );

    let wait = writer
        .admit_flush(one_sample_push(), PushHeaders::default())
        .expect("the queue has room");
    let answer = tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .expect("the loop settles the block");
    let err = answer.expect_err("every attempt failed, so the push is not stored");

    let describes = mock
        .requests()
        .iter()
        .filter(|r| r.body.starts_with("DESCRIBE TABLE"))
        .count();
    assert_eq!(
        describes,
        2,
        "one metadata read per attempt: the first, and the resend the \
         retryable class earns it: {:?}",
        mock.requests()
    );
    assert_eq!(
        writer.metrics().landing.retries_total,
        1,
        "the loop counted its resend"
    );
    let msg = err.to_string();
    assert!(
        msg.contains("spooled to poison"),
        "a failure proven to precede transmission is provably not stored, so \
         the push settles poison and the claim is released for the client's \
         own retry: {msg}"
    );
    assert!(
        msg.contains("209"),
        "the answer carries the server's own exception: {msg}"
    );
    assert_eq!(
        (spooled(&root, "poison"), spooled(&root, "uncertain")),
        (1, 0),
        "the block is filed under poison, not under uncertain"
    );

    writer.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

// -- issue #586: the trace landing insert ------------------------------

/// A `BlockInserter` that answers every call `Ok` and records nothing — the
/// old two-table path's stand-in, so the mock server serves the landing
/// insert alone.
struct NoopInserter;

impl<R: pulsus_clickhouse::ChRow> BlockInserter<R> for NoopInserter {
    fn insert<'a>(
        &'a self,
        _table: &'a str,
        _rows: &'a [R],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ChError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

/// **What the production inserter puts on the wire for a trace landing
/// insert is the client's MERGED set, not the constructor's** — and the
/// writer is what builds the constructor's half.
///
/// The merge order, read off `ChClient::insert_settings_with`, is: the
/// `ConsistencyConfig`'s own entries first, then `async_insert = 0`, then the
/// caller's entries by key, then `max_execution_time` last. So comparing the
/// captured set against the constructor's alone is what this case must not
/// do — the wire carries more than the constructor names, because
/// `async_insert` is pinned one layer down, and an equality against the
/// constructor would fail on a correct implementation.
///
/// **Two halves, because one mock cannot serve both.** The wire half drives
/// the production `ChBlockInserter` against the hermetic mock server, over a
/// one-column row: the driver reads `DESCRIBE TABLE` before it sends the
/// insert and refuses a 31-column landing row against the mock's one-column
/// answer, so a `TraceWriter` pointed at that mock never puts an `INSERT` on
/// the wire at all — measured, the mock serves the `DESCRIBE` and nothing
/// after it. The writer half therefore reads what the writer handed its own
/// `BlockInserter`, which is the other end of the same seam.
#[tokio::test]
async fn the_production_inserter_sends_the_trace_landing_settings_on_the_wire() {
    let cfg = WriterConfig::default();
    let mock = MockChInsert::start(DescribeAnswer::Ok, InsertAnswer::Ok);
    let client = Arc::new(
        ChClient::new(mock.conn_config())
            .await
            .expect("connect to the mock"),
    );

    // The baseline: the same client, the same adapter, no caller settings at
    // all. Whatever query parameters this request carries are the driver's
    // own plus the two the client pins for every insert it makes.
    let bare = ChBlockInserter::new(client.clone());
    BlockInserter::<OneCol>::insert_with(&bare, "t", &[OneCol { v: 1 }], &QuerySettings::new())
        .await
        .expect("the mock answers the insert with a 200");
    let baseline_keys = param_keys(&mock.insert_request().target);

    // The wire half.
    let token = "tok-trace-1";
    let expected = QuerySettings::trace_landing_insert(token, cfg.trace_landing_max_rows);
    let inserter = ChBlockInserter::new(client);
    BlockInserter::<OneCol>::insert_with(&inserter, "trace_landing", &[OneCol { v: 7 }], &expected)
        .await
        .expect("the mock answers the insert with a 200");
    let insert = mock
        .requests()
        .into_iter()
        .rfind(|r| r.body.starts_with("INSERT INTO"))
        .expect("the mock served the landing INSERT");

    // (a) Every entry the constructor gives, at its own value.
    let mut missing: Vec<(String, String, Option<String>)> = Vec::new();
    for (key, value) in expected.entries() {
        let got = insert.param(key);
        if got.as_deref() != Some(value) {
            missing.push((key.to_string(), value.to_string(), got));
        }
    }
    assert!(
        missing.is_empty(),
        "the wire must carry every pin the constructor names: {missing:?} in {}",
        insert.target
    );

    // (b) The already-shipped pin, exactly once.
    let query = insert
        .target
        .split_once('?')
        .map(|(_, q)| q)
        .unwrap_or_default();
    let async_entries: Vec<&str> = query
        .split('&')
        .filter(|pair| pair.starts_with("async_insert="))
        .collect();
    assert_eq!(
        async_entries,
        vec!["async_insert=0"],
        "`async_insert = 0` is pinned one layer down, so it must appear once \
         and not twice: {}",
        insert.target
    );

    // (c) Nothing else is a setting this path sends.
    let keys = param_keys(&insert.target);
    let constructor_keys: Vec<&str> = expected.entries().map(|(k, _)| k).collect();
    let extra: Vec<&String> = keys
        .iter()
        .filter(|k| !baseline_keys.contains(k) && !constructor_keys.contains(&k.as_str()))
        .collect();
    assert!(
        extra.is_empty(),
        "the landing insert sends the constructor's entries and nothing a \
         bare insert through the same client does not already carry: {extra:?}"
    );

    // The writer half: what `TraceWriter` hands its own `BlockInserter` for
    // one push is the trace constructor at the CONFIGURED ceiling, under a
    // token it minted. A writer that handed over `landing_insert` instead —
    // the metrics constructor — would be short the nineteen trace pins.
    let root = spool_root("trace-landing-wire");
    let recorder = Arc::new(SettingsRecorder::default());
    let mut runtime = WriterRuntime::from_config(&cfg);
    runtime.spool_dir = root.clone();
    runtime.trace_landing_inserters = 1;
    let writer = pulsus_write::TraceWriter::with_inserters_and_runtime(
        Arc::new(NoopInserter),
        Arc::new(NoopInserter),
        recorder.clone(),
        runtime,
        pulsus_write::TraceWriterTables::traces_default(),
    );
    let (parsed, landed) = one_span_trace_push();
    writer
        .admit_flush(parsed, landed, pulsus_write::PushHeaders::default())
        .expect("the queue has room")
        .await
        .expect("the old path's two no-op inserts commit");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while recorder.calls() == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "the writer never handed a landing block over"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    writer.shutdown(Duration::from_secs(5)).await;
    std::fs::remove_dir_all(&root).ok();

    let (table, handed) = recorder.first();
    assert_eq!(table, "trace_landing");
    let handed_token = handed
        .iter()
        .find(|(k, _)| k == "insert_deduplication_token")
        .map(|(_, v)| v.clone())
        .expect("every landing insert carries a minted token");
    let want = QuerySettings::trace_landing_insert(&handed_token, cfg.trace_landing_max_rows);
    let mut want_entries: Vec<(String, String)> = want
        .entries()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let mut got_entries = handed;
    want_entries.sort();
    got_entries.sort();
    assert_eq!(
        got_entries, want_entries,
        "the writer hands over the trace constructor's own entries, at the \
         configured row ceiling"
    );
}

/// One recorded call: the table it named and the settings it carried.
type RecordedCall = (String, Vec<(String, String)>);

/// A `BlockInserter` that records the table and settings of every call and
/// answers `Ok`.
#[derive(Default)]
struct SettingsRecorder {
    calls: std::sync::Mutex<Vec<RecordedCall>>,
}

impl SettingsRecorder {
    fn calls(&self) -> usize {
        self.calls.lock().expect("recorder mutex poisoned").len()
    }

    fn record(&self, table: &str, settings: &QuerySettings) {
        self.calls.lock().expect("recorder mutex poisoned").push((
            table.to_string(),
            settings
                .entries()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        ));
    }

    fn first(&self) -> RecordedCall {
        self.calls
            .lock()
            .expect("recorder mutex poisoned")
            .first()
            .cloned()
            .expect("at least one call")
    }
}

impl<R: pulsus_clickhouse::ChRow> BlockInserter<R> for SettingsRecorder {
    fn insert<'a>(
        &'a self,
        table: &'a str,
        _rows: &'a [R],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ChError>> + Send + 'a>> {
        self.record(table, &QuerySettings::new());
        Box::pin(async { Ok(()) })
    }

    fn insert_with<'a>(
        &'a self,
        table: &'a str,
        _rows: &'a [R],
        extra: &'a QuerySettings,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ChError>> + Send + 'a>> {
        self.record(table, extra);
        Box::pin(async { Ok(()) })
    }
}

/// Every query parameter name in a request target, in the order it appears.
fn param_keys(target: &str) -> Vec<String> {
    target
        .split_once('?')
        .map(|(_, q)| q)
        .unwrap_or_default()
        .split('&')
        .filter_map(|pair| pair.split_once('=').map(|(k, _)| k.to_string()))
        .collect()
}

/// One trace push of one span, decoded both ways exactly as a handler runs
/// it.
fn one_span_trace_push() -> (pulsus_write::ParsedTraces, pulsus_write::ParsedTraceLanding) {
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    const TS: i64 = 1_760_000_000_000_000_000;
    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("checkout".to_string())),
                    }),
                    key_strindex: 0,
                }],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: vec![0xaa; 16],
                    span_id: vec![0xbb; 8],
                    name: "GET /api".to_string(),
                    start_time_unix_nano: TS as u64,
                    end_time_unix_nano: TS as u64 + 1_000_000,
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    (
        pulsus_write::parse_traces(&req, TS).expect("the old path's decode"),
        pulsus_write::parse_trace_landing(&req, TS).expect("the landing decode"),
    )
}
