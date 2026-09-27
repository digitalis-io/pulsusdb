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
use pulsus_write::{MetricPoint, MetricSink, ParsedMetrics, PushHeaders, SeriesRef};

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
/// deduplication pins and the block-size ceiling are never sent — no retry
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
