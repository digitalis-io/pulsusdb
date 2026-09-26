//! What the **production** inserter puts on the wire, and how the production
//! insert path classifies what comes back (issue #603).
//!
//! Every case here drives `ChBlockInserter` — the adapter `MetricWriter::new`
//! builds — and `ChClient::insert_block_with` underneath it, against the
//! hermetic mock in `tests/common/mock_ch_insert.rs`. Nothing here uses a
//! `BlockInserter` mock: a test double that implements `insert_with` itself
//! proves only that the double does.

use std::sync::Arc;

use pulsus_clickhouse::{ChClient, ChError, QuerySettings};
use pulsus_write::writer::{BlockInserter, ChBlockInserter};

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

/// **A failure proven to precede transmission keeps its own class.** The
/// vendored client fetches the target's column metadata before it opens the
/// insert request, so an exception there is answered with none of the block
/// sent: it is genuine pre-commit poison and must stay the precise error, or
/// every bad statement would be reported as "may have committed" and the
/// claim would never be released for the client's retry.
#[tokio::test]
async fn a_failure_before_the_block_was_sent_keeps_its_own_class() {
    let mock = MockChInsert::start(
        DescribeAnswer::Exception {
            code: 60,
            text: "Table default.t does not exist",
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

    match err {
        ChError::Server { code, .. } => assert_eq!(code, 60, "the server's own code"),
        other => panic!("a pre-send failure must not be downgraded: {other:?}"),
    }
    assert!(
        mock.requests()
            .iter()
            .all(|r| !r.body.starts_with("INSERT INTO")),
        "no insert was ever opened: {:?}",
        mock.requests()
    );
}
