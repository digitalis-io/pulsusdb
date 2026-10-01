//! A **named** tuple column, inserted into and read back from a running
//! ClickHouse server (issue #585).
//!
//! `get_insert_metadata` runs `DESCRIBE TABLE` and calls `DataTypeNode::new`
//! on every column's type, and a SELECT response header carries the same
//! string in its compact form — so a named-tuple column is reached on both the
//! insert path and the read path, and both of them fail before the patch in
//! `vendor/clickhouse-types`. `named_tuple_types.rs` is the hermetic half,
//! against the parser; these are the two paths, against a server.
//!
//! **Not added to `live_clickhouse.rs`.** That suite runs read-only queries
//! against the image's pre-created `default` database and touches nothing;
//! each case here creates a table, so each creates and drops its own
//! database.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:26.3
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-clickhouse --test live_named_tuple
//! podman rm -f pulsus-ch-test
//! ```

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};

/// `true` when this suite should run. Skips cleanly on a developer machine
/// with no container; **panics** rather than skipping when the gate is absent
/// in a live CI job (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-clickhouse/tests/live_named_tuple.rs for setup)"
            );
            return;
        }
    };
}

fn base_config() -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: std::env::var("PULSUS_TEST_CH_DATABASE")
            .unwrap_or_else(|_| "default".to_string()),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(20),
        ..ChConnConfig::default()
    }
}

/// A fresh database of its own, and a client bound to it. `db` is already
/// composed by `pulsus_testkit::test_db` at the call site, so the prefix
/// reaches the name: two checkouts sharing one ClickHouse would otherwise
/// drop each other's data.
async fn fresh_db(db: &str) -> ChClient {
    let admin = ChClient::new(base_config()).await.expect("connect");
    for sql in [
        format!("DROP DATABASE IF EXISTS {db}"),
        format!("CREATE DATABASE {db}"),
    ] {
        admin
            .execute(&sql, &QuerySettings::new(), Idempotency::Idempotent)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    ChClient::new(ChConnConfig {
        database: db.to_string(),
        ..base_config()
    })
    .await
    .expect("connect to the test database")
}

async fn drop_db(db: &str) {
    let admin = ChClient::new(base_config()).await.expect("connect");
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the test database");
}

/// The named-tuple table L1, L3, L4 and L5 share.
async fn named_tuple_table(client: &ChClient) {
    client
        .execute(
            "CREATE TABLE l1 (c Array(Tuple(a Int64, b String))) \
             ENGINE = MergeTree ORDER BY tuple()",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("create the named-tuple table");
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct NamedTupleRow {
    c: Vec<(i64, String)>,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct OneFieldRow {
    c: Vec<(i64,)>,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct WrongFirstFieldRow {
    c: Vec<(String, String)>,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct TextRow {
    s: String,
}

async fn scalar(client: &ChClient, sql: &str) -> String {
    let mut stream = client
        .query_stream::<TextRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("scalar failed: {e}\nSQL:\n{sql}"));
    stream.next().await.expect("one row").expect("decode").s
}

/// **L1.** An insert into a named-tuple column succeeds, and the element the
/// name points at reads back.
///
/// Before the patch the `Result` is
/// `Decode("error while parsing columns header from the response: type
/// parsing error: Unknown data type: \n    a Int64")` — the newline because
/// `DESCRIBE TABLE` returns the pretty form. The assertion is on the
/// `Result`, and nothing unwraps.
#[tokio::test]
async fn l1_an_insert_into_a_named_tuple_column_succeeds() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_named_tuple_it_l1");
    let client = fresh_db(&db).await;
    named_tuple_table(&client).await;

    let rows = vec![NamedTupleRow {
        c: vec![(1_i64, "x".to_string())],
    }];
    let r = client.insert_block("l1", &rows).await;
    assert!(r.is_ok(), "the insert returned {:?}", r.err());

    let got = scalar(&client, "SELECT c[1].b AS s FROM l1").await;
    assert_eq!(got, "x", "the named element did not read back");

    drop_db(&db).await;
}

/// **L2** *(pin)*. The same insert against a **positional** tuple column still
/// works. Green before the patch; it is what catches the named reading being
/// taken for a positional tuple.
#[tokio::test]
async fn l2_an_insert_into_a_positional_tuple_column_still_succeeds() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_named_tuple_it_l2");
    let client = fresh_db(&db).await;
    client
        .execute(
            "CREATE TABLE l2 (c Array(Tuple(Int64, String))) ENGINE = MergeTree ORDER BY tuple()",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("create the positional-tuple table");

    let rows = vec![NamedTupleRow {
        c: vec![(1_i64, "x".to_string())],
    }];
    let r = client.insert_block("l2", &rows).await;
    assert!(r.is_ok(), "the insert returned {:?}", r.err());

    let got = scalar(&client, "SELECT c[1].2 AS s FROM l2").await;
    assert_eq!(got, "x", "the positional element did not read back");

    drop_db(&db).await;
}

/// **L3.** A row whose element tuple has one field too few is refused by the
/// driver, with the element it never reached named. The validator descended
/// **into** the named tuple rather than refusing it whole, and the leftover
/// element's **type** is rendered through `Display`.
///
/// **On the read, not on the insert, and that is a correction to the design.**
/// `check_tuple_fully_validated` — the only producer of this message
/// (`vendor/clickhouse/src/rowbinary/validation.rs`) — has exactly one caller
/// in the crate, `rowbinary/de.rs`'s `next_element_seed`, so it is reached
/// when the last element of a sequence has been **de**serialized and never on
/// the way out. Measured with the row below on the insert path: the driver
/// accepts the short tuple, sends the block, and the server answers
/// `Code: 32 … Attempt to read after eof`, so an insert cannot carry this
/// message at all. The too-MANY direction is the one the insert path sees, in
/// the same cursor's `None` branch.
#[tokio::test]
async fn l3_an_element_tuple_short_of_a_field_is_refused() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_named_tuple_it_l3");
    let client = fresh_db(&db).await;
    named_tuple_table(&client).await;

    let rows = vec![NamedTupleRow {
        c: vec![(1_i64, "x".to_string())],
    }];
    let inserted = client.insert_block("l1", &rows).await;
    assert!(inserted.is_ok(), "the insert returned {:?}", inserted.err());

    let fetched = client
        .query_stream::<OneFieldRow>("SELECT c FROM l1", &QuerySettings::new())
        .await;
    let first = match fetched {
        Ok(mut stream) => stream.next().await,
        Err(e) => Some(Err(e)),
    };
    let msg = match &first {
        Some(Err(e)) => e.to_string(),
        other => format!("not an error: {other:?}"),
    };
    assert!(
        msg.contains("tuple was not fully (de)serialized"),
        "the read-back gave {msg}; wanted an error naming the unreached element"
    );
    assert!(
        msg.contains("missing elements: String"),
        "the read-back gave {msg}; wanted the leftover element's TYPE, rendered \
         through `Display`"
    );

    drop_db(&db).await;
}

/// **L4.** A row whose first element field is a `String` where the column
/// declares `Int64` is refused, and the message carries the element type and
/// the whole column type through `Display`.
#[tokio::test]
async fn l4_a_wrong_element_type_is_refused_by_the_element_it_is_wrong_for() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_named_tuple_it_l4");
    let client = fresh_db(&db).await;
    named_tuple_table(&client).await;

    let rows = vec![WrongFirstFieldRow {
        c: vec![("one".to_string(), "x".to_string())],
    }];
    let r = client.insert_block("l1", &rows).await;
    let msg = r
        .as_ref()
        .err()
        .map(ToString::to_string)
        .unwrap_or_default();
    assert!(
        msg.contains("nested ClickHouse type Int64 as"),
        "the insert returned {r:?}; wanted an error naming the element type it descended to"
    );

    drop_db(&db).await;
}

/// **L5.** The **read** path: a SELECT response header carries the compact
/// form of the same type, and `Array(Tuple(a Int64, b String))` does not parse
/// before the patch either. Nothing unwraps before its own assertion.
#[tokio::test]
async fn l5_a_named_tuple_column_reads_back_into_a_typed_row() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_named_tuple_it_l5");
    let client = fresh_db(&db).await;
    named_tuple_table(&client).await;

    let inserted = client
        .execute(
            "INSERT INTO l1 (c) VALUES ([(1, 'x')])",
            &QuerySettings::new(),
            Idempotency::NonIdempotent,
        )
        .await;
    assert!(
        inserted.is_ok(),
        "the literal-SQL insert returned {:?}",
        inserted.err()
    );

    let fetched = client
        .query_stream::<NamedTupleRow>("SELECT c FROM l1", &QuerySettings::new())
        .await;
    assert!(
        fetched.is_ok(),
        "the read-back returned {:?}",
        fetched.as_ref().err()
    );
    let mut stream = fetched.expect("asserted above");
    // The header parse is where this fails before the patch, and measured it
    // surfaces on the FIRST ROW rather than on `query_stream`'s own `Result`:
    // `query_stream` answers `Ok` and the row answers
    // `Decode("error while parsing columns header from the response: type
    // parsing error: Unknown data type: a Int64")`. So the row's `Result`
    // carries its own assertion too, and nothing unwraps before one.
    let first = stream.next().await;
    assert!(
        matches!(&first, Some(Ok(_))),
        "the read-back's first row was {:?}",
        first.as_ref().map(|r| r.as_ref().err())
    );
    let row = first.expect("asserted above").expect("asserted above");
    assert_eq!(row.c, vec![(1_i64, "x".to_string())]);

    drop_db(&db).await;
}
