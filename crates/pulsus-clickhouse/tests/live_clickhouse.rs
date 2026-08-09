//! Integration tests against a real ClickHouse server.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1` so plain `cargo test --workspace`
//! stays hermetic (no network/container dependency) in CI. To run these:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-clickhouse --test live_clickhouse
//! podman rm -f pulsus-ch-test
//! ```
//!
//! (`docker` works identically if available instead of `podman`.) Connection
//! parameters can be overridden via `PULSUS_TEST_CH_HOST` /
//! `PULSUS_TEST_CH_HTTP_PORT` if the default `localhost:19123` does not fit
//! your environment.

use std::time::Duration;

use pulsus_clickhouse::{
    ChClient, ChConnConfig, ChError, ChProto, Idempotency, QuerySettings, Row,
};

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping when
/// the gate is absent in a live CI job, so a lost `env:` block reddens the
/// build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

fn test_config() -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        // The bare `clickhouse/clickhouse-server` image only pre-creates
        // `default`; `PULSUS_TEST_CH_DATABASE=pulsus` if your test server
        // already provisions the real `pulsus` database (docs/configuration.md §2).
        database: std::env::var("PULSUS_TEST_CH_DATABASE")
            .unwrap_or_else(|_| "default".to_string()),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(10),
        ..ChConnConfig::default()
    }
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct TestRow {
    fingerprint: u64,
    unix_milli: i64,
    value: f64,
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-clickhouse/tests/live_clickhouse.rs for setup)"
            );
            return;
        }
    };
}

#[tokio::test]
async fn ping_succeeds_against_a_live_server() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    client.ping().await.expect("ping");
}

#[tokio::test]
async fn insert_block_then_query_stream_round_trips_rows() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let table = "pulsus_clickhouse_it_roundtrip";

    client
        .execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS {table} (
                    fingerprint UInt64, unix_milli Int64, value Float64
                ) ENGINE = MergeTree ORDER BY (fingerprint, unix_milli)"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("create table");
    client
        .execute(
            &format!("TRUNCATE TABLE {table}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("truncate");

    // fingerprint > 2^63: the unsigned round-trip gate from the M0 spike
    // (docs/decisions/0001-clickhouse-client.md) must hold in the shipped
    // wrapper too, not just the benchmark harness.
    let rows = vec![
        TestRow {
            fingerprint: 0xFFFF_FFFF_FFFF_FFF1,
            unix_milli: 1_700_000_000_000,
            value: 1.5,
        },
        TestRow {
            fingerprint: 42,
            unix_milli: 1_700_000_000_100,
            value: 2.5,
        },
    ];
    client
        .insert_block(table, &rows)
        .await
        .expect("insert_block");

    let sql = format!("SELECT fingerprint, unix_milli, value FROM {table} ORDER BY fingerprint");
    let mut stream = client
        .query_stream::<TestRow>(&sql, &QuerySettings::new())
        .await
        .expect("query_stream");

    use futures::StreamExt;
    let mut got = Vec::new();
    while let Some(row) = stream.next().await {
        got.push(row.expect("row decode"));
    }
    got.sort_by_key(|r| r.fingerprint);
    let mut expected = rows;
    expected.sort_by_key(|r| r.fingerprint);
    assert_eq!(got, expected);
    assert_eq!(got[1].fingerprint, 0xFFFF_FFFF_FFFF_FFF1);
    assert!(got[1].fingerprint > (1u64 << 63));
}

/// Issue #382: a server exception raised after the query has already
/// written output must still classify by its numeric code, so a scan
/// budget stays a 422 `query_too_broad` and never becomes a 500.
///
/// The query and settings are the exact shape measured to produce the
/// defect: a compressible projection with the default response buffering,
/// so ClickHouse writes the `RowBinaryWithNamesAndTypes` column header,
/// then trips `max_result_bytes` and emits the exception into the same
/// (compressed) response. `randomPrintableASCII` padding does NOT
/// reproduce it — an incompressible payload takes the crate's streaming
/// extractor instead, which re-anchors the message at `Code:`
/// (`extract_exception_old`, `vendor/clickhouse/src/response.rs:392-401`)
/// and so hides the defect.
///
/// **This is the end-to-end gate on the ADR 0007 vendored patch**
/// (`vendor/clickhouse/PATCHES.md`), which is what makes the code readable
/// at all on this path: it puts the `X-ClickHouse-Exception-Code` value at
/// byte 0 of the `BadResponse` message. Revert that patch and this test
/// fails with `code: 0` on 26.3. It is not the only gate on the vendored
/// change — `pulsus-read`'s `traces_search_explain` catches the same revert
/// on 26.3 — but it is the cheapest, and the only one in this suite.
///
/// **What it proves depends on the server it runs against**, and that is
/// deliberate rather than papered over:
///
/// - On **26.3.17.110** it is the regression gate. The server keeps the
///   already-written output, the crate returns the whole body
///   (`collect_bad_response`, `vendor/clickhouse/src/response.rs:127-188`),
///   and without the
///   patch the message is `\u{1}\u{1}v\u{6}StringCode: 396. …`, whose code
///   sits past byte 0 where nothing can be trusted.
/// - On **24.8.14.39** — the version this suite's CI step runs — it is a
///   pin only: that server discards its not-yet-flushed output buffer
///   when it turns the response into an error, so the message begins at
///   `Code:` and the pre-#382 read passes too. Named as a pin, not
///   counted as a gate. The hermetic discriminating cases live in
///   `pulsus_clickhouse::error`'s
///   `the_code_the_patch_puts_at_byte_zero_is_what_gets_read`.
///
/// Neither leg says anything about 24.8's **streaming** path, which stays
/// forgeable by a tenant literal echoed into the exception description and
/// is issue #412, closing with #376. Pinned hermetically by
/// `on_24_8_a_streamed_forgery_reaches_byte_zero_and_is_read_issue_412`.
///
/// Read-only against `numbers()`, so this suite's existing CI step (which
/// runs against the bare image's `default` database) needs no new fixture
/// and no new step.
#[tokio::test]
async fn a_result_limit_tripped_after_output_has_been_written_carries_its_code() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");

    #[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
    struct OneCol {
        v: String,
    }

    let mut stream = client
        .query_stream::<OneCol>(
            "SELECT toString(number) AS v FROM numbers(100000000)",
            &QuerySettings::new()
                .set("max_result_bytes", 1_000_000u64)
                .set("result_overflow_mode", "throw"),
        )
        .await
        .expect("query_stream");

    use futures::StreamExt;
    let mut failure = None;
    while let Some(item) = stream.next().await {
        if let Err(e) = item {
            failure = Some(e);
            break;
        }
    }

    let err = failure.expect("the result-byte limit must trip");
    match err {
        ChError::Server { code, message } => {
            assert_eq!(
                code, 396,
                "TOO_MANY_ROWS_OR_BYTES must classify by code, not fall back to 0 \
                 (message: {message:?})"
            );
            assert!(
                message.contains("TOO_MANY_ROWS_OR_BYTES"),
                "the body must be the result-limit exception, not some other \
                 failure that happens to carry a code: {message:?}"
            );
        }
        other => panic!("expected ChError::Server, got {other:?}"),
    }
}

#[tokio::test]
async fn query_stream_lease_is_released_on_drop_before_exhaustion() {
    skip_unless_live!();
    // pool_size = 1: if a dropped-mid-stream lease were not released, the
    // second `query_stream` call below would hang forever on `pool.get()`.
    let mut cfg = test_config();
    cfg.pool_size = 1;
    let client = ChClient::new(cfg).await.expect("connect");

    {
        let mut stream = client
            .query_stream::<TestRow>(
                "SELECT number AS fingerprint, number AS unix_milli, 0.0 AS value \
                 FROM system.numbers LIMIT 1000",
                &QuerySettings::new(),
            )
            .await
            .expect("query_stream");
        use futures::StreamExt;
        // Consume one row, then drop the stream mid-read (early cancellation).
        let _ = stream.next().await;
    }

    // If the lease were leaked, this would block until PULSUS_QUERY_TIMEOUT.
    tokio::time::timeout(Duration::from_secs(5), client.ping())
        .await
        .expect("pool.get() did not hang — lease was released on drop")
        .expect("ping");
}

#[tokio::test]
async fn execute_rejects_ddl_against_a_nonexistent_database_as_poison() {
    skip_unless_live!();
    let mut cfg = test_config();
    cfg.database = "pulsus_clickhouse_it_missing_db".to_string();
    // ChClient::new pings at startup; a missing database is itself a
    // startup-time poison error, which is the behavior under test.
    let result = ChClient::new(cfg).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn query_stream_enforces_overall_deadline_on_a_stalled_query() {
    skip_unless_live!();
    // A short client/server deadline against a query that deliberately
    // sleeps far longer than it (issue #3 fix plan, finding 2): the
    // overall stream deadline (a `tokio::time::Sleep` polled first in
    // `ChRowStream::poll_next`) must interrupt the still-running query
    // rather than block the lease forever.
    let mut cfg = test_config();
    cfg.query_timeout = Duration::from_millis(300);
    let client = ChClient::new(cfg).await.expect("connect");

    let started = std::time::Instant::now();
    let mut stream = client
        .query_stream::<TestRow>(
            "SELECT toUInt64(1) AS fingerprint, toInt64(2) AS unix_milli, 3.0 AS value \
             FROM system.one WHERE sleep(3) = 0",
            &QuerySettings::new(),
        )
        .await
        .expect("query_stream");

    use futures::StreamExt;
    let first = stream
        .next()
        .await
        .expect("the deadline must yield an error, not a silent empty stream");
    let err = first.expect_err("a query still sleeping past the deadline must not succeed");
    assert!(
        matches!(err, ChError::Timeout(_)),
        "expected the overall client-side stream deadline (ChError::Timeout), got {err:?}"
    );
    assert!(
        err.is_retryable(),
        "reads are idempotent: stream deadline timeouts stay retryable"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the 300ms deadline must cut the lease well before the query's 3s sleep would complete"
    );
}

#[tokio::test]
async fn insert_block_returns_insert_uncertain_when_the_client_deadline_fires() {
    skip_unless_live!();
    let table = "pulsus_clickhouse_it_insert_timeout";

    // Create the table with a normally-configured client; only the
    // `insert_block` attempt below uses the pathological deadline.
    let setup = ChClient::new(test_config()).await.expect("connect (setup)");
    setup
        .execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS {table} (
                    fingerprint UInt64, unix_milli Int64, value Float64
                ) ENGINE = MergeTree ORDER BY (fingerprint, unix_milli)"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("create table");

    // `insert_block` has no SQL surface to inject a literal `sleep()`
    // (unlike the SELECT-based deadline test above), so this proves the
    // same client-side `tokio::time::timeout` wrapper by making an
    // unrealistically small deadline (1ns) certain to be exceeded by any
    // real network round trip — including the mandatory insert-time
    // schema-metadata fetch (validation is enabled by default on a fresh
    // client with an empty metadata cache).
    let mut cfg = test_config();
    cfg.query_timeout = Duration::from_nanos(1);
    let client = ChClient::new(cfg).await.expect("connect (tiny deadline)");

    let rows = vec![TestRow {
        fingerprint: 1,
        unix_milli: 1,
        value: 1.0,
    }];
    let err = client
        .insert_block(table, &rows)
        .await
        .expect_err("insert_block must not silently succeed within a 1ns deadline");

    // Load-bearing assertion (issue #3 fix plan, finding 2): the failure
    // must be the non-retryable `InsertUncertain`, never a bare retryable
    // `Timeout` — a caller retrying on `is_retryable()` would otherwise
    // duplicate the (possibly partially-committed) block.
    assert!(
        matches!(err, ChError::InsertUncertain(_)),
        "expected InsertUncertain (uncertain commit fate), got {err:?}"
    );
    assert!(
        !err.is_retryable(),
        "InsertUncertain must never be retried (docs/schemas.md §2.2/§8)"
    );
}

#[tokio::test]
async fn clustered_reader_settings_do_not_change_query_result_shape() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let settings = QuerySettings::clustered_reader(false);
    let mut stream = client
        .query_stream::<TestRow>(
            "SELECT toUInt64(1) AS fingerprint, toInt64(2) AS unix_milli, 3.0 AS value",
            &settings,
        )
        .await
        .expect("query_stream with clustered_reader settings");
    use futures::StreamExt;
    let row = stream.next().await.expect("one row").expect("decode");
    assert_eq!(row.fingerprint, 1);
}
