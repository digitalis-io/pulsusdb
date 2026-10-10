//! Integration tests against a real ClickHouse server.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1` so plain `cargo test --workspace`
//! stays hermetic (no network/container dependency) in CI. To run these:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:26.8.21.10
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

#[path = "common/exception_frame.rs"]
mod exception_frame;

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
    let table = &pulsus_testkit::test_ident("pulsus_clickhouse_it_roundtrip");

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
/// **It is a regression gate, on the only version we run** (issue #376).
/// On **26.3.17.110** the server keeps the already-written output, the
/// crate returns the whole body (`collect_bad_response`,
/// `vendor/clickhouse/src/response.rs:127-188`), and without the patch the
/// message is `\u{1}\u{1}v\u{6}StringCode: 396. …`, whose code sits past
/// byte 0 where nothing can be trusted.
///
/// It used to be described as "a pin on 24.8, a gate on 26.3", because
/// 24.8.14.39 discarded its not-yet-flushed output buffer when it turned
/// the response into an error, so the message began at `Code:` and the
/// pre-#382 read passed too. The floor is 26.3 now, so only the gate leg
/// exists; the hermetic discriminating cases stay in
/// `pulsus_clickhouse::error`'s
/// `the_code_the_patch_puts_at_byte_zero_is_what_gets_read`.
///
/// **What it does NOT say anything about**, stated so it is not read as
/// more than it is: this is the BUFFERED path (HTTP 500 with the
/// exception-code header). The streaming path — HTTP 200, output already on
/// the socket — is a different function and was issue #412's subject. That
/// is fixed now, in the same vendored crate
/// (`vendor/clickhouse/PATCHES.md` §2), and gated by the four tests below
/// this one plus `tests/mock_clickhouse.rs`. The 24.8 record of why the
/// floor moved stays pinned hermetically by
/// `on_24_8_a_streamed_forgery_reaches_byte_zero_and_is_read_issue_412`,
/// which keeps its name and its bytes.
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
    cfg.database = pulsus_testkit::test_db("pulsus_clickhouse_it_missing_db");
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

/// **A client deadline the metadata read cannot outrun keeps its own class.**
/// A 1ns deadline is exceeded by any network round trip, and the first round
/// trip an insert makes is the target's column-metadata read — so on a real
/// server this deadline is always cut in acquisition, with the insert request
/// never opened. Nothing can have committed, so the error stays the retryable
/// `Timeout` it is and the caller keeps the pre-send resend it is entitled to.
/// Reported as `InsertUncertain` instead, a log or trace writer would file a
/// block that never left the process as "may have been stored" and stop
/// retrying a stall it should retry (issue #603 code review round 3).
///
/// This is the half a live server settles: that the metadata read is a genuine
/// round trip a deadline can cut. The other half — the same deadline once the
/// insert request is open, which IS uncertainty — needs a server that reads a
/// request and never answers, so both halves are pinned together against the
/// hermetic mock in `pulsus-write`'s `landing_insert_settings.rs`.
///
/// **This case asserted the opposite** while the client classified every
/// deadline as post-send (issue #3 fix plan, finding 2: a deadline must not
/// reach the caller as a bare retryable `Timeout` a caller would duplicate the
/// block on). That reading held for the phase it was written about — write and
/// end, where the block may be on the wire — and was applied to this one,
/// which cannot reach it. The rule it stated is unchanged and now lives where
/// it belongs: the two cases named above.
#[tokio::test]
async fn a_client_deadline_the_metadata_read_cannot_outrun_keeps_its_own_class() {
    skip_unless_live!();
    let table = &pulsus_testkit::test_ident("pulsus_clickhouse_it_insert_timeout");

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
    // (unlike the SELECT-based deadline test above), so this drives the
    // client-side deadline by making an unrealistically small one (1ns)
    // certain to be exceeded by the mandatory insert-time schema-metadata
    // fetch, which is the first thing the insert does (validation is enabled
    // by default on a fresh client with an empty metadata cache).
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

    // Load-bearing assertions: the class the phase earns, and the
    // retryability the resend loop reads. `InsertUncertain` here would be a
    // block reported "may have committed" that provably never left the
    // process.
    assert!(
        matches!(err, ChError::Timeout(_)),
        "expected the deadline's own Timeout, not a downgrade to uncertainty, \
         got {err:?}"
    );
    assert!(
        err.is_retryable(),
        "a deadline proven to precede transmission keeps its retryable class: \
         {err:?}"
    );
}

// === Issue #412: a tagged response's result bytes are never searched ===
//
// AC1-AC4 and AC13 below are the **protocol** gates: they run against the real
// server, so they establish both that our parser is right and that ClickHouse
// still frames its exceptions the way the hermetic mock
// (`tests/mock_clickhouse.rs`) assumes. The client-parser gates for chunk
// splits we cannot make a real server produce live there, not here.

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct OneCol {
    v: String,
}

/// A row whose LAST projected column is a long byte string, which is the
/// shape this driver regression is about.
///
/// It was the trace point read's own projection until issue #587 deleted
/// that statement; the fetch's span tuple ends in a byte-string element
/// too (`traces/spans/fetch.rs`'s `scope_attrs_other`, read as
/// `serde_bytes::ByteBuf`), so the shape is still production's. The column
/// here is `String` rather than a byte buffer — the same RowBinary
/// length-prefixed byte string on the wire, readable as `String` because
/// this fixture's bytes are UTF-8.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct SpanShapedRow {
    trace_id: [u8; 16],
    span_id: [u8; 8],
    parent_id: [u8; 8],
    payload_type: i8,
    kind: i8,
    payload: String,
}

/// `logql::rows::SampleRow`'s column order (`logql/rows.rs:34-39`).
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct SampleShapedRow {
    fingerprint: u64,
    timestamp_ns: i64,
    body: String,
    structured_metadata: String,
}

/// **AC1 — the headline defect, on the real server.** A query that SUCCEEDS,
/// whose last row ends `))\n`, must deliver its row.
///
/// Measured on `cb9524c` against 26.3.17.110: `rows=0` and
/// `ChError::Server { code: 210 }` — the tenant's own bytes, read back as a
/// server exception. 210 is in `RETRYABLE_SERVER_CODES` and the query had not
/// failed at all, so the fabrication was retried and demoted a healthy
/// endpoint through `PooledConn::report_transport_failure`.
///
/// The literal's byte length is asserted alongside the behaviour: without the
/// trailing `\n` and the `(official build))` tail it stops matching what
/// `extract_exception_old` keys on and the test goes vacuous.
#[tokio::test]
async fn a_successful_read_whose_last_row_ends_in_close_parens_is_not_an_error() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");

    let expected = exception_frame::forged_line();
    let sql = format!(
        "SELECT concat('{}', '\\n') AS v",
        exception_frame::FORGED_BODY
    );
    let mut stream = client
        .query_stream::<OneCol>(&sql, &QuerySettings::new())
        .await
        .expect("query_stream");

    use futures::StreamExt;
    let mut rows = Vec::new();
    while let Some(item) = stream.next().await {
        rows.push(item.expect("a successful query must not yield an error"));
    }
    assert_eq!(rows.len(), 1, "exactly the row the server returned");
    assert_eq!(rows[0].v, expected, "byte-for-byte, not merely non-empty");
    assert_eq!(
        rows[0].v.len(),
        79,
        "the fixture still has its `))\\n` tail"
    );
}

/// **AC2 — the reachability criterion**, on a projection whose last column
/// is a long byte string rather than a synthetic one. The trace fetch's
/// span tuple ends in `scope_attrs_other`, a byte buffer holding whatever
/// the sender sent, so the last bytes of a fetch's last row are tenant
/// bytes with no alignment involved. One row whose trailing bytes end
/// `))\n` returned `rows=0` + `Code: 210` on `cb9524c`.
#[tokio::test]
async fn a_trace_shaped_read_whose_payload_ends_in_close_parens_delivers_its_span() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let table = &pulsus_testkit::test_ident("pulsus_clickhouse_it_412_spans");

    client
        .execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS {table} (
                    trace_id FixedString(16), span_id FixedString(8),
                    parent_id FixedString(8), payload_type Int8, kind Int8,
                    payload String
                ) ENGINE = MergeTree ORDER BY (trace_id, span_id)"
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

    let payload = exception_frame::forged_line();
    client
        .execute(
            &format!(
                "INSERT INTO {table} VALUES (
                    unhex('4bf92f3577b34da6a3ce929d0e0e4736'), unhex('00f067aa0ba902b7'),
                    unhex('0000000000000000'), 0, 1, '{}\\n')",
                exception_frame::FORGED_BODY
            ),
            &QuerySettings::new(),
            Idempotency::NonIdempotent,
        )
        .await
        .expect("insert the span");

    let sql = format!(
        "SELECT trace_id, span_id, parent_id, payload_type, kind, payload\n\
         FROM {table}\n\
         WHERE trace_id = unhex('4bf92f3577b34da6a3ce929d0e0e4736')"
    );
    let mut stream = client
        .query_stream::<SpanShapedRow>(&sql, &QuerySettings::new())
        .await
        .expect("query_stream");

    use futures::StreamExt;
    let mut rows = Vec::new();
    while let Some(item) = stream.next().await {
        rows.push(item.expect("the span must be delivered, not turned into an error"));
    }
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].payload, payload);

    client
        .execute(
            &format!("DROP TABLE IF EXISTS {table}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop table");
}

/// **AC3 — the LogQL row shape, swept across the LZ4 block boundary.**
///
/// `SampleRow`'s last column is JSON, so the deterministic end-of-row case
/// (AC1/AC2) does not apply and the mid-block cut does: ClickHouse flushes an
/// LZ4 block at exactly `max_compress_block_size` = 1 048 576 **uncompressed**
/// bytes, ignoring row boundaries. The `\n` that trips the old search is not
/// in the log line at all — it is the **RowBinary varint length prefix of the
/// next column**, which is `0x0A` whenever that column is exactly 10 bytes
/// (`{"a":"bc"}` is 10 bytes). A `body` ending `))` plus a 10-byte
/// `structured_metadata` writes `))\n` into the row.
///
/// Measured on `cb9524c`: pads **15 and 48** of 0..=63 fail with `rows=0` +
/// `Code: 210`; the other 62 pass. **The test is non-vacuous only as a sweep
/// and must not be narrowed to one pad** — which pad lands on the boundary is
/// a function of the row width, so a single pad would go silently vacuous on
/// any fixture edit.
#[tokio::test]
async fn a_log_shaped_read_survives_every_block_boundary_alignment() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let table = &pulsus_testkit::test_ident("pulsus_clickhouse_it_412_samples");

    const ROWS: usize = 30_000;
    const META: &str = "{\"a\":\"bc\"}"; // exactly 10 bytes -> varint prefix 0x0A
    assert_eq!(META.len(), 10, "the 0x0A length prefix is the whole point");

    for pad in 0..=63usize {
        client
            .execute(
                &format!(
                    "CREATE TABLE IF NOT EXISTS {table} (
                        fingerprint UInt64, timestamp_ns Int64, body String,
                        structured_metadata String
                    ) ENGINE = MergeTree ORDER BY fingerprint"
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

        let body = format!("{}{}", "p".repeat(pad), exception_frame::FORGED_BODY);
        let rows: Vec<SampleShapedRow> = (0..ROWS)
            .map(|i| SampleShapedRow {
                fingerprint: i as u64,
                timestamp_ns: 1_700_000_000_000_000_000 + i as i64,
                body: body.clone(),
                structured_metadata: META.to_string(),
            })
            .collect();
        client
            .insert_block(table, &rows)
            .await
            .unwrap_or_else(|e| panic!("insert pad {pad}: {e:?}"));

        let sql = format!(
            "SELECT fingerprint, timestamp_ns, body, structured_metadata \
             FROM {table} ORDER BY fingerprint"
        );
        let mut stream = client
            .query_stream::<SampleShapedRow>(&sql, &QuerySettings::new())
            .await
            .expect("query_stream");

        use futures::StreamExt;
        let mut got = 0usize;
        while let Some(item) = stream.next().await {
            match item {
                Ok(_) => got += 1,
                Err(e) => panic!(
                    "pad {pad}: a successful query must not fail (got {got} of {ROWS} rows): {e:?}"
                ),
            }
        }
        assert_eq!(got, ROWS, "pad {pad}");
    }

    client
        .execute(
            &format!("DROP TABLE IF EXISTS {table}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop table");
}

/// **AC4 — the control the fix must not break.** A real exception raised after
/// output has streamed still carries its own code, sliced out of the tagged
/// trailer by declared length.
///
/// The failing expression has to depend on the row: `intDiv(1, 0)` with
/// literal arguments is constant-folded and fails before any output, giving a
/// buffered 500 with no trailer at all.
#[tokio::test]
async fn a_streamed_exception_after_output_still_carries_its_real_code() {
    skip_unless_live!();
    let mut cfg = test_config();
    cfg.query_timeout = Duration::from_secs(60);
    let client = ChClient::new(cfg).await.expect("connect");

    let mut stream = client
        .query_stream::<OneCol>(
            "SELECT concat(toString(number), toString(throwIf(number = 2500000, 'boom'))) AS v \
             FROM numbers(3000000)",
            &QuerySettings::new(),
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

    match failure.expect("throwIf must trip") {
        ChError::Server { code, message } => {
            assert_eq!(
                code, 395,
                "the real code, not 0 and not a forgery: {message:?}"
            );
            assert!(
                message.contains("FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"),
                "the body must be the throwIf exception: {message:?}"
            );
        }
        other => panic!("expected ChError::Server, got {other:?}"),
    }
}

/// **AC13 — the mock's frame layout, checked against the live server on every
/// CI run.**
///
/// A hermetic mock whose byte layout is asserted only against our own reading
/// of our own code is one edit away from being a false gate. So the shared
/// [`exception_frame::frame_bytes`] builder — the one
/// `tests/mock_clickhouse.rs` builds every fixture with — is replayed against a
/// frame captured from a real streamed exception here, byte for byte. Nothing
/// is committed as a fixture; the capture is regenerated each run, so it
/// cannot go stale.
///
/// Deliberately **fails rather than skips** when the response comes back
/// buffered (HTTP 500 + `X-ClickHouse-Exception-Code`, no trailer) — a skip
/// here would be exactly the vacuum this gate exists to prevent.
///
/// The message is recovered from the **closing** trailer's declared length,
/// which is the rule `extract_exception_new` applies, so the rebuild is not
/// circular on the opening's field order. That matters: the frame's two ends
/// are in opposite orders — opening is marker-then-tag, closing is
/// tag-then-marker.
///
/// Raw `std::net::TcpStream` rather than the client, because the client hands
/// back the parsed message and this needs the wire bytes.
#[test]
fn the_mock_frame_layout_matches_a_real_streamed_exception() {
    skip_unless_live!();
    use std::io::{Read, Write};

    let cfg = test_config();
    let sql = "SELECT concat(toString(number), toString(throwIf(number = 2500000, 'boom'))) AS v \
               FROM numbers(3000000)";
    let mut sock = std::net::TcpStream::connect((cfg.server.as_str(), cfg.http_port))
        .expect("connect to the live server");
    sock.set_read_timeout(Some(Duration::from_secs(60))).ok();
    let req = format!(
        "POST /?default_format=RowBinary HTTP/1.1\r\nHost: {}:{}\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n{sql}",
        cfg.server,
        cfg.http_port,
        sql.len()
    );
    sock.write_all(req.as_bytes()).expect("write request");
    sock.flush().ok();

    let mut raw = Vec::new();
    sock.read_to_end(&mut raw).expect("read response");

    let head_end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| i + 4)
        .expect("a complete response head");
    let head = String::from_utf8_lossy(&raw[..head_end]).to_string();
    let head_lower = head.to_ascii_lowercase();

    assert!(
        head.starts_with("HTTP/1.1 200 "),
        "the exception must arrive AFTER output has streamed, not as a buffered \
         500 — a buffered response would leave this gate vacuous. Head: {head:?}"
    );
    assert!(
        !head_lower.contains("x-clickhouse-exception-code:"),
        "a streamed exception carries no code header (that is the whole reason \
         the trailer exists). Head: {head:?}"
    );
    let tag = head
        .lines()
        .find_map(|l| {
            let (name, value) = l.split_once(':')?;
            name.eq_ignore_ascii_case("X-ClickHouse-Exception-Tag")
                .then(|| value.trim().to_string())
        })
        .unwrap_or_else(|| panic!("X-ClickHouse-Exception-Tag must be present. Head: {head:?}"));

    let body = if head_lower.contains("transfer-encoding: chunked") {
        dechunk(&raw[head_end..])
    } else {
        raw[head_end..].to_vec()
    };

    let offsets = exception_frame::exc_open_offsets(&body);
    assert_eq!(
        offsets.len(),
        2,
        "a well-formed frame contains the open marker exactly twice — its \
         opening, and the `\\r\\n` before the closing marker (found {offsets:?})"
    );
    let frame = &body[offsets[0]..];
    assert!(frame.ends_with(exception_frame::EXC_CLOSE));

    let (message, declared) = exception_frame::message_from_closing_trailer(frame, &tag);
    assert_eq!(
        declared,
        message.len() + 1,
        "the declared length counts the message's terminating newline"
    );
    assert!(
        message.starts_with("Code: 395. DB::Exception:"),
        "the capture must be the throwIf exception: {message:?}"
    );

    let rebuilt = exception_frame::frame_bytes(&message, &tag);
    assert_eq!(
        rebuilt.len(),
        frame.len(),
        "the shared builder must reproduce the captured frame's length"
    );
    assert_eq!(
        rebuilt, frame,
        "the shared builder must reproduce the captured frame BYTE FOR BYTE; \
         if the server's framing, field order or delimiters moved, every \
         hermetic gate in tests/mock_clickhouse.rs is now testing a shape the \
         server no longer emits"
    );
}

/// Undoes HTTP/1.1 chunked transfer encoding. The frame can straddle chunk
/// boundaries, so searching the raw stream without de-chunking would find
/// framing bytes inside the frame.
fn dechunk(mut rest: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let Some(eol) = rest.windows(2).position(|w| w == b"\r\n") else {
            break;
        };
        let size_line = String::from_utf8_lossy(&rest[..eol]);
        let size_hex = size_line.split(';').next().unwrap_or("").trim();
        let Ok(size) = usize::from_str_radix(size_hex, 16) else {
            break;
        };
        rest = &rest[eol + 2..];
        if size == 0 || rest.len() < size {
            break;
        }
        out.extend_from_slice(&rest[..size]);
        rest = &rest[size..];
        if rest.starts_with(b"\r\n") {
            rest = &rest[2..];
        }
    }
    out
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

/// **The gate that proves `vendor/clickhouse/PATCHES.md` §3 is still doing
/// its job**: a `Vec` against a `JSON` column is accepted by the driver's
/// insert-side schema validator.
///
/// Upstream's `SerdeType::Seq(_)` arm matches `Array`, `Map`, `Ring`,
/// `Polygon`, `MultiPolygon`, `LineString` and `MultiLineString` and falls to
/// `err_on_schema_mismatch` for everything else, so a `Vec` against a `JSON`
/// column was `Error::SchemaMismatch`. The `JSON` type's RowBinary form is a
/// path count then one `(path, type tag, value)` per path, and the writer
/// owns those bytes, so the arm returns `Ok(None)` and the sequence is not
/// validated.
///
/// **It is live and not hermetic, for the reason PATCHES.md's own Gates
/// section gives**: the validating serializer runs only with column metadata
/// the client reads off the server, and the vendored crate is a
/// `[patch.crates-io]` path source rather than a workspace member, so a
/// `#[test]` inside it is never compiled by `cargo test --workspace` and
/// would be a case that cannot fail.
///
/// Three assertions, because the arm has to admit one thing and keep
/// refusing the rest: the `JSON` column takes the sequence and the value
/// reads back, an `Array(Tuple(…))` column still takes it, and a scalar
/// column still refuses it. **One table per shape**, because the driver
/// refuses an insert that leaves a non-default column out.
///
/// **The patch's stated limit is what this cannot show.** `Ok(None)` stops
/// validation for the whole sequence, so a wrong type tag reaches the server
/// rather than the driver; the byte-exact cases in
/// `crates/pulsus-write/src/writer/trace_json.rs` stand in for that.
#[tokio::test]
async fn a_vec_against_a_json_column_is_accepted() {
    skip_unless_live!();
    use futures::StreamExt;

    let client = ChClient::new(test_config()).await.expect("connect");

    /// The `JSON` column's wire form for `{"k":7}`: one path, the path
    /// string, the `Int64` type tag and eight little-endian bytes.
    /// `serialize_tuple` writes no framing, so the nested tuple is the tag
    /// followed by the value.
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct JsonRow {
        js: Vec<(String, (u8, i64))>,
    }

    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct ArrRow {
        arr: Vec<(String, u8)>,
    }

    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct ScalarRow {
        n: Vec<(String, u8)>,
    }

    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct OneInt {
        /// `js.k.:Int64` is `Nullable(Int64)` — a subcolumn read of a
        /// `JSON` path is null where the path is absent or holds another
        /// type.
        v: Option<i64>,
    }

    async fn fresh(client: &ChClient, table: &str, columns: &str) {
        for sql in [
            format!("DROP TABLE IF EXISTS {table}"),
            format!("CREATE TABLE {table} ({columns}) ENGINE = MergeTree ORDER BY tuple()"),
        ] {
            client
                .execute(&sql, &QuerySettings::new(), Idempotency::Idempotent)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
    }

    let json_table = &pulsus_testkit::test_ident("pulsus_clickhouse_it_json_seq");
    let arr_table = &pulsus_testkit::test_ident("pulsus_clickhouse_it_json_seq_arr");
    let scalar_table = &pulsus_testkit::test_ident("pulsus_clickhouse_it_json_seq_scalar");

    fresh(&client, json_table, "js JSON").await;
    fresh(&client, arr_table, "arr Array(Tuple(String, UInt8))").await;
    fresh(&client, scalar_table, "n UInt8").await;

    client
        .insert_block(
            json_table,
            &[JsonRow {
                js: vec![("k".to_string(), (0x0a, 7i64))],
            }],
        )
        .await
        .expect("a Vec against a JSON column is the patch's whole point");

    let mut rows = client
        .query_stream::<OneInt>(
            &format!("SELECT js.k.:Int64 AS v FROM {json_table}"),
            &QuerySettings::new(),
        )
        .await
        .expect("read the stored path back");
    let stored = rows.next().await.expect("one row").expect("decode").v;
    drop(rows);
    assert_eq!(
        stored,
        Some(7),
        "the bytes the writer sent are what the server stored"
    );

    client
        .insert_block(
            arr_table,
            &[ArrRow {
                arr: vec![("k".to_string(), 1u8)],
            }],
        )
        .await
        .expect("the arms the patch leaves alone still admit a Vec");

    let err = client
        .insert_block(
            scalar_table,
            &[ScalarRow {
                n: vec![("k".to_string(), 1u8)],
            }],
        )
        .await
        .expect_err("a Vec against UInt8 is still refused by the driver");
    assert!(
        err.to_string().to_lowercase().contains("schema") || err.to_string().contains("UInt8"),
        "wanted a schema mismatch naming the column type, got {err}"
    );

    for table in [json_table, arr_table, scalar_table] {
        client
            .execute(
                &format!("DROP TABLE IF EXISTS {table}"),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("drop");
    }
}
