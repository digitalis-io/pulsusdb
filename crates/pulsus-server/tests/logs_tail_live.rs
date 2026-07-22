//! Live end-to-end tests for `GET /api/logs/v1/tail` (issue #74): spawns
//! the real `pulsusdb` binary against a live ClickHouse (same podman
//! harness as `logs_api_live.rs`), seeds `log_streams`/`log_samples`
//! directly via `ChClient`, and drives the WebSocket with a minimal
//! hand-rolled client (bare `TcpStream` — the repo's no-HTTP-client-dep
//! idiom, extended with RFC 6455 frame parsing).
//!
//! Coverage (the adjudicated plan's live ACs):
//! - streaming through a real pipeline, ascending order, exactly-once;
//! - AC2 keyset semantics, deterministically at the ENGINE level
//!   (`LogQlEngine::tail_poll` driven page by page): a tie group split by
//!   `LIMIT` across pages delivered exactly once via `>=` + `OFFSET
//!   seen`; a post-page-1 same-timestamp row ABOVE the composite cursor
//!   delivered, BELOW it the documented late-arrival non-delivery; a
//!   byte-identical duplicate of the boundary row delivered exactly once;
//! - first-page SQL carries the explicit `timestamp_ns > <start>` bound
//!   (asserted from `system.query_log`, not just the builder);
//! - backlog catch-up is slice-bounded: per-poll `query_log.read_rows`
//!   stays within one `tail_catchup_slice` window (+ one granule of
//!   slack), never the whole backlog;
//! - AC1 differential: the same window and pipeline through paged
//!   `tail_poll` versus the ordinary `query()` streams path delivers
//!   the identical entry set (review round 1);
//! - AC3 live slow consumer: real WebSocket backpressure evicts the
//!   OLDEST frames with exact `dropped_total` accounting and a bounded
//!   `dropped_entries` sample, proven through the encoded frames
//!   (review round 1);
//! - retention clamp (issue #94 item 1): an ancient `start=0` is clamped
//!   up to the retention floor (`now − retention·day − 1 partition-day`),
//!   proven from the FIRST keyset poll's `timestamp_ns > N` bound in
//!   `system.query_log` — within-retention rows still delivered, catch-up
//!   collapsed to a handful of polls (port 31142);
//! - the `/loki/api/v1/tail` compat alias streams identically;
//! - bounded month refresh + orphan cache (issue #94, atomicity-safe
//!   revision): a stream registered ONLY in an older calendar month
//!   (simulating the non-atomic `log_streams`/`log_samples` write path)
//!   stays tail-resolvable after the stage-1 month window narrows past
//!   its registration month (port 31147);
//! - scan-gated phase split (issue #94 v6-v8): reworked 31147 now proves
//!   every stage-1 poll (catch-up AND live-edge) stays FULL-SPAN through
//!   the registration-visibility hold — no port, WS e2e through the real
//!   producer; L1 (no port, direct-drive on real ClickHouse — the test
//!   IS the producer, so insert-vs-scan ordering is program order) proves
//!   the transition instant: a registration inserted in the inter-poll
//!   gap right after the last pre-qualifying full-span scan is caught by
//!   the NEXT (qualifying) full-span scan and delivered, narrowing then
//!   excludes the registration month and further delivery is
//!   cache-attributed; a strand-replay phase demonstrates the abolished
//!   v6 time-gated rule (`narrow = live && dwell >= grace`, no scan gate)
//!   strands an equivalent registration.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`. Run locally:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-server --test logs_tail_live
//! podman rm -f pulsus-ch-test
//! ```
//!
//! Ports 31140-31147, distinct from every other live suite.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_read::{
    Direction, EngineConfig, LogQlEngine, QueryParams, QuerySpec, StreamResult, TailCursor,
    TailLower,
};

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
}

fn ch_host() -> String {
    std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string())
}

fn ch_http_port() -> u16 {
    std::env::var("PULSUS_TEST_CH_HTTP_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(19123)
}

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("now fits in i64")
}

// ---------------------------------------------------------------------
// Process lifecycle + seeding (the logs_api_live idiom)
// ---------------------------------------------------------------------

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn http_get_status(port: u16, path: &str) -> Option<u16> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .ok()?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).ok()?;
    let head = String::from_utf8_lossy(&buf);
    head.lines().next()?.split_whitespace().nth(1)?.parse().ok()
}

fn spawn_ready(port: u16, db: &str, extra_env: &[(&str, &str)]) -> ChildGuard {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pulsusdb"));
    command
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env("CLICKHOUSE_SERVER", ch_host())
        .env("CLICKHOUSE_HTTP_PORT", ch_http_port().to_string())
        .env("CLICKHOUSE_DB", db);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    let child = command.spawn().expect("spawn pulsusdb");
    let guard = ChildGuard(child);
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if http_get_status(port, "/ready") == Some(200) {
            return guard;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("/ready never reached 200 within 60s (port {port}, db {db})");
}

async fn data_client(db: &str) -> ChClient {
    ChClient::new(ChConnConfig {
        server: ch_host(),
        http_port: ch_http_port(),
        database: db.to_string(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(30),
        ..ChConnConfig::default()
    })
    .await
    .expect("connect data client")
}

async fn drop_db(db: &str) {
    let admin = ChClient::new(ChConnConfig {
        server: ch_host(),
        http_port: ch_http_port(),
        database: "default".to_string(),
        proto: ChProto::Http,
        pool_size: 2,
        query_timeout: Duration::from_secs(30),
        ..ChConnConfig::default()
    })
    .await
    .expect("connect admin client");
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop db");
}

/// Seeds one `log_streams` row whose `month` matches `at_ns` (stage-1
/// resolution is month-scoped, so the stream's month must overlap the
/// tail window). Direct `log_streams` inserts fire the
/// `log_streams_idx` MV, so stage 1 resolves without further seeding.
async fn seed_stream(client: &ChClient, db: &str, fp: u64, labels: &str, at_ns: i64) {
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, updated_ns) \
                 VALUES (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({at_ns}))), {fp}, \
                 'checkout', '{labels}', 0)"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_streams");
}

async fn seed_samples(client: &ChClient, db: &str, rows: &[(u64, i64, &str)]) {
    if rows.is_empty() {
        return;
    }
    let values = rows
        .iter()
        .map(|(fp, ts, body)| format!("('checkout', {fp}, {ts}, 0, '{body}')"))
        .collect::<Vec<_>>()
        .join(", ");
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, \
                 body) VALUES {values}"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_samples");
}

// ---------------------------------------------------------------------
// Minimal WebSocket client (RFC 6455, text/close only — the server never
// fragments or pings)
// ---------------------------------------------------------------------

struct WsClient {
    stream: TcpStream,
    buf: Vec<u8>,
}

enum WsFrame {
    Text(String),
    Close,
}

impl WsClient {
    fn connect(port: u16, target: &str) -> WsClient {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_millis(200)))
            .expect("read timeout");
        let head = format!(
            "GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
        );
        stream.write_all(head.as_bytes()).expect("handshake write");
        // Read up to the header terminator; anything after it is already
        // WebSocket frame data.
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        let deadline = Instant::now() + Duration::from_secs(10);
        let split_at = loop {
            if let Some(i) = find_subslice(&buf, b"\r\n\r\n") {
                break i;
            }
            assert!(Instant::now() < deadline, "no handshake response");
            match stream.read(&mut chunk) {
                Ok(0) => panic!("connection closed during handshake"),
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut => {}
                Err(e) => panic!("handshake read failed: {e}"),
            }
        };
        let head_text = String::from_utf8_lossy(&buf[..split_at]).into_owned();
        let status: u16 = head_text
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse().ok())
            .expect("status line");
        assert_eq!(status, 101, "handshake must upgrade: {head_text}");
        let leftover = buf[split_at + 4..].to_vec();
        WsClient {
            stream,
            buf: leftover,
        }
    }

    /// The next text frame before `deadline`; `None` on timeout or Close.
    fn next_text(&mut self, deadline: Instant) -> Option<String> {
        let mut chunk = [0u8; 65536];
        loop {
            if let Some((frame, consumed)) = parse_ws_frame(&self.buf) {
                self.buf.drain(..consumed);
                match frame {
                    Some(WsFrame::Text(text)) => return Some(text),
                    Some(WsFrame::Close) => return None,
                    None => continue, // ping/pong/etc — ignore
                }
            }
            if Instant::now() > deadline {
                return None;
            }
            match self.stream.read(&mut chunk) {
                Ok(0) => return None,
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut => {}
                Err(_) => return None,
            }
        }
    }

    fn close(mut self) {
        // A masked, empty client Close frame.
        let _ = self.stream.write_all(&[0x88, 0x80, 0x12, 0x34, 0x56, 0x78]);
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Parses one complete server frame off the front of `buf`:
/// `Some((frame, consumed))` when whole, `None` when more bytes are
/// needed. The inner `Option` is `None` for ignorable opcodes.
#[allow(clippy::type_complexity)]
fn parse_ws_frame(buf: &[u8]) -> Option<(Option<WsFrame>, usize)> {
    if buf.len() < 2 {
        return None;
    }
    let opcode = buf[0] & 0x0F;
    let len7 = (buf[1] & 0x7F) as usize;
    let (len, header) = match len7 {
        126 => {
            if buf.len() < 4 {
                return None;
            }
            (u16::from_be_bytes([buf[2], buf[3]]) as usize, 4)
        }
        127 => {
            if buf.len() < 10 {
                return None;
            }
            let mut b = [0u8; 8];
            b.copy_from_slice(&buf[2..10]);
            (u64::from_be_bytes(b) as usize, 10)
        }
        n => (n, 2),
    };
    if buf.len() < header + len {
        return None;
    }
    let payload = &buf[header..header + len];
    let frame = match opcode {
        0x1 => Some(WsFrame::Text(String::from_utf8_lossy(payload).into_owned())),
        0x8 => Some(WsFrame::Close),
        _ => None,
    };
    Some((frame, header + len))
}

/// Collects `(labels, ts, line)` entries from tail frames until `want`
/// entries arrive or `deadline` passes.
fn collect_entries(
    ws: &mut WsClient,
    want: usize,
    deadline: Instant,
) -> Vec<(serde_json::Value, i64, String)> {
    let mut out = Vec::new();
    while out.len() < want && Instant::now() < deadline {
        let Some(text) = ws.next_text(deadline) else {
            break;
        };
        let frame: serde_json::Value = serde_json::from_str(&text).expect("frame JSON");
        for stream in frame["streams"].as_array().expect("streams array") {
            for value in stream["values"].as_array().expect("values array") {
                let ts: i64 = value[0].as_str().expect("ns string").parse().expect("ns");
                out.push((
                    stream["stream"].clone(),
                    ts,
                    value[1].as_str().expect("line").to_string(),
                ));
            }
        }
    }
    out
}

// ---------------------------------------------------------------------
// 1) WS end-to-end: streaming through a real pipeline, ascending,
//    exactly-once (port 31140)
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn tail_streams_new_rows_through_a_pipeline_in_order_exactly_once() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_140;
    let db = "pulsus_tail_it_stream";
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[("PULSUS_TAIL_POLL_INTERVAL", "200ms")]);
    let client = data_client(db).await;
    seed_stream(&client, db, 1, r#"{"service_name":"checkout"}"#, now_ns()).await;

    let start = now_ns() - 60_000_000_000;
    let query = "query=%7Bservice_name%3D%22checkout%22%7D%20%7C%3D%20%22keep%22";
    let mut ws = WsClient::connect(port, &format!("/api/logs/v1/tail?{query}&start={start}"));

    // Insert AFTER connecting: two matching lines and one the pushed-down
    // `|= "keep"` filter must drop.
    let t0 = now_ns();
    seed_samples(
        &client,
        db,
        &[
            (1, t0, "keep alpha"),
            (1, t0 + 1_000_000, "drop beta"),
            (1, t0 + 2_000_000, "keep gamma"),
        ],
    )
    .await;

    let entries = collect_entries(&mut ws, 2, Instant::now() + Duration::from_secs(20));
    assert_eq!(entries.len(), 2, "both matching lines arrive: {entries:?}");
    assert_eq!(entries[0].2, "keep alpha");
    assert_eq!(entries[1].2, "keep gamma");
    assert!(entries[0].1 < entries[1].1, "ascending timestamps");
    assert_eq!(entries[0].0["service_name"], "checkout");

    // Two more poll cycles: nothing is re-delivered.
    let extra = collect_entries(&mut ws, 1, Instant::now() + Duration::from_millis(700));
    assert!(extra.is_empty(), "no duplicate delivery: {extra:?}");
    ws.close();
    drop_db(db).await;
}

// ---------------------------------------------------------------------
// 2) WS end-to-end: a same-timestamp tie group larger than the fetch
//    limit is delivered exactly once (port 31141)
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn tie_group_split_by_the_fetch_limit_is_delivered_exactly_once() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_141;
    let db = "pulsus_tail_it_ties";
    drop_db(db).await;
    let _guard = spawn_ready(
        port,
        db,
        &[
            ("PULSUS_TAIL_POLL_INTERVAL", "200ms"),
            // LIMIT 3 < the 8-row tie group: pages MUST split the group.
            ("PULSUS_TAIL_MAX_FETCH_LIMIT", "3"),
        ],
    );
    let client = data_client(db).await;
    seed_stream(&client, db, 1, r#"{"service_name":"checkout"}"#, now_ns()).await;
    seed_stream(
        &client,
        db,
        2,
        r#"{"env":"prod","service_name":"checkout"}"#,
        now_ns(),
    )
    .await;

    // Eight rows sharing ONE timestamp across two fingerprints.
    let ts0 = now_ns() - 10_000_000_000;
    let rows: Vec<(u64, i64, String)> = (0..8)
        .map(|i| ((i % 2) + 1, ts0, format!("tie-{i}")))
        .collect();
    let rows_ref: Vec<(u64, i64, &str)> = rows
        .iter()
        .map(|(fp, ts, b)| (*fp, *ts, b.as_str()))
        .collect();
    seed_samples(&client, db, &rows_ref).await;

    let start = ts0 - 1_000_000_000;
    let query = "query=%7Bservice_name%3D%22checkout%22%7D";
    let mut ws = WsClient::connect(port, &format!("/api/logs/v1/tail?{query}&start={start}"));

    let entries = collect_entries(&mut ws, 8, Instant::now() + Duration::from_secs(20));
    let mut bodies: Vec<&str> = entries.iter().map(|(_, _, line)| line.as_str()).collect();
    bodies.sort_unstable();
    let expected: Vec<String> = (0..8).map(|i| format!("tie-{i}")).collect();
    assert_eq!(
        bodies,
        expected.iter().map(String::as_str).collect::<Vec<_>>(),
        "every tied row exactly once despite LIMIT 3 pages"
    );
    assert!(entries.iter().all(|(_, ts, _)| *ts == ts0));

    // Two more poll cycles: the tie group is never re-delivered.
    let extra = collect_entries(&mut ws, 1, Instant::now() + Duration::from_millis(700));
    assert!(extra.is_empty(), "no duplicates after catch-up: {extra:?}");
    ws.close();
    drop_db(db).await;
}

// ---------------------------------------------------------------------
// 3) Engine-level keyset cursor semantics (deterministic, page by page)
//    — uses `--mode init` for the schema, no server needed (port n/a)
// ---------------------------------------------------------------------

fn engine_for_db(client: ChClient) -> LogQlEngine {
    LogQlEngine::new(
        client,
        EngineConfig {
            db: String::new(),
            streams_idx: "log_streams_idx".to_string(),
            streams: "log_streams".to_string(),
            samples: "log_samples".to_string(),
            rollup_table: "log_metrics_5s".to_string(),
            patterns_table: "log_patterns".to_string(),
            rollup_res_ns: 5_000_000_000,
            scan_budget_bytes: 50 * 1024 * 1024 * 1024,
            max_streams: 100_000,
            pipeline_scan_factor: 10,
        },
    )
}

fn init_schema(db: &str) {
    let status = Command::new(env!("CARGO_BIN_EXE_pulsusdb"))
        .env("CLICKHOUSE_SERVER", ch_host())
        .env("CLICKHOUSE_HTTP_PORT", ch_http_port().to_string())
        .env("CLICKHOUSE_DB", db)
        .args(["--mode", "init"])
        .status()
        .expect("run --mode init");
    assert!(status.success(), "--mode init must succeed");
}

/// A body whose `(fingerprint, cityHash64(body))` sorts strictly
/// below/above `boundary_hash` — searched deterministically over a
/// numbered candidate space.
fn body_with_hash<F: Fn(u64) -> bool>(prefix: &str, pred: F) -> (String, u64) {
    for i in 0..100_000u64 {
        let candidate = format!("{prefix}-{i}");
        let h = pulsus_model::raw_cityhash64(candidate.as_bytes());
        if pred(h) {
            return (candidate, h);
        }
    }
    panic!("no candidate body found");
}

#[tokio::test(flavor = "multi_thread")]
async fn engine_keyset_pages_resume_split_ties_and_honor_the_composite_cursor() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let db = "pulsus_tail_it_keyset";
    drop_db(db).await;
    init_schema(db);
    let client = data_client(db).await;
    // One hour back keeps the tie group inside the same stage-1 month
    // as the (now-derived) plan window.
    let ts0 = now_ns() - 3_600_000_000_000;
    seed_stream(&client, db, 7, r#"{"service_name":"checkout"}"#, ts0).await;

    // Five rows at ONE timestamp, one fingerprint, distinct bodies.
    let bodies: Vec<String> = (0..5).map(|i| format!("seed-{i}")).collect();
    let rows: Vec<(u64, i64, &str)> = bodies.iter().map(|b| (7u64, ts0, b.as_str())).collect();
    seed_samples(&client, db, &rows).await;

    let engine = engine_for_db(data_client(db).await);
    let expr = pulsus_logql::parse(r#"{service_name="checkout"}"#).expect("parse");
    let qp = QueryParams {
        spec: QuerySpec::Range {
            start_ns: ts0 - 1_000_000_000,
            end_ns: ts0 + 1_000_000_000,
            step_ns: 1_000_000_000,
        },
        limit: 100,
        direction: Direction::Forward,
    };
    let mut setup = engine.tail_setup(&expr, &qp).expect("setup");
    let upper = ts0 + 1_000_000_000;

    // Page 1: LIMIT 2 splits the 5-row tie group.
    let page1 = engine
        .tail_poll(
            &mut setup,
            TailLower::Start {
                start_ns: ts0 - 1_000_000_000,
            },
            upper,
            2,
        )
        .await
        .expect("page 1");
    assert_eq!(page1.fetched, 2);
    let c1: TailCursor = page1.next.expect("cursor after rows");
    assert_eq!(c1.tuple.0, ts0);
    // Distinct bodies ⇒ distinct hashes ⇒ the boundary tuple's trailing
    // run is exactly the last row.
    assert_eq!(c1.seen, 1);
    let delivered_1: Vec<String> = page_lines(&page1.streams);

    // Between pages: three same-timestamp inserts —
    //  (a) BELOW the composite cursor (hash < boundary hash): the
    //      documented late-arrival non-delivery;
    //  (b) ABOVE it (hash > boundary hash): must be delivered;
    //  (c) a BYTE-IDENTICAL duplicate of the boundary row itself: one
    //      more occurrence of the boundary tuple, delivered exactly once
    //      via the occurrence-count OFFSET.
    let boundary_hash = c1.tuple.2;
    let (below, _) = body_with_hash("below", |h| h < boundary_hash);
    let (above, _) = body_with_hash("above", |h| h > boundary_hash);
    // The boundary row is the delivered line whose hash IS the cursor's
    // (page_lines sorts lexicographically, so `.last()` would be wrong).
    let boundary_body = delivered_1
        .iter()
        .find(|b| pulsus_model::raw_cityhash64(b.as_bytes()) == boundary_hash)
        .expect("boundary row is in page 1")
        .clone();
    seed_samples(
        &client,
        db,
        &[
            (7, ts0, below.as_str()),
            (7, ts0, above.as_str()),
            (7, ts0, boundary_body.as_str()),
        ],
    )
    .await;

    // Page 2 (and onwards) from the cursor: everything at/after the
    // boundary tuple, minus the `seen` already-delivered occurrences.
    let mut delivered_rest: Vec<String> = Vec::new();
    let mut cursor = c1;
    for _ in 0..10 {
        let page = engine
            .tail_poll(&mut setup, TailLower::After(cursor), upper, 2)
            .await
            .expect("page");
        delivered_rest.extend(page_lines(&page.streams));
        match page.next {
            Some(next) if page.fetched > 0 => cursor = next,
            _ => break,
        }
        if page.fetched < 2 {
            break;
        }
    }

    let mut all: Vec<String> = delivered_1.to_vec();
    all.extend(delivered_rest.iter().cloned());
    all.sort_unstable();

    let mut expected: Vec<String> = bodies.clone();
    expected.push(above.clone()); // the above-cursor insert IS delivered
    expected.push(boundary_body.clone()); // the duplicate: a second copy, exactly once
    expected.sort_unstable();

    assert_eq!(
        all, expected,
        "exactly-once across split pages: all 5 seeds once, the boundary duplicate once \
         more, the above-cursor insert once, the below-cursor insert never"
    );
    assert!(
        !all.contains(&below),
        "the below-cursor late arrival is the documented non-delivery"
    );
    drop_db(db).await;
}

/// Issue #74 AC1 (review round 1: the required tail-versus-range
/// DIFFERENTIAL, not two invocations of the same helper): the same
/// window and the same pipeline'd expression through (a) the ordinary
/// `query()` streams path and (b) `tail_poll` paged with a small
/// `fetch_limit` across many keyset pages — the delivered
/// `(labels_json, ts, line)` multisets must be identical.
#[tokio::test(flavor = "multi_thread")]
async fn tail_pages_and_range_query_deliver_identical_entry_sets() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let db = "pulsus_tail_it_diff";
    drop_db(db).await;
    init_schema(db);
    let client = data_client(db).await;
    let ts0 = now_ns() - 3_600_000_000_000;
    seed_stream(&client, db, 1, r#"{"service_name":"checkout"}"#, ts0).await;
    seed_stream(
        &client,
        db,
        2,
        r#"{"env":"prod","service_name":"checkout"}"#,
        ts0,
    )
    .await;

    // 12 rows across two fingerprints, with timestamp ties and lines the
    // pipeline (`|= "keep" | logfmt | y="z"`) partially drops/regroups.
    let mut rows: Vec<(u64, i64, String)> = Vec::new();
    for i in 0..12i64 {
        let fp = (i % 2 + 1) as u64;
        let ts = ts0 + (i / 4) * 1_000_000_000; // groups of 4 share a ts
        let body = if i % 3 == 0 {
            format!("keep y=z msg=m{i}")
        } else if i % 3 == 1 {
            format!("keep y=other msg=m{i}") // dropped by the label filter
        } else {
            format!("drop me {i}") // dropped by the pushed |= filter
        };
        rows.push((fp, ts, body));
    }
    let rows_ref: Vec<(u64, i64, &str)> =
        rows.iter().map(|(f, t, b)| (*f, *t, b.as_str())).collect();
    seed_samples(&client, db, &rows_ref).await;

    let engine = engine_for_db(data_client(db).await);
    let expr = pulsus_logql::parse(r#"{service_name="checkout"} |= "keep" | logfmt | y="z""#)
        .expect("parse");
    let start = ts0 - 1_000_000_000;
    let end = ts0 + 60_000_000_000;
    let qp = QueryParams {
        spec: QuerySpec::Range {
            start_ns: start,
            end_ns: end,
            step_ns: 1_000_000_000,
        },
        limit: 5_000,
        direction: Direction::Forward,
    };

    // (a) The ordinary query path.
    let range_result = engine.query(&expr, &qp).await.expect("range query");
    let pulsus_read::QueryResult::Streams {
        items: range_streams,
        ..
    } = range_result
    else {
        panic!("stream selector must return Streams");
    };
    let mut range_entries = entry_set(&range_streams);

    // (b) tail_poll paged with fetch_limit 3 — several pages, split ties.
    let mut setup = engine.tail_setup(&expr, &qp).expect("setup");
    let mut tail_streams: Vec<StreamResult> = Vec::new();
    let mut lower = TailLower::Start { start_ns: start };
    for _ in 0..32 {
        let page = engine
            .tail_poll(&mut setup, lower, end, 3)
            .await
            .expect("tail page");
        tail_streams.extend(page.streams);
        let fetched = page.fetched;
        match page.next {
            Some(next) if fetched > 0 => lower = TailLower::After(next),
            _ => {}
        }
        if fetched < 3 {
            break;
        }
    }
    let mut tail_entries = entry_set(&tail_streams);

    range_entries.sort();
    tail_entries.sort();
    assert!(
        !range_entries.is_empty(),
        "the pipeline must keep some rows or the differential is vacuous"
    );
    assert_eq!(
        tail_entries, range_entries,
        "tail pages and the range query must deliver the identical entry set"
    );
    drop_db(db).await;
}

fn entry_set(streams: &[StreamResult]) -> Vec<(String, i64, String)> {
    streams
        .iter()
        .flat_map(|s| {
            s.entries
                .iter()
                .map(|(ts, line)| (s.labels_json.clone(), *ts, line.clone()))
        })
        .collect()
}

fn page_lines(streams: &[StreamResult]) -> Vec<String> {
    let mut out: Vec<String> = streams
        .iter()
        .flat_map(|s| s.entries.iter().map(|(_, line)| line.clone()))
        .collect();
    out.sort_unstable();
    out
}

// ---------------------------------------------------------------------
// 4) First-page start bound + slice-bounded backlog scans, proven from
//    system.query_log (port 31143)
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Row, serde::Serialize, serde::Deserialize)]
struct QueryLogRow {
    query: String,
    read_rows: u64,
}

#[tokio::test(flavor = "multi_thread")]
async fn backlog_catch_up_is_slice_bounded_and_the_first_page_carries_the_start_bound() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_143;
    let db = "pulsus_tail_it_backlog";
    drop_db(db).await;
    let _guard = spawn_ready(
        port,
        db,
        &[
            ("PULSUS_TAIL_POLL_INTERVAL", "100ms"),
            ("PULSUS_TAIL_CATCHUP_SLICE", "60s"),
            ("PULSUS_TAIL_MAX_FETCH_LIMIT", "1000000"),
        ],
    );
    let client = data_client(db).await;
    seed_stream(&client, db, 1, r#"{"service_name":"checkout"}"#, now_ns()).await;

    // A 5-minute backlog, 100k rows at 3ms spacing — 20k rows per 60s
    // slice, several granules per slice (index_granularity 8192), so an
    // unsliced scan would be caught red-handed by read_rows.
    const TOTAL_ROWS: u64 = 100_000;
    const ROWS_PER_SLICE: u64 = 20_000;
    let ts0 = now_ns() - 320_000_000_000;
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, \
                 body) SELECT 'checkout', 1, {ts0} + toInt64(number) * 3000000, 0, \
                 concat('line-', toString(number)) FROM numbers({TOTAL_ROWS})"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("bulk seed");

    let start = ts0 - 1_000_000_000;
    let query = "query=%7Bservice_name%3D%22checkout%22%7D&limit=1000000";
    // `system.query_log` outlives DROP DATABASE: scope the assertions to
    // THIS run via a wall-clock marker taken just before connecting.
    let run_marker_us = now_ns() / 1_000;
    let mut ws = WsClient::connect(port, &format!("/api/logs/v1/tail?{query}&start={start}"));
    let entries = collect_entries(
        &mut ws,
        TOTAL_ROWS as usize,
        Instant::now() + Duration::from_secs(60),
    );
    assert_eq!(entries.len() as u64, TOTAL_ROWS, "whole backlog delivered");
    // Global ascending order across the whole catch-up.
    assert!(
        entries.windows(2).all(|w| w[0].1 <= w[1].1),
        "ascending timestamps across frames"
    );
    ws.close();

    client
        .execute(
            "SYSTEM FLUSH LOGS",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("flush logs");
    let sql = format!(
        "SELECT query, read_rows FROM system.query_log \
         WHERE type = 'QueryFinish' AND current_database = '{db}' \
           AND query_start_time_microseconds >= fromUnixTimestamp64Micro({run_marker_us}) \
           AND query LIKE '%cityHash64(body) AS body_hash%' \
           AND query NOT LIKE '%system.query_log%' \
         ORDER BY query_start_time_microseconds ASC"
    );
    let mut polls: Vec<QueryLogRow> = Vec::new();
    {
        let mut stream = client
            .query_stream::<QueryLogRow>(&sql, &QuerySettings::new())
            .await
            .expect("query_log stream");
        while let Some(row) = stream.next().await {
            polls.push(row.expect("query_log row"));
        }
    }
    assert!(
        polls.len() >= 5,
        "a 5-slice backlog takes at least 5 keyset polls, got {}",
        polls.len()
    );

    // First-bound AC: the FIRST keyset query carries the API start bound,
    // exclusive, verbatim.
    assert!(
        polls[0].query.contains(&format!("timestamp_ns > {start}")),
        "first poll must carry the explicit start bound: {}",
        polls[0].query
    );

    // Slice-bound AC (Tier 1, scale-invariant): no single poll reads more
    // than one slice's rows (+ one granule of alignment slack) — checking
    // only the emitted LIMIT would not prove bounded work.
    const GRANULE_SLACK: u64 = 8_192 * 2;
    for poll in &polls {
        assert!(
            poll.read_rows <= ROWS_PER_SLICE + GRANULE_SLACK,
            "one poll read {} rows — more than one {ROWS_PER_SLICE}-row slice (+{GRANULE_SLACK} \
             granule slack); the catch-up window is not slice-bounded.\nquery: {}",
            poll.read_rows,
            poll.query
        );
        // Both time bounds present in every poll's SQL.
        assert!(
            poll.query.contains("timestamp_ns <= "),
            "sliced upper bound present: {}",
            poll.query
        );
        assert!(
            poll.query.contains("timestamp_ns > ") || poll.query.contains("timestamp_ns >= "),
            "lower bound present: {}",
            poll.query
        );
    }
    drop_db(db).await;
}

// ---------------------------------------------------------------------
// 5) Live slow consumer: real WebSocket backpressure evicts the OLDEST
//    frames with exact dropped_total accounting through the encoded
//    frames (port 31146) — review round 1 / AC3
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn slow_consumer_backpressure_evicts_oldest_with_exact_drop_accounting() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_146;
    let db = "pulsus_tail_it_slow";
    drop_db(db).await;
    let _guard = spawn_ready(
        port,
        db,
        &[
            ("PULSUS_TAIL_POLL_INTERVAL", "100ms"),
            // Depth 1: any second undelivered frame evicts the oldest.
            ("PULSUS_TAIL_CHANNEL_DEPTH", "1"),
            // 100-row pages: the 2000-row backlog becomes ~20 frames.
            ("PULSUS_TAIL_MAX_FETCH_LIMIT", "100"),
            // A small sample cap so the bounded-sample contract is
            // observable (each evicted frame alone exceeds it).
            ("PULSUS_TAIL_MAX_ENTRIES_PER_FRAME", "25"),
            // Longer than the deliberate non-reading window below — the
            // writer must stay blocked, never disconnect.
            ("PULSUS_TAIL_SEND_TIMEOUT", "60s"),
        ],
    );
    let client = data_client(db).await;
    seed_stream(&client, db, 1, r#"{"service_name":"checkout"}"#, now_ns()).await;

    // A 2000-row backlog of ~8KB bodies (~16MB of frames) — far beyond
    // loopback socket buffering, so a non-reading client genuinely
    // backpressures the writer mid-send while the producer keeps paging.
    const TOTAL_ROWS: u64 = 2_000;
    let ts0 = now_ns() - 30_000_000_000;
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, \
                 body) SELECT 'checkout', 1, {ts0} + toInt64(number) * 1000000, 0, \
                 concat('bulk-', toString(number), '-', repeat('x', 8000)) \
                 FROM numbers({TOTAL_ROWS})"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("bulk seed");

    let start = ts0 - 1_000_000_000;
    let query = "query=%7Bservice_name%3D%22checkout%22%7D&limit=100";
    let mut ws = WsClient::connect(port, &format!("/api/logs/v1/tail?{query}&start={start}"));

    // Do NOT read for a while: the producer walks the whole backlog into
    // a depth-1 buffer against a blocked writer — evicting oldest frames.
    std::thread::sleep(Duration::from_secs(4));

    // Now drain everything and account exactly: every seeded row is
    // either delivered in some frame or counted in a dropped_total.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut delivered: u64 = 0;
    let mut dropped_total_sum: u64 = 0;
    let mut saw_drop_report = false;
    while delivered + dropped_total_sum < TOTAL_ROWS && Instant::now() < deadline {
        let Some(text) = ws.next_text(deadline) else {
            break;
        };
        let frame: serde_json::Value = serde_json::from_str(&text).expect("frame JSON");
        let frame_dropped = frame["dropped_total"].as_u64().expect("dropped_total");
        dropped_total_sum += frame_dropped;
        let sample = frame["dropped_entries"].as_array().expect("array");
        assert!(
            sample.len() <= 25,
            "dropped_entries sample must stay within the cap, got {}",
            sample.len()
        );
        if frame_dropped > 0 {
            saw_drop_report = true;
            assert!(
                !sample.is_empty(),
                "a frame reporting drops carries a non-empty sample"
            );
        }
        let mut frame_min_ts: Option<i64> = None;
        for stream in frame["streams"].as_array().expect("streams") {
            for value in stream["values"].as_array().expect("values") {
                delivered += 1;
                let ts: i64 = value[0].as_str().expect("ns").parse().expect("ns");
                frame_min_ts = Some(frame_min_ts.map_or(ts, |m| m.min(ts)));
            }
        }
        // OLDEST-eviction proof on the wire: the evicted rows a frame
        // reports are strictly older pages than the frame that survived
        // to carry the report (all seeded timestamps are distinct).
        if let (true, Some(min_ts)) = (frame_dropped > 0, frame_min_ts) {
            for d in sample {
                let dropped_ts: i64 = d["timestamp"].as_str().expect("ns").parse().expect("ns");
                assert!(
                    dropped_ts < min_ts,
                    "evicted rows must be OLDER than the surviving frame's rows \
                     (dropped {dropped_ts} vs delivered min {min_ts})"
                );
            }
        }
    }
    assert!(
        saw_drop_report,
        "a genuinely backpressured consumer must see a non-zero dropped_total \
         (delivered={delivered}, dropped={dropped_total_sum})"
    );
    assert_eq!(
        delivered + dropped_total_sum,
        TOTAL_ROWS,
        "exact accounting: every row is delivered exactly once or counted dropped"
    );
    assert!(
        delivered < TOTAL_ROWS,
        "some frames must actually have been evicted"
    );
    ws.close();
    drop_db(db).await;
}

// ---------------------------------------------------------------------
// 7) Retention clamp: an ancient start=0 is clamped up to the retention
//    floor, proven from the FIRST keyset poll's start bound in
//    system.query_log (issue #94 item 1, port 31142)
// ---------------------------------------------------------------------

/// Extracts `N` from a first-page keyset query's `timestamp_ns > N` bound
/// (the exclusive `Start` form; the `>=` keyset-resume form is skipped by
/// the trailing space in the needle).
fn parse_first_page_start_bound(query: &str) -> i64 {
    let needle = "timestamp_ns > ";
    let idx = query.find(needle).expect("first-page start bound present");
    let rest = &query[idx + needle.len()..];
    let end = rest
        .find(|c: char| !c.is_ascii_digit() && c != '-')
        .unwrap_or(rest.len());
    rest[..end].parse().expect("bound integer")
}

#[tokio::test(flavor = "multi_thread")]
async fn ancient_start_is_clamped_to_the_retention_floor_in_the_first_poll() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    const DAY_NS: i64 = 86_400_000_000_000;
    let port = 31_142;
    let db = "pulsus_tail_it_clamp";
    drop_db(db).await;
    // Retention 1 day ⇒ clamp floor is now − 1·day − 1 partition-day slack
    // (issue #94). A 7-day catch-up slice makes the ~2-day clamped window a
    // single slice, so catch-up is a handful of polls, never thousands (a
    // raw start=0 without the clamp would grind ~decades of empty slices).
    let _guard = spawn_ready(
        port,
        db,
        &[
            ("PULSUS_RETENTION_DAYS", "1"),
            ("PULSUS_TAIL_POLL_INTERVAL", "200ms"),
            ("PULSUS_TAIL_CATCHUP_SLICE", "7d"),
            ("PULSUS_TAIL_MAX_FETCH_LIMIT", "1000000"),
        ],
    );
    let client = data_client(db).await;
    seed_stream(&client, db, 1, r#"{"service_name":"checkout"}"#, now_ns()).await;
    // Recent rows within the clamped window AND below the delay horizon
    // (delay_for=5 ⇒ horizon = now − 5s): seed synchronously before
    // connecting, well older than the horizon so they are query-visible.
    let base = now_ns();
    seed_samples(
        &client,
        db,
        &[
            (1, base - 30_000_000_000, "recent-a"),
            (1, base - 20_000_000_000, "recent-b"),
            (1, base - 10_000_000_000, "recent-c"),
        ],
    )
    .await;

    // #77 tail-visibility discipline: bound `start` (the clamp does this,
    // raising the ancient 0) AND set delay_for>=5 — seed-before-connect
    // does not by itself guarantee CH visibility under async batching.
    let query = "query=%7Bservice_name%3D%22checkout%22%7D&limit=1000000&delay_for=5";
    let run_marker_us = now_ns() / 1_000;
    let before_connect = now_ns();
    let mut ws = WsClient::connect(port, &format!("/api/logs/v1/tail?{query}&start=0"));

    // The three recent rows arrive (the clamp did NOT skip within-retention
    // data; ascending, exactly-once).
    let entries = collect_entries(&mut ws, 3, Instant::now() + Duration::from_secs(30));
    let after_connect = now_ns();
    assert_eq!(
        entries.len(),
        3,
        "recent within-window rows delivered: {entries:?}"
    );
    let bodies: Vec<&str> = entries.iter().map(|(_, _, l)| l.as_str()).collect();
    assert_eq!(
        bodies,
        vec!["recent-a", "recent-b", "recent-c"],
        "ascending order"
    );
    ws.close();

    client
        .execute(
            "SYSTEM FLUSH LOGS",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("flush logs");
    let sql = format!(
        "SELECT query, read_rows FROM system.query_log \
         WHERE type = 'QueryFinish' AND current_database = '{db}' \
           AND query_start_time_microseconds >= fromUnixTimestamp64Micro({run_marker_us}) \
           AND query LIKE '%cityHash64(body) AS body_hash%' \
           AND query NOT LIKE '%system.query_log%' \
         ORDER BY query_start_time_microseconds ASC"
    );
    let mut polls: Vec<QueryLogRow> = Vec::new();
    {
        let mut stream = client
            .query_stream::<QueryLogRow>(&sql, &QuerySettings::new())
            .await
            .expect("query_log stream");
        while let Some(row) = stream.next().await {
            polls.push(row.expect("query_log row"));
        }
    }
    assert!(!polls.is_empty(), "at least one keyset poll ran");

    // The clamp fired: the FIRST keyset poll's start bound is the retention
    // floor (≈ now − 2·day), NOT the raw start=0 (or a negative value).
    let n = parse_first_page_start_bound(&polls[0].query);
    let slice_ns = 7 * DAY_NS;
    let lo = before_connect - 2 * DAY_NS - slice_ns;
    assert!(
        n > 0,
        "the clamp fired — the first poll's bound is NOT the raw start=0: got {n}\n{}",
        polls[0].query
    );
    assert!(
        n >= lo && n <= after_connect,
        "first poll bound {n} must sit at the retention floor (≈ now − 2·day), \
         within [{lo}, {after_connect}]:\n{}",
        polls[0].query
    );

    // Catch-up collapsed to a handful of polls — an unclamped start=0 with a
    // 7-day slice would emit thousands (decades of empty slices).
    assert!(
        polls.len() < 100,
        "clamped catch-up must be a handful of polls, got {} (unclamped would be thousands)",
        polls.len()
    );
    drop_db(db).await;
}

// ---------------------------------------------------------------------
// 6) The /loki/api/v1/tail compat alias streams identically (port 31144)
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn loki_tail_alias_streams_like_native() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_144;
    let db = "pulsus_tail_it_alias";
    drop_db(db).await;
    let _guard = spawn_ready(
        port,
        db,
        &[
            ("PULSUS_TAIL_POLL_INTERVAL", "200ms"),
            ("PULSUS_COMPAT_ENDPOINTS", "true"),
        ],
    );
    let client = data_client(db).await;
    seed_stream(&client, db, 1, r#"{"service_name":"checkout"}"#, now_ns()).await;

    let start = now_ns() - 60_000_000_000;
    let query = "query=%7Bservice_name%3D%22checkout%22%7D";
    let mut ws = WsClient::connect(port, &format!("/loki/api/v1/tail?{query}&start={start}"));
    seed_samples(&client, db, &[(1, now_ns(), "via-alias")]).await;
    let entries = collect_entries(&mut ws, 1, Instant::now() + Duration::from_secs(20));
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].2, "via-alias");
    ws.close();
    drop_db(db).await;
}

// ---------------------------------------------------------------------
// 7) Bounded month refresh + orphan cache (issue #94, atomicity-safe
//    revision): a stream registered ONLY in an older calendar month
//    (simulating the non-atomic log_streams/log_samples write path,
//    writer/mod.rs:9-19) stays tail-resolvable after the narrowed
//    stage-1 month window scrolls past its registration month — proven
//    non-vacuous via system.query_log (port 31147)
// ---------------------------------------------------------------------

/// The `'YYYY-MM-01'` ClickHouse date literal a single UTC instant falls
/// in — the live-test-side equivalent of the month literal
/// `pulsus_read::logql::plan::months_overlapping` renders (not reachable
/// from here, `pub(crate)` to that crate).
fn month_literal(ts_ns: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(ts_ns)
        .format("'%Y-%m-01'")
        .to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn stage1_month_narrowing_keeps_an_older_registered_orphan_resolvable_via_the_cache() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    const DAY_NS: i64 = 86_400_000_000_000;
    let port = 31_147;
    let db = "pulsus_tail_it_orphan";
    drop_db(db).await;

    // B (sample month) and A (registration month): A is exactly TWO
    // CALENDAR MONTHS before B (not a fixed day-count) — deviation from
    // plan v3's literal "A_reg = B_sample − 45 days", documented in the
    // implementation notes. A fixed 45-day gap only guarantees "a
    // different month", not "a non-adjacent month": depending on the
    // day-of-month this suite happens to run on, 45 days can land A and B
    // in ADJACENT months (verified against an actual run), and an
    // intermediate catch-up poll straddling that single shared boundary
    // — one that never reaches `b_sample_ns` — then legitimately carries
    // BOTH month literals, which the plan's own assertion (2) forbids.
    // Two full calendar months guarantees a whole BUFFER month strictly
    // between month(A) and month(B); no single <=10-day poll window can
    // ever span three consecutive calendar months (every month is >= 28
    // days, far wider than the slice), so no poll can carry both literals
    // — assertion (2) now holds by construction, not by day-count luck.
    let now = now_ns();
    let b_sample_ns = now - 3 * DAY_NS;
    let b_date = chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(b_sample_ns);
    let a_date = b_date
        .checked_sub_months(chrono::Months::new(2))
        .expect("2 months before a valid UTC instant is representable");
    let a_reg_ns = a_date.timestamp_nanos_opt().expect("fits in i64 ns");
    let a_lit = month_literal(a_reg_ns);
    let b_lit = month_literal(b_sample_ns);
    assert_ne!(
        a_lit, b_lit,
        "construction sanity: A and B fall in different calendar months"
    );

    // PULSUS_RETENTION_DAYS=90 keeps the clamp floor (issue #94 item 1)
    // below A (~59-62 days back) and log_samples' TTL from expiring
    // either seed. PULSUS_TAIL_CATCHUP_SLICE=10d is strictly shorter than
    // any single calendar month, so the lowest lower bound of any poll
    // window containing B_sample sits at least `10d` before it — still
    // within B's month or the buffer month, never month(A) — guaranteeing
    // every B-containing poll excludes month(A).
    let _guard = spawn_ready(
        port,
        db,
        &[
            ("PULSUS_TAIL_POLL_INTERVAL", "50ms"),
            ("PULSUS_RETENTION_DAYS", "90"),
            ("PULSUS_TAIL_CATCHUP_SLICE", "10d"),
        ],
    );
    let client = data_client(db).await;
    const F: u64 = 99;
    // The partial-failure orphan: log_streams/log_streams_idx registered
    // ONLY in month(A); the sample lands in month(B), where no
    // registration exists (the non-atomic write path).
    seed_stream(&client, db, F, r#"{"service_name":"orphan"}"#, a_reg_ns).await;
    seed_samples(&client, db, &[(F, b_sample_ns, "orphan-line")]).await;

    let query = "query=%7Bservice_name%3D%22orphan%22%7D&delay_for=5";
    let start = a_reg_ns - 3_600_000_000_000;
    let run_marker_us = now_ns() / 1_000;
    let mut ws = WsClient::connect(port, &format!("/api/logs/v1/tail?{query}&start={start}"));

    // Under the pre-cache narrowing alone this times out empty (F is
    // unresolvable once the stage-1 window scrolls past month A); with
    // the resolved-fingerprint cache the orphan sample arrives.
    let entries = collect_entries(&mut ws, 1, Instant::now() + Duration::from_secs(60));

    // Reworked for the v6-v8 phase split (the v2-era narrowed-catch-up
    // month-set assertions below are abolished — under the hold every
    // catch-up poll (including B-covering ones) legitimately carries
    // BOTH months, which the abolished assertions forbade): keep the WS
    // open past delivery and poll `system.query_log` (event-gated, no
    // bare sleeps) until >= 15 stage-1 polls are visible. Catch-up is
    // <= 8 polls at 10d slices over ~63d; under `PULSUS_RETENTION_DAYS`'s
    // 1h production grace, live polls (every 50ms) never narrow within
    // this test's window, so >= 7 of the 15 are live-edge, still
    // full-span, polls. Cache attribution (delivery is NOT attributable
    // to a wide window) now lives in the `scan_gate_*` direct-drive test.
    let mut polls: Vec<QueryLogRow> = Vec::new();
    let query_log_deadline = Instant::now() + Duration::from_secs(60);
    while polls.len() < 15 && Instant::now() < query_log_deadline {
        client
            .execute(
                "SYSTEM FLUSH LOGS",
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("flush logs");
        let sql = format!(
            "SELECT query, read_rows FROM system.query_log \
             WHERE type = 'QueryFinish' AND current_database = '{db}' \
               AND query_start_time_microseconds >= fromUnixTimestamp64Micro({run_marker_us}) \
               AND query LIKE '%log_streams_idx%' \
               AND query NOT LIKE '%system.query_log%' \
             ORDER BY query_start_time_microseconds ASC"
        );
        polls = Vec::new();
        let mut stream = client
            .query_stream::<QueryLogRow>(&sql, &QuerySettings::new())
            .await
            .expect("query_log stream");
        while let Some(row) = stream.next().await {
            polls.push(row.expect("query_log row"));
        }
        if polls.len() < 15 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    ws.close();
    assert!(
        polls.len() >= 15,
        "expected >= 15 stage-1 polls within 60s (catch-up + live cadence), got {}",
        polls.len()
    );

    // Every stage-1 poll (catch-up AND live-edge, held through the
    // production grace) must stay full-span from the frozen floor — the
    // structural anti-strand property, proven e2e through the real
    // producer. Under a no-hold mutation the live polls narrow within
    // ~8 polls and this fails deterministically.
    for p in &polls {
        assert!(
            p.query.contains(&a_lit),
            "every stage-1 poll must remain full-span (contain month(A)) through the hold: {}",
            p.query
        );
    }
    // Subsumed by the above, kept as an explicit sanity pin: some poll's
    // window also reaches month(B).
    assert!(
        polls.iter().any(|p| p.query.contains(&b_lit)),
        "some stage-1 poll must also cover month(B)"
    );

    // The orphan sample is DELIVERED (F's sole registration is month(A),
    // scanned by every poll above — full-span, not narrowed).
    assert_eq!(
        entries.len(),
        1,
        "the orphan sample must be delivered: {entries:?}"
    );
    assert_eq!(entries[0].2, "orphan-line");

    drop_db(db).await;
}

// ---------------------------------------------------------------------
// 8) L1: scan-gate transition instant, direct-drive on real ClickHouse
//    (issue #94 v6-v8) — no server/port; the test IS the producer, so
//    insert-vs-scan ordering is program order, not timing.
// ---------------------------------------------------------------------

/// Phase 1 (scan-gated contract, AC9 steps (1)-(5)): a registration
/// inserted in the inter-poll gap right after the LAST pre-qualifying
/// full-span scan is caught by the NEXT (qualifying) full-span scan
/// (program order) and delivered; narrowing (`narrow=true`) then excludes
/// the registration month and a further sample is delivered attributably
/// only to the cached resolved-fingerprint union. Phase 2 (strand
/// replay, a SEPARATE database): repeats catch-up, then applies the
/// ABOLISHED v6 time-gated rule's next step directly (`narrow=true`
/// immediately on the first live-edge refresh, no scan gate) — the
/// registration is never caught and delivery is empty, the committed
/// demonstration that the scan gate (not a bare wall-clock hold) is
/// load-bearing.
#[tokio::test(flavor = "multi_thread")]
async fn scan_gate_catches_a_gap_registration_via_the_qualifying_scan_then_narrows_and_a_time_gated_replay_strands_it()
 {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    const DAY_NS: i64 = 86_400_000_000_000;
    const SLICE_NS: i64 = 10 * DAY_NS;

    // Month A: two calendar months back from "now" — a full buffer month
    // guaranteed (same construction rationale as the 31147 test), so no
    // <= 10-day poll window can ever span 3 consecutive calendar months.
    let now = now_ns();
    let now_date = chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(now);
    let a_date = now_date
        .checked_sub_months(chrono::Months::new(2))
        .expect("2 months before a valid UTC instant is representable");
    let a_reg_ns = a_date.timestamp_nanos_opt().expect("fits in i64 ns");
    let a_lit = month_literal(a_reg_ns);
    let now_lit = month_literal(now);
    assert_ne!(
        a_lit, now_lit,
        "construction sanity: month(A) differs from month(now)"
    );

    // -------------------- Phase 1 --------------------
    let db1 = "pulsus_tail_it_scan_gate";
    drop_db(db1).await;
    init_schema(db1);
    let client1 = data_client(db1).await;
    let engine1 = engine_for_db(data_client(db1).await);
    let expr = pulsus_logql::parse(r#"{service_name="scangate"}"#).expect("parse");
    let start = a_reg_ns - 3_600_000_000_000; // a_reg_ns - 1h
    let qp = QueryParams {
        spec: QuerySpec::Range {
            start_ns: start,
            end_ns: now,
            step_ns: 1_000_000_000,
        },
        limit: 1_000,
        direction: Direction::Forward,
    };
    let mut setup = engine1.tail_setup(&expr, &qp).expect("setup");

    // (1) Catch-up, narrow=false, from `start` to the live edge: no
    // registration exists yet — F stays unresolved, every stage-1 scan
    // is full-span (contains a_lit).
    let mut lower = start;
    while lower < now {
        let upper = (lower + SLICE_NS).min(now);
        engine1
            .tail_refresh_months(&mut setup, lower, upper, false)
            .expect("no I/O, cannot fail");
        assert!(
            setup.plan.stage1_sql.contains(&a_lit),
            "catch-up (narrow=false) must stay full-span through month A: {}",
            setup.plan.stage1_sql
        );
        let page = engine1
            .tail_poll(
                &mut setup,
                TailLower::Start { start_ns: lower },
                upper,
                1_000,
            )
            .await
            .expect("catch-up poll");
        assert_eq!(page.fetched, 0, "no data seeded yet");
        lower = upper;
    }

    // (2) >= 1 further narrow=false live-edge pair — the in-hold polls,
    // still full-span, still no registration.
    for _ in 0..2 {
        engine1
            .tail_refresh_months(&mut setup, lower, now, false)
            .expect("no I/O, cannot fail");
        assert!(setup.plan.stage1_sql.contains(&a_lit));
        let page = engine1
            .tail_poll(&mut setup, TailLower::Start { start_ns: lower }, now, 1_000)
            .await
            .expect("in-hold poll");
        assert_eq!(page.fetched, 0);
    }

    // The transition instant: immediately after the LAST pre-qualifying
    // full-span scan completes, sync-insert F's registration (month A)
    // and a sample — the exact just-after-the-prior-broad-scan gap the
    // codex v6 finding named.
    const F: u64 = 4_001;
    let gap_ns = now_ns();
    seed_stream(&client1, db1, F, r#"{"service_name":"scangate"}"#, a_reg_ns).await;
    seed_samples(&client1, db1, &[(F, gap_ns, "gap-line")]).await;

    // (3) ONE more narrow=false refresh+poll — the QUALIFYING scan: still
    // full-span (a_lit present), F resolves, the gap sample is delivered
    // in THIS poll's page (program order: the insert strictly precedes
    // this scan).
    engine1
        .tail_refresh_months(&mut setup, lower, gap_ns, false)
        .expect("no I/O, cannot fail");
    assert!(
        setup.plan.stage1_sql.contains(&a_lit),
        "the qualifying scan is still full-span: {}",
        setup.plan.stage1_sql
    );
    let page = engine1
        .tail_poll(
            &mut setup,
            TailLower::Start { start_ns: lower },
            gap_ns,
            1_000,
        )
        .await
        .expect("qualifying scan poll");
    let delivered: Vec<String> = page
        .streams
        .iter()
        .flat_map(|s| s.entries.iter().map(|(_, l)| l.clone()))
        .collect();
    assert_eq!(
        delivered,
        vec!["gap-line".to_string()],
        "the inter-poll-gap registration is caught by the qualifying full-span scan"
    );
    let cursor = page.next.expect("cursor after the delivered gap-line");

    // (4) narrow=true refresh with lower ~ the resumed cursor — real
    // narrowing; A is 2 months behind `lower - GRACE` on any calendar
    // date, so a_lit must be ABSENT.
    let resume_ns = cursor.tuple.0;
    let now2 = now_ns();
    engine1
        .tail_refresh_months(&mut setup, resume_ns, now2, true)
        .expect("no I/O, cannot fail");
    assert!(
        !setup.plan.stage1_sql.contains(&a_lit),
        "narrow=true must drop month A once the live floor advances past GRACE: {}",
        setup.plan.stage1_sql
    );

    // (5) A second F sample, polled over the narrowed plan — delivered
    // ONLY via the cached resolved-fingerprint union (stage-1 provably
    // excludes A).
    let now3 = now_ns();
    seed_samples(&client1, db1, &[(F, now3, "narrowed-line")]).await;
    engine1
        .tail_refresh_months(&mut setup, resume_ns, now3, true)
        .expect("no I/O, cannot fail");
    assert!(!setup.plan.stage1_sql.contains(&a_lit));
    let page2 = engine1
        .tail_poll(&mut setup, TailLower::After(cursor), now3, 1_000)
        .await
        .expect("narrowed poll");
    let delivered2: Vec<String> = page2
        .streams
        .iter()
        .flat_map(|s| s.entries.iter().map(|(_, l)| l.clone()))
        .collect();
    assert_eq!(
        delivered2,
        vec!["narrowed-line".to_string()],
        "the narrowed-window sample is delivered via the cached resolved-fingerprint union"
    );
    drop_db(db1).await;

    // -------------------- Phase 2: strand replay (separate database) ---
    // Repeats catch-up to the live edge, then applies the ABOLISHED v6
    // time-gated rule's contract directly (`narrow=true` immediately on
    // the first live-edge refresh — no scan gate, exactly the flag
    // sequence a `narrow = live && dwell >= grace` producer emits,
    // pinned against the real producer by the `narrow_gate_*` hermetic
    // tests) — F2's registration, inserted at the identical program
    // point, is never caught: a committed demonstration that the scan
    // gate is load-bearing.
    let db2 = "pulsus_tail_it_scan_gate_strand";
    drop_db(db2).await;
    init_schema(db2);
    let client2 = data_client(db2).await;
    let engine2 = engine_for_db(data_client(db2).await);
    let mut setup2 = engine2.tail_setup(&expr, &qp).expect("setup2");

    let mut lower2 = start;
    while lower2 < now {
        let upper2 = (lower2 + SLICE_NS).min(now);
        engine2
            .tail_refresh_months(&mut setup2, lower2, upper2, false)
            .expect("no I/O, cannot fail");
        let page = engine2
            .tail_poll(
                &mut setup2,
                TailLower::Start { start_ns: lower2 },
                upper2,
                1_000,
            )
            .await
            .expect("catch-up poll");
        assert_eq!(page.fetched, 0);
        lower2 = upper2;
    }
    // The FIRST live-edge poll is a full-span scan at dwell 0
    // (narrow=false).
    engine2
        .tail_refresh_months(&mut setup2, lower2, now, false)
        .expect("no I/O, cannot fail");
    assert!(setup2.plan.stage1_sql.contains(&a_lit));
    let page = engine2
        .tail_poll(
            &mut setup2,
            TailLower::Start { start_ns: lower2 },
            now,
            1_000,
        )
        .await
        .expect("dwell-0 live poll");
    assert_eq!(page.fetched, 0);

    const F2: u64 = 4_002;
    let strand_now = now_ns();
    seed_stream(
        &client2,
        db2,
        F2,
        r#"{"service_name":"scangate"}"#,
        a_reg_ns,
    )
    .await;
    seed_samples(&client2, db2, &[(F2, strand_now, "strand-line")]).await;

    // The TIME-GATED v6 rule's NEXT step: narrow=true immediately.
    engine2
        .tail_refresh_months(&mut setup2, lower2, strand_now, true)
        .expect("no I/O, cannot fail");
    assert!(
        !setup2.plan.stage1_sql.contains(&a_lit),
        "the time-gated rule narrows immediately, excluding month A: {}",
        setup2.plan.stage1_sql
    );
    let page2 = engine2
        .tail_poll(
            &mut setup2,
            TailLower::Start { start_ns: lower2 },
            strand_now,
            1_000,
        )
        .await
        .expect("time-gated poll");
    assert!(
        page2.streams.iter().all(|s| s.entries.is_empty()),
        "under the time-gated rule F2 is stranded (its only registration, month A, was \
         excluded before ever being scanned): {:?}",
        page2.streams
    );

    drop_db(db2).await;
}
