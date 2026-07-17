//! Live end-to-end round-trip for the Loki push receiver (issue #77,
//! `POST /loki/api/v1/push`): spawns the real `pulsusdb` binary against a
//! live ClickHouse (same podman harness as `logs_api_live.rs`), with
//! `PULSUS_COMPAT_ENDPOINTS=1` in `all` mode so both the writer-side push
//! route and the reader-side LogQL/tail surfaces are mounted, then proves
//! the load-bearing correctness gate at the highest tier: a stream **pushed
//! via #77** (in BOTH encodings — JSON and snappy-protobuf) is queryable via
//! LogQL `query_range` and appears in `/api/logs/v1/tail`, with its exact
//! entries + labels — i.e. it fingerprints into the same physical rows the
//! read path (#72/#73) and tail (#74) expect.
//!
//! This is the "live producer→us→query" round-trip the task-manager Q3
//! adjudication names as strongest: the committed real-promtail-capture
//! fixture (`crates/pulsus-write/tests/loki_push_fixtures.rs`) is the
//! hermetic wire-format oracle; this file is the live admit→CH→read gate.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`. Run locally:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-server --test loki_push_live
//! podman rm -f pulsus-ch-test
//! ```

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use prost::Message;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_write::protocols::loki_push::{EntryAdapter, PushRequest, StreamAdapter, Timestamp};

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
            .expect("system clock after epoch")
            .as_nanos(),
    )
    .expect("now fits in i64 ns")
}

// ---------------------------------------------------------------------
// Minimal raw HTTP/1.1 over loopback (KISS, same rationale as the sibling
// live suites: no HTTP client dependency for a handful of requests).
// ---------------------------------------------------------------------

struct HttpResponse {
    status: u16,
    body: String,
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn dechunk(mut raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let Some(line_end) = find_subslice(raw, b"\r\n") else {
            break;
        };
        let size_str = String::from_utf8_lossy(&raw[..line_end]);
        let Ok(size) = usize::from_str_radix(size_str.trim(), 16) else {
            break;
        };
        if size == 0 {
            break;
        }
        let data_start = line_end + 2;
        let data_end = data_start + size;
        if data_end > raw.len() {
            break;
        }
        out.extend_from_slice(&raw[data_start..data_end]);
        raw = &raw[(data_end + 2).min(raw.len())..];
    }
    out
}

/// One raw request with a binary body (`content_type` selects the Loki
/// encoding). `body` empty and `content_type` `None` → a GET.
fn http_request(
    port: u16,
    method: &str,
    path: &str,
    content_type: Option<&str>,
    body: &[u8],
) -> Option<HttpResponse> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(15))).ok();

    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    if let Some(ct) = content_type {
        head.push_str(&format!("Content-Type: {ct}\r\n"));
    }
    if method != "GET" {
        head.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    head.push_str("\r\n");

    let mut request = head.into_bytes();
    request.extend_from_slice(body);
    stream.write_all(&request).ok()?;

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).ok()?;
    let split_at = find_subslice(&buf, b"\r\n\r\n")?;
    let head_text = String::from_utf8_lossy(&buf[..split_at]).into_owned();
    let raw_body = &buf[split_at + 4..];

    let mut lines = head_text.lines();
    let status = lines
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse::<u16>()
        .ok()?;
    let headers: HashMap<String, String> = lines
        .filter_map(|line| {
            let (k, v) = line.split_once(": ")?;
            Some((k.to_ascii_lowercase(), v.to_string()))
        })
        .collect();
    let dechunked = if headers
        .get("transfer-encoding")
        .is_some_and(|v| v == "chunked")
    {
        dechunk(raw_body)
    } else {
        raw_body.to_vec()
    };
    Some(HttpResponse {
        status,
        body: String::from_utf8_lossy(&dechunked).into_owned(),
    })
}

fn http_get(port: u16, path: &str) -> Option<HttpResponse> {
    http_request(port, "GET", path, None, &[])
}

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_ready(port: u16, db: &str, extra_env: &[(&str, &str)]) -> ChildGuard {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pulsusdb"));
    command
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env("CLICKHOUSE_SERVER", ch_host())
        .env("CLICKHOUSE_HTTP_PORT", ch_http_port().to_string())
        .env("CLICKHOUSE_DB", db);
    for (k, v) in extra_env {
        command.env(k, v);
    }
    let guard = ChildGuard(command.spawn().expect("spawn pulsusdb"));
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if http_get(port, "/ready").is_some_and(|r| r.status == 200) {
            return guard;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("/ready never reached 200 within 60s (port {port}, db {db})");
}

async fn drop_db(db: &str) {
    let cfg = ChConnConfig {
        server: ch_host(),
        http_port: ch_http_port(),
        database: "default".to_string(),
        proto: ChProto::Http,
        pool_size: 2,
        query_timeout: Duration::from_secs(20),
        ..ChConnConfig::default()
    };
    let client = ChClient::new(cfg).await.expect("connect bootstrap client");
    client
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop db");
}

// ---------------------------------------------------------------------
// Push body builders.
// ---------------------------------------------------------------------

/// A snappy-protobuf push body for one stream / one line.
fn protobuf_body(service: &str, ts_ns: i64, line: &str) -> Vec<u8> {
    let req = PushRequest {
        streams: vec![StreamAdapter {
            labels: format!(r#"{{service_name="{service}", env="prod"}}"#),
            entries: vec![EntryAdapter {
                timestamp: Some(Timestamp {
                    seconds: ts_ns / 1_000_000_000,
                    nanos: (ts_ns % 1_000_000_000) as i32,
                }),
                line: line.to_string(),
            }],
        }],
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy compress")
}

/// A JSON push body for one stream / one line.
fn json_body(service: &str, ts_ns: i64, line: &str) -> String {
    format!(
        r#"{{"streams":[{{"stream":{{"service_name":"{service}","env":"prod"}},"values":[["{ts_ns}","{line}"]]}}]}}"#
    )
}

fn push(port: u16, content_type: &str, body: &[u8]) -> HttpResponse {
    http_request(port, "POST", "/loki/api/v1/push", Some(content_type), body)
        .expect("push reachable")
}

fn urlencode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Every `query_range` result stream for `{service_name="<service>"}` over a
/// wide window around now, each paired with its COMPLETE returned label map
/// and its log lines — so a test can assert the specific pushed stream
/// carries its full expected label set (service_name AND env) end to end,
/// not merely that some line came back.
fn query_streams(
    port: u16,
    path_prefix: &str,
    service: &str,
    base_ns: i64,
) -> Vec<(std::collections::BTreeMap<String, String>, Vec<String>)> {
    let query = urlencode(&format!(r#"{{service_name="{service}"}}"#));
    let start = base_ns - 3_600_000_000_000; // 1h before
    let end = base_ns + 3_600_000_000_000; // 1h after
    let path = format!("{path_prefix}/query_range?query={query}&start={start}&end={end}&limit=100");
    let res = http_get(port, &path).expect("query reachable");
    assert_eq!(res.status, 200, "query_range status (body: {})", res.body);
    let json: serde_json::Value =
        serde_json::from_str(&res.body).unwrap_or_else(|e| panic!("json: {e}: {}", res.body));
    let mut out = Vec::new();
    for stream in json["data"]["result"].as_array().unwrap_or(&Vec::new()) {
        let labels = stream["stream"]
            .as_object()
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        let lines = stream["values"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .map(|value| value[1].as_str().unwrap_or_default().to_string())
            .collect();
        out.push((labels, lines));
    }
    out
}

/// Every log line returned by `query_range` for `{service_name="<service>"}`
/// over a wide window around now (label maps flattened away).
fn query_lines(port: u16, path_prefix: &str, service: &str, base_ns: i64) -> Vec<String> {
    query_streams(port, path_prefix, service, base_ns)
        .into_iter()
        .flat_map(|(_, lines)| lines)
        .collect()
}

/// The COMPLETE label map of the query_range result stream that carries
/// `line`. Panics if no returned stream contains the line (callers gate on
/// `wait_for_line` first, so the stream is present).
fn labels_of_stream_carrying(
    port: u16,
    path_prefix: &str,
    service: &str,
    base_ns: i64,
    line: &str,
) -> std::collections::BTreeMap<String, String> {
    query_streams(port, path_prefix, service, base_ns)
        .into_iter()
        .find(|(_, lines)| lines.iter().any(|l| l == line))
        .unwrap_or_else(|| panic!("no query_range stream carried line {line:?}"))
        .0
}

/// The expected COMPLETE label map for a stream pushed by the test builders
/// (`service_name=<service>`, `env=prod`) — nothing else.
fn expected_pushed_labels(service: &str) -> std::collections::BTreeMap<String, String> {
    [("env", "prod"), ("service_name", service)]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Polls `query_range` until `line` shows up for `service` or the deadline
/// passes (the writer flushes asynchronously; the push handler's sync-flush
/// confirmation makes this near-immediate, but a small poll absorbs any
/// merge latency).
fn wait_for_line(port: u16, service: &str, base_ns: i64, line: &str) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let lines = query_lines(port, "/api/logs/v1", service, base_ns);
        if lines.iter().any(|l| l == line) {
            return lines;
        }
        assert!(
            Instant::now() < deadline,
            "line {line:?} never appeared for service {service:?} (got {lines:?})"
        );
        std::thread::sleep(Duration::from_millis(300));
    }
}

// ---------------------------------------------------------------------
// AC-7a: push (both encodings) -> LogQL query_range round-trip.
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn push_both_encodings_then_query_range_returns_the_exact_entries() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_150;
    let db = "pulsus_loki_push_it";
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[("PULSUS_COMPAT_ENDPOINTS", "1")]);

    let base_ns = now_ns();

    // Protobuf (the agent default): a distinct service label so the two
    // encodings' streams are independently verifiable.
    let proto_line = "loki push over snappy protobuf";
    let res = push(
        port,
        "application/x-protobuf",
        &protobuf_body("checkout-proto", base_ns, proto_line),
    );
    assert_eq!(res.status, 204, "protobuf push -> 204 (body {})", res.body);
    assert!(res.body.is_empty(), "204 carries no body");

    // JSON.
    let json_line = "loki push over json";
    let res = push(
        port,
        "application/json",
        json_body("checkout-json", base_ns, json_line).as_bytes(),
    );
    assert_eq!(res.status, 204, "json push -> 204 (body {})", res.body);

    // Native LogQL query_range returns each pushed line under its labels.
    let proto_lines = wait_for_line(port, "checkout-proto", base_ns, proto_line);
    assert!(
        proto_lines.contains(&proto_line.to_string()),
        "protobuf-pushed line queryable via LogQL: {proto_lines:?}"
    );
    let json_lines = wait_for_line(port, "checkout-json", base_ns, json_line);
    assert!(
        json_lines.contains(&json_line.to_string()),
        "json-pushed line queryable via LogQL: {json_lines:?}"
    );

    // The specific pushed stream must carry its COMPLETE label map end to
    // end — service_name AND env, and nothing else — proven via the actual
    // query result stream, not merely via global label-name presence.
    let proto_labels =
        labels_of_stream_carrying(port, "/api/logs/v1", "checkout-proto", base_ns, proto_line);
    assert_eq!(
        proto_labels,
        expected_pushed_labels("checkout-proto"),
        "protobuf-pushed stream must round-trip its exact label set"
    );
    let json_labels =
        labels_of_stream_carrying(port, "/api/logs/v1", "checkout-json", base_ns, json_line);
    assert_eq!(
        json_labels,
        expected_pushed_labels("checkout-json"),
        "json-pushed stream must round-trip its exact label set"
    );

    // The `/loki/api/v1/query_range` compat alias returns the same set (the
    // pushed stream is byte-shape-identical to any other log stream).
    let via_alias = query_lines(port, "/loki/api/v1", "checkout-proto", base_ns);
    assert!(
        via_alias.contains(&proto_line.to_string()),
        "pushed stream also queryable via the /loki alias: {via_alias:?}"
    );

    // The stream's labels are discoverable — `service_name` and `env` both
    // made it through the LabelSet::from_normalized seam.
    let labels = http_get(port, "/api/logs/v1/labels").expect("labels reachable");
    assert_eq!(labels.status, 200);
    let labels_json: serde_json::Value = serde_json::from_str(&labels.body).unwrap();
    let names: Vec<&str> = labels_json["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    assert!(names.contains(&"service_name"), "labels: {names:?}");
    assert!(names.contains(&"env"), "labels: {names:?}");
}

// ---------------------------------------------------------------------
// AC-7b: a pushed stream appears in /api/logs/v1/tail (WebSocket).
// ---------------------------------------------------------------------

struct WsClient {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl WsClient {
    fn connect(port: u16, target: &str) -> WsClient {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_millis(200)))
            .expect("timeout");
        let head = format!(
            "GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
        );
        stream.write_all(head.as_bytes()).expect("handshake");
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        let deadline = Instant::now() + Duration::from_secs(10);
        let split_at = loop {
            if let Some(i) = find_subslice(&buf, b"\r\n\r\n") {
                break i;
            }
            assert!(Instant::now() < deadline, "no handshake response");
            match stream.read(&mut chunk) {
                Ok(0) => panic!("closed during handshake"),
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut => {}
                Err(e) => panic!("handshake read: {e}"),
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
        WsClient {
            stream,
            buf: buf[split_at + 4..].to_vec(),
        }
    }

    fn next_text(&mut self, deadline: Instant) -> Option<String> {
        let mut chunk = [0u8; 65536];
        loop {
            if let Some((frame, consumed)) = parse_ws_frame(&self.buf) {
                self.buf.drain(..consumed);
                match frame {
                    Some(text) => return Some(text),
                    None => continue,
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
        let _ = self.stream.write_all(&[0x88, 0x80, 0x12, 0x34, 0x56, 0x78]);
    }
}

fn parse_ws_frame(buf: &[u8]) -> Option<(Option<String>, usize)> {
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
        0x1 => Some(Some(String::from_utf8_lossy(payload).into_owned())),
        0x8 => Some(None),
        _ => Some(None),
    };
    frame.map(|f| (f, header + len))
}

#[tokio::test(flavor = "multi_thread")]
async fn pushed_stream_appears_in_tail() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_151;
    let db = "pulsus_loki_push_tail_it";
    drop_db(db).await;
    let _guard = spawn_ready(
        port,
        db,
        &[
            ("PULSUS_COMPAT_ENDPOINTS", "1"),
            ("PULSUS_TAIL_POLL_INTERVAL", "200ms"),
        ],
    );

    let service = "checkout-tail";

    // Establish the stream first via a #77 push (tail resolves matching
    // streams from `log_streams`, then tails new rows into them — same shape
    // as `logs_tail_live.rs`'s own seed-then-tail flow), and wait until it is
    // queryable so the stream row is durably present.
    let base_ns = now_ns();
    let seed_line = "seed via loki push";
    let res = push(
        port,
        "application/x-protobuf",
        &protobuf_body(service, base_ns, seed_line),
    );
    assert_eq!(res.status, 204, "seed push -> 204 (body {})", res.body);
    wait_for_line(port, service, base_ns, seed_line);

    let query = urlencode(&format!(r#"{{service_name="{service}"}}"#));
    let mut ws = WsClient::connect(port, &format!("/api/logs/v1/tail?query={query}"));

    // Give the tail its initial poll cursor a moment to settle, then push a
    // brand-new line via #77 with a fresh timestamp.
    std::thread::sleep(Duration::from_millis(500));
    let line = "tailed loki push line";
    let ts = now_ns();
    let res = push(
        port,
        "application/x-protobuf",
        &protobuf_body(service, ts, line),
    );
    assert_eq!(res.status, 204, "push -> 204 (body {})", res.body);

    // The pushed line arrives on the tail stream carrying its COMPLETE label
    // set (not just service_name) — captured here for an exact assertion.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut found_labels: Option<std::collections::BTreeMap<String, String>> = None;
    while Instant::now() < deadline && found_labels.is_none() {
        let Some(text) = ws.next_text(deadline) else {
            continue;
        };
        let frame: serde_json::Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(_) => continue,
        };
        for stream in frame["streams"].as_array().unwrap_or(&Vec::new()) {
            let labels: std::collections::BTreeMap<String, String> = stream["stream"]
                .as_object()
                .map(|m| {
                    m.iter()
                        .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                        .collect()
                })
                .unwrap_or_default();
            let svc = labels
                .get("service_name")
                .map(String::as_str)
                .unwrap_or_default();
            for value in stream["values"].as_array().unwrap_or(&Vec::new()) {
                if svc == service && value[1].as_str() == Some(line) {
                    found_labels = Some(labels.clone());
                }
            }
        }
    }
    ws.close();
    let labels = found_labels.expect("the #77-pushed line must arrive on /api/logs/v1/tail");
    assert_eq!(
        labels,
        expected_pushed_labels(service),
        "the tailed frame's pushed stream must carry its full label set (service_name AND env)"
    );
}
