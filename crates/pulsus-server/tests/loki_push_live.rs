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
//! Issue #374 added the per-stream label bounds' live gates here, and with
//! them one case on the sibling OTLP logs receiver (`POST /v1/logs`): the
//! bounds' coverage gap is only visible in what was **stored**, so it is
//! asserted against ClickHouse rather than against a status.
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
//!     clickhouse/clickhouse-server:26.3
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-server --test loki_push_live
//! podman rm -f pulsus-ch-test
//! ```

#[path = "support/live_db.rs"]
mod live_db;

use live_db::drop_db;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use futures::StreamExt;
use prost::Message;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, QuerySettings};
use pulsus_write::protocols::loki_push::{
    EntryAdapter, LabelPairAdapter, PushRequest, StreamAdapter, Timestamp,
};

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping when
/// the gate is absent in a live CI job, so a lost `env:` block reddens the
/// build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
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

/// A single-column `count()` against `db`, used to prove a rejected push
/// left NOTHING behind — the response status alone cannot show that.
async fn ch_count(db: &str, sql: &str) -> u64 {
    let cfg = ChConnConfig {
        server: ch_host(),
        http_port: ch_http_port(),
        database: db.to_string(),
        proto: ChProto::Http,
        pool_size: 2,
        query_timeout: Duration::from_secs(20),
        ..ChConnConfig::default()
    };
    let client = ChClient::new(cfg).await.expect("connect count client");

    #[derive(pulsus_clickhouse::Row, serde::Serialize, serde::Deserialize, Debug)]
    struct CountRow {
        c: u64,
    }
    // The stream is scoped to this helper so its pooled connection lease is
    // released before the caller's next query.
    let mut stream = client
        .query_stream::<CountRow>(sql, &QuerySettings::new())
        .await
        .expect("count query");
    futures::StreamExt::next(&mut stream)
        .await
        .expect("count returns exactly one row")
        .expect("decode count row")
        .c
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
                structured_metadata: Vec::new(),
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

/// A snappy-protobuf push body for one stream / one line, carrying per-entry
/// structured metadata (issue #97).
fn protobuf_body_with_sm(service: &str, ts_ns: i64, line: &str, sm: &[(&str, &str)]) -> Vec<u8> {
    let req = PushRequest {
        streams: vec![StreamAdapter {
            labels: format!(r#"{{service_name="{service}", env="prod"}}"#),
            entries: vec![EntryAdapter {
                timestamp: Some(Timestamp {
                    seconds: ts_ns / 1_000_000_000,
                    nanos: (ts_ns % 1_000_000_000) as i32,
                }),
                line: line.to_string(),
                structured_metadata: sm
                    .iter()
                    .map(|(k, v)| LabelPairAdapter {
                        name: k.to_string(),
                        value: v.to_string(),
                    })
                    .collect(),
            }],
        }],
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy compress")
}

/// A JSON push body for one stream / one line, carrying per-entry structured
/// metadata as the values array's third element (issue #97).
fn json_body_with_sm(service: &str, ts_ns: i64, line: &str, sm: &[(&str, &str)]) -> String {
    let sm_obj: String = sm
        .iter()
        .map(|(k, v)| format!(r#""{k}":"{v}""#))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        r#"{{"streams":[{{"stream":{{"service_name":"{service}","env":"prod"}},"values":[["{ts_ns}","{line}",{{{sm_obj}}}]]}}]}}"#
    )
}

/// Runs `query` (a raw LogQL query, url-encoded here) over a wide window and
/// returns each result stream's COMPLETE label map paired with its lines.
fn query_streams_raw(
    port: u16,
    path_prefix: &str,
    query: &str,
    base_ns: i64,
) -> Vec<(std::collections::BTreeMap<String, String>, Vec<String>)> {
    let encoded = urlencode(query);
    let start = base_ns - 3_600_000_000_000;
    let end = base_ns + 3_600_000_000_000;
    let path =
        format!("{path_prefix}/query_range?query={encoded}&start={start}&end={end}&limit=100");
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
    let db = &pulsus_testkit::test_db("pulsus_loki_push_it");
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
// Issue #97 (AC-7): per-entry structured metadata fans into the response
// stream labels (the oracle-probed Loki 3.4.2 default), is byte-identical
// across encodings (AC-4), and is filterable via a `| key="value"` label
// filter in the pipeline.
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn push_structured_metadata_surfaces_in_query_range_and_is_filterable() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_152;
    let db = &pulsus_testkit::test_db("pulsus_loki_push_sm_it");
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[("PULSUS_COMPAT_ENDPOINTS", "1")]);

    let base_ns = now_ns();
    let sm = [("trace_id", "abc123"), ("user_id", "42")];

    // Push the SAME logical entry (line + SM) over both encodings.
    let proto_line = "sm over protobuf";
    let res = push(
        port,
        "application/x-protobuf",
        &protobuf_body_with_sm("sm-proto", base_ns, proto_line, &sm),
    );
    assert_eq!(res.status, 204, "protobuf SM push (body {})", res.body);

    let json_line = "sm over json";
    let res = push(
        port,
        "application/json",
        json_body_with_sm("sm-json", base_ns, json_line, &sm).as_bytes(),
    );
    assert_eq!(res.status, 204, "json SM push (body {})", res.body);

    // AC-7: the SM keys fan into the response stream labels alongside the
    // base labels (matching the oracle-probed Loki 3.4.2 default).
    wait_for_line(port, "sm-proto", base_ns, proto_line);
    let proto_labels =
        labels_of_stream_carrying(port, "/api/logs/v1", "sm-proto", base_ns, proto_line);
    let mut expected_proto = expected_pushed_labels("sm-proto");
    expected_proto.insert("trace_id".to_string(), "abc123".to_string());
    expected_proto.insert("user_id".to_string(), "42".to_string());
    assert_eq!(
        proto_labels, expected_proto,
        "structured metadata must fan into the protobuf-pushed stream's labels"
    );

    // AC-4: the JSON encoding yields the byte-identical merged label set.
    wait_for_line(port, "sm-json", base_ns, json_line);
    let json_labels =
        labels_of_stream_carrying(port, "/api/logs/v1", "sm-json", base_ns, json_line);
    let mut expected_json = expected_pushed_labels("sm-json");
    expected_json.insert("trace_id".to_string(), "abc123".to_string());
    expected_json.insert("user_id".to_string(), "42".to_string());
    assert_eq!(
        json_labels, expected_json,
        "the JSON encoding must produce the same merged SM label set as protobuf"
    );

    // AC-7: a `| key="value"` SM label filter selects the entry.
    let matching = query_streams_raw(
        port,
        "/api/logs/v1",
        r#"{service_name="sm-proto"} | trace_id="abc123""#,
        base_ns,
    );
    let matched_lines: Vec<String> = matching.into_iter().flat_map(|(_, l)| l).collect();
    assert!(
        matched_lines.contains(&proto_line.to_string()),
        "an SM label filter matching the entry must return it: {matched_lines:?}"
    );

    // And a non-matching SM filter rejects it.
    let rejecting = query_streams_raw(
        port,
        "/api/logs/v1",
        r#"{service_name="sm-proto"} | trace_id="nope""#,
        base_ns,
    );
    let rejected_lines: Vec<String> = rejecting.into_iter().flat_map(|(_, l)| l).collect();
    assert!(
        !rejected_lines.contains(&proto_line.to_string()),
        "an SM label filter that does not match must exclude the entry: {rejected_lines:?}"
    );
}

/// Issue #97 review round 1, finding 3 (+ grafana/loki:3.4.2 oracle probe):
/// a structured-metadata key that collides with a stream label key surfaces
/// under the `<key>_extracted` suffix; the stream label keeps the original key
/// and value, both appear exactly once (no duplicate key entries), and the
/// `_extracted` label is filterable. Non-colliding SM merges verbatim.
#[tokio::test(flavor = "multi_thread")]
async fn structured_metadata_colliding_with_a_stream_label_lands_under_extracted_suffix() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_153;
    let db = &pulsus_testkit::test_db("pulsus_loki_push_sm_collision_it");
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[("PULSUS_COMPAT_ENDPOINTS", "1")]);

    let base_ns = now_ns();
    // Base stream labels are service_name=<service>, env=prod. `env` collides;
    // `region` does not.
    let sm = [("env", "smval"), ("region", "us-east")];
    let line = "sm collides with stream label";
    let res = push(
        port,
        "application/json",
        json_body_with_sm("sm-collide", base_ns, line, &sm).as_bytes(),
    );
    assert_eq!(res.status, 204, "collision SM push (body {})", res.body);

    wait_for_line(port, "sm-collide", base_ns, line);
    let got = labels_of_stream_carrying(port, "/api/logs/v1", "sm-collide", base_ns, line);
    let mut expected = expected_pushed_labels("sm-collide"); // env=prod, service_name=...
    expected.insert("env_extracted".to_string(), "smval".to_string());
    expected.insert("region".to_string(), "us-east".to_string());
    assert_eq!(
        got, expected,
        "colliding SM key `env` must surface as `env_extracted` (stream `env` keeps `prod`), \
         non-colliding `region` merges verbatim, and each key appears exactly once"
    );

    // The renamed label is filterable under its `_extracted` name.
    let matching = query_streams_raw(
        port,
        "/api/logs/v1",
        r#"{service_name="sm-collide"} | env_extracted="smval""#,
        base_ns,
    );
    let matched: Vec<String> = matching.into_iter().flat_map(|(_, l)| l).collect();
    assert!(
        matched.contains(&line.to_string()),
        "the `_extracted` SM label must be filterable: {matched:?}"
    );

    // Filtering on the original key value keeps matching the STREAM label.
    let stream_label = query_streams_raw(
        port,
        "/api/logs/v1",
        r#"{service_name="sm-collide"} | env="prod""#,
        base_ns,
    );
    let stream_matched: Vec<String> = stream_label.into_iter().flat_map(|(_, l)| l).collect();
    assert!(
        stream_matched.contains(&line.to_string()),
        "the stream label `env=prod` must still match under its original key: {stream_matched:?}"
    );
}

/// Issue #97 review round 2, finding 1 (+ grafana/loki:3.4.2 oracle probe):
/// a DOUBLE collision must not emit a duplicate label entry. The stream's base
/// labels already carry BOTH `env` AND `env_extracted`; the colliding SM `env`
/// renames to `env_extracted`, which also exists — so it overwrites that slot
/// (last-write-wins), leaving exactly one `env_extracted` (the SM value). Probed
/// against grafana/loki:3.4.2's default query response: base
/// `env=prod`+`env_extracted=baseval` + SM `env=smval` renders one
/// `env_extracted=smval`; no `env_extracted_extracted`, no numeric suffix, no
/// drop.
#[tokio::test(flavor = "multi_thread")]
async fn structured_metadata_double_collision_overwrites_the_extracted_slot_once() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_154;
    let db = &pulsus_testkit::test_db("pulsus_loki_push_sm_double_collision_it");
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[("PULSUS_COMPAT_ENDPOINTS", "1")]);

    let base_ns = now_ns();
    let service = "sm-double";
    let line = "sm double-collides with a base _extracted label";
    // Base stream labels carry both `env` AND `env_extracted`; SM `env` collides
    // twice.
    let body = format!(
        r#"{{"streams":[{{"stream":{{"service_name":"{service}","env":"prod","env_extracted":"baseval"}},"values":[["{base_ns}","{line}",{{"env":"smval"}}]]}}]}}"#
    );
    let res = push(port, "application/json", body.as_bytes());
    assert_eq!(
        res.status, 204,
        "double-collision SM push (body {})",
        res.body
    );

    wait_for_line(port, service, base_ns, line);
    let got = labels_of_stream_carrying(port, "/api/logs/v1", service, base_ns, line);
    let expected: std::collections::BTreeMap<String, String> = [
        ("service_name", service),
        ("env", "prod"),
        ("env_extracted", "smval"), // SM value wins the single _extracted slot
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    assert_eq!(
        got, expected,
        "double collision must yield exactly one `env_extracted` (SM value wins), \
         `env=prod` kept, no duplicate key entries"
    );

    // The surviving `env_extracted` filters on the SM value, not the base value.
    let hit = query_streams_raw(
        port,
        "/api/logs/v1",
        &format!(r#"{{service_name="{service}"}} | env_extracted="smval""#),
        base_ns,
    );
    let hit_lines: Vec<String> = hit.into_iter().flat_map(|(_, l)| l).collect();
    assert!(
        hit_lines.contains(&line.to_string()),
        "the winning `env_extracted=smval` must be filterable: {hit_lines:?}"
    );
    let miss = query_streams_raw(
        port,
        "/api/logs/v1",
        &format!(r#"{{service_name="{service}"}} | env_extracted="baseval""#),
        base_ns,
    );
    let miss_lines: Vec<String> = miss.into_iter().flat_map(|(_, l)| l).collect();
    assert!(
        !miss_lines.contains(&line.to_string()),
        "the overwritten base `env_extracted=baseval` must NOT match: {miss_lines:?}"
    );
}

// ---------------------------------------------------------------------
// Issue #259: empty-valued structured metadata and empty-valued stream
// labels are stripped at INGEST, so they are never written. Asserted on the
// STORED ClickHouse rows (`log_samples.structured_metadata`,
// `log_streams.labels`) rather than on a query response — the defect is a
// write-path one, and a read-path assertion could be satisfied by a reader
// that filtered on the way out.
// ---------------------------------------------------------------------

/// One `log_samples` row's stored body + structured metadata.
#[derive(clickhouse::Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct StoredSample {
    body: String,
    structured_metadata: String,
    fingerprint: u64,
}

/// One `log_streams` row's stored canonical label JSON.
#[derive(clickhouse::Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct StoredStream {
    fingerprint: u64,
    labels: String,
}

async fn ch_client(db: &str) -> ChClient {
    let cfg = ChConnConfig {
        server: ch_host(),
        http_port: ch_http_port(),
        database: db.to_string(),
        proto: ChProto::Http,
        pool_size: 2,
        query_timeout: Duration::from_secs(20),
        ..ChConnConfig::default()
    };
    ChClient::new(cfg).await.expect("connect read-back client")
}

/// Every stored `log_samples` row for `service`, keyed by its `body`. Polls
/// until `expected` rows have landed (the writer batches), so this never
/// depends on a fixed sleep.
async fn stored_samples_by_body(
    client: &ChClient,
    db: &str,
    service: &str,
    expected: usize,
) -> HashMap<String, StoredSample> {
    let sql = format!(
        "SELECT body, structured_metadata, fingerprint FROM {db}.log_samples \
         WHERE service = '{service}'"
    );
    let mut last = HashMap::new();
    for _ in 0..80 {
        let mut out: HashMap<String, StoredSample> = HashMap::new();
        // Scoped so the pooled connection's lease is released before the next
        // poll iteration borrows one.
        {
            let mut rows = client
                .query_stream::<StoredSample>(&sql, &QuerySettings::new())
                .await
                .expect("query log_samples");
            while let Some(row) = rows.next().await {
                let row = row.expect("decode log_samples row");
                out.insert(row.body.clone(), row);
            }
        }
        if out.len() >= expected {
            return out;
        }
        last = out;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("only {} of {expected} samples landed: {last:?}", last.len());
}

/// Every stored `log_streams` row for `service`.
async fn stored_streams(client: &ChClient, db: &str, service: &str) -> Vec<StoredStream> {
    let sql = format!(
        "SELECT fingerprint, labels FROM {db}.log_streams WHERE service = '{service}' \
         GROUP BY fingerprint, labels ORDER BY fingerprint"
    );
    let mut out = Vec::new();
    let mut rows = client
        .query_stream::<StoredStream>(&sql, &QuerySettings::new())
        .await
        .expect("query log_streams");
    while let Some(row) = rows.next().await {
        out.push(row.expect("decode log_streams row"));
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_valued_structured_metadata_is_never_stored() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_162;
    let db = &pulsus_testkit::test_db("pulsus_loki_push_empty_sm_it");
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[("PULSUS_COMPAT_ENDPOINTS", "1")]);

    let base_ns = now_ns();
    let service = "sm-empty";

    // (1) JSON: one empty-valued pair beside a non-empty one.
    let mixed = "sm empty value beside a kept one";
    let res = push(
        port,
        "application/json",
        json_body_with_sm(service, base_ns, mixed, &[("a", ""), ("b", "v")]).as_bytes(),
    );
    assert_eq!(res.status, 204, "mixed SM push (body {})", res.body);

    // (2) protobuf: the SAME logical entry, to prove the strip is not
    // transport-specific.
    let mixed_proto = "sm empty value beside a kept one over protobuf";
    let res = push(
        port,
        "application/x-protobuf",
        &protobuf_body_with_sm(service, base_ns + 1, mixed_proto, &[("a", ""), ("b", "v")]),
    );
    assert_eq!(
        res.status, 204,
        "mixed SM protobuf push (body {})",
        res.body
    );

    // (3) an entry whose WHOLE metadata set is empty-valued: the entry is
    // still stored, with no structured metadata at all.
    let all_empty = "sm entirely empty valued";
    let res = push(
        port,
        "application/json",
        json_body_with_sm(service, base_ns + 2, all_empty, &[("a", "")]).as_bytes(),
    );
    assert_eq!(res.status, 204, "all-empty SM push (body {})", res.body);

    // (4) whitespace is NOT empty — it must survive verbatim.
    let whitespace = "sm whitespace value survives";
    let res = push(
        port,
        "application/json",
        json_body_with_sm(service, base_ns + 3, whitespace, &[("a", " ")]).as_bytes(),
    );
    assert_eq!(res.status, 204, "whitespace SM push (body {})", res.body);

    // (5)-(8) the delete runs on the NORMALIZED name, so an empty pair
    // suppresses whatever shares the key it would be stored under — and a
    // renamed non-empty pair resurrects a name a delete took. All four rows
    // measured on `grafana/loki:3.7.4`, each on both push encodings.
    let collides = "sm empty pair renames onto its twin";
    let res = push(
        port,
        "application/json",
        json_body_with_sm(
            service,
            base_ns + 4,
            collides,
            &[("a.b", ""), ("a_b", "keep")],
        )
        .as_bytes(),
    );
    assert_eq!(res.status, 204, "collision SM push (body {})", res.body);

    let collides_proto = "sm empty pair renames onto its twin over protobuf";
    let res = push(
        port,
        "application/x-protobuf",
        &protobuf_body_with_sm(
            service,
            base_ns + 5,
            collides_proto,
            &[("a.b", ""), ("a_b", "keep")],
        ),
    );
    assert_eq!(
        res.status, 204,
        "collision SM protobuf push (body {})",
        res.body
    );

    let resurrects = "sm rename resurrects the deleted name";
    let res = push(
        port,
        "application/json",
        json_body_with_sm(
            service,
            base_ns + 6,
            resurrects,
            &[("a.b", "keep"), ("a_b", "")],
        )
        .as_bytes(),
    );
    assert_eq!(res.status, 204, "resurrect SM push (body {})", res.body);

    let resurrects_proto = "sm rename resurrects the deleted name over protobuf";
    let res = push(
        port,
        "application/x-protobuf",
        &protobuf_body_with_sm(
            service,
            base_ns + 7,
            resurrects_proto,
            &[("a.b", ""), ("a.b", "keep")],
        ),
    );
    assert_eq!(
        res.status, 204,
        "resurrect SM protobuf push (body {})",
        res.body
    );

    let client = ch_client(db).await;
    let stored = stored_samples_by_body(&client, db, service, 8).await;

    assert_eq!(
        stored[mixed].structured_metadata, r#"{"b":"v"}"#,
        "the empty-valued pair must not be in the STORED JSON"
    );
    assert_eq!(
        stored[collides].structured_metadata, "",
        "`a.b=\"\"` normalizes onto `a_b` and deletes it, so nothing is stored"
    );
    assert_eq!(
        stored[collides_proto].structured_metadata, "",
        "…identically on the protobuf encoding"
    );
    assert_eq!(
        stored[resurrects].structured_metadata, r#"{"a_b":"keep"}"#,
        "a renamed non-empty pair outranks the delete its empty twin recorded"
    );
    assert_eq!(
        stored[resurrects_proto].structured_metadata, r#"{"a_b":"keep"}"#,
        "…and a duplicate renameable name resurrects in this order too"
    );
    assert_eq!(
        stored[mixed_proto].structured_metadata, r#"{"b":"v"}"#,
        "protobuf stores byte-identically to JSON"
    );
    assert_eq!(
        stored[all_empty].structured_metadata, "",
        "an all-empty-valued set stores as the empty string (not `{{}}`), and the \
         entry itself is still stored"
    );
    assert_eq!(
        stored[whitespace].structured_metadata, "{\"a\":\" \"}",
        "only an exactly-empty value is dropped — no trimming"
    );

    // Nothing anywhere in this service's stored metadata carries an empty
    // value. `to_canonical_json` renders a pair as `"k":"v"` with no spacing,
    // so the three-byte token `:""` appears if and only if some value is
    // empty — a value that merely CONTAINS `:""` is escaped to `:\"\"` and
    // does not match.
    for (body, row) in &stored {
        assert!(
            !row.structured_metadata.contains(":\"\""),
            "stored row {body:?} still carries an empty-valued pair: {}",
            row.structured_metadata
        );
    }
}

/// Issue #381: the collision resolution reaches STORAGE, not just the
/// parser — the value ClickHouse holds in `log_samples.structured_metadata`
/// is the one the reference stores.
///
/// Four rows, chosen so no two are decided by the same half of the builder:
/// `c01` a rename replacing a base twin, `c03` two renames onto one name,
/// `c06` two base duplicates and no rename at all, `c16` an empty value whose
/// `Reset` delete is then re-added by two renames. The expected values are
/// the pinned reference's, captured raw at
/// `pulsus-write/tests/fixtures/structured_metadata_collisions/capture.json`
/// and asserted against the parser by that suite; this test asserts the same
/// four survive the writer, the wire and ClickHouse unchanged. Each row is
/// pushed on BOTH encodings, because the storage claim is per transport.
///
/// Before this fix these four stored `keep`, `1`, `2` and `x` respectively —
/// the frozen greatest-original-key resolution.
#[tokio::test(flavor = "multi_thread")]
async fn colliding_structured_metadata_is_stored_as_the_reference_resolves_it() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_166;
    let db = &pulsus_testkit::test_db("pulsus_loki_push_sm_collision_it");
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[("PULSUS_COMPAT_ENDPOINTS", "1")]);

    let base_ns = now_ns();
    let service = "sm-collision";

    /// One capture row: its id, the pairs pushed in wire order, and the
    /// value the pinned reference stores for them.
    struct CollisionRow {
        id: &'static str,
        sm: &'static [(&'static str, &'static str)],
        expected: &'static str,
    }
    let rows = &[
        CollisionRow {
            id: "c01",
            sm: &[("a.b", "x"), ("a_b", "keep")],
            expected: r#"{"a_b":"x"}"#,
        },
        CollisionRow {
            id: "c03",
            sm: &[("a.b", "1"), ("a-b", "2")],
            expected: r#"{"a_b":"2"}"#,
        },
        CollisionRow {
            id: "c06",
            sm: &[("a_b", "2"), ("a_b", "1")],
            expected: r#"{"a_b":"1"}"#,
        },
        CollisionRow {
            id: "c16",
            sm: &[("a_b", ""), ("a.b", "x"), ("a-b", "y")],
            expected: r#"{"a_b":"y"}"#,
        },
    ];

    let mut expected_by_body: Vec<(String, &str)> = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        let (id, sm) = (row.id, row.sm);
        let offset = index as i64 * 2;
        let json_line = format!("collision {id} over json");
        let res = push(
            port,
            "application/json",
            json_body_with_sm(service, base_ns + offset, &json_line, sm).as_bytes(),
        );
        assert_eq!(res.status, 204, "{id} json push (body {})", res.body);
        expected_by_body.push((json_line, row.expected));

        let proto_line = format!("collision {id} over protobuf");
        let res = push(
            port,
            "application/x-protobuf",
            &protobuf_body_with_sm(service, base_ns + offset + 1, &proto_line, sm),
        );
        assert_eq!(res.status, 204, "{id} protobuf push (body {})", res.body);
        expected_by_body.push((proto_line, row.expected));
    }

    let client = ch_client(db).await;
    let stored = stored_samples_by_body(&client, db, service, expected_by_body.len()).await;
    for (body, expected) in &expected_by_body {
        assert_eq!(
            stored[body].structured_metadata, *expected,
            "stored metadata for {body:?}"
        );
    }
}

/// Issue #259: an inadmissible structured-metadata NAME is refused at the
/// wire on both push encodings, and nothing is stored for it.
///
/// Measured against `grafana/loki:3.7.4` (image ID `fe5a84aafad8`, index
/// digest `sha256:87f0a067…`, git revision `b318f282`) with the same
/// bodies: it refuses each of them too, and the response BODY BYTES match —
/// terminating `\n` included, which is why the assertion below compares the
/// whole body rather than a `.trim()`ed one. (An earlier revision trimmed,
/// and could not have seen a missing terminator; the reference writes every
/// push error through `http.Error` -> `fmt.Fprintln`,
/// `pkg/loghttp/push/push.go:606-608 @ v3.7.4`.) The STATUS is a deliberate
/// divergence — it answers `500` here and `400` for the identical condition
/// on its own OTLP receiver; docs/api.md §8.2 has the reasoning.
/// Before this change every one of these bodies was a `204` that stored a row.
#[tokio::test(flavor = "multi_thread")]
async fn inadmissible_structured_metadata_names_are_refused_and_nothing_is_stored() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_164;
    let db = &pulsus_testkit::test_db("pulsus_loki_push_sm_name_it");
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[("PULSUS_COMPAT_ENDPOINTS", "1")]);

    let base_ns = now_ns();
    let service = "sm-name";

    for (offset, content_type, sm, expected) in [
        // The empty name — the first rejection condition — on both encodings.
        (0i64, "application/json", ("", ""), "label name is empty"),
        (1, "application/x-protobuf", ("", ""), "label name is empty"),
        // The second condition: whitespace is not "empty", it sanitizes to
        // `_`. Its empty VALUE does not rescue it — the name is checked
        // first, so this is a reject rather than a silent strip.
        (
            2,
            "application/json",
            (" ", ""),
            r#"normalization for label name " " resulted in invalid name "_""#,
        ),
        (
            3,
            "application/x-protobuf",
            ("_", "v"),
            r#"normalization for label name "_" resulted in invalid name "_""#,
        ),
        // Non-ASCII names, both encodings. `µ` and `日本` keep no ASCII
        // alphanumeric at all, so they sanitize to a lone `_` and are
        // refused — measured on `grafana/loki:3.7.4`, which answers these
        // exact sentences. Their admitted counterpart `naïve` is pushed
        // below.
        (
            4,
            "application/json",
            ("µ", "v"),
            r#"normalization for label name "µ" resulted in invalid name "_""#,
        ),
        (
            5,
            "application/x-protobuf",
            ("日本", "v"),
            r#"normalization for label name "日本" resulted in invalid name "_""#,
        ),
    ] {
        let line = format!("sm name case {offset}");
        let ts = base_ns + offset;
        let res = if content_type == "application/json" {
            push(
                port,
                content_type,
                json_body_with_sm(service, ts, &line, &[sm]).as_bytes(),
            )
        } else {
            push(
                port,
                content_type,
                &protobuf_body_with_sm(service, ts, &line, &[sm]),
            )
        };
        assert_eq!(
            res.status, 400,
            "case {offset} ({content_type}): {}",
            res.body
        );
        // Exact bytes, terminator included — NOT `.trim()`ed. The reference's
        // own body for each of these is `<message>\n`.
        assert_eq!(
            res.body,
            format!("{expected}\n"),
            "case {offset} ({content_type}): body bytes {:?}",
            res.body.as_bytes()
        );
    }

    // Admissible names pushed afterwards prove the receiver is still healthy
    // and give the stored-row query something to find, so "no rows for the
    // rejected bodies" is a real absence rather than a dead server.
    let ok_line = "sm name admissible";
    let res = push(
        port,
        "application/json",
        json_body_with_sm(service, base_ns + 6, ok_line, &[("a.b", "v")]).as_bytes(),
    );
    assert_eq!(res.status, 204, "admissible SM name push: {}", res.body);

    // The accept side of the non-ASCII trio: `naïve` keeps four ASCII
    // letters, so the reference admits it (measured, 204) where it refuses
    // `µ` and `日本` above. Stored under PulsusDB's own canonical key —
    // `na_ve`, one `_` per non-ASCII CHARACTER, not per byte.
    let naive_line = "sm name admissible non-ascii";
    let res = push(
        port,
        "application/json",
        json_body_with_sm(service, base_ns + 7, naive_line, &[("naïve", "v")]).as_bytes(),
    );
    assert_eq!(
        res.status, 204,
        "admissible naive SM name push: {}",
        res.body
    );

    let client = ch_client(db).await;
    let stored = stored_samples_by_body(&client, db, service, 2).await;
    assert_eq!(
        stored.len(),
        2,
        "only the admissible pushes may be stored, found: {:?}",
        stored.keys().collect::<Vec<_>>()
    );
    assert_eq!(
        stored[ok_line].structured_metadata, r#"{"a_b":"v"}"#,
        "an admissible dotted name is canonicalized, not rejected"
    );
    assert_eq!(
        stored[naive_line].structured_metadata, r#"{"na_ve":"v"}"#,
        "an admissible non-ASCII name is canonicalized per character"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_valued_stream_labels_are_never_stored_and_merge_the_stream() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_163;
    let db = &pulsus_testkit::test_db("pulsus_loki_push_empty_labels_it");
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[("PULSUS_COMPAT_ENDPOINTS", "1")]);

    let base_ns = now_ns();
    let service = "labels-empty";

    // Two pushes differing ONLY by an empty-valued stream label. On the
    // reference these are one stream (`syntax.ParseLabels` -> `WithoutEmpty`,
    // measured on grafana/loki:3.7.4 AND :3.4.2), so they must share one
    // fingerprint and one `log_streams` label set here.
    let with_empty_line = "stream carrying an empty-valued label";
    let body = format!(
        r#"{{"streams":[{{"stream":{{"service_name":"{service}","env":"prod","region":""}},"values":[["{base_ns}","{with_empty_line}"]]}}]}}"#
    );
    let res = push(port, "application/json", body.as_bytes());
    assert_eq!(res.status, 204, "empty-label push (body {})", res.body);

    let without_line = "stream without the label at all";
    let ts = base_ns + 1;
    let body = format!(
        r#"{{"streams":[{{"stream":{{"service_name":"{service}","env":"prod"}},"values":[["{ts}","{without_line}"]]}}]}}"#
    );
    let res = push(port, "application/json", body.as_bytes());
    assert_eq!(res.status, 204, "no-label push (body {})", res.body);

    // The protobuf label-set literal takes the same route.
    let proto_line = "stream carrying an empty-valued label over protobuf";
    let ts = base_ns + 2;
    let req = PushRequest {
        streams: vec![StreamAdapter {
            labels: format!(r#"{{service_name="{service}", env="prod", region=""}}"#),
            entries: vec![EntryAdapter {
                timestamp: Some(Timestamp {
                    seconds: ts / 1_000_000_000,
                    nanos: (ts % 1_000_000_000) as i32,
                }),
                line: proto_line.to_string(),
                structured_metadata: Vec::new(),
            }],
        }],
    };
    let encoded = snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy compress");
    let res = push(port, "application/x-protobuf", &encoded);
    assert_eq!(res.status, 204, "protobuf empty-label push ({})", res.body);

    // A whitespace-only value is a DIFFERENT stream — only exactly-empty is
    // dropped.
    let ws_line = "stream carrying a whitespace-valued label";
    let ts = base_ns + 3;
    let body = format!(
        r#"{{"streams":[{{"stream":{{"service_name":"{service}","env":"prod","region":" "}},"values":[["{ts}","{ws_line}"]]}}]}}"#
    );
    let res = push(port, "application/json", body.as_bytes());
    assert_eq!(res.status, 204, "whitespace-label push (body {})", res.body);

    // A duplicate label name with one empty occurrence: the stream-label strip
    // is PAIR-WISE, so the non-empty twin survives and the stored stream is
    // `region="eu"`. (The structured-metadata strip is by-name and would leave
    // no `region` — see `empty_valued_structured_metadata_is_never_stored`.)
    // Only the protobuf literal can carry a duplicate name this far; measured
    // on `grafana/loki:3.7.4`, both orders store `region="eu"`.
    let dup_line = "stream carrying a duplicated label name, one empty";
    let ts = base_ns + 4;
    let req = PushRequest {
        streams: vec![StreamAdapter {
            labels: format!(r#"{{service_name="{service}", env="prod", region="", region="eu"}}"#),
            entries: vec![EntryAdapter {
                timestamp: Some(Timestamp {
                    seconds: ts / 1_000_000_000,
                    nanos: (ts % 1_000_000_000) as i32,
                }),
                line: dup_line.to_string(),
                structured_metadata: Vec::new(),
            }],
        }],
    };
    let encoded = snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy compress");
    let res = push(port, "application/x-protobuf", &encoded);
    assert_eq!(res.status, 204, "duplicate-label push ({})", res.body);

    let client = ch_client(db).await;
    let stored = stored_samples_by_body(&client, db, service, 5).await;

    assert_eq!(
        stored[with_empty_line].fingerprint, stored[without_line].fingerprint,
        "an empty-valued label must not perturb the stream fingerprint"
    );
    assert_eq!(
        stored[proto_line].fingerprint, stored[without_line].fingerprint,
        "the protobuf label-set literal takes the same route"
    );
    assert_ne!(
        stored[ws_line].fingerprint, stored[without_line].fingerprint,
        "a whitespace-only value is kept, so it IS a distinct stream"
    );

    // The stored `log_streams` rows: three label sets, none carrying
    // `region=""`.
    let streams = stored_streams(&client, db, service).await;
    let mut labels: Vec<String> = streams.iter().map(|s| s.labels.clone()).collect();
    labels.sort();
    labels.dedup();
    assert_eq!(
        labels,
        vec![
            r#"{"env":"prod","region":" ","service_name":"labels-empty"}"#.to_string(),
            r#"{"env":"prod","region":"eu","service_name":"labels-empty"}"#.to_string(),
            r#"{"env":"prod","service_name":"labels-empty"}"#.to_string(),
        ],
        "stored label sets: the empty-valued one merged away, the whitespace one \
         stayed, the duplicated name kept its non-empty twin"
    );

    // Nothing this service stored carries an empty-valued label. Rendered by
    // `to_canonical_json` as `"k":"v"` with no spacing, so the token `:""`
    // appears iff some value is empty — a value that merely CONTAINS `:""` is
    // escaped to `:\"\"` and does not match.
    for row in &streams {
        assert!(
            !row.labels.contains(":\"\""),
            "stored stream still carries an empty-valued label: {}",
            row.labels
        );
    }
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
    let db = &pulsus_testkit::test_db("pulsus_loki_push_tail_it");
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

    // Two robustness knobs, both the real production levers — this test
    // exercises the LIVE ingest path (push → LogSink → flush → CH), unlike
    // `logs_tail_live.rs`, which seeds rows straight into ClickHouse (so
    // they are visible the instant they are written and it needs neither
    // knob).
    //
    // 1. `start` — bound the tail to a recent window (mirroring every
    //    sibling live-tail test). Without an explicit `start` the tail
    //    defaults to one hour ago and must walk ~60 catch-up slices (three
    //    ClickHouse round-trips each) before it reaches "now"; on a loaded
    //    CI runner that backlog walk alone can exceed the 20s deadline. A
    //    60s-ago start caps catch-up at a single slice.
    //
    // 2. `delay_for` — hold the tail horizon behind wall-clock (docs/api.md
    //    §2.4), the production answer to ingest visibility latency. The
    //    tail's forward watermark advances with wall-clock and never
    //    re-scans a passed instant; a line pushed at `ts` only becomes
    //    queryable once its batch has flushed to ClickHouse (a window that
    //    widens under load). With `delay_for=0` the watermark can sweep past
    //    `ts` while that flush is still in flight, stranding the row below
    //    the cursor forever (a bimodal "delivered in ~2s or never" race). A
    //    5s delay (the adjudicated ceiling) keeps the horizon behind `ts`
    //    until the flush is certainly visible; the 20s deadline below
    //    comfortably absorbs it. Real tailing clients set `delay_for` for
    //    exactly this reason.
    let query = urlencode(&format!(r#"{{service_name="{service}"}}"#));
    let start = now_ns() - 60_000_000_000;
    let mut ws = WsClient::connect(
        port,
        &format!("/api/logs/v1/tail?query={query}&start={start}&delay_for=5"),
    );

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

    // AC-9: a second entry WITH structured metadata — the tail frame must
    // carry the SM fanned into its stream labels, just like query_range.
    let sm_line = "tailed loki push line with sm";
    let res = push(
        port,
        "application/x-protobuf",
        &protobuf_body_with_sm(service, now_ns(), sm_line, &[("trace_id", "tail-abc")]),
    );
    assert_eq!(res.status, 204, "sm push -> 204 (body {})", res.body);

    // Each pushed line arrives on the tail stream carrying its COMPLETE label
    // set — captured here per line for an exact assertion.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut base_labels: Option<std::collections::BTreeMap<String, String>> = None;
    let mut sm_labels: Option<std::collections::BTreeMap<String, String>> = None;
    while Instant::now() < deadline && (base_labels.is_none() || sm_labels.is_none()) {
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
                    base_labels = Some(labels.clone());
                }
                if svc == service && value[1].as_str() == Some(sm_line) {
                    sm_labels = Some(labels.clone());
                }
            }
        }
    }
    ws.close();
    let labels = base_labels.expect("the #77-pushed line must arrive on /api/logs/v1/tail");
    assert_eq!(
        labels,
        expected_pushed_labels(service),
        "the tailed frame's pushed stream must carry its full label set (service_name AND env)"
    );
    let sm = sm_labels.expect("the SM-bearing pushed line must arrive on /api/logs/v1/tail");
    let mut expected_sm = expected_pushed_labels(service);
    expected_sm.insert("trace_id".to_string(), "tail-abc".to_string());
    assert_eq!(
        sm, expected_sm,
        "the tailed SM-bearing frame must fan structured metadata into its stream labels (AC-9)"
    );
}

// ---------------------------------------------------------------------
// Issue #374: the four per-stream label bounds, at the wire, end to end.
//
// Reference: `pkg/distributor/validator.go:157-199 @ v3.7.4`, reached from
// `pkg/distributor/distributor.go:1380 @ v3.7.4`. Statuses and message text
// were captured side by side against the pinned `grafana/loki:3.7.4`
// container (revision `b318f282`) — 12 JSON cases and 6 protobuf cases, all
// agreeing on status. The text agrees outright since issue #379: PulsusDB
// synthesizes the same `service_name` label before validating, so the label
// set rendered into each message is the reference's own.
//
// The response status alone is not the claim being proven here. Before this
// change the same push answered `204` AND stored the row, so the test reads
// `log_streams`/`log_samples` back out of ClickHouse to show the reject left
// nothing behind.
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn over_wide_label_value_is_rejected_and_stores_nothing() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_155;
    let db = &pulsus_testkit::test_db("pulsus_loki_push_label_bounds_it");
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[("PULSUS_COMPAT_ENDPOINTS", "1")]);

    let base_ns = now_ns();
    let service = "checkout-374";
    let over = "b".repeat(2049);

    // A control push FIRST: the same shape with an in-bounds value must be
    // accepted and stored, so a later zero row count cannot be explained by
    // the writer being broken or the query being wrong.
    let control_line = "label bounds control line";
    let res = push(
        port,
        "application/json",
        format!(
            r#"{{"streams":[{{"stream":{{"service_name":"{service}","app":"{}"}},"values":[["{base_ns}","{control_line}"]]}}]}}"#,
            "b".repeat(2048)
        )
        .as_bytes(),
    );
    assert_eq!(
        res.status, 204,
        "at-bound value accepted (body {})",
        res.body
    );

    // 1 byte more is a 400 carrying the reference's message verbatim.
    let rejected_line = "label bounds rejected line";
    let res = push(
        port,
        "application/json",
        format!(
            r#"{{"streams":[{{"stream":{{"service_name":"{service}-rejected","app":"{over}"}},"values":[["{base_ns}","{rejected_line}"]]}}]}}"#
        )
        .as_bytes(),
    );
    assert_eq!(res.status, 400, "over-bound value rejected");
    assert_eq!(
        res.body,
        format!(
            "stream '{{app=\"{over}\", service_name=\"{service}-rejected\"}}' \
             has label value too long: '{over}'\n"
        ),
        "the 400 body is the reference's message"
    );

    // The same bound on the protobuf transport, plus the duplicate-name
    // bound — reachable only there, here and upstream.
    let res = push(port, "application/x-protobuf", &{
        let req = PushRequest {
            streams: vec![StreamAdapter {
                labels: format!(r#"{{service_name="{service}-pb", app="{over}"}}"#),
                entries: vec![EntryAdapter {
                    timestamp: Some(Timestamp {
                        seconds: base_ns / 1_000_000_000,
                        nanos: (base_ns % 1_000_000_000) as i32,
                    }),
                    line: "pb rejected line".to_string(),
                    structured_metadata: Vec::new(),
                }],
            }],
        };
        snap::raw::Encoder::new()
            .compress_vec(&req.encode_to_vec())
            .expect("snappy compress")
    });
    assert_eq!(res.status, 400, "over-bound value rejected on protobuf too");
    assert!(
        res.body
            .ends_with(&format!("has label value too long: '{over}'\n")),
        "protobuf 400 body: {}",
        res.body
    );

    // The control line must land, so the read-back below is discriminating.
    let control = wait_for_line(port, service, base_ns, control_line);
    assert!(
        control.contains(&control_line.to_string()),
        "the at-bound control line must be queryable: {control:?}"
    );

    // Nothing from the rejected pushes reached storage: no sample, and no
    // stream registration either (a stream row is written before the first
    // sample, so checking only `log_samples` would miss a half-applied push).
    assert_eq!(
        ch_count(
            db,
            "SELECT count() AS c FROM log_samples \
             WHERE body IN ('label bounds rejected line', 'pb rejected line')",
        )
        .await,
        0,
        "a rejected push must store no sample"
    );
    assert_eq!(
        ch_count(
            db,
            &format!(
                "SELECT count() AS c FROM log_streams \
                 WHERE service IN ('{service}-rejected', '{service}-pb')"
            ),
        )
        .await,
        0,
        "a rejected push must register no stream"
    );
    assert_eq!(
        ch_count(
            db,
            &format!("SELECT count() AS c FROM log_streams WHERE service = '{service}'"),
        )
        .await,
        1,
        "the accepted control push must still have registered its stream"
    );
}

/// Issue #374 review: the reference writes the good streams of a mixed batch
/// and answers `400` afterwards (`pkg/distributor/distributor.go:645-655,
/// 780-790, 929 @ v3.7.4`). Proven at the highest tier — the good line is read
/// back out of ClickHouse while the response was a `400` — because a client
/// that loses its good data on one malformed stream loses it permanently: a
/// `400` is not retried.
#[tokio::test(flavor = "multi_thread")]
async fn a_mixed_batch_stores_the_good_streams_and_still_answers_400() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_156;
    let db = &pulsus_testkit::test_db("pulsus_loki_push_mixed_batch_it");
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[("PULSUS_COMPAT_ENDPOINTS", "1")]);

    let base_ns = now_ns();
    let service = "checkout-mixed";
    let over = "b".repeat(2049);
    let good_line = "mixed batch good line";
    let bad_line = "mixed batch bad line";

    let res = push(
        port,
        "application/json",
        format!(
            r#"{{"streams":[
                {{"stream":{{"service_name":"{service}"}},"values":[["{base_ns}","{good_line}"]]}},
                {{"stream":{{"service_name":"{service}-bad","app":"{over}"}},"values":[["{base_ns}","{bad_line}"]]}}
            ]}}"#
        )
        .as_bytes(),
    );
    assert_eq!(res.status, 400, "a mixed batch still answers 400");
    assert!(
        res.body
            .ends_with(&format!("has label value too long: '{over}'\n")),
        "the 400 body is the reference's message for the one bad stream: {}",
        res.body
    );

    // The good stream was written anyway, and is queryable.
    let lines = wait_for_line(port, service, base_ns, good_line);
    assert!(lines.contains(&good_line.to_string()), "{lines:?}");
    assert_eq!(
        ch_count(
            db,
            &format!("SELECT count() AS c FROM log_samples WHERE body = '{good_line}'"),
        )
        .await,
        1,
        "the good stream of a mixed batch must be stored"
    );
    assert_eq!(
        ch_count(
            db,
            &format!("SELECT count() AS c FROM log_samples WHERE body = '{bad_line}'"),
        )
        .await,
        0,
        "the bad stream of a mixed batch must not be"
    );
    assert_eq!(
        ch_count(
            db,
            &format!("SELECT count() AS c FROM log_streams WHERE service = '{service}-bad'"),
        )
        .await,
        0,
        "the bad stream must not be registered"
    );
}

/// Issue #374 review: `WithoutEmpty` (`pkg/logql/syntax/parser.go:296 @
/// v3.7.4`) runs before `labels.StableHash`, so an empty-valued label must not
/// reach the stored label set. Both pushes must land in ONE stream row with
/// ONE label set, not two — a validator-only filter would give two.
#[tokio::test(flavor = "multi_thread")]
async fn an_empty_valued_label_does_not_split_the_stream() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_157;
    let db = &pulsus_testkit::test_db("pulsus_loki_push_empty_label_it");
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[("PULSUS_COMPAT_ENDPOINTS", "1")]);

    let base_ns = now_ns();
    let service = "checkout-empty";
    let plain_line = "empty label plain";
    let padded_line = "empty label padded";

    for (line, labels) in [
        (plain_line, format!(r#"{{"service_name":"{service}"}}"#)),
        (
            padded_line,
            format!(r#"{{"service_name":"{service}","ignored":""}}"#),
        ),
    ] {
        let res = push(
            port,
            "application/json",
            format!(r#"{{"streams":[{{"stream":{labels},"values":[["{base_ns}","{line}"]]}}]}}"#)
                .as_bytes(),
        );
        assert_eq!(res.status, 204, "{line}: {}", res.body);
    }

    wait_for_line(port, service, base_ns, plain_line);
    wait_for_line(port, service, base_ns, padded_line);

    // One stream row, one fingerprint: the empty-valued label never reached
    // the identity.
    assert_eq!(
        ch_count(
            db,
            &format!(
                "SELECT count(DISTINCT fingerprint) AS c FROM log_streams \
                 WHERE service = '{service}'"
            ),
        )
        .await,
        1,
        "an empty-valued label must not split the stream"
    );
    // ...and it is not stored as a label either.
    assert_eq!(
        ch_count(
            db,
            &format!(
                "SELECT count() AS c FROM log_streams \
                 WHERE service = '{service}' AND position(labels, 'ignored') > 0"
            ),
        )
        .await,
        0,
        "an empty-valued label must not be stored"
    );
    // Both lines belong to that one stream.
    let both = query_streams(port, "/api/logs/v1", service, base_ns);
    assert_eq!(both.len(), 1, "both pushes belong to one stream: {both:?}");
    assert_eq!(
        both[0].0,
        [("service_name".to_string(), service.to_string())]
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>()
    );
}

// ---------------------------------------------------------------------
// The OTLP logs receiver: what the bounds do NOT cover, at the storage
// tier (issue #374 round-3 review, adjudicated to #109).
// ---------------------------------------------------------------------

/// An OTLP/JSON logs body: one `ResourceLogs` per entry, its resource
/// attributes in wire order (so a repeated or colliding key is preserved as
/// sent), each carrying one record.
fn otlp_json_body(resources: &[(Vec<(&str, String)>, &str)], ts_ns: i64) -> Vec<u8> {
    let resource_logs: Vec<serde_json::Value> = resources
        .iter()
        .map(|(attrs, line)| {
            let attributes: Vec<serde_json::Value> = attrs
                .iter()
                .map(|(k, v)| serde_json::json!({"key": k, "value": {"stringValue": v}}))
                .collect();
            serde_json::json!({
                "resource": {"attributes": attributes},
                "scopeLogs": [{"logRecords": [{
                    "timeUnixNano": ts_ns.to_string(),
                    "body": {"stringValue": line},
                }]}],
            })
        })
        .collect();
    serde_json::to_vec(&serde_json::json!({ "resourceLogs": resource_logs })).expect("otlp body")
}

fn otlp_push(port: u16, body: &[u8]) -> HttpResponse {
    http_request(port, "POST", "/v1/logs", Some("application/json"), body)
        .expect("otlp push reachable")
}

/// Polls `sql` until it returns `want` or the deadline passes — the OTLP
/// receiver's rows reach ClickHouse through the same asynchronous writer the
/// push path uses.
async fn wait_for_count(db: &str, sql: &str, want: u64) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let got = ch_count(db, sql).await;
        if got == want || Instant::now() >= deadline {
            return got;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Issue #374: the bounds this branch introduces do not cover every label
/// they store, and issue #379 closed that hole for exactly one name. This is
/// the case that shows both at the **storage tier**, which is the only tier
/// that can see either.
///
/// `{k8s.pod.name: "ok", k8s_pod_name: <2049 B>}` is accepted by the pinned
/// oracle (`204`) and by PulsusDB (`200`) — upstream matches the raw dotted
/// spelling only (`otlp.go:193`, `otlp_config.go:88-99 @ v3.7.4`) and routes
/// the underscored one to structured metadata, which no bound reaches.
/// Storage does not agree: we index every resource attribute (#109), both
/// spellings canonicalize onto `k8s_pod_name`, and `from_normalized`'s frozen
/// rule (#4) keeps the greatest original key — `_` (0x5F) after `.` (0x2E) —
/// so the **unvalidated** value is written under a label the validator passed
/// at two bytes, and the stream's identity follows it.
///
/// The same shape spelled `service.name`/`service_name` no longer behaves that
/// way (issue #379): that slot is resolved from the raw attributes and written
/// last, exactly as the reference's map assignment is, so the validated value
/// wins and the near-miss is not stored at all. Measured on stock
/// `grafana/loki@sha256:87f0a067…` via `/loki/api/v1/series`:
/// `{service.name: "ok379", service_name: <2049 B>}` stores
/// `{service_name="ok379"}`.
///
/// Four rounds of status-only oracle comparison could not see any of this.
/// The hermetic twin is
/// `otlp_logs::tests::an_index_attribute_and_its_near_miss_collide_on_the_unvalidated_value`;
/// this one reads the labels and the fingerprints back out of ClickHouse.
#[tokio::test(flavor = "multi_thread")]
async fn an_otlp_near_miss_spelling_stores_an_over_wide_indexed_label() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_158;
    let db = &pulsus_testkit::test_db("pulsus_loki_push_otlp_near_miss_it");
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[("PULSUS_COMPAT_ENDPOINTS", "1")]);

    let base_ns = now_ns();
    let wide = "x".repeat(2049);
    let probe = "otlp-near-miss-374";

    // All five resources carry the same marker attribute, so one query sees
    // exactly these streams and the fingerprints are directly comparable.
    // `container.name` is the marker rather than `k8s.pod.name`, because the
    // near-miss half of this test now needs `k8s_pod_name` free — and it is a
    // discovery name, so it also fixes `service_name` for every resource here
    // EXCEPT the two that carry `service.name` themselves.
    let marker = ("container.name", probe.to_string());
    let body = otlp_json_body(
        &[
            // (a) the validated index name AND its unvalidated near-miss, on
            // a name the `service_name` slot does not govern.
            (
                vec![
                    marker.clone(),
                    ("k8s.pod.name", "ok".to_string()),
                    ("k8s_pod_name", wide.clone()),
                ],
                "otlp near miss both",
            ),
            // (b) the near-miss alone: the same stored label set as (a).
            (
                vec![marker.clone(), ("k8s_pod_name", wide.clone())],
                "otlp near miss alone",
            ),
            // (c) the control — only the validated index name, so the value
            // the bound was charged on is the one stored.
            (
                vec![marker.clone(), ("k8s.pod.name", "ok".to_string())],
                "otlp near miss control",
            ),
            // (d) the closed case (issue #379): the same shape on
            // `service_name`, where the slot decides instead.
            (
                vec![
                    marker.clone(),
                    ("service.name", "svcok".to_string()),
                    ("service_name", wide.clone()),
                ],
                "otlp service name both",
            ),
            // (e) its control: the validated spelling alone.
            (
                vec![marker.clone(), ("service.name", "svcok".to_string())],
                "otlp service name control",
            ),
        ],
        base_ns,
    );
    let res = otlp_push(port, &body);
    assert_eq!(
        res.status, 200,
        "the oracle accepts this body too (204); body {}",
        res.body
    );

    let of_probe =
        format!("FROM log_streams WHERE JSONExtractString(labels, 'container_name') = '{probe}'");

    // Three streams: (a) and (b) share one fingerprint because the near-miss
    // won the collapse in both; (c) is a second; (d) and (e) share a third,
    // because the SLOT decided `service_name` in both.
    assert_eq!(
        wait_for_count(
            db,
            &format!("SELECT count(DISTINCT fingerprint) AS c {of_probe}"),
            3
        )
        .await,
        3,
        "(a)+(b), (c), and (d)+(e) are three streams"
    );

    // The stored label is the 2049-byte value no bound was charged on...
    assert_eq!(
        ch_count(
            db,
            &format!(
                "SELECT count() AS c {of_probe} \
                 AND JSONExtractString(labels, 'k8s_pod_name') = '{wide}'"
            ),
        )
        .await,
        1,
        "the unvalidated near-miss value must be the stored one"
    );
    // ...and it is (a)'s stream, not merely (b)'s. This is the assertion that
    // distinguishes the rule: were the collapse to keep the VALIDATED
    // `k8s.pod.name` value, (a) would land in the `ok` stream instead and
    // every count above would look exactly the same.
    let wide_stream = format!(
        "fingerprint IN (SELECT fingerprint FROM log_streams \
         WHERE JSONExtractString(labels, 'k8s_pod_name') = '{wide}')"
    );
    assert_eq!(
        ch_count(
            db,
            &format!(
                "SELECT count() AS c FROM log_samples WHERE {wide_stream} \
                 AND body IN ('otlp near miss both', 'otlp near miss alone')"
            ),
        )
        .await,
        2,
        "the record pushed WITH the validated `k8s.pod.name` must be stored \
         under the over-wide label too"
    );
    assert_eq!(
        ch_count(
            db,
            &format!(
                "SELECT count() AS c FROM log_samples WHERE fingerprint IN \
                 (SELECT fingerprint FROM log_streams \
                  WHERE JSONExtractString(labels, 'container_name') = '{probe}' \
                  AND JSONExtractString(labels, 'k8s_pod_name') = 'ok') \
                 AND body != 'otlp near miss control'"
            ),
        )
        .await,
        0,
        "the validated value's stream carries only the control record"
    );
    // ...wider than the bound this branch introduces, as an INDEXED label.
    assert_eq!(
        ch_count(
            db,
            &format!(
                "SELECT count() AS c {of_probe} \
                 AND length(JSONExtractString(labels, 'k8s_pod_name')) > 2048"
            ),
        )
        .await,
        1,
        "exactly one stored stream exceeds the 2048-byte label value bound"
    );
    // The control stores the validated value, so the read-back above is not
    // explained by every stream carrying the wide one.
    assert_eq!(
        ch_count(
            db,
            &format!(
                "SELECT count() AS c {of_probe} \
                 AND JSONExtractString(labels, 'k8s_pod_name') = 'ok'"
            ),
        )
        .await,
        1,
        "the control stream stores the validated value"
    );

    // Issue #379, the inverted half: NO stored stream carries the over-wide
    // value under `service_name`, and (d) landed in (e)'s stream — the
    // validated one — which is the assertion that discriminates the slot rule
    // from `from_normalized`'s.
    assert_eq!(
        ch_count(
            db,
            &format!(
                "SELECT count() AS c {of_probe} \
                 AND length(JSONExtractString(labels, 'service_name')) > 2048"
            ),
        )
        .await,
        0,
        "the `service_name` slot is not decided by the unvalidated near-miss"
    );
    assert_eq!(
        ch_count(
            db,
            &format!(
                "SELECT count() AS c FROM log_samples WHERE fingerprint IN \
                 (SELECT fingerprint FROM log_streams \
                  WHERE JSONExtractString(labels, 'container_name') = '{probe}' \
                  AND JSONExtractString(labels, 'service_name') = 'svcok') \
                 AND body IN ('otlp service name both', 'otlp service name control')"
            ),
        )
        .await,
        2,
        "(d) and (e) are one stream, under the VALIDATED `service.name` value"
    );
    // And all five lines landed: nothing here was refused.
    assert_eq!(
        wait_for_count(
            db,
            "SELECT count() AS c FROM log_samples WHERE body IN \
             ('otlp near miss both', 'otlp near miss alone', 'otlp near miss control', \
              'otlp service name both', 'otlp service name control')",
            5,
        )
        .await,
        5,
        "all five records are accepted and stored"
    );
}

/// Issue #379 AC6: the discovered `service_name` reaches the PHYSICAL
/// `service` column on both receivers, on both push encodings, and the label
/// it was discovered from is queryable by LogQL.
///
/// This is the assertion behind the read-path claim. `log_samples` is
/// `ORDER BY (service, fingerprint, timestamp_ns)`
/// (`crates/pulsus-schema/src/catalog.rs`), and every logs read renders
/// `PREWHERE service = …` from the values hydrated in stage 2
/// (`crates/pulsus-read/src/logql/sql.rs`). Before discovery, a push carrying
/// no explicit `service_name` — and an OTLP resource carrying no
/// `service.name` — stored `service = ''`, so the leading primary-key column
/// was a constant and pruned nothing. Without this test, "the PREWHERE now
/// prunes" would be an unbacked sentence.
///
/// Measured on stock `grafana/loki@sha256:87f0a067…` via
/// `/loki/api/v1/series`: `{app="aa379", name="nn379"}` stores
/// `{app=…, name=…, service_name="aa379"}`, and an OTLP resource carrying
/// `container.name` stores `service_name` equal to it.
#[tokio::test(flavor = "multi_thread")]
async fn the_discovered_service_name_reaches_the_service_column_on_both_receivers() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_160;
    let db = &pulsus_testkit::test_db("pulsus_loki_push_service_discovery_it");
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[("PULSUS_COMPAT_ENDPOINTS", "1")]);

    let base_ns = now_ns();
    let discovered = "checkout-379";
    let otlp_discovered = "otlp-379";

    // JSON push: `app` is a discovery name, `name` is a later one, so the
    // stored `service_name` is `app`'s value — list order, not wire order.
    let json_line = "discovery json line";
    let res = push(
        port,
        "application/json",
        format!(
            r#"{{"streams":[{{"stream":{{"name":"nn-379","app":"{discovered}"}},"values":[["{base_ns}","{json_line}"]]}}]}}"#
        )
        .as_bytes(),
    );
    assert_eq!(res.status, 204, "json push (body: {})", res.body);

    // Protobuf push: the same stream shape through the other encoding, with
    // the two labels in the reverse wire order — the discovered value is the
    // same, because the DISCOVERY LIST decides on this transport, not the
    // wire.
    let pb_line = "discovery protobuf line";
    let pb_ns = base_ns + 1;
    let req = PushRequest {
        streams: vec![StreamAdapter {
            labels: format!(r#"{{app="{discovered}", name="nn-379"}}"#),
            entries: vec![EntryAdapter {
                timestamp: Some(Timestamp {
                    seconds: pb_ns / 1_000_000_000,
                    nanos: (pb_ns % 1_000_000_000) as i32,
                }),
                line: pb_line.to_string(),
                structured_metadata: Vec::new(),
            }],
        }],
    };
    let res = push(
        port,
        "application/x-protobuf",
        &snap::raw::Encoder::new()
            .compress_vec(&req.encode_to_vec())
            .expect("snappy compress"),
    );
    assert_eq!(res.status, 204, "protobuf push (body: {})", res.body);

    // OTLP: `container.name` is both an index attribute and a discovery name.
    let otlp_line = "discovery otlp line";
    let res = otlp_push(
        port,
        &otlp_json_body(
            &[(
                vec![("container.name", otlp_discovered.to_string())],
                otlp_line,
            )],
            base_ns + 2,
        ),
    );
    assert_eq!(res.status, 200, "otlp push (body: {})", res.body);

    // Both push encodings land in ONE stream (same labels, same identity),
    // and its physical `service` column carries the discovered value.
    assert_eq!(
        wait_for_count(
            db,
            &format!(
                "SELECT count() AS c FROM log_samples WHERE service = '{discovered}' \
                 AND body IN ('{json_line}', '{pb_line}')"
            ),
            2,
        )
        .await,
        2,
        "log_samples.service must carry the discovered value on both encodings"
    );
    assert_eq!(
        ch_count(
            db,
            &format!(
                "SELECT count(DISTINCT fingerprint) AS c FROM log_streams \
                 WHERE service = '{discovered}' \
                 AND JSONExtractString(labels, 'service_name') = '{discovered}' \
                 AND JSONExtractString(labels, 'app') = '{discovered}' \
                 AND JSONExtractString(labels, 'name') = 'nn-379'"
            ),
        )
        .await,
        1,
        "log_streams.service and the stored labels JSON both carry it, in one stream"
    );

    // The OTLP resource likewise, through its own (different) algorithm.
    assert_eq!(
        wait_for_count(
            db,
            &format!(
                "SELECT count() AS c FROM log_samples WHERE service = '{otlp_discovered}' \
                 AND body = '{otlp_line}'"
            ),
            1,
        )
        .await,
        1,
        "log_samples.service must carry the OTLP-discovered value"
    );
    assert_eq!(
        ch_count(
            db,
            &format!(
                "SELECT count() AS c FROM log_streams WHERE service = '{otlp_discovered}' \
                 AND JSONExtractString(labels, 'service_name') = '{otlp_discovered}' \
                 AND JSONExtractString(labels, 'container_name') = '{otlp_discovered}'"
            ),
        )
        .await,
        1,
        "log_streams.service and the stored labels JSON both carry it"
    );

    // And the synthesized label is a real selector: a LogQL query the client
    // never had labels for now returns the pushed lines.
    let lines = wait_for_line(port, discovered, base_ns, json_line);
    assert!(
        lines.contains(&pb_line.to_string()),
        "{{service_name=\"{discovered}\"}} must return both encodings' lines: {lines:?}"
    );
    let labels = labels_of_stream_carrying(port, "/api/logs/v1", discovered, base_ns, json_line);
    assert_eq!(
        labels,
        [
            ("app", discovered),
            ("name", "nn-379"),
            ("service_name", discovered)
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<std::collections::BTreeMap<String, String>>(),
        "the queried stream carries the discovered label alongside the pushed ones"
    );
}

/// Issue #374 review round 5: a push carrying **no streams at all** is `422`,
/// not an empty success. Upstream refuses it before any validation —
/// `PushWithResolver` returns `httpgrpc.Errorf(StatusUnprocessableEntity,
/// validation.MissingStreamsErrorMsg)` for `len(req.Streams) == 0`
/// (`pkg/distributor/distributor.go:579-581 @ v3.7.4`), and its OTLP
/// translation makes a record-less payload exactly that empty request
/// (`ld.LogRecordCount() == 0`, `pkg/loghttp/push/otlp.go:144-146`). Measured
/// on `grafana/loki@sha256:87f0a067…`: `422` on both receivers.
///
/// Run live rather than only at the wire because the status is half the
/// claim: the *neighbouring* shape — a stream that carries labels but no
/// entries — is `204` upstream and must stay accepted here, and neither shape
/// may leave a row behind. A control push proves the read-back would have
/// seen one.
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_less_push_is_422_on_both_receivers_and_stores_nothing() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_159;
    let db = &pulsus_testkit::test_db("pulsus_loki_push_stream_less_it");
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[("PULSUS_COMPAT_ENDPOINTS", "1")]);

    let base_ns = now_ns();
    let service = "stream-less-374";
    const MISSING: &str = "error at least one valid stream is required for ingestion";
    // The Loki push endpoint LF-terminates every error body it writes
    // (`push.HTTPError` -> `http.Error` -> `fmt.Fprintln`,
    // `pkg/loghttp/push/push.go:606-608 @ v3.7.4`); measured on
    // `grafana/loki:3.7.4`, this 422's last byte is `0x0a`. The OTLP twin
    // below carries the same text inside a `google.rpc.Status` and has no
    // terminator at all.
    let missing_lf = format!("{MISSING}\n");

    // Loki push, both JSON spellings of "no streams".
    for body in [r#"{"streams":[]}"#, "{}"] {
        let res = push(port, "application/json", body.as_bytes());
        assert_eq!(res.status, 422, "{body} -> 422 (body {})", res.body);
        assert_eq!(
            res.body, missing_lf,
            "the 422 body is the reference's message, terminator included"
        );
    }
    // ...and the protobuf transport, where "no streams" is an empty message.
    let res = push(
        port,
        "application/x-protobuf",
        &snap::raw::Encoder::new()
            .compress_vec(&PushRequest { streams: vec![] }.encode_to_vec())
            .expect("snappy compress"),
    );
    assert_eq!(res.status, 422, "empty protobuf push (body {})", res.body);
    assert_eq!(res.body, missing_lf);

    // OTLP: a resource with attributes but an empty `logRecords` — the shape
    // the review measured — plus the empty request.
    for body in [
        serde_json::to_vec(&serde_json::json!({"resourceLogs": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": service}}
            ]},
            "scopeLogs": [{"logRecords": []}],
        }]}))
        .expect("otlp body"),
        serde_json::to_vec(&serde_json::json!({"resourceLogs": []})).expect("otlp body"),
    ] {
        let res = otlp_push(port, &body);
        assert_eq!(res.status, 422, "record-less OTLP (body {})", res.body);
        // The OTLP error body is a `google.rpc.Status`: field 2 is the
        // message, and the reference's text is carried verbatim.
        assert!(
            res.body.contains(MISSING),
            "the 422 status message is the reference's: {}",
            res.body
        );
    }

    // The neighbour that must NOT be refused: a stream with labels and no
    // entries is still a stream (`distributor.go:639-641` only skips it when
    // validating), measured `204` upstream.
    let res = push(
        port,
        "application/json",
        format!(r#"{{"streams":[{{"stream":{{"service_name":"{service}-idle"}},"values":[]}}]}}"#)
            .as_bytes(),
    );
    assert_eq!(
        res.status, 204,
        "entry-less stream accepted (body {})",
        res.body
    );

    // A control push, so the zero counts below are not a broken writer.
    let control_line = "stream-less control line";
    let res = push(
        port,
        "application/json",
        format!(
            r#"{{"streams":[{{"stream":{{"service_name":"{service}"}},"values":[["{base_ns}","{control_line}"]]}}]}}"#
        )
        .as_bytes(),
    );
    assert_eq!(res.status, 204, "control push accepted (body {})", res.body);
    let control = wait_for_line(port, service, base_ns, control_line);
    assert!(
        control.contains(&control_line.to_string()),
        "the control line must be queryable: {control:?}"
    );

    // Neither the refused pushes nor the entry-less stream registered
    // anything; the control did.
    assert_eq!(
        ch_count(
            db,
            &format!(
                "SELECT count() AS c FROM log_streams \
                 WHERE service IN ('{service}-idle', '{service}')"
            ),
        )
        .await,
        1,
        "only the control push registers a stream"
    );
}

/// Issue #374 review round 9: the envelope's `streams` key is matched with
/// ASCII case folding, and a repeat of it is last-wins.
///
/// `loghttp.PushRequest` is a one-field struct decoded by jsoniter reflection
/// (`pkg/loghttp/query.go:91-93 @ v3.7.4`); `jsoniter.NewDecoder` runs
/// `ConfigDefault`, whose `CaseSensitive` is false, so the wire key is folded
/// over `'A'..='Z'` before it is matched, and the field decoder re-runs on
/// every match while the slice decoder re-grows from zero
/// (`iter_object.go:85-87`, `reflect_struct_decoder.go:36-41,574-590`,
/// `reflect_slice.go:66-99 @ jsoniter v1.1.12`).
///
/// Live rather than hermetic because status agreement is exactly what missed
/// this: before #374 both sides answered `204` here and we silently dropped
/// the lines, so only a read-back can tell "accepted" from "accepted and
/// stored". Measured on `grafana/loki@sha256:87f0a067…`: `204` for every
/// spelling with the line queryable afterwards.
#[tokio::test(flavor = "multi_thread")]
async fn a_case_variant_streams_key_is_accepted_and_its_lines_are_stored() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_178;
    let db = &pulsus_testkit::test_db("pulsus_loki_push_streams_case_it");
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[("PULSUS_COMPAT_ENDPOINTS", "1")]);

    let base_ns = now_ns();
    // LF-terminated, like every error body this endpoint writes -- see the
    // stream-less test above.
    const MISSING: &str = "error at least one valid stream is required for ingestion\n";
    let one = |spelling: &str, service: &str, line: &str| {
        format!(
            r#"{{"{spelling}":[{{"stream":{{"service_name":"{service}"}},"values":[["{base_ns}","{line}"]]}}]}}"#
        )
    };

    // Every spelling is accepted AND stores its line.
    for spelling in ["streams", "Streams", "STREAMS", "StReAmS", "streamS"] {
        let service = format!("case-374-{spelling}");
        let line = format!("case variant {spelling}");
        let res = push(
            port,
            "application/json",
            one(spelling, &service, &line).as_bytes(),
        );
        assert_eq!(res.status, 204, "{spelling} -> 204 (body {})", res.body);
        let lines = wait_for_line(port, &service, base_ns, &line);
        assert!(
            lines.contains(&line),
            "{spelling}: the line must be queryable, got {lines:?}"
        );
    }
    assert_eq!(
        wait_for_count(
            db,
            "SELECT count() AS c FROM log_streams WHERE service LIKE 'case-374-%'",
            5,
        )
        .await,
        5,
        "all five spellings register their stream"
    );

    // A repeat is last-wins, across spellings: only the second line is stored.
    let dup = format!(
        "{{{},{}}}",
        one("streams", "case-374-dup", "dup first")
            .trim_start_matches('{')
            .trim_end_matches('}'),
        one("Streams", "case-374-dup", "dup second")
            .trim_start_matches('{')
            .trim_end_matches('}'),
    );
    let res = push(port, "application/json", dup.as_bytes());
    assert_eq!(res.status, 204, "repeated key -> 204 (body {})", res.body);
    let lines = wait_for_line(port, "case-374-dup", base_ns, "dup second");
    assert!(
        lines.contains(&"dup second".to_string()) && !lines.contains(&"dup first".to_string()),
        "only the last occurrence is stored, got {lines:?}"
    );

    // ...and a trailing empty occurrence discards the populated earlier one,
    // which is how the stream-less `422` is reached from a body that does
    // carry a stream.
    let discard = format!(
        r#"{{{},"streams":[]}}"#,
        one("Streams", "case-374-discarded", "discarded line")
            .trim_start_matches('{')
            .trim_end_matches('}'),
    );
    let res = push(port, "application/json", discard.as_bytes());
    assert_eq!(res.status, 422, "trailing empty (body {})", res.body);
    assert_eq!(res.body, MISSING);

    // The fold does not skip the per-stream label bounds: an over-wide value
    // under `Streams` is the same `400` it is under `streams`.
    let wide = format!(
        r#"{{"Streams":[{{"stream":{{"service_name":"case-374-wide","app":"{}"}},"values":[["{base_ns}","wide line"]]}}]}}"#,
        "b".repeat(2049)
    );
    let res = push(port, "application/json", wide.as_bytes());
    assert_eq!(
        res.status, 400,
        "over-wide under Streams (body {})",
        res.body
    );
    assert!(
        res.body.contains("label value too long"),
        "the bound's message, not the envelope's: {}",
        res.body
    );

    // Nothing that was refused left a row behind.
    assert_eq!(
        ch_count(
            db,
            "SELECT count() AS c FROM log_streams \
             WHERE service IN ('case-374-discarded', 'case-374-wide')",
        )
        .await,
        0,
        "the refused pushes register nothing"
    );
    assert_eq!(
        ch_count(
            db,
            "SELECT count() AS c FROM log_samples WHERE body = 'dup first'",
        )
        .await,
        0,
        "the superseded occurrence stores nothing"
    );
}

/// Issue #374 review round 11: last-wins resolves BEFORE any of our structural
/// caps is charged, so a superseded occurrence that breaks one cannot refuse
/// the request.
///
/// The reference never inspects a discarded value — its one-field envelope
/// decoder re-runs the field decoder per occurrence
/// (`reflect_struct_decoder.go:574-590 @ jsoniter v1.1.12`) and a stream
/// object's hand-written switch re-runs per key (`pkg/loghttp/query.go:99-121 @
/// v3.7.4`) — so `204` with the LAST occurrence's line stored is the whole
/// answer. Measured on `grafana/loki@sha256:87f0a067…` for all three shapes
/// below; each was `400` here before this change.
///
/// Live, and read back, because this is the same trap the case-folding bug fell
/// into: a status assertion alone passes on "accepted and silently dropped".
/// Each shape therefore asserts the final line queryable, the superseded
/// stream's own labels absent, and — the discriminating half — the SAME value
/// placed last still refused.
#[tokio::test(flavor = "multi_thread")]
async fn a_superseded_over_cap_value_is_accepted_and_the_final_one_is_stored() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_161;
    let db = &pulsus_testkit::test_db("pulsus_loki_push_superseded_caps_it");
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[("PULSUS_COMPAT_ENDPOINTS", "1")]);

    let base_ns = now_ns();
    // 257 distinct labels: one past MAX_LABELS_PER_STREAM. `sup_label` marks the
    // stream so its absence is queryable rather than merely uncounted.
    let over_labels = {
        let mut m = String::from(r#"{"service_name":"sup-374-labels""#);
        for i in 0..257 {
            m.push_str(&format!(r#","k{i}":"v""#));
        }
        m.push('}');
        m
    };
    // 100_001 entries: one past MAX_ENTRIES_PER_STREAM, the reviewer's probe.
    let over_values = {
        let mut v = String::with_capacity(3_000_000);
        v.push('[');
        for i in 0..100_001 {
            if i > 0 {
                v.push(',');
            }
            v.push_str(&format!(r#"["{base_ns}","sup line"]"#));
        }
        v.push(']');
        v
    };
    let win = |service: &str| {
        format!(r#"{{"stream":{{"service_name":"{service}"}},"values":[["{base_ns}","FINAL"]]}}"#)
    };

    let cases: [(&str, String, String, &str); 3] = [
        (
            // A superseded `streams` occurrence whose stream is over the label
            // cap, under a case-folded spelling of the same key.
            "streams / label count",
            format!(
                r#"{{"streams":[{{"stream":{over_labels},"values":[["{base_ns}","sup line"]]}}],"StReAmS":[{}]}}"#,
                win("win-374-a")
            ),
            format!(
                r#"{{"StReAmS":[{}],"streams":[{{"stream":{over_labels},"values":[["{base_ns}","sup line"]]}}]}}"#,
                win("win-374-a")
            ),
            "win-374-a",
        ),
        (
            // A superseded `stream` key inside one stream object.
            "stream / label count",
            format!(
                r#"{{"streams":[{{"stream":{over_labels},"stream":{{"service_name":"win-374-b"}},"values":[["{base_ns}","FINAL"]]}}]}}"#
            ),
            format!(
                r#"{{"streams":[{{"stream":{{"service_name":"win-374-b"}},"stream":{over_labels},"values":[["{base_ns}","FINAL"]]}}]}}"#
            ),
            "win-374-b",
        ),
        (
            // A superseded `values` key carrying 100_001 entries.
            "values / entries cap",
            format!(
                r#"{{"streams":[{{"stream":{{"service_name":"win-374-c"}},"values":{over_values},"values":[["{base_ns}","FINAL"]]}}]}}"#
            ),
            format!(
                r#"{{"streams":[{{"stream":{{"service_name":"win-374-c"}},"values":[["{base_ns}","FINAL"]],"values":{over_values}}}]}}"#
            ),
            "win-374-c",
        ),
    ];

    for (case, superseded, surviving, service) in &cases {
        let res = push(port, "application/json", superseded.as_bytes());
        assert_eq!(
            res.status, 204,
            "{case}: superseded over-cap value still decided it (body {})",
            res.body
        );
        let lines = wait_for_line(port, service, base_ns, "FINAL");
        assert!(
            lines.contains(&"FINAL".to_string()) && !lines.contains(&"sup line".to_string()),
            "{case}: only the final occurrence's line is stored, got {lines:?}"
        );

        // The same value LAST is still refused — without this half, deleting
        // the cap outright would pass.
        let res = push(port, "application/json", surviving.as_bytes());
        assert_eq!(
            res.status, 400,
            "{case}: the SURVIVING over-cap value was admitted (body {})",
            res.body
        );
    }

    // Storage, not statuses: three winning streams, and nothing from any
    // superseded or refused occurrence.
    assert_eq!(
        wait_for_count(
            db,
            "SELECT count() AS c FROM log_streams WHERE service LIKE 'win-374-%'",
            3,
        )
        .await,
        3,
        "each case registers exactly its winning stream"
    );
    assert_eq!(
        ch_count(
            db,
            "SELECT count() AS c FROM log_streams WHERE service = 'sup-374-labels'",
        )
        .await,
        0,
        "the superseded stream is never registered"
    );
    assert_eq!(
        ch_count(
            db,
            "SELECT count() AS c FROM log_samples WHERE body = 'sup line'"
        )
        .await,
        0,
        "no superseded line is stored"
    );
    assert_eq!(
        ch_count(
            db,
            "SELECT count() AS c FROM log_samples WHERE body = 'FINAL'"
        )
        .await,
        3,
        "exactly the three final lines are stored"
    );
}
