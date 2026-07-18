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
use pulsus_write::protocols::loki_push::{
    EntryAdapter, LabelPairAdapter, PushRequest, StreamAdapter, Timestamp,
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
    let db = "pulsus_loki_push_sm_it";
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
    let db = "pulsus_loki_push_sm_collision_it";
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
    let db = "pulsus_loki_push_sm_double_collision_it";
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
