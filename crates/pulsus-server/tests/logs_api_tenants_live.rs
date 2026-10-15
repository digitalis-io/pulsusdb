//! Issue #653: `X-Scope-OrgID` on every log route, through the
//! `pulsusdb` binary against a live ClickHouse.
//!
//! ```text
//!   T-S  the six log tables' sorting keys, and per-tenant rows   org_id leads every key
//!   T-A  every log route over three tenants' pushes              each answers its own
//!   T-H  the header on every log route                           '' / valid / 400
//!   T-P  an invalid header beside an invalid parameter           the header's 400, every handler
//!   T-D  two tenants' byte-identical pushes                      two pushes
//! ```
//!
//! The pushes are the design's §9.3: three tenants on one stream `A`,
//! `tenant-q` alone on `E` and on an OTLP stream, `tenant-o` alone on `W`.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-server --test logs_api_tenants_live
//! ```

#[path = "support/live_db.rs"]
mod live_db;

use live_db::ScopedDb;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use pulsus_clickhouse::{ChClient, Idempotency, QuerySettings};

const SORTING_PORT: u16 = 31_860;
const ROUTES_PORT: u16 = 31_861;
const HEADER_PORT: u16 = 31_862;
const PRECEDENCE_PORT: u16 = 31_863;
const DEDUP_PORT: u16 = 31_864;

const SECOND_NS: i64 = 1_000_000_000;
const MINUTE_NS: i64 = 60 * SECOND_NS;

const Q1: &str = r#"level=info user=alice msg="get a""#;
const Q2: &str = r#"level=error user=alice msg="get b""#;
const Q3: &str = r#"level=warn user=bob msg="east only""#;
const O1: &str = r#"level=info order=7 msg="post c""#;
const O2: &str = r#"level=debug order=8 msg="west only""#;
const N1: &str = r#"level=info msg="anon""#;
const Q4: &str = r#"level=info msg="otlp q""#;

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
            return;
        }
    };
}

// ---------------------------------------------------------------------
// HTTP
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
        let Ok(size_str) = std::str::from_utf8(&raw[..line_end]) else {
            break;
        };
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

/// One request. Each `(name, value)` header is written as given, bytes and
/// all, so a header the HTTP layer would refuse to build can still be sent.
fn request(
    port: u16,
    method: &str,
    path: &str,
    content_type: Option<&str>,
    headers: &[(&str, &[u8])],
    body: &[u8],
) -> HttpResponse {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(60))).ok();
    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\n").into_bytes();
    // A caller's own `Connection` (the tail's upgrade) replaces the default.
    if !headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("connection"))
    {
        head.extend_from_slice(b"Connection: close\r\n");
    }
    if let Some(ct) = content_type {
        head.extend_from_slice(format!("Content-Type: {ct}\r\n").as_bytes());
    }
    for (name, value) in headers {
        head.extend_from_slice(name.as_bytes());
        head.extend_from_slice(b": ");
        head.extend_from_slice(value);
        head.extend_from_slice(b"\r\n");
    }
    if method != "GET" {
        head.extend_from_slice(format!("Content-Length: {}\r\n", body.len()).as_bytes());
    }
    head.extend_from_slice(b"\r\n");
    head.extend_from_slice(body);
    stream.write_all(&head).expect("send");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).ok();
    let split_at = find_subslice(&buf, b"\r\n\r\n").expect("a response head");
    let head_text = String::from_utf8_lossy(&buf[..split_at]).into_owned();
    let raw_body = &buf[split_at + 4..];
    let mut lines = head_text.lines();
    let status = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .expect("a status line");
    let headers: HashMap<String, String> = lines
        .filter_map(|line| {
            let (k, v) = line.split_once(": ")?;
            Some((k.to_ascii_lowercase(), v.to_string()))
        })
        .collect();
    let body = if headers
        .get("transfer-encoding")
        .is_some_and(|v| v == "chunked")
    {
        dechunk(raw_body)
    } else {
        raw_body.to_vec()
    };
    HttpResponse {
        status,
        body: String::from_utf8_lossy(&body).into_owned(),
    }
}

/// The `X-Scope-OrgID` header for `tenant`, or none.
fn org(tenant: Option<&str>) -> Vec<(&'static str, &[u8])> {
    tenant
        .map(|t| vec![("X-Scope-OrgID", t.as_bytes())])
        .unwrap_or_default()
}

fn get(port: u16, path: &str, tenant: Option<&str>) -> HttpResponse {
    request(port, "GET", path, None, &org(tenant), &[])
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

fn json(body: &str) -> serde_json::Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("{e}: {body}"))
}

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn assert_port_free(port: u16) {
    if let Err(e) = std::net::TcpListener::bind(("127.0.0.1", port)) {
        panic!(
            "port {port} is already in use ({e}): a previous run's server is probably \
             still listening. Find it with `ss -ltnp`; do not kill by program name."
        );
    }
}

/// The binary over `db`, with the `/loki/api/v1` surface on and ingest
/// level discovery off, so a stream's lines are one series in a metric
/// query.
fn spawn_ready(port: u16, db: &ScopedDb) -> ChildGuard {
    assert_port_free(port);
    let mut command = Command::new(env!("CARGO_BIN_EXE_pulsusdb"));
    command
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env("PULSUS_COMPAT_ENDPOINTS", "true")
        .env("PULSUS_DISCOVER_LOG_LEVELS", "false")
        .env("CLICKHOUSE_SERVER", live_db::ch_host())
        .env("CLICKHOUSE_HTTP_PORT", live_db::ch_http_port().to_string())
        .env("CLICKHOUSE_DB", db.name());
    let mut guard = ChildGuard(command.spawn().expect("spawn pulsusdb"));
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if let Ok(Some(status)) = guard.0.try_wait() {
            panic!("pulsusdb exited before becoming ready ({status}) on port {port}");
        }
        if TcpStream::connect(("127.0.0.1", port)).is_ok()
            && get(port, "/ready", None).status == 200
        {
            return guard;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("/ready never reached 200 within 60s (port {port})");
}

async fn client_for(db: &str) -> ChClient {
    ChClient::new(live_db::conn_config(db))
        .await
        .expect("connect to the live ClickHouse")
}

async fn strings(client: &ChClient, sql: &str) -> Vec<String> {
    client
        .query_strings(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"))
}

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::NonIdempotent)
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
}

/// `SELECT org_id, count() FROM log_samples WHERE body = <line>`, one
/// `"<org_id> <count>"` text per tenant, ordered.
async fn stored(client: &ChClient, line: &str) -> Vec<String> {
    strings(
        client,
        &format!(
            "SELECT concat(org_id, ' ', toString(count())) AS s FROM log_samples \
             WHERE body = '{}' GROUP BY org_id ORDER BY org_id",
            line.replace('\\', "\\\\").replace('\'', "\\'")
        ),
    )
    .await
}

fn t0_ns() -> i64 {
    let now = chrono::Utc::now().timestamp_nanos_opt().expect("now");
    (now - 10 * MINUTE_NS) / MINUTE_NS * MINUTE_NS
}

// ---------------------------------------------------------------------
// Pushes
// ---------------------------------------------------------------------

/// A `/loki/api/v1/push` JSON body: each `(labels, [(ns, line)])` one stream.
fn loki_body(streams: &[(&[(&str, &str)], Vec<(i64, &str)>)]) -> Vec<u8> {
    let streams: Vec<serde_json::Value> = streams
        .iter()
        .map(|(labels, values)| {
            let stream: serde_json::Map<String, serde_json::Value> = labels
                .iter()
                .map(|(k, v)| (k.to_string(), serde_json::json!(v)))
                .collect();
            serde_json::json!({
                "stream": stream,
                "values": values
                    .iter()
                    .map(|(ns, line)| serde_json::json!([ns.to_string(), line]))
                    .collect::<Vec<_>>(),
            })
        })
        .collect();
    serde_json::to_vec(&serde_json::json!({ "streams": streams })).expect("a JSON body")
}

/// An OTLP/JSON logs body: one record of `line` at `ns`, resource
/// `service.name` = `service`.
fn otlp_body(service: &str, ns: i64, line: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "resourceLogs": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": service}}
            ]},
            "scopeLogs": [{"logRecords": [{
                "timeUnixNano": ns.to_string(),
                "body": {"stringValue": line}
            }]}]
        }]
    }))
    .expect("a JSON body")
}

fn loki_push(port: u16, body: &[u8], headers: &[(&str, &[u8])]) -> HttpResponse {
    request(
        port,
        "POST",
        "/loki/api/v1/push",
        Some("application/json"),
        headers,
        body,
    )
}

fn otlp_push(port: u16, body: &[u8], headers: &[(&str, &[u8])]) -> HttpResponse {
    request(
        port,
        "POST",
        "/v1/logs",
        Some("application/json"),
        headers,
        body,
    )
}

const A: &[(&str, &str)] = &[("app", "api"), ("service_name", "api")];
const E: &[(&str, &str)] = &[("app", "api"), ("service_name", "api"), ("zone", "east")];
const W: &[(&str, &str)] = &[("app", "api"), ("region", "west"), ("service_name", "api")];

/// The design's §9.3 pushes, in order, each answered as a sync push is.
fn push_all(port: u16, t0: i64) {
    let s = SECOND_NS;
    let push1 = loki_body(&[
        (A, vec![(t0 + s, Q1), (t0 + 2 * s, Q2)]),
        (E, vec![(t0 + 3 * s, Q3)]),
    ]);
    let r = loki_push(port, &push1, &org(Some("tenant-q")));
    assert_eq!(r.status, 204, "push 1: {}", r.body);
    let push2 = loki_body(&[(A, vec![(t0 + 4 * s, O1)]), (W, vec![(t0 + 5 * s, O2)])]);
    let r = loki_push(port, &push2, &org(Some("tenant-o")));
    assert_eq!(r.status, 204, "push 2: {}", r.body);
    let push3 = loki_body(&[(A, vec![(t0 + 6 * s, N1)])]);
    let r = loki_push(port, &push3, &org(None));
    assert_eq!(r.status, 204, "push 3: {}", r.body);
    let push4 = otlp_body("otlp", t0 + 7 * s, Q4);
    let r = otlp_push(port, &push4, &org(Some("tenant-q")));
    assert_eq!(r.status, 200, "push 4: {}", r.body);
}

// ---------------------------------------------------------------------
// Reading the answers
// ---------------------------------------------------------------------

/// The design's name for a stream's label set: `A`, `E`, `W` or `OTLP`.
fn stream_name(labels: &serde_json::Value) -> String {
    let get = |k: &str| labels.get(k).and_then(|v| v.as_str()).unwrap_or("");
    if get("service_name") == "otlp" {
        "OTLP".to_string()
    } else if get("zone") == "east" {
        "E".to_string()
    } else if get("region") == "west" {
        "W".to_string()
    } else if get("app") == "api" && labels.as_object().map(|o| o.len()) == Some(2) {
        "A".to_string()
    } else {
        format!("other {labels}")
    }
}

fn ok_json(port: u16, path: &str, tenant: Option<&str>) -> serde_json::Value {
    let r = get(port, path, tenant);
    assert_eq!(r.status, 200, "{path} as {tenant:?}: {}", r.body);
    json(&r.body)
}

fn str_set(v: &serde_json::Value) -> BTreeSet<String> {
    v.as_array()
        .unwrap_or_else(|| panic!("an array: {v}"))
        .iter()
        .map(|x| x.as_str().expect("a string").to_string())
        .collect()
}

fn names(v: &serde_json::Value, key: &str) -> BTreeSet<String> {
    v.as_array()
        .unwrap_or_else(|| panic!("an array: {v}"))
        .iter()
        .map(|x| x[key].as_str().expect("a string").to_string())
        .collect()
}

fn set(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|s| s.to_string()).collect()
}

/// A metric result as `{stream: value}`.
fn metric_values(result: &serde_json::Value, matrix: bool) -> BTreeMap<String, String> {
    result
        .as_array()
        .unwrap_or_else(|| panic!("a result array: {result}"))
        .iter()
        .map(|s| {
            let value = if matrix {
                s["values"][0][1].clone()
            } else {
                s["value"][1].clone()
            };
            (
                stream_name(&s["metric"]),
                value.as_str().expect("a value").to_string(),
            )
        })
        .collect()
}

fn values_sum(result: &serde_json::Value) -> u64 {
    result
        .as_array()
        .unwrap_or_else(|| panic!("a result array: {result}"))
        .iter()
        .map(|s| {
            s["value"][1]
                .as_str()
                .expect("a value")
                .parse::<u64>()
                .expect("a whole number")
        })
        .sum()
}

/// One tenant's column of §9.4's case table.
struct Column {
    r1: BTreeMap<String, Vec<String>>,
    r2: BTreeMap<String, String>,
    r4_has: BTreeSet<String>,
    r4_not: BTreeSet<String>,
    r5: BTreeSet<String>,
    r6: BTreeSet<String>,
    r7_has: BTreeSet<String>,
    r7_not: BTreeSet<String>,
    r8_has: BTreeSet<String>,
    r8_not: BTreeSet<String>,
    r9: BTreeSet<String>,
    lines: Vec<&'static str>,
    streams: u64,
    r15: Vec<String>,
}

fn len_sum(lines: &[&str]) -> u64 {
    lines.iter().map(|l| l.len() as u64).sum()
}

/// Opens the tail socket as `tenant`.
fn tail_connect(port: u16, target: &str, tenant: Option<&str>) -> (TcpStream, Vec<u8>) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_millis(200)))
        .expect("read timeout");
    let mut head = format!(
        "GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\
         Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
    );
    if let Some(t) = tenant {
        head.push_str(&format!("X-Scope-OrgID: {t}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).expect("handshake write");
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
    assert!(
        head_text.starts_with("HTTP/1.1 101"),
        "the tail upgrades: {head_text}"
    );
    (stream, buf[split_at + 4..].to_vec())
}

/// Every line the tail delivers within `wait`.
fn tail_lines(port: u16, target: &str, tenant: Option<&str>, wait: Duration) -> Vec<String> {
    let (mut stream, mut buf) = tail_connect(port, target, tenant);
    let deadline = Instant::now() + wait;
    let mut out = Vec::new();
    let mut chunk = [0u8; 65536];
    while Instant::now() < deadline {
        // One whole server frame off the front of `buf`.
        if buf.len() >= 2 {
            let opcode = buf[0] & 0x0F;
            let len7 = (buf[1] & 0x7F) as usize;
            let (len, header) = match len7 {
                126 if buf.len() >= 4 => (u16::from_be_bytes([buf[2], buf[3]]) as usize, 4),
                127 if buf.len() >= 10 => {
                    let mut b = [0u8; 8];
                    b.copy_from_slice(&buf[2..10]);
                    (u64::from_be_bytes(b) as usize, 10)
                }
                126 | 127 => (usize::MAX, 0),
                n => (n, 2),
            };
            if len != usize::MAX && buf.len() >= header + len {
                let payload: Vec<u8> = buf.drain(..header + len).skip(header).collect();
                if opcode == 0x1 {
                    let frame = json(&String::from_utf8_lossy(&payload));
                    for s in frame["streams"].as_array().expect("streams") {
                        for v in s["values"].as_array().expect("values") {
                            out.push(v[1].as_str().expect("a line").to_string());
                        }
                    }
                } else if opcode == 0x8 {
                    break;
                }
                continue;
            }
        }
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut => {}
            Err(_) => break,
        }
    }
    let _ = stream.write_all(&[0x88, 0x80, 0x12, 0x34, 0x56, 0x78]);
    out.sort();
    out
}

/// Asserts every case of §9.4 for `tenant` against `want`.
async fn assert_column(port: u16, client: &ChClient, t0: i64, tenant: Option<&str>, want: &Column) {
    let who = tenant.unwrap_or("no header");
    let window = format!("start={}&end={}", t0 - MINUTE_NS, t0 + 2 * MINUTE_NS);
    let sel = urlencode(r#"{app="api"}"#);

    // R1: the streams.
    let v = ok_json(
        port,
        &format!("/api/logs/v1/query_range?query={sel}&limit=100&{window}"),
        tenant,
    );
    let r1: BTreeMap<String, Vec<String>> = v["data"]["result"]
        .as_array()
        .expect("streams")
        .iter()
        .map(|s| {
            (
                stream_name(&s["stream"]),
                s["values"]
                    .as_array()
                    .expect("values")
                    .iter()
                    .map(|p| p[1].as_str().expect("a line").to_string())
                    .collect(),
            )
        })
        .collect();
    assert_eq!(r1, want.r1, "R1 as {who}");

    // R2: a range metric query at one point.
    let at = t0 + MINUTE_NS;
    let q = urlencode(r#"count_over_time({app="api"}[10m])"#);
    let v = ok_json(
        port,
        &format!("/api/logs/v1/query_range?query={q}&start={at}&end={at}&step=60"),
        tenant,
    );
    assert_eq!(
        metric_values(&v["data"]["result"], true),
        want.r2,
        "R2 as {who}"
    );

    // R3: an instant metric query with a line filter.
    let q = urlencode(r#"count_over_time({app="api"} |= "msg" [10m])"#);
    let v = ok_json(
        port,
        &format!("/api/logs/v1/query?query={q}&time={at}"),
        tenant,
    );
    assert_eq!(
        metric_values(&v["data"]["result"], false),
        want.r2,
        "R3 as {who}"
    );

    // R4: label names.
    let v = ok_json(port, &format!("/api/logs/v1/labels?{window}"), tenant);
    let got = str_set(&v["data"]);
    assert!(want.r4_has.is_subset(&got), "R4 as {who}: {got:?}");
    assert!(want.r4_not.is_disjoint(&got), "R4 as {who}: {got:?}");

    // R5: the values of `zone`.
    let v = ok_json(
        port,
        &format!("/api/logs/v1/label/zone/values?{window}"),
        tenant,
    );
    assert_eq!(str_set(&v["data"]), want.r5, "R5 as {who}");

    // R6: series.
    let v = ok_json(
        port,
        &format!("/api/logs/v1/series?match[]={sel}&{window}"),
        tenant,
    );
    let got: BTreeSet<String> = v["data"]
        .as_array()
        .expect("series")
        .iter()
        .map(stream_name)
        .collect();
    assert_eq!(got, want.r6, "R6 as {who}");

    // R7: detected labels.
    let v = ok_json(
        port,
        &format!("/api/logs/v1/detected_labels?query={sel}&{window}"),
        tenant,
    );
    let got = names(&v["detectedLabels"], "label");
    assert!(want.r7_has.is_subset(&got), "R7 as {who}: {got:?}");
    assert!(want.r7_not.is_disjoint(&got), "R7 as {who}: {got:?}");

    // R8: detected fields.
    let v = ok_json(
        port,
        &format!("/api/logs/v1/detected_fields?query={sel}&{window}"),
        tenant,
    );
    let got = names(&v["fields"], "label");
    assert!(want.r8_has.is_subset(&got), "R8 as {who}: {got:?}");
    assert!(want.r8_not.is_disjoint(&got), "R8 as {who}: {got:?}");

    // R9: the values of the detected field `level`.
    let v = ok_json(
        port,
        &format!("/api/logs/v1/detected_field/level/values?query={sel}&{window}"),
        tenant,
    );
    assert_eq!(str_set(&v["values"]), want.r9, "R9 as {who}");

    // R10: volume.
    let bytes = len_sum(&want.lines);
    let v = ok_json(
        port,
        &format!("/api/logs/v1/volume?query={sel}&{window}"),
        tenant,
    );
    assert_eq!(values_sum(&v["data"]["result"]), bytes, "R10 as {who}");

    // R11, R12: stats from the rollup and from the raw read.
    for (case, query) in [
        ("R11", r#"{app="api"}"#),
        ("R12", r#"{app="api"} |= "msg""#),
    ] {
        let v = ok_json(
            port,
            &format!("/api/logs/v1/stats?query={}&{window}", urlencode(query)),
            tenant,
        );
        assert_eq!(
            (
                v["streams"].as_u64(),
                v["entries"].as_u64(),
                v["bytes"].as_u64()
            ),
            (
                Some(want.streams),
                Some(want.lines.len() as u64),
                Some(bytes)
            ),
            "{case} as {who}: {v}"
        );
    }

    // R13: patterns.
    let v = ok_json(
        port,
        &format!("/api/logs/v1/patterns?query={sel}&{window}"),
        tenant,
    );
    let total: u64 = v["data"]
        .as_array()
        .expect("patterns")
        .iter()
        .flat_map(|p| p["samples"].as_array().expect("samples").clone())
        .map(|s| s[1].as_u64().expect("a count"))
        .sum();
    assert_eq!(total, want.lines.len() as u64, "R13 as {who}: {v}");

    // R14: the tail.
    let got = tail_lines(
        port,
        &format!("/api/logs/v1/tail?query={sel}&start={t0}"),
        tenant,
        Duration::from_secs(6),
    );
    let mut lines: Vec<String> = want.lines.iter().map(|l| l.to_string()).collect();
    lines.sort();
    assert_eq!(got, lines, "R14 as {who}");

    // R15: the OTLP push, read and stored.
    let v = ok_json(
        port,
        &format!(
            "/api/logs/v1/query_range?query={}&limit=100&{window}",
            urlencode(r#"{service_name="otlp"}"#)
        ),
        tenant,
    );
    let got: Vec<String> = v["data"]["result"]
        .as_array()
        .expect("streams")
        .iter()
        .flat_map(|s| s["values"].as_array().expect("values").clone())
        .map(|p| p[1].as_str().expect("a line").to_string())
        .collect();
    assert_eq!(got, want.r15, "R15 as {who}");
    assert_eq!(
        stored(client, Q4).await,
        vec!["tenant-q 1".to_string()],
        "R15: the OTLP push is stored under tenant-q alone"
    );
}

fn s(items: &[&str]) -> Vec<String> {
    items.iter().map(|x| x.to_string()).collect()
}

/// **T-S (issue #653): the log tables lead with the tenant.** The six
/// local tables' sorting keys first; then, after §9.3's pushes and a merge
/// of the four collapsing tables, stream `A`'s rows grouped by `org_id`:
/// one registration and two index rows per tenant, and per-tenant counts.
#[tokio::test(flavor = "multi_thread")]
async fn the_log_tables_lead_with_the_tenant() {
    skip_unless_live!();
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_log_tenants_it_sorting")).await;
    let _server = spawn_ready(SORTING_PORT, &db);
    let client = client_for(db.name()).await;
    push_all(SORTING_PORT, t0_ns());
    for table in [
        "log_metrics_5s",
        "log_patterns",
        "log_streams",
        "log_streams_idx",
    ] {
        exec(&client, &format!("OPTIMIZE TABLE {table} FINAL")).await;
    }

    // One comparison of all six keys, so a failure names every table.
    let tables = [
        "log_landing",
        "log_metrics_5s",
        "log_patterns",
        "log_samples",
        "log_streams",
        "log_streams_idx",
    ];
    let mut got = Vec::new();
    for table in tables {
        let key = strings(
            &client,
            &format!(
                "SELECT sorting_key AS s FROM system.tables WHERE database = '{}' \
                 AND name = '{table}'",
                db.name()
            ),
        )
        .await;
        got.push(format!("{table}: {}", key.join(" | ")));
    }
    let want: Vec<String> = [
        "log_landing: org_id, kind, service, fingerprint, timestamp_ns",
        "log_metrics_5s: org_id, fingerprint, bucket_ns",
        "log_patterns: org_id, fingerprint, bucket_ns, pattern",
        "log_samples: org_id, service, fingerprint, timestamp_ns",
        "log_streams: org_id, fingerprint",
        "log_streams_idx: org_id, key, val, fingerprint",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    assert_eq!(got, want, "the six log tables' sorting keys");
    // The tests-first seam's setting is gone from both aggregating tables.
    let seamed = strings(
        &client,
        &format!(
            "SELECT name AS s FROM system.tables WHERE database = '{}' \
             AND name IN ('log_metrics_5s', 'log_patterns') \
             AND position(create_table_query, 'allow_dimensions_outside_sorting_key') > 0 \
             ORDER BY name",
            db.name()
        ),
    )
    .await;
    assert_eq!(
        seamed,
        Vec::<String>::new(),
        "allow_dimensions_outside_sorting_key is left on"
    );

    let fp = strings(
        &client,
        "SELECT DISTINCT toString(fingerprint) AS s FROM log_streams \
         WHERE JSONExtractString(labels, 'app') = 'api' AND JSONLength(labels) = 2",
    )
    .await;
    assert_eq!(fp.len(), 1, "stream A has one fingerprint: {fp:?}");
    let fp = &fp[0];
    for (table, measure, want) in [
        ("log_streams", "count()", ["1", "1", "1"]),
        ("log_streams_idx", "count()", ["2", "2", "2"]),
        ("log_metrics_5s", "sum(count)", ["1", "1", "2"]),
        ("log_patterns", "sum(count)", ["1", "1", "2"]),
    ] {
        let got = strings(
            &client,
            &format!(
                "SELECT concat('[', org_id, '] ', toString({measure})) AS s FROM {table} \
                 WHERE fingerprint = toUInt128('{fp}') GROUP BY org_id ORDER BY org_id"
            ),
        )
        .await;
        assert_eq!(
            got,
            vec![
                format!("[] {}", want[0]),
                format!("[tenant-o] {}", want[1]),
                format!("[tenant-q] {}", want[2]),
            ],
            "{table}: stream A's rows per tenant"
        );
    }
}

/// **T-A (issue #653): every log route answers its own tenant.** §9.3's
/// pushes, then §9.4's fifteen cases as `tenant-q`, as `tenant-o` and with
/// no header.
#[tokio::test(flavor = "multi_thread")]
async fn every_log_route_answers_its_own_tenant() {
    skip_unless_live!();
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_log_tenants_it_routes")).await;
    let _server = spawn_ready(ROUTES_PORT, &db);
    let client = client_for(db.name()).await;
    let t0 = t0_ns();
    push_all(ROUTES_PORT, t0);

    let q = Column {
        r1: BTreeMap::from([("A".to_string(), s(&[Q2, Q1])), ("E".to_string(), s(&[Q3]))]),
        r2: BTreeMap::from([
            ("A".to_string(), "2".to_string()),
            ("E".to_string(), "1".to_string()),
        ]),
        r4_has: set(&["app", "service_name", "zone"]),
        r4_not: set(&["region"]),
        r5: set(&["east"]),
        r6: set(&["A", "E"]),
        r7_has: set(&["zone"]),
        r7_not: set(&["region"]),
        r8_has: set(&["user"]),
        r8_not: set(&["order"]),
        r9: set(&["error", "info", "warn"]),
        lines: vec![Q1, Q2, Q3],
        streams: 2,
        r15: s(&[Q4]),
    };
    let o = Column {
        r1: BTreeMap::from([("A".to_string(), s(&[O1])), ("W".to_string(), s(&[O2]))]),
        r2: BTreeMap::from([
            ("A".to_string(), "1".to_string()),
            ("W".to_string(), "1".to_string()),
        ]),
        r4_has: set(&["app", "region", "service_name"]),
        r4_not: set(&["zone"]),
        r5: set(&[]),
        r6: set(&["A", "W"]),
        r7_has: set(&["region"]),
        r7_not: set(&["zone"]),
        r8_has: set(&["order"]),
        r8_not: set(&["user"]),
        r9: set(&["debug", "info"]),
        lines: vec![O1, O2],
        streams: 2,
        r15: Vec::new(),
    };
    let n = Column {
        r1: BTreeMap::from([("A".to_string(), s(&[N1]))]),
        r2: BTreeMap::from([("A".to_string(), "1".to_string())]),
        r4_has: set(&["app", "service_name"]),
        r4_not: set(&["zone", "region"]),
        r5: set(&[]),
        r6: set(&["A"]),
        r7_has: set(&[]),
        r7_not: set(&["zone", "region"]),
        r8_has: set(&[]),
        r8_not: set(&["user", "order"]),
        r9: set(&["info"]),
        lines: vec![N1],
        streams: 1,
        r15: Vec::new(),
    };
    for (tenant, want) in [(Some("tenant-q"), &q), (Some("tenant-o"), &o), (None, &n)] {
        assert_column(ROUTES_PORT, &client, t0, tenant, want).await;
    }
}

/// Every log route of the design's §2, each as `(method, path, content
/// type, body)`, with valid parameters, so a refusal can only be the
/// header's. The two writes push `line`.
fn every_route(t0: i64, line: &str) -> Vec<(&'static str, String, Option<&'static str>, Vec<u8>)> {
    let sel = r#"{app="api"}"#;
    let window = format!("start={}&end={}", t0 - MINUTE_NS, t0 + 2 * MINUTE_NS);
    let form = "application/x-www-form-urlencoded";
    let mut routes = vec![
        (
            "POST",
            "/loki/api/v1/push".to_string(),
            Some("application/json"),
            loki_body(&[(
                &[("app", "refused"), ("service_name", "refused")][..],
                vec![(t0, line)],
            )]),
        ),
        (
            "POST",
            "/v1/logs".to_string(),
            Some("application/json"),
            otlp_body("refused", t0, &format!("{line} otlp")),
        ),
    ];
    let reads: [(&str, Option<&str>); 11] = [
        ("query_range", Some(sel)),
        ("query", Some(r#"count_over_time({app="api"}[1m])"#)),
        ("labels", None),
        ("label/app/values", None),
        ("series", None),
        ("detected_labels", Some(sel)),
        ("detected_fields", Some(sel)),
        ("detected_field/level/values", Some(sel)),
        ("stats", Some(sel)),
        ("volume", Some(sel)),
        ("patterns", Some(sel)),
    ];
    for prefix in ["/api/logs/v1", "/loki/api/v1"] {
        for (route, query) in reads {
            let route = match (prefix, route) {
                ("/loki/api/v1", "stats") => "index/stats",
                ("/loki/api/v1", "volume") => "index/volume",
                _ => route,
            };
            let params = match query {
                Some(q) => format!("query={}&{window}", urlencode(q)),
                None => window.clone(),
            };
            routes.push((
                "GET",
                format!("{prefix}/{route}?{params}"),
                None,
                Vec::new(),
            ));
            routes.push((
                "POST",
                format!("{prefix}/{route}"),
                Some(form),
                params.into_bytes(),
            ));
        }
    }
    routes
}

/// **T-H (issue #653): the header is the metrics rule on every log
/// route.** No header reads the empty tenant; a 150-byte tenant writes and
/// reads; an empty value, 151 bytes, `a b`, `a'b`, a non-UTF-8 byte and the
/// header given twice are each refused `400` with the rule's message on
/// both write routes, all eleven read routes by GET and POST on both
/// surfaces, and the tail. A refused write stores no row.
#[tokio::test(flavor = "multi_thread")]
async fn the_header_is_the_metrics_rule_on_every_log_route() {
    skip_unless_live!();
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_log_tenants_it_header")).await;
    let _server = spawn_ready(HEADER_PORT, &db);
    let client = client_for(db.name()).await;
    let t0 = t0_ns();
    push_all(HEADER_PORT, t0);
    let window = format!("start={}&end={}", t0 - MINUTE_NS, t0 + 2 * MINUTE_NS);
    let sel = urlencode(r#"{app="api"}"#);
    let refused = "refused line";
    let invalid: Vec<(&str, Vec<Vec<u8>>)> = vec![
        ("an empty value", vec![Vec::new()]),
        ("151 bytes", vec!["a".repeat(151).into_bytes()]),
        ("a space", vec![b"a b".to_vec()]),
        ("a quote", vec![b"a'b".to_vec()]),
        ("a non-UTF-8 byte", vec![b"a\xffb".to_vec()]),
        (
            "the header twice",
            vec![b"tenant-q".to_vec(), b"tenant-o".to_vec()],
        ),
    ];
    for (what, values) in &invalid {
        let headers: Vec<(&str, &[u8])> = values
            .iter()
            .map(|v| ("X-Scope-OrgID", v.as_slice()))
            .collect();
        for (method, path, content_type, body) in every_route(t0, refused) {
            let r = request(HEADER_PORT, method, &path, content_type, &headers, &body);
            assert_eq!(r.status, 400, "{what}: {method} {path}: {}", r.body);
            if path == "/loki/api/v1/push" {
                assert_eq!(r.body, "invalid X-Scope-OrgID\n", "{what}: {method} {path}");
            } else if path == "/v1/logs" {
                assert!(
                    r.body.contains("invalid X-Scope-OrgID"),
                    "{what}: {method} {path}: {}",
                    r.body
                );
            } else {
                assert_eq!(r.body, "invalid X-Scope-OrgID", "{what}: {method} {path}");
            }
        }
        // The tail, before the upgrade.
        let mut tail_headers = headers.clone();
        tail_headers.extend([
            ("Connection", &b"Upgrade, close"[..]),
            ("Upgrade", &b"websocket"[..]),
            ("Sec-WebSocket-Version", &b"13"[..]),
            ("Sec-WebSocket-Key", &b"dGhlIHNhbXBsZSBub25jZQ=="[..]),
        ]);
        let r = request(
            HEADER_PORT,
            "GET",
            &format!("/api/logs/v1/tail?query={sel}&start={t0}"),
            None,
            &tail_headers,
            &[],
        );
        assert_eq!(r.status, 400, "{what}: tail: {}", r.body);
        assert_eq!(r.body, "invalid X-Scope-OrgID", "{what}: tail");
    }
    for line in [refused.to_string(), format!("{refused} otlp")] {
        assert_eq!(
            stored(&client, &line).await,
            Vec::<String>::new(),
            "a refused write stores no row: {line}"
        );
    }

    // No header reads the empty tenant's rows.
    let v = ok_json(
        HEADER_PORT,
        &format!("/api/logs/v1/query_range?query={sel}&limit=100&{window}"),
        None,
    );
    let got: Vec<String> = v["data"]["result"]
        .as_array()
        .expect("streams")
        .iter()
        .flat_map(|s| s["values"].as_array().expect("values").clone())
        .map(|p| p[1].as_str().expect("a line").to_string())
        .collect();
    assert_eq!(got, s(&[N1]), "no header reads the empty tenant");

    // A 150-byte tenant writes on both routes and reads its own, empty for
    // a stream it never pushed.
    let long = "a".repeat(150);
    let r = loki_push(
        HEADER_PORT,
        &loki_body(&[(
            &[("app", "long"), ("service_name", "long")][..],
            vec![(t0, "long line")],
        )]),
        &org(Some(&long)),
    );
    assert_eq!(r.status, 204, "a 150-byte tenant's push: {}", r.body);
    let r = otlp_push(
        HEADER_PORT,
        &otlp_body("long", t0, "long otlp"),
        &org(Some(&long)),
    );
    assert_eq!(r.status, 200, "a 150-byte tenant's OTLP push: {}", r.body);
    for (method, path, content_type, body) in every_route(t0, "unused").into_iter().skip(2) {
        let r = request(
            HEADER_PORT,
            method,
            &path,
            content_type,
            &org(Some(&long)),
            &body,
        );
        assert_eq!(
            r.status, 200,
            "a 150-byte tenant: {method} {path}: {}",
            r.body
        );
    }
    let v = ok_json(
        HEADER_PORT,
        &format!("/api/logs/v1/query_range?query={sel}&limit=100&{window}"),
        Some(&long),
    );
    assert_eq!(
        v["data"]["result"],
        serde_json::json!([]),
        "a 150-byte tenant reads nothing of another tenant's"
    );
    assert_eq!(
        stored(&client, "long line").await,
        vec![format!("{long} 1")],
        "the 150-byte tenant's push is stored under it"
    );
}

/// **T-P (issue #653): an invalid tenant is refused before any
/// parameter.** Each public log handler gets `X-Scope-OrgID: a b` and a
/// parameter error of its own: a query that does not parse by GET, a
/// `Content-Type` whose parameter does not parse by POST. Each answers the
/// header's `400`, so a handler that parses first fails its own row.
#[tokio::test(flavor = "multi_thread")]
async fn an_invalid_tenant_is_refused_before_any_parameter() {
    skip_unless_live!();
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_log_tenants_it_precedence")).await;
    let _server = spawn_ready(PRECEDENCE_PORT, &db);
    let bad = [("X-Scope-OrgID", &b"a b"[..])];
    let gets = [
        "/api/logs/v1/query_range?query={",
        "/api/logs/v1/query?query={",
        "/api/logs/v1/labels?query={",
        "/api/logs/v1/label/app/values?query={",
        "/api/logs/v1/series?match[]={",
        "/api/logs/v1/detected_labels?query={",
        "/api/logs/v1/detected_fields?query={",
        "/api/logs/v1/detected_field/level/values?query={",
        "/api/logs/v1/stats?query={",
        "/api/logs/v1/volume?query={",
        "/api/logs/v1/patterns?query={",
    ];
    let posts = [
        "/api/logs/v1/query_range",
        "/api/logs/v1/query",
        "/api/logs/v1/labels",
        "/api/logs/v1/label/app/values",
        "/api/logs/v1/series",
        "/api/logs/v1/detected_labels",
        "/api/logs/v1/detected_fields",
        "/api/logs/v1/detected_field/level/values",
        "/api/logs/v1/stats",
        "/api/logs/v1/volume",
        "/api/logs/v1/patterns",
    ];
    let mut rows: Vec<(String, HttpResponse)> = Vec::new();
    for path in gets {
        rows.push((
            format!("GET {path}"),
            request(PRECEDENCE_PORT, "GET", path, None, &bad, &[]),
        ));
    }
    let mut tail_headers = bad.to_vec();
    tail_headers.extend([
        ("Connection", &b"Upgrade, close"[..]),
        ("Upgrade", &b"websocket"[..]),
        ("Sec-WebSocket-Version", &b"13"[..]),
        ("Sec-WebSocket-Key", &b"dGhlIHNhbXBsZSBub25jZQ=="[..]),
    ]);
    rows.push((
        "GET /api/logs/v1/tail?query={".to_string(),
        request(
            PRECEDENCE_PORT,
            "GET",
            "/api/logs/v1/tail?query={",
            None,
            &tail_headers,
            &[],
        ),
    ));
    for path in posts {
        rows.push((
            format!("POST {path}"),
            request(
                PRECEDENCE_PORT,
                "POST",
                path,
                Some("text/plain; ="),
                &bad,
                b"query=%7B",
            ),
        ));
    }
    assert_eq!(rows.len(), 23, "one row per public handler");
    for (row, r) in rows {
        assert_eq!(
            (r.status, r.body.as_str()),
            (400, "invalid X-Scope-OrgID"),
            "{row}"
        );
    }
}

/// **T-D (issue #653): two tenants' identical pushes are two pushes.**
/// `tenant-q` then `tenant-o` push one byte-identical body, (a) under one
/// `Idempotency-Key` and (b) under none; then `tenant-q` repeats (a). Each
/// body is stored once per tenant, and the repeat stores nothing.
#[tokio::test(flavor = "multi_thread")]
async fn two_tenants_identical_pushes_are_two_pushes() {
    skip_unless_live!();
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_log_tenants_it_dedup")).await;
    let _server = spawn_ready(DEDUP_PORT, &db);
    let client = client_for(db.name()).await;
    let t0 = t0_ns();
    let stream: &[(&str, &str)] = &[("app", "dedup"), ("service_name", "dedup")];
    let keyed = loki_body(&[(stream, vec![(t0 + SECOND_NS, "dedup keyed")])]);
    let plain = loki_body(&[(stream, vec![(t0 + 2 * SECOND_NS, "dedup plain")])]);
    let with_key = |tenant: &'static str| {
        vec![
            ("X-Scope-OrgID", tenant.as_bytes()),
            ("Idempotency-Key", &b"k-653"[..]),
        ]
    };
    for (what, body, headers) in [
        ("tenant-q, keyed", &keyed, with_key("tenant-q")),
        ("tenant-o, keyed", &keyed, with_key("tenant-o")),
        ("tenant-q, plain", &plain, org(Some("tenant-q"))),
        ("tenant-o, plain", &plain, org(Some("tenant-o"))),
        ("tenant-q, keyed again", &keyed, with_key("tenant-q")),
    ] {
        let r = loki_push(DEDUP_PORT, body, &headers);
        assert_eq!(r.status, 204, "{what}: {}", r.body);
    }
    for line in ["dedup keyed", "dedup plain"] {
        assert_eq!(
            stored(&client, line).await,
            vec!["tenant-o 1".to_string(), "tenant-q 1".to_string()],
            "{line}: once per tenant"
        );
    }
    let window = format!("start={}&end={}", t0 - MINUTE_NS, t0 + 2 * MINUTE_NS);
    for tenant in ["tenant-q", "tenant-o"] {
        let v = ok_json(
            DEDUP_PORT,
            &format!(
                "/api/logs/v1/query_range?query={}&limit=100&{window}",
                urlencode(r#"{app="dedup"}"#)
            ),
            Some(tenant),
        );
        let mut got: Vec<String> = v["data"]["result"]
            .as_array()
            .expect("streams")
            .iter()
            .flat_map(|s| s["values"].as_array().expect("values").clone())
            .map(|p| p[1].as_str().expect("a line").to_string())
            .collect();
        got.sort();
        assert_eq!(
            got,
            s(&["dedup keyed", "dedup plain"]),
            "{tenant} reads each line once"
        );
    }
}
