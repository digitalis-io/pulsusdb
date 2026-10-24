//! Issue #738: `X-Scope-OrgID` on every trace route, through the
//! `pulsusdb` binary against a live ClickHouse.
//!
//! ```text
//!   T-A  every trace route over three tenants' pushes     each answers as its own database would
//!   T-J  the service-graph job over two tenants           pairs within one tenant
//!   T-H  the header on every trace route                  '' / valid / 400
//!   T-P  an invalid header beside an invalid parameter    the header's 400, every handler
//!   T-D  two tenants' byte-identical pushes               two pushes
//! ```
//!
//! The pushes are the design's §9.3: tenants `a` and `b` share trace `T1`,
//! its three span ids, the service `checkout` and the key `http.route`;
//! `b` alone has `T3` and `span.only_b`; no header has `T2`.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-server --test traces_api_tenants_live
//! ```

#[path = "support/live_db.rs"]
mod live_db;

use live_db::ScopedDb;

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use pulsus_clickhouse::{ChClient, Idempotency, QuerySettings};

const ROUTES_PORT: u16 = 31_890;
const ROUTES_A_PORT: u16 = 31_891;
const ROUTES_B_PORT: u16 = 31_892;
const ROUTES_N_PORT: u16 = 31_893;
const GRAPH_PORT: u16 = 31_894;
const HEADER_PORT: u16 = 31_895;
const PRECEDENCE_PORT: u16 = 31_896;
const DEDUP_PORT: u16 = 31_897;

const SECOND_NS: i64 = 1_000_000_000;
const MINUTE_NS: i64 = 60 * SECOND_NS;
const DAY_NS: i64 = 1_440 * MINUTE_NS;

const T1: [u8; 16] = [0x11; 16];
const T2: [u8; 16] = [0x22; 16];
const T3: [u8; 16] = [0x33; 16];
const R: [u8; 8] = [0xaa; 8];
const C: [u8; 8] = [0xcc; 8];
const S: [u8; 8] = [0xdd; 8];

const SERVER: i32 = 2;
const CLIENT: i32 = 3;
const PRODUCER: i32 = 4;
const CONSUMER: i32 = 5;

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
    let mut head = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
         Accept: application/json\r\n"
    )
    .into_bytes();
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
    let chunked = lines.any(|line| {
        line.to_ascii_lowercase()
            .starts_with("transfer-encoding: chunked")
    });
    let body = if chunked {
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

/// The binary over `db`, the compatibility routes on, push suppression on
/// only when `dedup`.
fn spawn_ready(port: u16, db: &ScopedDb, dedup: bool) -> ChildGuard {
    assert_port_free(port);
    let mut command = Command::new(env!("CARGO_BIN_EXE_pulsusdb"));
    command
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env("PULSUS_COMPAT_ENDPOINTS", "true")
        .env("PULSUS_INGEST_DEDUP", if dedup { "true" } else { "false" })
        .env("CLICKHOUSE_SERVER", live_db::ch_host())
        .env("CLICKHOUSE_HTTP_PORT", live_db::ch_http_port().to_string())
        .env("CLICKHOUSE_DB", db.name());
    let mut guard = ChildGuard(command.spawn().expect("spawn pulsusdb"));
    live_db::register_push_target(port, db.name());
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

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
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

/// The design's `T0`: the test's start less 60 s, floored to a minute.
fn t0_ns() -> i64 {
    (now_ns() - MINUTE_NS).div_euclid(MINUTE_NS) * MINUTE_NS
}

// ---------------------------------------------------------------------
// The fixture (§9.3)
// ---------------------------------------------------------------------

/// One span of the fixture.
struct Sp {
    trace: [u8; 16],
    id: [u8; 8],
    parent: Option<[u8; 8]>,
    name: &'static str,
    kind: i32,
    start_ms: i64,
    duration_ms: i64,
    route: &'static str,
    error: bool,
    only_b: bool,
}

/// One resource of the fixture and its spans.
type Res = (&'static str, &'static str, Vec<Sp>);

#[allow(clippy::too_many_arguments)]
fn sp(
    trace: [u8; 16],
    id: [u8; 8],
    parent: Option<[u8; 8]>,
    name: &'static str,
    kind: i32,
    start_ms: i64,
    duration_ms: i64,
    route: &'static str,
) -> Sp {
    Sp {
        trace,
        id,
        parent,
        name,
        kind,
        start_ms,
        duration_ms,
        route,
        error: false,
        only_b: false,
    }
}

/// The OTLP/JSON body of `resources`, every start `t0` plus its offset.
fn otlp_body(t0: i64, resources: &[Res]) -> Vec<u8> {
    let resource_spans: Vec<serde_json::Value> = resources
        .iter()
        .map(|(service, env, spans)| {
            let spans: Vec<serde_json::Value> = spans
                .iter()
                .map(|s| {
                    let start = t0 + s.start_ms * 1_000_000;
                    let end = start + s.duration_ms * 1_000_000;
                    let mut attributes = vec![serde_json::json!(
                        {"key": "http.route", "value": {"stringValue": s.route}}
                    )];
                    if s.only_b {
                        attributes.push(serde_json::json!(
                            {"key": "only_b", "value": {"stringValue": "x"}}
                        ));
                    }
                    let mut span = serde_json::json!({
                        "traceId": hex(&s.trace),
                        "spanId": hex(&s.id),
                        "name": s.name,
                        "kind": s.kind,
                        "startTimeUnixNano": start.to_string(),
                        "endTimeUnixNano": end.to_string(),
                        "attributes": attributes,
                        "status": {"code": if s.error { 2 } else { 1 }},
                    });
                    if let Some(parent) = s.parent {
                        span["parentSpanId"] = serde_json::json!(hex(&parent));
                    }
                    span
                })
                .collect();
            serde_json::json!({
                "resource": {"attributes": [
                    {"key": "service.name", "value": {"stringValue": service}},
                    {"key": "deployment.environment", "value": {"stringValue": env}},
                ]},
                "scopeSpans": [{"spans": spans}],
            })
        })
        .collect();
    serde_json::to_vec(&serde_json::json!({ "resourceSpans": resource_spans }))
        .expect("a JSON body")
}

/// The three pushes, `(tenant, resources)`, in the design's order.
fn fixture() -> Vec<(Option<&'static str>, Vec<Res>)> {
    let mut a_root = sp(T1, R, None, "GET /a", SERVER, 1_000, 50, "/a");
    a_root.error = true;
    let mut b_root = sp(T1, R, None, "GET /b", SERVER, 2_000, 70, "/b");
    b_root.only_b = true;
    vec![
        (
            None,
            vec![(
                "checkout",
                "env-n",
                vec![sp(T2, R, None, "GET /n", SERVER, 4_000, 20, "/n")],
            )],
        ),
        (
            Some("a"),
            vec![
                (
                    "checkout",
                    "env-a",
                    vec![a_root, sp(T1, C, Some(R), "call", CLIENT, 1_001, 10, "/a")],
                ),
                (
                    "payments",
                    "env-a",
                    vec![sp(T1, S, Some(C), "pay", SERVER, 1_002, 5, "/a")],
                ),
            ],
        ),
        (
            Some("b"),
            vec![
                (
                    "checkout",
                    "env-b",
                    vec![
                        b_root,
                        sp(T1, C, Some(R), "call", PRODUCER, 2_001, 10, "/b"),
                    ],
                ),
                (
                    "inventory",
                    "env-b",
                    vec![sp(T1, S, Some(C), "reserve", CONSUMER, 2_002, 5, "/b")],
                ),
                (
                    "checkout",
                    "env-b",
                    vec![sp(T3, R, None, "GET /b3", SERVER, 3_000, 30, "/b3")],
                ),
            ],
        ),
    ]
}

/// Pushes `resources` to `port` with `header`, sync, and waits until the
/// tables its landing block feeds hold it.
fn push(port: u16, t0: i64, header: Option<&str>, resources: &[Res], ctx: &str) {
    let body = otlp_body(t0, resources);
    let r = request(
        port,
        "POST",
        "/v1/traces",
        Some("application/json"),
        &org(header),
        &body,
    );
    assert_eq!(r.status, 200, "{ctx}: push: {}", r.body);
    let keys: Vec<(Vec<u8>, Vec<u8>)> = resources
        .iter()
        .flat_map(|(_, _, spans)| spans.iter().map(|s| (s.trace.to_vec(), s.id.to_vec())))
        .collect();
    live_db::settle_pushed_spans(port, &keys, ctx);
}

/// Every push of the fixture, each under its own header.
fn push_all(port: u16, t0: i64) {
    for (tenant, resources) in fixture() {
        push(port, t0, tenant, &resources, &format!("push as {tenant:?}"));
    }
}

// ---------------------------------------------------------------------
// The requests (§9.4) and their witnesses
// ---------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Fetch,
    FetchV2,
    Search,
    Tags,
    Values,
    Range,
    Instant,
    Graph,
}

/// `(id, kind, path)` for every request of §9.4, its window `[t0 − 60 s,
/// t0 + 300 s)`.
fn requests(t0: i64) -> Vec<(&'static str, Kind, String)> {
    let (s, e) = (
        (t0 - MINUTE_NS) / SECOND_NS,
        (t0 + 5 * MINUTE_NS) / SECOND_NS,
    );
    let w = format!("start={s}&end={e}");
    let t1 = hex(&T1);
    let t3 = hex(&T3);
    let search = |q: &str| format!("/api/search?q={}&{w}&limit=20", urlencode(q));
    let range = |p: &str, q: &str| format!("{p}?q={}&{w}&step=60s", urlencode(q));
    let mut out = vec![
        ("F1", Kind::Fetch, format!("/api/traces/v1/trace/{t1}")),
        ("F1", Kind::Fetch, format!("/api/traces/v1/trace/{t1}/json")),
        ("F1", Kind::Fetch, format!("/api/traces/{t1}")),
        ("F1", Kind::Fetch, format!("/tempo/api/traces/{t1}")),
        ("F2", Kind::Fetch, format!("/api/traces/v1/trace/{t3}")),
        ("F3", Kind::FetchV2, format!("/api/v2/traces/{t1}")),
        ("F4", Kind::Fetch, format!("/api/traces/v1/trace/{t1}?{w}")),
        (
            "S1",
            Kind::Search,
            search(r#"{ resource.service.name = "checkout" }"#),
        ),
        (
            "S2",
            Kind::Search,
            format!(
                "/api/traces/v1/search?q={}&{w}&limit=20",
                urlencode("{ status = error }")
            ),
        ),
        (
            "S3",
            Kind::Search,
            search(r#"{ resource.service.name = "checkout" } >> { kind = server }"#),
        ),
        (
            "S4",
            Kind::Search,
            search(r#"{ span.http.route != "" } | count() > 1"#),
        ),
        ("S5", Kind::Search, search("{}")),
        (
            "S6",
            Kind::Search,
            search(r#"{ span.http.route =~ "/.*" } | select(span.http.route)"#),
        ),
    ];
    for p in [
        "/api/v2/search/tags",
        "/api/search/tags",
        "/api/traces/v1/tags",
    ] {
        out.push(("T1", Kind::Tags, p.to_string()));
    }
    for p in [
        "/api/v2/search/tag/span.http.route/values",
        "/api/search/tag/span.http.route/values",
        "/api/traces/v1/tag/span.http.route/values",
    ] {
        out.push(("T2", Kind::Values, p.to_string()));
    }
    let q = |q: &str| urlencode(q);
    out.extend([
        (
            "T3",
            Kind::Values,
            format!(
                "/api/v2/search/tag/span.http.route/values?q={}&{w}",
                q(r#"{ resource.service.name = "checkout" }"#)
            ),
        ),
        (
            "T4",
            Kind::Values,
            format!(
                "/api/v2/search/tag/resource.deployment.environment/values?q={}&{w}",
                q(r#"{ resource.service.name = "checkout" }"#)
            ),
        ),
        (
            "T5",
            Kind::Values,
            "/api/v2/search/tag/name/values".to_string(),
        ),
        (
            "T6",
            Kind::Values,
            format!(
                "/api/v2/search/tag/span.http.route/values?q={}&{w}",
                q("{ kind = server }")
            ),
        ),
    ]);
    for p in [
        "/api/metrics/query_range",
        "/tempo/api/metrics/query_range",
        "/api/traces/v1/metrics/query_range",
    ] {
        out.push((
            "M1",
            Kind::Range,
            range(p, "{ } | count_over_time() by (resource.service.name)"),
        ));
    }
    out.extend([
        (
            "M2",
            Kind::Instant,
            format!(
                "/api/metrics/query?q={}&{w}",
                urlencode("{ } | count_over_time()")
            ),
        ),
        (
            "M3",
            Kind::Range,
            range(
                "/api/metrics/query_range",
                "{ } | compare({ status = error })",
            ),
        ),
        (
            "M4",
            Kind::Range,
            range(
                "/api/metrics/query_range",
                "{ } | quantile_over_time(duration, .9)",
            ),
        ),
        (
            "G1",
            Kind::Graph,
            format!("/api/traces/v1/service_graph?{w}"),
        ),
    ]);
    out
}

fn set(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|s| s.to_string()).collect()
}

/// Every span name in a fetch body.
fn span_names(v: &serde_json::Value) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    fn walk(v: &serde_json::Value, out: &mut BTreeSet<String>) {
        match v {
            serde_json::Value::Object(map) => {
                for (k, x) in map {
                    if k == "spans" {
                        for s in x.as_array().into_iter().flatten() {
                            if let Some(n) = s["name"].as_str() {
                                out.insert(n.to_string());
                            }
                        }
                    } else {
                        walk(x, out);
                    }
                }
            }
            serde_json::Value::Array(xs) => xs.iter().for_each(|x| walk(x, out)),
            _ => {}
        }
    }
    walk(v, &mut out);
    out
}

/// The trace ids a search body lists.
fn trace_ids(v: &serde_json::Value) -> BTreeSet<String> {
    v["traces"]
        .as_array()
        .unwrap_or_else(|| panic!("traces: {v}"))
        .iter()
        .map(|t| t["traceID"].as_str().expect("traceID").to_string())
        .collect()
}

/// Every `http.route` value a search body's spans carry.
fn selected_routes(v: &serde_json::Value) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for t in v["traces"].as_array().into_iter().flatten() {
        let mut sets: Vec<&serde_json::Value> =
            t["spanSets"].as_array().into_iter().flatten().collect();
        if let Some(one) = t.get("spanSet") {
            sets.push(one);
        }
        for set in sets {
            for s in set["spans"].as_array().into_iter().flatten() {
                for a in s["attributes"].as_array().into_iter().flatten() {
                    if a["key"].as_str().is_some_and(|k| k.ends_with("http.route"))
                        && let Some(v) = a["value"]["stringValue"].as_str()
                    {
                        out.insert(v.to_string());
                    }
                }
            }
        }
    }
    out
}

/// The values a tag-values body lists, in any of its three shapes.
fn tag_values(v: &serde_json::Value) -> BTreeSet<String> {
    v["tagValues"]
        .as_array()
        .unwrap_or_else(|| panic!("tagValues: {v}"))
        .iter()
        .map(|x| match x.as_str() {
            Some(s) => s.to_string(),
            None => x["value"].as_str().unwrap_or("").to_string(),
        })
        .collect()
}

/// Each metrics series, by its labels as `k=v` text, and the sum of its
/// samples (the range form) or its value (the instant form).
fn series(v: &serde_json::Value) -> BTreeMap<String, f64> {
    let mut out = BTreeMap::new();
    for s in v["series"].as_array().into_iter().flatten() {
        let labels: Vec<String> = s["labels"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|l| {
                let value = &l["value"];
                let text = value["stringValue"]
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| value.to_string());
                format!("{}={}", l["key"].as_str().unwrap_or(""), text)
            })
            .collect();
        let sum = match s.get("samples") {
            Some(samples) => samples
                .as_array()
                .into_iter()
                .flatten()
                .map(|p| p["value"].as_f64().unwrap_or(0.0))
                .sum(),
            None => s["value"].as_f64().unwrap_or(0.0),
        };
        out.insert(labels.join(","), sum);
    }
    out
}

/// `(client, server, connectionType, calls, failed)` per edge.
fn edges(v: &serde_json::Value) -> BTreeSet<(String, String, String, u64, u64)> {
    v["edges"]
        .as_array()
        .unwrap_or_else(|| panic!("edges: {v}"))
        .iter()
        .map(|e| {
            (
                e["client"].as_str().expect("client").to_string(),
                e["server"].as_str().expect("server").to_string(),
                e["connectionType"]
                    .as_str()
                    .expect("connectionType")
                    .to_string(),
                e["calls"].as_u64().expect("calls"),
                e["failed"].as_u64().expect("failed"),
            )
        })
        .collect()
}

fn edge(client: &str, server: &str, conn: &str) -> (String, String, String, u64, u64) {
    (
        client.to_string(),
        server.to_string(),
        conn.to_string(),
        1,
        0,
    )
}

/// G1's witness for `tenant`.
fn graph_witness(tenant: Option<&str>) -> BTreeSet<(String, String, String, u64, u64)> {
    match tenant {
        Some("a") => [edge("checkout", "payments", "rpc")].into(),
        Some("b") => [edge("checkout", "inventory", "messaging")].into(),
        _ => BTreeSet::new(),
    }
}

/// §9.4's witness for request `id` as `tenant`: `None` when it holds, else
/// what was found.
fn witness(id: &str, tenant: Option<&str>, r: &HttpResponse) -> Option<String> {
    let t1 = hex(&T1);
    let t2 = hex(&T2);
    let t3 = hex(&T3);
    let who = tenant.unwrap_or("none");
    let fail = |what: String| Some(format!("{id} as {who}: {what}"));
    let ok_json = || -> Result<serde_json::Value, Option<String>> {
        if r.status == 200 {
            Ok(json(&r.body))
        } else {
            Err(Some(format!(
                "{id} as {who}: status {}: {}",
                r.status, r.body
            )))
        }
    };
    let names = |want: &[&str]| -> Option<String> {
        let v = match ok_json() {
            Ok(v) => v,
            Err(e) => return e,
        };
        let got = span_names(&v);
        (got != set(want)).then(|| format!("{id} as {who}: span names {got:?}, want {want:?}"))
    };
    match (id, tenant) {
        ("F1" | "F4", Some("a")) => names(&["GET /a", "call", "pay"]),
        ("F1" | "F4", Some("b")) => names(&["GET /b", "call", "reserve"]),
        ("F1" | "F4", _) | ("F2", Some("a") | None) => {
            (r.status != 404).then(|| format!("{id} as {who}: status {}, want 404", r.status))
        }
        ("F2", _) => (r.status != 200).then(|| format!("{id} as {who}: status {}", r.status)),
        ("F3", Some("a")) => names(&["GET /a", "call", "pay"]),
        ("F3", Some("b")) => names(&["GET /b", "call", "reserve"]),
        ("F3", _) => names(&[]),
        ("S1" | "S5" | "S2" | "S3" | "S4", _) => {
            let v = match ok_json() {
                Ok(v) => v,
                Err(e) => return e,
            };
            let got = trace_ids(&v);
            let want: BTreeSet<String> = match (id, who) {
                ("S1" | "S5", "a") => [t1.clone()].into(),
                ("S1" | "S5", "b") => [t1.clone(), t3.clone()].into(),
                ("S1" | "S5", _) => [t2.clone()].into(),
                ("S2" | "S3" | "S4", "a") => [t1.clone()].into(),
                ("S4", "b") => [t1.clone()].into(),
                _ => BTreeSet::new(),
            };
            if got != want {
                return fail(format!("trace ids {got:?}, want {want:?}"));
            }
            if id == "S2" && who == "a" {
                let t = &v["traces"][0];
                if t["rootTraceName"] != "GET /a" || t["durationMs"] != 50 {
                    return fail(format!(
                        "root {} duration {}",
                        t["rootTraceName"], t["durationMs"]
                    ));
                }
            }
            None
        }
        ("S6", _) => {
            let v = match ok_json() {
                Ok(v) => v,
                Err(e) => return e,
            };
            let got = selected_routes(&v);
            let want = match who {
                "a" => set(&["/a"]),
                "b" => set(&["/b", "/b3"]),
                _ => set(&["/n"]),
            };
            (got != want).then(|| format!("{id} as {who}: routes {got:?}, want {want:?}"))
        }
        ("T1", _) => {
            if r.status != 200 {
                return fail(format!("status {}", r.status));
            }
            let listed = r.body.contains("only_b");
            (listed != (who == "b")).then(|| format!("{id} as {who}: only_b listed {listed}"))
        }
        ("T2" | "T3" | "T6" | "T4" | "T5", _) => {
            let v = match ok_json() {
                Ok(v) => v,
                Err(e) => return e,
            };
            let got = tag_values(&v);
            let want = match (id, who) {
                ("T4", "a") => set(&["env-a"]),
                ("T4", "b") => set(&["env-b"]),
                ("T4", _) => set(&["env-n"]),
                ("T5", "a") => set(&["GET /a", "call", "pay"]),
                ("T5", "b") => set(&["GET /b", "GET /b3", "call", "reserve"]),
                ("T5", _) => set(&["GET /n"]),
                (_, "a") => set(&["/a"]),
                (_, "b") => set(&["/b", "/b3"]),
                _ => set(&["/n"]),
            };
            (got != want).then(|| format!("{id} as {who}: values {got:?}, want {want:?}"))
        }
        ("M1", _) => {
            let v = match ok_json() {
                Ok(v) => v,
                Err(e) => return e,
            };
            let got = series(&v);
            let want: BTreeMap<String, f64> = match who {
                "a" => [
                    ("resource.service.name=checkout".to_string(), 2.0),
                    ("resource.service.name=payments".to_string(), 1.0),
                ]
                .into(),
                "b" => [
                    ("resource.service.name=checkout".to_string(), 3.0),
                    ("resource.service.name=inventory".to_string(), 1.0),
                ]
                .into(),
                _ => [("resource.service.name=checkout".to_string(), 1.0)].into(),
            };
            (got != want).then(|| format!("{id} as {who}: series {got:?}, want {want:?}"))
        }
        ("M2", _) => {
            let v = match ok_json() {
                Ok(v) => v,
                Err(e) => return e,
            };
            let total: f64 = series(&v).values().sum();
            let want = match who {
                "a" => 3.0,
                "b" => 4.0,
                _ => 1.0,
            };
            (total != want).then(|| format!("{id} as {who}: {total}, want {want}"))
        }
        ("M3", _) => {
            let v = match ok_json() {
                Ok(v) => v,
                Err(e) => return e,
            };
            let selection = series(&v)
                .keys()
                .any(|k| k.contains("__meta_type=selection"));
            (selection != (who == "a"))
                .then(|| format!("{id} as {who}: a selection series {selection}: {}", r.body))
        }
        ("M4", _) => {
            let v = match ok_json() {
                Ok(v) => v,
                Err(e) => return e,
            };
            series(&v)
                .is_empty()
                .then(|| format!("{id} as {who}: no series"))
        }
        ("G1", _) => {
            let v = match ok_json() {
                Ok(v) => v,
                Err(e) => return e,
            };
            let got = edges(&v);
            let want = graph_witness(tenant);
            (got != want).then(|| format!("{id} as {who}: edges {got:?}, want {want:?}"))
        }
        _ => fail("no witness".to_string()),
    }
}

// ---------------------------------------------------------------------
// T-A
// ---------------------------------------------------------------------

/// **T-A (issue #738): every trace route answers its own tenant.** The
/// binary over one database holding all three tenants' pushes; three more,
/// each over a database holding one tenant's pushes sent with no header.
/// For every request of §9.4 and each tenant, the answer as that tenant
/// over the first is the answer with no header over its own database, and
/// §9.4's witness holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_trace_route_answers_its_own_tenant() {
    skip_unless_live!();
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_trace_tenants_it_routes")).await;
    let db_a = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_trace_tenants_it_routes_a")).await;
    let db_b = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_trace_tenants_it_routes_b")).await;
    let db_n = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_trace_tenants_it_routes_n")).await;
    let _server = spawn_ready(ROUTES_PORT, &db, false);
    let _server_a = spawn_ready(ROUTES_A_PORT, &db_a, false);
    let _server_b = spawn_ready(ROUTES_B_PORT, &db_b, false);
    let _server_n = spawn_ready(ROUTES_N_PORT, &db_n, false);
    let t0 = t0_ns();
    push_all(ROUTES_PORT, t0);
    // `fixture()` is in the order none, `a`, `b`.
    for ((tenant, resources), own) in
        fixture()
            .into_iter()
            .zip([ROUTES_N_PORT, ROUTES_A_PORT, ROUTES_B_PORT])
    {
        push(
            own,
            t0,
            None,
            &resources,
            &format!("{tenant:?}'s own database"),
        );
    }

    let mut failures = Vec::new();
    for (id, _kind, path) in requests(t0) {
        for (tenant, own) in [
            (Some("a"), ROUTES_A_PORT),
            (Some("b"), ROUTES_B_PORT),
            (None, ROUTES_N_PORT),
        ] {
            let got = get(ROUTES_PORT, &path, tenant);
            let want = get(own, &path, None);
            if (got.status, &got.body) != (want.status, &want.body) {
                failures.push(format!(
                    "{id} {path} as {tenant:?}: {} {}\n  its own database: {} {}",
                    got.status, got.body, want.status, want.body
                ));
            }
            if let Some(f) = witness(id, tenant, &got) {
                failures.push(f);
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} failures: {failures:#?}",
        failures.len()
    );
}

// ---------------------------------------------------------------------
// T-J
// ---------------------------------------------------------------------

/// The edge job stopped, and no run in flight.
async fn stop_edges_view(client: &ChClient) {
    exec(client, "SYSTEM STOP VIEW service_edges_mv").await;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let status = strings(
            client,
            "SELECT status AS s FROM system.view_refreshes WHERE database = currentDatabase() \
             AND view = 'service_edges_mv'",
        )
        .await;
        if status == ["Disabled"] {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the edge job did not stop: {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// G1 for `[from, from + 10 min)` as each tenant: the failures.
fn graph_reads(port: u16, from: i64, what: &str) -> Vec<String> {
    let path = format!(
        "/api/traces/v1/service_graph?start={}&end={}",
        from / SECOND_NS,
        (from + 10 * MINUTE_NS) / SECOND_NS
    );
    let mut failures = Vec::new();
    for tenant in [Some("a"), Some("b"), None] {
        let r = get(port, &path, tenant);
        if let Some(f) = witness("G1", tenant, &r) {
            failures.push(format!("{what}: {f}"));
        }
    }
    failures
}

/// **T-J (issue #738): the graph job pairs within one tenant.** `a`'s and
/// `b`'s halves share a trace and span ids; each tenant's graph is its own
/// edge, read from the spans (the raw path), from the job's totals (the
/// covered path), and from the totals alone once the spans are gone. The
/// job's totals carry the tenant; its markers do not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_graph_job_pairs_within_one_tenant() {
    skip_unless_live!();
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_trace_tenants_it_graph")).await;
    let client = client_for(db.name()).await;
    stop_edges_view(&client).await;
    exec(&client, "TRUNCATE TABLE service_edges").await;
    let _server = spawn_ready(GRAPH_PORT, &db, false);
    let midnight = (now_ns().div_euclid(DAY_NS) - 1) * DAY_NS;
    let t0 = midnight + 30 * SECOND_NS;
    push_all(GRAPH_PORT, t0);

    let raw = graph_reads(GRAPH_PORT, midnight, "the raw read");
    assert!(raw.is_empty(), "{raw:#?}");

    exec(&client, "SYSTEM START VIEW service_edges_mv").await;
    let marker = format!(
        "toDateTime({}, 'UTC')",
        (midnight + 4 * MINUTE_NS) / SECOND_NS
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let found = strings(
            &client,
            &format!(
                "SELECT toString(count()) AS s FROM service_edges FINAL \
                 WHERE conn_type = '' AND minute = {marker}"
            ),
        )
        .await;
        if found != ["0"] {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no marker at yesterday 00:04 within 60 s"
        );
        exec(&client, "SYSTEM REFRESH VIEW service_edges_mv").await;
        exec(&client, "SYSTEM WAIT VIEW service_edges_mv").await;
    }
    let covered = graph_reads(GRAPH_PORT, midnight, "the covered read");
    assert!(covered.is_empty(), "{covered:#?}");

    exec(&client, "TRUNCATE TABLE spans").await;
    let totals = graph_reads(GRAPH_PORT, midnight, "the totals-only read");
    assert!(totals.is_empty(), "{totals:#?}");

    let stored = strings(
        &client,
        "SELECT concat(org_id, ' ', client, ' ', server, ' ', conn_type) AS s \
         FROM service_edges FINAL WHERE conn_type != '' ORDER BY s",
    )
    .await;
    assert_eq!(
        stored,
        ["a checkout payments rpc", "b checkout inventory messaging"],
        "the job's totals, each under its own tenant"
    );
    let markers = strings(
        &client,
        "SELECT DISTINCT org_id AS s FROM service_edges FINAL WHERE conn_type = ''",
    )
    .await;
    assert_eq!(markers, [""], "every marker row is under the empty tenant");
}

// ---------------------------------------------------------------------
// T-H
// ---------------------------------------------------------------------

/// The three write routes: `(path, content type, body)`, every span of
/// trace `6666…`.
fn writes(t0: i64) -> Vec<(&'static str, &'static str, Vec<u8>)> {
    let refused = [0x66u8; 16];
    let otlp = otlp_body(
        t0,
        &[(
            "refused",
            "env-r",
            vec![sp(refused, R, None, "refused", SERVER, 5_000, 1, "/r")],
        )],
    );
    let zipkin = format!(
        r#"[{{"traceId":"{}","id":"{}","name":"refused","timestamp":{},"duration":1000,"localEndpoint":{{"serviceName":"refused"}}}}]"#,
        hex(&refused),
        hex(&R),
        (t0 + 5 * SECOND_NS) / 1_000
    )
    .into_bytes();
    vec![
        ("/v1/traces", "application/json", otlp),
        ("/api/v2/spans", "application/json", zipkin.clone()),
        ("/tempo/spans", "application/json", zipkin),
    ]
}

/// **T-H (issue #738): the header is the metrics rule on every trace
/// route.** An empty value, 151 bytes, a space, `|`, a non-ASCII byte or
/// the header twice: `400 invalid X-Scope-OrgID` on the three write routes
/// and every read path, and the refused writes store nothing. No header:
/// the empty tenant's answers. A 150-byte tenant: its own, empty, answers,
/// and its writes are accepted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_header_is_the_metrics_rule_on_every_trace_route() {
    skip_unless_live!();
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_trace_tenants_it_header")).await;
    let _server = spawn_ready(HEADER_PORT, &db, false);
    let client = client_for(db.name()).await;
    let t0 = t0_ns();
    push_all(HEADER_PORT, t0);

    let invalid: Vec<(&str, Vec<Vec<u8>>)> = vec![
        ("an empty value", vec![Vec::new()]),
        ("151 bytes", vec!["a".repeat(151).into_bytes()]),
        ("a space", vec![b"a b".to_vec()]),
        ("a bar", vec![b"a|b".to_vec()]),
        ("a non-ASCII byte", vec![b"a\xffb".to_vec()]),
        ("the header twice", vec![b"a".to_vec(), b"b".to_vec()]),
    ];
    for (what, values) in &invalid {
        let headers: Vec<(&str, &[u8])> = values
            .iter()
            .map(|v| ("X-Scope-OrgID", v.as_slice()))
            .collect();
        for (path, ct, body) in writes(t0) {
            let r = request(HEADER_PORT, "POST", path, Some(ct), &headers, &body);
            assert_eq!(r.status, 400, "{what}: POST {path}: {}", r.body);
            if path == "/v1/traces" {
                assert!(
                    r.body.contains("invalid X-Scope-OrgID"),
                    "{what}: POST {path}: {}",
                    r.body
                );
            } else {
                assert_eq!(
                    r.body.trim_end(),
                    "invalid X-Scope-OrgID",
                    "{what}: POST {path}"
                );
            }
        }
        for (id, _, path) in requests(t0) {
            let r = request(HEADER_PORT, "GET", &path, None, &headers, &[]);
            assert_eq!(r.status, 400, "{what}: {id} {path}: {}", r.body);
            assert_eq!(r.body, "invalid X-Scope-OrgID", "{what}: {id} {path}");
        }
    }
    let stored = strings(
        &client,
        &format!(
            "SELECT toString(count()) AS s FROM spans WHERE trace_id = unhex('{}')",
            hex(&[0x66; 16])
        ),
    )
    .await;
    assert_eq!(stored, ["0"], "a refused write stores no span");

    // No header: the empty tenant's answers.
    let mut failures = Vec::new();
    for (id, _, path) in requests(t0) {
        if let Some(f) = witness(id, None, &get(HEADER_PORT, &path, None)) {
            failures.push(f);
        }
    }
    assert!(failures.is_empty(), "no header: {failures:#?}");

    // 150 bytes: a tenant with nothing stored reads nothing, then writes.
    let long = "a".repeat(150);
    for (id, kind, path) in requests(t0) {
        let r = get(HEADER_PORT, &path, Some(&long));
        let empty = match kind {
            Kind::Fetch => r.status == 404,
            _ if r.status != 200 => false,
            Kind::FetchV2 => span_names(&json(&r.body)).is_empty(),
            Kind::Search => trace_ids(&json(&r.body)).is_empty(),
            Kind::Tags => !r.body.contains("http.route"),
            Kind::Values => tag_values(&json(&r.body)).is_empty(),
            Kind::Range | Kind::Instant => series(&json(&r.body)).is_empty(),
            Kind::Graph => edges(&json(&r.body)).is_empty(),
        };
        if !empty {
            failures.push(format!("{id} {path}: {} {}", r.status, r.body));
        }
    }
    assert!(
        failures.is_empty(),
        "the 150-byte tenant reads nothing: {failures:#?}"
    );
    for (path, ct, body) in writes(t0) {
        let r = request(
            HEADER_PORT,
            "POST",
            path,
            Some(ct),
            &org(Some(&long)),
            &body,
        );
        let want = if path == "/v1/traces" { 200 } else { 202 };
        assert_eq!(r.status, want, "150 bytes: POST {path}: {}", r.body);
    }
}

// ---------------------------------------------------------------------
// T-P
// ---------------------------------------------------------------------

/// **T-P (issue #738): an invalid tenant is refused before any parameter.**
/// Each of the thirteen read handlers, sent `X-Scope-OrgID: a b` beside a
/// parameter it refuses, answers the header's `400`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_invalid_tenant_is_refused_before_any_parameter() {
    skip_unless_live!();
    let db = ScopedDb::fresh(pulsus_testkit::test_db(
        "pulsus_trace_tenants_it_precedence",
    ))
    .await;
    let _server = spawn_ready(PRECEDENCE_PORT, &db, false);
    let q = urlencode("{");
    let rows = [
        ("trace_by_id", "/api/traces/v1/trace/zz".to_string()),
        (
            "trace_by_id_json",
            "/api/traces/v1/trace/zz/json".to_string(),
        ),
        ("trace_by_id_v2", "/api/v2/traces/zz".to_string()),
        // A window, so the refused parameter is `q`.
        (
            "search",
            format!("/api/traces/v1/search?q={q}&start=1&end=2"),
        ),
        ("tags", "/api/traces/v1/tags?scope=bogus".to_string()),
        (
            "tag_values",
            "/api/traces/v1/tag/x/values?start=x".to_string(),
        ),
        ("tags_v1", "/api/search/tags?scope=bogus".to_string()),
        (
            "tag_values_v1",
            "/api/search/tag/x/values?start=x".to_string(),
        ),
        ("tags_v2", "/api/v2/search/tags?scope=bogus".to_string()),
        (
            "tag_values_v2",
            "/api/v2/search/tag/x/values?start=x".to_string(),
        ),
        (
            "metrics_query_range",
            format!("/api/traces/v1/metrics/query_range?q={q}"),
        ),
        (
            "metrics_query",
            format!("/api/traces/v1/metrics/query?q={q}"),
        ),
        (
            "service_graph",
            "/api/traces/v1/service_graph?start=x".to_string(),
        ),
    ];
    let headers = [("X-Scope-OrgID", &b"a b"[..])];
    let mut failures = Vec::new();
    for (handler, path) in rows {
        let r = request(PRECEDENCE_PORT, "GET", &path, None, &headers, &[]);
        if (r.status, r.body.as_str()) != (400, "invalid X-Scope-OrgID") {
            failures.push(format!("{handler} {path}: {} {}", r.status, r.body));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

// ---------------------------------------------------------------------
// T-D
// ---------------------------------------------------------------------

/// **T-D (issue #738): two tenants' identical pushes are two pushes.** One
/// body under an `Idempotency-Key`, another with none; each pushed by `a`,
/// then byte for byte by `b`, then by `a` again. Each tenant stores its
/// span once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_tenants_identical_pushes_are_two_pushes() {
    skip_unless_live!();
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_trace_tenants_it_dedup")).await;
    let _server = spawn_ready(DEDUP_PORT, &db, true);
    let client = client_for(db.name()).await;
    let t0 = t0_ns();
    for (trace, key) in [([0x44u8; 16], Some("k")), ([0x55u8; 16], None)] {
        let body = otlp_body(
            t0,
            &[(
                "checkout",
                "env",
                vec![sp(trace, R, None, "GET /d", SERVER, 1_000, 10, "/d")],
            )],
        );
        for tenant in ["a", "b", "a"] {
            let mut headers = org(Some(tenant));
            if let Some(k) = key {
                headers.push(("Idempotency-Key", k.as_bytes()));
            }
            let r = request(
                DEDUP_PORT,
                "POST",
                "/v1/traces",
                Some("application/json"),
                &headers,
                &body,
            );
            assert_eq!(r.status, 200, "{} as {tenant}: {}", hex(&trace), r.body);
        }
        let stored = strings(
            &client,
            &format!(
                "SELECT concat(org_id, ' ', toString(count())) AS s FROM spans \
                 WHERE trace_id = unhex('{}') GROUP BY org_id ORDER BY org_id",
                hex(&trace)
            ),
        )
        .await;
        assert_eq!(stored, ["a 1", "b 1"], "trace {}", hex(&trace));
    }
}
