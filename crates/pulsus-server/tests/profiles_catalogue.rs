//! Issue #821: the profiles query API's four catalogue methods — profile
//! types, label names, label values and series — over the Connect protocol
//! and the native `GET` routes, against the `pulsusdb` binary and a live
//! ClickHouse.
//!
//! The fixture is kind-2 and kind-3 landing rows, so the views fill
//! `profile_series` and `profile_entries` as a push does. Every answer is
//! compared as a whole JSON value, and every request's statements are
//! counted in `system.query_log` by the request's `X-Pulsus-Query-Id`.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-server --test profiles_catalogue
//! ```

#[path = "support/live_db.rs"]
mod live_db;

use live_db::ScopedDb;

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::process::{Child, Command};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use prost::Message;
use pulsus_clickhouse::{ChClient, Idempotency, QuerySettings};
use pulsus_profiles_wire::types_v1::{LabelValuesRequest, LabelValuesResponse};
use serde_json::{Value, json};

const CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const ALLOC: &str = "memory:alloc_space:bytes:space:bytes";
const GOR: &str = "goroutine:goroutine:count:goroutine:count";
const WALL: &str = "wall:wall:nanoseconds:wall:nanoseconds";
/// The reference's app label (D9): stored like any label, never answered by
/// the five-name `Series` request.
const APP: &str = "pyroscope_app";
const MINUTE_MS: i64 = 60_000;
const HOUR_MS: i64 = 3_600_000;

const DEADLINE_BODY: &str = concat!(
    r#"{"code":"deadline_exceeded","message":"request timed out, decrease the "#,
    r#"duration of the request or add more label matchers (prefer exact match "#,
    r#"over regex match) to reduce the amount of data processed"}"#
);

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

// ---------------------------------------------------------------------
// The fixture
// ---------------------------------------------------------------------

/// One series of the plan's fixture table.
struct Series {
    fp: u128,
    org: &'static str,
    type_id: &'static str,
    service: &'static str,
    labels: &'static [(&'static str, &'static str)],
    /// The entry's time after `T0`.
    at_ms: i64,
}

const SERIES: [Series; 4] = [
    Series {
        fp: 1,
        org: "t1",
        type_id: CPU,
        service: "checkout",
        labels: &[("env", "prod"), ("k8s.pod", "p1"), (APP, "checkout-app")],
        at_ms: 15 * MINUTE_MS,
    },
    Series {
        fp: 2,
        org: "t1",
        type_id: ALLOC,
        service: "checkout",
        labels: &[("env", "dev")],
        at_ms: 12 * MINUTE_MS,
    },
    Series {
        fp: 3,
        org: "t3",
        type_id: GOR,
        service: "cart",
        labels: &[("env", "late"), ("only3", "x")],
        at_ms: 55 * MINUTE_MS,
    },
    Series {
        fp: 4,
        org: "t2",
        type_id: WALL,
        service: "checkout",
        labels: &[("env", "t2only")],
        at_ms: 15 * MINUTE_MS,
    },
];

/// The stored label set of a series (high-level §3): the type's five parts,
/// the type, the service twice, and its own labels, key-sorted.
fn stored(s: &Series) -> BTreeMap<String, String> {
    let parts: Vec<&str> = s.type_id.split(':').collect();
    let mut out: BTreeMap<String, String> = [
        ("__name__", parts[0]),
        ("__type__", parts[1]),
        ("__unit__", parts[2]),
        ("__period_type__", parts[3]),
        ("__period_unit__", parts[4]),
        ("__profile_type__", s.type_id),
        ("__service_name__", s.service),
        ("service_name", s.service),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    for (k, v) in s.labels {
        out.insert(k.to_string(), v.to_string());
    }
    out
}

fn sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// The landing insert: per series one kind-2 row (its labels) and one
/// kind-3 row (its entry).
fn fixture_sql(t0: i64, now: i64) -> String {
    let mut values = Vec::new();
    for s in &SERIES {
        let ts = (t0 + s.at_ms) * 1_000_000;
        let labels = serde_json::to_string(&stored(s)).expect("labels as JSON");
        values.push(format!(
            "({}, {now}, 2, {}, {}, {}, {ts}, {}, '00000000-0000-0000-0000-000000000000', 0)",
            sql_string(s.org),
            sql_string(s.type_id),
            sql_string(s.service),
            s.fp,
            sql_string(&labels)
        ));
        values.push(format!(
            "({}, {now}, 3, {}, {}, {}, {ts}, '', '0190f5a4-1b7e-7cc0-8000-000000000001', 10)",
            sql_string(s.org),
            sql_string(s.type_id),
            sql_string(s.service),
            s.fp
        ));
    }
    format!(
        "INSERT INTO profile_landing \
         (org_id, received_ms, kind, type_id, service, fingerprint, timestamp_ns, labels, \
         profile_id, value) VALUES {}",
        values.join(", ")
    )
}

// ---------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------

struct Res {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Res {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn json(&self, ctx: &str) -> Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| {
            panic!(
                "{ctx}: not JSON ({e}): {:?}",
                String::from_utf8_lossy(&self.body)
            )
        })
    }

    fn query_id(&self, ctx: &str) -> String {
        self.header("x-pulsus-query-id")
            .unwrap_or_else(|| panic!("{ctx}: no X-Pulsus-Query-Id"))
            .to_string()
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

fn dechunk(mut raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(nl) = find(raw, b"\r\n") {
        let size = std::str::from_utf8(&raw[..nl])
            .ok()
            .and_then(|s| usize::from_str_radix(s.split(';').next().unwrap_or("").trim(), 16).ok());
        let Some(size) = size else { break };
        raw = &raw[nl + 2..];
        if size == 0 || raw.len() < size {
            break;
        }
        out.extend_from_slice(&raw[..size]);
        raw = raw[size..].strip_prefix(b"\r\n").unwrap_or(&raw[size..]);
    }
    out
}

/// One HTTP/1.1 exchange.
fn call(port: u16, method: &str, path: &str, headers: &[(&str, &str)], body: &[u8]) -> Res {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to pulsusdb");
    stream.set_read_timeout(Some(Duration::from_secs(60))).ok();
    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    for (k, v) in headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
    stream.write_all(head.as_bytes()).expect("write head");
    stream.write_all(body).expect("write body");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).expect("read response");
    let split = find(&buf, b"\r\n\r\n").expect("a header block");
    let head = String::from_utf8_lossy(&buf[..split]).into_owned();
    let mut lines = head.lines();
    let status = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .expect("a status line");
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect();
    let raw = &buf[split + 4..];
    let chunked = headers.iter().any(|(k, v)| {
        k.eq_ignore_ascii_case("transfer-encoding") && v.eq_ignore_ascii_case("chunked")
    });
    let body = if chunked { dechunk(raw) } else { raw.to_vec() };
    Res {
        status,
        headers,
        body,
    }
}

/// A Connect JSON call on `path` as `tenant` (none when empty).
fn connect(port: u16, path: &str, tenant: &str, body: &Value) -> Res {
    let text = body.to_string();
    let mut headers = vec![("Content-Type", "application/json")];
    if !tenant.is_empty() {
        headers.push(("X-Scope-OrgID", tenant));
    }
    call(port, "POST", path, &headers, text.as_bytes())
}

fn get_as(port: u16, path: &str, tenant: &str) -> Res {
    call(port, "GET", path, &[("X-Scope-OrgID", tenant)], &[])
}

fn encode(v: &str) -> String {
    let mut out = String::new();
    for b in v.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

// ---------------------------------------------------------------------
// The server
// ---------------------------------------------------------------------

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn assert_port_free(port: u16) {
    if let Err(e) = TcpListener::bind(("127.0.0.1", port)) {
        panic!(
            "port {port} is already in use ({e}): a previous run's server is probably \
             still listening. Find it with `ss -ltnp`; do not kill by program name."
        );
    }
}

fn spawn_ready(port: u16, db: &str, clickhouse_http: u16, env: &[(&str, &str)]) -> ChildGuard {
    assert_port_free(port);
    let mut command = Command::new(env!("CARGO_BIN_EXE_pulsusdb"));
    command
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env("CLICKHOUSE_SERVER", live_db::ch_host())
        .env("CLICKHOUSE_HTTP_PORT", clickhouse_http.to_string())
        .env("CLICKHOUSE_DB", db);
    for (k, v) in env {
        command.env(k, v);
    }
    let mut guard = ChildGuard(command.spawn().expect("spawn pulsusdb"));
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if let Ok(Some(status)) = guard.0.try_wait() {
            panic!("pulsusdb exited before becoming ready ({status}) on port {port}");
        }
        if TcpStream::connect(("127.0.0.1", port)).is_ok()
            && call(port, "GET", "/ready", &[], &[]).status == 200
        {
            return guard;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("/ready never reached 200 within 60s (port {port})");
}

// ---------------------------------------------------------------------
// Statement counting
// ---------------------------------------------------------------------

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::NonIdempotent)
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
}

/// The finished statements `system.query_log` holds for each request id.
async fn statements(client: &ChClient, ids: &[String]) -> BTreeMap<String, u64> {
    exec(client, "SYSTEM FLUSH LOGS").await;
    let list: Vec<String> = ids.iter().map(|id| sql_string(id)).collect();
    let sql = format!(
        "SELECT concat(log_comment, ' ', toString(count())) AS s FROM system.query_log \
         WHERE type != 'QueryStart' AND is_initial_query AND log_comment IN ({}) \
         GROUP BY log_comment",
        list.join(", ")
    );
    let rows = client
        .query_strings(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
    let mut out: BTreeMap<String, u64> = ids.iter().map(|id| (id.clone(), 0)).collect();
    for row in rows {
        let (id, n) = row.split_once(' ').expect("id and count");
        out.insert(id.to_string(), n.parse().expect("a count"));
    }
    out
}

// ---------------------------------------------------------------------
// Expected answers
// ---------------------------------------------------------------------

fn split_type(id: &str) -> Value {
    let p: Vec<&str> = id.split(':').collect();
    json!({"ID": id, "name": p[0], "sampleType": p[1], "sampleUnit": p[2],
           "periodType": p[3], "periodUnit": p[4]})
}

fn pairs(set: &[(&str, &str)]) -> Value {
    let labels: Vec<Value> = set
        .iter()
        .map(|(n, v)| json!({"name": n, "value": v}))
        .collect();
    json!({ "labels": labels })
}

/// A series' whole stored set, names that fail the legacy rule dropped.
fn legacy_set(s: &Series) -> Value {
    let kept: Vec<(String, String)> = stored(s)
        .into_iter()
        .filter(|(k, _)| {
            k.chars()
                .enumerate()
                .all(|(i, c)| c == '_' || c.is_ascii_alphabetic() || (i > 0 && c.is_ascii_digit()))
        })
        .collect();
    let refs: Vec<(&str, &str)> = kept.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    pairs(&refs)
}

const BASE_NAMES: [&str; 7] = [
    "__name__",
    "__period_type__",
    "__period_unit__",
    "__profile_type__",
    "__service_name__",
    "__type__",
    "__unit__",
];

fn names_with(extra: &[&str]) -> Vec<String> {
    let mut v: Vec<String> = BASE_NAMES.iter().map(|s| s.to_string()).collect();
    v.extend(extra.iter().map(|s| s.to_string()));
    v.sort();
    v
}

// ---------------------------------------------------------------------
// The four method requests L1–L4 send, as Connect bodies and as `GET`s
// ---------------------------------------------------------------------

struct Window {
    start: i64,
    end: i64,
}

impl Window {
    fn body(&self, extra: Value) -> Value {
        let mut v = json!({"start": self.start.to_string(), "end": self.end.to_string()});
        if let (Some(o), Value::Object(e)) = (v.as_object_mut(), extra) {
            o.extend(e);
        }
        v
    }

    fn query(&self) -> String {
        format!("from={}&until={}", self.start, self.end)
    }
}

const CHECKOUT: &str = r#"{service_name="checkout"}"#;

/// `(method, Connect body, GET path)` for L1, L2's first, L3's second and
/// L4's first request.
fn method_requests(w: &Window) -> Vec<(&'static str, Value, String)> {
    vec![
        (
            "ProfileTypes",
            w.body(json!({})),
            format!("/api/profiles/v1/types?{}", w.query()),
        ),
        (
            "LabelNames",
            w.body(json!({"matchers": []})),
            format!("/api/profiles/v1/labels?{}", w.query()),
        ),
        (
            "LabelValues",
            w.body(json!({"name": "env", "matchers": [CHECKOUT]})),
            format!(
                "/api/profiles/v1/label/env/values?query={}&{}",
                encode(CHECKOUT),
                w.query()
            ),
        ),
        (
            "Series",
            w.body(json!({"matchers": [CHECKOUT],
                          "labelNames": ["__profile_type__", "service_name"]})),
            format!(
                "/api/profiles/v1/series?match%5B%5D={}&label_names%5B%5D=__profile_type__\
                 &label_names%5B%5D=service_name&{}",
                encode(CHECKOUT),
                w.query()
            ),
        ),
    ]
}

fn compat(method: &str) -> String {
    format!("/querier.v1.QuerierService/{method}")
}

fn native(method: &str) -> String {
    format!("/api/profiles/v1/querier.v1.QuerierService/{method}")
}

/// One answered request: its JSON body, checked `200`, and its id.
fn answer(res: &Res, ctx: &str) -> (Value, String) {
    assert_eq!(res.status, 200, "{ctx}: {}", res.text());
    assert_eq!(
        res.header("content-type"),
        Some("application/json"),
        "{ctx}"
    );
    (res.json(ctx), res.query_id(ctx))
}

fn assert_error(res: &Res, status: u16, code: &str, ctx: &str) -> String {
    assert_eq!(res.status, status, "{ctx}: {}", res.text());
    assert_eq!(
        res.header("content-type"),
        Some("application/json"),
        "{ctx}"
    );
    let v = res.json(ctx);
    assert_eq!(v["code"], code, "{ctx}: {v}");
    v["message"].as_str().unwrap_or_default().to_string()
}

// ---------------------------------------------------------------------
// L1–L13
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn the_catalogue_methods_answer_their_tenants_series() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_profiles_catalogue_it")).await;
    let client = ChClient::new(live_db::conn_config(db.name()))
        .await
        .expect("connect");
    let now = chrono::Utc::now().timestamp_millis();
    let t0 = now.div_euclid(HOUR_MS) * HOUR_MS - 3 * HOUR_MS;
    exec(&client, &fixture_sql(t0, now)).await;
    let w = Window {
        start: t0 + 10 * MINUTE_MS,
        end: t0 + 20 * MINUTE_MS,
    };

    let port_a: u16 = 31_821;
    let port_b: u16 = 31_822;
    let port_c: u16 = 31_823;
    let clickhouse_http = live_db::ch_http_port();
    let _a = spawn_ready(
        port_a,
        db.name(),
        clickhouse_http,
        &[("PULSUS_COMPAT_ENDPOINTS", "true")],
    );
    let a = port_a;
    let mut counted: Vec<(String, String, u64)> = Vec::new();

    // L1
    let (got, id) = answer(
        &connect(a, &compat("ProfileTypes"), "t1", &w.body(json!({}))),
        "L1",
    );
    assert_eq!(
        got,
        json!({"profileTypes": [split_type(ALLOC), split_type(CPU)]}),
        "L1"
    );
    counted.push(("L1".to_string(), id, 1));

    // L2
    let body = w.body(json!({"matchers": []}));
    let (got, _) = answer(&connect(a, &compat("LabelNames"), "t1", &body), "L2");
    assert_eq!(
        got,
        json!({"names": names_with(&["env", APP, "service_name"])}),
        "L2 legacy"
    );
    let res = call(
        a,
        "POST",
        &compat("LabelNames"),
        &[
            ("Content-Type", "application/json"),
            ("Accept", "application/json; allow-utf8-labelnames=true"),
            ("X-Scope-OrgID", "t1"),
        ],
        body.to_string().as_bytes(),
    );
    let (got, _) = answer(&res, "L2 utf8");
    assert_eq!(
        got,
        json!({"names": names_with(&["env", "k8s.pod", APP, "service_name"])}),
        "L2 utf8"
    );

    // L3
    let (got, _) = answer(
        &connect(
            a,
            &compat("LabelValues"),
            "t1",
            &w.body(json!({"name": "service_name", "matchers": []})),
        ),
        "L3 service_name",
    );
    assert_eq!(got, json!({"names": ["checkout"]}), "L3 service_name");
    let (got, _) = answer(
        &connect(
            a,
            &compat("LabelValues"),
            "t1",
            &w.body(json!({"name": "env", "matchers": [CHECKOUT]})),
        ),
        "L3 env",
    );
    assert_eq!(got, json!({"names": ["dev", "prod"]}), "L3 env");

    // L4
    let two = json!({"matchers": [CHECKOUT], "labelNames": ["__profile_type__", "service_name"]});
    let (got, _) = answer(
        &connect(a, &compat("Series"), "t1", &w.body(two.clone())),
        "L4 two",
    );
    assert_eq!(
        got,
        json!({"labelsSet": [
            pairs(&[("__profile_type__", ALLOC), ("service_name", "checkout")]),
            pairs(&[("__profile_type__", CPU), ("service_name", "checkout")]),
        ]}),
        "L4 two"
    );
    let (got, _) = answer(
        &connect(
            a,
            &compat("Series"),
            "t1",
            &w.body(json!({"matchers": [], "labelNames": []})),
        ),
        "L4 all",
    );
    assert_eq!(
        got,
        json!({"labelsSet": [legacy_set(&SERIES[1]), legacy_set(&SERIES[0])]}),
        "L4 all"
    );

    // L5
    let (got, _) = answer(
        &connect(a, &compat("ProfileTypes"), "t2", &w.body(json!({}))),
        "L5",
    );
    assert_eq!(got, json!({"profileTypes": [split_type(WALL)]}), "L5 types");
    let (got, _) = answer(
        &connect(
            a,
            &compat("LabelNames"),
            "t2",
            &w.body(json!({"matchers": []})),
        ),
        "L5",
    );
    assert_eq!(
        got,
        json!({"names": names_with(&["env", "service_name"])}),
        "L5 names"
    );
    let (got, _) = answer(
        &connect(
            a,
            &compat("LabelValues"),
            "t2",
            &w.body(json!({"name": "service_name", "matchers": []})),
        ),
        "L5",
    );
    assert_eq!(got, json!({"names": ["checkout"]}), "L5 service_name");
    let (got, _) = answer(
        &connect(
            a,
            &compat("LabelValues"),
            "t2",
            &w.body(json!({"name": "env", "matchers": [CHECKOUT]})),
        ),
        "L5",
    );
    assert_eq!(got, json!({"names": ["t2only"]}), "L5 env");
    let (got, _) = answer(
        &connect(a, &compat("Series"), "t2", &w.body(two.clone())),
        "L5",
    );
    assert_eq!(
        got,
        json!({"labelsSet": [pairs(&[("__profile_type__", WALL), ("service_name", "checkout")])]}),
        "L5 series"
    );
    let (got, _) = answer(
        &connect(a, &compat("ProfileTypes"), "", &w.body(json!({}))),
        "L5",
    );
    assert_eq!(got, json!({}), "L5 no tenant");

    // L6
    let (got, id) = answer(
        &connect(
            a,
            &compat("ProfileTypes"),
            "t1",
            &json!({"start": "1000", "end": "4000"}),
        ),
        "L6",
    );
    assert_eq!(got, json!({}), "L6");
    counted.push(("L6".to_string(), id, 0));

    // L7: the twelve paths, each answering what L1–L4 answer, each in one
    // statement.
    for (method, body, get_path) in method_requests(&w) {
        let ctx = format!("L7 {method}");
        let (expected, id) = answer(&connect(a, &compat(method), "t1", &body), &ctx);
        counted.push((format!("{ctx} compat"), id, 1));
        let (got, id) = answer(&connect(a, &native(method), "t1", &body), &ctx);
        assert_eq!(got, expected, "{ctx} native");
        counted.push((format!("{ctx} native"), id, 1));
        let (got, id) = answer(&get_as(a, &get_path, "t1"), &ctx);
        assert_eq!(got, expected, "{ctx} GET");
        counted.push((format!("{ctx} GET"), id, 1));
    }

    // L8
    let res = connect(
        a,
        &compat("LabelNames"),
        "t1",
        &w.body(json!({"matchers": ["{a="]})),
    );
    let m = assert_error(&res, 400, "invalid_argument", "L8 parse");
    assert!(
        m.starts_with(r#"parsing label selector "{a=""#),
        "L8 parse: {m}"
    );
    let res = connect(
        a,
        &compat("LabelNames"),
        "t1",
        &w.body(json!({"matchers": [r#"{env=~"("}"#]})),
    );
    let m = assert_error(&res, 400, "invalid_argument", "L8 regex");
    assert!(m.contains("missing )"), "L8 regex: {m}");
    counted.push(("L8 regex".to_string(), res.query_id("L8 regex"), 1));
    let res = call(
        a,
        "POST",
        &compat("LabelNames"),
        &[
            ("Content-Type", "application/json"),
            ("X-Scope-OrgID", "a|b"),
        ],
        b"x",
    );
    let m = assert_error(&res, 400, "invalid_argument", "L8 tenant");
    assert_eq!(m, "invalid X-Scope-OrgID", "L8 tenant");
    let res = call(
        a,
        "POST",
        &compat("LabelNames"),
        &[("Content-Type", "text/plain"), ("X-Scope-OrgID", "t1")],
        b"{}",
    );
    assert_eq!(res.status, 415, "L8 content type");
    let res = connect(
        a,
        &compat("LabelValues"),
        "t1",
        &w.body(json!({"name": "", "matchers": []})),
    );
    let m = assert_error(&res, 400, "invalid_argument", "L8 name");
    assert_eq!(m, "name is required", "L8 name");
    let long = Window {
        start: now - 25 * HOUR_MS,
        end: now,
    };
    let res = connect(a, &compat("LabelNames"), "t1", &long.body(json!({})));
    let m = assert_error(&res, 400, "invalid_argument", "L8 length");
    assert!(
        m.starts_with("the query time range exceeds the limit"),
        "L8 length: {m}"
    );

    // L9
    let req = LabelValuesRequest {
        name: "env".to_string(),
        matchers: vec![CHECKOUT.to_string()],
        start: w.start,
        end: w.end,
    };
    let res = call(
        a,
        "POST",
        &compat("LabelValues"),
        &[
            ("Content-Type", "application/proto"),
            ("X-Scope-OrgID", "t1"),
        ],
        &req.encode_to_vec(),
    );
    assert_eq!(res.status, 200, "L9: {}", res.text());
    assert_eq!(res.header("content-type"), Some("application/proto"), "L9");
    let decoded = LabelValuesResponse::decode(&res.body[..]).expect("L9 decode");
    assert_eq!(decoded.names, ["dev", "prod"], "L9");

    // L12
    let (got, _) = answer(
        &connect(a, &compat("ProfileTypes"), "t3", &w.body(json!({}))),
        "L12",
    );
    assert_eq!(got, json!({"profileTypes": [split_type(GOR)]}), "L12 types");
    let (got, _) = answer(
        &connect(
            a,
            &compat("LabelNames"),
            "t3",
            &w.body(json!({"matchers": []})),
        ),
        "L12",
    );
    assert_eq!(
        got,
        json!({"names": names_with(&["env", "only3", "service_name"])}),
        "L12 names"
    );
    let (got, _) = answer(
        &connect(
            a,
            &compat("LabelValues"),
            "t3",
            &w.body(json!({"name": "service_name", "matchers": []})),
        ),
        "L12",
    );
    assert_eq!(got, json!({"names": ["cart"]}), "L12 service_name");
    let (got, _) = answer(
        &connect(
            a,
            &compat("LabelValues"),
            "t3",
            &w.body(json!({"name": "env", "matchers": [CHECKOUT]})),
        ),
        "L12",
    );
    assert_eq!(got, json!({}), "L12 env");
    let (got, _) = answer(
        &connect(
            a,
            &compat("Series"),
            "t3",
            &w.body(json!({"matchers": [], "labelNames": []})),
        ),
        "L12",
    );
    assert_eq!(
        got,
        json!({"labelsSet": [legacy_set(&SERIES[2])]}),
        "L12 series"
    );

    // L13
    let five = [
        "service_name",
        "__name__",
        APP,
        "__type__",
        "__profile_type__",
    ];
    let expected = json!({"labelsSet": [
        pairs(&[("__name__", "memory"), ("__profile_type__", ALLOC),
                ("__type__", "alloc_space"), ("service_name", "checkout")]),
        pairs(&[("__name__", "process_cpu"), ("__profile_type__", CPU),
                ("__type__", "cpu"), ("service_name", "checkout")]),
    ]});
    for matchers in [json!([]), json!(["{}"])] {
        let ctx = format!("L13 {matchers}");
        let (got, _) = answer(
            &connect(
                a,
                &compat("Series"),
                "t1",
                &w.body(json!({"matchers": matchers, "labelNames": five})),
            ),
            &ctx,
        );
        assert_eq!(got, expected, "{ctx}");
    }

    // L7, L6 and L8: the statements each request sent.
    let ids: Vec<String> = counted.iter().map(|(_, id, _)| id.clone()).collect();
    let sent = statements(&client, &ids).await;
    for (ctx, id, want) in &counted {
        assert_eq!(sent[id], *want, "{ctx}: statements for {id}");
    }

    // L10: spawn B, the compat flag unset.
    let _b = spawn_ready(port_b, db.name(), clickhouse_http, &[]);
    let b = port_b;
    for (method, body, get_path) in method_requests(&w) {
        let ctx = format!("L10 {method}");
        assert_eq!(get_as(b, &get_path, "t1").status, 200, "{ctx} GET");
        assert_eq!(
            connect(b, &native(method), "t1", &body).status,
            200,
            "{ctx} native"
        );
        assert_eq!(
            connect(b, &compat(method), "t1", &body).status,
            404,
            "{ctx} compat"
        );
    }

    // L11: spawn C, a scan budget of one row.
    let _c = spawn_ready(
        port_c,
        db.name(),
        clickhouse_http,
        &[
            ("PULSUS_COMPAT_ENDPOINTS", "true"),
            ("PULSUS_PROFILES_SCAN_BUDGET_ROWS", "1"),
        ],
    );
    let res = connect(
        port_c,
        &compat("LabelNames"),
        "t1",
        &w.body(json!({"matchers": []})),
    );
    let m = assert_error(&res, 429, "resource_exhausted", "L11");
    assert!(m.contains("query_too_broad"), "L11: {m}");
}

// ---------------------------------------------------------------------
// L14: the request deadline against a stalled ClickHouse
// ---------------------------------------------------------------------

/// Accepts on `listen_port` and forwards to `backend` until `stalled` flips,
/// after which it forwards nothing and holds every connection open
/// (`prom_deadline_live.rs`'s proxy).
fn spawn_stall_proxy(listen_port: u16, backend: u16, stalled: Arc<AtomicBool>) {
    let listener = TcpListener::bind(("127.0.0.1", listen_port)).expect("bind the stall proxy");
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let Ok(client) = conn else { continue };
            let stalled = Arc::clone(&stalled);
            std::thread::spawn(move || {
                if stalled.load(Ordering::SeqCst) {
                    hold_open(client);
                    return;
                }
                let Ok(backend) = TcpStream::connect(("127.0.0.1", backend)) else {
                    return;
                };
                let (client_rx, client_tx) = (
                    client.try_clone().expect("clone client"),
                    client.try_clone().expect("clone client"),
                );
                let (backend_rx, backend_tx) = (
                    backend.try_clone().expect("clone backend"),
                    backend.try_clone().expect("clone backend"),
                );
                let up = Arc::clone(&stalled);
                let handle = std::thread::spawn(move || pump(client_rx, backend_tx, &up));
                pump(backend_rx, client_tx, &stalled);
                let _ = handle.join();
            });
        }
    });
}

fn hold_open(mut client: TcpStream) {
    let mut buf = [0u8; 8192];
    client
        .set_read_timeout(Some(Duration::from_millis(200)))
        .ok();
    loop {
        match client.read(&mut buf) {
            Ok(0) => return,
            Ok(_) => {}
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut => {}
            Err(_) => return,
        }
    }
}

fn pump(mut from: TcpStream, mut to: TcpStream, stalled: &AtomicBool) {
    from.set_read_timeout(Some(Duration::from_millis(50))).ok();
    let mut buf = [0u8; 16384];
    loop {
        if stalled.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(100));
            continue;
        }
        match from.read(&mut buf) {
            Ok(0) => {
                let _ = to.shutdown(Shutdown::Write);
                return;
            }
            Ok(_) if stalled.load(Ordering::SeqCst) => {}
            Ok(n) => {
                if to.write_all(&buf[..n]).is_err() {
                    return;
                }
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut => {}
            Err(_) => return,
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn l14_a_stalled_read_answers_the_connect_deadline() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let db = ScopedDb::fresh(pulsus_testkit::test_db(
        "pulsus_profiles_catalogue_it_deadline",
    ))
    .await;
    let port_d: u16 = 31_824;
    let proxy_port: u16 = 31_825;
    let stalled = Arc::new(AtomicBool::new(false));
    spawn_stall_proxy(proxy_port, live_db::ch_http_port(), Arc::clone(&stalled));
    let _d = spawn_ready(
        port_d,
        db.name(),
        proxy_port,
        &[
            ("PULSUS_QUERY_TIMEOUT", "3s"),
            ("PULSUS_COMPAT_ENDPOINTS", "true"),
        ],
    );
    stalled.store(true, Ordering::SeqCst);
    let now = chrono::Utc::now().timestamp_millis();
    let w = Window {
        start: now - 20 * MINUTE_MS,
        end: now - 10 * MINUTE_MS,
    };
    for (ctx, res) in [
        (
            "L14 Connect",
            connect(
                port_d,
                &compat("LabelNames"),
                "t1",
                &w.body(json!({"matchers": []})),
            ),
        ),
        (
            "L14 GET",
            get_as(
                port_d,
                &format!("/api/profiles/v1/labels?{}", w.query()),
                "t1",
            ),
        ),
    ] {
        assert_eq!(res.status, 504, "{ctx}: {}", res.text());
        assert_eq!(
            res.header("content-type"),
            Some("application/json"),
            "{ctx}"
        );
        assert_eq!(res.text(), DEADLINE_BODY, "{ctx}");
    }
}
