//! Issue #824: `SelectSeries` — the per-step time series of profile totals —
//! over the Connect protocol and `GET /api/profiles/v1/select_series`,
//! against the `pulsusdb` binary and a live ClickHouse.
//!
//! The fixture is plan §6.2's `x` rows as kind 2 and 3 landing rows under
//! `t1` and, at 100× the totals, `t2`'s copy of fingerprint 1. Every answer
//! is compared whole, and every request's statements are counted in
//! `system.query_log` by its `X-Pulsus-Query-Id`.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-server --test profiles_series
//! ```

#[path = "support/live_db.rs"]
mod live_db;

use live_db::ScopedDb;

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use prost::Message;
use pulsus_clickhouse::{ChClient, Idempotency, QuerySettings};
use pulsus_profiles_wire::querier_v1::{SelectSeriesRequest, SelectSeriesResponse};
use pulsus_profiles_wire::types_v1::{LabelPair, Point, Series};
use serde_json::{Value, json};

const CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const MINUTE_MS: i64 = 60_000;
const STEP_MS: i64 = 15_000;
const DAY_MS: i64 = 86_400_000;
const NO_TRACE: &str = "00000000000000000000000000000000";
const PROFILE: &str = "00000000-0000-0000-0000-0000000000a0";

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

// ---------------------------------------------------------------------
// The fixture
// ---------------------------------------------------------------------

fn insert(rows: &[String]) -> String {
    format!(
        "INSERT INTO profile_landing \
         (org_id, received_ms, kind, type_id, service, fingerprint, timestamp_ns, stack_id, \
          value, span_id, trace_id, node_ids, names, labels, profile_id) VALUES {}",
        rows.join(", ")
    )
}

/// A series row and its entries, `pod` when given.
fn rows(org: &str, s: i64, fp: u128, pod: Option<&str>, entries: &[(i64, i64)]) -> Vec<String> {
    let pod = pod.map(|p| format!(r#""pod":"{p}","#)).unwrap_or_default();
    let labels = format!(r#"{{"__profile_type__":"{CPU}",{pod}"service_name":"x"}}"#);
    let mut out = vec![format!(
        "('{org}', {s}, 2, '{CPU}', 'x', {fp}, {}, 0, 0, 0, unhex('{NO_TRACE}'), [], [], \
         '{labels}', '{PROFILE}')",
        s * 1_000_000
    )];
    for (ts, total) in entries {
        out.push(format!(
            "('{org}', {s}, 3, '{CPU}', 'x', {fp}, {}, 0, {total}, 0, unhex('{NO_TRACE}'), [], \
             [], '', '{PROFILE}')",
            ts * 1_000_000
        ));
    }
    out
}

/// Plan §6.2's `x` rows. `S` is two hours ago, floored to the minute.
async fn land(client: &ChClient, s: i64) {
    let e = s + MINUTE_MS;
    let mut all = Vec::new();
    for (org, scale) in [("t1", 1), ("t2", 100)] {
        all.extend(rows(
            org,
            s,
            1,
            Some("a"),
            &[
                (s - STEP_MS, scale),
                (s + STEP_MS, 2 * scale),
                (e + 1, 4 * scale),
            ],
        ));
    }
    all.extend(rows(
        "t1",
        s,
        2,
        Some("b"),
        &[(s + 25_000, 2), (s + 30_000, 4)],
    ));
    all.extend(rows("t1", s, 4, None, &[(s + 45_000, 5)]));
    exec(client, &insert(&all)).await;
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
        .execute(
            sql,
            &QuerySettings::new().set("insert_deduplicate", 0u64),
            Idempotency::NonIdempotent,
        )
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

fn sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

// ---------------------------------------------------------------------
// Expected answers
// ---------------------------------------------------------------------

fn compat_path() -> &'static str {
    "/querier.v1.QuerierService/SelectSeries"
}

fn native_path() -> &'static str {
    "/api/profiles/v1/querier.v1.QuerierService/SelectSeries"
}

/// One answered request: its body text, checked `200` JSON, and its id.
fn answer(res: &Res, ctx: &str) -> (String, String) {
    assert_eq!(res.status, 200, "{ctx}: {}", res.text());
    assert_eq!(
        res.header("content-type"),
        Some("application/json"),
        "{ctx}"
    );
    (res.text(), res.query_id(ctx))
}

/// One refused request: its status, code and message, and its id.
fn refused(res: &Res, status: u16, code: &str, message: &str, ctx: &str) -> String {
    assert_eq!(res.status, status, "{ctx}: {}", res.text());
    assert_eq!(
        res.json(ctx),
        json!({"code": code, "message": message}),
        "{ctx}"
    );
    res.query_id(ctx)
}

// ---------------------------------------------------------------------
// R1–R8
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn the_time_series_answers_in_one_statement() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_profiles_series_it")).await;
    let client = ChClient::new(live_db::conn_config(db.name()))
        .await
        .expect("connect");
    let now = chrono::Utc::now().timestamp_millis();
    let s = (now - 120 * MINUTE_MS).div_euclid(MINUTE_MS) * MINUTE_MS;
    let e = s + MINUTE_MS;
    land(&client, s).await;
    let body = |selector: &str, extra: Value| -> Value {
        let mut v = json!({
            "profileTypeID": CPU,
            "labelSelector": selector,
            "start": s.to_string(),
            "end": e.to_string(),
            "groupBy": ["pod"],
            "step": 15,
        });
        if let (Some(o), Value::Object(x)) = (v.as_object_mut(), extra) {
            o.extend(x);
        }
        v
    };
    let x = r#"{service_name="x"}"#;
    let r1 = format!(
        r#"{{"series":[{{"labels":[{{"name":"pod","value":"b"}}],"points":[{{"value":6,"timestamp":"{b}"}}]}},{{"points":[{{"value":5,"timestamp":"{c}"}}]}},{{"labels":[{{"name":"pod","value":"a"}}],"points":[{{"value":1,"timestamp":"{s}"}},{{"value":2,"timestamp":"{a}"}}]}}]}}"#,
        a = s + STEP_MS,
        b = s + 2 * STEP_MS,
        c = s + 3 * STEP_MS,
    );
    let r2 = format!(
        r#"{{"series":[{{"labels":[{{"name":"pod","value":"b"}}],"points":[{{"value":3,"timestamp":"{b}"}}]}}]}}"#,
        b = s + 2 * STEP_MS,
    );

    let port_a: u16 = 31_872;
    let port_b: u16 = 31_873;
    let clickhouse_http = live_db::ch_http_port();
    let _a = spawn_ready(
        port_a,
        db.name(),
        clickhouse_http,
        &[("PULSUS_COMPAT_ENDPOINTS", "true")],
    );
    let a = port_a;
    let mut counted: Vec<(String, String, u64)> = Vec::new();

    // R1: the native Connect path, JSON, grouped by `pod`.
    let (got, id) = answer(&connect(a, native_path(), "t1", &body(x, json!({}))), "R1");
    assert_eq!(got, r1, "R1");
    counted.push(("R1".to_string(), id, 1));

    // R2: the compat Connect path, protobuf, `AVERAGE`, `limit` 1.
    let req = SelectSeriesRequest {
        profile_type_id: CPU.to_string(),
        label_selector: r#"{pod="b"}"#.to_string(),
        start: s,
        end: e,
        group_by: vec!["pod".to_string()],
        step: 15.0,
        aggregation: Some(1),
        limit: Some(1),
        ..SelectSeriesRequest::default()
    };
    let res = call(
        a,
        "POST",
        compat_path(),
        &[
            ("Content-Type", "application/proto"),
            ("X-Scope-OrgID", "t1"),
        ],
        &req.encode_to_vec(),
    );
    assert_eq!(res.status, 200, "R2: {}", res.text());
    assert_eq!(res.header("content-type"), Some("application/proto"), "R2");
    let decoded = SelectSeriesResponse::decode(&res.body[..]).expect("R2 decode");
    assert_eq!(
        decoded,
        SelectSeriesResponse {
            series: vec![Series {
                labels: vec![LabelPair {
                    name: "pod".to_string(),
                    value: "b".to_string(),
                }],
                points: vec![Point {
                    value: 3.0,
                    timestamp: s + 2 * STEP_MS,
                    annotations: Vec::new(),
                }],
            }],
        },
        "R2"
    );
    counted.push(("R2".to_string(), res.query_id("R2"), 1));

    // R4: a sixth `delta` part selects the first five; four parts refuse.
    let (got, id) = answer(
        &connect(
            a,
            native_path(),
            "t1",
            &body(x, json!({"profileTypeID": format!("{CPU}:delta")})),
        ),
        "R4 delta",
    );
    assert_eq!(got, r1, "R4 delta");
    counted.push(("R4 delta".to_string(), id, 1));
    let id = refused(
        &connect(
            a,
            native_path(),
            "t1",
            &body(x, json!({"profileTypeID": "a:b:c:d"})),
        ),
        400,
        "invalid_argument",
        "profile-type selection must be of the form \
         <name>:<sample-type>:<sample-unit>:<period-type>:<period-unit>(:delta), \
         got(4): \"a:b:c:d\"",
        "R4 four parts",
    );
    counted.push(("R4 four parts".to_string(), id, 0));

    // R5: a step below 1 ms.
    let id = refused(
        &connect(a, native_path(), "t1", &body(x, json!({"step": 0.0009}))),
        400,
        "invalid_argument",
        "step must be >= 1ms",
        "R5",
    );
    counted.push(("R5".to_string(), id, 0));

    // R6: a window ending before the lookback floor, no step.
    let mut old = body(
        x,
        json!({"start": (now - 9 * DAY_MS).to_string(),
               "end": (now - 8 * DAY_MS).to_string()}),
    );
    old.as_object_mut().expect("an object").remove("step");
    let (got, id) = answer(&connect(a, native_path(), "t1", &old), "R6");
    assert_eq!(got, "{}", "R6");
    counted.push(("R6".to_string(), id, 0));

    // R7: the exemplar types not served, and an unknown one.
    for (ctx, value, status, code, message) in [
        (
            "R7 individual",
            json!("EXEMPLAR_TYPE_INDIVIDUAL"),
            501,
            "unimplemented",
            "exemplars are not served yet",
        ),
        (
            "R7 span",
            json!("EXEMPLAR_TYPE_SPAN"),
            501,
            "unimplemented",
            "exemplar type span is not implemented",
        ),
        (
            "R7 unknown",
            json!(7),
            400,
            "invalid_argument",
            "unknown exemplar type: 7",
        ),
    ] {
        let id = refused(
            &connect(
                a,
                native_path(),
                "t1",
                &body(x, json!({"exemplarType": value})),
            ),
            status,
            code,
            message,
            ctx,
        );
        counted.push((ctx.to_string(), id, 0));
    }

    // R8: the tenant's own entries.
    let (got, id) = answer(
        &connect(a, native_path(), "t2", &body(r#"{pod="a"}"#, json!({}))),
        "R8",
    );
    assert_eq!(
        got,
        format!(
            r#"{{"series":[{{"labels":[{{"name":"pod","value":"a"}}],"points":[{{"value":100,"timestamp":"{s}"}},{{"value":200,"timestamp":"{a}"}}]}}]}}"#,
            a = s + STEP_MS,
        ),
        "R8"
    );
    counted.push(("R8".to_string(), id, 1));

    // R3: spawn B, the compat flag unset: the native `GET` route answers
    // R2's answer as JSON; the compat path is not mounted.
    let _b = spawn_ready(port_b, db.name(), clickhouse_http, &[]);
    let b = port_b;
    let query = encode(&format!(r#"{CPU}{{pod="b"}}"#));
    let group_by = encode("groupBy[]");
    let (got, id) = answer(
        &get_as(
            b,
            &format!(
                "/api/profiles/v1/select_series?query={query}&from={s}&until={e}&step=15\
                 &{group_by}=pod&aggregation=avg&limit=1"
            ),
            "t1",
        ),
        "R3",
    );
    assert_eq!(got, r2, "R3");
    counted.push(("R3".to_string(), id, 1));
    assert_eq!(
        connect(b, compat_path(), "t1", &body(x, json!({}))).status,
        404,
        "R3 compat"
    );

    let ids: Vec<String> = counted.iter().map(|(_, id, _)| id.clone()).collect();
    let sent = statements(&client, &ids).await;
    for (ctx, id, want) in &counted {
        assert_eq!(sent[id], *want, "{ctx}: statements for {id}");
    }
}
