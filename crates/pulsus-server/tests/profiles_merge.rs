//! Issue #823: `SelectMergeStacktraces` — the merged flame graph — over the
//! Connect protocol and `GET /api/profiles/v1/merge`, against the
//! `pulsusdb` binary and a live ClickHouse.
//!
//! The fixture is plan §6.2's `svc` rows as kind 0, 1 and 2 landing rows,
//! each stack stored three times, under `t1` and, at 100× the values, under
//! `t2`. Every answer is compared whole, and every request's statements are
//! counted in `system.query_log` by its `X-Pulsus-Query-Id`.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-server --test profiles_merge
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
use pulsus_profiles_wire::querier_v1::{
    FlameGraph, Level, SelectMergeStacktracesRequest, SelectMergeStacktracesResponse,
};
use serde_json::{Value, json};

const CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const MINUTE_MS: i64 = 60_000;
const DAY_MS: i64 = 86_400_000;
const NO_TRACE: &str = "00000000000000000000000000000000";
const PROFILE: &str = "00000000-0000-0000-0000-0000000000a0";
const FP: u128 = 101;

/// `svc`'s stacks (low-level §7.2): id, frames, and its samples as
/// `(value, ms after T)`.
/// One stack's id, frames and `(value, ms after T)` samples.
type Stack = (u128, &'static str, &'static [(i64, i64)]);

const STACKS: [Stack; 6] = [
    (1, "main,a", &[(2, 1_000)]),
    (2, "main,a,b", &[(10, 1_000), (7, 2_000)]),
    (3, "main,a,c", &[(5, 1_000)]),
    (4, "main,d", &[(3, 1_000)]),
    (5, "main,e,f", &[(1, 1_000)]),
    (6, "main,other,x", &[(4, 1_000)]),
];

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

// ---------------------------------------------------------------------
// The fixture
// ---------------------------------------------------------------------

/// A node id: a hash of the path from the root.
fn node_id(path: &[&str]) -> u128 {
    let mut h: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
    for name in path {
        for b in name.bytes().chain([0xff]) {
            h ^= u128::from(b);
            h = h.wrapping_mul(0x0000_0000_0100_0000_0000_0000_0000_013b);
        }
    }
    h
}

fn list<T: std::fmt::Display>(v: impl Iterator<Item = T>) -> String {
    format!(
        "[{}]",
        v.map(|x| x.to_string()).collect::<Vec<_>>().join(", ")
    )
}

fn insert(rows: &[String]) -> String {
    format!(
        "INSERT INTO profile_landing \
         (org_id, received_ms, kind, type_id, service, fingerprint, timestamp_ns, stack_id, \
          value, span_id, trace_id, node_ids, names, labels, profile_id) VALUES {}",
        rows.join(", ")
    )
}

/// Lands the fixture: each stack three times (two at `T`, one at
/// `D0 + 1 s`) with merges stopped, then the samples and the series.
async fn land(client: &ChClient, t: i64, d0: i64) {
    exec(client, "SYSTEM STOP MERGES profile_stacks").await;
    for received in [t, t, d0 + 1_000] {
        let mut rows = Vec::new();
        for org in ["t1", "t2"] {
            for (id, frames, _) in STACKS {
                let names: Vec<&str> = frames.split(',').collect();
                rows.push(format!(
                    "('{org}', {received}, 1, '', '', 0, 0, {id}, 0, 0, unhex('{NO_TRACE}'), \
                     {}, {}, '', '{PROFILE}')",
                    list((1..=names.len()).map(|n| node_id(&names[..n]))),
                    list(names.iter().map(|n| format!("'{n}'")))
                ));
            }
        }
        exec(client, &insert(&rows)).await;
    }
    let mut rows = Vec::new();
    for (org, scale) in [("t1", 1), ("t2", 100)] {
        let labels = format!(r#"{{"__profile_type__":"{CPU}","service_name":"svc"}}"#);
        rows.push(format!(
            "('{org}', {t}, 2, '{CPU}', 'svc', {FP}, {}, 0, 0, 0, unhex('{NO_TRACE}'), [], [], \
             '{labels}', '{PROFILE}')",
            (t + 1_000) * 1_000_000
        ));
        for (id, _, samples) in STACKS {
            for (value, after) in samples {
                rows.push(format!(
                    "('{org}', {t}, 0, '{CPU}', 'svc', {FP}, {}, {id}, {}, 0, \
                     unhex('{NO_TRACE}'), [], [], '', '{PROFILE}')",
                    (t + after) * 1_000_000,
                    value * scale
                ));
            }
        }
    }
    exec(client, &insert(&rows)).await;
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

const NAMES: [&str; 9] = ["total", "main", "other", "e", "f", "d", "a", "c", "b"];

/// Plan §3.1's first row, every value × `scale`: `[x, total, self]` scale,
/// the name index does not.
fn row1_levels(scale: i64) -> Vec<Vec<i64>> {
    let levels: [&[i64]; 4] = [
        &[0, 32, 0, 0],
        &[0, 32, 0, 1],
        &[0, 24, 2, 6, 0, 3, 3, 5, 0, 1, 0, 3, 0, 4, 4, 2],
        &[2, 17, 17, 8, 0, 5, 5, 7, 3, 1, 1, 4],
    ];
    levels
        .iter()
        .map(|l| {
            l.chunks(4)
                .flat_map(|c| [c[0] * scale, c[1] * scale, c[2] * scale, c[3]])
                .collect()
        })
        .collect()
}

/// The flame graph's JSON, as plan §3.2 writes it.
fn flame_json(names: &[&str], levels: &[Vec<i64>], total: i64, max_self: i64) -> Value {
    let levels: Vec<Value> = levels
        .iter()
        .map(|l| json!({"values": l.iter().map(|v| v.to_string()).collect::<Vec<_>>()}))
        .collect();
    let mut v = json!({"names": names, "levels": levels});
    if total != 0 {
        v["total"] = json!(total.to_string());
    }
    if max_self != 0 {
        v["maxSelf"] = json!(max_self.to_string());
    }
    v
}

fn row1(scale: i64) -> Value {
    flame_json(&NAMES, &row1_levels(scale), 32 * scale, 17 * scale)
}

fn empty() -> Value {
    flame_json(&["total"], &[vec![0, 0, 0, 0]], 0, 0)
}

fn compat_path() -> &'static str {
    "/querier.v1.QuerierService/SelectMergeStacktraces"
}

fn native_path() -> &'static str {
    "/api/profiles/v1/querier.v1.QuerierService/SelectMergeStacktraces"
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

/// A Connect answer's `flamegraph`, its `tree` checked present and not
/// empty: the trimmed tree's bytes, base64.
fn with_tree(got: &Value, ctx: &str) -> Value {
    let tree = got["tree"].as_str().unwrap_or("");
    assert!(!tree.is_empty(), "{ctx}: no tree bytes in {got}");
    let keys: Vec<&String> = got
        .as_object()
        .map(|o| o.keys().collect())
        .unwrap_or_default();
    assert_eq!(keys, ["flamegraph", "tree"], "{ctx}: {got}");
    got["flamegraph"].clone()
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
// M1–M9
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn the_merged_flame_graph_answers_in_one_statement() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_profiles_merge_it")).await;
    let client = ChClient::new(live_db::conn_config(db.name()))
        .await
        .expect("connect");
    let now = chrono::Utc::now().timestamp_millis();
    let d0 = now.div_euclid(DAY_MS) * DAY_MS;
    let t = d0 - 30 * MINUTE_MS;
    land(&client, t, d0).await;
    let (start, end) = (t - 30 * MINUTE_MS, t + 29 * MINUTE_MS);
    let body = |extra: Value| -> Value {
        let mut v = json!({
            "profileTypeID": CPU,
            "labelSelector": r#"{service_name="svc"}"#,
            "start": start.to_string(),
            "end": end.to_string(),
        });
        if let (Some(o), Value::Object(e)) = (v.as_object_mut(), extra) {
            o.extend(e);
        }
        v
    };

    let port_a: u16 = 31_870;
    let port_b: u16 = 31_871;
    let clickhouse_http = live_db::ch_http_port();
    let _a = spawn_ready(
        port_a,
        db.name(),
        clickhouse_http,
        &[("PULSUS_COMPAT_ENDPOINTS", "true")],
    );
    let a = port_a;
    let mut counted: Vec<(String, String, u64)> = Vec::new();

    // M1: the native Connect path, JSON, no `maxNodes`.
    let (got, id) = answer(&connect(a, native_path(), "t1", &body(json!({}))), "M1");
    assert_eq!(with_tree(&got, "M1"), row1(1), "M1");
    counted.push(("M1".to_string(), id, 1));

    // M2: the compat Connect path, protobuf, `maxNodes` 4.
    let req = SelectMergeStacktracesRequest {
        profile_type_id: CPU.to_string(),
        label_selector: r#"{service_name="svc"}"#.to_string(),
        start,
        end,
        max_nodes: Some(4),
        ..SelectMergeStacktracesRequest::default()
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
    assert_eq!(res.status, 200, "M2: {}", res.text());
    assert_eq!(res.header("content-type"), Some("application/proto"), "M2");
    let decoded = SelectMergeStacktracesResponse::decode(&res.body[..]).expect("M2 decode");
    let level = |v: &[i64]| Level { values: v.to_vec() };
    assert_eq!(
        decoded.flamegraph,
        Some(FlameGraph {
            names: ["total", "main", "other", "a", "b"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            levels: vec![
                level(&[0, 32, 0, 0]),
                level(&[0, 32, 0, 1]),
                level(&[0, 24, 2, 3, 0, 8, 8, 2]),
                level(&[2, 17, 17, 4, 0, 5, 5, 2]),
            ],
            total: 32,
            max_self: 17,
        }),
        "M2"
    );
    counted.push(("M2".to_string(), res.query_id("M2"), 1));

    // M4: a sixth `delta` part selects the first five; four parts refuse.
    let (got, id) = answer(
        &connect(
            a,
            native_path(),
            "t1",
            &body(json!({"profileTypeID": format!("{CPU}:delta")})),
        ),
        "M4 delta",
    );
    assert_eq!(with_tree(&got, "M4 delta"), row1(1), "M4 delta");
    counted.push(("M4 delta".to_string(), id, 1));
    let id = refused(
        &connect(
            a,
            native_path(),
            "t1",
            &body(json!({"profileTypeID": "a:b:c:d"})),
        ),
        400,
        "invalid_argument",
        "profile-type selection must be of the form \
         <name>:<sample-type>:<sample-unit>:<period-type>:<period-unit>(:delta), \
         got(4): \"a:b:c:d\"",
        "M4 four parts",
    );
    counted.push(("M4 four parts".to_string(), id, 0));

    // M5: a span and a trace selector together.
    let id = refused(
        &connect(
            a,
            native_path(),
            "t1",
            &body(json!({"spanSelector": ["0000000000000001"],
                         "traceIdSelector": ["0102030405060708090a0b0c0d0e0f10"]})),
        ),
        400,
        "invalid_argument",
        "span_selector and trace_id_selector cannot be combined",
        "M5",
    );
    counted.push(("M5".to_string(), id, 0));

    // M6: `maxNodes` above the largest allowed.
    let id = refused(
        &connect(
            a,
            native_path(),
            "t1",
            &body(json!({"maxNodes": "1048577"})),
        ),
        400,
        "invalid_argument",
        "max flamegraph nodes limit 1048577 is greater than allowed 1048576",
        "M6",
    );
    counted.push(("M6".to_string(), id, 0));

    // M7: a window ending before the lookback floor.
    let (got, id) = answer(
        &connect(
            a,
            native_path(),
            "t1",
            &body(json!({"start": (now - 9 * DAY_MS).to_string(),
                         "end": (now - 8 * DAY_MS).to_string()})),
        ),
        "M7",
    );
    assert_eq!(got, json!({"flamegraph": empty()}), "M7");
    counted.push(("M7".to_string(), id, 0));

    // M9: the tenant's own samples.
    let (got, id) = answer(&connect(a, native_path(), "t2", &body(json!({}))), "M9");
    assert_eq!(with_tree(&got, "M9"), row1(100), "M9");
    counted.push(("M9".to_string(), id, 1));

    // M3: spawn B, the compat flag unset: the native `GET` route answers
    // M1's inner object; the compat path is not mounted.
    let _b = spawn_ready(port_b, db.name(), clickhouse_http, &[]);
    let b = port_b;
    let query = encode(&format!(r#"{CPU}{{service_name="svc"}}"#));
    let (got, id) = answer(
        &get_as(
            b,
            &format!("/api/profiles/v1/merge?query={query}&from={start}&until={end}"),
            "t1",
        ),
        "M3",
    );
    assert_eq!(got, row1(1), "M3");
    counted.push(("M3".to_string(), id, 1));
    assert_eq!(
        connect(b, compat_path(), "t1", &body(json!({}))).status,
        404,
        "M3 compat"
    );

    let ids: Vec<String> = counted.iter().map(|(_, id, _)| id.clone()).collect();
    let sent = statements(&client, &ids).await;
    for (ctx, id, want) in &counted {
        assert_eq!(sent[id], *want, "{ctx}: statements for {id}");
    }
}
