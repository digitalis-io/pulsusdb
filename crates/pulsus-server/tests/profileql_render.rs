//! Issue #829: the render routes — `render`, `render-diff` and
//! `label-values` — and `SelectMergeStacktraces` with
//! `PROFILE_FORMAT_TREE`, against the `pulsusdb` binary and a live
//! ClickHouse.
//!
//! The fixture is plan §6.2's rows as kind 0–3 landing rows: service `svc`,
//! series 401 (`pod="p1"`) with `main;a` 3 and series 402 (`pod="p2"`) with
//! `main;b` 5, one profile each at `T`, under `t1` and, at 100× the values,
//! under `t2`. Every answer is compared whole, and every request's
//! statements are counted in `system.query_log` by its
//! `X-Pulsus-Query-Id`.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-server --test profileql_render
//! ```

#[path = "support/live_db.rs"]
mod live_db;

use live_db::ScopedDb;

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use pulsus_clickhouse::{ChClient, Idempotency, QuerySettings};
use pulsus_profiles_wire::querier_v1::SelectMergeStacktracesResponse;
use serde_json::{Value, json};

const CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const MINUTE_MS: i64 = 60_000;
const STEP_MS: i64 = 15_000;
const DAY_MS: i64 = 86_400_000;
const NO_TRACE: &str = "00000000000000000000000000000000";
const PROFILE: &str = "00000000-0000-0000-0000-0000000000a0";
const SELECTOR: &str = r#"{service_name="svc"}"#;

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

fn stack_row(org: &str, received: i64, id: u128, frames: &str) -> String {
    let names: Vec<&str> = frames.split(',').collect();
    format!(
        "('{org}', {received}, 1, '', '', 0, 0, {id}, 0, 0, unhex('{NO_TRACE}'), {}, {}, '', \
         '{PROFILE}')",
        list((1..=names.len()).map(|n| node_id(&names[..n]))),
        list(names.iter().map(|n| format!("'{n}'")))
    )
}

/// One profile of `svc`'s series `fp` (`pod`) at `t`, received 1 s before:
/// its series row, its one sample on `stack`, and its entry, whose total is
/// the sample's value.
fn profile(org: &str, fp: u128, pod: &str, t: i64, stack: u128, value: i64) -> Vec<String> {
    let received = t - 1_000;
    let ns = t * 1_000_000;
    let labels = format!(r#"{{"__profile_type__":"{CPU}","pod":"{pod}","service_name":"svc"}}"#);
    let row = |kind: u8, stack: u128, value: i64, labels: &str| {
        format!(
            "('{org}', {received}, {kind}, '{CPU}', 'svc', {fp}, {ns}, {stack}, {value}, 0, \
             unhex('{NO_TRACE}'), [], [], '{labels}', '{PROFILE}')"
        )
    };
    vec![
        row(2, 0, 0, &labels),
        row(0, stack, value, ""),
        row(3, 0, value, ""),
    ]
}

/// Plan §6.2's fixture under `t1` and, at 100×, `t2`.
async fn land(client: &ChClient, t: i64) {
    let mut rows = Vec::new();
    for (org, scale) in [("t1", 1), ("t2", 100)] {
        rows.push(stack_row(org, t, 1, "main,a"));
        rows.push(stack_row(org, t, 2, "main,b"));
        rows.extend(profile(org, 401, "p1", t, 1, 3 * scale));
        rows.extend(profile(org, 402, "p2", t, 2, 5 * scale));
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

fn encode_query(v: &str) -> String {
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

/// A `GET` of `path` with `params`, as `tenant`.
fn get(port: u16, path: &str, tenant: &str, params: &[(&str, String)]) -> Res {
    let query: Vec<String> = params
        .iter()
        .map(|(k, v)| format!("{}={}", encode_query(k), encode_query(v)))
        .collect();
    let target = if query.is_empty() {
        path.to_string()
    } else {
        format!("{path}?{}", query.join("&"))
    };
    call(port, "GET", &target, &[("X-Scope-OrgID", tenant)], &[])
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

/// 240 samples of 15 s, `v` at `at` and 0 elsewhere.
fn samples(at: Option<(usize, u64)>) -> String {
    let mut v = vec![0u64; 240];
    if let Some((i, x)) = at {
        v[i] = x;
    }
    v.iter().map(u64::to_string).collect::<Vec<_>>().join(",")
}

fn timeline(start_s: i64, at: Option<(usize, u64)>) -> String {
    format!(
        r#"{{"startTime":{start_s},"samples":[{}],"durationDelta":15,"watermarks":null}}"#,
        samples(at)
    )
}

const METADATA: &str = r#""metadata":{"format":"single","spyName":"","sampleRate":1000000000,"units":"samples","name":"cpu"}"#;

/// Plan §0's body, every value × `scale`; `groups` as given.
fn b1(start_s: i64, scale: i64, timeline_at: Option<(usize, u64)>, groups: &str) -> String {
    let (a, b, t) = (3 * scale, 5 * scale, 8 * scale);
    format!(
        r#"{{"version":1,"flamebearer":{{"names":["total","main","b","a"],"levels":[[0,{t},0,0],[0,{t},0,1],[0,{a},{a},3,0,{b},{b},2]],"numTicks":{t},"maxSelf":{b}}},{METADATA},"timeline":{},"groups":{groups},"heatmap":null}}"#,
        timeline(start_s, timeline_at)
    ) + "\n"
}

/// Plan §6.2's `B2`.
const B2: &str = concat!(
    r#"{"version":1,"flamebearer":{"names":["total","main","b","a"],"levels":[[0,8,0,0,8,0,0],[0,8,0,0,8,0,1],[0,3,3,0,3,3,3,0,5,5,0,5,5,2]],"numTicks":16,"maxSelf":5},"metadata":{"format":"double","spyName":"","sampleRate":1000000000,"units":"samples","name":"cpu"},"timeline":null,"groups":null,"heatmap":null,"leftTicks":8,"rightTicks":8}"#,
    "\n"
);

/// One answered request: its body, checked `200` and `application/json`,
/// and its id.
fn answer(res: &Res, ctx: &str) -> (String, String) {
    assert_eq!(res.status, 200, "{ctx}: {}", res.text());
    assert_eq!(
        res.header("content-type"),
        Some("application/json"),
        "{ctx}"
    );
    (res.text(), res.query_id(ctx))
}

/// One refused request: its status and code, and its id.
fn refused(res: &Res, status: u16, code: &str, ctx: &str) -> String {
    assert_eq!(res.status, status, "{ctx}: {}", res.text());
    assert_eq!(res.json(ctx)["code"], code, "{ctx}: {}", res.text());
    res.query_id(ctx)
}

/// The tree bytes read back: per node in the order written, its name, its
/// self and its child count.
fn decode_tree(mut b: &[u8]) -> Vec<(String, i64, u64)> {
    fn uv(b: &mut &[u8]) -> u64 {
        let (mut v, mut shift) = (0u64, 0);
        loop {
            let byte = b[0];
            *b = &b[1..];
            v |= u64::from(byte & 0x7f) << shift;
            if byte < 0x80 {
                return v;
            }
            shift += 7;
        }
    }
    let mut out = Vec::new();
    while !b.is_empty() {
        let len = uv(&mut b) as usize;
        let name = String::from_utf8(b[..len].to_vec()).expect("a UTF-8 name");
        b = &b[len..];
        let self_value = uv(&mut b) as i64;
        let children = uv(&mut b);
        out.push((name, self_value, children));
    }
    out
}

// ---------------------------------------------------------------------
// L1–L12
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn the_render_routes_answer_in_one_statement() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_profileql_render_it")).await;
    let client = ChClient::new(live_db::conn_config(db.name()))
        .await
        .expect("connect");
    let now = chrono::Utc::now().timestamp_millis();
    let t = (now - 10 * MINUTE_MS).div_euclid(STEP_MS) * STEP_MS;
    land(&client, t).await;
    let (from, until) = (t - 30 * MINUTE_MS, t + 30 * MINUTE_MS);
    let start_s = from / 1000;
    let query = format!("{CPU}{SELECTOR}");
    let w = |q: &str| -> Vec<(&'static str, String)> {
        vec![
            ("query", q.to_string()),
            ("from", from.to_string()),
            ("until", until.to_string()),
        ]
    };
    let with = |mut v: Vec<(&'static str, String)>, k: &'static str, x: &str| {
        v.push((k, x.to_string()));
        v
    };
    let render = "/api/profiles/v1/render";
    let render_diff = "/api/profiles/v1/render-diff";
    let label_values = "/api/profiles/v1/label-values";
    let one = Some((120, 8));

    let port_a: u16 = 31_878;
    let port_b: u16 = 31_879;
    let clickhouse_http = live_db::ch_http_port();
    let _a = spawn_ready(
        port_a,
        db.name(),
        clickhouse_http,
        &[("PULSUS_COMPAT_ENDPOINTS", "true")],
    );
    let a = port_a;
    let mut counted: Vec<(&str, String, u64)> = Vec::new();

    // L1: plan §0's request and body.
    let (got, id) = answer(&get(a, render, "t1", &w(&query)), "L1");
    let l1 = b1(start_s, 1, one, "null");
    assert_eq!(got, l1, "L1");
    counted.push(("L1", id, 1));

    // L2: the tenant's own samples.
    let (got, id) = answer(&get(a, render, "t2", &w(&query)), "L2");
    assert_eq!(got, b1(start_s, 100, Some((120, 800)), "null"), "L2");
    counted.push(("L2", id, 1));

    // L3: grouped by pod, two series: the timeline empty, each pod's group.
    let (got, id) = answer(
        &get(a, render, "t1", &with(w(&query), "groupBy", "pod")),
        "L3",
    );
    let groups = format!(
        r#"{{"p1":{},"p2":{}}}"#,
        timeline(start_s, Some((120, 3))),
        timeline(start_s, Some((120, 5)))
    );
    assert_eq!(got, b1(start_s, 1, None, &groups), "L3");
    counted.push(("L3", id, 1));

    // L4: a sixth part of the type is ignored.
    let (got, id) = answer(
        &get(a, render, "t1", &w(&format!("{CPU}:x{SELECTOR}"))),
        "L4",
    );
    assert_eq!(got, l1, "L4");
    counted.push(("L4", id, 1));

    // L5: a type of three parts, then no `query`.
    let id = refused(
        &get(
            a,
            render,
            "t1",
            &w(&format!("process_cpu:cpu:nanoseconds{SELECTOR}")),
        ),
        400,
        "invalid_argument",
        "L5 type",
    );
    counted.push(("L5 type", id, 0));
    let id = refused(
        &get(
            a,
            render,
            "t1",
            &[("from", from.to_string()), ("until", until.to_string())],
        ),
        400,
        "invalid_argument",
        "L5 no query",
    );
    counted.push(("L5 no query", id, 0));

    // L6: a window before the lookback floor: the empty flame graph and an
    // empty timeline, no statement.
    let old = (now - 8 * DAY_MS).to_string();
    let (got, id) = answer(
        &get(
            a,
            render,
            "t1",
            &[
                ("query", query.clone()),
                ("from", old.clone()),
                ("until", old),
            ],
        ),
        "L6",
    );
    let v: Value = serde_json::from_str(&got).expect("L6 JSON");
    assert_eq!(
        v["flamebearer"],
        json!({"names": ["total"], "levels": [[0, 0, 0, 0]], "numTicks": 0, "maxSelf": 0}),
        "L6"
    );
    assert_eq!(v["timeline"]["samples"], json!([]), "L6");
    counted.push(("L6", id, 0));

    // L7: the diff of a selection with itself.
    let diff_params = vec![
        ("leftQuery", query.clone()),
        ("leftFrom", from.to_string()),
        ("leftUntil", until.to_string()),
        ("rightQuery", query.clone()),
        ("rightFrom", from.to_string()),
        ("rightUntil", until.to_string()),
    ];
    let (got, id) = answer(&get(a, render_diff, "t1", &diff_params), "L7");
    assert_eq!(got, B2, "L7");
    counted.push(("L7", id, 1));

    // L8: the values of the label asked for.
    let (got, id) = answer(
        &get(a, label_values, "t1", &[("label", "pod".to_string())]),
        "L8",
    );
    assert_eq!(got, "[\"p1\",\"p2\"]\n", "L8");
    counted.push(("L8", id, 1));

    // L9: `__name__` is the profile types; no `label` is refused.
    let (got, id) = answer(
        &get(a, label_values, "t1", &[("label", "__name__".to_string())]),
        "L9",
    );
    assert_eq!(got, format!("[\"{CPU}\"]\n"), "L9");
    counted.push(("L9", id, 1));
    let id = refused(
        &get(a, label_values, "t1", &[]),
        400,
        "invalid_argument",
        "L9 no label",
    );
    counted.push(("L9 no label", id, 0));

    // L10: `PROFILE_FORMAT_TREE` answers the tree alone.
    let res = call(
        a,
        "POST",
        "/api/profiles/v1/querier.v1.QuerierService/SelectMergeStacktraces",
        &[
            ("Content-Type", "application/json"),
            ("X-Scope-OrgID", "t1"),
        ],
        json!({
            "profileTypeID": CPU,
            "labelSelector": SELECTOR,
            "start": from.to_string(),
            "end": until.to_string(),
            "format": "PROFILE_FORMAT_TREE",
        })
        .to_string()
        .as_bytes(),
    );
    let (got, id) = answer(&res, "L10");
    let keys: Vec<String> = serde_json::from_str::<Value>(&got)
        .expect("L10 JSON")
        .as_object()
        .expect("an object")
        .keys()
        .cloned()
        .collect();
    assert_eq!(keys, ["tree"], "L10: {got}");
    let tree = serde_json::from_str::<SelectMergeStacktracesResponse>(&got)
        .expect("L10 message")
        .tree;
    let n = |name: &str, s: i64, c: u64| (name.to_string(), s, c);
    assert_eq!(
        decode_tree(&tree),
        [n("", 0, 1), n("main", 0, 2), n("b", 5, 0), n("a", 3, 0)],
        "L10"
    );
    counted.push(("L10", id, 1));

    // L11: `format=dot` is not served yet.
    let id = refused(
        &get(a, render, "t1", &with(w(&query), "format", "dot")),
        501,
        "unimplemented",
        "L11",
    );
    counted.push(("L11", id, 0));

    // L12: the compat path answers L1's body; with the compat flag unset
    // the native routes answer and the compat path is not mounted.
    let (got, id) = answer(&get(a, "/pyroscope/render", "t1", &w(&query)), "L12 compat");
    assert_eq!(got, l1, "L12 compat");
    counted.push(("L12 compat", id, 1));
    let _b = spawn_ready(port_b, db.name(), clickhouse_http, &[]);
    let b = port_b;
    let (got, id) = answer(&get(b, render, "t1", &w(&query)), "L12 render");
    assert_eq!(got, l1, "L12 render");
    counted.push(("L12 render", id, 1));
    let (got, id) = answer(&get(b, render_diff, "t1", &diff_params), "L12 render-diff");
    assert_eq!(got, B2, "L12 render-diff");
    counted.push(("L12 render-diff", id, 1));
    let (got, id) = answer(
        &get(b, label_values, "t1", &[("label", "pod".to_string())]),
        "L12 label-values",
    );
    assert_eq!(got, "[\"p1\",\"p2\"]\n", "L12 label-values");
    counted.push(("L12 label-values", id, 1));
    assert_eq!(
        get(b, "/pyroscope/render", "t1", &w(&query)).status,
        404,
        "L12 compat off"
    );

    let ids: Vec<String> = counted.iter().map(|(_, id, _)| id.clone()).collect();
    let sent = statements(&client, &ids).await;
    for (ctx, id, want) in &counted {
        assert_eq!(sent[id], *want, "{ctx}: statements for {id}");
    }
}
