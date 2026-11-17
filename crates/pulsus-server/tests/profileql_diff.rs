//! Issue #827: `Diff` — the diff of two selections — over the Connect
//! protocol, against the `pulsusdb` binary and a live ClickHouse.
//!
//! The fixture is plan §6.2's rows as kind 0, 1 and 2 landing rows: service
//! `left` with `main,a` 3 and service `right` with `main,b` 5 near `T`, and
//! `right` with `main,c` 7 two days back, under `t1` and, at 100× the
//! values, under `t2`. Every answer is compared whole, and every request's
//! statements are counted in `system.query_log` by its `X-Pulsus-Query-Id`.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-server --test profileql_diff
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
    DiffRequest, DiffResponse, FlameGraphDiff, Level, SelectMergeStacktracesRequest,
};
use serde_json::{Value, json};

const CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const MINUTE_MS: i64 = 60_000;
const HOUR_MS: i64 = 3_600_000;
const DAY_MS: i64 = 86_400_000;
const NO_TRACE: &str = "00000000000000000000000000000000";
const PROFILE: &str = "00000000-0000-0000-0000-0000000000a0";
const FP_LEFT: u128 = 301;
const FP_RIGHT: u128 = 302;
const LEFT: &str = r#"{service_name="left"}"#;
const RIGHT: &str = r#"{service_name="right"}"#;

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

/// A sample row (`stack` > 0) or a series row (`stack` 0), received 1 s
/// before its time.
fn row(org: &str, service: &str, fp: u128, ts: i64, stack: u128, value: i64) -> String {
    let (kind, labels) = if stack == 0 {
        (
            2,
            format!(r#"{{"__profile_type__":"{CPU}","service_name":"{service}"}}"#),
        )
    } else {
        (0, String::new())
    };
    format!(
        "('{org}', {}, {kind}, '{CPU}', '{service}', {fp}, {}, {stack}, {value}, 0, \
         unhex('{NO_TRACE}'), [], [], '{labels}', '{PROFILE}')",
        ts - 1_000,
        ts * 1_000_000
    )
}

/// Lands the fixture with merges of the stacks stopped: `main,a` and
/// `main,b` at `T` and again at `D0 + 1 s`, `main,c` once at `U`; then the
/// samples and the series.
async fn land(client: &ChClient, t: i64, u: i64, d0: i64) {
    exec(client, "SYSTEM STOP MERGES profile_stacks").await;
    for received in [t, d0 + 1_000] {
        let mut rows = Vec::new();
        for org in ["t1", "t2"] {
            rows.push(stack_row(org, received, 1, "main,a"));
            rows.push(stack_row(org, received, 2, "main,b"));
        }
        exec(client, &insert(&rows)).await;
    }
    let mut rows = Vec::new();
    for (org, scale) in [("t1", 1), ("t2", 100)] {
        rows.push(stack_row(org, u, 3, "main,c"));
        let at = t + 1_000;
        rows.push(row(org, "left", FP_LEFT, at, 1, 3 * scale));
        rows.push(row(org, "left", FP_LEFT, at, 0, 0));
        rows.push(row(org, "right", FP_RIGHT, at, 2, 5 * scale));
        rows.push(row(org, "right", FP_RIGHT, at, 0, 0));
        let at = u + 1_000;
        rows.push(row(org, "right", FP_RIGHT, at, 3, 7 * scale));
        rows.push(row(org, "right", FP_RIGHT, at, 0, 0));
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

/// Plan §3.1's g1 levels, every value but the name index × `scale`.
fn g1_levels(scale: i64) -> Vec<Vec<i64>> {
    let levels: [&[i64]; 3] = [
        &[0, 3, 0, 0, 5, 0, 0],
        &[0, 3, 0, 0, 5, 0, 1],
        &[0, 3, 3, 0, 0, 0, 3, 0, 0, 0, 0, 5, 5, 2],
    ];
    levels
        .iter()
        .map(|l| {
            l.chunks(7)
                .flat_map(|c| {
                    let mut v: Vec<i64> = c[..6].iter().map(|x| x * scale).collect();
                    v.push(c[6]);
                    v
                })
                .collect()
        })
        .collect()
}

/// The diff's JSON, as plan §3.2 writes it: a field at 0 omitted.
fn diff_json(names: &[&str], levels: &[Vec<i64>], ticks: [i64; 4]) -> Value {
    let levels: Vec<Value> = levels
        .iter()
        .map(|l| json!({"values": l.iter().map(|v| v.to_string()).collect::<Vec<_>>()}))
        .collect();
    let mut v = json!({"names": names, "levels": levels});
    for (key, n) in ["total", "maxSelf", "leftTicks", "rightTicks"]
        .iter()
        .zip(ticks)
    {
        if n != 0 {
            v[*key] = json!(n.to_string());
        }
    }
    json!({ "flamegraph": v })
}

fn g1(scale: i64) -> Value {
    diff_json(
        &["total", "main", "b", "a"],
        &g1_levels(scale),
        [8 * scale, 5 * scale, 3 * scale, 5 * scale],
    )
}

fn g2() -> Value {
    diff_json(
        &["total", "main", "a"],
        &[
            vec![0, 3, 0, 0, 0, 0, 0],
            vec![0, 3, 0, 0, 0, 0, 1],
            vec![0, 3, 3, 0, 0, 0, 2],
        ],
        [3, 3, 3, 0],
    )
}

fn g3() -> Value {
    diff_json(&["total"], &[vec![0, 0, 0, 0, 0, 0, 0]], [0, 0, 0, 0])
}

fn compat_path() -> &'static str {
    "/querier.v1.QuerierService/Diff"
}

fn native_path() -> &'static str {
    "/api/profiles/v1/querier.v1.QuerierService/Diff"
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

/// `query_plan_join_swap_table` as the request's statement carried it.
async fn swap_setting(client: &ChClient, id: &str) -> Vec<String> {
    exec(client, "SYSTEM FLUSH LOGS").await;
    let sql = format!(
        "SELECT Settings['query_plan_join_swap_table'] AS s FROM system.query_log \
         WHERE type = 'QueryFinish' AND is_initial_query AND log_comment = {}",
        sql_string(id)
    );
    client
        .query_strings(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"))
}

// ---------------------------------------------------------------------
// R1–R9
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn the_diff_answers_both_sides_in_one_statement() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_profileql_diff_it")).await;
    let client = ChClient::new(live_db::conn_config(db.name()))
        .await
        .expect("connect");
    let now = chrono::Utc::now().timestamp_millis();
    let d0 = now.div_euclid(DAY_MS) * DAY_MS;
    let t = d0 - 30 * MINUTE_MS;
    let u = d0 - 2 * DAY_MS + HOUR_MS;
    land(&client, t, u, d0).await;
    let (start, end) = (t - 30 * MINUTE_MS, t + 29 * MINUTE_MS);
    let side = |selector: &str, extra: Value| -> Value {
        let mut v = json!({
            "profileTypeID": CPU,
            "labelSelector": selector,
            "start": start.to_string(),
            "end": end.to_string(),
        });
        if let (Some(o), Value::Object(e)) = (v.as_object_mut(), extra) {
            o.extend(e);
        }
        v
    };
    let body = |left: Value, right: Value| json!({"left": left, "right": right});
    let v1 = || body(side(LEFT, json!({})), side(RIGHT, json!({})));
    let window = |s: i64, e: i64| json!({"start": s.to_string(), "end": e.to_string()});
    let old = window(now - 8 * DAY_MS - HOUR_MS, now - 8 * DAY_MS);

    let port_a: u16 = 31_876;
    let port_b: u16 = 31_877;
    let clickhouse_http = live_db::ch_http_port();
    let _a = spawn_ready(
        port_a,
        db.name(),
        clickhouse_http,
        &[("PULSUS_COMPAT_ENDPOINTS", "true")],
    );
    let a = port_a;
    let mut counted: Vec<(String, String, u64)> = Vec::new();

    // R1: the native Connect path, JSON, no `maxNodes`.
    let (got, r1_id) = answer(&connect(a, native_path(), "t1", &v1()), "R1");
    assert_eq!(got, g1(1), "R1");
    counted.push(("R1".to_string(), r1_id.clone(), 1));

    // R2: the compat Connect path, protobuf.
    let req = |selector: &str| SelectMergeStacktracesRequest {
        profile_type_id: CPU.to_string(),
        label_selector: selector.to_string(),
        start,
        end,
        ..SelectMergeStacktracesRequest::default()
    };
    let msg = DiffRequest {
        left: Some(req(LEFT)),
        right: Some(req(RIGHT)),
    };
    let res = call(
        a,
        "POST",
        compat_path(),
        &[
            ("Content-Type", "application/proto"),
            ("X-Scope-OrgID", "t1"),
        ],
        &msg.encode_to_vec(),
    );
    assert_eq!(res.status, 200, "R2: {}", res.text());
    assert_eq!(res.header("content-type"), Some("application/proto"), "R2");
    let decoded = DiffResponse::decode(&res.body[..]).map_err(|e| e.to_string());
    assert_eq!(
        decoded.map(|d| d.flamegraph),
        Ok(Some(FlameGraphDiff {
            names: ["total", "main", "b", "a"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            levels: g1_levels(1)
                .into_iter()
                .map(|values| Level { values })
                .collect(),
            total: 8,
            max_self: 5,
            left_ticks: 3,
            right_ticks: 5,
        })),
        "R2"
    );
    counted.push(("R2".to_string(), res.query_id("R2"), 1));

    // R3: a right window with no data, then one before the lookback floor.
    let (got, id) = answer(
        &connect(
            a,
            native_path(),
            "t1",
            &body(
                side(LEFT, json!({})),
                side(RIGHT, window(t - 6 * HOUR_MS, t - 5 * HOUR_MS)),
            ),
        ),
        "R3 no data",
    );
    assert_eq!(got, g2(), "R3 no data");
    counted.push(("R3 no data".to_string(), id, 1));
    let (got, id) = answer(
        &connect(
            a,
            native_path(),
            "t1",
            &body(side(LEFT, json!({})), side(RIGHT, old.clone())),
        ),
        "R3 old",
    );
    assert_eq!(got, g2(), "R3 old");
    counted.push(("R3 old".to_string(), id, 1));

    // R4: both windows before the lookback floor.
    let (got, id) = answer(
        &connect(
            a,
            native_path(),
            "t1",
            &body(side(LEFT, old.clone()), side(RIGHT, old.clone())),
        ),
        "R4",
    );
    assert_eq!(got, g3(), "R4");
    counted.push(("R4".to_string(), id, 0));

    // R6: a sixth `delta` part selects the first five; four parts refuse.
    let (got, id) = answer(
        &connect(
            a,
            native_path(),
            "t1",
            &body(
                side(LEFT, json!({"profileTypeID": format!("{CPU}:delta")})),
                side(RIGHT, json!({})),
            ),
        ),
        "R6 delta",
    );
    assert_eq!(got, g1(1), "R6 delta");
    counted.push(("R6 delta".to_string(), id, 1));
    let id = refused(
        &connect(
            a,
            native_path(),
            "t1",
            &body(
                side(LEFT, json!({})),
                side(RIGHT, json!({"profileTypeID": "a:b:c:d"})),
            ),
        ),
        400,
        "invalid_argument",
        "profile-type selection must be of the form \
         <name>:<sample-type>:<sample-unit>:<period-type>:<period-unit>(:delta), \
         got(4): \"a:b:c:d\"",
        "R6 four parts",
    );
    counted.push(("R6 four parts".to_string(), id, 0));

    // R7: the right `maxNodes` above the largest allowed.
    let id = refused(
        &connect(
            a,
            native_path(),
            "t1",
            &body(
                side(LEFT, json!({})),
                side(RIGHT, json!({"maxNodes": "1048577"})),
            ),
        ),
        400,
        "invalid_argument",
        "max flamegraph nodes limit 1048577 is greater than allowed 1048576",
        "R7",
    );
    counted.push(("R7".to_string(), id, 0));

    // R8: a span and a trace selector together on the left.
    let id = refused(
        &connect(
            a,
            native_path(),
            "t1",
            &body(
                side(
                    LEFT,
                    json!({"spanSelector": ["0000000000000001"],
                           "traceIdSelector": ["0102030405060708090a0b0c0d0e0f10"]}),
                ),
                side(RIGHT, json!({})),
            ),
        ),
        400,
        "invalid_argument",
        "span_selector and trace_id_selector cannot be combined",
        "R8",
    );
    counted.push(("R8".to_string(), id, 0));

    // R9: the tenant's own samples.
    let (got, id) = answer(&connect(a, native_path(), "t2", &v1()), "R9");
    assert_eq!(got, g1(100), "R9");
    counted.push(("R9".to_string(), id, 1));

    // R5: spawn B, the compat flag unset: the native path answers R1's
    // body; the compat path is not mounted.
    let _b = spawn_ready(port_b, db.name(), clickhouse_http, &[]);
    let b = port_b;
    let (got, id) = answer(&connect(b, native_path(), "t1", &v1()), "R5");
    assert_eq!(got, g1(1), "R5");
    counted.push(("R5".to_string(), id, 1));
    assert_eq!(
        connect(b, compat_path(), "t1", &v1()).status,
        404,
        "R5 compat"
    );

    let ids: Vec<String> = counted.iter().map(|(_, id, _)| id.clone()).collect();
    let sent = statements(&client, &ids).await;
    for (ctx, id, want) in &counted {
        assert_eq!(sent[id], *want, "{ctx}: statements for {id}");
    }
    // R1: the statement keeps the join's build side on the aggregated
    // samples.
    assert_eq!(
        swap_setting(&client, &r1_id).await,
        ["false"],
        "R1 settings"
    );
}
