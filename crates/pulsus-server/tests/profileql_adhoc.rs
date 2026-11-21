//! Issue #831: ad hoc profile uploads — `adhocprofiles.v1.AdHocProfileService`
//! `Upload`, `Get`, `List` and `Diff` — against the `pulsusdb` binary and a
//! live ClickHouse (plan §7.2, L1–L12). Every request's statements are
//! counted in `system.query_log` by its `X-Pulsus-Query-Id`, and every
//! upload's rows are checked to sit in one part of `profile_adhoc`.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-server --test profileql_adhoc
//! ```

#[path = "../../pulsus-read/tests/profileql_adhoc_fixtures/mod.rs"]
mod fx;
#[path = "support/live_db.rs"]
mod live_db;

use live_db::ScopedDb;

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use pulsus_clickhouse::{ChClient, Idempotency, QuerySettings};
use serde_json::{Value, json};

const ADHOC: &str = "/api/profiles/v1/adhocprofiles.v1.AdHocProfileService";
const COMPAT: &str = "/adhocprofiles.v1.AdHocProfileService";
const ZERO: &str = "00000000-0000-0000-0000-000000000000";

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
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

/// A JSON Connect call of `path` as `tenant`.
fn post(port: u16, path: &str, tenant: &str, body: &Value) -> Res {
    call(
        port,
        "POST",
        path,
        &[
            ("Content-Type", "application/json"),
            ("X-Scope-OrgID", tenant),
        ],
        body.to_string().as_bytes(),
    )
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
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

fn sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// The finished statements `system.query_log` holds for each request id,
/// a refused one included.
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

/// The statements `system.query_log` holds for `id` of each kind.
async fn kinds(client: &ChClient, id: &str) -> Vec<String> {
    let sql = format!(
        "SELECT toString(query_kind) AS s FROM system.query_log \
         WHERE type = 'QueryFinish' AND is_initial_query AND log_comment = {} \
         ORDER BY event_time_microseconds",
        sql_string(id)
    );
    client
        .query_strings(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"))
}

/// The parts and rows of upload `id` in `profile_adhoc`.
async fn parts_of(client: &ChClient, id: &str) -> (u64, u64) {
    let sql = format!(
        "SELECT concat(toString(uniqExact(_part)), ' ', toString(count())) AS s \
         FROM profile_adhoc WHERE upload_id = toUUID({})",
        sql_string(id)
    );
    let s = client
        .query_strings(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"))
        .remove(0);
    let (p, n) = s.split_once(' ').expect("two counts");
    (p.parse().expect("parts"), n.parse().expect("rows"))
}

// ---------------------------------------------------------------------
// Answers
// ---------------------------------------------------------------------

/// A `200` JSON answer and its request id.
fn ok_json(res: &Res, ctx: &str) -> (Value, String) {
    assert_eq!(res.status, 200, "{ctx}: {}", res.text());
    assert_eq!(
        res.header("content-type"),
        Some("application/json"),
        "{ctx}"
    );
    (res.json(ctx), res.query_id(ctx))
}

/// A refused answer: its status and whole body, and its request id.
fn refused(res: &Res, status: u16, body: &str, ctx: &str) -> String {
    assert_eq!(res.status, status, "{ctx}: {}", res.text());
    assert_eq!(res.text(), body, "{ctx}");
    res.query_id(ctx)
}

fn error(code: &str, message: &str) -> String {
    json!({"code": code, "message": message}).to_string()
}

/// A flamebearer answer as the reference writes it.
fn fb(names: &[&str], levels: &str, ticks: i64, max_self: i64, meta: &str, groups: &str) -> String {
    let names: Vec<String> = names.iter().map(|n| format!("\"{n}\"")).collect();
    format!(
        r#"{{"version":1,"flamebearer":{{"names":[{}],"levels":[{levels}],"numTicks":{ticks},"maxSelf":{max_self}}},"metadata":{{{meta}}},"timeline":null,"groups":{groups},"heatmap":null}}"#,
        names.join(",")
    )
}

const META_CPU: &str =
    r#""format":"single","spyName":"","sampleRate":1000000000,"units":"samples","name":"cpu""#;
const META_SAMPLES: &str =
    r#""format":"single","spyName":"","sampleRate":100,"units":"objects","name":"samples""#;

fn p_cpu() -> String {
    fb(
        &["total", "main", "b", "a", "1f2e"],
        "[0,12,0,0],[0,12,0,1],[0,2,2,4,0,10,10,3,0,0,0,2]",
        12,
        10,
        META_CPU,
        "null",
    )
}

fn p_samples() -> String {
    fb(
        &["total", "main", "b", "a", "1f2e"],
        "[0,5,0,0],[0,5,0,1],[0,0,0,4,0,1,1,3,0,4,4,2]",
        5,
        4,
        META_SAMPLES,
        "null",
    )
}

fn c_at_2() -> String {
    fb(
        &["total", "main", "other"],
        "[0,11,0,0],[0,11,0,1],[0,11,11,2]",
        11,
        11,
        r#""format":"single","spyName":"unknown","sampleRate":100,"units":"","name":"c_1.txt""#,
        "{}",
    )
}

fn j_full() -> String {
    fb(
        &["total", "main", "b", "a"],
        "[0,11,0,0],[0,11,1,1],[1,7,7,3,0,3,3,2]",
        11,
        7,
        r#""format":"single","spyName":"gospy","sampleRate":250,"units":"bytes","name":"b""#,
        "{}",
    )
}

fn upload_body(name: &str, file: &[u8], max_nodes: Option<i64>) -> Value {
    let mut v = json!({"name": name, "profile": fx::b64(file)});
    if let Some(m) = max_nodes {
        v["maxNodes"] = json!(m);
    }
    v
}

/// The flamebearer of an answer, parsed.
fn graph(answer: &Value, ctx: &str) -> Value {
    serde_json::from_str(
        answer["flamebearerProfile"]
            .as_str()
            .unwrap_or_else(|| panic!("{ctx}: no flamebearerProfile in {answer}")),
    )
    .unwrap_or_else(|e| panic!("{ctx}: flamebearerProfile is not JSON: {e}"))
}

struct Run {
    port: u16,
    counted: Vec<(String, String, u64)>,
    uploads: Vec<(String, String)>,
}

impl Run {
    fn count(&mut self, ctx: &str, id: String, n: u64) {
        self.counted.push((ctx.to_string(), id, n));
    }

    /// Uploads `file` as `name`; the answer and the upload's id.
    fn upload(
        &mut self,
        tenant: &str,
        name: &str,
        file: &[u8],
        max_nodes: Option<i64>,
        ctx: &str,
    ) -> (Value, String) {
        let res = post(
            self.port,
            &format!("{ADHOC}/Upload"),
            tenant,
            &upload_body(name, file, max_nodes),
        );
        let (got, rid) = ok_json(&res, ctx);
        self.count(ctx, rid, 1);
        let id = got["id"]
            .as_str()
            .unwrap_or_else(|| panic!("{ctx}: no id in {got}"))
            .to_string();
        self.uploads.push((ctx.to_string(), id.clone()));
        (got, id)
    }

    fn get(&mut self, tenant: &str, body: Value, ctx: &str) -> Value {
        let (got, rid) = ok_json(
            &post(self.port, &format!("{ADHOC}/Get"), tenant, &body),
            ctx,
        );
        self.count(ctx, rid, 1);
        got
    }

    fn diff(&mut self, tenant: &str, body: Value, ctx: &str) -> Value {
        let (got, rid) = ok_json(
            &post(self.port, &format!("{ADHOC}/Diff"), tenant, &body),
            ctx,
        );
        self.count(ctx, rid, 1);
        got
    }

    /// A refused call: its status, body and statement count.
    #[allow(clippy::too_many_arguments)]
    fn refused(
        &mut self,
        path: &str,
        tenant: &str,
        body: Value,
        status: u16,
        want: &str,
        n: u64,
        ctx: &str,
    ) {
        let rid = refused(
            &post(self.port, &format!("{ADHOC}/{path}"), tenant, &body),
            status,
            want,
            ctx,
        );
        self.count(ctx, rid, n);
    }
}

// ---------------------------------------------------------------------
// L1–L12
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn ad_hoc_uploads_answer_in_one_statement_each() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_profileql_adhoc_it")).await;
    let client = ChClient::new(live_db::conn_config(db.name()))
        .await
        .expect("connect");
    let port_a: u16 = 31_886;
    let port_b: u16 = 31_887;
    let clickhouse_http = live_db::ch_http_port();
    let _a = spawn_ready(
        port_a,
        db.name(),
        clickhouse_http,
        &[("PULSUS_COMPAT_ENDPOINTS", "true")],
    );
    let _b = spawn_ready(port_b, db.name(), clickhouse_http, &[]);
    let mut r = Run {
        port: port_a,
        counted: Vec::new(),
        uploads: Vec::new(),
    };

    // L1: a pprof of two types, and its trees by type; an unknown type
    // draws the first (AB1).
    let (got, p) = r.upload("t1", "p.pb.gz", &fx::f_p(), None, "L1 upload");
    assert_eq!(got["name"], "p.pb.gz", "L1");
    assert_eq!(got["profileType"], "cpu", "L1");
    assert_eq!(got["profileTypes"], json!(["cpu", "samples"]), "L1");
    assert_eq!(got["flamebearerProfile"], p_cpu(), "L1 upload");
    for (ctx, ty, want, name) in [
        ("L1 get", None, p_cpu(), "cpu"),
        ("L1 get cpu", Some("cpu"), p_cpu(), "cpu"),
        ("L1 get samples", Some("samples"), p_samples(), "samples"),
        ("L1 get alloc", Some("alloc"), p_cpu(), "cpu"),
    ] {
        let mut body = json!({"id": p});
        if let Some(t) = ty {
            body["profileType"] = json!(t);
        }
        let got = r.get("t1", body, ctx);
        assert_eq!(got["id"], p.as_str(), "{ctx}");
        assert_eq!(got["profileType"], name, "{ctx}");
        assert_eq!(got["profileTypes"], json!(["cpu", "samples"]), "{ctx}");
        assert_eq!(got["flamebearerProfile"], want, "{ctx}");
    }

    // L2: the two size caps, before any statement.
    let big = "A".repeat(4_194_305);
    r.refused(
        "Upload",
        "t1",
        json!({"name": "a.txt", "profile": big}),
        400,
        &error(
            "invalid_argument",
            "profile payload size exceeds limit of 4.2 MB",
        ),
        0,
        "L2 text",
    );
    r.refused(
        "Upload",
        "t1",
        upload_body("p.pb.gz", &fx::gzip(&vec![0u8; 4_194_305]), None),
        400,
        &error(
            "invalid_argument",
            "uncompressed profile payload size exceeds limit of 4.2 MB",
        ),
        0,
        "L2 gzip",
    );

    // L3: the collapsed answer at 2 nodes (AA2), a JSON file's metadata
    // (AA3), and a diff read back from the flame graphs (AA1).
    let (got, c) = r.upload("t1", "c 1.txt", fx::F_C, Some(2), "L3 upload c");
    assert_eq!(got["name"], "c_1.txt", "L3");
    assert_eq!(got["flamebearerProfile"], c_at_2(), "L3 upload c");
    let (_, j) = r.upload("t1", "a.json", fx::F_J, None, "L3 upload j");
    let got = r.get("t1", json!({"id": j}), "L3 get j");
    assert_eq!(got["flamebearerProfile"], j_full(), "L3 get j");
    let (_, x) = r.upload("t1", "x.txt", fx::F_X, None, "L3 upload x");
    let got = r.diff("t1", json!({"leftId": x, "rightId": x}), "L3 diff x");
    assert_eq!(got["profileTypes"], json!(["x.txt"]), "L3 diff x");
    let g = graph(&got, "L3 diff x");
    assert_eq!(
        g["flamebearer"]["names"],
        json!(["total", "main", "y", "other"]),
        "L3"
    );
    assert_eq!(
        g["flamebearer"]["levels"],
        json!([
            [0, 8, 0, 0, 8, 0, 0],
            [0, 8, 0, 0, 8, 0, 1],
            [0, 5, 5, 0, 5, 5, 3, 0, 3, 3, 0, 3, 3, 2]
        ]),
        "L3"
    );
    assert_eq!(g["flamebearer"]["numTicks"], 16, "L3");
    assert_eq!(
        (g["leftTicks"].clone(), g["rightTicks"].clone()),
        (json!(8), json!(8)),
        "L3"
    );
    assert_eq!(g["metadata"]["format"], "double", "L3");
    assert_eq!(g["metadata"]["spyName"], "", "L3");
    assert_eq!(g["groups"], Value::Null, "L3");

    // L4: another tenant sees none of `t1`'s uploads.
    let (_, c4) = r.upload("t1", "c.txt", fx::F_C, None, "L4 upload");
    r.refused(
        "Get",
        "t2",
        json!({"id": c4}),
        404,
        &error("not_found", &format!("profile {c4} not found")),
        1,
        "L4 get",
    );
    r.refused(
        "Diff",
        "t2",
        json!({"leftId": c4, "rightId": c4}),
        404,
        &error("not_found", &format!("left profile {c4} not found")),
        1,
        "L4 diff",
    );
    let (got, rid) = ok_json(
        &post(port_a, &format!("{ADHOC}/List"), "t2", &json!({})),
        "L4 list",
    );
    assert_eq!(got, json!({}), "L4 list");
    r.count("L4 list", rid, 1);

    // L6: a type named twice draws the last match (AC2); a JSON file's
    // one type is its name (AC1); the first type is the file's first
    // (R14); a type with no samples is listed and drawn empty (V6).
    let (_, d) = r.upload("t1", "dup", &fx::f_d(), None, "L6 upload d");
    let got = r.get("t1", json!({"id": d, "profileType": "cpu"}), "L6 get d");
    assert_eq!(
        got["flamebearerProfile"],
        fb(
            &["total", "main", "b", "a"],
            "[0,8,0,0],[0,8,0,1],[0,1,1,3,0,7,7,2]",
            8,
            7,
            META_CPU,
            "null"
        ),
        "L6 get d"
    );
    let got = r.get("t1", json!({"id": j}), "L6 get j");
    assert_eq!(
        (got["profileType"].clone(), got["profileTypes"].clone()),
        (json!("b"), json!(["b"])),
        "L6 j"
    );
    let (_, ba) = r.upload("t1", "ba", &fx::f_ba(), None, "L6 upload ba");
    let got = r.get("t1", json!({"id": ba}), "L6 get ba");
    assert_eq!(got["profileType"], "samples", "L6 ba");
    assert_eq!(got["profileTypes"], json!(["samples", "cpu"]), "L6 ba");
    assert_eq!(
        got["flamebearerProfile"],
        fb(
            &["total", "main", "a"],
            "[0,3,0,0],[0,3,0,1],[0,3,3,2]",
            3,
            3,
            META_SAMPLES,
            "null"
        ),
        "L6 get ba"
    );
    let (_, e) = r.upload("t6", "empty", &fx::f_e(), None, "L6 upload e");
    let (got, rid) = ok_json(
        &post(port_a, &format!("{ADHOC}/List"), "t6", &json!({})),
        "L6 list",
    );
    r.count("L6 list", rid, 1);
    assert_eq!(got["profiles"][0]["id"], e.as_str(), "L6 list: {got}");
    assert_eq!(got["profiles"][0]["name"], "empty", "L6 list");
    let got = r.get("t6", json!({"id": e, "profileType": "samples"}), "L6 get e");
    assert_eq!(
        got["flamebearerProfile"],
        fb(&["total"], "[0,0,0,0]", 0, 0, META_SAMPLES, "null"),
        "L6 get e"
    );

    // L8: `Upload`'s in-process answer is `Get`'s, for each format (D2).
    for (name, file, m) in [
        ("p.pb.gz", fx::f_p(), 3),
        ("c.txt", fx::F_C.to_vec(), 2),
        ("a.json", fx::F_J.to_vec(), 1),
        ("perf.txt", fx::F_S.to_vec(), 8192),
    ] {
        let ctx = format!("L8 {name}");
        let (up, id) = r.upload("t1", name, &file, Some(m), &format!("{ctx} upload"));
        let got = r.get(
            "t1",
            json!({"id": id, "maxNodes": m}),
            &format!("{ctx} get"),
        );
        assert_eq!(got["flamebearerProfile"], up["flamebearerProfile"], "{ctx}");
        assert_eq!(got["profileTypes"], up["profileTypes"], "{ctx}");
    }

    // L9: the checks before any statement, in the reference's order.
    r.refused(
        "Get",
        "t1",
        json!({"id": "a/b"}),
        400,
        &error(
            "invalid_argument",
            "id 'a/b' is invalid: can only contain [a-zA-Z0-9_-.]",
        ),
        0,
        "L9 get a/b",
    );
    r.refused(
        "Get",
        "t1",
        json!({"id": "zz"}),
        404,
        &error("not_found", "profile zz not found"),
        0,
        "L9 get zz",
    );
    let limit = "could not determine max nodes: max flamegraph nodes limit must be set (max allowed 1048576)";
    r.refused(
        "Upload",
        "t1",
        upload_body("c.txt", fx::F_C, Some(-1)),
        500,
        &error("unknown", limit),
        0,
        "L9 upload -1",
    );
    r.refused(
        "Diff",
        "t1",
        json!({"leftId": "zz", "rightId": "zz", "maxNodes": -1}),
        400,
        &error("invalid_argument", limit),
        0,
        "L9 diff -1",
    );

    // L10: the type checks of `Diff`.
    r.refused(
        "Diff",
        "t1",
        json!({"leftId": p, "rightId": d, "profileType": "samples"}),
        400,
        &error(
            "invalid_argument",
            "profile type mismatch: left profile uses 'samples', right profile uses 'cpu'; please specify a profile type from the common types: [cpu]",
        ),
        1,
        "L10 mismatch",
    );
    r.refused(
        "Diff",
        "t1",
        json!({"leftId": p, "rightId": c}),
        400,
        &error(
            "invalid_argument",
            "no common profile types between left ([cpu samples]) and right ([c_1.txt])",
        ),
        1,
        "L10 no common",
    );

    // L12: an upload above the server's default block size lands in one
    // part: F-BIG, every value 1, read at 65,536 nodes.
    let (_, big) = r.upload(
        "t1",
        "big",
        &fx::f_big(),
        Some(fx::F_BIG_MAX_NODES),
        "L12 upload",
    );
    assert_eq!(
        parts_of(&client, &big).await,
        (1, fx::F_BIG_ROWS),
        "L12 parts"
    );
    let got = r.get(
        "t1",
        json!({"id": big, "maxNodes": fx::F_BIG_MAX_NODES}),
        "L12 get",
    );
    assert_eq!(got["profileType"], "t0", "L12");
    let g = graph(&got, "L12 get");
    let leaves = g["flamebearer"]["levels"][1]
        .as_array()
        .map(Vec::len)
        .unwrap_or(0)
        / 4;
    assert_eq!(leaves, 60_000, "L12: the first type's tree");
    assert_eq!(g["flamebearer"]["numTicks"], 60_000, "L12");

    // L7: the native paths with the compat flag unset; the compat paths
    // are not mounted.
    let (got, _) = ok_json(
        &post(
            port_b,
            &format!("{ADHOC}/Upload"),
            "t7",
            &upload_body("c.txt", fx::F_C, None),
        ),
        "L7 upload",
    );
    let seven = got["id"].as_str().expect("an id").to_string();
    r.uploads.push(("L7 upload".to_string(), seven));
    let (got, _) = ok_json(
        &post(port_b, &format!("{ADHOC}/List"), "t7", &json!({})),
        "L7 list",
    );
    assert_eq!(got["profiles"].as_array().map(Vec::len), Some(1), "L7 list");
    let res = post(port_b, &format!("{ADHOC}/Get"), "t7", &json!({"id": ZERO}));
    assert_eq!(res.status, 404, "L7 get: {}", res.text());
    let res = post(
        port_b,
        &format!("{ADHOC}/Diff"),
        "t7",
        &json!({"leftId": ZERO, "rightId": ZERO}),
    );
    assert_eq!(res.status, 404, "L7 diff: {}", res.text());
    for m in ["Upload", "Get", "List", "Diff"] {
        let res = post(port_b, &format!("{COMPAT}/{m}"), "t7", &json!({}));
        assert_eq!(res.status, 404, "L7 compat {m}: {}", res.text());
    }

    // L5: no upload row reached the push path's tables.
    let sql = "SELECT toString(count()) AS s FROM profile_samples WHERE org_id = 't1' \
               UNION ALL SELECT toString(count()) FROM profile_stacks WHERE org_id = 't1'";
    let counts = client
        .query_strings(sql, &QuerySettings::new())
        .await
        .expect("L5");
    assert_eq!(counts, ["0", "0"], "L5");

    // L11: every upload is one part; an upload the view refuses is not
    // listed and not found.
    for (ctx, id) in &r.uploads {
        let (parts, rows) = parts_of(&client, id).await;
        assert!(rows > 0, "{ctx}: no rows for {id}");
        assert_eq!(parts, 1, "{ctx}: {id} in {parts} parts");
    }
    let refused_id = "01900000-0000-7000-8000-0000000000aa";
    let now = now_ms();
    let row = |kind: u8, is_type: u8, at: i64| {
        format!(
            "('t1', {now}, {kind}, toUUID('{refused_id}'), 'two-days', {at}, 0, {is_type}, 'collapsed', 'two-days', 'unknown', 100, '')"
        )
    };
    let insert = format!(
        "INSERT INTO profile_landing (org_id, received_ms, kind, upload_id, upload_name, uploaded_at, \
         type_ordinal, is_type, upload_format, display_name, spy_name, sample_rate, units) VALUES {}, {}",
        row(5, 1, now),
        row(6, 0, now - 86_400_000)
    );
    let refusal = client
        .execute(
            &insert,
            &QuerySettings::new()
                .set("max_partitions_per_insert_block", 1u64)
                .set("insert_deduplicate", 0u64),
            Idempotency::NonIdempotent,
        )
        .await;
    assert!(refusal.is_err(), "L11: the two-day upload was accepted");
    let (got, rid) = ok_json(
        &post(port_a, &format!("{ADHOC}/List"), "t1", &json!({})),
        "L11 list",
    );
    r.count("L11 list", rid, 1);
    assert!(
        !got.to_string().contains(refused_id),
        "L11: the refused upload is listed: {got}"
    );
    r.refused(
        "Get",
        "t1",
        json!({"id": refused_id}),
        404,
        &error("not_found", &format!("profile {refused_id} not found")),
        1,
        "L11 get",
    );
    assert_eq!(parts_of(&client, refused_id).await.1, 0, "L11 rows");

    let ids: Vec<String> = r.counted.iter().map(|(_, id, _)| id.clone()).collect();
    let sent = statements(&client, &ids).await;
    for (ctx, id, want) in &r.counted {
        assert_eq!(sent[id], *want, "{ctx}: statements for {id}");
        if ctx.ends_with("upload") && *want == 1 {
            assert_eq!(
                kinds(&client, id).await,
                ["Insert"],
                "{ctx}: the one statement is the insert"
            );
        }
    }
}
