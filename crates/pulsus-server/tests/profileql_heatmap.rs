//! Issue #825: `SelectHeatmap` — the heatmap of profile and span totals —
//! over the Connect protocol, against the `pulsusdb` binary and a live
//! ClickHouse.
//!
//! The fixture is plan §6.2's rows as kind 0, 2 and 3 landing rows under
//! `t1` and, at 100× the totals, `t2`'s copy of fingerprint 1. Every answer
//! is compared whole, and every request's statements are counted in
//! `system.query_log` by its `X-Pulsus-Query-Id`.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-server --test profileql_heatmap
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
use pulsus_profiles_wire::querier_v1::{SelectHeatmapRequest, SelectHeatmapResponse};
use pulsus_profiles_wire::types_v1::{HeatmapSeries, HeatmapSlot};
use serde_json::{Value, json};

const CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const SECOND_MS: i64 = 1_000;
const MINUTE_MS: i64 = 60_000;
const STEP_MS: i64 = 15_000;
const DAY_MS: i64 = 86_400_000;
const NO_TRACE: &str = "00000000000000000000000000000000";
const T1: &str = "00000000000000000000000000000001";
const T2: &str = "00000000000000000000000000000002";
const PROFILE: &str = "00000000-0000-0000-0000-0000000000a0";
const P1: &str = "00000000-0000-0000-0000-0000000000a1";
const P2: &str = "00000000-0000-0000-0000-0000000000a2";
const SPAN_A: u64 = 10;
const SPAN_B: u64 = 11;

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

/// A kind-2 row: the series' stored labels, `pod` when given.
fn series_row(org: &str, s: i64, service: &str, fp: u128, pod: Option<&str>) -> String {
    let pod = pod.map(|p| format!(r#""pod":"{p}","#)).unwrap_or_default();
    let labels = format!(r#"{{"__profile_type__":"{CPU}",{pod}"service_name":"{service}"}}"#);
    format!(
        "('{org}', {s}, 2, '{CPU}', '{service}', {fp}, {}, 0, 0, 0, unhex('{NO_TRACE}'), [], [], \
         '{labels}', '{PROFILE}')",
        s * 1_000_000
    )
}

/// A kind-3 row: one profile's total at `ts_ns`.
fn entry_row(org: &str, s: i64, service: &str, fp: u128, ts_ns: i64, total: i64) -> String {
    format!(
        "('{org}', {s}, 3, '{CPU}', '{service}', {fp}, {ts_ns}, 0, {total}, 0, \
         unhex('{NO_TRACE}'), [], [], '', '{PROFILE}')"
    )
}

/// A kind-0 row of service `z`: one sample of `profile`, on `stack`, in
/// `span` of `trace`.
#[allow(clippy::too_many_arguments)]
fn sample_row(
    s: i64,
    fp: u128,
    ts_ms: i64,
    stack: u128,
    value: i64,
    span: u64,
    trace: &str,
    profile: &str,
) -> String {
    format!(
        "('t1', {s}, 0, '{CPU}', 'z', {fp}, {}, {stack}, {value}, {span}, unhex('{trace}'), \
         [], [], '', '{profile}')",
        ts_ms * 1_000_000
    )
}

/// Plan §6.2's rows. `S` is two hours ago, floored to the minute.
async fn land(client: &ChClient, s: i64) {
    let e = s + MINUTE_MS;
    let ns = |ms: i64| ms * 1_000_000;
    let mut rows = Vec::new();
    for (org, scale) in [("t1", 1), ("t2", 100)] {
        rows.push(series_row(org, s, "x", 1, Some("a")));
        for (ts_ns, total) in [
            (ns(s + 5 * SECOND_MS), 0),
            (ns(s + 10 * SECOND_MS), 10),
            (ns(s + STEP_MS) + 500_000, 20),
        ] {
            rows.push(entry_row(org, s, "x", 1, ts_ns, total * scale));
        }
    }
    rows.push(series_row("t1", s, "x", 2, Some("b")));
    for (ts_ms, total) in [
        (s - STEP_MS, 30),
        (s - STEP_MS - 1, 30),
        (e + 5 * SECOND_MS, 40),
    ] {
        rows.push(entry_row("t1", s, "x", 2, ns(ts_ms), total));
    }
    rows.push(series_row("t1", s, "x", 3, None));
    rows.push(entry_row("t1", s, "x", 3, ns(s + 50 * SECOND_MS), 10));
    rows.push(series_row("t1", s, "z", 5, Some("d")));
    let at = s + 5 * SECOND_MS;
    for (stack, value, span, trace, profile) in [
        (11, 3, SPAN_A, T1, P1),
        (12, 4, SPAN_A, T1, P1),
        (11, 5, SPAN_B, T1, P1),
        (13, 100, 0, NO_TRACE, P1),
        (11, 2, SPAN_A, T1, P2),
        (11, 9, SPAN_A, T2, P2),
    ] {
        rows.push(sample_row(s, 5, at, stack, value, span, trace, profile));
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

fn compat_path() -> &'static str {
    "/querier.v1.QuerierService/SelectHeatmap"
}

fn native_path() -> &'static str {
    "/api/profiles/v1/querier.v1.QuerierService/SelectHeatmap"
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

/// The 20 bucket starts, `f(i)` for bucket `i`.
fn starts(f: impl Fn(i64) -> i64) -> Vec<i64> {
    (0..20).map(f).collect()
}

/// 20 counts, `hits` set and the rest 0.
fn counts(hits: &[(usize, i32)]) -> Vec<i32> {
    let mut out = vec![0; 20];
    for (i, c) in hits {
        out[*i] = *c;
    }
    out
}

/// A slot as the answer's JSON writes it.
fn slot_json(ts: i64, y_min: &[i64], hits: &[(usize, i32)]) -> String {
    let join = |v: Vec<String>| v.join(",");
    format!(
        r#"{{"timestamp":"{ts}","yMin":[{}],"counts":[{}]}}"#,
        join(y_min.iter().map(|y| y.to_string()).collect()),
        join(counts(hits).iter().map(|c| c.to_string()).collect()),
    )
}

/// A series as the answer's JSON writes it: `pod` when given.
fn series_json(pod: Option<&str>, slots: &[String]) -> String {
    let labels = pod
        .map(|p| format!(r#""labels":[{{"name":"pod","value":"{p}"}}],"#))
        .unwrap_or_default();
    format!(r#"{{{labels}"slots":[{}]}}"#, slots.join(","))
}

fn response_json(series: &[String]) -> String {
    format!(r#"{{"series":[{}]}}"#, series.join(","))
}

// ---------------------------------------------------------------------
// R1–R8
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn the_heatmap_answers_in_one_statement() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_profileql_heatmap_it")).await;
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
            "step": 15,
            "queryType": "HEATMAP_QUERY_TYPE_INDIVIDUAL",
        });
        if let (Some(o), Value::Object(x)) = (v.as_object_mut(), extra) {
            o.extend(x);
        }
        v
    };
    let x = r#"{service_name="x"}"#;
    let a = r#"{pod="a"}"#;
    // L3's request and answer.
    let l3 = |extra: Value| {
        let mut v = body(
            x,
            json!({"end": (e + 7 * SECOND_MS).to_string(), "groupBy": ["pod"]}),
        );
        if let (Some(o), Value::Object(more)) = (v.as_object_mut(), extra) {
            o.extend(more);
        }
        v
    };
    let y3 = starts(|i| 2 * i);
    let r1 = response_json(&[
        series_json(None, &[slot_json(e, &y3, &[(5, 1)])]),
        series_json(
            Some("a"),
            &[slot_json(s + STEP_MS, &y3, &[(0, 1), (5, 1), (10, 1)])],
        ),
        series_json(
            Some("b"),
            &[
                slot_json(s, &y3, &[(15, 1)]),
                slot_json(e + STEP_MS, &y3, &[(19, 1)]),
            ],
        ),
    ]);
    // L1's answer at `scale`.
    let l1 = |scale: i64| {
        response_json(&[series_json(
            None,
            &[slot_json(
                s + STEP_MS,
                &starts(|i| scale * i),
                &[(0, 1), (10, 1), (19, 1)],
            )],
        )])
    };

    let port_a: u16 = 31_874;
    let port_b: u16 = 31_875;
    let clickhouse_http = live_db::ch_http_port();
    let _a = spawn_ready(
        port_a,
        db.name(),
        clickhouse_http,
        &[("PULSUS_COMPAT_ENDPOINTS", "true")],
    );
    let pa = port_a;
    let mut counted: Vec<(String, String, u64)> = Vec::new();

    // R1: the native Connect path, JSON, grouped by `pod`.
    let (got, id) = answer(&connect(pa, native_path(), "t1", &l3(json!({}))), "R1");
    assert_eq!(got, r1, "R1");
    counted.push(("R1".to_string(), id, 1));

    // R2: the compat Connect path, protobuf, `SPAN`.
    let req = SelectHeatmapRequest {
        profile_type_id: CPU.to_string(),
        label_selector: r#"{service_name="z"}"#.to_string(),
        start: s,
        end: e,
        step: 15.0,
        query_type: 2,
        ..SelectHeatmapRequest::default()
    };
    let res = call(
        pa,
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
    let decoded = SelectHeatmapResponse::decode(&res.body[..]).expect("R2 decode");
    assert_eq!(
        decoded,
        SelectHeatmapResponse {
            series: vec![HeatmapSeries {
                labels: Vec::new(),
                slots: vec![HeatmapSlot {
                    timestamp: s + STEP_MS,
                    y_min: starts(|i| 2 + i / 3)
                        .into_iter()
                        .map(|v| v as f64)
                        .collect(),
                    counts: counts(&[(2, 1), (11, 1), (17, 1), (19, 1)]),
                    exemplars: Vec::new(),
                }],
            }],
        },
        "R2"
    );
    counted.push(("R2".to_string(), res.query_id("R2"), 1));

    // R4: a sixth `delta` part selects the first five; four parts refuse.
    let (got, id) = answer(
        &connect(
            pa,
            native_path(),
            "t1",
            &l3(json!({"profileTypeID": format!("{CPU}:delta")})),
        ),
        "R4 delta",
    );
    assert_eq!(got, r1, "R4 delta");
    counted.push(("R4 delta".to_string(), id, 1));
    let id = refused(
        &connect(
            pa,
            native_path(),
            "t1",
            &l3(json!({"profileTypeID": "a:b:c:d"})),
        ),
        400,
        "invalid_argument",
        "profile-type selection must be of the form \
         <name>:<sample-type>:<sample-unit>:<period-type>:<period-unit>(:delta), \
         got(4): \"a:b:c:d\"",
        "R4 four parts",
    );
    counted.push(("R4 four parts".to_string(), id, 0));

    // R5: the request's own checks.
    let mut no_type = body(a, json!({}));
    no_type
        .as_object_mut()
        .expect("an object")
        .remove("queryType");
    for (ctx, request, message) in [
        (
            "R5 step",
            body(a, json!({"step": 0.0009})),
            "step must be >= 1ms, got 0.000900".to_string(),
        ),
        (
            "R5 limit",
            body(a, json!({"limit": "-1"})),
            "limit must be non-negative, got -1".to_string(),
        ),
        (
            "R5 start = end",
            body(a, json!({"end": s.to_string()})),
            format!("start time must be before end time, got start={s} end={s}"),
        ),
        (
            "R5 no query type",
            no_type,
            "valid heatmap query type must be specified".to_string(),
        ),
    ] {
        let id = refused(
            &connect(pa, native_path(), "t1", &request),
            400,
            "invalid_argument",
            &message,
            ctx,
        );
        counted.push((ctx.to_string(), id, 0));
    }

    // R6 (issue #826): exemplars of the query's own type, one per cell, with
    // all their series' labels; an exemplar type unlike the query type is
    // refused on every window, and an unknown one.
    let labels = |pod: &str, service: &str| {
        format!(
            r#""labels":[{{"name":"__profile_type__","value":"{CPU}"}},{{"name":"pod","value":"{pod}"}},{{"name":"service_name","value":"{service}"}}]"#
        )
    };
    let a_labels = labels("a", "x");
    let individual = |t: i64, value: &str| {
        format!(r#"{{"timestamp":"{t}","profileId":"{PROFILE}",{value}{a_labels}}}"#)
    };
    let r6 = format!(
        r#"{{"series":[{{"slots":[{{"timestamp":"{t}","yMin":[{y}],"counts":[{c}],"exemplars":[{e}]}}]}}]}}"#,
        t = s + STEP_MS,
        y = starts(|i| i)
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(","),
        c = counts(&[(0, 1), (10, 1), (19, 1)])
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(","),
        e = [
            individual(s + 5 * SECOND_MS, ""),
            individual(s + 10 * SECOND_MS, r#""value":"10","#),
            individual(s + STEP_MS, r#""value":"20","#),
        ]
        .join(","),
    );
    let (got, id) = answer(
        &connect(
            pa,
            native_path(),
            "t1",
            &body(a, json!({"exemplarType": "EXEMPLAR_TYPE_INDIVIDUAL"})),
        ),
        "R6 individual",
    );
    assert_eq!(got, r6, "R6 individual");
    counted.push(("R6 individual".to_string(), id, 1));
    let z_labels = labels("d", "z");
    let span = |value: i64, span: &str, trace: &str| {
        format!(
            r#"{{"timestamp":"{t}","spanId":"{span}","value":"{value}",{z_labels},"traceId":"{trace}"}}"#,
            t = s + 5 * SECOND_MS,
        )
    };
    let (got, id) = answer(
        &connect(
            pa,
            native_path(),
            "t1",
            &body(
                r#"{service_name="z"}"#,
                json!({"queryType": "HEATMAP_QUERY_TYPE_SPAN", "exemplarType": "EXEMPLAR_TYPE_SPAN"}),
            ),
        ),
        "R6 span",
    );
    let got: Value = serde_json::from_str(&got).expect("R6 span JSON");
    let exemplars = &got["series"][0]["slots"][0]["exemplars"];
    let want: Value = serde_json::from_str(&format!(
        "[{}]",
        [
            span(2, "0a00000000000000", T1),
            span(5, "0b00000000000000", T1),
            span(7, "0a00000000000000", T1),
            span(9, "0a00000000000000", T2),
        ]
        .join(",")
    ))
    .expect("R6 span want");
    assert_eq!(exemplars, &want, "R6 span");
    counted.push(("R6 span".to_string(), id, 1));
    let old_window = json!({"start": (now - 9 * DAY_MS).to_string(),
                            "end": (now - 8 * DAY_MS).to_string()});
    for (ctx, request, message) in [
        (
            "R6 individual with span",
            body(
                a,
                json!({"queryType": "HEATMAP_QUERY_TYPE_SPAN", "exemplarType": "EXEMPLAR_TYPE_INDIVIDUAL"}),
            ),
            "individual exemplars only available for individual query type",
        ),
        (
            "R6 span with individual",
            body(a, json!({"exemplarType": "EXEMPLAR_TYPE_SPAN"})),
            "span exemplars only available for span query type",
        ),
        (
            "R6 individual with span, old window",
            {
                let mut v = body(
                    a,
                    json!({"queryType": "HEATMAP_QUERY_TYPE_SPAN", "exemplarType": "EXEMPLAR_TYPE_INDIVIDUAL"}),
                );
                if let (Some(o), Value::Object(w)) = (v.as_object_mut(), old_window.clone()) {
                    o.extend(w);
                }
                v
            },
            "individual exemplars only available for individual query type",
        ),
        (
            "R6 unknown",
            body(a, json!({"exemplarType": 9})),
            "unknown exemplar type: 9",
        ),
    ] {
        let id = refused(
            &connect(pa, native_path(), "t1", &request),
            400,
            "invalid_argument",
            message,
            ctx,
        );
        counted.push((ctx.to_string(), id, 0));
    }

    // R7: a window ending before the lookback floor.
    let old = body(
        a,
        json!({"start": (now - 9 * DAY_MS).to_string(),
               "end": (now - 8 * DAY_MS).to_string()}),
    );
    let (got, id) = answer(&connect(pa, native_path(), "t1", &old), "R7");
    assert_eq!(got, "{}", "R7");
    counted.push(("R7".to_string(), id, 0));

    // R8: the tenant's own entries.
    let (got, id) = answer(&connect(pa, native_path(), "t2", &body(a, json!({}))), "R8");
    assert_eq!(got, l1(100), "R8");
    counted.push(("R8".to_string(), id, 1));

    // R3: spawn B, the compat flag unset: the native Connect path answers
    // L1; the compat path is not mounted.
    let _b = spawn_ready(port_b, db.name(), clickhouse_http, &[]);
    let pb = port_b;
    let (got, id) = answer(&connect(pb, native_path(), "t1", &body(a, json!({}))), "R3");
    assert_eq!(got, l1(1), "R3");
    counted.push(("R3".to_string(), id, 1));
    assert_eq!(
        connect(pb, compat_path(), "t1", &body(a, json!({}))).status,
        404,
        "R3 compat"
    );

    let ids: Vec<String> = counted.iter().map(|(_, id, _)| id.clone()).collect();
    let sent = statements(&client, &ids).await;
    for (ctx, id, want) in &counted {
        assert_eq!(sent[id], *want, "{ctx}: statements for {id}");
    }
}
