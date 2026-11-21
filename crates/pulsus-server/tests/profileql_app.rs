//! Issue #830: the exploration app's settings, profile stats and query
//! analysis — `settings.v1.SettingsService`, `GetProfileStats`, `GET
//! /api/profiles/v1/stats`, `AnalyzeQuery` and the two services not
//! served — against the `pulsusdb` binary and a live ClickHouse.
//!
//! `H` is the start of the UTC hour 3 h before now. The fixture is kind 2
//! and 3 landing rows: under `t1` one series with profiles at
//! `H + 1230123` ms and `H + 7500000` ms, under `t2` ten series with one
//! profile each at `H + 1800000`. Every request's statements are counted in
//! `system.query_log` by its `X-Pulsus-Query-Id`.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-server --test profileql_app
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
use pulsus_profiles_wire::settings_v1::{
    GetSettingsRequest, GetSettingsResponse, SetSettingsRequest, SetSettingsResponse, Setting,
};
use pulsus_profiles_wire::types_v1::GetProfileStatsResponse;
use serde_json::{Value, json};

const CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const HOUR_MS: i64 = 3_600_000;
const NO_TRACE: &str = "00000000000000000000000000000000";
const PROFILE: &str = "00000000-0000-0000-0000-0000000000a0";

const SETTINGS: &str = "/api/profiles/v1/settings.v1.SettingsService";
const QUERIER: &str = "/api/profiles/v1/querier.v1.QuerierService";

const UNIMPLEMENTED_RULES: &str =
    r#"{"code":"unimplemented","message":"recording rules are not served"}"#;
const UNIMPLEMENTED_VCS: &str =
    r#"{"code":"unimplemented","message":"GitHub integration is not configured"}"#;

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

// ---------------------------------------------------------------------
// The fixture
// ---------------------------------------------------------------------

/// One profile of series `fp` at `t`: its series row and its entry.
fn profile(org: &str, fp: u128, t: i64, received: i64) -> Vec<String> {
    let ns = t * 1_000_000;
    let labels = format!(r#"{{"__profile_type__":"{CPU}","service_name":"svc"}}"#);
    let row = |kind: u8, value: i64, labels: &str| {
        format!(
            "('{org}', {received}, {kind}, '{CPU}', 'svc', {fp}, {ns}, 0, {value}, 0, \
             unhex('{NO_TRACE}'), [], [], '{labels}', '{PROFILE}')"
        )
    };
    vec![row(2, 0, &labels), row(3, 7, "")]
}

async fn land(client: &ChClient, h: i64, received: i64) {
    let mut rows = Vec::new();
    rows.extend(profile("t1", 1, h + 1_230_123, received));
    rows.extend(profile("t1", 1, h + 7_500_000, received));
    for fp in 0..10u128 {
        rows.extend(profile("t2", 100 + fp, h + 1_800_000, received));
    }
    exec(
        client,
        &format!(
            "INSERT INTO profile_landing \
             (org_id, received_ms, kind, type_id, service, fingerprint, timestamp_ns, stack_id, \
              value, span_id, trace_id, node_ids, names, labels, profile_id) VALUES {}",
            rows.join(", ")
        ),
    )
    .await;
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

/// A proto Connect call of `path` as `tenant`.
fn post_proto(port: u16, path: &str, tenant: &str, body: &[u8]) -> Res {
    call(
        port,
        "POST",
        path,
        &[
            ("Content-Type", "application/proto"),
            ("X-Scope-OrgID", tenant),
        ],
        body,
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

/// The tables each of `ids`' statements read, from `system.query_log`.
async fn tables_read(client: &ChClient, ids: &[String]) -> Vec<String> {
    let list: Vec<String> = ids.iter().map(|id| sql_string(id)).collect();
    let sql = format!(
        "SELECT arrayStringConcat(arraySort(tables), ',') AS s FROM system.query_log \
         WHERE type = 'QueryFinish' AND is_initial_query AND log_comment IN ({}) \
         ORDER BY event_time_microseconds",
        list.join(", ")
    );
    client
        .query_strings(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"))
}

// ---------------------------------------------------------------------
// Answers
// ---------------------------------------------------------------------

/// A `200` JSON answer: its body as JSON and its id.
fn ok_json(res: &Res, ctx: &str) -> (Value, String) {
    assert_eq!(res.status, 200, "{ctx}: {}", res.text());
    assert_eq!(
        res.header("content-type"),
        Some("application/json"),
        "{ctx}"
    );
    (res.json(ctx), res.query_id(ctx))
}

/// A refused answer: its status and its whole JSON body, and its id.
fn refused(res: &Res, status: u16, body: &str, ctx: &str) -> String {
    assert_eq!(res.status, status, "{ctx}: {}", res.text());
    assert_eq!(res.text(), body, "{ctx}");
    res.query_id(ctx)
}

fn setting(name: &str, value: &str, modified_at: Option<i64>) -> Value {
    match modified_at {
        Some(m) => json!({"setting": {"name": name, "value": value, "modifiedAt": m.to_string()}}),
        None => json!({"setting": {"name": name, "value": value}}),
    }
}

fn listed(pairs: &[(&str, &str, i64)]) -> Value {
    if pairs.is_empty() {
        return json!({});
    }
    json!({"settings": pairs
        .iter()
        .map(|(n, v, m)| json!({"name": n, "value": v, "modifiedAt": m.to_string()}))
        .collect::<Vec<_>>()})
}

// ---------------------------------------------------------------------
// L1–L12
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn the_apps_settings_and_stats_answer_in_one_statement() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_profileql_app_it")).await;
    let client = ChClient::new(live_db::conn_config(db.name()))
        .await
        .expect("connect");
    let now = now_ms();
    let h = (now - 3 * HOUR_MS).div_euclid(HOUR_MS) * HOUR_MS;
    land(&client, h, now).await;

    let port_a: u16 = 31_884;
    let port_b: u16 = 31_885;
    let clickhouse_http = live_db::ch_http_port();
    let _a = spawn_ready(
        port_a,
        db.name(),
        clickhouse_http,
        &[("PULSUS_COMPAT_ENDPOINTS", "true")],
    );
    let _b = spawn_ready(port_b, db.name(), clickhouse_http, &[]);
    let (a, b) = (port_a, port_b);
    let get = format!("{SETTINGS}/Get");
    let set = format!("{SETTINGS}/Set");
    let delete = format!("{SETTINGS}/Delete");
    let stats = format!("{QUERIER}/GetProfileStats");
    let analyze = format!("{QUERIER}/AnalyzeQuery");
    let mut counted: Vec<(String, String, u64)> = Vec::new();
    let mut count = |ctx: &str, id: String, n: u64| counted.push((ctx.to_string(), id, n));

    // L1: a write, an older one refused, and the stored value listed.
    let (got, id) = ok_json(&post(a, &set, "t1", &setting("a", "1", Some(10))), "L1 set");
    assert_eq!(got, setting("a", "1", Some(10)), "L1 set");
    count("L1 set", id, 1);
    let id = refused(
        &post(a, &set, "t1", &setting("a", "2", Some(5))),
        409,
        r#"{"code":"already_exists","message":"failed to update a: newer update already written"}"#,
        "L1 older",
    );
    count("L1 older", id, 1);
    let (got, id) = ok_json(&post(a, &get, "t1", &json!({})), "L1 get");
    assert_eq!(got, listed(&[("a", "1", 10)]), "L1 get");
    count("L1 get", id, 1);

    // L2: a delete hides the setting and keeps its time.
    let (got, id) = ok_json(&post(a, &delete, "t1", &json!({"name": "a"})), "L2 delete");
    assert_eq!(got, json!({}), "L2 delete");
    count("L2 delete", id, 1);
    let (got, id) = ok_json(&post(a, &get, "t1", &json!({})), "L2 get");
    assert_eq!(got, json!({}), "L2 get");
    count("L2 get", id, 1);
    let id = refused(
        &post(a, &set, "t1", &setting("a", "old", Some(11))),
        409,
        r#"{"code":"already_exists","message":"failed to update a: newer update already written"}"#,
        "L2 older",
    );
    count("L2 older", id, 1);

    // L3: two servers, one time: the larger hash wins on both, whatever
    // the order the writes arrived in.
    let larger = client
        .query_strings(
            "SELECT if(cityHash64('x', toUInt8(0)) > cityHash64('y', toUInt8(0)), 'x', 'y') AS s",
            &QuerySettings::new(),
        )
        .await
        .expect("the larger hash")
        .remove(0);
    for (port, name, value) in [(a, "b", "x"), (b, "b", "y"), (a, "c", "y"), (b, "c", "x")] {
        let ctx = format!("L3 set {name}={value} on {port}");
        let (got, id) = ok_json(
            &post(port, &set, "t3", &setting(name, value, Some(100))),
            &ctx,
        );
        assert_eq!(got, setting(name, value, Some(100)), "{ctx}");
        count(&ctx, id, 1);
    }
    for port in [a, b] {
        let ctx = format!("L3 get on {port}");
        let (got, id) = ok_json(&post(port, &get, "t3", &json!({})), &ctx);
        assert_eq!(
            got,
            listed(&[("b", &larger, 100), ("c", &larger, 100)]),
            "{ctx}"
        );
        count(&ctx, id, 1);
    }

    // L4: another tenant's settings and an empty tenant's stats.
    let (got, id) = ok_json(&post(a, &get, "t2", &json!({})), "L4 get");
    assert_eq!(got, json!({}), "L4 get");
    count("L4 get", id, 1);
    let (got, id) = ok_json(&post(a, &stats, "t9", &json!({})), "L4 stats");
    assert_eq!(got, json!({}), "L4 stats");
    count("L4 stats", id, 1);

    // L5: the hour bounds, read from the series table, in JSON, in proto
    // and from the `GET` route.
    let want = json!({
        "dataIngested": true,
        "oldestProfileTime": h.to_string(),
        "newestProfileTime": (h + 3 * HOUR_MS - 1).to_string(),
    });
    let mut l5 = Vec::new();
    let (got, id) = ok_json(&post(a, &stats, "t1", &json!({})), "L5 json");
    assert_eq!(got, want, "L5 json");
    l5.push(id.clone());
    count("L5 json", id, 1);
    let res = post_proto(a, &stats, "t1", &[]);
    assert_eq!(res.status, 200, "L5 proto: {}", res.text());
    assert_eq!(
        res.header("content-type"),
        Some("application/proto"),
        "L5 proto"
    );
    let decoded = GetProfileStatsResponse::decode(&res.body[..]).expect("L5 proto");
    assert_eq!(
        (
            decoded.data_ingested,
            decoded.oldest_profile_time,
            decoded.newest_profile_time
        ),
        (true, h, h + 3 * HOUR_MS - 1),
        "L5 proto"
    );
    let id = res.query_id("L5 proto");
    l5.push(id.clone());
    count("L5 proto", id, 1);
    let (got, id) = ok_json(
        &call(
            a,
            "GET",
            "/api/profiles/v1/stats",
            &[("X-Scope-OrgID", "t1")],
            &[],
        ),
        "L5 get",
    );
    assert_eq!(got, want, "L5 get");
    l5.push(id.clone());
    count("L5 get", id, 1);

    // L6: the other tenant's bounds.
    let (got, id) = ok_json(&post(a, &stats, "t2", &json!({})), "L6");
    assert_eq!(
        got,
        json!({
            "dataIngested": true,
            "oldestProfileTime": h.to_string(),
            "newestProfileTime": (h + HOUR_MS - 1).to_string(),
        }),
        "L6"
    );
    count("L6", id, 1);

    // L7: the query analysis is not computed; a bad body is refused.
    let (got, id) = ok_json(
        &post(
            a,
            &analyze,
            "t1",
            &json!({"start": h.to_string(), "end": now_ms().to_string(), "query": "{}"}),
        ),
        "L7",
    );
    assert_eq!(got, json!({}), "L7");
    count("L7", id, 0);
    let res = call(
        a,
        "POST",
        &analyze,
        &[
            ("Content-Type", "application/json"),
            ("X-Scope-OrgID", "t1"),
        ],
        br#"{"start":"#,
    );
    assert_eq!(res.status, 400, "L7 bad body: {}", res.text());
    assert_eq!(res.json("L7 bad body")["code"], "invalid_argument", "L7");
    count("L7 bad body", res.query_id("L7 bad body"), 0);

    // L8: the two services not served, at both surfaces.
    for prefix in ["/api/profiles/v1", ""] {
        for (method, body) in [
            (
                "settings.v1.RecordingRulesService/ListRecordingRules",
                UNIMPLEMENTED_RULES,
            ),
            (
                "settings.v1.RecordingRulesService/UpsertRecordingRule",
                UNIMPLEMENTED_RULES,
            ),
            ("vcs.v1.VCSService/GithubApp", UNIMPLEMENTED_VCS),
            ("vcs.v1.VCSService/GetFile", UNIMPLEMENTED_VCS),
        ] {
            let ctx = format!("L8 {prefix}/{method}");
            let res = post(a, &format!("{prefix}/{method}"), "t1", &json!({}));
            let id = refused(&res, 501, body, &ctx);
            assert_eq!(
                res.header("content-type"),
                Some("application/json"),
                "{ctx}"
            );
            count(&ctx, id, 0);
        }
    }

    // L9: the checks before any statement, in order.
    let id = refused(
        &post(a, &set, "t1", &json!({})),
        400,
        r#"{"code":"invalid_argument","message":"no setting values provided"}"#,
        "L9 set",
    );
    count("L9 set", id, 0);
    let id = refused(
        &post(a, &delete, "t1", &json!({})),
        400,
        r#"{"code":"invalid_argument","message":"no setting name provided"}"#,
        "L9 delete",
    );
    count("L9 delete", id, 0);
    let res = call(
        a,
        "POST",
        &set,
        &[("Content-Type", "text/plain"), ("X-Scope-OrgID", "t1")],
        setting("a", "1", None).to_string().as_bytes(),
    );
    assert_eq!(res.status, 415, "L9 media: {}", res.text());
    count("L9 media", res.query_id("L9 media"), 0);

    // L10: no `modifiedAt` is the time of the call.
    let before = now_ms();
    let (got, id) = ok_json(&post(a, &set, "t1", &setting("d", "v", None)), "L10 set");
    let after = now_ms();
    count("L10 set", id, 1);
    let stamped: i64 = got["setting"]["modifiedAt"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("L10: no modifiedAt in {got}"));
    assert!(
        (before..=after).contains(&stamped),
        "L10: {stamped} not in [{before}, {after}]"
    );
    assert_eq!(got, setting("d", "v", Some(stamped)), "L10 set");
    let (got, id) = ok_json(&post(a, &get, "t1", &json!({})), "L10 get");
    assert_eq!(got, listed(&[("d", "v", stamped)]), "L10 get");
    count("L10 get", id, 1);

    // L11: L1's three calls in proto, on `p`.
    let msg = |value: &str, m: i64| SetSettingsRequest {
        setting: Some(Setting {
            name: "p".to_string(),
            value: value.to_string(),
            modified_at: m,
        }),
    };
    let res = post_proto(a, &set, "t4", &msg("1", 10).encode_to_vec());
    assert_eq!(res.status, 200, "L11 set: {}", res.text());
    assert_eq!(
        SetSettingsResponse::decode(&res.body[..]).expect("L11 set"),
        SetSettingsResponse {
            setting: msg("1", 10).setting
        },
        "L11 set"
    );
    count("L11 set", res.query_id("L11 set"), 1);
    let res = post_proto(a, &set, "t4", &msg("2", 5).encode_to_vec());
    let id = refused(
        &res,
        409,
        r#"{"code":"already_exists","message":"failed to update p: newer update already written"}"#,
        "L11 older",
    );
    count("L11 older", id, 1);
    let res = post_proto(a, &get, "t4", &GetSettingsRequest {}.encode_to_vec());
    assert_eq!(res.status, 200, "L11 get: {}", res.text());
    assert_eq!(
        GetSettingsResponse::decode(&res.body[..]).expect("L11 get"),
        GetSettingsResponse {
            settings: msg("1", 10).setting.into_iter().collect()
        },
        "L11 get"
    );
    count("L11 get", res.query_id("L11 get"), 1);

    // L12: with the compat flag unset the native paths answer and the
    // compat path is not mounted.
    for (path, body) in [
        (get.as_str(), json!({})),
        (set.as_str(), setting("e", "1", Some(1))),
        (delete.as_str(), json!({"name": "e"})),
        (stats.as_str(), json!({})),
        (analyze.as_str(), json!({})),
    ] {
        let res = post(b, path, "t5", &body);
        assert_eq!(res.status, 200, "L12 {path}: {}", res.text());
    }
    let res = call(
        b,
        "GET",
        "/api/profiles/v1/stats",
        &[("X-Scope-OrgID", "t5")],
        &[],
    );
    assert_eq!(res.status, 200, "L12 stats get: {}", res.text());
    assert_eq!(
        post(b, "/settings.v1.SettingsService/Get", "t5", &json!({})).status,
        404,
        "L12 compat off"
    );

    let ids: Vec<String> = counted.iter().map(|(_, id, _)| id.clone()).collect();
    let sent = statements(&client, &ids).await;
    for (ctx, id, want) in &counted {
        assert_eq!(sent[id], *want, "{ctx}: statements for {id}");
    }
    // L5: each read names the series table and not the entries.
    let tables = tables_read(&client, &l5).await;
    assert_eq!(tables.len(), 3, "L5 tables: {tables:?}");
    for t in &tables {
        assert!(
            t.contains("profile_series") && !t.contains("profile_entries"),
            "L5 tables: {t}"
        );
    }
}
