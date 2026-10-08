//! Issue #635 part 4: `X-Scope-OrgID` end to end, through the real
//! `pulsusdb` binary against a live ClickHouse.
//!
//! ```text
//!   T1   two tenants push one series, different values    each reads its own
//!   T2   after a merge, the collapsing tables              the series once per tenant
//!   T5   discovery, metadata and status/tsdb               nothing of the other tenant
//!   T6   the header                                        '' / valid / 400 everywhere
//!   T11  push suppression                                  two tenants, two pushes
//!   T13  every /api/v1 route, seeded rows                  tenant-q sees none of tenant-o
//!   T14  discovery with no range                           every day of the asking tenant, none of the other's
//! ```
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-server --test prom_api_tenants_live
//! ```

#[path = "support/live_db.rs"]
mod live_db;

use live_db::ScopedDb;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use prost::Message;
use pulsus_clickhouse::{ChClient, Idempotency, QuerySettings};
use pulsus_write::protocols::remote_write::{
    Label, MetricMetadataProto, Sample, TimeSeries, WriteRequest,
};

const PUSH_PORT: u16 = 31_800;
const HEADER_PORT: u16 = 31_801;
const DEDUP_PORT: u16 = 31_802;
const ROUTES_PORT: u16 = 31_803;
const DISCOVERY_PORT: u16 = 31_804;
const NO_RANGE_PORT: u16 = 31_805;

const MINUTE_MS: i64 = 60_000;
const DAY_MS: i64 = 86_400_000;

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
    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n")
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

fn spawn_ready(port: u16, db: &ScopedDb) -> ChildGuard {
    live_db::build_schema_blocking(db.name());
    assert_port_free(port);
    let mut command = Command::new(env!("CARGO_BIN_EXE_pulsusdb"));
    command
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env("PULSUS_CACHE_TTL", "1s")
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

/// The one number `sql` answers, its column named `n`.
async fn number(client: &ChClient, sql: &str) -> u64 {
    strings(client, &format!("SELECT toString(n) AS s FROM ({sql})"))
        .await
        .first()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("a number from {sql}"))
}

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::NonIdempotent)
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
}

/// Polls `sql` until it answers `want`, or fails naming both figures.
async fn wait_for_number(client: &ChClient, sql: &str, want: u64, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = u64::MAX;
    while Instant::now() < deadline {
        last = number(client, sql).await;
        if last == want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("{what}: expected {want}, the database holds {last}\n  {sql}");
}

fn json(body: &str) -> serde_json::Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("{e}: {body}"))
}

// ---------------------------------------------------------------------
// Pushes
// ---------------------------------------------------------------------

/// A remote write of one series, three samples fifteen seconds apart from
/// `t1_ms`, and its descriptor when `help` is given.
fn remote_write(
    metric: &str,
    labels: &[(&str, &str)],
    values: [f64; 3],
    t1_ms: i64,
    help: Option<&str>,
) -> Vec<u8> {
    let mut all = vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }];
    all.extend(labels.iter().map(|(k, v)| Label {
        name: k.to_string(),
        value: v.to_string(),
    }));
    let req = WriteRequest {
        timeseries: vec![TimeSeries {
            labels: all,
            samples: values
                .iter()
                .enumerate()
                .map(|(i, v)| Sample {
                    value: *v,
                    timestamp: t1_ms + i as i64 * 15_000,
                })
                .collect(),
            ..Default::default()
        }],
        metadata: help
            .map(|h| {
                vec![MetricMetadataProto {
                    r#type: 1,
                    metric_family_name: metric.to_string(),
                    help: h.to_string(),
                    unit: String::new(),
                }]
            })
            .unwrap_or_default(),
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy-compress the write")
}

fn push(port: u16, body: &[u8], headers: &[(&str, &[u8])]) -> HttpResponse {
    request(
        port,
        "POST",
        "/api/v1/write",
        Some("application/x-protobuf"),
        headers,
        body,
    )
}

/// An OTLP/JSON gauge of one point at `t_ms`.
fn otlp_gauge(metric: &str, t_ms: i64) -> Vec<u8> {
    format!(
        r#"{{"resourceMetrics":[{{"scopeMetrics":[{{"metrics":[
             {{"name":"{metric}","gauge":{{"dataPoints":[{{"timeUnixNano":"{}","asDouble":1}}]}}}}
           ]}}]}}]}}"#,
        t_ms * 1_000_000
    )
    .into_bytes()
}

/// The instant value of `query` at `at_ms` as `tenant`, polled until one
/// series answers: a new series is stored before the cache resolves it.
fn instant_value(port: u16, query: &str, at_ms: i64, tenant: Option<&str>) -> String {
    let path = format!(
        "/api/v1/query?query={}&time={}",
        urlencode(query),
        at_ms as f64 / 1000.0
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut last = String::new();
    while Instant::now() < deadline {
        let r = get(port, &path, tenant);
        last = r.body.clone();
        if r.status == 200 {
            let v = json(&r.body);
            if let Some(result) = v["data"]["result"].as_array()
                && result.len() == 1
            {
                return result[0]["value"][1].as_str().unwrap_or("").to_string();
            }
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    panic!("no series for {query} as {tenant:?} within 60s; last body: {last}");
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// **T1 and T2.** Two tenants push the same series with different values
/// and descriptors. Each tenant's `query` and `query_range` return its own
/// values; after a merge the five collapsing tables hold the series once
/// per tenant.
#[tokio::test(flavor = "multi_thread")]
async fn two_tenants_pushing_one_series_keep_their_own() {
    skip_unless_live!();
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_tenants_it_push")).await;
    let _server = spawn_ready(PUSH_PORT, &db);
    let client = client_for(db.name()).await;
    let t1 = (now_ms() / MINUTE_MS) * MINUTE_MS - 10 * MINUTE_MS;
    let labels = [("job", "t1")];
    for (tenant, values, help) in [
        ("tenant-a", [1.0, 2.0, 3.0], "a help"),
        ("tenant-b", [10.0, 20.0, 30.0], "b help"),
    ] {
        let body = remote_write("t1_total", &labels, values, t1, Some(help));
        let r = push(PUSH_PORT, &body, &org(Some(tenant)));
        assert_eq!(r.status, 204, "{tenant}: {}", r.body);
    }
    wait_for_number(
        &client,
        "SELECT count() AS n FROM metric_landing WHERE kind = 0 AND metric_name = 't1_total'",
        6,
        "both pushes landed",
    )
    .await;

    // T1: each tenant reads its own values.
    let at = t1 + 30_000;
    for (tenant, last, sum) in [("tenant-a", "3", "6"), ("tenant-b", "30", "60")] {
        assert_eq!(
            instant_value(PUSH_PORT, r#"t1_total{job="t1"}"#, at, Some(tenant)),
            last,
            "T1: {tenant}'s query"
        );
        assert_eq!(
            instant_value(
                PUSH_PORT,
                r#"sum_over_time(t1_total{job="t1"}[5m])"#,
                at,
                Some(tenant)
            ),
            sum,
            "T1: {tenant}'s query over its own samples"
        );
        let path = format!(
            "/api/v1/query_range?query={}&start={}&end={}&step=15",
            urlencode(r#"t1_total{job="t1"}"#),
            t1 as f64 / 1000.0,
            at as f64 / 1000.0
        );
        let r = get(PUSH_PORT, &path, Some(tenant));
        assert_eq!(r.status, 200, "{}", r.body);
        let v = json(&r.body);
        let result = v["data"]["result"].as_array().expect("a matrix");
        assert_eq!(result.len(), 1, "T1: {tenant}'s query_range: {}", r.body);
        let got: Vec<&str> = result[0]["values"]
            .as_array()
            .expect("values")
            .iter()
            .map(|p| p[1].as_str().unwrap_or(""))
            .collect();
        let want: Vec<String> = if tenant == "tenant-a" {
            vec!["1".into(), "2".into(), "3".into()]
        } else {
            vec!["10".into(), "20".into(), "30".into()]
        };
        assert_eq!(got, want, "T1: {tenant}'s query_range");
    }

    // T2: the collapsing tables hold the series once per tenant.
    for table in [
        "metric_labels",
        "metric_series",
        "metric_label_index",
        "metric_label_values",
        "metric_metadata",
    ] {
        exec(&client, &format!("OPTIMIZE TABLE {table} FINAL")).await;
    }
    for (table, filter) in [
        ("metric_labels", "metric_name = 't1_total'"),
        ("metric_series", "metric_name = 't1_total'"),
        ("metric_label_index", "key = 'job' AND value = 't1'"),
        ("metric_label_values", "key = 'job' AND value = 't1'"),
        ("metric_metadata", "metric_name = 't1_total'"),
    ] {
        let rows = strings(
            &client,
            &format!(
                "SELECT concat(org_id, ' ', toString(count())) AS s FROM {table} \
                 WHERE {filter} GROUP BY org_id ORDER BY s"
            ),
        )
        .await;
        assert_eq!(
            rows,
            vec!["tenant-a 1".to_string(), "tenant-b 1".to_string()],
            "T2: {table} holds the series once per tenant"
        );
    }
}

/// **T5: discovery, metadata and status/tsdb answer the asking tenant.**
/// Tenant-a pushes `t5_total{job="t5"}`; tenant-b pushes
/// `t5_total{job="t5",only_b="1"}` and a descriptor of its own.
#[tokio::test(flavor = "multi_thread")]
async fn discovery_answers_the_asking_tenant() {
    skip_unless_live!();
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_tenants_it_discovery")).await;
    let _server = spawn_ready(DISCOVERY_PORT, &db);
    let client = client_for(db.name()).await;
    let t1 = (now_ms() / MINUTE_MS) * MINUTE_MS - 10 * MINUTE_MS;
    let a = remote_write("t5_total", &[("job", "t5")], [1.0, 2.0, 3.0], t1, None);
    let b = remote_write(
        "t5_total",
        &[("job", "t5"), ("only_b", "1")],
        [1.0, 2.0, 3.0],
        t1,
        Some("only b"),
    );
    assert_eq!(push(DISCOVERY_PORT, &a, &org(Some("tenant-a"))).status, 204);
    assert_eq!(push(DISCOVERY_PORT, &b, &org(Some("tenant-b"))).status, 204);
    wait_for_number(
        &client,
        "SELECT count() AS n FROM metric_landing WHERE kind = 0",
        6,
        "both pushes landed",
    )
    .await;
    let window = format!(
        "start={}&end={}",
        (t1 - MINUTE_MS) as f64 / 1000.0,
        (t1 + 5 * MINUTE_MS) as f64 / 1000.0
    );
    let a_get = |path: &str| {
        let r = get(DISCOVERY_PORT, path, Some("tenant-a"));
        assert_eq!(r.status, 200, "{path}: {}", r.body);
        r.body
    };
    let labels = a_get(&format!(
        "/api/v1/labels?match[]={}&{window}",
        urlencode(r#"{job="t5"}"#)
    ));
    assert!(labels.contains("\"job\""), "T5 /labels: {labels}");
    assert!(!labels.contains("only_b"), "T5 /labels: {labels}");
    let all_labels = a_get(&format!("/api/v1/labels?{window}"));
    assert!(!all_labels.contains("only_b"), "T5 /labels: {all_labels}");
    let values = a_get(&format!("/api/v1/label/only_b/values?{window}"));
    assert_eq!(json(&values)["data"], serde_json::json!([]), "T5 values");
    let series = a_get(&format!(
        "/api/v1/series?match[]={}&{window}",
        urlencode(r#"{job="t5"}"#)
    ));
    assert_eq!(
        json(&series)["data"].as_array().map(Vec::len),
        Some(1),
        "T5 /series: {series}"
    );
    assert!(!series.contains("only_b"), "T5 /series: {series}");
    let metadata = a_get("/api/v1/metadata");
    assert!(!metadata.contains("only b"), "T5 /metadata: {metadata}");
    // The cache is swept per tenant; poll until tenant-a's sweep has run.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = String::new();
    while Instant::now() < deadline {
        last = a_get("/api/v1/status/tsdb");
        if json(&last)["data"]["headStats"]["numSeries"] == serde_json::json!(1) {
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    assert_eq!(
        json(&last)["data"]["headStats"]["numSeries"],
        serde_json::json!(1),
        "T5 /status/tsdb counts tenant-a's one series: {last}"
    );
}

/// Every metrics route of the design's §3, each as `(method, path,
/// content type, body)`. The write bodies are valid, so a refusal can only
/// be the header's.
fn every_route(t_ms: i64) -> Vec<(&'static str, String, Option<&'static str>, Vec<u8>)> {
    let q = urlencode(r#"t6refused{job="t6refused"}"#);
    let m = urlencode(r#"{job="t6refused"}"#);
    let time = t_ms as f64 / 1000.0;
    let range = format!("start={}&end={}&step=15", time - 60.0, time);
    let form = "application/x-www-form-urlencoded";
    vec![
        (
            "POST",
            "/api/v1/write".to_string(),
            Some("application/x-protobuf"),
            remote_write("t6_write", &[("job", "t6")], [1.0, 2.0, 3.0], t_ms, None),
        ),
        (
            "POST",
            "/v1/metrics".to_string(),
            Some("application/json"),
            otlp_gauge("t6_otlp", t_ms),
        ),
        (
            "GET",
            format!("/api/v1/query?query={q}&time={time}"),
            None,
            Vec::new(),
        ),
        (
            "POST",
            "/api/v1/query".to_string(),
            Some(form),
            format!("query={q}&time={time}").into_bytes(),
        ),
        (
            "GET",
            format!("/api/v1/query_range?query={q}&{range}"),
            None,
            Vec::new(),
        ),
        (
            "POST",
            "/api/v1/query_range".to_string(),
            Some(form),
            format!("query={q}&{range}").into_bytes(),
        ),
        (
            "GET",
            format!("/api/v1/labels?match[]={m}"),
            None,
            Vec::new(),
        ),
        (
            "POST",
            "/api/v1/labels".to_string(),
            Some(form),
            format!("match[]={m}").into_bytes(),
        ),
        (
            "GET",
            format!("/api/v1/label/t6label/values?match[]={m}"),
            None,
            Vec::new(),
        ),
        (
            "GET",
            format!("/api/v1/series?match[]={m}"),
            None,
            Vec::new(),
        ),
        (
            "POST",
            "/api/v1/series".to_string(),
            Some(form),
            format!("match[]={m}").into_bytes(),
        ),
        (
            "GET",
            "/api/v1/metadata?metric=t6meta".to_string(),
            None,
            Vec::new(),
        ),
        ("GET", "/api/v1/status/tsdb".to_string(), None, Vec::new()),
        (
            "GET",
            format!("/api/v1/query_exemplars?query={q}&{range}"),
            None,
            Vec::new(),
        ),
        (
            "POST",
            "/api/v1/query_exemplars".to_string(),
            Some(form),
            format!("query={q}&{range}").into_bytes(),
        ),
    ]
}

/// **T6: the header.** No header is the empty tenant, read back with no
/// header; `tenant-a` and a 150-byte value are accepted; an empty value,
/// 151 bytes, `a'b`, `a b`, `a\x01b`, a non-UTF-8 byte and the header
/// given twice each answer `400` on every metrics route, write and read.
/// A refused write stores no row; a refused read sends no statement.
#[tokio::test(flavor = "multi_thread")]
async fn the_header_names_a_tenant_or_is_refused() {
    skip_unless_live!();
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_tenants_it_header")).await;
    let _server = spawn_ready(HEADER_PORT, &db);
    let client = client_for(db.name()).await;
    let t = (now_ms() / MINUTE_MS) * MINUTE_MS - 10 * MINUTE_MS;

    let marker = strings(&client, "SELECT toString(now64(6)) AS s")
        .await
        .remove(0);
    let long_151 = "t".repeat(151);
    let invalid: Vec<(&str, Vec<Vec<u8>>)> = vec![
        ("an empty value", vec![Vec::new()]),
        ("151 bytes", vec![long_151.into_bytes()]),
        ("a quote", vec![b"a'b".to_vec()]),
        ("a space", vec![b"a b".to_vec()]),
        ("a control byte", vec![b"a\x01b".to_vec()]),
        ("a non-UTF-8 byte", vec![b"a\xffb".to_vec()]),
        (
            "the header twice",
            vec![b"tenant-a".to_vec(), b"tenant-b".to_vec()],
        ),
    ];
    for (what, values) in &invalid {
        for (method, path, content_type, body) in every_route(t) {
            let headers: Vec<(&str, &[u8])> = values
                .iter()
                .map(|v| ("X-Scope-OrgID", v.as_slice()))
                .collect();
            let r = request(HEADER_PORT, method, &path, content_type, &headers, &body);
            assert_eq!(r.status, 400, "{what}: {method} {path}: {}", r.body);
            // A control byte is refused by the HTTP layer before any route
            // runs; every other value reaches the route, which names it.
            if *what != "a control byte" {
                assert!(
                    r.body.contains("invalid X-Scope-OrgID"),
                    "{what}: {method} {path}: {}",
                    r.body
                );
                if path.starts_with("/api/v1/") && path != "/api/v1/write" {
                    assert_eq!(
                        json(&r.body)["errorType"],
                        "bad_data",
                        "{what}: {method} {path}"
                    );
                }
            }
        }
    }
    // A refused write stored nothing, and a refused read sent nothing.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        number(&client, "SELECT count() AS n FROM metric_landing").await,
        0,
        "T6: a refused write stores no row"
    );
    exec(&client, "SYSTEM FLUSH LOGS").await;
    let sent = number(
        &client,
        &format!(
            "SELECT count() AS n FROM system.query_log WHERE current_database = '{}' \
             AND query_start_time_microseconds >= toDateTime64('{marker}', 6) \
             AND (query LIKE '%t6refused%' OR query LIKE '%t6label%' OR query LIKE '%t6meta%')",
            db.name()
        ),
    )
    .await;
    assert_eq!(sent, 0, "T6: a refused read sends no statement");

    // Accepted: no header, `tenant-a`, and a 150-byte tenant, each read
    // back as itself.
    let long_150 = "t".repeat(150);
    for (i, tenant) in [None, Some("tenant-a"), Some(long_150.as_str())]
        .into_iter()
        .enumerate()
    {
        let metric = format!("t6_ok_{i}");
        let body = remote_write(&metric, &[("job", "t6")], [1.0, 2.0, 3.0], t, None);
        let r = push(HEADER_PORT, &body, &org(tenant));
        assert_eq!(r.status, 204, "{tenant:?}: {}", r.body);
        assert_eq!(
            instant_value(HEADER_PORT, &metric, t + 30_000, tenant),
            "3",
            "T6: {tenant:?} reads its own push"
        );
    }
}

/// **T11: two tenants' identical pushes are two pushes.** With suppression
/// on, tenants a and b send the same push, (a) with one `Idempotency-Key`
/// and (b) with none: all four are answered `204` and stored, each tenant
/// reading its own samples. Tenant-a repeating (b) is still suppressed.
#[tokio::test(flavor = "multi_thread")]
async fn two_tenants_identical_pushes_are_two_pushes() {
    skip_unless_live!();
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_tenants_it_dedup")).await;
    let _server = spawn_ready(DEDUP_PORT, &db);
    let client = client_for(db.name()).await;
    let t1 = (now_ms() / MINUTE_MS) * MINUTE_MS - 10 * MINUTE_MS;
    let keyed = remote_write("t11_total", &[("job", "keyed")], [1.0, 2.0, 3.0], t1, None);
    let plain = remote_write("t11_total", &[("job", "plain")], [1.0, 2.0, 3.0], t1, None);
    for tenant in ["tenant-a", "tenant-b"] {
        let mut headers = org(Some(tenant));
        headers.push(("Idempotency-Key", b"t11-key"));
        assert_eq!(
            push(DEDUP_PORT, &keyed, &headers).status,
            204,
            "{tenant} keyed"
        );
        assert_eq!(
            push(DEDUP_PORT, &plain, &org(Some(tenant))).status,
            204,
            "{tenant} plain"
        );
    }
    for (tenant, job) in [
        ("tenant-a", "keyed"),
        ("tenant-a", "plain"),
        ("tenant-b", "keyed"),
        ("tenant-b", "plain"),
    ] {
        wait_for_number(
            &client,
            &format!(
                "SELECT count() AS n FROM metric_landing WHERE kind = 0 AND org_id = '{tenant}' \
                 AND fingerprint IN (SELECT fingerprint FROM metric_labels \
                 WHERE org_id = '{tenant}' AND JSONExtractString(labels, 'job') = '{job}')"
            ),
            3,
            &format!("T11: {tenant}'s {job} push is stored"),
        )
        .await;
        assert_eq!(
            instant_value(
                DEDUP_PORT,
                &format!(r#"count_over_time(t11_total{{job="{job}"}}[5m])"#),
                t1 + 40_000,
                Some(tenant)
            ),
            "3",
            "T11: {tenant} reads its own {job} samples"
        );
    }
    // Tenant-a's repeat of the plain push is suppressed: still 3 rows.
    assert_eq!(
        push(DEDUP_PORT, &plain, &org(Some("tenant-a"))).status,
        204,
        "the repeat"
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        number(
            &client,
            "SELECT count() AS n FROM metric_landing WHERE kind = 0 AND org_id = 'tenant-a'"
        )
        .await,
        6,
        "T11: tenant-a's repeat is suppressed"
    );
}

// ---------------------------------------------------------------------
// T13: every route over the design's rows
// ---------------------------------------------------------------------

/// The design's §9 rows as landing rows (the same as
/// `crates/pulsus-read/tests/live_metrics_tenants.rs`), offsets from `t0`.
fn data_sql(t0: i64) -> Vec<String> {
    let m = |minutes: i64| t0 + minutes * MINUTE_MS;
    let back = t0 - 2 * DAY_MS;
    let series = [
        ("tenant-q", "m_x", 42, t0, r#"{"job":"api","status":"200"}"#),
        ("tenant-q", "m_x", 43, t0, r#"{"job":"api","status":"500"}"#),
        (
            "tenant-q",
            "m_x",
            45,
            back,
            r#"{"job":"api","status":"501"}"#,
        ),
        ("tenant-q", "m_x", 46, t0, r#"{"job":"web","status":"500"}"#),
        ("tenant-q", "h_x", 47, t0, r#"{"job":"api","status":"500"}"#),
        ("tenant-q", "m_w", 49, t0, r#"{"job":"api","status":"500"}"#),
        ("tenant-q", "m_x", 50, t0, r#"{"job":"api","status":"404"}"#),
        ("tenant-o", "m_x", 42, t0, r#"{"job":"api","status":"500"}"#),
        (
            "tenant-o",
            "m_x",
            43,
            t0,
            r#"{"job":"api","status":"500","zone":"o"}"#,
        ),
        ("tenant-o", "m_o", 44, t0, r#"{"job":"api","status":"502"}"#),
        ("tenant-o", "m_y", 45, t0, r#"{"job":"db","status":"200"}"#),
        ("tenant-o", "m_z", 46, t0, r#"{"job":"api","status":"200"}"#),
        ("tenant-o", "h_x", 47, t0, r#"{"job":"api","status":"500"}"#),
        ("tenant-o", "m_x", 49, t0, r#"{"job":"api","status":"500"}"#),
        ("tenant-o", "h_y", 50, t0, r#"{"job":"db"}"#),
    ];
    let floats = [
        ("tenant-q", "m_x", 42, m(0), 1.0),
        ("tenant-q", "m_x", 42, m(1), 11.0),
        ("tenant-q", "m_x", 43, m(0), 2.0),
        ("tenant-q", "m_x", 43, m(1), 12.0),
        ("tenant-q", "m_x", 45, back, 15.0),
        ("tenant-q", "m_x", 45, m(0), 5.0),
        ("tenant-q", "m_x", 45, m(1), 65.0),
        ("tenant-q", "m_x", 46, m(0), 6.0),
        ("tenant-q", "m_x", 46, m(1), 16.0),
        ("tenant-q", "m_w", 49, m(0), 9.0),
        ("tenant-q", "m_w", 49, m(1), 39.0),
        ("tenant-q", "m_x", 50, m(0), 10.0),
        ("tenant-q", "m_x", 50, m(1), 20.0),
        ("tenant-o", "m_x", 42, m(0), 3.0),
        ("tenant-o", "m_x", 42, m(1), 13.0),
        ("tenant-o", "m_x", 43, m(5), 4.0),
        ("tenant-o", "m_x", 43, m(6), 14.0),
        ("tenant-o", "m_o", 44, m(0), 5.0),
        ("tenant-o", "m_y", 45, m(0), 7.0),
        ("tenant-o", "m_z", 46, m(0), 8.0),
        ("tenant-o", "m_x", 49, m(0), 99.0),
        ("tenant-o", "m_x", 49, m(1), 109.0),
    ];
    let hists = [
        ("tenant-q", "h_x", 47, m(0), 10, 1.0),
        ("tenant-q", "h_x", 47, m(1), 20, 2.0),
        ("tenant-o", "h_x", 47, m(5), 30, 3.0),
        ("tenant-o", "h_y", 50, m(6), 40, 4.0),
    ];
    let descriptors = [
        ("tenant-q", "m_x", "counter", "q help"),
        ("tenant-o", "m_x", "counter", "o help"),
        ("tenant-o", "m_o", "gauge", "o only"),
    ];
    let rows: Vec<String> = series
        .iter()
        .map(|(org, name, id, at, labels)| {
            format!("('{org}', 0, 2, '{name}', toUInt128({id}), {at}, 0, '{labels}', 0)")
        })
        .chain(floats.iter().map(|(org, name, id, at, v)| {
            format!("('{org}', 0, 0, '{name}', toUInt128({id}), {at}, {v}, '', 0)")
        }))
        .collect();
    let mut out = vec![format!(
        "INSERT INTO metric_landing (org_id, received_ms, kind, metric_name, fingerprint, \
         unix_milli, value, labels, value_type) VALUES {}",
        rows.join(", ")
    )];
    let rows: Vec<String> = hists
        .iter()
        .map(|(org, name, id, at, count, sum)| {
            format!(
                "('{org}', 0, 1, '{name}', toUInt128({id}), {at}, 0, {count}, {sum}, [0], [1], [{count}])"
            )
        })
        .collect();
    out.push(format!(
        "INSERT INTO metric_landing (org_id, received_ms, kind, metric_name, fingerprint, \
         unix_milli, hist_schema, hist_count, hist_sum, hist_pos_span_offsets, \
         hist_pos_span_lengths, hist_pos_bucket_deltas) VALUES {}",
        rows.join(", ")
    ));
    let rows: Vec<String> = descriptors
        .iter()
        .map(|(org, name, ty, help)| format!("('{org}', 0, 3, '{name}', '{ty}', '{help}', 1)"))
        .collect();
    out.push(format!(
        "INSERT INTO metric_landing (org_id, received_ms, kind, metric_name, metric_type, \
         help, updated_ns) VALUES {}",
        rows.join(", ")
    ));
    out
}

/// What of tenant-o's an answer must not hold: its own series' names, the
/// `zone` key, its sample values, its histogram counts and its
/// descriptors.
const TENANT_O_ONLY: [&str; 15] = [
    "\"m_o\"",
    "\"m_y\"",
    "\"m_z\"",
    "\"zone\"",
    ",\"3\"]",
    ",\"4\"]",
    ",\"13\"]",
    ",\"14\"]",
    ",\"99\"]",
    ",\"109\"]",
    "\"count\":\"30\"",
    "\"count\":\"40\"",
    "o help",
    "o only",
    "\"h_y\"",
];

/// Whether an answer's payload is empty: no series, no names, no values,
/// and of label names only the `__name__` every `/labels` answer lists.
fn is_empty_answer(body: &str) -> bool {
    let v = json(body);
    let data = &v["data"];
    if let Some(result) = data.get("result") {
        return result.as_array().is_some_and(Vec::is_empty);
    }
    if let Some(stats) = data.get("headStats") {
        return stats["numSeries"] == serde_json::json!(0);
    }
    match data {
        // `/labels` always lists `__name__` (docs/api.md §3.3), data or none.
        serde_json::Value::Array(a) => a.iter().all(|v| v == "__name__"),
        serde_json::Value::Object(o) => o.is_empty(),
        _ => false,
    }
}

/// **T13: every route answers the asking tenant.** The design's rows,
/// seeded before the server starts. Once `/api/v1/status/tsdb` reports
/// each tenant's series count, every `/api/v1` route with the design's
/// case, as tenant-q, as tenant-o and with no header: tenant-q's answer is
/// non-empty and holds none of tenant-o's; the no-header answer is empty;
/// the routes that read no data answer the three alike.
#[tokio::test(flavor = "multi_thread")]
async fn every_route_answers_the_asking_tenant() {
    skip_unless_live!();
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_tenants_it_routes")).await;
    live_db::build_schema(db.name()).await;
    let client = client_for(db.name()).await;
    let t0 = (now_ms() / MINUTE_MS) * MINUTE_MS - 30 * MINUTE_MS;
    for sql in data_sql(t0) {
        exec(&client, &sql).await;
    }
    let _server = spawn_ready(ROUTES_PORT, &db);

    // The cache is swept per tenant; wait until both report their series.
    for (tenant, want) in [("tenant-q", 6), ("tenant-o", 8)] {
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut last = String::new();
        while Instant::now() < deadline {
            last = get(ROUTES_PORT, "/api/v1/status/tsdb", Some(tenant)).body;
            if json(&last)["data"]["headStats"]["numSeries"] == serde_json::json!(want) {
                break;
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        assert_eq!(
            json(&last)["data"]["headStats"]["numSeries"],
            serde_json::json!(want),
            "T13: {tenant}'s /status/tsdb: {last}"
        );
    }

    let sec = |minutes: f64| (t0 as f64 + minutes * MINUTE_MS as f64) / 1000.0;
    let a = urlencode(r#"{job="api", status=~"5.."}"#);
    let b = urlencode(r#"{job="api", zone!="o"}"#);
    let c1 = urlencode(r#"{job="api", status=~"5.."}[10m]"#);
    let w3 = urlencode(r#"sum(rate(m_x{job="api"}[5m]))"#);
    let window = format!("start={}&end={}", sec(-10.0), sec(10.0));
    let range = format!("start={}&end={}&step=270", sec(2.0), sec(6.5));
    let form = "application/x-www-form-urlencoded";
    let data_routes: Vec<(&str, String, Vec<u8>)> = vec![
        (
            "GET",
            format!("/api/v1/query?query={c1}&time={}", sec(9.0)),
            Vec::new(),
        ),
        (
            "POST",
            "/api/v1/query".to_string(),
            format!("query={c1}&time={}", sec(9.0)).into_bytes(),
        ),
        (
            "GET",
            format!("/api/v1/query_range?query={w3}&{range}"),
            Vec::new(),
        ),
        (
            "POST",
            "/api/v1/query_range".to_string(),
            format!("query={w3}&{range}").into_bytes(),
        ),
        (
            "GET",
            format!("/api/v1/labels?match[]={a}&{window}"),
            Vec::new(),
        ),
        (
            "POST",
            "/api/v1/labels".to_string(),
            format!("match[]={a}&{window}").into_bytes(),
        ),
        (
            "GET",
            format!("/api/v1/label/__name__/values?match[]={a}&{window}"),
            Vec::new(),
        ),
        (
            "GET",
            format!("/api/v1/label/job/values?match[]={b}&{window}"),
            Vec::new(),
        ),
        (
            "GET",
            format!("/api/v1/series?match[]={b}&{window}"),
            Vec::new(),
        ),
        (
            "POST",
            "/api/v1/series".to_string(),
            format!("match[]={b}&{window}").into_bytes(),
        ),
        ("GET", "/api/v1/metadata".to_string(), Vec::new()),
        ("GET", "/api/v1/status/tsdb".to_string(), Vec::new()),
    ];
    for (method, path, body) in &data_routes {
        let content_type = (*method == "POST").then_some(form);
        let ask = |tenant: Option<&str>| {
            request(ROUTES_PORT, method, path, content_type, &org(tenant), body)
        };
        let q = ask(Some("tenant-q"));
        assert_eq!(q.status, 200, "{method} {path} as tenant-q: {}", q.body);
        assert!(
            !is_empty_answer(&q.body),
            "{method} {path}: tenant-q's answer is empty: {}",
            q.body
        );
        for token in TENANT_O_ONLY {
            assert!(
                !q.body.contains(token),
                "{method} {path}: tenant-q's answer holds tenant-o's {token}: {}",
                q.body
            );
        }
        let o = ask(Some("tenant-o"));
        assert_eq!(o.status, 200, "{method} {path} as tenant-o: {}", o.body);
        assert!(
            !is_empty_answer(&o.body),
            "{method} {path}: tenant-o's answer"
        );
        let none = ask(None);
        assert_eq!(
            none.status, 200,
            "{method} {path} with no header: {}",
            none.body
        );
        assert!(
            is_empty_answer(&none.body),
            "{method} {path}: the empty tenant's answer is not empty: {}",
            none.body
        );
    }

    // The routes that read no data answer the three alike.
    let range_q = format!("query={a}&{range}");
    let no_data: Vec<(&str, String, Vec<u8>)> = vec![
        (
            "GET",
            format!("/api/v1/query_exemplars?{range_q}"),
            Vec::new(),
        ),
        (
            "POST",
            "/api/v1/query_exemplars".to_string(),
            range_q.into_bytes(),
        ),
        ("GET", "/api/v1/status/buildinfo".to_string(), Vec::new()),
        ("GET", "/api/v1/status/config".to_string(), Vec::new()),
        ("GET", "/api/v1/status/flags".to_string(), Vec::new()),
    ];
    for (method, path, body) in &no_data {
        let content_type = (*method == "POST").then_some(form);
        let answers: Vec<(u16, String)> = [Some("tenant-q"), Some("tenant-o"), None]
            .into_iter()
            .map(|t| {
                let r = request(ROUTES_PORT, method, path, content_type, &org(t), body);
                (r.status, r.body)
            })
            .collect();
        assert_eq!(answers[0], answers[1], "{method} {path}");
        assert_eq!(answers[0], answers[2], "{method} {path}");
    }
    let statuses: Vec<u16> = [Some("tenant-q"), Some("tenant-o"), None]
        .into_iter()
        .map(|t| get(ROUTES_PORT, "/api/v1/status/runtimeinfo", t).status)
        .collect();
    assert!(
        statuses.iter().all(|s| *s == statuses[0]),
        "runtimeinfo: {statuses:?}"
    );
}

/// T14 (issue #499): a discovery request with no `start`/`end` reads all
/// time, and still only the asking tenant's rows. Each tenant has one
/// series three days back, outside the hour main's default window read.
#[tokio::test]
async fn discovery_with_no_range_reads_every_day_of_the_asking_tenant() {
    skip_unless_live!();
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_tenants_it_no_range")).await;
    live_db::build_schema(db.name()).await;
    let client = client_for(db.name()).await;
    let old = (now_ms() / MINUTE_MS) * MINUTE_MS - 3 * DAY_MS;
    exec(
        &client,
        &format!(
            "INSERT INTO metric_landing (org_id, received_ms, kind, metric_name, fingerprint, \
             unix_milli, value, labels, value_type) VALUES \
             ('tenant-q', 0, 2, 'q14_old', toUInt128(61), {old}, 0, '{{\"q_old\":\"v\"}}', 0), \
             ('tenant-o', 0, 2, 'o14_old', toUInt128(62), {old}, 0, '{{\"o_old\":\"w\"}}', 0)"
        ),
    )
    .await;
    let _server = spawn_ready(NO_RANGE_PORT, &db);

    let answer = |path: &str, tenant: Option<&str>| {
        let res = get(NO_RANGE_PORT, path, tenant);
        assert_eq!(res.status, 200, "T14 {path} as {tenant:?}: {}", res.body);
        json(&res.body)["data"].clone()
    };
    let holds = |data: &serde_json::Value, s: &str| {
        data.as_array()
            .expect("an array")
            .contains(&serde_json::json!(s))
    };
    let labels_q = answer("/api/v1/labels", Some("tenant-q"));
    assert!(
        holds(&labels_q, "q_old"),
        "T14 /labels as tenant-q: {labels_q}"
    );
    assert!(
        !holds(&labels_q, "o_old"),
        "T14 /labels as tenant-q: {labels_q}"
    );
    let names_q = answer("/api/v1/label/__name__/values", Some("tenant-q"));
    assert!(
        holds(&names_q, "q14_old"),
        "T14 names as tenant-q: {names_q}"
    );
    assert!(
        !holds(&names_q, "o14_old"),
        "T14 names as tenant-q: {names_q}"
    );
    assert_eq!(
        answer("/api/v1/label/o_old/values", Some("tenant-q")),
        serde_json::json!([]),
        "T14 tenant-o's key as tenant-q"
    );
    let series_q = answer(
        &format!("/api/v1/series?match%5B%5D={}", urlencode(r#"{q_old="v"}"#)),
        Some("tenant-q"),
    );
    assert_eq!(
        series_q.as_array().map(Vec::len),
        Some(1),
        "T14 series as tenant-q: {series_q}"
    );
    let labels_o = answer("/api/v1/labels", Some("tenant-o"));
    assert!(
        holds(&labels_o, "o_old"),
        "T14 /labels as tenant-o: {labels_o}"
    );
    assert!(
        !holds(&labels_o, "q_old"),
        "T14 /labels as tenant-o: {labels_o}"
    );
    assert_eq!(
        answer("/api/v1/labels", None),
        serde_json::json!(["__name__"]),
        "T14 /labels with no tenant"
    );
}
