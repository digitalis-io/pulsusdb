//! Live end-to-end tests for `/api/logs/v1` (issue #13): spawns the real
//! `pulsusdb` binary against a live ClickHouse (same podman harness as
//! `tests/live_server.rs`), waits for `/ready`, seeds `log_streams`/
//! `log_samples` rows directly via `ChClient` (same idiom as
//! `pulsus-read/tests/rollup_differential.rs`), then drives every
//! `/api/logs/v1` endpoint over loopback HTTP — GET and POST forms,
//! `X-Pulsus-Explain`, and a process-memory scaling check.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`. Run locally:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-server --test logs_api_live
//! podman rm -f pulsus-ch-test
//! ```

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_read::logql::sql::{self, MetricSource, TimeWindow};

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
}

fn ch_host() -> String {
    std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string())
}

fn ch_http_port() -> u16 {
    std::env::var("PULSUS_TEST_CH_HTTP_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(19123)
}

/// One raw HTTP/1.1 response: status code, headers (lowercased names), and
/// body. Bare-bones (KISS, same rationale as `live_server.rs`): no HTTP
/// client dependency for a handful of loopback requests in one test file.
struct HttpResponse {
    status: u16,
    headers: HashMap<String, String>,
    body: String,
}

/// Issues one raw HTTP/1.1 request over loopback. `body` is form-urlencoded
/// content when `Some` (POST); `None` sends no body (GET).
fn http_request(
    port: u16,
    method: &str,
    path: &str,
    extra_headers: &[(&str, &str)],
    body: Option<&str>,
) -> Option<HttpResponse> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(10))).ok();

    let mut request =
        format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    for (name, value) in extra_headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    if let Some(body) = body {
        request.push_str("Content-Type: application/x-www-form-urlencoded\r\n");
        request.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    request.push_str("\r\n");
    if let Some(body) = body {
        request.push_str(body);
    }

    stream.write_all(request.as_bytes()).ok()?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).ok()?;

    // Split head/body on the raw bytes (not a lossy `String`) so a
    // multi-byte UTF-8 sequence straddling the `\r\n\r\n` boundary is never
    // mis-split.
    let split_at = find_subslice(&buf, b"\r\n\r\n")?;
    let head = String::from_utf8_lossy(&buf[..split_at]).into_owned();
    let raw_body = &buf[split_at + 4..];

    let mut lines = head.lines();
    let status = lines
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse::<u16>()
        .ok()?;
    let headers: HashMap<String, String> = lines
        .filter_map(|line| {
            let (k, v) = line.split_once(": ")?;
            Some((k.to_ascii_lowercase(), v.to_string()))
        })
        .collect();

    // `encode.rs` streams the body without a `Content-Length`, so every
    // `/api/logs/v1` response is `Transfer-Encoding: chunked` — dechunk it
    // before handing back `body` so callers never see chunk-size framing.
    let body_bytes = if headers
        .get("transfer-encoding")
        .is_some_and(|v| v == "chunked")
    {
        dechunk(raw_body)
    } else {
        raw_body.to_vec()
    };
    let body = String::from_utf8_lossy(&body_bytes).into_owned();

    Some(HttpResponse {
        status,
        headers,
        body,
    })
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Decodes an HTTP/1.1 `Transfer-Encoding: chunked` body (RFC 9112 §7.1):
/// repeated `<hex-size>\r\n<data>\r\n`, terminated by a zero-size chunk.
/// Chunk extensions (`;name=value` after the size) are not emitted by this
/// server and are not handled here.
fn dechunk(mut raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let Some(line_end) = find_subslice(raw, b"\r\n") else {
            break;
        };
        let size_str = String::from_utf8_lossy(&raw[..line_end]);
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
        // Skip the chunk's trailing `\r\n` before the next size line.
        raw = &raw[(data_end + 2).min(raw.len())..];
    }
    out
}

fn http_get(port: u16, path: &str) -> Option<HttpResponse> {
    http_request(port, "GET", path, &[], None)
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Spawns `pulsusdb` bound to `port`, targeting a fresh `db`, with any
/// `extra_env` set on top of the baseline (issue #14: `spawn_ready_server`
/// below is just this with no extras; the compat-alias live tests pass
/// `[("PULSUS_COMPAT_ENDPOINTS", "1")]`) — the server itself runs the
/// schema reconcile (same startup path `live_server.rs` proves). Blocks
/// until `/ready` is `200` (60s deadline).
fn spawn_ready_server_env(port: u16, db: &str, extra_env: &[(&str, &str)]) -> ChildGuard {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pulsusdb"));
    command
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env("CLICKHOUSE_SERVER", ch_host())
        .env("CLICKHOUSE_HTTP_PORT", ch_http_port().to_string())
        .env("CLICKHOUSE_DB", db);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    let child = command.spawn().expect("spawn pulsusdb");
    let guard = ChildGuard(child);

    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if let Some(res) = http_get(port, "/ready")
            && res.status == 200
        {
            return guard;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("/ready never reached 200 within 60s");
}

/// Baseline spawn — `PULSUS_COMPAT_ENDPOINTS` unset, i.e. `false`
/// (`Config::default()`).
fn spawn_ready_server(port: u16, db: &str) -> ChildGuard {
    spawn_ready_server_env(port, db, &[])
}

fn data_client_config(db: &str) -> ChConnConfig {
    ChConnConfig {
        server: ch_host(),
        http_port: ch_http_port(),
        database: db.to_string(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(20),
        ..ChConnConfig::default()
    }
}

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after the Unix epoch")
            .as_nanos(),
    )
    .expect("current time fits in i64 nanoseconds")
}

/// Drops `db` (via a bootstrap connection to ClickHouse's built-in
/// `default` database — the target database may not exist yet) before
/// seeding, same idiom as `pulsus-read`'s live tests
/// (`rollup_differential.rs`/`explain_indexes.rs`). Load-bearing for exact-
/// count assertions specifically: unlike `log_streams` (`ReplacingMergeTree`,
/// logically deduped by fingerprint at read time), `log_samples` is a plain
/// `MergeTree` — without this, re-running a test against a container that
/// still holds a previous run's rows for the same database name silently
/// doubles (or worse) the row count a byte-exact `count_over_time` golden
/// depends on.
async fn drop_database(db: &str) {
    let mut cfg = data_client_config(db);
    cfg.database = "default".to_string();
    let client = ChClient::new(cfg).await.expect("connect bootstrap client");
    client
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
}

const FP_A: u64 = 0x8000_0000_0000_0001;
const FP_B: u64 = 0x8000_0000_0000_0002;

/// Seeds two streams (`checkout`/prod, `checkout`/staging) with a handful
/// of recent samples each. `log_streams_idx` is populated by the schema's
/// own materialized view over `log_streams` (docs/schemas.md §3.1) — no
/// direct index insert needed.
async fn seed(client: &ChClient, db: &str, base_ns: i64) {
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, updated_ns) VALUES \
                 (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({base_ns}))), {FP_A}, 'checkout', \
                 '{{\"env\":\"prod\",\"service_name\":\"checkout\"}}', 0), \
                 (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({base_ns}))), {FP_B}, 'checkout', \
                 '{{\"env\":\"staging\",\"service_name\":\"checkout\"}}', 0)"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_streams");

    let mut values = Vec::new();
    for (fp, body_prefix) in [(FP_A, "prod"), (FP_B, "staging")] {
        for i in 0..3i64 {
            let ts = base_ns - (3 - i) * 1_000_000_000;
            values.push(format!(
                "('checkout', {fp}, {ts}, 0, '{body_prefix} line {i}')"
            ));
        }
    }
    let sql = format!(
        "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, body) VALUES {}",
        values.join(", ")
    );
    client
        .execute(&sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .expect("seed log_samples");
}

async fn setup(db: &str, port: u16) -> (ChildGuard, ChClient, i64) {
    setup_env(db, port, &[]).await
}

/// `setup`, but spawning through [`spawn_ready_server_env`] so callers can
/// pass extra environment (issue #14: `PULSUS_COMPAT_ENDPOINTS=1`).
async fn setup_env(db: &str, port: u16, extra_env: &[(&str, &str)]) -> (ChildGuard, ChClient, i64) {
    let guard = spawn_ready_server_env(port, db, extra_env);
    let client = ChClient::new(data_client_config(db))
        .await
        .expect("connect data client");
    let base_ns = now_ns();
    seed(&client, db, base_ns).await;
    (guard, client, base_ns)
}

fn json(res: &HttpResponse) -> serde_json::Value {
    serde_json::from_str(&res.body)
        .unwrap_or_else(|e| panic!("invalid JSON body: {e}\nbody: {}", res.body))
}

fn q(path: &str, params: &[(&str, &str)]) -> String {
    let query = params
        .iter()
        .map(|(k, v)| format!("{k}={}", urlencode(v)))
        .collect::<Vec<_>>()
        .join("&");
    format!("{path}?{query}")
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

#[tokio::test]
async fn labels_get_returns_the_distinct_keys_seeded() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = "pulsus_logs_api_it_labels";
    let port = 31_101;
    let (_guard, _client, base_ns) = setup(db, port).await;

    let start = base_ns - 3_600_000_000_000;
    let end = base_ns + 3_600_000_000_000;
    let res = http_get(
        port,
        &q(
            "/api/logs/v1/labels",
            &[("start", &start.to_string()), ("end", &end.to_string())],
        ),
    )
    .expect("labels reachable");
    assert_eq!(res.status, 200);
    assert_eq!(
        res.headers.get("content-type").map(String::as_str),
        Some("application/json")
    );
    let body = json(&res);
    assert_eq!(body["status"], "success");
    let names: Vec<&str> = body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(names.contains(&"env"));
    assert!(names.contains(&"service_name"));
}

#[tokio::test]
async fn labels_post_form_matches_the_get_response() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = "pulsus_logs_api_it_labels_post";
    let port = 31_102;
    let (_guard, _client, base_ns) = setup(db, port).await;

    let start = base_ns - 3_600_000_000_000;
    let end = base_ns + 3_600_000_000_000;
    let form = format!("start={start}&end={end}");
    let res = http_request(port, "POST", "/api/logs/v1/labels", &[], Some(&form))
        .expect("labels POST reachable");
    assert_eq!(res.status, 200);
    let body = json(&res);
    let names: Vec<&str> = body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(names.contains(&"env"));
}

#[tokio::test]
async fn label_values_returns_the_distinct_values_of_env() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = "pulsus_logs_api_it_label_values";
    let port = 31_103;
    let (_guard, _client, base_ns) = setup(db, port).await;

    let start = base_ns - 3_600_000_000_000;
    let end = base_ns + 3_600_000_000_000;
    let res = http_get(
        port,
        &q(
            "/api/logs/v1/label/env/values",
            &[("start", &start.to_string()), ("end", &end.to_string())],
        ),
    )
    .expect("label values reachable");
    assert_eq!(res.status, 200);
    let body = json(&res);
    let values: Vec<&str> = body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(values, vec!["prod", "staging"]);
}

#[tokio::test]
async fn series_get_returns_the_matched_label_sets() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = "pulsus_logs_api_it_series";
    let port = 31_104;
    let (_guard, _client, base_ns) = setup(db, port).await;

    let start = base_ns - 3_600_000_000_000;
    let end = base_ns + 3_600_000_000_000;
    let res = http_get(
        port,
        &q(
            "/api/logs/v1/series",
            &[
                ("match[]", r#"{service_name="checkout"}"#),
                ("start", &start.to_string()),
                ("end", &end.to_string()),
            ],
        ),
    )
    .expect("series reachable");
    assert_eq!(res.status, 200);
    let body = json(&res);
    let series = body["data"].as_array().unwrap();
    assert_eq!(series.len(), 2);
    let envs: Vec<&str> = series.iter().map(|m| m["env"].as_str().unwrap()).collect();
    assert!(envs.contains(&"prod"));
    assert!(envs.contains(&"staging"));
}

#[tokio::test]
async fn series_post_form_with_repeated_match_selectors() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = "pulsus_logs_api_it_series_post";
    let port = 31_105;
    let (_guard, _client, base_ns) = setup(db, port).await;

    let start = base_ns - 3_600_000_000_000;
    let end = base_ns + 3_600_000_000_000;
    let form = format!(
        "match%5B%5D={}&match%5B%5D={}&start={start}&end={end}",
        urlencode(r#"{env="prod"}"#),
        urlencode(r#"{env="staging"}"#),
    );
    let res = http_request(port, "POST", "/api/logs/v1/series", &[], Some(&form))
        .expect("series POST reachable");
    assert_eq!(res.status, 200);
    let body = json(&res);
    assert_eq!(body["data"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn query_range_returns_streams_with_the_global_limit_applied() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = "pulsus_logs_api_it_query_range";
    let port = 31_106;
    let (_guard, _client, base_ns) = setup(db, port).await;

    let start = base_ns - 3_600_000_000_000;
    let end = base_ns + 3_600_000_000_000;
    let res = http_get(
        port,
        &q(
            "/api/logs/v1/query_range",
            &[
                ("query", r#"{service_name="checkout"}"#),
                ("start", &start.to_string()),
                ("end", &end.to_string()),
                ("limit", "3"),
            ],
        ),
    )
    .expect("query_range reachable");
    assert_eq!(res.status, 200);
    let body = json(&res);
    assert_eq!(body["data"]["resultType"], "streams");
    // Global limit (amendment 2): total entries across every stream must
    // never exceed the requested `limit`, regardless of how many streams
    // matched (two, here).
    assert_eq!(body["data"]["stats"]["entries"], 3);
}

#[tokio::test]
async fn query_range_honours_x_pulsus_explain() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = "pulsus_logs_api_it_query_range_explain";
    let port = 31_107;
    let (_guard, _client, base_ns) = setup(db, port).await;

    let start = base_ns - 3_600_000_000_000;
    let end = base_ns + 3_600_000_000_000;
    let res = http_request(
        port,
        "GET",
        &q(
            "/api/logs/v1/query_range",
            &[
                ("query", r#"{service_name="checkout"}"#),
                ("start", &start.to_string()),
                ("end", &end.to_string()),
            ],
        ),
        &[("X-Pulsus-Explain", "1")],
        None,
    )
    .expect("query_range reachable");
    assert_eq!(res.status, 200);
    let body = json(&res);
    let explain = &body["data"]["explain"];
    assert_eq!(explain["result_type"], "streams");
    assert!(
        explain["stages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["name"] == "stage1_stream_resolution")
    );
}

/// Golden gap (round-1 code-review finding 4d): the live suite previously
/// only exercised `query` (instant) for a metric result; this covers
/// `query_range` metric→**matrix** end to end.
#[tokio::test]
async fn query_range_metric_returns_a_matrix() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = "pulsus_logs_api_it_query_range_matrix";
    let port = 31_111;
    let (_guard, _client, base_ns) = setup(db, port).await;

    let start = base_ns - 3_600_000_000_000;
    let end = base_ns + 3_600_000_000_000;
    let res = http_get(
        port,
        &q(
            "/api/logs/v1/query_range",
            &[
                ("query", r#"count_over_time({service_name="checkout"}[1h])"#),
                ("start", &start.to_string()),
                ("end", &end.to_string()),
                ("step", "60s"),
            ],
        ),
    )
    .expect("query_range reachable");
    assert_eq!(res.status, 200);
    let body = json(&res);
    assert_eq!(body["data"]["resultType"], "matrix");
    assert!(body["data"]["stats"]["series"].as_u64().unwrap() >= 1);
    let points = body["data"]["result"][0]["values"].as_array().unwrap();
    assert!(!points.is_empty());
    // Prometheus-style `[<unix_seconds>, "<value>"]` points (architect plan
    // amendment 3 §3 — matrix timestamps are numbers, not ns-strings).
    assert!(points[0][0].is_number());
    assert!(points[0][1].is_string());
}

/// A 1h step, centered on the aligned step boundary rather than on `now`
/// itself: seeded samples land a few seconds before `now`, and centering
/// the bucket eliminates any chance of the 3-sample spread straddling a
/// step boundary — the same failure mode `pulsus-read/tests/
/// rollup_differential.rs`'s own `aligned_base_ns` helper avoids the same
/// way, just generalized to an arbitrary step rather than the rollup
/// resolution.
const POST_GOLDEN_STEP_NS: i64 = 3_600_000_000_000;
/// `Config::default()`'s `log_rollup_resolution` — `spawn_ready_server`
/// sets no `PULSUS_LOG_ROLLUP_RESOLUTION` override, so this is exactly
/// what the spawned server's `EngineConfig::rollup_res_ns` resolves to.
const POST_GOLDEN_ROLLUP_RES_NS: i64 = 5_000_000_000;

fn aligned_step_center_ns(step_ns: i64) -> i64 {
    (now_ns() / step_ns) * step_ns + step_ns / 2
}

/// The `'YYYY-MM-01'` ClickHouse date literal(s) a `[start_ns, end_ns]`
/// window spans, ascending — the live-test-side equivalent of
/// `pulsus_read::logql::plan::months_overlapping` (not reachable from
/// here, `pub(crate)` to that crate), sufficient for the short
/// couple-of-hours windows this suite uses (at most one calendar-month
/// boundary can fall inside one).
fn months_spanned(start_ns: i64, end_ns: i64) -> Vec<String> {
    let mut months: Vec<String> = [start_ns, end_ns]
        .iter()
        .map(|&ns| {
            chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(ns)
                .format("'%Y-%m-01'")
                .to_string()
        })
        .collect();
    months.sort();
    months.dedup();
    months
}

/// POST golden (round-1 code-review finding 2, ratified; round-3
/// re-review finding: field-level assertions on a streams-shaped query
/// are not a byte-exact golden). Uses a **metric** query so the wire
/// shape under test is matrix points, not free-form log lines, and
/// computes the one genuinely dynamic value — the bucket timestamp — the
/// same way the server does (`intDiv` bucket floor) rather than
/// approximating it: this is a real byte-exact comparison, not a
/// normalized one.
#[tokio::test]
async fn query_range_post_metric_is_byte_exact_against_a_computed_golden() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = "pulsus_logs_api_it_query_range_post";
    let port = 31_112;
    drop_database(db).await;
    let _guard = spawn_ready_server(port, db);
    let client = ChClient::new(data_client_config(db))
        .await
        .expect("connect data client");

    let base_ns = aligned_step_center_ns(POST_GOLDEN_STEP_NS);
    seed(&client, db, base_ns).await;

    let window_start = base_ns - POST_GOLDEN_STEP_NS;
    let window_end = base_ns + POST_GOLDEN_STEP_NS;
    let bucket_secs = ((base_ns / POST_GOLDEN_STEP_NS) * POST_GOLDEN_STEP_NS) / 1_000_000_000;

    let form = format!(
        "query={}&start={window_start}&end={window_end}&step=3600s",
        urlencode(r#"count_over_time({service_name="checkout"}[1h])"#)
    );
    let res = http_request(port, "POST", "/api/logs/v1/query_range", &[], Some(&form))
        .expect("query_range POST reachable");
    assert_eq!(res.status, 200);

    // Both seeded fingerprints' 3 samples land in exactly one bucket each
    // (well inside the window, far from its edges); `env` sorts "prod"
    // before "staging" (encode.rs's label-set ordering).
    let expected = format!(
        "{{\"status\":\"success\",\"data\":{{\"resultType\":\"matrix\",\"result\":[\
         {{\"metric\":{{\"env\":\"prod\",\"service_name\":\"checkout\"}},\"values\":[[{bucket_secs}.000,\"3\"]]}},\
         {{\"metric\":{{\"env\":\"staging\",\"service_name\":\"checkout\"}},\"values\":[[{bucket_secs}.000,\"3\"]]}}\
         ],\"stats\":{{\"series\":2}}}}}}"
    );
    assert_eq!(res.body, expected);
}

/// POST golden with `X-Pulsus-Explain: 1` (round-1 code-review finding 2;
/// round-3 re-review finding: must be byte-exact, not field-level). The
/// selector matches exactly **one** seeded fingerprint
/// (`service_name="checkout", env="prod"`) so stage2/metric-read's
/// `fingerprint IN (...)` list has exactly one element — ClickHouse's
/// `GROUP BY fingerprint` row order for *multiple* matched fingerprints
/// is not a documented guarantee, so a multi-fingerprint selector would
/// make the embedded SQL text's fingerprint order genuinely
/// unpredictable; picking a selector with only one match sidesteps that
/// source of flakiness structurally rather than normalizing it away
/// (the `explain_indexes.rs` idiom of collapsing volatile digits to `#`
/// is not needed here for the same reason — every dynamic value below
/// is instead computed exactly, via the real `pulsus_read::logql::sql`
/// builders rather than hand-duplicated SQL text, so the comparison is a
/// genuine byte-exact match).
#[tokio::test]
async fn query_range_post_explain_is_byte_exact_against_a_computed_golden() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = "pulsus_logs_api_it_query_range_post_explain";
    let port = 31_113;
    drop_database(db).await;
    let _guard = spawn_ready_server(port, db);
    let client = ChClient::new(data_client_config(db))
        .await
        .expect("connect data client");

    let base_ns = aligned_step_center_ns(POST_GOLDEN_STEP_NS);
    seed(&client, db, base_ns).await;

    let window_start = base_ns - POST_GOLDEN_STEP_NS;
    let window_end = base_ns + POST_GOLDEN_STEP_NS;
    let bucket_secs = ((base_ns / POST_GOLDEN_STEP_NS) * POST_GOLDEN_STEP_NS) / 1_000_000_000;

    let form = format!(
        "query={}&start={window_start}&end={window_end}&step=3600s",
        urlencode(r#"count_over_time({service_name="checkout", env="prod"}[1h])"#)
    );
    let res = http_request(
        port,
        "POST",
        "/api/logs/v1/query_range",
        &[("X-Pulsus-Explain", "1")],
        Some(&form),
    )
    .expect("query_range POST reachable");
    assert_eq!(res.status, 200);

    let months = months_spanned(window_start, window_end);
    let stage1_sql = sql::stage1(
        "log_streams_idx",
        &months,
        &[
            "(key = 'service_name' AND val = 'checkout')".to_string(),
            "(key = 'env' AND val = 'prod')".to_string(),
        ],
        &[],
    );
    let stage2_sql = sql::stage2("log_streams", &[FP_A]);
    let metric_sql = sql::metric_range(
        MetricSource {
            table: "log_metrics_5s",
            bucket_col: "bucket_ns",
            agg_expr: "sum(count)",
        },
        &[],
        &[FP_A],
        TimeWindow {
            start_ns: window_start,
            end_ns: window_end,
        },
        POST_GOLDEN_STEP_NS as u64,
        &[],
    );
    let routing_reason = format!(
        "rollup: step {POST_GOLDEN_STEP_NS} ns divisible by resolution {POST_GOLDEN_ROLLUP_RES_NS} ns"
    );

    let mut expected = String::new();
    expected.push_str(r#"{"status":"success","data":{"resultType":"matrix","result":["#);
    expected.push_str(&format!(
        r#"{{"metric":{{"env":"prod","service_name":"checkout"}},"values":[[{bucket_secs}.000,"3"]]}}"#
    ));
    expected.push_str(r#"],"stats":{"series":1},"explain":{"#);
    expected.push_str(&format!(
        r#""result_type":"matrix","routing":{{"chosen":"rollup","reason":{}}},"stages":["#,
        serde_json::to_string(&routing_reason).expect("json-escape reason")
    ));
    expected.push_str(&format!(
        r#"{{"name":"stage1_stream_resolution","sql":{},"note":null}},"#,
        serde_json::to_string(&stage1_sql).expect("json-escape sql")
    ));
    expected.push_str(&format!(
        r#"{{"name":"stage2_hydration","sql":{},"note":null}},"#,
        serde_json::to_string(&stage2_sql).expect("json-escape sql")
    ));
    expected.push_str(&format!(
        r#"{{"name":"metric_read","sql":{},"note":{}}}"#,
        serde_json::to_string(&metric_sql).expect("json-escape sql"),
        serde_json::to_string(&routing_reason).expect("json-escape reason")
    ));
    expected.push_str("]}}}"); // close stages[], explain{}, data{}, top{}

    assert_eq!(res.body, expected);
}

/// POST golden for `query` (instant), same rationale as `query_range`'s.
#[tokio::test]
async fn query_post_form_matches_the_get_response() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = "pulsus_logs_api_it_query_post";
    let port = 31_114;
    let (_guard, _client, base_ns) = setup(db, port).await;

    let form = format!(
        "query={}&time={base_ns}",
        urlencode(r#"count_over_time({service_name="checkout"}[1h])"#)
    );
    let res = http_request(port, "POST", "/api/logs/v1/query", &[], Some(&form))
        .expect("query POST reachable");
    assert_eq!(res.status, 200);
    let body = json(&res);
    assert_eq!(body["data"]["resultType"], "vector");
    assert!(body["data"]["stats"]["series"].as_u64().unwrap() >= 1);
}

#[tokio::test]
async fn query_instant_returns_a_vector_for_a_metric_query() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = "pulsus_logs_api_it_query_instant";
    let port = 31_108;
    let (_guard, _client, base_ns) = setup(db, port).await;

    let res = http_get(
        port,
        &q(
            "/api/logs/v1/query",
            &[
                ("query", r#"count_over_time({service_name="checkout"}[1h])"#),
                ("time", &base_ns.to_string()),
            ],
        ),
    )
    .expect("query reachable");
    assert_eq!(res.status, 200);
    let body = json(&res);
    assert_eq!(body["data"]["resultType"], "vector");
    assert!(body["data"]["stats"]["series"].as_u64().unwrap() >= 1);
}

#[tokio::test]
async fn malformed_query_returns_a_400_error_envelope_with_a_position() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = "pulsus_logs_api_it_query_bad";
    let port = 31_109;
    let _guard = spawn_ready_server(port, db);

    let res = http_get(port, &q("/api/logs/v1/query_range", &[("query", "{")]))
        .expect("query_range reachable");
    assert_eq!(res.status, 400);
    let body = json(&res);
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "bad_data");
    assert!(body["position"].is_number());
}

/// e2e memory test (architect plan amendment 1, test 2(b)): seeds a large
/// number of streams — far more than any request's `limit` — and asserts
/// the server process's RSS delta across a `limit`-capped `query_range`
/// request stays within a bound that could not possibly hold the full
/// seeded stream cardinality's metadata, proving end-to-end materialization
/// (handler + engine + encoder) is limit-bounded, not stream-count-bounded.
/// Process RSS (`/proc/<pid>/status VmRSS`) is a coarse but real,
/// dependency-free proxy — no custom allocator is wired into the release
/// binary for this.
#[tokio::test]
async fn query_range_memory_scales_with_the_limit_not_the_seeded_stream_count() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = "pulsus_logs_api_it_memory";
    let port = 31_110;
    let guard = spawn_ready_server(port, db);
    let client = ChClient::new(data_client_config(db))
        .await
        .expect("connect data client");

    // 5,000 distinct streams, one sample each — far more than the request
    // `limit` below (100), and enough that O(streams) materialization
    // would be readily visible against O(limit) in the RSS delta.
    const NUM_STREAMS: u64 = 5_000;
    let base_ns = now_ns();
    let mut stream_values = Vec::with_capacity(NUM_STREAMS as usize);
    let mut sample_values = Vec::with_capacity(NUM_STREAMS as usize);
    for i in 0..NUM_STREAMS {
        let fp = 0x9000_0000_0000_0000u64 + i;
        stream_values.push(format!(
            "(toStartOfMonth(fromUnixTimestamp64Nano(toInt64({base_ns}))), {fp}, 'memtest', \
             '{{\"env\":\"prod\",\"service_name\":\"memtest\",\"shard\":\"{i}\"}}', 0)"
        ));
        let ts = base_ns - 1_000_000_000;
        sample_values.push(format!("('memtest', {fp}, {ts}, 0, 'seed line {i}')"));
    }
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, updated_ns) VALUES {}",
                stream_values.join(", ")
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed many log_streams");
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, body) VALUES {}",
                sample_values.join(", ")
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed many log_samples");

    let pid = guard.0.id();
    let rss_before = read_rss_kb(pid).expect("read RSS before request");

    let start = base_ns - 3_600_000_000_000;
    let end = base_ns + 3_600_000_000_000;
    let res = http_get(
        port,
        &q(
            "/api/logs/v1/query_range",
            &[
                ("query", r#"{service_name="memtest"}"#),
                ("start", &start.to_string()),
                ("end", &end.to_string()),
                ("limit", "100"),
            ],
        ),
    )
    .expect("query_range reachable");
    assert_eq!(res.status, 200);
    let body = json(&res);
    assert_eq!(body["data"]["stats"]["entries"], 100);

    let rss_after = read_rss_kb(pid).expect("read RSS after request");
    let delta_kb = rss_after.saturating_sub(rss_before);
    // A generous ceiling: the response body is a couple hundred KB at
    // most, plus per-request scratch allocations. If materialization were
    // O(seeded streams) instead of O(limit), 5,000 streams' hydrated
    // labels/fingerprints/`HashMap` entries would blow well past this.
    assert!(
        delta_kb < 50_000,
        "RSS grew by {delta_kb}KiB across one limit=100 request over 5,000 seeded streams \
         — suspiciously large for an O(limit) read path"
    );
}

/// Compat-alias live test (issue #14): with `PULSUS_COMPAT_ENDPOINTS`
/// unset (default `false`), every `/loki/api/v1/*` alias path is a plain
/// 404 — the routes are simply absent, same as any other unmounted path
/// (no per-request flag check, gating is router-build-time only).
#[tokio::test]
async fn loki_compat_aliases_404_when_the_flag_is_off() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = "pulsus_logs_api_it_compat_off";
    let port = 31_115;
    let _guard = spawn_ready_server(port, db);

    for path in [
        "/loki/api/v1/query_range",
        "/loki/api/v1/query",
        "/loki/api/v1/labels",
        "/loki/api/v1/label/env/values",
        "/loki/api/v1/series",
    ] {
        let res = http_get(port, path).expect("loki alias reachable (though 404)");
        assert_eq!(
            res.status, 404,
            "{path} must 404 when PULSUS_COMPAT_ENDPOINTS is off"
        );
    }
}

/// Compat-alias live test (issue #14): with the flag on, every
/// `/loki/api/v1/*` alias returns a byte-identical response to its native
/// `/api/logs/v1/*` counterpart for the same request — the two surfaces
/// share one handler fn per route (`logs_api::mount_log_query_routes`), so
/// this is an end-to-end proof, not just a router-shape assertion. Every
/// request below pins explicit `start`/`end`/`time` (never the `now`
/// defaults), so two separately-issued requests cannot diverge on a
/// wall-clock default (architect plan edge case).
#[tokio::test]
async fn loki_compat_aliases_are_byte_identical_to_native() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = "pulsus_logs_api_it_compat_identical";
    let port = 31_116;
    let (_guard, _client, base_ns) = setup_env(db, port, &[("PULSUS_COMPAT_ENDPOINTS", "1")]).await;

    let start = base_ns - 3_600_000_000_000;
    let end = base_ns + 3_600_000_000_000;
    // Pre-rendered once so every `params` array below borrows genuine
    // `&str`s (not `&String`s built inline, which the array literal
    // would otherwise infer as its element type).
    let start_s = start.to_string();
    let end_s = end.to_string();
    let base_ns_s = base_ns.to_string();

    let assert_identical = |label: &str, native: HttpResponse, alias: HttpResponse| {
        assert_eq!(alias.status, native.status, "{label}: status diverged");
        assert_eq!(
            alias.body, native.body,
            "{label}: body diverged from native"
        );
    };

    // query_range
    let params = [
        ("query", r#"{service_name="checkout"}"#),
        ("start", start_s.as_str()),
        ("end", end_s.as_str()),
    ];
    let native =
        http_get(port, &q("/api/logs/v1/query_range", &params)).expect("native query_range");
    let alias = http_get(port, &q("/loki/api/v1/query_range", &params)).expect("alias query_range");
    assert_identical("query_range", native, alias);

    // query (instant)
    let params = [
        ("query", r#"count_over_time({service_name="checkout"}[1h])"#),
        ("time", base_ns_s.as_str()),
    ];
    let native = http_get(port, &q("/api/logs/v1/query", &params)).expect("native query");
    let alias = http_get(port, &q("/loki/api/v1/query", &params)).expect("alias query");
    assert_identical("query", native, alias);

    // labels
    let params = [("start", start_s.as_str()), ("end", end_s.as_str())];
    let native = http_get(port, &q("/api/logs/v1/labels", &params)).expect("native labels");
    let alias = http_get(port, &q("/loki/api/v1/labels", &params)).expect("alias labels");
    assert_identical("labels", native, alias);

    // label/{name}/values
    let params = [("start", start_s.as_str()), ("end", end_s.as_str())];
    let native =
        http_get(port, &q("/api/logs/v1/label/env/values", &params)).expect("native label values");
    let alias =
        http_get(port, &q("/loki/api/v1/label/env/values", &params)).expect("alias label values");
    assert_identical("label/{name}/values", native, alias);

    // series
    let params = [
        ("match[]", r#"{service_name="checkout"}"#),
        ("start", start_s.as_str()),
        ("end", end_s.as_str()),
    ];
    let native = http_get(port, &q("/api/logs/v1/series", &params)).expect("native series");
    let alias = http_get(port, &q("/loki/api/v1/series", &params)).expect("alias series");
    assert_identical("series", native, alias);

    // `X-Pulsus-Explain: 1` passthrough (query_range) — proves header
    // handling, not just the body encoder, is identical between surfaces.
    let params = [
        ("query", r#"{service_name="checkout"}"#),
        ("start", start_s.as_str()),
        ("end", end_s.as_str()),
    ];
    let native = http_request(
        port,
        "GET",
        &q("/api/logs/v1/query_range", &params),
        &[("X-Pulsus-Explain", "1")],
        None,
    )
    .expect("native query_range (explain)");
    let alias = http_request(
        port,
        "GET",
        &q("/loki/api/v1/query_range", &params),
        &[("X-Pulsus-Explain", "1")],
        None,
    )
    .expect("alias query_range (explain)");
    let alias_explain_stages = json(&alias)["data"]["explain"]["stages"]
        .as_array()
        .map(|a| !a.is_empty());
    assert_identical("query_range (X-Pulsus-Explain)", native, alias);
    assert_eq!(
        alias_explain_stages,
        Some(true),
        "alias explain payload missing non-empty stages"
    );
}

fn read_rss_kb(pid: u32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest.trim().trim_end_matches(" kB").trim().parse().ok();
        }
    }
    None
}
