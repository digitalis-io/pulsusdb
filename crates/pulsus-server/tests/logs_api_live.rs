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

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use flate2::read::GzDecoder;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_read::logql::sql::{self, ScanLowerBound, TimeWindow};

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping when
/// the gate is absent in a live CI job, so a lost `env:` block reddens the
/// build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
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

/// Raw-bytes response variant of [`HttpResponse`] (issue #24): `body` is the
/// dechunked, and — when the response carries `Content-Encoding: gzip` —
/// gzip-decoded, exact bytes. Needed for the gzip live coverage below,
/// where the whole point is comparing decompressed bytes directly against
/// an identity-encoding response's bytes, never through a lossy UTF-8
/// rendering of either side (a real gzip stream is not valid UTF-8, so
/// [`http_request`]'s `String` body cannot represent it before decoding).
struct HttpResponseBytes {
    status: u16,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

/// Issues one raw HTTP/1.1 request over loopback and returns the response
/// with its exact body bytes (see [`HttpResponseBytes`]). `body` is
/// form-urlencoded content when `Some` (POST); `None` sends no body (GET).
/// [`http_request`] is a thin, lossy-`String` wrapper around this.
fn http_request_bytes(
    port: u16,
    method: &str,
    path: &str,
    extra_headers: &[(&str, &str)],
    body: Option<&str>,
) -> Option<HttpResponseBytes> {
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
    let dechunked = if headers
        .get("transfer-encoding")
        .is_some_and(|v| v == "chunked")
    {
        dechunk(raw_body)
    } else {
        raw_body.to_vec()
    };

    // Gzip-decode when the server negotiated it (`Accept-Encoding: gzip` in
    // `extra_headers`) — issue #24's fix point: this used to panic the
    // request task instead of ever reaching a well-formed gzip response.
    let body = if headers.get("content-encoding").is_some_and(|v| v == "gzip") {
        let mut decoded = Vec::new();
        GzDecoder::new(&dechunked[..])
            .read_to_end(&mut decoded)
            .ok()?;
        decoded
    } else {
        dechunked
    };

    Some(HttpResponseBytes {
        status,
        headers,
        body,
    })
}

/// Issues one raw HTTP/1.1 request over loopback. `body` is form-urlencoded
/// content when `Some` (POST); `None` sends no body (GET). None of this
/// suite's pre-existing callers send `Accept-Encoding`, so they never
/// receive a gzip body and this lossy `String` rendering is unaffected by
/// issue #24's gzip coverage (which uses [`http_request_bytes`] instead).
fn http_request(
    port: u16,
    method: &str,
    path: &str,
    extra_headers: &[(&str, &str)],
    body: Option<&str>,
) -> Option<HttpResponse> {
    let res = http_request_bytes(port, method, path, extra_headers, body)?;
    Some(HttpResponse {
        status: res.status,
        headers: res.headers,
        body: String::from_utf8_lossy(&res.body).into_owned(),
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
/// computes the one genuinely dynamic value — the emitted grid point —
/// the same way the server does (issue #227: the start-anchored sliding
/// grid, `base_ns` itself) rather than approximating it: this is a real
/// byte-exact comparison, not a normalized one.
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
    // Issue #227 (sliding windows): the start-anchored grid is
    // {window_start, base_ns, window_end}; only `t = base_ns`'s window
    // `(base_ns - 1h, base_ns]` contains the 3 seeded samples (at
    // `base_ns - 3s..-1s`), the other two windows are empty ⇒ gaps.
    let point_secs = base_ns / 1_000_000_000;

    let form = format!(
        "query={}&start={window_start}&end={window_end}&step=3600s",
        urlencode(r#"count_over_time({service_name="checkout"}[1h])"#)
    );
    let res = http_request(port, "POST", "/api/logs/v1/query_range", &[], Some(&form))
        .expect("query_range POST reachable");
    assert_eq!(res.status, 200);

    // Both seeded fingerprints' 3 samples land in exactly one sliding
    // window each (well inside the window, far from its edges); `env`
    // sorts "prod" before "staging" (encode.rs's label-set ordering).
    let expected = format!(
        "{{\"status\":\"success\",\"data\":{{\"resultType\":\"matrix\",\"result\":[\
         {{\"metric\":{{\"env\":\"prod\",\"service_name\":\"checkout\"}},\"values\":[[{point_secs}.000,\"3\"]]}},\
         {{\"metric\":{{\"env\":\"staging\",\"service_name\":\"checkout\"}},\"values\":[[{point_secs}.000,\"3\"]]}}\
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
    // Issue #227 (sliding windows): the samples land in the one window
    // ending at the grid point `base_ns` (see the metric golden above).
    let point_secs = base_ns / 1_000_000_000;

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
    // Issue #227: a range aggregation slides raw — the explain reports the
    // PK-ordered sliding scan over `log_samples`, its lower bound widened
    // a full `[1h]` range (== POST_GOLDEN_STEP_NS here) before
    // `window_start` so the first grid point sees its whole lookback.
    let metric_sql = sql::metric_raw_samples_sliding(
        "log_samples",
        &["'checkout'".to_string()],
        &[FP_A],
        TimeWindow {
            start_ns: window_start - POST_GOLDEN_STEP_NS,
            end_ns: window_end,
        },
        ScanLowerBound::Exclusive,
        &[],
    );
    let routing_reason = "raw: sliding-window range aggregation (issue #227)".to_string();

    let mut expected = String::new();
    expected.push_str(r#"{"status":"success","data":{"resultType":"matrix","result":["#);
    expected.push_str(&format!(
        r#"{{"metric":{{"env":"prod","service_name":"checkout"}},"values":[[{point_secs}.000,"3"]]}}"#
    ));
    expected.push_str(r#"],"stats":{"series":1},"explain":{"#);
    expected.push_str(&format!(
        r#""result_type":"matrix","routing":{{"chosen":"raw","reason":{}}},"stages":["#,
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

/// Gzip live coverage (issue #24): `Unfold` panicked when re-polled after
/// EOF, which `tower_http::compression::CompressionLayer`'s gzip encoder
/// does — aborting the request task on every real `Accept-Encoding: gzip`
/// request (Grafana's Loki client always sends one). This drives every
/// endpoint on both surfaces (native + `/loki` alias, flag on) with
/// `Accept-Encoding: gzip`, asserting each is reachable, 200, actually
/// gzip-encoded, and gzip-decodes byte-identical to the same request with
/// no `Accept-Encoding` — proof through the real compression layer and the
/// real server process, not just `encode.rs`'s in-process
/// `CompressionLayer` unit coverage. Then fires a concurrent burst of gzip
/// requests across all ten route entries (five endpoints x two surfaces):
/// the panic this issue fixes aborted the whole request task, so
/// overlapping in-flight requests is the sharpest end-to-end proof the
/// server process itself never crashes or drops a sibling request.
#[tokio::test]
async fn gzip_accept_encoding_is_byte_identical_and_never_panics_across_all_endpoints_and_surfaces()
{
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = "pulsus_logs_api_it_gzip";
    let port = 31_117;
    let (_guard, _client, base_ns) = setup_env(db, port, &[("PULSUS_COMPAT_ENDPOINTS", "1")]).await;

    let start = base_ns - 3_600_000_000_000;
    let end = base_ns + 3_600_000_000_000;
    let start_s = start.to_string();
    let end_s = end.to_string();
    let base_ns_s = base_ns.to_string();

    // One (method, route, params) per `/api/logs/v1` endpoint (docs/api.md
    // §2) — mirrored across both surfaces' prefixes below.
    type Endpoint<'a> = (&'a str, &'a str, Vec<(&'a str, &'a str)>);
    let endpoints: Vec<Endpoint> = vec![
        (
            "GET",
            "query_range",
            vec![
                ("query", r#"{service_name="checkout"}"#),
                ("start", start_s.as_str()),
                ("end", end_s.as_str()),
            ],
        ),
        (
            "GET",
            "query",
            vec![
                ("query", r#"count_over_time({service_name="checkout"}[1h])"#),
                ("time", base_ns_s.as_str()),
            ],
        ),
        (
            "GET",
            "labels",
            vec![("start", start_s.as_str()), ("end", end_s.as_str())],
        ),
        (
            "GET",
            "label/env/values",
            vec![("start", start_s.as_str()), ("end", end_s.as_str())],
        ),
        (
            "GET",
            "series",
            vec![
                ("match[]", r#"{service_name="checkout"}"#),
                ("start", start_s.as_str()),
                ("end", end_s.as_str()),
            ],
        ),
    ];

    for prefix in ["/api/logs/v1", "/loki/api/v1"] {
        for ep in &endpoints {
            let method = ep.0;
            let route = ep.1;
            let params = &ep.2;
            let path = q(&format!("{prefix}/{route}"), params);

            let identity = http_request_bytes(port, method, &path, &[], None)
                .unwrap_or_else(|| panic!("{prefix}/{route}: identity request reachable"));
            assert_eq!(identity.status, 200, "{prefix}/{route}: identity status");

            let gzip = http_request_bytes(
                port,
                method,
                &path,
                &[("Accept-Encoding", "gzip")],
                None,
            )
            .unwrap_or_else(|| {
                panic!(
                    "{prefix}/{route}: gzip request reachable (issue #24 regression: must not \
                     panic the request task)"
                )
            });
            assert_eq!(gzip.status, 200, "{prefix}/{route}: gzip status");
            assert_eq!(
                gzip.headers.get("content-encoding").map(String::as_str),
                Some("gzip"),
                "{prefix}/{route}: must actually be gzip-encoded for this assertion to be \
                 meaningful"
            );
            assert_eq!(
                gzip.body, identity.body,
                "{prefix}/{route}: gzip-decoded body must be byte-identical to identity"
            );
        }
    }

    // Concurrent burst across all ten route entries (both surfaces): each
    // thread issues its own gzip request over its own TCP connection, and a
    // `Barrier` lines every thread up so requests actually overlap
    // in-flight on the server instead of serializing incidentally.
    let mut requests: Vec<(String, String)> = Vec::new();
    for prefix in ["/api/logs/v1", "/loki/api/v1"] {
        for ep in &endpoints {
            requests.push((ep.0.to_string(), q(&format!("{prefix}/{}", ep.1), &ep.2)));
        }
    }
    let barrier = Arc::new(Barrier::new(requests.len()));
    let handles: Vec<_> = requests
        .into_iter()
        .map(|(method, path)| {
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                http_request_bytes(port, &method, &path, &[("Accept-Encoding", "gzip")], None)
            })
        })
        .collect();

    for handle in handles {
        let res = handle
            .join()
            .expect("request thread must not panic")
            .expect(
                "concurrent gzip request must be reachable and complete without panicking the \
             request task",
            );
        assert_eq!(res.status, 200, "concurrent gzip request must succeed");
        assert_eq!(
            res.headers.get("content-encoding").map(String::as_str),
            Some("gzip")
        );
    }
}

/// Issue M6-09 AC5: the fan-out pipeline path end to end — seed JSON
/// bodies, run `| json | status = "500" | line_format "{{.method}}"`, and
/// assert (a) only matching lines survive, (b) line bodies are
/// reformatted, (c) result streams split by parsed-label set (one source
/// stream fans out into one result stream per distinct `method`), with
/// parsed labels present in each stream's label object.
#[tokio::test]
async fn query_range_fan_out_pipeline_filters_reformats_and_relabels_streams() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = "pulsus_logs_api_it_pipeline_fanout";
    let port = 31_118;
    // Exact-count assertions below: `log_samples` is a plain MergeTree, so
    // a stale database from a previous run would double the seeded rows
    // (see `drop_database`'s doc comment).
    drop_database(db).await;
    let guard = spawn_ready_server(port, db);
    let client = ChClient::new(data_client_config(db))
        .await
        .expect("connect data client");
    let base_ns = now_ns();

    // One stream, five JSON lines: three status=500 (methods GET/GET/PUT),
    // one status=200, one non-JSON line (gets `__error__`, then dropped by
    // the status filter).
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, updated_ns) VALUES \
                 (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({base_ns}))), {FP_A}, 'checkout', \
                 '{{\"env\":\"prod\",\"service_name\":\"checkout\"}}', 0)"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_streams");
    let bodies = [
        r#"{"method":"GET","status":"500"}"#,
        r#"{"method":"GET","status":"500"}"#,
        r#"{"method":"PUT","status":"500"}"#,
        r#"{"method":"GET","status":"200"}"#,
        "plain text line",
    ];
    let values: Vec<String> = bodies
        .iter()
        .enumerate()
        .map(|(i, body)| {
            let ts = base_ns - (bodies.len() as i64 - i as i64) * 1_000_000_000;
            format!(
                "('checkout', {FP_A}, {ts}, 0, '{}')",
                body.replace('\'', "\\'")
            )
        })
        .collect();
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, body) VALUES {}",
                values.join(", ")
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_samples");

    let start = base_ns - 3_600_000_000_000;
    let end = base_ns + 3_600_000_000_000;
    let res = http_get(
        port,
        &q(
            "/api/logs/v1/query_range",
            &[
                (
                    "query",
                    r#"{service_name="checkout"} | json | status = "500" | line_format "{{.method}}""#,
                ),
                ("start", &start.to_string()),
                ("end", &end.to_string()),
                ("limit", "100"),
                ("direction", "forward"),
            ],
        ),
    )
    .expect("query_range reachable");
    assert_eq!(res.status, 200, "body: {}", res.body);
    let body = json(&res);
    assert_eq!(body["data"]["resultType"], "streams");
    let streams = body["data"]["result"].as_array().expect("streams array");

    // (c) fan-out: one result stream per final parsed-label set.
    assert_eq!(
        streams.len(),
        2,
        "expected a GET stream and a PUT stream, got: {streams:?}"
    );
    let mut by_method: HashMap<String, &serde_json::Value> = HashMap::new();
    for s in streams {
        let labels = &s["stream"];
        assert_eq!(labels["env"], "prod", "base labels must be preserved");
        assert_eq!(labels["service_name"], "checkout");
        assert_eq!(
            labels["status"], "500",
            "parsed labels must be stream-level"
        );
        let method = labels["method"].as_str().expect("method label").to_string();
        by_method.insert(method, s);
    }

    // (a)+(b): only matching lines survive, and bodies are reformatted to
    // the template output.
    let get_values = by_method["GET"]["values"].as_array().unwrap();
    assert_eq!(get_values.len(), 2);
    for v in get_values {
        assert_eq!(v[1], "GET", "line must be rewritten by line_format");
    }
    let put_values = by_method["PUT"]["values"].as_array().unwrap();
    assert_eq!(put_values.len(), 1);
    assert_eq!(put_values[0][1], "PUT");
    assert_eq!(
        body["data"]["stats"]["entries"], 3,
        "the status=200 and non-JSON lines must not survive"
    );

    drop(guard);
}

/// Issue #99 end-to-end: a stage-erroring pipeline surfaces BOTH
/// `__error__` and its byte-exact `__error_details__` companion label in
/// the streams response `labels_json` over the wire, and the
/// `| __error__ != ""` / `| __error__ = ""` filters interact with the new
/// label (keep both / drop the errored stream). Streams-path only — the
/// detail is set solely on the streams branch (the metric path stays
/// byte-identical; hermetic goldens pin that). Seeds two dedicated streams
/// (a non-JSON line and a `n=oops` logfmt line) so each query scopes to its
/// own error class.
#[tokio::test]
async fn query_range_surfaces_error_details_label_end_to_end() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = "pulsus_logs_api_it_error_details";
    let port = 31_119;
    drop_database(db).await;
    let guard = spawn_ready_server(port, db);
    let client = ChClient::new(data_client_config(db))
        .await
        .expect("connect data client");
    let base_ns = now_ns();

    // Two streams: `errjson` carries a top-level non-object line (fails
    // `| json`); `errnum` carries a logfmt line whose `n` cannot convert
    // (fails a numeric label filter).
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, updated_ns) VALUES \
                 (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({base_ns}))), {FP_A}, 'errsvc', \
                 '{{\"service_name\":\"errjson\"}}', 0), \
                 (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({base_ns}))), {FP_B}, 'errsvc', \
                 '{{\"service_name\":\"errnum\"}}', 0)"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_streams");
    let ts = base_ns - 1_000_000_000;
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, body) VALUES \
                 ('errsvc', {FP_A}, {ts}, 0, 'not a json line'), \
                 ('errsvc', {FP_B}, {ts}, 0, 'n=oops')"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_samples");

    let start = base_ns - 3_600_000_000_000;
    let end = base_ns + 3_600_000_000_000;
    let start_s = start.to_string();
    let end_s = end.to_string();

    // The single stream a streams `query_range` response carries.
    let one_stream = |query: &str| -> serde_json::Value {
        let res = http_get(
            port,
            &q(
                "/api/logs/v1/query_range",
                &[
                    ("query", query),
                    ("start", start_s.as_str()),
                    ("end", end_s.as_str()),
                    ("limit", "100"),
                ],
            ),
        )
        .expect("query_range reachable");
        assert_eq!(res.status, 200, "body: {}", res.body);
        let body = json(&res);
        assert_eq!(body["data"]["resultType"], "streams");
        let streams = body["data"]["result"].as_array().expect("streams array");
        assert_eq!(streams.len(), 1, "expected exactly one stream: {streams:?}");
        streams[0]["stream"].clone()
    };

    // JSONParserErr: both labels present, detail byte-exact.
    let labels = one_stream(r#"{service_name="errjson"} | json"#);
    assert_eq!(labels["__error__"], "JSONParserErr");
    assert_eq!(
        labels["__error_details__"],
        "Value looks like object, but can't find closing '}' symbol",
    );

    // LabelFilterErr: the byte-exact Go strconv.ParseFloat detail.
    let labels = one_stream(r#"{service_name="errnum"} | logfmt | n > 5"#);
    assert_eq!(labels["__error__"], "LabelFilterErr");
    assert_eq!(
        labels["__error_details__"],
        r#"strconv.ParseFloat: parsing "oops": invalid syntax"#,
    );

    // `| __error__ != ""` keeps the errored stream, both labels intact.
    let labels = one_stream(r#"{service_name="errjson"} | json | __error__ != """#);
    assert_eq!(labels["__error__"], "JSONParserErr");
    assert_eq!(
        labels["__error_details__"],
        "Value looks like object, but can't find closing '}' symbol",
    );

    // `| __error__ = ""` drops it — no streams at all.
    let res = http_get(
        port,
        &q(
            "/api/logs/v1/query_range",
            &[
                (
                    "query",
                    r#"{service_name="errjson"} | json | __error__ = """#,
                ),
                ("start", start_s.as_str()),
                ("end", end_s.as_str()),
                ("limit", "100"),
            ],
        ),
    )
    .expect("query_range reachable");
    assert_eq!(res.status, 200);
    let body = json(&res);
    assert_eq!(body["data"]["resultType"], "streams");
    assert!(
        body["data"]["result"]
            .as_array()
            .is_some_and(|s| s.is_empty()),
        "| __error__ = \"\" must drop the errored stream: {}",
        res.body
    );

    drop(guard);
}

/// Issue #343 — `offset` END TO END, over the wire, against real
/// ClickHouse.
///
/// **The layer matters here more than usual.** This construct's whole
/// history is a verdict that was true one layer up and false where the
/// user stands: it parsed while the planner refused it, and the #339
/// census had to carry a parse column and a wire column separately to say
/// so. A hermetic test cannot close that gap — an INSTANT window is a SQL
/// predicate, so the pure evaluator never sees it — which is exactly why
/// the instant leg below is here and not in `b19_offset.test`.
///
/// Two claims, each with its own control:
///
/// 1. **The data window moves.** One batch 30s back and one at `now`; the
///    same `[10s]` selector answers 3 without an offset and 5 with
///    `offset 30s`. A `[10m]` control window spans both and answers 8, so
///    the 3/5 split is the window moving rather than rows missing.
/// 2. **The emitted timestamps do NOT move.** The two range queries share
///    one grid and must report the same instant, differing only in value.
///
/// **What it does NOT cover, established by mutation rather than
/// asserted:** the SIGN. Every offset here is positive, so replacing the
/// planner's `t - d` with `t - |d|` leaves this test green — it was tried.
/// The sign is owned by `plan.rs`'s
/// `a_negative_offset_shifts_the_window_forward_not_back` and by
/// `b19_offset.test`'s negative rows, both of which that mutant reddens.
/// Reading this test as end-to-end coverage of `offset` would be reading
/// it for more than it measures.
#[tokio::test]
async fn offset_shifts_the_data_window_and_not_the_reported_timestamps() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = "pulsus_logs_api_it_offset";
    let port = 31_120;
    // Dropped first: every count below is EXACT, so a previous run's rows
    // surviving in the window would inflate them (the sibling tests here
    // assert `>= 1` and do not care).
    drop_database(db).await;
    let (_guard, client, base_ns) = setup(db, port).await;

    // `setup` seeds 3 samples on the prod stream at base-3s/-2s/-1s. Add 5
    // more at base-35s .. base-39s — STRICTLY inside `(base-40s,
    // base-30s]`, the window `[10s] offset 30s` evaluates. Not on the
    // -40s edge: that bound is exclusive (Loki's `(t-range, t]`), so a
    // sample placed there would be excluded and this would read as an
    // off-by-one in the shift rather than in the fixture.
    let mut values = Vec::new();
    for i in 1..=5i64 {
        let ts = base_ns - 34_000_000_000 - i * 1_000_000_000;
        values.push(format!("('checkout', {FP_A}, {ts}, 0, 'prod old {i}')"));
    }
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, body) \
                 VALUES {}",
                values.join(", ")
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed the older batch");

    let instant = |query: &str| -> String {
        let res = http_get(
            port,
            &q(
                "/api/logs/v1/query",
                &[("query", query), ("time", &base_ns.to_string())],
            ),
        )
        .expect("query reachable");
        assert_eq!(res.status, 200, "{query}: {}", res.body);
        let body = json(&res);
        assert_eq!(body["data"]["resultType"], "vector", "{query}");
        body["data"]["result"][0]["value"][1]
            .as_str()
            .unwrap_or_else(|| panic!("{query}: no vector value in {}", res.body))
            .to_string()
    };

    // The instant leg — the half no hermetic test can reach.
    assert_eq!(
        instant(r#"count_over_time({env="prod", service_name="checkout"}[10s])"#),
        "3",
        "the recent batch"
    );
    assert_eq!(
        instant(r#"count_over_time({env="prod", service_name="checkout"}[10s] offset 30s)"#),
        "5",
        "the window moved back 30s onto the older batch"
    );
    assert_eq!(
        instant(r#"count_over_time({env="prod", service_name="checkout"}[10s] offset 0s)"#),
        "3",
        "`offset 0s` is the identity"
    );
    // THE CONTROL: both batches are present and visible, so 3-vs-5 above
    // is the window moving, not rows missing.
    assert_eq!(
        instant(r#"count_over_time({env="prod", service_name="checkout"}[10m])"#),
        "8",
        "the control window spans both batches"
    );

    // The range leg: same grid, so the timestamps must be identical.
    let range = |query: &str| -> (serde_json::Value, String) {
        let res = http_get(
            port,
            &q(
                "/api/logs/v1/query_range",
                &[
                    ("query", query),
                    ("start", &base_ns.to_string()),
                    ("end", &base_ns.to_string()),
                    ("step", "10s"),
                ],
            ),
        )
        .expect("query_range reachable");
        assert_eq!(res.status, 200, "{query}: {}", res.body);
        let body = json(&res);
        assert_eq!(body["data"]["resultType"], "matrix", "{query}");
        let point = &body["data"]["result"][0]["values"][0];
        (
            point[0].clone(),
            point[1]
                .as_str()
                .unwrap_or_else(|| panic!("{query}: no matrix point in {}", res.body))
                .to_string(),
        )
    };
    let (plain_ts, plain_v) =
        range(r#"count_over_time({env="prod", service_name="checkout"}[10s])"#);
    let (shifted_ts, shifted_v) =
        range(r#"count_over_time({env="prod", service_name="checkout"}[10s] offset 30s)"#);
    assert_eq!(plain_v, "3");
    assert_eq!(shifted_v, "5", "the value moved with the window");
    assert_eq!(
        plain_ts, shifted_ts,
        "the offset must move the DATA window, never the reported grid"
    );
}

/// Issue #343 (boundary fix), **AC 5b/5c — the WIRE**: an offset that
/// shifts the evaluation domain past `i64::MAX` must issue NO scan at all.
///
/// **The layer is the whole finding.** A planner-field assertion, or the
/// rendered-SQL snapshot in `pulsus-read/tests/sql_snapshots.rs`, both sit
/// above the place the user stands. What shipped was a plan that
/// SATURATED onto the `i64` rail, and its observable was a `metric_read`
/// stage carrying a scan over a span the request never named. So this
/// asserts on the explain payload the server actually returns: **no
/// `metric_read` stage exists**, the stage that IS reported carries the
/// degenerate window's signature month, and the result is empty.
///
/// **Under the 5-year caps the extreme lives in the request's POSITION.**
/// Every duration below is an ordinary hour: `offset`, `[range]` and the
/// query span are each capped at 43,800 h, and nothing bounds where on the
/// axis `time` sits, so a request one hour below `i64::MAX` shifted one
/// hour forward is how the domain is left now.
///
/// **The control varies the position, not the offset, and that is forced.**
/// An off-axis request is by construction within 5 years of a rail, so its
/// partition months can never overlap a month that holds data — stage 1
/// resolves nothing and no `metric_read` follows for that reason alone.
/// The control is therefore an ordinary request with an ordinary offset,
/// which still does what a control must: it fails if the explain plumbing
/// is deleted or every plan is made degenerate.
#[tokio::test]
async fn an_offset_past_the_timestamp_axis_issues_no_scan_at_all() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = "pulsus_logs_api_it_offset_domain_edge";
    let port = 31_122;
    let (_guard, _client, base_ns) = setup(db, port).await;

    // One hour below `i64::MAX`: `T - (-1h)` leaves the axis by 1 ns.
    const AT_RAIL_NS: i64 = i64::MAX - 3_600_000_000_000 + 1;

    let explain = |query: &str, at_ns: i64| -> serde_json::Value {
        let res = http_request(
            port,
            "GET",
            &q(
                "/api/logs/v1/query",
                &[("query", query), ("time", &at_ns.to_string())],
            ),
            &[("X-Pulsus-Explain", "1")],
            None,
        )
        .expect("query reachable");
        assert_eq!(res.status, 200, "{query}: {}", res.body);
        json(&res)
    };
    let stages = |body: &serde_json::Value| -> Vec<(String, String)> {
        body["data"]["explain"]["stages"]
            .as_array()
            .unwrap_or_else(|| panic!("no explain stages in {body}"))
            .iter()
            .map(|s| {
                (
                    s["name"].as_str().unwrap_or_default().to_string(),
                    s["sql"].as_str().unwrap_or_default().to_string(),
                )
            })
            .collect()
    };

    // Off the axis.
    let off_axis = explain(
        r#"count_over_time({env="prod"}[5m] offset -1h)"#,
        AT_RAIL_NS,
    );
    let off_stages = stages(&off_axis);
    assert!(
        !off_stages.iter().any(|(n, _)| n == "metric_read"),
        "a query whose window left the timestamp axis must issue no scan, \
         got stages {:?}",
        off_stages.iter().map(|(n, _)| n).collect::<Vec<_>>()
    );
    // The degenerate window's signature at the wire: `months_overlapping(0,
    // -1)` yields the single literal `'1970-01-01'`. The saturating
    // predecessor named months at the far end of the axis instead.
    let stage1 = off_stages
        .iter()
        .find(|(n, _)| n == "stage1_stream_resolution")
        .map(|(_, sql)| sql.clone())
        .unwrap_or_default();
    assert!(
        stage1.contains("month = '1970-01-01'"),
        "expected the degenerate window's partition list, got:\n{stage1}"
    );
    assert_eq!(
        off_axis["data"]["result"].as_array().map(Vec::len),
        Some(0),
        "and it answers empty: {off_axis}"
    );

    // The control: an ordinary request with an ordinary offset DOES plan a
    // scan and DOES report it.
    let on_axis = explain(r#"count_over_time({env="prod"}[10m] offset 1h)"#, base_ns);
    let on_stages = stages(&on_axis);
    assert!(
        on_stages.iter().any(|(n, _)| n == "metric_read"),
        "a representable shift must still be scanned and reported, got {:?}",
        on_stages.iter().map(|(n, _)| n).collect::<Vec<_>>()
    );
}

/// Issue #343 — the 5-year caps over the wire (owner mandate): each of the
/// three places is a `400 bad_data` echoing what the user sent, and the
/// query one nanosecond under each cap is served. Hermetic tests own the
/// arithmetic; this owns the STATUS, which is the thing a client sees.
#[tokio::test]
async fn nothing_in_a_query_may_span_more_than_five_years() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = "pulsus_logs_api_it_span_cap";
    let port = 31_123;
    let (_guard, _client, base_ns) = setup(db, port).await;
    const CAP_NS: i64 = 157_680_000_000_000_000;

    let instant = |query: &str| -> (u16, String) {
        let res = http_get(
            port,
            &q(
                "/api/logs/v1/query",
                &[("query", query), ("time", &base_ns.to_string())],
            ),
        )
        .expect("query reachable");
        (res.status, res.body)
    };
    // 1. `offset`, both directions.
    for (query, want) in [
        (r#"count_over_time({env="prod"}[5m] offset 43800h)"#, 200),
        (r#"count_over_time({env="prod"}[5m] offset -43800h)"#, 200),
        (r#"count_over_time({env="prod"}[5m] offset 43801h)"#, 400),
        (r#"count_over_time({env="prod"}[5m] offset -43801h)"#, 400),
        // 2. the `[range]` selector.
        (r#"count_over_time({env="prod"}[43800h])"#, 200),
        (r#"count_over_time({env="prod"}[43801h])"#, 400),
    ] {
        let (status, body) = instant(query);
        assert_eq!(status, want, "{query}: {body}");
        if want == 400 {
            assert!(
                body.contains("\"errorType\":\"bad_data\"") && body.contains("too long"),
                "{query}: {body}"
            );
        }
    }

    // 3. the query's own start-to-end span.
    let range = |start_ns: i64, end_ns: i64| -> (u16, String) {
        let res = http_get(
            port,
            &q(
                "/api/logs/v1/query_range",
                &[
                    ("query", r#"count_over_time({env="prod"}[5m])"#),
                    ("start", &start_ns.to_string()),
                    ("end", &end_ns.to_string()),
                    // 5 years / 43800s = 3,600 grid points, under the
                    // reference's own 11,000-point resolution fence — which
                    // would otherwise refuse a 5-year span first and hide
                    // the cap this test is for.
                    ("step", "43800s"),
                ],
            ),
        )
        .expect("query_range reachable");
        (res.status, res.body)
    };
    let (status, body) = range(base_ns - CAP_NS, base_ns);
    assert_eq!(status, 200, "exactly 5 years is served: {body}");
    let (status, body) = range(base_ns - CAP_NS - 1, base_ns);
    assert_eq!(status, 400, "one nanosecond more is refused: {body}");
    assert!(
        body.contains("\"errorType\":\"bad_data\"") && body.contains("query time range of"),
        "{body}"
    );

    // 3b. EVERY route that carries `start`/`end`, not just `query_range`.
    // `/labels` and `/label/{name}/values` are the reason this loop
    // exists: they resolve label names and values without building a plan,
    // so the first cut of the span cap — which lived in the planner — let
    // a 20-year range straight through. Those two and only those two:
    // measured by deleting the `parse_bounds` call and running this loop,
    // which is also why it collects rather than short-circuits. `/series`
    // is NOT one of them despite taking only a selector — the engine
    // builds a synthetic Range spec and calls `plan()` per selector — and
    // neither are `detected_*`, `patterns`, `stats`, `volume`. The cap now
    // sits in the one function all nine share
    // (`handlers::parse_bounds`), and this is what says so.
    let over = (base_ns - CAP_NS - 1).to_string();
    let now = base_ns.to_string();
    let at_cap = (base_ns - CAP_NS).to_string();
    let mut uncapped: Vec<String> = Vec::new();
    for (path, extra) in [
        ("/api/logs/v1/series", vec![("match[]", r#"{env="prod"}"#)]),
        ("/api/logs/v1/labels", vec![]),
        ("/api/logs/v1/label/env/values", vec![]),
        (
            "/api/logs/v1/detected_labels",
            vec![("query", r#"{env="prod"}"#)],
        ),
        (
            "/api/logs/v1/detected_fields",
            vec![("query", r#"{env="prod"}"#)],
        ),
        ("/api/logs/v1/patterns", vec![("query", r#"{env="prod"}"#)]),
        ("/api/logs/v1/stats", vec![("query", r#"{env="prod"}"#)]),
        ("/api/logs/v1/volume", vec![("query", r#"{env="prod"}"#)]),
    ] {
        let call = |start: &str| {
            let mut params: Vec<(&str, &str)> = extra.clone();
            params.push(("start", start));
            params.push(("end", now.as_str()));
            http_get(port, &q(path, &params)).expect("route reachable")
        };
        let res = call(&over);
        if res.status != 400 || !res.body.contains("query time range of") {
            uncapped.push(format!("{path} -> {} {}", res.status, res.body));
        }
        let res = call(&at_cap);
        assert_eq!(
            res.status, 200,
            "{path} must still serve exactly 5 years: {}",
            res.body
        );
    }
    // Collected, not short-circuited: the first cut of this cap covered
    // some of these routes by accident (through the planner) and missed
    // others, so a loop that stopped at the first failure would have
    // reported one name and hidden the rest.
    assert!(
        uncapped.is_empty(),
        "these routes carry start/end and do NOT cap the span:\n{}",
        uncapped.join("\n")
    );
}

/// Issue #343 — `variants(...)` reads each variant's OWN offset window,
/// INTERSECTED with the common range's single shared scan.
///
/// **The `[63500ms]` case is the reason this test exists.** Every other
/// arrangement is satisfied by more than one model of what
/// `variants(...)` does; this one is not. The common range reaches two of
/// the three older lines and the variant's shifted window wants all
/// three, so:
///
/// | model | predicts |
/// |---|---|
/// | shared scan, then per-variant filter | **2** |
/// | a scan per variant | 3 |
/// | no coverage of a divergent offset | 0 |
/// | refusing a divergent offset | 400 |
///
/// The pinned v3.7.4 reference answers **2** (seeded store; the common
/// range alone answers 5 and the variant window unconstrained answers 3,
/// so 2 is neither). PulsusDB must answer 2 for the same reason it does:
/// one scan, each variant filtering its own window inside it.
///
/// It is here and not in `b19_offset.test` because that intersection IS
/// the SQL scan predicate. The hermetic runner hands the evaluator every
/// loaded row, so it answers 3 for this query and empty for none of them
/// — measured before moving it, not assumed.
///
/// The empty rows are kept deliberately: they are the ones that would go
/// green if the shared scan were ever widened to COVER the union of the
/// variants' shifted windows. That widening reads like a fix and is a
/// divergence — the reference answers empty there, not 3.
///
/// **Which rows actually discriminate, measured not assumed.** Reading a
/// variant's window off the COMMON range's offset instead of its own —
/// the one-token slip this whole test exists to catch — changes:
///
/// | row | correct | that slip |
/// |---|---|---|
/// | `[63500ms]` discriminator | 2 | 7 |
/// | wide-cover `[70s]` | 3 | 7 |
/// | empty, disjoint | no series | 7 |
/// | empty, mirror | no series | 3 |
/// | two variants, one offset | variant 1 only | both |
///
/// and leaves the rest identical. **`EMPTY#3` (`offset 60s` variant under
/// an `offset 30s` common range) does NOT catch it** — the slip moves that
/// variant's window to the common range's, which is also empty, so both
/// answer nothing. It is kept because it still separates this model from a
/// union-widened scan, which would answer 3 there; it is simply not
/// evidence about the offset the variant reads. Said plainly rather than
/// left to be assumed from the row's presence.
///
/// The two batches are deliberately different sizes (3 old, 7 recent). At
/// 3-and-3, which is where this fixture started, the wide-cover row reads
/// 3 under both the correct code and the slip and catches nothing.
#[tokio::test]
async fn variants_reads_each_variants_offset_window_intersected_with_the_shared_scan() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = "pulsus_logs_api_it_variants_offset";
    let port = 31_121;
    // Exact counts throughout, so a previous run's rows must not survive.
    drop_database(db).await;
    let (_guard, client, base_ns) = setup(db, port).await;

    // THE REFERENCE PROBE'S FIXTURE, on a compressed timescale (seconds
    // for its minutes, so the whole thing stays inside one partition
    // month): **3 old lines** at base-64s/-63s/-62s and **7 recent** at
    // base-5s..base-1s. `setup` already seeded 3 of the recent ones
    // (base-3s/-2s/-1s), so 4 more join them.
    //
    // The two batches are DIFFERENT SIZES on purpose. With 3 and 3 — the
    // shape this started as — several rows below are satisfied by reading
    // the wrong batch, and the mutant that reads the wrong window still
    // passes them. Every number asserted here is the number the pinned
    // v3.7.4 container answered for the same shape.
    let mut values = Vec::new();
    for i in 62..=64i64 {
        let ts = base_ns - i * 1_000_000_000;
        values.push(format!("('checkout', {FP_A}, {ts}, 0, 'prod old {i}')"));
    }
    for i in 1..=4i64 {
        let ts = base_ns - 4_000_000_000 - i * 100_000_000;
        values.push(format!("('checkout', {FP_A}, {ts}, 0, 'prod new {i}')"));
    }
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, body) \
                 VALUES {}",
                values.join(", ")
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed the older batch");

    // Variant values by `__variant__` index; an absent index means the
    // variant produced no series at all, which is a real answer here.
    let variants = |query: &str| -> BTreeMap<String, String> {
        let res = http_get(
            port,
            &q(
                "/api/logs/v1/query",
                &[("query", query), ("time", &base_ns.to_string())],
            ),
        )
        .expect("query reachable");
        assert_eq!(res.status, 200, "{query}: {}", res.body);
        let body = json(&res);
        let mut out = BTreeMap::new();
        for s in body["data"]["result"].as_array().expect("result array") {
            let idx = s["metric"]["__variant__"]
                .as_str()
                .expect("__variant__ label")
                .to_string();
            out.insert(idx, s["value"][1].as_str().expect("value").to_string());
        }
        out
    };
    let one = |query: &str| -> Option<String> { variants(query).get("0").cloned() };

    let sel = r#"{env="prod", service_name="checkout"}"#;

    // Baselines, so the fixture is established before anything is read
    // from a variants query.
    assert_eq!(
        one(&format!(
            r#"variants(count_over_time({sel}[5s])) of ({sel}[5s])"#
        )),
        Some("7".to_string()),
        "the recent batch"
    );
    assert_eq!(
        one(&format!(
            r#"variants(count_over_time({sel}[70s])) of ({sel}[70s])"#
        )),
        Some("10".to_string()),
        "the wide control spans both batches"
    );

    // THE DISCRIMINATOR. Common `[63500ms]` reaches base-63s and base-62s
    // but NOT base-64s; the variant's `(base-65s, base-60s]` wants all
    // three. The intersection is 2.
    assert_eq!(
        one(&format!(
            r#"variants(count_over_time({sel}[5s] offset 60s)) of ({sel}[63500ms])"#
        )),
        Some("2".to_string()),
        "the variant reads its shifted window INTERSECTED with the shared scan"
    );

    // The two controls that make that 2 mean something: the common range
    // alone, and the variant's window unconstrained.
    assert_eq!(
        one(&format!(
            r#"variants(count_over_time({sel}[63500ms])) of ({sel}[63500ms])"#
        )),
        Some("9".to_string()),
        "the common range alone"
    );
    assert_eq!(
        one(&format!(
            r#"variants(count_over_time({sel}[5s] offset 60s)) of ({sel}[70s])"#
        )),
        Some("3".to_string()),
        "a common range wide enough to cover the shifted window serves it whole \
         — 3, the OLD batch, which the recent batch's 7 cannot be mistaken for"
    );

    // EMPTY where the shared scan does not reach the shifted window. A
    // union-widened scan would answer 3 here, which the reference does not.
    assert!(
        one(&format!(
            r#"variants(count_over_time({sel}[5s] offset 60s)) of ({sel}[5s])"#
        ))
        .is_none(),
        "a variant window disjoint from the shared scan yields no series"
    );
    // The mirror image: the COMMON range carries the offset, the variant
    // does not.
    assert!(
        one(&format!(
            r#"variants(count_over_time({sel}[5s])) of ({sel}[5s] offset 60s)"#
        ))
        .is_none(),
        "the scan moved and the variant window did not"
    );
    // Both offset, differently.
    assert!(
        one(&format!(
            r#"variants(count_over_time({sel}[5s] offset 60s)) of ({sel}[5s] offset 30s)"#
        ))
        .is_none(),
        "two different offsets, neither covering the other"
    );

    // Two variants, only the first offset: one scan, one series out — the
    // shape the reference answers with variant 1 alone.
    let mixed = variants(&format!(
        r#"variants(count_over_time({sel}[5s] offset 60s), count_over_time({sel}[5s])) of ({sel}[5s])"#
    ));
    assert_eq!(mixed.get("0"), None, "the offset variant reads nothing");
    assert_eq!(
        mixed.get("1"),
        Some(&"7".to_string()),
        "its neighbour is unaffected"
    );

    // `offset 0s` on a variant is the identity against no offset at all.
    assert_eq!(
        one(&format!(
            r#"variants(count_over_time({sel}[5s] offset 0s)) of ({sel}[5s])"#
        )),
        Some("7".to_string()),
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
