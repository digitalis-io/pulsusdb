//! Live end-to-end tests for `GET /api/logs/v1/stats` (issue #74,
//! docs/api.md §2.5): spawns the real `pulsusdb` binary against a live
//! ClickHouse, seeds rows across two fingerprints and two partition
//! days, and asserts:
//! - exact `{streams,chunks,entries,bytes}` counters (chunks = the
//!   adjudicated partition-count proxy);
//! - the Tier-1 pushdown routing gate, from `X-Pulsus-Explain`'s stage
//!   SQL: no line filter → the `log_metrics_5s` rollup (zero body
//!   reads); a line filter → a `log_samples` scan carrying the exact
//!   stage-3 skip-index prefilters;
//! - line-filtered counters are exact (matching rows only);
//! - a selector matching nothing returns all-zero 200;
//! - the `/loki/api/v1/index/stats` alias is byte-identical to native.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`. Run locally:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-server --test logs_stats_live
//! podman rm -f pulsus-ch-test
//! ```
//!
//! Port 31145, distinct from every other live suite.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};

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

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("now fits in i64")
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

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
        raw = &raw[(data_end + 2).min(raw.len())..];
    }
    out
}

struct HttpResponse {
    status: u16,
    body: String,
}

fn http_get(port: u16, path_and_query: &str, explain: bool) -> HttpResponse {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("read timeout");
    let mut head =
        format!("GET {path_and_query} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    if explain {
        head.push_str("X-Pulsus-Explain: 1\r\n");
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).expect("write");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).expect("read");
    let split_at = find_subslice(&buf, b"\r\n\r\n").expect("header terminator");
    let head_text = String::from_utf8_lossy(&buf[..split_at]).into_owned();
    let status: u16 = head_text
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .expect("status line");
    let headers: HashMap<String, String> = head_text
        .lines()
        .skip(1)
        .filter_map(|line| {
            let (k, v) = line.split_once(':')?;
            Some((k.trim().to_ascii_lowercase(), v.trim().to_string()))
        })
        .collect();
    let raw_body = &buf[split_at + 4..];
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

fn spawn_ready(port: u16, db: &str, extra_env: &[(&str, &str)]) -> ChildGuard {
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
        // The server may not be listening at all yet — probe gently.
        let ready = TcpStream::connect(("127.0.0.1", port)).is_ok()
            && http_get(port, "/ready", false).status == 200;
        if ready {
            return guard;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("/ready never reached 200 within 60s (port {port}, db {db})");
}

async fn drop_db(db: &str) {
    let admin = ChClient::new(ChConnConfig {
        server: ch_host(),
        http_port: ch_http_port(),
        database: "default".to_string(),
        proto: ChProto::Http,
        pool_size: 2,
        query_timeout: Duration::from_secs(30),
        ..ChConnConfig::default()
    })
    .await
    .expect("connect admin client");
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop db");
}

/// Finds the `stats_read` stage in an explain block.
fn stats_read_stage(explain: &serde_json::Value) -> &serde_json::Value {
    explain["stages"]
        .as_array()
        .expect("stages array")
        .iter()
        .find(|s| s["name"] == "stats_read")
        .expect("a stats_read stage")
}

#[tokio::test(flavor = "multi_thread")]
async fn stats_counters_routing_gate_and_alias_identity() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_145;
    let db = "pulsus_stats_it_live";
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[("PULSUS_COMPAT_ENDPOINTS", "true")]);

    let client = ChClient::new(ChConnConfig {
        server: ch_host(),
        http_port: ch_http_port(),
        database: db.to_string(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(30),
        ..ChConnConfig::default()
    })
    .await
    .expect("connect data client");

    // Two streams; five rows over TWO partition days (today + yesterday)
    // — the chunks proxy must report 2 for the unfiltered selector.
    let now = now_ns();
    let yesterday = now - 24 * 3_600_000_000_000;
    for (fp, labels) in [
        (1u64, r#"{"service_name":"checkout"}"#),
        (2u64, r#"{"env":"prod","service_name":"checkout"}"#),
    ] {
        client
            .execute(
                &format!(
                    "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, \
                     updated_ns) VALUES (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({now}))), \
                     {fp}, 'checkout', '{labels}', 0)"
                ),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("seed log_streams");
    }
    let rows: [(u64, i64, &str); 5] = [
        (1, now - 3_000_000_000, "has err one"),
        (1, now - 2_000_000_000, "clean"),
        (1, now - 1_000_000_000, "err again"),
        (2, yesterday, "clean a"),
        (2, yesterday + 1_000_000_000, "clean b"),
    ];
    let values = rows
        .iter()
        .map(|(fp, ts, body)| format!("('checkout', {fp}, {ts}, 0, '{body}')"))
        .collect::<Vec<_>>()
        .join(", ");
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, \
                 body) VALUES {values}"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_samples");

    let start = now - 3 * 24 * 3_600_000_000_000;
    let end = now + 60_000_000_000;
    let total_bytes: usize = rows.iter().map(|(_, _, b)| b.len()).sum();

    // -- No line filter: rollup-served, zero body reads (Tier-1 gate) ---
    let selector = "query=%7Bservice_name%3D%22checkout%22%7D";
    let native = http_get(
        port,
        &format!("/api/logs/v1/stats?{selector}&start={start}&end={end}"),
        true,
    );
    assert_eq!(native.status, 200, "body: {}", native.body);
    let json: serde_json::Value = serde_json::from_str(&native.body).expect("stats JSON");
    assert_eq!(json["streams"], 2, "body: {json}");
    assert_eq!(json["chunks"], 2, "two partition days — the proxy");
    assert_eq!(json["entries"], 5, "body: {json}");
    assert_eq!(json["bytes"], total_bytes as u64, "body: {json}");
    let explain = &json["explain"];
    assert_eq!(explain["routing"]["chosen"], "rollup", "explain: {explain}");
    let stage = stats_read_stage(explain);
    let sql = stage["sql"].as_str().expect("stage sql");
    assert!(
        sql.contains("log_metrics_5s"),
        "no-filter stats must read the rollup: {sql}"
    );
    assert!(
        !sql.contains("body"),
        "rollup-served stats must never touch the body column: {sql}"
    );

    // -- Line filter: exact log_samples scan with skip-index pushdown ---
    let filtered = "query=%7Bservice_name%3D%22checkout%22%7D%20%7C%3D%20%22err%22";
    let res = http_get(
        port,
        &format!("/api/logs/v1/stats?{filtered}&start={start}&end={end}"),
        true,
    );
    assert_eq!(res.status, 200, "body: {}", res.body);
    let json: serde_json::Value = serde_json::from_str(&res.body).expect("stats JSON");
    assert_eq!(json["streams"], 1, "only fp 1 has matching rows: {json}");
    assert_eq!(json["chunks"], 1, "matching rows sit in one day: {json}");
    assert_eq!(json["entries"], 2, "body: {json}");
    let err_bytes = ("has err one".len() + "err again".len()) as u64;
    assert_eq!(json["bytes"], err_bytes, "body: {json}");
    let explain = &json["explain"];
    assert_eq!(explain["routing"]["chosen"], "raw", "explain: {explain}");
    let stage = stats_read_stage(explain);
    let sql = stage["sql"].as_str().expect("stage sql");
    assert!(
        sql.contains("log_samples"),
        "filtered stats must scan log_samples: {sql}"
    );
    assert!(
        sql.contains("hasToken(body, 'err')") && sql.contains("position(body, 'err') > 0"),
        "the stage-3 skip-index line-filter prefilters must be pushed down: {sql}"
    );
    assert!(
        sql.contains("PREWHERE service = 'checkout'"),
        "the primary-key service prefix must stay engaged: {sql}"
    );

    // -- A selector matching nothing: all-zero 200 -----------------------
    let none = http_get(
        port,
        &format!(
            "/api/logs/v1/stats?query=%7Bservice_name%3D%22nope%22%7D&start={start}&end={end}"
        ),
        false,
    );
    assert_eq!(none.status, 200);
    assert_eq!(
        none.body,
        r#"{"streams":0,"chunks":0,"entries":0,"bytes":0}"#
    );

    // -- The /loki/api/v1/index/stats alias is byte-identical ------------
    let native_plain = http_get(
        port,
        &format!("/api/logs/v1/stats?{selector}&start={start}&end={end}"),
        false,
    );
    let alias = http_get(
        port,
        &format!("/loki/api/v1/index/stats?{selector}&start={start}&end={end}"),
        false,
    );
    assert_eq!(alias.status, 200);
    assert_eq!(
        alias.body, native_plain.body,
        "the alias is a pure route binding — byte-identical"
    );

    drop_db(db).await;
}
