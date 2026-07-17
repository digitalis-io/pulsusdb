//! Live end-to-end smoke test for `/api/v1/*` (issue #32) against a real
//! ClickHouse: spawns the real `pulsusdb` binary, seeds `metric_series`/
//! `metric_samples` directly (mirrors `pulsus-read`'s own
//! `live_metrics_engine.rs` precedent: `ChClient::insert_block`, not
//! through `pulsus-write` — the read-path tests' established seeding
//! style), and drives the query/discovery/status surface over loopback
//! HTTP exactly as `live_server.rs` does (bare TcpStream HTTP/1.1, no new
//! client dependency, KISS: no TLS, no DNS, static ports).
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, same podman setup as
//! `live_server.rs`/`crates/pulsus-read/tests/live_metrics_engine.rs`:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-server --test prom_api_live
//! podman rm -f pulsus-ch-test
//! ```

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, QuerySettings, Row};

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
}

fn test_ch_config(database: &str) -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: database.to_string(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(30),
        ..ChConnConfig::default()
    }
}

/// Bare HTTP/1.1 GET over loopback, mirroring `live_server.rs`'s own
/// helper (KISS: no HTTP client dependency for a handful of smoke-test
/// requests).
fn http_get(port: u16, path: &str) -> Option<(u16, String)> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .ok()?;
    let mut buf = String::new();
    stream.read_to_string(&mut buf).ok()?;
    let mut parts = buf.splitn(2, "\r\n\r\n");
    let head = parts.next()?;
    let body = parts.next().unwrap_or("").to_string();
    let status = head
        .lines()
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()?;
    Some((status, body))
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedSeriesRow {
    metric_name: String,
    fingerprint: u64,
    unix_milli: i64,
    labels: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedSampleRow {
    metric_name: String,
    fingerprint: u64,
    unix_milli: i64,
    value: f64,
}

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("now fits in i64")
}

#[tokio::test(flavor = "multi_thread")]
async fn prom_api_serves_discovery_and_query_against_real_clickhouse() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see crates/pulsus-clickhouse/tests/live_clickhouse.rs for setup)"
        );
        return;
    }

    let db = std::env::var("PULSUS_TEST_CH_DATABASE")
        .unwrap_or_else(|_| "pulsus_prom_api_live_test".to_string());
    let port: u16 = 31_101;

    let child = Command::new(env!("CARGO_BIN_EXE_pulsusdb"))
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        // Fast enough that the label cache is warm well within this
        // test's own deadline (default 60s would make this test slow).
        .env("PULSUS_CACHE_TTL", "1s")
        .env(
            "CLICKHOUSE_SERVER",
            std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        )
        .env(
            "CLICKHOUSE_HTTP_PORT",
            std::env::var("PULSUS_TEST_CH_HTTP_PORT").unwrap_or_else(|_| "19123".to_string()),
        )
        .env("CLICKHOUSE_DB", &db)
        .spawn()
        .expect("spawn pulsusdb");
    let _guard = ChildGuard(child);

    let deadline = Instant::now() + Duration::from_secs(60);
    let mut became_ready = false;
    while Instant::now() < deadline {
        if let Some((200, _)) = http_get(port, "/ready") {
            became_ready = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(became_ready, "/ready never reached 200 within 60s");

    // Seed directly (mirrors `live_metrics_engine.rs`'s own precedent) —
    // `pulsusdb` itself already created the schema during startup above.
    let client = ChClient::new(test_ch_config(&db))
        .await
        .expect("connect to seed data");
    let bucket_ms: i64 = 3_600_000;
    let now = now_ms();
    let recent_bucket = (now / bucket_ms) * bucket_ms;
    client
        .insert_block(
            "metric_series",
            &[
                SeedSeriesRow {
                    metric_name: "up".to_string(),
                    fingerprint: 1,
                    unix_milli: recent_bucket,
                    labels: r#"{"job":"api"}"#.to_string(),
                },
                SeedSeriesRow {
                    metric_name: "up".to_string(),
                    fingerprint: 2,
                    unix_milli: recent_bucket,
                    labels: r#"{"job":"web"}"#.to_string(),
                },
            ],
        )
        .await
        .expect("seed metric_series");
    client
        .insert_block(
            "metric_samples",
            &[
                SeedSampleRow {
                    metric_name: "up".to_string(),
                    fingerprint: 1,
                    unix_milli: now,
                    value: 1.0,
                },
                SeedSampleRow {
                    metric_name: "up".to_string(),
                    fingerprint: 2,
                    unix_milli: now,
                    value: 0.0,
                },
            ],
        )
        .await
        .expect("seed metric_samples");

    // Discovery endpoints go straight to `metric_series` (never the
    // cache's coarse superset — the #30 handoff AC this issue implements),
    // so they need no cache-warm wait at all.
    let (status, body) = http_get(port, "/api/v1/series?match[]=up").expect("/series reachable");
    assert_eq!(status, 200);
    assert!(body.contains("\"__name__\":\"up\""), "body: {body}");
    assert!(body.contains("\"job\":\"api\""), "body: {body}");

    let (status, body) = http_get(port, "/api/v1/labels?match[]=up").expect("/labels reachable");
    assert_eq!(status, 200);
    assert!(body.contains("__name__"), "body: {body}");
    assert!(body.contains("job"), "body: {body}");

    // Code-review round-1 fix: a matcher-only `match[]` selector (no
    // concrete metric name, e.g. `{job="api"}`) is a valid Prometheus
    // discovery selector — must reach the real `metric_series` data, not
    // `422 execution` from the PromQL query-planner's stricter contract.
    let matcher_only = "%7Bjob%3D%22api%22%7D"; // {job="api"}
    let (status, body) = http_get(port, &format!("/api/v1/series?match[]={matcher_only}"))
        .expect("/series (matcher-only) reachable");
    assert_eq!(status, 200, "body: {body}");
    assert!(body.contains("\"__name__\":\"up\""), "body: {body}");
    assert!(body.contains("\"job\":\"api\""), "body: {body}");

    let (status, body) = http_get(port, &format!("/api/v1/labels?match[]={matcher_only}"))
        .expect("/labels (matcher-only) reachable");
    assert_eq!(status, 200, "body: {body}");
    assert!(body.contains("__name__"), "body: {body}");
    assert!(body.contains("job"), "body: {body}");

    let (status, body) = http_get(
        port,
        &format!("/api/v1/label/job/values?match[]={matcher_only}"),
    )
    .expect("/label/job/values (matcher-only) reachable");
    assert_eq!(status, 200, "body: {body}");
    assert!(body.contains("\"api\""), "body: {body}");

    // `/query` needs the label cache to have swept the seeded series in —
    // poll until it does (bounded, `PULSUS_CACHE_TTL=1s` above).
    let deadline = Instant::now() + Duration::from_secs(30);
    let query_body;
    loop {
        if let Some((200, body)) = http_get(port, "/api/v1/query?query=up")
            && body.contains("\"job\":\"api\"")
        {
            query_body = body;
            break;
        }
        if Instant::now() > deadline {
            panic!("label cache never warmed with the seeded series within 30s");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(query_body.contains("\"resultType\":\"vector\""));
    assert!(query_body.contains("\"job\":\"web\""));

    // Issue #89 (AC4): a regex-`__name__` discovery selector is now served
    // rather than rejected — unlike the concrete-name/matcher-only paths
    // above (which read `metric_series` directly), it resolves candidate
    // metric names through the label cache under the fan-out cap, so it is
    // asserted here, after the cache-warm poll. `{__name__=~"up.*"}`
    // resolves `up` and returns its seeded series (one flat `metric_name
    // IN … AND fingerprint IN …` fetch).
    let name_regex = "%7B__name__%3D~%22up.%2A%22%7D"; // {__name__=~"up.*"}
    let (status, body) = http_get(port, &format!("/api/v1/series?match[]={name_regex}"))
        .expect("/series (name regex) reachable");
    assert_eq!(status, 200, "body: {body}");
    assert!(body.contains("\"__name__\":\"up\""), "body: {body}");
    assert!(body.contains("\"job\":\"api\""), "body: {body}");
    assert!(body.contains("\"job\":\"web\""), "body: {body}");

    let (status, body) = http_get(port, &format!("/api/v1/labels?match[]={name_regex}"))
        .expect("/labels (name regex) reachable");
    assert_eq!(status, 200, "body: {body}");
    assert!(body.contains("__name__"), "body: {body}");
    assert!(body.contains("job"), "body: {body}");

    let (status, body) =
        http_get(port, "/api/v1/status/tsdb").expect("/api/v1/status/tsdb reachable");
    assert_eq!(status, 200);
    assert!(body.contains("\"numSeries\":2"), "body: {body}");

    let (status, body) =
        http_get(port, "/api/v1/status/buildinfo").expect("/api/v1/status/buildinfo reachable");
    assert_eq!(status, 200);
    assert!(body.contains("\"version\""), "body: {body}");

    // Cheap error-path proof end to end: a malformed query is 400
    // `bad_data`, no `position` field on the wire.
    let (status, body) =
        http_get(port, "/api/v1/query?query=up%7B").expect("malformed query reachable");
    assert_eq!(status, 400);
    assert!(body.contains("\"errorType\":\"bad_data\""), "body: {body}");
    assert!(!body.contains("\"position\""), "body: {body}");

    let bootstrap = ChClient::new(test_ch_config("default"))
        .await
        .expect("connect (bootstrap)");
    bootstrap
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            pulsus_clickhouse::Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
}

/// Issue #89 (AC5): a regex-`__name__` discovery selector whose resolved
/// candidate-name set exceeds `PULSUS_PROMQL_MAX_METRIC_FANOUT` is
/// `422 execution` — the same `QueryTooBroad(MetricFanout)` mapping the
/// query path uses, now reached from the discovery surface. A dedicated
/// server process (the cap is a load-time config knob) seeded with two
/// metric names and a cap of 1.
#[tokio::test(flavor = "multi_thread")]
async fn prom_api_name_regex_discovery_over_the_fanout_cap_is_422_execution() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test");
        return;
    }

    let db = "pulsus_prom_api_live_fanout_test";
    let port: u16 = 31_102;

    let child = Command::new(env!("CARGO_BIN_EXE_pulsusdb"))
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env("PULSUS_CACHE_TTL", "1s")
        // The cap under test: two matching metric names resolve, one is
        // the ceiling -> the fan-out breach the assertion pins.
        .env("PULSUS_PROMQL_MAX_METRIC_FANOUT", "1")
        .env(
            "CLICKHOUSE_SERVER",
            std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        )
        .env(
            "CLICKHOUSE_HTTP_PORT",
            std::env::var("PULSUS_TEST_CH_HTTP_PORT").unwrap_or_else(|_| "19123".to_string()),
        )
        .env("CLICKHOUSE_DB", db)
        .spawn()
        .expect("spawn pulsusdb");
    let _guard = ChildGuard(child);

    let deadline = Instant::now() + Duration::from_secs(60);
    let mut became_ready = false;
    while Instant::now() < deadline {
        if let Some((200, _)) = http_get(port, "/ready") {
            became_ready = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(became_ready, "/ready never reached 200 within 60s");

    let client = ChClient::new(test_ch_config(db))
        .await
        .expect("connect to seed data");
    let bucket_ms: i64 = 3_600_000;
    let now = now_ms();
    let recent_bucket = (now / bucket_ms) * bucket_ms;
    // Two distinct metric names, both matching `up.*` -> a resolved
    // candidate-name set of 2 against a cap of 1.
    client
        .insert_block(
            "metric_series",
            &[
                SeedSeriesRow {
                    metric_name: "up".to_string(),
                    fingerprint: 1,
                    unix_milli: recent_bucket,
                    labels: r#"{"job":"api"}"#.to_string(),
                },
                SeedSeriesRow {
                    metric_name: "up_alias".to_string(),
                    fingerprint: 2,
                    unix_milli: recent_bucket,
                    labels: r#"{"job":"web"}"#.to_string(),
                },
            ],
        )
        .await
        .expect("seed metric_series");

    // Warm the label cache with BOTH seeded names before asserting. The
    // fan-out count is taken over the resident snapshot, so until both `up`
    // and `up_alias` are swept in the name-less selector can transiently
    // fail as `NamelessSelectorUnresolvable` (a cold-cache race) — which
    // maps to the *same* (422, "execution") tuple as the fan-out breach
    // (prom_api/error.rs), differing only in message text. Warming first
    // makes the breach deterministic so the message assertion below proves
    // the FAN-OUT CAP specifically, not the cold-cache race. `status/tsdb`
    // is served entirely from the resident label cache (zero ClickHouse),
    // so its `numSeries` reaching 2 is a direct signal that both seeded
    // series are resident — and unlike `/query` it needs no seeded samples
    // (this test seeds `metric_series` rows only).
    let warm_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some((200, body)) = http_get(port, "/api/v1/status/tsdb")
            && body.contains("\"numSeries\":2")
            && body.contains("up_alias")
        {
            break;
        }
        if Instant::now() > warm_deadline {
            panic!("label cache never warmed with both seeded names within 30s");
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    // Both names are resident: the name-less selector now resolves 2 names
    // against a cap of 1 -> a deterministic fan-out breach.
    let name_regex = "%7B__name__%3D~%22up.%2A%22%7D"; // {__name__=~"up.*"}
    let (status, body) = http_get(port, &format!("/api/v1/series?match[]={name_regex}"))
        .expect("/series (name regex over cap) reachable");
    assert_eq!(status, 422, "body: {body}");
    assert!(body.contains("\"errorType\":\"execution\""), "body: {body}");
    // Discriminate the fan-out breach from the (identically-tupled)
    // `NamelessSelectorUnresolvable` cold-cache error by its message text:
    // only the fan-out message names the cap knob.
    assert!(
        body.contains("fan-out cap (reader.promql_max_metric_fanout)"),
        "expected the fan-out-cap breach message, not the nameless-unresolvable one; body: {body}"
    );

    let bootstrap = ChClient::new(test_ch_config("default"))
        .await
        .expect("connect (bootstrap)");
    bootstrap
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            pulsus_clickhouse::Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
}
