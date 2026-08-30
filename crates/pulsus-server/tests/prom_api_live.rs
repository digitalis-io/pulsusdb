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
//!     clickhouse/clickhouse-server:26.3
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-server --test prom_api_live
//! podman rm -f pulsus-ch-test
//! ```

#[path = "support/live_db.rs"]
mod live_db;

use live_db::drop_db;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, QuerySettings, Row};

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping when
/// the gate is absent in a live CI job, so a lost `env:` block reddens the
/// build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
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
    let body = decode_body(head, parts.next().unwrap_or(""));
    let status = head
        .lines()
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()?;
    Some((status, body))
}

/// Bare HTTP/1.1 POST with an `application/x-www-form-urlencoded` body —
/// the exact shape issue #471 M1 is about, including the empty-body case
/// (`Content-Length: 0`) that carries every parameter in the URL.
fn http_post_form(port: u16, path: &str, body: &str) -> Option<(u16, String)> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(30))).ok();
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: localhost\r\n\
         Content-Type: application/x-www-form-urlencoded\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .ok()?;
    let mut buf = String::new();
    stream.read_to_string(&mut buf).ok()?;
    let mut parts = buf.splitn(2, "\r\n\r\n");
    let head = parts.next()?;
    let body = decode_body(head, parts.next().unwrap_or(""));
    let status = head
        .lines()
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()?;
    Some((status, body))
}

/// Undoes `Transfer-Encoding: chunked` framing. The `/api/v1/*` encoders
/// stream their bodies (issue #24), so a discovery/query response arrives
/// chunked and a byte-exact assertion against the raw socket text would be
/// asserting the framing rather than the envelope.
fn decode_body(head: &str, raw: &str) -> String {
    if !head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        return raw.to_string();
    }
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(nl) = rest.find("\r\n") {
        let Ok(size) = usize::from_str_radix(rest[..nl].split(';').next().unwrap_or("").trim(), 16)
        else {
            break;
        };
        rest = &rest[nl + 2..];
        if size == 0 {
            break;
        }
        if rest.len() < size {
            out.push_str(rest);
            break;
        }
        out.push_str(&rest[..size]);
        rest = rest[size..].strip_prefix("\r\n").unwrap_or(&rest[size..]);
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

    let db = pulsus_testkit::test_db("pulsus_prom_api_live_it");
    let port: u16 = 31_173;

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

    drop_db(&db).await;
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

    let db = &pulsus_testkit::test_db("pulsus_prom_api_fanout_it");
    let port: u16 = 31_174;

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

    drop_db(db).await;
}

/// Issue #89 (retroactive re-review, plan v2 AC5b): a regex-`__name__`
/// discovery selector whose resolution *examines* more cache entries than
/// `PULSUS_PROMQL_MAX_CACHE_SCAN` is `422 execution` on a **warm** cache —
/// distinct from the fan-out-cap breach above (which counts only matched
/// names) and never the degraded-cache probe fallback (issue #96). A
/// dedicated server process (the budget is a load-time config knob) seeded
/// with two metric names and a scan budget of 1, well under both seeded
/// names' combined name+fingerprint entry count.
#[tokio::test(flavor = "multi_thread")]
async fn prom_api_name_regex_discovery_over_the_cache_scan_budget_is_422_execution() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test");
        return;
    }

    let db = &pulsus_testkit::test_db("pulsus_prom_api_scan_budget_it");
    let port: u16 = 31_175;

    let child = Command::new(env!("CARGO_BIN_EXE_pulsusdb"))
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env("PULSUS_CACHE_TTL", "1s")
        // The budget under test: examining even one name's fingerprint
        // pushes the walk past this — a deterministic breach regardless of
        // `HashMap` iteration order over the two seeded names.
        .env("PULSUS_PROMQL_MAX_CACHE_SCAN", "1")
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

    // Warm the label cache with BOTH seeded names before asserting — the
    // scan budget is examined against the resident snapshot, so a cold
    // cache would instead surface `NamelessSelectorUnresolvable` (the same
    // (422, "execution") tuple, differing only in message text).
    // `status/tsdb` is served entirely from the resident label cache (zero
    // ClickHouse), so its `numSeries` reaching 2 is a direct residency
    // signal.
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

    // `.+` matches every resident name (both seeded names are non-empty)
    // and, unlike `.*`, does not itself match the empty string — so it is
    // a valid "non-empty matcher" under the PromQL vector-selector rule
    // (Prometheus rejects an all-empty-matcher selector before it ever
    // reaches resolution). The walk always has at least one name+
    // fingerprint pair to examine past a budget of 1.
    let name_regex_all = "%7B__name__%3D~%22.%2B%22%7D"; // {__name__=~".+"}
    for path in ["series", "labels"] {
        let (status, body) = http_get(port, &format!("/api/v1/{path}?match[]={name_regex_all}"))
            .unwrap_or_else(|| panic!("/{path} (name regex over scan budget) reachable"));
        assert_eq!(status, 422, "path {path}, body: {body}");
        assert!(
            body.contains("\"errorType\":\"execution\""),
            "path {path}, body: {body}"
        );
        // Discriminate the scan-budget breach from the (identically-tupled)
        // `MetricFanout`/`NamelessSelectorUnresolvable` errors by message
        // text: only the scan-budget message names its knob.
        assert!(
            body.contains("scan budget (reader.promql_max_cache_scan)"),
            "path {path}: expected the scan-budget breach message; body: {body}"
        );
    }

    drop_db(db).await;
}

// ---------------------------------------------------------------------
// Issue #398 — the PromQL half, and the one acceptance criterion in this
// issue that needs a non-vacuity assertion.
//
// The 500 this issue is about was NOT reproducible on this surface. Under
// the same ClickHouse ceiling that made five LogQL endpoints answer 500,
// every metrics shape answered 200 — the only breach was the background
// label-cache refresh sweep, which by design logs and keeps serving the
// last good snapshot. So on the metrics surface the user-visible symptom
// of memory exhaustion is stale or empty results, not a status code (that
// is recorded as remaining work on #398 and deliberately NOT fixed here).
//
// The bound still ships: `metrics_read_settings` setting only
// `max_query_size` was the same missing bound, the 500 mapping is
// reachable by construction, and leaving one of three surfaces unbounded
// would reproduce the carve-out shape the issue exists to remove.
//
// Which is exactly why the test below asserts DISPATCH as well as status.
// The metrics request path is heavily cache-fronted and frequently answers
// 200 without touching ClickHouse at all, so a status-only assertion could
// pass for the wrong reason on BOTH sides of the fix. `PROBE_METRIC` is
// the discriminator: `metrics::sql::discovery_query` renders `metric_name
// = '<name>'` into the SQL, and the background sweep's SQL carries no
// metric name at all — so a `system.query_log` row for this run's database
// whose text contains that name can only have come from this request.
// ---------------------------------------------------------------------

/// A metric name that appears in no other query this process issues.
const PROBE_METRIC: &str = "pulsus_issue398_dispatch_probe";

/// Series seeded for the breach fixture.
///
/// **Sizing, measured through this test at `PULSUS_PROMQL_READ_MAX_MEMORY_BYTES
/// = 1024` — on `/api/v1/series`, not on a SQL statement in isolation.** The
/// route answers 200 at 0, 100, 1 000, 5 000 and 20 000 series, and 422 at
/// 30 000, 40 000, 50 000, 60 000 and 100 000. The threshold is therefore
/// between 20 000 and 30 000.
///
/// 100 000 is 3.3x the measured threshold and runs in ~1.0 s (0.84 s at
/// 30 000). Deliberately not larger: 150 000 was measured at 5.3 s, and buying
/// more margin than the threshold's stability warrants is not worth five
/// seconds on every CI run.
const PROBE_SERIES: u64 = 100_000;

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct QueryLogProbeRow {
    n: u64,
}

/// Issue #398 AC M3. Asserts BOTH halves, and the second is what makes the
/// first mean anything:
///
/// - (a) a PromQL read that breaches `reader.promql_read_max_memory_bytes`
///   answers **422** with `errorType: "execution"` — the envelope
///   prometheus/prometheus v3.13.0 itself returns for a memory refusal
///   (`--query.max-samples=1` measured at 422 `execution`,
///   `web/api/v1/api.go:2236-2237 @ v3.13.0`) — and never a 500; and
/// - (b) the request **actually dispatched to ClickHouse**, proved from
///   `system.query_log` by the request-only marker described above.
#[tokio::test(flavor = "multi_thread")]
async fn promql_memory_breach_is_422_and_actually_dispatched() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test");
        return;
    }

    // Per-run nonce'd database: `system.query_log` outlives databases, so a
    // fixed name would let a previous local run satisfy (b).
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis();
    let db = pulsus_testkit::test_db(&format!("pulsus_prom_api_mem_it_{nonce}"));
    let db = db.as_str();
    let port: u16 = 31_149;

    let child = Command::new(env!("CARGO_BIN_EXE_pulsusdb"))
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env("PULSUS_PROMQL_READ_MAX_MEMORY_BYTES", "1024")
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
    let recent_bucket = (now_ms() / bucket_ms) * bucket_ms;
    client
        .execute(
            &format!(
                "INSERT INTO {db}.metric_series (metric_name, fingerprint, unix_milli, labels) \
                 SELECT '{PROBE_METRIC}', number + 1, {recent_bucket}, \
                        concat('{{\"job\":\"j', toString(number), '\"}}') \
                 FROM numbers({PROBE_SERIES})"
            ),
            &QuerySettings::new(),
            pulsus_clickhouse::Idempotency::Idempotent,
        )
        .await
        .expect("seed metric_series");

    // (a) The status. `/api/v1/series` with a concrete metric name reads
    // `metric_series` directly (never the cache's coarse superset — the
    // #30 handoff), so this is a request that must touch ClickHouse.
    let (status, body) = http_get(port, &format!("/api/v1/series?match[]={PROBE_METRIC}"))
        .expect("/api/v1/series reachable");
    assert_eq!(status, 422, "body: {body}");
    assert!(
        body.contains("\"errorType\":\"execution\""),
        "the memory refusal must use Prometheus's own envelope: {body}"
    );
    assert!(
        body.contains("reader.promql_read_max_memory_bytes"),
        "the body must name the knob an operator would raise: {body}"
    );
    assert!(
        !body.contains("DB::Exception") && !body.contains("official build"),
        "the 422 body must carry only our own message: {body}"
    );

    // (b) Non-vacuity: the request really dispatched. Without this, a
    // cache-fronted 200 (or a pre-dispatch rejection) would satisfy a
    // status-only assertion on either side of the fix.
    let admin = ChClient::new(test_ch_config("default"))
        .await
        .expect("connect (admin)");
    admin
        .execute(
            "SYSTEM FLUSH LOGS",
            &QuerySettings::new(),
            pulsus_clickhouse::Idempotency::Idempotent,
        )
        .await
        .expect("flush logs");
    let probe_sql = format!(
        "SELECT count() AS n FROM system.query_log \
         WHERE current_database = '{db}' AND type != 'QueryStart' \
           AND query_kind = 'Select' AND query LIKE '%{PROBE_METRIC}%'"
    );
    let mut n = 0u64;
    {
        use futures::StreamExt;
        let mut stream = admin
            .query_stream::<QueryLogProbeRow>(&probe_sql, &QuerySettings::new())
            .await
            .expect("query system.query_log");
        while let Some(row) = stream.next().await {
            n = row.expect("decode probe row").n;
        }
    }
    assert!(
        n > 0,
        "the request never reached ClickHouse — a status-only assertion here would be vacuous \
         (no system.query_log Select for {db} mentions {PROBE_METRIC})"
    );

    // And that dispatch carried the ceiling: the completeness half.
    let settings_sql = format!(
        "SELECT count() AS n FROM system.query_log \
         WHERE current_database = '{db}' AND type != 'QueryStart' \
           AND query_kind = 'Select' AND query LIKE '%{PROBE_METRIC}%' \
           AND mapContains(Settings, 'max_memory_usage') = 0"
    );
    let mut unbounded = 0u64;
    {
        use futures::StreamExt;
        let mut stream = admin
            .query_stream::<QueryLogProbeRow>(&settings_sql, &QuerySettings::new())
            .await
            .expect("query system.query_log");
        while let Some(row) = stream.next().await {
            unbounded = row.expect("decode probe row").n;
        }
    }
    assert_eq!(
        unbounded, 0,
        "every dispatched metrics read must carry max_memory_usage"
    );

    drop_db(db).await;
}

// ---------------------------------------------------------------------
// Issue #471 — the PromQL query-surface bundle, end to end
// ---------------------------------------------------------------------

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedMetadataRow {
    metric_name: String,
    metric_type: String,
    help: String,
    unit: String,
    updated_ns: i64,
}

/// Issue #471, M1/M2(parse+controls)/M3/M4/M6 against a live server.
///
/// **One fixture, chosen so no assertion can pass vacuously.** Four
/// series over three metric names, carrying four distinct label keys:
///
/// | series | label keys it contributes |
/// |---|---|
/// | `up{job="api"}` | `__name__`, `job` |
/// | `up{job="web"}` | `__name__`, `job` |
/// | `http_requests_total{handler="/api",job="api"}` | `handler` |
/// | `dashed{a-b="dash"}` | `a-b` |
///
/// So the unscoped label-name set is exactly `["__name__","a-b","handler",
/// "job"]` and the `up{job="api"}`-scoped set is exactly
/// `["__name__","job"]` — **different**, which is what makes M1's
/// POST-equals-GET assertion mean something. And `match[]=up` matches two
/// series while `match[]=http_requests_total` matches one, so body-only,
/// URL-only and merged `/series` answers are 2, 1 and 3 — pairwise
/// distinct, so asserting three separates every partial implementation at
/// once, the URL-only one included.
#[tokio::test(flavor = "multi_thread")]
async fn prom_api_query_surface_bundle_issue_471() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test");
        return;
    }

    let db = pulsus_testkit::test_db("pulsus_prom_471_it");
    let port: u16 = 31_300;

    let child = Command::new(env!("CARGO_BIN_EXE_pulsusdb"))
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
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

    let client = ChClient::new(test_ch_config(&db))
        .await
        .expect("connect to seed data");
    let bucket_ms: i64 = 3_600_000;
    let now = now_ms();
    let recent_bucket = (now / bucket_ms) * bucket_ms;
    let series = [
        ("up", 1u64, r#"{"job":"api"}"#),
        ("up", 2, r#"{"job":"web"}"#),
        (
            "http_requests_total",
            3,
            r#"{"handler":"/api","job":"api"}"#,
        ),
        ("dashed", 4, r#"{"a-b":"dash"}"#),
    ];
    client
        .insert_block(
            "metric_series",
            &series
                .iter()
                .map(|(name, fp, labels)| SeedSeriesRow {
                    metric_name: (*name).to_string(),
                    fingerprint: *fp,
                    unix_milli: recent_bucket,
                    labels: (*labels).to_string(),
                })
                .collect::<Vec<_>>(),
        )
        .await
        .expect("seed metric_series");
    client
        .insert_block(
            "metric_samples",
            &series
                .iter()
                .map(|(name, fp, _)| SeedSampleRow {
                    metric_name: (*name).to_string(),
                    fingerprint: *fp,
                    unix_milli: now,
                    value: 1.0,
                })
                .collect::<Vec<_>>(),
        )
        .await
        .expect("seed metric_samples");
    // Seeded so M4's `/metadata?limit=0` assertion is not vacuous: with an
    // empty table `{}` would be the answer under every rule.
    client
        .insert_block(
            "metric_metadata",
            &[
                SeedMetadataRow {
                    metric_name: "up".to_string(),
                    metric_type: "gauge".to_string(),
                    help: "up".to_string(),
                    unit: String::new(),
                    updated_ns: now * 1_000_000,
                },
                SeedMetadataRow {
                    metric_name: "http_requests_total".to_string(),
                    metric_type: "counter".to_string(),
                    help: "requests".to_string(),
                    unit: String::new(),
                    updated_ns: now * 1_000_000,
                },
            ],
        )
        .await
        .expect("seed metric_metadata");

    // -----------------------------------------------------------------
    // M1 — a POST reads the URL query string
    // -----------------------------------------------------------------

    let scoped_url = "/api/v1/labels?match%5B%5D=up%7Bjob%3D%22api%22%7D";
    let (status, get_scoped) = http_get(port, scoped_url).expect("GET scoped /labels");
    assert_eq!(status, 200, "{get_scoped}");
    let (status, unscoped) = http_get(port, "/api/v1/labels").expect("GET unscoped /labels");
    assert_eq!(status, 200, "{unscoped}");
    assert_eq!(
        unscoped.trim(),
        r#"{"status":"success","data":["__name__","a-b","handler","job"]}"#
    );
    // The two-metric fixture is what stops the equality below passing
    // vacuously: the scoped and unscoped answers differ.
    assert_ne!(get_scoped.trim(), unscoped.trim());

    let (status, post_scoped) =
        http_post_form(port, scoped_url, "").expect("POST scoped /labels, empty body");
    assert_eq!(status, 200, "{post_scoped}");
    assert_eq!(
        post_scoped.trim(),
        get_scoped.trim(),
        "a POST carrying its parameters in the URL must answer exactly what the GET does"
    );
    assert_ne!(post_scoped.trim(), unscoped.trim());

    // Body and URL are MERGED, not one or the other: 2 + 1 = 3.
    let (status, merged) = http_post_form(
        port,
        "/api/v1/series?match%5B%5D=http_requests_total",
        "match%5B%5D=up",
    )
    .expect("POST /series with body and URL match[]");
    assert_eq!(status, 200, "{merged}");
    let merged_json: serde_json::Value = serde_json::from_str(merged.trim()).expect("json");
    assert_eq!(
        merged_json["data"].as_array().expect("data array").len(),
        3,
        "body-only is 2 and URL-only is 1, so three is the only merged answer: {merged}"
    );

    // Body wins for a single-valued key repeated in both halves.
    let (status, body_wins) = http_post_form(
        port,
        "/api/v1/query?time=200",
        "query=vector%281%29&time=100",
    )
    .expect("POST /query with time in both halves");
    assert_eq!(status, 200, "{body_wins}");
    assert!(
        body_wins.contains("[100,\"1\"]"),
        "the body's `time` must win over the URL's: {body_wins}"
    );

    // -----------------------------------------------------------------
    // M3 — the resolution cap counts step intervals
    // -----------------------------------------------------------------

    const CAP_SENTENCE: &str = "exceeded maximum resolution of 11,000 points per timeseries. \
                                Try decreasing the query resolution (?step=XX)";
    for (query, want_status) in [
        // 11,000 intervals, reached three ways — whole seconds, a
        // fractional `end`, and a sub-second `step`. The last two are what
        // discriminate a fix that special-cases whole-second inputs.
        ("query=up&start=0&end=11000&step=1", 200),
        ("query=up&start=0&end=11000.5&step=1", 200),
        ("query=up&start=0&end=5500&step=0.5", 200),
        // 11,001 intervals.
        ("query=up&start=0&end=11001&step=1", 400),
        ("query=up&start=0&end=5500.5&step=0.5", 400),
    ] {
        let (status, body) =
            http_get(port, &format!("/api/v1/query_range?{query}")).expect("query_range");
        assert_eq!(status, want_status, "{query}: {body}");
        let json: serde_json::Value = serde_json::from_str(body.trim()).expect("json");
        if want_status == 200 {
            assert_eq!(json["data"]["resultType"], "matrix", "{query}");
        } else {
            assert_eq!(json["errorType"], "bad_data", "{query}");
            assert_eq!(json["error"], CAP_SENTENCE, "{query}");
        }
    }

    // -----------------------------------------------------------------
    // M4 — `limit` on the three discovery endpoints
    // -----------------------------------------------------------------

    const WARNED: &str =
        r#"{"status":"success","data":["__name__"],"warnings":["results truncated due to limit"]}"#;
    let (status, body) = http_get(port, "/api/v1/labels?limit=1").expect("/labels?limit=1");
    assert_eq!(status, 200);
    assert_eq!(body.trim(), WARNED);

    // Exactly at the count: all four, and NO `warnings` key at all.
    for raw in ["limit=4", "limit=0", "limit="] {
        let (status, body) = http_get(port, &format!("/api/v1/labels?{raw}")).expect("/labels");
        assert_eq!(status, 200, "{raw}: {body}");
        assert_eq!(body.trim(), unscoped.trim(), "{raw}");
        assert!(!body.contains("warnings"), "{raw}: {body}");
    }

    let (status, body) =
        http_get(port, "/api/v1/label/job/values?limit=1").expect("/label/job/values?limit=1");
    assert_eq!(status, 200);
    assert_eq!(
        body.trim(),
        r#"{"status":"success","data":["api"],"warnings":["results truncated due to limit"]}"#
    );
    let (status, body) =
        http_get(port, "/api/v1/label/job/values?limit=2").expect("/label/job/values?limit=2");
    assert_eq!(status, 200);
    assert_eq!(body.trim(), r#"{"status":"success","data":["api","web"]}"#);

    let (status, body) =
        http_get(port, "/api/v1/series?match%5B%5D=up&limit=1").expect("/series?limit=1");
    assert_eq!(status, 200, "{body}");
    let json: serde_json::Value = serde_json::from_str(body.trim()).expect("json");
    assert_eq!(json["data"].as_array().expect("data").len(), 1, "{body}");
    assert_eq!(json["warnings"][0], "results truncated due to limit");
    let (status, body) =
        http_get(port, "/api/v1/series?match%5B%5D=up&limit=2").expect("/series?limit=2");
    assert_eq!(status, 200, "{body}");
    assert!(!body.contains("warnings"), "{body}");

    // Both rejection strings, as literals rather than by status.
    for (path, raw, want) in [
        (
            "/api/v1/labels",
            "limit=-1",
            "invalid parameter \"limit\": limit must be non-negative",
        ),
        (
            "/api/v1/labels",
            "limit=abc",
            "invalid parameter \"limit\": cannot parse \"abc\" to an integer",
        ),
    ] {
        let (status, body) = http_get(port, &format!("{path}?{raw}")).expect("bad limit");
        assert_eq!(status, 400, "{raw}: {body}");
        let json: serde_json::Value = serde_json::from_str(body.trim()).expect("json");
        assert_eq!(json["errorType"], "bad_data", "{raw}");
        assert_eq!(json["error"], want, "{raw}");
    }

    // `/metadata` is the OTHER rule and must not be unified: `limit=0`
    // means *return nothing* there, on both servers. Non-vacuous because
    // the unlimited answer below is not empty.
    let (status, body) = http_get(port, "/api/v1/metadata").expect("/metadata");
    assert_eq!(status, 200);
    assert!(body.contains("\"up\""), "{body}");
    let (status, body) = http_get(port, "/api/v1/metadata?limit=0").expect("/metadata?limit=0");
    assert_eq!(status, 200);
    assert_eq!(body.trim(), r#"{"status":"success","data":{}}"#);

    // -----------------------------------------------------------------
    // M6 — `U__` unescaping
    // -----------------------------------------------------------------

    let (status, plain_ab) = http_get(port, "/api/v1/label/a-b/values").expect("/label/a-b/values");
    assert_eq!(status, 200);
    assert_eq!(plain_ab.trim(), r#"{"status":"success","data":["dash"]}"#);
    let (status, plain_job) =
        http_get(port, "/api/v1/label/job/values").expect("/label/job/values");
    assert_eq!(status, 200);
    assert_eq!(
        plain_job.trim(),
        r#"{"status":"success","data":["api","web"]}"#
    );

    for (escaped, plain) in [
        // A naive `strip_prefix("U__")` answers `[]` here.
        ("/api/v1/label/U__a_2d_b/values", &plain_ab),
        // A case-sensitive hex decoder answers `[]` here (`_6F_` is `o`).
        ("/api/v1/label/U__j_6F_b/values", &plain_job),
        // An escape at position 0.
        ("/api/v1/label/U___6a_ob/values", &plain_job),
        // An escaped LEGACY name: holds regardless of what data exists.
        ("/api/v1/label/U__job/values", &plain_job),
    ] {
        let (status, body) = http_get(port, escaped).expect("escaped label name");
        assert_eq!(status, 200, "{escaped}: {body}");
        assert_eq!(body.trim(), plain.trim(), "{escaped}");
        // The second clause: an empty fixture would make the first pass
        // vacuously.
        assert_ne!(
            body.trim(),
            r#"{"status":"success","data":[]}"#,
            "{escaped} must not be the empty list"
        );
    }

    let (status, body) = http_get(port, "/api/v1/label/U__/values").expect("/label/U__/values");
    assert_eq!(status, 400, "{body}");
    let json: serde_json::Value = serde_json::from_str(body.trim()).expect("json");
    assert_eq!(json["errorType"], "bad_data");
    assert_eq!(json["error"], "invalid label name: \"\"");

    // A malformed escape reaches the engine unchanged — `200`, empty, not
    // an error. Same for a name that is merely not legacy-legal.
    for path in [
        "/api/v1/label/U__bad_zz/values",
        "/api/v1/label/U__x_/values",
        "/api/v1/label/U__U__job/values",
        // Six hex digits bail out even though five decode.
        "/api/v1/label/U__x_10ffff_y/values",
    ] {
        let (status, body) = http_get(port, path).expect("malformed escape");
        assert_eq!(status, 200, "{path}: {body}");
        assert_eq!(body.trim(), r#"{"status":"success","data":[]}"#, "{path}");
    }

    // -----------------------------------------------------------------
    // M2 — the `timeout` parameter's parse-time half, plus the two
    // positive controls (12a/12b): a healthy short-but-sufficient timeout
    // must return a real answer, not an eager timeout envelope.
    // -----------------------------------------------------------------

    for raw in ["abc", "1ns", "0", "-1"] {
        let (status, body) =
            http_get(port, &format!("/api/v1/query?query=up&timeout={raw}")).expect("bad timeout");
        assert_eq!(status, 400, "timeout={raw}: {body}");
        let json: serde_json::Value = serde_json::from_str(body.trim()).expect("json");
        assert_eq!(json["errorType"], "bad_data", "timeout={raw}");
    }

    // `/query` needs the label cache to have swept the seeded series in.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some((200, body)) = http_get(port, "/api/v1/query?query=up&timeout=5")
            && body.contains("\"job\":\"api\"")
        {
            break;
        }
        if Instant::now() > deadline {
            panic!("label cache never warmed with the seeded series within 30s");
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    // 12a: `/query` with a strictly-shorter-but-sufficient timeout returns
    // a NON-EMPTY result. An implementation that answers the
    // requested-timeout envelope whenever the parameter is present fails
    // here.
    let (status, body) =
        http_get(port, "/api/v1/query?query=up&timeout=5").expect("/query?timeout=5");
    assert_eq!(status, 200, "{body}");
    let json: serde_json::Value = serde_json::from_str(body.trim()).expect("json");
    assert!(
        !json["data"]["result"]
            .as_array()
            .expect("result")
            .is_empty(),
        "{body}"
    );

    // 12b: the same control on `/query_range`, because the branch is
    // written twice and one control would let the second site be eager.
    // `end` is deliberately AFTER the seeded sample, not at it: `now /
    // 1000` truncates the millisecond part, so an `end` of exactly that
    // second can land before the sample and the grid's last point then
    // has nothing to look back at. Measured: that fixture returned an
    // empty matrix under the per-test process model.
    let start = (now / 1000) - 60;
    let end = (now / 1000) + 60;
    let (status, body) = http_get(
        port,
        &format!("/api/v1/query_range?query=up&start={start}&end={end}&step=15&timeout=5"),
    )
    .expect("/query_range?timeout=5");
    assert_eq!(status, 200, "{body}");
    let json: serde_json::Value = serde_json::from_str(body.trim()).expect("json");
    assert_eq!(json["data"]["resultType"], "matrix", "{body}");
    assert!(
        !json["data"]["result"]
            .as_array()
            .expect("result")
            .is_empty(),
        "{body}"
    );

    drop_db(&db).await;
}
