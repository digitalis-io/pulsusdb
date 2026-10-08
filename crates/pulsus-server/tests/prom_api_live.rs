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

use pulsus_clickhouse::{ChClient, ChConnConfig, ChError, ChProto, QuerySettings, Row};

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
    fingerprint: u128,
    unix_milli: i64,
    labels: String,
}

/// Issue #623: a series is an activity row in `metric_series` and its label
/// set, once, in `metric_labels` — what the two views write from one kind-2
/// landing row.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedActivityRow {
    org_id: String,
    day: u16,
    fingerprint: u128,
    metric_name: String,
    hours: u32,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedLabelRow {
    org_id: String,
    metric_name: String,
    fingerprint: u128,
    labels: String,
    first_seen: i64,
    last_seen: i64,
}

trait InsertSeries {
    async fn insert_series(&self, rows: &[SeedSeriesRow]) -> Result<(), ChError>;
}

impl InsertSeries for ChClient {
    async fn insert_series(&self, rows: &[SeedSeriesRow]) -> Result<(), ChError> {
        let activity: Vec<SeedActivityRow> = rows
            .iter()
            .map(|r| SeedActivityRow {
                org_id: String::new(),
                day: r.unix_milli.div_euclid(86_400_000) as u16,
                fingerprint: r.fingerprint,
                metric_name: r.metric_name.clone(),
                hours: 1u32 << (r.unix_milli.rem_euclid(86_400_000) / 3_600_000),
            })
            .collect();
        let labels: Vec<SeedLabelRow> = rows
            .iter()
            .map(|r| SeedLabelRow {
                org_id: String::new(),
                metric_name: r.metric_name.clone(),
                fingerprint: r.fingerprint,
                labels: r.labels.clone(),
                first_seen: r.unix_milli,
                last_seen: r.unix_milli,
            })
            .collect();
        self.insert_block("metric_series", &activity).await?;
        self.insert_block("metric_labels", &labels).await?;
        // Issue #635: the label index and its values, as the two views
        // write them from the same kind-2 row.
        let (index, values) = index_rows(rows.iter().map(|r| (r.fingerprint, r.labels.as_str())));
        self.insert_block("metric_label_index", &index).await?;
        self.insert_block("metric_label_values", &values).await
    }
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedIndexRow {
    org_id: String,
    key: String,
    value: String,
    fingerprint: u128,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedValueRow {
    org_id: String,
    key: String,
    value: String,
}

/// One index row per label of each series, and one values row per
/// distinct key and value.
fn index_rows<'a>(
    series: impl Iterator<Item = (u128, &'a str)>,
) -> (Vec<SeedIndexRow>, Vec<SeedValueRow>) {
    let mut index = Vec::new();
    let mut pairs = std::collections::BTreeSet::new();
    for (fingerprint, labels) in series {
        let map: std::collections::BTreeMap<String, String> =
            serde_json::from_str(labels).expect("canonical label JSON");
        for (key, value) in map {
            pairs.insert((key.clone(), value.clone()));
            index.push(SeedIndexRow {
                org_id: String::new(),
                key,
                value,
                fingerprint,
            });
        }
    }
    let values = pairs
        .into_iter()
        .map(|(key, value)| SeedValueRow {
            org_id: String::new(),
            key,
            value,
        })
        .collect();
    (index, values)
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedSampleRow {
    org_id: String,
    fingerprint: u128,
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

    // The binary creates no schema; build it before the spawn or `/ready`
    // never reaches 200.
    live_db::build_schema_blocking(&db);

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
        .insert_series(&[
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
        ])
        .await
        .expect("seed metric_series");
    client
        .insert_block(
            "metric_samples",
            &[
                SeedSampleRow {
                    org_id: String::new(),
                    fingerprint: 1,
                    unix_milli: now,
                    value: 1.0,
                },
                SeedSampleRow {
                    org_id: String::new(),
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

    // The binary creates no schema; build it before the spawn or `/ready`
    // never reaches 200.
    live_db::build_schema_blocking(db);

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
        .insert_series(&[
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
        ])
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
    // Issue #499: a request with no range reads all time, which no cache
    // window covers; this test is about the cache route, so it asks for
    // the last half hour.
    let now_s = now_ms() / 1_000;
    let range = format!("start={}&end={now_s}", now_s - 1_800);
    let (status, body) = http_get(
        port,
        &format!("/api/v1/series?match[]={name_regex}&{range}"),
    )
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

    // The binary creates no schema; build it before the spawn or `/ready`
    // never reaches 200.
    live_db::build_schema_blocking(db);

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
        .insert_series(&[
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
        ])
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
        // Issue #499: the cache route needs a range inside its window.
        let now_s = now_ms() / 1_000;
        let range = format!("start={}&end={now_s}", now_s - 1_800);
        let (status, body) = http_get(
            port,
            &format!("/api/v1/{path}?match[]={name_regex_all}&{range}"),
        )
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

    // The binary creates no schema; build it before the spawn or `/ready`
    // never reaches 200.
    live_db::build_schema_blocking(db);

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
                "INSERT INTO {db}.metric_landing (kind, metric_name, fingerprint, unix_milli, labels) \
                 SELECT 2, '{PROBE_METRIC}', number + 1, {recent_bucket}, \
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
    org_id: String,
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

    // The binary creates no schema; build it before the spawn or `/ready`
    // never reaches 200.
    live_db::build_schema_blocking(&db);

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
        .insert_series(
            &series
                .iter()
                .map(|(name, fp, labels)| SeedSeriesRow {
                    metric_name: (*name).to_string(),
                    fingerprint: u128::from(*fp),
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
                .map(|(_, fp, _)| SeedSampleRow {
                    org_id: String::new(),
                    fingerprint: u128::from(*fp),
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
                    org_id: String::new(),
                    metric_name: "up".to_string(),
                    metric_type: "gauge".to_string(),
                    help: "up".to_string(),
                    unit: String::new(),
                    updated_ns: now * 1_000_000,
                },
                SeedMetadataRow {
                    org_id: String::new(),
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

// ---------------------------------------------------------------------
// Issue #472 — `/api/v1/label/__name__/values` end to end
// ---------------------------------------------------------------------

/// One `count()` off `system.query_log`.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct StatementCountRow {
    n: u64,
}

/// One logged statement's text.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct StatementTextRow {
    query: String,
}

async fn flush_logs(admin: &ChClient) {
    admin
        .execute(
            "SYSTEM FLUSH LOGS",
            &QuerySettings::new(),
            pulsus_clickhouse::Idempotency::Idempotent,
        )
        .await
        .expect("flush logs");
}

/// `count()` of finalized `Select`s this run's database saw whose text
/// satisfies `predicate` — a whole SQL boolean over the `query` column
/// (`query LIKE '…'`, optionally `AND query NOT LIKE '…'`), because the
/// #472 builder and the #96 degraded probe share a projection prefix and
/// are told apart by what the probe additionally carries.
async fn statements_matching(admin: &ChClient, db: &str, predicate: &str) -> u64 {
    use futures::StreamExt;
    let sql = format!(
        "SELECT count() AS n FROM system.query_log \
         WHERE current_database = '{db}' AND type != 'QueryStart' \
           AND query_kind = 'Select' AND ({predicate})"
    );
    let mut stream = admin
        .query_stream::<StatementCountRow>(&sql, &QuerySettings::new())
        .await
        .expect("query system.query_log");
    let mut n = 0u64;
    while let Some(row) = stream.next().await {
        n = row.expect("decode count row").n;
    }
    n
}

/// The distinct statement texts this run's database saw, for the test
/// output — so a failure shows what actually dispatched instead of only
/// that a count was wrong.
async fn statement_texts(admin: &ChClient, db: &str) -> Vec<String> {
    use futures::StreamExt;
    let sql = format!(
        "SELECT DISTINCT query FROM system.query_log \
         WHERE current_database = '{db}' AND type != 'QueryStart' \
           AND query_kind = 'Select' AND query LIKE '%metric_series%' ORDER BY query"
    );
    let mut stream = admin
        .query_stream::<StatementTextRow>(&sql, &QuerySettings::new())
        .await
        .expect("query system.query_log");
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode statement row").query);
    }
    out
}

/// Issue #472 — every `/api/v1/label/__name__/values` body, byte for byte,
/// plus proof that the narrow statement is what dispatched.
///
/// **The response is unchanged by this issue, so no test that only reads
/// the response can prove the fix happened** — a body assertion passes
/// identically on both sides of it. The `system.query_log` half is what
/// distinguishes them, and it is scoped to this run's nonce'd database
/// because `system.query_log` outlives databases (the
/// `promql_memory_breach_is_422_and_actually_dispatched` precedent).
///
/// **The fixture is adversarial and the activity bucket is deliberately the
/// current one.** A one-character name sorting before everything; a name
/// that is a strict prefix of another; a non-ASCII metric name and a
/// non-ASCII label value carrying a `/`; an empty label value; a `+Inf`
/// label value. Rows are seeded **directly into `metric_series`** (this
/// suite's established style), so nothing here exercises the writer's
/// registration path. They are seeded at the CURRENT activity bucket on
/// purpose: the default window is all time; the fixture is the only data
/// (issue #499), so the no-bounds request answers exactly what was
/// seeded.
///
/// The `{zone=""}` rejection's wording differs from the reference's
/// (`vector selector must contain at least one non-empty matcher` against
/// its own `match[] must …`) at the same status and `errorType`. That
/// divergence is **pre-existing and out of scope here** — recorded so the
/// next reader comparing the two responses does not rediscover and file it.
#[tokio::test(flavor = "multi_thread")]
async fn prom_api_name_values_bodies_and_narrow_dispatch_issue_472() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test");
        return;
    }

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis();
    let db = pulsus_testkit::test_db(&format!("pulsus_prom_472_it_{nonce}"));
    let db = db.as_str();
    let port: u16 = 31_303;

    // The binary creates no schema; build it before the spawn or `/ready`
    // never reaches 200.
    live_db::build_schema_blocking(db);

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
    // `h` is the CURRENT activity bucket — see the doc comment.
    let h = (now_ms() / bucket_ms) * bucket_ms;
    let corpus: &[(&str, u64, &str)] = &[
        ("a", 1001, r#"{"job":"api"}"#),
        (
            "http_requests_total",
            2001,
            r#"{"instance":"host-1:9100","job":"api"}"#,
        ),
        (
            "http_requests_total",
            2002,
            r#"{"instance":"host-2:9100","job":"web"}"#,
        ),
        (
            "http_requests_total",
            2003,
            r#"{"instance":"host-3:9100","job":"api","zone":""}"#,
        ),
        (
            "http_requests_created",
            3001,
            r#"{"instance":"host-1:9100","job":"api"}"#,
        ),
        (
            "http_request_duration_seconds_bucket",
            4001,
            r#"{"instance":"host-1:9100","job":"api","le":"0.1"}"#,
        ),
        (
            "http_request_duration_seconds_bucket",
            4002,
            r#"{"instance":"host-1:9100","job":"api","le":"+Inf"}"#,
        ),
        (
            "node_cpu_seconds_total",
            5001,
            r#"{"cpu":"0","instance":"host-9:9100","job":"node","mode":"idle"}"#,
        ),
        (
            "node_cpu_seconds_total",
            5002,
            r#"{"cpu":"1","instance":"host-9:9100","job":"node","mode":"user"}"#,
        ),
        ("up", 6001, r#"{"instance":"host-1:9100","job":"api"}"#),
        ("up", 6002, r#"{"instance":"host-9:9100","job":"node"}"#),
        (
            "käse_temperatur_celsius",
            7001,
            r#"{"job":"küche","raum":"kühl/lager"}"#,
        ),
    ];
    client
        .insert_series(
            &corpus
                .iter()
                .map(|(name, fp, labels)| SeedSeriesRow {
                    metric_name: (*name).to_string(),
                    fingerprint: u128::from(*fp),
                    unix_milli: h,
                    labels: (*labels).to_string(),
                })
                .collect::<Vec<_>>(),
        )
        .await
        .expect("seed metric_series");

    let start = (h - 3_600_000) / 1000;
    let end = (h + 3_600_000) / 1000;
    let bounds = format!("start={start}&end={end}");
    const ALL_SEVEN: &str = "{\"status\":\"success\",\"data\":[\"a\",\
        \"http_request_duration_seconds_bucket\",\"http_requests_created\",\
        \"http_requests_total\",\"käse_temperatur_celsius\",\"node_cpu_seconds_total\",\"up\"]}";

    // The discovery client's own first resource call.
    let (status, body) = http_get(
        port,
        &format!("/api/v1/label/__name__/values?limit=40000&{bounds}"),
    )
    .expect("/label/__name__/values");
    assert_eq!(status, 200, "{body}");
    assert_eq!(body.trim(), ALL_SEVEN);

    // No bounds at all: the default window is all time; the fixture is
    // the only data (issue #499).
    let (status, body) = http_get(port, "/api/v1/label/__name__/values").expect("no bounds");
    assert_eq!(status, 200, "{body}");
    assert_eq!(body.trim(), ALL_SEVEN);

    // The limit boundary — applied last, to the already-sorted answer.
    let (status, body) = http_get(
        port,
        &format!("/api/v1/label/__name__/values?limit=3&{bounds}"),
    )
    .expect("limit=3");
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body.trim(),
        "{\"status\":\"success\",\"data\":[\"a\",\"http_request_duration_seconds_bucket\",\
         \"http_requests_created\"],\"warnings\":[\"results truncated due to limit\"]}"
    );

    // A label matcher. `node_cpu_seconds_total` and
    // `käse_temperatur_celsius` are absent and `up` is present — a builder
    // that dropped the matcher conjunct would return all seven.
    let (status, body) = http_get(
        port,
        &format!("/api/v1/label/__name__/values?match[]=%7Bjob%3D%22api%22%7D&{bounds}"),
    )
    .expect("match[]={job=\"api\"}");
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body.trim(),
        "{\"status\":\"success\",\"data\":[\"a\",\"http_request_duration_seconds_bucket\",\
         \"http_requests_created\",\"http_requests_total\",\"up\"]}"
    );

    // The concrete-name arm. Dropping the `metric_name =` head returns all
    // seven; confusing the prefix returns two.
    let (status, body) = http_get(
        port,
        &format!("/api/v1/label/__name__/values?match[]=http_requests_total&{bounds}"),
    )
    .expect("match[]=http_requests_total");
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body.trim(),
        "{\"status\":\"success\",\"data\":[\"http_requests_total\"]}"
    );

    let (status, body) = http_get(
        port,
        &format!("/api/v1/label/__name__/values?match[]=nosuchmetric&{bounds}"),
    )
    .expect("match[]=nosuchmetric");
    assert_eq!(status, 200, "{body}");
    assert_eq!(body.trim(), "{\"status\":\"success\",\"data\":[]}");

    // Two `match[]`, one narrow query each, unioned in Rust exactly where
    // the wide path unioned. The non-ASCII value survives into SQL.
    let (status, body) = http_get(
        port,
        &format!(
            "/api/v1/label/__name__/values?match[]=%7Bjob%3D%22node%22%7D\
             &match[]=%7Bjob%3D%22k%C3%BCche%22%7D&{bounds}"
        ),
    )
    .expect("two match[]");
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body.trim(),
        "{\"status\":\"success\",\"data\":[\"käse_temperatur_celsius\",\
         \"node_cpu_seconds_total\",\"up\"]}"
    );

    // Must FAIL, unchanged.
    let (status, body) = http_get(
        port,
        &format!("/api/v1/label/__name__/values?limit=-1&{bounds}"),
    )
    .expect("limit=-1");
    assert_eq!(status, 400, "{body}");
    assert_eq!(
        body.trim(),
        "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"invalid parameter \
         \\\"limit\\\": limit must be non-negative\"}"
    );

    let (status, body) = http_get(
        port,
        &format!("/api/v1/label/__name__/values?match[]=%7Bzone%3D%22%22%7D&{bounds}"),
    )
    .expect("match[]={zone=\"\"}");
    assert_eq!(status, 400, "{body}");
    assert_eq!(
        body.trim(),
        "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"vector selector must \
         contain at least one non-empty matcher\"}"
    );

    // The unchanged neighbours: the `name != "__name__"` branch and the
    // endpoint this issue does not touch.
    const JOB_VALUES: &str =
        "{\"status\":\"success\",\"data\":[\"api\",\"küche\",\"node\",\"web\"]}";
    for path in ["/api/v1/label/job/values", "/api/v1/label/U__job/values"] {
        let (status, body) = http_get(port, &format!("{path}?{bounds}")).expect("label values");
        assert_eq!(status, 200, "{path}: {body}");
        assert_eq!(body.trim(), JOB_VALUES, "{path}");
    }
    let (status, body) = http_get(port, &format!("/api/v1/labels?{bounds}")).expect("/labels");
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body.trim(),
        "{\"status\":\"success\",\"data\":[\"__name__\",\"cpu\",\"instance\",\"job\",\"le\",\
         \"mode\",\"raum\",\"zone\"]}"
    );

    // -----------------------------------------------------------------
    // Proof of dispatch, and that the adjudicated route stayed wide.
    // -----------------------------------------------------------------
    let admin = ChClient::new(test_ch_config("default"))
        .await
        .expect("connect (admin)");
    flush_logs(&admin).await;

    // The #472 statement, told apart from the #96 degraded probe: both
    // begin `SELECT DISTINCT metric_name FROM metric_series WHERE org_id =
    // '' AND day BETWEEN` (issues #623 and #635), and the probe additionally
    // carries the name-matcher regex's compile probe and a `LIMIT <fanout
    // cap + 1>`. The discovery `limit` is a response-size cap applied last
    // (docs/api.md §3.3), so the #472 statement never carries a `LIMIT` at
    // all.
    const NARROW_PREFIX: &str = "query LIKE 'SELECT DISTINCT metric_name\\nFROM metric_series\\nWHERE org_id = \\'\\'\\n  AND day BETWEEN %'";
    let narrow_472 = format!("{NARROW_PREFIX} AND query NOT LIKE '%\\nLIMIT %'");
    let narrow_472 = narrow_472.as_str();
    // The #96 degraded probe, named unambiguously: the same projection
    // prefix, PLUS the bounded `LIMIT <fanout cap + 1>` and the anchored
    // `match(metric_name, …)` predicate only a name-matcher selector
    // renders. The #472 builder can carry neither.
    let probe_96 = format!(
        "{NARROW_PREFIX} AND query LIKE '%\\nLIMIT %' AND query LIKE '%match(metric_name,%'"
    );
    let probe_96 = probe_96.as_str();
    let narrow = statements_matching(&admin, db, narrow_472).await;
    assert!(
        narrow > 0,
        "no narrow `SELECT DISTINCT metric_name` statement dispatched for {db} — a \
         body-only assertion would pass on both sides of this fix.\nstatements:\n{}",
        statement_texts(&admin, db).await.join("\n---\n")
    );
    assert_eq!(
        statements_matching(&admin, db, probe_96).await,
        0,
        "no request so far takes the degraded-probe route, so every narrow statement here \
         must be the #472 builder's — otherwise the count above is measuring the probe"
    );

    // A `SELECT DISTINCT metric_name` naming `fingerprint` or `labels` in
    // its projection is impossible under the `LIKE` above — the projection
    // is pinned from the first byte. The other direction needs a marker
    // that only a `__name__` request could have produced, because
    // `/api/v1/labels` and `/api/v1/label/job/values` legitimately still
    // read the wide statement over the same window (measured: an
    // unfiltered wide read appears in this run's log, and it is theirs plus
    // the label-cache sweep's own unbounded-upper query — a different path,
    // out of scope here). `match[]=http_requests_total` is that marker: it
    // is the only request in this test that renders `metric_name =
    // 'http_requests_total'`, and the regex route renders `metric_name IN
    // (…)` rather than `= '…'`. Since issue #623 the name is a lookup
    // predicate in both statements. The quotes are backslash-escaped because
    // the pattern becomes a ClickHouse string literal.
    // Issue #635: the lookup's tenant term leads it, so the name follows
    // on an `AND` line.
    const CONCRETE: &str = "%AND metric_name = \\'http_requests_total\\'%";
    // The wide statement's head since issue #623: statement 2, the lookup
    // rows of the series the activity read finds.
    const WIDE_HEAD: &str =
        "SELECT fingerprint, any(name) AS metric_name, any(label_text) AS labels\\nFROM (";
    let concrete_narrow = statements_matching(
        &admin,
        db,
        &format!("query LIKE 'SELECT DISTINCT metric_name\\nFROM metric_series{CONCRETE}'"),
    )
    .await;
    let concrete_wide =
        statements_matching(&admin, db, &format!("query LIKE '{WIDE_HEAD}{CONCRETE}'")).await;
    assert!(
        concrete_narrow > 0,
        "the concrete-name `__name__` request must render the narrow projection.\nstatements:\n{}",
        statement_texts(&admin, db).await.join("\n---\n")
    );
    assert_eq!(
        concrete_wide,
        0,
        "`/api/v1/label/__name__/values?match[]=http_requests_total` must no longer dispatch \
         the wide `fingerprint, metric_name, labels` read — this is the marker no other \
         request in this test can produce.\nstatements:\n{}",
        statement_texts(&admin, db).await.join("\n---\n")
    );

    // The adjudicated name-matcher route (#85/#89/#96) must be untouched:
    // the narrow projection must not reach it, and the route must still run
    // its OWN two statements.
    //
    // **This leg widens the window to `H + 2h`, and that is what makes the
    // assertion below deterministic rather than a coin flip.** Which of the
    // two adjudicated shapes runs depends on whether the resident label
    // cache is authoritative for the *request* window: the cache is
    // non-authoritative once `end` is past its sweep time plus
    // `staleness_multiplier * PULSUS_CACHE_TTL`. With `end` at the end of
    // the current hour that holds for all but the last few seconds of the
    // hour — so the degraded route is what normally runs, but not always.
    // With `end` two hours past the bucket start it is at least one hour
    // ahead of `now` on every run, so the degraded two-stage route is
    // forced. The answer does not move with the wider window: nothing is
    // seeded past `H`.
    //
    // Asserting the pair POSITIVELY is the point. "Zero narrow statements
    // and at least one wide fetch" is satisfied by a stale cache wrongly
    // treated as authoritative, which would answer from the cache's own
    // resolution and omit newly active names — a wrong answer passing the
    // check.
    let narrow_before = narrow;
    // Issue #623: the names-only fetch is statement 2 scoped to the probed
    // names on the lookup, after its tenant term (issue #635).
    const IN_FETCH: &str = "query LIKE 'SELECT fingerprint, any(name) AS metric_name, \
                            any(label_text) AS labels\\nFROM (\\n  SELECT fingerprint, \
                            metric_name AS name, labels AS label_text\\n  FROM metric_labels\\n  \
                            WHERE org_id = \\'\\'\\n    AND metric_name IN (%'";
    let in_fetch_before = statements_matching(&admin, db, IN_FETCH).await;
    let probes_before = statements_matching(&admin, db, probe_96).await;

    let regex_end = (h + 2 * 3_600_000) / 1000;
    let name_regex = "%7B__name__%3D~%22http_requests.%2A%22%7D";
    let (status, regex_body) = http_get(
        port,
        &format!(
            "/api/v1/label/__name__/values?match[]={name_regex}&start={start}&end={regex_end}"
        ),
    )
    .expect("match[]={__name__=~...}");
    assert_eq!(status, 200, "{regex_body}");
    assert_eq!(
        regex_body.trim(),
        "{\"status\":\"success\",\"data\":[\"http_requests_created\",\"http_requests_total\"]}",
        "`http_request_duration_seconds_bucket` matches the fragment but not the anchored \
         pattern, so it must stay absent"
    );

    flush_logs(&admin).await;
    let statements = statement_texts(&admin, db).await.join("\n---\n");
    assert_eq!(
        statements_matching(&admin, db, narrow_472).await,
        narrow_before,
        "the regex-`__name__` route must not render the narrow projection — its fan-out \
         semantics are adjudicated (#85/#89/#96, docs/api.md §3.3).\nstatements:\n{statements}"
    );
    assert_eq!(
        statements_matching(&admin, db, probe_96).await - probes_before,
        1,
        "the degraded route must run exactly ONE bounded `SELECT DISTINCT metric_name … \
         match(metric_name, …) … LIMIT <cap + 1>` probe. Zero means the cache was treated as \
         authoritative for a window it cannot cover, which answers from its own resolution \
         and would omit newly active names.\nstatements:\n{statements}"
    );
    assert_eq!(
        statements_matching(&admin, db, IN_FETCH).await - in_fetch_before,
        1,
        "…and exactly ONE wide `metric_name IN (…)` fetch after it — the second half of the \
         adjudicated pair, which is where this route reads the series' label \
         sets.\nstatements:\n{statements}"
    );

    eprintln!(
        "#472 statements this run's database saw:\n{}",
        statement_texts(&admin, db).await.join("\n---\n")
    );

    drop_db(db).await;
}

// ---------------------------------------------------------------------
// Issue #539: two series whose `bs` label differs only at U+0008.
//
// The stored label text is `LabelSet::to_canonical_json`'s output — the
// exact expression `MetricSeriesRow::from_series_at_bucket` uses at
// `crates/pulsus-write/src/writer/rows.rs:344` — so this seed is the
// writer's own bytes and not a hand-written literal.
//
// Before the fix both series decoded to the same label set and the
// evaluator refused the whole query with `422 vector cannot contain
// metrics with the same labelset`.
// ---------------------------------------------------------------------

/// `a` + `c` + `b`.
fn around(c: char) -> String {
    format!("a{c}b")
}

/// The two seeded series, in the order their fingerprints are assigned:
/// the U+0008 one carries value 1, the three-letter one carries value 2.
/// Returns the sample timestamp, so a caller can evaluate both routes at
/// one instant rather than at two.
async fn seed_c0_series(client: &ChClient, values: [f64; 2]) -> i64 {
    let bucket_ms: i64 = 3_600_000;
    let now = now_ms();
    let recent_bucket = (now / bucket_ms) * bucket_ms;
    let label_json = |bs: &str| {
        let (set, _collisions) = pulsus_model::LabelSet::from_normalized(vec![
            ("bs".to_string(), bs.to_string()),
            ("job".to_string(), "m539".to_string()),
        ]);
        set.to_canonical_json()
    };
    client
        .insert_series(&[
            SeedSeriesRow {
                metric_name: "t539".to_string(),
                fingerprint: 1,
                unix_milli: recent_bucket,
                labels: label_json(&around('\u{8}')),
            },
            SeedSeriesRow {
                metric_name: "t539".to_string(),
                fingerprint: 2,
                unix_milli: recent_bucket,
                labels: label_json("abb"),
            },
        ])
        .await
        .expect("seed metric_series");
    client
        .insert_block(
            "metric_samples",
            &[
                SeedSampleRow {
                    org_id: String::new(),
                    fingerprint: 1,
                    unix_milli: now,
                    value: values[0],
                },
                SeedSampleRow {
                    org_id: String::new(),
                    fingerprint: 2,
                    unix_milli: now,
                    value: values[1],
                },
            ],
        )
        .await
        .expect("seed metric_samples");
    now
}

fn urlencode_query(s: &str) -> String {
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

/// Spawns a server against `db` on `port` with `extra_env`, waits for
/// `/ready`.
fn spawn_prom_server(port: u16, db: &str, extra_env: &[(&str, &str)]) -> ChildGuard {
    // The binary creates no schema; build it before the spawn or `/ready`
    // never reaches 200.
    live_db::build_schema_blocking(db);

    let mut command = Command::new(env!("CARGO_BIN_EXE_pulsusdb"));
    command
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
        .env("CLICKHOUSE_DB", db);
    for (k, v) in extra_env {
        command.env(k, v);
    }
    let guard = ChildGuard(command.spawn().expect("spawn pulsusdb"));
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if let Some((200, _)) = http_get(port, "/ready") {
            return guard;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("/ready never reached 200 within 60s (port {port})");
}

/// Polls `path` until it answers 200 with a body `accept` likes, and
/// returns the parsed body. The label cache sweeps on a timer, so a
/// query over freshly seeded series needs a bounded wait.
fn wait_for_body(
    port: u16,
    path: &str,
    accept: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(40);
    let mut last = String::new();
    loop {
        if let Some((status, body)) = http_get(port, path) {
            last = format!("{status} {body}");
            if status == 200
                && let Ok(json) = serde_json::from_str::<serde_json::Value>(&body)
                && accept(&json)
            {
                return json;
            }
        }
        assert!(
            Instant::now() < deadline,
            "{path} never answered acceptably within 40s; last was {last}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// `[(the series' `bs` label, its value)]`, sorted by the label — the
/// shape every assertion below is written against.
fn vector_bs_values(json: &serde_json::Value) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = json["data"]["result"]
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .map(|s| {
            (
                s["metric"]["bs"].as_str().unwrap_or_default().to_string(),
                s["value"][1].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    out.sort();
    out
}

/// Q7, Q8 and Q9 of issue #539, and the criterion that pins the cache
/// path and the SQL path to the same answer
/// (`crates/pulsus-read/src/metrics/labels.rs:630-633`).
#[tokio::test(flavor = "multi_thread")]
async fn two_metric_series_differing_only_at_a_c0_escape_stay_two_series() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test");
        return;
    }
    let db = &pulsus_testkit::test_db("pulsus_prom_api_c0_escape_it");
    drop_db(db).await;
    let cache_port: u16 = 31_230;
    let backspace = around('\u{8}');

    let guard = spawn_prom_server(cache_port, db, &[]);
    let client = ChClient::new(test_ch_config(db))
        .await
        .expect("connect to seed data");
    let _sample_ms = seed_c0_series(&client, [1.0, 2.0]).await;

    // Q7 — the whole metric. Two series, not one, and HTTP 200: before the
    // fix the two label sets collided and the evaluator answered 422.
    let all = wait_for_body(cache_port, "/api/v1/query?query=t539", |json| {
        json["data"]["result"]
            .as_array()
            .is_some_and(|r| r.len() == 2)
    });
    assert_eq!(
        vector_bs_values(&all),
        vec![
            (backspace.clone(), "1".to_string()),
            ("abb".to_string(), "2".to_string()),
        ],
        "t539 must return both series, each with its own value: {all}"
    );

    // …and the selector on the U+0008 value picks the other one. The
    // PromQL lexer decodes `\b` itself, so the matcher carries the right
    // byte and only the decode of the STORED value was ever wrong.
    let backspace_path = format!(
        "/api/v1/query?query={}",
        urlencode_query(r#"t539{bs="a\bb"}"#)
    );
    let via_backspace = wait_for_body(cache_port, &backspace_path, |json| {
        json["data"]["result"]
            .as_array()
            .is_some_and(|r| r.len() == 1)
    });
    assert_eq!(
        vector_bs_values(&via_backspace),
        vec![(backspace.clone(), "1".to_string())],
        "the backspace selector must pick the U+0008 series: {via_backspace}"
    );

    // Q9 — the discovery endpoints see two series and two label values.
    let series = wait_for_body(cache_port, "/api/v1/series?match[]=t539", |json| {
        json["data"].as_array().is_some_and(|r| r.len() == 2)
    });
    let mut seen: Vec<&str> = series["data"]
        .as_array()
        .expect("two series")
        .iter()
        .map(|s| s["bs"].as_str().unwrap_or_default())
        .collect();
    seen.sort();
    assert_eq!(
        seen,
        vec![backspace.as_str(), "abb"],
        "/series must list both label values: {series}"
    );

    let values = wait_for_body(cache_port, "/api/v1/label/bs/values", |json| {
        json["data"].as_array().is_some_and(|r| r.len() == 2)
    });
    let listed: Vec<&str> = values["data"]
        .as_array()
        .expect("two values")
        .iter()
        .map(|v| v.as_str().unwrap_or_default())
        .collect();
    assert_eq!(
        listed,
        vec![backspace.as_str(), "abb"],
        "/label/bs/values must list both: {values}"
    );

    drop(guard);
    drop_db(db).await;
}

/// Criterion 9 of issue #539: **the two metric routes answer the same
/// question the same way.**
///
/// A selector is served either from the in-process label cache or by
/// pushing `JSONExtractString(labels, 'bs') = 'abb'` down to ClickHouse,
/// and which one runs is a runtime decision
/// (`crates/pulsus-read/src/metrics/labels.rs:9-14`). The cache decodes
/// the stored label text with our decoder; the SQL route lets ClickHouse
/// decode it. So the two agree only while our decoder agrees with
/// ClickHouse's, which is what
/// `crates/pulsus-read/src/metrics/labels.rs:630-633` states as a
/// contract and what nothing checked.
///
/// `PULSUS_CACHE_MAX_SERIES=1` puts the second server over its
/// cardinality ceiling, which is one of the runtime conditions that sends
/// a selector down the SQL route.
///
/// This is a case of its own rather than a step of the one above, so that
/// the route disagreement reddens by itself: with the `\b` arm removed
/// the cache route answers `422 vector cannot contain metrics with the
/// same labelset` and the SQL route answers `200` with one series
/// (measured), and that difference IS the assertion here.
#[tokio::test(flavor = "multi_thread")]
async fn the_cache_route_and_the_sql_route_answer_a_c0_selector_identically() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test");
        return;
    }
    let db = &pulsus_testkit::test_db("pulsus_prom_api_c0_routes_it");
    drop_db(db).await;
    let cache_port: u16 = 31_232;
    let sql_port: u16 = 31_233;

    let cache_guard = spawn_prom_server(cache_port, db, &[]);
    let client = ChClient::new(test_ch_config(db))
        .await
        .expect("connect to seed data");
    let sample_ms = seed_c0_series(&client, [1.0, 2.0]).await;
    let sql_guard = spawn_prom_server(sql_port, db, &[("PULSUS_CACHE_MAX_SERIES", "1")]);

    // Both routes are asked for the value at ONE instant: `/query` with no
    // `time` evaluates at the server's own now, and two requests a
    // millisecond apart carry two different timestamps in the body, which
    // is a difference about the clock and not about the decoder.
    let letters_path = format!(
        "/api/v1/query?query={}&time={}.{:03}",
        urlencode_query(r#"t539{bs="abb"}"#),
        sample_ms / 1000,
        sample_ms % 1000
    );

    // Wait for the cache sweep to have SEEN the seeded series, without
    // waiting for it to answer correctly: a cold cache returns an empty
    // vector, so "any result at all, or a definite error" is the point at
    // which the two routes can be compared. This terminates whether the
    // decoder is right or wrong.
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        match http_get(cache_port, "/api/v1/query?query=t539") {
            Some((422, _)) => break,
            Some((200, body))
                if serde_json::from_str::<serde_json::Value>(&body)
                    .ok()
                    .and_then(|j| j["data"]["result"].as_array().map(|r| !r.is_empty()))
                    .unwrap_or(false) =>
            {
                break;
            }
            _ => {}
        }
        assert!(
            Instant::now() < deadline,
            "the label cache never swept the seeded series in within 40s"
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    let (cache_status, cache_body) = http_get(cache_port, &letters_path).expect("cache route");
    let (sql_status, sql_body) = http_get(sql_port, &letters_path).expect("sql route");
    assert_eq!(
        (cache_status, cache_body.trim()),
        (sql_status, sql_body.trim()),
        "the two metric routes must answer this selector identically"
    );

    // …and the answer they agree on is the right one.
    let json: serde_json::Value = serde_json::from_str(&cache_body).expect("a JSON body");
    assert_eq!(
        vector_bs_values(&json),
        vec![("abb".to_string(), "2".to_string())],
        "the three-letter selector must return its own series only: {cache_body}"
    );

    drop(sql_guard);
    drop(cache_guard);
    drop_db(db).await;
}

// ---------------------------------------------------------------------
// Issue #495: remote write stores label names as sent
// ---------------------------------------------------------------------

/// Bare HTTP/1.1 POST of `body` with `content_type`.
fn http_post_bytes(port: u16, path: &str, content_type: &str, body: &[u8]) -> (u16, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(30))).ok();
    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: {content_type}\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    request.extend_from_slice(body);
    stream.write_all(&request).expect("send");
    let mut buf = String::new();
    stream.read_to_string(&mut buf).ok();
    let mut parts = buf.splitn(2, "\r\n\r\n");
    let head = parts.next().unwrap_or_default();
    let body = decode_body(head, parts.next().unwrap_or(""));
    let status = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    (status, body)
}

/// One remote-write series: `(labels, value, ms)`.
type Series495<'a> = (&'a [(&'a str, &'a str)], f64, i64);

/// A remote write of one series per entry, each `(labels, value, ms)`.
fn remote_write_495(series: &[Series495<'_>]) -> Vec<u8> {
    use prost::Message;
    use pulsus_write::protocols::remote_write::{Label, Sample, TimeSeries, WriteRequest};
    let req = WriteRequest {
        timeseries: series
            .iter()
            .map(|(labels, value, ms)| TimeSeries {
                labels: labels
                    .iter()
                    .map(|(n, v)| Label {
                        name: n.to_string(),
                        value: v.to_string(),
                    })
                    .collect(),
                samples: vec![Sample {
                    value: *value,
                    timestamp: *ms,
                }],
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy-compress the write")
}

/// T5's two pushes: an OTLP gauge `checkout_requests` with the data-point
/// attribute `service.name=checkout`, value 1 at `t`, and a remote write
/// of `{__name__="checkout_requests", "service.name"="checkout"}`, value 2
/// at `t + 15 s`.
fn push_checkout_495(port: u16, t: i64) {
    let otlp = format!(
        r#"{{"resourceMetrics":[{{"scopeMetrics":[{{"metrics":[
             {{"name":"checkout_requests","gauge":{{"dataPoints":[
               {{"timeUnixNano":"{}","asDouble":1,
                 "attributes":[{{"key":"service.name","value":{{"stringValue":"checkout"}}}}]}}]}}}}
           ]}}]}}]}}"#,
        t * 1_000_000
    );
    let (status, body) = http_post_bytes(port, "/v1/metrics", "application/json", otlp.as_bytes());
    assert_eq!(status, 200, "the OTLP push: {body}");
    let rw = remote_write_495(&[(
        &[
            ("__name__", "checkout_requests"),
            ("service.name", "checkout"),
        ],
        2.0,
        t + 15_000,
    )]);
    let (status, body) = http_post_bytes(port, "/api/v1/write", "application/x-protobuf", &rw);
    assert_eq!(status, 204, "the remote write: {body}");
}

/// The anchor of the #495 tests: five minutes ago, minute-aligned.
fn anchor_495() -> i64 {
    (now_ms() / 60_000) * 60_000 - 300_000
}

/// `query_range` of `q` over `[t, t + 15 s]` at 15 s, as `(labels, values)`
/// per series, sorted.
fn range_495(port: u16, q: &str, t: i64) -> Vec<(serde_json::Value, Vec<String>)> {
    let path = format!(
        "/api/v1/query_range?query={}&start={}&end={}&step=15",
        urlencode_query(q),
        t as f64 / 1000.0,
        (t + 15_000) as f64 / 1000.0
    );
    let (status, body) = http_get(port, &path).expect("query_range reachable");
    assert_eq!(status, 200, "{q}: {body}");
    let json: serde_json::Value = serde_json::from_str(&body).expect("JSON");
    let mut out: Vec<(serde_json::Value, Vec<String>)> = json["data"]["result"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|s| {
            let values = s["values"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .iter()
                .map(|p| p[1].as_str().unwrap_or_default().to_string())
                .collect();
            (s["metric"].clone(), values)
        })
        .collect();
    out.sort_by_key(|(m, _)| m.to_string());
    out
}

/// The instant answer of `q` at `at_ms`, as `(labels, value)` per series.
fn instant_495(port: u16, q: &str, at_ms: i64) -> Vec<(serde_json::Value, String)> {
    let path = format!(
        "/api/v1/query?query={}&time={}",
        urlencode_query(q),
        at_ms as f64 / 1000.0
    );
    let (status, body) = http_get(port, &path).expect("query reachable");
    assert_eq!(status, 200, "{q}: {body}");
    let json: serde_json::Value = serde_json::from_str(&body).expect("JSON");
    let mut out: Vec<(serde_json::Value, String)> = json["data"]["result"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|s| {
            (
                s["metric"].clone(),
                s["value"][1].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    out.sort_by_key(|(m, _)| m.to_string());
    out
}

/// Waits until `q` at `t + 15 s` answers `want` series: the writer flushes
/// and the cache sweeps on timers.
fn wait_for_series_495(port: u16, q: &str, t: i64, want: usize) {
    let deadline = Instant::now() + Duration::from_secs(40);
    while Instant::now() < deadline {
        if instant_495(port, q, t + 15_000).len() >= want {
            return;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// **T5 (issue #495): a dotted label is one series on both transports**,
/// with the OTLP translation strategy that keeps names.
#[tokio::test(flavor = "multi_thread")]
async fn a_dotted_label_is_one_series_on_both_transports() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test");
        return;
    }
    let db = &pulsus_testkit::test_db("pulsus_prom_api_495_t5_it");
    drop_db(db).await;
    let port: u16 = 31_810;
    let _guard = spawn_prom_server(
        port,
        db,
        &[("PULSUS_OTLP_TRANSLATION_STRATEGY", "NoTranslation")],
    );
    let t = anchor_495();
    push_checkout_495(port, t);
    wait_for_series_495(port, "checkout_requests", t, 2);

    let path = format!(
        "/api/v1/series?match[]={}&start={}&end={}",
        urlencode_query(r#"{"service.name"="checkout"}"#),
        (t - 60_000) / 1000,
        (t + 60_000) / 1000
    );
    let (status, body) = http_get(port, &path).expect("series reachable");
    assert_eq!(status, 200, "{body}");
    let json: serde_json::Value = serde_json::from_str(&body).expect("JSON");
    assert_eq!(
        json["data"],
        serde_json::json!([{"__name__": "checkout_requests", "service.name": "checkout"}]),
        "T5 /series"
    );
    for q in [
        r#"{"service.name"="checkout"}"#,
        r#"checkout_requests{"service.name"="checkout"}"#,
        r#"{__name__="checkout_requests", "service.name"="checkout"}"#,
    ] {
        assert_eq!(
            range_495(port, q, t),
            vec![(
                serde_json::json!({"__name__": "checkout_requests", "service.name": "checkout"}),
                vec!["1".to_string(), "2".to_string()]
            )],
            "T5 {q}"
        );
    }
    assert!(
        range_495(port, r#"{service_name="checkout"}"#, t).is_empty(),
        "T5: nothing is stored as service_name"
    );
    drop_db(db).await;
}

/// **T6 (issue #495): under the default strategy OTLP escapes the name and
/// remote write does not**, so the two pushes are two series.
#[tokio::test(flavor = "multi_thread")]
async fn under_the_default_strategy_otlp_escapes_and_remote_write_does_not() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test");
        return;
    }
    let db = &pulsus_testkit::test_db("pulsus_prom_api_495_t6_it");
    drop_db(db).await;
    let port: u16 = 31_811;
    let _guard = spawn_prom_server(port, db, &[]);
    let t = anchor_495();
    push_checkout_495(port, t);
    wait_for_series_495(port, "checkout_requests", t, 2);
    let at = t + 15_000;
    assert_eq!(
        instant_495(port, r#"{service_name="checkout"}"#, at),
        vec![(
            serde_json::json!({"__name__": "checkout_requests", "service_name": "checkout"}),
            "1".to_string()
        )],
        "T6: the OTLP half"
    );
    assert_eq!(
        instant_495(port, r#"{"service.name"="checkout"}"#, at),
        vec![(
            serde_json::json!({"__name__": "checkout_requests", "service.name": "checkout"}),
            "2".to_string()
        )],
        "T6: the remote-write half"
    );
    assert_eq!(
        instant_495(port, "checkout_requests", at).len(),
        2,
        "T6: two series"
    );
    drop_db(db).await;
}

/// **T7 (issue #495): a dotted name reaches every label route.**
#[tokio::test(flavor = "multi_thread")]
async fn a_dotted_name_reaches_every_label_route() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test");
        return;
    }
    let db = &pulsus_testkit::test_db("pulsus_prom_api_495_t7_it");
    drop_db(db).await;
    let port: u16 = 31_812;
    let _guard = spawn_prom_server(
        port,
        db,
        &[("PULSUS_OTLP_TRANSLATION_STRATEGY", "NoTranslation")],
    );
    let t = anchor_495();
    push_checkout_495(port, t);
    wait_for_series_495(port, "checkout_requests", t, 2);
    let window = format!("start={}&end={}", (t - 60_000) / 1000, (t + 60_000) / 1000);
    let (_, labels) = http_get(port, &format!("/api/v1/labels?{window}")).expect("labels");
    let labels: serde_json::Value = serde_json::from_str(&labels).expect("JSON");
    assert!(
        labels["data"]
            .as_array()
            .is_some_and(|a| a.contains(&serde_json::json!("service.name"))),
        "T7 /labels: {labels}"
    );
    let (_, values) = http_get(
        port,
        &format!("/api/v1/label/U__service_2e_name/values?{window}"),
    )
    .expect("values");
    let values: serde_json::Value = serde_json::from_str(&values).expect("JSON");
    assert_eq!(values["data"], serde_json::json!(["checkout"]), "T7 values");
    let at = t + 15_000;
    assert_eq!(
        instant_495(port, r#"sum by ("service.name") (checkout_requests)"#, at),
        vec![(
            serde_json::json!({"service.name": "checkout"}),
            "2".to_string()
        )],
        "T7 sum by"
    );
    assert_eq!(
        instant_495(port, "count(checkout_requests)", at),
        vec![(serde_json::json!({}), "1".to_string())],
        "T7 count"
    );
    drop_db(db).await;
}

/// **T8 (issue #495): every name shape is found by a quoted matcher and by
/// its escaped name.**
#[tokio::test(flavor = "multi_thread")]
async fn every_name_shape_is_found_by_a_quoted_matcher_and_by_its_escaped_name() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test");
        return;
    }
    let db = &pulsus_testkit::test_db("pulsus_prom_api_495_t8_it");
    drop_db(db).await;
    let port: u16 = 31_813;
    let _guard = spawn_prom_server(port, db, &[]);
    let t = anchor_495();
    let cases = [
        ("service.name", "U__service_2e_name"),
        ("http-method", "U__http_2d_method"),
        ("k8s:pod", "U__k8s_3a_pod"),
        ("path/segment", "U__path_2f_segment"),
        ("with space", "U__with_20_space"),
        ("café", "U__caf_e9_"),
    ];
    let values: Vec<String> = (0..cases.len()).map(|i| format!("v{i}")).collect();
    type Owned<'a> = (Vec<(&'a str, &'a str)>, f64, i64);
    let series: Vec<Owned<'_>> = cases
        .iter()
        .zip(&values)
        .map(|((name, _), v)| {
            (
                vec![("__name__", "name_shapes"), (*name, v.as_str())],
                1.0,
                t,
            )
        })
        .collect();
    let refs: Vec<Series495<'_>> = series
        .iter()
        .map(|(l, v, ms)| (l.as_slice(), *v, *ms))
        .collect();
    let (status, body) = http_post_bytes(
        port,
        "/api/v1/write",
        "application/x-protobuf",
        &remote_write_495(&refs),
    );
    assert_eq!(status, 204, "{body}");
    wait_for_series_495(port, "name_shapes", t - 15_000, cases.len());
    let window = format!("start={}&end={}", (t - 60_000) / 1000, (t + 60_000) / 1000);
    for ((name, url), v) in cases.iter().zip(&values) {
        let q = format!(r#"name_shapes{{"{name}"="{v}"}}"#);
        assert_eq!(
            instant_495(port, &q, t),
            vec![(
                serde_json::json!({"__name__": "name_shapes", (*name): v}),
                "1".to_string()
            )],
            "T8 {q}"
        );
        let (_, body) =
            http_get(port, &format!("/api/v1/label/{url}/values?{window}")).expect("values");
        let json: serde_json::Value = serde_json::from_str(&body).expect("JSON");
        assert_eq!(json["data"], serde_json::json!([v]), "T8 {url}");
    }
    drop_db(db).await;
}

// ---------------------------------------------------------------------
// Issue #500: every distinct descriptor of a name
// ---------------------------------------------------------------------

/// One remote-write request: a sample of each named series, and the
/// descriptors `(name, help, unit)`, every one a gauge.
fn remote_write_descriptors(t_ms: i64, descriptors: &[(&str, &str, &str)]) -> Vec<u8> {
    use prost::Message;
    use pulsus_write::protocols::remote_write::{
        Label, MetricMetadataProto, Sample, TimeSeries, WriteRequest,
    };
    let mut names: Vec<&str> = descriptors.iter().map(|(n, ..)| *n).collect();
    names.dedup();
    let req = WriteRequest {
        timeseries: names
            .iter()
            .map(|name| TimeSeries {
                labels: vec![Label {
                    name: "__name__".to_string(),
                    value: name.to_string(),
                }],
                samples: vec![Sample {
                    value: 1.0,
                    timestamp: t_ms,
                }],
                ..Default::default()
            })
            .collect(),
        metadata: descriptors
            .iter()
            .map(|(name, help, unit)| MetricMetadataProto {
                r#type: 2,
                metric_family_name: name.to_string(),
                help: help.to_string(),
                unit: unit.to_string(),
            })
            .collect(),
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy-compress the write")
}

/// Bare HTTP/1.1 POST of a remote-write body; the status.
fn post_remote_write(port: u16, body: &[u8]) -> u16 {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .expect("timeout");
    let head = format!(
        "POST /api/v1/write HTTP/1.1\r\nHost: localhost\r\n\
         Content-Type: application/x-protobuf\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n",
        body.len()
    );
    let mut request = head.into_bytes();
    request.extend_from_slice(body);
    stream.write_all(&request).expect("send the write");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).expect("read the answer");
    String::from_utf8_lossy(&buf)
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .expect("a status line")
}

/// T5's pushes: `m` "A" and `m` "B" in two requests, then one request
/// carrying `s` "S1" and "S2", and `u` "U" without and with a unit.
fn push_t5_descriptors(port: u16) {
    let t = now_ms();
    for body in [
        remote_write_descriptors(t, &[("m", "A", "")]),
        remote_write_descriptors(t + 1_000, &[("m", "B", "")]),
        remote_write_descriptors(
            t + 2_000,
            &[
                ("s", "S1", ""),
                ("s", "S2", ""),
                ("u", "U", ""),
                ("u", "U", "seconds"),
            ],
        ),
    ] {
        let status = post_remote_write(port, &body);
        assert!(
            (200..300).contains(&status),
            "the remote write answered {status}"
        );
    }
}

/// Issue #500, T5: two requests carrying `m` "A" and "B", and one request
/// carrying two descriptors each of `s` and `u`, list every descriptor —
/// the single request goes through the parser and the writer's per-push
/// dedup, so either keeping one per name loses an entry.
#[tokio::test(flavor = "multi_thread")]
async fn metadata_lists_each_descriptor_of_a_name() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test");
        return;
    }
    let db = pulsus_testkit::test_db("pulsus_prom_500_t5");
    let port: u16 = 31_510;
    let guard = spawn_prom_server(port, &db, &[]);
    push_t5_descriptors(port);
    let entry =
        |help: &str, unit: &str| serde_json::json!({"type": "gauge", "help": help, "unit": unit});
    let want = serde_json::json!({
        "m": [entry("A", ""), entry("B", "")],
        "s": [entry("S1", ""), entry("S2", "")],
        "u": [entry("U", ""), entry("U", "seconds")],
    });
    let mut last = serde_json::Value::Null;
    let deadline = Instant::now() + Duration::from_secs(40);
    while Instant::now() < deadline {
        if let Some((200, body)) = http_get(port, "/api/v1/metadata")
            && let Ok(json) = serde_json::from_str::<serde_json::Value>(&body)
        {
            last = json["data"].clone();
            if last == want {
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    drop(guard);
    drop_db(&db).await;
    assert_eq!(last, want, "/api/v1/metadata's data");
}

/// Issue #500, T10: `limit_per_metric` is read — one entry per name — and
/// a value that is not an integer is `400` `bad_data`.
#[tokio::test(flavor = "multi_thread")]
async fn limit_per_metric_is_read_and_checked() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test");
        return;
    }
    let db = pulsus_testkit::test_db("pulsus_prom_500_t10");
    let port: u16 = 31_511;
    let guard = spawn_prom_server(port, &db, &[]);
    push_t5_descriptors(port);
    let entry =
        |help: &str, unit: &str| serde_json::json!({"type": "gauge", "help": help, "unit": unit});
    let want = serde_json::json!({
        "m": [entry("A", "")],
        "s": [entry("S1", "")],
        "u": [entry("U", "")],
    });
    // Waits until every pushed name is listed, then reads one per name.
    let _ = wait_for_body(port, "/api/v1/metadata", |json| {
        ["m", "s", "u"]
            .iter()
            .all(|name| json["data"][name].as_array().is_some())
    });
    let one = http_get(port, "/api/v1/metadata?limit_per_metric=1");
    let bad = http_get(port, "/api/v1/metadata?limit_per_metric=x");
    drop(guard);
    drop_db(&db).await;
    let (status, body) = bad.expect("/metadata?limit_per_metric=x");
    assert_eq!(status, 400, "limit_per_metric=x: {body}");
    let json: serde_json::Value = serde_json::from_str(&body).expect("json");
    assert_eq!(json["errorType"], "bad_data", "{body}");
    assert_eq!(json["error"], "limit_per_metric must be a number", "{body}");
    let (status, body) = one.expect("/metadata?limit_per_metric=1");
    assert_eq!(status, 200, "{body}");
    let json: serde_json::Value = serde_json::from_str(&body).expect("json");
    assert_eq!(json["data"], want, "limit_per_metric=1: {body}");
}

// ---------------------------------------------------------------------
// Issue #499: every query API parameter honoured, rejected or warned
// ---------------------------------------------------------------------

/// `path`'s status and parsed body.
fn get_json(port: u16, path: &str) -> (u16, serde_json::Value) {
    let (status, body) = http_get(port, path).unwrap_or_else(|| panic!("{path} unreachable"));
    let json = serde_json::from_str(&body).unwrap_or_else(|e| panic!("{path}: {e}: {body}"));
    (status, json)
}

/// The `label` values of a vector or matrix result, in the result's order.
fn label_values_of(json: &serde_json::Value, label: &str) -> Vec<String> {
    json["data"]["result"]
        .as_array()
        .unwrap_or_else(|| panic!("no result array: {json}"))
        .iter()
        .map(|r| r["metric"][label].as_str().unwrap_or("").to_string())
        .collect()
}

/// Issue #499, H2: every `lookback_delta` the reference accepts reaches
/// the engine, on both query routes. The one series' last sample is eight
/// minutes before `t`: outside the default 5m lookback, inside 10m. So
/// `10m` returns it, and `""`, `0` and `-5`, which mean the default, are
/// answered (not refused) without it.
#[tokio::test(flavor = "multi_thread")]
async fn an_accepted_lookback_delta_reaches_the_engine() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test");
        return;
    }
    let db = pulsus_testkit::test_db("pulsus_prom_499_lookback_it");
    let port: u16 = 31_521;
    let guard = spawn_prom_server(port, &db, &[]);
    let client = ChClient::new(test_ch_config(&db))
        .await
        .expect("connect to seed data");

    const MIN: i64 = 60_000;
    let t = (now_ms() / 15_000) * 15_000 - MIN;
    let ts = t / 1_000;
    client
        .insert_series(&[SeedSeriesRow {
            metric_name: "lb499".to_string(),
            fingerprint: 9921,
            unix_milli: t - 20 * MIN,
            labels: r#"{"inst":"1"}"#.to_string(),
        }])
        .await
        .expect("seed series");
    let mut samples = Vec::new();
    let mut at = t - 20 * MIN;
    while at <= t - 8 * MIN {
        samples.push(SeedSampleRow {
            org_id: String::new(),
            fingerprint: 9921,
            unix_milli: at,
            value: 1.0,
        });
        at += 15_000;
    }
    client
        .insert_block("metric_samples", &samples)
        .await
        .expect("seed samples");
    let _ = wait_for_body(port, "/api/v1/status/tsdb", |json| {
        json["data"]["headStats"]["numSeries"] == 1
    });

    for route in [
        format!("/api/v1/query?query=lb499&time={ts}"),
        format!("/api/v1/query_range?query=lb499&start={ts}&end={ts}&step=60"),
    ] {
        for (raw, want) in [
            ("10m", vec!["1"]),
            ("", vec![]),
            ("0", vec![]),
            ("-5", vec![]),
        ] {
            let path = format!("{route}&lookback_delta={raw}");
            let (status, json) = get_json(port, &path);
            assert_eq!(status, 200, "H2 {path}: {json}");
            assert_eq!(label_values_of(&json, "inst"), want, "H2 {path}: {json}");
        }
    }

    drop(guard);
    drop_db(&db).await;
}

/// Issue #499, L1-L10: `lookback_delta`, `limit`, `stats`, the discovery
/// default window, the tsdb `limit`, the exemplar validation and empty
/// values, through the real binary.
#[tokio::test(flavor = "multi_thread")]
async fn prom_api_query_parameters_issue_499() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test");
        return;
    }
    let db = pulsus_testkit::test_db("pulsus_prom_499_it");
    let port: u16 = 31_520;
    let guard = spawn_prom_server(port, &db, &[]);
    let client = ChClient::new(test_ch_config(&db))
        .await
        .expect("connect to seed data");

    const MIN: i64 = 60_000;
    let t = (now_ms() / 15_000) * 15_000 - MIN;
    let ts = t / 1_000;
    let series = |name: &str, fp: u128, labels: &str, at: i64| SeedSeriesRow {
        metric_name: name.to_string(),
        fingerprint: fp,
        unix_milli: at,
        labels: labels.to_string(),
    };
    let mut rows = vec![
        series("p499", 9901, r#"{"inst":"1","job":"a"}"#, t),
        series("p499", 9902, r#"{"inst":"2","job":"a"}"#, t),
        series("p499", 9903, r#"{"inst":"3","job":"b"}"#, t),
        series("q499", 9904, r#"{"inst":"4"}"#, t),
        series("q499", 9905, r#"{"inst":"5"}"#, t),
        series("r499", 9906, r#"{"inst":"6"}"#, t),
        series("v499", 9911, r#"{"inst":"c"}"#, t),
        series("v499", 9912, r#"{"inst":"a"}"#, t),
        series("v499", 9913, r#"{"inst":"b"}"#, t),
        series("old499", 9907, r#"{"old_key":"v"}"#, t - 3 * 86_400_000),
    ];
    // Each p499 series is also active half an hour back, where its
    // samples start.
    for fp in [9901u128, 9902, 9903] {
        let r = rows
            .iter()
            .find(|r| r.fingerprint == fp)
            .expect("seeded")
            .clone();
        rows.push(SeedSeriesRow {
            unix_milli: t - 30 * MIN,
            ..r
        });
    }
    client.insert_series(&rows).await.expect("seed series");
    let sample = |fp: u128, at: i64, value: f64| SeedSampleRow {
        org_id: String::new(),
        fingerprint: fp,
        unix_milli: at,
        value,
    };
    let mut samples = Vec::new();
    for (fp, value, until) in [
        (9901u128, 1.0, t),
        (9902, 2.0, t - 10 * MIN),
        (9903, 3.0, t),
    ] {
        let mut at = t - 30 * MIN;
        while at <= until {
            samples.push(sample(fp, at, value));
            at += 15_000;
        }
    }
    for fp in [9904u128, 9905, 9906, 9911, 9912, 9913] {
        samples.push(sample(fp, t, 1.0));
    }
    client
        .insert_block("metric_samples", &samples)
        .await
        .expect("seed samples");
    let _ = wait_for_body(port, "/api/v1/status/tsdb", |json| {
        json["data"]["headStats"]["numSeries"] == 9
    });

    let q = |path: &str| get_json(port, path);
    let at = format!("time={ts}");
    let insts = |path: &str| {
        let (status, json) = q(path);
        assert_eq!(status, 200, "{path}: {json}");
        label_values_of(&json, "inst")
    };

    // L1: the lookback is the request's.
    assert_eq!(insts(&format!("/api/v1/query?query=p499&{at}")), ["1", "3"]);
    assert_eq!(
        insts(&format!("/api/v1/query?query=p499&{at}&lookback_delta=15m")),
        ["1", "2", "3"],
        "L1: lookback_delta=15m"
    );
    for empty in ["0", ""] {
        assert_eq!(
            insts(&format!(
                "/api/v1/query?query=p499&{at}&lookback_delta={empty}"
            )),
            ["1", "3"],
            "L1: lookback_delta={empty}"
        );
    }

    // L2: the pushed and the fetched route both read the requested
    // lookback.
    for (query, needle) in [
        ("count(p499)", "%900000 AS lookback%".to_string()),
        (
            "count(p499%20*%201)",
            format!("%unix_milli > {}%", t - 900_000),
        ),
    ] {
        let (status, json) = q(&format!(
            "/api/v1/query?query={query}&{at}&lookback_delta=15m"
        ));
        assert_eq!(status, 200, "L2 {query}: {json}");
        assert_eq!(
            json["data"]["result"][0]["value"][1], "3",
            "L2 {query}: {json}"
        );
        flush_logs(&client).await;
        let sent = statements_matching(&client, &db, &format!("query LIKE '{needle}'")).await;
        assert!(sent >= 1, "L2 {query}: no statement read `{needle}`");
    }

    // L3: and on a range query.
    let range = format!("start={}&end={ts}&step=60", ts - 60);
    assert_eq!(
        insts(&format!("/api/v1/query_range?query=p499&{range}")).len(),
        2
    );
    let (status, json) = q(&format!(
        "/api/v1/query_range?query=p499&{range}&lookback_delta=15m"
    ));
    assert_eq!(status, 200, "L3: {json}");
    assert_eq!(
        label_values_of(&json, "inst"),
        ["1", "2", "3"],
        "L3: {json}"
    );
    assert_eq!(
        json["data"]["result"][1]["values"].as_array().map(Vec::len),
        Some(2),
        "L3: inst=2 has two points: {json}"
    );

    // L4: `limit` truncates as the reference does.
    let truncated = serde_json::json!(["results truncated due to limit"]);
    let (status, json) = q(&format!("/api/v1/query?query=v499&{at}&limit=2"));
    assert_eq!(status, 200, "L4: {json}");
    assert_eq!(label_values_of(&json, "inst"), ["a", "c"], "L4: {json}");
    assert_eq!(json["warnings"], truncated, "L4: {json}");
    let (_, json) = q(&format!("/api/v1/query?query=v499&{at}&limit=3"));
    assert_eq!(
        label_values_of(&json, "inst").len(),
        3,
        "L4 limit=3: {json}"
    );
    assert!(json.get("warnings").is_none(), "L4 limit=3: {json}");
    let (_, json) = q(&format!(
        "/api/v1/query?query=sort_desc(p499)&{at}&lookback_delta=15m&limit=1"
    ));
    assert_eq!(
        label_values_of(&json, "inst"),
        ["3"],
        "L4 sort_desc: {json}"
    );
    assert_eq!(
        json["data"]["result"][0]["value"][1], "3",
        "L4 sort_desc: {json}"
    );
    let (_, json) = q(&format!(
        "/api/v1/query_range?query=p499&{range}&lookback_delta=15m&limit=1"
    ));
    assert_eq!(label_values_of(&json, "inst"), ["1"], "L4 range: {json}");
    assert_eq!(json["warnings"], truncated, "L4 range: {json}");
    let (_, json) = q(&format!("/api/v1/query?query=1&{at}&limit=1"));
    assert_eq!(json["data"]["resultType"], "scalar", "L4 scalar: {json}");
    assert!(json.get("warnings").is_none(), "L4 scalar: {json}");

    // L5: `stats` adds the notice and no statistics.
    let stats_notice = serde_json::json!([
        "parameter \"stats\" is not supported; no query statistics are returned"
    ]);
    for path in [
        format!("/api/v1/query?query=p499&{at}&stats=all"),
        format!("/api/v1/query_range?query=p499&{range}&stats=all"),
    ] {
        let (status, json) = q(&path);
        assert_eq!(status, 200, "L5 {path}: {json}");
        assert_eq!(label_values_of(&json, "inst").len(), 2, "L5 {path}: {json}");
        assert_eq!(json["warnings"], stats_notice, "L5 {path}: {json}");
        assert!(json["data"].get("stats").is_none(), "L5 {path}: {json}");
    }
    for path in [
        format!("/api/v1/query?query=p499&{at}&stats="),
        format!("/api/v1/query_range?query=p499&{range}&stats="),
    ] {
        let (_, json) = q(&path);
        assert!(json.get("warnings").is_none(), "L5 {path}: {json}");
    }

    // L6: a discovery with no range reads all time.
    let (status, labels) = q("/api/v1/labels");
    assert_eq!(status, 200, "L6: {labels}");
    assert!(
        labels["data"]
            .as_array()
            .expect("names")
            .contains(&serde_json::json!("old_key")),
        "L6 /labels: {labels}"
    );
    let (status, empty_range) = q("/api/v1/labels?start=&end=");
    assert_eq!(status, 200, "L6 empty range: {empty_range}");
    assert_eq!(empty_range, labels, "L6 empty range");
    let (_, values) = q("/api/v1/label/old_key/values");
    assert_eq!(values["data"], serde_json::json!(["v"]), "L6: {values}");
    let (_, names) = q("/api/v1/label/__name__/values");
    assert!(
        names["data"]
            .as_array()
            .expect("names")
            .contains(&serde_json::json!("old499")),
        "L6: {names}"
    );
    for path in [
        "/api/v1/series?match%5B%5D=old499",
        "/api/v1/series?match%5B%5D=%7B__name__%3D~%22old4..%22%7D",
    ] {
        let (status, json) = q(path);
        assert_eq!(status, 200, "L6 {path}: {json}");
        assert_eq!(
            json["data"].as_array().map(Vec::len),
            Some(1),
            "L6 {path}: {json}"
        );
    }

    // L8: the tsdb limit.
    let counts = |json: &serde_json::Value| -> Vec<(String, u64)> {
        json["data"]["seriesCountByMetricName"]
            .as_array()
            .expect("counts")
            .iter()
            .map(|e| {
                (
                    e["name"].as_str().expect("name").to_string(),
                    e["value"].as_u64().expect("value"),
                )
            })
            .collect()
    };
    let named = |pairs: &[(&str, u64)]| -> Vec<(String, u64)> {
        pairs.iter().map(|(n, c)| (n.to_string(), *c)).collect()
    };
    let (_, json) = q("/api/v1/status/tsdb");
    assert_eq!(
        counts(&json),
        named(&[("p499", 3), ("v499", 3), ("q499", 2), ("r499", 1)]),
        "L8: {json}"
    );
    let (_, json) = q("/api/v1/status/tsdb?limit=2");
    assert_eq!(
        counts(&json),
        named(&[("p499", 3), ("v499", 3)]),
        "L8 limit=2: {json}"
    );
    for (raw, error) in [
        ("0", "limit must be a positive number"),
        ("10001", "limit must not exceed 10000"),
    ] {
        let (status, json) = q(&format!("/api/v1/status/tsdb?limit={raw}"));
        assert_eq!(status, 400, "L8 limit={raw}: {json}");
        assert_eq!(json["error"], error, "L8 limit={raw}");
    }

    // L9: the exemplar validation.
    let (status, json) = q("/api/v1/query_exemplars?query=p499");
    assert_eq!(status, 200, "L9: {json}");
    assert_eq!(json, serde_json::json!({"status": "success", "data": []}));
    for params in [
        "",
        "query=p499%7B",
        "query=p499&start=abc",
        "query=p499&start=10&end=5",
    ] {
        let (status, json) = q(&format!("/api/v1/query_exemplars?{params}"));
        assert_eq!(status, 400, "L9 {params}: {json}");
        assert_eq!(json["errorType"], "bad_data", "L9 {params}");
    }
    let (_, json) = q("/api/v1/query_exemplars?query=p499&start=10&end=5");
    assert_eq!(
        json["error"],
        "end timestamp must not be before start timestamp"
    );

    // L10: an empty `time` or `timeout` means absent.
    for path in [
        "/api/v1/query?query=p499&time=".to_string(),
        format!("/api/v1/query?query=p499&{at}&timeout="),
    ] {
        let (status, json) = q(&path);
        assert_eq!(status, 200, "L10 {path}: {json}");
    }

    drop(guard);
    drop_db(&db).await;
}
