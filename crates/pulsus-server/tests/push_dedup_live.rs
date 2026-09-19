//! Issue #494 end to end: **a push a writer has already accepted, and still
//! remembers, is not stored twice by that writer** — and at one
//! `(series, millisecond)` the metric read path gives one answer.
//!
//! The defect this removes is invisible on the panels most people watch. A
//! client that retries a push after a network timeout stored its entries or
//! samples twice, so `count_over_time`/`sum_over_time` doubled while `rate`
//! looked right: two panels over the same data disagreed and neither
//! reported an error. The only check that can see that is the answer.
//!
//! ```text
//!   L1   one log body, pushed twice, one writer      2 entries, not 4
//!   L1b  a body straddling both bucket boundaries    each bucket back to
//!                                                    its single-push value
//!   L3   the same body as two requests               1 push stored
//!   L4   the Idempotency-Key                         2 / 1 / 400
//!   L5   the same body to two writer processes       2 — the declared limit
//!   L7   Retry-Attempt with nothing to match         stored
//!   L8   what is NOT a retry                         stored
//!   M1   one remote-write body, pushed twice         3 samples, not 6
//!   M2-4 several rows at one millisecond             the §7 rule
//!   C12  PULSUS_PROMQL_MAX_SAMPLES                   still counts fetched rows
//! ```
//!
//! **`T` is pinned, and the pinning is load-bearing.** An unconstrained
//! whole-second `T` can straddle the 5-second rollup bucket and the
//! 10-second pattern bucket, which gives volume/pattern figures of `46/2`
//! where the test means `92/4`. L1 therefore takes `T mod 5s == 0`, which
//! puts both of its entries inside one rollup bucket and one pattern
//! bucket; L1b takes `T mod 10s == 9s`, which crosses **both** boundaries,
//! so only one outcome is possible there too.
//!
//! ```text
//!        T mod 10s == 4s      5s rows split   pattern rows DO NOT split  <- ambiguous
//!        T mod 10s == 9s      5s rows split   pattern rows split         <- L1b
//! ```
//!
//! ```text
//!   PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=18123 \
//!     PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!     cargo test -p pulsus-server --test push_dedup_live
//! ```
//!
//! **Run the workspace suite WITHOUT the live gate, and this suite with
//! it.** Exporting `PULSUS_TEST_CLICKHOUSE` for a `cargo nextest run
//! --workspace` also wakes the multi-node suites — `live_spreading`,
//! `live_cluster`, `live_metrics_cluster_fallback`,
//! `live_metrics_grouped_cluster` — whose shard variables a single-node
//! server does not set. They fail to connect and take the run down:
//! measured at `13 failed` where the same tree gives `7480 passed` with
//! the gate absent. `.github/workflows/ci.yml` encodes the same split, and
//! `--test-threads=1` here because one test restarts a server on its own
//! port with different settings.
//!
//! **And the working directory must be writable.** The writer's spool root
//! is `./spool`, relative to the process working directory, so the
//! committed writer tests create a directory: in a read-only tree the
//! workspace command printed `7200 passed, 1 failed` where a writable copy
//! of the same source printed `7486 passed`.
//!
//! **A shared build directory can carry another clone's paths.** Two
//! clones pointed at one `CARGO_TARGET_DIR` reuse each other's compiled
//! artefacts, and a test that embeds a path at compile time then reads
//! the OTHER clone's. Round 3 of issue #494's code review hit it: a first
//! run in a clean workspace printed `217 passed, 2 failed`, both
//! `chart_surface`, both `repo root: No such file or directory`, because
//! a scratch clone that had since been removed shared the warm tree and
//! its manifest directory was baked in. `cargo clean -p pulsus-config`
//! cleared it and the rerun printed `7500 passed, 39 skipped`. A failure
//! naming a path that is not in your tree is this, not a defect in the
//! code under test.
//!
//! **The documentation-sites self-test writes to the git object store.**
//! `ci/checks/doc_sites_self_test.sh` builds throwaway commits and
//! temporary refs for its commit-graph tests, so a checkout whose object
//! store is read-only makes it refuse, saying so, rather than fail
//! midway with a `git` error. It also needs the base branch commit
//! passed in — `PULSUSDB_DOC_SITES_UPSTREAM` — because the check it
//! exercises refuses to guess one.

#[path = "support/live_db.rs"]
mod live_db;

use live_db::ScopedDb;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use futures::StreamExt;
use prost::Message;
use pulsus_clickhouse::{ChClient, QuerySettings, Row};
use pulsus_model::Fingerprint;
use pulsus_write::protocols::loki_push::{EntryAdapter, PushRequest, StreamAdapter, Timestamp};
use pulsus_write::protocols::remote_write::{
    Label, MetricMetadataProto, Sample, TimeSeries, WriteRequest,
};
use pulsus_write::writer::{MetricHistSampleRow, MetricSampleRow, MetricSeriesRow};

/// One fixed listener port per test: the suites in this crate run as
/// separate processes, so each test needs its own. Integer literals,
/// because `live_port_uniqueness.rs` reads them and a port built by
/// arithmetic is invisible to it.
const LOGS_PORT: u16 = 31_630;
/// The two-writer test binds TWO, and neither may be another test's: the
/// suites here run in parallel, so a port shared between two tests in one
/// file collides exactly as one shared between two files would. The first
/// version of this suite gave this test `LOGS_PORT`, and the occupied-port
/// guard below is what found it.
const TWO_WRITERS_FIRST_PORT: u16 = 31_637;
const TWO_WRITERS_SECOND_PORT: u16 = 31_631;
const BUCKETS_PORT: u16 = 31_632;
const METRICS_PORT: u16 = 31_633;
const COLLAPSE_PORT: u16 = 31_634;
const BUDGET_PORT: u16 = 31_635;
const MECHANISM_OFF_PORT: u16 = 31_636;
const DESCRIPTOR_RACE_PORT: u16 = 31_638;

/// `2026-01-15T00:00:00Z` in nanoseconds — `T mod 5s == 0` and
/// `T mod 10s == 0`, so L1's two entries share one rollup bucket and one
/// pattern bucket.
const T_NS: i64 = 1_768_435_200_000_000_000;

/// `T_NS` moved so that `T mod 10s == 9s`: the 5-second rollup rows split
/// AND the 10-second pattern rows split, so L1b has one possible outcome.
const T_SPLIT_NS: i64 = T_NS + 9_000_000_000;

/// A hundred years of retention: the pinned instant is in the past and the
/// default seven-day TTL would delete it before the first query.
const RETENTION_DAYS: &str = "36500";

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

// ---------------------------------------------------------------------
// HTTP, the same shape the other live suites use
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

/// One request, with an arbitrary set of extra headers — which is what
/// this suite needs that the other live suites do not: `Idempotency-Key`
/// and `Retry-Attempt` are the whole subject.
fn http_request_with(
    port: u16,
    method: &str,
    path: &str,
    content_type: Option<&str>,
    extra: &[(&str, &str)],
    body: &[u8],
) -> Option<HttpResponse> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(60))).ok();

    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    if let Some(ct) = content_type {
        head.push_str(&format!("Content-Type: {ct}\r\n"));
    }
    for (name, value) in extra {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    if method != "GET" {
        head.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    head.push_str("\r\n");

    let mut request = head.into_bytes();
    request.extend_from_slice(body);
    stream.write_all(&request).ok()?;

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).ok()?;
    let split_at = find_subslice(&buf, b"\r\n\r\n")?;
    let head_text = String::from_utf8_lossy(&buf[..split_at]).into_owned();
    let raw_body = &buf[split_at + 4..];

    let mut lines = head_text.lines();
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
    let dechunked = if headers
        .get("transfer-encoding")
        .is_some_and(|v| v == "chunked")
    {
        dechunk(raw_body)
    } else {
        raw_body.to_vec()
    };
    Some(HttpResponse {
        status,
        body: String::from_utf8_lossy(&dechunked).into_owned(),
    })
}

fn http_get(port: u16, path: &str) -> HttpResponse {
    http_request_with(port, "GET", path, None, &[], &[]).expect("request reachable")
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

/// Refuses to start when `port` is already in use, and says so.
///
/// Issue #494's code review round 1 measured what happens without this: a
/// cancelled run left a server of its own listening on this suite's fixed
/// port, the next run's `/ready` probe was answered by THAT server — which
/// was pointed at a different database — and the suite reported a count
/// mismatch. A wrong count reads as a defect in the branch under review;
/// an occupied port is a fact about the machine, and the two must not look
/// the same.
///
/// The bind is released immediately, so there is a window between this
/// check and the child's own bind. That window is not what this closes:
/// what it closes is a port occupied for the whole run by something that
/// was never this test's child.
fn assert_port_free(port: u16) {
    match std::net::TcpListener::bind(("127.0.0.1", port)) {
        Ok(listener) => drop(listener),
        Err(e) => panic!(
            "port {port} is already in use ({e}). A previous run's server is \
             probably still listening — this suite binds fixed ports, so it \
             cannot start beside one. Find it with `ss -ltnp` and stop that \
             process before re-running; do not kill by program name, other \
             work on this machine shares it."
        ),
    }
}

fn spawn_ready(port: u16, db: &ScopedDb, extra_env: &[(&str, &str)]) -> ChildGuard {
    assert_port_free(port);
    let mut command = Command::new(env!("CARGO_BIN_EXE_pulsusdb"));
    command
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        // The label cache's default TTL is 60s; a shorter one keeps the
        // metric legs inside their own deadline.
        .env("PULSUS_CACHE_TTL", "1s")
        .env("PULSUS_COMPAT_ENDPOINTS", "true")
        .env("PULSUS_RETENTION_DAYS", RETENTION_DAYS)
        .env("CLICKHOUSE_SERVER", live_db::ch_host())
        .env("CLICKHOUSE_HTTP_PORT", live_db::ch_http_port().to_string())
        .env("CLICKHOUSE_DB", db.name());
    for (k, v) in extra_env {
        command.env(k, v);
    }
    let mut guard = ChildGuard(command.spawn().expect("spawn pulsusdb"));
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        // A child that exited is reported now, rather than after the full
        // sixty seconds of probing a port nobody is listening on.
        if let Ok(Some(status)) = guard.0.try_wait() {
            panic!("pulsusdb exited before becoming ready ({status}) on port {port}");
        }
        if http_request_with(port, "GET", "/ready", None, &[], &[]).is_some_and(|r| r.status == 200)
        {
            return guard;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "/ready never reached 200 within 60s (port {port}, db {})",
        db.name()
    );
}

// ---------------------------------------------------------------------
// Bodies
// ---------------------------------------------------------------------

/// **L1's body**: one stream, two entries a second apart, snappy-framed
/// exactly as a log shipper sends it.
fn logs_body(service: &str, base_ns: i64) -> Vec<u8> {
    let entry = |ts_ns: i64, line: &str| EntryAdapter {
        timestamp: Some(Timestamp {
            seconds: ts_ns / 1_000_000_000,
            nanos: (ts_ns % 1_000_000_000) as i32,
        }),
        line: line.to_string(),
        structured_metadata: Vec::new(),
    };
    let req = PushRequest {
        streams: vec![StreamAdapter {
            labels: format!(r#"{{service_name="{service}", job="a"}}"#),
            entries: vec![
                entry(base_ns, "checkout failed order=1"),
                entry(base_ns + 1_000_000_000, "checkout failed order=2"),
            ],
        }],
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy-compress the push")
}

/// **M1's body**: one series, three samples fifteen seconds apart.
fn remote_write_body(job: &str, t1_ms: i64) -> Vec<u8> {
    let req = WriteRequest {
        timeseries: vec![TimeSeries {
            labels: vec![
                Label {
                    name: "__name__".to_string(),
                    value: "http_requests_total".to_string(),
                },
                Label {
                    name: "instance".to_string(),
                    value: "i1".to_string(),
                },
                Label {
                    name: "job".to_string(),
                    value: job.to_string(),
                },
            ],
            samples: vec![
                Sample {
                    value: 10.0,
                    timestamp: t1_ms,
                },
                Sample {
                    value: 20.0,
                    timestamp: t1_ms + 15_000,
                },
                Sample {
                    value: 30.0,
                    timestamp: t1_ms + 30_000,
                },
            ],
            ..Default::default()
        }],
        ..Default::default()
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy-compress the write")
}

/// A remote write that carries a metric DESCRIPTOR as well as samples.
///
/// The descriptor is what makes a suppressed push still do work: the rows
/// it would have stored are dropped, and the descriptor is still offered
/// to the cache gate.
fn remote_write_with_descriptor(metric: &str, job: &str, t1_ms: i64) -> Vec<u8> {
    let req = WriteRequest {
        timeseries: vec![TimeSeries {
            labels: vec![
                Label {
                    name: "__name__".to_string(),
                    value: metric.to_string(),
                },
                Label {
                    name: "job".to_string(),
                    value: job.to_string(),
                },
            ],
            samples: vec![
                Sample {
                    value: 1.0,
                    timestamp: t1_ms,
                },
                Sample {
                    value: 2.0,
                    timestamp: t1_ms + 15_000,
                },
                Sample {
                    value: 3.0,
                    timestamp: t1_ms + 30_000,
                },
            ],
            ..Default::default()
        }],
        metadata: vec![MetricMetadataProto {
            r#type: 1,
            metric_family_name: metric.to_string(),
            help: "requests served".to_string(),
            unit: String::new(),
        }],
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy-compress the write")
}

fn push_logs(port: u16, body: &[u8], extra: &[(&str, &str)]) -> HttpResponse {
    http_request_with(
        port,
        "POST",
        "/loki/api/v1/push",
        Some("application/x-protobuf"),
        extra,
        body,
    )
    .expect("log push reachable")
}

fn push_metrics(port: u16, body: &[u8], extra: &[(&str, &str)]) -> HttpResponse {
    http_request_with(
        port,
        "POST",
        "/api/v1/write",
        Some("application/x-protobuf"),
        extra,
        body,
    )
    .expect("remote write reachable")
}

// ---------------------------------------------------------------------
// Reading back
// ---------------------------------------------------------------------

#[derive(Debug, Row, serde::Serialize, serde::Deserialize)]
struct CountRow {
    n: u64,
}

#[derive(Debug, Row, serde::Serialize, serde::Deserialize)]
struct BucketRow {
    bucket: i64,
    count: u64,
    bytes: u64,
}

async fn client_for(db: &str) -> ChClient {
    ChClient::new(live_db::conn_config(db))
        .await
        .expect("connect to the live ClickHouse")
}

async fn collect<R: pulsus_clickhouse::ChRow>(client: &ChClient, sql: &str) -> Vec<R> {
    let mut stream = client
        .query_stream::<R>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("query {sql}: {e}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.unwrap_or_else(|e| panic!("decode a row of {sql}: {e}")));
    }
    out
}

async fn scalar(client: &ChClient, sql: &str) -> u64 {
    collect::<CountRow>(client, sql)
        .await
        .first()
        .map(|r| r.n)
        .unwrap_or(0)
}

/// Polls `sql` until it answers `want`, or fails naming both figures. The
/// writer flushes on `PULSUS_BATCH_MS` (200 ms by default), so a row is
/// not readable the instant its `204` returns.
async fn wait_for_scalar(client: &ChClient, sql: &str, want: u64, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = u64::MAX;
    while Instant::now() < deadline {
        last = scalar(client, sql).await;
        if last == want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("{what}: expected {want}, the database holds {last}\n  {sql}");
}

/// The JSON value of a single-series instant query, as the string the API
/// renders. Polls until the series resolves: a newly registered metric is
/// stored before it is query-resolvable, and the gap is a whole cache
/// refresh interval.
fn instant_value(port: u16, query: &str, at_ms: i64) -> String {
    let path = format!(
        "/api/v1/query?query={}&time={}",
        urlencode(query),
        at_ms as f64 / 1000.0
    );
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut last = String::new();
    while Instant::now() < deadline {
        let response = http_get(port, &path);
        last = response.body.clone();
        if response.status == 200
            && let Some(v) = single_value(&response.body)
        {
            return v;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    panic!("no series resolved for {query} within 90s; last body: {last}");
}

/// The `value[1]` of a result carrying exactly one series, or `None` while
/// the result is still empty.
fn single_value(body: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    let result = v.get("data")?.get("result")?.as_array()?;
    if result.len() != 1 {
        return None;
    }
    let value = result[0].get("value")?.as_array()?;
    Some(value.get(1)?.as_str()?.to_string())
}

/// How many series a query returned. Distinguishes "absent" from "NaN",
/// which is what M2's post-change assertion turns on.
fn series_count(port: u16, query: &str, at_ms: i64) -> usize {
    let path = format!(
        "/api/v1/query?query={}&time={}",
        urlencode(query),
        at_ms as f64 / 1000.0
    );
    let response = http_get(port, &path);
    assert_eq!(response.status, 200, "{query}: {}", response.body);
    let v: serde_json::Value = serde_json::from_str(&response.body).expect("JSON body");
    v["data"]["result"].as_array().map(Vec::len).unwrap_or(0)
}

// ---------------------------------------------------------------------
// L1, L3, L5, L7, L8 — the retried log push
// ---------------------------------------------------------------------

/// **L1 and L3.** The same body pushed twice through one writer stores one
/// push's entries, and every reader agrees: the raw range, the rollup the
/// counting queries read, the volume endpoint, and the pattern counts.
///
/// Before this change each of those doubled. `rate` did not, which is what
/// made the defect hard to see.
#[tokio::test]
async fn a_retried_log_push_stores_one_copy_on_every_reader() {
    if !should_run() {
        eprintln!("skipping: PULSUS_TEST_CLICKHOUSE is not set");
        return;
    }
    let db = ScopedDb::fresh(pulsus_testkit::test_db("a494_logs")).await;
    let _server = spawn_ready(LOGS_PORT, &db, &[]);
    let client = client_for(db.name()).await;

    let body = logs_body("dupB", T_NS);
    for attempt in 0..2 {
        let response = push_logs(LOGS_PORT, &body, &[]);
        assert_eq!(
            response.status, 204,
            "push {attempt} answered {}: {}",
            response.status, response.body
        );
    }

    wait_for_scalar(
        &client,
        "SELECT count() AS n FROM log_samples WHERE service = 'dupB'",
        2,
        "L1: the retried push stores one copy",
    )
    .await;

    // The rollup the counting queries read, and the pattern counts, are
    // fed by materialized views over `log_samples`: a duplicate INSERT
    // fires the view and nothing later subtracts from the sum, which is
    // why suppression has to happen BEFORE the insert.
    let rollup: Vec<BucketRow> = collect(
        &client,
        "SELECT toInt64(0) AS bucket, sum(count) AS count, sum(bytes) AS bytes \
         FROM log_metrics_5s \
         WHERE fingerprint IN (SELECT fingerprint FROM log_streams WHERE service = 'dupB')",
    )
    .await;
    assert_eq!(
        (rollup[0].count, rollup[0].bytes),
        (2, 46),
        "L1: the rollup must hold one push's two entries, 23 bytes each"
    );

    wait_for_scalar(
        &client,
        "SELECT sum(count) AS n FROM log_patterns \
         WHERE fingerprint IN (SELECT fingerprint FROM log_streams WHERE service = 'dupB')",
        2,
        "L1: the pattern counts must hold one push's two entries",
    )
    .await;

    let start_ns = T_NS - 3_600_000_000_000;
    let end_ns = T_NS + 3_600_000_000_000;
    let range = http_get(
        LOGS_PORT,
        &format!(
            "/loki/api/v1/query_range?query={}&start={start_ns}&end={end_ns}&limit=100&direction=forward",
            urlencode(r#"{service_name="dupB"}"#)
        ),
    );
    assert_eq!(range.status, 200, "{}", range.body);
    let parsed: serde_json::Value = serde_json::from_str(&range.body).expect("JSON body");
    let values = parsed["data"]["result"][0]["values"]
        .as_array()
        .map(Vec::len)
        .unwrap_or(0);
    assert_eq!(values, 2, "L1: the raw range returns two entries, not four");

    let volume = http_get(
        LOGS_PORT,
        &format!(
            "/loki/api/v1/index/volume?query={}&start={start_ns}&end={end_ns}",
            urlencode(r#"{service_name="dupB"}"#)
        ),
    );
    assert_eq!(volume.status, 200, "{}", volume.body);
    assert_eq!(
        single_value(&volume.body).as_deref(),
        Some("46"),
        "L1: the volume endpoint reports one push's bytes: {}",
        volume.body
    );

    // ---- L8: what is NOT a retry is stored -------------------------
    // One nanosecond later is a different push; one changed label value
    // is a different push. Asserted by row counts, never by status.
    let one_ns_later = logs_body("dupB", T_NS + 1);
    assert_eq!(push_logs(LOGS_PORT, &one_ns_later, &[]).status, 204);
    let relabelled = logs_body("dupB2", T_NS);
    assert_eq!(push_logs(LOGS_PORT, &relabelled, &[]).status, 204);
    wait_for_scalar(
        &client,
        "SELECT count() AS n FROM log_samples WHERE service = 'dupB'",
        4,
        "L8: one nanosecond apart is not a retry",
    )
    .await;
    wait_for_scalar(
        &client,
        "SELECT count() AS n FROM log_samples WHERE service = 'dupB2'",
        2,
        "L8: a changed stream label is not a retry",
    )
    .await;

    // ---- L7: a lone Retry-Attempt with nothing to match -------------
    let fresh = logs_body("dupRetry", T_NS);
    assert_eq!(
        push_logs(LOGS_PORT, &fresh, &[("Retry-Attempt", "1")]).status,
        204
    );
    wait_for_scalar(
        &client,
        "SELECT count() AS n FROM log_samples WHERE service = 'dupRetry'",
        2,
        "L7: the header is never a suppression key",
    )
    .await;
}

/// **L5, the declared limit.** The same body to a second writer process is
/// stored again: the index is per writer, and nothing in this change
/// crosses processes.
#[tokio::test]
async fn the_same_body_to_a_second_writer_is_stored_again() {
    if !should_run() {
        eprintln!("skipping: PULSUS_TEST_CLICKHOUSE is not set");
        return;
    }
    let db = ScopedDb::fresh(pulsus_testkit::test_db("a494_two_writers")).await;
    let first = spawn_ready(TWO_WRITERS_FIRST_PORT, &db, &[]);
    let client = client_for(db.name()).await;
    let body = logs_body("dupCross", T_NS);

    assert_eq!(push_logs(TWO_WRITERS_FIRST_PORT, &body, &[]).status, 204);
    wait_for_scalar(
        &client,
        "SELECT count() AS n FROM log_samples WHERE service = 'dupCross'",
        2,
        "the first writer stores the push",
    )
    .await;

    // A second process against the same database — the deployment shape
    // the chart ships in split mode (`replicaCount: 2`).
    let second = spawn_ready(TWO_WRITERS_SECOND_PORT, &db, &[]);
    assert_eq!(push_logs(TWO_WRITERS_SECOND_PORT, &body, &[]).status, 204);
    wait_for_scalar(
        &client,
        "SELECT count() AS n FROM log_samples WHERE service = 'dupCross'",
        4,
        "the cross-writer case is the declared limit: both are stored",
    )
    .await;
    drop(second);
    drop(first);
}

/// **L1b, the boundary.** A body whose two entries straddle the 5-second
/// rollup boundary AND the 10-second pattern boundary: each bucket must
/// come back to its single-push value, and the aggregate is never
/// asserted — every bucket is.
///
/// **L4, the key**, on the same server: two different `Idempotency-Key`
/// values store two pushes, the same key with the same content stores one,
/// and the same key with different content is a client error that stores
/// nothing.
#[tokio::test]
async fn the_rollup_buckets_return_to_their_single_push_values() {
    if !should_run() {
        eprintln!("skipping: PULSUS_TEST_CLICKHOUSE is not set");
        return;
    }
    let db = ScopedDb::fresh(pulsus_testkit::test_db("a494_buckets")).await;
    let _server = spawn_ready(BUCKETS_PORT, &db, &[]);
    let client = client_for(db.name()).await;

    // One push first, to record what a single push looks like.
    let single = logs_body("dupSingle", T_SPLIT_NS);
    assert_eq!(push_logs(BUCKETS_PORT, &single, &[]).status, 204);
    wait_for_scalar(
        &client,
        "SELECT count() AS n FROM log_samples WHERE service = 'dupSingle'",
        2,
        "the single push lands",
    )
    .await;
    // `log_metrics_5s` is keyed on `(fingerprint, bucket_ns)` and carries
    // no service column, so the stream table resolves the service.
    let buckets = |service: &str| {
        format!(
            "SELECT toInt64(bucket_ns) AS bucket, sum(count) AS count, sum(bytes) AS bytes \
             FROM log_metrics_5s \
             WHERE fingerprint IN (SELECT fingerprint FROM log_streams WHERE service = '{service}') \
             GROUP BY bucket ORDER BY bucket"
        )
    };
    let single_rows: Vec<BucketRow> = collect(&client, &buckets("dupSingle")).await;
    assert_eq!(
        single_rows
            .iter()
            .map(|r| (r.count, r.bytes))
            .collect::<Vec<_>>(),
        vec![(1, 23), (1, 23)],
        "L1b: the entries straddle the 5-second boundary, one each side"
    );

    // Now the duplicated push, on its own stream.
    let duplicated = logs_body("dupSplit", T_SPLIT_NS);
    for _ in 0..2 {
        assert_eq!(push_logs(BUCKETS_PORT, &duplicated, &[]).status, 204);
    }
    wait_for_scalar(
        &client,
        "SELECT count() AS n FROM log_samples WHERE service = 'dupSplit'",
        2,
        "L1b: the retry stores nothing",
    )
    .await;
    let split_rows: Vec<BucketRow> = collect(&client, &buckets("dupSplit")).await;
    assert_eq!(
        split_rows
            .iter()
            .map(|r| (r.count, r.bytes))
            .collect::<Vec<_>>(),
        vec![(1, 23), (1, 23)],
        "L1b: each bucket returns to its single-push value, not (2, 46)"
    );

    let pattern_buckets: Vec<BucketRow> = collect(
        &client,
        "SELECT toInt64(bucket_ns) AS bucket, sum(count) AS count, toUInt64(0) AS bytes \
         FROM log_patterns \
         WHERE fingerprint IN (SELECT fingerprint FROM log_streams WHERE service = 'dupSplit') \
         GROUP BY bucket ORDER BY bucket",
    )
    .await;
    assert_eq!(
        pattern_buckets.iter().map(|r| r.count).collect::<Vec<_>>(),
        vec![1, 1],
        "L1b: the 10-second pattern buckets split too, one entry each"
    );

    // ---- L4: the key ------------------------------------------------
    let keyed = logs_body("dupKey", T_NS);
    for key in ["k-one", "k-two"] {
        assert_eq!(
            push_logs(BUCKETS_PORT, &keyed, &[("Idempotency-Key", key)]).status,
            204
        );
    }
    // The same key, the same content: one push.
    assert_eq!(
        push_logs(BUCKETS_PORT, &keyed, &[("Idempotency-Key", "k-one")]).status,
        204
    );
    wait_for_scalar(
        &client,
        "SELECT count() AS n FROM log_samples WHERE service = 'dupKey'",
        4,
        "L4: two distinct keys are two pushes; the repeat under one of them is not a third",
    )
    .await;

    // The same key, different content: a client error, and nothing stored.
    let different = logs_body("dupKeyOther", T_NS);
    let refused = push_logs(BUCKETS_PORT, &different, &[("Idempotency-Key", "k-one")]);
    assert_eq!(
        refused.status, 400,
        "L4: a reused key carrying different content is a client error"
    );
    assert!(
        refused.body.contains("Idempotency-Key"),
        "L4: the body names the header: {:?}",
        refused.body
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        scalar(
            &client,
            "SELECT count() AS n FROM log_samples WHERE service = 'dupKeyOther'"
        )
        .await,
        0,
        "L4: the refused push stores nothing"
    );
}

/// **The control.** With `PULSUS_INGEST_DEDUP=false` the same two pushes
/// store both copies and every reader doubles — which is the defect, and
/// is what the tests above would report if they were passing for some
/// other reason (a body that never arrived, a query that matched nothing).
///
/// The figures are the pre-change answers, stated so the difference is on
/// the page rather than inferred — and the metric row is where the two
/// halves of this change come apart:
///
/// ```text
///                                   mechanism on   mechanism off
///   log_samples rows                           2               4
///   log_metrics_5s count/bytes            2 / 46          4 / 92
///   log_patterns count                         2               4
///   metric_samples rows                        3               6
///   count_over_time over those samples         3               3
/// ```
///
/// **The last row is not a mistake.** Suppression at ingest and the §7
/// read rule are independent, and the metric read path collapses two rows
/// identical in `(fingerprint, unix_milli, value bits)` whether or not the
/// push that stored them was suppressed. So with the mechanism off the
/// metric side costs storage rather than a wrong answer.
///
/// The log side has no such second line of defence, which is the whole of
/// the engine ruling: `log_metrics_5s` is an `AggregatingMergeTree` fed by
/// a materialized view, the duplicate INSERT fires the view, and nothing
/// afterwards subtracts from the sum. That is why the suppression has to
/// happen before the insert rather than at read time or at merge time.
#[tokio::test]
async fn with_the_mechanism_off_the_retry_doubles_every_reader() {
    if !should_run() {
        eprintln!("skipping: PULSUS_TEST_CLICKHOUSE is not set");
        return;
    }
    let db = ScopedDb::fresh(pulsus_testkit::test_db("a494_off")).await;
    let _server = spawn_ready(MECHANISM_OFF_PORT, &db, &[("PULSUS_INGEST_DEDUP", "false")]);
    let client = client_for(db.name()).await;

    let body = logs_body("dupOff", T_NS);
    for _ in 0..2 {
        assert_eq!(push_logs(MECHANISM_OFF_PORT, &body, &[]).status, 204);
    }
    wait_for_scalar(
        &client,
        "SELECT count() AS n FROM log_samples WHERE service = 'dupOff'",
        4,
        "off: the retry stores a second copy",
    )
    .await;
    let rollup: Vec<BucketRow> = collect(
        &client,
        "SELECT toInt64(0) AS bucket, sum(count) AS count, sum(bytes) AS bytes \
         FROM log_metrics_5s \
         WHERE fingerprint IN (SELECT fingerprint FROM log_streams WHERE service = 'dupOff')",
    )
    .await;
    assert_eq!(
        (rollup[0].count, rollup[0].bytes),
        (4, 92),
        "off: the rollup doubles, and no later merge subtracts from it"
    );
    wait_for_scalar(
        &client,
        "SELECT sum(count) AS n FROM log_patterns \
         WHERE fingerprint IN (SELECT fingerprint FROM log_streams WHERE service = 'dupOff')",
        4,
        "off: the pattern counts double too",
    )
    .await;

    let t1_ms = T_NS / 1_000_000;
    let retried = remote_write_body("dupOffC", t1_ms);
    for _ in 0..2 {
        assert_eq!(push_metrics(MECHANISM_OFF_PORT, &retried, &[]).status, 204);
    }
    wait_for_scalar(
        &client,
        "SELECT count() AS n FROM metric_samples WHERE metric_name = 'http_requests_total'",
        6,
        "off: six sample rows for three samples",
    )
    .await;
    assert_eq!(
        instant_value(
            MECHANISM_OFF_PORT,
            r#"count_over_time(http_requests_total{job="dupOffC"}[5m])"#,
            t1_ms + 40_000
        ),
        "3",
        "off: six rows are stored, and the §7 read rule still answers three — \
         the two halves of this change are independent"
    );
}

// ---------------------------------------------------------------------
// M3 — the concurrent descriptor race, read back from the table
// ---------------------------------------------------------------------

/// **M3.** Four content-identical, descriptor-bearing writes sent at once
/// each enqueue their descriptor, and one row is visible.
///
/// **Why both halves are asserted somewhere.** Suppression drops the
/// sample, series and histogram rows of the three it suppresses, and
/// still offers each descriptor to the cache gate; the gate emits unless
/// the descriptor equals the one last CONFIRMED-flushed, and under a
/// barrier none of the four has confirmed anything, so all four emit.
/// `concurrent_identical_descriptor_bearing_pushes_store_one_copy` in
/// `crates/pulsus-write/tests/a494_push_dedup.rs` asserts that count is
/// exactly four. What a reader sees is one row, because `metric_metadata`
/// is a `ReplacingMergeTree(updated_ns)` ordered by `metric_name` alone —
/// the receiver-injected `updated_ns` is excluded from the key, so the
/// four collapse. This test reads that back from a live table, which the
/// writer-level one cannot.
///
/// Round 4 of this issue's code review found the pair established visible
/// retry correctness but neither of these two figures; the notes claimed
/// them anyway. They are assertions now.
#[tokio::test]
async fn a_concurrent_descriptor_race_leaves_one_visible_row() {
    if !should_run() {
        eprintln!("skipping: PULSUS_TEST_CLICKHOUSE is not set");
        return;
    }
    assert_port_free(DESCRIPTOR_RACE_PORT);
    let db = ScopedDb::fresh(pulsus_testkit::test_db("a494_descriptor_race")).await;
    let _server = spawn_ready(DESCRIPTOR_RACE_PORT, &db, &[]);
    let client = client_for(db.name()).await;

    let t1_ms = T_NS / 1_000_000;
    let body = remote_write_with_descriptor("dedup_descriptor_race_total", "dupDesc", t1_ms);

    // Four at once, so none of them has confirmed a flush when the others
    // reach the cache gate. Threads rather than tasks: `push_metrics`
    // blocks on a socket.
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));
    let mut handles = Vec::new();
    for _ in 0..4 {
        let body = body.clone();
        let barrier = std::sync::Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            push_metrics(DESCRIPTOR_RACE_PORT, &body, &[]).status
        }));
    }
    for h in handles {
        assert_eq!(
            h.join().expect("push thread"),
            204,
            "every push is accepted"
        );
    }

    wait_for_scalar(
        &client,
        "SELECT count() AS n FROM metric_samples \
         WHERE metric_name = 'dedup_descriptor_race_total'",
        3,
        "M3: one push's three samples, not four pushes' twelve",
    )
    .await;

    wait_for_scalar(
        &client,
        "SELECT count() AS n FROM metric_metadata FINAL \
         WHERE metric_name = 'dedup_descriptor_race_total'",
        1,
        "M3: one descriptor row is visible however many were enqueued",
    )
    .await;
}

// ---------------------------------------------------------------------
// M1 — the retried remote write
// ---------------------------------------------------------------------

/// **M1.** One remote-write body sent twice stores three samples, not six,
/// and the counting queries answer accordingly. A once-only control under
/// a second job name pins that the answers are the retry's doing.
#[tokio::test]
async fn a_retried_remote_write_stores_one_copy() {
    if !should_run() {
        eprintln!("skipping: PULSUS_TEST_CLICKHOUSE is not set");
        return;
    }
    let db = ScopedDb::fresh(pulsus_testkit::test_db("a494_metrics")).await;
    let _server = spawn_ready(METRICS_PORT, &db, &[]);
    let client = client_for(db.name()).await;

    let t1_ms = T_NS / 1_000_000;
    let retried = remote_write_body("dupC", t1_ms);
    for _ in 0..2 {
        assert_eq!(push_metrics(METRICS_PORT, &retried, &[]).status, 204);
    }
    let control = remote_write_body("dupCLEAN", t1_ms);
    assert_eq!(push_metrics(METRICS_PORT, &control, &[]).status, 204);

    wait_for_scalar(
        &client,
        "SELECT count() AS n FROM metric_samples WHERE metric_name = 'http_requests_total'",
        6,
        "M1: three samples per job, two jobs",
    )
    .await;

    let at_ms = t1_ms + 40_000;
    for (query, want) in [
        (
            r#"count_over_time(http_requests_total{job="dupC"}[5m])"#,
            "3",
        ),
        (
            r#"sum_over_time(http_requests_total{job="dupC"}[5m])"#,
            "60",
        ),
        (
            r#"rate(http_requests_total{job="dupC"}[5m])"#,
            "0.10555555555555556",
        ),
        (
            r#"increase(http_requests_total{job="dupC"}[5m])"#,
            "31.666666666666664",
        ),
        (
            r#"last_over_time(http_requests_total{job="dupC"}[5m])"#,
            "30",
        ),
        (
            r#"avg_over_time(http_requests_total{job="dupC"}[5m])"#,
            "20",
        ),
    ] {
        assert_eq!(
            instant_value(METRICS_PORT, query, at_ms),
            want,
            "M1: {query}"
        );
    }

    // The once-only control answers the same, which is the point: the
    // retried series is now indistinguishable from a series pushed once.
    for (query, want) in [
        (
            r#"count_over_time(http_requests_total{job="dupCLEAN"}[5m])"#,
            "3",
        ),
        (
            r#"sum_over_time(http_requests_total{job="dupCLEAN"}[5m])"#,
            "60",
        ),
        (
            r#"rate(http_requests_total{job="dupCLEAN"}[5m])"#,
            "0.10555555555555556",
        ),
        (
            r#"increase(http_requests_total{job="dupCLEAN"}[5m])"#,
            "31.666666666666664",
        ),
    ] {
        assert_eq!(
            instant_value(METRICS_PORT, query, at_ms),
            want,
            "M1 control: {query}"
        );
    }
}

// ---------------------------------------------------------------------
// M2, M3, M4 — one answer per (series, millisecond)
// ---------------------------------------------------------------------

/// Seeds rows straight into the sample tables, which is the only way to
/// place several rows at ONE millisecond: the ingest path suppresses a
/// repeated push, and a single push carrying three samples at one
/// millisecond is not the state M2 is about (that one is a cross-writer
/// or pre-#494 store).
async fn seed_series(client: &ChClient, name: &str, fp: u128, at_ms: i64, job: &str) {
    let series = vec![MetricSeriesRow {
        metric_name: name.to_string(),
        fingerprint: Fingerprint::from_raw(fp),
        unix_milli: at_ms - (at_ms % 3_600_000),
        labels: format!(r#"{{"job":"{job}"}}"#),
        value_type: 0,
    }];
    client
        .insert_block("metric_series", &series)
        .await
        .expect("seed metric_series");
}

async fn seed_floats(client: &ChClient, name: &str, fp: u128, rows: &[(i64, f64)]) {
    let samples: Vec<MetricSampleRow> = rows
        .iter()
        .map(|(at_ms, value)| MetricSampleRow {
            metric_name: name.to_string(),
            fingerprint: Fingerprint::from_raw(fp),
            unix_milli: *at_ms,
            value: *value,
        })
        .collect();
    client
        .insert_block("metric_samples", &samples)
        .await
        .expect("seed metric_samples");
}

/// **M2, M3 and M4**: the §7 rule, read back through the API.
///
/// ```text
///   stored at one millisecond                  answered
///   42.0, 42.0                                 one sample   (M2)
///   5.0, 7.0, 5.0                              5.0 and 7.0  (M3)
///   two floats and one histogram               the histogram alone (M4)
///   1.0, 2.0                                   both — a contradiction the
///                                              stored columns cannot settle
/// ```
#[tokio::test]
async fn one_answer_per_series_millisecond() {
    if !should_run() {
        eprintln!("skipping: PULSUS_TEST_CLICKHOUSE is not set");
        return;
    }
    let db = ScopedDb::fresh(pulsus_testkit::test_db("a494_collapse")).await;
    let _server = spawn_ready(COLLAPSE_PORT, &db, &[]);
    let client = client_for(db.name()).await;

    let at_ms = T_NS / 1_000_000;
    let eval_ms = at_ms + 60_000;

    // M2: one series, one sample, stored twice.
    seed_series(&client, "m2_probe", 0x494_0002, at_ms, "m2").await;
    seed_floats(
        &client,
        "m2_probe",
        0x494_0002,
        &[(at_ms, 42.0), (at_ms, 42.0)],
    )
    .await;

    // M3: 5.0, 7.0, 5.0 at one millisecond.
    seed_series(&client, "m3_probe", 0x494_0003, at_ms, "m3").await;
    seed_floats(
        &client,
        "m3_probe",
        0x494_0003,
        &[(at_ms, 5.0), (at_ms, 7.0), (at_ms, 5.0)],
    )
    .await;

    // M5: two different values at one millisecond — unchanged, both stored
    // and both returned.
    seed_series(&client, "m5_probe", 0x494_0005, at_ms, "m5").await;
    seed_floats(
        &client,
        "m5_probe",
        0x494_0005,
        &[(at_ms, 1.0), (at_ms, 2.0)],
    )
    .await;

    // M4: two floats and one histogram at one millisecond.
    seed_series(&client, "m4_probe", 0x494_0004, at_ms, "m4").await;
    seed_floats(
        &client,
        "m4_probe",
        0x494_0004,
        &[(at_ms, 5.0), (at_ms, 7.0)],
    )
    .await;
    let hist = vec![MetricHistSampleRow {
        metric_name: "m4_probe".to_string(),
        fingerprint: Fingerprint::from_raw(0x494_0004),
        unix_milli: at_ms,
        schema: 0,
        zero_threshold: 0.0,
        zero_count: 0,
        count: 3,
        sum: 12.0,
        pos_span_offsets: vec![0],
        pos_span_lengths: vec![1],
        pos_bucket_deltas: vec![3],
        neg_span_offsets: Vec::new(),
        neg_span_lengths: Vec::new(),
        neg_bucket_deltas: Vec::new(),
        custom_values: Vec::new(),
        counter_reset_hint: 0,
    }];
    client
        .insert_block("metric_hist_samples", &hist)
        .await
        .expect("seed metric_hist_samples");

    // M2 — the sample stored twice is one sample, and `rate` over a single
    // sample has no series at all rather than a `NaN`.
    assert_eq!(
        instant_value(COLLAPSE_PORT, r#"count_over_time(m2_probe[5m])"#, eval_ms),
        "1",
        "M2: two rows identical in (fingerprint, unix_milli, value bits) are one sample"
    );
    assert_eq!(
        instant_value(COLLAPSE_PORT, r#"sum_over_time(m2_probe[5m])"#, eval_ms),
        "42",
        "M2: and their sum is one sample's value"
    );
    assert_eq!(
        instant_value(COLLAPSE_PORT, r#"stddev_over_time(m2_probe[5m])"#, eval_ms),
        "0"
    );
    for query in [r#"rate(m2_probe[5m])"#, r#"increase(m2_probe[5m])"#] {
        assert_eq!(
            series_count(COLLAPSE_PORT, query, eval_ms),
            0,
            "M2: {query} over a single sample returns no series — the assertion \
             does not depend on how a NaN would have been rendered"
        );
    }

    // M3 — the repeat two places later is still the repeat.
    assert_eq!(
        instant_value(COLLAPSE_PORT, r#"count_over_time(m3_probe[5m])"#, eval_ms),
        "2",
        "M3: 5.0, 7.0, 5.0 at one millisecond is two samples"
    );
    assert_eq!(
        instant_value(COLLAPSE_PORT, r#"sum_over_time(m3_probe[5m])"#, eval_ms),
        "12",
        "M3: 5 + 7, with the repeat dropped"
    );

    // M5 — a contradiction is left exactly as stored.
    assert_eq!(
        instant_value(COLLAPSE_PORT, r#"count_over_time(m5_probe[5m])"#, eval_ms),
        "2",
        "M5: two different values at one millisecond are both stored and both returned"
    );

    // M4 — the histogram consumes the whole float run.
    assert_eq!(
        instant_value(COLLAPSE_PORT, r#"count_over_time(m4_probe[5m])"#, eval_ms),
        "1",
        "M4: a histogram at a timestamp leaves no float behind"
    );
}

// ---------------------------------------------------------------------
// Criterion 12 — the sample budget still counts fetched rows
// ---------------------------------------------------------------------

/// `PULSUS_PROMQL_MAX_SAMPLES` counts a **fetched** row, before the
/// collapse: a cap at the exact fetched-row count serves, and one lower
/// refuses. Moving the collapse upstream of the charge would make the cap
/// count collapsed samples instead, and the lower cap would start serving.
#[tokio::test]
async fn the_sample_budget_still_counts_fetched_rows() {
    if !should_run() {
        eprintln!("skipping: PULSUS_TEST_CLICKHOUSE is not set");
        return;
    }
    let db = ScopedDb::fresh(pulsus_testkit::test_db("a494_budget")).await;

    // Four stored rows at one millisecond, which the read path answers as
    // ONE sample. The two numbers differ, which is what makes this a test.
    let at_ms = T_NS / 1_000_000;
    {
        // The server creates the database, so the client is built after it.
        let _seeder = spawn_ready(BUDGET_PORT, &db, &[("PULSUS_PROMQL_MAX_SAMPLES", "100")]);
        let client = client_for(db.name()).await;
        seed_series(&client, "budget_probe", 0x494_0012, at_ms, "budget").await;
        seed_floats(
            &client,
            "budget_probe",
            0x494_0012,
            &[(at_ms, 1.0), (at_ms, 1.0), (at_ms, 1.0), (at_ms, 1.0)],
        )
        .await;
        assert_eq!(
            instant_value(
                BUDGET_PORT,
                r#"count_over_time(budget_probe[5m])"#,
                at_ms + 60_000
            ),
            "1",
            "the four stored rows are one sample"
        );
    }

    let eval_ms = at_ms + 60_000;
    let path = format!(
        "/api/v1/query?query={}&time={}",
        urlencode(r#"count_over_time(budget_probe[5m])"#),
        eval_ms as f64 / 1000.0
    );

    {
        let _at_cap = spawn_ready(BUDGET_PORT, &db, &[("PULSUS_PROMQL_MAX_SAMPLES", "4")]);
        let response = http_get(BUDGET_PORT, &path);
        assert_eq!(
            response.status, 200,
            "a cap at the FETCHED row count serves: {}",
            response.body
        );
    }
    {
        let _under_cap = spawn_ready(BUDGET_PORT, &db, &[("PULSUS_PROMQL_MAX_SAMPLES", "3")]);
        let response = http_get(BUDGET_PORT, &path);
        assert_eq!(
            response.status, 422,
            "one under the fetched row count refuses: {}",
            response.body
        );
        assert!(
            response.body.contains("query too broad")
                && response.body.contains("reader.promql_max_samples"),
            "the refusal names the guard: {}",
            response.body
        );
    }
}
