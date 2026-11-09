//! Issue #778 part 1: the read cache, live. Two `pulsusdb` processes in mode
//! `all` over one database: **A** with `PULSUS_READ_CACHE_BYTES=64MiB`,
//! **B** with `0`. Unless a case says otherwise, each request goes to A then
//! B, the bodies are compared byte for byte, and A's `X-Pulsus-Read-Cache`
//! header is checked; the `hit` assertions keep a comparison from passing on
//! a cache that never serves.
//!
//! Fixture: 40 series over 7 h at 5 s for tenants `a` and `b` (same names,
//! different values): 20 counters `rc_total{i}`, 20 gauges `g{i}`, with
//! `g{i="19"}` stopping 4 min 30 s before L1's start and `g{i="0"}` 60 for
//! the first hour and 40 after. It is pushed through B's `/api/v1/write`, so
//! the writer lands it as kinds 0 and 2 with its own fingerprints, which the
//! later pushes of L3, L8 and L15 must share. The level views are stopped and
//! emptied before the push; where a case needs the 60 s level its rows are
//! written straight from the raw samples, 1,000 above them, with one run row.
//!
//! Gated by `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-server --test read_cache_live
//! ```

#[path = "support/live_db.rs"]
mod live_db;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use pulsus_clickhouse::{ChClient, Idempotency, QuerySettings};

const MINUTE: i64 = 60_000;
const HOUR: i64 = 3_600_000;
const STEP: i64 = 15_000;

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test"
            );
            return;
        }
    };
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as i64
}

// -- HTTP over loopback ---------------------------------------------------

struct Resp {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

impl Resp {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
    fn cache(&self) -> &str {
        self.header("x-pulsus-read-cache").unwrap_or("")
    }
    fn through(&self) -> Option<i64> {
        self.header("x-pulsus-read-cache-through")
            .map(|v| v.parse().expect("a time in ms"))
    }
}

fn decode_chunked(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut rest = raw;
    loop {
        let Some(nl) = rest.windows(2).position(|w| w == b"\r\n") else {
            break;
        };
        let size_text = String::from_utf8_lossy(&rest[..nl]);
        let Ok(size) = usize::from_str_radix(size_text.split(';').next().unwrap_or("").trim(), 16)
        else {
            break;
        };
        rest = &rest[nl + 2..];
        if size == 0 || rest.len() < size {
            out.extend_from_slice(&rest[..size.min(rest.len())]);
            break;
        }
        out.extend_from_slice(&rest[..size]);
        rest = rest[size..].strip_prefix(b"\r\n").unwrap_or(&rest[size..]);
    }
    out
}

fn http(port: u16, method: &str, path: &str, headers: &[(&str, &str)], body: &[u8]) -> Resp {
    try_http(port, method, path, headers, body)
        .unwrap_or_else(|| panic!("{method} {path} on port {port}: no response"))
}

fn try_http(
    port: u16,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Option<Resp> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(120))).ok();
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
    let mut bytes = req.into_bytes();
    bytes.extend_from_slice(body);
    stream.write_all(&bytes).ok()?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).ok();
    let split = buf.windows(4).position(|w| w == b"\r\n\r\n")?;
    let head = String::from_utf8_lossy(&buf[..split]).to_string();
    let raw = &buf[split + 4..];
    let mut lines = head.lines();
    let status = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect();
    let chunked = headers.iter().any(|(k, v)| {
        k.eq_ignore_ascii_case("transfer-encoding") && v.eq_ignore_ascii_case("chunked")
    });
    let body = if chunked {
        decode_chunked(raw)
    } else {
        raw.to_vec()
    };
    Some(Resp {
        status,
        headers,
        body: String::from_utf8_lossy(&body).to_string(),
    })
}

fn encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn secs(ms: i64) -> String {
    format!("{}.{:03}", ms.div_euclid(1000), ms.rem_euclid(1000))
}

/// A range request.
#[derive(Clone, Copy)]
struct Range<'a> {
    q: &'a str,
    start: i64,
    end: i64,
    step: i64,
    tenant: &'a str,
    extra: &'a str,
}

impl<'a> Range<'a> {
    fn new(q: &'a str, start: i64, end: i64, step: i64) -> Self {
        Range {
            q,
            start,
            end,
            step,
            tenant: "a",
            extra: "",
        }
    }
    fn shifted(self, by: i64) -> Self {
        Range {
            start: self.start + by,
            end: self.end + by,
            ..self
        }
    }
    fn tenant(self, tenant: &'a str) -> Self {
        Range { tenant, ..self }
    }
    fn extra(self, extra: &'a str) -> Self {
        Range { extra, ..self }
    }
    fn send(&self, port: u16) -> Resp {
        let path = format!(
            "/api/v1/query_range?query={}&start={}&end={}&step={}{}",
            encode(self.q),
            secs(self.start),
            secs(self.end),
            secs(self.step),
            self.extra
        );
        http(port, "GET", &path, &[("X-Scope-OrgID", self.tenant)], b"")
    }
}

// -- servers --------------------------------------------------------------

struct Server {
    child: Child,
    port: u16,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Server {
    fn spawn(port: u16, db: &live_db::ScopedDb, cache_bytes: &str, env: &[(&str, &str)]) -> Self {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_pulsusdb"));
        cmd.env("PULSUS_HOST", "127.0.0.1")
            .env("PULSUS_PORT", port.to_string())
            .env("PULSUS_MODE", "all")
            .env("PULSUS_CACHE_TTL", "1s")
            .env("PULSUS_READ_CACHE_BYTES", cache_bytes)
            .env("CLICKHOUSE_SERVER", live_db::ch_host())
            .env("CLICKHOUSE_HTTP_PORT", live_db::ch_http_port().to_string())
            .env("CLICKHOUSE_DB", db.name());
        for (k, v) in env {
            cmd.env(k, v);
        }
        let child = cmd.spawn().expect("spawn pulsusdb");
        let server = Server { child, port };
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if try_http(port, "GET", "/ready", &[], b"").map(|r| r.status) == Some(200) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "port {port}: /ready never reached 200"
            );
            std::thread::sleep(Duration::from_millis(200));
        }
        server
    }

    /// A counter of `/metrics`, `0` when absent.
    fn metric(&self, name: &str) -> u64 {
        let body = http(self.port, "GET", "/metrics", &[], b"").body;
        body.lines()
            .find(|l| l.split_whitespace().next() == Some(name))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse::<f64>().ok())
            .map_or(0, |v| v as u64)
    }

    fn bypassed(&self, reason: &str) -> u64 {
        self.metric(&format!(
            "pulsus_read_cache_bypass_total{{reason=\"{reason}\"}}"
        ))
    }

    /// The requests that reached the cache's lookup: `hit` and `miss`, and
    /// the bypasses recorded after it (`no_fill`).
    fn lookups(&self) -> u64 {
        self.metric("pulsus_read_cache_requests_total{outcome=\"hit\"}")
            + self.metric("pulsus_read_cache_requests_total{outcome=\"miss\"}")
            + self.bypassed("no_fill")
    }

    /// Waits for the cache's poll to have run at least once more.
    fn settle_poll() {
        std::thread::sleep(Duration::from_secs(6));
    }
}

/// Sends `r` to A then B, asserts both 200 and the same body, and returns
/// A's response.
fn same(a: &Server, b: &Server, r: Range<'_>, what: &str) -> Resp {
    let ra = r.send(a.port);
    let rb = r.send(b.port);
    assert_eq!(ra.status, 200, "{what}: A {}", ra.body);
    assert_eq!(rb.status, 200, "{what}: B {}", rb.body);
    assert!(
        ra.body == rb.body,
        "{what}: A and B differ\nA: {}\nB: {}",
        cut(&ra.body),
        cut(&rb.body)
    );
    ra
}

fn cut(s: &str) -> &str {
    &s[..s.len().min(2_000)]
}

// -- the fixture ----------------------------------------------------------

/// Times of one fixture, fixed when it is loaded.
#[derive(Clone, Copy)]
struct Times {
    /// The last sample, on the 5 s grid.
    now0: i64,
    /// The first sample: `now0 − 7 h`.
    t0: i64,
}

impl Times {
    fn new() -> Self {
        let now0 = now_ms().div_euclid(5_000) * 5_000;
        Times {
            now0,
            t0: now0 - 7 * HOUR,
        }
    }
    /// L1's range: 2 h at 15 s, ending 30 min before the fixture's end.
    fn l1(&self, q: &'static str) -> Range<'static> {
        let end = (self.now0 - 30 * MINUTE).div_euclid(STEP) * STEP;
        Range::new(q, end - 2 * HOUR, end, STEP)
    }
}

/// The value of sample `k` of series `i` of a fixture metric, by tenant.
fn value(metric: &str, tenant: &str, i: usize, k: i64) -> f64 {
    let scale = if tenant == "b" { 2.0 } else { 1.0 };
    match metric {
        "rc_total" => scale * (i as f64 + 1.0) * k as f64,
        "g" if i == 0 => {
            if k < 720 {
                60.0
            } else {
                40.0
            }
        }
        _ => scale * (50.0 + i as f64 + (k % 60) as f64 / 10.0),
    }
}

/// A remote write of float series, each `(labels, [(ms, value)])`.
/// One float series of a remote write: its labels and its `(ms, value)`
/// samples.
type FloatSeries = (Vec<(String, String)>, Vec<(i64, f64)>);

fn remote_write(series: &[FloatSeries]) -> Vec<u8> {
    use prost::Message;
    use pulsus_write::protocols::remote_write::{Label, Sample, TimeSeries, WriteRequest};
    let req = WriteRequest {
        timeseries: series
            .iter()
            .map(|(labels, points)| TimeSeries {
                labels: labels
                    .iter()
                    .map(|(n, v)| Label {
                        name: n.clone(),
                        value: v.clone(),
                    })
                    .collect(),
                samples: points
                    .iter()
                    .map(|(t, v)| Sample {
                        value: *v,
                        timestamp: *t,
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy")
}

fn push(port: u16, tenant: &str, body: &[u8]) {
    let r = http(
        port,
        "POST",
        "/api/v1/write",
        &[
            ("Content-Type", "application/x-protobuf"),
            ("X-Scope-OrgID", tenant),
        ],
        body,
    );
    assert!(r.status / 100 == 2, "push: {} {}", r.status, r.body);
}

fn labels(name: &str, extra: &[(&str, String)]) -> Vec<(String, String)> {
    let mut out = vec![("__name__".to_string(), name.to_string())];
    out.extend(extra.iter().map(|(k, v)| (k.to_string(), v.clone())));
    out
}

/// Pushes one float series through `port`.
fn push_series(port: u16, tenant: &str, labels: Vec<(String, String)>, points: Vec<(i64, f64)>) {
    for chunk in points.chunks(5_000) {
        push(
            port,
            tenant,
            &remote_write(&[(labels.clone(), chunk.to_vec())]),
        );
    }
}

/// The 40-series fixture for both tenants, through `port`.
fn load_fixture(port: u16, times: &Times, offset: f64) {
    let samples = (times.now0 - times.t0) / 5_000;
    let g19_stop = times.l1("g").start - 270_000;
    for tenant in ["a", "b"] {
        for i in 0..20 {
            for metric in ["rc_total", "g"] {
                let points: Vec<(i64, f64)> = (0..=samples)
                    .map(|k| (times.t0 + k * 5_000, value(metric, tenant, i, k) + offset))
                    .filter(|(t, _)| !(metric == "g" && i == 19 && *t > g19_stop))
                    .collect();
                push_series(
                    port,
                    tenant,
                    labels(metric, &[("i", i.to_string())]),
                    points,
                );
            }
        }
    }
}

async fn client(db: &str) -> ChClient {
    ChClient::new(live_db::conn_config(db))
        .await
        .expect("connect")
}

async fn exec(c: &ChClient, sql: &str) {
    c.execute(sql, &QuerySettings::new(), Idempotency::NonIdempotent)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

const LEVEL_VIEWS: [&str; 4] = [
    "metric_samples_60s_mv",
    "metric_samples_720s_mv",
    "metric_samples_7200s_mv",
    "metric_samples_86400s_mv",
];

/// Stops the level views and empties their tables, so nothing builds a level
/// while a case runs.
async fn stop_levels(db: &str) {
    let c = client(db).await;
    for v in LEVEL_VIEWS {
        exec(&c, &format!("SYSTEM WAIT VIEW {db}.{v}")).await;
        exec(&c, &format!("SYSTEM STOP VIEW {db}.{v}")).await;
        let table = v.trim_end_matches("_mv");
        exec(&c, &format!("TRUNCATE TABLE {db}.{table}")).await;
    }
}

/// Writes the 60 s level for `(from, to]` from the raw samples, 1,000 above
/// them, and one run row `(from, to]`.
async fn write_level(db: &str, from: i64, to: i64) {
    let c = client(db).await;
    exec(
        &c,
        &format!(
            "INSERT INTO {db}.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, \
             min, max, sum, count, inc, flags, built_at) \
             SELECT 0, org_id, fingerprint, (intDiv(unix_milli - 1, 60000) + 1) * 60000 AS te, \
             max(unix_milli), argMax(value, unix_milli) + 1000, min(value) + 1000, \
             max(value) + 1000, sum(value) + 1000, toUInt32(count()), \
             max(value) - min(value) + 1000, 0, {to} \
             FROM {db}.metric_samples WHERE unix_milli > {from} AND unix_milli <= {to} \
             GROUP BY org_id, fingerprint, te"
        ),
    )
    .await;
    exec(
        &c,
        &format!(
            "INSERT INTO {db}.metric_samples_60s (kind, org_id, fingerprint, t_last, t_end) \
             VALUES (1, '', 0, {from}, {to})"
        ),
    )
    .await;
}

/// A database with the fixture, and servers A (64 MiB) and B (off).
struct Rig {
    a: Server,
    b: Server,
    times: Times,
    db: live_db::ScopedDb,
}

async fn rig(name: &str, a_port: u16, b_port: u16, env: &[(&str, &str)]) -> Rig {
    rig_retained(name, a_port, b_port, env, None).await
}

async fn rig_retained(
    name: &str,
    a_port: u16,
    b_port: u16,
    env: &[(&str, &str)],
    retention_days: Option<u32>,
) -> Rig {
    let db = live_db::ScopedDb::fresh(name.to_string()).await;
    if let Some(days) = retention_days {
        live_db::state_retention(&db, days).await;
    }
    stop_levels(&db).await;
    let times = Times::new();
    let b = Server::spawn(b_port, &db, "0", env);
    load_fixture(b.port, &times, 0.0);
    let a = Server::spawn(a_port, &db, "64MiB", env);
    Server::settle_poll();
    Rig { a, b, times, db }
}

// -- the cases ------------------------------------------------------------

/// L1 and L2: a raw range is kept, and its shifted repeat is a hit; a series
/// with no point in the new range is dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l1_l2_a_shifted_repeat_is_a_hit() {
    skip_unless_live!();
    let a_port: u16 = 31_426;
    let b_port: u16 = 31_427;
    let r = rig(
        &pulsus_testkit::test_db("pulsus_read_cache_l1_it"),
        a_port,
        b_port,
        &[],
    )
    .await;
    let l1 = r.times.l1("rate(rc_total[5m])");
    let first = same(&r.a, &r.b, l1, "L1");
    assert_eq!(first.cache(), "miss", "L1");
    let second = same(&r.a, &r.b, l1.shifted(MINUTE), "L2");
    assert_eq!(second.cache(), "hit", "L2");
    let g = r.times.l1("g");
    let g1 = same(&r.a, &r.b, g, "L1 g");
    assert!(g1.body.contains(r#""i":"19""#), "g{{i=19}} in L1's answer");
    let g2 = same(&r.a, &r.b, g.shifted(MINUTE), "L2 g");
    assert_eq!(g2.cache(), "hit", "L2 g");
    assert!(
        !g2.body.contains(r#""i":"19""#),
        "g{{i=19}} not in L2's answer"
    );
}

/// L3: a sample sent an hour late is not in the points kept before it
/// arrived; the stated behaviour, not identity.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l3_late_data_is_not_in_kept_points() {
    skip_unless_live!();
    let a_port: u16 = 31_428;
    let b_port: u16 = 31_429;
    let r = rig(
        &pulsus_testkit::test_db("pulsus_read_cache_l3_it"),
        a_port,
        b_port,
        &[],
    )
    .await;
    let l1 = r.times.l1("rate(rc_total[5m])");
    same(&r.a, &r.b, l1, "L1");
    let l2 = l1.shifted(MINUTE);
    let before = same(&r.a, &r.b, l2, "L2");
    assert_eq!(before.cache(), "hit", "L2");
    let c1 = before.through().expect("a kept point");
    // A counter reset an hour before L2's end, between two samples.
    let late = l2.end - HOUR + 2_500;
    push_series(
        r.a.port,
        "a",
        labels("rc_total", &[("i", "0".to_string())]),
        vec![(late, 1.0)],
    );
    let ra = l2.send(r.a.port);
    let rb = l2.send(r.b.port);
    assert_eq!(ra.cache(), "hit");
    assert_eq!(ra.body, before.body, "A's points up to c1 are L2's");
    assert_ne!(rb.body, before.body, "B shows the late sample");
    assert!(
        c1 >= l2.end - (l2.end - l2.start) % STEP,
        "every point of L2 was kept"
    );
}

/// L4: on a process with a 300 KiB budget, an eviction lets the late
/// sample into the answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l4_eviction_lets_late_data_in() {
    skip_unless_live!();
    let a_port: u16 = 31_430;
    let b_port: u16 = 31_431;
    let c_port: u16 = 31_432;
    let r = rig(
        &pulsus_testkit::test_db("pulsus_read_cache_l4_it"),
        a_port,
        b_port,
        &[],
    )
    .await;
    // The share is 100 % so that L2's entry, 20 series of 481 points, fits
    // on its own and two such entries do not.
    let c = Server::spawn(
        c_port,
        &r.db,
        "300KiB",
        &[("PULSUS_READ_CACHE_TENANT_SHARE", "100")],
    );
    Server::settle_poll();
    let l2 = r.times.l1("rate(rc_total[5m])").shifted(MINUTE);
    assert_eq!(l2.send(c.port).cache(), "miss", "C keeps L2");
    push_series(
        c.port,
        "a",
        labels("rc_total", &[("i", "0".to_string())]),
        vec![(l2.end - HOUR + 2_500, 1.0)],
    );
    let evictions = c.metric("pulsus_read_cache_evictions_total");
    // About the size of L2's entry, so the two pass 300 KiB.
    let other = r.times.l1("rate(rc_total[10m])").shifted(MINUTE);
    assert_eq!(other.send(c.port).status, 200);
    assert!(
        c.metric("pulsus_read_cache_evictions_total") > evictions,
        "the other query evicted L2's entry"
    );
    let rc = l2.send(c.port);
    let rb = l2.send(r.b.port);
    assert_eq!(rc.status, 200);
    assert!(rc.body == rb.body, "C = B after the eviction");
}

/// L5: a 6 h range at 60 s on the 60 s level; more level rows; the repeat is
/// a hit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l5_new_level_rows_are_read_by_the_tail() {
    skip_unless_live!();
    let a_port: u16 = 31_433;
    let b_port: u16 = 31_434;
    let r = rig(
        &pulsus_testkit::test_db("pulsus_read_cache_l5_it"),
        a_port,
        b_port,
        &[],
    )
    .await;
    let t1 = (r.times.now0 - 90 * MINUTE).div_euclid(MINUTE) * MINUTE;
    let t2 = (r.times.now0 - 30 * MINUTE).div_euclid(MINUTE) * MINUTE;
    let from = r.times.t0.div_euclid(MINUTE) * MINUTE;
    write_level(&r.db, from, t1).await;
    Server::settle_poll();
    let end = now_ms().div_euclid(MINUTE) * MINUTE;
    let q = Range::new("rate(rc_total[5m])", end - 6 * HOUR, end, MINUTE);
    let first = same(&r.a, &r.b, q, "L5 first");
    assert_eq!(first.cache(), "miss");
    write_level(&r.db, t1, t2).await;
    Server::settle_poll();
    let second = same(&r.a, &r.b, q, "L5 repeat");
    assert_eq!(second.cache(), "hit", "L5 repeat");
}

/// L6: 6 h at 15 s, 7 s off the minute; four refreshes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l6_a_tail_is_routed_as_its_whole_request() {
    skip_unless_live!();
    let a_port: u16 = 31_435;
    let b_port: u16 = 31_436;
    let r = rig(
        &pulsus_testkit::test_db("pulsus_read_cache_l6_it"),
        a_port,
        b_port,
        &[],
    )
    .await;
    let t1 = (r.times.now0 - 30 * MINUTE).div_euclid(MINUTE) * MINUTE;
    write_level(&r.db, r.times.t0.div_euclid(MINUTE) * MINUTE, t1).await;
    Server::settle_poll();
    let end = now_ms().div_euclid(MINUTE) * MINUTE + 7_000;
    let q = Range::new("rate(rc_total[5m])", end - 6 * HOUR, end, STEP);
    for n in 0..4 {
        let resp = same(&r.a, &r.b, q.shifted(n * STEP), &format!("L6 refresh {n}"));
        if n > 0 {
            assert_eq!(resp.cache(), "hit", "L6 refresh {n}");
        }
    }
}

/// L7: `@` is refused by the gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l7_at_is_refused() {
    skip_unless_live!();
    let a_port: u16 = 31_437;
    let b_port: u16 = 31_438;
    let r = rig(
        &pulsus_testkit::test_db("pulsus_read_cache_l7_it"),
        a_port,
        b_port,
        &[],
    )
    .await;
    let before = r.a.bypassed("shape");
    let q = r.times.l1("rate(rc_total[5m] @ end())");
    for n in 0..2 {
        let resp = same(&r.a, &r.b, q, &format!("L7 {n}"));
        assert_eq!(resp.cache(), "bypass");
    }
    assert_eq!(r.a.bypassed("shape"), before + 2);
}

/// L8: an on-time sample is shown by the refresh, which is a hit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l8_an_on_time_sample_is_shown() {
    skip_unless_live!();
    let a_port: u16 = 31_439;
    let b_port: u16 = 31_440;
    let r = rig(
        &pulsus_testkit::test_db("pulsus_read_cache_l8_it"),
        a_port,
        b_port,
        &[],
    )
    .await;
    let end = now_ms().div_euclid(STEP) * STEP;
    let q = Range::new("rate(rc_total[5m])", end - 30 * MINUTE, end, STEP);
    assert_eq!(same(&r.a, &r.b, q, "L8 first").cache(), "miss");
    let at = now_ms() - 30_000;
    for i in 0..20 {
        push_series(
            r.a.port,
            "a",
            labels("rc_total", &[("i", i.to_string())]),
            vec![(at, 1.0e9)],
        );
    }
    let refresh = q.shifted(STEP);
    let resp = same(&r.a, &r.b, refresh, "L8 refresh");
    assert_eq!(resp.cache(), "hit");
}

/// L9: tenants do not share entries.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l9_tenants_do_not_share() {
    skip_unless_live!();
    let a_port: u16 = 31_441;
    let b_port: u16 = 31_442;
    let r = rig(
        &pulsus_testkit::test_db("pulsus_read_cache_l9_it"),
        a_port,
        b_port,
        &[],
    )
    .await;
    let l1 = r.times.l1("rate(rc_total[5m])");
    same(&r.a, &r.b, l1, "L1 a");
    let a = same(&r.a, &r.b, l1.shifted(MINUTE), "L2 a");
    // Control: tenant a's own repeat is served from its entry.
    assert_eq!(a.cache(), "hit", "L2 a");
    let b = same(&r.a, &r.b, l1.shifted(MINUTE).tenant("b"), "L2 b");
    assert_ne!(a.body, b.body, "a and b differ");
}

/// L10: a recreated database is cleared within 10 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l10_a_recreated_database_is_cleared() {
    skip_unless_live!();
    let a_port: u16 = 31_443;
    let b_port: u16 = 31_444;
    let r = rig(
        &pulsus_testkit::test_db("pulsus_read_cache_l10_it"),
        a_port,
        b_port,
        &[],
    )
    .await;
    let l2 = r.times.l1("rate(rc_total[5m])").shifted(MINUTE);
    assert_eq!(same(&r.a, &r.b, l2, "L2").cache(), "miss");
    let clears = r.a.metric("pulsus_read_cache_clears_total");
    live_db::drop_db(&r.db).await;
    live_db::build_schema(&r.db).await;
    stop_levels(&r.db).await;
    load_fixture(r.b.port, &r.times, 10.0);
    let deadline = Instant::now() + Duration::from_secs(10);
    while r.a.metric("pulsus_read_cache_clears_total") <= clears {
        assert!(
            Instant::now() < deadline,
            "A's clears_total did not rise within 10 s"
        );
        std::thread::sleep(Duration::from_millis(250));
    }
    same(&r.a, &r.b, l2, "L2 after the rebuild");
}

/// L12: a gauge under `rate`, twice; the repeat is a hit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l12_an_info_is_kept_with_its_points() {
    skip_unless_live!();
    let a_port: u16 = 31_445;
    let b_port: u16 = 31_446;
    let r = rig(
        &pulsus_testkit::test_db("pulsus_read_cache_l12_it"),
        a_port,
        b_port,
        &[],
    )
    .await;
    let end = now_ms().div_euclid(STEP) * STEP;
    let q = Range::new("rate(g[5m])", end - 2 * HOUR, end, STEP);
    same(&r.a, &r.b, q, "L12 first");
    assert_eq!(same(&r.a, &r.b, q, "L12 second").cache(), "hit");
}

/// L13: `limit` is applied after the merge.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l13_limit_is_applied_after_the_merge() {
    skip_unless_live!();
    let a_port: u16 = 31_447;
    let b_port: u16 = 31_448;
    let r = rig(
        &pulsus_testkit::test_db("pulsus_read_cache_l13_it"),
        a_port,
        b_port,
        &[],
    )
    .await;
    let l1 = r.times.l1("rate(rc_total[5m])");
    same(&r.a, &r.b, l1.extra("&limit=3"), "L1 limit 3");
    let second = same(
        &r.a,
        &r.b,
        l1.shifted(MINUTE).extra("&limit=5"),
        "L2 limit 5",
    );
    assert_eq!(second.cache(), "hit");
}

/// L14: a query reading data less than a day inside retention bypasses.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l14_data_near_the_retention_floor_bypasses() {
    skip_unless_live!();
    let a_port: u16 = 31_449;
    let b_port: u16 = 31_450;
    let r = rig_retained(
        &pulsus_testkit::test_db("pulsus_read_cache_l14_it"),
        a_port,
        b_port,
        &[],
        Some(2),
    )
    .await;
    let before = r.a.bypassed("retention");
    let end = now_ms().div_euclid(STEP) * STEP;
    let start = (end - 36 * HOUR).div_euclid(STEP) * STEP;
    let first = same(
        &r.a,
        &r.b,
        Range::new("rate(rc_total[5m])", start, end, MINUTE),
        "L14 first",
    );
    assert_eq!(first.cache(), "bypass");
    let start = (end - 12 * HOUR).div_euclid(STEP) * STEP;
    let second = same(
        &r.a,
        &r.b,
        Range::new("rate(rc_total[30h])", start, end, MINUTE),
        "L14 second",
    );
    assert_eq!(second.cache(), "bypass");
    assert_eq!(r.a.bypassed("retention"), before + 2);
}

/// L15: samples pushed at each refresh's end are shown by the next.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l15_a_sample_at_the_refresh_end_is_shown() {
    skip_unless_live!();
    let a_port: u16 = 31_451;
    let b_port: u16 = 31_452;
    let r = rig(
        &pulsus_testkit::test_db("pulsus_read_cache_l15_it"),
        a_port,
        b_port,
        &[],
    )
    .await;
    let mut end = now_ms().div_euclid(STEP) * STEP;
    for n in 0..3 {
        let q = Range::new("rate(rc_total[5m])", end - 30 * MINUTE, end, STEP);
        let resp = same(&r.a, &r.b, q, &format!("L15 refresh {n}"));
        if n > 0 {
            assert_eq!(resp.cache(), "hit", "L15 refresh {n}");
        }
        for i in 0..20 {
            push_series(
                r.a.port,
                "a",
                labels("rc_total", &[("i", i.to_string())]),
                vec![(end, 1.0e9 + n as f64)],
            );
        }
        std::thread::sleep(Duration::from_secs(15));
        end += STEP;
    }
}

/// L16: a duration expression is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l16_a_duration_expression_is_refused() {
    skip_unless_live!();
    let a_port: u16 = 31_453;
    let b_port: u16 = 31_454;
    let r = rig(
        &pulsus_testkit::test_db("pulsus_read_cache_l16_it"),
        a_port,
        b_port,
        &[("PULSUS_PROMQL_EXPERIMENTAL_FUNCTIONS", "true")],
    )
    .await;
    let (lookups, shape) = (r.a.lookups(), r.a.bypassed("shape"));
    let l1 = r.times.l1("rate(rc_total[range() / 24])");
    for (n, q) in [l1, l1, Range::new(l1.q, l1.end - 3 * HOUR, l1.end, STEP)]
        .into_iter()
        .enumerate()
    {
        assert_eq!(same(&r.a, &r.b, q, &format!("L16 {n}")).cache(), "bypass");
    }
    assert_eq!(r.a.lookups(), lookups, "A's lookups");
    assert_eq!(r.a.bypassed("shape"), shape + 3);
}

/// The samples the raw read of `rate(rc_total[5m])` over `(start, end]`
/// materialises: its fetch window is `(start − 5 m − 5 m lookback, end]`.
async fn charge_of(db: &str, start: i64, end: i64) -> u64 {
    #[derive(pulsus_clickhouse::Row, serde::Deserialize, serde::Serialize)]
    struct N {
        n: u64,
    }
    let c = client(db).await;
    let sql = format!(
        "SELECT count() AS n FROM {db}.metric_samples WHERE org_id = 'a' \
         AND unix_milli > {} AND unix_milli <= {end} AND fingerprint IN \
         (SELECT fingerprint FROM {db}.metric_labels WHERE org_id = 'a' AND metric_name = 'rc_total')",
        start - 10 * MINUTE
    );
    use futures::StreamExt;
    let mut s = c
        .query_stream::<N>(&sql, &QuerySettings::new())
        .await
        .expect("count");
    s.next().await.expect("a row").expect("a count").n
}

/// L17: a hit whose charge could pass the cap is evaluated in full, and
/// refused as the cache-off server refuses it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l17_a_hit_past_the_sample_cap_is_refused_in_full() {
    skip_unless_live!();
    let a_port: u16 = 31_455;
    let b_port: u16 = 31_456;
    // The cap is read from the fixture, so the servers start after it.
    let db = live_db::ScopedDb::fresh(pulsus_testkit::test_db("pulsus_read_cache_l17_it")).await;
    stop_levels(&db).await;
    let times = Times::new();
    {
        let loader_port: u16 = 31_457;
        let loader = Server::spawn(loader_port, &db, "0", &[]);
        load_fixture(loader.port, &times, 0.0);
    }
    let end = now_ms().div_euclid(STEP) * STEP;
    let l1 = Range::new("rate(rc_total[5m])", end - 2 * HOUR, end, STEP);
    let charge = charge_of(&db, l1.start, l1.end).await;
    // Just above L1's charge, with room for L2's one-minute tail.
    let cap = (charge + 4_000).to_string();
    let env = [("PULSUS_PROMQL_MAX_SAMPLES", cap.as_str())];
    let b = Server::spawn(b_port, &db, "0", &env);
    let a = Server::spawn(a_port, &db, "64MiB", &env);
    Server::settle_poll();
    same(&a, &b, l1, "L1");
    assert_eq!(same(&a, &b, l1.shifted(MINUTE), "L2").cache(), "hit", "L2");
    // 40 more series, 200 samples each, ending now.
    let now = now_ms().div_euclid(5_000) * 5_000;
    for i in 20..60 {
        push_series(
            a.port,
            "a",
            labels("rc_total", &[("i", i.to_string())]),
            (0..200)
                .map(|k| (now - (199 - k) * 5_000, k as f64))
                .collect(),
        );
    }
    let refresh = l1.shifted(2 * MINUTE);
    let ra = refresh.send(a.port);
    let rb = refresh.send(b.port);
    assert_eq!(rb.status, 422, "B refuses: {}", rb.body);
    assert_eq!(ra.status, 422, "A refuses: {}", ra.body);
    assert_eq!(ra.body, rb.body);
    drop(a);
    drop(b);
}

/// The histogram fixture of L21 and L24, through `port`.
fn load_histograms(port: u16, times: &Times) {
    use prost::Message;
    use pulsus_write::protocols::remote_write::{
        BucketSpan, Histogram, HistogramCount, Label, TimeSeries, WriteRequest,
    };
    let samples = (times.now0 - times.t0) / 5_000;
    // L21: a native-histogram counter `hh`.
    let hs: Vec<Histogram> = (0..=samples)
        .map(|k| {
            let n = k as u64;
            Histogram {
                count: Some(HistogramCount::Int(3 * n)),
                sum: 1.5 * n as f64,
                schema: 0,
                zero_threshold: 0.001,
                zero_count: Some(HistogramCount::Int(0)),
                positive_spans: vec![BucketSpan {
                    offset: 0,
                    length: 3,
                }],
                // Absolute counts n, n, n, delta-encoded.
                positive_deltas: vec![n as i64, 0, 0],
                timestamp: times.t0 + k * 5_000,
                ..Default::default()
            }
        })
        .collect();
    for chunk in hs.chunks(2_000) {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![Label {
                    name: "__name__".to_string(),
                    value: "hh".to_string(),
                }],
                histograms: chunk.to_vec(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let body = snap::raw::Encoder::new()
            .compress_vec(&req.encode_to_vec())
            .expect("snappy");
        push(port, "a", &body);
    }
    // L24: two classic histograms; `hb_bucket`'s `le="10"` bucket grows
    // slower than its `le="1"` one, so its rates are not monotonic in `le`.
    for (name, broken) in [("hb_ok_bucket", false), ("hb_bucket", true)] {
        for (j, le) in ["0.1", "1", "10", "+Inf"].into_iter().enumerate() {
            let points: Vec<(i64, f64)> = (0..=samples)
                .map(|k| {
                    let base = (j as f64 + 1.0) * k as f64;
                    let v = if broken && j == 2 {
                        1.5 * k as f64
                    } else {
                        base
                    };
                    (times.t0 + k * 5_000, v)
                })
                .collect();
            push_series(port, "a", labels(name, &[("le", le.to_string())]), points);
        }
    }
}

/// L21: native histograms are kept and merged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l21_native_histograms_are_kept() {
    skip_unless_live!();
    let a_port: u16 = 31_458;
    let b_port: u16 = 31_459;
    let r = rig(
        &pulsus_testkit::test_db("pulsus_read_cache_l21_it"),
        a_port,
        b_port,
        &[],
    )
    .await;
    load_histograms(r.b.port, &r.times);
    for q in ["histogram_quantile(0.9, rate(hh[5m]))", "hh"] {
        let l1 = r.times.l1(q);
        same(&r.a, &r.b, l1, &format!("L1 {q}"));
        let second = same(&r.a, &r.b, l1.shifted(MINUTE), &format!("L2 {q}"));
        assert_eq!(second.cache(), "hit", "{q}");
    }
}

/// L22: every refused shape bypasses without a lookup.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l22_refused_shapes_never_touch_the_cache() {
    skip_unless_live!();
    let a_port: u16 = 31_465;
    let b_port: u16 = 31_466;
    let r = rig(
        &pulsus_testkit::test_db("pulsus_read_cache_l22_it"),
        a_port,
        b_port,
        &[],
    )
    .await;
    let (lookups, shape) = (r.a.lookups(), r.a.bypassed("shape"));
    for q in [
        "(g > 50) or (g + 1)",
        "rate(rc_total[5m:1m])",
        "rate(rc_total[5m] @ end())",
        "label_replace(g, \"x\", \"$1\", \"i\", \"(.*)\")",
        "stddev(g)",
        "time()",
    ] {
        for n in 0..2 {
            let resp = same(&r.a, &r.b, r.times.l1(q), &format!("L22 {q} {n}"));
            assert_eq!(resp.cache(), "bypass", "{q}");
        }
    }
    assert_eq!(r.a.lookups(), lookups, "A's lookups");
    assert_eq!(r.a.bypassed("shape"), shape + 12);
}

/// L23: `or`, whose name latch can differ by step, bypasses.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l23_or_bypasses() {
    skip_unless_live!();
    let a_port: u16 = 31_467;
    let b_port: u16 = 31_468;
    let r = rig(
        &pulsus_testkit::test_db("pulsus_read_cache_l23_it"),
        a_port,
        b_port,
        &[],
    )
    .await;
    // `g{i="0"}` is 60 for the fixture's first hour and 40 after; L1 here
    // spans that change.
    let end = r.times.t0 + HOUR + 30 * MINUTE;
    let l1 = Range::new(
        "(g > 50) or (g + 1)",
        end - 2 * HOUR + 5 * MINUTE,
        end,
        STEP,
    );
    let shape = r.a.bypassed("shape");
    assert_eq!(same(&r.a, &r.b, l1, "L23 L1").cache(), "bypass");
    assert_eq!(
        same(&r.a, &r.b, l1.shifted(MINUTE), "L23 L2").cache(),
        "bypass"
    );
    // Both by the gate.
    assert_eq!(r.a.bypassed("shape"), shape + 2);
}

/// L24: an answer carrying the forced-monotonicity info is never kept; the
/// control without it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l24_a_forced_monotonicity_info_is_never_kept() {
    skip_unless_live!();
    let a_port: u16 = 31_469;
    let b_port: u16 = 31_470;
    let r = rig(
        &pulsus_testkit::test_db("pulsus_read_cache_l24_it"),
        a_port,
        b_port,
        &[],
    )
    .await;
    load_histograms(r.b.port, &r.times);
    let ok = r
        .times
        .l1("histogram_quantile(0.9, rate(hb_ok_bucket[5m]))");
    same(&r.a, &r.b, ok, "control L1");
    let second = same(&r.a, &r.b, ok.shifted(MINUTE), "control L2");
    assert_eq!(second.cache(), "hit", "control");
    assert!(second.through().is_some(), "control: Through");
    let hb = r.times.l1("histogram_quantile(0.9, rate(hb_bucket[5m]))");
    for (n, q) in [hb, hb.shifted(MINUTE)].into_iter().enumerate() {
        let resp = same(&r.a, &r.b, q, &format!("hb {n}"));
        assert!(resp.body.contains("\"infos\""), "hb {n}: the info");
        assert_eq!(resp.cache(), "bypass", "hb {n}");
        assert!(resp.through().is_none(), "hb {n}: no Through");
    }
}
