//! Issue #778 part 1: the read cache's replica-lag gate on the 2-shard
//! fixture (`ci/clickhouse-cluster/compose.yaml`), with fetches stopped on
//! shard 1's second replica. L19 and L20 of the plan; every other live case
//! is single-node, in `read_cache_live.rs`.
//!
//! Servers A (`PULSUS_READ_CACHE_BYTES=64MiB`) and B (`0`), mode `all`, read
//! the cluster through shard 1 and shard 2. The fixture, L1's 40 series, is
//! pushed through B's `/api/v1/write`, so each series lands through
//! `metric_landing_dist` on the shard it hashes to.
//!
//! Gated by `PULSUS_TEST_CLICKHOUSE=1`; the nodes are reached the way
//! `crates/pulsus-read/tests/live_metrics_rollup_cluster.rs` reaches them.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

const CLUSTER_NAME: &str = "pulsus_test_cluster";
const MINUTE: i64 = 60_000;
const HOUR: i64 = 3_600_000;
const STEP: i64 = 15_000;

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as i64
}

fn shard_config(
    host_env: &str,
    default_host: &str,
    port_env: &str,
    database: &str,
) -> ChConnConfig {
    ChConnConfig {
        server: std::env::var(host_env).unwrap_or_else(|_| default_host.to_string()),
        http_port: std::env::var(port_env)
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(8123),
        database: database.to_string(),
        proto: ChProto::Http,
        pool_size: 8,
        query_timeout: Duration::from_secs(120),
        ..ChConnConfig::default()
    }
}

fn shard1_config(database: &str) -> ChConnConfig {
    shard_config(
        "PULSUS_TEST_CH_SHARD1_HOST",
        "172.28.0.11",
        "PULSUS_TEST_CH_SHARD1_HTTP_PORT",
        database,
    )
}

fn shard2_config(database: &str) -> ChConnConfig {
    shard_config(
        "PULSUS_TEST_CH_SHARD2_HOST",
        "172.28.0.12",
        "PULSUS_TEST_CH_SHARD2_HTTP_PORT",
        database,
    )
}

fn shard1b_config(database: &str) -> ChConnConfig {
    shard_config(
        "PULSUS_TEST_CH_SHARD1B_HOST",
        "172.28.0.13",
        "PULSUS_TEST_CH_SHARD1B_HTTP_PORT",
        database,
    )
}

async fn exec(c: &ChClient, sql: &str) {
    c.execute(sql, &QuerySettings::new(), Idempotency::NonIdempotent)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// Drops the database on every node, on the way in and out.
struct ClusterDb {
    name: String,
}

impl ClusterDb {
    async fn fresh(name: String) -> Self {
        let bootstrap = ChClient::new(shard1_config("default"))
            .await
            .expect("bootstrap");
        exec(
            &bootstrap,
            &format!("DROP DATABASE IF EXISTS {name} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
        )
        .await;
        let ctx = RenderCtx {
            cluster: Some(CLUSTER_NAME.to_string()),
            dist_suffix: "_dist".to_string(),
            ..RenderCtx::for_tests(&name, 30)
        };
        run_init(&bootstrap, &ctx)
            .await
            .expect("run_init (clustered)");
        for config in [
            shard1_config(&name),
            shard2_config(&name),
            shard1b_config(&name),
        ] {
            let node = ChClient::new(config).await.expect("a node");
            for r in &ctx.rollup_levels {
                let view = format!("{name}.metric_samples_{r}s_mv");
                exec(&node, &format!("SYSTEM WAIT VIEW {view}")).await;
                exec(&node, &format!("SYSTEM STOP VIEW {view}")).await;
            }
        }
        ClusterDb { name }
    }
}

impl Drop for ClusterDb {
    fn drop(&mut self) {
        let name = self.name.clone();
        let _ = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime")
                .block_on(async {
                    if let Ok(c) = ChClient::new(shard1_config("default")).await {
                        let _ = c
                            .execute(
                                &format!(
                                    "DROP DATABASE IF EXISTS {name} ON CLUSTER '{CLUSTER_NAME}' SYNC"
                                ),
                                &QuerySettings::new(),
                                Idempotency::Idempotent,
                            )
                            .await;
                    }
                });
        })
        .join();
    }
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
    while let Some(nl) = rest.windows(2).position(|w| w == b"\r\n") {
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

fn http(port: u16, method: &str, path: &str, headers: &[(&str, &str)], body: &[u8]) -> Resp {
    try_http(port, method, path, headers, body)
        .unwrap_or_else(|| panic!("{method} {path} on port {port}: no response"))
}

fn encode(s: &str) -> String {
    s.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

fn secs(ms: i64) -> String {
    format!("{}.{:03}", ms.div_euclid(1000), ms.rem_euclid(1000))
}

fn query_range(port: u16, q: &str, start: i64, end: i64) -> Resp {
    let path = format!(
        "/api/v1/query_range?query={}&start={}&end={}&step={}",
        encode(q),
        secs(start),
        secs(end),
        secs(STEP)
    );
    http(port, "GET", &path, &[("X-Scope-OrgID", "a")], b"")
}

// -- servers and the fixture ----------------------------------------------

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

fn endpoint(c: &ChConnConfig) -> String {
    format!("{}:{}", c.server, c.http_port)
}

impl Server {
    fn spawn(port: u16, db: &ClusterDb, cache_bytes: &str) -> Self {
        let servers = format!(
            "{},{}",
            endpoint(&shard1_config(&db.name)),
            endpoint(&shard2_config(&db.name))
        );
        let child = Command::new(env!("CARGO_BIN_EXE_pulsusdb"))
            .env("PULSUS_HOST", "127.0.0.1")
            .env("PULSUS_PORT", port.to_string())
            .env("PULSUS_MODE", "all")
            .env("PULSUS_CACHE_TTL", "1s")
            .env("PULSUS_READ_CACHE_BYTES", cache_bytes)
            .env("PULSUS_CLUSTER", CLUSTER_NAME)
            .env("CLICKHOUSE_SERVERS", servers)
            .env("CLICKHOUSE_DB", &db.name)
            .spawn()
            .expect("spawn pulsusdb");
        let server = Server { child, port };
        let deadline = Instant::now() + Duration::from_secs(90);
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
}

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

fn push(port: u16, metric: &str, i: usize, points: Vec<(i64, f64)>) {
    let labels = vec![
        ("__name__".to_string(), metric.to_string()),
        ("i".to_string(), i.to_string()),
    ];
    for chunk in points.chunks(5_000) {
        let r = http(
            port,
            "POST",
            "/api/v1/write",
            &[
                ("Content-Type", "application/x-protobuf"),
                ("X-Scope-OrgID", "a"),
            ],
            &remote_write(&[(labels.clone(), chunk.to_vec())]),
        );
        assert!(r.status / 100 == 2, "push: {} {}", r.status, r.body);
    }
}

/// L1's 40 series, 7 h at 5 s, through `port`.
fn load_fixture(port: u16) -> i64 {
    let now0 = now_ms().div_euclid(5_000) * 5_000;
    let t0 = now0 - 7 * HOUR;
    for i in 0..20 {
        let rc: Vec<(i64, f64)> = (0..=(7 * 720))
            .map(|k| (t0 + k * 5_000, (i as f64 + 1.0) * k as f64))
            .collect();
        let g: Vec<(i64, f64)> = (0..=(7 * 720))
            .map(|k| (t0 + k * 5_000, 50.0 + i as f64 + (k % 60) as f64 / 10.0))
            .collect();
        push(port, "rc_total", i, rc);
        push(port, "g", i, g);
    }
    now0
}

/// On-time counter samples for every `rc_total` series, at `at`.
fn push_on_time(port: u16, at: i64, v: f64) {
    for i in 0..20 {
        push(port, "rc_total", i, vec![(at, v)]);
    }
}

/// `absolute_delay` of `metric_samples` on shard 1's second replica.
async fn shard1b_delay(db: &str) -> u64 {
    #[derive(pulsus_clickhouse::Row, serde::Deserialize, serde::Serialize)]
    struct D {
        d: u64,
    }
    use futures::StreamExt;
    let c = ChClient::new(shard1b_config(db)).await.expect("shard1b");
    let sql = format!(
        "SELECT toUInt64(absolute_delay) AS d FROM system.replicas \
         WHERE database = '{db}' AND table = 'metric_samples'"
    );
    let mut s = c
        .query_stream::<D>(&sql, &QuerySettings::new())
        .await
        .expect("delay");
    s.next().await.map_or(0, |r| r.expect("a delay").d)
}

/// Waits until shard 1's second replica lags at least 5 s.
async fn wait_for_lag(db: &str, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let d = shard1b_delay(db).await;
        if d >= 5 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{what}: shard1b's delay stayed {d}"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn stop_fetches(db: &str) {
    let c = ChClient::new(shard1b_config(db)).await.expect("shard1b");
    exec(&c, &format!("SYSTEM STOP FETCHES {db}.metric_samples")).await;
}

/// Restarts fetches, syncs the replica, and waits out the lag history.
async fn start_fetches(db: &str) {
    let c = ChClient::new(shard1b_config(db)).await.expect("shard1b");
    exec(&c, &format!("SYSTEM START FETCHES {db}.metric_samples")).await;
    exec(&c, &format!("SYSTEM SYNC REPLICA {db}.metric_samples")).await;
    let deadline = Instant::now() + Duration::from_secs(60);
    while shard1b_delay(db).await > 0 {
        assert!(Instant::now() < deadline, "shard1b did not catch up");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    // Two lag reads 5 s apart, both under 5 s, since 10 s before a read.
    tokio::time::sleep(Duration::from_secs(16)).await;
}

/// L19: no fill while a replica lags; after it catches up, A = B.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l19_a_lagging_replica_stops_fills() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with the cluster fixture");
        return;
    }
    let a_port: u16 = 31_471;
    let b_port: u16 = 31_472;
    let db = ClusterDb::fresh(pulsus_testkit::test_db("pulsus_read_cache_cluster_l19")).await;
    let b = Server::spawn(b_port, &db, "0");
    let now0 = load_fixture(b.port);
    let a = Server::spawn(a_port, &db, "64MiB");
    stop_fetches(&db.name).await;
    push_on_time(b.port, now_ms(), 1.0e9);
    wait_for_lag(&db.name, "L19").await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    let end = (now0 - 30 * MINUTE).div_euclid(STEP) * STEP;
    let start = end - 2 * HOUR;
    let first = query_range(a.port, "rate(rc_total[5m])", start, end);
    assert_eq!(first.status, 200, "{}", first.body);
    assert_eq!(first.cache(), "bypass", "no fill while shard1b lags");
    start_fetches(&db.name).await;
    let ra = query_range(a.port, "rate(rc_total[5m])", start, end);
    let rb = query_range(b.port, "rate(rc_total[5m])", start, end);
    assert_eq!(ra.status, 200);
    assert!(ra.body == rb.body, "after catch-up A = B");
    // Control: with the replica caught up, the answer is kept.
    assert_eq!(ra.cache(), "miss", "kept after catch-up");
}

/// L20: a hit while a replica lags is served with `Through` unchanged; after
/// catch-up, A = B.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l20_a_lagging_hit_keeps_nothing_more() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with the cluster fixture");
        return;
    }
    let a_port: u16 = 31_473;
    let b_port: u16 = 31_474;
    let db = ClusterDb::fresh(pulsus_testkit::test_db("pulsus_read_cache_cluster_l20")).await;
    let b = Server::spawn(b_port, &db, "0");
    load_fixture(b.port);
    let a = Server::spawn(a_port, &db, "64MiB");
    tokio::time::sleep(Duration::from_secs(16)).await;
    let end = now_ms().div_euclid(STEP) * STEP;
    let first = query_range(a.port, "rate(rc_total[5m])", end - 30 * MINUTE, end);
    assert_eq!(first.status, 200, "{}", first.body);
    assert_eq!(first.cache(), "miss", "kept");
    let t0 = first.through().expect("Through");
    stop_fetches(&db.name).await;
    let p = now_ms();
    push_on_time(b.port, p, 1.0e9);
    wait_for_lag(&db.name, "L20").await;
    // Five minutes on, P is past the freshness window; still lagging.
    tokio::time::sleep(Duration::from_secs(300)).await;
    assert!(shard1b_delay(&db.name).await >= 5, "still lagging");
    let end2 = now_ms().div_euclid(STEP) * STEP;
    let lagging = query_range(a.port, "rate(rc_total[5m])", end2 - 30 * MINUTE, end2);
    assert_eq!(lagging.status, 200, "{}", lagging.body);
    assert_eq!(lagging.cache(), "hit", "the lagging refresh");
    assert_eq!(lagging.through(), Some(t0), "Through still T0");
    start_fetches(&db.name).await;
    let end3 = now_ms().div_euclid(STEP) * STEP;
    let ra = query_range(a.port, "rate(rc_total[5m])", end3 - 30 * MINUTE, end3);
    let rb = query_range(b.port, "rate(rc_total[5m])", end3 - 30 * MINUTE, end3);
    assert_eq!(ra.status, 200);
    assert!(ra.body == rb.body, "after catch-up A = B");
}

/// L27 (issue #778, #803's rule): every poll of the read cache is one read
/// request on one server. Over 27 s of polls, the poll's statements, read
/// back from each node's `system.query_log` and merged by start time, come
/// in groups P1, [P2,] P3, each group on one node.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l27_each_poll_reads_on_one_server() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with the cluster fixture");
        return;
    }
    let port: u16 = 31_478;
    let db = ClusterDb::fresh(pulsus_testkit::test_db("pulsus_read_cache_cluster_l27")).await;
    let _server = Server::spawn(port, &db, "64MiB");
    tokio::time::sleep(Duration::from_secs(27)).await;
    let mut rows: Vec<(u64, u8, &str)> = Vec::new();
    for (node, config) in [
        ("shard1", shard1_config(&db.name)),
        ("shard2", shard2_config(&db.name)),
    ] {
        let c = ChClient::new(config).await.expect("a node");
        exec(&c, "SYSTEM FLUSH LOGS").await;
        let sql = format!(
            "SELECT toString(toUInt64(toUnixTimestamp64Micro(query_start_time_microseconds))) || ' ' || \
             toString(multiIf(position(query, 'absolute_delay') > 0, 1, position(query, 'watermark_ms') > 0, 2, \
             position(query, 'system.databases') > 0, 3, 0)) AS s \
             FROM system.query_log WHERE current_database = '{}' \
             AND log_comment = 'background:read-cache-poll' AND type = 'QueryFinish' \
             AND is_initial_query = 1 ORDER BY query_start_time_microseconds",
            db.name
        );
        for line in c
            .query_strings(&sql, &QuerySettings::new().set("log_comment", "l27-read"))
            .await
            .expect("the poll's statements")
        {
            let (t, p) = line.split_once(' ').expect("t p");
            rows.push((t.parse().expect("t"), p.parse().expect("p"), node));
        }
    }
    rows.sort();
    assert!(
        rows.iter().all(|(_, p, _)| *p != 0),
        "L27: a poll statement of no known kind: {rows:?}"
    );
    let first = rows
        .iter()
        .position(|(_, p, _)| *p == 1)
        .expect("L27: no lag read");
    let mut groups: Vec<Vec<(u8, &str)>> = Vec::new();
    let mut open: Vec<(u8, &str)> = Vec::new();
    for (_, p, node) in &rows[first..] {
        open.push((*p, node));
        if *p == 3 {
            groups.push(std::mem::take(&mut open));
        }
    }
    for g in &groups {
        let kinds: Vec<u8> = g.iter().map(|(p, _)| *p).collect();
        assert!(
            kinds == [1, 3] || kinds == [1, 2, 3],
            "L27: a poll's statements out of order: {g:?} in {rows:?}"
        );
        assert!(
            g.iter().all(|(_, n)| *n == g[0].1),
            "L27: one poll on two servers: {g:?}"
        );
    }
    assert!(
        open.len() < 3 && open.iter().all(|(p, _)| *p != 3),
        "L27: trailing statements {open:?}"
    );
    let with_watermarks = groups.iter().filter(|g| g.len() == 3).count();
    assert!(
        with_watermarks >= 4,
        "L27: {with_watermarks} polls read the watermarks: {groups:?}"
    );
}

// -- issue #810, addendum r6 -----------------------------------------------

/// A range request at a 12 minute step, on the 720 s level's grid: a 30 h
/// `last_over_time(g[1m])` is read at the 720 s level's bucket ends, its
/// range one bucket, and each step is the last sample before it, as raw.
const TWELVE: i64 = 12 * MINUTE;

fn query_range_twelve(port: u16, q: &str, start: i64, end: i64) -> Resp {
    let path = format!(
        "/api/v1/query_range?query={}&start={}&end={}&step=720",
        encode(q),
        secs(start),
        secs(end),
    );
    http(port, "GET", &path, &[("X-Scope-OrgID", "a")], b"")
}

/// Waits until no replica of `db`'s metrics tables lags 5 s or more, as the
/// read cache's poll measures it.
async fn wait_for_no_lag(db: &str, what: &str) {
    let c = ChClient::new(shard1_config(db)).await.expect("shard1");
    let sql = format!(
        "SELECT toString(max(absolute_delay)) AS s FROM clusterAllReplicas('{CLUSTER_NAME}', system.replicas) \
         WHERE database = '{db}' AND startsWith(table, 'metric_')"
    );
    let deadline = Instant::now() + Duration::from_secs(600);
    loop {
        let lag: u64 = c
            .query_strings(&sql, &QuerySettings::new())
            .await
            .expect("lag")
            .first()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        if lag < 5 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{what}: the replicas still lag {lag} s"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// The 60 s and 720 s levels for `(from, to]` from the raw samples of both
/// shards, as a build writes them, and their run rows `(from, to]` on each
/// shard: a 30 h range reads the 720 s level, and both levels' watermarks
/// count for it. `to` is on both levels' grids.
async fn write_level_cluster(db: &str, from: i64, to: i64) {
    let s1 = ChClient::new(shard1_config(db)).await.expect("shard1");
    for width in [60_000i64, 720_000] {
        let level = width / 1000;
        exec(
            &s1,
            &format!(
                "INSERT INTO {db}.metric_samples_{level}s_dist (kind, org_id, fingerprint, t_end, t_last, last, \
                 min, max, sum, count, inc, flags, built_at) \
                 SETTINGS insert_distributed_sync = 1 \
                 SELECT 0, org_id, fingerprint, (intDiv(unix_milli - 1, {width}) + 1) * {width} AS te, \
                 max(unix_milli), argMax(value, unix_milli), min(value), max(value), sum(value), \
                 toUInt32(count()), max(value) - min(value), 0, {to} \
                 FROM {db}.metric_samples_dist WHERE unix_milli > {from} AND unix_milli <= {to} \
                 GROUP BY org_id, fingerprint, te"
            ),
        )
        .await;
    }
    for config in [shard1_config(db), shard2_config(db)] {
        let node = ChClient::new(config).await.expect("a node");
        for level in ["60s", "720s"] {
            exec(
                &node,
                &format!(
                    "INSERT INTO {db}.metric_samples_{level} (kind, org_id, fingerprint, t_last, t_end) \
                     VALUES (1, '', 0, {from}, {to})"
                ),
            )
            .await;
        }
    }
}

/// Each top-level statement the request `id` sent, on either shard, with
/// its replica delay setting.
async fn delays_of(db: &str, id: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for config in [shard1_config(db), shard2_config(db)] {
        let node = ChClient::new(config).await.expect("a node");
        exec(&node, "SYSTEM FLUSH LOGS").await;
        let rows = node
            .query_strings(
                &format!(
                    "SELECT Settings['max_replica_delay_for_distributed_queries'] || '\t' || query AS s \
                     FROM system.query_log \
                     WHERE type = 'QueryFinish' AND is_initial_query AND log_comment = '{id}'"
                ),
                &QuerySettings::new().set("log_comment", "t810-log"),
            )
            .await
            .expect("query_log");
        for r in rows {
            let (d, q) = r.split_once('\t').unwrap_or(("", ""));
            out.push((q.to_string(), d.to_string()));
        }
    }
    out
}

/// One native-histogram sample of `g{i="h"}` at `at`, through `port`.
fn push_one_histogram(port: u16, at: i64) {
    use prost::Message;
    use pulsus_write::protocols::remote_write::{
        BucketSpan, Histogram, HistogramCount, Label, TimeSeries, WriteRequest,
    };
    let req = WriteRequest {
        timeseries: vec![TimeSeries {
            labels: vec![
                Label {
                    name: "__name__".to_string(),
                    value: "g".to_string(),
                },
                Label {
                    name: "i".to_string(),
                    value: "h".to_string(),
                },
            ],
            histograms: vec![Histogram {
                count: Some(HistogramCount::Int(3)),
                sum: 1.5,
                schema: 0,
                zero_threshold: 0.001,
                zero_count: Some(HistogramCount::Int(0)),
                positive_spans: vec![BucketSpan {
                    offset: 0,
                    length: 3,
                }],
                positive_deltas: vec![1, 0, 0],
                timestamp: at,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let body = snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy");
    let r = http(
        port,
        "POST",
        "/api/v1/write",
        &[
            ("Content-Type", "application/x-protobuf"),
            ("X-Scope-OrgID", "a"),
        ],
        &body,
    );
    assert!(r.status / 100 == 2, "push: {} {}", r.status, r.body);
}

/// T36 and T40: the fixture, the 60 s level built over it, servers A and
/// B, a 30 h `last_over_time(g[1m])` at a 12 minute step kept by A, and
/// shard 1's second replica made to lag; the repeat is one step later. With `histogram`, a native-histogram sample lands
/// in the newer points before the repeat. The repeat on A and on B, and the
/// statements A's repeat sent.
async fn tail_case(
    name: &str,
    a_port: u16,
    b_port: u16,
    histogram: bool,
) -> (Resp, Resp, Vec<(String, String)>) {
    let db = ClusterDb::fresh(pulsus_testkit::test_db(name)).await;
    let b = Server::spawn(b_port, &db, "0");
    let now0 = load_fixture(b.port);
    let built_to = now0.div_euclid(TWELVE) * TWELVE;
    write_level_cluster(
        &db.name,
        (now0 - 7 * HOUR).div_euclid(TWELVE) * TWELVE,
        built_to,
    )
    .await;
    let a = Server::spawn(a_port, &db, "64MiB");
    // The bulk writes replicated, then two lag reads under 5 s since 10 s
    // before the first request.
    wait_for_no_lag(&db.name, name).await;
    tokio::time::sleep(Duration::from_secs(16)).await;
    let q = "last_over_time(g[1m])";
    // One step before the newest built bucket, so the repeat's last step
    // and the sample landed in it are not in the future.
    let end = built_to - TWELVE;
    let first = query_range_twelve(a.port, q, end - 30 * HOUR, end);
    assert_eq!(first.status, 200, "{name}: {}", first.body);
    assert_eq!(
        first.cache(),
        "miss",
        "{name}: the first is kept: {}",
        &first.body[..first.body.len().min(300)]
    );
    if histogram {
        // In the repeat's newest step's window, after the kept points'.
        push_one_histogram(b.port, end + TWELVE - 30_000);
    }
    stop_fetches(&db.name).await;
    push_on_time(b.port, now_ms(), 1.0e9);
    wait_for_lag(&db.name, name).await;
    let ra = query_range_twelve(a.port, q, end + TWELVE - 30 * HOUR, end + TWELVE);
    let rb = query_range_twelve(b.port, q, end + TWELVE - 30 * HOUR, end + TWELVE);
    let id = ra
        .header("x-pulsus-query-id")
        .expect("an X-Pulsus-Query-Id")
        .to_string();
    let sent = delays_of(&db.name, &id).await;
    start_fetches(&db.name).await;
    drop(a);
    drop(b);
    (ra, rb, sent)
}

/// **T36 (issue #810, addendum r6): a cache tail keeps the lag guard.** A
/// 30 h range is past the label cache's 24 h window; its repeat's tail is
/// not, so the tail's one statement names its series by the cache's list
/// and carries the lag guard.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cache_tail_keeps_the_lag_guard() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with the cluster fixture");
        return;
    }
    let (ra, _, sent) = tail_case("pulsus_read_cache_cluster_t810_36", 31_911, 31_912, false).await;
    assert_eq!(ra.status, 200, "T36: {}", ra.body);
    assert_eq!(ra.cache(), "hit", "T36: the repeat");
    assert_eq!(sent.len(), 1, "T36: the tail's statements {sent:#?}");
    assert_eq!(sent[0].1, "5", "T36: the lag guard: {}", sent[0].0);
}

/// **T40 (issue #810, addendum r6): a histogram tail keeps the lag guard.**
/// As T36, with a native-histogram sample landed in the newer points: the
/// repeat is a hit whose one tail statement is read raw and carries the lag
/// guard, no statement reads the whole range, and the answer is the
/// cache-off server's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_histogram_tail_keeps_the_lag_guard() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with the cluster fixture");
        return;
    }
    let (ra, rb, sent) = tail_case("pulsus_read_cache_cluster_t810_40", 31_913, 31_914, true).await;
    assert_eq!(ra.status, 200, "T40: {}", ra.body);
    assert_eq!(rb.status, 200, "T40: B {}", rb.body);
    assert_eq!(ra.cache(), "hit", "T40: the repeat");
    assert_eq!(sent.len(), 1, "T40: the tail's statements {sent:#?}");
    assert_eq!(sent[0].1, "5", "T40: the lag guard: {}", sent[0].0);
    assert!(
        sent[0].0.contains("hs.unix_milli"),
        "T40: the tail is read raw: {}",
        sent[0].0
    );
    assert!(
        ra.body.contains("\"h\""),
        "T40: the histogram is in the answer"
    );
    assert!(
        ra.body == rb.body,
        "T40: A and B differ\nA: {}\nB: {}",
        &ra.body[..ra.body.len().min(2000)],
        &rb.body[..rb.body.len().min(2000)]
    );
}
