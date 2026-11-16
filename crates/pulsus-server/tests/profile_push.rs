//! Live tests for the profile push receivers (issue #817, L1 to L5): spawns
//! the `pulsusdb` binary as a writer with the compat routes on and a landing
//! batch of one push, pushes over loopback HTTP on both transports, and reads
//! what was stored and what statements each push sent.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, with `PULSUS_TEST_CH_HTTP_PORT`
//! naming a ClickHouse server's HTTP port.

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
use pulsus_clickhouse::{ChClient, Idempotency, QuerySettings, Row};
use pulsus_profiles_wire::google_v1::{Function, Line, Location, Profile, Sample, ValueType};
use pulsus_profiles_wire::push_v1::{PushRequest, RawProfileSeries, RawSample};
use pulsus_profiles_wire::types_v1::LabelPair;

/// `true` when the gated suite should run; panics in a live CI job whose
/// gate went missing (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

const PORT: u16 = 31_817;

/// The `log_comment` the test's own clock reads carry, so L5 leaves them out.
const PROBE: &str = "l5-probe";

struct RawResponse {
    status: u16,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn dechunk(mut raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(eol) = find_subslice(raw, b"\r\n") {
        let size = std::str::from_utf8(&raw[..eol])
            .ok()
            .and_then(|s| usize::from_str_radix(s.trim(), 16).ok())
            .unwrap_or(0);
        if size == 0 {
            break;
        }
        let start = eol + 2;
        out.extend_from_slice(&raw[start..start + size]);
        raw = &raw[start + size + 2..];
    }
    out
}

fn request(
    port: u16,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Option<RawResponse> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .ok()?;
    let mut head = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (k, v) in headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).ok()?;
    stream.write_all(body).ok()?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).ok()?;
    let split = find_subslice(&buf, b"\r\n\r\n")?;
    let head = String::from_utf8_lossy(&buf[..split]).into_owned();
    let raw_body = &buf[split + 4..];
    let mut lines = head.lines();
    let status = lines.next()?.split_whitespace().nth(1)?.parse().ok()?;
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    let body = if headers
        .get("transfer-encoding")
        .is_some_and(|v| v == "chunked")
    {
        dechunk(raw_body)
    } else {
        raw_body.to_vec()
    };
    Some(RawResponse {
        status,
        headers,
        body,
    })
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// `traces_api_live.rs`'s `spawn_ready_with_env`, which already sets
/// `PULSUS_COMPAT_ENDPOINTS=true`.
fn spawn_ready_with_env(port: u16, db: &ScopedDb, extra_env: &[(&str, &str)]) -> ChildGuard {
    live_db::build_schema_blocking(db);
    live_db::register_push_target(port, db);
    let mut command = Command::new(env!("CARGO_BIN_EXE_pulsusdb"));
    let command = command
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env("CLICKHOUSE_SERVER", live_db::ch_host())
        .env("CLICKHOUSE_HTTP_PORT", live_db::ch_http_port().to_string())
        .env("CLICKHOUSE_DB", db.name())
        .env("PULSUS_COMPAT_ENDPOINTS", "true");
    for (name, value) in extra_env {
        command.env(name, value);
    }
    let guard = ChildGuard(command.spawn().expect("spawn pulsusdb"));
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if request(port, "GET", "/ready", &[], b"").is_some_and(|r| r.status == 200) {
            return guard;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("/ready never reached 200 within 60s");
}

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after 1970")
        .as_nanos() as i64
}

/// A string-table index, interning `v`.
fn s(table: &mut Vec<String>, v: &str) -> i64 {
    if let Some(i) = table.iter().position(|x| x == v) {
        return i as i64;
    }
    table.push(v.to_string());
    (table.len() - 1) as i64
}

/// A profile timed now with one stack `main -> a`, its sample types
/// `types` and its period `period`.
fn profile(types: &[(&str, &str)], period: (&str, &str)) -> Profile {
    let mut t = vec![String::new()];
    let sample_type = types
        .iter()
        .map(|(ty, unit)| ValueType {
            r#type: s(&mut t, ty),
            unit: s(&mut t, unit),
        })
        .collect::<Vec<_>>();
    let period_type = Some(ValueType {
        r#type: s(&mut t, period.0),
        unit: s(&mut t, period.1),
    });
    let file = s(&mut t, "main.go");
    let function = vec![
        Function {
            id: 1,
            name: s(&mut t, "main"),
            filename: file,
            ..Function::default()
        },
        Function {
            id: 2,
            name: s(&mut t, "a"),
            filename: file,
            ..Function::default()
        },
    ];
    let location = [1u64, 2]
        .map(|id| Location {
            id,
            line: vec![Line {
                function_id: id,
                line: 10,
            }],
            ..Location::default()
        })
        .to_vec();
    Profile {
        sample: vec![Sample {
            location_id: vec![2, 1],
            value: vec![3; types.len()],
            label: Vec::new(),
        }],
        sample_type,
        period_type,
        location,
        function,
        string_table: t,
        time_nanos: now_ns(),
        duration_nanos: 10_000_000_000,
        ..Profile::default()
    }
}

/// Part A §0's two sample types.
fn cpu_profile() -> Profile {
    profile(
        &[("samples", "count"), ("cpu", "nanoseconds")],
        ("cpu", "nanoseconds"),
    )
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gz.write_all(bytes).expect("gzip");
    gz.finish().expect("gzip")
}

fn push_request(raw_profile: Vec<u8>) -> PushRequest {
    let label = |name: &str, value: &str| LabelPair {
        name: name.to_string(),
        value: value.to_string(),
    };
    PushRequest {
        series: vec![RawProfileSeries {
            labels: vec![
                label("__name__", "process_cpu"),
                label("service_name", "checkout"),
                label("env", "prod"),
            ],
            samples: vec![RawSample {
                raw_profile,
                id: String::new(),
            }],
            annotations: Vec::new(),
        }],
    }
}

fn base64(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, b)| n | (*b as u32) << (16 - 8 * i));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(A[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

fn multipart(boundary: &str, parts: &[(&str, &[u8])]) -> Vec<u8> {
    let mut out = Vec::new();
    for (name, bytes) in parts {
        out.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"; \
                 filename=\"{name}\"\r\nContent-Type: application/octet-stream\r\n\r\n"
            )
            .as_bytes(),
        );
        out.extend_from_slice(bytes);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    out
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct NowRow {
    t: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, PartialEq)]
struct SeriesRow {
    type_id: String,
    labels: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct StatementRow {
    r#type: String,
    query_kind: String,
    query_id: String,
    query: String,
    tables: Vec<String>,
}

async fn rows<R>(client: &ChClient, sql: &str, settings: &QuerySettings) -> Vec<R>
where
    R: pulsus_clickhouse::ChRow,
{
    let mut stream = client
        .query_stream::<R>(sql, settings)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.unwrap_or_else(|e| panic!("{sql}: {e}")));
    }
    out
}

/// The server's clock, read by a statement L5 leaves out.
async fn server_now(client: &ChClient) -> String {
    let settings = QuerySettings::new().set("log_comment", PROBE);
    rows::<NowRow>(client, "SELECT toString(now64(6)) AS t", &settings)
        .await
        .remove(0)
        .t
}

async fn series(client: &ChClient, tenant: &str) -> Vec<SeriesRow> {
    rows(
        client,
        &format!(
            "SELECT type_id, labels FROM profile_series WHERE org_id = '{tenant}' \
             GROUP BY type_id, labels ORDER BY type_id"
        ),
        &QuerySettings::new(),
    )
    .await
}

/// One push and the window it ran in.
struct Pushed {
    name: &'static str,
    query_id: String,
    t0: String,
    t1: String,
}

/// Sends one push, timed by the server's clock on each side, and checks it
/// was answered `200`.
async fn push(
    client: &ChClient,
    name: &'static str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Pushed {
    let t0 = server_now(client).await;
    let res = request(PORT, "POST", path, headers, body)
        .unwrap_or_else(|| panic!("{name}: the server must be reachable"));
    let t1 = server_now(client).await;
    assert_eq!(
        res.status,
        200,
        "{name}: {}",
        String::from_utf8_lossy(&res.body)
    );
    let query_id = res
        .headers
        .get("x-pulsus-query-id")
        .cloned()
        .unwrap_or_else(|| panic!("{name}: the answer names its request id"));
    Pushed {
        name,
        query_id,
        t0,
        t1,
    }
}

const SAMPLES_LABELS: &str = r#"{"__name__":"process_cpu","__period_type__":"cpu","__period_unit__":"nanoseconds","__profile_type__":"process_cpu:samples:count:cpu:nanoseconds","__service_name__":"checkout","__type__":"samples","__unit__":"count","env":"prod","service_name":"checkout"}"#;

#[tokio::test(flavor = "multi_thread")]
async fn each_profile_push_is_stored_by_one_insert_and_no_read() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_profile_push_it")).await;
    let _server = spawn_ready_with_env(
        PORT,
        &db,
        &[("PULSUS_MODE", "writer"), ("PULSUS_LANDING_BATCH_MS", "0")],
    );
    let client = ChClient::new(live_db::conn_config(&db))
        .await
        .expect("connect");
    let proto = ("content-type", "application/proto");
    let pb = gzip(&cpu_profile().encode_to_vec());
    let mut pushes = Vec::new();

    // L1: the Connect push, protobuf, as `t1`.
    pushes.push(
        push(
            &client,
            "L1",
            "/push.v1.PusherService/Push",
            &[proto, ("x-scope-orgid", "t1")],
            &push_request(pb.clone()).encode_to_vec(),
        )
        .await,
    );
    let t1 = series(&client, "t1").await;
    assert_eq!(t1.len(), 2, "L1: {t1:?}");
    let samples = t1
        .iter()
        .find(|r| r.type_id == "process_cpu:samples:count:cpu:nanoseconds")
        .unwrap_or_else(|| panic!("L1: the samples series, {t1:?}"));
    assert_eq!(samples.labels, SAMPLES_LABELS, "L1");

    // L2: the same push as proto3 JSON, as `t2`.
    let json = format!(
        r#"{{"series":[{{"labels":[{{"name":"__name__","value":"process_cpu"}},{{"name":"service_name","value":"checkout"}},{{"name":"env","value":"prod"}}],"samples":[{{"rawProfile":"{}"}}]}}]}}"#,
        base64(&pb)
    );
    pushes.push(
        push(
            &client,
            "L2",
            "/push.v1.PusherService/Push",
            &[
                ("content-type", "application/json"),
                ("x-scope-orgid", "t2"),
            ],
            json.as_bytes(),
        )
        .await,
    );
    assert_eq!(series(&client, "t2").await, t1, "L2: t2 holds t1's rows");
    assert_eq!(series(&client, "t1").await.len(), 2, "L2: t1 unchanged");

    // L3: `/ingest`, a whole-body pprof as `t3` and a multipart form with a
    // sample-type config as `t4`.
    pushes.push(
        push(
            &client,
            "L3 pprof",
            "/ingest?name=app.cpu%7Benv%3Dprod%7D&format=pprof",
            &[("x-scope-orgid", "t3")],
            &pb,
        )
        .await,
    );
    let t3 = series(&client, "t3").await;
    assert!(!t3.is_empty(), "L3: t3 stored");
    for row in &t3 {
        assert!(
            row.labels.contains(r#""service_name":"app.cpu""#)
                && row.labels.contains(r#""env":"prod""#),
            "L3: {row:?}"
        );
    }
    let lock = profile(
        &[("contentions", "count"), ("delay", "nanoseconds")],
        ("contentions", "count"),
    )
    .encode_to_vec();
    let config = br#"{"contentions":{"display-name":"block_count"},"delay":{"display-name":"block_duration"}}"#;
    pushes.push(
        push(
            &client,
            "L3 multipart",
            "/ingest?name=app&spyName=gospy",
            &[
                ("content-type", "multipart/form-data; boundary=l3boundary"),
                ("x-scope-orgid", "t4"),
            ],
            &multipart(
                "l3boundary",
                &[("profile", &lock), ("sample_type_config", config)],
            ),
        )
        .await,
    );
    let t4 = series(&client, "t4").await;
    assert_eq!(t4.len(), 2, "L3: {t4:?}");
    assert!(
        t4.iter().all(|r| r.type_id.starts_with("block:")),
        "L3: {t4:?}"
    );

    // L4: the request and ingest metrics.
    let res = request(PORT, "GET", "/metrics", &[], b"").expect("/metrics reachable");
    let text = String::from_utf8_lossy(&res.body).into_owned();
    for line in [
        r#"pulsus_http_requests_total{route="/push.v1.PusherService/Push",status="200"} 2"#,
        r#"pulsus_http_requests_total{route="/ingest",status="200"} 2"#,
    ] {
        assert!(text.lines().any(|l| l == line), "L4: {line} in\n{text}");
    }
    let rows_total = text
        .lines()
        .find_map(|l| l.strip_prefix(r#"pulsus_ingest_rows_total{table="profile_landing"} "#))
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or_else(|| panic!("L4: pulsus_ingest_rows_total for profile_landing in\n{text}"));
    assert!(rows_total > 0.0, "L4: {rows_total}");

    // L5: every statement attributable to each push. A statement is the
    // push's when it carries the push's own request id as its log comment,
    // or names a table of this test's database; the test's clock reads are
    // left out by their own log comment, and a background probe of the
    // server, which names no table here, is not the push's.
    client
        .execute(
            "SYSTEM FLUSH LOGS",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("flush logs");
    let landing = format!("{}.profile_landing", db.name());
    for p in &pushes {
        let sql = format!(
            "SELECT toString(type) AS type, query_kind, query_id, query, tables \
             FROM system.query_log \
             WHERE type != 'QueryStart' \
               AND event_time_microseconds BETWEEN toDateTime64('{t0}', 6) AND toDateTime64('{t1}', 6) \
               AND log_comment != '{PROBE}' \
               AND (log_comment = '{id}' OR arrayExists(t -> startsWith(t, '{db}.'), tables)) \
             ORDER BY event_time_microseconds",
            t0 = p.t0,
            t1 = p.t1,
            id = p.query_id,
            db = db.name(),
        );
        let statements: Vec<StatementRow> = rows(&client, &sql, &QuerySettings::new()).await;
        assert_eq!(statements.len(), 1, "L5 {}: {statements:#?}", p.name);
        let s = &statements[0];
        assert_eq!(s.r#type, "QueryFinish", "L5 {}: {s:?}", p.name);
        assert_eq!(s.query_kind, "Insert", "L5 {}: {s:?}", p.name);
        assert!(s.query.starts_with("INSERT INTO"), "L5 {}: {s:?}", p.name);
        assert!(
            !s.query.to_ascii_uppercase().contains("SELECT"),
            "L5 {}: the insert reads nothing, {s:?}",
            p.name
        );
        assert!(s.tables.contains(&landing), "L5 {}: {s:?}", p.name);
        assert!(!s.query_id.is_empty(), "L5 {}", p.name);
    }
}
