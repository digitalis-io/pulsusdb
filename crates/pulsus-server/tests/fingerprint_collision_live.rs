//! Issue #498 criterion 7: **the end-to-end answers, with the two recorded
//! colliding label-set pairs ingested through the shipped routes.**
//!
//! The defect this issue removes is not visible in any SQL text. Two label
//! sets whose 64-bit hash collided shared one identity, so the store
//! answered a query about one series with the other series' data — and it
//! did so with a `200` and no warning. The only check that can see that is
//! the answer.
//!
//! **The fixture**, constructed at issue #498's base by Floyd's cycle
//! detection over `f(x) = hash("service_name" 0xFF <16 hex of x> 0xFF)`
//! through the shipped functions:
//!
//! ```text
//!   label set                            64-bit              128-bit (decimal)
//!   {service_name="f47da23e240616e1"}    0xf468babd2484e99c  324875417358418231230721791952787510312
//!   {service_name="1d9f6a817a133b68"}    0xf468babd2484e99c  324875417358418231224418266131907151976
//!   {service_name="daf4c59f97f55535"}    0x698b6328161f1bde  169254982756129762653057753242817993694
//!   {service_name="4714b1c6956770d2"}    0x698b6328161f1bde  290831904718675152733872074466791463902
//! ```
//!
//! The logs pair collides under `cityHash64`, so it shares the **high** 64
//! bits, which lead the sort key; the metrics pair collides under
//! `xxHash64`, so it differs there. A build that widened the column but
//! compared only the leading word passes one and fails the other.
//!
//! **Every request carries explicit time bounds.** Both families default a
//! missing `start` to a one-hour window anchored on `now`
//! (`prom_api/handlers.rs`, `logs_api/handlers.rs`), so a fixed instant in
//! the past ages out of the default window and the suite would report an
//! empty result rather than a wrong answer. Nothing here omits a bound.
//!
//! **The instants are derived from one `T0` taken at the start of the
//! run.** The metric answers depend on the instant: at `T0+1s` only the
//! first sample exists and both a broken and a repaired build answer the
//! same number, so the queries evaluate at `T0+16s`, the first instant at
//! which the merge of the two series is visible.
//!
//! ```text
//!   PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=18123 \
//!     PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!     cargo test -p pulsus-server --test fingerprint_collision_live
//! ```

#[path = "support/live_db.rs"]
mod live_db;

use live_db::drop_db;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use futures::StreamExt;
use prost::Message;
use pulsus_clickhouse::{ChClient, QuerySettings, Row};
use pulsus_write::protocols::loki_push::{EntryAdapter, PushRequest, StreamAdapter, Timestamp};
use pulsus_write::protocols::remote_write::{Label, Sample, TimeSeries, WriteRequest};

/// The logs pair: one `cityHash64` value, two label sets.
const LOGS_A: &str = "f47da23e240616e1";
const LOGS_B: &str = "1d9f6a817a133b68";
/// The metrics pair: one `xxHash64` value, two label sets.
const METRICS_A: &str = "daf4c59f97f55535";
const METRICS_B: &str = "4714b1c6956770d2";

/// The two fixed listener ports this suite binds, one per test: the
/// suites in this crate run as separate processes, so each test needs its
/// own. Written as integer literals because the uniqueness guard reads
/// them (`live_port_uniqueness.rs`) and a port built by arithmetic is
/// invisible to it.
const ANSWERS_PORT: u16 = 31_620;
const SURFACE_PORT: u16 = 31_621;

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

/// **The pinned instant.** `2026-01-15T00:00:00Z`, as nanoseconds.
///
/// Criterion 7 requires a fixed `T0` and says in as many words that it is
/// never `now`. Every bound this suite sends is derived from it, and no
/// request relies on a default window: both families default a missing
/// `start` to one hour anchored on the wall clock, so a fixed instant in
/// the past would fall outside a default window and the suite would report
/// `[]` as success.
///
/// ```text
///   fixture      A's line, B's line        at T0
///                B's line again            at T0, after the restart
///                pulsus_probe = 11         at T0
///                pulsus_probe = 22         at T0 + 15s
///   evaluated    logs        T0 - 1h .. T0 + 1h, explicit on every request
///                metrics     at T0 + 16s, the first instant at which both
///                              series have a sample
/// ```
///
/// **Its TTL cannot erase it.** The sample tables carry
/// `timestamp_ns + INTERVAL <retention_days> DAY DELETE`, so at the default
/// seven days an instant this far in the past is dropped before the first
/// query runs. The server this suite spawns sets `PULSUS_RETENTION_DAYS`
/// to a hundred years, which `apply_ttl` puts on the tables at every
/// `run_init` — so the pinned instant stays readable however long the
/// constant sits here.
const T0_NS: i64 = 1_768_435_200_000_000_000;

/// The retention this suite's server runs with, in days: a hundred years,
/// so [`T0_NS`] is never inside the delete window. The schema applies it at
/// start-up, and the databases are dropped at the end of each test.
const RETENTION_DAYS: &str = "36500";

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

fn http_request(
    port: u16,
    method: &str,
    path: &str,
    content_type: Option<&str>,
    body: &[u8],
) -> Option<HttpResponse> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(30))).ok();

    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    if let Some(ct) = content_type {
        head.push_str(&format!("Content-Type: {ct}\r\n"));
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
    http_request(port, "GET", path, None, &[]).expect("request reachable")
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

fn spawn_ready(port: u16, db: &str) -> ChildGuard {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pulsusdb"));
    command
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        // The label cache's default TTL is 60s; a shorter one keeps the
        // metrics half inside this test's own deadline. The first attempt
        // at this measurement returned empty for exactly that reason.
        .env("PULSUS_CACHE_TTL", "1s")
        .env("PULSUS_COMPAT_ENDPOINTS", "true")
        // The fixture's instant is pinned in the past; the default
        // seven-day TTL would delete it before the first query.
        .env("PULSUS_RETENTION_DAYS", RETENTION_DAYS)
        .env("CLICKHOUSE_SERVER", ch_host())
        .env("CLICKHOUSE_HTTP_PORT", ch_http_port().to_string())
        .env("CLICKHOUSE_DB", db);
    let guard = ChildGuard(command.spawn().expect("spawn pulsusdb"));
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if http_request(port, "GET", "/ready", None, &[]).is_some_and(|r| r.status == 200) {
            return guard;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("/ready never reached 200 within 60s (port {port}, db {db})");
}

/// One Loki push carrying both colliding streams in ONE request — the
/// shape that used to drop the loser before ClickHouse ever saw it,
/// because the writer's per-request `seen_streams` set keyed on the
/// fingerprint alone.
fn logs_push_body(ts_ns: i64) -> Vec<u8> {
    let entry = |line: &str| EntryAdapter {
        timestamp: Some(Timestamp {
            seconds: ts_ns / 1_000_000_000,
            nanos: (ts_ns % 1_000_000_000) as i32,
        }),
        line: line.to_string(),
        structured_metadata: Vec::new(),
    };
    let req = PushRequest {
        streams: vec![
            StreamAdapter {
                labels: format!(r#"{{service_name="{LOGS_A}"}}"#),
                entries: vec![entry("line from stream A")],
            },
            StreamAdapter {
                labels: format!(r#"{{service_name="{LOGS_B}"}}"#),
                entries: vec![entry("line from stream B")],
            },
        ],
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy-compress the push")
}

/// The **second push of B**, alone, carrying the same entry as the first:
/// the same nanosecond timestamp and the same line.
///
/// This is what produces the second of the two states the criterion
/// names. A stream row is written once per fingerprint per process, from
/// a cache the writer holds in memory, so a restart is what makes the
/// writer consider `B` again. Under a 64-bit fingerprint the two label
/// sets hashed to one value and `log_streams` is a
/// `ReplacingMergeTree` ordered on `fingerprint` alone
/// (`crates/pulsus-schema/src/catalog.rs` migration 6), so the second
/// write replaced the first row: the same fingerprint then resolved to
/// `B`'s labels, and `A`'s selector answered with `B`'s label set.
fn logs_push_b_again(ts_ns: i64) -> Vec<u8> {
    let req = PushRequest {
        streams: vec![StreamAdapter {
            labels: format!(r#"{{service_name="{LOGS_B}"}}"#),
            entries: vec![EntryAdapter {
                timestamp: Some(Timestamp {
                    seconds: ts_ns / 1_000_000_000,
                    nanos: (ts_ns % 1_000_000_000) as i32,
                }),
                line: "line from stream B".to_string(),
                structured_metadata: Vec::new(),
            }],
        }],
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy-compress the push")
}

fn remote_write_body(series: &[(&str, f64, i64)]) -> Vec<u8> {
    let req = WriteRequest {
        timeseries: series
            .iter()
            .map(|(service, value, ts_ms)| TimeSeries {
                labels: vec![
                    Label {
                        name: "__name__".to_string(),
                        value: "pulsus_probe".to_string(),
                    },
                    Label {
                        name: "service_name".to_string(),
                        value: (*service).to_string(),
                    },
                ],
                samples: vec![Sample {
                    value: *value,
                    timestamp: *ts_ms,
                }],
                histograms: vec![],
            })
            .collect(),
        metadata: vec![],
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy-compress the write request")
}

/// Every returned stream's label map paired with its lines, for one
/// `query_range` over an explicit window.
fn query_streams(
    port: u16,
    selector: &str,
    t0_ns: i64,
) -> Vec<(BTreeMap<String, String>, Vec<String>)> {
    let start = t0_ns - 3_600_000_000_000;
    let end = t0_ns + 3_600_000_000_000;
    let path = format!(
        "/api/logs/v1/query_range?query={}&start={start}&end={end}&limit=100",
        urlencode(selector)
    );
    let res = http_get(port, &path);
    assert_eq!(res.status, 200, "query_range status (body: {})", res.body);
    let json: serde_json::Value =
        serde_json::from_str(&res.body).unwrap_or_else(|e| panic!("json: {e}: {}", res.body));
    let empty = Vec::new();
    json["data"]["result"]
        .as_array()
        .unwrap_or(&empty)
        .iter()
        .map(|stream| {
            let labels = stream["stream"]
                .as_object()
                .expect("stream labels")
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                .collect();
            let lines = stream["values"]
                .as_array()
                .unwrap_or(&empty)
                .iter()
                .map(|v| v[1].as_str().unwrap_or_default().to_string())
                .collect();
            (labels, lines)
        })
        .collect()
}

/// The `{label -> value}` of one instant-vector sample, with its value.
fn instant_series(port: u16, query: &str, at_ns: i64) -> Vec<(BTreeMap<String, String>, String)> {
    let time = at_ns / 1_000_000_000;
    let path = format!("/api/v1/query?query={}&time={time}", urlencode(query));
    let res = http_get(port, &path);
    assert_eq!(res.status, 200, "instant query status (body: {})", res.body);
    let json: serde_json::Value =
        serde_json::from_str(&res.body).unwrap_or_else(|e| panic!("json: {e}: {}", res.body));
    let empty = Vec::new();
    let mut out: Vec<(BTreeMap<String, String>, String)> = json["data"]["result"]
        .as_array()
        .unwrap_or(&empty)
        .iter()
        .map(|s| {
            let labels = s["metric"]
                .as_object()
                .expect("metric labels")
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                .collect();
            (
                labels,
                s["value"][1].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    out.sort();
    out
}

/// The values a logs `label/{name}/values` request answers over an
/// explicit window, sorted.
fn label_values(port: u16, name: &str, t0_ns: i64) -> Vec<String> {
    let start = t0_ns - 3_600_000_000_000;
    let end = t0_ns + 3_600_000_000_000;
    let path = format!("/api/logs/v1/label/{name}/values?start={start}&end={end}");
    let res = http_get(port, &path);
    assert_eq!(res.status, 200, "label values status (body: {})", res.body);
    let json: serde_json::Value =
        serde_json::from_str(&res.body).unwrap_or_else(|e| panic!("json: {e}: {}", res.body));
    let empty = Vec::new();
    let mut out: Vec<String> = json["data"]
        .as_array()
        .unwrap_or(&empty)
        .iter()
        .map(|v| v.as_str().unwrap_or_default().to_string())
        .collect();
    out.sort();
    out
}

/// One answered series: its full label map, paired with whatever the
/// reader collects from its points.
type Series<T> = (BTreeMap<String, String>, T);

/// One logs aggregation answered as a matrix: each series' full label map
/// paired with the distinct values it carries across the steps.
///
/// The values are collected rather than counted because a series count on
/// its own does not say which streams answered: two series both labelled
/// `1d9…` is the shape the widening exists to prevent, and a length check
/// passes on it.
fn logs_aggregation(port: u16, query: &str, t0_ns: i64) -> Vec<Series<BTreeSet<String>>> {
    let start = t0_ns - 3_600_000_000_000;
    let end = t0_ns + 3_600_000_000_000;
    let path = format!(
        "/api/logs/v1/query_range?query={}&start={start}&end={end}&step=3600",
        urlencode(query)
    );
    let res = http_get(port, &path);
    assert_eq!(res.status, 200, "aggregation status (body: {})", res.body);
    let json: serde_json::Value =
        serde_json::from_str(&res.body).unwrap_or_else(|e| panic!("json: {e}: {}", res.body));
    let empty = Vec::new();
    let mut out: Vec<Series<BTreeSet<String>>> = json["data"]["result"]
        .as_array()
        .unwrap_or(&empty)
        .iter()
        .map(|s| {
            let labels = s["metric"]
                .as_object()
                .expect("series labels")
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                .collect();
            let values = s["values"]
                .as_array()
                .unwrap_or(&empty)
                .iter()
                .map(|v| v[1].as_str().unwrap_or_default().to_string())
                .collect();
            (labels, values)
        })
        .collect();
    out.sort();
    out
}

/// Every series a **range** expression answers, with its points as
/// `(millisecond, value)` pairs — the raw `pulsus_probe[2m]` answer,
/// which is a matrix rather than the instant vector `instant_series`
/// reads.
///
/// The wire carries the timestamp as a number of seconds that may have a
/// fractional part, so it is read as `f64` and converted; `as_i64` on a
/// JSON float returns `None` and would silently make every point `0`.
fn range_series(port: u16, query: &str, at_ns: i64) -> Vec<Series<Vec<(i64, String)>>> {
    let time = at_ns / 1_000_000_000;
    let path = format!("/api/v1/query?query={}&time={time}", urlencode(query));
    let res = http_get(port, &path);
    assert_eq!(res.status, 200, "range query status (body: {})", res.body);
    let json: serde_json::Value =
        serde_json::from_str(&res.body).unwrap_or_else(|e| panic!("json: {e}: {}", res.body));
    assert_eq!(
        json["data"]["resultType"].as_str(),
        Some("matrix"),
        "a range expression must answer a matrix: {}",
        res.body
    );
    let empty = Vec::new();
    let mut out: Vec<Series<Vec<(i64, String)>>> = json["data"]["result"]
        .as_array()
        .unwrap_or(&empty)
        .iter()
        .map(|s| {
            let labels = s["metric"]
                .as_object()
                .expect("metric labels")
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                .collect();
            let points = s["values"]
                .as_array()
                .unwrap_or(&empty)
                .iter()
                .map(|v| {
                    let secs = v[0].as_f64().expect("a matrix point timestamp is a number");
                    (
                        (secs * 1000.0).round() as i64,
                        v[1].as_str().unwrap_or_default().to_string(),
                    )
                })
                .collect();
            (labels, points)
        })
        .collect();
    out.sort();
    out
}

/// How many rows `log_streams` holds, and how many distinct fingerprints
/// are among them, read straight from ClickHouse.
///
/// **This is what makes a state's name a checked fact rather than a
/// description.** "Both stream rows" is a statement about storage, not
/// about an answer, and every route above it reads through the same
/// resolution step — so a build that collapsed two streams into one row
/// could still be described as being in that state by a test that only
/// looks at responses. `FINAL` because `log_streams` is a
/// `ReplacingMergeTree` ordered on `fingerprint` alone: without it the
/// superseded row is still on disk and the count is a merge schedule
/// rather than an identity.
async fn stream_rows(db: &str) -> (u64, u64) {
    #[derive(Row, serde::Serialize, serde::Deserialize)]
    struct Counts {
        rows: u64,
        fingerprints: u64,
    }
    let client = ChClient::new(live_db::conn_config(db))
        .await
        .expect("connect to count log_streams");
    let mut stream = client
        .query_stream::<Counts>(
            &format!(
                "SELECT count() AS rows, uniqExact(fingerprint) AS fingerprints \
                 FROM {db}.log_streams FINAL"
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("query log_streams");
    let row = stream
        .next()
        .await
        .expect("one row")
        .expect("decode the counts");
    (row.rows, row.fingerprints)
}

/// How many sample rows each stream has, across the whole table rather
/// than a query window.
///
/// **This is the half that makes a state's name checkable.** The stream
/// rows are `(2, 2)` at every state, so they cannot tell the three apart;
/// what distinguishes them is what each push wrote, and that is a sample
/// count. A state whose name says `B` was written again, run against a
/// fixture that did not write it, fails here rather than passing every
/// assertion that happens not to look.
async fn sample_rows(db: &str) -> Vec<(String, u64)> {
    #[derive(Row, serde::Serialize, serde::Deserialize)]
    struct PerService {
        service: String,
        samples: u64,
    }
    let client = ChClient::new(live_db::conn_config(db))
        .await
        .expect("connect to count log_samples");
    let mut stream = client
        .query_stream::<PerService>(
            &format!(
                "SELECT service, count() AS samples FROM {db}.log_samples \
                 GROUP BY service ORDER BY service"
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("query log_samples");
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        let row = row.expect("decode the per-service counts");
        out.push((row.service, row.samples));
    }
    out
}

/// Every logs answer the criterion requires, at one of the three states.
///
/// `b_lines` is how many copies of `B`'s line are **inside the query
/// window** by then. `A` has one at every state, which is what makes the
/// later states discriminating: the answer to `A`'s selector must not
/// change when `B` is written again.
///
/// **A state is named for the write history that produced it, not for a
/// row count.** All three hold two stream rows in a repaired build —
/// asserted below — so a name like "one stream row" would describe what
/// the *broken* build stored and contradict the assertion beside it. The
/// plan for issue #498 names its two states that way, because it named
/// them by the wrong answers they produced; the mapping is in the table.
///
/// ```text
///   state                         what wrote it              the plan's name
///   ---------------------------   ------------------------   ---------------
///   one request, both streams     one push carrying both     "one stream row"
///                                   streams
///   restarted, B outside window   restart, then B written    "both stream rows"
///                                   again six hours past T0
///   B written inside the window   B written again at T0      (not in the plan;
///                                                              see below)
///
///   state                         A's sel.      B's sel.       regex  sum by()  labels
///   ---------------------------   ------------  -------------  -----  --------  ------
///   one request, both streams     1 line, f47…  1 line, 1d9…   2      1 and 1   both
///   restarted, B outside window   1 line, f47…  1 line, 1d9…   2      1 and 1   both
///   B written inside the window   1 line, f47…  2 lines, 1d9…  2      1 and 2   both
/// ```
///
/// **What each state is for.**
///
/// The first is the plan's `one stream row` row: the writer keeps a
/// per-request set of streams it has already written, and under a 64-bit
/// fingerprint the two label sets collided in it, so the second stream
/// was dropped before ClickHouse saw it. The plan requires `A`'s line
/// from `A`'s selector and `B`'s line, with `B`'s own labels, from `B`'s.
///
/// The second is the plan's `both stream rows` row, and the answers below
/// are the plan's: two streams with one line each, `sum by()` value one
/// per series, both label values.
///
/// The third is not a state the plan tables. It is the same second push
/// the plan describes, landed inside the window, and it is kept because
/// it is the sharper test of the same property — `A`'s line count must
/// stay at one while `B`'s doubles.
///
/// **The middle row is the state the approved plan for issue #498 names,
/// and its answers are the plan's.** The plan reaches that state by
/// pushing `B` a second time, and asks for one `B` line and `sum by()`
/// value `1`. Both cannot hold at once if the second push lands inside
/// the window: `log_samples` is a plain `MergeTree`
/// (`crates/pulsus-schema/src/catalog.rs`, migration 8), it does not
/// deduplicate, and ClickHouse's insert deduplication is a `Replicated*`
/// feature — so a second in-window push is a second row and
/// `count_over_time` counts two. The state the plan describes is reached
/// by putting the second push **outside** the window: the stream row is
/// written again, which is the part that matters, and the window still
/// holds one sample per stream.
///
/// **The third row keeps the other reading**, because it is the one that
/// showed the fixture and the answer table could not both be right, and
/// because it is a sharper test of the same property: `A`'s line count
/// stays at one while `B`'s doubles. Measured there,
/// `{service_name="1d9…"}` answers
///
/// ```text
///   "values": [["<T0>", "line from stream B"],
///              ["<T0>", "line from stream B"]]
/// ```
///
/// with `"entries": 2`, and the aggregation answers `1` for `A` and `2`
/// for `B`.
async fn assert_logs_answers(
    port: u16,
    db: &str,
    written: (u64, u64),
    b_lines: usize,
    t0_ns: i64,
    state: &str,
) {
    // The state's name is a claim about what has been written, so both
    // halves of it are read from storage before any answer is asked for.
    //
    // The stream rows are the same at every state — two, one per stream,
    // with distinct fingerprints — and that is the part a collapsed
    // identity breaks: every route below resolves a selector through
    // `log_streams`, so without this the collapse would surface as a
    // wrong answer several layers downstream instead of as itself.
    //
    // The sample counts are what tell the three states apart, and they
    // are what the names describe. `written` is `(A's samples, B's)`
    // across the whole table, not the query window, because what a state
    // is named for is what it wrote.
    assert_eq!(
        stream_rows(db).await,
        (2, 2),
        "[{state}] log_streams must hold one row per stream, with distinct fingerprints"
    );
    assert_eq!(
        sample_rows(db).await,
        vec![
            (LOGS_B.to_string(), written.1),
            (LOGS_A.to_string(), written.0),
        ],
        "[{state}] log_samples must hold what this state's name says was written"
    );

    // Each selector names ONE stream and gets that stream's own lines,
    // labelled with its own label set. Before the widening, `{…="f47…"}`
    // returned B's line labelled `1d9…`, and `{…="1d9…"}` returned `[]`.
    for (service, line, want_lines) in [
        (LOGS_A, "line from stream A", 1usize),
        (LOGS_B, "line from stream B", b_lines),
    ] {
        let got = query_streams(port, &format!(r#"{{service_name="{service}"}}"#), t0_ns);
        assert_eq!(
            got.len(),
            1,
            "[{state}] {{service_name=\"{service}\"}} answered {} streams, not one: {got:?}",
            got.len()
        );
        assert_eq!(
            got[0].0.get("service_name").map(String::as_str),
            Some(service),
            "[{state}] the returned stream carries the other stream's labels: {got:?}"
        );
        assert_eq!(
            got[0].1,
            vec![line.to_string(); want_lines],
            "[{state}] the returned lines are not {service}'s own {want_lines}: {got:?}"
        );
    }

    // The regex selector reaches BOTH through `log_streams_idx`'s
    // `HAVING uniqExact(key, val) = 1`, which a shared fingerprint used to
    // fail for both streams at once — not just the loser.
    let both = query_streams(
        port,
        &format!(r#"{{service_name=~"{LOGS_A}|{LOGS_B}"}}"#),
        t0_ns,
    );
    let by_service: BTreeMap<&str, &Vec<String>> = both
        .iter()
        .filter_map(|(labels, lines)| {
            labels
                .get("service_name")
                .map(|s| (s.as_str(), lines))
                .filter(|(s, _)| *s == LOGS_A || *s == LOGS_B)
        })
        .collect();
    assert_eq!(
        both.len(),
        2,
        "[{state}] the regex selector answered {} streams, not two: {both:?}",
        both.len()
    );
    assert_eq!(
        by_service.len(),
        2,
        "[{state}] the two answered streams are not {LOGS_A} and {LOGS_B}: {both:?}"
    );
    assert_eq!(
        by_service.get(LOGS_A).map(|v| v.as_slice()),
        Some(["line from stream A".to_string()].as_slice()),
        "[{state}] the regex answer for {LOGS_A}: {both:?}"
    );
    assert_eq!(
        by_service.get(LOGS_B).map(|v| v.as_slice()),
        Some(vec!["line from stream B".to_string(); b_lines].as_slice()),
        "[{state}] the regex answer for {LOGS_B}: {both:?}"
    );

    // And the metric form over the same selector. The labels and the
    // values are asserted, not the series count: two series both labelled
    // `1d9…` is exactly the shape the widening prevents, and a count of
    // two passes on it.
    let agg = format!(
        r#"sum by (service_name) (count_over_time({{service_name=~"{LOGS_A}|{LOGS_B}"}}[1h]))"#
    );
    let series = logs_aggregation(port, &agg, t0_ns);
    let mut want: Vec<(BTreeMap<String, String>, BTreeSet<String>)> =
        [(LOGS_A, "1".to_string()), (LOGS_B, b_lines.to_string())]
            .into_iter()
            .map(|(service, v)| {
                (
                    BTreeMap::from([("service_name".to_string(), service.to_string())]),
                    BTreeSet::from([v]),
                )
            })
            .collect();
    want.sort();
    assert_eq!(
        series, want,
        "[{state}] the aggregation must answer one series per stream, each carrying that \
         stream's own count"
    );

    // And the discovery route over the same window.
    assert_eq!(
        label_values(port, "service_name", t0_ns),
        vec![LOGS_B.to_string(), LOGS_A.to_string()],
        "[{state}] label/service_name/values must list both colliding values"
    );
}

/// **The two colliding pairs are two streams and two series, end to end.**
#[tokio::test(flavor = "multi_thread")]
async fn the_recorded_colliding_pairs_answer_as_two_streams_and_two_series() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test");
        return;
    }
    let db = pulsus_testkit::test_db("pulsus_server_it_fp_collision");
    drop_db(&db).await;
    let guard = spawn_ready(ANSWERS_PORT, &db);

    // `T0` is a constant, never the clock, and every bound below is
    // derived from it (criterion 7).
    let t0_ns = T0_NS;
    let t0_ms = t0_ns / 1_000_000;

    // ---- logs, state one: one request carrying both streams ---------
    // The writer's per-request set of already-written streams keyed on
    // the fingerprint alone, so under 64 bits the second stream was
    // dropped before ClickHouse saw it. Two stream rows is the repaired
    // answer, which is what the state asserts.
    let res = http_request(
        ANSWERS_PORT,
        "POST",
        "/loki/api/v1/push",
        Some("application/x-protobuf"),
        &logs_push_body(t0_ns),
    )
    .expect("push reachable");
    assert_eq!(res.status, 204, "push status (body: {})", res.body);

    assert_logs_answers(
        ANSWERS_PORT,
        &db,
        (1, 1),
        1,
        t0_ns,
        "one request, both streams",
    )
    .await;

    // ---- logs, state two: a restart, then B written again ------------
    // The writer holds its written-stream set in memory, so only a
    // restart makes it consider `B` again. Under a 64-bit fingerprint
    // that second write landed on the same `log_streams` key as `A` —
    // the table replaces on `fingerprint` alone — and `A`'s selector
    // began answering with `B`'s label set.
    //
    // **The second push lands outside the query window**, six hours past
    // `T0` and inside the same month, so the stream row is written again
    // while the window still holds one sample per stream. That is the
    // state the plan names, and the answers below are the plan's: two
    // streams, one line each, and `sum by()` value one each.
    drop(guard);
    let guard = spawn_ready(ANSWERS_PORT, &db);
    let outside_window_ns = t0_ns + 6 * 3_600_000_000_000;
    let res = http_request(
        ANSWERS_PORT,
        "POST",
        "/loki/api/v1/push",
        Some("application/x-protobuf"),
        &logs_push_b_again(outside_window_ns),
    )
    .expect("second push reachable");
    assert_eq!(res.status, 204, "second push status (body: {})", res.body);

    assert_logs_answers(
        ANSWERS_PORT,
        &db,
        (1, 2),
        1,
        t0_ns,
        "restarted, B outside the window",
    )
    .await;

    // ---- logs, state three: B written again INSIDE the window --------
    // The same second push the plan describes, run to its own
    // consequence. No restart: the stream row is already there and this
    // state is about the sample, which a plain `MergeTree` stores twice.
    // `A`'s line count must not move while `B`'s doubles.
    //
    // Issue #494 does not change this, and the reason is worth stating
    // rather than leaving to be rediscovered: the suppression added there
    // matches a WHOLE push by its content, and this third push is not a
    // repeat of either earlier one. The first body carries streams A and B
    // together; this one carries B alone. Two different pushes, so two
    // different identities, so nothing is suppressed and the second row is
    // still written.
    let res = http_request(
        ANSWERS_PORT,
        "POST",
        "/loki/api/v1/push",
        Some("application/x-protobuf"),
        &logs_push_b_again(t0_ns),
    )
    .expect("third push reachable");
    assert_eq!(res.status, 204, "third push status (body: {})", res.body);

    assert_logs_answers(
        ANSWERS_PORT,
        &db,
        (1, 3),
        2,
        t0_ns,
        "B written inside the window",
    )
    .await;
    let _guard = guard;

    // ---- metrics ----------------------------------------------------
    // A at T0, B at T0+15s: at T0+1s only A has a sample and both a broken
    // and a repaired build answer 11, so nothing is asserted before the
    // merge is visible.
    let res = http_request(
        ANSWERS_PORT,
        "POST",
        "/api/v1/write",
        Some("application/x-protobuf"),
        &remote_write_body(&[(METRICS_A, 11.0, t0_ms), (METRICS_B, 22.0, t0_ms + 15_000)]),
    )
    .expect("remote write reachable");
    assert_eq!(res.status, 204, "remote write status (body: {})", res.body);

    let at = t0_ns + 16_000_000_000;

    // The label cache resolves a selector to fingerprints, so the series
    // must be resident before the answer means anything. The TTL above is
    // 1s; poll rather than sleep a fixed amount.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut resident = false;
    while Instant::now() < deadline {
        if instant_series(ANSWERS_PORT, "count(pulsus_probe)", at)
            .first()
            .is_some_and(|(_, v)| v == "2")
        {
            resident = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(
        resident,
        "`count(pulsus_probe)` never reached 2 within 60s; before the widening it answered 1, \
         because both series shared one identity"
    );

    // **The sharpest single statement of the defect:** the selector names
    // one series and the value used to come from the other.
    for (service, want) in [(METRICS_A, "11"), (METRICS_B, "22")] {
        let got = instant_series(
            ANSWERS_PORT,
            &format!(r#"pulsus_probe{{service_name="{service}"}}"#),
            at,
        );
        assert_eq!(
            got.len(),
            1,
            "pulsus_probe{{service_name=\"{service}\"}} answered {} series, not one: {got:?}",
            got.len()
        );
        assert_eq!(
            got[0].0.get("service_name").map(String::as_str),
            Some(service),
            "the answer carries the other series' labels: {got:?}"
        );
        assert_eq!(
            got[0].1, want,
            "pulsus_probe{{service_name=\"{service}\"}} answered {} rather than {want}",
            got[0].1
        );
    }

    // The raw range answer, before any aggregation reduces it: two
    // series, one point each. Today one series carries both points, so
    // the range selector alone shows the merge.
    let raw = range_series(ANSWERS_PORT, "pulsus_probe[2m]", at);
    let want_raw: Vec<Series<Vec<(i64, String)>>> =
        [(METRICS_B, t0_ms + 15_000, "22"), (METRICS_A, t0_ms, "11")]
            .into_iter()
            .map(|(service, ts_ms, v)| {
                (
                    BTreeMap::from([
                        ("__name__".to_string(), "pulsus_probe".to_string()),
                        ("service_name".to_string(), service.to_string()),
                    ]),
                    vec![(ts_ms, v.to_string())],
                )
            })
            .collect();
    assert_eq!(
        raw, want_raw,
        "`pulsus_probe[2m]` must answer two series with one point each; before the widening one \
         series carried both points and the other was absent"
    );

    // `min`/`max` over the range: one label set used to carry both 11 and
    // 22, and no series in the fixture has both.
    for (op, a, b) in [("max_over_time", "11", "22"), ("min_over_time", "11", "22")] {
        let got = instant_series(ANSWERS_PORT, &format!("{op}(pulsus_probe[2m])"), at);
        let by_service: BTreeMap<String, String> = got
            .iter()
            .map(|(labels, v)| {
                (
                    labels.get("service_name").cloned().unwrap_or_default(),
                    v.clone(),
                )
            })
            .collect();
        assert_eq!(
            by_service.get(METRICS_A).map(String::as_str),
            Some(a),
            "{op}: {METRICS_A} answered {by_service:?}"
        );
        assert_eq!(
            by_service.get(METRICS_B).map(String::as_str),
            Some(b),
            "{op}: {METRICS_B} answered {by_service:?}"
        );
    }

    // `/api/v1/series` with BOTH bounds: with the start omitted and an end
    // past the fixture it returns `[]`, because the default window is one
    // hour anchored on `now`.
    let start_s = t0_ms / 1_000;
    let end_s = start_s + 16;
    let path = format!(
        "/api/v1/series?match[]={}&start={start_s}&end={end_s}",
        urlencode("pulsus_probe")
    );
    let res = http_get(ANSWERS_PORT, &path);
    assert_eq!(res.status, 200, "series status (body: {})", res.body);
    let json: serde_json::Value = serde_json::from_str(&res.body).expect("json");
    let no_series = Vec::new();
    let mut listed: Vec<BTreeMap<String, String>> = json["data"]
        .as_array()
        .unwrap_or(&no_series)
        .iter()
        .map(|s| {
            s.as_object()
                .expect("a series is an object")
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                .collect()
        })
        .collect();
    listed.sort();
    let want_listed: Vec<BTreeMap<String, String>> = [METRICS_B, METRICS_A]
        .into_iter()
        .map(|service| {
            BTreeMap::from([
                ("__name__".to_string(), "pulsus_probe".to_string()),
                ("service_name".to_string(), service.to_string()),
            ])
        })
        .collect();
    assert_eq!(
        listed, want_listed,
        "/api/v1/series must list both colliding label sets: {}",
        res.body
    );

    drop_db(&db).await;
}

/// **The accept/reject surface does not move.** No route takes a
/// fingerprint as a client input, so widening it cannot change what is
/// accepted; this pins the two cases the issue names, against the same
/// running server.
#[tokio::test(flavor = "multi_thread")]
async fn the_accept_reject_surface_is_unchanged() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test");
        return;
    }
    let db = pulsus_testkit::test_db("pulsus_server_it_fp_surface");
    drop_db(&db).await;
    let port = SURFACE_PORT;
    let _guard = spawn_ready(port, &db);

    // The same pinned instant, for the same reason: nothing here depends
    // on the wall clock, and a suite that reads it in one test and not the
    // other invites the next reader to assume one of them must need it.
    let at_s = T0_NS / 1_000_000_000;
    // A malformed selector is still a 400.
    let res = http_get(
        port,
        &format!(
            "/api/v1/query?query={}&time={at_s}",
            urlencode("pulsus_probe{service_name=")
        ),
    );
    assert_eq!(
        res.status, 400,
        "a malformed selector is a 400: {}",
        res.body
    );

    // An unknown metric name is still a 200 with an empty result.
    let res = http_get(
        port,
        &format!(
            "/api/v1/query?query={}&time={at_s}",
            urlencode("pulsus_absent_metric")
        ),
    );
    assert_eq!(res.status, 200, "an unknown name is a 200: {}", res.body);
    let json: serde_json::Value = serde_json::from_str(&res.body).expect("json");
    assert_eq!(
        json["data"]["result"].as_array().map(Vec::len),
        Some(0),
        "an unknown name answers an empty result: {}",
        res.body
    );

    drop_db(&db).await;
}
