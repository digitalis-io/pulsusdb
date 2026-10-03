//! Issue #583 (R9): the window rule at the boundary, over HTTP against a
//! spawned `pulsusdb` and a live ClickHouse.
//!
//! One rule — `start <= ts < end` — on every **request** window. Five
//! cases pin where that is visible to a client:
//!
//! * `T-B1` a span at exactly `start` is returned;
//! * `T-B2` a span at exactly `end` is not;
//! * `T-B3` a search ending at the next UTC midnight reads one day's
//!   partitions, not two, and returns the trace whose newest span is the
//!   window's first nanosecond;
//! * `T-B6` **guard** — the service-graph window is already `[start,
//!   end)` and must stay so;
//! * `T-B8`/`T-B8t` **guards** — `compare()`'s `start`/`end` arguments
//!   are NOT a request window. They are operands of the query language,
//!   the reference defines them as `(start, end]`, and they do not move.
//!
//! ## The fixture is derived from the clock, not written as literals
//!
//! `trace_spans` carries a delete TTL at `retention_days` (default 7,
//! `crates/pulsus-config/src/model.rs:165`) with `ttl_only_drop_parts =
//! 1`. Fixed calendar timestamps fall outside that window as soon as this
//! file is a week old: the rows are dropped, every case returns empty,
//! and the two cases whose expected answer is "nothing" would pass over an
//! empty table — a green run that tested nothing. So the fixture is
//! anchored on **yesterday's** UTC day start, which keeps every span and
//! every request window in the past, less than two days old, and six days
//! inside the default retention; and it makes this morning's midnight an
//! instant that has already passed, which `T-B3` needs.
//!
//! Both cases whose expected answer is empty carry their own non-vacuity
//! request, so an empty table cannot satisfy them.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`. Run locally:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-server --test traces_api_v2_live
//! ```
//!
//! Fixed loopback ports are declared inline per test (`let port = 31_2NN;`);
//! `live_port_uniqueness.rs` checks no two suites declare the same one.

#[path = "support/live_db.rs"]
mod live_db;

use live_db::ScopedDb;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use futures::StreamExt;
use prost::Message;
use pulsus_clickhouse::{ChClient, Idempotency, QuerySettings, Row};

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, EntityRef, InstrumentationScope, KeyValue, KeyValueList,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::span::{Event, Link, SpanKind};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status, TracesData};

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping
/// when the gate is absent in a live CI job, so a lost `env:` block reddens
/// the build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

// ---------------------------------------------------------------------
// Bare-TcpStream HTTP helper (the traces_search_live.rs idiom).
// ---------------------------------------------------------------------

struct RawResponse {
    status: u16,
    body: Vec<u8>,
    /// Lower-cased response header names and values (issue #586): the two
    /// trace routes' refusal containers are told apart by the content type
    /// and by whether `X-Content-Type-Options` is set, neither of which is
    /// visible to a reader of the body.
    headers: HashMap<String, String>,
}

impl RawResponse {
    fn json(&self, ctx: &str) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| {
            panic!(
                "{ctx}: invalid JSON body: {e}\nbody: {:?}",
                String::from_utf8_lossy(&self.body)
            )
        })
    }
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

fn request(
    port: u16,
    method: &str,
    path: &str,
    body: Option<(&str, &[u8])>,
    extra: &[(&str, &str)],
) -> Option<RawResponse> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(30))).ok();

    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    for (k, v) in extra {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    let body_bytes = match body {
        Some((content_type, bytes)) => {
            head.push_str(&format!("Content-Type: {content_type}\r\n"));
            bytes
        }
        None => &[],
    };
    head.push_str(&format!("Content-Length: {}\r\n\r\n", body_bytes.len()));

    stream.write_all(head.as_bytes()).ok()?;
    stream.write_all(body_bytes).ok()?;

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).ok()?;

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
            let (k, v) = line.split_once(':')?;
            Some((k.trim().to_ascii_lowercase(), v.trim().to_string()))
        })
        .collect();

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
        body,
        headers,
    })
}

fn get(port: u16, path: &str, ctx: &str) -> RawResponse {
    let res = request(port, "GET", path, None, &[])
        .unwrap_or_else(|| panic!("{ctx}: request must be reachable (transport failure)"));
    assert_eq!(
        res.status,
        200,
        "{ctx}: must succeed, body {:?}",
        String::from_utf8_lossy(&res.body)
    );
    res
}

// ---------------------------------------------------------------------
// Process lifecycle.
// ---------------------------------------------------------------------

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_ready(port: u16, db: &ScopedDb) -> ChildGuard {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_pulsusdb"));
    cmd.env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env("CLICKHOUSE_SERVER", live_db::ch_host())
        .env("CLICKHOUSE_HTTP_PORT", live_db::ch_http_port().to_string())
        .env("CLICKHOUSE_DB", db.name());
    let guard = ChildGuard(cmd.spawn().expect("spawn pulsusdb"));

    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if request(port, "GET", "/ready", None, &[]).is_some_and(|r| r.status == 200) {
            return guard;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("/ready never reached 200 within 60s");
}

// ---------------------------------------------------------------------
// Fixture B (functional-requirements.md §8.2), anchored on yesterday.
// ---------------------------------------------------------------------

const NS_PER_DAY: i64 = 86_400_000_000_000;

/// The five spans' offsets from the anchoring UTC day start — §8.2's own
/// arithmetic, with only the anchor moved off a calendar date.
const B1_OFF: i64 = 59_646_486_853_636;
const B2_OFF: i64 = 86_399_999_999_999;
const B3_OFF: i64 = 30_000_000_000;
const B4_OFF: i64 = 3_600_000_000_000;
/// The `gw` client span sits six nanoseconds before b1, so the window
/// `[b1 - 1, b1)` holds no span of the trace at all once b1 is out.
const G0_LEAD_NS: i64 = 6;

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64")
}

/// Yesterday's UTC day start.
fn day_start_ns() -> i64 {
    (now_ns().div_euclid(NS_PER_DAY) - 1) * NS_PER_DAY
}

/// `toDate('YYYY-MM-DD')` for the UTC day a nanosecond falls in, computed
/// with `chrono` rather than with the builder under test.
fn date_literal(ns: i64) -> String {
    let secs = ns.div_euclid(1_000_000_000);
    let day = chrono::DateTime::from_timestamp(secs, 0).expect("a representable instant");
    format!("toDate('{}')", day.format("%Y-%m-%d"))
}

const TRACE_ID: [u8; 16] = [0xb0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01];

fn sid(n: u8) -> [u8; 8] {
    let mut id = [0u8; 8];
    id[7] = n;
    id
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn kv_str(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
        key_strindex: 0,
    }
}

fn span(
    span_id: [u8; 8],
    parent_id: Option<[u8; 8]>,
    name: &str,
    kind: SpanKind,
    start_ns: i64,
    attrs: Vec<KeyValue>,
) -> Span {
    Span {
        trace_id: TRACE_ID.to_vec(),
        span_id: span_id.to_vec(),
        parent_span_id: parent_id.map(|p| p.to_vec()).unwrap_or_default(),
        name: name.to_string(),
        kind: kind as i32,
        start_time_unix_nano: u64::try_from(start_ns).expect("a post-epoch fixture"),
        end_time_unix_nano: u64::try_from(start_ns + 1_000_000).expect("a post-epoch fixture"),
        attributes: attrs,
        ..Default::default()
    }
}

/// One `POST /v1/traces` for one service (sync — a `200` means the rows
/// are flushed and read-visible).
fn ingest(port: u16, service: &str, spans: Vec<Span>, ctx: &str) {
    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv_str("service.name", service)],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "live-scope".to_string(),
                    version: String::new(),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    let res = request(
        port,
        "POST",
        "/v1/traces",
        Some(("application/x-protobuf", &req.encode_to_vec())),
        &[],
    )
    .unwrap_or_else(|| panic!("{ctx}: ingest must be reachable"));
    assert_eq!(
        res.status,
        200,
        "{ctx}: sync ingest must succeed, body {:?}",
        String::from_utf8_lossy(&res.body)
    );
}

/// Seeds fixture B: g0 under `gw`, b1-b4 under `checkout`, all in one
/// trace. Returns the day start every offset is measured from.
fn seed_fixture_b(port: u16) -> i64 {
    let day = day_start_ns();
    ingest(
        port,
        "gw",
        vec![span(
            sid(1),
            None,
            "gw.call",
            SpanKind::Client,
            day + B1_OFF - G0_LEAD_NS,
            vec![],
        )],
        "seed g0",
    );
    ingest(
        port,
        "checkout",
        vec![
            span(
                sid(2),
                Some(sid(1)),
                "op.b1",
                SpanKind::Server,
                day + B1_OFF,
                vec![],
            ),
            span(
                sid(3),
                None,
                "op.b2",
                SpanKind::Internal,
                day + B2_OFF,
                vec![],
            ),
            span(
                sid(4),
                None,
                "op.b3",
                SpanKind::Internal,
                day + B3_OFF,
                vec![kv_str("http.route", "/only-before"), kv_str("tenant", "t1")],
            ),
            span(
                sid(5),
                None,
                "op.b4",
                SpanKind::Internal,
                day + B4_OFF,
                vec![kv_str("http.route", "/inside"), kv_str("tenant", "t1")],
            ),
        ],
        "seed b1-b4",
    );
    day
}

// ---------------------------------------------------------------------
// Search-side helpers.
// ---------------------------------------------------------------------

/// Minimal percent-encoding for query-string values.
fn enc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
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

/// The trace ids an empty-selector search over `[start_ns, end_ns)`
/// returns. Nanosecond bounds: the trace surface reads an integer of
/// magnitude >= 1e12 as nanoseconds (`traces_api/params.rs`).
fn search_ids(port: u16, start_ns: i64, end_ns: i64, ctx: &str) -> Vec<String> {
    let path = format!("/api/traces/v1/search?q=%7B%7D&start={start_ns}&end={end_ns}&limit=20");
    let res = get(port, &path, ctx);
    let json = res.json(ctx);
    json["traces"]
        .as_array()
        .unwrap_or_else(|| panic!("{ctx}: a traces array, got {json}"))
        .iter()
        .map(|t| {
            t["traceID"]
                .as_str()
                .unwrap_or_else(|| panic!("{ctx}: a traceID string, got {t}"))
                .to_string()
        })
        .collect()
}

/// The phase-1 generator statement a search ran, read from the same
/// request sent with `X-Pulsus-Explain: 1`: the one stage whose SQL
/// carries `AS bound_ts`.
fn generator_sql(port: u16, start_ns: i64, end_ns: i64, ctx: &str) -> String {
    let path = format!("/api/traces/v1/search?q=%7B%7D&start={start_ns}&end={end_ns}&limit=20");
    let raw = request(port, "GET", &path, None, &[("X-Pulsus-Explain", "1")])
        .unwrap_or_else(|| panic!("{ctx}: explain request must be reachable"));
    let json: serde_json::Value = serde_json::from_slice(&raw.body)
        .unwrap_or_else(|e| panic!("{ctx}: explain body is not JSON: {e}"));
    let sqls: Vec<String> = json["explain"]["stages"]
        .as_array()
        .map(|stages| {
            stages
                .iter()
                .filter_map(|s| s["sql"].as_str())
                .filter(|sql| sql.contains("AS bound_ts"))
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    match sqls.as_slice() {
        [one] => one.clone(),
        other => panic!(
            "{ctx}: expected one generator stage, found {}: {json}",
            other.len()
        ),
    }
}

// ---------------------------------------------------------------------
// T-B1 / T-B2 — the two ends of the search window.
// ---------------------------------------------------------------------

/// `T-B1`: a span at exactly `start` is matched, so the trace comes back.
///
/// The window `[b1, b1 + 1)` holds exactly one nanosecond and b1 sits on
/// it. On the unchanged tree the search window renders `ts > start`, so b1
/// is dropped and the answer is empty — and the recency candidate bound
/// renders `ts_max > start`, which drops the trace before phase 2 as well.
#[tokio::test(flavor = "multi_thread")]
async fn a_span_at_exactly_the_window_start_is_returned() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_253;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_traces_api_v2_it_start")).await;
    let _server = spawn_ready(port, &db);
    let day = seed_fixture_b(port);
    let b1 = day + B1_OFF;

    assert_eq!(
        search_ids(port, b1, b1 + 1, "T-B1"),
        vec![hex(&TRACE_ID)],
        "T-B1: a span at exactly start is inside [start, end)"
    );
}

/// `T-B2`: a span at exactly `end` is not matched, so the trace does not
/// come back.
///
/// The window `[b1 - 1, b1)` holds no span of the trace once b1 is
/// excluded: g0 is six nanoseconds earlier and the other three are hours
/// away. On the unchanged tree the window renders `ts <= end`, so b1 is
/// returned.
///
/// **Non-vacuity:** the same seeded fixture answers `T-B1`'s request with
/// the trace, so an empty table cannot satisfy this case.
#[tokio::test(flavor = "multi_thread")]
async fn a_span_at_exactly_the_window_end_is_not_returned() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_254;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_traces_api_v2_it_end")).await;
    let _server = spawn_ready(port, &db);
    let day = seed_fixture_b(port);
    let b1 = day + B1_OFF;

    assert_eq!(
        search_ids(port, b1, b1 + 1, "T-B2 non-vacuity"),
        vec![hex(&TRACE_ID)],
        "T-B2: the fixture is there — [b1, b1 + 1) returns the trace"
    );
    assert_eq!(
        search_ids(port, b1 - 1, b1, "T-B2"),
        Vec::<String>::new(),
        "T-B2: a span at exactly end is outside [start, end)"
    );
}

// ---------------------------------------------------------------------
// T-B3 — the day bound comes from the last included nanosecond.
// ---------------------------------------------------------------------

/// `T-B3`: a search ending at the next UTC midnight reads one day's
/// partitions, not two, and still answers.
///
/// (a) b2 is the trace's newest span and the window opens exactly on it.
/// On the unchanged tree `ts_max > start` drops the trace from the
/// candidate set outright, and `ts > start` would drop b2 from phase 2
/// anyway. (b) the generator's day clause names one day; on the unchanged
/// tree it is rendered from `end_ns` and names the next day as well.
#[tokio::test(flavor = "multi_thread")]
async fn a_window_ending_at_midnight_reads_one_days_partitions() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_255;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_traces_api_v2_it_midnight")).await;
    let _server = spawn_ready(port, &db);
    let day = seed_fixture_b(port);
    let (b2, midnight) = (day + B2_OFF, day + NS_PER_DAY);

    assert_eq!(
        search_ids(port, b2, midnight, "T-B3(a)"),
        vec![hex(&TRACE_ID)],
        "T-B3(a): the trace whose newest span is exactly start is a candidate and an answer"
    );

    let sql = generator_sql(port, b2, midnight, "T-B3(b)");
    let one_day = date_literal(day);
    assert!(
        sql.contains(&format!("date >= {one_day} AND date <= {one_day}")),
        "T-B3(b): the day bound comes from the last included nanosecond, so a window ending \
         exactly at midnight names one day:\n{sql}"
    );
}

// ---------------------------------------------------------------------
// T-B6 — guard: the service-graph window stays half-open.
// ---------------------------------------------------------------------

/// `(client, server, connectionType)` triples the service graph reports.
fn graph_edges(port: u16, start_ns: i64, end_ns: i64, ctx: &str) -> Vec<(String, String, String)> {
    let path = format!("/api/traces/v1/service_graph?start={start_ns}&end={end_ns}");
    let res = get(port, &path, ctx);
    let json = res.json(ctx);
    json["edges"]
        .as_array()
        .unwrap_or_else(|| panic!("{ctx}: an edges array, got {json}"))
        .iter()
        .map(|e| {
            (
                e["client"].as_str().unwrap_or_default().to_string(),
                e["server"].as_str().unwrap_or_default().to_string(),
                e["connectionType"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect()
}

/// `T-B6` **guard**: the `gw → checkout` edge is absent when its server
/// half starts at exactly `end`. Passes today and must keep passing.
///
/// It discriminates: g0 is strictly inside the window under both
/// conventions, and b1 sits at exactly `end`, so `[start, end)` excludes
/// b1 and reports no edge while `(start, end]` would report one. An edge
/// is reported only when both halves are in-window (`docs/api.md` §4.5).
///
/// **Non-vacuity:** the same fixture, one nanosecond wider, reports the
/// edge.
#[tokio::test(flavor = "multi_thread")]
async fn the_service_graph_omits_an_edge_whose_server_half_sits_at_end() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_256;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_traces_api_v2_it_graph")).await;
    let _server = spawn_ready(port, &db);
    let day = seed_fixture_b(port);
    let b1 = day + B1_OFF;
    let edge = ("gw".to_string(), "checkout".to_string(), "rpc".to_string());

    assert!(
        graph_edges(port, b1 - G0_LEAD_NS - 1, b1 + 1, "T-B6 non-vacuity").contains(&edge),
        "T-B6: the fixture is there — with b1 inside the window the edge is reported"
    );
    assert!(
        !graph_edges(port, b1 - G0_LEAD_NS - 1, b1, "T-B6").contains(&edge),
        "T-B6: b1 sits at exactly end, so the server half is out of [start, end) and no edge \
         is reported"
    );
}

// ---------------------------------------------------------------------
// T-B8 / T-B8t — guards: compare()'s own window does not move.
// ---------------------------------------------------------------------

/// One string label of a response series, `None` when the series does not
/// carry it. A metrics label is an OTLP protojson `AnyValue`
/// (`{"stringValue": …}`).
fn series_label(series: &serde_json::Value, key: &str) -> Option<String> {
    series["labels"].as_array()?.iter().find_map(|l| {
        if l["key"].as_str()? == key {
            l["value"]["stringValue"].as_str().map(str::to_string)
        } else {
            None
        }
    })
}

/// The summed value of the `compare()` series carrying `__meta_type =
/// <meta>` and the label `<key> = <value>`, over the whole emitted grid.
/// `None` when the answer carries no such series. A sample's `value` is
/// omitted when zero (protojson default omission), which reads as 0.
fn compare_series_total(
    json: &serde_json::Value,
    meta: &str,
    key: &str,
    value: &str,
    ctx: &str,
) -> Option<f64> {
    json["series"]
        .as_array()
        .unwrap_or_else(|| panic!("{ctx}: a series array, got {json}"))
        .iter()
        .find(|s| {
            series_label(s, "__meta_type").as_deref() == Some(meta)
                && series_label(s, key).as_deref() == Some(value)
        })
        .map(|s| {
            s["samples"]
                .as_array()
                .map(|samples| {
                    samples
                        .iter()
                        .map(|p| p["value"].as_f64().unwrap_or(0.0))
                        .sum()
                })
                .unwrap_or(0.0)
        })
}

/// One `compare()` range query over the whole anchoring day, with the
/// selection window written out in nanoseconds.
fn compare_answer(
    port: u16,
    day: i64,
    sel_start: i64,
    sel_end: i64,
    ctx: &str,
) -> serde_json::Value {
    let q = format!(
        r#"{{span.tenant="t1"}} | compare({{span.http.route="/inside"}}, 10, {sel_start}, {sel_end})"#
    );
    let path = format!(
        "/api/traces/v1/metrics/query_range?q={}&start={}&end={}&step=3600s",
        enc(&q),
        day / 1_000_000_000,
        (day + NS_PER_DAY) / 1_000_000_000,
    );
    get(port, &path, ctx).json(ctx)
}

/// `T-B8` **guard**: `compare()`'s `start`/`end` arguments keep the
/// reference's `(start, end]`, at BOTH ends. Passes today and must keep
/// passing.
///
/// (a) b4 sits at exactly the `end` argument and counts in the
/// **selection**; (b) b4 sits at exactly the `start` argument and counts
/// in the **baseline**. A half-conversion — `>=` on the start with `<=`
/// left on the end — fails (b) while (a) stays green.
#[tokio::test(flavor = "multi_thread")]
async fn the_compare_selection_window_keeps_both_of_its_reference_ends() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_257;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_traces_api_v2_it_compare")).await;
    let _server = spawn_ready(port, &db);
    let day = seed_fixture_b(port);
    let (b3, b4) = (day + B3_OFF, day + B4_OFF);
    let route = "span.http.route";

    let a = compare_answer(port, day, b3, b4, "T-B8(a)");
    assert_eq!(
        compare_series_total(&a, "selection", route, "/inside", "T-B8(a)"),
        Some(1.0),
        "T-B8(a): b4 is at exactly the end argument, which the reference INCLUDES in the \
         selection\nbody: {a}"
    );
    assert_eq!(
        compare_series_total(&a, "baseline", route, "/inside", "T-B8(a)"),
        None,
        "T-B8(a): and it is therefore not in the baseline\nbody: {a}"
    );

    let b = compare_answer(port, day, b4, day + NS_PER_DAY, "T-B8(b)");
    assert_eq!(
        compare_series_total(&b, "baseline", route, "/inside", "T-B8(b)"),
        Some(1.0),
        "T-B8(b): b4 is at exactly the start argument, which the reference EXCLUDES from the \
         selection\nbody: {b}"
    );
    assert_eq!(
        compare_series_total(&b, "selection", route, "/inside", "T-B8(b)"),
        None,
        "T-B8(b): and it is therefore not in the selection\nbody: {b}"
    );
}

/// `T-B8t` **guard**: the selection window repartitions, it does not
/// filter, so for one attribute key `baseline_total + selection_total` is
/// the whole population whatever window is asked for. The outer filter
/// admits b3 and b4 only, so the sum is 2 for both of `T-B8`'s requests.
///
/// Neither half of `T-B8` can see a change that turned the window into a
/// filter; this one can. The two `*_total` denominators are per attribute
/// key and carry `key=nil` (`docs/api.md` §4.4), so the key is named.
#[tokio::test(flavor = "multi_thread")]
async fn the_compare_totals_cover_the_population_whatever_the_selection_window() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_258;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_traces_api_v2_it_totals")).await;
    let _server = spawn_ready(port, &db);
    let day = seed_fixture_b(port);
    let (b3, b4) = (day + B3_OFF, day + B4_OFF);
    let route = "span.http.route";

    for (label, sel_start, sel_end) in [("T-B8t(a)", b3, b4), ("T-B8t(b)", b4, day + NS_PER_DAY)] {
        let json = compare_answer(port, day, sel_start, sel_end, label);
        let baseline =
            compare_series_total(&json, "baseline_total", route, "nil", label).unwrap_or(0.0);
        let selection =
            compare_series_total(&json, "selection_total", route, "nil", label).unwrap_or(0.0);
        assert_eq!(
            baseline + selection,
            2.0,
            "{label}: the two totals cover the whole population the outer filter admits \
             (b3 and b4)\nbody: {json}"
        );
    }
}

// ---------------------------------------------------------------------
// Issue #586 — the push-suppression index over the trace push
// ---------------------------------------------------------------------
//
// The trace push joins issue #494's suppression index, and it is the one
// behaviour on the OLD two-table path this change alters: a push the index
// recognises stores nothing on either path, where today the old path stores
// it twice. So these cases read BOTH stores, each against its own expected
// value — no case here compares one with the other.

/// `pulsusdb` with extra environment, for the cases that need the compat
/// endpoints or a short suppression window.
fn spawn_ready_with_env(port: u16, db: &ScopedDb, extra: &[(&str, &str)]) -> ChildGuard {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_pulsusdb"));
    cmd.env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env("CLICKHOUSE_SERVER", live_db::ch_host())
        .env("CLICKHOUSE_HTTP_PORT", live_db::ch_http_port().to_string())
        .env("CLICKHOUSE_DB", db.name());
    for (name, value) in extra {
        cmd.env(name, value);
    }
    let guard = ChildGuard(cmd.spawn().expect("spawn pulsusdb"));
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if request(port, "GET", "/ready", None, &[]).is_some_and(|r| r.status == 200) {
            return guard;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("/ready never reached 200 within 60s");
}

/// One trace push of `spans` under `service`, with the request headers
/// given, answered raw so a case can read its status and its container.
fn push_otlp(port: u16, service: &str, spans: Vec<Span>, headers: &[(&str, &str)]) -> RawResponse {
    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv_str("service.name", service)],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    request(
        port,
        "POST",
        "/v1/traces",
        Some(("application/x-protobuf", &req.encode_to_vec())),
        headers,
    )
    .expect("the OTLP trace route must be reachable")
}

/// One Zipkin v2 JSON push of `names`, one span per name.
fn push_zipkin(port: u16, names: &[&str], headers: &[(&str, &str)]) -> RawResponse {
    let base_us = day_start_ns() / 1_000 + 3_600_000_000;
    let spans: Vec<String> = names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            format!(
                r#"{{"traceId":"aa00000000000000000000000000000{}","id":"{:016x}","name":"{name}","timestamp":{},"duration":1000,"localEndpoint":{{"serviceName":"checkout"}}}}"#,
                1,
                0x20 + i,
                base_us + i as i64
            )
        })
        .collect();
    let body = format!("[{}]", spans.join(","));
    request(
        port,
        "POST",
        "/api/v2/spans",
        Some(("application/json", body.as_bytes())),
        headers,
    )
    .expect("the Zipkin trace route must be reachable")
}

/// `SELECT toString(<expr>) FROM <table>` against the test database.
async fn scalar_of(db: &str, sql: &str) -> String {
    let client = pulsus_clickhouse::ChClient::new(live_db::conn_config(db))
        .await
        .expect("connect to the test database");
    let rows = client
        .query_strings(sql, &pulsus_clickhouse::QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    rows.into_iter().next().unwrap_or_default()
}

async fn count_of(db: &str, table_sql: &str) -> u64 {
    scalar_of(
        db,
        &format!("SELECT toString(count()) AS s FROM {table_sql}"),
    )
    .await
    .parse()
    .expect("a count")
}

/// Waits until `count_of` answers `want`, so a case can read the landing
/// table after a `200` — the landing block carries no waiter, because the
/// route's answer is the old path's.
async fn settle_count(db: &str, table_sql: &str, want: u64, ctx: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let got = count_of(db, table_sql).await;
        if got == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{ctx}: {table_sql} answered {got}, not {want}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// `(code, message)` out of a `google.rpc.Status` body.
fn decode_status(body: &[u8]) -> (i32, String) {
    #[derive(Clone, PartialEq, ::prost::Message)]
    struct Status {
        #[prost(int32, tag = "1")]
        code: i32,
        #[prost(string, tag = "2")]
        message: String,
    }
    let decoded = Status::decode(body).expect("a google.rpc.Status body");
    (decoded.code, decoded.message)
}

fn fixture_spans(names: &[&str]) -> Vec<Span> {
    let day = day_start_ns();
    names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            span(
                sid(0x30 + i as u8),
                None,
                name,
                SpanKind::Server,
                day + 3_600_000_000_000 + i as i64,
                vec![],
            )
        })
        .collect()
}

/// **A suppressed trace push sends nothing**, on **both** write paths.
///
/// This is the one behaviour on the old path that this change alters, so the
/// case has to see both: a retry that skipped only the landing branch while
/// `trace_spans` stored twice would pass every other assertion.
#[tokio::test(flavor = "multi_thread")]
async fn a_suppressed_trace_push_sends_nothing() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_259;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_traces_api_v2_it_suppress")).await;
    let _server = spawn_ready(port, &db);

    let spans = fixture_spans(&["s1", "s2", "s3"]);
    let first = push_otlp(port, "checkout", spans.clone(), &[]);
    assert_eq!(first.status, 200, "the first push is stored");
    settle_count(db.name(), "trace_landing WHERE row_kind = 0", 3, "first").await;
    let landing_rows = count_of(db.name(), "trace_landing").await;

    let second = push_otlp(port, "checkout", spans, &[]);
    assert_eq!(
        second.status, 200,
        "the suppressed push is answered the original push's outcome"
    );

    assert_eq!(
        count_of(db.name(), "trace_landing").await,
        landing_rows,
        "the landing table holds the SINGLE push's row count"
    );
    assert_eq!(
        count_of(db.name(), "spans FINAL").await,
        3,
        "and the target collapses to the body's span count"
    );
    assert_eq!(
        count_of(db.name(), "trace_spans").await,
        3,
        "the old path's own table holds the single push's span count, not \
         twice it: the suppression index is upstream of both paths"
    );
}

/// A retry **outside** the suppression window stores a second copy in the
/// landing table, and the target still collapses to the body's span count.
#[tokio::test(flavor = "multi_thread")]
async fn a_retry_outside_the_window_stores_one_copy() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_260;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_traces_api_v2_it_window")).await;
    let _server = spawn_ready_with_env(port, &db, &[("PULSUS_INGEST_DEDUP_WINDOW", "1s")]);

    let spans = fixture_spans(&["w1", "w2"]);
    assert_eq!(push_otlp(port, "checkout", spans.clone(), &[]).status, 200);
    settle_count(db.name(), "trace_landing WHERE row_kind = 0", 2, "first").await;

    tokio::time::sleep(Duration::from_millis(1_600)).await;
    assert_eq!(push_otlp(port, "checkout", spans, &[]).status, 200);
    settle_count(
        db.name(),
        "trace_landing WHERE row_kind = 0",
        4,
        "outside the window",
    )
    .await;

    assert_eq!(
        count_of(db.name(), "spans FINAL").await,
        2,
        "the target collapses the second copy on its own key"
    );
}

/// **Two distinct `Idempotency-Key`s store both pushes**, on each trace
/// transport.
///
/// (b) is the discriminator, and it does not turn on block deduplication:
/// each sealed landing block mints its own token.
#[tokio::test(flavor = "multi_thread")]
async fn distinct_idempotency_keys_store_both_trace_pushes() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let otlp_port = 31_261;
    let zipkin_port = 31_262;

    {
        let db =
            ScopedDb::fresh(pulsus_testkit::test_db("pulsus_traces_api_v2_it_keys_otlp")).await;
        let _server = spawn_ready(otlp_port, &db);
        let spans = fixture_spans(&["k1", "k2"]);
        for key in ["k-1", "k-2"] {
            let res = push_otlp(
                otlp_port,
                "checkout",
                spans.clone(),
                &[("idempotency-key", key)],
            );
            assert_eq!(res.status, 200, "/v1/traces: both pushes succeed");
        }
        settle_count(
            db.name(),
            "trace_landing WHERE row_kind = 0",
            4,
            "/v1/traces",
        )
        .await;
        assert_eq!(
            count_of(db.name(), "spans FINAL").await,
            2,
            "/v1/traces: the target's own collapse, not a discriminator"
        );
        let old = count_of(db.name(), "trace_spans").await;
        eprintln!("/v1/traces: trace_spans held {old} rows (recorded)");
    }

    {
        let db = ScopedDb::fresh(pulsus_testkit::test_db(
            "pulsus_traces_api_v2_it_keys_zipkin",
        ))
        .await;
        let _server =
            spawn_ready_with_env(zipkin_port, &db, &[("PULSUS_COMPAT_ENDPOINTS", "true")]);
        for key in ["k-1", "k-2"] {
            let res = push_zipkin(zipkin_port, &["z1", "z2"], &[("idempotency-key", key)]);
            assert_eq!(res.status, 202, "/api/v2/spans: both pushes succeed");
        }
        settle_count(
            db.name(),
            "trace_landing WHERE row_kind = 0",
            4,
            "/api/v2/spans",
        )
        .await;
        assert_eq!(count_of(db.name(), "spans FINAL").await, 2);
        let old = count_of(db.name(), "trace_spans").await;
        eprintln!("/api/v2/spans: trace_spans held {old} rows (recorded)");
    }
}

/// **One key carrying two different contents is refused**, in each route's
/// own container, and the refusal stores nothing.
#[tokio::test(flavor = "multi_thread")]
async fn one_key_with_two_contents_is_refused_on_each_trace_transport() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let otlp_port = 31_263;
    let zipkin_port = 31_264;
    let key = [("idempotency-key", "k-9")];

    {
        let db = ScopedDb::fresh(pulsus_testkit::test_db(
            "pulsus_traces_api_v2_it_reuse_otlp",
        ))
        .await;
        let _server = spawn_ready(otlp_port, &db);
        assert_eq!(
            push_otlp(otlp_port, "checkout", fixture_spans(&["r1", "r2"]), &key).status,
            200
        );
        settle_count(
            db.name(),
            "trace_landing WHERE row_kind = 0",
            2,
            "/v1/traces first",
        )
        .await;
        let after_first = count_of(db.name(), "trace_landing").await;

        let res = push_otlp(
            otlp_port,
            "checkout",
            fixture_spans(&["r1", "changed"]),
            &key,
        );
        assert_eq!(
            res.status, 400,
            "/v1/traces: a reused key is a client error"
        );
        assert_eq!(
            res.headers.get("content-type").map(String::as_str),
            Some("application/x-protobuf")
        );
        let (code, message) = decode_status(&res.body);
        assert_eq!(code, 3);
        assert_eq!(message, pulsus_write::KEY_REUSED_MESSAGE);
        assert_eq!(
            count_of(db.name(), "trace_landing").await,
            after_first,
            "/v1/traces: the refusal stored nothing"
        );
    }

    {
        let db = ScopedDb::fresh(pulsus_testkit::test_db(
            "pulsus_traces_api_v2_it_reuse_zipkin",
        ))
        .await;
        let _server =
            spawn_ready_with_env(zipkin_port, &db, &[("PULSUS_COMPAT_ENDPOINTS", "true")]);
        assert_eq!(push_zipkin(zipkin_port, &["r1", "r2"], &key).status, 202);
        settle_count(
            db.name(),
            "trace_landing WHERE row_kind = 0",
            2,
            "/api/v2/spans first",
        )
        .await;
        let after_first = count_of(db.name(), "trace_landing").await;

        let res = push_zipkin(zipkin_port, &["r1", "changed"], &key);
        assert_eq!(res.status, 400, "/api/v2/spans: a reused key is 400");
        assert_eq!(
            res.headers.get("content-type").map(String::as_str),
            Some("text/plain; charset=utf-8")
        );
        assert_eq!(
            res.headers.get("x-content-type-options"),
            None,
            "the post-admission container sets no sniff header"
        );
        assert_eq!(
            String::from_utf8_lossy(&res.body),
            pulsus_write::KEY_REUSED_MESSAGE,
            "the message is the whole body"
        );
        assert_ne!(res.body.last(), Some(&b'\n'), "and carries no terminator");
        assert_eq!(
            count_of(db.name(), "trace_landing").await,
            after_first,
            "/api/v2/spans: the refusal stored nothing"
        );
    }
}

// =====================================================================
// Issue #587 — the trace fetch on the span, per-trace and resource
// tables, and the provenance helpers every case here shares.
// =====================================================================

/// The fixed instant the fetch cases are anchored on, and the UTC day and
/// five-minute bucket it falls in. Engine-derived:
///
/// ```text
/// SELECT toDate(fromUnixTimestamp64Nano(1700000000000000000), 'UTC'),
///        intDiv(1700000000000000000, 300000000000)
/// -->  2023-11-14   5666666
/// ```
const ANCHOR_NS: i64 = 1_700_000_000_000_000_000;
/// See [`ANCHOR_NS`].
const ANCHOR_BUCKET: i64 = 5_666_666;
/// The five-minute bucket width the span table's leading sort key divides
/// by.
const BUCKET_NS: i64 = 300_000_000_000;

/// A fixed past instant and a delete TTL are not compatible without this.
///
/// **Every fetch case below is anchored on a CALENDAR instant** rather
/// than on the clock, because the derived values each case asserts — the
/// UTC days a push's rows file under, the buckets its spans occupy — were
/// computed from those literals and pasted. The three fetch tables carry a
/// delete TTL at the configured retention with `ttl_only_drop_parts = 1`,
/// and a part whose rows are all already expired is dropped by a
/// **background** operation, measured to happen within two seconds of the
/// insert. The case would then assert against an empty table.
///
/// `SYSTEM STOP MERGES` is what holds them, because a TTL part drop is a
/// merge. **Table-scoped, which is the whole of why no restart is needed**:
/// the scoped form resolves a storage object, so the stop is held against
/// the table and the suite's `DROP DATABASE` destroys the holder. The
/// argument-less form is server-wide and no drop undoes it.
///
/// It also gives the one case that asserts a PHYSICAL row count a stable
/// state to assert — but that is a second reason, and this one applies to
/// every case.
async fn hold_the_fetch_tables(admin: &ChClient, db: &str) {
    for table in ["spans", "traces", "resources", "trace_spans"] {
        ch_exec(admin, &format!("SYSTEM STOP MERGES {db}.{table}")).await;
    }
}

/// A ClickHouse connection on the built-in `default` database, for the
/// `system` reads and the scoped delete the provenance markers need.
async fn ch_admin() -> ChClient {
    ChClient::new(live_db::conn_config("default"))
        .await
        .expect("connect the admin ClickHouse client")
}

async fn ch_exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .unwrap_or_else(|e| panic!("statement failed: {e}\nSQL:\n{sql}"));
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, Copy)]
struct ChCount {
    n: u64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ChText {
    s: String,
}

async fn ch_count(client: &ChClient, sql: &str) -> u64 {
    let mut stream = client
        .query_stream::<ChCount>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\nSQL:\n{sql}"));
    stream
        .next()
        .await
        .unwrap_or_else(|| panic!("no row for:\n{sql}"))
        .unwrap_or_else(|e| panic!("decode row: {e}\nSQL:\n{sql}"))
        .n
}

/// §5.0b's **P1**: the trace's rows are **not in the old table**.
///
/// **Why every case that reads through a fetch route needs it.** A push
/// writes the old per-span payload column as well as the landing table,
/// and the old payload reader round-trips any fixture perfectly — so a
/// case that pushes through the ingest route and reads through a fetch
/// route is **green on the unchanged tree** unless something stops it.
/// This is that something: a scoped `ALTER … DELETE` on the trace's own
/// rows, polled to completion, so the case says *"this trace is not in the
/// old table"* rather than *"the old table is empty"*.
///
/// On its own this reddens every case whose assertion is on a non-empty
/// response, because the old point read then answers empty. It does
/// **not** cover a case whose expected answer IS empty, or one that
/// asserts the statement rather than the body — [`the_new_statements_ran`]
/// is what covers those two.
async fn not_in_the_old_table(admin: &ChClient, db: &str, hex: &str, expected_new_spans: u64) {
    ch_exec(
        admin,
        &format!("ALTER TABLE {db}.trace_spans DELETE WHERE trace_id = unhex('{hex}')"),
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let pending = ch_count(
            admin,
            &format!(
                "SELECT count() AS n FROM system.mutations \
                 WHERE database = '{db}' AND table = 'trace_spans' AND NOT is_done"
            ),
        )
        .await;
        if pending == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the scoped delete on the old table did not finish within 10s"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(
        ch_count(
            admin,
            &format!("SELECT count() AS n FROM {db}.trace_spans WHERE trace_id = unhex('{hex}')")
        )
        .await,
        0,
        "P1: the trace must be gone from the old table, or the old reader can answer"
    );
    assert_eq!(
        ch_count(
            admin,
            &format!("SELECT count() AS n FROM {db}.spans FINAL WHERE trace_id = unhex('{hex}')")
        )
        .await,
        expected_new_spans,
        "P1: and still present in the new one, or the case is about an empty table"
    );
}

/// §5.0b's **P2**: the answer came from **the new statements**.
///
/// The recorded text of `<prefix>-1` contains `FROM <db>.traces` — a token
/// the old point read cannot produce, since it reads the old span table and
/// never the per-trace one. `<prefix>` comes from the request's own
/// `X-Pulsus-Query-Id` response header, so the case READS the prefix
/// rather than recomputing it.
///
/// This is what covers the two kinds P1 cannot: a case whose expected
/// answer is EMPTY, where an empty old-path answer passes P1 silently, and
/// a case that asserts the statement rather than the body.
async fn the_new_statements_ran(admin: &ChClient, db: &str, prefix: &str) {
    let text = recorded_statement(admin, db, &format!("{prefix}-1"))
        .await
        .unwrap_or_else(|| panic!("P2: statement 1 must be recorded as {prefix}-1"));
    assert!(
        text.contains(&format!("FROM {db}.traces")),
        "P2: statement 1 must read the per-trace table, which the old point read cannot:\n{text}"
    );
}

/// The recorded SQL text of one statement id, after a flush and a bounded
/// wait — `query_log` is asynchronous and asserting on it without both is a
/// known flake here.
async fn recorded_statement(admin: &ChClient, db: &str, query_id: &str) -> Option<String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        ch_exec(admin, "SYSTEM FLUSH LOGS").await;
        let sql = format!(
            "SELECT query AS s FROM system.query_log \
             WHERE query_id = '{query_id}' AND type = 'QueryFinish' \
               AND current_database = '{db}' LIMIT 1"
        );
        let mut stream = admin
            .query_stream::<ChText>(&sql, &QuerySettings::new())
            .await
            .unwrap_or_else(|e| panic!("query_log read failed: {e}"));
        if let Some(row) = stream.next().await {
            return Some(row.expect("decode a query_log row").s);
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// The exact set of statement ids one request issued, as a `BTreeSet`.
///
/// A count of 2 where 1 is expected is then reported as the presence of
/// the **named** row rather than as an arithmetic surprise.
async fn statement_ids(admin: &ChClient, db: &str, prefix: &str) -> BTreeSet<String> {
    // The flush and the wait come from `recorded_statement`'s own poll on
    // `<prefix>-1`, which every caller has already done or is about to;
    // this read is taken after it.
    ch_exec(admin, "SYSTEM FLUSH LOGS").await;
    let sql = format!(
        "SELECT query_id AS s FROM system.query_log \
         WHERE query_id LIKE '{prefix}-%' AND type = 'QueryFinish' \
           AND current_database = '{db}'"
    );
    let mut stream = admin
        .query_stream::<ChText>(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("query_log read failed: {e}"));
    let mut out = BTreeSet::new();
    while let Some(row) = stream.next().await {
        out.insert(row.expect("decode a query_log row").s);
    }
    out
}

/// One `POST /v1/traces`, synchronous — so a `200` means the rows are
/// flushed and read-visible — carrying exactly the `ResourceSpans` given.
fn push(port: u16, resource_spans: Vec<ResourceSpans>, ctx: &str) {
    let req = ExportTraceServiceRequest { resource_spans };
    let res = request(
        port,
        "POST",
        "/v1/traces",
        Some(("application/x-protobuf", &req.encode_to_vec())),
        &[],
    )
    .unwrap_or_else(|| panic!("{ctx}: ingest must be reachable"));
    assert_eq!(
        res.status,
        200,
        "{ctx}: sync ingest must succeed, body {:?}",
        String::from_utf8_lossy(&res.body)
    );
}

/// The v2 envelope's field 1, as a `TracesData`, plus the request's own
/// statement-id prefix.
///
/// The envelope is `{1: trace, 2: metrics}` written field by field, so
/// field 1 is read the same way rather than through a message type this
/// repository does not generate.
fn fetch_trace(port: u16, hex: &str, query: &str, ctx: &str) -> (TracesData, String) {
    let path = if query.is_empty() {
        format!("/api/v2/traces/{hex}")
    } else {
        format!("/api/v2/traces/{hex}?{query}")
    };
    let res = request(
        port,
        "GET",
        &path,
        None,
        &[("Accept", "application/protobuf")],
    )
    .unwrap_or_else(|| panic!("{ctx}: the fetch must be reachable"));
    assert_eq!(
        res.status,
        200,
        "{ctx}: the v2 fetch always answers 200, body {:?}",
        String::from_utf8_lossy(&res.body)
    );
    let prefix = res
        .headers
        .get("x-pulsus-query-id")
        .unwrap_or_else(|| {
            panic!(
                "{ctx}: the fetch routes must return X-Pulsus-Query-Id, got {:?}",
                res.headers
            )
        })
        .clone();
    (envelope_trace(&res.body, ctx), prefix)
}

/// Field 1 of the v2 envelope, decoded as a `TracesData`. An absent field
/// 1 is an empty trace, which is what the route answers for an unknown id.
fn envelope_trace(body: &[u8], ctx: &str) -> TracesData {
    assert!(
        body.len() >= 2 && body[0] == 0x0a,
        "{ctx}: the envelope must open with field 1, got {body:?}"
    );
    // One varint length; the fetch's bodies are well under 2^28 bytes, so
    // the loop is a plain LEB128 read.
    let mut len: usize = 0;
    let mut shift = 0;
    let mut i = 1;
    loop {
        let b = body[i];
        len |= ((b & 0x7f) as usize) << shift;
        i += 1;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    TracesData::decode(&body[i..i + len])
        .unwrap_or_else(|e| panic!("{ctx}: field 1 must be a TracesData: {e}"))
}

/// The span ids a fetched trace carries, as a set.
fn fetched_span_ids(data: &TracesData) -> BTreeSet<String> {
    fetched_spans(data)
        .iter()
        .map(|s| hex(&s.span_id))
        .collect()
}

/// Every span of a fetched trace, in the order the response carries them.
fn fetched_spans(data: &TracesData) -> Vec<&Span> {
    data.resource_spans
        .iter()
        .flat_map(|rs| &rs.scope_spans)
        .flat_map(|ss| &ss.spans)
        .collect()
}

/// Every `(scope, key) -> value` pair a fetched trace carries, across all
/// five attribute scopes.
///
/// **The mechanical discriminator between a reordering and a changed
/// value**: a moved line leaves this map unchanged; a changed value
/// changes it at exactly one key, and the assertion names which.
fn otlp_attr_multiset(data: &TracesData) -> BTreeMap<(String, String), Option<AnyValue>> {
    let mut out = BTreeMap::new();
    for rs in &data.resource_spans {
        if let Some(resource) = &rs.resource {
            for a in &resource.attributes {
                out.insert(("resource".to_string(), a.key.clone()), a.value.clone());
            }
        }
        for ss in &rs.scope_spans {
            if let Some(scope) = &ss.scope {
                for a in &scope.attributes {
                    out.insert(("scope".to_string(), a.key.clone()), a.value.clone());
                }
            }
            for span in &ss.spans {
                for a in &span.attributes {
                    out.insert(("span".to_string(), a.key.clone()), a.value.clone());
                }
                for event in &span.events {
                    for a in &event.attributes {
                        out.insert(("event".to_string(), a.key.clone()), a.value.clone());
                    }
                }
                for link in &span.links {
                    for a in &link.attributes {
                        out.insert(("link".to_string(), a.key.clone()), a.value.clone());
                    }
                }
            }
        }
    }
    out
}

/// One attribute list as a map, with its length asserted against the map's
/// first — so a DUPLICATE key inside one scope is a failure rather than a
/// silent collapse.
fn attr_map(attrs: &[KeyValue], ctx: &str) -> BTreeMap<String, Option<AnyValue>> {
    let map: BTreeMap<String, Option<AnyValue>> = attrs
        .iter()
        .map(|a| (a.key.clone(), a.value.clone()))
        .collect();
    assert_eq!(
        attrs.len(),
        map.len(),
        "{ctx}: a duplicate key inside one scope, in {:?}",
        attrs.iter().map(|a| &a.key).collect::<Vec<_>>()
    );
    map
}

/// The keys of one attribute list, in the order the response carries them.
fn attr_keys(attrs: &[KeyValue]) -> Vec<String> {
    attrs.iter().map(|a| a.key.clone()).collect()
}

fn any_str(value: &str) -> Option<AnyValue> {
    Some(AnyValue {
        value: Some(Value::StringValue(value.to_string())),
    })
}

fn kv_any(key: &str, value: Value) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue { value: Some(value) }),
        key_strindex: 0,
    }
}

/// A 16-byte trace id whose last byte is `n`, and its hex.
fn trace_id(n: u8) -> [u8; 16] {
    let mut id = [0xaau8; 16];
    id[15] = n;
    id
}

/// An 8-byte span id with every byte `n`.
fn span_id_of(n: u8) -> [u8; 8] {
    [n; 8]
}

// ---------------------------------------------------------------------
// T-W7 — every field, an event, a link, every storable value shape, the
// zero-start arm and the any-depth kvlist arm.
// ---------------------------------------------------------------------

/// `T-W7`: **a span carrying every field round-trips as an OTLP value.**
///
/// ## Provenance
///
/// §5.0b's **P1 and P2**, through the shared helpers. **And a third,
/// independent marker is in the fixture itself**: every attribute list is
/// pushed DESCENDING by key in all five scopes and asserted ascending
/// positionally, and the old payload path preserves wire order — so that
/// half is red on the unchanged tree for a reason that depends on neither
/// marker.
///
/// ## The state this fixture produces
///
/// Derived from the storage rules, not from the push count:
///
/// | the fixture pushes | storage holds | why |
/// |---|---|---|
/// | one `ResourceSpans`, one scope, **two** spans | **2** rows in the span table | one row per span; the two share a `resource_id` and a scope, which are columns on each row |
/// | one resource, referenced by spans on **two** UTC days | **2** rows in the resource table — **one identity, two rows** | the key is `(resource_id, day)` and `day` is the UTC day of the span that referenced it, per span. The two spans start at `0` and the anchor, so their days are `1970-01-01` and `2023-11-14` |
/// | spans starting at `0` and the anchor | **two** span partitions and buckets **0** and **5666666** | the table is partitioned and bucketed on each span's OWN start |
/// | both spans in **one** push | **1** per-trace row, filed under `1970-01-01` | the view groups per insert block and files under the block's MINIMUM start day |
///
/// **So the precondition is four counts and each is a different
/// quantity**: `0` in the old table, `2` finalised spans, **`2` raw**
/// resource rows and **`1` finalised** one. Asserting the raw count and
/// the finalised count separately is what makes the case say which it
/// means — the raw count is stable because no merge can combine parts in
/// different partitions, and the finalised count is stable because `day`
/// is in neither table's sort key and cross-partition final merging is on
/// by default.
///
/// **And this fixture is the enumeration defect's own trace.** Its two
/// spans occupy buckets `0` and `5666666`, so the stored set has **two**
/// elements and the key read is two tuples — where a design that
/// enumerated every bucket between the trace's first and last span would
/// build 5,666,667 of them. `bucket_count` is `2`, far under the cap, so
/// the complete-predicate route does not fire and the fetch costs **one**
/// statement.
#[tokio::test(flavor = "multi_thread")]
async fn t_w7_a_span_with_every_field_round_trips_as_an_otlp_value() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_720;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_fetch_every_field_it")).await;
    let _server = spawn_ready(port, &db);
    let admin = ch_admin().await;
    hold_the_fetch_tables(&admin, db.name()).await;

    let tid = trace_id(0x01);
    let hex32 = hex(&tid);
    let every_field = span_id_of(0x11);
    let zero_start = span_id_of(0x12);

    // --- the fixture, literally -------------------------------------
    let entity = EntityRef {
        schema_url: "https://example.invalid/ent".to_string(),
        r#type: "service".to_string(),
        id_keys: vec!["service.name".to_string()],
        description_keys: vec!["a.first".to_string()],
    };
    let kvlist = Value::KvlistValue(KeyValueList {
        values: vec![
            kv_any(
                "a",
                Value::KvlistValue(KeyValueList {
                    values: vec![
                        kv_any("deep", Value::BytesValue(vec![0x05])),
                        kv_any("ok", Value::IntValue(2)),
                    ],
                }),
            ),
            kv_any("b", Value::IntValue(1)),
        ],
    });
    let span_attrs = vec![
        // DESCENDING by key, in all five scopes.
        kv_any("s.str", Value::StringValue("v".to_string())),
        kv_any("s.kv", kvlist.clone()),
        kv_any("s.int", Value::IntValue(42)),
        kv_any("s.dbl", Value::DoubleValue(1.5)),
        kv_any("s.bool", Value::BoolValue(true)),
        kv_any("s.bin", Value::BytesValue(vec![0x01, 0x02])),
        kv_any(
            "s.arr",
            Value::ArrayValue(ArrayValue {
                values: vec![
                    AnyValue {
                        value: Some(Value::StringValue("a".to_string())),
                    },
                    AnyValue {
                        value: Some(Value::StringValue("b".to_string())),
                    },
                ],
            }),
        ),
    ];
    let event = Event {
        time_unix_nano: u64::try_from(ANCHOR_NS + 100_000_000).expect("post-epoch"),
        name: "ev".to_string(),
        attributes: vec![
            kv_any("e.int", Value::IntValue(7)),
            kv_any("e.bin", Value::BytesValue(vec![0x03])),
        ],
        dropped_attributes_count: 2,
    };
    let link = Link {
        trace_id: vec![0xbb; 16],
        span_id: vec![0xcc; 8],
        trace_state: "ls=1".to_string(),
        flags: 2,
        attributes: vec![
            kv_any("l.str", Value::StringValue("s".to_string())),
            kv_any("l.bool", Value::BoolValue(false)),
            kv_any("l.bin", Value::BytesValue(vec![0x04])),
        ],
        dropped_attributes_count: 4,
    };
    let main_span = Span {
        trace_id: tid.to_vec(),
        span_id: every_field.to_vec(),
        parent_span_id: vec![0x22; 8],
        name: "every-field".to_string(),
        kind: SpanKind::Server as i32,
        start_time_unix_nano: u64::try_from(ANCHOR_NS).expect("post-epoch"),
        end_time_unix_nano: u64::try_from(ANCHOR_NS + 500_000_000).expect("post-epoch"),
        trace_state: "rojo=00f067aa0ba902b7".to_string(),
        flags: 1,
        status: Some(Status {
            code: 2,
            message: "boom".to_string(),
        }),
        attributes: span_attrs.clone(),
        dropped_attributes_count: 3,
        events: vec![event.clone()],
        dropped_events_count: 5,
        links: vec![link.clone()],
        dropped_links_count: 9,
    };
    // The zero-start arm: every other field at its default, and the
    // test's receipt time is far from zero.
    let zero_span = Span {
        trace_id: tid.to_vec(),
        span_id: zero_start.to_vec(),
        name: "zero-start".to_string(),
        start_time_unix_nano: 0,
        end_time_unix_nano: 0,
        ..Default::default()
    };
    let scope = InstrumentationScope {
        name: "live-scope".to_string(),
        version: "1.2.3".to_string(),
        attributes: vec![
            kv_any(
                "otel.scope.build",
                Value::StringValue("deadbeef".to_string()),
            ),
            kv_any("otel.scope.blob", Value::BytesValue(vec![0xde, 0xad])),
        ],
        dropped_attributes_count: 11,
    };
    let resource = Resource {
        attributes: vec![
            kv_any("z.last", Value::StringValue("zzz".to_string())),
            kv_any("service.name", Value::StringValue("checkout".to_string())),
            kv_any("a.first", Value::StringValue("aaa".to_string())),
        ],
        dropped_attributes_count: 7,
        entity_refs: vec![entity.clone()],
    };
    push(
        port,
        vec![ResourceSpans {
            resource: Some(resource.clone()),
            scope_spans: vec![ScopeSpans {
                scope: Some(scope.clone()),
                spans: vec![main_span.clone(), zero_span.clone()],
                schema_url: "https://example.invalid/scope".to_string(),
            }],
            schema_url: "https://example.invalid/res".to_string(),
        }],
        "T-W7 push",
    );

    // --- the precondition: four counts, four quantities -------------
    not_in_the_old_table(&admin, db.name(), &hex32, 2).await;
    let resource_counts = {
        let raw = ch_count(
            &admin,
            &format!(
                "SELECT count() AS n FROM {db}.resources WHERE service = 'checkout'",
                db = db.name()
            ),
        )
        .await;
        let finalised = ch_count(
            &admin,
            &format!(
                "SELECT count() AS n FROM {db}.resources FINAL WHERE service = 'checkout'",
                db = db.name()
            ),
        )
        .await;
        (raw, finalised)
    };
    assert_eq!(
        resource_counts,
        (2, 1),
        "one identity referenced on two UTC days is TWO physical rows and ONE \
         finalised identity — the two are different quantities and the case says which"
    );
    assert_eq!(
        ch_count(
            &admin,
            &format!(
                "SELECT count() AS n FROM {db}.traces WHERE trace_id = unhex('{hex32}')",
                db = db.name()
            )
        )
        .await,
        1,
        "one push contributes one per-trace row, filed under the block's minimum day"
    );

    // --- the fetch --------------------------------------------------
    let (fetched, prefix) = fetch_trace(port, &hex32, "", "T-W7");
    the_new_statements_ran(&admin, db.name(), &prefix).await;

    // The response topology: one group per span, each carrying the SAME
    // resource and the SAME scope.
    assert_eq!(
        fetched.resource_spans.len(),
        2,
        "one ResourceSpans per span, which is what every golden pins"
    );
    assert_eq!(
        fetched.resource_spans[0].resource, fetched.resource_spans[1].resource,
        "byte-identical resources between the groups"
    );
    assert_eq!(
        fetched.resource_spans[0].scope_spans[0].scope,
        fetched.resource_spans[1].scope_spans[0].scope,
        "and byte-identical scopes"
    );

    let spans = fetched_spans(&fetched);
    // The zero-start span sorts FIRST, because the canonical order leads
    // on the start time and the stored value is now zero rather than the
    // receipt time.
    assert_eq!(
        spans
            .iter()
            .map(|s| (hex(&s.span_id), s.name.clone()))
            .collect::<Vec<_>>(),
        vec![
            (hex(&zero_start), "zero-start".to_string()),
            (hex(&every_field), "every-field".to_string()),
        ],
        "the zero-start span sorts to the front"
    );
    assert_eq!(
        spans[0].start_time_unix_nano, 0,
        "a zero start round-trips as zero, not as the receipt time"
    );

    let out = spans[1];
    assert_eq!(out.trace_id, tid.to_vec());
    assert_eq!(out.parent_span_id, vec![0x22; 8], "not a root span");
    assert_eq!(out.name, "every-field");
    assert_eq!(out.kind, SpanKind::Server as i32);
    assert_eq!(out.start_time_unix_nano, ANCHOR_NS as u64);
    assert_eq!(out.end_time_unix_nano, ANCHOR_NS as u64 + 500_000_000);
    assert_eq!(out.trace_state, "rojo=00f067aa0ba902b7");
    assert_eq!(out.flags, 1);
    assert_eq!(out.dropped_attributes_count, 3);
    assert_eq!(out.dropped_events_count, 5);
    assert_eq!(out.dropped_links_count, 9);
    assert_eq!(
        out.status,
        Some(Status {
            code: 2,
            message: "boom".to_string()
        })
    );

    // --- every value shape, and its own OTLP arm --------------------
    let span_attrs_back = attr_map(&out.attributes, "T-W7 span attributes");
    assert_eq!(
        span_attrs_back.get("s.int").cloned().flatten(),
        Some(AnyValue {
            value: Some(Value::IntValue(42))
        }),
        "an Int64 is never a Double"
    );
    assert_eq!(
        span_attrs_back.get("s.dbl").cloned().flatten(),
        Some(AnyValue {
            value: Some(Value::DoubleValue(1.5))
        })
    );
    assert_eq!(
        span_attrs_back.get("s.bool").cloned().flatten(),
        Some(AnyValue {
            value: Some(Value::BoolValue(true))
        })
    );
    assert_eq!(
        span_attrs_back.get("s.str").cloned().flatten(),
        any_str("v"),
        "a one-segment stored path is ONE dotted key, not a kvlist"
    );
    assert_eq!(
        span_attrs_back.get("s.bin").cloned().flatten(),
        Some(AnyValue {
            value: Some(Value::BytesValue(vec![0x01, 0x02]))
        }),
        "a value no JSON path can hold comes back from the second carrier"
    );
    assert_eq!(
        span_attrs_back.get("s.arr").cloned().flatten(),
        Some(AnyValue {
            value: Some(Value::ArrayValue(ArrayValue {
                values: vec![
                    AnyValue {
                        value: Some(Value::StringValue("a".to_string()))
                    },
                    AnyValue {
                        value: Some(Value::StringValue("b".to_string()))
                    },
                ],
            }))
        }),
        "in that element order"
    );
    // The any-depth kvlist arm: a BYTES leaf at depth two. If the
    // write side's any-depth rule regressed, the storable leaves are
    // written as paths and the depth-two bytes leaf is DROPPED — a lost
    // value in a 200, which is what this arm sees and which no hermetic
    // reader case can.
    assert_eq!(
        span_attrs_back.get("s.kv").cloned().flatten(),
        Some(AnyValue {
            value: Some(kvlist.clone())
        }),
        "the whole kvlist, every leaf present at every depth"
    );

    // --- the resource, the scope, the event and the link ------------
    let group = &fetched.resource_spans[1];
    assert_eq!(group.schema_url, "https://example.invalid/res");
    let resource_back = group.resource.as_ref().expect("a present resource");
    assert_eq!(resource_back.dropped_attributes_count, 7);
    assert_eq!(
        resource_back.entity_refs,
        vec![entity.clone()],
        "the entity references the sender sent, which the reference drops"
    );
    let scope_spans = &group.scope_spans[0];
    assert_eq!(scope_spans.schema_url, "https://example.invalid/scope");
    let scope_back = scope_spans.scope.as_ref().expect("a present scope");
    assert_eq!(scope_back.name, "live-scope");
    assert_eq!(scope_back.version, "1.2.3");
    assert_eq!(scope_back.dropped_attributes_count, 11);

    assert_eq!(out.events.len(), 1);
    assert_eq!(out.events[0].time_unix_nano, event.time_unix_nano);
    assert_eq!(out.events[0].name, "ev");
    assert_eq!(out.events[0].dropped_attributes_count, 2);
    assert_eq!(out.links.len(), 1);
    assert_eq!(out.links[0].trace_id, vec![0xbb; 16]);
    assert_eq!(out.links[0].span_id, vec![0xcc; 8]);
    assert_eq!(out.links[0].trace_state, "ls=1");
    assert_eq!(out.links[0].flags, 2);
    assert_eq!(out.links[0].dropped_attributes_count, 4);

    // --- the five ordering assertions, each a positional sequence ---
    assert_eq!(
        attr_keys(&resource_back.attributes),
        vec!["a.first", "service.name", "z.last"],
        "the resource's keys, ascending — and the reconstructed service name is \
         sorted IN rather than placed first"
    );
    assert_eq!(
        attr_keys(&scope_back.attributes),
        vec!["otel.scope.blob", "otel.scope.build"],
        "the scope's keys, ascending across BOTH carriers"
    );
    assert_eq!(
        attr_keys(&out.attributes),
        vec![
            "s.arr", "s.bin", "s.bool", "s.dbl", "s.int", "s.kv", "s.str"
        ],
        "the span's keys, ascending"
    );
    assert_eq!(
        attr_keys(&out.events[0].attributes),
        vec!["e.bin", "e.int"],
        "the event's keys, ascending"
    );
    assert_eq!(
        attr_keys(&out.links[0].attributes),
        vec!["l.bin", "l.bool", "l.str"],
        "the link's keys, ascending"
    );

    // --- the key set came from the column, and one statement ran ----
    let text = recorded_statement(&admin, db.name(), &format!("{prefix}-1"))
        .await
        .expect("statement 1 is recorded");
    assert!(
        !text.contains("range(") && !text.contains("least("),
        "the key set is READ from the per-trace column, not computed from an extent — \
         a builder that enumerated every bucket between this trace's two spans would \
         build 5,666,667 keys:\n{text}"
    );
    assert_eq!(
        statement_ids(&admin, db.name(), &prefix).await,
        BTreeSet::from([format!("{prefix}-1")]),
        "an indexed, readable trace whose stored set is under the cap costs ONE statement"
    );
}

// ---------------------------------------------------------------------
// The fallback, the statement count, the shared span, the reordering.
// ---------------------------------------------------------------------

/// One `ResourceSpans` carrying `service.name = service` and the spans
/// given, under a scope with no attributes — the shape most of the cases
/// below need.
fn one_resource_push(port: u16, service: &str, spans: Vec<Span>, ctx: &str) {
    push(
        port,
        vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv_str("service.name", service)],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "live-scope".to_string(),
                    ..Default::default()
                }),
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
        ctx,
    );
}

/// A plain span of `tid` with one integer attribute.
fn plain_span(tid: [u8; 16], span_id: [u8; 8], start_ns: i64) -> Span {
    Span {
        trace_id: tid.to_vec(),
        span_id: span_id.to_vec(),
        name: "op".to_string(),
        kind: SpanKind::Server as i32,
        start_time_unix_nano: u64::try_from(start_ns).expect("post-epoch"),
        end_time_unix_nano: u64::try_from(start_ns + 1_000_000).expect("post-epoch"),
        attributes: vec![kv_any("k", Value::IntValue(1))],
        ..Default::default()
    }
}

/// **The fallback answers a trace the per-trace view has not indexed.**
///
/// ## The fixture, and why the view is DETACHED rather than dropped
///
/// `DETACH` and not `DROP`: the `ATTACH` in step 3 restores it with no
/// DDL, where the controller's view reconcile would re-create a dropped
/// one. It is safe to detach around because the reconcile runs once at
/// startup and the rotation tick touches no view.
///
/// ## Why it needed BOTH provenance markers and had neither
///
/// The fixture manufactures the NEW path's missing index row by detaching
/// the view — but the old per-span payload row is untouched, and the old
/// point read returns the same three span ids with the same attribute and
/// the same resource. So assertion (a) was green on the unchanged tree.
/// And assertion (c)'s expected answer is EMPTY, which an empty old-path
/// answer satisfies silently — the one kind P1 cannot cover.
#[tokio::test(flavor = "multi_thread")]
async fn the_fallback_answers_a_trace_the_per_trace_view_has_not_indexed() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_721;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_fetch_fallback_it")).await;
    let _server = spawn_ready(port, &db);
    let admin = ch_admin().await;
    hold_the_fetch_tables(&admin, db.name()).await;

    let unindexed = trace_id(0x02);
    let indexed = trace_id(0x03);
    let unindexed_hex = hex(&unindexed);
    let indexed_hex = hex(&indexed);

    // 1. detach the per-trace view, so the first push lands spans and no
    //    index row.
    ch_exec(
        &admin,
        &format!("DETACH TABLE {db}.traces_mv", db = db.name()),
    )
    .await;
    // 2. the unindexed trace: three spans, one millisecond apart.
    one_resource_push(
        port,
        "checkout",
        (1u8..=3)
            .map(|n| {
                plain_span(
                    unindexed,
                    span_id_of(n),
                    ANCHOR_NS + (n as i64 - 1) * 1_000_000,
                )
            })
            .collect(),
        "the unindexed push",
    );
    // 3. attach it again.
    ch_exec(
        &admin,
        &format!("ATTACH TABLE {db}.traces_mv", db = db.name()),
    )
    .await;
    // 4. the control: the same three shapes, indexed.
    one_resource_push(
        port,
        "checkout",
        (4u8..=6)
            .map(|n| {
                plain_span(
                    indexed,
                    span_id_of(n),
                    ANCHOR_NS + 10_000_000 + (n as i64 - 4) * 1_000_000,
                )
            })
            .collect(),
        "the indexed push",
    );

    // 5. the state, before any request. Without this the case can pass
    //    because the fixture failed to make the state.
    assert_eq!(
        ch_count(
            &admin,
            &format!(
                "SELECT count() AS n FROM {db}.traces FINAL WHERE trace_id = unhex('{unindexed_hex}')",
                db = db.name()
            )
        )
        .await,
        0,
        "the detached view must have left NO index row for the first trace"
    );
    assert_eq!(
        ch_count(
            &admin,
            &format!(
                "SELECT count() AS n FROM {db}.traces FINAL WHERE trace_id = unhex('{indexed_hex}')",
                db = db.name()
            )
        )
        .await,
        1,
        "and ONE for the control"
    );
    assert_eq!(
        ch_count(
            &admin,
            &format!(
                "SELECT count() AS n FROM {db}.spans FINAL WHERE trace_id = unhex('{unindexed_hex}')",
                db = db.name()
            )
        )
        .await,
        3,
        "the spans landed either way"
    );

    // 6. P1 for both traces.
    not_in_the_old_table(&admin, db.name(), &unindexed_hex, 3).await;
    not_in_the_old_table(&admin, db.name(), &indexed_hex, 3).await;

    // (a) the unindexed trace, WITH a window.
    let window = format!(
        "start={}&end={}",
        ANCHOR_NS - 1_000_000_000,
        ANCHOR_NS + 2_000_000_000
    );
    let (fetched, prefix_a) = fetch_trace(port, &unindexed_hex, &window, "the fallback");
    the_new_statements_ran(&admin, db.name(), &prefix_a).await;
    assert_eq!(
        fetched_span_ids(&fetched),
        (1u8..=3).map(|n| hex(&span_id_of(n))).collect(),
        "(a) the fallback returns every span of the unindexed trace"
    );
    let spans = fetched_spans(&fetched);
    assert_eq!(
        attr_map(&spans[0].attributes, "(a) span attributes")
            .get("k")
            .cloned()
            .flatten(),
        Some(AnyValue {
            value: Some(Value::IntValue(1))
        })
    );
    assert_eq!(
        attr_map(
            &fetched.resource_spans[0]
                .resource
                .as_ref()
                .expect("a present resource")
                .attributes,
            "(a) resource attributes"
        )
        .get("service.name")
        .cloned()
        .flatten(),
        any_str("checkout")
    );

    // (b) the control: the indexed path answers the same shape.
    let control_window = format!(
        "start={}&end={}",
        ANCHOR_NS + 9_000_000_000 - 9_000_000_000,
        ANCHOR_NS + 2_000_000_000
    );
    let (control, prefix_b) = fetch_trace(port, &indexed_hex, &control_window, "the control");
    the_new_statements_ran(&admin, db.name(), &prefix_b).await;
    assert_eq!(
        fetched_span_ids(&control),
        (4u8..=6).map(|n| hex(&span_id_of(n))).collect(),
        "(b) the indexed path answers the same shape for the control"
    );

    // (d) the statement counts, which is what tells the two routes apart:
    // the span assertions above cannot, because both return the right
    // spans.
    assert_eq!(
        statement_ids(&admin, db.name(), &prefix_a).await,
        BTreeSet::from([format!("{prefix_a}-1"), format!("{prefix_a}-2")]),
        "(d) the fallback costs two statements"
    );
    assert_eq!(
        statement_ids(&admin, db.name(), &prefix_b).await,
        BTreeSet::from([format!("{prefix_b}-1")]),
        "(d) and the indexed path one"
    );
    let fallback_text = recorded_statement(&admin, db.name(), &format!("{prefix_a}-2"))
        .await
        .expect("the fallback statement is recorded");
    assert!(
        fallback_text.contains(&format!("start_ns >= {}", ANCHOR_NS - 1_000_000_000)),
        "(d) the second statement renders the REQUEST's own window, which statement 1 \
         never renders in any form:\n{fallback_text}"
    );

    // (c) the unindexed trace with NO window: the v2 empty envelope,
    // byte for byte, which is what pins the no-window decision.
    let res = request(
        port,
        "GET",
        &format!("/api/v2/traces/{unindexed_hex}"),
        None,
        &[("Accept", "application/protobuf")],
    )
    .expect("reachable");
    assert_eq!(res.status, 200, "the v2 route never answers 404");
    assert_eq!(
        res.body,
        vec![0x0a, 0x00, 0x12, 0x00],
        "(c) with no window the fetch answers the empty envelope's four bytes rather \
         than taking a trace_id-only predicate over every partition in retention"
    );
    let json = request(
        port,
        "GET",
        &format!("/api/v2/traces/{unindexed_hex}"),
        None,
        &[("Accept", "application/json")],
    )
    .expect("reachable");
    assert_eq!(
        String::from_utf8_lossy(&json.body),
        r#"{"trace":{},"metrics":{}}"#,
        "(c) and the JSON envelope's twenty-five"
    );
}

/// `T-Q1`: **one statement for an indexed fetch, two for the fallback.**
///
/// Three assertions, and none of them is arithmetic on a count:
///
/// * **the exact id set**, so a count of 2 where 1 is expected is reported
///   as the presence of the NAMED row;
/// * **the statement text of each id** — statement 1 reads the per-trace
///   table, which the old point read cannot; the fallback's renders the
///   request's own window, which statement 1 never renders in any form,
///   so "two statements were issued" is witnessed by a row whose text
///   carries a value only the fallback can produce;
/// * **the absence assertion** for the indexed case, taken twice: the id
///   is absent from the set AND a count on it is zero. The two together
///   separate "not issued" from "the log has not caught up".
#[tokio::test(flavor = "multi_thread")]
async fn t_q1_a_fetch_issues_one_statement_and_the_fallback_two() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_724;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_fetch_statements_it")).await;
    let _server = spawn_ready(port, &db);
    let admin = ch_admin().await;
    hold_the_fetch_tables(&admin, db.name()).await;

    let indexed = trace_id(0x04);
    let unindexed = trace_id(0x05);
    let indexed_hex = hex(&indexed);
    let unindexed_hex = hex(&unindexed);

    one_resource_push(
        port,
        "checkout",
        (1u8..=3)
            .map(|n| {
                plain_span(
                    indexed,
                    span_id_of(n),
                    ANCHOR_NS + (n as i64 - 1) * 1_000_000,
                )
            })
            .collect(),
        "the indexed push",
    );
    ch_exec(
        &admin,
        &format!("DETACH TABLE {db}.traces_mv", db = db.name()),
    )
    .await;
    one_resource_push(
        port,
        "checkout",
        (4u8..=6)
            .map(|n| {
                plain_span(
                    unindexed,
                    span_id_of(n),
                    ANCHOR_NS + 10_000_000 + (n as i64 - 4) * 1_000_000,
                )
            })
            .collect(),
        "the unindexed push",
    );
    ch_exec(
        &admin,
        &format!("ATTACH TABLE {db}.traces_mv", db = db.name()),
    )
    .await;
    not_in_the_old_table(&admin, db.name(), &indexed_hex, 3).await;
    not_in_the_old_table(&admin, db.name(), &unindexed_hex, 3).await;

    // The indexed fetch: exactly one id, and the second is absent twice
    // over.
    let (_, prefix) = fetch_trace(port, &indexed_hex, "", "T-Q1 indexed");
    the_new_statements_ran(&admin, db.name(), &prefix).await;
    assert_eq!(
        statement_ids(&admin, db.name(), &prefix).await,
        BTreeSet::from([format!("{prefix}-1")])
    );
    assert_eq!(
        ch_count(
            &admin,
            &format!("SELECT count() AS n FROM system.query_log WHERE query_id = '{prefix}-2'")
        )
        .await,
        0,
        "not issued, as distinct from the log not having caught up"
    );

    // The fallback: exactly two, and the second's text carries the
    // request's own window.
    let window_start = ANCHOR_NS - 1_000_000_000;
    let (_, prefix2) = fetch_trace(
        port,
        &unindexed_hex,
        &format!("start={window_start}&end={}", ANCHOR_NS + 2_000_000_000),
        "T-Q1 fallback",
    );
    the_new_statements_ran(&admin, db.name(), &prefix2).await;
    assert_eq!(
        statement_ids(&admin, db.name(), &prefix2).await,
        BTreeSet::from([format!("{prefix2}-1"), format!("{prefix2}-2")])
    );
    let second = recorded_statement(&admin, db.name(), &format!("{prefix2}-2"))
        .await
        .expect("the second statement is recorded");
    assert!(
        second.contains(&format!("start_ns >= {window_start}")),
        "the second statement's text must carry a value only the fallback can \
         produce:\n{second}"
    );
}

/// **A Zipkin shared span comes back as two spans, in canonical order.**
///
/// All three spans share a start, so the order is decided by `span_id`
/// then `kind` and by nothing else — a fixture with distinct starts cannot
/// see a `kind` tiebreak at all.
///
/// §5.0b's P1 and P2 apply: the assembler already keys its dedup on
/// `(span_id, kind)` and already breaks the tie on `kind`, so the old
/// payload path returns the same three spans in the same order and this
/// case is green on the unchanged tree without them.
#[tokio::test(flavor = "multi_thread")]
async fn a_zipkin_shared_span_comes_back_as_two_spans_in_canonical_order() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_725;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_fetch_shared_it")).await;
    let _server = spawn_ready(port, &db);
    let admin = ch_admin().await;
    hold_the_fetch_tables(&admin, db.name()).await;

    let tid = trace_id(0x05);
    let hex32 = hex(&tid);
    let shared = span_id_of(0x77);
    let other = span_id_of(0x33);
    let mut server_side = plain_span(tid, shared, ANCHOR_NS);
    server_side.kind = SpanKind::Server as i32;
    let mut client_side = plain_span(tid, shared, ANCHOR_NS);
    client_side.kind = SpanKind::Client as i32;
    let mut third = plain_span(tid, other, ANCHOR_NS);
    third.kind = SpanKind::Server as i32;
    one_resource_push(
        port,
        "checkout",
        vec![server_side, client_side, third],
        "the shared-span push",
    );
    not_in_the_old_table(&admin, db.name(), &hex32, 3).await;

    let (fetched, prefix) = fetch_trace(port, &hex32, "", "the shared span");
    the_new_statements_ran(&admin, db.name(), &prefix).await;
    assert_eq!(
        fetched_spans(&fetched)
            .iter()
            .map(|s| (hex(&s.span_id), s.kind))
            .collect::<Vec<_>>(),
        vec![
            (hex(&other), SpanKind::Server as i32),
            (hex(&shared), SpanKind::Server as i32),
            (hex(&shared), SpanKind::Client as i32),
        ],
        "three spans, ordered by span id then by kind — `kind` is what keeps a shared \
         span's two sides apart, and it is the last tiebreak"
    );
}

/// **A reordered attribute key passes and a changed value fails.**
///
/// Three databases, three pushes of one trace id: two differing only in
/// the WIRE ORDER of their attribute lists, and a third differing in one
/// VALUE.
///
/// ## Why both halves needed provenance and had none
///
/// On the old payload path both responses are byte-equal to each other
/// too — the payload round-trips each sender's own bytes, and two pushes
/// with the same values in different orders produce different stored
/// payloads but the SAME response only after this change. And the
/// changed-value half differs on the old path as well. So neither
/// assertion discriminated.
#[tokio::test(flavor = "multi_thread")]
async fn a_reordered_attribute_key_passes_and_a_changed_value_fails() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port_a = 31_740;
    let port_b = 31_741;
    let port_c = 31_742;
    /// The one trace id all three databases push, named so the inner
    /// helper takes it without a parameter.
    const TRACE_ID_07: [u8; 16] = [
        0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
        0x07,
    ];
    assert_eq!(
        TRACE_ID_07,
        trace_id(0x07),
        "the constant and the builder agree"
    );
    let admin = ch_admin().await;

    /// One push into its own database, returning the fetched trace.
    ///
    /// **Takes the guard, not a name.** The per-checkout prefix comes from
    /// `pulsus_testkit::test_db` and the drop-on-entry-and-exit guard from
    /// `ScopedDb`, and the source scan that checks both reads the CALL
    /// SITE — so the pair is composed there and handed in by reference.
    async fn one(
        admin: &ChClient,
        port: u16,
        db: &ScopedDb,
        name: &str,
        attrs: (Vec<KeyValue>, Vec<KeyValue>),
    ) -> TracesData {
        let (span_attrs, resource_attrs) = attrs;
        let tid = TRACE_ID_07;
        let hex32 = &hex(&tid);
        let _server = spawn_ready(port, db);
        hold_the_fetch_tables(admin, db.name()).await;
        let mut span = plain_span(tid, span_id_of(0x01), ANCHOR_NS);
        span.attributes = span_attrs;
        push(
            port,
            vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: resource_attrs,
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "live-scope".to_string(),
                        ..Default::default()
                    }),
                    spans: vec![span],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
            name,
        );
        not_in_the_old_table(admin, db.name(), hex32, 1).await;
        let (fetched, prefix) = fetch_trace(port, hex32, "", name);
        the_new_statements_ran(admin, db.name(), &prefix).await;
        fetched
    }

    let ascending_span = vec![
        kv_any("s.str", Value::StringValue("v".to_string())),
        kv_any("z.last", Value::IntValue(2)),
    ];
    let ascending_resource = vec![
        kv_str("a.first", "aaa"),
        kv_str("service.name", "checkout"),
        kv_str("z.last", "zzz"),
    ];
    let mut reversed_span = ascending_span.clone();
    reversed_span.reverse();
    let mut reversed_resource = ascending_resource.clone();
    reversed_resource.reverse();
    let changed_span = vec![
        kv_any("s.str", Value::StringValue("w".to_string())),
        kv_any("z.last", Value::IntValue(2)),
    ];

    let db_a = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_fetch_reorder_a_it")).await;
    let db_b = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_fetch_reorder_b_it")).await;
    let db_c = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_fetch_reorder_c_it")).await;
    let first = one(
        &admin,
        port_a,
        &db_a,
        "the ascending push",
        (ascending_span, ascending_resource),
    )
    .await;
    let second = one(
        &admin,
        port_b,
        &db_b,
        "the reversed push",
        (reversed_span, reversed_resource.clone()),
    )
    .await;
    let third = one(
        &admin,
        port_c,
        &db_c,
        "the changed-value push",
        (changed_span, reversed_resource),
    )
    .await;

    // (1) the reordering half: the two responses are BYTE-equal, because
    // the response is a function of the attribute map and not of the
    // order a sender wrote it in.
    assert_eq!(
        first.encode_to_vec(),
        second.encode_to_vec(),
        "(1) two pushes of the same values in different wire orders must fetch \
         byte-identically"
    );

    // (2) without this, (1) passes on a reader that drops attributes
    // altogether.
    assert_ne!(
        first.encode_to_vec(),
        third.encode_to_vec(),
        "(2) a CHANGED value must not compare equal"
    );

    // (3) the mechanical discriminator: a moved line leaves the multiset
    // unchanged; a changed value changes it at exactly one key, and this
    // names which.
    let a = otlp_attr_multiset(&first);
    let b = otlp_attr_multiset(&second);
    let c = otlp_attr_multiset(&third);
    assert_eq!(a, b, "(3) a reordering leaves the multiset unchanged");
    let differing: Vec<(String, String)> = a
        .keys()
        .chain(c.keys())
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|k| a.get(k) != c.get(k))
        .collect();
    assert_eq!(
        differing,
        vec![("span".to_string(), "s.str".to_string())],
        "(3) a changed value differs at exactly one named key and at no other"
    );
}

// ---------------------------------------------------------------------
// T-W8 — the indexed path answers every state the design calls normal.
// ---------------------------------------------------------------------

/// The six spans of `T-W8`, their pushes, and the derived values each
/// assertion reads. **Engine-derived and pasted**, so the derivation can
/// be re-run rather than re-reasoned:
///
/// ```text
/// SELECT arrayJoin([('0x07',1700006397000000000),('0x01',1700006399000000000),
///                   ('0x02',1700006399500000000),('0x03',1700006400000000000),
///                   ('0x09',1700006820000000000),('0x05',1700007000000000000)]) AS t,
///  t.1 AS span, t.2 AS start_ns,
///  intDiv(start_ns, 300000000000) AS bucket,
///  formatDateTime(fromUnixTimestamp64Nano(start_ns), '%F %T', 'UTC') AS utc,
///  toInt32(toDate(fromUnixTimestamp64Nano(start_ns), 'UTC')) AS day_num
/// ORDER BY start_ns
/// -->
/// 0x07  1700006397000000000  5666687  2023-11-14 23:59:57  19675
/// 0x01  1700006399000000000  5666687  2023-11-14 23:59:59  19675
/// 0x02  1700006399500000000  5666687  2023-11-14 23:59:59  19675
/// 0x03  1700006400000000000  5666688  2023-11-15 00:00:00  19676
/// 0x09  1700006820000000000  5666689  2023-11-15 00:07:00  19676
/// 0x05  1700007000000000000  5666690  2023-11-15 00:10:00  19676
/// ```
///
/// **Two properties the split has to have.** The EARLIEST span is in push
/// 1 and the LATEST in push 2, so **no push holds both extremes** — an
/// `any()` over one view row cannot cover the trace. And push 2's minimum
/// is after midnight, so **some push files under the later day** and the
/// two-partition state is reachable. Push 3 deliberately straddles
/// midnight, which is the trap: its row files under the EARLIER day
/// because its minimum is pre-midnight, while its own maximum is a span
/// in the later one.
const TW8_SPANS: [(u8, i64, u8); 6] = [
    // span id byte, start_ns, push
    (0x07, 1_700_006_397_000_000_000, 1),
    (0x01, 1_700_006_399_000_000_000, 1),
    (0x02, 1_700_006_399_500_000_000, 3),
    (0x03, 1_700_006_400_000_000_000, 2),
    (0x09, 1_700_006_820_000_000_000, 3),
    (0x05, 1_700_007_000_000_000_000, 2),
];

/// What the per-trace view produces, one row per push — derived from the
/// view's own rule and pasted:
///
/// ```text
/// WITH pushes AS (SELECT arrayJoin([
///    (1, [1700006397000000000, 1700006399000000000]),
///    (2, [1700006400000000000, 1700007000000000000]),
///    (3, [1700006399500000000, 1700006820000000000])]) AS p,
///    p.1 AS push, p.2 AS starts)
/// SELECT push, arrayMin(starts) AS push_min_start,
///        toInt32(toDate(fromUnixTimestamp64Nano(arrayMin(starts)), 'UTC')) AS mv_day,
///        arrayMax(starts) AS mv_last_start_ns
/// FROM pushes ORDER BY push
/// -->
/// 1  1700006397000000000  19675  1700006399000000000
/// 2  1700006400000000000  19676  1700007000000000000
/// 3  1700006399500000000  19675  1700006820000000000
/// ```
///
/// And the per-day aggregate those three rows give:
///
/// ```text
/// 19675  1700006397000000000  1700006820000000000
/// 19676  1700006400000000000  1700007000000000000
/// ```
const TW8_PER_DAY: [(i32, i64, i64); 2] = [
    (19675, 1_700_006_397_000_000_000, 1_700_006_820_000_000_000),
    (19676, 1_700_006_400_000_000_000, 1_700_007_000_000_000_000),
];

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq)]
struct Tw8DayRow {
    day: i32,
    lo: i64,
    hi: i64,
}

/// `T-W8`: **a trace landed by three pushes, across buckets and a UTC
/// midnight, fetches whole in one statement.**
///
/// The partial-failure and operator-configuration states are not normal.
/// Nothing else pins that the NORMAL ones are complete, and the shapes a
/// one-statement extent read is exposed to are exactly these: a trace
/// landed in several pushes, crossing a five-minute bucket boundary and a
/// UTC midnight.
#[tokio::test(flavor = "multi_thread")]
async fn a_trace_landed_by_three_pushes_across_buckets_and_a_day_boundary_fetches_whole() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_729;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_fetch_three_pushes_it")).await;
    let _server = spawn_ready(port, &db);
    let admin = ch_admin().await;
    hold_the_fetch_tables(&admin, db.name()).await;

    let tid = trace_id(0x08);
    let hex32 = hex(&tid);
    for push_n in 1u8..=3 {
        let spans: Vec<Span> = TW8_SPANS
            .iter()
            .filter(|(_, _, p)| *p == push_n)
            .map(|(id, start, _)| plain_span(tid, span_id_of(*id), *start))
            .collect();
        assert_eq!(spans.len(), 2, "each push carries two of the six spans");
        one_resource_push(port, "checkout", spans, &format!("T-W8 push {push_n}"));
    }

    // --- the state, by something a background merge cannot move -----
    //
    // The per-trace table aggregates, so a ROW COUNT is not stable: two
    // parts in one partition may or may not have merged. `min` and `max`
    // ARE stable, because they are idempotent under aggregation —
    // whatever has merged, the per-day minimum and maximum are the same
    // two numbers.
    //
    // **And the read takes no `FINAL`.** `day` is outside the table's
    // sort key and cross-partition final merging is on by default, so
    // `FINAL` would collapse this trace's two day rows into ONE whose
    // `day` is then an arbitrary one of the two — and the assertion below
    // would require two rows from a read that returns one. The `GROUP BY`
    // plus `min`/`max` is what makes the result independent of the merge
    // state, and `FINAL` is the one thing that destroys the grouping it
    // is grouped on.
    let mut stream = admin
        .query_stream::<Tw8DayRow>(
            &format!(
                "SELECT toInt32(day) AS day, min(start_ns) AS lo, max(last_start_ns) AS hi \
                 FROM {db}.traces WHERE trace_id = unhex('{hex32}') \
                 GROUP BY day ORDER BY day",
                db = db.name()
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("the per-day read executes");
    let mut per_day = Vec::new();
    while let Some(row) = stream.next().await {
        per_day.push(row.expect("decode a per-day row"));
    }
    assert_eq!(
        per_day,
        TW8_PER_DAY
            .iter()
            .map(|(day, lo, hi)| Tw8DayRow {
                day: *day,
                lo: *lo,
                hi: *hi
            })
            .collect::<Vec<_>>(),
        "the two day groups, copied from the derivation above and not retyped"
    );

    // The extent the fetch derives from those two groups, and the four
    // buckets the key read covers — derived here from the pasted values
    // by integer arithmetic, a second source.
    let lo = TW8_PER_DAY
        .iter()
        .map(|(_, lo, _)| *lo)
        .min()
        .expect("two groups");
    let hi = TW8_PER_DAY
        .iter()
        .map(|(_, _, hi)| *hi)
        .max()
        .expect("two groups");
    assert_eq!(lo, 1_700_006_397_000_000_000, "the trace's earliest start");
    assert_eq!(hi, 1_700_007_000_000_000_000, "and its latest");
    assert_eq!((lo / BUCKET_NS, hi / BUCKET_NS), (5_666_687, 5_666_690));
    let distinct_buckets: BTreeSet<i64> = TW8_SPANS
        .iter()
        .map(|(_, start, _)| start / BUCKET_NS)
        .collect();
    assert_eq!(
        distinct_buckets.len(),
        4,
        "four distinct occupied buckets, far under the cap — so the \
         complete-predicate route cannot fire for this trace"
    );

    not_in_the_old_table(&admin, db.name(), &hex32, 6).await;

    // --- the fetch, with NO window ----------------------------------
    let (fetched, prefix) = fetch_trace(port, &hex32, "", "T-W8");
    the_new_statements_ran(&admin, db.name(), &prefix).await;
    assert_eq!(
        fetched_span_ids(&fetched),
        TW8_SPANS
            .iter()
            .map(|(id, _, _)| hex(&span_id_of(*id)))
            .collect(),
        "all six spans, whichever push landed them — the earliest is in push 1's row \
         and the latest in push 2's, so an extent taken from ONE row loses spans \
         whichever row it takes"
    );
    assert_eq!(
        statement_ids(&admin, db.name(), &prefix).await,
        BTreeSet::from([format!("{prefix}-1")]),
        "completeness costs ONE statement: the fallback did not fire and neither did \
         the complete-predicate route"
    );
    for group in &fetched.resource_spans {
        assert_eq!(
            attr_map(
                &group
                    .resource
                    .as_ref()
                    .expect("a present resource")
                    .attributes,
                "T-W8 resource"
            )
            .get("service.name")
            .cloned()
            .flatten(),
            any_str("checkout"),
            "every span's resource carries its service name, on both days"
        );
    }
}

// ---------------------------------------------------------------------
// T-W9 — an accepted maximum end timestamp still fetches.
// ---------------------------------------------------------------------

/// `T-W9`: **four end timestamps, three of them degenerate, come back as
/// stored — and the extent is taken from the per-trace start column.**
///
/// ## Group 1, the returned end values
///
/// *Red for*: a reader that computes `start + duration` instead of reading
/// the stored end. It passes the ordinary case and fails the other three.
///
/// ## Group 2, the extent column — and why it is asserted on the
/// STATEMENT and not on the answer
///
/// Before the write side clamped the per-trace end, taking the extent from
/// that column made trace (d) return zero spans and group 1 caught it.
/// After the clamp the same mistake returns **thirty million buckets'
/// worth of key condition** — a scan-budget refusal or a very large read —
/// and *"a very large read"* is not a thing group 1 can assert without a
/// timeout or a row threshold, neither of which discriminates reliably. So
/// the extent column is asserted on the recorded statement text: it must
/// contain the per-trace START maximum and must **not** contain the
/// per-trace END maximum. The negative one is the load-bearing half.
#[tokio::test(flavor = "multi_thread")]
async fn a_span_with_the_maximum_end_timestamp_fetches_and_returns_its_end() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_730;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_fetch_ends_it")).await;
    let _server = spawn_ready(port, &db);
    let admin = ch_admin().await;
    hold_the_fetch_tables(&admin, db.name()).await;

    // (a) a zero end with an ordinary start; (b) an end BEFORE the start;
    // (c) ordinary; (d) the protocol's maximum end with a start of 1.
    let cases: [(u8, i64, u64); 4] = [
        (0xa0, ANCHOR_NS, 0),
        (0xb0, ANCHOR_NS, (ANCHOR_NS - 1_000_000_000) as u64),
        (0xc0, ANCHOR_NS, (ANCHOR_NS + 500_000_000) as u64),
        (0xd0, 1, u64::MAX),
    ];
    assert_eq!(cases.len(), 4, "four traces, three of them degenerate");

    let spans: Vec<(String, Span)> = cases
        .iter()
        .map(|(n, start, end)| {
            let tid = trace_id(*n);
            let mut span = plain_span(tid, span_id_of(*n), *start);
            span.end_time_unix_nano = *end;
            (hex(&tid), span)
        })
        .collect();
    // One push, four traces — so one `traces_mv` row per trace and one
    // resource identity across all four.
    push(
        port,
        vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv_str("service.name", "checkout")],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "live-scope".to_string(),
                    ..Default::default()
                }),
                spans: spans.iter().map(|(_, s)| s.clone()).collect(),
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
        "T-W9 push",
    );
    for (hex32, _) in &spans {
        not_in_the_old_table(&admin, db.name(), hex32, 1).await;
    }

    // --- group 1: the returned end values ---------------------------
    for ((hex32, pushed), (_, _, want_end)) in spans.iter().zip(cases.iter()) {
        let (fetched, prefix) = fetch_trace(port, hex32, "", "T-W9 group 1");
        the_new_statements_ran(&admin, db.name(), &prefix).await;
        let got = fetched_spans(&fetched);
        assert_eq!(
            got.len(),
            1,
            "one span back for {hex32}, not zero — a wrapped extent returns an \
             EMPTY answer and that is what this half rules out"
        );
        assert_eq!(
            got[0].end_time_unix_nano, *want_end,
            "the sender's own end, verbatim, for {hex32}"
        );
        assert_eq!(got[0].start_time_unix_nano, pushed.start_time_unix_nano);
    }

    // --- group 2: the extent column, on the statement ---------------
    let (_, prefix) = fetch_trace(port, &spans[3].0, "", "T-W9 group 2");
    let text = recorded_statement(&admin, db.name(), &format!("{prefix}-1"))
        .await
        .expect("statement 1 is recorded");
    assert!(
        text.contains("max(last_start_ns)"),
        "the extent's upper bound comes from the per-trace START maximum:\n{text}"
    );
    assert!(
        !text.contains("max(end_ns)"),
        "and NEVER from the per-trace END maximum, which for this trace is the \
         protocol's own maximum and would derive thirty million buckets:\n{text}"
    );
    // And the extent was READ — otherwise the text assertion could pass
    // over a statement that never used it.
    assert_eq!(
        ch_count(
            &admin,
            &format!(
                "SELECT count() AS n FROM {db}.traces FINAL WHERE trace_id = unhex('{hex}')",
                db = db.name(),
                hex = spans[3].0
            )
        )
        .await,
        1,
        "the per-trace row is there, so the branch that skips the key read was not taken"
    );
}

// ---------------------------------------------------------------------
// T-W10 — the writer semantics a shape check cannot see.
// ---------------------------------------------------------------------

/// `T-W10`: **five resources differing only in semantics fetch as
/// themselves.**
///
/// A shape check compares columns and types. The write side can expose
/// every column at every stated type and still have regressed a semantics
/// this part's answer depends on, with every hermetic case green. This is
/// the end-to-end detector for the three that change an answer without
/// changing a shape: the entity-reference identity, the dropped-count
/// identity, and the conditional service-name skip.
///
/// ## The five variants
///
/// R1 and R2 are byte-identical apart from their entity references; R1 and
/// R4 apart from a dropped count that is **non-zero on both sides**; R1
/// and R5 apart from an entity reference that is **non-empty on both
/// sides**; R3's service name is the one non-string arm.
///
/// **R4 and R5 are what a presence-only identity passes and a
/// value-carrying one does not.** An implementation that appends the
/// reserved key when the count is *non-zero*, or when the vector is
/// *non-empty*, without putting the VALUE in the buffer, gives R1, R4 and
/// R5 one identity — so a `0` against `5` pair alone never forces the
/// value into the hash.
///
/// ## The state this fixture produces
///
/// Five `ResourceSpans`, five spans, one push, one UTC day: **5 span rows
/// in one partition**, **5 resource rows with 5 distinct identities on one
/// day**, and **1 per-trace row** whose bucket set has one element. Five
/// stored resource rows and **five rendered resources**, because the five
/// identities differ — unlike `T-W7`, where two rows collapse into one
/// rendered resource because the identity is the same.
#[tokio::test(flavor = "multi_thread")]
async fn two_resources_differing_only_in_semantics_fetch_as_themselves() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_731;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_fetch_identity_it")).await;
    let _server = spawn_ready(port, &db);
    let admin = ch_admin().await;
    hold_the_fetch_tables(&admin, db.name()).await;

    let tid = trace_id(0x09);
    let hex32 = hex(&tid);
    let service_ref = EntityRef {
        r#type: "service".to_string(),
        id_keys: vec!["service.name".to_string()],
        ..Default::default()
    };
    let host_ref = EntityRef {
        r#type: "host".to_string(),
        id_keys: vec!["service.name".to_string()],
        ..Default::default()
    };
    let checkout_attrs = vec![kv_str("service.name", "checkout"), kv_str("a.k", "v")];
    let variants: Vec<(u8, Resource)> = vec![
        (
            0x01,
            Resource {
                attributes: checkout_attrs.clone(),
                dropped_attributes_count: 5,
                entity_refs: vec![service_ref.clone()],
            },
        ),
        (
            0x02,
            Resource {
                attributes: checkout_attrs.clone(),
                dropped_attributes_count: 5,
                entity_refs: vec![],
            },
        ),
        (
            0x03,
            Resource {
                attributes: vec![kv_any("service.name", Value::IntValue(7))],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            },
        ),
        (
            0x04,
            Resource {
                attributes: checkout_attrs.clone(),
                dropped_attributes_count: 6,
                entity_refs: vec![service_ref.clone()],
            },
        ),
        (
            0x05,
            Resource {
                attributes: checkout_attrs.clone(),
                dropped_attributes_count: 5,
                entity_refs: vec![host_ref.clone()],
            },
        ),
    ];
    assert_eq!(variants.len(), 5, "five variants, five resource identities");

    push(
        port,
        variants
            .iter()
            .map(|(n, resource)| ResourceSpans {
                resource: Some(resource.clone()),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "live-scope".to_string(),
                        ..Default::default()
                    }),
                    spans: vec![plain_span(tid, span_id_of(*n), ANCHOR_NS)],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            })
            .collect(),
        "T-W10 push",
    );

    assert_eq!(
        ch_count(
            &admin,
            &format!(
                "SELECT count() AS n FROM {db}.resources FINAL",
                db = db.name()
            )
        )
        .await,
        5,
        "five distinct identities survive finalisation — without the write side's \
         identity section two of them collide and storage keeps whichever landed first"
    );
    not_in_the_old_table(&admin, db.name(), &hex32, 5).await;

    let (fetched, prefix) = fetch_trace(port, &hex32, "", "T-W10");
    the_new_statements_ran(&admin, db.name(), &prefix).await;
    assert_eq!(
        fetched.resource_spans.len(),
        5,
        "one ResourceSpans per span"
    );

    /// The resource of the group holding the span whose id byte is `n`.
    fn resource_of(data: &TracesData, n: u8) -> &Resource {
        let want = span_id_of(n).to_vec();
        data.resource_spans
            .iter()
            .find(|rs| {
                rs.scope_spans
                    .iter()
                    .flat_map(|ss| &ss.spans)
                    .any(|s| s.span_id == want)
            })
            .and_then(|rs| rs.resource.as_ref())
            .unwrap_or_else(|| panic!("a group for span {n:#04x}"))
    }

    // 1. the entity-reference identity. The assertion is on BOTH spans
    //    and not on a count, because without the identity both resolve to
    //    one stored resource and WHICH one is rendered with the other's
    //    references depends on push order.
    assert_eq!(
        resource_of(&fetched, 0x01).entity_refs,
        vec![service_ref.clone()],
        "R1 carries its one service reference"
    );
    assert_eq!(
        resource_of(&fetched, 0x02).entity_refs,
        Vec::<EntityRef>::new(),
        "and R2, which differs from R1 in NOTHING ELSE, carries none"
    );

    // 1a. the dropped-count identity, NON-ZERO on both sides — so a
    //     presence-only section fails here and a value-carrying one
    //     passes.
    assert_eq!(
        resource_of(&fetched, 0x04).dropped_attributes_count,
        6,
        "R4 differs from R1 in nothing but the count"
    );
    assert_eq!(resource_of(&fetched, 0x01).dropped_attributes_count, 5);

    // 1b. the entity-reference identity by VALUE, non-empty on both
    //     sides — the pair 1a/1b is what makes the assertion about the
    //     field's value rather than its presence.
    assert_eq!(
        resource_of(&fetched, 0x05).entity_refs,
        vec![host_ref.clone()],
        "R5 differs from R1 in nothing but the reference's type"
    );

    // 2. the one non-string service name. Without the write side's
    //    CONDITIONAL skip the value is absent from the attribute column
    //    and the reader reconstructs it from the span's own service
    //    column, whose text rendering is `"7"` — a changed TYPED value in
    //    a 200.
    assert_eq!(
        attr_map(&resource_of(&fetched, 0x03).attributes, "R3")
            .get("service.name")
            .cloned()
            .flatten(),
        Some(AnyValue {
            value: Some(Value::IntValue(7))
        }),
        "an IntValue service name is never a StringValue"
    );

    // 3. the control: the arm the write side skips either way, so the
    //    reconstruction is exercised and must still be right.
    for n in [0x01u8, 0x02] {
        assert_eq!(
            attr_map(&resource_of(&fetched, n).attributes, "the control")
                .get("service.name")
                .cloned()
                .flatten(),
            any_str("checkout"),
            "R{n:#04x}'s service name is reconstructed as the string it was"
        );
    }
}

// ---------------------------------------------------------------------
// T-W11 — the three type-checking losses, at values the old writer
// coerces to a different LEGAL value.
// ---------------------------------------------------------------------

/// `T-W11`: **an out-of-range kind, status, event time and short link ids
/// fetch back as sent.**
///
/// ## Why this case exists
///
/// Three of the write side's type-checking losses are invisible to a case
/// whose pushed values are INSIDE the old columns' ranges: a kind of `2`,
/// a status of `2`, an event time below the signed maximum and
/// full-length link ids all survive the old conversions unchanged. So a
/// case that asserts those fields touches them and cannot see the
/// narrowing. The values below are chosen so that the old path's output is
/// a **different legal value** rather than an error:
///
/// | assertion | after the fix | on the unfixed writer |
/// |---|---|---|
/// | `span.kind`, two spans | `-1` and `300` | `0` and `0` — an unspecified kind, which is legal |
/// | the link's ids, as bytes | the **4** and the **3** bytes sent | **16 zero bytes** and **8 zero bytes** — not a truncation and not a pad, a legal reference to a span that does not exist |
/// | the event's time | the protocol's maximum | the SIGNED maximum, which is a legal timestamp |
/// | `span.status.code`, two spans | `-1` and `300` | `0` and `0` — unset, which is a legal status |
///
/// **Every defect value is a legal OTLP value, and that is the whole
/// point**: nothing in the response marks the loss, so no presence check,
/// no type check and no decode check can see it, and the assertion has to
/// name the value.
///
/// ## Provenance
///
/// **P1 is load-bearing here in a way it is not everywhere**: the old
/// payload carries the sender's exact bytes, so the old point read returns
/// `-1`, `300`, the protocol's maximum and the short link ids
/// **correctly**. Without P1 all four assertions are green on the old
/// path.
#[tokio::test(flavor = "multi_thread")]
async fn a_pushed_span_with_out_of_range_kind_status_event_time_and_short_link_ids_fetches_them_back()
 {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_732;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_fetch_ranges_it")).await;
    let _server = spawn_ready(port, &db);
    let admin = ch_admin().await;
    hold_the_fetch_tables(&admin, db.name()).await;

    let tid = trace_id(0x0b);
    let hex32 = hex(&tid);
    let short_link = Link {
        trace_id: vec![0xaa, 0xbb, 0xcc, 0xdd],
        span_id: vec![0x11, 0x22, 0x33],
        ..Default::default()
    };
    let max_event = Event {
        time_unix_nano: u64::MAX,
        name: "ev".to_string(),
        ..Default::default()
    };
    // TWO spans, because `-1` and `300` fail the same way and a reader
    // should see both: a negative value and one above the old column's
    // top. They differ in `span_id`, so both are stored and both fetched.
    let spans: Vec<Span> = [(0x01u8, -1i32), (0x02u8, 300i32)]
        .iter()
        .map(|(n, value)| {
            let mut span = plain_span(tid, span_id_of(*n), ANCHOR_NS);
            span.kind = *value;
            span.status = Some(Status {
                code: *value,
                message: String::new(),
            });
            span.events = vec![max_event.clone()];
            span.links = vec![short_link.clone()];
            span
        })
        .collect();
    one_resource_push(port, "checkout", spans, "T-W11 push");
    not_in_the_old_table(&admin, db.name(), &hex32, 2).await;

    let (fetched, prefix) = fetch_trace(port, &hex32, "", "T-W11");
    the_new_statements_ran(&admin, db.name(), &prefix).await;
    let got = fetched_spans(&fetched);
    assert_eq!(got.len(), 2, "two spans, differing in their span ids");
    let by_id: BTreeMap<String, &Span> = got.iter().map(|s| (hex(&s.span_id), *s)).collect();

    for (n, value) in [(0x01u8, -1i32), (0x02u8, 300i32)] {
        let span = by_id
            .get(&hex(&span_id_of(n)))
            .unwrap_or_else(|| panic!("a span {n:#04x}"));
        // row 7a — the kind. The unfixed writer answers `0`.
        assert_eq!(
            span.kind, value,
            "the protocol's own signed kind, verbatim — the unfixed writer answers 0, \
             which is a legal unspecified kind"
        );
        // row 12 — the status code. The unfixed writer answers `0`.
        assert_eq!(
            span.status.as_ref().expect("a present status").code,
            value,
            "the protocol's own signed status, verbatim — the unfixed writer answers 0, \
             which is a legal unset status"
        );
        // row 11 — the event time. The unfixed writer saturates to the
        // SIGNED maximum, which is a legal timestamp.
        assert_eq!(span.events.len(), 1);
        assert_eq!(
            span.events[0].time_unix_nano,
            u64::MAX,
            "an event time above the signed maximum is stored unsaturated"
        );
        // row 7b — the link's ids. The unfixed writer answers all zeros,
        // which is a legal reference to a span that does not exist.
        assert_eq!(span.links.len(), 1);
        assert_eq!(
            span.links[0].trace_id,
            vec![0xaa, 0xbb, 0xcc, 0xdd],
            "four bytes, as sent — neither truncated nor padded nor zeroed"
        );
        assert_eq!(span.links[0].span_id, vec![0x11, 0x22, 0x33], "and three");
    }
}

// ---------------------------------------------------------------------
// T-Q3 — the statement body against the stored bytes.
// ---------------------------------------------------------------------

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq)]
struct LengthsRow {
    spans: u64,
    resources: u64,
}

/// The statement's own RowBinary body length, measured by re-issuing the
/// text production recorded.
///
/// **Why re-issue rather than read a `query_log` column.** `result_bytes`
/// is the server's own accounting and nothing here establishes that it
/// equals the formatted RowBinary body for this shape. Re-issuing the
/// rewritten text measures the quantity the requirement names with no
/// claim about a column's meaning. What it gives up: it measures the
/// statement, not that request's execution of it, and the text differs
/// from production's by exactly the appended clause the absence assertion
/// below makes visible.
///
/// **The absence assertion is the right check.** The driver sends the
/// format as a URL parameter, never appended to the SQL, so production SQL
/// carries no format token — and asserting the ABSENCE means a future
/// change that starts appending one reddens this case rather than silently
/// changing the quantity measured.
async fn statement_body_bytes(admin: &ChClient, recorded: &str) -> u64 {
    assert!(
        !recorded.contains("FORMAT "),
        "production SQL carries no format token — the driver sends it as a URL \
         parameter — so appending one here is what makes the measured quantity the \
         statement's own body:\n{recorded}"
    );
    // `length(...)` over the RowBinary body is not expressible, so the
    // body is measured by the driver: one statement, one row, its bytes.
    // `reinterpretAsString` of an aggregate state would not be the same
    // quantity, so the body is measured as the sum of every projected
    // value's own `byteSize`, which is what RowBinary writes.
    let sql = format!(
        "SELECT sum(b) AS n FROM (SELECT arrayJoin([byteSize(spans), byteSize(resources)]) AS b \
         FROM ({recorded}))"
    );
    ch_count_with(admin, &sql, "final = 1").await
}

async fn ch_count_with(client: &ChClient, sql: &str, settings: &str) -> u64 {
    let full = format!("{sql} SETTINGS {settings}");
    let mut stream = client
        .query_stream::<ChCount>(&full, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\nSQL:\n{full}"));
    stream
        .next()
        .await
        .unwrap_or_else(|| panic!("no row for:\n{full}"))
        .unwrap_or_else(|e| panic!("decode row: {e}\nSQL:\n{full}"))
        .n
}

/// The two array cardinalities one statement returns, read by wrapping
/// the recorded text — legitimate because both statements return exactly
/// one row.
async fn statement_lengths(admin: &ChClient, recorded: &str) -> LengthsRow {
    let sql = format!(
        "SELECT toUInt64(length(spans)) AS spans, toUInt64(length(resources)) AS resources \
         FROM ({recorded}) SETTINGS final = 1"
    );
    let mut stream = admin
        .query_stream::<LengthsRow>(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\nSQL:\n{sql}"));
    stream
        .next()
        .await
        .unwrap_or_else(|| panic!("no row for:\n{sql}"))
        .unwrap_or_else(|e| panic!("decode row: {e}\nSQL:\n{sql}"))
}

/// The corrected denominator: the trace's span rows **plus the distinct
/// resource rows the statement returns**, both finalised.
///
/// **Why the resource term belongs in it.** The statement returns spans
/// AND resources, so a denominator over the span rows alone measures a
/// narrower quantity than the thing it bounds — and for a one-span trace
/// whose resource is larger than its span a CORRECT statement exceeds the
/// published ceiling. The predicate is the full `(service, resource_id)`
/// sort key, and `final = 1` collapses one identity's day rows to one,
/// which is exactly the set the statement's own grouping renders. The two
/// sides count the same rows.
///
/// **It is a check and it changed nothing about what the fetch returns.**
async fn stored_bytes_denominator(admin: &ChClient, db: &str, hex: &str) -> u64 {
    let spans = ch_count_with(
        admin,
        &format!("SELECT sum(byteSize(*)) AS n FROM {db}.spans WHERE trace_id = unhex('{hex}')"),
        "final = 1",
    )
    .await;
    let resources = ch_count_with(
        admin,
        &format!(
            "SELECT sum(byteSize(*)) AS n FROM {db}.resources WHERE (service, resource_id) IN (\
               SELECT service, resource_id FROM {db}.spans WHERE trace_id = unhex('{hex}')\
             )"
        ),
        "final = 1",
    )
    .await;
    spans + resources
}

/// Twenty stored attributes of 64 bytes each, under keys other than the
/// service name so they are kept in the attribute column rather than
/// skipped.
fn fat_resource_attrs(tag: u8) -> Vec<KeyValue> {
    let mut attrs = vec![kv_str("service.name", &format!("svc-{tag}"))];
    for i in 0..20u8 {
        attrs.push(kv_str(&format!("r.pad.{tag}.{i:02}"), &"x".repeat(64)));
    }
    attrs
}

/// `T-Q3`: **the statement's body is within twice the stored bytes, and
/// each resource is shipped ONCE.**
///
/// ## The numerator is the statement's body, not the API envelope
///
/// The requirement bounds the statement's returned bytes. The envelope is
/// a different quantity by design: the response emits one group per span,
/// so one resource's attributes repeat per span in the response while the
/// statement ships each resource once. With the envelope as numerator a
/// correct statement can fail on a trace with many spans over one
/// resource, and an over-fetching one can pass.
///
/// ## Why the fixture is shaped the way it is
///
/// With `s` one span tuple's bytes and `r` one resource tuple's, forty
/// spans and four resources:
///
/// ```text
///    correct      = 40s + 4r
///    the defect   = 40s + 40r          (a resource shipped once per span)
///    the ceiling  = 2 * (40s + 4r)
///    the defect is inside the ceiling  <=>  r(40 - 8) <= 40s  <=>  r <= 1.25 s
/// ```
///
/// **So the ratio assertion discriminates only when `r > 1.25 s`**, which
/// is why each resource carries twenty stored attributes of 64 bytes
/// against each span's handful. The case STATES the threshold rather than
/// relying on the proportion staying as written.
///
/// **And the cardinality is asserted directly**, which discriminates
/// whatever `r` and `s` turn out to be. That assertion is the
/// discriminator; the ratio is the requirement's own bound, and the case
/// says which is which.
#[tokio::test(flavor = "multi_thread")]
async fn t_q3_the_fetch_statement_body_is_within_twice_the_stored_bytes() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_733;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_fetch_bytes_bound_it")).await;
    let _server = spawn_ready(port, &db);
    let admin = ch_admin().await;
    hold_the_fetch_tables(&admin, db.name()).await;

    let tid = trace_id(0x04);
    let hex32 = hex(&tid);
    // Four resources, ten spans each — forty spans, four identities.
    push(
        port,
        (0u8..4)
            .map(|tag| ResourceSpans {
                resource: Some(Resource {
                    attributes: fat_resource_attrs(tag),
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "live-scope".to_string(),
                        ..Default::default()
                    }),
                    spans: (0u8..10)
                        .map(|i| {
                            let n = tag * 10 + i;
                            let mut span = plain_span(
                                tid,
                                span_id_of(n + 1),
                                ANCHOR_NS + n as i64 * 1_000_000,
                            );
                            span.attributes = (0..5)
                                .map(|k| kv_str(&format!("s.p{k}"), &"y".repeat(16)))
                                .collect();
                            span
                        })
                        .collect(),
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            })
            .collect(),
        "T-Q3 push",
    );
    not_in_the_old_table(&admin, db.name(), &hex32, 40).await;

    let (fetched, prefix) = fetch_trace(port, &hex32, "", "T-Q3");
    the_new_statements_ran(&admin, db.name(), &prefix).await;
    assert_eq!(
        fetched_span_ids(&fetched).len(),
        40,
        "the answer first: forty spans"
    );
    let recorded = recorded_statement(&admin, db.name(), &format!("{prefix}-1"))
        .await
        .expect("statement 1 is recorded");

    // Assertion 2 — THE DISCRIMINATOR, and it does not depend on sizes at
    // all: a per-span resource shipping returns 40 and 40.
    assert_eq!(
        statement_lengths(&admin, &recorded).await,
        LengthsRow {
            spans: 40,
            resources: 4
        },
        "forty span tuples and FOUR resource tuples — each resource shipped once"
    );

    // Assertion 1 — the requirement's own bound, over the corrected
    // denominator.
    let numerator = statement_body_bytes(&admin, &recorded).await;
    let denominator = stored_bytes_denominator(&admin, db.name(), &hex32).await;
    assert!(
        denominator > 0,
        "the denominator must be measured over a populated trace, got 0"
    );
    assert!(
        numerator as f64 <= 2.0 * denominator as f64,
        "the statement's body is {numerator} bytes against a stored {denominator}, a \
         ratio of {:.3} — the published ceiling is 2x",
        numerator as f64 / denominator as f64
    );

    // And the fixture's own shape, stated rather than left to proportion:
    // the resource tuple must be more than 1.25x the span tuple, or the
    // ratio assertion above cannot see a tenfold duplication.
    let one_span = ch_count_with(
        &admin,
        &format!(
            "SELECT sum(byteSize(*)) AS n FROM {db}.spans \
             WHERE trace_id = unhex('{hex32}') AND span_id = unhex('0101010101010101')",
            db = db.name()
        ),
        "final = 1",
    )
    .await;
    let one_resource = ch_count_with(
        &admin,
        &format!(
            "SELECT sum(byteSize(*)) AS n FROM {db}.resources WHERE service = 'svc-0'",
            db = db.name()
        ),
        "final = 1",
    )
    .await;
    assert!(
        one_resource as f64 > 1.25 * one_span as f64,
        "the fixture must satisfy r > 1.25 s for the ratio to discriminate, got \
         r = {one_resource} and s = {one_span}"
    );
}

/// `T-Q3`, the truncated-set route: **statement 1's span array is EMPTY
/// on it, so the two bodies carry one copy of the trace between them.**
///
/// The span read carries a conjunct over the stored set's own length, and
/// that length is bound in a `WITH` before the read — so the conjunct is a
/// constant at analysis time. With a complete set it is true and nothing
/// changes; with a set that may have truncated it is false, the span read
/// matches no row, and the array is empty. **Nothing is discarded because
/// nothing was returned.**
///
/// A direct insert into the per-trace table, because 4,096 distinct
/// buckets is not a state a push can be asked to build — and **all three**
/// of the fields the statements read are supplied, because with the extent
/// left at zero the complete-predicate statement's span range is `[0, 0]`,
/// every span falls outside it, and the case passes with an EMPTY answer.
#[tokio::test(flavor = "multi_thread")]
async fn t_q3_the_truncated_set_route_suppresses_the_discarded_array() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_734;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_fetch_suppression_it")).await;
    let _server = spawn_ready(port, &db);
    let admin = ch_admin().await;
    hold_the_fetch_tables(&admin, db.name()).await;
    let data = ChClient::new(live_db::conn_config(db.name()))
        .await
        .expect("connect a data client");

    let tid = trace_id(0x0a);
    let hex32 = hex(&tid);
    // The stored set is exactly at the cap, starting at the anchor's
    // bucket; the extent spans those 4,096 buckets' first and last
    // nanosecond.
    let first_bucket = ANCHOR_BUCKET;
    let lo = first_bucket * BUCKET_NS;
    let hi = (first_bucket + 4096) * BUCKET_NS - 1;
    ch_exec(
        &data,
        &format!(
            "INSERT INTO {db}.traces (day, trace_id, start_ns, last_start_ns, buckets) VALUES \
             (toDate(fromUnixTimestamp64Nano({lo}), 'UTC'), unhex('{hex32}'), {lo}, {hi}, \
              range({first_bucket}, {past}))",
            db = db.name(),
            past = first_bucket + 4096,
        ),
    )
    .await;
    // Forty spans inside the FIRST of those buckets, and four resources
    // on the UTC day those bounds fall in, with the same `r > s` shape as
    // the case above.
    for n in 0u8..40 {
        let tag = n / 10;
        ch_exec(
            &data,
            &format!(
                "INSERT INTO {db}.spans \
                 (trace_id, span_id, parent_span_id, start_ns, end_ns, service, \
                  resource_id, name, kind, status_code) VALUES \
                 (unhex('{hex32}'), unhex('{sid}'), toFixedString('', 8), {start}, \
                  {end}, 'svc-{tag}', {rid}, 'op', 2, 0)",
                db = db.name(),
                sid = hex(&span_id_of(n + 1)),
                start = lo + n as i64 * 1_000_000,
                end = lo + n as i64 * 1_000_000 + 1_000_000,
                rid = tag as u32 + 1,
            ),
        )
        .await;
    }
    for tag in 0u8..4 {
        let attrs: Vec<String> = (0..20u8)
            .map(|i| format!("\"r.pad.{tag}.{i:02}\":\"{}\"", "x".repeat(64)))
            .collect();
        ch_exec(
            &data,
            &format!(
                "INSERT INTO {db}.resources (day, resource_id, service, attrs, schema_url) \
                 VALUES (toDate(fromUnixTimestamp64Nano({lo}), 'UTC'), {rid}, 'svc-{tag}', \
                 '{{{json}}}', '')",
                db = db.name(),
                rid = tag as u32 + 1,
                json = attrs.join(","),
            ),
        )
        .await;
    }

    let (fetched, prefix) = fetch_trace(port, &hex32, "", "the truncated route");
    let first = recorded_statement(&admin, db.name(), &format!("{prefix}-1"))
        .await
        .expect("statement 1 is recorded");

    // (a) THE SUPPRESSION, and it is what makes the bound hold.
    assert_eq!(
        statement_lengths(&admin, &first).await,
        LengthsRow {
            spans: 0,
            resources: 0
        },
        "(a) statement 1's arrays are EMPTY on this route — the resource subquery \
         filters on the span read's own keys, which are empty too"
    );

    // (b) the route.
    assert_eq!(
        statement_ids(&admin, db.name(), &prefix).await,
        BTreeSet::from([format!("{prefix}-1"), format!("{prefix}-2")]),
        "(b) two statements"
    );
    let second = recorded_statement(&admin, db.name(), &format!("{prefix}-2"))
        .await
        .expect("statement 1w is recorded");
    assert!(
        second.contains("min(start_ns)") && !second.contains("arrayJoin(bk)"),
        "(b) the second statement reads its own bounds and carries no bucket \
         condition at all:\n{second}"
    );

    // (b2) THE ANSWER. A case that measures a body without asserting what
    // is in it is measuring the wrong thing.
    assert_eq!(
        fetched_span_ids(&fetched),
        (0u8..40).map(|n| hex(&span_id_of(n + 1))).collect(),
        "(b2) all forty spans come back"
    );

    // (b3) three different numbers over one answer, asserted separately.
    assert_eq!(
        fetched.resource_spans.len(),
        40,
        "(b3)(i) one ResourceSpans per span, whatever the resources are"
    );
    let distinct_resources: BTreeSet<Vec<u8>> = fetched
        .resource_spans
        .iter()
        .map(|rs| {
            rs.resource
                .as_ref()
                .expect("a present resource")
                .encode_to_vec()
        })
        .collect();
    assert_eq!(
        distinct_resources.len(),
        4,
        "(b3)(ii) four DISTINCT resource values across those forty groups"
    );
    assert_eq!(
        statement_lengths(&admin, &second).await,
        LengthsRow {
            spans: 40,
            resources: 4
        },
        "(b3)(iii) and the second statement's own resource array holds four tuples — \
         which is what `shipped once per resource` actually means"
    );

    // (c) the requirement, over the SUM of the two bodies and over THIS
    // fixture's own trace id. Taken even though (a) makes it near
    // certain, because (a) asserts a mechanism and (c) asserts the
    // requirement.
    let numerator =
        statement_body_bytes(&admin, &first).await + statement_body_bytes(&admin, &second).await;
    let denominator = stored_bytes_denominator(&admin, db.name(), &hex32).await;
    assert!(
        denominator > 0,
        "the denominator must be measured over a populated trace, got 0"
    );
    assert!(
        numerator as f64 <= 2.0 * denominator as f64,
        "the two bodies together are {numerator} bytes against a stored {denominator}, \
         a ratio of {:.3} — the published ceiling is 2x, and the two bodies carry ONE \
         copy of the trace between them",
        numerator as f64 / denominator as f64
    );
}
