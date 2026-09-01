//! Issue #479 — the matched-span PROJECTION differential: a real
//! two-system comparison of what a matched span object carries.
//!
//! **One corpus, two ingests, identical bytes.** The OTLP/JSON body is
//! built ONCE. Those exact bytes are pushed to the reference's OTLP
//! receiver and decoded by OUR OWN ingest parser
//! (`pulsus_write::protocols::otlp_traces::decode_json` +
//! `parse_traces`) into the real trace writer, so neither side gets a
//! payload hand-written for it. A differential whose two sides are
//! written separately compares two authors, not two systems.
//!
//! **What is compared, per case, per span:** the MULTISET of projected
//! attribute keys, and the `name`-present flag. Values are compared for
//! the cases whose reference value is a `stringValue`; the three cases
//! whose reference value is typed (`http.status_code`, `nestedSetLeft`,
//! `duration_ms` — all `intValue`) are KEY-compared only, because value
//! typing is a separate issue and a wrong number rendering would pass
//! this leg. That limit is stated rather than implied.
//!
//! **The validity gate is direction-neutral and runs BEFORE the
//! comparison.** Two empty multisets are equal, so a case that runs
//! before either side has indexed the corpus would compare nothing and
//! pass green — precisely the failure mode a reference that serves a
//! trace by ID while its SEARCH route still answers `{"traces":[]}`
//! produces. So the reference's SEARCH route is polled until the fixture
//! trace comes back, and every case asserts BOTH sides returned at least
//! one span before anything is compared.
//!
//! Gate: skips unless `PULSUS_TEST_CLICKHOUSE=1` AND
//! `PULSUSDB_PROJECTION_DIFF_URL` (the reference's search API base) AND
//! `PULSUSDB_PROJECTION_OTLP_URL` (its OTLP HTTP base) are set. All three
//! are FAIL-CLOSED in a live CI job: dropping the `env:` block panics
//! rather than skipping green.
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=19124 \
//!   PULSUSDB_PROJECTION_DIFF_URL=http://localhost:3200 \
//!   PULSUSDB_PROJECTION_OTLP_URL=http://localhost:4318 \
//!   cargo test -p pulsus-read --test traces_search_projection_differential -- --nocapture
//! ```
//!
//! Clean-room: no reference source, grammar or test corpus is read — the
//! fixture is our own authorship and the reference's answers are read
//! back as black-box runtime output.

use std::collections::BTreeMap;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_config::WriterConfig;
use pulsus_read::traces::search_plan::{SearchParams, plan_search};
use pulsus_read::{TraceEngine, TraceReadConfig};
use pulsus_schema::{RenderCtx, run_init};
use pulsus_write::TraceSink;
use pulsus_write::writer::{ChBlockInserter, TraceWriter, TraceWriterTables};

/// One differential case: the query, and whether its reference value is a
/// `stringValue` (so the VALUES are compared too, not only the keys).
struct ProjectionCase {
    name: &'static str,
    q: &'static str,
    compare_values: bool,
}

const fn c(name: &'static str, q: &'static str) -> ProjectionCase {
    ProjectionCase {
        name,
        q,
        compare_values: true,
    }
}

const fn keys_only(name: &'static str, q: &'static str) -> ProjectionCase {
    ProjectionCase {
        name,
        q,
        compare_values: false,
    }
}

/// The registry — one entry per numbered row of the issue's query table.
/// The length is asserted at compile time so the count cannot drift from
/// the table it describes.
const CASES: [ProjectionCase; 30] = [
    c("01_name_eq", r#"{ name = "GET /pay" }"#),
    c("02_duration_gt", "{ duration > 1s }"),
    c("03_match_all", "{}"),
    c(
        "04_service_name",
        r#"{ resource.service.name = "proj-checkout" }"#,
    ),
    c("05_status_error", "{ status = error }"),
    c("06_span_http_method", r#"{ span.http.method = "GET" }"#),
    c(
        "07_disjunction_per_span",
        r#"{ name = "slow-op" || span.http.method = "GET" }"#,
    ),
    c(
        "08_scope_collision",
        r#"{ span.foo = "S-span" && resource.foo = "R-resource" }"#,
    ),
    c("09_unscoped_foo", r#"{ .foo = "S-span" }"#),
    keys_only("10_status_code_num", "{ span.http.status_code >= 500 }"),
    c("11_method_regex", r#"{ span.http.method =~ "GE.*" }"#),
    c("12_empty_value", r#"{ span.note = "" }"#),
    c("13_non_ascii_value", r#"{ span.city = "München" }"#),
    c("14_select_attr", "{} | select(span.http.method)"),
    c("15_select_name", "{} | select(name)"),
    c("16_select_duration", "{} | select(duration)"),
    c(
        "17_condition_and_select_same_field",
        r#"{ span.http.method = "GET" } | select(span.http.method)"#,
    ),
    c("18_status_message", r#"{ statusMessage = "boom" }"#),
    c("19_kind_client", "{ kind = client }"),
    c("20_parent_id", r#"{ span:parentID = "a479000000000001" }"#),
    c(
        "21_instrumentation_name",
        r#"{ instrumentation:name = "proj-scope" }"#,
    ),
    c(
        "22_event_scoped_attr",
        r#"{ event.exception.type = "IOError" }"#,
    ),
    c("23_event_name", r#"{ event:name = "exception" }"#),
    c("24_link_span_id", r#"{ link:spanID = "0a1b2c3d4e5f6071" }"#),
    keys_only("25_nested_set_left", "{ nestedSetLeft > 0 }"),
    c("26_trace_duration", "{ traceDuration > 1s }"),
    keys_only(
        "27_single_field_arithmetic",
        "{ span.duration_ms * 1000 > 5000 }",
    ),
    c("28_key_existence", "{ span.http.method != nil }"),
    c("29_root_name", r#"{ rootName = "GET /pay" }"#),
    c("30_name_neq_empty", r#"{ name != "" }"#),
];

const _: () = assert!(CASES.len() == 30);

// ---------------------------------------------------------------------------
// the corpus — built ONCE, as OTLP/JSON bytes
// ---------------------------------------------------------------------------

const TRACE_HEX: &str = "a4790000000000000000000000000001";
const S_PAY: &str = "a479000000000001";
const S_CHARGE: &str = "a479000000000002";
const S_CONFIRM: &str = "a479000000000011";
const S_SLOW: &str = "a479000000000021";
const LINK_SPAN: &str = "0a1b2c3d4e5f6071";

fn kv(key: &str, value: serde_json::Value) -> serde_json::Value {
    serde_json::json!({"key": key, "value": value})
}

fn s(v: &str) -> serde_json::Value {
    serde_json::json!({"stringValue": v})
}

fn i(v: i64) -> serde_json::Value {
    serde_json::json!({"intValue": v.to_string()})
}

/// The OTLP/JSON body both systems ingest, byte for byte.
fn corpus(base_ns: i64) -> Vec<u8> {
    let ns = |off: i64| (base_ns + off).to_string();
    let sec = 1_000_000_000i64;
    let pay = serde_json::json!({
        "traceId": TRACE_HEX,
        "spanId": S_PAY,
        "name": "GET /pay",
        "kind": 1,
        "startTimeUnixNano": ns(0),
        "endTimeUnixNano": ns(2 * sec),
        "status": {"code": 2, "message": "boom"},
        "attributes": [
            kv("http.method", s("GET")),
            kv("http.method.raw", s("GET")),
            kv("http.status_code", i(500)),
            kv("foo", s("S-span")),
            kv("note", s("")),
            kv("city", s("München")),
            kv("duration_ms", i(8)),
        ],
        "events": [{
            "name": "exception",
            "timeUnixNano": ns(sec / 2),
            "attributes": [kv("exception.type", s("IOError"))],
        }],
        "links": [{
            "traceId": TRACE_HEX,
            "spanId": LINK_SPAN,
            "attributes": [kv("relation", s("child_of"))],
        }],
    });
    let charge = serde_json::json!({
        "traceId": TRACE_HEX,
        "spanId": S_CHARGE,
        "parentSpanId": S_PAY,
        "name": "charge",
        "kind": 3,
        "startTimeUnixNano": ns(sec / 10),
        "endTimeUnixNano": ns(sec / 10 + sec / 2),
        "attributes": [kv("http.method", s("POST"))],
    });
    let confirm = serde_json::json!({
        "traceId": TRACE_HEX,
        "spanId": S_CONFIRM,
        "parentSpanId": S_PAY,
        "name": "GET /pay/confirm",
        "kind": 1,
        "startTimeUnixNano": ns(sec / 5),
        "endTimeUnixNano": ns(sec / 5 + 3 * sec / 2),
        "attributes": [kv("http.method", s("GET"))],
    });
    let slow = serde_json::json!({
        "traceId": TRACE_HEX,
        "spanId": S_SLOW,
        "parentSpanId": S_PAY,
        "name": "slow-op",
        "kind": 1,
        "startTimeUnixNano": ns(sec / 4),
        "endTimeUnixNano": ns(sec / 4 + 3 * sec),
        "attributes": [kv("http.method", s("DELETE"))],
    });
    let scope = serde_json::json!({"name": "proj-scope", "version": "1.2.3"});
    serde_json::to_vec(&serde_json::json!({
        "resourceSpans": [
            {
                "resource": {"attributes": [
                    kv("service.name", s("proj-checkout")),
                    kv("foo", s("R-resource")),
                ]},
                "scopeSpans": [{"scope": scope, "spans": [pay, charge, confirm]}],
            },
            {
                "resource": {"attributes": [kv("service.name", s("proj-db"))]},
                "scopeSpans": [{"scope": scope, "spans": [slow]}],
            },
        ]
    }))
    .expect("serialise the corpus")
}

// ---------------------------------------------------------------------------
// ClickHouse / our side
// ---------------------------------------------------------------------------

fn ch_config(database: &str) -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: database.to_string(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(20),
        ..ChConnConfig::default()
    }
}

fn engine_config() -> TraceReadConfig {
    TraceReadConfig {
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        spans_table: "trace_spans".to_string(),
        attrs_table: "trace_attrs_idx".to_string(),
        edges_table: "trace_edges".to_string(),
        max_candidates: 100_000,
        scan_budget_rows: 50_000_000,
        max_series: 1_000,
        generator_max_memory_bytes: 536_870_912,
        distributed: false,
        skip_unavailable_shards: false,
    }
}

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .unwrap_or_else(|e| panic!("execute failed: {e}\nSQL:\n{sql}"));
}

async fn init_db(bootstrap: &ChClient, db: &str) {
    exec(bootstrap, &format!("DROP DATABASE IF EXISTS {db}")).await;
    run_init(
        bootstrap,
        &RenderCtx {
            db: db.to_string(),
            cluster: None,
            dist_suffix: "_dist".to_string(),
            storage_policy: None,
            retention_days: 7,
            log_rollup: Duration::from_secs(5),
        },
    )
    .await
    .expect("run_init");
}

/// One matched span's projected shape: the ordered `(key, value)` pairs
/// and whether the object carried a `name`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Projected {
    /// SORTED, so the comparison is a MULTISET: the reference's attribute
    /// order is not stable (six consecutive identical requests returned
    /// six distinct orders), and ours is deterministic. Order is a ledger
    /// row, not a parity claim.
    attrs: Vec<(String, String)>,
    name: Option<String>,
}

impl Projected {
    fn keys(&self) -> Vec<&str> {
        self.attrs.iter().map(|(k, _)| k.as_str()).collect()
    }
}

/// Our side: the REAL two-phase executor, read back as span-id hex ->
/// projected shape.
async fn ours(engine: &TraceEngine, q: &str, window: (i64, i64)) -> BTreeMap<String, Projected> {
    let query = pulsus_traceql::parse(q).unwrap_or_else(|e| panic!("parse {q:?}: {e}"));
    let plan = plan_search(
        &query,
        &SearchParams {
            start_ns: window.0,
            end_ns: window.1,
            limit: 100,
            spss: 100,
        },
        &engine.search_ctx(),
    )
    .unwrap_or_else(|e| panic!("plan {q:?}: {e}"));
    let out = engine
        .search(&plan)
        .await
        .unwrap_or_else(|e| panic!("search {q:?}: {e}"));
    let mut map = BTreeMap::new();
    for t in &out.traces {
        for s in &t.spans {
            let mut attrs: Vec<(String, String)> = s
                .attributes
                .iter()
                .map(|a| (a.key().to_string(), a.value().to_string()))
                .collect();
            attrs.sort();
            map.insert(
                s.span_id.iter().map(|b| format!("{b:02x}")).collect(),
                Projected {
                    attrs,
                    name: s.name().map(str::to_string),
                },
            );
        }
    }
    map
}

// ---------------------------------------------------------------------------
// the reference side
// ---------------------------------------------------------------------------

fn otlp_push(otlp_base: &str, body: &[u8]) {
    let url = format!("{}/v1/traces", otlp_base.trim_end_matches('/'));
    let out = Command::new("curl")
        .args([
            "-s",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "--max-time",
            "20",
            "-X",
            "POST",
            "-H",
            "Content-Type: application/json",
            "--data-binary",
            "@-",
        ])
        .arg(&url)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write as _;
            child.stdin.as_mut().expect("stdin piped").write_all(body)?;
            child.wait_with_output()
        })
        .expect("curl on PATH");
    let code = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        code.trim(),
        "200",
        "OTLP push to {url} failed (http {code})"
    );
}

/// The reference's answer for one query, as span-id hex -> projected
/// shape. `None` when the request itself did not produce a `traces` array.
fn reference(api_base: &str, q: &str) -> Option<BTreeMap<String, Projected>> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    let url = format!("{}/api/search", api_base.trim_end_matches('/'));
    let out = Command::new("curl")
        .args(["-s", "-G", "--max-time", "20"])
        .args(["--data-urlencode", &format!("q={q}")])
        .args(["--data-urlencode", &format!("start={}", now - 7200)])
        .args(["--data-urlencode", &format!("end={}", now + 120)])
        .args(["--data-urlencode", "limit=100"])
        .args(["--data-urlencode", "spss=100"])
        .arg(&url)
        .output()
        .expect("curl on PATH");
    let body: serde_json::Value = serde_json::from_slice(&out.stdout).ok()?;
    let traces = body.get("traces")?.as_array()?;
    let mut map = BTreeMap::new();
    for t in traces {
        // The reference strips leading zero bytes from the traceID; match
        // on the trimmed hex suffix, with the exact form as the fallback
        // for an id that carries no leading zero.
        let tid = t.get("traceID").and_then(|v| v.as_str()).unwrap_or("");
        if !TRACE_HEX.trim_start_matches('0').ends_with(tid) && tid != TRACE_HEX {
            continue;
        }
        let sets = t
            .get("spanSets")
            .and_then(|v| v.as_array())
            .cloned()
            .or_else(|| t.get("spanSet").map(|v| vec![v.clone()]))
            .unwrap_or_default();
        for set in sets {
            for span in set
                .get("spans")
                .and_then(|v| v.as_array())
                .into_iter()
                .flatten()
            {
                let Some(id) = span.get("spanID").and_then(|v| v.as_str()) else {
                    continue;
                };
                let mut attrs: Vec<(String, String)> = span
                    .get("attributes")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .map(|kv| {
                                let key = kv
                                    .get("key")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or_default()
                                    .to_string();
                                let value = kv.get("value").map(untyped_value).unwrap_or_default();
                                (key, value)
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                attrs.sort();
                map.insert(
                    id.to_string(),
                    Projected {
                        attrs,
                        name: span
                            .get("name")
                            .and_then(|v| v.as_str())
                            .map(str::to_string),
                    },
                );
            }
        }
    }
    Some(map)
}

/// The reference's attribute value as TEXT, whatever wire type carries
/// it. Typing is a separate issue; this leg compares the KEY for every
/// case and the VALUE only for the cases whose reference value is a
/// `stringValue`, so this normalisation never hides a typing difference
/// the value comparison would have caught.
fn untyped_value(v: &serde_json::Value) -> String {
    for field in ["stringValue", "intValue"] {
        if let Some(s) = v.get(field).and_then(|x| x.as_str()) {
            return s.to_string();
        }
        if let Some(n) = v.get(field).and_then(|x| x.as_i64()) {
            return n.to_string();
        }
    }
    if let Some(f) = v.get("doubleValue").and_then(|x| x.as_f64()) {
        return f.to_string();
    }
    if let Some(b) = v.get("boolValue").and_then(|x| x.as_bool()) {
        return b.to_string();
    }
    String::new()
}

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64")
}

// ---------------------------------------------------------------------------
// the differential
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn traces_search_projection_differential() {
    // FAIL-CLOSED on all three: dropping any `env:` block from this
    // suite's CI step PANICS rather than skipping green. The endpoint
    // gates go through `require_live_endpoint_gate`, not the boolean one —
    // a URL-valued gate read by the boolean rule looks "not set" while the
    // `env:` block is right there in the log.
    let api_gate = pulsus_testkit::require_live_endpoint_gate("PULSUSDB_PROJECTION_DIFF_URL");
    let otlp_gate = pulsus_testkit::require_live_endpoint_gate("PULSUSDB_PROJECTION_OTLP_URL");
    if !(api_gate.is_running()
        && otlp_gate.is_running()
        && pulsus_testkit::live_clickhouse_enabled())
    {
        eprintln!(
            "skipping the matched-span projection differential — set PULSUS_TEST_CLICKHOUSE=1, \
             PULSUSDB_PROJECTION_DIFF_URL and PULSUSDB_PROJECTION_OTLP_URL."
        );
        return;
    }
    let api_base = std::env::var("PULSUSDB_PROJECTION_DIFF_URL").expect("gate is running");
    let otlp_base = std::env::var("PULSUSDB_PROJECTION_OTLP_URL").expect("gate is running");

    let base = now_ns() - 60_000_000_000;
    let window = (base - 60_000_000_000, base + 600_000_000_000);
    let body = corpus(base);

    // The reference first, so it has the whole poll window to index.
    otlp_push(&otlp_base, &body);

    // Our side: the SAME bytes through our own ingest parser and the real
    // trace writer.
    let bootstrap = ChClient::new(ch_config("default"))
        .await
        .expect("connect bootstrap");
    let db = &pulsus_testkit::test_db("pulsus_projdiff_it");
    init_db(&bootstrap, db).await;
    let client = Arc::new(ChClient::new(ch_config(db)).await.expect("connect db"));
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;
    let writer = TraceWriter::with_inserters_with_tables(
        Arc::new(ChBlockInserter::new(client.clone())),
        Arc::new(ChBlockInserter::new(client.clone())),
        &cfg,
        TraceWriterTables::traces_default(),
    );
    let req = pulsus_write::protocols::otlp_traces::decode_json(&body)
        .expect("our own ingest decodes the same body the reference got");
    let parsed =
        pulsus_write::parse_traces(&req, base).expect("our own ingest parses the same body");
    assert_eq!(parsed.spans.len(), 4, "the corpus is four spans");
    let wait = writer.admit_flush(parsed).expect("queue has room");
    tokio::time::timeout(Duration::from_secs(20), wait)
        .await
        .expect("flush settles")
        .expect("the corpus commits");

    let engine = TraceEngine::new(
        ChClient::new(ch_config(db)).await.expect("connect engine"),
        engine_config(),
    );

    // ---- the validity gate, before any comparison ---------------------
    // The reference will serve a trace by ID while its SEARCH route still
    // answers `{"traces":[]}` for the same trace in the same window, so
    // the SEARCH route is what is polled. A case issued before this goes
    // green compares two empty maps and passes having compared nothing.
    let mut ready = false;
    for _ in 0..60 {
        if reference(&api_base, "{}").is_some_and(|m| !m.is_empty()) {
            ready = true;
            break;
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    assert!(
        ready,
        "the reference never returned trace {TRACE_HEX} on its SEARCH route within the poll \
         budget — every case below would have compared two empty answers and passed"
    );
    // The same gate for our side, so neither direction can be the empty
    // one.
    let mut ours_ready = false;
    for _ in 0..60 {
        if !ours(&engine, "{}", window).await.is_empty() {
            ours_ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(ours_ready, "our own side never returned the seeded corpus");

    let mut diverged: Vec<String> = Vec::new();
    for case in &CASES {
        let theirs = reference(&api_base, case.q)
            .unwrap_or_else(|| panic!("[{}] the reference returned no traces array", case.name));
        let mine = ours(&engine, case.q, window).await;

        // Per-case validity: BOTH sides must have returned at least one
        // span, else the case failed rather than agreed.
        if theirs.is_empty() || mine.is_empty() {
            diverged.push(format!(
                "[{}] {}: the corpus was not visible — reference {} span(s), ours {} span(s)",
                case.name,
                case.q,
                theirs.len(),
                mine.len()
            ));
            continue;
        }

        let mut mism: Vec<String> = Vec::new();
        let ids: std::collections::BTreeSet<&String> = theirs.keys().chain(mine.keys()).collect();
        for id in ids {
            match (theirs.get(id), mine.get(id)) {
                (Some(t), Some(m)) => {
                    if t.keys() != m.keys() {
                        mism.push(format!(
                            "{id}: attribute keys reference {:?} != ours {:?}",
                            t.keys(),
                            m.keys()
                        ));
                    }
                    if t.name.is_some() != m.name.is_some() {
                        mism.push(format!(
                            "{id}: name presence reference {:?} != ours {:?}",
                            t.name, m.name
                        ));
                    }
                    if t.name.is_some() && t.name != m.name {
                        mism.push(format!(
                            "{id}: name value reference {:?} != ours {:?}",
                            t.name, m.name
                        ));
                    }
                    if case.compare_values && t.attrs != m.attrs {
                        mism.push(format!(
                            "{id}: attribute values reference {:?} != ours {:?}",
                            t.attrs, m.attrs
                        ));
                    }
                }
                (Some(t), None) => mism.push(format!("{id}: only the reference matched ({t:?})")),
                (None, Some(m)) => mism.push(format!("{id}: only we matched ({m:?})")),
                (None, None) => unreachable!("the id came from one of the two maps"),
            }
        }

        if mism.is_empty() {
            eprintln!("[{}] AGREES — {} span(s)", case.name, mine.len());
        } else {
            eprintln!(
                "[{}] {} DIVERGES:\n  {}",
                case.name,
                case.q,
                mism.join("\n  ")
            );
            diverged.push(case.name.to_string());
        }
    }

    // The deliberately-empty case is asserted SEPARATELY and never through
    // the comparison above, so a mutual empty can never read as agreement.
    let empty_q = r#"{ name = "GET /pay" && span.zzz = "nope" }"#;
    assert!(
        reference(&api_base, empty_q).is_some_and(|m| m.is_empty()),
        "{empty_q}: the reference must return no spans"
    );
    assert!(
        ours(&engine, empty_q, window).await.is_empty(),
        "{empty_q}: we must return no spans"
    );

    writer.shutdown(Duration::from_secs(5)).await;
    exec(&bootstrap, &format!("DROP DATABASE IF EXISTS {db}")).await;

    assert!(
        diverged.is_empty(),
        "matched-span projection divergence in {diverged:?} (from REAL output on both sides)"
    );
}
