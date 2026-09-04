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
//! the cases whose reference value is a `stringValue`; the FOUR cases
//! whose reference value is typed (`http.status_code`, `nestedSetLeft`
//! twice — the range form and the same-field form — and `duration_ms`,
//! all `intValue`) are KEY-compared only, because value typing is a
//! separate issue and a wrong number rendering would pass this leg. That
//! limit is stated rather than implied:
//! `pulsus_read::traces::search_plan::tests::the_live_differential_registry_is_well_formed`
//! holds the same four names as data and fails if the registry's
//! keys-only set stops being them.
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
//! **The registry's DOMAIN, and how to tell a shape is missing from it.**
//! A shape is a PAIR drawn from two axes:
//!
//!  * the **value source** a projected entry is filled from — one label
//!    per `ProjectionValue` variant (`value:name`, `value:probe-value`,
//!    `value:select-value`, …);
//!  * the **leaf class** that supplied it — one per projecting arm of
//!    `projection_value` (`leaf:physical`, `leaf:attr`,
//!    `leaf:nested-set`, `leaf:field-compare`) plus the gate-free
//!    `select()` half (`leaf:select`).
//!
//! The registry is required to be complete over a NAMED SET OF PAIRS, not
//! over each axis separately. Covering the axes separately was the wave-3
//! gap: the mandated same-field physical-column case could be removed and
//! both registry guards stayed green, because `leaf:field-compare` was
//! still supplied by the same-field ATTRIBUTE case and `value:service` by
//! the plain service-name case.
//!
//! Everything else here is enumerated but NOT mechanically complete: the
//! seven envelope fields, the scope collision, the empty and non-ASCII
//! values, the numeric wire types and the deliberately-empty case. The
//! classes we knowingly answer differently from the reference (a negated
//! attribute leaf, the genuine multi-field conditions) are deliberately
//! ABSENT — they are pinned by
//! `pulsus_read::traces::search_plan::tests::wave_two_shapes_project_nothing`
//! and carry ledger rows, and putting them here would assert an agreement
//! that does not exist.
//!
//! A reader tells a shape is missing by RUNNING
//! `pulsus_read::traces::search_plan::tests::the_live_differential_registry_covers_every_projection_shape`,
//! which holds the required PAIR set — `REQUIRED_SHAPE_PAIRS`, stated in
//! `crates/pulsus-read/src/traces/search_plan.rs`, where this registry
//! cannot reach it — checks each pair's witness query really produces
//! that pair, and then fails naming every pair no case's `q` is EQUAL to.
//! The two AXES are still closed against the planner's own enums (both
//! label matches are exhaustive, so a new variant fails to compile, and
//! every label must appear in some required pair); the cross product is
//! not, which that constant states as its own limit. That test is why
//! cases 31–35 exist: the 30-case registry contained no same-field
//! `FieldCompare` and nothing reaching `instrumentation:version`, and
//! reading it could not show that. It reads `projection_cases::CASES` as
//! typed data, which this file `mod`s and that test `include!`s; it used
//! to read this file as TEXT, and a witness query written in a case NAME
//! satisfied it while the case exercised nothing (code review wave 2).
//!
//! **Instance isolation.** This suite requires a reference instance that
//! holds NO other suite's corpus, and asserts it below before pushing:
//! its spans move another differential's aggregates (a `compare()` top-N
//! picked up this corpus's `statusMessage`, service and root names), and
//! that failure surfaces in the OTHER suite as an ordinary-looking value
//! mismatch. Two guards, each with its contract written on it:
//! `assert_reference_instance_is_exclusive` (runtime — no trace over the
//! widest window the reference accepts, no tag key at all, and every
//! request fail-closed) and
//! `the_projection_leg_does_not_share_an_instance_with_another_suite`
//! (configuration — instance identity and container lifecycle, not URL
//! strings). The reverse-order half, another suite running against the
//! instance this one has already filled, is caught in that other suite:
//! `compare_value_differential` refuses to run while this corpus's trace
//! is resident.
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

mod projection_cases;

use projection_cases::CASES;

// ---------------------------------------------------------------------------
// the corpus — built ONCE, as OTLP/JSON bytes
// ---------------------------------------------------------------------------

/// This suite's corpus trace id. Declared in `pulsus_testkit` because
/// `compare_value_differential` guards against exactly this residue —
/// see [`pulsus_testkit::PROJECTION_DIFFERENTIAL_TRACE_HEX`].
const TRACE_HEX: &str = pulsus_testkit::PROJECTION_DIFFERENTIAL_TRACE_HEX;
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
                .map(|a| (a.key().to_string(), our_untyped_value(a.value())))
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

/// OUR projected value as the same TEXT [`untyped_value`] renders the
/// reference's wire value to (issue #510 made the projected value typed).
/// Both sides are flattened to text by construction, so this leg keeps
/// comparing what it always compared — see [`untyped_value`] for where
/// the typing is pinned instead.
fn our_untyped_value(v: &pulsus_read::GroupValue) -> String {
    match v {
        pulsus_read::GroupValue::Str(s) => s.clone(),
        pulsus_read::GroupValue::Int(i) => i.to_string(),
        pulsus_read::GroupValue::Double(bits) => f64::from_bits(*bits).to_string(),
        pulsus_read::GroupValue::Bool(b) => b.to_string(),
        pulsus_read::GroupValue::Nil => String::new(),
    }
}

/// The reference's attribute value as TEXT, whatever wire type carries
/// it. This leg compares the KEY for every case and the VALUE only for
/// the cases whose reference value is a `stringValue`, so this
/// normalisation never hides a typing difference the value comparison
/// would have caught.
///
/// **The WIRE ARM is pinned elsewhere** (issue #510): the projected
/// attribute's arm is compared type-tagged against the reference by the
/// `projected_*` fixtures of
/// `tests/traces_search_grouping_differential.rs`. This suite stays
/// untyped on purpose — its own comparison is a multiset over five value
/// SOURCES, and flattening keeps that comparison independent of the arm.
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

/// A GET against the reference, FAIL-CLOSED.
///
/// Every transport failure, every non-200 status and every unparseable
/// body is an `Err` carrying what happened. The exclusivity guard below
/// reads "the instance holds nothing" out of these answers, and the
/// previous version of that read returned an empty vector on any error —
/// a guard that reports success exactly when it cannot see (code review
/// wave 2).
fn reference_get(url: &str, params: &[(&str, String)]) -> Result<serde_json::Value, String> {
    let mut cmd = Command::new("curl");
    cmd.args(["-s", "-G", "--max-time", "20", "-w", "\n%{http_code}"]);
    for (key, value) in params {
        cmd.args(["--data-urlencode", &format!("{key}={value}")]);
    }
    let out = cmd
        .arg(url)
        .output()
        .map_err(|e| format!("curl {url} could not be run: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "curl {url} exited {:?}: {}",
            out.status.code(),
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    let body = String::from_utf8_lossy(&out.stdout);
    let (payload, status) = body
        .rsplit_once('\n')
        .ok_or_else(|| format!("curl {url} wrote no status line: {body:?}"))?;
    if status.trim() != "200" {
        return Err(format!(
            "{url} answered HTTP {} — body {payload:?}",
            status.trim()
        ));
    }
    serde_json::from_str(payload).map_err(|e| format!("{url} returned {payload:?}: {e}"))
}

/// A JSON array field that MUST be present. An absent field is a failure,
/// not an empty answer — the same rule as [`reference_get`], one level
/// down.
fn required_array<'a>(
    body: &'a serde_json::Value,
    field: &str,
    what: &str,
) -> Result<&'a Vec<serde_json::Value>, String> {
    body.get(field)
        .and_then(|v| v.as_array())
        .ok_or_else(|| format!("{what} carried no `{field}` array: {body}"))
}

/// The reference instance this leg is pointed at holds NO trace and NO
/// ingested attribute key, and no other leg in this environment names it.
///
/// **What this prevents.**
///
/// * Another leg's corpus being resident when this suite pushes its own.
///   The search probe uses the WIDEST window the pinned reference accepts
///   — it refuses a range over 168h with
///   `400 range specified by start and end exceeds 168h0m0s` — so a
///   corpus timestamped anywhere within the past week is seen, not only
///   one inside the 25h window the first version of this guard used.
/// * Catalog residue a search cannot reach: the unbounded tag-name route
///   still lists an attribute key after the spans that carried it have
///   left every legal search window, so an instance that looks empty to
///   `{}` but has ingested before is rejected.
/// * A failed request read as an empty instance: every error above is a
///   panic here.
/// * Another differential leg pointed at this same instance from the same
///   environment, compared by canonical host and port rather than by URL
///   string, so `localhost` and `127.0.0.1` do not pass as two instances.
///
/// **The response classes this guard treats as free, measured — two of
/// them (issue #479, code review wave 3).** Rows 1-4 and 7-8 are answered
/// by a loopback responder; rows 1-4 and 7 serve the reference's own
/// build-info envelope on the identity route, so the row isolates the
/// search and tag routes, and row 8 does not.
///
/// | # | what answers `/api/search` and `/api/search/tags` | verdict | rejected at |
/// |---|---|---|---|
/// | 1 | `200` carrying traces | REJECTED | search route |
/// | 2 | `200`, empty `traces` AND empty `tagNames` | **FREE** | — |
/// | 3 | `404` | REJECTED | search route |
/// | 4 | `503` | REJECTED | search route |
/// | 5 | no answer within `--max-time` | REJECTED | identity route |
/// | 6 | connection refused | REJECTED | identity route |
/// | 7 | `200` with a malformed body | REJECTED | search route |
/// | 8 | a DIFFERENT service on the port | REJECTED | identity route |
///
/// Row 2 is free only when BOTH arrays are empty: an empty search with a
/// non-empty tag catalog is the catalog-residue case and is rejected. Row
/// 8 is the wave-3 finding: before
/// [`pulsus_testkit::assert_traces_reference_identity`] ran first, an
/// unrelated service answering the two empty envelopes satisfied this
/// guard outright.
///
/// **What it does NOT prevent.**
///
/// * A suite that runs AFTER this one against this instance: this corpus
///   is resident by then, and the guard that catches it belongs to that
///   suite —
///   `compare_value_differential` refuses to run while
///   [`pulsus_testkit::PROJECTION_DIFFERENTIAL_TRACE_HEX`] is resident,
///   which is the reverse-order half of the hazard.
/// * A trace older than a week carrying no attribute and no resource
///   attribute at all: outside every legal search window and contributing
///   no tag name.
/// * A proxy or port forward reaching this instance under an address
///   neither the workflow nor this process's environment names.
/// * A SECOND container of the same pinned image on this endpoint: the
///   identity check below authenticates the service and its build, not
///   which instance of it answers. What that costs is bounded by the two
///   checks that do compare instances — the runtime emptiness probes here
///   and the configuration check in
///   [`the_projection_leg_does_not_share_an_instance_with_another_suite`].
fn assert_reference_instance_is_exclusive(api_base: &str) {
    // WHO is answering, before any emptiness is read as isolation: an
    // empty envelope from something that is not the reference says
    // nothing at all (code review wave 3).
    pulsus_testkit::assert_traces_reference_identity(api_base);
    let base = api_base.trim_end_matches('/');
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_secs(),
    )
    .expect("fits i64");

    let search = format!("{base}/api/search");
    let body = reference_get(
        &search,
        &[
            ("q", "{}".to_string()),
            // 167h30m: under the reference's 168h refusal, and the widest
            // window this probe can legally ask for.
            ("start", (now - 167 * 3600).to_string()),
            ("end", (now + 1800).to_string()),
            ("limit", "100".to_string()),
        ],
    )
    .unwrap_or_else(|e| {
        panic!(
            "the exclusivity precondition could not read {search}: {e}\nThis suite needs a \
             reference instance of its OWN and cannot confirm it has one, so it refuses to run."
        )
    });
    let resident = required_array(&body, "traces", &search).unwrap_or_else(|e| panic!("{e}"));
    let ids: Vec<&str> = resident
        .iter()
        .filter_map(|t| t.get("traceID").and_then(|v| v.as_str()))
        .collect();
    assert!(
        resident.is_empty(),
        "the reference at {api_base} already holds {} trace(s) ({ids:?}) over the widest window \
         it accepts — this suite needs an instance of its OWN. Its corpus enters other legs' \
         aggregates (compare()'s top-N), and the resulting failure surfaces in THAT suite as an \
         ordinary-looking value mismatch. Start a fresh instance for this leg.",
        resident.len()
    );

    let tags = format!("{base}/api/search/tags");
    let body = reference_get(&tags, &[]).unwrap_or_else(|e| {
        panic!(
            "the exclusivity precondition could not read {tags}: {e}\nThis suite needs a \
             reference instance of its OWN and cannot confirm it has one, so it refuses to run."
        )
    });
    let names = required_array(&body, "tagNames", &tags).unwrap_or_else(|e| panic!("{e}"));
    assert!(
        names.is_empty(),
        "the reference at {api_base} has ingested attribute key(s) {names:?} — its tag catalog \
         is not this suite's own corpus. Spans that have aged out of every searchable window \
         still leave their keys here, so this instance has served another leg. Start a fresh \
         instance."
    );

    // The same instance reached through a second variable in this very
    // environment: the reverse-order hazard's configuration half, checked
    // at RUNTIME because a shell or a script sets what the workflow does
    // not.
    let ours: Vec<(String, String)> = OUR_ENDPOINT_VARS
        .iter()
        .filter_map(|var| std::env::var(var).ok().map(|v| ((*var).to_string(), v)))
        .collect();
    for (their_var, their_url) in std::env::vars()
        .filter(|(k, _)| k.starts_with("PULSUSDB_") && k.ends_with("_URL"))
        .filter(|(k, _)| !OUR_ENDPOINT_VARS.contains(&k.as_str()))
    {
        for (our_var, our_url) in &ours {
            assert_ne!(
                endpoint_identity(&their_url),
                endpoint_identity(our_url),
                "{their_var}={their_url} and {our_var}={our_url} name the SAME instance \
                 ({:?}) — this suite's corpus would enter that leg's aggregates. The two \
                 spellings differ; the host and port do not.",
                endpoint_identity(our_url)
            );
        }
    }
}

/// This leg's two endpoint variables.
const OUR_ENDPOINT_VARS: [&str; 2] = [
    "PULSUSDB_PROJECTION_DIFF_URL",
    "PULSUSDB_PROJECTION_OTLP_URL",
];

/// A canonical INSTANCE identity for an endpoint URL: `(host, port)` with
/// every loopback spelling folded to one token.
///
/// `http://localhost:13200` and `http://127.0.0.1:13200` are one instance
/// and are equal here. As literal strings they are not, which is how a
/// shared instance passed the configuration check unnoticed (code review
/// wave 2).
fn endpoint_identity(url: &str) -> (String, String) {
    let rest = url.split("://").nth(1).unwrap_or(url);
    let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) => (h, p),
        // No explicit port: the scheme's default. Recorded as the literal
        // empty port rather than guessed, so two portless URLs on one host
        // still compare equal and a portless one never accidentally
        // matches an explicit port.
        _ => (authority, ""),
    };
    let host = host.trim_matches(|c| c == '[' || c == ']');
    let canonical = match host {
        "localhost" | "127.0.0.1" | "::1" | "0.0.0.0" | "" => "loopback",
        other => other,
    };
    (canonical.to_string(), port.to_string())
}

/// Every `PULSUSDB_*_URL` endpoint the CI workflow assigns, as
/// `(variable, value)` in file order. Comment lines are skipped, so a
/// commented-out endpoint is not an assignment.
fn workflow_endpoint_assignments(workflow: &str) -> Vec<(String, String)> {
    workflow
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            let (key, value) = line.split_once(':')?;
            if !key.starts_with("PULSUSDB_") || !key.ends_with("_URL") {
                return None;
            }
            Some((key.to_string(), value.trim().trim_matches('"').to_string()))
        })
        .collect()
}

/// The workflow's steps, as `(title, body)` — the body being every line
/// from the `- name:` line to the next one. Scoping the container checks
/// to a STEP is what makes "the step that starts our instance" a
/// well-defined thing to assert about.
fn workflow_steps(workflow: &str) -> Vec<(String, String)> {
    let mut steps: Vec<(String, String)> = Vec::new();
    for line in workflow.lines() {
        if let Some(title) = line.trim().strip_prefix("- name:") {
            steps.push((title.trim().to_string(), String::new()));
        } else if let Some((_, body)) = steps.last_mut() {
            body.push_str(line);
            body.push('\n');
        }
    }
    steps
}

/// The HOST ports a step's `docker run` publishes — the left half of each
/// `-p <host>:<container>`.
fn published_host_ports(step_body: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut tokens = step_body.split_whitespace();
    while let Some(token) = tokens.next() {
        if token == "-p"
            && let Some(mapping) = tokens.next()
            && let Some((host, _)) = mapping.split_once(':')
        {
            out.push(host.to_string());
        }
    }
    out
}

/// The container names a step declares with `--name`.
fn declared_container_names(step_body: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut tokens = step_body.split_whitespace();
    while let Some(token) = tokens.next() {
        if token == "--name"
            && let Some(name) = tokens.next()
        {
            out.push(name.to_string());
        }
    }
    out
}

/// Hermetic — issue #479, code review waves 1 and 2. **The projection leg
/// may not share a reference instance with any other differential leg.**
///
/// Running this suite before `compare_value_differential` on ONE instance
/// makes the latter fail: this corpus's `statusMessage`, service and root
/// names enter that query's `compare()` top-N. The failure appears in the
/// OTHER suite, names a value mismatch against the reference, and reads
/// exactly like a parity defect in the code under review — so the fix is
/// isolation, not a documented running order.
///
/// This is the CONFIGURATION half. It no longer compares endpoint STRINGS:
/// wave 2 pointed this leg at `127.0.0.1:13200/4318` while the others used
/// `localhost:13200/4318` and the check passed on one instance. What is
/// asserted now is instance IDENTITY and LIFECYCLE —
///
///  * each of our two variables is assigned exactly once, and at least two
///    other legs exist, so nothing is compared vacuously;
///  * no other leg's endpoint shares our canonical `(host, port)`, and none
///    shares either of our PORTS under any host spelling;
///  * exactly one step starts a container publishing our ports, it
///    publishes BOTH of them, it declares exactly one container name, and
///    that container publishes no other leg's port — the container, not the
///    URL text, is the instance;
///  * that container name is started once and torn down by an
///    `if: always()` step, so a leftover container cannot be silently
///    inherited by the next run.
///
/// What it still cannot see: a proxy, a port forward or a value built at
/// runtime that reaches the same instance under an address the workflow
/// does not contain. The RUNTIME half —
/// [`assert_reference_instance_is_exclusive`] — covers that from the other
/// direction by refusing an instance that holds any trace or any tag key.
///
/// *RED when:* a second leg reuses this leg's endpoint or either of its
/// ports under any spelling, our variables stop being assigned exactly
/// once, the start step stops being unique or stops publishing both ports,
/// its container is shared with another leg, or the teardown step goes
/// away.
#[test]
fn the_projection_leg_does_not_share_an_instance_with_another_suite() {
    let workflow = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(std::path::Path::parent)
            .expect("workspace root")
            .join(".github/workflows/ci.yml"),
    )
    .expect("read .github/workflows/ci.yml");
    let assignments = workflow_endpoint_assignments(&workflow);

    let (ours, others): (Vec<_>, Vec<_>) = assignments
        .iter()
        .partition(|(key, _)| OUR_ENDPOINT_VARS.contains(&key.as_str()));
    for var in OUR_ENDPOINT_VARS {
        assert_eq!(
            ours.iter().filter(|(key, _)| key == var).count(),
            1,
            "{var} must be assigned exactly once in the workflow; found {ours:?}"
        );
    }
    // The check would pass vacuously on a workflow that ran no other
    // differential leg, which is the state it exists to rule out.
    assert!(
        others.len() >= 2,
        "no other PULSUSDB_*_URL leg is configured — this check would pass having compared \
         nothing"
    );

    let our_ports: Vec<String> = ours
        .iter()
        .map(|(_, url)| endpoint_identity(url).1)
        .collect();
    for (our_var, our_url) in &ours {
        let (our_host, our_port) = endpoint_identity(our_url);
        assert!(
            !our_port.is_empty(),
            "{our_var}={our_url} names no port, so this leg's instance cannot be identified"
        );
        for (their_var, their_url) in &others {
            let (their_host, their_port) = endpoint_identity(their_url);
            assert!(
                (our_host.clone(), our_port.clone()) != (their_host, their_port.clone()),
                "{our_var}={our_url} and {their_var}={their_url} name the SAME reference \
                 instance: this suite's corpus would move that leg's aggregates, and the \
                 failure would surface there as a value mismatch against the reference"
            );
            assert_ne!(
                our_port, their_port,
                "{our_var}={our_url} and {their_var}={their_url} share the port {our_port}: on \
                 this runner every oracle is published on the loopback interface, so one port \
                 is one instance whatever the host is spelled as"
            );
        }
    }

    // The LIFECYCLE half: our ports belong to exactly one container, and
    // that container is ours alone.
    let steps = workflow_steps(&workflow);
    let starters: Vec<&(String, String)> = steps
        .iter()
        .filter(|(_, body)| {
            body.contains("docker run")
                && published_host_ports(body)
                    .iter()
                    .any(|p| our_ports.contains(p))
        })
        .collect();
    assert_eq!(
        starters.len(),
        1,
        "exactly one step must start the container publishing {our_ports:?}; found {:?}",
        starters.iter().map(|(t, _)| t).collect::<Vec<_>>()
    );
    let (start_title, start_body) = starters[0];
    let published = published_host_ports(start_body);
    for port in &our_ports {
        assert!(
            published.contains(port),
            "the step {start_title:?} publishes {published:?} but not {port} — this leg's two \
             endpoints must be two ports of ONE container"
        );
    }
    for (their_var, their_url) in &others {
        // Destructured rather than indexed so the workspace's fixed-port
        // uniqueness guard can see this is not a port literal.
        let (_their_host, their_port) = endpoint_identity(their_url);
        assert!(
            !published.contains(&their_port),
            "the step {start_title:?} publishes {their_port}, which {their_var}={their_url} \
             also names: that container serves two legs and is one shared instance"
        );
    }
    let names = declared_container_names(start_body);
    assert_eq!(
        names.len(),
        1,
        "the step {start_title:?} must declare exactly one --name; found {names:?}"
    );
    let name = &names[0];
    let started = steps
        .iter()
        .filter(|(_, body)| body.contains("docker run") && body.contains(&format!("--name {name}")))
        .count();
    assert_eq!(
        started, 1,
        "the container {name} is started by {started} steps; a second start would attach this \
         leg to whatever the first one left behind"
    );
    let torn_down = steps.iter().any(|(_, body)| {
        body.contains(&format!("docker rm -f {name}")) && body.contains("if: always()")
    });
    assert!(
        torn_down,
        "no `if: always()` step removes the container {name} — a leftover instance is inherited \
         by the next run of this job, corpus and tag catalog included"
    );
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

    // ---- the isolation precondition, BEFORE anything is pushed --------
    // This suite's corpus moves another differential's aggregates (a
    // `compare()` top-N picked up its statusMessage / service / root
    // names), and that shows up in the OTHER suite as a value mismatch
    // against the reference. So the instance must hold nothing else, and
    // must be provably readable while it says so. The contract — what
    // this prevents and what it does not — is on
    // `assert_reference_instance_is_exclusive`; the configuration half is
    // `the_projection_leg_does_not_share_an_instance_with_another_suite`.
    assert_reference_instance_is_exclusive(&api_base);

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
