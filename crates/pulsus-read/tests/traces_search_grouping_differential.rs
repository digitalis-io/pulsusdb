//! Env-gated by()/coalesce() VALUE differential (issue #193) — a REAL
//! two-system differential proving grouped / coalesced spanSet-array
//! parity (not just parse acceptance).
//!
//! For each fixture it ingests the SAME spans into both systems and reads
//! the grouped response back live:
//!
//!   * **PulsusDB side** — the fixture spans are written to a live
//!     ClickHouse (a throwaway DB) and the `by()`/`coalesce()` response is
//!     read through this crate's REAL two-phase search executor
//!     ([`TraceEngine::search`]) — genuine engine output (the
//!     `TraceSearchResult.groups` layer #193 builds), never a constant.
//!   * **Tempo side** — the same spans are pushed to the pinned
//!     `grafana/tempo:3.0.2` OTLP receiver and the grouped spanSets are
//!     read back from its live `/api/search` with the identical `q=`.
//!
//! The gate compares, per trace: the SET of group key-tuples, the per-group
//! span-id membership, and the group `attributes` TYPING — the value is a
//! TYPE-TAGGED token (`stringValue=…`/`intValue=…`/`doubleValue=…`/
//! `boolValue=…`), so a wire-type mismatch (e.g. Tempo `stringValue "error"`
//! vs an `intValue 2`) fails the gate, not just a value mismatch. A
//! `coalesce()` fixture asserts the groups collapse to a single flat
//! spanSet on BOTH sides.
//!
//! **Type coverage (flag-5).** One representative case of EACH by-key type
//! so a single CI pass reveals any remaining wire-type divergence: `name`
//! (string), `status`/`kind` (lowercase keyword `stringValue`), `duration`
//! (Go `time.Duration.String()` `stringValue`), and `nestedSetParent`
//! (`intValue`). Numeric-attribute `doubleValue` and the `-0.0`/NaN
//! `canonical_double_bits` folding are pinned by the HERMETIC
//! `search_eval::tests::float_by_key_collapses_signed_zero_and_all_nan`
//! and `..::grouped_charges_equal_retained_plus_counter_exactly` units
//! (OTLP/JSON cannot even carry a NaN attribute), so those need no live
//! oracle.
//!
//! Gate: skips unless `PULSUS_TEST_CLICKHOUSE=1` AND
//! `PULSUSDB_GROUPING_DIFF_URL` (Tempo search API base, e.g.
//! `http://localhost:3200`) AND `PULSUSDB_GROUPING_OTLP_URL` (Tempo OTLP
//! HTTP base, e.g. `http://localhost:4318`) are all set. Run locally:
//!
//! ```text
//! # ClickHouse 24.8 on 19124, Tempo 3.0.2 on 3200 (API) / 4318 (OTLP)
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=19124 \
//!   PULSUSDB_GROUPING_DIFF_URL=http://localhost:3200 \
//!   PULSUSDB_GROUPING_OTLP_URL=http://localhost:4318 \
//!   cargo test -p pulsus-read --test traces_search_grouping_differential -- --nocapture
//! ```
//!
//! Clean-room: no Tempo/Grafana source, grammar, or test corpus is read —
//! the fixtures are our own authorship and the Tempo values are read back
//! as black-box runtime output.

use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_read::traces::search_plan::{SearchParams, plan_search};
use pulsus_read::{GroupValue, TraceEngine, TraceReadConfig};
use pulsus_schema::{RenderCtx, run_init};

// ---------------------------------------------------------------------------
// ClickHouse setup
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
        spans_table: "trace_spans".to_string(),
        attrs_table: "trace_attrs_idx".to_string(),
        catalog_table: "trace_tag_catalog".to_string(),
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
    let params = RenderCtx {
        db: db.to_string(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
    };
    run_init(bootstrap, &params).await.expect("run_init");
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct SpanDef {
    id: u8,
    /// 0 = root (no `parentSpanId`).
    parent: u8,
    name: &'static str,
    ts_ns: i64,
    duration_ns: i64,
    /// OTLP StatusCode (0 unset / 1 ok / 2 error).
    status: i32,
    /// OTLP SpanKind (1 internal / 2 server / 3 client / 4 producer /
    /// 5 consumer).
    kind: i32,
}

impl SpanDef {
    fn new(id: u8, name: &'static str, ts_ns: i64) -> Self {
        SpanDef {
            id,
            parent: 0,
            name,
            ts_ns,
            duration_ns: 1_000,
            status: 0,
            kind: 1,
        }
    }
    fn status(mut self, status: i32) -> Self {
        self.status = status;
        self
    }
    fn kind(mut self, kind: i32) -> Self {
        self.kind = kind;
        self
    }
    fn duration(mut self, duration_ns: i64) -> Self {
        self.duration_ns = duration_ns;
        self
    }
    fn parent(mut self, parent: u8) -> Self {
        self.parent = parent;
        self
    }
}

struct Fixture {
    /// The differential name.
    name: &'static str,
    /// The TraceQL query.
    q: &'static str,
    /// `true` when the query coalesces back to a single flat spanSet.
    coalesced: bool,
    spans: Vec<SpanDef>,
}

fn fixtures(base: i64) -> Vec<Fixture> {
    let sec = 1_000_000_000i64;
    vec![
        Fixture {
            name: "by_name_string_groups",
            q: "{} | by(name)",
            coalesced: false,
            spans: vec![
                SpanDef::new(1, "gold", base),
                SpanDef::new(2, "gold", base + sec),
                SpanDef::new(3, "silver", base + 2 * sec),
            ],
        },
        Fixture {
            name: "by_name_then_coalesce_collapses",
            q: "{} | by(name) | coalesce()",
            coalesced: true,
            spans: vec![
                SpanDef::new(1, "gold", base),
                SpanDef::new(2, "silver", base + sec),
            ],
        },
        // Flag-5 coverage: one representative case of EACH by-key TYPE so a
        // single CI pass reveals any remaining wire-type divergence.
        // `status` renders its lowercase keyword as `stringValue`.
        Fixture {
            name: "by_status_keyword_string",
            q: "{} | by(status)",
            coalesced: false,
            spans: vec![
                SpanDef::new(1, "s", base).status(2),           // error
                SpanDef::new(2, "s", base + sec).status(2),     // error
                SpanDef::new(3, "s", base + 2 * sec).status(1), // ok
            ],
        },
        // `kind` renders its lowercase keyword as `stringValue`.
        Fixture {
            name: "by_kind_keyword_string",
            q: "{} | by(kind)",
            coalesced: false,
            spans: vec![
                SpanDef::new(1, "s", base).kind(2),           // server
                SpanDef::new(2, "s", base + sec).kind(2),     // server
                SpanDef::new(3, "s", base + 2 * sec).kind(3), // client
            ],
        },
        // `duration` renders Go's `time.Duration.String()` as `stringValue`.
        Fixture {
            name: "by_duration_go_string",
            q: "{} | by(duration)",
            coalesced: false,
            spans: vec![
                SpanDef::new(1, "s", base).duration(1_500_000_000), // 1.5s
                SpanDef::new(2, "s", base + sec).duration(1_500_000_000),
                SpanDef::new(3, "s", base + 2 * sec).duration(2_000_000_000), // 2s
            ],
        },
        // A nested-set (COUNT/numbering) intrinsic renders as `intValue`.
        // A simple root -> single-child tree (no siblings, in-window) has
        // an unambiguous numbering both systems agree on: root
        // nestedSetParent = -1, child nestedSetParent = root's left (1).
        Fixture {
            name: "by_nested_set_parent_int",
            q: "{} | by(nestedSetParent)",
            coalesced: false,
            spans: vec![
                SpanDef::new(1, "root", base),
                SpanDef::new(2, "child", base + sec).parent(1),
            ],
        },
    ]
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn sid_bytes(id: u8) -> [u8; 8] {
    let mut b = [0u8; 8];
    b[7] = id;
    b
}

// ---------------------------------------------------------------------------
// PulsusDB side
// ---------------------------------------------------------------------------

async fn pulsus_insert(client: &ChClient, db: &str, trace: &[u8; 16], spans: &[SpanDef]) {
    let mut rows = Vec::new();
    for s in spans {
        let parent = if s.parent == 0 {
            "0000000000000000".to_string()
        } else {
            hex(&sid_bytes(s.parent))
        };
        rows.push(format!(
            "(toFixedString(unhex('{tid}'),16), toFixedString(unhex('{sid}'),8), \
             toFixedString(unhex('{parent}'),8), '{name}', 'svc', {ts}, {dur}, {status}, {kind}, 1, 'x')",
            tid = hex(trace),
            sid = hex(&sid_bytes(s.id)),
            name = s.name,
            ts = s.ts_ns,
            dur = s.duration_ns,
            status = s.status,
            kind = s.kind,
        ));
    }
    exec(
        client,
        &format!(
            "INSERT INTO {db}.trace_spans \
             (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
              status_code, kind, payload_type, payload) VALUES {}",
            rows.join(", ")
        ),
    )
    .await;
}

/// The grouped spanSets PulsusDB returns for one single-trace fixture:
/// group-value string → the set of member span-id low bytes. A coalesced
/// (flat) response maps under the sentinel `"<flat>"` key.
async fn pulsus_groups(
    engine: &TraceEngine,
    q: &str,
    window: (i64, i64),
) -> BTreeMap<String, BTreeSet<u8>> {
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
    let mut map: BTreeMap<String, BTreeSet<u8>> = BTreeMap::new();
    for t in &out.traces {
        match &t.groups {
            Some(groups) => {
                for g in groups {
                    let key = group_value_typed(&g.attributes[0].1);
                    let members: BTreeSet<u8> = g.spans.iter().map(|s| s.span_id[7]).collect();
                    map.entry(key).or_default().extend(members);
                }
            }
            None => {
                let members: BTreeSet<u8> = t.spans.iter().map(|s| s.span_id[7]).collect();
                map.entry("<flat>".to_string()).or_default().extend(members);
            }
        }
    }
    map
}

/// PulsusDB's group value as a TYPE-TAGGED token: the wire-type tag PLUS
/// the value, so `intValue 2` never compares equal to `doubleValue 2.0`
/// or `stringValue "2"`. This is what makes the differential genuinely
/// pin the reference's exact rendering (finding: a type-blind string
/// reduction cannot distinguish int/double/string wire types).
fn group_value_typed(value: &GroupValue) -> String {
    match value {
        GroupValue::Str(s) => format!("stringValue={s}"),
        GroupValue::Int(i) => format!("intValue={i}"),
        GroupValue::Double(bits) => format!("doubleValue={}", f64::from_bits(*bits)),
        GroupValue::Bool(b) => format!("boolValue={b}"),
        GroupValue::Nil => "null".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Tempo side
// ---------------------------------------------------------------------------

fn otlp_push(otlp_base: &str, trace: &[u8; 16], spans: &[SpanDef]) {
    let otlp_spans: Vec<serde_json::Value> = spans
        .iter()
        .map(|s| {
            let mut span = serde_json::json!({
                "traceId": hex(trace),
                "spanId": hex(&sid_bytes(s.id)),
                "name": s.name,
                "startTimeUnixNano": s.ts_ns.to_string(),
                "endTimeUnixNano": (s.ts_ns + s.duration_ns).to_string(),
                "kind": s.kind,
                "status": {"code": s.status},
            });
            if s.parent != 0 {
                span["parentSpanId"] = serde_json::Value::String(hex(&sid_bytes(s.parent)));
            }
            span
        })
        .collect();
    let body = serde_json::json!({
        "resourceSpans": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "svc"}}
            ]},
            "scopeSpans": [{"spans": otlp_spans}],
        }]
    });
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
            "-d",
            &body.to_string(),
        ])
        .arg(&url)
        .output()
        .expect("curl on PATH");
    let code = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        code.trim(),
        "200",
        "OTLP push to {url} failed (http {code})"
    );
}

/// Polls Tempo's search API until the pushed trace's grouped spanSets are
/// queryable, returning group-value string → member span-id low bytes (or
/// the `"<flat>"` sentinel for a coalesced response).
fn tempo_groups(api_base: &str, q: &str, trace: &[u8; 16]) -> BTreeMap<String, BTreeSet<u8>> {
    let trace_hex = hex(trace);
    for _ in 0..60 {
        if let Some(map) = tempo_query_once(api_base, q, &trace_hex)
            && !map.is_empty()
        {
            return map;
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    panic!("Tempo never returned grouped spanSets for trace {trace_hex} within the poll budget");
}

fn tempo_query_once(
    api_base: &str,
    q: &str,
    trace_hex: &str,
) -> Option<BTreeMap<String, BTreeSet<u8>>> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    let start = (now - 7200).to_string();
    let end = (now + 120).to_string();
    let url = format!("{}/api/search", api_base.trim_end_matches('/'));
    let out = Command::new("curl")
        .args(["-s", "-G", "--max-time", "20"])
        .args(["--data-urlencode", &format!("q={q}")])
        .args(["--data-urlencode", &format!("start={start}")])
        .args(["--data-urlencode", &format!("end={end}")])
        .args(["--data-urlencode", "limit=100"])
        .args(["--data-urlencode", "spss=100"])
        .arg(&url)
        .output()
        .expect("curl on PATH");
    let body: serde_json::Value = serde_json::from_slice(&out.stdout).ok()?;
    let traces = body.get("traces")?.as_array()?;
    let mut map: BTreeMap<String, BTreeSet<u8>> = BTreeMap::new();
    for t in traces {
        // Tempo strips leading zero bytes from the traceID; match on the
        // trimmed hex suffix.
        let tid = t.get("traceID")?.as_str().unwrap_or("");
        if !trace_hex.trim_start_matches('0').ends_with(tid) && tid != trace_hex {
            continue;
        }
        let span_sets = t
            .get("spanSets")
            .and_then(|s| s.as_array())
            .map(|v| v.as_slice());
        let flat = t.get("spanSet").map(std::slice::from_ref);
        let sets = span_sets.or(flat)?;
        for set in sets {
            // A grouped spanSet carries `attributes`; a flat one does not.
            let key = set
                .get("attributes")
                .and_then(|a| a.as_array())
                .and_then(|a| a.first())
                .and_then(tempo_attr_typed)
                .unwrap_or_else(|| "<flat>".to_string());
            let members: BTreeSet<u8> = set
                .get("spans")
                .and_then(|s| s.as_array())
                .map(|spans| {
                    spans
                        .iter()
                        .filter_map(|s| s.get("spanID").and_then(|v| v.as_str()))
                        .filter_map(|h| {
                            u8::from_str_radix(&h[h.len().saturating_sub(2)..], 16).ok()
                        })
                        .collect()
                })
                .unwrap_or_default();
            map.entry(key).or_default().extend(members);
        }
    }
    Some(map)
}

/// A Tempo group `attributes[0]` value as the SAME TYPE-TAGGED token
/// [`group_value_typed`] produces: WHICH `value:{…}` field is populated is
/// the wire type, and it is compared alongside the value. So a Tempo
/// `by(status)` rendered `stringValue "error"` will NOT match PulsusDB's
/// `intValue 2` — the differential fails on the exact int-vs-double-vs-
/// string typing question, as intended.
fn tempo_attr_typed(attr: &serde_json::Value) -> Option<String> {
    let value = attr.get("value")?;
    if let Some(s) = value.get("stringValue").and_then(|v| v.as_str()) {
        return Some(format!("stringValue={s}"));
    }
    // protojson renders 64-bit ints as strings; tolerate a bare number too.
    if let Some(s) = value.get("intValue").and_then(|v| v.as_str()) {
        return Some(format!("intValue={s}"));
    }
    if let Some(n) = value.get("intValue").and_then(|v| v.as_i64()) {
        return Some(format!("intValue={n}"));
    }
    if let Some(f) = value.get("doubleValue").and_then(|v| v.as_f64()) {
        return Some(format!("doubleValue={f}"));
    }
    if let Some(b) = value.get("boolValue").and_then(|v| v.as_bool()) {
        return Some(format!("boolValue={b}"));
    }
    None
}

// ---------------------------------------------------------------------------
// The differential
// ---------------------------------------------------------------------------

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64")
}

#[tokio::test(flavor = "multi_thread")]
async fn traces_search_grouping_differential() {
    let (Ok(api_base), Ok(otlp_base), true) = (
        std::env::var("PULSUSDB_GROUPING_DIFF_URL"),
        std::env::var("PULSUSDB_GROUPING_OTLP_URL"),
        std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1"),
    ) else {
        eprintln!(
            "skipping the by()/coalesce() grouping differential — set PULSUS_TEST_CLICKHOUSE=1, \
             PULSUSDB_GROUPING_DIFF_URL (Tempo API) and PULSUSDB_GROUPING_OTLP_URL (Tempo OTLP)."
        );
        return;
    };

    let bootstrap = ChClient::new(ch_config("default"))
        .await
        .expect("connect bootstrap");
    let base = now_ns();
    let sec = 1_000_000_000i64;
    let window = (base - 60 * sec, base + 60 * sec);
    let mut diverged: Vec<String> = Vec::new();

    for fx in fixtures(base) {
        let trace = *uuid::Uuid::new_v4().as_bytes();

        // Tempo: push first so it has the whole poll window to index.
        otlp_push(&otlp_base, &trace, &fx.spans);

        // PulsusDB: throwaway DB, real ingest + real grouped search readback.
        let db = format!("pulsus_grpdiff_it_{}", hex(&trace));
        init_db(&bootstrap, &db).await;
        let client = ChClient::new(ch_config(&db)).await.expect("connect db");
        pulsus_insert(&client, &db, &trace, &fx.spans).await;
        let engine = TraceEngine::new(
            ChClient::new(ch_config(&db)).await.expect("connect engine"),
            engine_config(),
        );
        let pulsus = pulsus_groups(&engine, fx.q, window).await;
        let tempo = tempo_groups(&api_base, fx.q, &trace);

        let mut mism: Vec<String> = Vec::new();
        if fx.coalesced {
            // BOTH sides must present a single flat spanSet (no groups).
            if pulsus.keys().collect::<Vec<_>>() != vec![&"<flat>".to_string()] {
                mism.push(format!("pulsus did not collapse: {:?}", pulsus.keys()));
            }
            if !tempo.contains_key("<flat>") || tempo.len() != 1 {
                mism.push(format!("tempo did not collapse: {:?}", tempo.keys()));
            }
        }
        if pulsus != tempo {
            mism.push(format!(
                "group map mismatch: pulsus {pulsus:?} != tempo {tempo:?}"
            ));
        }

        if mism.is_empty() {
            eprintln!("[{}] AGREES — {} group(s)", fx.name, pulsus.len());
        } else {
            eprintln!("[{}] DIVERGES:\n  {}", fx.name, mism.join("\n  "));
            diverged.push(fx.name.to_string());
        }

        exec(&bootstrap, &format!("DROP DATABASE IF EXISTS {db}")).await;
    }

    assert!(
        diverged.is_empty(),
        "by()/coalesce() grouped spanSet-array value parity divergence in {diverged:?} \
         (from REAL PulsusDB + Tempo output)."
    );
}
