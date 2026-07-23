//! Env-gated nested-set VALUE differential (issue #181 Plan v3 Δ7; the
//! #185 closeout hook) — a REAL two-system differential.
//!
//! The status-only #180 oracle cannot verify that our `nestedSetLeft/
//! Right/Parent` INTEGERS equal Tempo's for a given span. This leg is the
//! value-level gate. For each fixture it ingests the SAME spans into both
//! systems and compares their per-span numbering read back live:
//!
//!   * **PulsusDB side** — the fixture spans are written to a live
//!     ClickHouse (a throwaway DB) and the numbering is read back through
//!     this crate's REAL two-phase search executor
//!     ([`TraceEngine::search`]). Because #181 exposes nested-set only as
//!     filterable numeric intrinsics (no `select(nestedSet)` — filter-only
//!     scope), each span's `left`/`right`/`parent` is extracted by probing
//!     `{ nestedSetLeft = v }` / `{ nestedSetRight = v }` /
//!     `{ nestedSetParent < 0 }` / `{ nestedSetParent = v }` over the real
//!     query path — genuine engine output, never a hard-coded constant.
//!   * **Tempo side** — the same spans are pushed to the pinned
//!     `grafana/tempo:3.0.2` OTLP receiver and the numbering is read back
//!     with `{} | select(nestedSetLeft, nestedSetRight, nestedSetParent)`
//!     against its live search API.
//!
//! **Honest by construction (the v2→v3 review finding).** Three fixture
//! classes, deliberately including the two that DIVERGE under the current
//! implementation, so the gate CAN fail — a value-parity gate that cannot
//! fail is not a gate. The test asserts span-by-span EQUALITY for all
//! three; the divergence comes from REAL system outputs, not constants:
//!
//!   1. **agreeing baseline** — per-parent ingest order equals our
//!      `(timestamp_ns, span_id)` proxy and the trace is wholly in the
//!      PulsusDB search window: byte-exact today (PASSES).
//!   2. **contrary-sibling-order** — siblings are PUSHED to Tempo in an
//!      order different from our sort, so Tempo (ingest/document order)
//!      numbers them differently: EXPECTED-FAIL today.
//!   3. **window-clipped** — an out-of-window ancestor is excluded from
//!      the PulsusDB search window (so our windowed-forest numbering makes
//!      the in-window child a root) while Tempo numbers over the whole
//!      trace: EXPECTED-FAIL today.
//!
//! The divergent classes are NOT rigged to pass and NOT silently skipped:
//! a gated run today fails loudly on classes 2 and 3 with the two #185
//! resolution paths named. Because it is env-gated, fast CI never runs it.
//!
//! **#185 activation.** #185 flips this leg to a BLOCKING close condition.
//! It cannot close at 100% parity until classes 2 and 3 are resolved by
//! exactly one of: (a) byte-exact parity — a stored per-span
//! ingest-sequence column so siblings follow ingest order, plus trace-wide
//! hydration on the nested-set path; or (b) a formally ledgered divergence
//! per the #180 schema.
//!
//! Gate: skips unless `PULSUS_TEST_CLICKHOUSE=1` AND
//! `PULSUSDB_NESTEDSET_DIFF_URL` (Tempo search API base, e.g.
//! `http://localhost:3200`) AND `PULSUSDB_NESTEDSET_OTLP_URL` (Tempo OTLP
//! HTTP base, e.g. `http://localhost:4318`) are all set. Run locally:
//!
//! ```text
//! # ClickHouse 24.8 on 19124, Tempo 3.0.2 on 3200 (API) / 4318 (OTLP)
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=19124 \
//!   PULSUSDB_NESTEDSET_DIFF_URL=http://localhost:3200 \
//!   PULSUSDB_NESTEDSET_OTLP_URL=http://localhost:4318 \
//!   cargo test -p pulsus-read --test nestedset_value_differential -- --nocapture
//! ```
//!
//! Clean-room: no Tempo/Grafana source, grammar, or test corpus is read —
//! the fixtures are our own authorship and the Tempo values are read back
//! as black-box runtime output.

use std::collections::BTreeMap;
use std::process::Command;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_read::traces::search_plan::{SearchParams, plan_search};
use pulsus_read::{TraceEngine, TraceReadConfig};
use pulsus_schema::{RenderCtx, run_init};

// ---------------------------------------------------------------------------
// Gating + ClickHouse setup
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

/// One fixture span. `parent == 0` is a root. Vector order == ingest /
/// document order (the order pushed to Tempo's OTLP receiver).
struct SpanDef {
    id: u8,
    parent: u8,
    name: &'static str,
    ts_ns: i64,
}

struct Fixture {
    name: &'static str,
    note: &'static str,
    spans: Vec<SpanDef>,
    /// The PulsusDB search window `(start_ns, end_ns)`; spans with
    /// `ts_ns <= start_ns` are clipped from the windowed forest.
    window: (i64, i64),
}

fn fixtures(base: i64) -> Vec<Fixture> {
    let sec = 1_000_000_000i64;
    let wide = (base - 60 * sec, base + 60 * sec);
    vec![
        // Class 1 — agreeing baseline. Ingest order [R, A, B, C] equals
        // the (timestamp_ns, span_id) order (A before B), wholly in-window.
        Fixture {
            name: "agreeing_baseline",
            note: "ingest order == (timestamp_ns, span_id); wholly in-window",
            spans: vec![
                SpanDef {
                    id: 1,
                    parent: 0,
                    name: "R",
                    ts_ns: base,
                },
                SpanDef {
                    id: 2,
                    parent: 1,
                    name: "A",
                    ts_ns: base + sec,
                },
                SpanDef {
                    id: 3,
                    parent: 1,
                    name: "B",
                    ts_ns: base + 2 * sec,
                },
                SpanDef {
                    id: 4,
                    parent: 3,
                    name: "C",
                    ts_ns: base + 3 * sec,
                },
            ],
            window: wide,
        },
        // Class 2 — contrary sibling order. Siblings are PUSHED [X, Y]
        // (X ingested first) but sort Y-then-X (Y has the earlier ts).
        // Tempo numbers by ingest order, we by the sort proxy → diverge.
        Fixture {
            name: "contrary_sibling_order",
            note: "pushed X-then-Y but sorts Y-then-X; Tempo uses ingest order",
            spans: vec![
                SpanDef {
                    id: 1,
                    parent: 0,
                    name: "R",
                    ts_ns: base,
                },
                SpanDef {
                    id: 2,
                    parent: 1,
                    name: "X",
                    ts_ns: base + 2 * sec,
                },
                SpanDef {
                    id: 3,
                    parent: 1,
                    name: "Y",
                    ts_ns: base + sec,
                },
            ],
            window: wide,
        },
        // Class 3 — window-clipped. Root R is 30 min old (outside the
        // PulsusDB search window) but ingested into Tempo whole-trace, so
        // Tempo numbers R/A/B together while our windowed forest hydrates
        // only A/B and makes A a root → diverge.
        Fixture {
            name: "window_clipped",
            note: "out-of-window root R; Tempo numbers whole-trace, we number the windowed forest",
            spans: vec![
                SpanDef {
                    id: 1,
                    parent: 0,
                    name: "R",
                    ts_ns: base - 1800 * sec,
                },
                SpanDef {
                    id: 2,
                    parent: 1,
                    name: "A",
                    ts_ns: base,
                },
                SpanDef {
                    id: 3,
                    parent: 2,
                    name: "B",
                    ts_ns: base + sec,
                },
            ],
            window: wide,
        },
    ]
}

// ---------------------------------------------------------------------------
// Hex helpers
// ---------------------------------------------------------------------------

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
// PulsusDB side — ingest via ClickHouse, read numbering via the search API
// ---------------------------------------------------------------------------

async fn pulsus_insert(client: &ChClient, db: &str, trace: &[u8; 16], spans: &[SpanDef]) {
    let mut rows = Vec::new();
    for s in spans {
        let pid = if s.parent == 0 {
            "0000000000000000".to_string()
        } else {
            hex(&sid_bytes(s.parent))
        };
        rows.push(format!(
            "(toFixedString(unhex('{tid}'),16), toFixedString(unhex('{sid}'),8), \
             toFixedString(unhex('{pid}'),8), '{name}', 'svc', {ts}, 1000, 0, 1, 1, 'x')",
            tid = hex(trace),
            sid = hex(&sid_bytes(s.id)),
            name = s.name,
            ts = s.ts_ns,
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

/// Matched span ids for one TraceQL query over the fixture's window.
async fn matched(engine: &TraceEngine, q: &str, window: (i64, i64)) -> Vec<[u8; 8]> {
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
    out.traces
        .iter()
        .flat_map(|t| t.spans.iter().map(|s| s.span_id))
        .collect()
}

/// PulsusDB's per-span `(left, right, parent)` for the in-window spans,
/// extracted through the real search API by value probing.
async fn pulsus_numbering(
    engine: &TraceEngine,
    window: (i64, i64),
) -> BTreeMap<[u8; 8], (i64, i64, i64)> {
    let in_window = matched(engine, "{}", window).await;
    let n = in_window.len() as i64;
    let mut out: BTreeMap<[u8; 8], (i64, i64, i64)> =
        in_window.into_iter().map(|s| (s, (0, 0, 0))).collect();
    for v in 1..=2 * n {
        for s in matched(engine, &format!("{{ nestedSetLeft = {v} }}"), window).await {
            if let Some(e) = out.get_mut(&s) {
                e.0 = v;
            }
        }
        for s in matched(engine, &format!("{{ nestedSetRight = {v} }}"), window).await {
            if let Some(e) = out.get_mut(&s) {
                e.1 = v;
            }
        }
        for s in matched(engine, &format!("{{ nestedSetParent = {v} }}"), window).await {
            if let Some(e) = out.get_mut(&s) {
                e.2 = v;
            }
        }
    }
    for s in matched(engine, "{ nestedSetParent < 0 }", window).await {
        if let Some(e) = out.get_mut(&s) {
            e.2 = -1;
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Tempo side — OTLP push, read numbering via select()
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
                "endTimeUnixNano": (s.ts_ns + 1_000_000_000).to_string(),
                "kind": 1,
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

/// Polls Tempo's search API until the pushed trace's numbering is
/// queryable, returning its per-span `(left, right, parent)`.
fn tempo_numbering(api_base: &str, trace: &[u8; 16]) -> BTreeMap<[u8; 8], (i64, i64, i64)> {
    let trace_hex = hex(trace);
    for _ in 0..60 {
        if let Some(map) = tempo_query_once(api_base, &trace_hex)
            && !map.is_empty()
        {
            return map;
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    panic!("Tempo never returned numbering for trace {trace_hex} within the poll budget");
}

fn tempo_query_once(api_base: &str, trace_hex: &str) -> Option<BTreeMap<[u8; 8], (i64, i64, i64)>> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    let start = (now - 7200).to_string();
    let end = (now + 120).to_string();
    let url = format!("{}/api/search", api_base.trim_end_matches('/'));
    let out = Command::new("curl")
        .args(["-s", "-G", "--max-time", "20"])
        .args([
            "--data-urlencode",
            "q={} | select(nestedSetLeft, nestedSetRight, nestedSetParent)",
        ])
        .args(["--data-urlencode", &format!("start={start}")])
        .args(["--data-urlencode", &format!("end={end}")])
        .args(["--data-urlencode", "limit=100"])
        // Tempo caps spans-per-spanset at 3 by default; lift it so a
        // whole trace's numbering comes back (not just the first 3 spans).
        .args(["--data-urlencode", "spss=100"])
        .arg(&url)
        .output()
        .expect("curl on PATH");
    let body: serde_json::Value = serde_json::from_slice(&out.stdout).ok()?;
    let traces = body.get("traces")?.as_array()?;
    let mut map = BTreeMap::new();
    for t in traces {
        // Tempo strips leading zero bytes from the traceID; match on the
        // trimmed hex suffix.
        let tid = t.get("traceID")?.as_str().unwrap_or("");
        if !trace_hex.trim_start_matches('0').ends_with(tid) && tid != trace_hex {
            continue;
        }
        let spans = t.get("spanSet")?.get("spans")?.as_array()?;
        for s in spans {
            let span_hex = s.get("spanID")?.as_str().unwrap_or("");
            let id = u8::from_str_radix(&span_hex[span_hex.len().saturating_sub(2)..], 16).ok()?;
            let mut vals = (i64::MIN, i64::MIN, i64::MIN);
            if let Some(attrs) = s.get("attributes").and_then(|a| a.as_array()) {
                for a in attrs {
                    let key = a.get("key").and_then(|k| k.as_str()).unwrap_or("");
                    let v: i64 = a
                        .get("value")
                        .and_then(|v| v.get("intValue"))
                        .and_then(|v| v.as_str())
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(i64::MIN);
                    match key {
                        "nestedSetLeft" => vals.0 = v,
                        "nestedSetRight" => vals.1 = v,
                        "nestedSetParent" => vals.2 = v,
                        _ => {}
                    }
                }
            }
            map.insert(sid_bytes(id), vals);
        }
    }
    Some(map)
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
async fn nestedset_value_differential() {
    let (Ok(api_base), Ok(otlp_base), true) = (
        std::env::var("PULSUSDB_NESTEDSET_DIFF_URL"),
        std::env::var("PULSUSDB_NESTEDSET_OTLP_URL"),
        std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1"),
    ) else {
        eprintln!(
            "skipping the nested-set value differential — set PULSUS_TEST_CLICKHOUSE=1, \
             PULSUSDB_NESTEDSET_DIFF_URL (Tempo API) and PULSUSDB_NESTEDSET_OTLP_URL (Tempo OTLP). \
             Env-gated, unenforced in fast CI; #185 activates enforcement."
        );
        return;
    };

    let bootstrap = ChClient::new(ch_config("default"))
        .await
        .expect("connect bootstrap");
    let base = now_ns();
    let mut diverged: Vec<&'static str> = Vec::new();

    for fx in fixtures(base) {
        let trace = *uuid::Uuid::new_v4().as_bytes();

        // Tempo: push first so it has the whole poll window to become
        // queryable while PulsusDB probing runs.
        otlp_push(&otlp_base, &trace, &fx.spans);

        // PulsusDB: throwaway DB, real ingest + real search-path readback.
        let db = format!("pulsus_nsdiff_it_{}", hex(&trace));
        init_db(&bootstrap, &db).await;
        let client = ChClient::new(ch_config(&db)).await.expect("connect db");
        pulsus_insert(&client, &db, &trace, &fx.spans).await;
        let engine = TraceEngine::new(
            ChClient::new(ch_config(&db)).await.expect("connect engine"),
            engine_config(),
        );
        let pulsus = pulsus_numbering(&engine, fx.window).await;

        // Tempo readback (whole-trace numbering).
        let tempo = tempo_numbering(&api_base, &trace);

        // Compare over the spans PulsusDB hydrated (in-window).
        let mut mism: Vec<String> = Vec::new();
        for (sid, pv) in &pulsus {
            match tempo.get(sid) {
                Some(tv) if tv == pv => {}
                Some(tv) => mism.push(format!("span {}: pulsus {pv:?} != tempo {tv:?}", sid[7])),
                None => mism.push(format!(
                    "span {}: present in pulsus, absent from tempo",
                    sid[7]
                )),
            }
        }
        if mism.is_empty() {
            eprintln!(
                "[{}] AGREES ({}) — {} spans",
                fx.name,
                fx.note,
                pulsus.len()
            );
        } else {
            eprintln!(
                "[{}] DIVERGES ({}):\n  {}",
                fx.name,
                fx.note,
                mism.join("\n  ")
            );
            diverged.push(fx.name);
        }

        exec(&bootstrap, &format!("DROP DATABASE IF EXISTS {db}")).await;
    }

    assert!(
        diverged.is_empty(),
        "nested-set value parity divergence in {diverged:?} (from REAL PulsusDB + Tempo output). \
         The contrary-sibling-order and window-clipped classes are EXPECTED-FAIL today: issue #181 \
         numbers siblings by (timestamp_ns, span_id) over the windowed hydrated forest, while Tempo \
         numbers by ingest/document order over the whole trace. The #185 closeout must drive these \
         to byte-exact parity — either (a) a stored per-span ingest-sequence column plus trace-wide \
         hydration on the nested-set path, or (b) a formally ledgered divergence (retaining \
         invariant + structural-result parity) — before it can close at 100% parity."
    );
}
