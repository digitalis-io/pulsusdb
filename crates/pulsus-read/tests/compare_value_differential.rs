//! Env-gated `compare()` VALUE differential (issue #189; the #185
//! closeout hook) — a REAL two-system differential, structured exactly
//! like [`nestedset_value_differential`].
//!
//! #189 wires `compare()` to emit real per-value baseline/selection counts
//! for three schema-unblocked well-known keys — `statusMessage`,
//! `rootName`, `rootServiceName` — instead of the old `key=nil`. The
//! hermetic golden/live suites pin the SQL and the seeded counts; this leg
//! is the value-level parity gate against Tempo itself. For one shared
//! corpus it ingests the SAME spans into both systems and compares their
//! `compare()` output read back live:
//!
//!   * **PulsusDB side** — the spans are written to a throwaway ClickHouse
//!     DB and `{} | compare({ status = error })` is read back through this
//!     crate's REAL metrics executor ([`TraceEngine::metrics_range`]).
//!   * **Tempo side** — the same spans are pushed to `grafana/tempo:3.0.2`
//!     over OTLP and the same query is read back from its
//!     `/api/metrics/query_range` metrics API (the Tempo-native
//!     `{series:[{labels, samples}]}` body PulsusDB mirrors byte-for-byte).
//!
//! **Honest by construction.** The corpus deliberately exercises the
//! empty-`statusMessage` case — spans WITH a non-empty status message and
//! spans WITHOUT (which emit as the distinct `""` value) — and a multi-span
//! trace (root + child, both in-window) so `rootName`/`rootServiceName`
//! must propagate the ROOT's value across a child of a different
//! name/service.
//! Tempo v3.0.2 emits `statusMessage=""` as a DISTINCT value (verified
//! against the pinned reference, #185), so the metrics_sql builder emits it
//! verbatim (no `arrayFilter` fold to nil) to match. Because it is
//! env-gated, fast CI never runs it.
//!
//! Gate: skips unless `PULSUS_TEST_CLICKHOUSE=1` AND
//! `PULSUSDB_COMPARE_DIFF_URL` (Tempo metrics API base, e.g.
//! `http://localhost:3200`) AND `PULSUSDB_COMPARE_OTLP_URL` (Tempo OTLP
//! HTTP base, e.g. `http://localhost:4318`) are all set. Run locally:
//!
//! ```text
//! # ClickHouse 24.8 on 19124, Tempo 3.0.2 on 3200 (API) / 4318 (OTLP)
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=19124 \
//!   PULSUSDB_COMPARE_DIFF_URL=http://localhost:3200 \
//!   PULSUSDB_COMPARE_OTLP_URL=http://localhost:4318 \
//!   cargo test -p pulsus-read --test compare_value_differential -- --nocapture
//! ```
//!
//! Clean-room: no Tempo/Grafana source, grammar, or test corpus is read —
//! the fixtures are our own authorship and the Tempo values are read back
//! as black-box runtime output.

use std::collections::BTreeMap;
use std::process::Command;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_read::traces::metrics_plan::{MetricsParams, plan_trace_metrics};
use pulsus_read::{MetricLabelValue, TraceEngine, TraceMetricsResult, TraceReadConfig};
use pulsus_schema::{RenderCtx, run_init};

/// The three keys #189 makes data-driven; the differential is scoped to
/// exactly these.
const KEYS: &[&str] = &["statusMessage", "rootName", "rootServiceName"];

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

/// One fixture span. `parent == 0` is a root; `status` is the OTLP status
/// code (0 unset / 1 ok / 2 error); `service` is the span's resource
/// service. `msg` is the span status message (`''` means absent).
struct SpanDef {
    trace: u8,
    id: u8,
    parent: u8,
    name: &'static str,
    service: &'static str,
    status: u8,
    msg: &'static str,
    ts_ns: i64,
}

/// The shared corpus (three traces):
///   T1 — a multi-span trace: root `frontend`/`gateway` (ok, no message)
///        and child `checkout`/`cart` (error, "boom"). The child's
///        rootName/rootServiceName MUST be the ROOT's `frontend`/`gateway`.
///   T2 — single error span `worker`/`batch` ("timeout").
///   T3 — single ok span `idle`/`batch` (no message → the nil branch).
/// Selection is `{ status = error }` → selection {C, S}, baseline {R, U}.
fn corpus(base: i64) -> Vec<SpanDef> {
    let sec = 1_000_000_000i64;
    vec![
        SpanDef {
            trace: 1,
            id: 1,
            parent: 0,
            name: "frontend",
            service: "gateway",
            status: 1,
            msg: "",
            ts_ns: base,
        },
        SpanDef {
            trace: 1,
            id: 2,
            parent: 1,
            name: "checkout",
            service: "cart",
            status: 2,
            msg: "boom",
            ts_ns: base + sec,
        },
        SpanDef {
            trace: 2,
            id: 1,
            parent: 0,
            name: "worker",
            service: "batch",
            status: 2,
            msg: "timeout",
            ts_ns: base + 2 * sec,
        },
        SpanDef {
            trace: 3,
            id: 1,
            parent: 0,
            name: "idle",
            service: "batch",
            status: 1,
            msg: "",
            ts_ns: base + 3 * sec,
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

/// A per-run trace id: a random 15-byte nonce prefix plus the trace index
/// (so Tempo runs never collide, and PulsusDB's throwaway DB is isolated).
fn tid_bytes(nonce: &[u8; 16], trace: u8) -> [u8; 16] {
    let mut b = *nonce;
    b[15] = trace;
    b
}

// ---------------------------------------------------------------------------
// The per-(meta_type, key, value) count map — the shared comparison shape
// ---------------------------------------------------------------------------

type Counts = BTreeMap<(String, String, String), i64>;

/// Reads back PulsusDB's `compare()` over the corpus window through the
/// REAL metrics executor and projects the `(meta, key, value) -> count`
/// map for the three #189 keys (baseline/selection only).
async fn pulsus_counts(engine: &TraceEngine, window: (i64, i64)) -> Counts {
    let window_s = (window.1 - window.0) / 1_000_000_000;
    let query = pulsus_traceql::parse(r#"{} | compare({ status = error })"#).expect("parse");
    let plan = plan_trace_metrics(
        &query,
        &MetricsParams {
            start_ns: window.0,
            end_ns: window.1,
            step_s: window_s, // one whole-window bucket for exact counts
        },
        &engine.metrics_ctx(),
    )
    .expect("plan compare");
    let res: TraceMetricsResult = engine.metrics_range(&plan).await.expect("compare executes");

    let mut out = Counts::new();
    for s in &res.series {
        let Some(meta) = str_label(&s.labels, "__meta_type") else {
            continue;
        };
        if meta != "baseline" && meta != "selection" {
            continue;
        }
        for l in &s.labels {
            if l.key == "__meta_type" || !KEYS.contains(&l.key.as_str()) {
                continue;
            }
            if let MetricLabelValue::Str(val) = &l.value {
                let count = s.samples.iter().map(|(_, v)| v).sum::<f64>().round() as i64;
                out.insert((meta.clone(), l.key.clone(), val.clone()), count);
            }
        }
    }
    out
}

fn str_label(labels: &[pulsus_read::MetricLabel], key: &str) -> Option<String> {
    labels
        .iter()
        .find(|l| l.key == key)
        .and_then(|l| match &l.value {
            MetricLabelValue::Str(v) => Some(v.clone()),
            MetricLabelValue::Double(_) => None,
        })
}

// ---------------------------------------------------------------------------
// PulsusDB side — ingest via ClickHouse
// ---------------------------------------------------------------------------

async fn pulsus_insert(client: &ChClient, db: &str, nonce: &[u8; 16], spans: &[SpanDef]) {
    let mut rows = Vec::new();
    for s in spans {
        let pid = if s.parent == 0 {
            "0000000000000000".to_string()
        } else {
            hex(&sid_bytes(s.parent))
        };
        rows.push(format!(
            "(toFixedString(unhex('{tid}'),16), toFixedString(unhex('{sid}'),8), \
             toFixedString(unhex('{pid}'),8), '{name}', '{service}', '{msg}', {ts}, 1000, \
             {status}, 1, 1, 'x')",
            tid = hex(&tid_bytes(nonce, s.trace)),
            sid = hex(&sid_bytes(s.id)),
            name = s.name,
            service = s.service,
            msg = s.msg,
            ts = s.ts_ns,
            status = s.status,
        ));
    }
    exec(
        client,
        &format!(
            "INSERT INTO {db}.trace_spans \
             (trace_id, span_id, parent_id, name, service, status_message, timestamp_ns, \
              duration_ns, status_code, kind, payload_type, payload) VALUES {}",
            rows.join(", ")
        ),
    )
    .await;
}

// ---------------------------------------------------------------------------
// Tempo side — OTLP push, read `compare()` via the metrics API
// ---------------------------------------------------------------------------

fn otlp_push(otlp_base: &str, nonce: &[u8; 16], spans: &[SpanDef]) {
    // One resourceSpans per span so each carries its own resource
    // service.name (rootServiceName is the ROOT span's resource service);
    // Tempo assembles the trace across blocks by trace id.
    let resource_spans: Vec<serde_json::Value> = spans
        .iter()
        .map(|s| {
            let mut span = serde_json::json!({
                "traceId": hex(&tid_bytes(nonce, s.trace)),
                "spanId": hex(&sid_bytes(s.id)),
                "name": s.name,
                "startTimeUnixNano": s.ts_ns.to_string(),
                "endTimeUnixNano": (s.ts_ns + 1_000_000_000).to_string(),
                "kind": 1,
                "status": {
                    "code": s.status,
                    "message": s.msg,
                },
            });
            if s.parent != 0 {
                span["parentSpanId"] = serde_json::Value::String(hex(&sid_bytes(s.parent)));
            }
            serde_json::json!({
                "resource": {"attributes": [
                    {"key": "service.name", "value": {"stringValue": s.service}}
                ]},
                "scopeSpans": [{"spans": [span]}],
            })
        })
        .collect();
    let body = serde_json::json!({ "resourceSpans": resource_spans });
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

/// The Tempo metrics-query step. Small and FIXED (not the whole-window span):
/// a single whole-window bucket is aligned to the step grid and can land its
/// right edge in the future even for a past-anchored window, which reads back
/// empty; a small fixed step keeps every bucket the query touches finely
/// aligned and — because the corpus is anchored in the past (see
/// [`compare_value_differential`]) — already finalised. `compare()` counts
/// are additive across disjoint time buckets, so summing the per-step samples
/// (see [`tempo_query_once`]) yields the same totals a single bucket would.
const TEMPO_STEP_S: i64 = 60;

/// Polls Tempo's metrics API until `compare()` returns the corpus's
/// baseline/selection counts for the three keys.
///
/// Because the corpus is anchored in the PAST and `window` ends in the past,
/// every bucket this query touches is already finalised, so the FIRST
/// non-empty response carries the COMPLETE counts — the poll loop exists only
/// to wait out Tempo's flush of the freshly-pushed spans (~seconds), not to
/// wait for any future wall-clock boundary. The budget is a generous safety
/// net.
fn tempo_counts(api_base: &str, window: (i64, i64)) -> Counts {
    for _ in 0..60 {
        if let Some(map) = tempo_query_once(api_base, window)
            && !map.is_empty()
        {
            return map;
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    panic!("Tempo never returned compare() counts within the poll budget");
}

fn tempo_query_once(api_base: &str, window: (i64, i64)) -> Option<Counts> {
    let url = format!("{}/api/metrics/query_range", api_base.trim_end_matches('/'));
    let out = Command::new("curl")
        .args(["-s", "-G", "--max-time", "20"])
        .args(["--data-urlencode", "q={} | compare({ status = error })"])
        .args(["--data-urlencode", &format!("start={}", window.0)])
        .args(["--data-urlencode", &format!("end={}", window.1)])
        .args(["--data-urlencode", &format!("step={TEMPO_STEP_S}s")])
        .arg(&url)
        .output()
        .expect("curl on PATH");
    let body: serde_json::Value = serde_json::from_slice(&out.stdout).ok()?;
    let series = body.get("series")?.as_array()?;
    let mut map = Counts::new();
    for s in series {
        let labels = s.get("labels")?.as_array()?;
        let meta = label_str(labels, "__meta_type")?;
        if meta != "baseline" && meta != "selection" {
            continue;
        }
        // The sample values (a zero `value` is omitted, protojson default).
        let count: f64 = s
            .get("samples")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|sm| sm.get("value").and_then(|v| v.as_f64()).unwrap_or(0.0))
                    .sum()
            })
            .unwrap_or(0.0);
        for l in labels {
            let key = l.get("key").and_then(|k| k.as_str()).unwrap_or("");
            if key == "__meta_type" || !KEYS.contains(&key) {
                continue;
            }
            if let Some(val) = l
                .get("value")
                .and_then(|v| v.get("stringValue"))
                .and_then(|v| v.as_str())
            {
                map.insert(
                    (meta.clone(), key.to_string(), val.to_string()),
                    count.round() as i64,
                );
            }
        }
    }
    Some(map)
}

fn label_str(labels: &[serde_json::Value], key: &str) -> Option<String> {
    labels.iter().find_map(|l| {
        (l.get("key").and_then(|k| k.as_str()) == Some(key))
            .then(|| {
                l.get("value")
                    .and_then(|v| v.get("stringValue"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .flatten()
    })
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
async fn compare_value_differential() {
    let (Ok(api_base), Ok(otlp_base), true) = (
        std::env::var("PULSUSDB_COMPARE_DIFF_URL"),
        std::env::var("PULSUSDB_COMPARE_OTLP_URL"),
        std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1"),
    ) else {
        eprintln!(
            "skipping the compare() value differential — set PULSUS_TEST_CLICKHOUSE=1, \
             PULSUSDB_COMPARE_DIFF_URL (Tempo metrics API) and PULSUSDB_COMPARE_OTLP_URL \
             (Tempo OTLP). Env-gated, unenforced in fast CI; #185 activates enforcement."
        );
        return;
    };

    let sec = 1_000_000_000i64;
    // Anchor the corpus ~90s in the PAST (span timestamps `base .. base+3s`).
    // This is the crux of the flake fix. Tempo v3.0.2's live_store only
    // finalises a TraceQL-metrics time bucket once wall-clock passes that
    // bucket's right edge, and it counts spans by their span time. The
    // previous design anchored the corpus at "now" and queried a window
    // ending 120s in the FUTURE, so the value-bearing bucket only finalised
    // ~120s+ later — racing the poll budget and intermittently red-ing main.
    // With `base` in the past, EVERY bucket covering the corpus is already
    // finalised by the time the test queries, so the first non-empty poll
    // returns COMPLETE counts within a few seconds of Tempo flushing the push
    // (observed ~3s locally), deterministically. 90s stays well inside Tempo's
    // ~15m `query_backend_after` live_store window, so live_store serves it.
    let base = now_ns() - 90 * sec;
    // Both reads use this window; it brackets the corpus and, crucially, ENDS
    // in the past (`base+60s`) so every Tempo bucket it touches is complete.
    // PulsusDB's side is a plain ClickHouse timestamp range and is unaffected
    // by the anchor.
    let window = (base - 60 * sec, base + 60 * sec);
    let nonce = *uuid::Uuid::new_v4().as_bytes();
    let spans = corpus(base);

    // Tempo: push first so it has the whole poll window to become
    // queryable while the PulsusDB side runs.
    otlp_push(&otlp_base, &nonce, &spans);

    // PulsusDB: throwaway DB, real ingest + real metrics-path readback.
    let bootstrap = ChClient::new(ch_config("default"))
        .await
        .expect("connect bootstrap");
    let db = format!("pulsus_cmpdiff_it_{}", hex(&nonce));
    init_db(&bootstrap, &db).await;
    let client = ChClient::new(ch_config(&db)).await.expect("connect db");
    pulsus_insert(&client, &db, &nonce, &spans).await;
    let engine = TraceEngine::new(
        ChClient::new(ch_config(&db)).await.expect("connect engine"),
        engine_config(),
    );
    let pulsus = pulsus_counts(&engine, window).await;

    // Tempo readback (past-anchored, already-complete buckets; see tempo_counts).
    let tempo = tempo_counts(&api_base, window);

    eprintln!("pulsus compare() counts: {pulsus:#?}");
    eprintln!("tempo  compare() counts: {tempo:#?}");

    exec(&bootstrap, &format!("DROP DATABASE IF EXISTS {db}")).await;

    // Span-by-span byte-match on the three keys' baseline/selection counts.
    let mut mism: Vec<String> = Vec::new();
    let all_keys: std::collections::BTreeSet<_> = pulsus.keys().chain(tempo.keys()).collect();
    for k in all_keys {
        let (p, t) = (pulsus.get(k), tempo.get(k));
        if p != t {
            mism.push(format!("{k:?}: pulsus {p:?} != tempo {t:?}"));
        }
    }
    assert!(
        mism.is_empty(),
        "compare() value-parity divergence for statusMessage/rootName/rootServiceName \
         (REAL PulsusDB + Tempo output):\n  {}\n\nPulsusDB emits an empty statusMessage as a \
         distinct \"\" value (the `arrayFilter` fold-to-nil was removed, #185) to match Tempo \
         v3.0.2 — a residual divergence here is a NEW mismatch, not the known empty-message case.",
        mism.join("\n  ")
    );
}
