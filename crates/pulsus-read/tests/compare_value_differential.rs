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
//! verbatim (no `arrayFilter` fold to nil) to match. The `ci` job's
//! aggregate `--workspace` run executes this binary, where it self-skips
//! green; the `schema-it` job supplies its gate and runs it **enforced**
//! (`.github/workflows/ci.yml:686-691`, no `continue-on-error`). (Issue
//! #278 measured the claim this sentence replaced — that being env-gated
//! kept fast CI from running the binary at all — and found it false in
//! both of those ways.)
//!
//! Gate: skips unless `PULSUS_TEST_CLICKHOUSE=1` AND
//! `PULSUSDB_COMPARE_DIFF_URL` (Tempo metrics API base, e.g.
//! `http://localhost:3200`) AND `PULSUSDB_COMPARE_OTLP_URL` (Tempo OTLP
//! HTTP base, e.g. `http://localhost:4318`) are all set. Run locally:
//!
//! ```text
//! # ClickHouse 26.3 on 19124, Tempo 3.0.2 on 3200 (API) / 4318 (OTLP)
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=19124 \
//!   PULSUSDB_COMPARE_DIFF_URL=http://localhost:3200 \
//!   PULSUSDB_COMPARE_OTLP_URL=http://localhost:4318 \
//!   cargo test -p pulsus-read --test compare_value_differential -- --nocapture
//! ```
//!
//! **Fail-closed on all three gates** (issue #458 recorded the hole,
//! issue #523 closed it here; the same fix landed on
//! `traces_search_grouping_differential.rs` under #492 part 3). Both
//! endpoint URLs go through `pulsus_testkit::live_endpoint` — the one
//! endpoint read in the workspace (issue #523 review round 1) — and it
//! classifies them as ENDPOINT gates, not boolean ones: a URL-valued variable read by the boolean rule
//! looks "not set" while the `env:` block is right there in the log.
//! Before this, the URLs were read with a bare `env::var` and taken as a
//! skip, so dropping only them from this suite's `schema-it` step
//! reported GREEN having compared nothing. Measured at `d542869b`
//! (`PULSUS_TEST_CLICKHOUSE=1 GITHUB_JOB=schema-it`, no URLs):
//! `1 passed`; with this change the same invocation panics naming the
//! variable and the job. A developer machine — no `GITHUB_JOB` — still
//! skips cleanly. Ledger entry
//! `traceql-differential-legs-skip-green-on-a-missing-endpoint`.
//!
//! **Two differential binaries against ONE reference endpoint contaminate
//! each other. Recorded, not fixed** (issue #477 wave 4 review). This
//! suite's TraceQL matcher is bare `{}` — every span the reference holds
//! in the query window is in its baseline population — and its window is
//! wall-clock derived (`now-150s .. now-30s`, see `base` below). Its
//! sibling [`compare_arity_differential`] pushes its own corpus to the
//! same reference over OTLP and scopes only its QUERY, by
//! `resource.service.name`; the spans still land in the same instance.
//! Run the two binaries against one reference container inside the same
//! two-and-a-half minutes and this suite counts the sibling's spans as
//! baseline, which reads exactly like a value-parity fault.
//!
//! Measured (issue #477 wave 4, one reference container of our own, one
//! shared ClickHouse): `compare_arity_differential` alone passed, and
//! `compare_value_differential` run against the same container
//! immediately afterwards failed with exit 100 — its diff listing the
//! sibling's nonce-suffixed service values and `mNN` status messages as
//! `pulsus None != reference Some(N)`, since our side reads a throwaway
//! ClickHouse database while the reference side is shared. The same
//! binary against a freshly started container, nothing else pushed to it,
//! passed: `2 tests run: 2 passed, exit 0`.
//!
//! The fix is per-agent reference containers, which is already the
//! project rule for reference containers and is what CI does — the
//! `schema-it` job starts one reference per job. Nothing here scopes the
//! matcher, deliberately: `{}` is the shape the parity claim is about, and
//! narrowing it to dodge a test-isolation problem would narrow the claim
//! with it.
//!
//! Clean-room: no Tempo/Grafana source, grammar, or test corpus is read —
//! the fixtures are our own authorship and the Tempo values are read back
//! as black-box runtime output.

use std::collections::BTreeMap;
use std::process::Command;
use std::time::{Duration, Instant};

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_read::traces::metrics_plan::{MetricsParams, plan_trace_metrics};
use pulsus_read::{MetricLabelValue, TraceEngine, TraceMetricsResult, TraceReadConfig};
use pulsus_schema::{RenderCtx, run_init};

/// The keys the value differential is scoped to: the three #189 keys plus
/// the two #192 instrumentation-scope intrinsics (both data-driven from the
/// per-span `scope_name`/`scope_version` columns).
const KEYS: &[&str] = &[
    "statusMessage",
    "rootName",
    "rootServiceName",
    "instrumentation:name",
    "instrumentation:version",
];

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
        // Issue #398: the per-query ClickHouse memory ceiling; the
        // production default, so this fixture keeps today's behaviour.
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
    /// OTLP `InstrumentationScope.name`/`version` (issue #192) — each span
    /// carries a distinct populated scope so `compare()` value parity is
    /// exercised per span.
    scope_name: &'static str,
    scope_version: &'static str,
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
            scope_name: "otel-frontend",
            scope_version: "1.0.0",
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
            scope_name: "otel-checkout",
            scope_version: "1.1.0",
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
            scope_name: "otel-worker",
            scope_version: "2.0.0",
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
            scope_name: "otel-idle",
            scope_version: "2.1.0",
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
            // one whole-window bucket for exact counts
            step_ms: window_s * 1_000,
            exemplars: None,
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
             toFixedString(unhex('{pid}'),8), '{name}', '{service}', '{msg}', \
             '{scope_name}', '{scope_version}', {ts}, 1000, \
             {status}, 1, 1, 'x')",
            tid = hex(&tid_bytes(nonce, s.trace)),
            sid = hex(&sid_bytes(s.id)),
            name = s.name,
            service = s.service,
            msg = s.msg,
            scope_name = s.scope_name,
            scope_version = s.scope_version,
            ts = s.ts_ns,
            status = s.status,
        ));
    }
    exec(
        client,
        &format!(
            "INSERT INTO {db}.trace_spans \
             (trace_id, span_id, parent_id, name, service, status_message, \
              scope_name, scope_version, timestamp_ns, \
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
                "scopeSpans": [{
                    "scope": {"name": s.scope_name, "version": s.scope_version},
                    "spans": [span],
                }],
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

/// The WALL-CLOCK budget for the settle poll (issue #460), replacing the
/// old `for _ in 0..90` counter.
///
/// A poll count is not a bound: each iteration costs `curl --max-time 20`
/// PLUS the 2 s sleep, so 90 iterations is `90 x 22 s` = 33 minutes. A CI
/// step that sits for half an hour before failing reads as a hang, gets
/// killed, and the panic message the design relies on is never seen. The
/// true worst case here is 180 s plus one in-flight poll (<= 22 s).
///
/// 180 s preserves the old sleep budget's evident intent while making the
/// number mean what it says. Observed settle latency on this corpus shape
/// is 4-15 polls (roughly 8-30 s), so the deadline is a backstop rather
/// than a tuning parameter.
const SETTLE_BUDGET_S: u64 = 180;

/// Polls Tempo's metrics API until `compare()` returns a STABLE view of the
/// corpus.
///
/// **Not the first non-empty response.** This function used to take that,
/// and its doc used to say the first non-empty response necessarily carries
/// the COMPLETE counts, because the corpus is past-anchored and every bucket
/// the query touches is already finalised. **That claim is false, and it was
/// measured false** (issue #458): the corpus arrives as one OTLP batch and
/// Tempo cuts live-store blocks on a timer, so an already-finalised BUCKET
/// can still be read before every span in it has been flushed. The sibling
/// suite `traces_metrics_filter_differential.rs` measured an 18-span corpus
/// reading back as **10** on its first non-zero poll, and this suite has
/// reddened `schema-it` the same way **four times over a month** — CI runs
/// 30258308527, 30610626381, 31510222855 and 33110647702, 2026-07-27 to
/// 2026-08-27, every one of them on attempt 1 and with the IDENTICAL
/// five-cell divergence: the reference returned a view missing the whole T3
/// trace (`rootName "idle"`, `rootServiceName "batch"`,
/// `instrumentation:name "otel-idle"` and `instrumentation:version "2.1.0"`
/// all absent) with `statusMessage ""` counted 1 against the corpus's 2.
/// Two of the four (30610626381, 33110647702) were re-run and passed on
/// attempt 2 with nothing changed; the other two were never re-run.
/// Partial views are attested by all four; the re-run behaviour by two.
/// The flake predates the change that finally chased it down.
///
/// Past-anchoring is still necessary — it is what stops the poll waiting on
/// a future wall-clock bucket boundary, which is the flake this suite fixed
/// earlier — it is just not sufficient. What the loop waits out is the
/// FLUSH, and a flush is done when the answer stops moving.
///
/// The rule and the loop both live in `pulsus_testkit` (issue #460), so
/// this suite and `compare_arity_differential.rs` cannot drift apart on
/// either. The emptiness decision stays HERE, at the call site, because
/// it is this suite's domain knowledge: an empty count map is not an
/// answer.
fn tempo_counts(api_base: &str, window: (i64, i64)) -> Counts {
    pulsus_testkit::settle_by(
        Instant::now() + Duration::from_secs(SETTLE_BUDGET_S),
        Duration::from_secs(2),
        "Tempo compare() value view",
        || tempo_query_once(api_base, window).filter(|m| !m.is_empty()),
    )
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
    // FAIL-CLOSED on all three: dropping any `env:` block from this
    // suite's CI step PANICS rather than skipping green. On a developer
    // machine with no reference container it still skips cleanly — the
    // guard fires only when the gate is missing inside a CI job that
    // exists to supply it. Both URL gates take the ENDPOINT classifier,
    // because the boolean one counts a gate as set only when its value is
    // exactly `1`.
    let api_endpoint = pulsus_testkit::live_endpoint("PULSUSDB_COMPARE_DIFF_URL");
    let otlp_endpoint = pulsus_testkit::live_endpoint("PULSUSDB_COMPARE_OTLP_URL");
    if !(api_endpoint.is_some()
        && otlp_endpoint.is_some()
        && pulsus_testkit::live_clickhouse_enabled())
    {
        eprintln!(
            "skipping the compare() value differential — set PULSUS_TEST_CLICKHOUSE=1, \
             PULSUSDB_COMPARE_DIFF_URL (Tempo metrics API) and PULSUSDB_COMPARE_OTLP_URL \
             (Tempo OTLP)."
        );
        return;
    }
    let api_base = api_endpoint.expect("checked just above");
    let otlp_base = otlp_endpoint.expect("checked just above");

    // The reverse-order half of the projection leg's isolation hazard
    // (issue #479, code review wave 2). That suite refuses an instance
    // holding another leg's corpus, but its own corpus is resident only
    // AFTER it has run — so the check that catches the two sharing an
    // instance in THIS order has to live here, where the symptom would
    // otherwise appear as a value mismatch against the reference.
    pulsus_testkit::assert_reference_instance_is_free_of(
        &api_base,
        pulsus_testkit::PROJECTION_DIFFERENTIAL_TRACE_HEX,
        "the matched-span projection differential",
    );

    let sec = 1_000_000_000i64;
    // Anchor the corpus ~90s in the PAST (span timestamps `base .. base+3s`).
    // This is the crux of the flake fix. Tempo v3.0.2's live_store only
    // finalises a TraceQL-metrics time bucket once wall-clock passes that
    // bucket's right edge, and it counts spans by their span time. The
    // previous design anchored the corpus at "now" and queried a window
    // ending 120s in the FUTURE, so the value-bearing bucket only finalised
    // ~120s+ later — racing the poll budget and intermittently red-ing main.
    // With `base` in the past, EVERY bucket covering the corpus is already
    // finalised by the time the test queries, so no read waits on a future
    // wall-clock boundary. 90s stays well inside Tempo's ~15m
    // `query_backend_after` live_store window, so live_store serves it.
    //
    // What past-anchoring does NOT buy is a complete FIRST read: a
    // finalised bucket can still be queried before every span of the push
    // has flushed into it. This comment used to claim the opposite — that
    // the first non-empty poll returns complete counts "deterministically"
    // — and that claim is false; see `tempo_counts`, which waits for the
    // answer to stop moving, and the four CI failures it lists.
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
    let db = pulsus_testkit::test_db(&format!("pulsus_cmpdiff_it_{}", hex(&nonce)));
    init_db(&bootstrap, &db).await;
    let client = ChClient::new(ch_config(&db)).await.expect("connect db");
    pulsus_insert(&client, &db, &nonce, &spans).await;
    let engine = TraceEngine::new(
        ChClient::new(ch_config(&db)).await.expect("connect engine"),
        engine_config(),
    );
    let pulsus = pulsus_counts(&engine, window).await;

    // Tempo readback: past-anchored so no bucket is still forming, then
    // waited until the answer STOPS MOVING (see `tempo_counts` — the
    // first non-empty read is not necessarily a complete one).
    let tempo = tempo_counts(&api_base, window);

    eprintln!("pulsus compare() counts: {pulsus:#?}");
    eprintln!("tempo  compare() counts: {tempo:#?}");

    exec(&bootstrap, &format!("DROP DATABASE IF EXISTS {db}")).await;

    // Span-by-span byte-match on the five keys' baseline/selection counts.
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
        "compare() value-parity divergence for statusMessage/rootName/rootServiceName/\
         instrumentation:name/instrumentation:version (REAL PulsusDB + Tempo output):\n  {}\n\n\
         PulsusDB emits an empty statusMessage as a distinct \"\" value (the `arrayFilter` \
         fold-to-nil was removed, #185) to match Tempo v3.0.2, and emits instrumentation \
         name/version verbatim from the per-span scope columns (#192) — a residual divergence \
         here is a NEW mismatch, not the known empty-message case.",
        mism.join("\n  ")
    );
}

/// Issue #458: the partial view that reddened `schema-it`, replayed, and
/// the wait absorbing it.
///
/// **The reproduction is the real numbers**, and they are the same numbers
/// four separate times: CI runs 30258308527, 30610626381, 31510222855 and
/// 33110647702 (2026-07-27 to 2026-08-27) each failed on attempt 1 with the
/// reference returning a view missing the whole T3 trace —
/// `("baseline","rootName","idle")` absent where the corpus has one,
/// likewise its `rootServiceName`, `instrumentation:name` and
/// `instrumentation:version` — and `("baseline","statusMessage","")`
/// counted **1** against the corpus's **2**. Each passed on a re-run with
/// nothing changed. `partial` below is that response; `complete` is the
/// corpus.
///
/// That the divergence is identical across a month is what says this is one
/// mechanism rather than four coincidences, and it is why the fixture can
/// be a single scripted pair.
///
/// The old rule — take the first non-empty read — returns `partial`, and
/// the value-parity assertion at the end of the differential then reports a
/// divergence that is really a flush race. The wait does not: `partial`
/// never repeats three times, because the flush moves on.
///
/// Hermetic: no container, no ClickHouse. It rides the `ci` job, so the
/// rule stays checked even in a lane where the differential itself skips.
#[test]
fn the_wait_absorbs_the_partial_view_that_reddened_ci() {
    // Gated binary: a lost `env:` block in a live job must redden rather
    // than silently skip this file (issue #320) — the same guard the
    // hermetic half of `traces_api_live.rs` carries.
    pulsus_testkit::require_live_gate(pulsus_testkit::CLICKHOUSE_GATE);

    fn counts(cells: &[(&str, &str, &str, i64)]) -> Counts {
        cells
            .iter()
            .map(|(meta, key, val, n)| ((meta.to_string(), key.to_string(), val.to_string()), *n))
            .collect()
    }

    // The response CI actually got: T3 absent, statusMessage "" at 1.
    let partial = counts(&[
        ("baseline", "statusMessage", "", 1),
        ("baseline", "rootName", "frontend", 1),
        ("selection", "statusMessage", "boom", 1),
    ]);
    // The same read one flush later, plus T3.
    let complete = counts(&[
        ("baseline", "statusMessage", "", 2),
        ("baseline", "rootName", "frontend", 1),
        ("baseline", "rootName", "idle", 1),
        ("baseline", "rootServiceName", "batch", 1),
        ("baseline", "instrumentation:name", "otel-idle", 1),
        ("baseline", "instrumentation:version", "2.1.0", 1),
        ("selection", "statusMessage", "boom", 1),
    ]);
    assert_ne!(
        partial, complete,
        "the fixture must actually be a partial view, or this test proves nothing"
    );

    // The CI sequence: nothing, nothing, the partial view, then the
    // complete one from there on. Since issue #460 the emptiness decision
    // lives at the CALL SITE, so the scripted reads go through the same
    // `.filter(|m| !m.is_empty())` the live poll uses — otherwise this
    // test would exercise a different composition from production.
    let empty = Counts::new();
    let sequence = [
        None,
        None,
        Some(partial.clone()),
        Some(complete.clone()),
        Some(complete.clone()),
        Some(complete.clone()),
    ];

    // What the OLD rule would have done — take the first non-empty read.
    let first_non_empty = sequence
        .iter()
        .flatten()
        .find(|m| !m.is_empty())
        .expect("the sequence has a non-empty read");
    assert_eq!(
        *first_non_empty, partial,
        "the first non-empty read is the partial one — this is the bug being fixed"
    );

    // What the wait does.
    let settle = |seq: &[Option<Counts>]| -> Option<(usize, Counts)> {
        let mut wait = pulsus_testkit::StabilityWait::default();
        seq.iter().enumerate().find_map(|(i, read)| {
            wait.observe(read.clone().filter(|m: &Counts| !m.is_empty()))
                .map(|got| (i, got))
        })
    };
    let (index, settled) = settle(&sequence).expect("the wait must settle within the sequence");
    assert_eq!(
        settled, complete,
        "the wait must return the COMPLETE view, never the partial one"
    );
    assert_eq!(
        index, 5,
        "it must settle on the THIRD identical read (index 5), not earlier — settling sooner \
         would mean a single repeat was accepted as stable"
    );

    // Issue #460: an EMPTY read arriving MID-RUN neither settles the run
    // nor resets it. The committed sequence above never contained an empty
    // map and both its `None`s arrived before any run had started, so it
    // could not establish this — the property was asserted for two rounds
    // and tested by nothing.
    let mid_run_empty = [
        Some(complete.clone()), // run -> 1
        Some(complete.clone()), // run -> 2
        Some(empty.clone()),    // the case the old sequence never had
        Some(complete.clone()), // run -> 3 iff the empty neither settled nor reset
    ];
    assert_eq!(
        settle(&mid_run_empty).map(|(i, _)| i),
        Some(3),
        "an empty read arriving mid-run must PRESERVE the run: settling earlier would mean the \
         empty read settled something, later that it reset the run"
    );
    // The control: an empty read between two identical ones does not let
    // three arrive any sooner.
    let control = [
        Some(complete.clone()),
        Some(empty.clone()),
        Some(complete.clone()),
        Some(complete.clone()),
    ];
    assert_eq!(
        settle(&control).map(|(i, _)| i),
        Some(3),
        "the control: the empty read contributes nothing, so the third COMPLETE read is what \
         settles"
    );

    // And the property the number 3 is there for: two identical reads are
    // not enough, because a batch can pause mid-flush for one interval.
    let mut two_only = pulsus_testkit::StabilityWait::default();
    assert!(two_only.observe(Some(partial.clone())).is_none());
    assert!(
        two_only.observe(Some(partial.clone())).is_none(),
        "two identical reads must not settle"
    );
    assert_eq!(
        two_only.observe(Some(partial.clone())),
        Some(partial),
        "three do"
    );
    assert_eq!(
        pulsus_testkit::STABLE_READS,
        3,
        "the assertions above are written against three consecutive identical reads"
    );
}
