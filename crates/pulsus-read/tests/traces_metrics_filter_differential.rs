//! Issue #458 AC 4: the metrics-filter VALUE differential — a real
//! two-system comparison of what `{ nestedSetParent < 0 } |
//! count_over_time()` actually *counts*, structured exactly like
//! [`compare_value_differential`].
//!
//! One shared corpus goes into both systems: `TRACES` traces of
//! `SPANS_PER_TRACE` spans each, **exactly one parentless span per
//! trace**. Then both sides answer the same two queries over the same
//! window, and the identity asserted is scale-invariant:
//!
//! * `{ nestedSetParent < 0 } | count_over_time()` totals `TRACES`
//! * `{} | count_over_time()` totals `TRACES × SPANS_PER_TRACE`
//!
//! This is the only criterion on issue #458 that catches an
//! **accept-preserving answer corruption** against the reference itself.
//! Rendering the root test as the constant `1` moves our total from
//! `TRACES` to the whole span count while every accept/reject verdict in
//! `fixtures/metrics_filter_accept.json` stays exactly where it was;
//! rendering it inverted moves it to the non-root count.
//!
//! # What is compared, and what deliberately is not
//!
//! **The multiset of non-null sample values per series, never the
//! timestamps.** Bucket labelling already differs between the two systems
//! for the same window and data, and the instant route's envelope differs
//! too. Both are pre-existing and out of this issue's scope; a
//! differential that compared timestamps would fail for a reason this
//! change did not cause. The totals are bucketing-independent, which is
//! what makes them a property of the corpus rather than of the step.
//!
//! Gate: skips unless `PULSUS_TEST_CLICKHOUSE=1` AND
//! `PULSUSDB_METRICS_FILTER_DIFF_URL` (the reference's metrics API base)
//! AND `PULSUSDB_METRICS_FILTER_OTLP_URL` (its OTLP HTTP base) are all
//! set. Run locally:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=19123 \
//!   PULSUSDB_METRICS_FILTER_DIFF_URL=http://localhost:13200 \
//!   PULSUSDB_METRICS_FILTER_OTLP_URL=http://localhost:4318 \
//!   cargo test -p pulsus-read --test traces_metrics_filter_differential -- --nocapture
//! ```
//!
//! Clean-room: no reference source, grammar or test corpus is read — the
//! fixture is our own authorship and the reference's answers are read
//! back as black-box runtime output.

use std::process::Command;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_read::traces::metrics_plan::{MetricsParams, plan_trace_metrics};
use pulsus_read::{TraceEngine, TraceReadConfig};
use pulsus_schema::{RenderCtx, run_init};

/// Traces in the corpus — one parentless span each.
const TRACES: u8 = 6;
/// Spans per trace: one root plus two children.
const SPANS_PER_TRACE: u8 = 3;
/// Total spans. Deliberately ≠ `TRACES` and ≠ `SPANS − TRACES`, asserted
/// below: a corpus where two of the three totals coincide cannot tell a
/// correct lowering from an inverted one.
const SPANS: u8 = TRACES * SPANS_PER_TRACE;

const NS_PER_S: i64 = 1_000_000_000;
/// The reference's metrics step. Small and fixed: the totals are additive
/// across disjoint buckets, and a single whole-window bucket can align its
/// right edge into the future and read back empty.
const STEP_S: i64 = 60;

fn ch_config(db: &str) -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: db.to_string(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(60),
        ..ChConnConfig::default()
    }
}

fn engine_config() -> TraceReadConfig {
    TraceReadConfig {
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
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

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// A per-run trace id: a random 15-byte nonce prefix plus the trace index,
/// so reference runs never collide.
fn tid(nonce: &[u8; 16], trace: u8) -> [u8; 16] {
    let mut b = *nonce;
    b[15] = trace + 1;
    b
}

/// Span ids are `1..` — never zero, because a root whose own id was the
/// all-zero `FixedString(8)` would make its children look parentless.
fn sid(trace: u8, index: u8) -> [u8; 8] {
    let mut b = [0u8; 8];
    b[6] = trace + 1;
    b[7] = index + 1;
    b
}

/// One fixture span: `(trace, index within trace, timestamp)`. Index `0`
/// is the trace's single parentless span.
struct SpanDef {
    trace: u8,
    index: u8,
    ts_ns: i64,
}

fn corpus(base: i64) -> Vec<SpanDef> {
    let mut out = Vec::new();
    for trace in 0..TRACES {
        for index in 0..SPANS_PER_TRACE {
            out.push(SpanDef {
                trace,
                index,
                ts_ns: base + i64::from(trace) * NS_PER_S + i64::from(index) * 100_000_000,
            });
        }
    }
    out
}

async fn pulsus_insert(
    client: &ChClient,
    db: &str,
    service: &str,
    nonce: &[u8; 16],
    spans: &[SpanDef],
) {
    let rows: Vec<String> = spans
        .iter()
        .map(|s| {
            let parent = if s.index == 0 {
                "0000000000000000".to_string()
            } else {
                hex(&sid(s.trace, 0))
            };
            format!(
                "(toFixedString(unhex('{tid}'),16), toFixedString(unhex('{sid}'),8), \
                 toFixedString(unhex('{parent}'),8), 'op', '{service}', {ts}, 1000000, 0, 1, 1, 'x')",
                tid = hex(&tid(nonce, s.trace)),
                sid = hex(&sid(s.trace, s.index)),
                ts = s.ts_ns,
            )
        })
        .collect();
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

fn otlp_push(otlp_base: &str, service: &str, nonce: &[u8; 16], spans: &[SpanDef]) {
    let resource_spans: Vec<serde_json::Value> = spans
        .iter()
        .map(|s| {
            let mut span = serde_json::json!({
                "traceId": hex(&tid(nonce, s.trace)),
                "spanId": hex(&sid(s.trace, s.index)),
                "name": "op",
                "startTimeUnixNano": s.ts_ns.to_string(),
                "endTimeUnixNano": (s.ts_ns + 1_000_000).to_string(),
                "kind": 1,
            });
            if s.index != 0 {
                span["parentSpanId"] = serde_json::Value::String(hex(&sid(s.trace, 0)));
            }
            serde_json::json!({
                "resource": {"attributes": [
                    {"key": "service.name", "value": {"stringValue": service}}
                ]},
                "scopeSpans": [{
                    "scope": {"name": "diff-scope", "version": "1.0.0"},
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

/// The sum of every non-null sample across every series our own engine
/// returns for `q`. Built with `plan_trace_metrics` and executed through
/// `TraceEngine` — never over hand-assembled SQL.
async fn pulsus_total(engine: &TraceEngine, q: &str, window: (i64, i64)) -> f64 {
    let query = pulsus_traceql::parse(q).unwrap_or_else(|e| panic!("{q} parses: {e:?}"));
    let plan = plan_trace_metrics(
        &query,
        &MetricsParams {
            start_ns: window.0,
            end_ns: window.1,
            step_s: STEP_S,
        },
        &engine.metrics_ctx(),
    )
    .unwrap_or_else(|e| panic!("{q} plans: {e:?}"));
    engine
        .metrics_range(&plan)
        .await
        .unwrap_or_else(|e| panic!("{q} executes: {e}"))
        .series
        .iter()
        .flat_map(|s| s.samples.iter())
        .map(|(_, v)| *v)
        .filter(|v| !v.is_nan())
        .sum()
}

/// The same total from the reference, read back from its metrics API.
/// `None` while the push has not yet flushed.
fn reference_total_once(api_base: &str, q: &str, window: (i64, i64)) -> Option<f64> {
    let url = format!("{}/api/metrics/query_range", api_base.trim_end_matches('/'));
    let out = Command::new("curl")
        .args(["-s", "-G", "--max-time", "20"])
        .args(["--data-urlencode", &format!("q={q}")])
        .args([
            "--data-urlencode",
            &format!("start={}", window.0 / NS_PER_S),
        ])
        .args(["--data-urlencode", &format!("end={}", window.1 / NS_PER_S)])
        .args(["--data-urlencode", &format!("step={STEP_S}s")])
        .arg(&url)
        .output()
        .expect("curl on PATH");
    let body: serde_json::Value = serde_json::from_slice(&out.stdout).ok()?;
    let series = body.get("series")?.as_array()?;
    if series.is_empty() {
        return None;
    }
    // Sum every non-null sample value. Timestamps are never read.
    let total: f64 = series
        .iter()
        .filter_map(|s| s.get("samples").and_then(|v| v.as_array()))
        .flatten()
        .map(|sm| sm.get("value").and_then(|v| v.as_f64()).unwrap_or(0.0))
        .sum();
    Some(total)
}

/// How many consecutive identical non-zero reads count as "flushed".
///
/// **Not a first-non-empty read.** The corpus arrives as one OTLP batch
/// and the reference cuts live-store blocks on a timer, so the first
/// non-empty answer can be a PARTIAL one — measured: a 18-span corpus read
/// back as 10 on the first non-zero poll. Taking that would turn a
/// deliberate break into a failure of the CONTROL assertion (the corpus
/// did not arrive) instead of the decision assertion, which is a
/// misleading transcript at best and a flake on a green tree at worst.
/// Three equal reads two seconds apart span more than one block-cut
/// period (`max_block_duration: 2s`, `flush_check_period: 1s` in
/// `ci/tempo/tempo-compare.yaml`).
const STABLE_READS: usize = 3;

/// Polls until the reference's answer for `q` is non-zero and STABLE. The
/// corpus is anchored in the PAST and the window ends in the past, so
/// every bucket the query touches is already finalised — the loop waits
/// out the flush, not a future wall-clock boundary.
fn reference_total(api_base: &str, q: &str, window: (i64, i64)) -> f64 {
    let mut stable: Option<(f64, usize)> = None;
    for _ in 0..90 {
        if let Some(total) = reference_total_once(api_base, q, window)
            && total > 0.0
        {
            stable = match stable {
                Some((prev, n)) if prev == total => Some((total, n + 1)),
                _ => Some((total, 1)),
            };
            if stable.is_some_and(|(_, n)| n >= STABLE_READS) {
                return total;
            }
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    panic!(
        "the reference never returned a stable non-empty total for {q:?} within the poll budget \
         (last observation {stable:?})"
    );
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

#[tokio::test(flavor = "multi_thread")]
async fn nested_set_root_counts_match_the_reference_on_a_shared_corpus() {
    // Fail-closed on BOTH endpoint gates as well as the ClickHouse one
    // (issue #320, and issue #458 review round 3). Without these two
    // lines the leg has a hole its `schema-it` step cannot see: with
    // `PULSUS_TEST_CLICKHOUSE=1` still set but the two URL variables
    // dropped, the `else` arm below would print a skip notice and report
    // GREEN in the job that exists to run it. `require_live_endpoint_gate`
    // — not `require_live_gate` — because these values are URLs, and the
    // boolean helper counts a gate as set only when it is exactly `"1"`.
    pulsus_testkit::require_live_endpoint_gate("PULSUSDB_METRICS_FILTER_DIFF_URL");
    pulsus_testkit::require_live_endpoint_gate("PULSUSDB_METRICS_FILTER_OTLP_URL");
    let (Ok(api_base), Ok(otlp_base), true) = (
        std::env::var("PULSUSDB_METRICS_FILTER_DIFF_URL"),
        std::env::var("PULSUSDB_METRICS_FILTER_OTLP_URL"),
        pulsus_testkit::live_clickhouse_enabled(),
    ) else {
        eprintln!(
            "skipping the metrics-filter value differential — set PULSUS_TEST_CLICKHOUSE=1, \
             PULSUSDB_METRICS_FILTER_DIFF_URL (the reference's metrics API) and \
             PULSUSDB_METRICS_FILTER_OTLP_URL (its OTLP HTTP base)"
        );
        return;
    };

    // Validity gate, before anything is measured: the three totals the
    // identities compare must be pairwise distinct, or a correct lowering
    // and a broken one answer the same number.
    assert_ne!(TRACES, SPANS - TRACES, "T and S-T must differ");
    assert_ne!(TRACES, SPANS, "T and S must differ");
    assert_ne!(SPANS - TRACES, SPANS, "S-T and S must differ");

    // Anchor the corpus ~90 s in the PAST and query a window that also
    // ends in the past, so every reference bucket is already finalised
    // (the `compare_value_differential` flake fix, same reasoning).
    let base = now_ns() - 90 * NS_PER_S;
    let window = (base - 60 * NS_PER_S, base + 60 * NS_PER_S);
    let nonce = *uuid::Uuid::new_v4().as_bytes();
    let spans = corpus(base);
    assert_eq!(spans.len(), usize::from(SPANS), "corpus size");

    // Every query below is scoped to THIS run's service name. The
    // reference is a long-lived shared container and its store is not
    // reset between runs, so an unscoped `{}` counts a previous run's
    // corpus too when the two windows overlap — measured: 36 where 18 was
    // seeded, two runs a minute apart. Scoping also puts the root
    // conjunct beside a hoistable service equality, which is the shape
    // the Grafana panel actually sends.
    let service = format!("ns-diff-{}", hex(&nonce[..4]));
    otlp_push(&otlp_base, &service, &nonce, &spans);

    let bootstrap = ChClient::new(ch_config("default"))
        .await
        .expect("connect bootstrap");
    let db = pulsus_testkit::test_db(&format!("pulsus_nsfilterdiff_it_{}", hex(&nonce)));
    init_db(&bootstrap, &db).await;
    let client = ChClient::new(ch_config(&db)).await.expect("connect db");
    pulsus_insert(&client, &db, &service, &nonce, &spans).await;
    let engine = TraceEngine::new(
        ChClient::new(ch_config(&db)).await.expect("connect engine"),
        engine_config(),
    );

    let all_q = format!(r#"{{ resource.service.name = "{service}" }} | count_over_time()"#);
    let roots_q = format!(
        r#"{{ resource.service.name = "{service}" && nestedSetParent < 0 }} | count_over_time()"#
    );

    let ours_roots = pulsus_total(&engine, &roots_q, window).await;
    let ours_all = pulsus_total(&engine, &all_q, window).await;

    let ref_all = reference_total(&api_base, &all_q, window);
    let ref_roots = reference_total(&api_base, &roots_q, window);

    eprintln!(
        "issue #458 AC 4: ours roots={ours_roots} all={ours_all}; \
         reference roots={ref_roots} all={ref_all} (T={TRACES}, S={SPANS})"
    );

    exec(&bootstrap, &format!("DROP DATABASE IF EXISTS {db}")).await;

    // The control first: if the whole-corpus total is wrong on either
    // side, the root total proves nothing about the lowering.
    assert_eq!(
        ours_all,
        f64::from(SPANS),
        "our whole-corpus total must be S ({SPANS})"
    );
    assert_eq!(
        ref_all,
        f64::from(SPANS),
        "the reference's whole-corpus total must be S ({SPANS}) — if this fails the corpus did \
         not arrive intact and nothing below is conclusive"
    );

    // The decision assertion, on both sides independently and then
    // against each other.
    assert_eq!(
        ours_roots,
        f64::from(TRACES),
        "one parentless span per trace: our root total must be T ({TRACES}), not S ({SPANS}) \
         (a match-all lowering) and not S-T ({}) (an inverted one)",
        SPANS - TRACES
    );
    assert_eq!(
        ref_roots,
        f64::from(TRACES),
        "the reference's root total must be T ({TRACES})"
    );
    assert_eq!(
        ours_roots, ref_roots,
        "value parity on {{ nestedSetParent < 0 }} | count_over_time()"
    );
}
