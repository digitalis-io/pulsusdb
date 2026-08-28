//! Env-gated `compare()` ARITY value differential (issue #460) — a REAL
//! two-system differential over the two argument forms Grafana Traces
//! Drilldown's Comparison tab generates and we used to refuse.
//!
//! Its sibling [`compare_value_differential`] gates which *values*
//! `compare()` emits for a fixed one-argument query. This leg gates what
//! the **arguments** do:
//!
//!   * `topN` (argument 2, reference default **10**) trims each attribute
//!     to its top `topN` values PER SIDE — so it changes the answer to the
//!     one-argument form too, not merely to the new arities;
//!   * `start`/`end` (arguments 3 and 4) repartition the population into
//!     baseline/selection on the half-open `(start, end]` interval — they
//!     do not filter it, so the two totals still sum to the whole corpus.
//!
//! For each corpus it ingests the SAME spans into both systems:
//!
//!   * **PulsusDB side** — written to a throwaway ClickHouse DB and read
//!     back through this crate's REAL metrics executor
//!     ([`TraceEngine::metrics_range`]);
//!   * **reference side** — the same spans pushed to
//!     `grafana/tempo:3.0.2` over OTLP and read back from its
//!     `/api/metrics/query_range`.
//!
//! **The fixtures are tie-free on purpose.** The reference ranks values
//! with `sort.Slice`, which is not stable, so its survivors among EQUAL
//! counts are arbitrary — measured twice on this issue, two different
//! arbitrary sets. Every value below therefore has a DISTINCT count, so
//! the top-N set is unique and no assertion here depends on tie order.
//! Our own order (descending count, then ascending value) is deterministic
//! and is ledgered as a deliberate refinement,
//! `traceql-compare-topn-tie-order`.
//!
//! **Per-run service names.** Both corpora are addressed by
//! `resource.service.name`, and the reference container outlives a single
//! run, so each corpus carries a per-run nonce in its service name. A
//! fixed name would double every count on the second run in the same
//! container.
//!
//! Gate: skips unless `PULSUS_TEST_CLICKHOUSE=1` AND
//! `PULSUSDB_COMPARE_DIFF_URL` (reference metrics API base) AND
//! `PULSUSDB_COMPARE_OTLP_URL` (reference OTLP HTTP base) are all set.
//! Run locally:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=18123 \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   PULSUSDB_COMPARE_DIFF_URL=http://localhost:13460 \
//!   PULSUSDB_COMPARE_OTLP_URL=http://localhost:44318 \
//!   cargo test -p pulsus-read --test compare_arity_differential -- --nocapture
//! ```
//!
//! Clean-room: no Tempo/Grafana source, grammar or test corpus is read —
//! the fixtures are our own authorship and the reference values are read
//! back as black-box runtime output.

use std::collections::BTreeMap;
use std::process::Command;
use std::time::{Duration, Instant};

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_read::traces::metrics_plan::{MetricsParams, plan_trace_metrics};
use pulsus_read::{MetricLabelValue, TraceEngine, TraceReadConfig};
use pulsus_schema::{RenderCtx, run_init};

const SEC: i64 = 1_000_000_000;
const MS: i64 = 1_000_000;

/// The reference metrics-query step. Small and FIXED, for the reason the
/// sibling suite records: a single whole-window bucket can land its right
/// edge in the future and read back empty. `compare()` counts are additive
/// across disjoint buckets, and `topN` ranks on the SUM of a value's
/// counts over the whole query (`engine_metrics_compare.go:535-563`), so
/// summing per-step samples yields the same totals AND the same top-N set
/// a single bucket would.
const TEMPO_STEP_S: i64 = 60;

/// The WALL-CLOCK budget for the settle poll — see
/// [`pulsus_testkit::settle_by`] for why this is not a poll count.
const SETTLE_BUDGET_S: u64 = 180;

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

/// One fixture span. Every span is `status = error` (OTLP code 2), so the
/// `{ status = error }` selection admits the whole corpus and the ONLY
/// thing that can put a span in `baseline` is the selection window.
struct SpanDef {
    idx: u32,
    service: String,
    name: &'static str,
    msg: String,
    /// An optional `span.<key>` attribute, written to `trace_attrs_idx`
    /// here and emitted as an OTLP span attribute there.
    attr: Option<(&'static str, String)>,
    ts_ns: i64,
}

/// **Corpus E — the topN fixture.** 78 spans, one service, one span name,
/// twelve DISTINCT `statusMessage` values `m00..m11` where `m<i>` occurs
/// exactly `i + 1` times. Every count is distinct, so the top-N set is
/// unique at every `topN` and tie order never enters an assertion.
///
/// `m<i>`'s spans sit at `base + i·1s + (j+1)·1ms`, which is what makes
/// corpus E also the `(start, end]` fixture: a window between two whole
/// seconds cuts cleanly between message groups.
fn corpus_topn(base: i64, service: &str) -> Vec<SpanDef> {
    let mut spans = Vec::new();
    let mut idx = 0u32;
    for i in 0..12i64 {
        for j in 0..=i {
            idx += 1;
            spans.push(SpanDef {
                idx,
                service: service.to_string(),
                name: "ope",
                msg: format!("m{i:02}"),
                attr: None,
                ts_ns: base + i * SEC + (j + 1) * MS,
            });
        }
    }
    assert_eq!(spans.len(), 78, "1 + 2 + … + 12 = 78");
    spans
}

/// **Corpus B — the boundary fixture.** Ten spans, one per second at
/// `base + i·1s`, each carrying `span.idx = i<i>`. One span per value
/// makes every membership question a set question rather than a counting
/// one, which is what the `(start, end]` boundary needs.
fn corpus_boundary(base: i64, service: &str) -> Vec<SpanDef> {
    (0..10i64)
        .map(|i| SpanDef {
            idx: 1000 + i as u32,
            service: service.to_string(),
            name: "opb",
            msg: String::new(),
            attr: Some(("idx", format!("i{i}"))),
            ts_ns: base + i * SEC,
        })
        .collect()
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

fn sid_bytes(idx: u32) -> [u8; 8] {
    let mut b = [0u8; 8];
    b[4..].copy_from_slice(&idx.to_be_bytes());
    b
}

/// A per-run trace id: a random 12-byte nonce prefix plus the span index,
/// so reference runs never collide and every span is its own trace (this
/// fixture has no parent/child structure to preserve).
fn tid_bytes(nonce: &[u8; 16], idx: u32) -> [u8; 16] {
    let mut b = *nonce;
    b[12..].copy_from_slice(&idx.to_be_bytes());
    b
}

// ---------------------------------------------------------------------------
// The per-(meta_type, key, value) count map — the shared comparison shape
// ---------------------------------------------------------------------------

type Counts = BTreeMap<(String, String, String), i64>;

/// Renders a count map for a report, one cell per line and sorted, so a
/// failure prints something a reader can diff by eye.
fn render(counts: &Counts) -> String {
    counts
        .iter()
        .map(|((meta, key, val), n)| format!("  {meta:<16} {key}={val:<6} -> {n}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn counts_from(cells: &[(&str, &str, &str, i64)]) -> Counts {
    cells
        .iter()
        .map(|(meta, key, val, n)| ((meta.to_string(), key.to_string(), val.to_string()), *n))
        .collect()
}

// ---------------------------------------------------------------------------
// PulsusDB side
// ---------------------------------------------------------------------------

async fn pulsus_insert(client: &ChClient, db: &str, nonce: &[u8; 16], spans: &[SpanDef]) {
    let mut rows = Vec::new();
    for s in spans {
        rows.push(format!(
            "(toFixedString(unhex('{tid}'),16), toFixedString(unhex('{sid}'),8), \
             toFixedString(unhex('0000000000000000'),8), '{name}', '{service}', '{msg}', \
             '', '', {ts}, 1000, 2, 1, 1, 'x')",
            tid = hex(&tid_bytes(nonce, s.idx)),
            sid = hex(&sid_bytes(s.idx)),
            name = s.name,
            service = s.service,
            msg = s.msg,
            ts = s.ts_ns,
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

    let attr_rows: Vec<String> = spans
        .iter()
        .filter_map(|s| {
            let (key, val) = s.attr.as_ref()?;
            Some(format!(
                "(toDate(fromUnixTimestamp64Nano({ts})), '{key}', '{val}', 'span', NULL, {ts}, \
                 toFixedString(unhex('{tid}'),16), toFixedString(unhex('{sid}'),8), 1000)",
                ts = s.ts_ns,
                tid = hex(&tid_bytes(nonce, s.idx)),
                sid = hex(&sid_bytes(s.idx)),
            ))
        })
        .collect();
    if !attr_rows.is_empty() {
        exec(
            client,
            &format!(
                "INSERT INTO {db}.trace_attrs_idx \
                 (date, key, val, scope, val_num, timestamp_ns, trace_id, span_id, duration_ns) \
                 VALUES {}",
                attr_rows.join(", ")
            ),
        )
        .await;
    }
}

/// Reads `query` back through the REAL metrics executor and projects the
/// `(meta, key, value) -> count` map for `keys`, over ONE whole-window
/// bucket. `*_total` rows are kept: this suite gates the denominators too.
async fn pulsus_counts(
    engine: &TraceEngine,
    query: &str,
    window: (i64, i64),
    keys: &[&str],
) -> Counts {
    let window_s = (window.1 - window.0) / SEC;
    let parsed = pulsus_traceql::parse(query).unwrap_or_else(|e| panic!("{query}: {e}"));
    let plan = plan_trace_metrics(
        &parsed,
        &MetricsParams {
            start_ns: window.0,
            end_ns: window.1,
            step_s: window_s,
        },
        &engine.metrics_ctx(),
    )
    .unwrap_or_else(|e| panic!("{query}: plan failed: {e}"));
    let res = engine
        .metrics_range(&plan)
        .await
        .unwrap_or_else(|e| panic!("{query}: execute failed: {e}"));

    let mut out = Counts::new();
    for s in &res.series {
        let Some(MetricLabelValue::Str(meta)) = s
            .labels
            .iter()
            .find(|l| l.key == "__meta_type")
            .map(|l| l.value.clone())
        else {
            continue;
        };
        for l in &s.labels {
            if l.key == "__meta_type" || !keys.contains(&l.key.as_str()) {
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

// ---------------------------------------------------------------------------
// Reference side
// ---------------------------------------------------------------------------

fn otlp_push(otlp_base: &str, nonce: &[u8; 16], spans: &[SpanDef]) {
    let resource_spans: Vec<serde_json::Value> = spans
        .iter()
        .map(|s| {
            let mut span = serde_json::json!({
                "traceId": hex(&tid_bytes(nonce, s.idx)),
                "spanId": hex(&sid_bytes(s.idx)),
                "name": s.name,
                "startTimeUnixNano": s.ts_ns.to_string(),
                "endTimeUnixNano": (s.ts_ns + 1000).to_string(),
                "kind": 1,
                "status": { "code": 2, "message": s.msg },
            });
            if let Some((key, val)) = &s.attr {
                span["attributes"] = serde_json::json!([
                    { "key": key, "value": { "stringValue": val } }
                ]);
            }
            serde_json::json!({
                "resource": {"attributes": [
                    {"key": "service.name", "value": {"stringValue": s.service}}
                ]},
                "scopeSpans": [{ "scope": {"name": "pulsus-arity-fixture"}, "spans": [span] }],
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
            "30",
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

fn tempo_body(api_base: &str, query: &str, window: (i64, i64)) -> String {
    let url = format!("{}/api/metrics/query_range", api_base.trim_end_matches('/'));
    let out = Command::new("curl")
        .args(["-s", "-G", "--max-time", "20"])
        .args(["--data-urlencode", &format!("q={query}")])
        .args(["--data-urlencode", &format!("start={}", window.0 / SEC)])
        .args(["--data-urlencode", &format!("end={}", window.1 / SEC)])
        .args(["--data-urlencode", &format!("step={TEMPO_STEP_S}s")])
        .arg(&url)
        .output()
        .expect("curl on PATH");
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn tempo_query_once(
    api_base: &str,
    query: &str,
    window: (i64, i64),
    keys: &[&str],
) -> Option<Counts> {
    let body: serde_json::Value =
        serde_json::from_str(&tempo_body(api_base, query, window)).ok()?;
    let series = body.get("series")?.as_array()?;
    let mut map = Counts::new();
    for s in series {
        let labels = s.get("labels")?.as_array()?;
        let meta = label_str(labels, "__meta_type")?;
        // A sample `value` is OMITTED when zero (protojson default), and a
        // total series carries one sample per bucket — so this sums across
        // buckets and reads a missing `value` as 0.0. An assertion written
        // against `samples[0].value` would fail on a correct answer.
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
            if key == "__meta_type" || !keys.contains(&key) {
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

/// Reads one settled reference answer for `query`.
///
/// The settle rule and the wall-clock loop come from `pulsus_testkit`,
/// shared with `compare_value_differential.rs` so the two suites cannot
/// drift. The emptiness decision stays here: an empty count map is not an
/// answer.
///
/// **This is called only AFTER [`await_complete_corpus`] has proved the
/// fixture is fully loaded**, and that ordering is the whole design — see
/// that function for why settling alone is not a readiness oracle.
fn tempo_counts(api_base: &str, query: &str, window: (i64, i64), keys: &[&str]) -> Counts {
    pulsus_testkit::settle_by(
        Instant::now() + Duration::from_secs(SETTLE_BUDGET_S),
        Duration::from_secs(2),
        &format!("reference compare() view for {query:?}"),
        || tempo_query_once(api_base, query, window, keys).filter(|m| !m.is_empty()),
    )
}

/// Blocks until the reference holds the WHOLE of one corpus, judged by an
/// answer whose value is known in advance.
///
/// **Why settling is not enough, measured.** "The same non-empty payload
/// three times running" proves the view stopped changing; it does not
/// prove the fixture finished loading, because **three stable reads of a
/// partial view are still three stable reads**. Across 15 clean runs of
/// this suite before this gate existed, 13 passed and 2 failed with the
/// reference's B1 view having settled PARTIAL — once at `m00..m07` with
/// `selection_total = 36`, once at `m00..m06` with `28` — while a later
/// query in the same run returned the full 78. The stability rule was
/// measuring the wrong property.
///
/// **What this measures instead.** Completeness here has a known answer:
/// the corpus is ours, so its total and its distinct-value count are
/// constants. So the gate asserts the answer rather than the absence of
/// change. The probe is:
///
///   * **service-scoped** — `{resource.service.name="<per-run service>"}`,
///     so another suite's spans in a shared reference container cannot
///     satisfy it (and, symmetrically, cannot break it: the base suite's
///     polluted run was seen carrying THIS suite's labels, so the
///     contamination runs both ways);
///   * **untrimmed** — `topN` is set above the corpus's distinct-value
///     count, so a `topN` trim can never be mistaken for a missing span;
///   * checked on **both** numbers, the `*_total` denominator AND the
///     distinct present-value count, because either alone admits a
///     partial view that happens to match the other.
///
/// It still runs through the shared [`pulsus_testkit::settle_by`], so the
/// completeness predicate is ANDed with three identical reads rather than
/// replacing them. That is redundant by construction — no span arrives
/// after the total is reached — and it costs four seconds, which is worth
/// paying to keep one settle implementation in the workspace.
fn await_complete_corpus(
    api_base: &str,
    service: &str,
    key: &str,
    window: (i64, i64),
    want_total: i64,
    want_values: usize,
) {
    // `topN` far above the distinct-value count: this probe must never
    // trim, or a trimmed view would read as an incomplete one.
    let query =
        format!(r#"{{resource.service.name="{service}"}} | compare({{status=error}}, 1000)"#);
    let keys = [key];
    pulsus_testkit::settle_by(
        Instant::now() + Duration::from_secs(SETTLE_BUDGET_S),
        Duration::from_secs(2),
        &format!(
            "reference corpus {service:?} to load completely ({key}: selection_total \
             {want_total}, {want_values} distinct values) — the per-poll progression is on \
             stderr above, since a settle timeout can only report its last SETTLED observation"
        ),
        || {
            let counts = tempo_query_once(api_base, &query, window, &keys)?;
            let total = counts
                .get(&(
                    "selection_total".to_string(),
                    key.to_string(),
                    "nil".to_string(),
                ))
                .copied()
                .unwrap_or(0);
            let values = counts
                .keys()
                .filter(|(meta, k, _)| meta == "selection" && k == key)
                .count();
            let complete = total == want_total && values == want_values;
            if !complete {
                // `settle_by`'s timeout message can only name its last
                // SETTLED observation, which is `None` here by
                // construction — so the progression is logged as it
                // happens. Bounded: one line per 2 s poll, inside a 180 s
                // deadline. This is the line that would have named the
                // two stable-partial views (36 and 28) directly.
                eprintln!(
                    "[readiness] {service}: {key} selection_total={total}/{want_total} \
                     values={values}/{want_values} — not loaded yet"
                );
            }
            complete.then_some(counts)
        },
    );
    eprintln!("[readiness] {service}: complete ({want_total}, {want_values} values)");
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
// The oracle rows
// ---------------------------------------------------------------------------

/// One differential row: a query, the answer BOTH systems must give, and
/// how much of the population `topN` trimmed away on each side.
struct Row {
    label: &'static str,
    /// `{}` is replaced by the per-run service name; `{s}`/`{e}` by the
    /// selection-window bounds this row asks for, in unix nanoseconds.
    query: String,
    keys: &'static [&'static str],
    want: Counts,
    /// `(meta, key, Σ counts of the values topN dropped on that side)` —
    /// the AC 18 balance term. A side with nothing trimmed carries 0.
    trimmed: Vec<(&'static str, &'static str, i64)>,
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
async fn compare_arity_differential() {
    let (Ok(api_base), Ok(otlp_base), true) = (
        std::env::var("PULSUSDB_COMPARE_DIFF_URL"),
        std::env::var("PULSUSDB_COMPARE_OTLP_URL"),
        pulsus_testkit::live_clickhouse_enabled(),
    ) else {
        eprintln!(
            "skipping the compare() arity differential — set PULSUS_TEST_CLICKHOUSE=1, \
             PULSUSDB_COMPARE_DIFF_URL (reference metrics API) and PULSUSDB_COMPARE_OTLP_URL \
             (reference OTLP)."
        );
        return;
    };

    // Anchor ~90 s in the PAST and query a window that also ENDS in the
    // past, so every reference bucket the query touches is already
    // finalised and no read waits on a future wall-clock boundary. See
    // `compare_value_differential.rs` for why past-anchoring is necessary
    // but not sufficient (the settle wait covers the rest).
    let base = ((now_ns() - 90 * SEC) / SEC) * SEC;
    let window = (base - 60 * SEC, base + 60 * SEC);
    let nonce = *uuid::Uuid::new_v4().as_bytes();
    let run = hex(&nonce[..4]);
    let svc_e = format!("svc-e-{run}");
    let svc_b = format!("svc-b5-{run}");

    let spans_e = corpus_topn(base, &svc_e);
    let spans_b = corpus_boundary(base, &svc_b);

    // Reference: push first so it has the whole poll window to become
    // queryable while the PulsusDB side runs.
    otlp_push(&otlp_base, &nonce, &spans_e);
    otlp_push(&otlp_base, &nonce, &spans_b);

    let bootstrap = ChClient::new(ch_config("default"))
        .await
        .expect("connect bootstrap");
    let db = pulsus_testkit::test_db(&format!("pulsus_cmparity_it_{run}"));
    init_db(&bootstrap, &db).await;
    let client = ChClient::new(ch_config(&db)).await.expect("connect db");
    pulsus_insert(&client, &db, &nonce, &spans_e).await;
    pulsus_insert(&client, &db, &nonce, &spans_b).await;
    let engine = TraceEngine::new(
        ChClient::new(ch_config(&db)).await.expect("connect engine"),
        engine_config(),
    );

    const MSG: &[&str] = &["statusMessage"];
    const IDX: &[&str] = &["span.idx"];
    // Corpus E's `(start, end]` window: (base+5.5s, base+8.5s], chosen off
    // every span timestamp so no boundary is exercised by accident here —
    // the boundary is row B5's job.
    let (e_start, e_end) = (base + 5 * SEC + 500 * MS, base + 8 * SEC + 500 * MS);
    // Corpus B's window: EXACTLY i4's and i7's start times. `>` is strict
    // and `<=` is inclusive, so i4 is baseline and i7 is selection.
    let (b_start, b_end) = (base + 4 * SEC, base + 7 * SEC);

    // B2's full ladder, reused to build B1's trimmed-to-10 subset.
    let ladder: Vec<(String, i64)> = (0..12i64).map(|i| (format!("m{i:02}"), i + 1)).collect();
    let msg_cell = |meta: &str, val: &str, n: i64| -> (String, String, String, i64) {
        (
            meta.to_string(),
            "statusMessage".to_string(),
            val.to_string(),
            n,
        )
    };
    let to_counts = |cells: Vec<(String, String, String, i64)>| -> Counts {
        cells
            .into_iter()
            .map(|(a, b, c, n)| ((a, b, c), n))
            .collect()
    };

    let rows: Vec<Row> = vec![
        // ---- B1: the DEFAULT topN=10 bites on the one-argument form. ---
        // Twelve distinct values, ten survive; `m00` (count 1) and `m01`
        // (count 2) are dropped. Before issue #460 we returned all twelve.
        Row {
            label: "B1 compare(f) — the default topN=10 trims",
            query: format!(r#"{{resource.service.name="{svc_e}"}} | compare({{status=error}})"#),
            keys: MSG,
            want: to_counts(
                ladder
                    .iter()
                    .filter(|(_, n)| *n >= 3)
                    .map(|(v, n)| msg_cell("selection", v, *n))
                    .chain(std::iter::once(msg_cell("selection_total", "nil", 78)))
                    .collect(),
            ),
            // 1 + 2 — trimmed values are DROPPED, never folded into the
            // denominator, which is why the total is still 78.
            trimmed: vec![("selection", "statusMessage", 3)],
        },
        // ---- B2: topN above the distinct count trims nothing. ----------
        Row {
            label: "B2 compare(f, 12) — nothing to trim",
            query: format!(
                r#"{{resource.service.name="{svc_e}"}} | compare({{status=error}}, 12)"#
            ),
            keys: MSG,
            want: to_counts(
                ladder
                    .iter()
                    .map(|(v, n)| msg_cell("selection", v, *n))
                    .chain(std::iter::once(msg_cell("selection_total", "nil", 78)))
                    .collect(),
            ),
            trimmed: vec![("selection", "statusMessage", 0)],
        },
        // ---- B3: an explicit small topN. The denominator does NOT move.
        Row {
            label: "B3 compare(f, 3) — the total stays the whole population",
            query: format!(r#"{{resource.service.name="{svc_e}"}} | compare({{status=error}}, 3)"#),
            keys: MSG,
            want: to_counts(
                ladder
                    .iter()
                    .filter(|(_, n)| *n >= 10)
                    .map(|(v, n)| msg_cell("selection", v, *n))
                    .chain(std::iter::once(msg_cell("selection_total", "nil", 78)))
                    .collect(),
            ),
            // 1 + 2 + … + 9 = 45: rendered 33 + trimmed 45 = 78.
            trimmed: vec![("selection", "statusMessage", 45)],
        },
        // ---- B4: the FOUR-argument form the Comparison tab generates. --
        // topN and the window compose: the window decides the side, then
        // each side is ranked. Nine baseline values is under topN=10, so
        // nothing is trimmed — and 54 + 24 = 78, the window REPARTITIONED
        // the population rather than filtering it.
        Row {
            label: "B4 compare(f, 10, start, end) — topN and the window compose",
            query: format!(
                r#"{{resource.service.name="{svc_e}"}} | compare({{status=error}}, 10, {e_start}, {e_end})"#
            ),
            keys: MSG,
            want: to_counts(
                ladder
                    .iter()
                    .filter(|(_, n)| *n <= 6 || *n >= 10)
                    .map(|(v, n)| msg_cell("baseline", v, *n))
                    .chain(
                        ladder
                            .iter()
                            .filter(|(_, n)| (7..=9).contains(n))
                            .map(|(v, n)| msg_cell("selection", v, *n)),
                    )
                    .chain([
                        msg_cell("baseline_total", "nil", 54),
                        msg_cell("selection_total", "nil", 24),
                    ])
                    .collect(),
            ),
            trimmed: vec![
                ("baseline", "statusMessage", 0),
                ("selection", "statusMessage", 0),
            ],
        },
        // ---- B5: the `(start, end]` boundary, both ends. ----------------
        // `start` is i4's EXACT start time and `end` is i7's. `>` is
        // strict, so i4 is baseline; `<=` is inclusive, so i7 is
        // selection. i8/i9 sit AFTER `end` and are baseline, not absent —
        // the window repartitions, it does not filter. A coder who writes
        // `>=` or `<` fails exactly this row.
        Row {
            label: "B5 the (start, end] boundary is exclusive-then-inclusive",
            query: format!(
                r#"{{resource.service.name="{svc_b}"}} | compare({{status=error}}, 10, {b_start}, {b_end})"#
            ),
            keys: IDX,
            want: counts_from(&[
                ("baseline", "span.idx", "i0", 1),
                ("baseline", "span.idx", "i1", 1),
                ("baseline", "span.idx", "i2", 1),
                ("baseline", "span.idx", "i3", 1),
                ("baseline", "span.idx", "i4", 1),
                ("baseline", "span.idx", "i8", 1),
                ("baseline", "span.idx", "i9", 1),
                ("baseline_total", "span.idx", "nil", 7),
                ("selection", "span.idx", "i5", 1),
                ("selection", "span.idx", "i6", 1),
                ("selection", "span.idx", "i7", 1),
                ("selection_total", "span.idx", "nil", 3),
            ]),
            trimmed: vec![("baseline", "span.idx", 0), ("selection", "span.idx", 0)],
        },
    ];

    // ---- READINESS, before any row is evaluated. ----------------------
    // Not "the answer stopped changing" — "the answer is the one we know
    // this corpus has". See `await_complete_corpus`: a partial reference
    // view settles just as readily as a complete one, and did so twice in
    // fifteen runs before this gate existed. Both corpora are gated,
    // because B5 reads the second one.
    await_complete_corpus(&api_base, &svc_e, "statusMessage", window, 78, 12);
    await_complete_corpus(&api_base, &svc_b, "span.idx", window, 10, 10);

    let mut faults: Vec<String> = Vec::new();
    for row in &rows {
        let pulsus = pulsus_counts(&engine, &row.query, window, row.keys).await;
        let tempo = tempo_counts(&api_base, &row.query, window, row.keys);
        eprintln!(
            "[{}]\n  query: {}\n-- pulsus --\n{}\n-- reference --\n{}",
            row.label,
            row.query,
            render(&pulsus),
            render(&tempo)
        );

        // (1) The captured oracle. These numbers were captured from the
        // reference, not derived from our output — if our answer disagrees
        // with one, our answer is wrong until proven otherwise.
        if pulsus != row.want {
            faults.push(format!(
                "{}: PulsusDB does not match the CAPTURED reference answer.\n  want:\n{}\n  got:\n{}",
                row.label,
                render(&row.want),
                render(&pulsus)
            ));
        }
        // (2) The live two-system differential, so a captured answer that
        // has gone stale cannot pass either.
        if tempo != row.want {
            faults.push(format!(
                "{}: the LIVE reference does not match the captured answer.\n  want:\n{}\n  got:\n{}",
                row.label,
                render(&row.want),
                render(&tempo)
            ));
        }
        // (3) AC 18 — every table balances. Each side's `*_total` equals
        // the sum of that side's RENDERED members plus whatever topN
        // trimmed. This is the check that catches a transcribed member
        // list whose own total contradicts it.
        for (meta, key, trimmed) in &row.trimmed {
            let rendered: i64 = pulsus
                .iter()
                .filter(|((m, k, _), _)| m == meta && k == key)
                .map(|(_, n)| *n)
                .sum();
            let total = pulsus
                .get(&(
                    format!("{meta}_total"),
                    (*key).to_string(),
                    "nil".to_string(),
                ))
                .copied()
                .unwrap_or(0);
            if rendered + trimmed != total {
                faults.push(format!(
                    "{}: {meta} {key} does not balance — rendered {rendered} + trimmed \
                     {trimmed} = {} != total {total}",
                    row.label,
                    rendered + trimmed
                ));
            }
        }
    }

    // ---- B6: `__meta_error` is unreachable, on BOTH systems. -----------
    //
    // **The scope of the source-level half, stated where the literal
    // lives.** No PRODUCTION source in this workspace may carry the label
    // name. That is:
    //
    //     git grep -c '__meta_error' -- crates/ ':(exclude,glob)crates/*/tests/**'
    //
    // which exits 1 (verified against this tree, and verified to MATCH
    // after planting the literal in `crates/pulsus-read/src/traces/
    // metrics_plan.rs` and in `crates/pulsus-server/build.rs` — a pathspec
    // that matches nothing proves nothing, so it was checked against a
    // violation before being written down). Its domain is every tracked
    // file of every crate under `crates/`, minus that crate's `tests/`
    // tree — the single carve-out, needed because THIS file must name the
    // label to assert its absence. Outside the domain, by name: `xtask/`,
    // `e2e/`, `vendor/` — a dev tool, a CI harness and a vendored PromQL
    // parser, none of which links into `pulsus-server` -> `pulsus-read`.
    // The opt-OUT form is deliberate: an opt-in list of production
    // locations fails open the day a crate lays sources somewhere new.
    // The reference's raw job emits a `__meta_error="__too_many_values__"`
    // series when a key exceeds topN, with `Values: nil` — and
    // `SeriesSet.ToProto` drops zero-sample series, so it never reaches
    // the wire. Source and container agree; the container is what a user
    // sees, so we must not emit one either. Checked on the two queries
    // that DO exceed topN (`, 3` over twelve values, and `, 1`).
    for arg in [", 3", ", 1"] {
        let q = format!(r#"{{resource.service.name="{svc_e}"}} | compare({{status=error}}{arg})"#);
        let reference = tempo_body(&api_base, &q, window);
        assert!(
            !reference.contains("__meta_error"),
            "the reference emitted __meta_error for {q:?} — the premise of this check moved:\n\
             {reference}"
        );
        let ours = pulsus_counts(&engine, &q, window, &["statusMessage", "__meta_error"]).await;
        let leaked: Vec<&(String, String, String)> = ours
            .keys()
            .filter(|(m, k, _)| m.contains("__meta_error") || k == "__meta_error")
            .collect();
        assert!(
            leaked.is_empty(),
            "PulsusDB emitted __meta_error for {q:?}: {leaked:?}"
        );
    }

    exec(&bootstrap, &format!("DROP DATABASE IF EXISTS {db}")).await;

    assert!(
        faults.is_empty(),
        "compare() ARITY divergence(s) — {} fault(s), REAL PulsusDB and reference output:\n\n{}",
        faults.len(),
        faults.join("\n\n")
    );
}
