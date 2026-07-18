//! The M6-09 LogQL-pipeline differential (`logs_pipeline_differential`):
//! a deterministic OTLP-logs corpus (`logs_corpus.rs`) pushed **once**
//! through the real collector, which fans it out — `otlphttp` to
//! PulsusDB and `otlphttp/loki` to the pinned reference log store
//! (`grafana/loki:3.4.2`, tag+digest in
//! `deploy/e2e/compose.single.yaml`) — as **identical typed wire data**;
//! then, per committed case in `test/fixtures/logs/differential.json`,
//! both stores' `query_range` answers for the identical pipeline query
//! and window are compared **set-equal**: `{stream-label-set →
//! {(timestamp, line)}}`.
//!
//! **Gate discipline (plan v3 delta 5, the traces precedent):**
//! - validity gates run BEFORE any set comparison: a bounded
//!   completeness poll (absorbs export/visibility lag), raw result
//!   counts strictly below the requested limit on both stores (a
//!   truncated top-K is never compared as a set), and no duplicate
//!   entries;
//! - PulsusDB is ALWAYS hard-gated against the corpus's by-construction
//!   expected set — `mode: "informational"` only downgrades the oracle
//!   comparison, and only with a precisely classified ledger entry
//!   (docs/benchmarks/logs-differential-ledger.md);
//! - any gating mismatch dumps a minimal repro under
//!   `target/e2e-artifacts/logs-diff/<variant>/` and fails the scenario.
//!
//! **Tier placement (plan v2 delta A):** nightly/dispatch `e2e-single`
//! only — the scenario self-gates on `PULSUS_E2E_LOGS_DIFFERENTIAL=1`
//! (set by ci.yml's existing nightly full-tier job; no per-PR gate, no
//! new job).

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::corpus::Scale;
use crate::harness::{completeness_poll_timeout, poll_until, query_request_timeout};
use crate::logs_corpus::{
    self, ExpectedResult, LogCorpus, LogCorpusSpec, MetricMatrix, MetricVector, OrderedEntries,
};
use crate::logs_sm_corpus;
use crate::metrics::write_artifact;
use crate::scenarios::Ctx;

const FIXTURE_PATH: &str = "logs/differential.json";
const ARTIFACT_AREA: &str = "logs-diff";

const COLLECTOR_READY_POLL_TIMEOUT: Duration = Duration::from_secs(90);
const COLLECTOR_READY_POLL_INTERVAL: Duration = Duration::from_millis(500);
// The completeness-poll deadline is tier-aware (issue #106,
// `harness::completeness_poll_timeout`): 600s full / 180s ci.
const COMPLETENESS_POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Progress-log rate limit (issue #106): between unchanged
/// `pulsusdb=X oracle=Y` counts, emit at most one completeness line per
/// this interval so a long full-tier poll stays diagnosable without
/// flooding CI logs.
const COMPLETENESS_PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(3);

/// Margin between the corpus's last record and "now" at generation time,
/// and the query-window slack on each side (both stores get identical
/// nanosecond bounds).
const CORPUS_NOW_MARGIN_NS: i64 = 5_000_000_000;
const WINDOW_SLACK_NS: i64 = 3_600_000_000_000;

/// The `reader.logql_pipeline_scan_factor` the deployed e2e server runs
/// with (issue #100): the config default 10 (pinned by the
/// `pulsus-config` golden tests), which `deploy/e2e/compose.single.yaml`
/// overrides with neither the config key nor its
/// `PULSUS_LOGQL_PIPELINE_SCAN_FACTOR` env var (asserted hermetically).
/// The fetch-until-limit page size is `result_limit × this factor`, so
/// the `streams_limited` case's page-1 arithmetic (survivors on the
/// first `limit × factor` rows < `limit`) holds against the live server.
const E2E_DEPLOYED_SCAN_FACTOR: u32 = 10;

// ---------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct TierCounts {
    record_count: usize,
}

#[derive(Debug, Deserialize)]
struct CaseRaw {
    case_id: String,
    /// Which committed pipeline stage this case covers — documentation,
    /// validated non-empty by a unit test.
    construct: String,
    /// `"gated"` or `"informational"` — informational requires a
    /// `ledger` entry id (unit-tested against the committed ledger).
    mode: String,
    #[serde(default)]
    ledger: Option<String>,
    /// Case shape (issue M6-10): absent/`"streams"` = the M6-09 streams
    /// comparison; `"metric_instant"` = `/query` vector comparison
    /// (instant windows are semantically identical on both stores);
    /// `"metric_range"` = `/query_range` matrix comparison (the tumbling
    /// divergence surface — see the ledger).
    #[serde(default)]
    kind: Option<String>,
    /// `metric_range` only: the request step in seconds.
    #[serde(default)]
    step_s: Option<u64>,
    /// `metric_match_error` only (issue #91): the shared error-body
    /// substring both stores must carry. Oracle-pinned against
    /// `grafana/loki:3.4.2`; status codes are NOT gated (Loki returns 500
    /// for these runtime matching errors, PulsusDB 400 — see the ledger).
    #[serde(default)]
    expect_error_substr: Option<String>,
    /// `streams_limited` only (issue #100): the per-case request limit,
    /// overriding the global fixture `limit`. The fetch-until-limit
    /// ordered case requires exactly this many entries on both stores.
    #[serde(default)]
    limit: Option<u32>,
    query: String,
}

impl CaseRaw {
    fn kind(&self) -> &str {
        self.kind.as_deref().unwrap_or("streams")
    }
}

#[derive(Debug, Deserialize)]
struct LogsFixture {
    #[expect(
        dead_code,
        reason = "shape parity with the traces fixture; no PRNG consumes it yet"
    )]
    seed: u64,
    step_ns: i64,
    ci: TierCounts,
    full: TierCounts,
    limit: u32,
    cases: Vec<CaseRaw>,
}

fn load_fixture(ctx: &Ctx) -> Result<LogsFixture> {
    let path = ctx.fixtures_dir.join(FIXTURE_PATH);
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read fixture {}", path.display()))?;
    let fixture: LogsFixture = serde_json::from_str(&raw)
        .with_context(|| format!("fixture {} was not valid JSON", path.display()))?;
    for case in &fixture.cases {
        if !logs_corpus::CASE_IDS.contains(&case.case_id.as_str())
            && !logs_corpus::METRIC_CASE_IDS.contains(&case.case_id.as_str())
        {
            bail!(
                "fixture {} names case {:?}, which the corpus does not project",
                path.display(),
                case.case_id
            );
        }
    }
    Ok(fixture)
}

fn parse_logs_scale(raw: Option<&str>) -> Result<Scale> {
    match raw {
        None => Ok(Scale::Ci),
        Some(v) if v.eq_ignore_ascii_case("ci") => Ok(Scale::Ci),
        Some(v) if v.eq_ignore_ascii_case("full") => Ok(Scale::Full),
        Some(other) => bail!("PULSUS_E2E_LOGS_SCALE={other:?} must be \"ci\" or \"full\""),
    }
}

fn resolve_scale() -> Result<Scale> {
    match std::env::var("PULSUS_E2E_LOGS_SCALE") {
        Ok(v) => parse_logs_scale(Some(&v)),
        Err(std::env::VarError::NotPresent) => parse_logs_scale(None),
        Err(std::env::VarError::NotUnicode(raw)) => {
            bail!("PULSUS_E2E_LOGS_SCALE was not valid UTF-8: {raw:?}")
        }
    }
}

/// The nightly-tier self-gate (plan v2 delta A: "no per-PR gate, no new
/// job") — ci.yml's nightly/dispatch full-tier job sets this.
fn differential_enabled() -> bool {
    std::env::var("PULSUS_E2E_LOGS_DIFFERENTIAL").as_deref() == Ok("1")
}

fn now_unix_nanos() -> Result<i64> {
    let dur = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?;
    i64::try_from(dur.as_nanos()).context("current time does not fit in i64 nanoseconds")
}

fn build_corpus(fixture: &LogsFixture, scale: Scale) -> Result<LogCorpus> {
    let record_count = match scale {
        Scale::Ci => fixture.ci.record_count,
        Scale::Full => fixture.full.record_count,
    };
    let run_id = format!("e2e-logs-diff-{:x}", crate::metrics::unique_id()?);
    let now_ns = now_unix_nanos()?;
    let base_ns = now_ns - fixture.step_ns * record_count as i64 - CORPUS_NOW_MARGIN_NS;
    Ok(logs_corpus::generate(&LogCorpusSpec {
        scale,
        record_count,
        step_ns: fixture.step_ns,
        base_ns,
        run_id,
    }))
}

#[derive(Debug, Clone, Copy)]
struct QueryWindow {
    start_ns: i64,
    end_ns: i64,
}

fn query_window(corpus: &LogCorpus) -> QueryWindow {
    QueryWindow {
        start_ns: corpus.first_ts_ns - WINDOW_SLACK_NS,
        end_ns: corpus.last_ts_ns + WINDOW_SLACK_NS,
    }
}

// ---------------------------------------------------------------------
// Corpus push + per-store queries
// ---------------------------------------------------------------------

async fn post_otlp_logs(
    ctx: &Ctx,
    payload: &serde_json::Value,
) -> Result<Option<reqwest::Response>> {
    let res = ctx
        .http
        .post(format!("{}/v1/logs", ctx.collector_url))
        .json(payload)
        .send()
        .await?;
    Ok(Some(res))
}

async fn push_log_corpus(ctx: &Ctx, corpus: &LogCorpus) -> Result<()> {
    let request = logs_corpus::to_otlp_export_request(corpus);
    let res = poll_until(
        COLLECTOR_READY_POLL_TIMEOUT,
        COLLECTOR_READY_POLL_INTERVAL,
        || post_otlp_logs(ctx, &request),
    )
    .await
    .context("collector otlp/v1/logs endpoint never accepted a connection")?;
    if !res.status().is_success() {
        bail!("collector otlp/v1/logs export returned {}", res.status());
    }
    Ok(())
}

async fn query_store(
    ctx: &Ctx,
    url: &str,
    query: &str,
    window: QueryWindow,
    limit: u32,
    query_timeout: Duration,
) -> Result<serde_json::Value> {
    let start = window.start_ns.to_string();
    let end = window.end_ns.to_string();
    let limit_s = limit.to_string();
    let res = ctx
        .http
        .get(url)
        .query(&[
            ("query", query),
            ("start", start.as_str()),
            ("end", end.as_str()),
            ("limit", limit_s.as_str()),
            ("direction", "forward"),
        ])
        // Issue #92 (all four GET chokepoints in this module): a
        // request-level timeout replaces the shared client's 5s
        // readiness budget for scenario queries. Tier-aware (issue #106,
        // `harness::query_request_timeout`): 120s full / 60s ci.
        .timeout(query_timeout)
        .send()
        .await
        .with_context(|| format!("GET {url} failed"))?;
    if !res.status().is_success() {
        let status = res.status();
        let body = res.text().await.unwrap_or_default();
        bail!("{url} for {query:?} returned {status}: {body}");
    }
    res.json()
        .await
        .with_context(|| format!("{url} body was not JSON"))
}

async fn query_pulsus(
    ctx: &Ctx,
    query: &str,
    window: QueryWindow,
    limit: u32,
    query_timeout: Duration,
) -> Result<serde_json::Value> {
    query_store(
        ctx,
        &ctx.url("/api/logs/v1/query_range"),
        query,
        window,
        limit,
        query_timeout,
    )
    .await
}

async fn query_loki(
    ctx: &Ctx,
    query: &str,
    window: QueryWindow,
    limit: u32,
    query_timeout: Duration,
) -> Result<serde_json::Value> {
    query_store(
        ctx,
        &format!("{}/loki/api/v1/query_range", ctx.loki_url),
        query,
        window,
        limit,
        query_timeout,
    )
    .await
}

// ---------------------------------------------------------------------
// Response normalization + validity gates
// ---------------------------------------------------------------------

/// Normalizes either store's `query_range` streams response (both emit
/// `data.result[] = {"stream": {labels}, "values": [[ts,line],…]}`) to
/// the comparable set shape.
fn result_set(body: &serde_json::Value) -> Result<ExpectedResult> {
    let mut out = ExpectedResult::new();
    let result_type = body["data"]["resultType"].as_str().unwrap_or_default();
    if result_type != "streams" {
        bail!("expected a streams result, got {result_type:?}: {body}");
    }
    for stream in body["data"]["result"].as_array().into_iter().flatten() {
        let labels: std::collections::BTreeMap<String, String> = stream["stream"]
            .as_object()
            .with_context(|| format!("stream missing a labels object: {stream}"))?
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
            .collect();
        let entry_set = out.entry(labels).or_default();
        for value in stream["values"].as_array().into_iter().flatten() {
            let ts: i64 = value[0]
                .as_str()
                .and_then(|s| s.parse().ok())
                .with_context(|| format!("entry timestamp was not a ns string: {value}"))?;
            let line = value[1]
                .as_str()
                .with_context(|| format!("entry line was not a string: {value}"))?
                .to_string();
            entry_set.insert((ts, line));
        }
    }
    Ok(out)
}

/// RAW entry count, pre-set-collapse — the truncation/duplication gates
/// are judged on this (a duplicate-carrying response must not slip under
/// the limit after set-collapse; traces precedent).
fn raw_entry_count(body: &serde_json::Value) -> usize {
    body["data"]["result"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|s| s["values"].as_array())
        .map(Vec::len)
        .sum()
}

fn set_entry_count(set: &ExpectedResult) -> usize {
    set.values().map(BTreeSet::len).sum()
}

/// Flattens a streams response to `(labels, ts_ns, line)` in global
/// ascending-ts order (issue #100), preserving ORDER (and duplicates) so
/// the fetch-until-limit case can compare an ordered earliest-`limit`
/// prefix. Unlike [`result_set`]'s set-collapse this VERIFIES response
/// order rather than assuming it:
///
///  1. Each stream's `values` are parsed in RECEIVED order and asserted
///     ascending by timestamp — the forward-direction contract
///     (`docs/api.md` §2.1). A within-stream descending pair is a
///     response-order regression and fails HARD (a blind global sort
///     would silently launder it, plan v2 item 5).
///  2. The verified-ascending per-stream sequences are k-way MERGED into
///     the global order. This RELIES on the per-stream ordering just
///     verified — it does not re-sort the flattened list.
///
/// The corpus assigns globally-distinct timestamps, so the merge is a
/// total order with no tie (`run_streams_limited_case` additionally gates
/// distinct timestamps across the merged result).
fn ordered_entries(body: &serde_json::Value) -> Result<OrderedEntries> {
    let result_type = body["data"]["resultType"].as_str().unwrap_or_default();
    if result_type != "streams" {
        bail!("expected a streams result, got {result_type:?}: {body}");
    }
    let mut streams: Vec<OrderedEntries> = Vec::new();
    for stream in body["data"]["result"].as_array().into_iter().flatten() {
        let labels: std::collections::BTreeMap<String, String> = stream["stream"]
            .as_object()
            .with_context(|| format!("stream missing a labels object: {stream}"))?
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
            .collect();
        let mut entries: OrderedEntries = Vec::new();
        for value in stream["values"].as_array().into_iter().flatten() {
            let ts: i64 = value[0]
                .as_str()
                .and_then(|s| s.parse().ok())
                .with_context(|| format!("entry timestamp was not a ns string: {value}"))?;
            let line = value[1]
                .as_str()
                .with_context(|| format!("entry line was not a string: {value}"))?
                .to_string();
            if let Some((_, prev_ts, _)) = entries.last()
                && ts < *prev_ts
            {
                bail!(
                    "stream {labels:?} returned entries out of forward order: ts {ts} follows \
                     {prev_ts} — a within-stream descending pair violates the ascending \
                     forward-direction contract"
                );
            }
            entries.push((labels.clone(), ts, line));
        }
        streams.push(entries);
    }
    // k-way merge the verified-ascending per-stream sequences.
    let total: usize = streams.iter().map(Vec::len).sum();
    let mut heads: Vec<usize> = vec![0; streams.len()];
    let mut out: OrderedEntries = Vec::with_capacity(total);
    for _ in 0..total {
        let mut pick: Option<(usize, i64)> = None;
        for (si, s) in streams.iter().enumerate() {
            if let Some(entry) = s.get(heads[si])
                && pick.is_none_or(|(_, best)| entry.1 < best)
            {
                pick = Some((si, entry.1));
            }
        }
        let (si, _) = pick.expect("every remaining entry is counted by `total`");
        out.push(streams[si][heads[si]].clone());
        heads[si] += 1;
    }
    Ok(out)
}

// ---------------------------------------------------------------------
// Metric-case normalization + comparison (issue M6-10)
// ---------------------------------------------------------------------

fn labels_of(sample: &serde_json::Value) -> Result<std::collections::BTreeMap<String, String>> {
    Ok(sample["metric"]
        .as_object()
        .with_context(|| format!("sample missing a metric labels object: {sample}"))?
        .iter()
        .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
        .collect())
}

fn parse_value_str(v: &serde_json::Value) -> Result<f64> {
    v.as_str()
        .with_context(|| format!("metric value was not a string: {v}"))?
        .parse::<f64>()
        .with_context(|| format!("metric value was not a float: {v}"))
}

/// Normalizes either store's INSTANT metric response (`resultType:
/// "vector"`, `value: [<unix seconds>, "<float>"]`). Duplicate label
/// sets are a hard comparison-validity failure (they would collapse in
/// the map).
fn vector_result_set(body: &serde_json::Value) -> Result<MetricVector> {
    let result_type = body["data"]["resultType"].as_str().unwrap_or_default();
    if result_type != "vector" {
        bail!("expected a vector result, got {result_type:?}: {body}");
    }
    let mut out = MetricVector::new();
    for sample in body["data"]["result"].as_array().into_iter().flatten() {
        let labels = labels_of(sample)?;
        let value = parse_value_str(&sample["value"][1])?;
        if out.insert(labels.clone(), value).is_some() {
            bail!("duplicate label set in a vector result: {labels:?}");
        }
    }
    Ok(out)
}

/// Normalizes either store's RANGE metric response (`resultType:
/// "matrix"`, `values: [[<unix seconds>, "<float>"], ...]`), timestamps
/// converted to nanoseconds.
fn matrix_result_set(body: &serde_json::Value) -> Result<MetricMatrix> {
    let result_type = body["data"]["resultType"].as_str().unwrap_or_default();
    if result_type != "matrix" {
        bail!("expected a matrix result, got {result_type:?}: {body}");
    }
    let mut out = MetricMatrix::new();
    for series in body["data"]["result"].as_array().into_iter().flatten() {
        let labels = labels_of(series)?;
        let mut points = std::collections::BTreeMap::new();
        for value in series["values"].as_array().into_iter().flatten() {
            let ts_s = value[0]
                .as_f64()
                .with_context(|| format!("matrix timestamp was not a number: {value}"))?;
            let ts_ns = (ts_s * 1e9).round() as i64;
            points.insert(ts_ns, parse_value_str(&value[1])?);
        }
        if out.insert(labels.clone(), points).is_some() {
            bail!("duplicate label set in a matrix result: {labels:?}");
        }
    }
    Ok(out)
}

/// Tight relative tolerance: both stores execute the same f64
/// operations over identical inputs; this only absorbs
/// summation-order/last-ulp noise, never a semantic delta.
fn approx_eq(a: f64, b: f64) -> bool {
    if a == b {
        return true;
    }
    (a - b).abs() <= 1e-9 * a.abs().max(b.abs()).max(1e-300)
}

fn vectors_match(got: &MetricVector, expected: &MetricVector) -> bool {
    got.len() == expected.len()
        && expected
            .iter()
            .all(|(labels, v)| got.get(labels).is_some_and(|g| approx_eq(*g, *v)))
}

fn matrices_match(got: &MetricMatrix, expected: &MetricMatrix) -> bool {
    got.len() == expected.len()
        && expected.iter().all(|(labels, points)| {
            got.get(labels).is_some_and(|g| {
                g.len() == points.len()
                    && points
                        .iter()
                        .all(|(ts, v)| g.get(ts).is_some_and(|gv| approx_eq(*gv, *v)))
            })
        })
}

// ---------------------------------------------------------------------
// The scenario
// ---------------------------------------------------------------------

fn run_scope_query(run_id: &str) -> String {
    format!(r#"{{{}="{run_id}"}}"#, logs_corpus::RUN_ATTR)
}

pub async fn logs_pipeline_differential(ctx: &Ctx) -> Result<()> {
    if !differential_enabled() {
        println!(
            "pulsus-e2e:   logs_pipeline_differential: skipped (set \
             PULSUS_E2E_LOGS_DIFFERENTIAL=1 — nightly/dispatch tier only, plan v2 delta A)"
        );
        return Ok(());
    }
    let fixture = load_fixture(ctx)?;
    let scale = resolve_scale()?;
    let corpus = build_corpus(&fixture, scale)?;
    let window = query_window(&corpus);
    println!(
        "pulsus-e2e:   logs_pipeline_differential [{:?}]: pushing {} records ({:?} tier, run_id={:?})",
        ctx.variant,
        corpus.total_records(),
        corpus.scale,
        corpus.run_id
    );

    push_log_corpus(ctx, &corpus)
        .await
        .context("pushing the logs corpus through the collector failed")?;

    wait_for_completeness(ctx, &corpus, window, fixture.limit).await?;

    for case in &fixture.cases {
        run_case(ctx, &corpus, &fixture, case, window)
            .await
            .with_context(|| format!("logs differential case {:?}", case.case_id))?;
    }
    Ok(())
}

/// A single `(labels, ts_ns, line)` record — the granularity the
/// completeness diagnostic reports missing/extra shortfalls at (issue
/// #106).
type LabeledEntry = (BTreeMap<String, String>, i64, String);

/// One store's completeness shortfall against the corpus expectation
/// (issue #106): how many expected entries it currently carries, and the
/// symmetric difference vs `expected` at `(labels, ts, line)` granularity.
struct CompletenessSetDiff {
    matched: usize,
    /// In `expected`, absent from the store — the records CI needs to see.
    missing: Vec<LabeledEntry>,
    /// In the store, absent from `expected` — an unexpected delivery.
    extra: Vec<LabeledEntry>,
}

/// The pure symmetric-difference of a store's result set against the
/// corpus expectation (issue #106 completeness diagnostic core). Unit-
/// tested, so the on-timeout artifact's missing/extra sets are known
/// correct before the nightly next fails.
fn completeness_set_diff(store: &ExpectedResult, expected: &ExpectedResult) -> CompletenessSetDiff {
    let mut matched = 0usize;
    let mut missing = Vec::new();
    for (labels, entries) in expected {
        let store_entries = store.get(labels);
        for (ts, line) in entries {
            if store_entries.is_some_and(|s| s.contains(&(*ts, line.clone()))) {
                matched += 1;
            } else {
                missing.push((labels.clone(), *ts, line.clone()));
            }
        }
    }
    let mut extra = Vec::new();
    for (labels, entries) in store {
        let exp_entries = expected.get(labels);
        for (ts, line) in entries {
            if !exp_entries.is_some_and(|e| e.contains(&(*ts, line.clone()))) {
                extra.push((labels.clone(), *ts, line.clone()));
            }
        }
    }
    CompletenessSetDiff {
        matched,
        missing,
        extra,
    }
}

fn labeled_entries_json(entries: &[LabeledEntry]) -> Vec<serde_json::Value> {
    entries
        .iter()
        .map(|(labels, ts, line)| serde_json::json!({ "labels": labels, "ts": ts, "line": line }))
        .collect()
}

/// Per-attempt progress line (issue #106), rate-limited to at most one
/// unchanged line per [`COMPLETENESS_PROGRESS_LOG_INTERVAL`]: without it
/// the "still filling / set mismatch" path was silent every poll, so CI
/// could not tell a real convergence bug (plateaued low) from budget
/// (climbing steadily toward the total).
fn log_completeness_progress(
    last: &Cell<(usize, usize)>,
    last_log_at: &Cell<Instant>,
    label: &str,
    total: usize,
    pulsus: usize,
    oracle: usize,
) {
    let now = Instant::now();
    let changed = last.get() != (pulsus, oracle);
    if changed || now.duration_since(last_log_at.get()) >= COMPLETENESS_PROGRESS_LOG_INTERVAL {
        let reached = pulsus.min(oracle);
        println!(
            "pulsus-e2e:   {label} completeness: reached {reached}/{total}: pulsusdb={pulsus} \
             oracle={oracle}"
        );
        last.set((pulsus, oracle));
        last_log_at.set(now);
    }
}

/// The run-scoped completeness probe both logs gates re-run on timeout —
/// bundled so the diagnostic fn stays within clippy's argument threshold.
struct CompletenessProbe<'a> {
    q: &'a str,
    window: QueryWindow,
    limit: u32,
    query_timeout: Duration,
}

/// On the FINAL completeness timeout (issue #106): re-query both stores
/// once, compute each store's raw/distinct counts and the missing/extra
/// symmetric difference vs `expected`, and write the artifact CI needs to
/// diagnose the next nightly. Best-effort — a failed final query is
/// recorded rather than swallowing the diagnostic. Returns the timeout
/// error enriched with the artifact path so a wider full-tier budget can
/// never mask a real convergence bug.
async fn completeness_timeout_diagnostic(
    ctx: &Ctx,
    surface: &str,
    prefix: &str,
    probe: &CompletenessProbe<'_>,
    expected: &ExpectedResult,
    timeout_err: anyhow::Error,
) -> anyhow::Error {
    let CompletenessProbe {
        q,
        window,
        limit,
        query_timeout,
    } = *probe;
    let mut stores = serde_json::Map::new();
    for (store, body) in [
        (
            "pulsusdb",
            query_pulsus(ctx, q, window, limit, query_timeout).await,
        ),
        (
            "oracle",
            query_loki(ctx, q, window, limit, query_timeout).await,
        ),
    ] {
        let entry = match body {
            Ok(body) => {
                let raw = raw_entry_count(&body);
                match result_set(&body) {
                    Ok(set) => {
                        let distinct = set_entry_count(&set);
                        let diff = completeness_set_diff(&set, expected);
                        serde_json::json!({
                            "raw_entries": raw,
                            "distinct_entries": distinct,
                            "matched": diff.matched,
                            "missing_count": diff.missing.len(),
                            "extra_count": diff.extra.len(),
                            "missing": labeled_entries_json(&diff.missing),
                            "extra": labeled_entries_json(&diff.extra),
                        })
                    }
                    Err(err) => serde_json::json!({
                        "raw_entries": raw,
                        "error": format!("could not normalize result set: {err:#}"),
                    }),
                }
            }
            Err(err) => serde_json::json!({ "error": format!("final query failed: {err:#}") }),
        };
        stores.insert(store.to_string(), entry);
    }
    let artifact = serde_json::json!({
        "surface": surface,
        "kind": "completeness_timeout",
        "query": q,
        "limit": limit,
        "expected_total": set_entry_count(expected),
        "stores": stores,
    });
    match write_artifact(ctx, ARTIFACT_AREA, prefix, &artifact) {
        Ok(path) => timeout_err.context(format!(
            "completeness timed out; per-store counts + missing/extra records written to {}",
            path.display()
        )),
        Err(werr) => timeout_err.context(format!(
            "completeness timed out; ALSO failed to write the missing-record diagnostic: {werr:#}"
        )),
    }
}

/// Bounded completeness poll (validity gate (a)): the run-scoped bare
/// query returns exactly the corpus's full record set on BOTH stores —
/// absorbs collector-export and store-visibility lag without fixed
/// sleeps, and proves the fan-out delivered identical data before any
/// pipeline comparison runs.
///
/// **Raw-count gates run BEFORE the set comparison** (issue #72 review
/// round 1, finding 4): set equality would collapse duplicate delivery
/// — and a duplicated record matched by no case would then evade every
/// later per-case duplicate check. On each attempt the RAW entry count
/// is validated first: at/over the limit → hard truncation failure;
/// raw > distinct → hard duplicate-delivery failure (duplicates never
/// self-heal — collector retries / MergeTree rows persist); raw below
/// the corpus size → still filling, keep polling.
async fn wait_for_completeness(
    ctx: &Ctx,
    corpus: &LogCorpus,
    window: QueryWindow,
    limit: u32,
) -> Result<()> {
    let q = run_scope_query(&corpus.run_id);
    let expected = corpus.expected_all_records();
    let expected_total = set_entry_count(&expected);
    let query_timeout = query_request_timeout(corpus.scale);
    // Rate-limit state for the per-attempt progress line (issue #106):
    // interior-mutability so the poll closure stays `Fn` (no `&mut`
    // capture across the awaited future).
    let progress = Cell::new((usize::MAX, usize::MAX));
    let last_log_at = Cell::new(Instant::now());
    // `poll_until` retries a closure `Err` — so permanent invalidity
    // (truncation / duplicate delivery, which never self-heal) is
    // yielded as `Ok(Some(Err(...)))` to stop polling immediately, and
    // propagated after the poll.
    let poll_result: Result<Result<()>> = poll_until(
        completeness_poll_timeout(corpus.scale),
        COMPLETENESS_POLL_INTERVAL,
        || async {
            // Pass 1 — validity gates on BOTH stores' responses, before
            // ANY set comparison (round-2 finding 2: comparing one store
            // first would keep retrying while the OTHER store's response
            // is already permanently invalid).
            let bodies = [
                (
                    "pulsusdb",
                    query_pulsus(ctx, &q, window, limit, query_timeout).await?,
                ),
                (
                    "oracle",
                    query_loki(ctx, &q, window, limit, query_timeout).await?,
                ),
            ];
            let mut sets = Vec::with_capacity(bodies.len());
            for (store, body) in &bodies {
                let raw = raw_entry_count(body);
                if raw as u32 >= limit {
                    let artifact = serde_json::json!({
                        "surface": "logs_pipeline_completeness",
                        "kind": "truncation",
                        "store": store,
                        "query": q,
                        "raw_entries": raw,
                        "limit": limit,
                        "result": body,
                    });
                    let path =
                        write_artifact(ctx, ARTIFACT_AREA, "completeness-truncation", &artifact)?;
                    return Ok(Some(Err(anyhow::anyhow!(
                        "completeness: {store} returned {raw} raw entries at limit {limit} — \
                         corpus/limit sizing invalid (repro {})",
                        path.display()
                    ))));
                }
                let set = result_set(body)?;
                let distinct = set_entry_count(&set);
                if raw > distinct {
                    let artifact = serde_json::json!({
                        "surface": "logs_pipeline_completeness",
                        "kind": "duplicate_delivery",
                        "store": store,
                        "query": q,
                        "raw_entries": raw,
                        "distinct_entries": distinct,
                        "result": body,
                    });
                    let path =
                        write_artifact(ctx, ARTIFACT_AREA, "completeness-duplicates", &artifact)?;
                    return Ok(Some(Err(anyhow::anyhow!(
                        "completeness: {store} returned {raw} raw entries but only {distinct} \
                         distinct — duplicate delivery, comparison invalid (repro {})",
                        path.display()
                    ))));
                }
                sets.push(set);
            }
            // Pass 2 — set comparisons, only once both stores passed
            // every gate. On the still-filling path emit a rate-limited
            // progress line so the "set mismatch" case is no longer silent
            // (issue #106).
            let pulsus_matched = completeness_set_diff(&sets[0], &expected).matched;
            let oracle_matched = completeness_set_diff(&sets[1], &expected).matched;
            log_completeness_progress(
                &progress,
                &last_log_at,
                "logs",
                expected_total,
                pulsus_matched,
                oracle_matched,
            );
            if sets.iter().any(|set| *set != expected) {
                return Ok(None); // still filling — keep polling
            }
            Ok(Some(Ok(())))
        },
    )
    .await;
    match poll_result {
        Ok(verdict) => verdict,
        // The `Ok(None)` deadline branch (issue #106): compute + write the
        // missing-record diagnostic right before surfacing the timeout.
        Err(timeout_err) => Err(completeness_timeout_diagnostic(
            ctx,
            "logs_pipeline_completeness",
            "completeness-timeout",
            &CompletenessProbe {
                q: &q,
                window,
                limit,
                query_timeout,
            },
            &expected,
            timeout_err.context(format!(
                "run {:?} never reached completeness ({} records) on both stores",
                corpus.run_id,
                corpus.total_records()
            )),
        )
        .await),
    }
}

/// One committed case, dispatched by shape (issue M6-10): the M6-09
/// streams comparison, or a metric vector/matrix comparison.
async fn run_case(
    ctx: &Ctx,
    corpus: &LogCorpus,
    fixture: &LogsFixture,
    case: &CaseRaw,
    window: QueryWindow,
) -> Result<()> {
    match case.kind() {
        "streams" => run_streams_case(ctx, corpus, fixture, case, window).await,
        "streams_limited" => run_streams_limited_case(ctx, corpus, case, window).await,
        "metric_instant" => run_metric_instant_case(ctx, corpus, case).await,
        "metric_range" => run_metric_range_case(ctx, corpus, case, window).await,
        "metric_error" => run_metric_error_case(ctx, corpus, case).await,
        "metric_match_error" => run_metric_match_error_case(ctx, corpus, case).await,
        other => bail!("case {:?} has unknown kind {other:?}", case.case_id),
    }
}

/// The M6-10 D1 witness (adjudication #1): a GENUINE unwrap conversion
/// failure surviving the pipeline must FAIL the metric query on BOTH
/// stores — HTTP 400 carrying the `SampleExtractionErr` class — never a
/// silently reduced/empty success. Oracle-verified live during plan D1's
/// mandated probe; this pins it in the nightly differential.
async fn run_metric_error_case(ctx: &Ctx, corpus: &LogCorpus, case: &CaseRaw) -> Result<()> {
    let q = case.query.replace("{R}", &corpus.run_id);
    let eval_ns = metric_eval_ns(corpus);
    let query_timeout = query_request_timeout(corpus.scale);
    println!(
        "pulsus-e2e:     case {:?} [{}] — {}: expecting HTTP 400 + SampleExtractionErr on both \
         stores",
        case.case_id, case.mode, case.construct,
    );

    let fetch = |url: String| {
        let q = q.clone();
        async move {
            let time = eval_ns.to_string();
            let res = ctx
                .http
                .get(&url)
                .query(&[("query", q.as_str()), ("time", time.as_str())])
                .timeout(query_timeout) // issue #92/#106, see query_store
                .send()
                .await
                .with_context(|| format!("GET {url} failed"))?;
            let status = res.status().as_u16();
            let body = res.text().await.unwrap_or_default();
            Ok::<(u16, String), anyhow::Error>((status, body))
        }
    };
    let pulsus_started = std::time::Instant::now();
    let (pulsus_status, pulsus_body) = fetch(ctx.url("/api/logs/v1/query")).await?;
    let pulsus_elapsed = pulsus_started.elapsed();
    let oracle_started = std::time::Instant::now();
    let (oracle_status, oracle_body) = fetch(format!("{}/loki/api/v1/query", ctx.loki_url)).await?;
    let oracle_elapsed = oracle_started.elapsed();
    // Per-case elapsed line (issue #92, see `run_streams_case`).
    println!(
        "pulsus-e2e: query {q:?} (case {:?}) pulsusdb {}ms oracle {}ms",
        case.case_id,
        pulsus_elapsed.as_millis(),
        oracle_elapsed.as_millis(),
    );

    let dump = |detail: &str| -> Result<std::path::PathBuf> {
        let artifact = serde_json::json!({
            "surface": "logs_metric_pipeline",
            "case_id": case.case_id,
            "mode": case.mode,
            "kind": "unwrap_error_witness",
            "query": q,
            "eval_ns": eval_ns,
            "pulsusdb_status": pulsus_status,
            "pulsusdb_body": pulsus_body,
            "oracle_status": oracle_status,
            "oracle_body": oracle_body,
            "detail": detail,
        });
        write_artifact(ctx, ARTIFACT_AREA, "metric-error-witness", &artifact)
    };

    for (store, status, body) in [
        ("pulsusdb", pulsus_status, &pulsus_body),
        ("oracle", oracle_status, &oracle_body),
    ] {
        if status != 400 {
            let path = dump(&format!("{store} returned {status}, expected 400"))?;
            bail!(
                "case {:?}: {store} returned {status} instead of 400 for a surviving unwrap \
                 conversion error (repro {})",
                case.case_id,
                path.display()
            );
        }
        if !body.contains("SampleExtractionErr") {
            let path = dump(&format!("{store} 400 body lacks SampleExtractionErr"))?;
            bail!(
                "case {:?}: {store} error does not carry the SampleExtractionErr class (repro {})",
                case.case_id,
                path.display()
            );
        }
    }
    Ok(())
}

/// Issue #91 matching-error witness: a vector-matching query that is a
/// runtime error on BOTH stores (`many-to-one`/`many-to-many`). Gated on
/// the shared error-body substring (oracle-pinned against
/// `grafana/loki:3.4.2`); the HTTP status is deliberately NOT gated —
/// Loki returns 500, PulsusDB 400 for these, an informational divergence
/// recorded in docs/benchmarks/logs-differential-ledger.md. Both stores
/// must still return SOME error (>= 400).
async fn run_metric_match_error_case(ctx: &Ctx, corpus: &LogCorpus, case: &CaseRaw) -> Result<()> {
    let q = case.query.replace("{R}", &corpus.run_id);
    let eval_ns = metric_eval_ns(corpus);
    let query_timeout = query_request_timeout(corpus.scale);
    let substr = case.expect_error_substr.as_deref().with_context(|| {
        format!(
            "case {:?} is metric_match_error but carries no expect_error_substr",
            case.case_id
        )
    })?;
    println!(
        "pulsus-e2e:     case {:?} [{}] — {}: expecting an error carrying {:?} on both stores \
         (status not gated — Loki 500 vs PulsusDB 400)",
        case.case_id, case.mode, case.construct, substr,
    );

    let fetch = |url: String| {
        let q = q.clone();
        async move {
            let time = eval_ns.to_string();
            let res = ctx
                .http
                .get(&url)
                .query(&[("query", q.as_str()), ("time", time.as_str())])
                .timeout(query_timeout) // issue #92/#106, see query_store
                .send()
                .await
                .with_context(|| format!("GET {url} failed"))?;
            let status = res.status().as_u16();
            let body = res.text().await.unwrap_or_default();
            Ok::<(u16, String), anyhow::Error>((status, body))
        }
    };
    let (pulsus_status, pulsus_body) = fetch(ctx.url("/api/logs/v1/query")).await?;
    let (oracle_status, oracle_body) = fetch(format!("{}/loki/api/v1/query", ctx.loki_url)).await?;

    let dump = |detail: &str| -> Result<std::path::PathBuf> {
        let artifact = serde_json::json!({
            "surface": "logs_metric_pipeline",
            "case_id": case.case_id,
            "mode": case.mode,
            "kind": "matching_error_witness",
            "query": q,
            "eval_ns": eval_ns,
            "expect_error_substr": substr,
            "pulsusdb_status": pulsus_status,
            "pulsusdb_body": pulsus_body,
            "oracle_status": oracle_status,
            "oracle_body": oracle_body,
            "detail": detail,
        });
        write_artifact(
            ctx,
            ARTIFACT_AREA,
            "metric-matching-error-witness",
            &artifact,
        )
    };

    for (store, status, body) in [
        ("pulsusdb", pulsus_status, &pulsus_body),
        ("oracle", oracle_status, &oracle_body),
    ] {
        if status < 400 {
            let path = dump(&format!(
                "{store} returned {status}, expected an error (>= 400)"
            ))?;
            bail!(
                "case {:?}: {store} returned {status} instead of an error for a matching failure \
                 (repro {})",
                case.case_id,
                path.display()
            );
        }
        if !body.contains(substr) {
            let path = dump(&format!("{store} error body lacks {substr:?}"))?;
            bail!(
                "case {:?}: {store} error body does not carry {substr:?} (repro {})",
                case.case_id,
                path.display()
            );
        }
    }
    Ok(())
}

/// The eval instant for the metric-instant cases: just past the last
/// record, so every fixture query's `[30m]` window covers the whole
/// corpus on both tiers (record spans are <= ~5m + margins).
fn metric_eval_ns(corpus: &LogCorpus) -> i64 {
    corpus.last_ts_ns + CORPUS_NOW_MARGIN_NS
}

async fn query_instant(
    ctx: &Ctx,
    url: &str,
    query: &str,
    time_ns: i64,
    query_timeout: Duration,
) -> Result<serde_json::Value> {
    let time = time_ns.to_string();
    let res = ctx
        .http
        .get(url)
        .query(&[("query", query), ("time", time.as_str())])
        .timeout(query_timeout) // issue #92/#106, see query_store
        .send()
        .await
        .with_context(|| format!("GET {url} failed"))?;
    if !res.status().is_success() {
        let status = res.status();
        let body = res.text().await.unwrap_or_default();
        bail!("{url} for {query:?} returned {status}: {body}");
    }
    res.json()
        .await
        .with_context(|| format!("{url} body was not JSON"))
}

/// Instant metric case: both stores answer `/query` for the identical
/// expression and evaluation instant — instant windows `(t - range, t]`
/// are semantically identical on both stores, so every instant case is
/// fully gated. Values compare with a tight relative tolerance; label
/// sets compare exactly.
async fn run_metric_instant_case(ctx: &Ctx, corpus: &LogCorpus, case: &CaseRaw) -> Result<()> {
    let q = case.query.replace("{R}", &corpus.run_id);
    let expected = corpus.expected_metric_vector(&case.case_id);
    let gated = case.mode == "gated";
    let eval_ns = metric_eval_ns(corpus);
    let query_timeout = query_request_timeout(corpus.scale);
    println!(
        "pulsus-e2e:     case {:?} [{}] — {}: {} expected series",
        case.case_id,
        case.mode,
        case.construct,
        expected.len(),
    );

    let pulsus_started = std::time::Instant::now();
    let pulsus_body = query_instant(
        ctx,
        &ctx.url("/api/logs/v1/query"),
        &q,
        eval_ns,
        query_timeout,
    )
    .await?;
    let pulsus_elapsed = pulsus_started.elapsed();
    let oracle_started = std::time::Instant::now();
    let oracle_body = query_instant(
        ctx,
        &format!("{}/loki/api/v1/query", ctx.loki_url),
        &q,
        eval_ns,
        query_timeout,
    )
    .await?;
    let oracle_elapsed = oracle_started.elapsed();
    // Per-case elapsed line (issue #92, see `run_streams_case`).
    println!(
        "pulsus-e2e: query {q:?} (case {:?}) pulsusdb {}ms oracle {}ms",
        case.case_id,
        pulsus_elapsed.as_millis(),
        oracle_elapsed.as_millis(),
    );
    // `vector_result_set` hard-fails on duplicate label sets (validity
    // gate; a truncation gate is not applicable — metric vectors carry
    // no request limit).
    let pulsus_set = vector_result_set(&pulsus_body)?;
    let oracle_set = vector_result_set(&oracle_body)?;

    let dump = |kind: &str, detail: &str| -> Result<std::path::PathBuf> {
        let artifact = serde_json::json!({
            "surface": "logs_metric_pipeline",
            "case_id": case.case_id,
            "mode": case.mode,
            "kind": kind,
            "query": q,
            "eval_ns": eval_ns,
            "expected": expected.iter().map(|(l, v)| serde_json::json!({"labels": l, "value": v})).collect::<Vec<_>>(),
            "pulsusdb_result": pulsus_body,
            "oracle_result": oracle_body,
            "detail": detail,
        });
        write_artifact(
            ctx,
            ARTIFACT_AREA,
            if gated {
                "metric-case-mismatch"
            } else {
                "informational-case"
            },
            &artifact,
        )
    };

    if !vectors_match(&pulsus_set, &expected) {
        let path = dump(
            "pulsus_vs_corpus",
            &format!("pulsusdb vector diverged: got {pulsus_set:?}, expected {expected:?}"),
        )?;
        bail!(
            "case {:?}: pulsusdb diverged from the corpus expectation (repro {})",
            case.case_id,
            path.display()
        );
    }
    if !vectors_match(&oracle_set, &expected) {
        let path = dump(
            "oracle_vs_corpus",
            &format!("oracle vector diverged: got {oracle_set:?}, expected {expected:?}"),
        )?;
        if gated {
            bail!(
                "case {:?}: oracle diverged from the corpus expectation (repro {})",
                case.case_id,
                path.display()
            );
        }
        println!(
            "pulsus-e2e:   logs informational delta (never gating): case {:?} (ledger {:?}) \
             (dumped to {})",
            case.case_id,
            case.ledger.as_deref().unwrap_or(""),
            path.display()
        );
    } else if !gated {
        let path = dump(
            "stale_exclusion",
            "informational metric case matched the oracle",
        )?;
        bail!(
            "case {:?}: ledgered divergence ({:?}) is stale — re-gate the case (repro {})",
            case.case_id,
            case.ledger.as_deref().unwrap_or(""),
            path.display()
        );
    }
    Ok(())
}

/// Range metric case: both stores answer `/query_range`. PulsusDB is
/// hard-gated against the tumbling by-construction expectation; the
/// oracle comparison is informational for the ledgered
/// tumbling-vs-sliding case, with the standard anti-rot.
async fn run_metric_range_case(
    ctx: &Ctx,
    corpus: &LogCorpus,
    case: &CaseRaw,
    window: QueryWindow,
) -> Result<()> {
    let q = case.query.replace("{R}", &corpus.run_id);
    let step_s = case
        .step_s
        .with_context(|| format!("case {:?} is metric_range but has no step_s", case.case_id))?;
    let step_ns = step_s as i64 * 1_000_000_000;
    let expected = corpus.expected_metric_matrix(&case.case_id, step_ns);
    let gated = case.mode == "gated";
    let query_timeout = query_request_timeout(corpus.scale);
    println!(
        "pulsus-e2e:     case {:?} [{}] — {}: {} expected series",
        case.case_id,
        case.mode,
        case.construct,
        expected.len(),
    );

    let query_range = |url: String| {
        let q = q.clone();
        async move {
            let start = window.start_ns.to_string();
            let end = window.end_ns.to_string();
            let step = step_s.to_string();
            let res = ctx
                .http
                .get(&url)
                .query(&[
                    ("query", q.as_str()),
                    ("start", start.as_str()),
                    ("end", end.as_str()),
                    ("step", step.as_str()),
                ])
                .timeout(query_timeout) // issue #92/#106, see query_store
                .send()
                .await
                .with_context(|| format!("GET {url} failed"))?;
            if !res.status().is_success() {
                let status = res.status();
                let body = res.text().await.unwrap_or_default();
                bail!("{url} for {q:?} returned {status}: {body}");
            }
            res.json::<serde_json::Value>()
                .await
                .with_context(|| format!("{url} body was not JSON"))
        }
    };
    let pulsus_started = std::time::Instant::now();
    let pulsus_body = query_range(ctx.url("/api/logs/v1/query_range")).await?;
    let pulsus_elapsed = pulsus_started.elapsed();
    let oracle_started = std::time::Instant::now();
    let oracle_body = query_range(format!("{}/loki/api/v1/query_range", ctx.loki_url)).await?;
    let oracle_elapsed = oracle_started.elapsed();
    // Per-case elapsed line (issue #92, see `run_streams_case`).
    println!(
        "pulsus-e2e: query {q:?} (case {:?}) pulsusdb {}ms oracle {}ms",
        case.case_id,
        pulsus_elapsed.as_millis(),
        oracle_elapsed.as_millis(),
    );
    let pulsus_set = matrix_result_set(&pulsus_body)?;
    let oracle_set = matrix_result_set(&oracle_body)?;

    let dump = |kind: &str, detail: &str| -> Result<std::path::PathBuf> {
        let artifact = serde_json::json!({
            "surface": "logs_metric_pipeline",
            "case_id": case.case_id,
            "mode": case.mode,
            "kind": kind,
            "query": q,
            "window": { "start_ns": window.start_ns, "end_ns": window.end_ns, "step_s": step_s },
            "pulsusdb_result": pulsus_body,
            "oracle_result": oracle_body,
            "detail": detail,
        });
        write_artifact(
            ctx,
            ARTIFACT_AREA,
            if gated {
                "metric-case-mismatch"
            } else {
                "informational-case"
            },
            &artifact,
        )
    };

    // PulsusDB vs the tumbling by-construction expectation: ALWAYS hard.
    if !matrices_match(&pulsus_set, &expected) {
        let path = dump(
            "pulsus_vs_corpus",
            &format!("pulsusdb matrix diverged: got {pulsus_set:?}, expected {expected:?}"),
        )?;
        bail!(
            "case {:?}: pulsusdb diverged from the tumbling corpus expectation (repro {})",
            case.case_id,
            path.display()
        );
    }
    if !matrices_match(&oracle_set, &expected) {
        let path = dump("oracle_vs_corpus", "oracle sliding-window result diverged")?;
        if gated {
            bail!(
                "case {:?}: oracle diverged from the corpus expectation (repro {})",
                case.case_id,
                path.display()
            );
        }
        println!(
            "pulsus-e2e:   logs informational delta (never gating): case {:?} (ledger {:?}) \
             (dumped to {})",
            case.case_id,
            case.ledger.as_deref().unwrap_or(""),
            path.display()
        );
    } else if !gated {
        let path = dump(
            "stale_exclusion",
            "informational metric case matched the oracle",
        )?;
        bail!(
            "case {:?}: ledgered divergence ({:?}) is stale — re-gate the case (repro {})",
            case.case_id,
            case.ledger.as_deref().unwrap_or(""),
            path.display()
        );
    }
    Ok(())
}

/// The M6-09 streams comparison: validity gates first (raw counts
/// strictly below the limit on both stores; no duplicate entries), then
/// PulsusDB == corpus (ALWAYS hard) == oracle (hard for `gated`,
/// recorded for `informational`).
async fn run_streams_case(
    ctx: &Ctx,
    corpus: &LogCorpus,
    fixture: &LogsFixture,
    case: &CaseRaw,
    window: QueryWindow,
) -> Result<()> {
    let q = case.query.replace("{R}", &corpus.run_id);
    let expected = corpus.expected_case_result(&case.case_id);
    let gated = case.mode == "gated";
    let query_timeout = query_request_timeout(corpus.scale);
    println!(
        "pulsus-e2e:     case {:?} [{}] — {}: {} expected entry(ies) across {} stream(s)",
        case.case_id,
        case.mode,
        case.construct,
        set_entry_count(&expected),
        expected.len(),
    );

    // One elapsed line per case (issue #92, the metrics-differential
    // precedent): budget breaches against the tier-aware query timeout
    // stay diagnosable from CI logs alone. Elapsed only — these helpers
    // return parsed JSON, so no raw byte count is in hand.
    let pulsus_started = std::time::Instant::now();
    let pulsus_body = query_pulsus(ctx, &q, window, fixture.limit, query_timeout).await?;
    let pulsus_elapsed = pulsus_started.elapsed();
    let loki_started = std::time::Instant::now();
    let loki_body = query_loki(ctx, &q, window, fixture.limit, query_timeout).await?;
    let loki_elapsed = loki_started.elapsed();
    println!(
        "pulsus-e2e: query {q:?} (case {:?}) pulsusdb {}ms oracle {}ms",
        case.case_id,
        pulsus_elapsed.as_millis(),
        loki_elapsed.as_millis(),
    );
    let pulsus_set = result_set(&pulsus_body)?;
    let loki_set = result_set(&loki_body)?;

    let dump = |kind: &str, detail: &str| -> Result<std::path::PathBuf> {
        let artifact = serde_json::json!({
            "surface": "logs_pipeline",
            "case_id": case.case_id,
            "mode": case.mode,
            "kind": kind,
            "query": q,
            "window": { "start_ns": window.start_ns, "end_ns": window.end_ns, "limit": fixture.limit },
            "expected_entry_count": set_entry_count(&expected),
            "pulsusdb_result": pulsus_body,
            "oracle_result": loki_body,
            "detail": detail,
        });
        write_artifact(
            ctx,
            ARTIFACT_AREA,
            if gated {
                "case-mismatch"
            } else {
                "informational-case"
            },
            &artifact,
        )
    };

    // Validity gate (b): a raw count at the limit means truncation — a
    // top-K, not a set. Hard on both stores, even for informational
    // cases (it invalidates the comparison, not the semantics).
    for (store, body) in [("pulsusdb", &pulsus_body), ("oracle", &loki_body)] {
        let raw = raw_entry_count(body);
        if raw as u32 >= fixture.limit {
            let path = dump(
                "truncation",
                &format!("{store} raw entry count reached the limit"),
            )?;
            bail!(
                "case {:?}: {store} returned {raw} raw entries at limit {} — comparison invalid \
                 (repro {})",
                case.case_id,
                fixture.limit,
                path.display()
            );
        }
    }
    // Validity gate (c): duplicate entries would collapse in the set
    // comparison and mask a real response-shaping bug. Hard on both.
    for (store, body, set) in [
        ("pulsusdb", &pulsus_body, &pulsus_set),
        ("oracle", &loki_body, &loki_set),
    ] {
        let raw = raw_entry_count(body);
        let distinct = set_entry_count(set);
        if raw != distinct {
            let path = dump(
                "duplicate_entries",
                &format!("{store} returned {raw} raw entries but only {distinct} distinct"),
            )?;
            bail!(
                "case {:?}: {store} response carried duplicate entries (repro {})",
                case.case_id,
                path.display()
            );
        }
    }

    // PulsusDB vs the corpus expectation: ALWAYS hard.
    if pulsus_set != expected {
        let detail = describe_diff("pulsusdb", &pulsus_set, &expected);
        let path = dump("pulsus_vs_corpus", &detail)?;
        bail!(
            "case {:?}: {detail} (repro {})",
            case.case_id,
            path.display()
        );
    }

    // Oracle vs the corpus expectation (== vs PulsusDB, transitively).
    if loki_set != expected {
        let detail = describe_diff("oracle", &loki_set, &expected);
        let path = dump("oracle_vs_corpus", &detail)?;
        if gated {
            bail!(
                "case {:?}: {detail} (repro {})",
                case.case_id,
                path.display()
            );
        }
        println!(
            "pulsus-e2e:   logs informational delta (never gating): case {:?} (ledger {:?}): \
             {detail} (dumped to {})",
            case.case_id,
            case.ledger.as_deref().unwrap_or(""),
            path.display()
        );
    } else if !gated {
        // Anti-rot (issue #72 review round 1, finding 5, mirroring the
        // ledger discipline): a ledgered oracle divergence that has
        // STARTED MATCHING again must fail the run — the stale exclusion
        // has to be removed (case re-gated, ledger entry kept for
        // history), never left silently passing.
        let path = dump(
            "stale_exclusion",
            "informational case matched the oracle — the ledgered divergence no longer exists",
        )?;
        bail!(
            "case {:?}: ledgered divergence ({:?}) is stale — the oracle now matches; re-gate \
             the case and drop its ledger reference (repro {})",
            case.case_id,
            case.ledger.as_deref().unwrap_or(""),
            path.display()
        );
    }
    Ok(())
}

/// The fetch-until-limit ordered-limited comparison (issue #100): a
/// heavily-dropping pipeline (`| json | status = "503" | took_ms =
/// "500"` — two dropping label filters ⇒ `fetch_until_limit`) whose
/// earliest-`limit` survivors span >= 2 keyset pages. Unlike
/// [`run_streams_case`] this REQUIRES exactly `limit` raw entries on both
/// stores and compares an ORDERED `Vec<(labels, ts, line)>` (earliest-
/// `limit` by ascending ts) against the corpus prefix, not a set.
///
/// **Full tier only.** At CI scale the page-1 window (`limit × factor`
/// records) exceeds the whole svc-json corpus, so the case cannot page;
/// it skips with a printed reason (the nightly lane always runs full).
///
/// **`raw == limit` IS the page-2 proof (plan v2 delta 3, no engine
/// change).** A single page yields at most `S1 < limit` survivors
/// (asserted hermetically), so returning exactly `limit` is physically
/// impossible without a second fetch — a paging-removal regression
/// (revert to the old oversample-and-truncate) returns `S1 != limit` and
/// fails the gate.
async fn run_streams_limited_case(
    ctx: &Ctx,
    corpus: &LogCorpus,
    case: &CaseRaw,
    window: QueryWindow,
) -> Result<()> {
    let limit = case.limit.with_context(|| {
        format!(
            "case {:?} is streams_limited but carries no per-case limit",
            case.case_id
        )
    })?;
    // Full-tier self-gate (plan v2 delta 2): a multi-page phenomenon needs
    // a corpus larger than one page. Skip cleanly at CI scale.
    if corpus.scale != Scale::Full {
        println!(
            "pulsus-e2e:     case {:?} [{}] — skipped: streams_limited needs the full tier (the \
             page-1 window of {} svc-json records exceeds the CI-tier corpus, so it cannot page)",
            case.case_id,
            case.mode,
            limit * E2E_DEPLOYED_SCAN_FACTOR,
        );
        return Ok(());
    }

    let q = case.query.replace("{R}", &corpus.run_id);
    let expected = corpus.expected_ordered_limited(&case.case_id, limit);
    let gated = case.mode == "gated";
    let query_timeout = query_request_timeout(corpus.scale);
    println!(
        "pulsus-e2e:     case {:?} [{}] — {}: expecting exactly {} ordered entry(ies) across {} \
         page(s)",
        case.case_id,
        case.mode,
        case.construct,
        expected.len(),
        2, // >= 2 by construction (see AC3′); logged for CI diagnosis
    );

    let pulsus_started = std::time::Instant::now();
    let pulsus_body = query_pulsus(ctx, &q, window, limit, query_timeout).await?;
    let pulsus_elapsed = pulsus_started.elapsed();
    let loki_started = std::time::Instant::now();
    let loki_body = query_loki(ctx, &q, window, limit, query_timeout).await?;
    let loki_elapsed = loki_started.elapsed();
    println!(
        "pulsus-e2e: query {q:?} (case {:?}) pulsusdb {}ms oracle {}ms",
        case.case_id,
        pulsus_elapsed.as_millis(),
        loki_elapsed.as_millis(),
    );

    let dump = |kind: &str, detail: &str| -> Result<std::path::PathBuf> {
        let artifact = serde_json::json!({
            "surface": "logs_pipeline_limited",
            "case_id": case.case_id,
            "mode": case.mode,
            "kind": kind,
            "query": q,
            "window": { "start_ns": window.start_ns, "end_ns": window.end_ns, "limit": limit },
            "expected_ordered": expected
                .iter()
                .map(|(l, ts, line)| serde_json::json!({"labels": l, "ts": ts, "line": line}))
                .collect::<Vec<_>>(),
            "pulsusdb_result": pulsus_body,
            "oracle_result": loki_body,
            "detail": detail,
        });
        write_artifact(
            ctx,
            ARTIFACT_AREA,
            if gated {
                "limited-case-mismatch"
            } else {
                "informational-case"
            },
            &artifact,
        )
    };

    // Validity gates, HARD on both stores (they invalidate the comparison
    // regardless of gated/informational):
    //   1. raw == limit — raw < limit is the #90 fetch-until-limit
    //      under-return regression (a single-page stop); raw > limit
    //      breaks the response cap. Also the page-2 proof (plan v2 delta 3).
    //   2. no duplicate entries (they would collapse and mask a bug).
    //   3. strictly distinct timestamps — the ordered comparison must not
    //      depend on tie-breaking (a duplicate ts signals ambiguity and
    //      invalidates the comparison rather than passing silently).
    for (store, body) in [("pulsusdb", &pulsus_body), ("oracle", &loki_body)] {
        let raw = raw_entry_count(body);
        if raw as u32 != limit {
            let path = dump(
                "limit_mismatch",
                &format!("{store} returned {raw} raw entries, expected exactly {limit}"),
            )?;
            bail!(
                "case {:?}: {store} returned {raw} raw entries, expected exactly {limit} — a \
                 count below the limit is the #90 fetch-until-limit under-return (single-page \
                 stop); above breaks the cap (repro {})",
                case.case_id,
                path.display()
            );
        }
        let entries = ordered_entries(body)?;
        let distinct: BTreeSet<_> = entries.iter().cloned().collect();
        if distinct.len() != entries.len() {
            let path = dump(
                "duplicate_entries",
                &format!(
                    "{store} returned {} entries but only {} distinct",
                    entries.len(),
                    distinct.len()
                ),
            )?;
            bail!(
                "case {:?}: {store} response carried duplicate entries (repro {})",
                case.case_id,
                path.display()
            );
        }
        if entries.windows(2).any(|w| w[0].1 == w[1].1) {
            let path = dump(
                "ambiguous_order",
                &format!("{store} response carried a duplicate timestamp"),
            )?;
            bail!(
                "case {:?}: {store} response has a duplicate timestamp — the ordered comparison \
                 is ambiguous (repro {})",
                case.case_id,
                path.display()
            );
        }
    }

    // PulsusDB vs the corpus ordered prefix: ALWAYS hard.
    let pulsus_entries = ordered_entries(&pulsus_body)?;
    if pulsus_entries != expected {
        let path = dump(
            "pulsus_vs_corpus",
            &format!("pulsusdb ordered result {pulsus_entries:?} != expected {expected:?}"),
        )?;
        bail!(
            "case {:?}: pulsusdb ordered result diverged from the corpus earliest-{limit} prefix \
             (repro {})",
            case.case_id,
            path.display()
        );
    }

    // Oracle vs the corpus ordered prefix (== vs PulsusDB, transitively).
    let loki_entries = ordered_entries(&loki_body)?;
    if loki_entries != expected {
        let path = dump(
            "oracle_vs_corpus",
            &format!("oracle ordered result {loki_entries:?} != expected {expected:?}"),
        )?;
        if gated {
            bail!(
                "case {:?}: oracle ordered result diverged from the corpus earliest-{limit} \
                 prefix (repro {})",
                case.case_id,
                path.display()
            );
        }
        println!(
            "pulsus-e2e:   logs informational delta (never gating): case {:?} (ledger {:?}) \
             (dumped to {})",
            case.case_id,
            case.ledger.as_deref().unwrap_or(""),
            path.display()
        );
    } else if !gated {
        // Anti-rot, mirroring `run_streams_case`.
        let path = dump(
            "stale_exclusion",
            "informational case matched the oracle — the ledgered divergence no longer exists",
        )?;
        bail!(
            "case {:?}: ledgered divergence ({:?}) is stale — re-gate the case (repro {})",
            case.case_id,
            case.ledger.as_deref().unwrap_or(""),
            path.display()
        );
    }
    Ok(())
}

fn describe_diff(store: &str, got: &ExpectedResult, expected: &ExpectedResult) -> String {
    let got_streams: BTreeSet<String> = got.keys().map(|k| format!("{k:?}")).collect();
    let expected_streams: BTreeSet<String> = expected.keys().map(|k| format!("{k:?}")).collect();
    format!(
        "{store} result set diverged from the corpus expectation: {} vs {} streams, {} vs {} \
         entries; streams only in {store}: {:?}; streams missing: {:?}",
        got.len(),
        expected.len(),
        set_entry_count(got),
        set_entry_count(expected),
        got_streams
            .difference(&expected_streams)
            .collect::<Vec<_>>(),
        expected_streams
            .difference(&got_streams)
            .collect::<Vec<_>>(),
    )
}

// ---------------------------------------------------------------------
// Issue #102: the Loki-push structured-metadata (SM) differential.
//
// A NEW scenario, own `run_id`/fixture/completeness gate. The M6-09 OTLP
// corpus carries NO per-entry structured metadata (OTLP has no SM on the
// collector path), so this lane instead pushes identical native Loki JSON
// `[ts, line, {sm}]` bodies DIRECTLY to both stores' `/loki/api/v1/push`
// endpoints and asserts the SM surfacing/collision behavior #97 shipped is
// byte-parity against `grafana/loki:3.4.2`. No SM pushdown: label filters on
// SM keys are the #97 client-side baseline (no new read-path SQL).
// ---------------------------------------------------------------------

const SM_FIXTURE_PATH: &str = "logs/sm_differential.json";

#[derive(Debug, Deserialize)]
struct SmCaseRaw {
    case_id: String,
    /// Which SM behavior this case covers — documentation, unit-tested
    /// non-empty.
    construct: String,
    /// Always `"gated"` for the SM lane (every SM behavior is byte-exact
    /// against the oracle; no informational downgrade, no ledger id).
    mode: String,
    query: String,
}

#[derive(Debug, Deserialize)]
struct SmFixture {
    limit: u32,
    cases: Vec<SmCaseRaw>,
}

fn load_sm_fixture(ctx: &Ctx) -> Result<SmFixture> {
    let path = ctx.fixtures_dir.join(SM_FIXTURE_PATH);
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read fixture {}", path.display()))?;
    let fixture: SmFixture = serde_json::from_str(&raw)
        .with_context(|| format!("fixture {} was not valid JSON", path.display()))?;
    for case in &fixture.cases {
        if !logs_sm_corpus::SM_CASE_IDS.contains(&case.case_id.as_str()) {
            bail!(
                "fixture {} names SM case {:?}, which the corpus does not project",
                path.display(),
                case.case_id
            );
        }
    }
    Ok(fixture)
}

fn build_sm_corpus() -> Result<logs_sm_corpus::SmCorpus> {
    let run_id = format!("e2e-logs-sm-diff-{:x}", crate::metrics::unique_id()?);
    let now_ns = now_unix_nanos()?;
    // Anchor the last record near "now" (avoids Loki's reject_old_samples /
    // creation_grace_period), like `build_corpus`.
    let span_ns = logs_sm_corpus::STEP_NS * (logs_sm_corpus::ENTRY_COUNT as i64 - 1);
    let base_ns = now_ns - span_ns - CORPUS_NOW_MARGIN_NS;
    Ok(logs_sm_corpus::generate(&logs_sm_corpus::SmCorpusSpec {
        base_ns,
        run_id,
    }))
}

fn sm_query_window(corpus: &logs_sm_corpus::SmCorpus) -> QueryWindow {
    QueryWindow {
        start_ns: corpus.first_ts_ns - WINDOW_SLACK_NS,
        end_ns: corpus.last_ts_ns + WINDOW_SLACK_NS,
    }
}

pub async fn logs_structured_metadata_differential(ctx: &Ctx) -> Result<()> {
    if !differential_enabled() {
        println!(
            "pulsus-e2e:   logs_structured_metadata_differential: skipped (set \
             PULSUS_E2E_LOGS_DIFFERENTIAL=1 — nightly/dispatch tier only, issue #102)"
        );
        return Ok(());
    }
    let fixture = load_sm_fixture(ctx)?;
    // The SM corpus is a fixed, non-tiered size, but the lane runs in the
    // same saturated nightly full-tier job (issue #106): resolve the same
    // `PULSUS_E2E_LOGS_SCALE` the main logs lane does, purely to select the
    // tier-aware completeness/query budgets (the corpus itself is
    // unchanged).
    let scale = resolve_scale()?;
    let corpus = build_sm_corpus()?;
    let window = sm_query_window(&corpus);
    println!(
        "pulsus-e2e:   logs_structured_metadata_differential [{:?}]: dual-pushing {} SM records \
         (run_id={:?})",
        ctx.variant,
        corpus.entries.len(),
        corpus.run_id
    );

    push_sm_corpus(ctx, &corpus)
        .await
        .context("dual-pushing the SM corpus to both stores failed")?;

    wait_for_sm_completeness(ctx, &corpus, window, fixture.limit, scale).await?;

    for case in &fixture.cases {
        run_sm_case(ctx, &corpus, &fixture, case, window, scale)
            .await
            .with_context(|| format!("SM differential case {:?}", case.case_id))?;
    }
    Ok(())
}

/// One `POST {url}` of one Loki JSON push body: `Ok(Some(response))` once the
/// request reaches the store at all (any HTTP response — the caller checks
/// the status), the [`poll_until`]-on-transport-failure shape
/// [`logs_roundtrip`](crate::scenarios)'s `post_otlp_logs` uses. A transport
/// `Err` triggers a retry of the identical body; a connect-time failure is
/// safe (zero bytes reached the server), but a response-read failure *after*
/// the server ingested the body would double-ingest on retry. That is not
/// silent: it trips the `raw > distinct` duplicate-delivery validity gate in
/// [`run_sm_case`] as a loud, diagnosable failure. Retry idempotency across
/// this and `post_otlp_logs` is tracked as a follow-up.
async fn push_loki_json(
    ctx: &Ctx,
    url: &str,
    body: &serde_json::Value,
) -> Result<Option<reqwest::Response>> {
    let res = ctx.http.post(url).json(body).send().await?;
    Ok(Some(res))
}

/// Fans the SM corpus's per-stream push bodies to BOTH stores' native
/// `/loki/api/v1/push` (identical wire bytes — stronger than the OTLP
/// fan-out, no collector transform between the two), each expecting a 204.
/// Every body polls-until-listening (absorbs slow container start).
async fn push_sm_corpus(ctx: &Ctx, corpus: &logs_sm_corpus::SmCorpus) -> Result<()> {
    let bodies = logs_sm_corpus::to_loki_push_json(corpus);
    let pulsus_url = ctx.url("/loki/api/v1/push");
    let loki_url = format!("{}/loki/api/v1/push", ctx.loki_url);
    for (store, url) in [("pulsusdb", &pulsus_url), ("oracle", &loki_url)] {
        for body in &bodies {
            let res = poll_until(
                COLLECTOR_READY_POLL_TIMEOUT,
                COLLECTOR_READY_POLL_INTERVAL,
                || push_loki_json(ctx, url, body),
            )
            .await
            .with_context(|| format!("{store} loki push endpoint never accepted a connection"))?;
            if !res.status().is_success() {
                let status = res.status();
                let text = res.text().await.unwrap_or_default();
                bail!("{store} loki push returned {status}: {text}");
            }
        }
    }
    Ok(())
}

/// Bounded completeness poll for the SM lane (validity gate (a)), the same
/// two-pass shape as [`wait_for_completeness`] scoped to the SM `run_id`: raw
/// counts checked on BOTH stores before any set comparison, then the merged
/// expected set on both. Absorbs PulsusDB sync-flush + Loki ingester-flush
/// lag.
async fn wait_for_sm_completeness(
    ctx: &Ctx,
    corpus: &logs_sm_corpus::SmCorpus,
    window: QueryWindow,
    limit: u32,
    scale: Scale,
) -> Result<()> {
    let q = run_scope_query(&corpus.run_id);
    let expected = logs_sm_corpus::expected_all_records(corpus);
    let expected_total = set_entry_count(&expected);
    let query_timeout = query_request_timeout(scale);
    let progress = Cell::new((usize::MAX, usize::MAX));
    let last_log_at = Cell::new(Instant::now());
    let poll_result: Result<Result<()>> = poll_until(
        completeness_poll_timeout(scale),
        COMPLETENESS_POLL_INTERVAL,
        || async {
            let bodies = [
                (
                    "pulsusdb",
                    query_pulsus(ctx, &q, window, limit, query_timeout).await?,
                ),
                (
                    "oracle",
                    query_loki(ctx, &q, window, limit, query_timeout).await?,
                ),
            ];
            let mut sets = Vec::with_capacity(bodies.len());
            for (store, body) in &bodies {
                let raw = raw_entry_count(body);
                if raw as u32 >= limit {
                    let artifact = serde_json::json!({
                        "surface": "logs_sm_completeness",
                        "kind": "truncation",
                        "store": store,
                        "query": q,
                        "raw_entries": raw,
                        "limit": limit,
                        "result": body,
                    });
                    let path = write_artifact(
                        ctx,
                        ARTIFACT_AREA,
                        "sm-completeness-truncation",
                        &artifact,
                    )?;
                    return Ok(Some(Err(anyhow::anyhow!(
                        "sm completeness: {store} returned {raw} raw entries at limit {limit} — \
                         corpus/limit sizing invalid (repro {})",
                        path.display()
                    ))));
                }
                let set = result_set(body)?;
                let distinct = set_entry_count(&set);
                if raw > distinct {
                    let artifact = serde_json::json!({
                        "surface": "logs_sm_completeness",
                        "kind": "duplicate_delivery",
                        "store": store,
                        "query": q,
                        "raw_entries": raw,
                        "distinct_entries": distinct,
                        "result": body,
                    });
                    let path = write_artifact(
                        ctx,
                        ARTIFACT_AREA,
                        "sm-completeness-duplicates",
                        &artifact,
                    )?;
                    return Ok(Some(Err(anyhow::anyhow!(
                        "sm completeness: {store} returned {raw} raw entries but only {distinct} \
                         distinct — duplicate delivery, comparison invalid (repro {})",
                        path.display()
                    ))));
                }
                sets.push(set);
            }
            let pulsus_matched = completeness_set_diff(&sets[0], &expected).matched;
            let oracle_matched = completeness_set_diff(&sets[1], &expected).matched;
            log_completeness_progress(
                &progress,
                &last_log_at,
                "sm logs",
                expected_total,
                pulsus_matched,
                oracle_matched,
            );
            if sets.iter().any(|set| *set != expected) {
                return Ok(None); // still filling — keep polling
            }
            Ok(Some(Ok(())))
        },
    )
    .await;
    match poll_result {
        Ok(verdict) => verdict,
        Err(timeout_err) => Err(completeness_timeout_diagnostic(
            ctx,
            "logs_sm_completeness",
            "sm-completeness-timeout",
            &CompletenessProbe {
                q: &q,
                window,
                limit,
                query_timeout,
            },
            &expected,
            timeout_err.context(format!(
                "SM run {:?} never reached completeness ({} records) on both stores",
                corpus.run_id,
                corpus.entries.len()
            )),
        )
        .await),
    }
}

/// One SM case: validity gates first (raw counts strictly below the limit on
/// both stores; no duplicate entries), then PulsusDB == corpus (ALWAYS hard)
/// == oracle (hard — every SM case is `gated`). The comparison key is the
/// FULL merged label set, so a silent SM drop on either store is caught.
async fn run_sm_case(
    ctx: &Ctx,
    corpus: &logs_sm_corpus::SmCorpus,
    fixture: &SmFixture,
    case: &SmCaseRaw,
    window: QueryWindow,
    scale: Scale,
) -> Result<()> {
    if case.mode != "gated" {
        bail!(
            "SM case {:?} has mode {:?}; every SM case is byte-exact and stays gated",
            case.case_id,
            case.mode
        );
    }
    let q = case.query.replace("{R}", &corpus.run_id);
    let expected = logs_sm_corpus::expected_case_result(corpus, &case.case_id);
    let query_timeout = query_request_timeout(scale);
    println!(
        "pulsus-e2e:     SM case {:?} [{}] — {}: {} expected entry(ies) across {} stream(s)",
        case.case_id,
        case.mode,
        case.construct,
        set_entry_count(&expected),
        expected.len(),
    );

    let pulsus_started = std::time::Instant::now();
    let pulsus_body = query_pulsus(ctx, &q, window, fixture.limit, query_timeout).await?;
    let pulsus_elapsed = pulsus_started.elapsed();
    let loki_started = std::time::Instant::now();
    let loki_body = query_loki(ctx, &q, window, fixture.limit, query_timeout).await?;
    let loki_elapsed = loki_started.elapsed();
    println!(
        "pulsus-e2e: query {q:?} (SM case {:?}) pulsusdb {}ms oracle {}ms",
        case.case_id,
        pulsus_elapsed.as_millis(),
        loki_elapsed.as_millis(),
    );
    let pulsus_set = result_set(&pulsus_body)?;
    let loki_set = result_set(&loki_body)?;

    let dump = |kind: &str, detail: &str| -> Result<std::path::PathBuf> {
        let artifact = serde_json::json!({
            "surface": "logs_sm_pipeline",
            "case_id": case.case_id,
            "mode": case.mode,
            "kind": kind,
            "query": q,
            "window": { "start_ns": window.start_ns, "end_ns": window.end_ns, "limit": fixture.limit },
            "expected_entry_count": set_entry_count(&expected),
            "pulsusdb_result": pulsus_body,
            "oracle_result": loki_body,
            "detail": detail,
        });
        write_artifact(ctx, ARTIFACT_AREA, "sm-case-mismatch", &artifact)
    };

    // Validity gates, HARD on both stores (they invalidate the comparison).
    for (store, body) in [("pulsusdb", &pulsus_body), ("oracle", &loki_body)] {
        let raw = raw_entry_count(body);
        if raw as u32 >= fixture.limit {
            let path = dump(
                "truncation",
                &format!("{store} raw entry count reached the limit"),
            )?;
            bail!(
                "SM case {:?}: {store} returned {raw} raw entries at limit {} — comparison invalid \
                 (repro {})",
                case.case_id,
                fixture.limit,
                path.display()
            );
        }
    }
    for (store, body, set) in [
        ("pulsusdb", &pulsus_body, &pulsus_set),
        ("oracle", &loki_body, &loki_set),
    ] {
        let raw = raw_entry_count(body);
        let distinct = set_entry_count(set);
        if raw != distinct {
            let path = dump(
                "duplicate_entries",
                &format!("{store} returned {raw} raw entries but only {distinct} distinct"),
            )?;
            bail!(
                "SM case {:?}: {store} response carried duplicate entries (repro {})",
                case.case_id,
                path.display()
            );
        }
    }

    // PulsusDB vs the corpus expectation: ALWAYS hard.
    if pulsus_set != expected {
        let detail = describe_diff("pulsusdb", &pulsus_set, &expected);
        let path = dump("pulsus_vs_corpus", &detail)?;
        bail!(
            "SM case {:?}: {detail} (repro {})",
            case.case_id,
            path.display()
        );
    }
    // Oracle vs the corpus expectation (== vs PulsusDB, transitively) — hard,
    // every SM case is gated.
    if loki_set != expected {
        let detail = describe_diff("oracle", &loki_set, &expected);
        let path = dump("oracle_vs_corpus", &detail)?;
        bail!(
            "SM case {:?}: {detail} (repro {})",
            case.case_id,
            path.display()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logs_corpus::{CASE_IDS, METRIC_CASE_IDS};

    /// The committed exclusion list (plan v3 delta 5): every case starts
    /// gated; a case id appears here ONLY after a triaged divergence is
    /// recorded in docs/benchmarks/logs-differential-ledger.md. Update
    /// deliberately, with the ledger entry, never as a quick fix for a
    /// red run. `metric_rate_tumbling` is the issue-M6-10 SEEDED entry:
    /// the tumbling-vs-sliding range-window divergence is documented
    /// by-construction (the M1 tumbling contract), classified in the
    /// ledger at introduction per the M6-10 plan — PulsusDB stays
    /// hard-gated against the tumbling corpus expectation.
    const INFORMATIONAL_CASE_IDS: &[&str] = &[
        "metric_rate_tumbling",
        // Issue #91 range matching cases share the tumbling-vs-sliding
        // window divergence (pulsus == the tumbling corpus is still hard-
        // gated; only the oracle comparison is informational).
        "metric_match_on_range",
        "metric_match_ignoring_range",
        "metric_match_group_left_range",
        "metric_match_group_right_range",
    ];

    fn shipped_fixture() -> LogsFixture {
        let root = crate::engine::workspace_root();
        let raw = std::fs::read_to_string(root.join("test/fixtures").join(FIXTURE_PATH)).unwrap();
        serde_json::from_str(&raw).unwrap()
    }

    fn shipped_corpus(fixture: &LogsFixture, record_count: usize) -> LogCorpus {
        logs_corpus::generate(&LogCorpusSpec {
            scale: Scale::Ci,
            record_count,
            step_ns: fixture.step_ns,
            base_ns: 1_700_000_000_000_000_000,
            run_id: "fixture-check".to_string(),
        })
    }

    /// AC8 (hermetic half): the fixture's case ids are exactly
    /// `logs_corpus::CASE_IDS`, in order — corpus projection and the
    /// committed matrix can never drift.
    #[test]
    fn shipped_fixture_cases_match_the_corpus_case_ids_exactly() {
        let fixture = shipped_fixture();
        let fixture_ids: Vec<&str> = fixture.cases.iter().map(|c| c.case_id.as_str()).collect();
        // Issue M6-10: the id-set lock covers the streams cases followed
        // by the metric cases, in committed order.
        let mut all_ids: Vec<&str> = CASE_IDS.to_vec();
        all_ids.extend_from_slice(METRIC_CASE_IDS);
        assert_eq!(fixture_ids, all_ids);
    }

    #[test]
    fn shipped_metric_cases_carry_the_right_kinds_and_step() {
        let fixture = shipped_fixture();
        for case in &fixture.cases {
            if !METRIC_CASE_IDS.contains(&case.case_id.as_str()) {
                // Issue #100: the fetch-until-limit case is `streams_limited`
                // and carries a per-case `limit`; every other streams case is
                // plain `streams` with no limit override.
                match case.kind() {
                    "streams" => assert!(case.limit.is_none(), "{}", case.case_id),
                    "streams_limited" => assert!(case.limit.is_some(), "{}", case.case_id),
                    other => panic!("streams case {:?} has kind {other:?}", case.case_id),
                }
                continue;
            }
            match case.kind() {
                "metric_instant" | "metric_error" | "metric_match_error" => {
                    assert!(case.step_s.is_none(), "{}", case.case_id)
                }
                "metric_range" => assert!(case.step_s.is_some(), "{}", case.case_id),
                other => panic!("metric case {:?} has kind {other:?}", case.case_id),
            }
        }
    }

    /// The pinned exclusion list: every case is gated unless it appears
    /// on the ledger-backed list above.
    #[test]
    fn shipped_fixture_gated_set_is_exactly_the_committed_subset() {
        let fixture = shipped_fixture();
        for case in &fixture.cases {
            let expect_informational = INFORMATIONAL_CASE_IDS.contains(&case.case_id.as_str());
            match case.mode.as_str() {
                "gated" => assert!(
                    !expect_informational,
                    "case {:?} is on the pinned exclusion list but marked gated",
                    case.case_id
                ),
                "informational" => assert!(
                    expect_informational,
                    "case {:?} is informational but not on the pinned exclusion list — a case \
                     moves off the gate only via the ledger discipline",
                    case.case_id
                ),
                other => panic!("case {:?} has unknown mode {other:?}", case.case_id),
            }
        }
    }

    /// Every informational case must reference a ledger entry that the
    /// committed markdown actually contains — the mechanical
    /// fixture↔ledger link, both ways.
    #[test]
    fn informational_cases_are_recorded_in_the_committed_ledger() {
        let fixture = shipped_fixture();
        let ledger_path =
            crate::engine::workspace_root().join("docs/benchmarks/logs-differential-ledger.md");
        let ledger = std::fs::read_to_string(&ledger_path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", ledger_path.display()));
        for case in fixture.cases.iter().filter(|c| c.mode == "informational") {
            let entry = case.ledger.as_deref().unwrap_or_else(|| {
                panic!(
                    "informational case {:?} names no ledger entry",
                    case.case_id
                )
            });
            assert!(!entry.is_empty());
            assert!(
                ledger.contains(entry),
                "ledger is missing entry {entry:?} for case {:?}",
                case.case_id
            );
            assert!(
                ledger.contains(&case.case_id),
                "ledger entry {entry:?} does not name case {:?}",
                case.case_id
            );
        }
    }

    #[test]
    fn shipped_fixture_queries_are_run_scoped_and_substitutable() {
        let fixture = shipped_fixture();
        for case in &fixture.cases {
            assert!(
                case.query.contains(r#"run_id="{R}""#),
                "case {:?} is not run-scoped: {}",
                case.case_id,
                case.query
            );
            assert!(!case.construct.is_empty());
            let rendered = case.query.replace("{R}", "e2e-logs-test");
            assert!(!rendered.contains("{R}"));
        }
    }

    fn hermetic_plan_ctx() -> pulsus_read::logql::PlanCtx<'static> {
        pulsus_read::logql::PlanCtx {
            db: "pulsus",
            streams_idx: "log_streams_idx",
            streams: "log_streams",
            samples: "log_samples",
            rollup_table: "log_metrics_5s",
            rollup_res_ns: 5_000_000_000,
            scan_budget_bytes: 50 * 1024 * 1024 * 1024,
            max_streams: 100_000,
            pipeline_scan_factor: 10,
        }
    }

    fn hermetic_params(case: &CaseRaw, corpus: &LogCorpus) -> pulsus_read::logql::QueryParams {
        let spec = match case.kind() {
            "metric_instant" | "metric_error" | "metric_match_error" => {
                pulsus_read::logql::QuerySpec::Instant {
                    at_ns: metric_eval_ns(corpus),
                }
            }
            _ => pulsus_read::logql::QuerySpec::Range {
                start_ns: corpus.first_ts_ns - WINDOW_SLACK_NS,
                end_ns: corpus.last_ts_ns + WINDOW_SLACK_NS,
                step_ns: case.step_s.unwrap_or(60) * 1_000_000_000,
            },
        };
        pulsus_read::logql::QueryParams {
            spec,
            limit: 1000,
            direction: pulsus_read::logql::Direction::Forward,
        }
    }

    /// Every committed case query PARSES under the shipped grammar and
    /// its pipeline COMPILES under the shipped evaluator (streams cases)
    /// / PLANS under the shipped planner with every leaf pipeline
    /// compiling (metric cases) — a fixture typo fails hermetically, not
    /// at nightly runtime.
    #[test]
    fn shipped_fixture_queries_parse_and_their_pipelines_compile() {
        let fixture = shipped_fixture();
        let corpus = shipped_corpus(&fixture, fixture.ci.record_count);
        for case in &fixture.cases {
            let rendered = case.query.replace("{R}", "e2e-logs-test");
            let expr = pulsus_logql::parse(&rendered)
                .unwrap_or_else(|e| panic!("case {:?} query does not parse: {e}", case.case_id));
            // Issue #100: `streams_limited` compiles as a log pipeline too.
            if matches!(case.kind(), "streams" | "streams_limited") {
                let pulsus_logql::Expr::Log(log) = expr else {
                    panic!("case {:?} must be a log (streams) query", case.case_id);
                };
                pulsus_read::logql::pipeline::CompiledPipeline::compile(&log.pipeline)
                    .unwrap_or_else(|e| {
                        panic!("case {:?} pipeline does not compile: {e}", case.case_id)
                    });
                continue;
            }
            assert!(
                matches!(expr, pulsus_logql::Expr::Metric(_)),
                "case {:?} must be a metric query",
                case.case_id
            );
            let plan = pulsus_read::logql::plan(
                &expr,
                &hermetic_params(case, &corpus),
                &hermetic_plan_ctx(),
            )
            .unwrap_or_else(|e| panic!("case {:?} does not plan: {e}", case.case_id));
            let leaves: Vec<&pulsus_read::logql::MetricPlan> = match &plan {
                pulsus_read::logql::Plan::Metric(mp) => vec![mp],
                pulsus_read::logql::Plan::MetricBinary(node) => node.leaves(),
                pulsus_read::logql::Plan::Streams(_) => {
                    panic!("case {:?} planned as streams", case.case_id)
                }
            };
            for leaf in leaves {
                if let Some(client) = &leaf.client {
                    pulsus_read::logql::CompiledPipeline::compile(&client.pipeline).unwrap_or_else(
                        |e| {
                            panic!(
                                "case {:?} client pipeline does not compile: {e}",
                                case.case_id
                            )
                        },
                    );
                }
            }
        }
    }

    /// Set comparisons are only well-defined unclipped: at both shipped
    /// tier sizes, every case's expected entry set is non-empty and
    /// strictly below the fixture's request limit.
    #[test]
    fn shipped_fixture_expected_sets_are_non_vacuous_and_below_the_limit() {
        let fixture = shipped_fixture();
        for count in [fixture.ci.record_count, fixture.full.record_count] {
            let corpus = shipped_corpus(&fixture, count);
            for case in fixture.cases.iter().filter(|c| c.kind() == "streams") {
                let expected = corpus.expected_case_result(&case.case_id);
                let entries = set_entry_count(&expected);
                assert!(
                    entries > 0,
                    "case {:?} is vacuous at record_count {count}",
                    case.case_id
                );
                assert!(
                    (entries as u32) < fixture.limit,
                    "case {:?} has {entries} entries at record_count {count} — not strictly \
                     below limit {}",
                    case.case_id,
                    fixture.limit
                );
            }
        }
    }

    /// The metric cases' by-construction expectations are non-vacuous at
    /// both tiers (a vacuous expectation would gate nothing).
    #[test]
    fn shipped_metric_expectations_are_non_vacuous() {
        let fixture = shipped_fixture();
        for count in [fixture.ci.record_count, fixture.full.record_count] {
            let corpus = shipped_corpus(&fixture, count);
            for case in &fixture.cases {
                match case.kind() {
                    "metric_instant" => {
                        let expected = corpus.expected_metric_vector(&case.case_id);
                        assert!(
                            !expected.is_empty(),
                            "case {:?} is vacuous at record_count {count}",
                            case.case_id
                        );
                    }
                    "metric_range" => {
                        let step_ns = case.step_s.unwrap() as i64 * 1_000_000_000;
                        let expected = corpus.expected_metric_matrix(&case.case_id, step_ns);
                        let points: usize = expected.values().map(|p| p.len()).sum();
                        assert!(
                            points > 0,
                            "case {:?} is vacuous at record_count {count}",
                            case.case_id
                        );
                    }
                    _ => {}
                }
            }
        }
    }

    /// The metric-instant expectations agree with running the SHIPPED
    /// client-aggregation path over the generated bodies — corpus
    /// projection, fixture query, planner mode split, and the engine's
    /// own reducers cannot drift apart (hermetic anti-drift, mirroring
    /// the streams test below; the live lane then compares both stores).
    #[test]
    fn shipped_metric_expectations_agree_with_the_shipped_evaluator() {
        let fixture = shipped_fixture();
        let corpus = shipped_corpus(&fixture, fixture.ci.record_count);
        for case in fixture
            .cases
            .iter()
            .filter(|c| c.kind() == "metric_instant")
        {
            let rendered = case.query.replace("{R}", &corpus.run_id);
            let expr = pulsus_logql::parse(&rendered).expect("parse");
            let service = first_selector_service(&expr);
            let params = hermetic_params(case, &corpus);
            let plan =
                pulsus_read::logql::plan(&expr, &params, &hermetic_plan_ctx()).expect("plan");
            let result = match &plan {
                pulsus_read::logql::Plan::Metric(mp) => {
                    evaluate_leaf_hermetically(&corpus, mp, &service)
                }
                pulsus_read::logql::Plan::MetricBinary(node) => {
                    evaluate_node_hermetically(&corpus, node, &service)
                }
                pulsus_read::logql::Plan::Streams(_) => panic!("metric case planned as streams"),
            };
            let pulsus_read::logql::QueryResult::Vector(samples) = result else {
                panic!("case {:?} did not evaluate to a vector", case.case_id);
            };
            let mut evaluated = MetricVector::new();
            for s in samples {
                let labels: std::collections::BTreeMap<String, String> =
                    s.labels.into_iter().collect();
                assert!(
                    evaluated.insert(labels, s.value).is_none(),
                    "duplicate label set in the evaluated vector"
                );
            }
            let expected = corpus.expected_metric_vector(&case.case_id);
            assert_eq!(
                evaluated.keys().collect::<Vec<_>>(),
                expected.keys().collect::<Vec<_>>(),
                "case {:?}: label sets diverged",
                case.case_id
            );
            for (labels, v) in &expected {
                assert!(
                    approx_eq(evaluated[labels], *v),
                    "case {:?}: value diverged for {labels:?}: {} vs {v}",
                    case.case_id,
                    evaluated[labels]
                );
            }
        }
    }

    /// The D1 witness, hermetic half: the SHIPPED evaluator FAILS the
    /// `metric_unwrap_error` case over the generated witness record with
    /// the named surviving-`__error__` error — the same 400 the live
    /// lane asserts on both stores.
    #[test]
    fn shipped_unwrap_error_witness_fails_the_shipped_evaluator_by_name() {
        let fixture = shipped_fixture();
        let corpus = shipped_corpus(&fixture, fixture.ci.record_count);
        let case = fixture
            .cases
            .iter()
            .find(|c| c.kind() == "metric_error")
            .expect("the witness case is committed");
        let rendered = case.query.replace("{R}", &corpus.run_id);
        let expr = pulsus_logql::parse(&rendered).expect("parse");
        let service = first_selector_service(&expr);
        let params = pulsus_read::logql::QueryParams {
            spec: pulsus_read::logql::QuerySpec::Instant {
                at_ns: metric_eval_ns(&corpus),
            },
            limit: 1000,
            direction: pulsus_read::logql::Direction::Forward,
        };
        let plan = pulsus_read::logql::plan(&expr, &params, &hermetic_plan_ctx()).expect("plan");
        let pulsus_read::logql::Plan::Metric(mp) = &plan else {
            panic!("witness case must plan as a single metric leaf");
        };
        let client = mp.client.as_ref().expect("client-aggregated");
        let compiled =
            pulsus_read::logql::CompiledPipeline::compile(&client.pipeline).expect("compile");
        let meta = std::collections::HashMap::from([(
            1u64,
            pulsus_read::logql::rows::StreamMetaRow {
                fingerprint: 1,
                service: service.clone(),
                labels: format!(
                    r#"{{"run_id":"{}","service_name":"{service}"}}"#,
                    corpus.run_id
                ),
            },
        )]);
        let rows: Vec<pulsus_read::logql::rows::MetricScanRow> = corpus
            .records
            .iter()
            .filter(|r| r.service == service)
            .map(|r| pulsus_read::logql::rows::MetricScanRow {
                fingerprint: 1,
                timestamp_ns: r.ts_ns,
                body: r.body.clone(),
            })
            .collect();
        assert!(!rows.is_empty(), "the witness record must exist");
        let err = pulsus_read::logql::run_client_agg_rows(
            &rows,
            &compiled,
            &meta,
            client,
            pulsus_read::logql::ClientWindow {
                start_ns: mp.start_ns,
                end_ns: mp.end_ns,
                step_ns: mp.step_ns,
            },
            mp.rate_window_ns,
        )
        .expect_err("a surviving conversion failure must fail the query");
        let pulsus_read::logql::ReadError::MetricPipelineError { error_type, .. } = &err else {
            panic!("expected MetricPipelineError, got {err:?}");
        };
        assert_eq!(error_type, pulsus_read::logql::SAMPLE_EXTRACTION_ERROR);
    }

    /// The `service_name` the case's (single) selector pins.
    fn first_selector_service(expr: &pulsus_logql::Expr) -> String {
        fn walk(me: &pulsus_logql::MetricExpr) -> Option<String> {
            match me {
                pulsus_logql::MetricExpr::Range { range, .. } => range
                    .selector
                    .selector
                    .matchers
                    .iter()
                    .find(|m| m.name == "service_name")
                    .map(|m| m.value.clone()),
                pulsus_logql::MetricExpr::Vector { inner, .. } => walk(inner),
                pulsus_logql::MetricExpr::Binary { lhs, rhs, .. } => {
                    walk(lhs).or_else(|| walk(rhs))
                }
                pulsus_logql::MetricExpr::Literal(_) => None,
            }
        }
        let pulsus_logql::Expr::Metric(me) = expr else {
            panic!("metric expr expected");
        };
        walk(me).expect("metric case selectors pin a service")
    }

    /// Runs one leaf `MetricPlan`'s client-aggregation over the corpus
    /// records the leaf's selector matches — the same pure sequence the
    /// engine executes post-fetch.
    fn evaluate_leaf_hermetically(
        corpus: &LogCorpus,
        mp: &pulsus_read::logql::MetricPlan,
        service: &str,
    ) -> pulsus_read::logql::QueryResult {
        let client = mp
            .client
            .as_ref()
            .expect("fixture metric leaves are client-aggregated");
        let compiled =
            pulsus_read::logql::CompiledPipeline::compile(&client.pipeline).expect("compile");
        let meta = std::collections::HashMap::from([(
            1u64,
            pulsus_read::logql::rows::StreamMetaRow {
                fingerprint: 1,
                service: service.to_string(),
                labels: format!(
                    r#"{{"run_id":"{}","service_name":"{service}"}}"#,
                    corpus.run_id
                ),
            },
        )]);
        let rows: Vec<pulsus_read::logql::rows::MetricScanRow> = corpus
            .records
            .iter()
            .filter(|r| r.service == service)
            .map(|r| pulsus_read::logql::rows::MetricScanRow {
                fingerprint: 1,
                timestamp_ns: r.ts_ns,
                body: r.body.clone(),
            })
            .collect();
        let result = pulsus_read::logql::run_client_agg_rows(
            &rows,
            &compiled,
            &meta,
            client,
            pulsus_read::logql::ClientWindow {
                start_ns: mp.start_ns,
                end_ns: mp.end_ns,
                step_ns: mp.step_ns,
            },
            mp.rate_window_ns,
        )
        .expect("client aggregation");
        pulsus_read::logql::apply_vector_aggs(result, &mp.vector_aggs)
    }

    fn evaluate_node_hermetically(
        corpus: &LogCorpus,
        node: &pulsus_read::logql::MetricNode,
        service: &str,
    ) -> pulsus_read::logql::QueryResult {
        match node {
            pulsus_read::logql::MetricNode::Leaf(mp) => {
                evaluate_leaf_hermetically(corpus, mp, service)
            }
            pulsus_read::logql::MetricNode::Scalar(v) => {
                pulsus_read::logql::QueryResult::Scalar(*v)
            }
            pulsus_read::logql::MetricNode::VectorAgg { aggs, inner } => {
                pulsus_read::logql::apply_vector_aggs(
                    evaluate_node_hermetically(corpus, inner, service),
                    aggs,
                )
            }
            pulsus_read::logql::MetricNode::Binary {
                op,
                return_bool,
                matching,
                lhs,
                rhs,
            } => pulsus_read::logql::combine_binary(
                *op,
                *return_bool,
                matching.as_ref(),
                evaluate_node_hermetically(corpus, lhs, service),
                evaluate_node_hermetically(corpus, rhs, service),
            )
            .expect("combine"),
        }
    }

    /// The corpus's expected sets agree with running the SHIPPED
    /// evaluator over the generated bodies — the projection, the fixture
    /// query, and `pulsus-read`'s own pipeline cannot drift apart
    /// (hermetic; the live lane then compares against the oracle).
    #[test]
    fn shipped_fixture_expected_sets_agree_with_the_shipped_evaluator() {
        let fixture = shipped_fixture();
        let corpus = shipped_corpus(&fixture, fixture.ci.record_count);
        for case in fixture.cases.iter().filter(|c| c.kind() == "streams") {
            let rendered = case.query.replace("{R}", &corpus.run_id);
            let expr = pulsus_logql::parse(&rendered).expect("parse");
            let pulsus_logql::Expr::Log(log) = expr else {
                panic!("streams query expected");
            };
            let selector_service = log
                .selector
                .matchers
                .iter()
                .find(|m| m.name == "service_name")
                .map(|m| m.value.clone())
                .expect("case selectors pin a service");
            let compiled = pulsus_read::logql::pipeline::CompiledPipeline::compile(&log.pipeline)
                .expect("compile");

            let mut evaluated = ExpectedResult::new();
            for r in corpus
                .records
                .iter()
                .filter(|r| r.service == selector_service)
            {
                let base = vec![
                    ("run_id".to_string(), corpus.run_id.clone()),
                    ("service_name".to_string(), r.service.to_string()),
                ];
                let Some(out) = compiled.run(&r.body, &base) else {
                    continue;
                };
                let labels: std::collections::BTreeMap<String, String> = out
                    .labels
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect();
                evaluated
                    .entry(labels)
                    .or_default()
                    .insert((r.ts_ns, out.line.into_owned()));
            }
            assert_eq!(
                evaluated,
                corpus.expected_case_result(&case.case_id),
                "case {:?}: shipped evaluator disagrees with the corpus projection",
                case.case_id
            );
        }
    }

    #[test]
    fn parse_logs_scale_defaults_and_rejects_like_the_sibling_parsers() {
        assert_eq!(parse_logs_scale(None).unwrap(), Scale::Ci);
        assert_eq!(parse_logs_scale(Some("CI")).unwrap(), Scale::Ci);
        assert_eq!(parse_logs_scale(Some("full")).unwrap(), Scale::Full);
        assert!(parse_logs_scale(Some("bogus")).is_err());
    }

    #[test]
    fn result_set_normalizes_the_streams_shape_and_rejects_non_streams() {
        let body = serde_json::json!({"data":{"resultType":"streams","result":[
            {"stream":{"service_name":"svc-json","status":"500"},
             "values":[["1700000000000000000","line a"],["1700000001000000000","line b"]]}
        ]}});
        let set = result_set(&body).unwrap();
        assert_eq!(set.len(), 1);
        assert_eq!(set_entry_count(&set), 2);
        assert_eq!(raw_entry_count(&body), 2);
        let matrix = serde_json::json!({"data":{"resultType":"matrix","result":[]}});
        assert!(result_set(&matrix).is_err());
    }

    #[test]
    fn raw_entry_count_counts_duplicates_that_the_set_collapses() {
        let body = serde_json::json!({"data":{"resultType":"streams","result":[
            {"stream":{"a":"1"},"values":[["100","x"],["100","x"]]}
        ]}});
        assert_eq!(raw_entry_count(&body), 2);
        assert_eq!(set_entry_count(&result_set(&body).unwrap()), 1);
    }

    // ---------------------------------------------------------------
    // Issue #100: the fetch-until-limit ordered-limited case.
    // ---------------------------------------------------------------

    fn fetch_until_limit_case(fixture: &LogsFixture) -> &CaseRaw {
        fixture
            .cases
            .iter()
            .find(|c| c.kind() == "streams_limited")
            .expect("the issue #100 fetch-until-limit case is committed")
    }

    /// AC-plan (hermetic): the case plans as a paged dropping streams
    /// scan — `fetch_until_limit`, `scan_limit == limit × factor`,
    /// `result_limit == limit`. Proves the engaged read path is the
    /// fetch-until-limit paging (not a single truncated scan).
    #[test]
    fn fetch_until_limit_case_plans_as_a_paged_dropping_scan() {
        let fixture = shipped_fixture();
        let case = fetch_until_limit_case(&fixture);
        let limit = case.limit.expect("streams_limited carries a limit");
        let corpus = shipped_corpus(&fixture, fixture.full.record_count);
        let rendered = case.query.replace("{R}", &corpus.run_id);
        let expr = pulsus_logql::parse(&rendered).expect("parse");
        let params = pulsus_read::logql::QueryParams {
            spec: pulsus_read::logql::QuerySpec::Range {
                start_ns: corpus.first_ts_ns - WINDOW_SLACK_NS,
                end_ns: corpus.last_ts_ns + WINDOW_SLACK_NS,
                step_ns: 0,
            },
            limit,
            direction: pulsus_read::logql::Direction::Forward,
        };
        let plan = pulsus_read::logql::plan(&expr, &params, &hermetic_plan_ctx()).expect("plan");
        let pulsus_read::logql::Plan::Streams(sp) = &plan else {
            panic!("case {:?} must plan as streams", case.case_id);
        };
        assert!(
            sp.fetch_until_limit,
            "two dropping label filters must engage fetch-until-limit"
        );
        assert_eq!(sp.result_limit, limit);
        assert_eq!(
            sp.scan_limit,
            limit * E2E_DEPLOYED_SCAN_FACTOR,
            "scan_limit must be the first-page size (limit × factor)"
        );
    }

    /// AC3′ (hermetic, full tier): the earliest-`limit` survivors
    /// provably span >= 2 pages — the survivors among the first
    /// `limit × factor` svc-json records (page 1) are strictly fewer than
    /// `limit`, forcing a second fetch, and total matches are >= `limit`.
    /// A corpus change that makes page 1 self-sufficient fails here.
    #[test]
    fn fetch_until_limit_case_provably_pages_at_full_tier() {
        let fixture = shipped_fixture();
        let case = fetch_until_limit_case(&fixture);
        let limit = case.limit.expect("streams_limited carries a limit") as usize;
        let corpus = shipped_corpus(&fixture, fixture.full.record_count);
        let page_size = limit * E2E_DEPLOYED_SCAN_FACTOR as usize;
        let svc_json: Vec<&logs_corpus::GeneratedRecord> = corpus
            .records
            .iter()
            .filter(|r| r.service == logs_corpus::SVC_JSON)
            .collect();
        let matches = |r: &logs_corpus::GeneratedRecord| {
            logs_corpus::case_projection(&case.case_id, r).is_some()
        };
        let s1 = svc_json
            .iter()
            .take(page_size)
            .filter(|r| matches(r))
            .count();
        let total = svc_json.iter().filter(|r| matches(r)).count();
        assert!(
            s1 < limit,
            "page-1 survivors {s1} must be < limit {limit} to force a second fetch"
        );
        assert!(
            limit <= total,
            "limit {limit} must be <= total matches {total}"
        );
        // At least one of the earliest-`limit` survivors is beyond page 1.
        let earliest_positions: Vec<usize> = svc_json
            .iter()
            .enumerate()
            .filter(|(_, r)| matches(r))
            .map(|(pos, _)| pos)
            .take(limit)
            .collect();
        assert!(
            earliest_positions.iter().any(|&pos| pos >= page_size),
            "at least one earliest-{limit} survivor must sit beyond the first page ({page_size} \
             svc-json records) — got positions {earliest_positions:?}"
        );
    }

    /// Tie-freedom (hermetic): the expected earliest-`limit` prefix has
    /// exactly `limit` entries with strictly increasing (distinct)
    /// timestamps, so the ordered comparison never depends on tie-breaking.
    #[test]
    fn fetch_until_limit_expected_prefix_has_strictly_increasing_distinct_ts() {
        let fixture = shipped_fixture();
        let case = fetch_until_limit_case(&fixture);
        let limit = case.limit.expect("streams_limited carries a limit");
        let corpus = shipped_corpus(&fixture, fixture.full.record_count);
        let expected = corpus.expected_ordered_limited(&case.case_id, limit);
        assert_eq!(expected.len(), limit as usize);
        for w in expected.windows(2) {
            assert!(
                w[0].1 < w[1].1,
                "expected prefix timestamps must be strictly increasing: {expected:?}"
            );
        }
    }

    /// Ordered-prefix anti-drift (hermetic): `expected_ordered_limited`
    /// equals the earliest-`limit` output of running the SHIPPED
    /// `CompiledPipeline` over the corpus in index (== ascending-ts)
    /// order. The corpus projection, the fixture query, and the engine's
    /// pipeline cannot drift apart. (`naive_matches` vs `case_projection`
    /// for this id is covered by the corpus circularity-breaker test.)
    #[test]
    fn fetch_until_limit_expected_prefix_agrees_with_the_shipped_evaluator() {
        let fixture = shipped_fixture();
        let case = fetch_until_limit_case(&fixture);
        let limit = case.limit.expect("streams_limited carries a limit") as usize;
        let corpus = shipped_corpus(&fixture, fixture.full.record_count);
        let rendered = case.query.replace("{R}", &corpus.run_id);
        let expr = pulsus_logql::parse(&rendered).expect("parse");
        let pulsus_logql::Expr::Log(log) = expr else {
            panic!("streams query expected");
        };
        let service = log
            .selector
            .matchers
            .iter()
            .find(|m| m.name == "service_name")
            .map(|m| m.value.clone())
            .expect("case selectors pin a service");
        let compiled = pulsus_read::logql::pipeline::CompiledPipeline::compile(&log.pipeline)
            .expect("compile");
        let mut evaluated: OrderedEntries = Vec::new();
        for r in corpus.records.iter().filter(|r| r.service == service) {
            let base = vec![
                ("run_id".to_string(), corpus.run_id.clone()),
                ("service_name".to_string(), r.service.to_string()),
            ];
            let Some(out) = compiled.run(&r.body, &base) else {
                continue;
            };
            let labels: std::collections::BTreeMap<String, String> = out
                .labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            evaluated.push((labels, r.ts_ns, out.line.into_owned()));
            if evaluated.len() == limit {
                break;
            }
        }
        assert_eq!(
            evaluated,
            corpus.expected_ordered_limited(&case.case_id, limit as u32),
            "shipped evaluator disagrees with the corpus ordered prefix"
        );
    }

    /// AC-deploy (hermetic): `deploy/e2e/compose.single.yaml` overrides
    /// neither `logql_pipeline_scan_factor` nor its env var, so the
    /// deployed factor is the config default (`E2E_DEPLOYED_SCAN_FACTOR`)
    /// and the page-1 arithmetic stays valid against the live server.
    #[test]
    fn deployed_compose_does_not_override_the_scan_factor() {
        let compose = crate::engine::workspace_root().join("deploy/e2e/compose.single.yaml");
        let raw = std::fs::read_to_string(&compose)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", compose.display()));
        assert!(
            !raw.contains("logql_pipeline_scan_factor"),
            "compose must not override reader.logql_pipeline_scan_factor"
        );
        assert!(
            !raw.contains("PULSUS_LOGQL_PIPELINE_SCAN_FACTOR"),
            "compose must not override the scan-factor env var"
        );
    }

    /// The deployed factor constant matches the hermetic plan context's
    /// factor (both are the config default 10) — the plan-time and
    /// live-time page sizes agree.
    #[test]
    fn deployed_scan_factor_matches_the_hermetic_plan_ctx() {
        assert_eq!(
            E2E_DEPLOYED_SCAN_FACTOR,
            hermetic_plan_ctx().pipeline_scan_factor
        );
    }

    /// Response-order trip (hermetic, issue #100 fix, plan v2 item 5):
    /// the ordered comparison must CATCH a within-stream descending pair
    /// as received, not launder it with a blind global sort. Models the
    /// `limit=4` case's GET stream, which carries two entries (j9 & j69,
    /// both `GET /api/users 503 500`): returning them out of order
    /// (j69 before j9) must fail HARD; the correct ascending order passes
    /// and k-way merges into the global ascending sequence.
    #[test]
    fn ordered_entries_rejects_a_within_stream_descending_pair() {
        let get = serde_json::json!({"method": "GET", "status": "503", "took_ms": "500"});
        let delete = serde_json::json!({"method": "DELETE", "status": "503", "took_ms": "500"});
        let put = serde_json::json!({"method": "PUT", "status": "503", "took_ms": "500"});
        // Two single-entry streams (DELETE j29, PUT j49) interleave the
        // GET stream's two entries (j9 ts=100, j69 ts=400) in global order.
        let mk = |get_values: serde_json::Value| {
            serde_json::json!({
                "data": {
                    "resultType": "streams",
                    "result": [
                        {"stream": get.clone(), "values": get_values},
                        {"stream": delete.clone(), "values": [["200", "c"]]},
                        {"stream": put.clone(), "values": [["300", "d"]]},
                    ]
                }
            })
        };

        // Correct forward order within the GET stream (ascending ts).
        let ok_body = mk(serde_json::json!([["100", "a"], ["400", "b"]]));
        let merged = ordered_entries(&ok_body).expect("ascending streams must merge");
        let ts_order: Vec<i64> = merged.iter().map(|(_, ts, _)| *ts).collect();
        assert_eq!(
            ts_order,
            vec![100, 200, 300, 400],
            "k-way merge must yield the global ascending order"
        );
        // The GET stream (two entries) bookends the merged sequence.
        assert_eq!(merged[0].2, "a");
        assert_eq!(merged[3].2, "b");

        // Descending pair within the GET stream (j69 arrives before j9):
        // a blind global sort would launder this; the fix must reject it.
        let tripped = mk(serde_json::json!([["400", "b"], ["100", "a"]]));
        let err = ordered_entries(&tripped).expect_err("a within-stream descending pair must fail");
        assert!(
            err.to_string().contains("out of forward order"),
            "expected a forward-order violation, got: {err}"
        );
    }

    // ---------------------------------------------------------------
    // Issue #102: the Loki-push structured-metadata differential.
    // ---------------------------------------------------------------

    fn shipped_sm_fixture() -> SmFixture {
        let root = crate::engine::workspace_root();
        let raw =
            std::fs::read_to_string(root.join("test/fixtures").join(SM_FIXTURE_PATH)).unwrap();
        serde_json::from_str(&raw).unwrap()
    }

    fn shipped_sm_corpus() -> logs_sm_corpus::SmCorpus {
        logs_sm_corpus::generate(&logs_sm_corpus::SmCorpusSpec {
            base_ns: 1_700_000_000_000_000_000,
            run_id: "sm-fixture-check".to_string(),
        })
    }

    /// AC3 (id-lock, append-only): the SM fixture's case ids are exactly
    /// `SM_CASE_IDS`, in order. Mirrors the OTLP lock; independent of it.
    #[test]
    fn sm_fixture_cases_match_the_sm_case_ids_exactly() {
        let fixture = shipped_sm_fixture();
        let ids: Vec<&str> = fixture.cases.iter().map(|c| c.case_id.as_str()).collect();
        assert_eq!(ids, logs_sm_corpus::SM_CASE_IDS.to_vec());
    }

    /// Every SM case is `gated` (byte-exact, no informational downgrade) and
    /// run-scoped/substitutable.
    #[test]
    fn sm_fixture_cases_are_gated_and_run_scoped() {
        let fixture = shipped_sm_fixture();
        for case in &fixture.cases {
            assert_eq!(
                case.mode, "gated",
                "SM case {:?} must be gated",
                case.case_id
            );
            assert!(!case.construct.is_empty());
            assert!(
                case.query.contains(r#"run_id="{R}""#),
                "SM case {:?} is not run-scoped: {}",
                case.case_id,
                case.query
            );
            let rendered = case.query.replace("{R}", "e2e-sm-test");
            assert!(!rendered.contains("{R}"));
        }
    }

    /// Every SM case query PARSES as a log (streams) query and its pipeline
    /// COMPILES under the shipped evaluator — a fixture typo fails
    /// hermetically, not at nightly runtime.
    #[test]
    fn sm_fixture_queries_parse_and_their_pipelines_compile() {
        let fixture = shipped_sm_fixture();
        for case in &fixture.cases {
            let rendered = case.query.replace("{R}", "e2e-sm-test");
            let expr = pulsus_logql::parse(&rendered)
                .unwrap_or_else(|e| panic!("SM case {:?} does not parse: {e}", case.case_id));
            let pulsus_logql::Expr::Log(log) = expr else {
                panic!("SM case {:?} must be a log (streams) query", case.case_id);
            };
            pulsus_read::logql::pipeline::CompiledPipeline::compile(&log.pipeline).unwrap_or_else(
                |e| panic!("SM case {:?} pipeline does not compile: {e}", case.case_id),
            );
        }
    }

    /// Set comparisons are only well-defined unclipped: every SM case's
    /// expected set is non-empty and strictly below the fixture limit.
    #[test]
    fn sm_fixture_expected_sets_are_non_vacuous_and_below_the_limit() {
        let fixture = shipped_sm_fixture();
        let corpus = shipped_sm_corpus();
        for case in &fixture.cases {
            let expected = logs_sm_corpus::expected_case_result(&corpus, &case.case_id);
            let entries = set_entry_count(&expected);
            assert!(entries > 0, "SM case {:?} is vacuous", case.case_id);
            assert!(
                (entries as u32) < fixture.limit,
                "SM case {:?} has {entries} entries — not below limit {}",
                case.case_id,
                fixture.limit
            );
        }
    }

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn expected_set() -> ExpectedResult {
        let mut set = ExpectedResult::new();
        set.insert(
            labels(&[("stream", "a")]),
            BTreeSet::from([(1_i64, "one".to_string()), (2, "two".to_string())]),
        );
        set.insert(
            labels(&[("stream", "b")]),
            BTreeSet::from([(3, "three".to_string())]),
        );
        set
    }

    /// Issue #106: the on-timeout completeness diagnostic's core computes
    /// the correct per-store matched count and missing/extra symmetric
    /// difference from a partial store result — so the artifact CI reads
    /// when the nightly next fails is known to be right.
    #[test]
    fn completeness_set_diff_reports_matched_and_missing_and_extra() {
        let expected = expected_set();

        // pulsusdb: missing (b,3,"three"); carries an extra (a,9,"nine").
        let mut pulsus = ExpectedResult::new();
        pulsus.insert(
            labels(&[("stream", "a")]),
            BTreeSet::from([
                (1_i64, "one".to_string()),
                (2, "two".to_string()),
                (9, "nine".to_string()),
            ]),
        );
        let diff = completeness_set_diff(&pulsus, &expected);
        assert_eq!(diff.matched, 2, "two of the three expected entries present");
        assert_eq!(
            diff.missing,
            vec![(labels(&[("stream", "b")]), 3, "three".to_string())]
        );
        assert_eq!(
            diff.extra,
            vec![(labels(&[("stream", "a")]), 9, "nine".to_string())]
        );

        // oracle: still filling — only the first entry landed.
        let mut oracle = ExpectedResult::new();
        oracle.insert(
            labels(&[("stream", "a")]),
            BTreeSet::from([(1_i64, "one".to_string())]),
        );
        let odiff = completeness_set_diff(&oracle, &expected);
        assert_eq!(odiff.matched, 1);
        assert_eq!(odiff.missing.len(), 2, "(a,2,two) and (b,3,three) missing");
        assert!(odiff.extra.is_empty());
    }

    /// The fully-converged store has zero shortfall and matches the total.
    #[test]
    fn completeness_set_diff_is_empty_when_the_store_equals_expected() {
        let expected = expected_set();
        let diff = completeness_set_diff(&expected, &expected);
        assert_eq!(diff.matched, set_entry_count(&expected));
        assert!(diff.missing.is_empty());
        assert!(diff.extra.is_empty());
    }
}
