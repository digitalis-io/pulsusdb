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
//!
//! **Cluster variant (issue #204):** the reference log store ships only on
//! the single overlay, so under `Variant::Cluster` the differential runs
//! ORACLE-LESS (`oracle_present`) — every `PulsusDB(cluster) ==` the
//! by-construction corpus hard gate stays (the completeness gate and every
//! streams/metric/ordered/error case), only the reference-oracle comparison
//! is skipped. This proves the shard fan-out reassembles the full corpus
//! with no lost or duplicated rows; reference-semantics parity is
//! topology-invariant and inherited transitively from the single leg
//! (`single == corpus == oracle`). The same nightly `e2e-metrics-full`
//! matrix leg already exports the differential flag to both variants, so no
//! new job/leg is added.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::corpus::Scale;
use crate::harness::{
    classify_push_send, completeness_poll_timeout, poll_until, query_request_timeout,
};
use crate::logs_corpus::{
    self, ExpectedResult, LogCorpus, LogCorpusSpec, MetricMatrix, MetricVector, OrderedEntries,
    RangeGrid,
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
    /// `"metric_range"` = `/query_range` matrix comparison over Loki's
    /// sliding `(t-range, t]` windows (issue #227).
    #[serde(default)]
    kind: Option<String>,
    /// `metric_range` only: the request step in seconds.
    #[serde(default)]
    step_s: Option<u64>,
    /// `metric_range` only (issue #227): the case query's `[range]`
    /// selector width in seconds. The sliding window `(t - range, t]` and
    /// the `rate` divisor both track this, never `step_s`, so the
    /// by-construction expectation needs it explicitly; a hermetic test
    /// pins it against every range selector in the case's parse tree.
    #[serde(default)]
    range_s: Option<u64>,
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
) -> Result<Option<Result<reqwest::Response>>> {
    classify_push_send(
        ctx.http
            .post(format!("{}/v1/logs", ctx.collector_url))
            .json(payload)
            .send()
            .await,
    )
}

async fn push_log_corpus(ctx: &Ctx, corpus: &LogCorpus) -> Result<()> {
    let request = logs_corpus::to_otlp_export_request(corpus);
    let res = poll_until(
        COLLECTOR_READY_POLL_TIMEOUT,
        COLLECTOR_READY_POLL_INTERVAL,
        || post_otlp_logs(ctx, &request),
    )
    .await
    .context("collector otlp/v1/logs endpoint never accepted a connection")??;
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

/// Wraps [`ordered_entries`] so an order/shape violation dumps a repro
/// artifact (kind "order_violation") BEFORE bailing — #115 re-review of
/// #100: the bare `?` skipped the dump for exactly the failure class the
/// ordered gate exists to catch. Passing path: dump is never invoked.
fn ordered_entries_or_dump(
    store: &str,
    body: &serde_json::Value,
    case_id: &str,
    dump: &dyn Fn(&str, &str) -> Result<std::path::PathBuf>,
) -> Result<OrderedEntries> {
    match ordered_entries(body) {
        Ok(entries) => Ok(entries),
        Err(err) => {
            let path = dump(
                "order_violation",
                &format!("{store} response failed the ordered-entries gate: {err:#}"),
            )?;
            bail!(
                "case {case_id:?}: {store} response failed the forward-order/shape gate: \
                 {err:#} (repro {})",
                path.display()
            )
        }
    }
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
/// converted to epoch MILLISECONDS ([`logs_corpus::point_key_ms`]).
///
/// Milliseconds, not nanoseconds (issue #227): both stores stamp a matrix
/// point at millisecond resolution, and a start-anchored sliding grid no
/// longer lands on whole seconds, so `seconds × 1e9` would leave the f64
/// exact-integer range (ulp is 256 ns around 2^60) and round to a value a
/// nanosecond-keyed expectation could not predict. `seconds × 1e3` stays
/// far below 2^53 and is exact.
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
            let ts_ms = (ts_s * 1e3).round() as i64;
            points.insert(ts_ms, parse_value_str(&value[1])?);
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

/// Whether the live reference log store (oracle) is available for this run
/// (issue #204). The reference store ships only on the single overlay
/// (`deploy/e2e/compose.single.yaml`); the cluster overlay ships no
/// reference store, so under `Variant::Cluster` the differential runs
/// oracle-less: every `PulsusDB == corpus` hard gate is kept, only the
/// reference-oracle comparison is skipped. Reference parity is inherited
/// transitively from the single leg (`single == corpus == oracle`), which
/// is topology-invariant.
fn oracle_present(ctx: &Ctx) -> bool {
    ctx.variant == crate::scenarios::Variant::Single
}

/// One completeness probe target. `Pulsus` is always present and always at
/// index 0; `Oracle` is present only when [`oracle_present`] holds (issue
/// #204).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum CompletenessStore {
    Pulsus,
    Oracle,
}

impl CompletenessStore {
    fn label(self) -> &'static str {
        match self {
            Self::Pulsus => "pulsusdb",
            Self::Oracle => "oracle",
        }
    }
}

/// The completeness probe targets, in stable order (Pulsus always index 0).
/// On cluster (`with_oracle == false`) the list is PulsusDB-only, so the
/// oracle slot is never built, indexed, or queried — both `wait_for_completeness`
/// and `completeness_timeout_diagnostic` iterate this one selector (issue
/// #204).
fn completeness_stores(with_oracle: bool) -> &'static [CompletenessStore] {
    if with_oracle {
        &[CompletenessStore::Pulsus, CompletenessStore::Oracle]
    } else {
        &[CompletenessStore::Pulsus]
    }
}

/// Progress `reached` counter: the min of the stores actually queried —
/// PulsusDB alone when oracle-less, else `min(pulsus, oracle)` (issue #204).
fn completeness_reached(pulsus: usize, oracle: Option<usize>) -> usize {
    oracle.map_or(pulsus, |o| pulsus.min(o))
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
    oracle: Option<usize>,
) {
    let now = Instant::now();
    // Oracle-less (cluster) runs collapse the change-detection key to the
    // pulsus count alone, so an unchanged PulsusDB count still rate-limits
    // the line (issue #204).
    let oracle_key = oracle.unwrap_or(pulsus);
    let changed = last.get() != (pulsus, oracle_key);
    if changed || now.duration_since(last_log_at.get()) >= COMPLETENESS_PROGRESS_LOG_INTERVAL {
        let reached = completeness_reached(pulsus, oracle);
        let oracle_disp = match oracle {
            Some(o) => format!("oracle={o}"),
            None => "oracle=n/a (oracle-less)".to_string(),
        };
        println!(
            "pulsus-e2e:   {label} completeness: reached {reached}/{total}: pulsusdb={pulsus} \
             {oracle_disp}"
        );
        last.set((pulsus, oracle_key));
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
    // Oracle-less on cluster (issue #204): iterate the same selector the
    // poll uses, so `query_loki` is never invoked and no `"oracle"` key is
    // emitted when the reference store is absent.
    let with_oracle = oracle_present(ctx);
    let mut stores = serde_json::Map::new();
    for store in completeness_stores(with_oracle) {
        let body = match store {
            CompletenessStore::Pulsus => query_pulsus(ctx, q, window, limit, query_timeout).await,
            CompletenessStore::Oracle => query_loki(ctx, q, window, limit, query_timeout).await,
        };
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
        stores.insert(store.label().to_string(), entry);
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
    // Oracle-less on cluster (issue #204): the reference store ships only on
    // the single overlay, so the oracle probe/body/`sets[1]` is never built
    // or queried when it is absent.
    let with_oracle = oracle_present(ctx);
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
            // Pass 1 — validity gates on every present store's response,
            // before ANY set comparison (round-2 finding 2: comparing one
            // store first would keep retrying while the OTHER store's
            // response is already permanently invalid). The oracle body is
            // NOT constructed/queried when absent (issue #204).
            let mut bodies: Vec<(&str, serde_json::Value)> = Vec::new();
            for store in completeness_stores(with_oracle) {
                let body = match store {
                    CompletenessStore::Pulsus => {
                        query_pulsus(ctx, &q, window, limit, query_timeout).await?
                    }
                    CompletenessStore::Oracle => {
                        query_loki(ctx, &q, window, limit, query_timeout).await?
                    }
                };
                bodies.push((store.label(), body));
            }
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
            // `sets[1]` is dereferenced only when the oracle is present, so
            // an oracle-less (cluster) run indexes no oracle slot (issue #204).
            let oracle_matched =
                with_oracle.then(|| completeness_set_diff(&sets[1], &expected).matched);
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
                "run {:?} never reached completeness ({} records) on {}",
                corpus.run_id,
                corpus.total_records(),
                if with_oracle {
                    "both stores"
                } else {
                    "PulsusDB"
                },
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
        "metric_instant_ordered" => run_metric_instant_ordered_case(ctx, corpus, case).await,
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
    // Oracle-less on cluster (issue #204): the error witness is asserted on
    // the reference store only on the single overlay.
    let with_oracle = oracle_present(ctx);
    let pulsus_started = std::time::Instant::now();
    let (pulsus_status, pulsus_body) = fetch(ctx.url("/api/logs/v1/query")).await?;
    let pulsus_elapsed = pulsus_started.elapsed();
    let (oracle_status, oracle_body) = if with_oracle {
        let oracle_started = std::time::Instant::now();
        let (status, body) = fetch(format!("{}/loki/api/v1/query", ctx.loki_url)).await?;
        let oracle_elapsed = oracle_started.elapsed();
        // Per-case elapsed line (issue #92, see `run_streams_case`).
        println!(
            "pulsus-e2e: query {q:?} (case {:?}) pulsusdb {}ms oracle {}ms",
            case.case_id,
            pulsus_elapsed.as_millis(),
            oracle_elapsed.as_millis(),
        );
        (Some(status), body)
    } else {
        println!(
            "pulsus-e2e: query {q:?} (case {:?}) pulsusdb {}ms oracle n/a (oracle-less cluster)",
            case.case_id,
            pulsus_elapsed.as_millis(),
        );
        (None, String::new())
    };

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

    let mut checks: Vec<(&str, u16, &str)> =
        vec![("pulsusdb", pulsus_status, pulsus_body.as_str())];
    if let Some(status) = oracle_status {
        checks.push(("oracle", status, oracle_body.as_str()));
    }
    for (store, status, body) in checks {
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
    // Oracle-less on cluster (issue #204): asserted on the reference store
    // only on the single overlay.
    let with_oracle = oracle_present(ctx);
    let (pulsus_status, pulsus_body) = fetch(ctx.url("/api/logs/v1/query")).await?;
    let (oracle_status, oracle_body) = if with_oracle {
        let (status, body) = fetch(format!("{}/loki/api/v1/query", ctx.loki_url)).await?;
        (Some(status), body)
    } else {
        (None, String::new())
    };

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

    let mut checks: Vec<(&str, u16, &str)> =
        vec![("pulsusdb", pulsus_status, pulsus_body.as_str())];
    if let Some(status) = oracle_status {
        checks.push(("oracle", status, oracle_body.as_str()));
    }
    for (store, status, body) in checks {
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

    // Oracle-less on cluster (issue #204): PulsusDB stays hard-gated against
    // the corpus; the reference-vector comparison runs on the single overlay
    // only.
    let with_oracle = oracle_present(ctx);
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
    let (oracle_body, oracle_set) = if with_oracle {
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
        let oracle_set = vector_result_set(&oracle_body)?;
        (oracle_body, oracle_set)
    } else {
        println!(
            "pulsus-e2e: query {q:?} (case {:?}) pulsusdb {}ms oracle n/a (oracle-less cluster)",
            case.case_id,
            pulsus_elapsed.as_millis(),
        );
        (serde_json::Value::Null, MetricVector::new())
    };
    let pulsus_set = vector_result_set(&pulsus_body)?;

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
    if with_oracle {
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
    }
    Ok(())
}

/// The ordered (received-order) `(labels, value)` sequence of an INSTANT
/// vector response — the order-preserving analog of `vector_result_set`.
/// A terminal `sort`/`sort_desc` survives the encoder's label re-sort
/// (`preserve_vector_order`), so `data.result[]` arrives in value order.
fn ordered_vector(body: &serde_json::Value) -> Result<Vec<(BTreeMap<String, String>, f64)>> {
    let result_type = body["data"]["resultType"].as_str().unwrap_or_default();
    if result_type != "vector" {
        bail!("expected a vector result, got {result_type:?}: {body}");
    }
    let mut out = Vec::new();
    for sample in body["data"]["result"].as_array().into_iter().flatten() {
        out.push((labels_of(sample)?, parse_value_str(&sample["value"][1])?));
    }
    Ok(out)
}

/// One maximal run of equal-valued samples in a value-ordered instant
/// vector: the half-open index range `[start, end)` into the sequence it
/// was computed from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TieGroup {
    start: usize,
    end: usize,
}

/// Partition a value-ordered `(labels, value)` sequence into maximal runs
/// of ANCHOR-EQUAL value. Equality is [`approx_eq`] against the run's
/// FIRST value (its anchor), never against the previous element.
/// `approx_eq` is not transitive, so no partition of it is canonical;
/// this one is a deliberate choice and its consequences are stated:
///   * a CHAIN of near-equal values is deliberately SPLIT where an
///     element compares equal to its predecessor but not to the anchor —
///     a chained walk would merge them, and its result would depend on
///     where the walk started;
///   * the rule can conversely place in one run two elements that are
///     each close to the anchor but not to each other.
///
/// The runs are therefore ANCHOR-DEFINED over the PulsusDB sequence only.
/// They are NOT a tie partition the two stores agree on, and nothing here
/// asserts anything about the oracle's own boundaries.
/// Keyed on the VALUE only — nothing here knows a label name, so the rule
/// survives a corpus change and covers `sort_desc` unaltered.
fn tie_groups(seq: &[(BTreeMap<String, String>, f64)]) -> Vec<TieGroup> {
    let mut out = Vec::new();
    let mut start = 0usize;
    while start < seq.len() {
        let anchor = seq[start].1;
        let mut end = start + 1;
        while end < seq.len() && approx_eq(anchor, seq[end].1) {
            end += 1;
        }
        out.push(TieGroup { start, end });
        start = end;
    }
    out
}

/// Issue #406: the store-vs-store comparison for a terminal
/// `sort`/`sort_desc`. Order BY VALUE is asserted exactly; order WITHIN a
/// run of equal values is not asserted at all — see the `sort-tie-order`
/// entry in docs/benchmarks/logs-differential-ledger.md.
///
/// Three checks, in this order; the first to fail names itself:
/// 1. equal length;
/// 2. `approx_eq(pulsus[i].1, oracle[i].1)` at every index `i`;
/// 3. for every group of [`tie_groups`]`(pulsus)`, the label sets of
///    `pulsus[start..end]` and `oracle[start..end]` are the same MULTISET
///    (both sides sorted and compared).
///
/// The partition is taken from the PulsusDB side ONLY: it is the side
/// already hard-gated monotone in the sort direction and set-equal to the
/// by-construction corpus, and deriving a partition from each side would
/// add a boundary-disagreement failure mode that says nothing about
/// ordering. Check 2 independently pins the oracle's value at every
/// position to PulsusDB's, so the oracle's sequence is monotone in the
/// same direction to within the tolerance — no separate oracle-
/// monotonicity check is needed and none is added. Stated so it is not
/// read as more: an oracle whose own anchored partition differs can pass
/// this check.
///
/// **Float equality, decided explicitly.** Both the pointwise value check
/// and the run partition use [`approx_eq`] (relative 1e-9), as
/// `vectors_match` and `matrices_match` already do: the two stores compute
/// these values through entirely different machinery, so bit equality
/// across stores is not something any comparison in this file assumes.
/// Non-transitivity is handled by anchoring each run on its first element,
/// so the partition is a well-defined function of the PulsusDB sequence
/// alone — and that is all it is. The grouping is defined from PulsusDB's
/// anchored tolerance partition; it asserts nothing about where the
/// oracle's own run boundaries fall. An oracle sequence that partitions
/// differently under the same rule can pass, because pointwise
/// `approx_eq` can hold at every index while the oracle's internal
/// boundaries lie elsewhere. The comparison *declines to observe* oracle
/// boundaries; it does not establish that they coincide. What it does
/// establish is exactly the ledger's **Asserted** list and nothing past
/// it.
///
/// Direction-free by construction: nothing branches on `sort` vs
/// `sort_desc`, which is why both cases share one code path.
fn value_ordered_sequences_agree(
    pulsus: &[(BTreeMap<String, String>, f64)],
    oracle: &[(BTreeMap<String, String>, f64)],
) -> Result<(), String> {
    if pulsus.len() != oracle.len() {
        return Err(format!(
            "ordered sequence length diverged: pulsusdb {} vs oracle {} \
             (pulsusdb {pulsus:?} vs oracle {oracle:?})",
            pulsus.len(),
            oracle.len()
        ));
    }
    for (i, (p, o)) in pulsus.iter().zip(oracle.iter()).enumerate() {
        if !approx_eq(p.1, o.1) {
            return Err(format!(
                "value diverged at position {i}: pulsusdb {} vs oracle {} \
                 (pulsusdb {pulsus:?} vs oracle {oracle:?})",
                p.1, o.1
            ));
        }
    }
    for group in tie_groups(pulsus) {
        let mut pulsus_run: Vec<&BTreeMap<String, String>> = pulsus[group.start..group.end]
            .iter()
            .map(|(l, _)| l)
            .collect();
        let mut oracle_run: Vec<&BTreeMap<String, String>> = oracle[group.start..group.end]
            .iter()
            .map(|(l, _)| l)
            .collect();
        pulsus_run.sort();
        oracle_run.sort();
        if pulsus_run != oracle_run {
            return Err(format!(
                "equal-value run [{}, {}) holds different series: pulsusdb {pulsus_run:?} \
                 vs oracle {oracle_run:?} (pulsusdb {pulsus:?} vs oracle {oracle:?})",
                group.start, group.end
            ));
        }
    }
    Ok(())
}

/// Issue M8-LQ3 AC9: the `sort`/`sort_desc` VALUE-ORDER differential. A
/// terminal sort establishes the wire order of the instant vector, so
/// both stores must return the same series in the same value order. The
/// set-equal `expected_metric_vector` comparison is the direction-neutral
/// validity gate (dup-label hard-fail); the ordered sequence is then
/// asserted (a) monotone in the sort direction on PulsusDB and (b)
/// value-sequence-equal pointwise between the two stores, with the
/// entries of each equal-value run compared as an unordered multiset
/// (issue #406, ledger `sort-tie-order`). Kept separate from
/// `run_metric_instant_case` because that path normalizes to a set.
async fn run_metric_instant_ordered_case(
    ctx: &Ctx,
    corpus: &LogCorpus,
    case: &CaseRaw,
) -> Result<()> {
    let q = case.query.replace("{R}", &corpus.run_id);
    let expected = corpus.expected_metric_vector(&case.case_id);
    let descending = q.contains("sort_desc");
    let eval_ns = metric_eval_ns(corpus);
    let query_timeout = query_request_timeout(corpus.scale);
    println!(
        "pulsus-e2e:     case {:?} [{}] — {}: {} expected series, {} order",
        case.case_id,
        case.mode,
        case.construct,
        expected.len(),
        if descending {
            "descending"
        } else {
            "ascending"
        },
    );

    // Oracle-less on cluster (issue #204): the PulsusDB value-order gate is
    // unchanged; the cross-store value-order comparison runs on the
    // single overlay only.
    let with_oracle = oracle_present(ctx);
    let pulsus_body = query_instant(
        ctx,
        &ctx.url("/api/logs/v1/query"),
        &q,
        eval_ns,
        query_timeout,
    )
    .await?;
    let (oracle_body, oracle_ordered, oracle_set) = if with_oracle {
        let oracle_body = query_instant(
            ctx,
            &format!("{}/loki/api/v1/query", ctx.loki_url),
            &q,
            eval_ns,
            query_timeout,
        )
        .await?;
        let oracle_ordered = ordered_vector(&oracle_body)?;
        // Set-equal validity gate (dup-label hard-fail lives in vector_result_set).
        let oracle_set = vector_result_set(&oracle_body)?;
        (oracle_body, oracle_ordered, oracle_set)
    } else {
        (serde_json::Value::Null, Vec::new(), MetricVector::new())
    };

    let pulsus_ordered = ordered_vector(&pulsus_body)?;
    let pulsus_set = vector_result_set(&pulsus_body)?;

    let dump = |detail: &str| -> Result<std::path::PathBuf> {
        let artifact = serde_json::json!({
            "surface": "logs_metric_ordered",
            "case_id": case.case_id,
            "mode": case.mode,
            "kind": "metric_instant_ordered",
            "query": q,
            "eval_ns": eval_ns,
            "descending": descending,
            "expected_set": expected.iter().map(|(l, v)| serde_json::json!({"labels": l, "value": v})).collect::<Vec<_>>(),
            "pulsusdb_result": pulsus_body,
            "oracle_result": oracle_body,
            "detail": detail,
        });
        write_artifact(ctx, ARTIFACT_AREA, "metric-case-mismatch", &artifact)
    };

    if !vectors_match(&pulsus_set, &expected) {
        let path = dump(&format!(
            "pulsusdb vector diverged: got {pulsus_set:?}, expected {expected:?}"
        ))?;
        bail!(
            "case {:?}: pulsusdb diverged from the corpus expectation (repro {})",
            case.case_id,
            path.display()
        );
    }
    if with_oracle && !vectors_match(&oracle_set, &expected) {
        let path = dump(&format!(
            "oracle vector diverged: got {oracle_set:?}, expected {expected:?}"
        ))?;
        bail!(
            "case {:?}: oracle diverged from the corpus expectation (repro {})",
            case.case_id,
            path.display()
        );
    }
    // (a) PulsusDB's value sequence is monotone in the sort direction.
    let monotone = pulsus_ordered.windows(2).all(|w| {
        if descending {
            w[0].1 >= w[1].1
        } else {
            w[0].1 <= w[1].1
        }
    });
    if !monotone {
        let path = dump(&format!(
            "pulsusdb value sequence not monotone ({}): {pulsus_ordered:?}",
            if descending {
                "descending"
            } else {
                "ascending"
            }
        ))?;
        bail!(
            "case {:?}: pulsusdb result is not value-ordered (repro {})",
            case.case_id,
            path.display()
        );
    }
    // (b) The two stores agree on VALUE order (issue #406): pointwise on
    // value, and on the multiset of series occupying each equal-value run.
    // The arrangement inside a run is not asserted — ledger
    // `sort-tie-order`. Cross-store only, so skipped oracle-less on
    // cluster (issue #204).
    if with_oracle
        && let Err(reason) = value_ordered_sequences_agree(&pulsus_ordered, &oracle_ordered)
    {
        let path = dump(&reason)?;
        bail!(
            "case {:?}: the two stores disagree on value order (repro {})",
            case.case_id,
            path.display()
        );
    }
    Ok(())
}

/// Range metric case: both stores answer `/query_range`. PulsusDB is
/// hard-gated against the by-construction expectation, which encodes
/// Loki's SLIDING window contract (issue #227): the half-open window
/// `(t - range, t]` re-evaluated at every start-anchored step point
/// `{start + k·step ≤ end}`, overlapping when `range > step`, emitting no
/// point for an empty window, with `rate` divided by the `[range]`. The
/// oracle comparison keeps the standard gated/informational split plus its
/// anti-rot (no range case is ledgered as divergent any more — #227
/// resolved the one entry that was).
///
/// The oracle comparison REQUIRES `deploy/e2e/loki.yaml`'s
/// `split_queries_by_interval: 0` (issue #301): with the reference's
/// default the query-frontend floors the request `start` to a multiple of
/// `step` before its engine runs, so it answers on an epoch-aligned grid
/// and every start-anchored expectation misses. If this case turns red
/// with the oracle's timestamps all whole multiples of `step_s` while
/// PulsusDB's carry the request's sub-second offset, that limit is what
/// regressed — see the ledger's `frontend-step-alignment`.
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
    let range_s = case
        .range_s
        .with_context(|| format!("case {:?} is metric_range but has no range_s", case.case_id))?;
    let grid = RangeGrid {
        start_ns: window.start_ns,
        end_ns: window.end_ns,
        step_ns: step_s as i64 * 1_000_000_000,
        range_ns: range_s as i64 * 1_000_000_000,
    };
    let expected = corpus.expected_metric_matrix(&case.case_id, grid);
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
    // Oracle-less on cluster (issue #204): PulsusDB stays hard-gated against
    // the sliding corpus expectation; the reference-matrix comparison runs on
    // the single overlay only.
    let with_oracle = oracle_present(ctx);
    let pulsus_started = std::time::Instant::now();
    let pulsus_body = query_range(ctx.url("/api/logs/v1/query_range")).await?;
    let pulsus_elapsed = pulsus_started.elapsed();
    let (oracle_body, oracle_set) = if with_oracle {
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
        let oracle_set = matrix_result_set(&oracle_body)?;
        (oracle_body, oracle_set)
    } else {
        println!(
            "pulsus-e2e: query {q:?} (case {:?}) pulsusdb {}ms oracle n/a (oracle-less cluster)",
            case.case_id,
            pulsus_elapsed.as_millis(),
        );
        (serde_json::Value::Null, MetricMatrix::new())
    };
    let pulsus_set = matrix_result_set(&pulsus_body)?;

    let dump = |kind: &str, detail: &str| -> Result<std::path::PathBuf> {
        let artifact = serde_json::json!({
            "surface": "logs_metric_pipeline",
            "case_id": case.case_id,
            "mode": case.mode,
            "kind": kind,
            "query": q,
            "window": {
                "start_ns": window.start_ns,
                "end_ns": window.end_ns,
                "step_s": step_s,
                "range_s": range_s,
            },
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

    // PulsusDB vs the sliding by-construction expectation: ALWAYS hard.
    if !matrices_match(&pulsus_set, &expected) {
        let path = dump(
            "pulsus_vs_corpus",
            &format!("pulsusdb matrix diverged: got {pulsus_set:?}, expected {expected:?}"),
        )?;
        bail!(
            "case {:?}: pulsusdb diverged from the sliding-window corpus expectation (repro {})",
            case.case_id,
            path.display()
        );
    }
    if with_oracle {
        if !matrices_match(&oracle_set, &expected) {
            let path = dump("oracle_vs_corpus", "oracle range-window result diverged")?;
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
    //
    // Oracle-less on cluster (issue #204): the reference store is queried
    // and compared only on the single overlay; on cluster `loki_body` stays
    // null and `loki_set` empty (neither is consulted below the guard).
    let with_oracle = oracle_present(ctx);
    let pulsus_started = std::time::Instant::now();
    let pulsus_body = query_pulsus(ctx, &q, window, fixture.limit, query_timeout).await?;
    let pulsus_elapsed = pulsus_started.elapsed();
    let (loki_body, loki_set) = if with_oracle {
        let loki_started = std::time::Instant::now();
        let loki_body = query_loki(ctx, &q, window, fixture.limit, query_timeout).await?;
        let loki_elapsed = loki_started.elapsed();
        println!(
            "pulsus-e2e: query {q:?} (case {:?}) pulsusdb {}ms oracle {}ms",
            case.case_id,
            pulsus_elapsed.as_millis(),
            loki_elapsed.as_millis(),
        );
        let loki_set = result_set(&loki_body)?;
        (loki_body, loki_set)
    } else {
        println!(
            "pulsus-e2e: query {q:?} (case {:?}) pulsusdb {}ms oracle n/a (oracle-less cluster)",
            case.case_id,
            pulsus_elapsed.as_millis(),
        );
        (serde_json::Value::Null, ExpectedResult::new())
    };
    let pulsus_set = result_set(&pulsus_body)?;

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
    // top-K, not a set. Hard on every present store, even for informational
    // cases (it invalidates the comparison, not the semantics). The oracle
    // is skipped when absent (issue #204).
    let mut truncation_bodies: Vec<(&str, &serde_json::Value)> = vec![("pulsusdb", &pulsus_body)];
    if with_oracle {
        truncation_bodies.push(("oracle", &loki_body));
    }
    for (store, body) in truncation_bodies {
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
    // comparison and mask a real response-shaping bug. Hard on every
    // present store.
    let mut dup_sets: Vec<(&str, &serde_json::Value, &ExpectedResult)> =
        vec![("pulsusdb", &pulsus_body, &pulsus_set)];
    if with_oracle {
        dup_sets.push(("oracle", &loki_body, &loki_set));
    }
    for (store, body, set) in dup_sets {
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

    // Oracle vs the corpus expectation (== vs PulsusDB, transitively) —
    // skipped oracle-less on cluster (issue #204: reference parity is
    // inherited transitively from the single leg).
    if with_oracle {
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
    }

    // Placement discriminator (issue #109): scope is per-entry structured
    // metadata, NOT an indexed stream label — so a `{scope_name="…"}` STREAM
    // selector (not the `| scope_name=` pipeline filter compared above) matches
    // no stream on EITHER store. This is exactly what fails against the pre-#109
    // scope-as-stream-label behaviour; asserted on both stores so the
    // placement, not just the value, is proven identical.
    if case.case_id == "scope_structured_metadata" {
        let selector = format!(
            r#"{{scope_name="{}", {}="{}"}}"#,
            logs_corpus::SCOPE_WITNESS_NAME,
            logs_corpus::RUN_ATTR,
            corpus.run_id,
        );
        let pulsus_sel = query_pulsus(ctx, &selector, window, fixture.limit, query_timeout).await?;
        // The reference-store placement check runs only on the single
        // overlay; the PulsusDB placement gate is unchanged (issue #204).
        let mut sel_bodies: Vec<(&str, serde_json::Value)> = vec![("pulsusdb", pulsus_sel)];
        if with_oracle {
            let loki_sel = query_loki(ctx, &selector, window, fixture.limit, query_timeout).await?;
            sel_bodies.push(("oracle", loki_sel));
        }
        for (store, body) in &sel_bodies {
            let matched = raw_entry_count(body);
            if matched != 0 {
                let path = dump(
                    "scope_placement",
                    &format!(
                        "{store} matched {matched} entries for the {selector:?} STREAM selector — \
                         scope must be structured metadata, never an indexed label"
                    ),
                )?;
                bail!(
                    "case {:?}: {store} indexed a scope key as a stream label (repro {})",
                    case.case_id,
                    path.display()
                );
            }
        }
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

    // Oracle-less on cluster (issue #204): the reference store is queried
    // and compared only on the single overlay.
    let with_oracle = oracle_present(ctx);
    let pulsus_started = std::time::Instant::now();
    let pulsus_body = query_pulsus(ctx, &q, window, limit, query_timeout).await?;
    let pulsus_elapsed = pulsus_started.elapsed();
    let loki_body = if with_oracle {
        let loki_started = std::time::Instant::now();
        let loki_body = query_loki(ctx, &q, window, limit, query_timeout).await?;
        let loki_elapsed = loki_started.elapsed();
        println!(
            "pulsus-e2e: query {q:?} (case {:?}) pulsusdb {}ms oracle {}ms",
            case.case_id,
            pulsus_elapsed.as_millis(),
            loki_elapsed.as_millis(),
        );
        loki_body
    } else {
        println!(
            "pulsus-e2e: query {q:?} (case {:?}) pulsusdb {}ms oracle n/a (oracle-less cluster)",
            case.case_id,
            pulsus_elapsed.as_millis(),
        );
        serde_json::Value::Null
    };

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
    // The oracle is skipped when absent (issue #204).
    let mut validity_bodies: Vec<(&str, &serde_json::Value)> = vec![("pulsusdb", &pulsus_body)];
    if with_oracle {
        validity_bodies.push(("oracle", &loki_body));
    }
    for (store, body) in validity_bodies {
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
        let entries = ordered_entries_or_dump(store, body, &case.case_id, &dump)?;
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
    let pulsus_entries = ordered_entries_or_dump("pulsusdb", &pulsus_body, &case.case_id, &dump)?;
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

    // Oracle vs the corpus ordered prefix (== vs PulsusDB, transitively) —
    // skipped oracle-less on cluster (issue #204).
    if with_oracle {
        let loki_entries = ordered_entries_or_dump("oracle", &loki_body, &case.case_id, &dump)?;
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

/// One `POST {url}` of one Loki JSON push body, routed through
/// [`classify_push_send`] (issue #105): `Ok(Some(Ok(response)))` once the
/// request reaches the store at all (any HTTP response — the caller checks
/// the status). A connect-phase `Err` triggers a [`poll_until`] retry of the
/// identical body (safe: zero bytes reached the server), but a post-connect
/// failure *after* the server may have ingested the body maps to
/// `Ok(Some(Err(_)))` and fails fast, so the idempotency guard is in place —
/// the identical body is never resent once the connection was established.
/// Pinned end-to-end through the real [`push_sm_corpus`] call site by
/// `sm_push_lane_cannot_replay_the_corpus_on_an_ambiguous_post_ingest_failure`
/// (issue #102), not just the classifier in isolation (issue #105).
async fn push_loki_json(
    ctx: &Ctx,
    url: &str,
    body: &serde_json::Value,
) -> Result<Option<Result<reqwest::Response>>> {
    classify_push_send(ctx.http.post(url).json(body).send().await)
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
            .with_context(|| format!("{store} loki push endpoint never accepted a connection"))??;
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
            // The SM lane is single-only, so the oracle is always present.
            let oracle_matched = completeness_set_diff(&sets[1], &expected).matched;
            log_completeness_progress(
                &progress,
                &last_log_at,
                "sm logs",
                expected_total,
                pulsus_matched,
                Some(oracle_matched),
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
    use std::ops::ControlFlow;

    use super::*;
    use crate::logs_corpus::{CASE_IDS, METRIC_CASE_IDS};

    /// The committed exclusion list (plan v3 delta 5): every case starts
    /// gated; a case id appears here ONLY after a triaged divergence is
    /// recorded in docs/benchmarks/logs-differential-ledger.md. Update
    /// deliberately, with the ledger entry, never as a quick fix for a
    /// red run.
    ///
    /// **EMPTY again since issue #227.** The one seeded entry
    /// (`metric_rate_tumbling`, plus the four issue-#91 range
    /// vector-matching cases that shared its ledger id) recorded the
    /// tumbling-vs-sliding range-window divergence. #227 replaced the
    /// tumbling buckets with Loki's sliding `(t - range, t]` windows, so
    /// that divergence no longer exists: the cases are re-gated (the very
    /// move `run_metric_range_case`'s stale-exclusion anti-rot demands
    /// once an informational case starts matching the oracle) and the rate
    /// case is renamed `metric_rate_sliding`. The ledger entry stays,
    /// marked RESOLVED.
    const INFORMATIONAL_CASE_IDS: &[&str] = &[];

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
                "metric_instant"
                | "metric_instant_ordered"
                | "metric_error"
                | "metric_match_error" => {
                    assert!(case.step_s.is_none(), "{}", case.case_id);
                    assert!(case.range_s.is_none(), "{}", case.case_id);
                }
                // Issue #227: a range case declares BOTH the request step
                // and the `[range]` width — the sliding window and the
                // `rate` divisor track the latter, never the former.
                "metric_range" => {
                    assert!(case.step_s.is_some(), "{}", case.case_id);
                    assert!(case.range_s.is_some(), "{}", case.case_id);
                }
                other => panic!("metric case {:?} has kind {other:?}", case.case_id),
            }
        }
    }

    /// Issue #227: the fixture's declared `range_s` is EXACTLY the
    /// `[range]` every leaf of the case's query carries. The sliding
    /// expectation is computed from `range_s`, so a drift between the two
    /// would silently measure a window the query never asked for.
    #[test]
    fn shipped_range_cases_declare_the_query_selector_range() {
        let fixture = shipped_fixture();
        let corpus = shipped_corpus(&fixture, fixture.ci.record_count);
        for case in fixture.cases.iter().filter(|c| c.kind() == "metric_range") {
            let rendered = case.query.replace("{R}", &corpus.run_id);
            let expr = pulsus_logql::parse(&rendered).expect("parse");
            let params = hermetic_params(case, &corpus);
            let plan =
                pulsus_read::logql::plan(&expr, &params, &hermetic_plan_ctx()).expect("plan");
            let leaves: Vec<&pulsus_read::logql::MetricPlan> = match &plan {
                pulsus_read::logql::Plan::Metric(mp) => vec![mp],
                pulsus_read::logql::Plan::MetricBinary(node) => node.leaves(),
                pulsus_read::logql::Plan::Streams(_) => {
                    panic!("range case {:?} planned as streams", case.case_id)
                }
            };
            let declared =
                case.range_s.expect("a metric_range case declares range_s") as i64 * 1_000_000_000;
            for leaf in leaves {
                assert_eq!(
                    leaf.range_ns.get(),
                    declared,
                    "case {:?}: fixture range_s does not match the query's [range]",
                    case.case_id
                );
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

    /// The shipped fixture's prose must not promise a flattened label the
    /// corpus no longer emits (#259 reopen). `construct` is the only
    /// place a case's expected response is written in words; prose rot
    /// here is invisible to every other gate, and it is what would tempt
    /// the next reader to put `emptyattr=""` back into
    /// `logs_corpus::SCOPE_WITNESS_ATTRS`.
    #[test]
    fn the_scope_case_construct_does_not_promise_an_empty_attribute() {
        let fixture = shipped_fixture();
        let case = fixture
            .cases
            .iter()
            .find(|c| c.case_id == "scope_structured_metadata")
            .expect("the scope case is shipped");
        assert!(
            !case.construct.contains("emptyattr"),
            "case {:?} still promises `emptyattr`, which the corpus no longer \
             emits: the e2e oracle (grafana/loki:3.4.2) keeps an empty-valued \
             attribute while the pinned reference (grafana/loki:3.7.4) and \
             PulsusDB drop it. See docs/benchmarks/logs-differential-ledger.md \
             `empty-value-oracle-version-skew`.\n{}",
            case.case_id,
            case.construct
        );
    }

    /// The oracle-version skew is recorded where the divergence
    /// discipline says it is recorded, and the entry names the artifacts
    /// it governs — the corpus constant, the guard test, and both image
    /// digests — so the close condition cannot be read without them
    /// (#259 reopen).
    #[test]
    fn the_oracle_version_skew_is_recorded_in_the_committed_ledger() {
        let ledger_path =
            crate::engine::workspace_root().join("docs/benchmarks/logs-differential-ledger.md");
        let ledger = std::fs::read_to_string(&ledger_path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", ledger_path.display()));
        for needle in [
            "empty-value-oracle-version-skew",
            "SCOPE_WITNESS_ATTRS",
            "sha256:58a6c186",
            "sha256:87f0a067",
            "the_shared_corpus_carries_no_empty_valued_attribute",
        ] {
            assert!(ledger.contains(needle), "ledger is missing {needle:?}");
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
            "metric_instant" | "metric_instant_ordered" | "metric_error" | "metric_match_error" => {
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

    /// The sliding evaluation grid a committed `metric_range` case runs on
    /// (issue #227) — the live request window, the fixture's `step_s`, and
    /// the fixture's `[range]` width. Mirrors `run_metric_range_case`, so
    /// the hermetic gates below evaluate exactly the live grid.
    fn case_grid(case: &CaseRaw, corpus: &LogCorpus) -> RangeGrid {
        let window = query_window(corpus);
        RangeGrid {
            start_ns: window.start_ns,
            end_ns: window.end_ns,
            step_ns: case.step_s.expect("a metric_range case declares step_s") as i64
                * 1_000_000_000,
            range_ns: case.range_s.expect("a metric_range case declares range_s") as i64
                * 1_000_000_000,
        }
    }

    /// The evaluation window for a planned metric leaf — the e2e mirror of
    /// `pulsus-read`'s private `metric_plan_window` (issue #227). A range
    /// plan carries the START-ANCHORED grid start and the validated
    /// `[range]` width, never the range-widened scan `start_ns`.
    fn hermetic_window(mp: &pulsus_read::logql::MetricPlan) -> pulsus_read::logql::ClientWindow {
        match mp.step_ns {
            Some(step_ns) => pulsus_read::logql::ClientWindow::Range {
                grid_start_ns: mp.grid_start_ns,
                end_ns: mp.end_ns,
                step_ns,
                range_ns: mp.range_ns,
                // Issue #343: the plan's bounds are already offset-shifted.
                offset_ns: mp.offset_ns,
            },
            None => pulsus_read::logql::ClientWindow::Instant {
                start_ns: mp.grid_start_ns,
                end_ns: mp.end_ns,
            },
        }
    }

    /// Normalizes a shipped-engine `Matrix` result into the comparison
    /// shape `matrix_result_set` builds from the wire — same label-set key,
    /// same millisecond point key (issue #227), so a hermetic evaluation
    /// and a live response are directly comparable.
    fn matrix_of_result(result: pulsus_read::logql::QueryResult) -> MetricMatrix {
        let pulsus_read::logql::QueryResult::Matrix(series) = result else {
            panic!("a range case must evaluate to a matrix");
        };
        let mut out = MetricMatrix::new();
        for s in series {
            let labels: BTreeMap<String, String> = s.labels.into_iter().collect();
            let points: BTreeMap<i64, f64> = s
                .points
                .into_iter()
                .map(|(ts_ns, v)| (logs_corpus::point_key_ms(ts_ns), v))
                .collect();
            assert!(
                out.insert(labels, points).is_none(),
                "duplicate label set in the evaluated matrix"
            );
        }
        out
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
                    "metric_instant" | "metric_instant_ordered" => {
                        let expected = corpus.expected_metric_vector(&case.case_id);
                        assert!(
                            !expected.is_empty(),
                            "case {:?} is vacuous at record_count {count}",
                            case.case_id
                        );
                    }
                    "metric_range" => {
                        let expected =
                            corpus.expected_metric_matrix(&case.case_id, case_grid(case, &corpus));
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
            .filter(|c| matches!(c.kind(), "metric_instant" | "metric_instant_ordered"))
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

    /// Issue #227, the RANGE mirror of the test above and the strongest
    /// hermetic evidence for the sliding by-construction expectation: every
    /// committed `metric_range` case's expected MATRIX equals what the
    /// shipped sliding evaluator produces over the same generated bodies.
    /// If `expected_metric_matrix` and `RangeSlideState` ever disagree on
    /// the start-anchored grid, the half-open `(t-range, t]` window, the
    /// empty-window gap rule, or the `rate` divisor, this fails on every PR
    /// — no compose stack required.
    #[test]
    fn shipped_metric_range_expectations_agree_with_the_shipped_evaluator() {
        let fixture = shipped_fixture();
        let corpus = shipped_corpus(&fixture, fixture.ci.record_count);
        let mut checked = 0usize;
        for case in fixture.cases.iter().filter(|c| c.kind() == "metric_range") {
            let evaluated = evaluate_case_matrix(&corpus, case);
            let expected = corpus.expected_metric_matrix(&case.case_id, case_grid(case, &corpus));
            assert_matrices_agree(&evaluated, &expected, &case.case_id);
            checked += 1;
        }
        assert!(checked > 0, "the fixture must carry range cases");
    }

    /// Issue #227 AC4/overlap, hermetic: with `range = 3 x step` the
    /// windows OVERLAP — one record lands in three consecutive step
    /// windows, which the retired tumbling bucketing could not express —
    /// and the shipped engine agrees with the by-construction expectation
    /// point for point. Also pins the two other sliding rules the
    /// committed `range == step` cases cannot exercise: empty windows are
    /// GAPS (the grid is far wider than the data), and the value tracks the
    /// `[range]`, so `rate(...[3m]) != rate(...[1m])` on the same grid.
    #[test]
    fn a_range_wider_than_the_step_overlaps_gaps_and_rescales_like_the_engine() {
        let fixture = shipped_fixture();
        let corpus = shipped_corpus(&fixture, fixture.ci.record_count);
        let window = query_window(&corpus);
        let step_ns = 60 * 1_000_000_000;
        let wide = RangeGrid {
            start_ns: window.start_ns,
            end_ns: window.end_ns,
            step_ns,
            range_ns: 3 * step_ns,
        };
        let expected = corpus.expected_metric_matrix("metric_rate_sliding", wide);

        // 1. The shipped sliding evaluator produces exactly this matrix.
        let rendered = format!(
            r#"rate({{run_id="{}", service_name="{}"}}[3m])"#,
            corpus.run_id,
            logs_corpus::SVC_JSON
        );
        let expr = pulsus_logql::parse(&rendered).expect("parse");
        let params = pulsus_read::logql::QueryParams {
            spec: pulsus_read::logql::QuerySpec::Range {
                start_ns: wide.start_ns,
                end_ns: wide.end_ns,
                step_ns: wide.step_ns as u64,
            },
            limit: 1000,
            direction: pulsus_read::logql::Direction::Forward,
        };
        let plan = pulsus_read::logql::plan(&expr, &params, &hermetic_plan_ctx()).expect("plan");
        let pulsus_read::logql::Plan::Metric(mp) = &plan else {
            panic!("a bare rate() plans as a single metric leaf");
        };
        let evaluated = matrix_of_result(evaluate_leaf_hermetically(
            &corpus,
            mp,
            logs_corpus::SVC_JSON,
        ));
        assert_matrices_agree(&evaluated, &expected, "metric_rate_sliding[3m]");

        // 2. Overlap: summed over the emitted points, the records are
        // counted MORE times than they exist (each sits in several
        // windows) — impossible under non-overlapping tumbling buckets.
        let points = expected
            .values()
            .next()
            .expect("the wide-range matrix has one series");
        let range_seconds = wide.range_ns as f64 / 1e9;
        let counted: f64 = points.values().map(|v| v * range_seconds).sum();
        let records = corpus
            .records
            .iter()
            .filter(|r| r.service == logs_corpus::SVC_JSON)
            .count() as f64;
        assert!(
            counted > records,
            "range > step must count records in several windows: {counted} vs {records}"
        );

        // 3. Gaps: the grid is far wider than the data, so most step
        // points emit nothing at all rather than a zero.
        assert!(
            points.len() < wide.points().len(),
            "empty windows must be gaps: {} points on a {}-point grid",
            points.len(),
            wide.points().len()
        );

        // 4. The `[range]` — not the step — sets the window and the
        // divisor: the same grid at `[1m]` differs.
        let narrow = RangeGrid {
            range_ns: step_ns,
            ..wide
        };
        let narrow_expected = corpus.expected_metric_matrix("metric_rate_sliding", narrow);
        assert_ne!(
            narrow_expected, expected,
            "rate(...[1m]) and rate(...[3m]) must differ on the same grid"
        );
    }

    /// Evaluates a committed `metric_range` case through the shipped
    /// planner + sliding evaluator over the generated corpus.
    fn evaluate_case_matrix(corpus: &LogCorpus, case: &CaseRaw) -> MetricMatrix {
        let rendered = case.query.replace("{R}", &corpus.run_id);
        let expr = pulsus_logql::parse(&rendered).expect("parse");
        let service = first_selector_service(&expr);
        let params = hermetic_params(case, corpus);
        let plan = pulsus_read::logql::plan(&expr, &params, &hermetic_plan_ctx()).expect("plan");
        let result = match &plan {
            pulsus_read::logql::Plan::Metric(mp) => {
                evaluate_leaf_hermetically(corpus, mp, &service)
            }
            pulsus_read::logql::Plan::MetricBinary(node) => {
                evaluate_node_hermetically(corpus, node, &service)
            }
            pulsus_read::logql::Plan::Streams(_) => {
                panic!("range case {:?} planned as streams", case.case_id)
            }
        };
        matrix_of_result(result)
    }

    /// Series-for-series, point-for-point matrix equality with a diffable
    /// message (`matrices_match` is a bare bool for the live lane).
    fn assert_matrices_agree(got: &MetricMatrix, expected: &MetricMatrix, case_id: &str) {
        assert_eq!(
            got.keys().collect::<Vec<_>>(),
            expected.keys().collect::<Vec<_>>(),
            "case {case_id:?}: label sets diverged"
        );
        for (labels, points) in expected {
            let got_points = &got[labels];
            assert_eq!(
                got_points.keys().collect::<Vec<_>>(),
                points.keys().collect::<Vec<_>>(),
                "case {case_id:?}: step points diverged for {labels:?}"
            );
            for (ts_ms, v) in points {
                assert!(
                    approx_eq(got_points[ts_ms], *v),
                    "case {case_id:?}: value diverged at {ts_ms} for {labels:?}: {} vs {v}",
                    got_points[ts_ms]
                );
            }
        }
    }

    /// Issue M8-LQ3 AC9 (hermetic half): the `metric_sort_order` case's
    /// SHIPPED evaluator output is in the pinned value order `b, a, c`
    /// (ascending by value, the equal-value `a`/`c` tie broken by label
    /// ascending). The live lane asserts the two stores agree on the
    /// VALUE order only — the arrangement inside an equal-value run is
    /// not asserted there (issue #406, ledger `sort-tie-order`). This
    /// test observes PulsusDB only, so it is evidence about our own
    /// determinism and never about the reference; the ordered engine
    /// output is pinned here so a reducer/encoder regression fails
    /// hermetically every PR.
    #[test]
    fn shipped_sort_case_evaluates_in_the_pinned_value_order() {
        assert_eq!(
            hermetic_ordered_grps("metric_sort_order"),
            vec![
                ("b".to_string(), 1.0),
                ("a".to_string(), 5.0),
                ("c".to_string(), 5.0),
            ],
            "sort must order ascending by value with the a/c tie broken by label ascending"
        );
    }

    /// Issue M8-LQ3 (code review round 2, test gap): the `sort_desc` mirror
    /// of the AC9 hermetic gate — the SHIPPED evaluator output is in the
    /// pinned DESCENDING value order `a, c, b` (the equal-value `a`/`c` tie
    /// still broken by label ascending). Covers the sort_desc handler/
    /// encoder path independently every PR. This test observes PulsusDB
    /// only, so it is evidence about our own determinism and never about
    /// the reference; the live lane asserts the two stores agree on the
    /// VALUE order only (issue #406, ledger `sort-tie-order`).
    #[test]
    fn shipped_sort_desc_case_evaluates_in_the_pinned_value_order() {
        assert_eq!(
            hermetic_ordered_grps("metric_sort_desc_order"),
            vec![
                ("a".to_string(), 5.0),
                ("c".to_string(), 5.0),
                ("b".to_string(), 1.0),
            ],
            "sort_desc must order descending by value with the a/c tie broken by label ascending"
        );
    }

    /// Shared driver for the sort/sort_desc hermetic order gates: evaluates
    /// the committed ordered case `case_id` through the shipped engine and
    /// returns the `(grp, value)` sequence in the engine's emitted order.
    fn hermetic_ordered_grps(case_id: &str) -> Vec<(String, f64)> {
        let fixture = shipped_fixture();
        let corpus = shipped_corpus(&fixture, fixture.ci.record_count);
        let case = fixture
            .cases
            .iter()
            .find(|c| c.case_id == case_id)
            .unwrap_or_else(|| panic!("the {case_id} case is committed"));
        let rendered = case.query.replace("{R}", &corpus.run_id);
        let expr = pulsus_logql::parse(&rendered).expect("parse");
        let service = first_selector_service(&expr);
        let params = hermetic_params(case, &corpus);
        let plan = pulsus_read::logql::plan(&expr, &params, &hermetic_plan_ctx()).expect("plan");
        let pulsus_read::logql::Plan::Metric(mp) = &plan else {
            panic!("the sort case plans as a single metric leaf");
        };
        let pulsus_read::logql::QueryResult::Vector(samples) =
            evaluate_leaf_hermetically(&corpus, mp, &service)
        else {
            panic!("sort case did not evaluate to a vector");
        };
        samples
            .into_iter()
            .map(|s| {
                let grp = s
                    .labels
                    .iter()
                    .find(|(k, _)| k == "grp")
                    .map(|(_, v)| v.clone())
                    .expect("grp label");
                (grp, s.value)
            })
            .collect()
    }

    // -----------------------------------------------------------------
    // Issue #406 — the relaxed cross-store order comparison, its anchor
    // rule, and the terminality of every committed `sort` case.
    // -----------------------------------------------------------------

    /// Builds an ordered `(labels, value)` sequence over a single `grp`
    /// label. The label NAME is a parameter of the fixture, never of the
    /// comparison: `tie_groups` keys on the value alone.
    fn grp_seq(rows: &[(&str, f64)]) -> Vec<(BTreeMap<String, String>, f64)> {
        rows.iter()
            .map(|(g, v)| (BTreeMap::from([("grp".to_string(), (*g).to_string())]), *v))
            .collect()
    }

    fn logs_rs_source() -> String {
        let path = crate::engine::workspace_root().join("e2e/src/logs.rs");
        std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()))
    }

    /// The `///` block immediately above `fn <fn_name>(`, joined into one
    /// line so a phrase that wraps across lines is still found. The first
    /// occurrence is the DEFINITION: a test naming the function writes it
    /// without the `fn ` prefix.
    fn doc_comment_above(src: &str, fn_name: &str) -> String {
        let lines: Vec<&str> = src.lines().collect();
        let needle = format!("fn {fn_name}(");
        let at = lines
            .iter()
            .position(|l| l.trim_start().starts_with(&needle))
            .unwrap_or_else(|| panic!("{fn_name} is defined in e2e/src/logs.rs"));
        let mut doc = Vec::new();
        for line in lines[..at].iter().rev() {
            let trimmed = line.trim_start();
            if let Some(rest) = trimmed.strip_prefix("///") {
                doc.push(rest.trim().to_string());
                continue;
            }
            if trimmed.starts_with("#[") {
                continue;
            }
            break;
        }
        doc.reverse();
        doc.join(" ")
    }

    /// AC1: the relaxed comparison is the ONLY cross-store order check on
    /// this path. Behavioural half — it accepts the arrangement the
    /// deleted whole-sequence equality rejected; source half — that
    /// equality no longer appears anywhere in the file.
    #[test]
    fn value_ordered_agreement_is_the_only_cross_store_order_check() {
        let pulsus = grp_seq(&[("b", 1.0), ("a", 5.0), ("c", 5.0)]);
        let oracle = grp_seq(&[("b", 1.0), ("c", 5.0), ("a", 5.0)]);
        assert!(value_ordered_sequences_agree(&pulsus, &oracle).is_ok());

        // ASSEMBLED, never written out: a literal here would satisfy the
        // very search this test performs and the check would pass on its
        // own text.
        let deleted = ["pulsus_seq", "!=", "oracle_seq"].join(" ");
        let src = logs_rs_source();
        assert!(
            !src.contains(&deleted),
            "the deleted whole-label-sequence equality is back in e2e/src/logs.rs"
        );
        assert!(
            src.contains("value_ordered_sequences_agree(&pulsus_ordered, &oracle_ordered)"),
            "the ordered case no longer calls the relaxed comparison"
        );
    }

    /// AC2: the exact sequences from the `e2e-metrics-full` failure of
    /// run 31439057683 — the `a`/`c` tie arrives in the other order at the
    /// oracle, both stores ascending by value — are now accepted.
    #[test]
    fn value_ordered_agreement_accepts_the_run_31439057683_tie_reordering() {
        let pulsus = grp_seq(&[("b", 1.0), ("a", 5.0), ("c", 5.0)]);
        let oracle = grp_seq(&[("b", 1.0), ("c", 5.0), ("a", 5.0)]);
        assert_eq!(value_ordered_sequences_agree(&pulsus, &oracle), Ok(()));
    }

    /// AC3: the tie group sitting on the wrong side of the untied entry
    /// is still a value-order divergence.
    #[test]
    fn value_ordered_agreement_rejects_a_tie_group_at_the_wrong_position() {
        let pulsus = grp_seq(&[("b", 1.0), ("a", 5.0), ("c", 5.0)]);
        let oracle = grp_seq(&[("a", 5.0), ("c", 5.0), ("b", 1.0)]);
        let err = value_ordered_sequences_agree(&pulsus, &oracle).expect_err("must reject");
        assert!(err.contains("position 0"), "{err}");
    }

    /// AC4: a wrong VALUE at a position whose labels and monotonicity are
    /// both fine is rejected.
    #[test]
    fn value_ordered_agreement_rejects_a_wrong_value() {
        let pulsus = grp_seq(&[("b", 1.0), ("a", 5.0), ("c", 5.0)]);
        let oracle = grp_seq(&[("b", 1.0), ("a", 5.0), ("c", 7.0)]);
        let err = value_ordered_sequences_agree(&pulsus, &oracle).expect_err("must reject");
        assert!(err.contains("position 2"), "{err}");
    }

    /// AC5: the right multiset in fully reversed value order is rejected —
    /// the relaxation frees the arrangement inside a run, never the
    /// direction.
    #[test]
    fn value_ordered_agreement_rejects_a_fully_reversed_value_order() {
        let pulsus = grp_seq(&[("b", 1.0), ("a", 5.0), ("c", 5.0)]);
        let oracle = grp_seq(&[("c", 5.0), ("a", 5.0), ("b", 1.0)]);
        assert!(value_ordered_sequences_agree(&pulsus, &oracle).is_err());
    }

    /// AC6 (first half): a series the oracle never returned is rejected.
    #[test]
    fn value_ordered_agreement_rejects_a_missing_entry() {
        let pulsus = grp_seq(&[("b", 1.0), ("a", 5.0), ("c", 5.0)]);
        let oracle = grp_seq(&[("b", 1.0), ("a", 5.0)]);
        let err = value_ordered_sequences_agree(&pulsus, &oracle).expect_err("must reject");
        assert!(err.contains("length diverged"), "{err}");
    }

    /// AC6 (second half): a series only the oracle returned is rejected.
    #[test]
    fn value_ordered_agreement_rejects_an_extra_entry() {
        let pulsus = grp_seq(&[("b", 1.0), ("a", 5.0), ("c", 5.0)]);
        let oracle = grp_seq(&[("b", 1.0), ("a", 5.0), ("c", 5.0), ("d", 5.0)]);
        let err = value_ordered_sequences_agree(&pulsus, &oracle).expect_err("must reject");
        assert!(err.contains("length diverged"), "{err}");
    }

    /// AC7, the sharpest form of AC6: same length, every value equal, one
    /// label set inside the tie group SUBSTITUTED. Nothing but the
    /// multiset check can see this.
    #[test]
    fn value_ordered_agreement_rejects_a_substituted_label_set_inside_a_tie_group() {
        let pulsus = grp_seq(&[("b", 1.0), ("a", 5.0), ("c", 5.0)]);
        let oracle = grp_seq(&[("b", 1.0), ("a", 5.0), ("d", 5.0)]);
        let err = value_ordered_sequences_agree(&pulsus, &oracle).expect_err("must reject");
        assert!(err.contains("equal-value run [1, 3)"), "{err}");
    }

    /// AC8: the freedom is PER RUN, not global — a swap that moves an
    /// entry across a run boundary is rejected.
    #[test]
    fn value_ordered_agreement_rejects_a_swap_across_two_tie_groups() {
        let pulsus = grp_seq(&[("x", 1.0), ("y", 1.0), ("z", 5.0), ("w", 5.0)]);
        let oracle = grp_seq(&[("x", 1.0), ("z", 5.0), ("y", 1.0), ("w", 5.0)]);
        assert!(value_ordered_sequences_agree(&pulsus, &oracle).is_err());
    }

    /// AC9: `tie_groups` partitions on the VALUE and never on a label —
    /// the label names here are deliberately not the corpus's `a`/`b`/`c`.
    #[test]
    fn tie_groups_partitions_by_value_and_never_by_label() {
        assert_eq!(
            tie_groups(&grp_seq(&[("p", 1.0), ("q", 5.0), ("r", 5.0)])),
            vec![TieGroup { start: 0, end: 1 }, TieGroup { start: 1, end: 3 },],
        );
        assert_eq!(
            tie_groups(&grp_seq(&[("r", 5.0), ("q", 5.0), ("p", 5.0)])),
            vec![TieGroup { start: 0, end: 3 }],
        );
        assert_eq!(
            tie_groups(&grp_seq(&[("p", 1.0), ("q", 2.0), ("r", 3.0)])),
            vec![
                TieGroup { start: 0, end: 1 },
                TieGroup { start: 1, end: 2 },
                TieGroup { start: 2, end: 3 },
            ],
        );
        assert_eq!(tie_groups(&grp_seq(&[])), vec![]);
        // Inside the `approx_eq` tolerance: one run. Outside it: two.
        assert_eq!(
            tie_groups(&grp_seq(&[("p", 1.0), ("q", 1.0 + 5e-10)])),
            vec![TieGroup { start: 0, end: 2 }],
        );
        assert_eq!(
            tie_groups(&grp_seq(&[("p", 1.0), ("q", 1.0 + 5e-9)])),
            vec![TieGroup { start: 0, end: 1 }, TieGroup { start: 1, end: 2 },],
        );
    }

    /// AC10: `sort_desc` runs through the identical function — the tie is
    /// free in the descending case too, and the direction is still
    /// asserted.
    #[test]
    fn value_ordered_agreement_covers_the_descending_case() {
        let pulsus = grp_seq(&[("a", 5.0), ("c", 5.0), ("b", 1.0)]);
        assert!(
            value_ordered_sequences_agree(&pulsus, &grp_seq(&[("c", 5.0), ("a", 5.0), ("b", 1.0)]))
                .is_ok()
        );
        assert!(
            value_ordered_sequences_agree(&pulsus, &grp_seq(&[("b", 1.0), ("a", 5.0), ("c", 5.0)]))
                .is_err()
        );
    }

    /// AC19: the anchor rule, including the SPLIT it deliberately causes.
    /// `b - a` and `c - b` are each inside the tolerance while `c - a` is
    /// outside it, so a chained walk returns ONE group over `[a, b, c]`
    /// and this test fails. That is the point of it.
    #[test]
    fn tie_groups_anchors_each_run_on_its_first_value() {
        let a = 1.0_f64;
        let b = 1.000_000_000_6_f64;
        let c = 1.000_000_001_2_f64;
        assert_eq!(
            tie_groups(&grp_seq(&[("p", a), ("q", b)])),
            vec![TieGroup { start: 0, end: 2 }],
            "adjacent pair a/b is inside the tolerance",
        );
        assert_eq!(
            tie_groups(&grp_seq(&[("q", b), ("r", c)])),
            vec![TieGroup { start: 0, end: 2 }],
            "adjacent pair b/c is inside the tolerance",
        );
        assert_eq!(
            tie_groups(&grp_seq(&[("p", a), ("q", b), ("r", c)])),
            vec![TieGroup { start: 0, end: 2 }, TieGroup { start: 2, end: 3 },],
            "the run is anchored on `a`, so `c` starts a new one",
        );
    }

    /// AC11: every committed ordered case shares one code path,
    /// established from the fixture rather than assumed. Issue #406 R2
    /// added the wrapped case to the same path — a third entry here and
    /// no third comparison function.
    #[test]
    fn both_sort_cases_share_the_ordered_comparison_path() {
        let fixture = shipped_fixture();
        let mut ordered: Vec<&str> = fixture
            .cases
            .iter()
            .filter(|c| c.kind() == "metric_instant_ordered")
            .map(|c| c.case_id.as_str())
            .collect();
        ordered.sort_unstable();
        assert_eq!(
            ordered,
            vec![
                "metric_sort_desc_order",
                "metric_sort_order",
                "metric_sort_wrapped_order"
            ],
            "the ordered comparison path serves exactly the committed sort cases",
        );
    }

    /// AC12: the two determinism pins stay, and their docs stop implying
    /// they are evidence about the reference.
    #[test]
    fn the_sort_pins_do_not_claim_reference_evidence() {
        // Assembled for the same reason as in AC1's source half.
        let stale = ["both stores", "agree on this order"].join(" ");
        let src = logs_rs_source();
        for pin in [
            "shipped_sort_case_evaluates_in_the_pinned_value_order",
            "shipped_sort_desc_case_evaluates_in_the_pinned_value_order",
        ] {
            let doc = doc_comment_above(&src, pin);
            assert!(
                !doc.contains(&stale),
                "{pin}'s doc still claims the live lane pins this arrangement across stores:\n{doc}"
            );
            assert!(
                doc.contains("PulsusDB only"),
                "{pin}'s doc does not say it observes PulsusDB only:\n{doc}"
            );
        }
    }

    /// The marker the ledger carries on the "Not covered" exclusion.
    /// Deliberately a token no prose would produce by accident, so
    /// counting it is a meaningful uniqueness test. It sits in the
    /// bullet's RENDERED text rather than in an HTML comment: the point
    /// of recording the exclusion is that a person reads it, and a
    /// comment is invisible to exactly that reader.
    const NOT_COVERED_MARKER: &str = "ledger-marker: sort-tie-order/not-covered";

    /// Operator names the marker line must carry, matched as WHOLE
    /// TOKENS.
    ///
    /// Token equality, not `contains`, and the reason is a defect this
    /// test shipped with: `approx_topk` contains `topk`, so a `contains`
    /// check for `topk` is satisfied by any line carrying `approx_topk`
    /// and can never fail. Splitting the line into `[A-Za-z0-9_]+` runs
    /// and comparing for equality makes the three independently
    /// breakable.
    const MARKER_TOKENS: &[&str] = &["topk", "bottomk", "approx_topk", "composed"];

    /// Phrases the marker line must carry, matched by `contains` because
    /// they are prose. Chosen so that no phrase contains another, and no
    /// phrase carries a [`MARKER_TOKENS`] entry — both asserted below.
    /// The earlier list violated the second rule: its lead-in phrase
    /// contained the word `composed`, so deleting `composed` failed on
    /// the lead-in and the `composed` needle itself was never exercised.
    const MARKER_PHRASES: &[&str] = &["Not covered", "#406"];

    /// The `[A-Za-z0-9_]+` runs of `s`, in order.
    fn identifier_tokens(s: &str) -> Vec<&str> {
        s.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .filter(|t| !t.is_empty())
            .collect()
    }

    /// AC13: the divergence is registered where the divergence discipline
    /// says it is registered, and the exclusion the AC exists to hold —
    /// the composed/`topk` case the cosmetic conclusion does NOT cover —
    /// is pinned by an explicit machine-readable marker rather than by
    /// anything about the document's shape.
    ///
    /// **Why a marker and not a section scan.** Three review rounds each
    /// found the next markdown edge case in a hand-written scanner here —
    /// whole-document versus entry, substring versus bullet, a bullet
    /// wrapped in a fence, a malformed closing fence, an indented
    /// heading. Every finding was correct and none of them was the
    /// product. Markdown has more edge cases than this test will ever
    /// have rounds, so the mechanism changed: a marker the ledger carries
    /// on purpose, asserted UNIQUE. Uniqueness is what replaces section
    /// extraction — a claim that was relational ("recorded in THIS
    /// entry") becomes checkable without locating the entry at all,
    /// because a marker that occurs exactly once cannot be satisfied by
    /// text somewhere else. Same shape as the corpus provenance markers
    /// (`# provenance: divergence(...)`, bound by
    /// `crates/pulsus-read/tests/logqltest_provenance.rs`).
    ///
    /// **Residual failure surface — written down and left alone.** Each
    /// bullet below was MEASURED: the mutation was applied to the
    /// committed ledger and this test was observed to stay GREEN. None of
    /// them is reasoned about.
    /// * **the bullet relocated.** Move the marker to another bullet, or
    ///   to another entry, and this test stays green. It pins that the
    ///   exclusion is recorded ONCE in the ledger, not which heading it
    ///   sits under. Locating it was what the deleted scanner attempted,
    ///   at the cost of a markdown parser that took three review rounds
    ///   and still had edge cases left;
    /// * **the marker fenced.** Wrap the bullet in a code fence and the
    ///   exclusion renders as a code SAMPLE rather than as normative
    ///   text, while the line — and so this test — is unchanged.
    ///   Detecting it needs fence parsing, the mechanism that was
    ///   removed;
    /// * **an unclosed fence elsewhere in the file.** Nothing here reads
    ///   fences, so appending one changes nothing. The guard that used to
    ///   catch this existed only to protect the deleted scanner;
    /// * **the entry heading demoted to `####`.** The uniqueness count
    ///   looks for the substring ``### `sort-tie-order` ``, which
    ///   ``#### `sort-tie-order` `` still contains, so the demotion is
    ///   invisible here;
    /// * **the marker's own prose edited to say something false** while
    ///   keeping every needle. Reading the sentence is the only thing
    ///   that catches that, and this test does not claim to;
    /// * nothing here validates the rest of the ledger. It answers one
    ///   question.
    ///
    /// That trade is deliberate: "recorded exactly once, with its content
    /// intact" in exchange for "rendered as a bullet under that heading".
    #[test]
    fn the_sort_tie_order_divergence_is_recorded_in_the_committed_ledger() {
        let ledger_path =
            crate::engine::workspace_root().join("docs/benchmarks/logs-differential-ledger.md");
        let ledger = std::fs::read_to_string(&ledger_path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", ledger_path.display()));

        // The entry exists, once. `### `<id>`` is the heading form
        // `logqltest_provenance.rs::ledger_ids` resolves a
        // `divergence(...)` marker against.
        assert_eq!(
            ledger.matches("### `sort-tie-order`").count(),
            1,
            "the ledger must carry exactly one `sort-tie-order` entry heading"
        );

        // The artifacts the entry governs.
        for needle in [
            "metric_sort_order",
            "metric_sort_desc_order",
            "value_ordered_sequences_agree",
            "differential_metric_reducers.test",
            "timestamp-tie-order",
        ] {
            assert!(
                ledger.contains(needle),
                "the ledger does not name {needle:?}"
            );
        }

        // The exclusion, pinned by a UNIQUE marker.
        let hits: Vec<&str> = ledger
            .lines()
            .filter(|l| l.contains(NOT_COVERED_MARKER))
            .collect();
        assert_eq!(
            hits.len(),
            1,
            "{NOT_COVERED_MARKER:?} must occur exactly once in the ledger, found {}",
            hits.len()
        );

        // Every needle must be INDEPENDENTLY breakable — removing one
        // from the marker line must fail on that needle and on no other.
        // Two needles that overlap cannot both be exercised, and the
        // survivor certifies a check that does not hold. Asserted here
        // rather than trusted, because this list has been wrong once.
        for phrase in MARKER_PHRASES {
            for other in MARKER_PHRASES {
                assert!(
                    phrase == other || !other.contains(phrase),
                    "phrase needle {phrase:?} is contained in {other:?}, so it can never fail alone"
                );
            }
            let phrase_tokens = identifier_tokens(phrase);
            for token in MARKER_TOKENS {
                assert!(
                    !phrase_tokens.contains(token),
                    "phrase needle {phrase:?} carries the token needle {token:?}, \
                     so removing the token would fail on the phrase instead"
                );
            }
        }

        // Everything the exclusion has to say, on the marker's own line,
        // so no part of it can drift away from the marker.
        let line = hits[0];
        let line_tokens = identifier_tokens(line);
        for token in MARKER_TOKENS {
            assert!(
                line_tokens.contains(token),
                "the `{NOT_COVERED_MARKER}` line carries no {token:?} token:\n{line}"
            );
        }
        for phrase in MARKER_PHRASES {
            assert!(
                line.contains(phrase),
                "the `{NOT_COVERED_MARKER}` line is missing {phrase:?}:\n{line}"
            );
        }
    }

    /// The committed ledger, read from the workspace.
    fn committed_ledger() -> String {
        let path =
            crate::engine::workspace_root().join("docs/benchmarks/logs-differential-ledger.md");
        std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()))
    }

    /// The `nested-sort-order` entry's three markers, each written on
    /// purpose and each asserted to occur EXACTLY ONCE in the ledger.
    ///
    /// **Nothing here extracts a section, and that is the point** (issue
    /// #406, code review rounds 1 and 2). The first version of this guard
    /// sliced the entry out of the document between `### ` headings; the
    /// reviewer demoted the heading to `####`, the substring `### ` was
    /// still found inside it, and the test stayed green. That was the
    /// fourth distinct markdown edge case this issue hit in a section
    /// scanner — after a bullet inside a fence, an unclosed fence
    /// elsewhere, and an indented heading — and the sibling
    /// `sort-tie-order` guard had already deleted its own scanner for
    /// exactly that reason. Patching the slicer to understand `####`
    /// would have been the fifth patch, not a fix.
    ///
    /// Uniqueness replaces extraction. A claim that reads as relational —
    /// "recorded in THIS entry" — becomes checkable without ever
    /// computing where the entry ends: each marker occurs exactly once in
    /// the whole file, so it cannot be satisfied by text elsewhere, and
    /// everything the claim asserts sits on the marker's own line.
    ///
    /// Two single-line predicates carry the rest, and neither needs a
    /// section:
    /// * [`NESTED_ENTRY_MARKER`] rides the `###` heading, so asserting
    ///   its line begins with the exact prefix `"### "` rejects the
    ///   reviewer's `####` demotion, and an indented heading, without
    ///   looking at any other line;
    /// * the other two markers are bound to the entry by
    ///   [`assert_marker_is_under_the_entry`], which asks whether an
    ///   entry boundary was CROSSED between two line numbers uniqueness
    ///   already gave us — three integers and the same prefix predicate.
    ///   It never searches for the end of anything.
    const NESTED_ENTRY_MARKER: &str = "ledger-marker: nested-sort-order/entry";
    /// The entry's machine-checkable one-line record.
    const NESTED_RECORD_MARKER: &str = "ledger-marker: nested-sort-order/record";
    /// Its "Not covered" bullet.
    const NESTED_NOT_COVERED_MARKER: &str = "ledger-marker: nested-sort-order/not-covered";

    /// Everything the record must carry, asserted ON
    /// [`NESTED_RECORD_MARKER`]'s own line. Chosen so no needle contains
    /// another — asserted below, because a contained needle can never
    /// fail on its own and its survivor certifies a check that does not
    /// hold.
    const NESTED_RECORD_NEEDLES: &[&str] = &[
        // The reference rule and its call site.
        "evaluator.go:242-260",
        "engine.go:564",
        // The mechanism that makes its surviving order arbitrary.
        "evaluator.go:584",
        "map[uint64]*groupedAggregation",
        // Ours, which is why this is a correctness argument and not a
        // parity one.
        "post_agg.rs:1122-1135",
        // The measurement: both images by digest, both buildinfo
        // revisions, and the repeat count.
        "sha256:87f0a067673756a3cede1bcbf0c74875f7df9b09fddb53e399d0c576f756cfcc",
        "sha256:58a6c186ce78ba04d58bfe2a927eff296ba733a430df09645d56cdc158f3ba08",
        "b318f282",
        "4fa045d3",
        "20 repeats per store per query",
        // Our rule, by symbol.
        "sorted_order_reaches_the_wire",
        // The five enumerated sub-cases where we stay deterministic.
        "sum by (svc) (sort(X))",
        "topk(2, sort(X))",
        "Y * sort(X)",
        "sort(X) or Y",
        "variants(…) of (…)",
        // The agreeing half, so the record cannot be read as a wholesale
        // divergence.
        "sort(A) or sort(B)",
        "group_right",
    ];

    /// The index of the one line in `lines` carrying `marker`, with the
    /// marker asserted to occur exactly once in the whole document first.
    fn unique_marker_index(ledger: &str, lines: &[&str], marker: &str) -> usize {
        assert_eq!(
            ledger.matches(marker).count(),
            1,
            "{marker:?} must occur exactly once in the ledger, found {}",
            ledger.matches(marker).count()
        );
        let hits: Vec<usize> = lines
            .iter()
            .enumerate()
            .filter(|(_, l)| l.contains(marker))
            .map(|(i, _)| i)
            .collect();
        assert_eq!(
            hits.len(),
            1,
            "{marker:?} must sit on exactly one line, found {}",
            hits.len()
        );
        hits[0]
    }

    /// Whether `line` opens a `###` ledger entry — a single-line
    /// predicate, and the only thing about markdown either of the two
    /// rules below knows.
    ///
    /// It follows CommonMark's ATX rules for exactly this one level,
    /// because every form it does NOT recognise is a false GREEN: a
    /// heading that opens a new entry while this returns `false` makes
    /// the region look larger than it is, and a marker moved into the
    /// next entry then passes.
    /// * up to **three** leading spaces are still a heading; four or more
    ///   make an indented code block, which is correctly not a boundary;
    /// * the separator after `###` may be a space **or a tab**;
    /// * `####` is a sub-heading, not an entry, so an entry may carry
    ///   sub-sections without its own markers falling out of it;
    /// * `###` with no separator at all is a paragraph in CommonMark, not
    ///   a heading, so it is correctly not a boundary.
    ///
    /// The entry's OWN heading is checked separately and strictly
    /// (`starts_with("### ")`), because our own heading is ours to keep
    /// clean: an indented or demoted `nested-sort-order` heading must be
    /// loud, not tolerated.
    fn opens_a_ledger_entry(line: &str) -> bool {
        let indent = line.len() - line.trim_start_matches(' ').len();
        if indent > 3 {
            return false;
        }
        match line[indent..].strip_prefix("###") {
            Some(rest) => rest.starts_with(' ') || rest.starts_with('\t'),
            None => false,
        }
    }

    /// Asserts that the line at `marker_idx` belongs to the entry whose
    /// heading is at `entry_idx`: it comes after the heading, **its own
    /// line does not open an entry**, and no entry heading lies between
    /// them.
    ///
    /// The middle clause arrived in code review round 3, which built the
    /// counterexample the previous version's residual note had claimed
    /// was impossible: merge both marker contents onto one line, make
    /// that line a `### ` heading, and the strictly-between scan never
    /// examines it — the markers now open a new entry and the guard
    /// passed anyway. The interval a boundary can sit in includes the
    /// marker's own line.
    ///
    /// **This is not the section slicing that was deleted twice, and the
    /// difference is what it needs to know** (issue #406, code review
    /// rounds 1 and 2). The slicer had to find where an entry *ends*, so
    /// it needed a next-heading search, fence handling, and a rule for
    /// every markdown shape a heading can take — and each round found the
    /// next shape it got wrong. This asks a different question about two
    /// line numbers that uniqueness already handed us: *was an entry
    /// boundary crossed between them?* Nothing is extracted, no needle is
    /// matched against a range, and where the entry ends is never
    /// computed.
    ///
    /// A bounded line window was the other candidate and is not taken: it
    /// works, but only with a constant chosen to be larger than this
    /// entry and smaller than the distance to a plausible relocation
    /// target, and a constant tuned against today's document is a gate
    /// that drifts silently as the document grows. This rule has no
    /// constant to tune.
    ///
    /// **What this guarantees, and what it does not.** The previous
    /// version of this note said "never a false GREEN". That was wrong,
    /// the review built the counterexample above, and a false green is
    /// the failure nobody sees — so the claim is now stated as a bound
    /// rather than as a reassurance.
    ///
    /// *Guaranteed:* the marker's line comes after the
    /// `nested-sort-order` heading, its own line does not open an entry,
    /// and no line between them opens one — where "opens an entry" is
    /// [`opens_a_ledger_entry`], which follows CommonMark's ATX rules for
    /// `###`.
    ///
    /// *A false RED, and deliberately so:* a `### ` line inside a fenced
    /// code block between the heading and a marker reads as a boundary.
    /// Loud, and fixed by moving the marker or the fence. Nothing in the
    /// committed ledger fences a `### ` line today.
    ///
    /// *False GREENs that remain, named rather than denied:*
    /// * a marker moved ELSEWHERE INSIDE its own entry. Deliberate — the
    ///   claim is that it is recorded under this entry, not at a fixed
    ///   offset within it;
    /// * the FOLLOWING entry's heading deleted, or written in a form
    ///   CommonMark does not make a heading either (`###` with no
    ///   separator, four spaces of indent). The region then genuinely
    ///   extends further, and a marker moved into that text passes. The
    ///   next entry losing its heading is a defect in that entry, and one
    ///   this guard does not claim to find;
    /// * an entry heading emitted by an HTML block or a template rather
    ///   than written literally. This file is hand-written markdown;
    ///   nothing generates it.
    fn assert_marker_is_under_the_entry(
        lines: &[&str],
        entry_idx: usize,
        marker_idx: usize,
        marker: &str,
    ) {
        match entry_boundary_between(lines, entry_idx, marker_idx) {
            EntryRelation::Under => {}
            EntryRelation::BeforeTheHeading => panic!(
                "{marker:?} is at line {} but the `nested-sort-order` heading is at line {} — \
                 a marker that precedes its own entry belongs to an earlier one",
                marker_idx + 1,
                entry_idx + 1
            ),
            EntryRelation::OnAnEntryHeading => panic!(
                "{marker:?} is at line {}, and that line itself OPENS a new ledger entry — \
                 the marker is the heading of something else, not a record under the \
                 `nested-sort-order` heading at line {}:\n{}",
                marker_idx + 1,
                entry_idx + 1,
                lines[marker_idx]
            ),
            EntryRelation::PastABoundaryAt(i) => panic!(
                "{marker:?} is at line {} but a NEW ledger entry opens at line {} between it \
                 and the `nested-sort-order` heading at line {} — the marker has been moved \
                 out of its entry:\n{}",
                marker_idx + 1,
                i + 1,
                entry_idx + 1,
                lines[i]
            ),
        }
    }

    /// Where a marker line sits relative to an entry heading.
    #[derive(Debug, PartialEq, Eq)]
    enum EntryRelation {
        /// After the heading with no entry boundary in between.
        Under,
        /// At or before the heading.
        BeforeTheHeading,
        /// The marker's OWN line opens an entry, so it is a heading
        /// rather than a record under one (code review round 3).
        OnAnEntryHeading,
        /// After the heading, but a new entry opens at this line index
        /// first.
        PastABoundaryAt(usize),
    }

    /// [`assert_marker_is_under_the_entry`]'s whole decision as a pure
    /// function of three integers and a slice, so the rule can be
    /// exercised on a synthetic document without a panic hook.
    ///
    /// The interval a boundary can occupy is `entry_idx < i <=
    /// marker_idx` — **inclusive of the marker's own line**. Excluding
    /// it was round 3's defect: a marker merged onto a `### ` line was
    /// never examined and passed.
    fn entry_boundary_between(
        lines: &[&str],
        entry_idx: usize,
        marker_idx: usize,
    ) -> EntryRelation {
        if marker_idx <= entry_idx {
            return EntryRelation::BeforeTheHeading;
        }
        // Checked ahead of the interval scan only so the diagnostic can
        // name this case precisely; the interval it belongs to is the
        // inclusive one either way.
        if opens_a_ledger_entry(lines[marker_idx]) {
            return EntryRelation::OnAnEntryHeading;
        }
        match lines[entry_idx + 1..marker_idx]
            .iter()
            .position(|l| opens_a_ledger_entry(l))
        {
            None => EntryRelation::Under,
            Some(i) => EntryRelation::PastABoundaryAt(entry_idx + 1 + i),
        }
    }

    /// Issue #406 R2, AC 9: the `nested-sort-order` divergence is
    /// registered where the divergence discipline says it is registered.
    ///
    /// Three markers, three claims, each pinned by exactly-once
    /// uniqueness and by what sits on the marker's own line — the shape
    /// the sibling `sort-tie-order` guard arrived at after three review
    /// rounds, and the one this test arrived at after a fourth. See
    /// [`NESTED_ENTRY_MARKER`] for why no markdown is parsed.
    ///
    /// **Residual failure surface, measured rather than reasoned about**
    /// (each mutation applied to the committed ledger and observed
    /// GREEN — none of these is a prediction):
    /// * **a marker line's own prose edited to say something false**
    ///   while keeping every needle. Reading the sentence is the only
    ///   thing that catches that, and this test does not claim to;
    /// * **a marker relocated ELSEWHERE INSIDE its own entry** — to a
    ///   different bullet, or above the record line. That is an ordinary
    ///   edit and is deliberately allowed; the claim is that the marker
    ///   is recorded under this entry, not at a fixed offset within it;
    /// * the remaining ways an entry BOUNDARY can go unseen, which are
    ///   enumerated on [`assert_marker_is_under_the_entry`] rather than
    ///   here, because that is where the rule lives;
    /// * nothing here validates the rest of the ledger, or the prose
    ///   below the record line. It answers three questions.
    ///
    /// An earlier version of this list said the mechanism could produce
    /// "never a false GREEN". Code review round 3 built the
    /// counterexample — both marker contents merged onto one line that
    /// itself opened a `### ` entry — and it is fixed rather than
    /// re-worded. The sentence is kept in the record because a false
    /// green is the failure nobody sees, and a confident claim about one
    /// is worth more scepticism than the mechanism it describes.
    #[test]
    fn the_nested_sort_order_divergence_is_recorded_in_the_committed_ledger() {
        let ledger = committed_ledger();
        let lines: Vec<&str> = ledger.lines().collect();

        // (1) The entry exists, once, at `###` — the marker rides the
        // heading, so this is a one-line shape check, not a section scan.
        // `"#### x".starts_with("### ")` is FALSE (the fourth byte is
        // `#`, not a space), which is exactly the demotion the review
        // caught an earlier version on.
        let entry_idx = unique_marker_index(&ledger, &lines, NESTED_ENTRY_MARKER);
        let heading = lines[entry_idx];
        assert!(
            opens_a_ledger_entry(heading),
            "the `nested-sort-order` entry must be a `###` heading, not {:?}:\n{heading}",
            heading.chars().take_while(|c| *c == '#').count()
        );
        assert!(
            heading.contains("`nested-sort-order`"),
            "the entry marker's line must name the entry:\n{heading}"
        );

        // (2) The record, entirely on its marker's own line, and that
        // line under THIS entry — see `assert_marker_is_under_the_entry`
        // for why a boundary-crossing test is not the deleted slicer.
        for needle in NESTED_RECORD_NEEDLES {
            for other in NESTED_RECORD_NEEDLES {
                assert!(
                    needle == other || !other.contains(needle),
                    "needle {needle:?} is contained in {other:?}, so it can never fail alone"
                );
            }
        }
        let record_idx = unique_marker_index(&ledger, &lines, NESTED_RECORD_MARKER);
        assert_marker_is_under_the_entry(&lines, entry_idx, record_idx, NESTED_RECORD_MARKER);
        let record = lines[record_idx];
        for needle in NESTED_RECORD_NEEDLES {
            assert!(
                record.contains(needle),
                "the `{NESTED_RECORD_MARKER}` line does not carry {needle:?}:\n{record}"
            );
        }

        // (3) The exclusion, entirely on its marker's own line, and that
        // line under this entry too.
        let excl_idx = unique_marker_index(&ledger, &lines, NESTED_NOT_COVERED_MARKER);
        assert_marker_is_under_the_entry(&lines, entry_idx, excl_idx, NESTED_NOT_COVERED_MARKER);
        let line = lines[excl_idx];
        let tokens = identifier_tokens(line);
        for token in ["topk", "bottomk", "R1"] {
            assert!(
                tokens.contains(&token),
                "the `{NESTED_NOT_COVERED_MARKER}` line carries no {token:?} token:\n{line}"
            );
        }
        for phrase in ["Not covered", "#406", "no work"] {
            assert!(
                line.contains(phrase),
                "the `{NESTED_NOT_COVERED_MARKER}` line is missing {phrase:?}:\n{line}"
            );
        }

        // The three markers are distinct claims, so none may be a
        // substring of another — otherwise one exactly-once count would
        // be satisfied by another marker's line.
        let markers = [
            NESTED_ENTRY_MARKER,
            NESTED_RECORD_MARKER,
            NESTED_NOT_COVERED_MARKER,
        ];
        for a in markers {
            for b in markers {
                assert!(
                    a == b || !b.contains(a),
                    "marker {a:?} is contained in {b:?}, so its count can never be 1 alone"
                );
            }
        }
    }

    /// The entry-binding rule, exercised on a synthetic document so its
    /// four answers are pinned independently of the committed ledger
    /// (issue #406, code review rounds 2 and 3).
    ///
    /// `PastABoundaryAt` is round 2's case: a line whose content is
    /// intact but which now sits under a DIFFERENT entry.
    /// `OnAnEntryHeading` is round 3's: a marker merged onto a line that
    /// itself opens an entry, which the strictly-between scan never
    /// examined and which therefore passed. `Under` is what must NOT
    /// break — ordinary edits inside the entry move a marker's line
    /// number without moving it out of the entry.
    #[test]
    fn a_marker_moved_past_an_entry_heading_is_no_longer_under_its_entry() {
        let doc: Vec<&str> = vec![
            "### `a` (one)",     // 0
            "",                  // 1
            "marker-in-a",       // 2
            "an added sentence", // 3
            "another bullet",    // 4
            "",                  // 5
            "### `b` (two)",     // 6
            "",                  // 7
            "marker-in-b",       // 8
        ];
        // Under its own entry, at two different offsets — the rule is
        // about the boundary, not about a distance.
        assert_eq!(entry_boundary_between(&doc, 0, 2), EntryRelation::Under);
        assert_eq!(entry_boundary_between(&doc, 0, 4), EntryRelation::Under);
        // Moved past `### \`b\``: the boundary is named by line index.
        assert_eq!(
            entry_boundary_between(&doc, 0, 8),
            EntryRelation::PastABoundaryAt(6)
        );
        // Moved ABOVE its own heading — it now belongs to whatever came
        // before.
        assert_eq!(
            entry_boundary_between(&doc, 6, 2),
            EntryRelation::BeforeTheHeading
        );

        // ROUND 3's case: the marker's OWN line opens an entry. The
        // strictly-between scan never looked at it, so a marker merged
        // onto a `### ` line read as `Under` while it was in fact the
        // heading of something else.
        let merged: Vec<&str> = vec![
            "### `a` (one)",                    // 0
            "",                                 // 1
            "### merged marker line, an entry", // 2
        ];
        assert_eq!(
            entry_boundary_between(&merged, 0, 2),
            EntryRelation::OnAnEntryHeading
        );

        // A `####` sub-heading is NOT an entry boundary, so an entry may
        // carry sub-sections without its own markers falling out of it —
        // in either position.
        let with_sub: Vec<&str> = vec!["### `a`", "#### a detail", "marker-in-a"];
        assert_eq!(
            entry_boundary_between(&with_sub, 0, 2),
            EntryRelation::Under
        );
        let sub_marker: Vec<&str> = vec!["### `a`", "", "#### marker on a sub-heading"];
        assert_eq!(
            entry_boundary_between(&sub_marker, 0, 2),
            EntryRelation::Under
        );

        // The forms `opens_a_ledger_entry` MUST recognise, because each
        // one it misses is a false GREEN: an intervening heading it does
        // not see makes the region look larger than it is.
        for heading in [
            "   ### three spaces is still a heading",
            "###\tthe separator may be a tab",
        ] {
            let doc: Vec<&str> = vec!["### `a`", heading, "marker"];
            assert_eq!(
                entry_boundary_between(&doc, 0, 2),
                EntryRelation::PastABoundaryAt(1),
                "{heading:?} must read as an entry boundary"
            );
        }
        // …and the forms it must NOT recognise, because CommonMark does
        // not make them headings either: four spaces opens an indented
        // code block, and `###` with no separator is a paragraph.
        // Treating these as boundaries would redden ordinary prose.
        for not_heading in ["    ### four spaces is a code block", "###no separator"] {
            let doc: Vec<&str> = vec!["### `a`", not_heading, "marker"];
            assert_eq!(
                entry_boundary_between(&doc, 0, 2),
                EntryRelation::Under,
                "{not_heading:?} is not a heading in CommonMark either"
            );
        }
    }

    /// AC15: the fixture prose no longer promises a reference tie order.
    /// `construct` is the only place a case's expected response is written
    /// in words, and prose rot there is invisible to every other gate.
    #[test]
    fn the_sort_case_constructs_do_not_promise_a_reference_tie_order() {
        let fixture = shipped_fixture();
        for case_id in ["metric_sort_order", "metric_sort_desc_order"] {
            let case = fixture
                .cases
                .iter()
                .find(|c| c.case_id == case_id)
                .unwrap_or_else(|| panic!("the {case_id} case is shipped"));
            assert!(
                case.construct.contains("sort-tie-order"),
                "case {case_id:?} does not name the ledger entry:\n{}",
                case.construct
            );
            assert!(
                !case.construct.contains("oracle-confirmed"),
                "case {case_id:?} still promises a reference tie order:\n{}",
                case.construct
            );
        }
    }

    /// Issue #406: where a rendered LogQL query's `sort`/`sort_desc` and
    /// k-selecting vector aggregations sit in its parse tree. A log
    /// (streams) query has no metric tree and reports all-zero.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct SortShape {
        /// `sort`/`sort_desc` nodes ANYWHERE in the tree, root included.
        sorts: usize,
        /// The AST ROOT is itself a `sort`/`sort_desc`.
        root_is_sort: bool,
        /// `topk`/`bottomk`/`approx_topk` nodes anywhere in the tree.
        k_selectors: usize,
    }

    /// Parses under the shipped grammar; panics with the query's own text
    /// if it does not parse (a fixture typo is already caught upstream by
    /// `shipped_fixture_queries_parse_and_their_pipelines_compile`).
    ///
    /// Counts come from `pulsus_logql::for_each_metric_expr`, the
    /// iterative SCC-2 driver (`crates/pulsus-logql/src/ast.rs:1453-1455`),
    /// which visits the root and descends through non-aggregation
    /// wrappers such as `MetricExpr::LabelReplace` — so a sort buried
    /// under one is counted.
    fn sort_shape(rendered: &str) -> SortShape {
        use pulsus_logql::{Expr, MeNode, MetricExpr, VectorAggOp};

        let expr = pulsus_logql::parse(rendered)
            .unwrap_or_else(|e| panic!("query does not parse: {rendered:?}: {e}"));
        let me = match &expr {
            Expr::Log(_) => {
                return SortShape {
                    sorts: 0,
                    root_is_sort: false,
                    k_selectors: 0,
                };
            }
            Expr::Metric(me) => me,
        };
        let root_is_sort = matches!(
            me,
            MetricExpr::Vector {
                op: VectorAggOp::Sort | VectorAggOp::SortDesc,
                ..
            }
        );
        let mut sorts = 0usize;
        let mut k_selectors = 0usize;
        pulsus_logql::for_each_metric_expr(me, |node| {
            if let MeNode::Expr(MetricExpr::Vector { op, .. }) = node {
                match op {
                    VectorAggOp::Sort | VectorAggOp::SortDesc => sorts += 1,
                    VectorAggOp::Topk | VectorAggOp::Bottomk | VectorAggOp::ApproxTopk => {
                        k_selectors += 1
                    }
                    _ => {}
                }
            }
        });
        SortShape {
            sorts,
            root_is_sort,
            k_selectors,
        }
    }

    /// AC20's property as a PURE predicate, so it can be broken without a
    /// fixture: no sort at all, or exactly one and it is the root. The
    /// "exactly one" clause is load-bearing — `sort(sort(X))` has a sort
    /// at the root AND a second, non-terminal sort beneath it.
    fn sort_is_terminal(rendered: &str) -> bool {
        let s = sort_shape(rendered);
        s.sorts == 0 || (s.sorts == 1 && s.root_is_sort)
    }

    /// The inner metric expression the AC20 break/control rows are built
    /// around — the committed sort cases' own operand.
    const AC20_INNER: &str =
        r#"sum by (grp) (count_over_time({run_id="r", service_name="svc-sort"} | logfmt [30m]))"#;

    /// AC20(a), widened by issue #406 R2: every committed case either
    /// carries no `sort`/`sort_desc` at all, or its order **reaches the
    /// wire** — and none carries a k-selector.
    ///
    /// The question changed with the corpus. It used to be "is the sort
    /// terminal", because a non-terminal sort's order was thrown away by
    /// the encoder; the shipped `metric_sort_wrapped_order` case is a
    /// non-terminal sort whose order now survives, so the property the
    /// corpus actually has is the PRODUCTION predicate's
    /// (`pulsus_read::logql::sorted_order_reaches_the_wire`). Asking the
    /// production predicate is deliberate: it is the same function the
    /// server consults, so a case whose order the encoder would discard
    /// cannot be committed as an ordered one.
    ///
    /// The `k_selectors == 0` assertion is unchanged and unwidened — it
    /// holds the committed set inside the `sort-tie-order` ledger's
    /// cosmetic conclusion, which a truncating operator would leave.
    #[test]
    fn every_committed_ordered_case_carries_its_sort_to_the_wire() {
        let fixture = shipped_fixture();
        for case in &fixture.cases {
            let rendered = case.query.replace("{R}", "e2e-logs-test");
            let shape = sort_shape(&rendered);
            if shape.sorts > 0 {
                let expr = pulsus_logql::parse(&rendered)
                    .unwrap_or_else(|e| panic!("query does not parse: {rendered:?}: {e}"));
                assert!(
                    pulsus_read::logql::sorted_order_reaches_the_wire(&expr),
                    "case {:?} has a sort whose order the encoder would discard: \
                     {shape:?} for {rendered}",
                    case.case_id
                );
            }
            assert_eq!(
                shape.k_selectors, 0,
                "case {:?} carries a k-selector, which truncates and so falls \
                 outside the `sort-tie-order` cosmetic conclusion: {rendered}",
                case.case_id
            );
        }
    }

    /// AC20(b): the break cases. Each is a query a `starts_with("sort(")`
    /// / literal-substring rule reads as terminal-and-k-free, and each
    /// fails the AST rule.
    #[test]
    fn sort_terminality_rejects_a_composed_or_repeated_sort() {
        let b = AC20_INNER;
        // THE named break case: `sort(B) + 5` starts with `sort(`, so a
        // `starts_with("sort(")` test passes it, but its root is
        // `Binary(Add)` and the sort is an operand.
        assert!(!sort_is_terminal(&format!("sort({b}) + 5")));
        // Whitespace before the paren: no literal `sort(` at all, so a
        // substring rule passes it vacuously.
        assert!(!sort_is_terminal(&format!("sort ({b}) + 5")));
        // A root sort with a second sort beneath it.
        assert!(!sort_is_terminal(&format!("sort(sort({b}))")));
        // A k-selector wrapping a sort — the composed case R1 records.
        assert!(!sort_is_terminal(&format!("topk(1, sort({b}))")));
        // The k-selector half, which a literal-substring rule also misses
        // because the keywords are case-insensitive
        // (`VectorAggOp::from_ident` lowercases first).
        for k in [
            format!("TopK(1, {b})"),
            format!("bottomk(2, {b})"),
            format!("approx_topk(2, {b})"),
        ] {
            assert_eq!(sort_shape(&k).k_selectors, 1, "{k}");
        }
    }

    /// AC20(c): the positive controls. Rows 7 and 8 are terminal-or-
    /// sortless queries a prefix/literal rule would have wrongly RED-ed,
    /// so the new rule cannot be "stronger" merely by rejecting more.
    #[test]
    fn sort_terminality_accepts_the_committed_and_parenthesised_forms() {
        let b = AC20_INNER;
        assert!(sort_is_terminal(&format!("sort({b})")));
        assert!(sort_is_terminal(&format!("sort_desc({b})")));
        // Parenthesised: no `sort(` at position 0.
        assert!(sort_is_terminal(&format!("(sort({b}))")));
        // `sort(` inside a label-filter string literal, in a log query
        // that has no metric tree at all.
        assert!(sort_is_terminal(
            r#"{run_id="r"} | logfmt | msg = "sort(x)""#
        ));
        // The committed queries themselves, taken from the fixture rather
        // than retyped. Issue #406 R2: `metric_sort_wrapped_order` is a
        // deliberately NON-terminal sort, and the two predicates differ
        // on it — the syntactic one rejects it, the production one
        // accepts it. That difference is the change the case exists to
        // gate, so it is asserted here rather than excluded.
        let fixture = shipped_fixture();
        let mut wrapped_seen = false;
        for case in fixture
            .cases
            .iter()
            .filter(|c| c.kind() == "metric_instant_ordered")
        {
            let rendered = case.query.replace("{R}", "e2e-logs-test");
            if case.case_id == "metric_sort_wrapped_order" {
                wrapped_seen = true;
                assert!(
                    !sort_is_terminal(&rendered),
                    "the wrapped case is supposed to be non-terminal: {rendered}"
                );
                let expr = pulsus_logql::parse(&rendered).expect("parse");
                assert!(
                    pulsus_read::logql::sorted_order_reaches_the_wire(&expr),
                    "the wrapped case's order must still reach the wire: {rendered}"
                );
            } else {
                assert!(sort_is_terminal(&rendered), "case {:?}", case.case_id);
            }
        }
        assert!(
            wrapped_seen,
            "the wrapped case is committed, so this loop must have reached it"
        );
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
                // The shared logs corpus carries no per-entry structured
                // metadata, so the hermetic mirror of the metric path sees
                // exactly what the live one does (issue #249).
                structured_metadata: String::new(),
            })
            .collect();
        assert!(!rows.is_empty(), "the witness record must exist");
        let err = pulsus_read::logql::run_client_agg_rows(
            &rows,
            &compiled,
            &meta,
            client,
            hermetic_window(mp),
            mp.rate_window_ns,
        )
        .expect_err("a surviving conversion failure must fail the query");
        let pulsus_read::logql::ReadError::MetricPipelineError { error_type, .. } = &err else {
            panic!("expected MetricPipelineError, got {err:?}");
        };
        assert_eq!(error_type, pulsus_read::logql::SAMPLE_EXTRACTION_ERROR);
    }

    /// The `service_name` the case's (single) selector pins.
    ///
    /// Issue #272: a driver consumer (`walk::find_preorder`), not a
    /// recursion. Pre-order left-to-right with an early break reproduces
    /// the old `walk(lhs).or_else(|| walk(rhs))` order exactly, and
    /// `Step::Prune` at `MetricExpr::Variants` reproduces the old arm,
    /// which read ONLY the common range and never descended into the
    /// variant expressions (issue #221: only the common range selects
    /// data). Without the prune, a variant's own selector would be
    /// consulted and the answer would change.
    fn first_selector_service(expr: &pulsus_logql::Expr) -> String {
        fn service_of(sel: &pulsus_logql::StreamSelector) -> Option<String> {
            sel.matchers
                .iter()
                .find(|m| m.name == "service_name")
                .map(|m| m.value.clone())
        }
        let pulsus_logql::Expr::Metric(me) = expr else {
            panic!("metric expr expected");
        };
        pulsus_logql::walk::find_preorder::<pulsus_logql::MetricScc, String>(
            pulsus_logql::MeNode::Expr(me),
            |n| match n {
                pulsus_logql::MeNode::Expr(pulsus_logql::MetricExpr::Range { range, .. }) => {
                    match service_of(&range.selector.selector) {
                        Some(s) => ControlFlow::Break(s),
                        None => ControlFlow::Continue(pulsus_logql::walk::Step::Descend),
                    }
                }
                // issue #221: only the COMMON range selects data, so the
                // variant expressions are pruned rather than visited.
                pulsus_logql::MeNode::Expr(pulsus_logql::MetricExpr::Variants(_)) => {
                    ControlFlow::Continue(pulsus_logql::walk::Step::Descend)
                }
                pulsus_logql::MeNode::Var(v) => match service_of(&v.range.selector.selector) {
                    Some(s) => ControlFlow::Break(s),
                    None => ControlFlow::Continue(pulsus_logql::walk::Step::Prune),
                },
                pulsus_logql::MeNode::Expr(_) => {
                    ControlFlow::Continue(pulsus_logql::walk::Step::Descend)
                }
            },
        )
        .expect("metric case selectors pin a service")
    }

    /// Issue #272 AC 32: `Step::Prune` is load-bearing.
    ///
    /// The common range carries NO `service_name`, and a variant
    /// expression carries the WRONG one. The pre-#272 recursive arm read
    /// only `v.range` and returned `None` for the whole `Variants`
    /// subtree, so the answer came from the `Binary`'s rhs. Pre-order
    /// left-to-right reaches the variant's selector first, so without the
    /// prune the answer would be `"wrong"`.
    #[test]
    fn first_selector_service_prunes_variant_selectors() {
        use pulsus_logql::walk::{Child, ChildVec};

        fn range_of(service: Option<&str>) -> pulsus_logql::LogRange {
            let template = format!(
                "count_over_time({{{}}}[5m])",
                match service {
                    Some(s) => format!("service_name=\"{s}\", app=\"a\""),
                    None => "app=\"a\"".to_string(),
                }
            );
            let expr = pulsus_logql::parse(&template).expect("fixture parses");
            match &expr {
                pulsus_logql::Expr::Metric(pulsus_logql::MetricExpr::Range { range, .. }) => {
                    range.clone()
                }
                other => panic!("unexpected fixture shape: {other:?}"),
            }
        }
        fn range_expr(service: Option<&str>) -> pulsus_logql::MetricExpr {
            pulsus_logql::MetricExpr::Range {
                op: pulsus_logql::RangeAggOp::CountOverTime,
                range: range_of(service),
                param: None,
                grouping: None,
            }
        }

        let fixture = pulsus_logql::Expr::Metric(pulsus_logql::MetricExpr::Binary {
            op: pulsus_logql::BinOp::Add,
            modifier: None,
            lhs: Child::new(pulsus_logql::MetricExpr::Variants(Child::new(
                pulsus_logql::VariantsExpr {
                    variants: ChildVec::new(vec![range_expr(Some("wrong"))]),
                    // The COMMON range has no `service_name`.
                    range: range_of(None),
                },
            ))),
            rhs: Child::new(range_expr(Some("right"))),
        });

        assert_eq!(first_selector_service(&fixture), "right");
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
                // The shared logs corpus carries no per-entry structured
                // metadata, so the hermetic mirror of the metric path sees
                // exactly what the live one does (issue #249).
                structured_metadata: String::new(),
            })
            .collect();
        let result = pulsus_read::logql::run_client_agg_rows(
            &rows,
            &compiled,
            &meta,
            client,
            hermetic_window(mp),
            mp.rate_window_ns,
        )
        .expect("client aggregation");
        pulsus_read::logql::apply_vector_aggs(result, &mp.vector_aggs)
            .expect("a differential fixture is far below MAX_POST_AGG_BYTES")
    }

    fn evaluate_node_hermetically(
        corpus: &LogCorpus,
        node: &pulsus_read::logql::MetricNode,
        service: &str,
    ) -> pulsus_read::logql::QueryResult {
        // Issue #272: a post-order fold over a value stack, mirroring the
        // engine's `run_metric_node`; left-to-right post-order evaluates
        // `lhs`'s whole subtree before `rhs`'s, so every expectation is
        // unchanged.
        let mut nodes = Vec::new();
        pulsus_logql::walk::postorder_into::<pulsus_read::logql::MetricNodeScc>(node, &mut nodes);
        let mut vals: Vec<pulsus_read::logql::QueryResult> = Vec::with_capacity(nodes.len());
        for node in nodes {
            let v = match node {
                pulsus_read::logql::MetricNode::Leaf(mp) => {
                    evaluate_leaf_hermetically(corpus, mp, service)
                }
                pulsus_read::logql::MetricNode::Scalar(v) => {
                    pulsus_read::logql::QueryResult::Scalar(*v)
                }
                pulsus_read::logql::MetricNode::VectorLit { value, window } => {
                    pulsus_read::logql::materialize_vector_lit(*value, window)
                        .expect("vector() grid within bucket cap")
                }
                pulsus_read::logql::MetricNode::VectorAgg { aggs, .. } => {
                    let inner = vals.pop().expect("post-order pushes inner");
                    pulsus_read::logql::apply_vector_aggs(inner, aggs)
                        .expect("a differential fixture is far below MAX_POST_AGG_BYTES")
                }
                pulsus_read::logql::MetricNode::Binary {
                    op,
                    return_bool,
                    matching,
                    ..
                } => {
                    let r = vals.pop().expect("post-order pushes rhs");
                    let l = vals.pop().expect("post-order pushes lhs");
                    pulsus_read::logql::combine_binary(*op, *return_bool, matching.as_ref(), l, r)
                        .expect("combine")
                }
                // No differential fixture declares a `variants(...)` case (the
                // approx_topk precedent set the bar at hermetic corpus + syntax
                // differential; a live case would additionally need the
                // per-tenant flag on the e2e oracle deployment) — reaching this
                // arm means a fixture drifted (issue #221).
                pulsus_read::logql::MetricNode::Variants { .. } => {
                    panic!("no differential fixture declares variants (issue #221)")
                }
                // `label_replace(...)` (issue #276): the engine's exact
                // post-fetch transform, so a future fixture case replays
                // hermetically like every other combinator.
                pulsus_read::logql::MetricNode::LabelReplace { spec, .. } => {
                    let inner = vals.pop().expect("post-order pushes inner");
                    pulsus_read::logql::apply_label_replace(inner, spec)
                        .expect("a differential fixture is far below MAX_POST_AGG_BYTES")
                }
            };
            vals.push(v);
        }
        vals.pop().expect("a post-order fold leaves one value")
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
            // The issue #109 scope case selects on per-entry STRUCTURED
            // METADATA (`| scope_name="…"`), not body text — this evaluator
            // compiles the pipeline against `[run_id, service_name]` base labels
            // only and cannot model SM injection, so it is validated instead by
            // the corpus projection + the `scope_witness_sm_labels` collision
            // test + the live differential (set-equal PulsusDB==Loki) and its
            // stream-selector-empty placement discriminator.
            if case.case_id == "scope_structured_metadata" {
                continue;
            }
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
                let Some(out) = compiled
                    .run(&r.body, &base, r.ts_ns)
                    .expect("no template budget breach")
                else {
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
            let Some(out) = compiled
                .run(&r.body, &base, r.ts_ns)
                .expect("no template budget breach")
            else {
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

    /// #100 re-review fix (issue #115 finding, plan comment 5024235495):
    /// `ordered_entries_or_dump` must dump a repro artifact BEFORE bailing
    /// on an order/shape violation, and must never dump on a passing body.
    /// Reuses the tripped/ok bodies from
    /// `ordered_entries_rejects_a_within_stream_descending_pair` with a
    /// recording `dump` closure — no live stack, no `write_artifact`. Also
    /// covers the mechanical companion check (AC2): `run_streams_limited_case`
    /// must route all three `ordered_entries` call sites through the
    /// wrapper, source-inspected below, so a bare call can't silently
    /// reintroduce the skip.
    #[test]
    fn an_order_validity_failure_dumps_a_repro_before_bailing() {
        let get = serde_json::json!({"method": "GET", "status": "503", "took_ms": "500"});
        let delete = serde_json::json!({"method": "DELETE", "status": "503", "took_ms": "500"});
        let put = serde_json::json!({"method": "PUT", "status": "503", "took_ms": "500"});
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
        let ok_body = mk(serde_json::json!([["100", "a"], ["400", "b"]]));
        let tripped = mk(serde_json::json!([["400", "b"], ["100", "a"]]));

        let calls: std::cell::RefCell<Vec<(String, String)>> = std::cell::RefCell::new(Vec::new());
        let dump = |kind: &str, detail: &str| -> Result<std::path::PathBuf> {
            calls
                .borrow_mut()
                .push((kind.to_string(), detail.to_string()));
            Ok(std::path::PathBuf::from("/sentinel/repro.json"))
        };

        // Passing path: the wrapper is transparent and dumps nothing.
        let ok = ordered_entries_or_dump("pulsusdb", &ok_body, "case-ok", &dump)
            .expect("an ascending body must pass through unchanged");
        assert_eq!(ok, ordered_entries(&ok_body).unwrap());
        assert!(
            calls.borrow().is_empty(),
            "a passing body must never invoke dump, got {:?}",
            calls.borrow()
        );

        // Failing path: exactly one dump, before the bail, path embedded.
        let err = ordered_entries_or_dump("pulsusdb", &tripped, "case-tripped", &dump)
            .expect_err("a within-stream descending pair must fail");
        let recorded = calls.borrow();
        assert_eq!(
            recorded.len(),
            1,
            "an order-validity failure must dump exactly once, got {recorded:?}"
        );
        assert_eq!(recorded[0].0, "order_violation");
        assert!(
            recorded[0].1.contains("out of forward order"),
            "dump detail must carry the underlying cause: {}",
            recorded[0].1
        );
        assert!(
            err.to_string().contains("/sentinel/repro.json"),
            "the bail message must embed the dump's repro path: {err}"
        );

        // Mechanical companion (AC2): `run_streams_limited_case` must route
        // every `ordered_entries` call through the dump-then-bail wrapper —
        // a bare `ordered_entries(...)` call inside that function silently
        // reintroduces the #115 skip. Source-inspects this file's shipped
        // copy (not a compiled artifact), scoped to the function body.
        let root = crate::engine::workspace_root();
        let src = std::fs::read_to_string(root.join("e2e/src/logs.rs")).unwrap();
        let start = src
            .find("async fn run_streams_limited_case(")
            .expect("run_streams_limited_case must still exist");
        let end = src[start..]
            .find("\nfn describe_diff(")
            .map(|rel| start + rel)
            .expect("describe_diff must still follow run_streams_limited_case");
        let body = &src[start..end];
        assert_eq!(
            body.matches("ordered_entries_or_dump(").count(),
            3,
            "expected exactly the three known call sites to route through the wrapper"
        );
        assert!(
            !body
                .replace("ordered_entries_or_dump(", "")
                .contains("ordered_entries("),
            "run_streams_limited_case must not call ordered_entries(...) directly — route it \
             through ordered_entries_or_dump so a failure dumps a repro before bailing"
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

    // ---------------------------------------------------------------
    // Issue #102 (un-defer): the SM push lane cannot replay a corpus
    // body it has already sent, under a fault the store can only
    // observe as "ingested, then the transport died".
    // ---------------------------------------------------------------

    /// Finds the first byte offset of `needle` in `haystack`, or `None`.
    fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack.windows(needle.len()).position(|w| w == needle)
    }

    /// Reads one full HTTP request off `socket` — headers, then exactly
    /// `Content-Length` body bytes — without ever writing a response. This
    /// is the "ingested, then the transport died" fault: from the client's
    /// perspective the connection was established and the request fully
    /// sent, but the response read fails, which `classify_push_send`
    /// classifies as post-connect (issue #105's terminal arm), never a
    /// safe-to-retry connect failure.
    async fn read_full_request_then_drop(socket: &mut tokio::net::TcpStream) {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        let header_end = loop {
            match socket.read(&mut chunk).await {
                Ok(0) | Err(_) => return, // closed before headers completed
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
            if let Some(pos) = find_subsequence(&buf, b"\r\n\r\n") {
                break pos;
            }
        };
        let headers = String::from_utf8_lossy(&buf[..header_end]);
        let content_length: usize = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                if name.eq_ignore_ascii_case("content-length") {
                    value.trim().parse().ok()
                } else {
                    None
                }
            })
            .unwrap_or(0);
        let mut have = buf.len() - (header_end + 4);
        while have < content_length {
            match socket.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => have += n,
            }
        }
        // Full body read; drop the socket without writing any response —
        // the deterministic post-ingest transport-death fault.
    }

    /// Ingest-then-drop fake store: accepts connections in a LOOP,
    /// reading one full request per connection then dropping it without a
    /// response. The loop is the non-vacuity linchpin (plan edge case): if
    /// the fake store stopped accepting after one connection, a mutated
    /// replay would hit connect-refused (silently retried by
    /// `classify_push_send`'s safe arm) and `hits` would never move past 1,
    /// masking the exact mutation this test exists to kill.
    async fn serve_ingest_then_drop(
        listener: tokio::net::TcpListener,
        hits: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            read_full_request_then_drop(&mut socket).await;
            hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            drop(socket);
        }
    }

    /// A [`Ctx`] whose `base_url` and `loki_url` both point at the fake
    /// ingest-then-drop store, so the real `push_sm_corpus` fans its very
    /// first push attempt at it; `collector_url`/`prometheus_url`/
    /// `tempo_url` are unroutable placeholders this test never touches, and
    /// `compose` is never invoked.
    fn faulty_store_ctx(addr: std::net::SocketAddr) -> Ctx {
        let fake_url = format!("http://{addr}");
        Ctx {
            http: reqwest::Client::new(),
            base_url: fake_url.clone(),
            collector_url: "http://127.0.0.1:1".to_string(),
            prometheus_url: "http://127.0.0.1:1".to_string(),
            tempo_url: "http://127.0.0.1:1".to_string(),
            loki_url: fake_url,
            variant: crate::scenarios::Variant::Single,
            fixtures_dir: crate::engine::workspace_root().join("test/fixtures"),
            compose: crate::engine::Compose::new(
                crate::engine::EngineKind::Docker,
                vec![],
                "sm-retry-guard-test",
            ),
        }
    }

    /// Resolves once `hits` reaches 2 — a replayed body was counted. Never
    /// resolves under correct code (the terminal arm never resends), so it
    /// only ever wins the `select!` race below when the mutation under test
    /// is present.
    async fn hits_reached_two(hits: &std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        loop {
            if hits.load(std::sync::atomic::Ordering::SeqCst) >= 2 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// The un-deferred retry-differential (issue #102, replaces the FAIL'd
    /// retro-review's TEST GAP): drives the REAL `push_sm_corpus` against a
    /// loopback store that deterministically reproduces "server ingested
    /// the body, then the transport died before the response" — an
    /// ambiguous post-ingest fault that #105's `classify_push_send` fails
    /// fast on rather than retries. Proves the lane's actual wiring, not
    /// just the classifier in isolation (harness.rs:421,451).
    #[tokio::test]
    async fn sm_push_lane_cannot_replay_the_corpus_on_an_ambiguous_post_ingest_failure() {
        let corpus = logs_sm_corpus::generate(&logs_sm_corpus::SmCorpusSpec {
            base_ns: 1_700_000_000_000_000_000,
            run_id: "e2e-logs-sm-retry-guard".to_string(),
        });

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let accept_task = tokio::spawn(serve_ingest_then_drop(listener, hits.clone()));

        let ctx = faulty_store_ctx(addr);

        let result = tokio::time::timeout(Duration::from_secs(30), async {
            tokio::select! {
                result = push_sm_corpus(&ctx, &corpus) => result,
                _ = hits_reached_two(&hits) => {
                    panic!(
                        "the SM push lane replayed an already-sent body (hits >= 2) — the \
                         idempotency guard did not hold end-to-end"
                    );
                }
            }
        })
        .await
        .expect("push_sm_corpus (or the replay watchdog) did not resolve within 30s");

        let err = result.expect_err(
            "an ambiguous post-ingest transport failure must fail the push, not succeed",
        );
        let chain = format!("{err:#}");
        assert!(
            chain.contains("idempotency guard, issue #105"),
            "expected the #105 terminal-arm context in the error chain, got: {chain}"
        );
        assert_eq!(
            hits.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "only the first body of the first store must ever reach the fake store"
        );

        // Liveness ceiling (plan step 6): a late replay after resolution is
        // exactly the defect a background/spawned resend would produce. The
        // accept task MUST stay alive through this window (review fix): if
        // the listener were dropped before the settle, a late resend would
        // hit connect-refused instead of being counted, making this exact
        // assertion vacuous against the stray-background-resend mutation.
        tokio::time::sleep(COLLECTOR_READY_POLL_INTERVAL * 4).await;
        assert_eq!(
            hits.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "no late/background resend after the terminal failure (2s settle)"
        );

        accept_task.abort();
    }

    // ---------------------------------------------------------------
    // Issue #204: the oracle-less cluster differential — the reference
    // store's completeness probe is NEVER built/indexed/queried on the
    // cluster leg (helper-level floors + the two dispatch-site floors).
    // ---------------------------------------------------------------

    /// AC2 (part): `oracle_present` is single-only.
    #[test]
    fn oracle_present_is_true_only_on_the_single_overlay() {
        let single = probe_ctx(
            "http://127.0.0.1:1".to_string(),
            "http://127.0.0.1:1".to_string(),
            crate::scenarios::Variant::Single,
        );
        let cluster = probe_ctx(
            "http://127.0.0.1:1".to_string(),
            "http://127.0.0.1:1".to_string(),
            crate::scenarios::Variant::Cluster,
        );
        assert!(oracle_present(&single));
        assert!(!oracle_present(&cluster));
    }

    /// AC7 (helper floor): the cluster store list excludes the oracle slot,
    /// so `sets[1]`/`query_loki` are structurally unreachable; single still
    /// fans to both, in stable order (Pulsus index 0).
    #[test]
    fn cluster_completeness_selects_pulsus_only() {
        let stores = completeness_stores(false);
        assert_eq!(stores, &[CompletenessStore::Pulsus]);
        assert!(!stores.contains(&CompletenessStore::Oracle));
        assert_eq!(
            completeness_stores(true),
            &[CompletenessStore::Pulsus, CompletenessStore::Oracle]
        );
    }

    /// AC2b: the progress `reached` counter reads PulsusDB alone oracle-less,
    /// else the min of both.
    #[test]
    fn oracle_less_progress_counts_pulsus_only() {
        assert_eq!(completeness_reached(7, None), 7);
        assert_eq!(completeness_reached(7, Some(4)), 4);
        assert_eq!(completeness_reached(3, Some(9)), 3);
    }

    /// A [`Ctx`] with an explicit variant + PulsusDB/oracle URLs; the
    /// collector/prometheus/tempo URLs are unroutable placeholders these
    /// tests never touch, and `compose` is never invoked.
    fn probe_ctx(base_url: String, loki_url: String, variant: crate::scenarios::Variant) -> Ctx {
        Ctx {
            http: reqwest::Client::new(),
            base_url,
            collector_url: "http://127.0.0.1:1".to_string(),
            prometheus_url: "http://127.0.0.1:1".to_string(),
            tempo_url: "http://127.0.0.1:1".to_string(),
            loki_url,
            variant,
            fixtures_dir: crate::engine::workspace_root().join("test/fixtures"),
            compose: crate::engine::Compose::new(
                crate::engine::EngineKind::Docker,
                vec![],
                "logs-cluster-oracle-guard-test",
            ),
        }
    }

    /// Inverse of [`result_set`]: serializes a corpus expectation back into
    /// the `query_range` streams wire shape the PulsusDB stub returns, so a
    /// single poll of `wait_for_completeness` sees the full set and succeeds.
    fn streams_response_json(expected: &ExpectedResult) -> serde_json::Value {
        let result: Vec<serde_json::Value> = expected
            .iter()
            .map(|(labels, entries)| {
                let values: Vec<serde_json::Value> = entries
                    .iter()
                    .map(|(ts, line)| serde_json::json!([ts.to_string(), line]))
                    .collect();
                serde_json::json!({ "stream": labels, "values": values })
            })
            .collect();
        serde_json::json!({ "data": { "resultType": "streams", "result": result } })
    }

    /// Serves `body` as a well-formed HTTP/1.1 200 JSON response for every
    /// connection on an ephemeral loopback port — the stand-in PulsusDB
    /// query backend (mirrors `metrics::spawn_stub_backend`).
    async fn spawn_json_stub(body: String) -> std::net::SocketAddr {
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let body = body.clone();
                tokio::spawn(async move {
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len(),
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.shutdown().await;
                });
            }
        });
        addr
    }

    /// Counts inbound connections then drops them — the oracle endpoint that
    /// must NEVER be contacted on the cluster leg. A single connection is the
    /// regression signal (no body read needed).
    async fn count_connections(
        listener: tokio::net::TcpListener,
        hits: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            drop(socket);
        }
    }

    /// Resolves the instant `hits` reaches 1 — never under correct code
    /// (the oracle is off the cluster store list), so it only wins the
    /// `select!` below when an unconditional oracle probe is re-introduced.
    async fn oracle_hit_watchdog(hits: &std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        loop {
            if hits.load(std::sync::atomic::Ordering::SeqCst) >= 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    fn tiny_cluster_corpus(run_id: &str) -> LogCorpus {
        logs_corpus::generate(&LogCorpusSpec {
            scale: Scale::Ci,
            record_count: 3,
            step_ns: 1_000_000_000,
            base_ns: 1_700_000_000_000_000_000,
            run_id: run_id.to_string(),
        })
    }

    /// AC7b (dispatch-site floor): driving the REAL `wait_for_completeness`
    /// on a `Variant::Cluster` Ctx completes `Ok` using PulsusDB only and
    /// leaves `oracle_hits == 0`. A regression re-adding an unconditional
    /// oracle probe (`sets[1]`/`query_loki`) trips the `select!` watchdog.
    #[tokio::test]
    async fn cluster_wait_for_completeness_never_contacts_oracle() {
        let corpus = tiny_cluster_corpus("e2e-logs-cluster-oracle-guard");
        let window = query_window(&corpus);
        let expected = corpus.expected_all_records();
        let body = streams_response_json(&expected).to_string();
        let pulsus_addr = spawn_json_stub(body).await;

        let oracle_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let oracle_addr = oracle_listener.local_addr().unwrap();
        let oracle_hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let oracle_task = tokio::spawn(count_connections(oracle_listener, oracle_hits.clone()));

        let ctx = probe_ctx(
            format!("http://{pulsus_addr}"),
            format!("http://{oracle_addr}"),
            crate::scenarios::Variant::Cluster,
        );

        // limit well above the 3-record corpus (raw == distinct == 3 < limit),
        // so the first poll's `sets[0] == expected` returns Ok immediately.
        let result = tokio::time::timeout(Duration::from_secs(30), async {
            tokio::select! {
                r = wait_for_completeness(&ctx, &corpus, window, 1000) => r,
                _ = oracle_hit_watchdog(&oracle_hits) => panic!(
                    "oracle endpoint contacted on the cluster leg — an unconditional oracle probe \
                     was re-introduced"
                ),
            }
        })
        .await
        .expect("wait_for_completeness did not resolve within 30s");
        result.expect("cluster completeness must succeed using PulsusDB only");
        assert_eq!(
            oracle_hits.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "oracle must never be contacted on cluster"
        );
        oracle_task.abort();
    }

    /// AC7b (dispatch-site floor, second call site): the REAL
    /// `completeness_timeout_diagnostic` on a cluster Ctx iterates the
    /// PulsusDB-only store list, so it never fires `query_loki` and emits no
    /// `"oracle"` key. The PulsusDB probe fails fast (unroutable base_url,
    /// recorded as an error entry) — the point is `oracle_hits == 0`.
    #[tokio::test]
    async fn cluster_completeness_timeout_diagnostic_omits_oracle() {
        let corpus = tiny_cluster_corpus("e2e-logs-cluster-oracle-diag");
        let window = query_window(&corpus);
        let expected = corpus.expected_all_records();

        let oracle_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let oracle_addr = oracle_listener.local_addr().unwrap();
        let oracle_hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let oracle_task = tokio::spawn(count_connections(oracle_listener, oracle_hits.clone()));

        let ctx = probe_ctx(
            "http://127.0.0.1:1".to_string(),
            format!("http://{oracle_addr}"),
            crate::scenarios::Variant::Cluster,
        );
        let q = run_scope_query(&corpus.run_id);
        let probe = CompletenessProbe {
            q: &q,
            window,
            limit: 1000,
            query_timeout: Duration::from_secs(2),
        };

        let _ = completeness_timeout_diagnostic(
            &ctx,
            "logs_pipeline_completeness",
            "completeness-timeout-cluster-test",
            &probe,
            &expected,
            anyhow::anyhow!("synthetic timeout for the cluster no-oracle diagnostic test"),
        )
        .await;

        assert_eq!(
            oracle_hits.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the timeout diagnostic must never contact the oracle on cluster"
        );
        oracle_task.abort();
    }
}
