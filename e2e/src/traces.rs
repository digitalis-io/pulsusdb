//! The M4 milestone gate (docs/features.md §7, issue #60 architect plan
//! v1 + v2 deltas): the collector traces pipeline e2e plus the runtime
//! differential against a pinned reference Tempo.
//!
//! [`traces_roundtrip`] (both variants, no Tempo) pushes the
//! deterministic corpus (`traces_corpus.rs`) **once** as OTLP/HTTP JSON
//! through the real collector to `POST /v1/traces`, then asserts every
//! native `/api/traces/v1` endpoint round-trips it: trace-by-ID span
//! sets, search trace-ID set (with `metrics.partial == false`),
//! tags/tag-values containing the corpus's known keys/values, and
//! TraceQL-metrics counts reconciling with the ingested span count; the
//! cluster leg adds the shard-local `trace_spans` count-sum check
//! (mirrors `scenarios::assert_shard_local_row_counts`).
//!
//! [`traces_differential`] (single variant only — task-manager
//! adjudication 1 on #60) pushes its own corpus once through the
//! collector, which fans it out to PulsusDB (`otlphttp`) and the pinned
//! Tempo (`otlp/tempo`) as **identical typed wire data**, then gates on
//! the two ratified deterministic surfaces. The reference is
//! `grafana/tempo:3.0.2`, pinned by tag AND digest in
//! `deploy/e2e/compose.single.yaml` (adjudication 2, the
//! `prom/prometheus:v3.13.0` precedent — upgrades are deliberate
//! commits, never floating; config notes in `deploy/e2e/tempo.yaml`):
//!
//! - **trace-by-ID (hard):** for every seeded id, PulsusDB's span-ID set
//!   == the corpus's by-construction set == Tempo's (sets of hex ids,
//!   never order; plan v2 delta 4). Structural fields (`name`, `kind`,
//!   `parentSpanId`, `startTimeUnixNano`) are a separate INFORMATIONAL
//!   comparison, never bundled into the gate.
//! - **search (hard):** for each committed `mode: "gated"` case in
//!   `test/fixtures/traces/differential.json`, over an identical
//!   snapped window, PulsusDB's trace-ID set == the corpus expected set
//!   == Tempo's — with the validity gates asserted first (ours:
//!   `metrics.partial == false`; Tempo's, plan v2 delta 5: result count
//!   strictly below the requested limit AND set-equality with the
//!   corpus, classified as oracle invalidity/divergence).
//!
//! Cases the ledger (docs/benchmarks/traces-differential-ledger.md) has
//! ratified as documented cross-store differences run `mode:
//! "informational"`: PulsusDB is still hard-gated against the corpus
//! expectation (our own documented semantics), but the Tempo delta is
//! dumped as an artifact instead of failing. Tags-vs-Tempo and
//! metrics-vs-Tempo comparisons are always informational (ratified on
//! #19). Any gating mismatch dumps a minimal repro under
//! `target/e2e-artifacts/traces-diff/<variant>/` (the #33 pattern) and
//! fails the scenario.

use std::cell::Cell;
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::corpus::Scale;
use crate::harness::{
    classify_push_send, completeness_poll_timeout, poll_until, query_request_timeout,
};
use crate::metrics::write_artifact;
use crate::scenarios::{Ctx, Variant};
use crate::traces_corpus::{self, TraceCorpus, TraceCorpusSpec, hex};

const FIXTURE_PATH: &str = "traces/differential.json";
/// Artifact area under `target/e2e-artifacts/` (the #33 pattern;
/// `metrics-diff` is the metrics scenario's).
const ARTIFACT_AREA: &str = "traces-diff";

/// Collector readiness poll bounds (issue #15/#33 precedent): only the
/// first per-trace OTLP export retries past a not-yet-listening
/// collector.
const COLLECTOR_READY_POLL_TIMEOUT: Duration = Duration::from_secs(90);
const COLLECTOR_READY_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Completeness pre-check poll bounds — generous enough to absorb the
/// cluster leg's `_dist` fan-out lag and Tempo's live-store search
/// visibility lag (`poll_until` returns the instant the condition is
/// met, so the long deadline costs nothing on a healthy run). The
/// deadline is tier-aware (issue #106,
/// `harness::completeness_poll_timeout`): 600s full / 180s ci.
const COMPLETENESS_POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Progress-log rate limit (issue #106): between unchanged reached-counts,
/// emit at most one completeness line per this interval.
const COMPLETENESS_PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(3);

/// Margin between the corpus's last span end and "now" at generation
/// time, and the search-window slack on each side (both stores get the
/// same snapped unix-second bounds).
const CORPUS_NOW_MARGIN_NS: i64 = 5_000_000_000;
const WINDOW_SLACK_S: i64 = 3_600;

// ---------------------------------------------------------------------
// Fixture (the committed coverage matrix — plan v2 deltas 2+3)
// ---------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct TierCounts {
    trace_count: usize,
}

#[derive(Debug, Deserialize)]
struct CaseRaw {
    case_id: String,
    q: String,
    /// Which committed M4 TraceQL construct this case covers — carried
    /// in the fixture as documentation; validated non-empty by a unit
    /// test.
    construct: String,
    attr_type: String,
    /// `"gated"` or `"informational"` — informational requires a
    /// `ledger` entry id (unit-tested against the committed ledger).
    mode: String,
    #[serde(default)]
    ledger: Option<String>,
}

#[derive(Debug, Deserialize)]
struct InformationalRaw {
    metrics_queries: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct TracesFixture {
    seed: u64,
    step_ns: i64,
    ci: TierCounts,
    full: TierCounts,
    limit: u32,
    cases: Vec<CaseRaw>,
    informational: InformationalRaw,
}

fn load_fixture(ctx: &Ctx) -> Result<TracesFixture> {
    let path = ctx.fixtures_dir.join(FIXTURE_PATH);
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read fixture {}", path.display()))?;
    let fixture: TracesFixture = serde_json::from_str(&raw)
        .with_context(|| format!("fixture {} was not valid JSON", path.display()))?;
    // Runtime belt-and-suspenders on top of the hermetic drift test: an
    // unknown case id would silently produce an empty expected set and
    // fail the gate with a confusing message.
    for case in &fixture.cases {
        if !traces_corpus::CASE_IDS.contains(&case.case_id.as_str()) {
            bail!(
                "fixture {} names case {:?}, which the corpus does not label",
                path.display(),
                case.case_id
            );
        }
    }
    Ok(fixture)
}

/// The pure core of `PULSUS_E2E_TRACES_SCALE` parsing (the
/// `metrics::parse_scale` pattern — no `unsafe` env mutation in tests).
fn parse_traces_scale(raw: Option<&str>) -> Result<Scale> {
    match raw {
        None => Ok(Scale::Ci),
        Some(v) if v.eq_ignore_ascii_case("ci") => Ok(Scale::Ci),
        Some(v) if v.eq_ignore_ascii_case("full") => Ok(Scale::Full),
        Some(other) => bail!("PULSUS_E2E_TRACES_SCALE={other:?} must be \"ci\" or \"full\""),
    }
}

fn resolve_scale() -> Result<Scale> {
    match std::env::var("PULSUS_E2E_TRACES_SCALE") {
        Ok(v) => parse_traces_scale(Some(&v)),
        Err(std::env::VarError::NotPresent) => parse_traces_scale(None),
        Err(std::env::VarError::NotUnicode(raw)) => {
            bail!("PULSUS_E2E_TRACES_SCALE was not valid UTF-8: {raw:?}")
        }
    }
}

fn now_unix_nanos() -> Result<i64> {
    let dur = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?;
    i64::try_from(dur.as_nanos()).context("current time does not fit in i64 nanoseconds")
}

/// Builds a corpus whose last span ends comfortably before "now" (both
/// stores reject/mistreat future timestamps; staleness of a few seconds
/// is irrelevant to set semantics).
fn build_corpus(fixture: &TracesFixture, scale: Scale, run_prefix: &str) -> Result<TraceCorpus> {
    let trace_count = match scale {
        Scale::Ci => fixture.ci.trace_count,
        Scale::Full => fixture.full.trace_count,
    };
    let run_id = format!("{run_prefix}-{:x}", crate::metrics::unique_id()?);
    let now_ns = now_unix_nanos()?;
    let base_ns = now_ns - fixture.step_ns * trace_count as i64 - CORPUS_NOW_MARGIN_NS;
    let spec = TraceCorpusSpec {
        seed: fixture.seed,
        scale,
        trace_count,
        step_ns: fixture.step_ns,
        base_ns,
        run_id,
    };
    Ok(traces_corpus::generate(&spec))
}

/// The identical snapped unix-second search window both stores are
/// queried over.
#[derive(Debug, Clone, Copy)]
struct SearchWindow {
    start_s: i64,
    end_s: i64,
}

fn search_window(corpus: &TraceCorpus) -> SearchWindow {
    SearchWindow {
        start_s: corpus.first_ts_ns / 1_000_000_000 - WINDOW_SLACK_S,
        end_s: corpus.last_ts_ns / 1_000_000_000 + WINDOW_SLACK_S,
    }
}

// ---------------------------------------------------------------------
// Corpus push (through the collector — the real wire path)
// ---------------------------------------------------------------------

/// One `POST {collector_url}/v1/traces` attempt — the shape
/// `scenarios::post_otlp_logs` established (issue #15), routed through
/// [`classify_push_send`] (issue #105): `Ok(Some(Ok(response)))` once the
/// request reaches the collector, a connect-phase `Err` retried by
/// [`poll_until`], and `Ok(Some(Err(_)))` on a post-connect failure (fail
/// fast — the body may have been ingested).
async fn post_otlp_traces(
    ctx: &Ctx,
    payload: &serde_json::Value,
) -> Result<Option<Result<reqwest::Response>>> {
    classify_push_send(
        ctx.http
            .post(format!("{}/v1/traces", ctx.collector_url))
            .json(payload)
            .send()
            .await,
    )
}

/// Pushes one export request per trace; only the first request polls
/// past a not-yet-listening collector (issue #33's `push_corpus` shape).
async fn push_trace_corpus(ctx: &Ctx, corpus: &TraceCorpus) -> Result<()> {
    let requests = traces_corpus::to_otlp_export_requests(corpus);
    let (first, rest) = requests
        .split_first()
        .context("corpus produced no OTLP export requests to push")?;

    let res = poll_until(
        COLLECTOR_READY_POLL_TIMEOUT,
        COLLECTOR_READY_POLL_INTERVAL,
        || post_otlp_traces(ctx, first),
    )
    .await
    .context("collector otlp/v1/traces endpoint never accepted a connection")??;
    if !res.status().is_success() {
        bail!(
            "collector otlp/v1/traces export (trace 0) returned {}",
            res.status()
        );
    }
    for (i, req) in rest.iter().enumerate() {
        // Non-retrying (single `.await`): a connect `Err` propagates, and a
        // post-connect failure surfaces via the inner `Result` — neither
        // resends. `classify_push_send` never yields `Ok(None)`.
        let res = post_otlp_traces(ctx, req)
            .await?
            .expect("classify_push_send never yields Ok(None)")
            .with_context(|| {
                format!(
                    "collector otlp/v1/traces export (trace index {}) failed",
                    i + 1
                )
            })?;
        if !res.status().is_success() {
            bail!(
                "collector otlp/v1/traces export (trace index {}) returned {}",
                i + 1,
                res.status()
            );
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------
// Wire-shape normalization (per-store extraction to comparable sets)
// ---------------------------------------------------------------------

/// Left-pads a hex trace id to the canonical 32 chars (Tempo's search
/// response strips leading zeros; PulsusDB always emits 32).
fn normalize_trace_id_hex(raw: &str) -> String {
    let lowered = raw.to_ascii_lowercase();
    if lowered.len() >= 32 {
        lowered
    } else {
        format!("{}{lowered}", "0".repeat(32 - lowered.len()))
    }
}

/// Decodes one wire id (span or trace) to lowercase hex of
/// `expected_bytes`: PulsusDB emits protojson hex; Tempo's trace-by-ID
/// JSON is gogoproto jsonpb, which base64-encodes bytes fields. A
/// `2 * expected_bytes`-char all-hex string is hex; anything else is
/// base64 (the two encodings' lengths never coincide: 8 bytes -> 16 hex
/// vs 12 base64 chars, 16 bytes -> 32 vs 24).
fn decode_wire_id(raw: &str, expected_bytes: usize) -> Result<String> {
    if raw.len() == expected_bytes * 2 && raw.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Ok(raw.to_ascii_lowercase());
    }
    let bytes =
        base64_decode(raw).with_context(|| format!("id {raw:?} is neither hex nor base64"))?;
    if bytes.len() != expected_bytes {
        bail!(
            "id {raw:?} decoded to {} bytes, expected {expected_bytes}",
            bytes.len()
        );
    }
    Ok(hex(&bytes))
}

/// Minimal standard-alphabet base64 decoder (with `=` padding) — this
/// crate stays dependency-light (the `pulsus-write::base64_encode`
/// "duplicate a small wire-format helper" precedent).
fn base64_decode(s: &str) -> Result<Vec<u8>> {
    fn val(b: u8) -> Result<u32> {
        Ok(match b {
            b'A'..=b'Z' => (b - b'A') as u32,
            b'a'..=b'z' => (b - b'a' + 26) as u32,
            b'0'..=b'9' => (b - b'0' + 52) as u32,
            b'+' => 62,
            b'/' => 63,
            other => bail!("invalid base64 byte {other:#x}"),
        })
    }
    let trimmed = s.trim_end_matches('=');
    let mut out = Vec::with_capacity(trimmed.len() * 3 / 4);
    for chunk in trimmed.as_bytes().chunks(4) {
        if chunk.len() < 2 {
            bail!("truncated base64 input {s:?}");
        }
        let mut acc = 0u32;
        for (i, &b) in chunk.iter().enumerate() {
            acc |= val(b)? << (18 - 6 * i);
        }
        out.push((acc >> 16) as u8);
        if chunk.len() > 2 {
            out.push((acc >> 8) as u8);
        }
        if chunk.len() > 3 {
            out.push(acc as u8);
        }
    }
    Ok(out)
}

/// Iterates every span object in an OTLP-shaped trace body — PulsusDB's
/// `resourceSpans[].scopeSpans[].spans[]` or Tempo's
/// `batches[].scopeSpans[].spans[]` (same inner shape).
fn each_span(body: &serde_json::Value) -> impl Iterator<Item = &serde_json::Value> {
    ["resourceSpans", "batches"]
        .into_iter()
        .filter_map(|key| body[key].as_array())
        .flatten()
        .filter_map(|rs| rs["scopeSpans"].as_array())
        .flatten()
        .filter_map(|ss| ss["spans"].as_array())
        .flatten()
}

/// The span-ID set (lowercase hex) of one fetched trace body — the
/// trace-by-ID hard gate's per-store projection (plan v2 delta 4).
fn fetch_span_ids(body: &serde_json::Value) -> Result<BTreeSet<String>> {
    let mut out = BTreeSet::new();
    for span in each_span(body) {
        let raw = span["spanId"]
            .as_str()
            .with_context(|| format!("span missing a spanId string: {span}"))?;
        out.insert(decode_wire_id(raw, 8)?);
    }
    Ok(out)
}

/// The span-id set shortfall of one fetched trace against its corpus
/// expectation (issue #106 fetch-completeness diagnostic core): which
/// expected span-ids are absent from the store and which unexpected ones
/// are present. Unit-tested so the on-timeout artifact's missing/extra
/// span sets are known correct before the nightly next fails.
struct SpanIdSetDiff {
    /// Expected span-ids absent from the store — the records CI needs.
    missing: Vec<String>,
    /// Span-ids in the store but not expected — an unexpected delivery.
    extra: Vec<String>,
}

fn span_id_set_diff(expected: &BTreeSet<String>, actual: &BTreeSet<String>) -> SpanIdSetDiff {
    SpanIdSetDiff {
        missing: expected.difference(actual).cloned().collect(),
        extra: actual.difference(expected).cloned().collect(),
    }
}

/// OTEL SpanKind normalization: PulsusDB's protojson emits the enum
/// number, Tempo's jsonpb emits `SPAN_KIND_*` names.
fn normalize_kind(v: &serde_json::Value) -> i64 {
    if let Some(n) = v.as_i64() {
        return n;
    }
    match v.as_str().unwrap_or_default() {
        "SPAN_KIND_INTERNAL" => 1,
        "SPAN_KIND_SERVER" => 2,
        "SPAN_KIND_CLIENT" => 3,
        "SPAN_KIND_PRODUCER" => 4,
        "SPAN_KIND_CONSUMER" => 5,
        _ => 0,
    }
}

/// One span's INFORMATIONAL structural tuple: `(spanId, name, kind,
/// parentSpanId, startTimeUnixNano)` — direct OTLP passthrough both
/// stores preserve, compared outside the hard gate (plan v2 delta 4). A
/// missing/all-zero parent normalizes to `""` (Tempo omits the field on
/// roots).
fn fetch_structural_tuples(body: &serde_json::Value) -> Result<BTreeSet<String>> {
    let mut out = BTreeSet::new();
    for span in each_span(body) {
        let span_id = decode_wire_id(span["spanId"].as_str().unwrap_or_default(), 8)?;
        let parent = match span["parentSpanId"].as_str() {
            None | Some("") => String::new(),
            Some(raw) => {
                let decoded = decode_wire_id(raw, 8)?;
                if decoded == "0000000000000000" {
                    String::new()
                } else {
                    decoded
                }
            }
        };
        let name = span["name"].as_str().unwrap_or_default();
        let kind = normalize_kind(&span["kind"]);
        let start = span["startTimeUnixNano"].as_str().unwrap_or_default();
        out.insert(format!("{span_id}|{name}|{kind}|{parent}|{start}"));
    }
    Ok(out)
}

/// `traces[].traceID` as a normalized hex set — both stores' search
/// response shape (PulsusDB: docs/api.md §4.2; Tempo: `/api/search`; an
/// absent `traces` key is an empty result on both).
fn search_trace_ids(body: &serde_json::Value) -> Result<BTreeSet<String>> {
    let Some(traces) = body["traces"].as_array() else {
        return Ok(BTreeSet::new());
    };
    traces
        .iter()
        .map(|t| {
            t["traceID"]
                .as_str()
                .map(normalize_trace_id_hex)
                .with_context(|| format!("search result missing a traceID string: {t}"))
        })
        .collect()
}

/// The RAW `traces[]` array length of a search response — pre-dedup,
/// the number the limit-truncation validity gate must be judged on
/// (issue #60 code review: `BTreeSet` cardinality can undercount a
/// duplicate-carrying response and mask truncation at the limit).
fn search_raw_result_count(body: &serde_json::Value) -> usize {
    body["traces"].as_array().map(Vec::len).unwrap_or(0)
}

/// PulsusDB's `metrics.partial` — the search validity gate asserted
/// before any set comparison (a truncated response compares a top-K,
/// not a set).
fn pulsus_search_is_partial(body: &serde_json::Value) -> Result<bool> {
    body["metrics"]["partial"]
        .as_bool()
        .with_context(|| format!("search response missing metrics.partial: {body}"))
}

// ---------------------------------------------------------------------
// Per-store HTTP surfaces
// ---------------------------------------------------------------------

/// One PulsusDB trace fetch attempt: `Ok(Some(body))` on 200,
/// `Ok(None)` on 404 (not visible yet — the poll-until condition),
/// `Err` on anything else.
async fn fetch_pulsus_trace(
    ctx: &Ctx,
    trace_hex: &str,
    query_timeout: Duration,
) -> Result<Option<serde_json::Value>> {
    let res = ctx
        .http
        .get(ctx.url(&format!("/api/traces/v1/trace/{trace_hex}/json")))
        // Issue #92 (every GET query chokepoint in this module): a
        // request-level timeout replaces the shared client's 5s
        // readiness budget for scenario queries. Tier-aware (issue #106,
        // `harness::query_request_timeout`): 120s full / 60s ci.
        .timeout(query_timeout)
        .send()
        .await
        .context("GET /api/traces/v1/trace/{id}/json failed")?;
    match res.status() {
        s if s.is_success() => Ok(Some(
            res.json().await.context("pulsus trace body was not JSON")?,
        )),
        reqwest::StatusCode::NOT_FOUND => Ok(None),
        s => bail!("pulsus trace fetch for {trace_hex} returned {s}"),
    }
}

/// One Tempo trace fetch attempt — same contract as
/// [`fetch_pulsus_trace`]; transport errors are surfaced as `Err`
/// (tolerated and retried by `poll_until` while Tempo finishes
/// booting).
async fn fetch_tempo_trace(
    ctx: &Ctx,
    trace_hex: &str,
    query_timeout: Duration,
) -> Result<Option<serde_json::Value>> {
    let res = ctx
        .http
        .get(format!("{}/api/traces/{trace_hex}", ctx.tempo_url))
        .timeout(query_timeout) // issue #92/#106, see fetch_pulsus_trace
        .send()
        .await
        .context("GET tempo /api/traces/{id} failed")?;
    match res.status() {
        s if s.is_success() => Ok(Some(
            res.json().await.context("tempo trace body was not JSON")?,
        )),
        reqwest::StatusCode::NOT_FOUND => Ok(None),
        s => bail!("tempo trace fetch for {trace_hex} returned {s}"),
    }
}

async fn search_pulsus(
    ctx: &Ctx,
    q: &str,
    window: SearchWindow,
    limit: u32,
    query_timeout: Duration,
) -> Result<serde_json::Value> {
    let start = window.start_s.to_string();
    let end = window.end_s.to_string();
    let limit_s = limit.to_string();
    let res = ctx
        .http
        .get(ctx.url("/api/traces/v1/search"))
        .query(&[
            ("q", q),
            ("start", start.as_str()),
            ("end", end.as_str()),
            ("limit", limit_s.as_str()),
        ])
        .timeout(query_timeout) // issue #92/#106, see fetch_pulsus_trace
        .send()
        .await
        .context("GET /api/traces/v1/search failed")?;
    if !res.status().is_success() {
        let status = res.status();
        let body = res.text().await.unwrap_or_default();
        bail!("pulsus search for {q:?} returned {status}: {body}");
    }
    res.json().await.context("pulsus search body was not JSON")
}

async fn search_tempo(
    ctx: &Ctx,
    q: &str,
    window: SearchWindow,
    limit: u32,
    query_timeout: Duration,
) -> Result<serde_json::Value> {
    let start = window.start_s.to_string();
    let end = window.end_s.to_string();
    let limit_s = limit.to_string();
    let res = ctx
        .http
        .get(format!("{}/api/search", ctx.tempo_url))
        .query(&[
            ("q", q),
            ("start", start.as_str()),
            ("end", end.as_str()),
            ("limit", limit_s.as_str()),
        ])
        .timeout(query_timeout) // issue #92/#106, see fetch_pulsus_trace
        .send()
        .await
        .context("GET tempo /api/search failed")?;
    if !res.status().is_success() {
        let status = res.status();
        let body = res.text().await.unwrap_or_default();
        bail!("tempo search for {q:?} returned {status}: {body}");
    }
    res.json().await.context("tempo search body was not JSON")
}

// ---------------------------------------------------------------------
// Completeness pre-checks (bounded polls, no fixed sleeps)
// ---------------------------------------------------------------------

/// Which stores a completeness wait covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stores {
    PulsusOnly,
    PulsusAndTempo,
}

impl Stores {
    fn covers_tempo(self) -> bool {
        matches!(self, Stores::PulsusAndTempo)
    }
}

/// Per-attempt trace-completeness progress line (issue #106), rate-limited
/// like the logs gate: the "still filling" path was silent every poll, so
/// CI could not tell a real convergence bug from budget. `tempo` is
/// `None` on the PulsusOnly leg.
fn log_trace_completeness_progress(
    last: &Cell<(usize, usize)>,
    last_log_at: &Cell<Instant>,
    label: &str,
    total: usize,
    pulsus: usize,
    tempo: Option<usize>,
) {
    let now = Instant::now();
    let key = (pulsus, tempo.unwrap_or(usize::MAX));
    if last.get() != key
        || now.duration_since(last_log_at.get()) >= COMPLETENESS_PROGRESS_LOG_INTERVAL
    {
        match tempo {
            Some(t) => {
                let reached = pulsus.min(t);
                println!(
                    "pulsus-e2e:   {label} completeness: reached {reached}/{total}: \
                     pulsusdb={pulsus} tempo={t}"
                );
            }
            None => println!(
                "pulsus-e2e:   {label} completeness: reached {pulsus}/{total}: pulsusdb={pulsus}"
            ),
        }
        last.set(key);
        last_log_at.set(now);
    }
}

/// Polls until every seeded trace is fetchable by id with the expected
/// **span count** on every covered store (count, not set: the set
/// equality itself is the gate, asserted once, with an artifact on
/// mismatch — this poll only absorbs ingest/visibility lag).
async fn wait_for_fetch_completeness(
    ctx: &Ctx,
    corpus: &TraceCorpus,
    stores: Stores,
) -> Result<()> {
    let total = corpus.traces.len();
    let query_timeout = query_request_timeout(corpus.scale);
    let last_reached = Cell::new(usize::MAX);
    let last_log_at = Cell::new(Instant::now());
    let poll_result = poll_until(
        completeness_poll_timeout(corpus.scale),
        COMPLETENESS_POLL_INTERVAL,
        || async {
            let reached = fetch_completeness_attempt(ctx, corpus, stores, query_timeout).await?;
            // Rate-limited joint progress (issue #106): the "still filling"
            // path was silent every poll. `reached` is the leading prefix
            // complete on every covered store (interleaved short-circuit,
            // kept cheap — the on-timeout diagnostic does the full per-store
            // scan once).
            let now = Instant::now();
            if last_reached.get() != reached
                || now.duration_since(last_log_at.get()) >= COMPLETENESS_PROGRESS_LOG_INTERVAL
            {
                println!(
                    "pulsus-e2e:   traces fetch completeness: reached {reached}/{total} \
                     (traces complete on {stores:?}, up to the first gap)"
                );
                last_reached.set(reached);
                last_log_at.set(now);
            }
            Ok((reached == total).then_some(()))
        },
    )
    .await;
    match poll_result {
        Ok(()) => Ok(()),
        Err(timeout_err) => Err(fetch_completeness_timeout_diagnostic(
            ctx,
            corpus,
            stores,
            query_timeout,
            timeout_err.context(format!(
                "run {:?} never reached trace-by-ID completeness ({total} traces) on {stores:?}",
                corpus.run_id,
            )),
        )
        .await),
    }
}

/// One cheap completeness attempt: the count of leading traces present
/// with the expected span count on EVERY covered store (interleaved
/// short-circuit — stops at the first gap on either store). A cheap
/// progress proxy that never fans an all-traces scan out per poll; the
/// on-timeout diagnostic does the full per-store scan once.
async fn fetch_completeness_attempt(
    ctx: &Ctx,
    corpus: &TraceCorpus,
    stores: Stores,
    query_timeout: Duration,
) -> Result<usize> {
    let mut reached = 0usize;
    for trace in &corpus.traces {
        let trace_hex = hex(&trace.trace_id);
        let Some(body) = fetch_pulsus_trace(ctx, &trace_hex, query_timeout).await? else {
            break;
        };
        if each_span(&body).count() != trace.spans.len() {
            break;
        }
        if stores.covers_tempo() {
            let Some(body) = fetch_tempo_trace(ctx, &trace_hex, query_timeout).await? else {
                break;
            };
            if each_span(&body).count() != trace.spans.len() {
                break;
            }
        }
        reached += 1;
    }
    Ok(reached)
}

/// On the trace-by-ID completeness timeout (issue #106): full per-store
/// scan recording, for each covered store, how many traces are present
/// with the right span count and which are missing / short — the artifact
/// CI needs to tell a visibility bug from budget.
async fn fetch_completeness_timeout_diagnostic(
    ctx: &Ctx,
    corpus: &TraceCorpus,
    stores: Stores,
    query_timeout: Duration,
    timeout_err: anyhow::Error,
) -> anyhow::Error {
    let mut store_reports = serde_json::Map::new();
    store_reports.insert(
        "pulsusdb".to_string(),
        fetch_store_report(ctx, corpus, false, query_timeout).await,
    );
    if stores.covers_tempo() {
        store_reports.insert(
            "tempo".to_string(),
            fetch_store_report(ctx, corpus, true, query_timeout).await,
        );
    }
    let artifact = serde_json::json!({
        "surface": "traces_fetch_completeness",
        "kind": "completeness_timeout",
        "run_id": corpus.run_id,
        "expected_traces": corpus.traces.len(),
        "stores": store_reports,
    });
    match write_artifact(ctx, ARTIFACT_AREA, "fetch-completeness-timeout", &artifact) {
        Ok(path) => timeout_err.context(format!(
            "trace-by-ID completeness timed out; per-store present/missing/short scan written to {}",
            path.display()
        )),
        Err(werr) => timeout_err.context(format!(
            "trace-by-ID completeness timed out; ALSO failed to write the diagnostic: {werr:#}"
        )),
    }
}

async fn fetch_store_report(
    ctx: &Ctx,
    corpus: &TraceCorpus,
    tempo: bool,
    query_timeout: Duration,
) -> serde_json::Value {
    let mut present = 0usize;
    let mut missing: Vec<String> = Vec::new();
    let mut short: Vec<serde_json::Value> = Vec::new();
    for trace in &corpus.traces {
        let trace_hex = hex(&trace.trace_id);
        let expected_ids = corpus.expected_span_ids(&trace_hex).unwrap_or_default();
        let fetched = if tempo {
            fetch_tempo_trace(ctx, &trace_hex, query_timeout).await
        } else {
            fetch_pulsus_trace(ctx, &trace_hex, query_timeout).await
        };
        match fetched {
            Ok(Some(body)) => match fetch_span_ids(&body) {
                Ok(actual_ids) => {
                    if actual_ids == expected_ids {
                        present += 1;
                    } else {
                        // Set-diff granularity (issue #106): the missing/extra
                        // span-ID sets distinguish a systematically dropped span
                        // from generic lag, matching the logs gate's set-diff.
                        let diff = span_id_set_diff(&expected_ids, &actual_ids);
                        short.push(serde_json::json!({
                            "trace_id": trace_hex,
                            "expected_spans": expected_ids.len(),
                            "got_spans": actual_ids.len(),
                            "missing_span_ids": diff.missing,
                            "extra_span_ids": diff.extra,
                        }));
                    }
                }
                Err(err) => missing.push(format!("{trace_hex} (span parse error: {err:#})")),
            },
            Ok(None) => missing.push(trace_hex),
            Err(err) => missing.push(format!("{trace_hex} (fetch error: {err:#})")),
        }
    }
    serde_json::json!({
        "present_count": present,
        "missing_count": missing.len(),
        "short_span_count": short.len(),
        "missing": missing,
        "short_span": short,
    })
}

/// Polls until the run-scoped search (`{ resource.run_id = "R" }`)
/// returns exactly the corpus's trace-id set on every covered store —
/// search visibility can lag trace-by-ID visibility (Tempo serves
/// by-ID from live traces before they become searchable; `_dist`
/// forwarding is eventually consistent on the cluster leg).
async fn wait_for_search_completeness(
    ctx: &Ctx,
    corpus: &TraceCorpus,
    window: SearchWindow,
    limit: u32,
    stores: Stores,
) -> Result<()> {
    let q = run_scope_query(&corpus.run_id);
    let all = corpus.trace_id_hexes();
    let total = all.len();
    let query_timeout = query_request_timeout(corpus.scale);
    let progress = Cell::new((usize::MAX, usize::MAX));
    let last_log_at = Cell::new(Instant::now());
    let poll_result = poll_until(
        completeness_poll_timeout(corpus.scale),
        COMPLETENESS_POLL_INTERVAL,
        || async {
            let pulsus_ids =
                search_trace_ids(&search_pulsus(ctx, &q, window, limit, query_timeout).await?)?;
            let pulsus_matched = pulsus_ids.intersection(&all).count();
            let pulsus_complete = pulsus_ids == all;
            // Short-circuit (issue #106): don't query Tempo every tick during
            // the saturated "still filling" phase — that doubles load on the
            // single node this fix exists to relieve. Only consult the oracle
            // once PulsusDB has already converged; the on-timeout diagnostic
            // re-queries both stores once for the artifact.
            let mut tempo_matched = None;
            let mut complete = pulsus_complete;
            if pulsus_complete && stores.covers_tempo() {
                let tempo_ids =
                    search_trace_ids(&search_tempo(ctx, &q, window, limit, query_timeout).await?)?;
                tempo_matched = Some(tempo_ids.intersection(&all).count());
                complete = tempo_ids == all;
            }
            log_trace_completeness_progress(
                &progress,
                &last_log_at,
                "traces search",
                total,
                pulsus_matched,
                tempo_matched,
            );
            Ok(complete.then_some(()))
        },
    )
    .await;
    match poll_result {
        Ok(()) => Ok(()),
        Err(timeout_err) => Err(search_completeness_timeout_diagnostic(
            ctx,
            corpus,
            &SearchProbe {
                q: &q,
                window,
                limit,
                stores,
                query_timeout,
            },
            &all,
            timeout_err.context(format!(
                "run {:?} never reached search completeness ({total} traces) on {stores:?}",
                corpus.run_id,
            )),
        )
        .await),
    }
}

/// The run-scoped search probe re-run on a search-completeness timeout —
/// bundled so the diagnostic fn stays within clippy's argument threshold.
struct SearchProbe<'a> {
    q: &'a str,
    window: SearchWindow,
    limit: u32,
    stores: Stores,
    query_timeout: Duration,
}

/// On the search completeness timeout (issue #106): re-query each covered
/// store once and record its trace-id count plus the missing/extra
/// symmetric difference vs the corpus set — the set-parity analog of the
/// logs completeness diagnostic.
async fn search_completeness_timeout_diagnostic(
    ctx: &Ctx,
    corpus: &TraceCorpus,
    probe: &SearchProbe<'_>,
    all: &BTreeSet<String>,
    timeout_err: anyhow::Error,
) -> anyhow::Error {
    let SearchProbe {
        q,
        window,
        limit,
        stores,
        query_timeout,
    } = *probe;
    let mut store_reports = serde_json::Map::new();
    store_reports.insert(
        "pulsusdb".to_string(),
        search_store_report(
            search_pulsus(ctx, q, window, limit, query_timeout).await,
            all,
        ),
    );
    if stores.covers_tempo() {
        store_reports.insert(
            "tempo".to_string(),
            search_store_report(
                search_tempo(ctx, q, window, limit, query_timeout).await,
                all,
            ),
        );
    }
    let artifact = serde_json::json!({
        "surface": "traces_search_completeness",
        "kind": "completeness_timeout",
        "run_id": corpus.run_id,
        "query": q,
        "expected_traces": all.len(),
        "stores": store_reports,
    });
    match write_artifact(ctx, ARTIFACT_AREA, "search-completeness-timeout", &artifact) {
        Ok(path) => timeout_err.context(format!(
            "search completeness timed out; per-store counts + missing/extra trace ids written to {}",
            path.display()
        )),
        Err(werr) => timeout_err.context(format!(
            "search completeness timed out; ALSO failed to write the diagnostic: {werr:#}"
        )),
    }
}

fn search_store_report(
    body: Result<serde_json::Value>,
    all: &BTreeSet<String>,
) -> serde_json::Value {
    let ids = match body.and_then(|b| search_trace_ids(&b)) {
        Ok(ids) => ids,
        Err(err) => return serde_json::json!({ "error": format!("search failed: {err:#}") }),
    };
    let missing: Vec<&String> = all.difference(&ids).collect();
    let extra: Vec<&String> = ids.difference(all).collect();
    serde_json::json!({
        "returned_count": ids.len(),
        "matched": ids.intersection(all).count(),
        "missing_count": missing.len(),
        "extra_count": extra.len(),
        "missing": missing,
        "extra": extra,
    })
}

fn run_scope_query(run_id: &str) -> String {
    format!(r#"{{ resource.{} = "{run_id}" }}"#, traces_corpus::RUN_ATTR)
}

// ---------------------------------------------------------------------
// Informational outcome plumbing (AC4: never gates)
// ---------------------------------------------------------------------

/// Converts an informational comparison's computed delta into the
/// section's outcome: always `Ok` — informational comparisons never
/// fail the scenario (ratified on #19/#60; unit-tested below as AC4).
/// The caller dumps the delta artifact *before* consulting this, so a
/// non-empty delta is preserved as evidence without gating.
fn informational_result(delta: Option<&str>) -> Result<()> {
    if let Some(detail) = delta {
        println!("pulsus-e2e:   traces informational delta (never gating): {detail}");
    }
    Ok(())
}

// ---------------------------------------------------------------------
// traces_roundtrip (both variants; native surfaces only)
// ---------------------------------------------------------------------

pub async fn traces_roundtrip(ctx: &Ctx) -> Result<()> {
    let fixture = load_fixture(ctx)?;
    let scale = resolve_scale()?;
    let query_timeout = query_request_timeout(scale);
    let corpus = build_corpus(&fixture, scale, "e2e-traces-rt")?;
    let window = search_window(&corpus);
    println!(
        "pulsus-e2e:   traces_roundtrip [{:?}]: pushing {} traces / {} spans ({:?} tier, run_id={:?})",
        ctx.variant,
        corpus.traces.len(),
        corpus.total_spans(),
        corpus.scale,
        corpus.run_id
    );

    push_trace_corpus(ctx, &corpus)
        .await
        .context("pushing the traces corpus through the collector failed")?;

    wait_for_fetch_completeness(ctx, &corpus, Stores::PulsusOnly)
        .await
        .context("traces corpus never became fetchable by id")?;

    // Trace-by-ID: exact span-ID set equality against the corpus.
    for trace in &corpus.traces {
        let trace_hex = hex(&trace.trace_id);
        let body = fetch_pulsus_trace(ctx, &trace_hex, query_timeout)
            .await?
            .with_context(|| format!("trace {trace_hex} vanished after completeness"))?;
        let actual = fetch_span_ids(&body)?;
        let expected = corpus
            .expected_span_ids(&trace_hex)
            .context("corpus is missing its own trace")?;
        if actual != expected {
            bail!(
                "trace {trace_hex}: span-ID set diverged from the corpus\nexpected: {expected:?}\nactual:   {actual:?}"
            );
        }
    }

    wait_for_search_completeness(ctx, &corpus, window, fixture.limit, Stores::PulsusOnly)
        .await
        .context("traces corpus never became searchable")?;

    // Search: the run-scoped query returns exactly the corpus, complete.
    let body = search_pulsus(
        ctx,
        &run_scope_query(&corpus.run_id),
        window,
        fixture.limit,
        query_timeout,
    )
    .await?;
    if pulsus_search_is_partial(&body)? {
        bail!("run-scoped search reported metrics.partial=true: {body}");
    }
    let returned = body["metrics"]["returned"].as_u64().unwrap_or_default();
    if returned as usize != corpus.traces.len() {
        bail!(
            "run-scoped search metrics.returned={returned}, expected {}",
            corpus.traces.len()
        );
    }

    assert_tags_roundtrip(ctx, query_timeout).await?;
    assert_metrics_roundtrip(ctx, &corpus, window).await?;

    if ctx.variant == Variant::Cluster {
        assert_shard_local_span_counts(ctx, &corpus).await?;
    }
    Ok(())
}

/// Tags/tag-values (docs/api.md §4.3): the catalog is global and
/// time-less, so this asserts *containment* of the corpus's known
/// keys/values, never set equality.
async fn assert_tags_roundtrip(ctx: &Ctx, query_timeout: Duration) -> Result<()> {
    let res = ctx
        .http
        .get(ctx.url("/api/traces/v1/tags"))
        .timeout(query_timeout) // issue #92/#106, see fetch_pulsus_trace
        .send()
        .await
        .context("GET /api/traces/v1/tags failed")?;
    if !res.status().is_success() {
        bail!("tags returned {}", res.status());
    }
    let body: serde_json::Value = res.json().await.context("tags body was not JSON")?;
    let scope_tags = |scope: &str| -> BTreeSet<String> {
        body["scopes"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|s| s["name"] == scope)
            .filter_map(|s| s["tags"].as_array())
            .flatten()
            .filter_map(|t| t.as_str())
            .map(str::to_string)
            .collect()
    };
    let resource_tags = scope_tags("resource");
    let span_tags = scope_tags("span");
    for key in [traces_corpus::RUN_ATTR, "env", "region", "service.name"] {
        if !resource_tags.contains(key) {
            bail!("tags response missing resource tag {key:?}: {body}");
        }
    }
    for key in ["http.status_code", "cache_hit", "sample_ratio", "tier"] {
        if !span_tags.contains(key) {
            bail!("tags response missing span tag {key:?}: {body}");
        }
    }

    let res = ctx
        .http
        .get(ctx.url("/api/traces/v1/tag/resource.region/values"))
        .timeout(query_timeout) // issue #92/#106, see fetch_pulsus_trace
        .send()
        .await
        .context("GET tag values failed")?;
    if !res.status().is_success() {
        bail!("tag values returned {}", res.status());
    }
    let body: serde_json::Value = res.json().await.context("tag values body was not JSON")?;
    let values: BTreeSet<&str> = body["tagValues"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|v| v["value"].as_str())
        .collect();
    for expected in ["eu", "us"] {
        if !values.contains(expected) {
            bail!("tag resource.region values missing {expected:?}: {body}");
        }
    }
    Ok(())
}

/// TraceQL metrics (docs/api.md §4.4): `count_over_time()` over the
/// run-scoped spanset — summed across every bucket/series it must equal
/// the ingested span count exactly (replay-deduped uniqExact counting),
/// on both the matrix (`query_range`) and vector (`query`) forms.
async fn assert_metrics_roundtrip(
    ctx: &Ctx,
    corpus: &TraceCorpus,
    window: SearchWindow,
) -> Result<()> {
    let q = format!("{} | count_over_time()", run_scope_query(&corpus.run_id));
    let start = window.start_s.to_string();
    let end = window.end_s.to_string();
    let expected = corpus.total_spans() as f64;
    let query_timeout = query_request_timeout(corpus.scale);

    let res = ctx
        .http
        .get(ctx.url("/api/traces/v1/metrics/query_range"))
        .query(&[
            ("q", q.as_str()),
            ("start", start.as_str()),
            ("end", end.as_str()),
            ("step", "60s"),
        ])
        .timeout(query_timeout) // issue #92/#106, see fetch_pulsus_trace
        .send()
        .await
        .context("GET metrics/query_range failed")?;
    if !res.status().is_success() {
        bail!("metrics/query_range returned {}", res.status());
    }
    let body: serde_json::Value = res
        .json()
        .await
        .context("metrics/query_range body was not JSON")?;
    if body["data"]["resultType"] != "matrix" {
        bail!("metrics/query_range resultType was not matrix: {body}");
    }
    let total: f64 = body["data"]["result"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|series| series["values"].as_array())
        .flatten()
        .filter_map(|point| point[1].as_str())
        .filter_map(|s| s.parse::<f64>().ok())
        .sum();
    if total != expected {
        bail!("metrics/query_range count_over_time summed to {total}, expected {expected}: {body}");
    }

    let res = ctx
        .http
        .get(ctx.url("/api/traces/v1/metrics/query"))
        .query(&[
            ("q", q.as_str()),
            ("start", start.as_str()),
            ("end", end.as_str()),
        ])
        .timeout(query_timeout) // issue #92/#106, see fetch_pulsus_trace
        .send()
        .await
        .context("GET metrics/query failed")?;
    if !res.status().is_success() {
        bail!("metrics/query returned {}", res.status());
    }
    let body: serde_json::Value = res
        .json()
        .await
        .context("metrics/query body was not JSON")?;
    if body["data"]["resultType"] != "vector" {
        bail!("metrics/query resultType was not vector: {body}");
    }
    let instant: f64 = body["data"]["result"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|sample| sample["value"][1].as_str())
        .filter_map(|s| s.parse::<f64>().ok())
        .sum();
    if instant != expected {
        bail!("metrics/query count_over_time returned {instant}, expected {expected}: {body}");
    }
    Ok(())
}

/// Cluster-only shard-local sanity (mirrors
/// `scenarios::assert_shard_local_row_counts`, table `trace_spans`):
/// polls until the per-shard local counts sum to the ingested span
/// total — proves real fan-out through `trace_spans_dist` without
/// requiring any particular per-shard split.
///
/// **Run-scoped and replay-immune (issue #60 code review, medium):**
/// the per-shard count is restricted to THIS corpus's trace ids
/// ([`scoped_span_count_sql`]) and counts distinct `(trace_id,
/// span_id)` pairs — a raw global `count()` would let stale rows from a
/// `--keep` rerun satisfy the sum, and at-least-once exporter-retry
/// duplicates overshoot it. Cross-shard double counting is impossible:
/// spans shard by `cityHash64(trace_id)` (docs/schemas.md §7), so one
/// span's replays always land on the same shard. Note: the pre-existing
/// `logs_roundtrip` (#15) / `log_samples` counterpart still uses the
/// unscoped raw-count pattern — flagged as a follow-up candidate,
/// deliberately not touched in this issue.
async fn assert_shard_local_span_counts(ctx: &Ctx, corpus: &TraceCorpus) -> Result<()> {
    let expected_total = corpus.total_spans();
    let sql = scoped_span_count_sql(corpus);
    let compose = ctx.compose.clone();
    poll_until(
        completeness_poll_timeout(corpus.scale),
        COMPLETENESS_POLL_INTERVAL,
        move || {
            let compose = compose.clone();
            let sql = sql.clone();
            async move {
                let (shard1, shard2) =
                    tokio::task::spawn_blocking(move || -> Result<(usize, usize)> {
                        Ok((
                            shard_local_trace_spans_count(&compose, "ch-shard1", &sql)?,
                            shard_local_trace_spans_count(&compose, "ch-shard2", &sql)?,
                        ))
                    })
                    .await
                    .context("shard-local trace_spans count task panicked")??;
                Ok((shard1 + shard2 == expected_total).then_some(()))
            }
        },
    )
    .await
    .with_context(|| {
        format!(
            "shard-local run-scoped distinct trace_spans counts never summed to the \
             {expected_total} ingested spans"
        )
    })?;
    Ok(())
}

/// The run-scoped, replay-deduped per-shard count query: distinct
/// `(trace_id, span_id)` over exactly this corpus's trace ids
/// (`unhex('…')` literals, the `pulsus-read` `search_sql` convention —
/// ids are corpus-generated hex, never user input).
fn scoped_span_count_sql(corpus: &TraceCorpus) -> String {
    let ids: Vec<String> = corpus
        .traces
        .iter()
        .map(|t| format!("unhex('{}')", hex(&t.trace_id)))
        .collect();
    format!(
        "SELECT uniqExact(trace_id, span_id) FROM pulsus.trace_spans WHERE trace_id IN ({})",
        ids.join(", ")
    )
}

fn shard_local_trace_spans_count(
    compose: &crate::engine::Compose,
    shard_service: &str,
    sql: &str,
) -> Result<usize> {
    let output = compose
        .exec(shard_service, &["clickhouse-client", "--query", sql])
        .with_context(|| format!("compose exec {shard_service} clickhouse-client failed"))?;
    output
        .trim()
        .parse::<usize>()
        .with_context(|| format!("shard {shard_service} row count {output:?} was not a number"))
}

// ---------------------------------------------------------------------
// traces_differential (single variant; PulsusDB vs corpus vs Tempo)
// ---------------------------------------------------------------------

pub async fn traces_differential(ctx: &Ctx) -> Result<()> {
    let fixture = load_fixture(ctx)?;
    let scale = resolve_scale()?;
    let corpus = build_corpus(&fixture, scale, "e2e-traces-diff")?;
    let window = search_window(&corpus);
    println!(
        "pulsus-e2e:   traces_differential [{:?}]: pushing {} traces / {} spans ({:?} tier, run_id={:?})",
        ctx.variant,
        corpus.traces.len(),
        corpus.total_spans(),
        corpus.scale,
        corpus.run_id
    );

    push_trace_corpus(ctx, &corpus)
        .await
        .context("pushing the differential corpus through the collector failed")?;

    wait_for_fetch_completeness(ctx, &corpus, Stores::PulsusAndTempo)
        .await
        .context("differential corpus never reached trace-by-ID completeness on both stores")?;

    assert_trace_by_id_gate(ctx, &corpus).await?;

    wait_for_search_completeness(ctx, &corpus, window, fixture.limit, Stores::PulsusAndTempo)
        .await
        .context("differential corpus never reached search completeness on both stores")?;

    for case in &fixture.cases {
        run_search_case(ctx, &corpus, &fixture, case, window)
            .await
            .with_context(|| format!("search case {:?}", case.case_id))?;
    }

    run_informational_comparisons(ctx, &corpus, &fixture, window).await
}

/// The trace-by-ID hard gate (plan v2 delta 4): span-ID **sets** only —
/// PulsusDB == corpus == Tempo per seeded trace, artifact + fail on any
/// divergence. Structural tuples are compared afterwards as a separate
/// INFORMATIONAL section.
async fn assert_trace_by_id_gate(ctx: &Ctx, corpus: &TraceCorpus) -> Result<()> {
    let mut structural_deltas: Vec<String> = Vec::new();
    let mut structural_bodies: Vec<serde_json::Value> = Vec::new();
    let query_timeout = query_request_timeout(corpus.scale);

    for trace in &corpus.traces {
        let trace_hex = hex(&trace.trace_id);
        let expected = corpus
            .expected_span_ids(&trace_hex)
            .context("corpus is missing its own trace")?;
        let pulsus_body = fetch_pulsus_trace(ctx, &trace_hex, query_timeout)
            .await?
            .with_context(|| format!("pulsus lost trace {trace_hex} after completeness"))?;
        let tempo_body = fetch_tempo_trace(ctx, &trace_hex, query_timeout)
            .await?
            .with_context(|| format!("tempo lost trace {trace_hex} after completeness"))?;

        let pulsus_ids = fetch_span_ids(&pulsus_body)?;
        let tempo_ids = fetch_span_ids(&tempo_body)?;
        if pulsus_ids != expected || tempo_ids != expected {
            let detail = format!(
                "trace {trace_hex}: span-ID sets diverged: corpus={expected:?} pulsusdb={pulsus_ids:?} tempo={tempo_ids:?}"
            );
            let artifact = serde_json::json!({
                "surface": "trace_by_id",
                "trace_id": trace_hex,
                "expected_span_ids": expected,
                "pulsusdb_result": pulsus_body,
                "tempo_result": tempo_body,
                "detail": detail,
            });
            let path = write_artifact(ctx, ARTIFACT_AREA, "trace-mismatch", &artifact)?;
            bail!(
                "traces differential mismatch: {detail} (repro dumped to {})",
                path.display()
            );
        }

        // Structural fields — informational, own report section.
        let pulsus_tuples = fetch_structural_tuples(&pulsus_body)?;
        let tempo_tuples = fetch_structural_tuples(&tempo_body)?;
        if pulsus_tuples != tempo_tuples {
            structural_deltas.push(format!(
                "trace {trace_hex}: only-pulsusdb={:?} only-tempo={:?}",
                pulsus_tuples.difference(&tempo_tuples).collect::<Vec<_>>(),
                tempo_tuples.difference(&pulsus_tuples).collect::<Vec<_>>(),
            ));
            if structural_bodies.len() < 3 {
                structural_bodies.push(serde_json::json!({
                    "trace_id": trace_hex,
                    "pulsusdb_result": pulsus_body,
                    "tempo_result": tempo_body,
                }));
            }
        }
    }

    let delta = if structural_deltas.is_empty() {
        None
    } else {
        let artifact = serde_json::json!({
            "surface": "trace_by_id_structural",
            "note": "INFORMATIONAL (issue #60 plan v2 delta 4): structural-field deltas are \
                     reported outside the span-ID-set hard gate and never fail the scenario.",
            "deltas": structural_deltas,
            "samples": structural_bodies,
        });
        let path = write_artifact(ctx, ARTIFACT_AREA, "informational-structural", &artifact)?;
        Some(format!(
            "{} trace(s) with structural-field deltas (dumped to {})",
            structural_deltas.len(),
            path.display()
        ))
    };
    informational_result(delta.as_deref())
}

/// One committed search case: validity gates first (ours
/// `metrics.partial == false`; Tempo's result count strictly below the
/// requested limit), then the three-way set comparison. `mode:
/// "informational"` cases keep PulsusDB hard-gated against the corpus
/// (our documented semantics) but report the Tempo delta as an
/// artifact instead of failing (ledger-ratified difference).
async fn run_search_case(
    ctx: &Ctx,
    corpus: &TraceCorpus,
    fixture: &TracesFixture,
    case: &CaseRaw,
    window: SearchWindow,
) -> Result<()> {
    let q = case.q.replace("{R}", &corpus.run_id);
    let expected = corpus.expected_case_trace_ids(&case.case_id);
    let gated = case.mode == "gated";
    let query_timeout = query_request_timeout(corpus.scale);
    println!(
        "pulsus-e2e:     case {:?} [{}] — {} ({}): {} expected trace(s)",
        case.case_id,
        case.mode,
        case.construct,
        case.attr_type,
        expected.len()
    );

    // One elapsed line per case (issue #92, the metrics-differential
    // precedent): budget breaches against the tier-aware query timeout
    // stay diagnosable from CI logs alone. Elapsed only — these helpers
    // return parsed JSON, so no raw byte count is in hand.
    let pulsus_started = std::time::Instant::now();
    let pulsus_body = search_pulsus(ctx, &q, window, fixture.limit, query_timeout).await?;
    let pulsus_elapsed = pulsus_started.elapsed();
    let tempo_started = std::time::Instant::now();
    let tempo_body = search_tempo(ctx, &q, window, fixture.limit, query_timeout).await?;
    let tempo_elapsed = tempo_started.elapsed();
    println!(
        "pulsus-e2e: query {q:?} (case {:?}) pulsusdb {}ms tempo {}ms",
        case.case_id,
        pulsus_elapsed.as_millis(),
        tempo_elapsed.as_millis(),
    );
    let pulsus_ids = search_trace_ids(&pulsus_body)?;
    let tempo_ids = search_trace_ids(&tempo_body)?;

    let dump = |kind: &str, detail: &str| -> Result<std::path::PathBuf> {
        let artifact = serde_json::json!({
            "surface": "search",
            "case_id": case.case_id,
            "mode": case.mode,
            "kind": kind,
            "q": q,
            "window": { "start": window.start_s, "end": window.end_s, "limit": fixture.limit },
            "expected_trace_ids": expected,
            "pulsusdb_result": pulsus_body,
            "tempo_result": tempo_body,
            "tempo_metrics": tempo_body.get("metrics").cloned().unwrap_or_default(),
            "detail": detail,
        });
        write_artifact(
            ctx,
            ARTIFACT_AREA,
            if gated {
                "search-mismatch"
            } else {
                "informational-search"
            },
            &artifact,
        )
    };

    // Our validity gate: a truncated response is a top-K, not a set —
    // always hard, even on informational cases (it invalidates the
    // comparison itself, not the semantics under comparison).
    if pulsus_search_is_partial(&pulsus_body)? {
        let path = dump("pulsus_validity", "pulsusdb reported metrics.partial=true")?;
        bail!(
            "case {:?}: pulsusdb response was partial — corpus/limit sizing is invalid (repro {})",
            case.case_id,
            path.display()
        );
    }
    // Tempo's symmetric validity gate (plan v2 delta 5): a result count
    // at the requested limit means Tempo may have truncated. Counted on
    // the RAW `traces[]` array, pre-dedup (issue #60 code review,
    // medium): a response holding `limit` rows with duplicate ids would
    // otherwise slip under the limit after set-collapse and mask the
    // truncation.
    let tempo_raw = search_raw_result_count(&tempo_body);
    if tempo_raw as u32 >= fixture.limit {
        let path = dump(
            "tempo_validity",
            "tempo raw result count reached the requested limit",
        )?;
        bail!(
            "case {:?}: tempo returned {tempo_raw} raw results at limit {} — oracle invalidity \
             (repro {})",
            case.case_id,
            fixture.limit,
            path.display()
        );
    }
    // Duplicate ids in the oracle's response are their own invalidity
    // class (same review finding): the set comparison below would hide
    // them, so they are rejected explicitly before it runs.
    if tempo_raw != tempo_ids.len() {
        let path = dump(
            "tempo_duplicate_ids",
            "tempo response carried duplicate traceIDs",
        )?;
        bail!(
            "case {:?}: tempo returned {tempo_raw} raw results but only {} distinct traceIDs — \
             oracle invalidity (repro {})",
            case.case_id,
            tempo_ids.len(),
            path.display()
        );
    }
    // Symmetric on our side: duplicate traceIDs in a PulsusDB response
    // would be a real response-shaping bug the set comparison could
    // mask — always hard.
    let pulsus_raw = search_raw_result_count(&pulsus_body);
    if pulsus_raw != pulsus_ids.len() {
        let path = dump(
            "pulsus_duplicate_ids",
            "pulsusdb response carried duplicate traceIDs",
        )?;
        bail!(
            "case {:?}: pulsusdb returned {pulsus_raw} raw results but only {} distinct traceIDs \
             (repro {})",
            case.case_id,
            pulsus_ids.len(),
            path.display()
        );
    }

    // PulsusDB vs the corpus expectation: ALWAYS hard — informational
    // mode only relaxes the cross-store comparison, never our own
    // documented semantics.
    if pulsus_ids != expected {
        let detail = format!(
            "pulsusdb trace-ID set diverged from the corpus expectation: \
             missing={:?} unexpected={:?}",
            expected.difference(&pulsus_ids).collect::<Vec<_>>(),
            pulsus_ids.difference(&expected).collect::<Vec<_>>(),
        );
        let path = dump("pulsus_vs_corpus", &detail)?;
        bail!(
            "case {:?}: {detail} (repro {})",
            case.case_id,
            path.display()
        );
    }

    // Tempo vs the corpus expectation (== vs PulsusDB, transitively).
    if tempo_ids != expected {
        let detail = format!(
            "tempo trace-ID set diverged from the corpus expectation: \
             missing-from-tempo={:?} unexpected-in-tempo={:?} (tempo metrics: {})",
            expected.difference(&tempo_ids).collect::<Vec<_>>(),
            tempo_ids.difference(&expected).collect::<Vec<_>>(),
            tempo_body.get("metrics").cloned().unwrap_or_default(),
        );
        let path = dump("tempo_vs_corpus", &detail)?;
        if gated {
            bail!(
                "case {:?}: {detail} (repro {})",
                case.case_id,
                path.display()
            );
        }
        return informational_result(Some(&format!(
            "case {:?} (ledger {:?}): {detail} (dumped to {})",
            case.case_id,
            case.ledger.as_deref().unwrap_or(""),
            path.display()
        )));
    }
    Ok(())
}

/// PulsusDB's Prometheus-matrix metrics response normalized to
/// `bucket-ms -> summed value` (docs/api.md §4.4: matrix envelope,
/// `values: [[ts_seconds, "value"], …]`; series are summed per bucket —
/// both stores return a single series for the run-scoped informational
/// queries, so summing is a stable normalization either way).
fn pulsus_metrics_points(body: &serde_json::Value) -> std::collections::BTreeMap<i64, f64> {
    let mut out = std::collections::BTreeMap::new();
    for series in body["data"]["result"].as_array().into_iter().flatten() {
        for point in series["values"].as_array().into_iter().flatten() {
            let Some(ts_ms) = point[0]
                .as_f64()
                .map(|s| (s * 1000.0).round() as i64)
                .or_else(|| {
                    point[0]
                        .as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .map(|s| (s * 1000.0).round() as i64)
                })
            else {
                continue;
            };
            let Some(val) = point[1].as_str().and_then(|s| s.parse::<f64>().ok()) else {
                continue;
            };
            *out.entry(ts_ms).or_insert(0.0) += val;
        }
    }
    out
}

/// Tempo's `/api/metrics/query_range` response normalized the same way
/// (`series[].samples[]` of `{timestampMs, value}` — jsonpb, so
/// `timestampMs` is a string, and a zero `value` may be omitted
/// entirely).
fn tempo_metrics_points(body: &serde_json::Value) -> std::collections::BTreeMap<i64, f64> {
    let mut out = std::collections::BTreeMap::new();
    for series in body["series"].as_array().into_iter().flatten() {
        for sample in series["samples"].as_array().into_iter().flatten() {
            let Some(ts_ms) = sample["timestampMs"]
                .as_i64()
                .or_else(|| sample["timestampMs"].as_str().and_then(|s| s.parse().ok()))
            else {
                continue;
            };
            let val = sample["value"]
                .as_f64()
                .or_else(|| sample["value"].as_str().and_then(|s| s.parse().ok()))
                .unwrap_or(0.0); // proto3 zero-default omitted by jsonpb
            *out.entry(ts_ms).or_insert(0.0) += val;
        }
    }
    out
}

/// A structured, per-bucket numeric comparison of the two stores'
/// metrics answers (issue #60 code review, low): max absolute and
/// relative differences over the bucket union (a bucket present on one
/// side only compares against 0), plus the one-sided bucket counts.
/// Recorded in the informational artifact and summary line — never
/// gating.
#[derive(Debug, PartialEq)]
struct MetricsDelta {
    compared_buckets: usize,
    only_pulsus: usize,
    only_tempo: usize,
    max_abs_diff: f64,
    max_rel_diff: f64,
}

impl MetricsDelta {
    fn summary(&self) -> String {
        format!(
            "buckets={} only-pulsusdb={} only-tempo={} max_abs_diff={} max_rel_diff={}",
            self.compared_buckets,
            self.only_pulsus,
            self.only_tempo,
            self.max_abs_diff,
            self.max_rel_diff
        )
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "compared_buckets": self.compared_buckets,
            "only_pulsus": self.only_pulsus,
            "only_tempo": self.only_tempo,
            "max_abs_diff": self.max_abs_diff,
            "max_rel_diff": self.max_rel_diff,
        })
    }
}

fn metrics_points_delta(
    pulsus: &std::collections::BTreeMap<i64, f64>,
    tempo: &std::collections::BTreeMap<i64, f64>,
) -> MetricsDelta {
    let buckets: BTreeSet<i64> = pulsus.keys().chain(tempo.keys()).copied().collect();
    let mut delta = MetricsDelta {
        compared_buckets: buckets.len(),
        only_pulsus: 0,
        only_tempo: 0,
        max_abs_diff: 0.0,
        max_rel_diff: 0.0,
    };
    for ts in buckets {
        let a = pulsus.get(&ts).copied();
        let b = tempo.get(&ts).copied();
        match (a, b) {
            (Some(_), None) => delta.only_pulsus += 1,
            (None, Some(_)) => delta.only_tempo += 1,
            _ => {}
        }
        let a = a.unwrap_or(0.0);
        let b = b.unwrap_or(0.0);
        let abs = (a - b).abs();
        let denom = a.abs().max(b.abs());
        let rel = if denom > 0.0 { abs / denom } else { 0.0 };
        delta.max_abs_diff = delta.max_abs_diff.max(abs);
        delta.max_rel_diff = delta.max_rel_diff.max(rel);
    }
    delta
}

/// The never-gating comparisons (ratified on #19/#60): tags-vs-Tempo and
/// TraceQL-metrics-vs-Tempo. Every delta — including a Tempo-side error
/// (e.g. its metrics endpoint needs the metrics-generator, deliberately
/// not enabled in `deploy/e2e/tempo.yaml`) — is dumped as an
/// informational artifact; the section always returns `Ok`.
async fn run_informational_comparisons(
    ctx: &Ctx,
    corpus: &TraceCorpus,
    fixture: &TracesFixture,
    window: SearchWindow,
) -> Result<()> {
    let mut deltas: Vec<String> = Vec::new();
    let mut sections: Vec<serde_json::Value> = Vec::new();
    let query_timeout = query_request_timeout(corpus.scale);

    // Tags: our scoped shape vs Tempo's `/api/search/tags` (structurally
    // divergent by design — scope/intrinsic handling differs).
    let pulsus_tags = get_json(ctx, &ctx.url("/api/traces/v1/tags"), query_timeout).await;
    let tempo_tags = get_json(
        ctx,
        &format!("{}/api/search/tags", ctx.tempo_url),
        query_timeout,
    )
    .await;
    match (&pulsus_tags, &tempo_tags) {
        (Ok(p), Ok(t)) => {
            let ours: BTreeSet<String> = p["scopes"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|s| s["tags"].as_array())
                .flatten()
                .filter_map(|v| v.as_str())
                .map(str::to_string)
                .collect();
            let theirs: BTreeSet<String> = t["tagNames"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|v| v.as_str())
                .map(str::to_string)
                .collect();
            if ours != theirs {
                deltas.push(format!(
                    "tags: only-pulsusdb={:?} only-tempo={:?}",
                    ours.difference(&theirs).collect::<Vec<_>>(),
                    theirs.difference(&ours).collect::<Vec<_>>()
                ));
            }
        }
        (p, t) => deltas.push(format!(
            "tags: fetch failed (pulsusdb ok={}, tempo ok={})",
            p.is_ok(),
            t.is_ok()
        )),
    }
    sections.push(serde_json::json!({
        "section": "tags",
        "pulsusdb_result": pulsus_tags.unwrap_or_default(),
        "tempo_result": tempo_tags.unwrap_or_default(),
    }));

    // TraceQL metrics: identical queries against both `query_range`
    // endpoints; per-bucket numeric deltas computed and recorded (issue
    // #60 code review, low — an actual comparison, never just "raw
    // bodies preserved"), raw bodies kept alongside, values never
    // gated.
    for raw_q in &fixture.informational.metrics_queries {
        let q = raw_q.replace("{R}", &corpus.run_id);
        let params = [
            ("q", q.clone()),
            ("start", window.start_s.to_string()),
            ("end", window.end_s.to_string()),
            ("step", "60s".to_string()),
        ];
        let pulsus = get_json_with(
            ctx,
            &ctx.url("/api/traces/v1/metrics/query_range"),
            &params,
            query_timeout,
        )
        .await;
        let tempo = get_json_with(
            ctx,
            &format!("{}/api/metrics/query_range", ctx.tempo_url),
            &params,
            query_timeout,
        )
        .await;
        let mut delta_json = serde_json::Value::Null;
        match (&pulsus, &tempo) {
            (Ok(p), Ok(t)) => {
                let ours = pulsus_metrics_points(p);
                let theirs = tempo_metrics_points(t);
                let delta = metrics_points_delta(&ours, &theirs);
                deltas.push(format!("metrics {q:?}: {}", delta.summary()));
                delta_json = delta.to_json();
            }
            (p, t) => deltas.push(format!(
                "metrics {q:?}: fetch failed (pulsusdb ok={}, tempo ok={})",
                p.is_ok(),
                t.is_ok()
            )),
        }
        sections.push(serde_json::json!({
            "section": "metrics_query_range",
            "q": q,
            "delta": delta_json,
            "pulsusdb_result": pulsus.unwrap_or_else(|e| serde_json::json!({"error": format!("{e:#}")})),
            "tempo_result": tempo.unwrap_or_else(|e| serde_json::json!({"error": format!("{e:#}")})),
        }));
    }

    let artifact = serde_json::json!({
        "surface": "informational",
        "note": "never-gating comparisons (issue #60 plan: tags-vs-Tempo and metrics-vs-Tempo \
                 are informational, ratified on #19) — evidence only, the scenario returns Ok.",
        "deltas": deltas,
        "sections": sections,
    });
    let path = write_artifact(ctx, ARTIFACT_AREA, "informational-tags-metrics", &artifact)?;
    informational_result(Some(&format!(
        "{} informational section delta(s), dumped to {}",
        deltas.len(),
        path.display()
    )))
}

async fn get_json(ctx: &Ctx, url: &str, query_timeout: Duration) -> Result<serde_json::Value> {
    let res = ctx
        .http
        .get(url)
        .timeout(query_timeout) // issue #92/#106, see fetch_pulsus_trace
        .send()
        .await
        .with_context(|| format!("GET {url} failed"))?;
    if !res.status().is_success() {
        bail!("GET {url} returned {}", res.status());
    }
    res.json()
        .await
        .with_context(|| format!("GET {url} body was not JSON"))
}

async fn get_json_with(
    ctx: &Ctx,
    url: &str,
    params: &[(&str, String)],
    query_timeout: Duration,
) -> Result<serde_json::Value> {
    let res = ctx
        .http
        .get(url)
        .query(params)
        .timeout(query_timeout) // issue #92/#106, see fetch_pulsus_trace
        .send()
        .await
        .with_context(|| format!("GET {url} failed"))?;
    if !res.status().is_success() {
        let status = res.status();
        let body = res.text().await.unwrap_or_default();
        bail!("GET {url} returned {status}: {body}");
    }
    res.json()
        .await
        .with_context(|| format!("GET {url} body was not JSON"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traces_corpus::{CASE_IDS, naive_matches};

    /// The committed exclusion list: every case starts gated; a case id
    /// appears here ONLY after an observed live divergence was triaged
    /// per the #33 discipline and recorded in
    /// docs/benchmarks/traces-differential-ledger.md (issue #60 plan v2
    /// deltas 2+3). Update deliberately, with the ledger entry, never
    /// as a quick fix for a red run.
    const INFORMATIONAL_CASE_IDS: &[&str] = &["neg_attr_missing_key"];

    fn shipped_fixture() -> TracesFixture {
        let root = crate::engine::workspace_root();
        let raw = std::fs::read_to_string(root.join("test/fixtures").join(FIXTURE_PATH)).unwrap();
        serde_json::from_str(&raw).unwrap()
    }

    fn shipped_corpus(fixture: &TracesFixture, trace_count: usize) -> TraceCorpus {
        traces_corpus::generate(&TraceCorpusSpec {
            seed: fixture.seed,
            scale: Scale::Ci,
            trace_count,
            step_ns: fixture.step_ns,
            base_ns: 1_700_000_000_000_000_000,
            run_id: "fixture-check".to_string(),
        })
    }

    /// The coverage matrix and the corpus labeling can never drift: the
    /// fixture's case ids are exactly `traces_corpus::CASE_IDS`, in
    /// order.
    #[test]
    fn shipped_fixture_cases_match_the_corpus_case_ids_exactly() {
        let fixture = shipped_fixture();
        let fixture_ids: Vec<&str> = fixture.cases.iter().map(|c| c.case_id.as_str()).collect();
        assert_eq!(fixture_ids, CASE_IDS.to_vec());
    }

    /// AC5: the gated list is exactly the committed set — every case is
    /// gated unless it appears on the pinned, ledger-backed exclusion
    /// list.
    #[test]
    fn shipped_fixture_gated_set_is_exactly_the_committed_subset() {
        let fixture = shipped_fixture();
        for case in &fixture.cases {
            let expect_informational = INFORMATIONAL_CASE_IDS.contains(&case.case_id.as_str());
            match case.mode.as_str() {
                "gated" => assert!(
                    !expect_informational,
                    "case {:?} is on the pinned exclusion list but marked gated — update both \
                     deliberately",
                    case.case_id
                ),
                "informational" => assert!(
                    expect_informational,
                    "case {:?} is informational but not on the pinned exclusion list — a class \
                     moves off the gate only via the ledger discipline",
                    case.case_id
                ),
                other => panic!("case {:?} has unknown mode {other:?}", case.case_id),
            }
        }
    }

    /// Every informational case must reference a ledger entry, and the
    /// committed ledger must actually contain both the entry id and the
    /// case id — the mechanical fixture↔ledger link.
    #[test]
    fn informational_cases_are_recorded_in_the_committed_ledger() {
        let fixture = shipped_fixture();
        let ledger_path =
            crate::engine::workspace_root().join("docs/benchmarks/traces-differential-ledger.md");
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
                case.q.contains(r#"resource.run_id = "{R}""#),
                "case {:?} is not run-scoped: {}",
                case.case_id,
                case.q
            );
            assert!(!case.construct.is_empty() && !case.attr_type.is_empty());
            let rendered = case.q.replace("{R}", "e2e-traces-test");
            assert!(!rendered.contains("{R}"));
        }
        for q in &fixture.informational.metrics_queries {
            assert!(
                q.contains("{R}"),
                "informational metrics query {q:?} is not run-scoped"
            );
        }
    }

    /// AC5 oracle-correctness for the SHIPPED fixture: every committed
    /// case's expected set (from the by-construction labels) agrees
    /// with the independent naive evaluator at both committed tier
    /// sizes.
    #[test]
    fn shipped_fixture_expected_sets_agree_with_the_naive_evaluator() {
        let fixture = shipped_fixture();
        for count in [fixture.ci.trace_count, fixture.full.trace_count] {
            let corpus = shipped_corpus(&fixture, count);
            for case in &fixture.cases {
                let expected = corpus.expected_case_trace_ids(&case.case_id);
                let naive: BTreeSet<String> = corpus
                    .traces
                    .iter()
                    .filter(|t| naive_matches(&case.case_id, t, &corpus.run_id))
                    .map(|t| hex(&t.trace_id))
                    .collect();
                assert_eq!(
                    expected, naive,
                    "case {:?} disagrees at trace_count {count}",
                    case.case_id
                );
                assert!(!expected.is_empty(), "case {:?} is vacuous", case.case_id);
                assert!((expected.len() as u32) < fixture.limit);
            }
        }
    }

    /// AC4: the informational path returns Ok on a non-empty delta —
    /// informational comparisons never gate.
    #[test]
    fn informational_result_is_ok_on_a_non_empty_delta() {
        assert!(informational_result(Some("tags: only-tempo=[\"intrinsic\"]")).is_ok());
        assert!(informational_result(None).is_ok());
    }

    #[test]
    fn parse_traces_scale_defaults_and_rejects_like_the_metrics_parser() {
        assert_eq!(parse_traces_scale(None).unwrap(), Scale::Ci);
        assert_eq!(parse_traces_scale(Some("CI")).unwrap(), Scale::Ci);
        assert_eq!(parse_traces_scale(Some("full")).unwrap(), Scale::Full);
        assert!(parse_traces_scale(Some("bogus")).is_err());
    }

    #[test]
    fn normalize_trace_id_hex_left_pads_tempo_stripped_ids() {
        assert_eq!(
            normalize_trace_id_hex("af7651916cd43dd8448eb211c80319c"),
            "0af7651916cd43dd8448eb211c80319c"
        );
        assert_eq!(
            normalize_trace_id_hex("0AF7651916CD43DD8448EB211C80319C"),
            "0af7651916cd43dd8448eb211c80319c"
        );
    }

    #[test]
    fn decode_wire_id_handles_hex_and_base64_span_ids() {
        // Tempo jsonpb base64 for 0xb7ad6b7169203331.
        assert_eq!(
            decode_wire_id("t61rcWkgMzE=", 8).unwrap(),
            "b7ad6b7169203331"
        );
        assert_eq!(
            decode_wire_id("B7AD6B7169203331", 8).unwrap(),
            "b7ad6b7169203331"
        );
        assert!(decode_wire_id("zzzz", 8).is_err());
    }

    #[test]
    fn base64_decode_matches_known_vectors() {
        assert_eq!(base64_decode("aGVsbG8=").unwrap(), b"hello");
        assert_eq!(base64_decode("aGVsbG8h").unwrap(), b"hello!");
        assert!(base64_decode("a").is_err());
    }

    #[test]
    fn fetch_span_ids_reads_both_stores_shapes() {
        let pulsus = serde_json::json!({"resourceSpans":[{"scopeSpans":[{"spans":[
            {"spanId":"b7ad6b7169203331"}, {"spanId":"c7ad6b7169203331"}]}]}]});
        let tempo = serde_json::json!({"batches":[{"scopeSpans":[{"spans":[
            {"spanId":"t61rcWkgMzE="}, {"spanId":"x61rcWkgMzE="}]}]}]});
        assert_eq!(
            fetch_span_ids(&pulsus).unwrap(),
            fetch_span_ids(&tempo).unwrap()
        );
    }

    /// Issue #106: the fetch-completeness diagnostic's core reports the
    /// exact missing/extra span-id sets from a partial store result — so a
    /// systematically dropped span is distinguishable from generic lag in
    /// the artifact CI reads when the nightly next fails.
    #[test]
    fn span_id_set_diff_reports_missing_and_extra() {
        let expected: BTreeSet<String> = ["aa", "bb", "cc"].into_iter().map(String::from).collect();

        // Store dropped "cc" and carries an unexpected "zz".
        let actual: BTreeSet<String> = ["aa", "bb", "zz"].into_iter().map(String::from).collect();
        let diff = span_id_set_diff(&expected, &actual);
        assert_eq!(diff.missing, vec!["cc".to_string()]);
        assert_eq!(diff.extra, vec!["zz".to_string()]);

        // Fully converged: no shortfall either way.
        let same = span_id_set_diff(&expected, &expected);
        assert!(same.missing.is_empty());
        assert!(same.extra.is_empty());
    }

    #[test]
    fn structural_tuples_normalize_kind_and_root_parent_across_stores() {
        let pulsus = serde_json::json!({"resourceSpans":[{"scopeSpans":[{"spans":[
            {"spanId":"b7ad6b7169203331","name":"root-op","kind":2,
             "parentSpanId":"0000000000000000","startTimeUnixNano":"100"}]}]}]});
        let tempo = serde_json::json!({"batches":[{"scopeSpans":[{"spans":[
            {"spanId":"t61rcWkgMzE=","name":"root-op","kind":"SPAN_KIND_SERVER",
             "startTimeUnixNano":"100"}]}]}]});
        assert_eq!(
            fetch_structural_tuples(&pulsus).unwrap(),
            fetch_structural_tuples(&tempo).unwrap()
        );
    }

    #[test]
    fn search_trace_ids_normalizes_and_tolerates_an_absent_traces_key() {
        let pulsus = serde_json::json!({"traces":[{"traceID":"0af7651916cd43dd8448eb211c80319c"}],
            "metrics":{"partial":false,"limit":500,"returned":1}});
        let tempo = serde_json::json!({"traces":[{"traceID":"af7651916cd43dd8448eb211c80319c"}],
            "metrics":{"inspectedBytes":"123"}});
        assert_eq!(
            search_trace_ids(&pulsus).unwrap(),
            search_trace_ids(&tempo).unwrap()
        );
        assert!(
            search_trace_ids(&serde_json::json!({"metrics":{}}))
                .unwrap()
                .is_empty()
        );
    }

    /// Issue #60 code review (test gap): the cluster count must ignore
    /// stale/foreign rows and collapse duplicate delivery — enforced
    /// structurally by the query itself: distinct `(trace_id, span_id)`
    /// restricted to exactly this corpus's trace ids. A foreign row
    /// (different trace id) can never enter the IN list; a duplicated
    /// span (same ids redelivered) can never count twice under
    /// `uniqExact`. (Behavior additionally verified against a live
    /// ClickHouse with seeded foreign + duplicate rows — issue notes.)
    #[test]
    fn scoped_span_count_sql_is_run_scoped_and_replay_deduped() {
        let fixture = shipped_fixture();
        let corpus = shipped_corpus(&fixture, fixture.ci.trace_count);
        let sql = scoped_span_count_sql(&corpus);
        assert!(
            sql.starts_with("SELECT uniqExact(trace_id, span_id) FROM pulsus.trace_spans"),
            "count must be distinct-(trace_id, span_id), got: {sql}"
        );
        // Every corpus trace id is in the predicate — and nothing else:
        // the IN list is exactly the corpus (a stale run's ids, which
        // differ per run_id-folded seed, cannot satisfy it).
        for t in &corpus.traces {
            assert!(sql.contains(&format!("unhex('{}')", hex(&t.trace_id))));
        }
        assert_eq!(
            sql.matches("unhex('").count(),
            corpus.traces.len(),
            "IN list must contain exactly the corpus trace ids"
        );
        // And a foreign trace id (another run's corpus — same seed,
        // different run_id, so different ids) is not listed.
        let foreign = traces_corpus::generate(&TraceCorpusSpec {
            seed: fixture.seed,
            scale: Scale::Ci,
            trace_count: fixture.ci.trace_count,
            step_ns: fixture.step_ns,
            base_ns: 1_700_000_000_000_000_000,
            run_id: "some-other-run".to_string(),
        });
        assert!(!sql.contains(&format!("unhex('{}')", hex(&foreign.traces[0].trace_id))));
    }

    /// Issue #60 code review (medium): truncation validity is judged on
    /// the RAW `traces[]` length — a duplicate-carrying response must
    /// not slip under the limit after set-collapse.
    #[test]
    fn search_raw_result_count_counts_duplicates_that_the_id_set_collapses() {
        let body = serde_json::json!({"traces":[
            {"traceID":"0af7651916cd43dd8448eb211c80319c"},
            {"traceID":"0af7651916cd43dd8448eb211c80319c"},
            {"traceID":"af7651916cd43dd8448eb211c80319c"}
        ]});
        assert_eq!(search_raw_result_count(&body), 3);
        // The set collapses all three (the third only differs by
        // Tempo's stripped leading zero) — exactly the masking the raw
        // count exists to catch.
        assert_eq!(search_trace_ids(&body).unwrap().len(), 1);
        assert_eq!(
            search_raw_result_count(&serde_json::json!({"metrics":{}})),
            0
        );
    }

    #[test]
    fn pulsus_metrics_points_reads_the_prometheus_matrix_shape() {
        let body = serde_json::json!({"data":{"resultType":"matrix","result":[
            {"metric":{},"values":[[1784170200, "3"],[1784170260, "5"]]}
        ]}});
        let points = pulsus_metrics_points(&body);
        assert_eq!(points.get(&1_784_170_200_000), Some(&3.0));
        assert_eq!(points.get(&1_784_170_260_000), Some(&5.0));
    }

    #[test]
    fn tempo_metrics_points_reads_jsonpb_samples_including_omitted_zero_values() {
        let body = serde_json::json!({"series":[{"labels":[],"samples":[
            {"timestampMs":"1784170200000","value":3.0},
            {"timestampMs":"1784170260000"}
        ]}]});
        let points = tempo_metrics_points(&body);
        assert_eq!(points.get(&1_784_170_200_000), Some(&3.0));
        // jsonpb omits proto3 zero defaults: a missing value is 0.
        assert_eq!(points.get(&1_784_170_260_000), Some(&0.0));
    }

    #[test]
    fn metrics_points_delta_reports_zero_for_identical_answers() {
        let a = std::collections::BTreeMap::from([(1000, 3.0), (2000, 5.0)]);
        let delta = metrics_points_delta(&a, &a.clone());
        assert_eq!(delta.compared_buckets, 2);
        assert_eq!(delta.only_pulsus, 0);
        assert_eq!(delta.only_tempo, 0);
        assert_eq!(delta.max_abs_diff, 0.0);
        assert_eq!(delta.max_rel_diff, 0.0);
    }

    #[test]
    fn metrics_points_delta_summarizes_value_and_bucket_divergence() {
        let ours = std::collections::BTreeMap::from([(1000, 4.0), (2000, 5.0)]);
        let theirs = std::collections::BTreeMap::from([(1000, 3.0), (3000, 2.0)]);
        let delta = metrics_points_delta(&ours, &theirs);
        assert_eq!(delta.compared_buckets, 3);
        assert_eq!(delta.only_pulsus, 1); // ts 2000
        assert_eq!(delta.only_tempo, 1); // ts 3000
        assert_eq!(delta.max_abs_diff, 5.0); // ts 2000: 5 vs missing (0)
        assert_eq!(delta.max_rel_diff, 1.0); // one-sided buckets are total
        let json = delta.to_json();
        assert_eq!(json["max_abs_diff"], 5.0);
        assert!(delta.summary().contains("max_abs_diff=5"));
    }

    #[test]
    fn pulsus_search_is_partial_requires_the_field() {
        assert!(
            pulsus_search_is_partial(&serde_json::json!({"metrics":{"partial":true}})).unwrap()
        );
        assert!(pulsus_search_is_partial(&serde_json::json!({})).is_err());
    }
}
