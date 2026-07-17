//! The M2 milestone gate (docs/features.md §7, issue #33 architect plan +
//! amendment): [`metrics_differential`] pushes a deterministic metric
//! corpus **once** as OTLP into the collector, which fans it out — via two
//! identical `prometheusremotewrite` exporters
//! (`deploy/e2e/otel-config.{single,cluster}.yaml`) — to both PulsusDB and
//! a reference Prometheus (`deploy/e2e/compose.{single,cluster}.yaml`),
//! then drives the full M2 PromQL proof subset through `/api/v1/query`
//! and `/api/v1/query_range` on **both** backends and asserts per-series
//! per-timestamp value equality. Prometheus is the oracle throughout —
//! this module never re-implements PromQL evaluation to compute an
//! expected value; it only compares two live answers.
//!
//! Structurally mirrors `scenarios::logs_roundtrip` (issue #15): per-run
//! `run_id` isolation label ([`corpus::RUN_ID_LABEL`]), `poll_until`
//! visibility, cluster-leg parity via the same `poll_until` absorbing
//! `_dist` eventual consistency. The one new primitive `logs_roundtrip`
//! didn't need is the **completeness pre-check** (issue #33 amendment):
//! because the two `prometheusremotewrite` exporters have independent
//! queues/batching/retries, the only provable guarantee is *eventual*
//! sample-set equivalence, not byte-identical delivery — so no query
//! matrix runs until both backends independently reach the corpus's own
//! manifest (`expected_series`/`expected_samples`, counted via
//! `count(count_over_time(...))`/`sum(count_over_time(...))`, which is
//! itself immune to `_dist` lag and to the write path's own retry
//! timing).
//!
//! **Value equality** ([`samples_equal`]) is bit-exact-except-NaN on the
//! parsed `f64` — never a string compare (architect plan: both engines
//! render shortest-round-trip, so parsing back and bit-comparing is the
//! semantically correct ULP-level equality; `#32`'s `prom_float` goldens
//! police wire-format rendering separately, this scenario tests *values*).
//! Any mismatch dumps a minimal repro artifact under
//! `target/e2e-artifacts/metrics-diff/<variant>/` and fails the scenario —
//! never an allowlist (docs/features.md §7 AC).
//!
//! **StaleNaN micro-case** ([`stalenan_micro_case`], task-manager
//! resolution on issue #33's Open Question 1, **unconditionally in
//! scope**): OTLP has no wire representation for Prometheus's explicit
//! stale-marker sample, so the collector-fed corpus can only realize
//! staleness as gaps (real differential coverage on its own — both
//! engines must agree on where their 5-minute lookback goes stale). This
//! supplements that with a **direct** remote-write (bypassing the
//! collector, clearly labeled below) pushing an explicit stale-NaN sample
//! to both `/api/v1/write` endpoints, under the same guardrails: its own
//! `run_id`, the same completeness pre-check, the same bit-exact-except-
//! NaN comparison, `stalenan-*.json` repro on mismatch. It supplements,
//! never substitutes for, the collector-fed gate above.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use prost::Message;
use serde::Deserialize;

use crate::corpus::{self, Corpus, CorpusSpec, FamilyCounts, Scale};
use crate::harness::{QUERY_REQUEST_TIMEOUT, poll_until};
use crate::scenarios::{Ctx, Variant};

const FIXTURE_PATH: &str = "metrics/differential.json";

/// Collector readiness poll bounds (mirrors `scenarios::
/// COLLECTOR_READY_POLL_TIMEOUT`, issue #15 precedent) — the first
/// per-timestamp OTLP export retries past a not-yet-listening collector,
/// exactly like `logs_roundtrip`'s own first `POST /v1/logs`.
const COLLECTOR_READY_POLL_TIMEOUT: Duration = Duration::from_secs(90);
const COLLECTOR_READY_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Completeness pre-check poll bounds (issue #33 amendment: "bounded
/// deadline (~90s magnitude, like the logs scenario)") — generous enough
/// to absorb the `Full` tier's larger corpus and the cluster leg's `_dist`
/// fan-out lag; `PULSUS_E2E_METRICS_SCALE=full` runs are also given more
/// wall-clock budget by the nightly job, not by this constant (a longer
/// deadline here costs nothing on the CI tier, since `poll_until` returns
/// the instant the condition is met).
const COMPLETENESS_POLL_TIMEOUT: Duration = Duration::from_secs(180);
const COMPLETENESS_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The StaleNaN micro-case's canonical stale-NaN bit pattern (duplicated
/// from `pulsus_model::STALE_NAN_BITS` — this crate stays HTTP-only with
/// no internal-crate dependency, `e2e/Cargo.toml`'s own doc comment; same
/// "duplicate a small wire-format constant across a crate boundary" call
/// `pulsus-write::protocols::otlp_metrics.rs` already makes for
/// `base64_encode`).
const STALE_NAN_BITS: u64 = 0x7FF0_0000_0000_0002;
const STALE_METRIC: &str = "stale_marker_seconds";

/// One entry in `test/fixtures/metrics/differential.json`'s
/// `query_matrix` (issue #33 architect plan interface: `MatrixQuery`).
/// `{R}` inside `expr` is replaced with the run's `run_id` at execution
/// time ([`run_query_matrix`]).
#[derive(Debug, Deserialize)]
struct MatrixQueryRaw {
    expr: String,
    modes: Vec<String>,
}

/// The corpus/query-matrix fixture (architect plan: "declarative corpus
/// params ... so scale/queries are adjustable without recompiling").
#[derive(Debug, Deserialize)]
struct DifferentialFixture {
    seed: u64,
    step_ms: i64,
    sample_count: usize,
    histogram_bounds: Vec<f64>,
    ci: FamilyCounts,
    full: FamilyCounts,
    query_matrix: Vec<MatrixQueryRaw>,
}

fn load_fixture(ctx: &Ctx) -> Result<DifferentialFixture> {
    let path = ctx.fixtures_dir.join(FIXTURE_PATH);
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read fixture {}", path.display()))?;
    serde_json::from_str(&raw)
        .with_context(|| format!("fixture {} was not valid JSON", path.display()))
}

/// The pure core of `PULSUS_E2E_METRICS_SCALE` parsing (code review
/// finding, issue #33, [low]): factored out of [`resolve_scale`] so tests
/// exercise it by passing a value directly instead of mutating the
/// process environment — `std::env::remove_var`/`set_var` are `unsafe` in
/// edition 2024 precisely because other threads may concurrently read/
/// write the same process environment, and a `#[test]` (which may run
/// concurrently with unrelated tests in the same binary) provides no
/// synchronization against that; a pure function sidesteps the need for
/// `unsafe` entirely rather than trying to justify it with an unproven
/// per-test invariant. `raw = None` is "unset" (`ci`, the default); `ci`/
/// `full` are matched case-insensitively.
fn parse_scale(raw: Option<&str>) -> Result<Scale> {
    match raw {
        None => Ok(Scale::Ci),
        Some(v) if v.eq_ignore_ascii_case("ci") => Ok(Scale::Ci),
        Some(v) if v.eq_ignore_ascii_case("full") => Ok(Scale::Full),
        Some(other) => bail!("PULSUS_E2E_METRICS_SCALE={other:?} must be \"ci\" or \"full\""),
    }
}

/// `PULSUS_E2E_METRICS_SCALE` (architect plan): `ci` (default, ~1k series,
/// gates every PR inside `e2e-single`/`e2e-cluster`) or `full` (~10k
/// series, the literal docs/features.md §7 AC — the closure/nightly/
/// dispatch tier, issue #33 amendment).
fn resolve_scale() -> Result<Scale> {
    match std::env::var("PULSUS_E2E_METRICS_SCALE") {
        Ok(v) => parse_scale(Some(&v)),
        Err(std::env::VarError::NotPresent) => parse_scale(None),
        Err(std::env::VarError::NotUnicode(raw)) => {
            bail!("PULSUS_E2E_METRICS_SCALE was not valid UTF-8: {raw:?}")
        }
    }
}

fn now_unix_millis() -> Result<i64> {
    let dur = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?;
    i64::try_from(dur.as_millis()).context("current time does not fit in i64 milliseconds")
}

fn now_unix_nanos() -> Result<i128> {
    let dur = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?;
    Ok(dur.as_nanos() as i128)
}

/// Never reset — every call within this process gets a distinct value,
/// even multiple calls in the same nanosecond (see [`unique_id`]'s doc
/// comment on why the wall clock alone isn't enough).
static UNIQUE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A collision-resistant identifier for `run_id`s and mismatch-artifact
/// filenames (code review finding, issue #33, [low]): `SystemTime::
/// as_nanos()` alone is not guaranteed unique at the clock's *actual*
/// resolution (coarser than a nanosecond on many platforms/containers),
/// so two concurrent runs — or even two calls within the same process,
/// e.g. the main corpus's `run_id` and the StaleNaN micro-case's own —
/// could otherwise share a value. Mixes wall-clock nanoseconds with the
/// OS process ID and a monotonically-increasing per-process counter
/// (never two equal values from this process) through a `splitmix64`
/// finishing step (same constants `corpus.rs`'s own `splitmix64` uses —
/// duplicated rather than shared, since the mix step is trivial and this
/// module has no dependency on `corpus`'s PRNG internals otherwise; not
/// used here for reproducibility, just as a cheap, well-distributed
/// combiner). [`dump_mismatch`] additionally guards against the residual
/// (astronomically unlikely, now) collision risk with an atomic
/// `create_new` + retry rather than trusting uniqueness alone.
pub(crate) fn unique_id() -> Result<u64> {
    let nanos = now_unix_nanos()? as u64;
    let pid = u64::from(std::process::id());
    let seq = UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut x = nanos ^ pid.rotate_left(32) ^ seq.rotate_left(17);
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    Ok(z ^ (z >> 31))
}

/// The M2 milestone gate (docs/features.md §7): see this module's doc
/// comment for the full data flow.
pub async fn metrics_differential(ctx: &Ctx) -> Result<()> {
    let fixture = load_fixture(ctx)?;
    let scale = resolve_scale()?;
    let families = match scale {
        Scale::Ci => fixture.ci,
        Scale::Full => fixture.full,
    };

    let run_id = format!("e2e-metrics-{:x}", unique_id()?);
    let now_ms = now_unix_millis()?;
    // "Corpus emits ascending, <= now" (architect plan edge case 9):
    // `base_ms` is chosen so the corpus's *last* sample lands at (or just
    // before) "now", never in the future — required for both a real
    // remote-write receiver (Prometheus rejects far-future samples) and
    // for staleness/lookback math to mean what it's meant to.
    let base_ms = now_ms - fixture.step_ms * (fixture.sample_count.saturating_sub(1) as i64);

    let spec = CorpusSpec {
        seed: fixture.seed,
        scale,
        step_ms: fixture.step_ms,
        sample_count: fixture.sample_count,
        base_ms,
        run_id: run_id.clone(),
        families,
        histogram_bounds: fixture.histogram_bounds.clone(),
    };
    let corpus = corpus::generate(&spec);

    let mut family_counts: BTreeMap<&str, usize> = BTreeMap::new();
    for s in &corpus.series {
        *family_counts.entry(s.metric_name.as_str()).or_insert(0) += 1;
    }
    println!(
        "pulsus-e2e:   metrics_differential [{:?}]: pushing {} series across families {family_counts:?} \
         / {} samples ({:?} tier, run_id={run_id:?})",
        ctx.variant, corpus.expected_series, corpus.expected_samples, corpus.scale
    );

    push_corpus(ctx, &corpus)
        .await
        .context("pushing the differential corpus through the collector failed")?;

    wait_for_completeness(
        ctx,
        &run_id,
        &corpus.per_metric_manifest(),
        corpus.first_ts_ms,
    )
    .await
    .context("differential corpus never reached completeness on both backends")?;

    run_query_matrix(ctx, &corpus, &fixture).await?;
    assert_label_resolution(ctx, &corpus).await?;
    stalenan_micro_case(ctx).await?;

    Ok(())
}

// ---------------------------------------------------------------------
// Corpus push (through the collector)
// ---------------------------------------------------------------------

/// One `POST {collector_url}/v1/metrics` attempt — same transport-failure-
/// tolerant shape as `scenarios::post_otlp_logs` (issue #15 precedent):
/// `Ok(Some(response))` once the request reaches the collector at all,
/// `Ok(None)` only on a transport-level failure (not yet listening).
async fn post_otlp_metrics(
    ctx: &Ctx,
    payload: &serde_json::Value,
) -> Result<Option<reqwest::Response>> {
    let res = ctx
        .http
        .post(format!("{}/v1/metrics", ctx.collector_url))
        .json(payload)
        .send()
        .await?;
    Ok(Some(res))
}

/// Pushes every per-timestamp OTLP export request in ascending order
/// (architect plan: "remote-write in-order requirement"). Only the first
/// request polls past a not-yet-listening collector — once any response
/// has been observed, the collector is known to be up, so the remainder
/// are plain sequential posts.
async fn push_corpus(ctx: &Ctx, corpus: &Corpus) -> Result<()> {
    let requests = corpus::to_otlp_export_requests(corpus);
    let (first, rest) = requests
        .split_first()
        .context("corpus produced no OTLP export requests to push")?;

    let res = poll_until(
        COLLECTOR_READY_POLL_TIMEOUT,
        COLLECTOR_READY_POLL_INTERVAL,
        || post_otlp_metrics(ctx, first),
    )
    .await
    .context("collector otlp/v1/metrics endpoint never accepted a connection")?;
    if !res.status().is_success() {
        bail!(
            "collector otlp/v1/metrics export (ts 0) returned {}",
            res.status()
        );
    }

    for (i, req) in rest.iter().enumerate() {
        let res = post_otlp_metrics(ctx, req).await?.with_context(|| {
            format!(
                "collector connection dropped while pushing ts index {}",
                i + 1
            )
        })?;
        if !res.status().is_success() {
            bail!(
                "collector otlp/v1/metrics export (ts index {}) returned {}",
                i + 1,
                res.status()
            );
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------
// Completeness pre-check (issue #33 amendment)
// ---------------------------------------------------------------------

/// A completeness manifest: metric name -> `(expected_series,
/// expected_samples)`. Compared **per metric name**, never as grand
/// totals (code review finding, issue #33: summed totals let a
/// compensating miscount in one family — e.g. `mem_usage_bytes` short 5,
/// `requests_total` over by 5 — silently cancel out and false-pass).
type Manifest = BTreeMap<String, (usize, usize)>;

/// Polls both backends (bounded deadline, no fixed sleeps) until each
/// independently satisfies `manifest` — for **every** metric name in it,
/// exactly `count(count_over_time(<name>{run_id="R"}[window]))==
/// expected_series` **and** `sum(count_over_time(<name>{run_id="R"}
/// [window]))==expected_samples`, never a cross-family sum ([`Manifest`]'s
/// own doc comment). Queried per concrete metric name, never a single
/// bare `{run_id="R"}` selector (code review/live-run finding):
/// PulsusDB's PromQL engine rejects a selector with no `__name__`
/// outright (`422`, "not yet supported: selector without a concrete
/// metric name" — docs/schemas.md's metric-scoped model requires one),
/// unlike real Prometheus. Uses `count_over_time` (a raw-sample range
/// selection) rather than an instant vector selector deliberately: the
/// corpus's own staleness gaps mean an instant `count(<name>{run_id="R"})`
/// evaluated after a gapped series' own lookback expires would
/// *undercount* relative to the manifest (which counts every series that
/// ever emitted, not every series live *right now*) — `count_over_time`
/// with a window spanning the whole corpus has no such lookback
/// dependency.
async fn wait_for_completeness(
    ctx: &Ctx,
    run_id: &str,
    manifest: &Manifest,
    first_ts_ms: i64,
) -> Result<()> {
    poll_until(
        COMPLETENESS_POLL_TIMEOUT,
        COMPLETENESS_POLL_INTERVAL,
        || completeness_attempt(ctx, run_id, manifest, first_ts_ms),
    )
    .await
    .with_context(|| {
        format!("run_id {run_id:?} never reached the manifest {manifest:?} on both backends")
    })?;
    Ok(())
}

/// `true` only when every metric name in `manifest` has an *exact*
/// `(series, samples)` match in `actual` — see [`Manifest`]'s doc
/// comment for why this must never degrade to a grand-total sum
/// comparison.
fn manifest_satisfied(manifest: &Manifest, actual: &Manifest) -> bool {
    manifest
        .iter()
        .all(|(name, expected)| actual.get(name) == Some(expected))
}

async fn completeness_attempt(
    ctx: &Ctx,
    run_id: &str,
    manifest: &Manifest,
    first_ts_ms: i64,
) -> Result<Option<()>> {
    let now_ms = now_unix_millis()?;
    // A window guaranteed to span the whole corpus regardless of how long
    // the poll has been retrying, recomputed fresh every attempt (an
    // earlier fixed window would under-cover once real time passes the
    // corpus's own span).
    let window_s = ((now_ms - first_ts_ms) / 1000 + 60).max(60);
    let time = ts_param(now_ms);

    for base_url in [ctx.base_url.as_str(), ctx.prometheus_url.as_str()] {
        let mut actual: Manifest = BTreeMap::new();
        for name in manifest.keys() {
            let series_expr = format!(
                r#"count(count_over_time({name}{{{label}="{run_id}"}}[{window_s}s]))"#,
                label = corpus::RUN_ID_LABEL
            );
            let samples_expr = format!(
                r#"sum(count_over_time({name}{{{label}="{run_id}"}}[{window_s}s]))"#,
                label = corpus::RUN_ID_LABEL
            );
            let series_body = query_get(
                &ctx.http,
                base_url,
                "/api/v1/query",
                &[("query", series_expr.as_str()), ("time", time.as_str())],
            )
            .await?;
            let samples_body = query_get(
                &ctx.http,
                base_url,
                "/api/v1/query",
                &[("query", samples_expr.as_str()), ("time", time.as_str())],
            )
            .await?;
            let series = single_instant_value(&series_body)?.unwrap_or(0.0).round() as usize;
            let samples = single_instant_value(&samples_body)?.unwrap_or(0.0).round() as usize;
            actual.insert(name.clone(), (series, samples));
        }
        if !manifest_satisfied(manifest, &actual) {
            return Ok(None);
        }
    }
    Ok(Some(()))
}

/// Reads a `count(...)`/`sum(...)` instant-query response's single scalar-
/// like vector element — `None` when the aggregate matched zero series
/// (Prometheus/PulsusDB both return an *empty* vector for `count()`/
/// `sum()` over no input, never a literal `0`), which is exactly "not
/// there yet" from [`completeness_attempt`]'s point of view.
fn single_instant_value(body: &serde_json::Value) -> Result<Option<f64>> {
    let result_type = body["data"]["resultType"]
        .as_str()
        .with_context(|| format!("missing data.resultType: {body}"))?;
    if result_type != "vector" {
        bail!(
            "expected a vector result from a completeness aggregate, got {result_type:?}: {body}"
        );
    }
    let arr = body["data"]["result"]
        .as_array()
        .with_context(|| format!("data.result was not an array: {body}"))?;
    match arr.first() {
        None => Ok(None),
        Some(item) => Ok(Some(parse_val(&item["value"][1])?)),
    }
}

// ---------------------------------------------------------------------
// Query matrix
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Instant,
    Range,
}

fn parse_mode(raw: &str) -> Result<Mode> {
    match raw {
        "instant" => Ok(Mode::Instant),
        "range" => Ok(Mode::Range),
        other => bail!(
            "fixture query_matrix entry has unknown mode {other:?} (expected \"instant\" or \"range\")"
        ),
    }
}

/// The instant-eval-time / range-bounds a [`compare_query`] call runs at —
/// bundled to keep that fn's (and [`dump_mismatch`]'s) argument count
/// within clippy's default threshold rather than threading four `i64`s
/// individually through every call site.
#[derive(Debug, Clone, Copy)]
struct QueryWindow {
    eval_ms: i64,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
}

async fn run_query_matrix(ctx: &Ctx, corpus: &Corpus, fixture: &DifferentialFixture) -> Result<()> {
    let window = QueryWindow {
        eval_ms: corpus.last_ts_ms,
        start_ms: corpus.first_ts_ms,
        end_ms: corpus.last_ts_ms,
        step_ms: corpus.step_ms,
    };
    for raw in &fixture.query_matrix {
        let expr = raw.expr.replace("{R}", &corpus.run_id);
        for mode_raw in &raw.modes {
            let mode = parse_mode(mode_raw)?;
            compare_query(ctx, "mismatch", &expr, mode, window)
                .await
                .with_context(|| format!("query {expr:?} ({mode:?})"))?;
        }
    }
    Ok(())
}

fn ts_param(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let millis = ms.rem_euclid(1000);
    format!("{secs}.{millis:03}")
}

fn seconds_param(ms: i64) -> String {
    ts_param(ms.max(0))
}

/// The raw response body text — kept alongside the parsed
/// [`serde_json::Value`] ([`query_get`]) so timestamp fields can be
/// re-extracted byte-exact from the wire ([`extract_raw_timestamps`]),
/// rather than only through `serde_json`'s own lossy `f64` number
/// representation (code review finding, issue #33: `parse_ts` previously
/// routed every timestamp through `as_f64()` + `* 1000.0` + `.round()`).
async fn query_get_raw(
    http: &reqwest::Client,
    base_url: &str,
    path: &str,
    params: &[(&str, &str)],
) -> Result<String> {
    let res = http
        .get(format!("{base_url}{path}"))
        .query(params)
        // Issue #92: a *request*-level timeout replaces the shared
        // client's 5s readiness budget for this request (reqwest
        // semantics) — matrix queries get a realistic 60s ceiling
        // instead of failing mid-body on a slow shared CI runner.
        .timeout(QUERY_REQUEST_TIMEOUT)
        .send()
        .await
        .with_context(|| format!("GET {base_url}{path} failed"))?;
    if !res.status().is_success() {
        bail!("GET {base_url}{path} returned {}", res.status());
    }
    res.text()
        .await
        .with_context(|| format!("GET {base_url}{path} body was not valid UTF-8 text"))
}

async fn query_get(
    http: &reqwest::Client,
    base_url: &str,
    path: &str,
    params: &[(&str, &str)],
) -> Result<serde_json::Value> {
    let raw = query_get_raw(http, base_url, path, params).await?;
    serde_json::from_str(&raw)
        .with_context(|| format!("GET {base_url}{path} body was not JSON: {raw}"))
}

/// One query, run against both backends at identical params, compared
/// bit-exact-except-NaN. `window.eval_ms` is only used for
/// [`Mode::Instant`]; `window.{start,end,step}_ms` only for
/// [`Mode::Range`] — callers always pass a full [`QueryWindow`] so this fn
/// stays a single reusable entry point for both the main query matrix and
/// the StaleNaN micro-case.
async fn compare_query(
    ctx: &Ctx,
    artifact_prefix: &str,
    expr: &str,
    mode: Mode,
    window: QueryWindow,
) -> Result<()> {
    // Owned strings throughout (not `&str` borrows of a match arm's own
    // locals): the params vec must outlive both awaited requests below,
    // well past the match expression's own scope.
    let (path, params, window_json): (&str, Vec<(String, String)>, serde_json::Value) = match mode {
        Mode::Instant => {
            let time = ts_param(window.eval_ms);
            (
                "/api/v1/query",
                vec![
                    ("query".to_string(), expr.to_string()),
                    ("time".to_string(), time.clone()),
                ],
                serde_json::json!({ "time": time }),
            )
        }
        Mode::Range => {
            let start = ts_param(window.start_ms);
            let end = ts_param(window.end_ms);
            let step = seconds_param(window.step_ms);
            (
                "/api/v1/query_range",
                vec![
                    ("query".to_string(), expr.to_string()),
                    ("start".to_string(), start.clone()),
                    ("end".to_string(), end.clone()),
                    ("step".to_string(), step.clone()),
                ],
                serde_json::json!({ "start": start, "end": end, "step": step }),
            )
        }
    };
    let params_ref: Vec<(&str, &str)> = params
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    // Raw text kept alongside the parsed body so timestamps can be
    // re-extracted byte-exact ([`extract_raw_timestamps`]) rather than
    // only through `serde_json`'s lossy `f64` number representation.
    let pulsus_started = std::time::Instant::now();
    let pulsus_raw = query_get_raw(&ctx.http, &ctx.base_url, path, &params_ref).await?;
    let pulsus_elapsed = pulsus_started.elapsed();
    let prom_started = std::time::Instant::now();
    let prom_raw = query_get_raw(&ctx.http, &ctx.prometheus_url, path, &params_ref).await?;
    let prom_elapsed = prom_started.elapsed();
    // One elapsed line per matrix entry (issue #92): the nightly's only
    // per-query diagnostic. A future budget breach (or creeping slowdown
    // toward `QUERY_REQUEST_TIMEOUT`) is then locatable from CI logs
    // alone — the 5s-cutoff failure this replaces was invisible until a
    // local replay reproduced it.
    println!(
        "pulsus-e2e: query {expr:?} ({mode:?}) pulsusdb {}ms/{}B prometheus {}ms/{}B",
        pulsus_elapsed.as_millis(),
        pulsus_raw.len(),
        prom_elapsed.as_millis(),
        prom_raw.len(),
    );
    let pulsus_body: serde_json::Value = serde_json::from_str(&pulsus_raw)
        .with_context(|| format!("pulsusdb {path} body was not JSON: {pulsus_raw}"))?;
    let prom_body: serde_json::Value = serde_json::from_str(&prom_raw)
        .with_context(|| format!("prometheus {path} body was not JSON: {prom_raw}"))?;
    let pulsus_timestamps = extract_raw_timestamps(&pulsus_raw);
    let prom_timestamps = extract_raw_timestamps(&prom_raw);

    // Both `extract_series` calls are evaluated eagerly (not short-
    // circuited with `?`) so a malformed/unexpected-shape response from
    // *either* backend still reaches the same [`dump_mismatch`] path as a
    // value/label divergence — every failure mode gets a repro artifact
    // with both raw bodies, never just the value-comparison case (review
    // finding, issue #33 live-run follow-up: a `resultType` contract
    // violation on one backend previously bailed via `?` before a repro
    // was ever written).
    let pulsus_series = extract_series(&pulsus_body, &pulsus_timestamps, mode);
    let prom_series = extract_series(&prom_body, &prom_timestamps, mode);

    let detail = match (pulsus_series, prom_series) {
        (Ok(p), Ok(r)) => diff_series_maps(&p, &r),
        (Err(err), _) => Some(format!(
            "pulsusdb response did not match the expected shape: {err:#}"
        )),
        (_, Err(err)) => Some(format!(
            "prometheus response did not match the expected shape: {err:#}"
        )),
    };

    if let Some(detail) = detail {
        let mode_str = format!("{mode:?}");
        let mismatch = Mismatch {
            expr,
            mode: &mode_str,
            window: &window_json,
            pulsus_body: &pulsus_body,
            prom_body: &prom_body,
            detail: &detail,
        };
        let path = dump_mismatch(ctx, artifact_prefix, &mismatch)?;
        bail!(
            "metrics differential mismatch: {detail} (repro dumped to {})",
            path.display()
        );
    }
    Ok(())
}

type LabelKey = Vec<(String, String)>;

fn label_key(metric: &serde_json::Value) -> LabelKey {
    let mut pairs: Vec<(String, String)> = metric
        .as_object()
        .into_iter()
        .flatten()
        .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
        .collect();
    pairs.sort();
    pairs
}

/// Extracts every bare (unquoted) JSON number immediately following an
/// opening `[` in `body`, in document order — Prometheus's/PulsusDB's
/// wire format (docs/api.md §3.1: "Sample values are always quoted JSON
/// strings") never emits an unquoted number anywhere except a `[ts,
/// "value"]`/`[[ts,"value"],...]` pair's `ts` position, so this reliably
/// enumerates every timestamp token, byte-exact from the wire, in the
/// same left-to-right order [`extract_series`]'s structural
/// `serde_json::Value` walk visits them (JSON arrays never reorder their
/// elements on parse, regardless of object-key ordering). Tracks quoted-
/// string state (with `\"` escapes) so a label value that happened to
/// contain a literal `[` followed by a digit can never be mistaken for a
/// timestamp token — code review finding, issue #33: `parse_ts` (below)
/// previously routed every timestamp through `serde_json`'s own lossy
/// `f64` number representation (`as_f64()` + `* 1000.0` + `.round()`),
/// tolerating up to 0.5ms of drift a genuinely exact wire-format
/// comparison should never need to.
fn extract_raw_timestamps(body: &str) -> Vec<&str> {
    let bytes = body.as_bytes();
    let mut out = Vec::new();
    let mut in_string = false;
    let mut escaped = false;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if in_string {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
            i += 1;
            continue;
        }
        match b {
            b'"' => {
                in_string = true;
                i += 1;
            }
            b'[' => {
                let start = i + 1;
                let mut j = start;
                while j < bytes.len()
                    && matches!(bytes[j], b'0'..=b'9' | b'.' | b'-' | b'+' | b'e' | b'E')
                {
                    j += 1;
                }
                if j > start {
                    out.push(&body[start..j]);
                }
                i += 1;
            }
            _ => i += 1,
        }
    }
    out
}

/// Parses a raw timestamp token (as extracted by [`extract_raw_timestamps`]
/// or rendered by [`ts_param`]/`prom_api::encode::prom_timestamp`) into
/// exact integer milliseconds — pure string/integer arithmetic, no `f64`
/// anywhere. The wire format is always `<seconds>` or `<seconds>.<frac>`
/// with `frac` the millisecond remainder, zero-padded to 3 digits then
/// right-trimmed of trailing zeros (`prom_timestamp`'s own algorithm) —
/// so re-padding `frac` back out to 3 digits on the right recovers the
/// exact millisecond remainder. A `frac` longer than 3 digits means the
/// value carries sub-millisecond precision this scenario's model has no
/// representation for — rejected outright (code review finding: "reject
/// non-millisecond-aligned values") rather than silently truncated.
/// Negative timestamps are out of this scenario's domain (always real,
/// recent Unix milliseconds) and rejected too, rather than guessing at
/// floor/truncate semantics for a case that should never occur.
fn parse_ts_exact(raw: &str) -> Result<i64> {
    if let Some(stripped) = raw.strip_prefix('-') {
        bail!("timestamp {raw:?} is negative (unsupported — stripped: {stripped:?})");
    }
    let (secs_str, frac_str) = raw.split_once('.').unwrap_or((raw, ""));
    if !secs_str.bytes().all(|b| b.is_ascii_digit()) || secs_str.is_empty() {
        bail!("timestamp {raw:?} has a non-decimal seconds component {secs_str:?}");
    }
    if frac_str.len() > 3 {
        bail!(
            "timestamp {raw:?} has sub-millisecond precision (fractional part {frac_str:?} \
             longer than 3 digits) — not millisecond-aligned"
        );
    }
    if !frac_str.bytes().all(|b| b.is_ascii_digit()) {
        bail!("timestamp {raw:?} has a non-decimal fractional component {frac_str:?}");
    }
    let secs: i64 = secs_str
        .parse()
        .with_context(|| format!("timestamp {raw:?} seconds component overflowed i64"))?;
    let mut frac = frac_str.to_string();
    while frac.len() < 3 {
        frac.push('0');
    }
    let millis: i64 = frac.parse().with_context(|| {
        format!("timestamp {raw:?} fractional component {frac:?} was not decimal")
    })?;
    secs.checked_mul(1000)
        .and_then(|ms| ms.checked_add(millis))
        .with_context(|| format!("timestamp {raw:?} overflowed i64 milliseconds"))
}

fn parse_val(v: &serde_json::Value) -> Result<f64> {
    let s = v
        .as_str()
        .with_context(|| format!("value {v} was not a string"))?;
    s.parse::<f64>()
        .with_context(|| format!("value {s:?} was not a parseable f64"))
}

/// Bit-exact-except-NaN value equality (architect plan, pinned): any NaN
/// class equals any NaN class; every other value (incl. `±Inf`, `-0.0` vs
/// `0.0`) must match `to_bits()` exactly.
fn samples_equal(a: f64, b: f64) -> bool {
    (a.is_nan() && b.is_nan()) || a.to_bits() == b.to_bits()
}

/// `raw_timestamps` is the [`extract_raw_timestamps`] queue for this same
/// `body`'s raw text, consumed strictly in the order this fn's structural
/// walk encounters each `.value`/`.values[]` timestamp — every timestamp
/// is parsed byte-exact via [`parse_ts_exact`], never through `body`'s
/// own (lossy) `f64`-backed `serde_json::Value` number representation.
fn extract_series(
    body: &serde_json::Value,
    raw_timestamps: &[&str],
    mode: Mode,
) -> Result<BTreeMap<LabelKey, BTreeMap<i64, f64>>> {
    let result_type = body["data"]["resultType"]
        .as_str()
        .with_context(|| format!("missing data.resultType: {body}"))?;
    let result = body["data"]["result"]
        .as_array()
        .with_context(|| format!("data.result was not an array: {body}"))?;
    let mut out = BTreeMap::new();
    let mut ts_iter = raw_timestamps.iter();
    let mut next_ts = || -> Result<i64> {
        let raw = ts_iter
            .next()
            .context("ran out of raw timestamp tokens while parsing the response body")?;
        parse_ts_exact(raw)
    };
    match (mode, result_type) {
        (Mode::Instant, "vector") => {
            for item in result {
                let key = label_key(&item["metric"]);
                let ts = next_ts()?;
                let val = parse_val(&item["value"][1])?;
                out.entry(key).or_insert_with(BTreeMap::new).insert(ts, val);
            }
        }
        // Issue #66 (M6-03): scalar-typed instant queries (`time()`,
        // `scalar(...)`). The wire shape is a bare `[ts, "value"]` pair
        // in `data.result` (docs/api.md §3.1 — identical on both stores),
        // not an array of series. Stored under a synthetic label key so a
        // scalar on one store can never silently compare equal to an
        // empty-labelset vector on the other — a resultType disagreement
        // must surface as a series-set diff, not a false pass. Values and
        // the timestamp go through the exact same bit-exact-except-NaN /
        // `parse_ts_exact` discipline as every vector sample (both stores
        // evaluate at the identical pinned eval time, so the pair must
        // match exactly).
        (Mode::Instant, "scalar") => {
            if result.len() != 2 {
                bail!("scalar result must be a [ts, value] pair: {body}");
            }
            let key = vec![("__result_type__".to_string(), "scalar".to_string())];
            out.insert(key, BTreeMap::from([(next_ts()?, parse_val(&result[1])?)]));
        }
        // Range mode needs no scalar arm: both stores wrap a scalar-typed
        // expression's range result into an ordinary matrix with one
        // empty-labelset series (Prometheus's engine does this natively;
        // PulsusDB's evaluator returns `QueryValue::Matrix` for every
        // range query), so the `(Range, "matrix")` arm below covers it.
        (Mode::Range, "matrix") => {
            for item in result {
                let key = label_key(&item["metric"]);
                let points = item["values"]
                    .as_array()
                    .with_context(|| format!("series {key:?} missing a values array: {body}"))?;
                let mut map = BTreeMap::new();
                for p in points {
                    map.insert(next_ts()?, parse_val(&p[1])?);
                }
                out.insert(key, map);
            }
        }
        (m, rt) => bail!("unexpected resultType {rt:?} for query mode {m:?}: {body}"),
    }
    Ok(out)
}

/// `None` when every series and every timestamp within it matches
/// bit-exact-except-NaN; otherwise a human-readable description of the
/// first divergence found (used in both the bail message and the
/// [`dump_mismatch`] artifact's `detail` field).
fn diff_series_maps(
    a: &BTreeMap<LabelKey, BTreeMap<i64, f64>>,
    b: &BTreeMap<LabelKey, BTreeMap<i64, f64>>,
) -> Option<String> {
    let a_keys: BTreeSet<&LabelKey> = a.keys().collect();
    let b_keys: BTreeSet<&LabelKey> = b.keys().collect();
    if a_keys != b_keys {
        let only_pulsus: Vec<&&LabelKey> = a_keys.difference(&b_keys).collect();
        let only_prom: Vec<&&LabelKey> = b_keys.difference(&a_keys).collect();
        return Some(format!(
            "series set differs: only-on-pulsusdb={only_pulsus:?} only-on-prometheus={only_prom:?}"
        ));
    }
    for (key, pts_a) in a {
        // Infallible: `a_keys == b_keys` was just asserted above.
        let pts_b = b.get(key).expect("key set equality checked above");
        let ts_a: BTreeSet<&i64> = pts_a.keys().collect();
        let ts_b: BTreeSet<&i64> = pts_b.keys().collect();
        if ts_a != ts_b {
            return Some(format!(
                "series {key:?}: timestamp set differs: only-on-pulsusdb={:?} only-on-prometheus={:?}",
                ts_a.difference(&ts_b).collect::<Vec<_>>(),
                ts_b.difference(&ts_a).collect::<Vec<_>>()
            ));
        }
        for (ts, va) in pts_a {
            let vb = pts_b[ts];
            if !samples_equal(*va, vb) {
                return Some(format!(
                    "series {key:?} at ts {ts}: pulsusdb={va} prometheus={vb}"
                ));
            }
        }
    }
    None
}

/// Everything one mismatch repro artifact needs (architect plan: "query,
/// mode, window, both raw responses") — bundled to keep [`dump_mismatch`]'s
/// argument count within clippy's default threshold. `mode` is a plain
/// `&str` (not [`Mode`]) so this same artifact shape covers both the
/// query-matrix path (`"Instant"`/`"Range"`) and the historical-window
/// label-resolution path (`"series"`/`"labels"`/`"label_values"` — code
/// review finding, issue #33: those comparisons previously bailed with no
/// persisted artifact at all).
struct Mismatch<'a> {
    expr: &'a str,
    mode: &'a str,
    window: &'a serde_json::Value,
    pulsus_body: &'a serde_json::Value,
    prom_body: &'a serde_json::Value,
    detail: &'a str,
}

/// Bounds [`write_artifact`]'s `create_new` retry loop — [`unique_id`]
/// already makes a same-name collision astronomically unlikely; this is
/// belt-and-suspenders, not a realistic retry count.
const ARTIFACT_CREATE_RETRIES: u32 = 8;

/// Writes one pre-built artifact `serde_json::Value` under
/// `target/e2e-artifacts/<area>/<variant>/<prefix>-<unique_id>.json`
/// — the shared file-creation mechanics both [`dump_mismatch`] (the
/// value-matrix path) and `assert_discovery_contract_window`'s
/// documented-contract dump share (`area = "metrics-diff"`), and the
/// issue #60 traces differential reuses under `area = "traces-diff"`;
/// only the artifact *shape* differs between callers.
pub(crate) fn write_artifact(
    ctx: &Ctx,
    area: &str,
    prefix: &str,
    artifact: &serde_json::Value,
) -> Result<PathBuf> {
    let variant_dir = match ctx.variant {
        Variant::Single => "single",
        Variant::Cluster => "cluster",
    };
    let dir = crate::engine::workspace_root()
        .join("target/e2e-artifacts")
        .join(area)
        .join(variant_dir);
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("failed to create artifact dir {}", dir.display()))?;
    let body = serde_json::to_string_pretty(artifact)?;

    // `create_new` (code review finding, issue #33, [low]): never
    // silently overwrites a same-named artifact from a concurrent run —
    // on a name collision (astronomically unlikely with `unique_id`, but
    // not structurally impossible), retries with a freshly generated
    // name rather than clobbering another run's evidence.
    use std::io::Write;
    for _ in 0..ARTIFACT_CREATE_RETRIES {
        let path = dir.join(format!("{prefix}-{:x}.json", unique_id()?));
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(mut file) => {
                file.write_all(body.as_bytes()).with_context(|| {
                    format!("failed to write mismatch artifact {}", path.display())
                })?;
                return Ok(path);
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to create mismatch artifact {}", path.display())
                });
            }
        }
    }
    bail!(
        "failed to create a mismatch artifact in {}: exhausted {ARTIFACT_CREATE_RETRIES} unique-name retries",
        dir.display()
    )
}

fn dump_mismatch(ctx: &Ctx, prefix: &str, m: &Mismatch) -> Result<PathBuf> {
    let artifact = serde_json::json!({
        "query": m.expr,
        "mode": m.mode,
        "window": m.window,
        "pulsusdb_result": m.pulsus_body,
        "prometheus_result": m.prom_body,
        "detail": m.detail,
    });
    write_artifact(ctx, "metrics-diff", prefix, &artifact)
}

// ---------------------------------------------------------------------
// Historical-window label resolution (issue #33 architect adjudication,
// #30/#31 finding 2) — a DOCUMENTED-CONTRACT test, not a value-matrix
// comparison
// ---------------------------------------------------------------------
//
// PulsusDB's `/series`/`/labels`/label-values are bucket-granularity by
// design (docs/schemas.md §2.1: the bucket-floored read bounds "can
// over-include series adjacent to the query window ... but can never miss
// one") — a documented, bounded superset of Prometheus's exact-sample-
// window answer, not a bug. The differential's job here is therefore to
// assert PulsusDB matches **our own documented contract**, computed from
// the corpus manifest + the §2.1 bucket-floor rule
// ([`corpus::Corpus::discoverable_series`]) — Prometheus's answer is
// fetched only as a **logged reference** for the mismatch artifact's
// diagnostic delta, never asserted against. This is unrelated to (and
// does not weaken) the query **value matrix**'s strict, never-allowlisted
// Prometheus-equality comparison ([`compare_query`]/[`diff_series_maps`]),
// which PromQL `count`/`group` now also belongs to (the architect
// adjudication's finding 1: those functions are exact lookback-aware
// PromQL evaluation, not a discovery endpoint, so they stay in the
// value-matrix fixture, not here).

/// Three windows over the corpus's own span (architect plan): (a) fully
/// containing it, (b) partially overlapping its leading edge, (c) a
/// narrow slice landing strictly *between* two adjacent samples — under
/// the bucket-floor contract this is still expected to resolve every
/// series whose activity bucket overlaps the (bucket-floored) window,
/// including ones a literal-timestamp match would miss (the "false-empty
/// class" #30 fixed).
async fn assert_label_resolution(ctx: &Ctx, corpus: &Corpus) -> Result<()> {
    let selector = format!(
        r#"{{{label}="{run_id}"}}"#,
        label = corpus::RUN_ID_LABEL,
        run_id = corpus.run_id
    );
    let full_span = (corpus.last_ts_ms - corpus.first_ts_ms).max(1);

    let mid_idx = corpus.sample_count / 2;
    let mid_ts = corpus::ts_ms(corpus, mid_idx) + corpus.step_ms / 2;

    let windows: [(&str, i64, i64); 3] = [
        (
            "fully-containing",
            corpus.first_ts_ms - 60_000,
            corpus.last_ts_ms + 60_000,
        ),
        (
            "leading-edge-overlap",
            corpus.first_ts_ms - full_span,
            corpus.first_ts_ms + full_span / 4,
        ),
        ("bucket-floor-only", mid_ts - 1_000, mid_ts + 1_000),
    ];

    let discoverable = corpus.discoverable_series();
    for (name, start_ms, end_ms) in windows {
        assert_discovery_contract_window(ctx, &selector, &discoverable, name, start_ms, end_ms)
            .await
            .with_context(|| format!("historical-window label-resolution case {name:?}"))?;
    }
    Ok(())
}

fn string_set(body: &serde_json::Value) -> Result<BTreeSet<String>> {
    Ok(body["data"]
        .as_array()
        .with_context(|| format!("data was not an array: {body}"))?
        .iter()
        .filter_map(|v| v.as_str())
        .map(str::to_string)
        .collect())
}

fn series_label_set(body: &serde_json::Value) -> Result<BTreeSet<LabelKey>> {
    Ok(body["data"]
        .as_array()
        .with_context(|| format!("data was not an array: {body}"))?
        .iter()
        .map(label_key)
        .collect())
}

/// `series_label_set`/`label_key`'s result set, rendered as
/// `BTreeSet<String>` (each series' sorted label pairs, `Debug`-formatted)
/// — the same comparable shape [`corpus::DiscoverableSeries::labels`]
/// renders to, so the two sides of [`assert_discovery_endpoint`]'s
/// `/series` comparison are directly comparable strings. Kept separate
/// from `string_set` (the `/labels`/`/label/{name}/values` shape) since
/// `/series` returns label *objects*, not bare strings.
fn series_label_strings(body: &serde_json::Value) -> Result<BTreeSet<String>> {
    Ok(series_label_set(body)?
        .into_iter()
        .map(|pairs| format!("{pairs:?}"))
        .collect())
}

/// The same `Debug`-formatted shape [`series_label_strings`] renders a
/// live response's label object to — applied to a
/// [`corpus::DiscoverableSeries`]'s already-sorted `labels` so both sides
/// of the `/series` comparison are directly comparable strings.
fn discoverable_label_string(labels: &[(String, String)]) -> String {
    format!("{labels:?}")
}

/// One discovery endpoint (`/series`/`/labels`/`/label/{name}/values`),
/// compared against the **documented bucket-floor contract's**
/// expectation (not Prometheus) — see this section's own doc comment.
/// Prometheus's answer is still fetched and included in the mismatch
/// artifact as a logged reference/diagnostic delta, never as the
/// expectation; a failure to even parse Prometheus's reference response
/// does not fail this assertion (it is not load-bearing), only a
/// mismatch between PulsusDB's actual set and `expected` does.
async fn assert_discovery_endpoint(
    ctx: &Ctx,
    path: &str,
    case_name: &str,
    params: &[(&str, &str)],
    window: &serde_json::Value,
    expected: &BTreeSet<String>,
    extract: impl Fn(&serde_json::Value) -> Result<BTreeSet<String>>,
) -> Result<()> {
    let pulsus_body = query_get(&ctx.http, &ctx.base_url, path, params).await?;
    let pulsus_actual = extract(&pulsus_body)
        .with_context(|| format!("pulsusdb {path} response did not match the expected shape"))?;

    if &pulsus_actual == expected {
        return Ok(());
    }

    // Reference only from here — Prometheus's answer is diagnostic
    // context for the artifact, not part of the pass/fail decision above
    // (already made): a transport/shape failure fetching it must not mask
    // the real (already-detected) contract violation.
    let prom_reference = match query_get(&ctx.http, &ctx.prometheus_url, path, params).await {
        Ok(body) => extract(&body).unwrap_or_default(),
        Err(_) => BTreeSet::new(),
    };
    let only_expected: Vec<&String> = expected.difference(&pulsus_actual).collect();
    let only_actual: Vec<&String> = pulsus_actual.difference(expected).collect();
    let detail = format!(
        "{path} diverged from the documented bucket-floor contract (docs/schemas.md §2.1): \
         missing-from-pulsusdb={only_expected:?} unexpected-in-pulsusdb={only_actual:?} \
         (prometheus exact-sample-window reference, not the expectation: {prom_reference:?})"
    );
    let artifact = serde_json::json!({
        "query": path,
        "case": case_name,
        "window": window,
        "note": "documented-contract test (docs/schemas.md §2.1 bucket-floor rule): \
                 `expected` is computed from the corpus manifest + the bucket-floor rule, \
                 NOT Prometheus's exact-sample-window answer. `prometheus_reference` is \
                 diagnostic context only, never the expectation — this is not a value-matrix \
                 allowlist, and the query value matrix's never-allowlist rule is untouched.",
        "expected_from_bucket_floor_contract": expected,
        "pulsusdb_result": pulsus_actual,
        "prometheus_reference_only": prom_reference,
        "detail": detail,
    });
    let artifact_path = write_artifact(ctx, "metrics-diff", "series-labels", &artifact)?;
    bail!(
        "documented-contract mismatch: {detail} (repro dumped to {})",
        artifact_path.display()
    );
}

async fn assert_discovery_contract_window(
    ctx: &Ctx,
    selector: &str,
    discoverable: &[corpus::DiscoverableSeries],
    case_name: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<()> {
    let start = ts_param(start_ms);
    let end = ts_param(end_ms);
    let params = [
        ("match[]", selector),
        ("start", start.as_str()),
        ("end", end.as_str()),
    ];
    let window = serde_json::json!({ "match[]": selector, "start": start, "end": end });

    // The §2.1 bucket-floor rule, applied to this window's own bounds —
    // the exact same floor `discovery_query` (`pulsus-read/src/metrics/
    // sql.rs`) renders the SQL `WHERE unix_milli >= .. AND unix_milli <=
    // ..` predicate from.
    let lower_bucket = corpus::activity_bucket(start_ms);
    let upper_bucket = corpus::activity_bucket(end_ms);
    let in_window: Vec<&corpus::DiscoverableSeries> = discoverable
        .iter()
        .filter(|d| {
            d.buckets
                .iter()
                .any(|&b| b >= lower_bucket && b <= upper_bucket)
        })
        .collect();

    let expected_series: BTreeSet<String> = in_window
        .iter()
        .map(|d| discoverable_label_string(&d.labels))
        .collect();
    let expected_label_names: BTreeSet<String> = in_window
        .iter()
        .flat_map(|d| d.labels.iter().map(|(k, _)| k.clone()))
        .collect();
    let expected_service_values: BTreeSet<String> = in_window
        .iter()
        .filter_map(|d| {
            d.labels
                .iter()
                .find(|(k, _)| k == "service")
                .map(|(_, v)| v.clone())
        })
        .collect();

    assert_discovery_endpoint(
        ctx,
        "/api/v1/series",
        case_name,
        &params,
        &window,
        &expected_series,
        series_label_strings,
    )
    .await?;
    assert_discovery_endpoint(
        ctx,
        "/api/v1/labels",
        case_name,
        &params,
        &window,
        &expected_label_names,
        string_set,
    )
    .await?;
    assert_discovery_endpoint(
        ctx,
        "/api/v1/label/service/values",
        case_name,
        &params,
        &window,
        &expected_service_values,
        string_set,
    )
    .await?;

    Ok(())
}

// ---------------------------------------------------------------------
// StaleNaN micro-case (direct remote-write, bypassing the collector —
// module doc comment)
// ---------------------------------------------------------------------

/// `prompb.WriteRequest`/`TimeSeries`/`Label`/`Sample`: the same hand-
/// rolled tag layout `pulsus-write::protocols::remote_write` uses (RW-1.0
/// stable schema) — duplicated rather than imported for the same "no
/// internal-crate dependency" reason as [`STALE_NAN_BITS`] above.
#[derive(Clone, PartialEq, ::prost::Message)]
struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    timeseries: Vec<TimeSeries>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    samples: Vec<Sample>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct Label {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    value: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct Sample {
    #[prost(double, tag = "1")]
    value: f64,
    #[prost(int64, tag = "2")]
    timestamp: i64,
}

/// `services` independent series, each carrying two ordinary samples
/// (`base_ms`, `base_ms + step_ms`) followed by an explicit StaleNaN
/// marker sample at `base_ms + 2*step_ms` — labels sorted by name (real
/// Prometheus's remote-write receiver rejects out-of-order label sets).
fn build_stalenan_write_request(
    run_id: &str,
    base_ms: i64,
    step_ms: i64,
    services: usize,
) -> WriteRequest {
    let timeseries = (0..services)
        .map(|service_idx| {
            let mut labels = vec![
                Label {
                    name: "__name__".to_string(),
                    value: STALE_METRIC.to_string(),
                },
                Label {
                    name: "service".to_string(),
                    value: corpus::service_label(service_idx),
                },
                Label {
                    name: corpus::RUN_ID_LABEL.to_string(),
                    value: run_id.to_string(),
                },
            ];
            labels.sort_by(|a, b| a.name.cmp(&b.name));

            let samples = vec![
                Sample {
                    value: 100.0 + service_idx as f64,
                    timestamp: base_ms,
                },
                Sample {
                    value: 200.0 + service_idx as f64,
                    timestamp: base_ms + step_ms,
                },
                Sample {
                    value: f64::from_bits(STALE_NAN_BITS),
                    timestamp: base_ms + step_ms * 2,
                },
            ];
            TimeSeries { labels, samples }
        })
        .collect();
    WriteRequest { timeseries }
}

fn encode_stalenan_write_request(req: &WriteRequest) -> Result<Vec<u8>> {
    let bytes = req.encode_to_vec();
    snap::raw::Encoder::new()
        .compress_vec(&bytes)
        .context("failed to snappy-compress the StaleNaN micro-case WriteRequest")
}

/// Direct `POST {base_url}/api/v1/write` (bypassing the collector — module
/// doc comment): both PulsusDB (`docs/api.md §1.2`, issue #28) and a real
/// Prometheus started with `--web.enable-remote-write-receiver` accept
/// RW-1.0 at this same path.
async fn post_remote_write(http: &reqwest::Client, base_url: &str, body: Vec<u8>) -> Result<()> {
    let res = http
        .post(format!("{base_url}/api/v1/write"))
        .header("content-encoding", "snappy")
        .header("content-type", "application/x-protobuf")
        .header("x-prometheus-remote-write-version", "0.1.0")
        .body(body)
        .send()
        .await
        .with_context(|| format!("POST {base_url}/api/v1/write failed"))?;
    if !res.status().is_success() {
        bail!("POST {base_url}/api/v1/write returned {}", res.status());
    }
    Ok(())
}

/// The StaleNaN micro-case (module doc comment): same guardrails as the
/// collector-fed corpus (own `run_id`, completeness pre-check, bit-exact-
/// except-NaN comparison, `stalenan-*.json` repro on mismatch) — proves
/// both backends treat an explicit stale-marker sample identically
/// (dropped from every query surface, per `pulsus-promql::eval::staleness`
/// — the same behavior real Prometheus's storage layer implements), a
/// representation OTLP itself cannot carry.
async fn stalenan_micro_case(ctx: &Ctx) -> Result<()> {
    let run_id = format!("e2e-metrics-stalenan-{:x}", unique_id()?);
    let now_ms = now_unix_millis()?;
    let step_ms: i64 = 15_000;
    let base_ms = now_ms - step_ms * 2;
    const SERVICES: usize = 3;

    let req = build_stalenan_write_request(&run_id, base_ms, step_ms, SERVICES);
    let body = encode_stalenan_write_request(&req)?;

    post_remote_write(&ctx.http, &ctx.base_url, body.clone())
        .await
        .context("direct remote-write of the StaleNaN micro-case to PulsusDB failed")?;
    post_remote_write(&ctx.http, &ctx.prometheus_url, body)
        .await
        .context("direct remote-write of the StaleNaN micro-case to Prometheus failed")?;

    // The marker sample itself is never counted by `count_over_time`
    // (both engines drop stale-NaN-marked samples from every range-vector
    // selection, `pulsus-promql::eval::mod::windowed_non_stale`) — the
    // manifest only covers the two ordinary samples per series.
    let manifest: Manifest = BTreeMap::from([(STALE_METRIC.to_string(), (SERVICES, SERVICES * 2))]);
    wait_for_completeness(ctx, &run_id, &manifest, base_ms)
        .await
        .context("StaleNaN micro-case never reached completeness on both backends")?;

    let marker_ts = base_ms + step_ms * 2;
    let expr = format!(
        r#"{{__name__="{STALE_METRIC}",{label}="{run_id}"}}"#,
        label = corpus::RUN_ID_LABEL
    );

    let instant_window = QueryWindow {
        eval_ms: marker_ts,
        start_ms: base_ms,
        end_ms: marker_ts,
        step_ms,
    };
    compare_query(ctx, "stalenan", &expr, Mode::Instant, instant_window)
        .await
        .context("StaleNaN micro-case: instant query at the marker timestamp")?;

    let range_window = QueryWindow {
        eval_ms: marker_ts,
        start_ms: base_ms - step_ms,
        end_ms: marker_ts + step_ms,
        step_ms,
    };
    compare_query(ctx, "stalenan", &expr, Mode::Range, range_window)
        .await
        .context("StaleNaN micro-case: range query spanning the marker timestamp")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Issue #92 regression proof, tripping the nightly's actual failure
    /// mode: the shared harness client carries a tight readiness-probe
    /// timeout, and before the fix that budget bounded every matrix
    /// query — a backend merely *slower* than the budget failed mid-body
    /// with zero server-side errors. Reproduced here with a server that
    /// answers correctly but only after the client-level budget has
    /// elapsed: the bare client request (the pre-fix path) must time
    /// out, while [`query_get_raw`] — which sets the per-request
    /// `QUERY_REQUEST_TIMEOUT` — must succeed against the same server,
    /// proving `RequestBuilder::timeout` *replaces* the client's total
    /// timeout rather than merely racing it.
    #[tokio::test]
    async fn query_request_timeout_overrides_the_clients_readiness_budget() {
        use tokio::io::AsyncWriteExt;

        const CLIENT_BUDGET: Duration = Duration::from_millis(100);
        const RESPONSE_DELAY: Duration = Duration::from_millis(400);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    tokio::time::sleep(RESPONSE_DELAY).await;
                    let _ = socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok",
                        )
                        .await;
                    let _ = socket.shutdown().await;
                });
            }
        });

        // Same shape as the harness's scenario client: a client-level
        // total timeout shorter than the server's response time.
        let http = reqwest::Client::builder()
            .timeout(CLIENT_BUDGET)
            .build()
            .unwrap();
        let base = format!("http://{addr}");

        // Pre-fix path: the client-level budget cuts the request off.
        let bare = http.get(format!("{base}/api/v1/query")).send().await;
        assert!(
            bare.is_err(),
            "expected the {CLIENT_BUDGET:?} client budget to cut off a {RESPONSE_DELAY:?} response"
        );

        // Fixed path: `query_get_raw`'s per-request timeout wins and the
        // full body arrives intact.
        let body = query_get_raw(&http, &base, "/api/v1/query", &[("query", "up")])
            .await
            .expect("the per-request query timeout should override the client budget");
        assert_eq!(body, "ok");

        server.abort();
    }

    /// Serves `body` as a well-formed `HTTP/1.1 200` JSON response on an
    /// ephemeral loopback port for every connection — a stand-in query
    /// backend for [`compare_query`]'s hermetic success-path test below.
    async fn spawn_stub_backend(body: &'static str) -> std::net::SocketAddr {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
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

    /// Issue #92: drives [`compare_query`] end-to-end against two stub
    /// backends returning byte-identical vector responses — the compose
    /// leg stays CI-authoritative for real matrix replays, but this
    /// proves hermetically that the success path (including the new
    /// per-entry elapsed log line, visible under `--nocapture`) runs
    /// clean: identical answers compare equal and no mismatch artifact
    /// is attempted.
    #[tokio::test]
    async fn compare_query_accepts_identical_stub_backends_and_logs_elapsed() {
        const BODY: &str = r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1435781451.781,"1"]}]}}"#;
        let pulsus_addr = spawn_stub_backend(BODY).await;
        let prom_addr = spawn_stub_backend(BODY).await;

        let ctx = Ctx {
            http: reqwest::Client::new(),
            base_url: format!("http://{pulsus_addr}"),
            collector_url: String::new(),
            prometheus_url: format!("http://{prom_addr}"),
            tempo_url: String::new(),
            loki_url: String::new(),
            variant: Variant::Single,
            fixtures_dir: PathBuf::new(),
            compose: crate::engine::Compose::new(
                crate::engine::EngineKind::Docker,
                vec![],
                "pulsus-e2e-metrics-unit-stub",
            ),
        };
        let window = QueryWindow {
            eval_ms: 1_435_781_451_781,
            start_ms: 0,
            end_ms: 0,
            step_ms: 0,
        };
        compare_query(&ctx, "mismatch", "up", Mode::Instant, window)
            .await
            .expect("byte-identical stub responses must compare equal");
    }

    /// Issue #33 code review, [medium]: a per-metric-family compensating
    /// miscount (one family short, another over, by the same amount) must
    /// still be rejected — the manifest is compared per metric name, never
    /// as a grand total that could net the two errors out to zero.
    #[test]
    fn manifest_satisfied_rejects_a_compensating_cross_family_miscount() {
        let manifest: Manifest = BTreeMap::from([
            ("mem_usage_bytes".to_string(), (100, 1000)),
            ("requests_total".to_string(), (100, 1000)),
        ]);
        // Grand totals still match (200 series / 2000 samples both ways),
        // but each individual family is wrong.
        let actual: Manifest = BTreeMap::from([
            ("mem_usage_bytes".to_string(), (95, 1000)),
            ("requests_total".to_string(), (105, 1000)),
        ]);
        assert!(!manifest_satisfied(&manifest, &actual));
    }

    #[test]
    fn manifest_satisfied_accepts_an_exact_per_metric_match() {
        let manifest: Manifest = BTreeMap::from([
            ("mem_usage_bytes".to_string(), (100, 1000)),
            ("requests_total".to_string(), (50, 500)),
        ]);
        let actual = manifest.clone();
        assert!(manifest_satisfied(&manifest, &actual));
    }

    #[test]
    fn samples_equal_treats_any_two_nans_as_equal() {
        assert!(samples_equal(f64::NAN, f64::from_bits(STALE_NAN_BITS)));
        assert!(samples_equal(f64::NAN, f64::NAN));
    }

    #[test]
    fn samples_equal_is_bit_exact_for_non_nan_values() {
        assert!(samples_equal(1.0, 1.0));
        assert!(!samples_equal(1.0, 1.0 + f64::EPSILON));
        assert!(!samples_equal(0.0, -0.0));
        assert!(samples_equal(f64::INFINITY, f64::INFINITY));
        assert!(!samples_equal(f64::INFINITY, f64::NEG_INFINITY));
    }

    #[test]
    fn parse_mode_accepts_only_the_two_documented_strings() {
        assert_eq!(parse_mode("instant").unwrap(), Mode::Instant);
        assert_eq!(parse_mode("range").unwrap(), Mode::Range);
        assert!(parse_mode("bogus").is_err());
    }

    #[test]
    fn ts_param_renders_fractional_milliseconds() {
        assert_eq!(ts_param(1_435_781_451_781), "1435781451.781");
        assert_eq!(ts_param(1_435_781_451_000), "1435781451.000");
    }

    /// Issue #33 code review, [medium]: exact string/integer parsing —
    /// including the `prom_timestamp`-style trailing-zero-trimmed
    /// fractional forms real Prometheus (and PulsusDB's own encoder)
    /// actually emit (`.5` -> 500ms, `.78` -> 780ms), never routed
    /// through `f64`.
    #[test]
    fn parse_ts_exact_reconstructs_millisecond_precision_without_float_rounding() {
        assert_eq!(parse_ts_exact("1435781451").unwrap(), 1_435_781_451_000);
        assert_eq!(parse_ts_exact("1435781451.781").unwrap(), 1_435_781_451_781);
        assert_eq!(parse_ts_exact("1435781451.5").unwrap(), 1_435_781_451_500);
        assert_eq!(parse_ts_exact("1435781451.78").unwrap(), 1_435_781_451_780);
        assert_eq!(parse_ts_exact("1435781451.005").unwrap(), 1_435_781_451_005);
        assert_eq!(parse_ts_exact("0").unwrap(), 0);
    }

    #[test]
    fn parse_ts_exact_rejects_sub_millisecond_precision() {
        assert!(parse_ts_exact("1435781451.7810").is_err());
        assert!(parse_ts_exact("1435781451.1234").is_err());
    }

    #[test]
    fn parse_ts_exact_rejects_non_decimal_and_negative_input() {
        assert!(parse_ts_exact("-5").is_err());
        assert!(parse_ts_exact("abc").is_err());
        assert!(parse_ts_exact("1.2.3").is_err());
    }

    /// Round-trips every `ts_param` rendering back through
    /// `parse_ts_exact` — the same shape a live query's `time`/`start`/
    /// `end` params, and the wire timestamps returned in a response, both
    /// use.
    #[test]
    fn parse_ts_exact_round_trips_ts_param() {
        for ms in [
            0i64,
            1,
            500,
            999,
            1_000,
            1_435_781_451_781,
            1_784_064_779_679,
        ] {
            assert_eq!(
                parse_ts_exact(&ts_param(ms)).unwrap(),
                ms,
                "round trip failed for {ms}"
            );
        }
    }

    #[test]
    fn extract_raw_timestamps_finds_vector_and_matrix_shapes() {
        let vector = r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"a":"1"},"value":[1435781451.781,"42"]}]}}"#;
        assert_eq!(extract_raw_timestamps(vector), vec!["1435781451.781"]);

        let matrix = r#"{"data":{"result":[{"metric":{},"values":[[1000,"1"],[1000.5,"2"]]}]}}"#;
        assert_eq!(extract_raw_timestamps(matrix), vec!["1000", "1000.5"]);
    }

    /// Issue #66 (M6-03): scalar-typed instant responses (`time()`,
    /// `scalar(...)`) — the `[ts, "value"]` pair parses through the same
    /// byte-exact timestamp path and lands under the synthetic
    /// `__result_type__=scalar` key.
    #[test]
    fn extract_series_parses_an_instant_scalar_result() {
        let raw = r#"{"status":"success","data":{"resultType":"scalar","result":[1435781451.781,"55.5"]}}"#;
        let body: serde_json::Value = serde_json::from_str(raw).unwrap();
        let timestamps = extract_raw_timestamps(raw);
        assert_eq!(timestamps, vec!["1435781451.781"]);
        let series = extract_series(&body, &timestamps, Mode::Instant).unwrap();
        assert_eq!(series.len(), 1);
        let key = vec![("__result_type__".to_string(), "scalar".to_string())];
        assert_eq!(series[&key], BTreeMap::from([(1_435_781_451_781, 55.5)]));
    }

    /// A scalar result on one store and an empty-labelset vector on the
    /// other must diff as a series-set mismatch — never a false pass
    /// (the synthetic key's whole purpose).
    #[test]
    fn a_scalar_and_an_empty_labelset_vector_never_compare_equal() {
        let scalar_raw =
            r#"{"status":"success","data":{"resultType":"scalar","result":[1000,"42"]}}"#;
        let vector_raw = r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1000,"42"]}]}}"#;
        let scalar_body: serde_json::Value = serde_json::from_str(scalar_raw).unwrap();
        let vector_body: serde_json::Value = serde_json::from_str(vector_raw).unwrap();
        let a = extract_series(
            &scalar_body,
            &extract_raw_timestamps(scalar_raw),
            Mode::Instant,
        )
        .unwrap();
        let b = extract_series(
            &vector_body,
            &extract_raw_timestamps(vector_raw),
            Mode::Instant,
        )
        .unwrap();
        let detail = diff_series_maps(&a, &b).expect("scalar vs vector must diff");
        assert!(detail.contains("series set differs"), "{detail}");
    }

    /// A malformed scalar pair (wrong arity) is a shape error, not a
    /// silent partial parse.
    #[test]
    fn extract_series_rejects_a_malformed_scalar_pair() {
        let raw = r#"{"status":"success","data":{"resultType":"scalar","result":[1000]}}"#;
        let body: serde_json::Value = serde_json::from_str(raw).unwrap();
        let err = extract_series(&body, &extract_raw_timestamps(raw), Mode::Instant)
            .expect_err("a one-element scalar result must be rejected");
        assert!(err.to_string().contains("[ts, value] pair"), "{err:#}");
    }

    /// Range mode never accepts a bare scalar resultType — both stores
    /// wrap scalar-typed range results into a matrix (asserted live by
    /// the `time()`/`scalar(...)` range rows), so a scalar here is a
    /// contract violation that must fail loudly.
    #[test]
    fn extract_series_still_rejects_scalar_in_range_mode() {
        let raw = r#"{"status":"success","data":{"resultType":"scalar","result":[1000,"42"]}}"#;
        let body: serde_json::Value = serde_json::from_str(raw).unwrap();
        let err = extract_series(&body, &extract_raw_timestamps(raw), Mode::Range)
            .expect_err("scalar in range mode must be rejected");
        assert!(err.to_string().contains("unexpected resultType"), "{err:#}");
    }

    /// A label value containing a literal `[` followed by a digit must
    /// never be mistaken for a timestamp token — the scanner tracks
    /// quoted-string state precisely to guard against this (issue #33
    /// code review follow-up hardening, not itself cited by the review
    /// but load-bearing for the fix's own correctness).
    #[test]
    fn extract_raw_timestamps_ignores_bracket_digit_sequences_inside_quoted_strings() {
        let body = r#"{"metric":{"weird":"foo[123bar"},"value":[1000,"42"]}"#;
        assert_eq!(extract_raw_timestamps(body), vec!["1000"]);
    }

    /// The documented-contract discovery comparison ([`assert_discovery_endpoint`])
    /// hinges on [`discoverable_label_string`] (built from a
    /// [`corpus::DiscoverableSeries`]) and [`series_label_strings`] (built
    /// from a live `/series` response) rendering the *same* label set to
    /// byte-identical strings — this pins that both sides agree.
    #[test]
    fn discoverable_label_string_matches_series_label_strings_format() {
        let body = serde_json::json!({"data": [{
            "__name__": "mem_usage_bytes",
            "instance": "inst-000",
            "run_id": "r",
            "service": "svc-0",
            "slot": "slot-0",
        }]});
        let actual = series_label_strings(&body).unwrap();
        let labels = vec![
            ("__name__".to_string(), "mem_usage_bytes".to_string()),
            ("instance".to_string(), "inst-000".to_string()),
            ("run_id".to_string(), "r".to_string()),
            ("service".to_string(), "svc-0".to_string()),
            ("slot".to_string(), "slot-0".to_string()),
        ];
        let expected = discoverable_label_string(&labels);
        assert!(
            actual.contains(&expected),
            "{actual:?} did not contain {expected:?}"
        );
    }

    #[test]
    fn diff_series_maps_detects_a_missing_series() {
        let mut a: BTreeMap<LabelKey, BTreeMap<i64, f64>> = BTreeMap::new();
        a.insert(
            vec![("service".to_string(), "svc-0".to_string())],
            BTreeMap::from([(0, 1.0)]),
        );
        let b: BTreeMap<LabelKey, BTreeMap<i64, f64>> = BTreeMap::new();
        let detail = diff_series_maps(&a, &b).expect("missing series must be reported");
        assert!(detail.contains("series set differs"));
    }

    #[test]
    fn diff_series_maps_detects_a_value_mismatch() {
        let key = vec![("service".to_string(), "svc-0".to_string())];
        let mut a: BTreeMap<LabelKey, BTreeMap<i64, f64>> = BTreeMap::new();
        a.insert(key.clone(), BTreeMap::from([(0, 1.0)]));
        let mut b: BTreeMap<LabelKey, BTreeMap<i64, f64>> = BTreeMap::new();
        b.insert(key, BTreeMap::from([(0, 2.0)]));
        let detail = diff_series_maps(&a, &b).expect("value mismatch must be reported");
        assert!(detail.contains("at ts 0"));
    }

    #[test]
    fn diff_series_maps_accepts_matching_series_including_nan_class_equality() {
        let key = vec![("service".to_string(), "svc-0".to_string())];
        let mut a: BTreeMap<LabelKey, BTreeMap<i64, f64>> = BTreeMap::new();
        a.insert(key.clone(), BTreeMap::from([(0, f64::NAN)]));
        let mut b: BTreeMap<LabelKey, BTreeMap<i64, f64>> = BTreeMap::new();
        b.insert(key, BTreeMap::from([(0, f64::from_bits(STALE_NAN_BITS))]));
        assert!(diff_series_maps(&a, &b).is_none());
    }

    #[test]
    fn build_stalenan_write_request_sorts_labels_and_carries_the_marker_last() {
        let req = build_stalenan_write_request("r1", 1_000, 15_000, 2);
        assert_eq!(req.timeseries.len(), 2);
        for ts in &req.timeseries {
            let names: Vec<&str> = ts.labels.iter().map(|l| l.name.as_str()).collect();
            let mut sorted = names.clone();
            sorted.sort();
            assert_eq!(names, sorted, "labels must be sorted by name");
            assert_eq!(ts.samples.len(), 3);
            assert_eq!(ts.samples[2].value.to_bits(), STALE_NAN_BITS);
        }
    }

    #[test]
    fn encode_stalenan_write_request_round_trips_through_snappy() {
        // Not a plain `assert_eq!(decoded, req)`: `req` carries a genuine
        // NaN value (the stale marker), and `f64`'s `PartialEq` makes
        // `NaN != NaN` — a derived `WriteRequest: PartialEq` would always
        // report unequal here regardless of a correct round trip.
        // Comparing every field's bits instead is the correct equality
        // for this round-trip assertion.
        let req = build_stalenan_write_request("r1", 1_000, 15_000, 1);
        let compressed = encode_stalenan_write_request(&req).unwrap();
        let decompressed = snap::raw::Decoder::new()
            .decompress_vec(&compressed)
            .unwrap();
        let decoded = WriteRequest::decode(decompressed.as_slice()).unwrap();
        assert_eq!(decoded.timeseries.len(), req.timeseries.len());
        for (d, r) in decoded.timeseries.iter().zip(&req.timeseries) {
            assert_eq!(d.labels, r.labels);
            assert_eq!(d.samples.len(), r.samples.len());
            for (ds, rs) in d.samples.iter().zip(&r.samples) {
                assert_eq!(ds.value.to_bits(), rs.value.to_bits());
                assert_eq!(ds.timestamp, rs.timestamp);
            }
        }
    }

    #[test]
    fn parse_scale_defaults_to_ci_when_none() {
        assert_eq!(parse_scale(None).unwrap(), Scale::Ci);
    }

    #[test]
    fn parse_scale_accepts_ci_and_full_case_insensitively() {
        assert_eq!(parse_scale(Some("ci")).unwrap(), Scale::Ci);
        assert_eq!(parse_scale(Some("CI")).unwrap(), Scale::Ci);
        assert_eq!(parse_scale(Some("full")).unwrap(), Scale::Full);
        assert_eq!(parse_scale(Some("FULL")).unwrap(), Scale::Full);
    }

    #[test]
    fn parse_scale_rejects_an_unknown_value() {
        assert!(parse_scale(Some("bogus")).is_err());
    }

    fn shipped_fixture() -> DifferentialFixture {
        let root = crate::engine::workspace_root();
        let raw = std::fs::read_to_string(root.join("test/fixtures").join(FIXTURE_PATH)).unwrap();
        serde_json::from_str(&raw).unwrap()
    }

    /// Architect plan: "a unit test asserts it parses and the Full tier
    /// sums to ≈10k".
    #[test]
    fn shipped_fixture_parses_and_full_tier_sums_to_approximately_ten_thousand_series() {
        let fixture = shipped_fixture();
        let spec = CorpusSpec {
            seed: fixture.seed,
            scale: Scale::Full,
            step_ms: fixture.step_ms,
            sample_count: fixture.sample_count,
            base_ms: 0,
            run_id: "fixture-check".to_string(),
            families: fixture.full,
            histogram_bounds: fixture.histogram_bounds.clone(),
        };
        let corpus = corpus::generate(&spec);
        assert!(
            (9_000..=11_000).contains(&corpus.expected_series),
            "expected ~10k series from the shipped `full` tier, got {}",
            corpus.expected_series
        );
        assert!(!fixture.query_matrix.is_empty());
    }

    #[test]
    fn shipped_fixture_ci_tier_sums_to_approximately_one_tenth_of_full() {
        let fixture = shipped_fixture();
        let ci_spec = CorpusSpec {
            seed: fixture.seed,
            scale: Scale::Ci,
            step_ms: fixture.step_ms,
            sample_count: fixture.sample_count,
            base_ms: 0,
            run_id: "fixture-check".to_string(),
            families: fixture.ci,
            histogram_bounds: fixture.histogram_bounds.clone(),
        };
        let ci = corpus::generate(&ci_spec);
        assert!(
            (800..=1_200).contains(&ci.expected_series),
            "expected ~1k series from the shipped `ci` tier, got {}",
            ci.expected_series
        );
    }

    #[test]
    fn shipped_fixture_every_query_matrix_entry_has_at_least_one_valid_mode() {
        let fixture = shipped_fixture();
        for entry in &fixture.query_matrix {
            assert!(
                !entry.modes.is_empty(),
                "query {:?} lists no modes",
                entry.expr
            );
            for mode in &entry.modes {
                parse_mode(mode).unwrap_or_else(|_| {
                    panic!("query {:?} has an invalid mode {mode:?}", entry.expr)
                });
            }
        }
    }

    /// Code review test gap, issue #33: pins the matrix's total `(query,
    /// mode)` row count exactly — the number `run_query_matrix` actually
    /// executes — so a future fixture edit that silently drops an entry
    /// or a mode fails this test immediately, rather than merely
    /// shrinking coverage unnoticed. Update this constant deliberately
    /// (with a comment explaining the change) whenever the fixture's
    /// matrix is intentionally resized. Resized 56 -> 78 by issue #65
    /// (M6-02): 11 new entries x 2 modes for the IEEE-exact elementwise
    /// math functions (abs/ceil/floor/sqrt/sgn/deg/rad/clamp/clamp_min/
    /// clamp_max/round) — the transcendental subset is deliberately
    /// excluded from this bit-exact matrix (Go-vs-Rust libm ULP
    /// divergence; those are proven by the executed upstream corpus at
    /// its own 1e-6 epsilon instead, per the #65 adjudication). Resized
    /// 78 -> 108 by issue #66 (M6-03): 15 new entries x 2 modes for
    /// time/timestamp (bare + offset)/scalar/vector and the eight date
    /// functions, plus the selector-free `time()`/`vector(time())`/
    /// `month()` shapes. Resized 108 -> 120 by issue #67 (M6-04): 6 new
    /// entries x 2 modes for the bit-exact-eligible non-experimental
    /// range-vector functions (idelta/resets/changes/last_over_time/
    /// present_over_time/absent_over_time) — the accumulation- and
    /// interpolation-based rest (deriv/predict_linear/quantile/mad/
    /// stddev/stdvar/double_exp) is deliberately excluded from this
    /// bit-exact matrix, and the experimental functions cannot appear at
    /// all (the e2e Prometheus runs without
    /// --enable-feature=promql-experimental-functions and would error
    /// one-sidedly). Resized 120 -> 126 by issue #68 (M6-05): 3 new
    /// entries x 2 modes for label_replace/label_join/absent
    /// (pass-through values, bit-exact-eligible; value+labelset only —
    /// the harness's set comparison cannot see ordering, so sort/
    /// sort_desc rows would prove nothing and the experimental
    /// sort_by_label pair cannot appear at all, both per the #68
    /// adjudication — ordering is proven Tier-1 by the proof corpus's
    /// eval_ordered cases plus the encode-level wire-order test).
    /// Resized 126 -> 132 by issue #69 (M6-06): 3 new entries x 2 modes
    /// for `group by`/`group without` (incl. a computed body — the lifted
    /// bare-selector restriction, live) and `count_values` (the corpus
    /// gauge is integer-valued, so the formatted value labels are
    /// cross-engine formatting-safe) — stddev/stdvar/quantile are
    /// deliberately excluded from this bit-exact matrix
    /// (accumulation/interpolation ULP, the same #67-recorded discipline)
    /// and the experimental limitk/limit_ratio cannot appear at all (the
    /// e2e Prometheus runs without
    /// --enable-feature=promql-experimental-functions and would error
    /// one-sidedly; their selection is additionally hash/order-dependent
    /// — see the coverage manifest rationale). Those are proven Tier-1 by
    /// the proof corpus + unit tests, per the #69 adjudication. Resized
    /// 132 -> 146 by issue #70 (M6-07): 7 new entries x 2 modes for the
    /// set operators (with and without on/ignoring — verbatim
    /// passthrough, bit-exact-eligible) and group_left/group_right with
    /// an include label (IEEE basic-op values over an order-independent
    /// `max by` one side) — `atan2` is deliberately excluded from this
    /// bit-exact matrix (Go-vs-Rust libm ULP, the #65-recorded
    /// discipline; proven by the corpus at its 1e-6 epsilon) and the
    /// experimental fill modifiers cannot appear at all (the e2e
    /// Prometheus runs without
    /// --enable-feature=promql-experimental-functions and would error
    /// one-sidedly; proven by the fully-green upstream fill-modifier
    /// corpus file instead, per the #70 adjudication). Resized 146 -> 156
    /// by issue #86 (M6-08d, incl. review rounds 1-2): 5 new entries x 2
    /// modes proving bit-parity under DELAYED name removal against the
    /// flag-on oracle (both compose files now run the Prometheus
    /// reference with --enable-feature=promql-delayed-name-removal,
    /// matching PulsusDB's sole unconditional model) —
    /// `sum by(__name__)` over a name-dropping single-name body; the
    /// same aggregation fed by TWO distinct metric names with MIXED
    /// verdicts (gauge kept + counter rate dropped: a delayed engine
    /// leaking the retained counter name emits `requests_total` instead
    /// of `{}` and mismatches the oracle); the round-2 GENUINELY
    /// eager-vs-delayed-discriminating row — two distinctly-named
    /// DROP-MARKED sources bridged through `label_replace` (reads the
    /// retained names, re-writes `__name__`, clears the verdicts) into
    /// one `by(__name__)` aggregation, live-verified BOTH ways: flag-on
    /// v3.13.0 yields two NAMED series, flag-off yields one anonymous
    /// `{}` group (the UNBRIDGED both-dropped form necessarily ERRORS
    /// under delayed — post-drop `{}` identities always collide, also
    /// live-verified — and this harness's contract is success-only, so
    /// that form is pinned by the corpus/proof expect-fail cases
    /// instead); the OR name-propagation shape whose group merges only
    /// under the retained-name model; and the plan-v2-Δ1 alternating
    /// filter/arithmetic range expression whose per-series `__name__` is
    /// decided by the first-step drop_name latch (the 200000150
    /// threshold splits the pinned seed's noise band for the
    /// svc-0/inst-000/slot-1 gauge series both ways across the window).
    #[test]
    fn shipped_fixture_query_matrix_has_exactly_one_hundred_fifty_six_query_mode_rows() {
        let fixture = shipped_fixture();
        let rows: usize = fixture.query_matrix.iter().map(|e| e.modes.len()).sum();
        assert_eq!(
            rows, 156,
            "query_matrix now expands to {rows} (query, mode) rows, not the pinned 156 — update \
             this test deliberately if the matrix was intentionally resized"
        );
    }

    /// Issue #66 (M6-03): the only fixture entries allowed to skip
    /// `run_id` scoping — queries with **no selector at all** (they touch
    /// no corpus data, so run isolation is structurally moot). Pinned by
    /// exact query text so the guard below stays strict for every
    /// data-touching entry.
    const SELECTOR_FREE_ENTRIES: &[&str] = &["time()", "vector(time())", "month()"];

    #[test]
    fn shipped_fixture_every_query_matrix_entry_substitutes_run_id_cleanly() {
        let fixture = shipped_fixture();
        for entry in &fixture.query_matrix {
            if SELECTOR_FREE_ENTRIES.contains(&entry.expr.as_str()) {
                assert!(
                    !entry.expr.contains("{R}"),
                    "selector-free entry {:?} must not carry a run_id placeholder",
                    entry.expr
                );
                continue;
            }
            let rendered = entry.expr.replace("{R}", "e2e-metrics-test-run");
            assert!(
                !rendered.contains("{R}"),
                "query {:?} left an unsubstituted placeholder",
                entry.expr
            );
            assert!(
                rendered.contains("run_id=\"e2e-metrics-test-run\""),
                "query {:?} does not scope by run_id",
                entry.expr
            );
        }
    }
}
