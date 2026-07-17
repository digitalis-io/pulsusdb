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

use std::collections::BTreeSet;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::corpus::Scale;
use crate::harness::poll_until;
use crate::logs_corpus::{self, ExpectedResult, LogCorpus, LogCorpusSpec};
use crate::metrics::write_artifact;
use crate::scenarios::Ctx;

const FIXTURE_PATH: &str = "logs/differential.json";
const ARTIFACT_AREA: &str = "logs-diff";

const COLLECTOR_READY_POLL_TIMEOUT: Duration = Duration::from_secs(90);
const COLLECTOR_READY_POLL_INTERVAL: Duration = Duration::from_millis(500);
const COMPLETENESS_POLL_TIMEOUT: Duration = Duration::from_secs(180);
const COMPLETENESS_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Margin between the corpus's last record and "now" at generation time,
/// and the query-window slack on each side (both stores get identical
/// nanosecond bounds).
const CORPUS_NOW_MARGIN_NS: i64 = 5_000_000_000;
const WINDOW_SLACK_NS: i64 = 3_600_000_000_000;

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
    query: String,
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
        if !logs_corpus::CASE_IDS.contains(&case.case_id.as_str()) {
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
) -> Result<serde_json::Value> {
    query_store(
        ctx,
        &ctx.url("/api/logs/v1/query_range"),
        query,
        window,
        limit,
    )
    .await
}

async fn query_loki(
    ctx: &Ctx,
    query: &str,
    window: QueryWindow,
    limit: u32,
) -> Result<serde_json::Value> {
    query_store(
        ctx,
        &format!("{}/loki/api/v1/query_range", ctx.loki_url),
        query,
        window,
        limit,
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
    // `poll_until` retries a closure `Err` — so permanent invalidity
    // (truncation / duplicate delivery, which never self-heal) is
    // yielded as `Ok(Some(Err(...)))` to stop polling immediately, and
    // propagated after the poll.
    let verdict: Result<()> = poll_until(
        COMPLETENESS_POLL_TIMEOUT,
        COMPLETENESS_POLL_INTERVAL,
        || async {
            // Pass 1 — validity gates on BOTH stores' responses, before
            // ANY set comparison (round-2 finding 2: comparing one store
            // first would keep retrying while the OTHER store's response
            // is already permanently invalid).
            let bodies = [
                ("pulsusdb", query_pulsus(ctx, &q, window, limit).await?),
                ("oracle", query_loki(ctx, &q, window, limit).await?),
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
            // every gate.
            if sets.iter().any(|set| *set != expected) {
                return Ok(None); // still filling — keep polling
            }
            Ok(Some(Ok(())))
        },
    )
    .await
    .with_context(|| {
        format!(
            "run {:?} never reached completeness ({} records) on both stores",
            corpus.run_id,
            corpus.total_records()
        )
    })?;
    verdict
}

/// One committed case: validity gates first (raw counts strictly below
/// the limit on both stores; no duplicate entries), then PulsusDB ==
/// corpus (ALWAYS hard) == oracle (hard for `gated`, recorded for
/// `informational`).
async fn run_case(
    ctx: &Ctx,
    corpus: &LogCorpus,
    fixture: &LogsFixture,
    case: &CaseRaw,
    window: QueryWindow,
) -> Result<()> {
    let q = case.query.replace("{R}", &corpus.run_id);
    let expected = corpus.expected_case_result(&case.case_id);
    let gated = case.mode == "gated";
    println!(
        "pulsus-e2e:     case {:?} [{}] — {}: {} expected entry(ies) across {} stream(s)",
        case.case_id,
        case.mode,
        case.construct,
        set_entry_count(&expected),
        expected.len(),
    );

    let pulsus_body = query_pulsus(ctx, &q, window, fixture.limit).await?;
    let loki_body = query_loki(ctx, &q, window, fixture.limit).await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logs_corpus::CASE_IDS;

    /// The committed exclusion list (plan v3 delta 5): every case starts
    /// gated; a case id appears here ONLY after an observed live
    /// divergence was triaged and recorded in
    /// docs/benchmarks/logs-differential-ledger.md. Update deliberately,
    /// with the ledger entry, never as a quick fix for a red run.
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
        assert_eq!(fixture_ids, CASE_IDS.to_vec());
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

    /// Every committed case query PARSES under the shipped grammar and
    /// its pipeline COMPILES under the shipped evaluator — a fixture
    /// typo fails hermetically, not at nightly runtime.
    #[test]
    fn shipped_fixture_queries_parse_and_their_pipelines_compile() {
        let fixture = shipped_fixture();
        for case in &fixture.cases {
            let rendered = case.query.replace("{R}", "e2e-logs-test");
            let expr = pulsus_logql::parse(&rendered)
                .unwrap_or_else(|e| panic!("case {:?} query does not parse: {e}", case.case_id));
            let pulsus_logql::Expr::Log(log) = expr else {
                panic!("case {:?} must be a log (streams) query", case.case_id);
            };
            pulsus_read::logql::pipeline::CompiledPipeline::compile(&log.pipeline).unwrap_or_else(
                |e| panic!("case {:?} pipeline does not compile: {e}", case.case_id),
            );
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
            for case in &fixture.cases {
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

    /// The corpus's expected sets agree with running the SHIPPED
    /// evaluator over the generated bodies — the projection, the fixture
    /// query, and `pulsus-read`'s own pipeline cannot drift apart
    /// (hermetic; the live lane then compares against the oracle).
    #[test]
    fn shipped_fixture_expected_sets_agree_with_the_shipped_evaluator() {
        let fixture = shipped_fixture();
        let corpus = shipped_corpus(&fixture, fixture.ci.record_count);
        for case in &fixture.cases {
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
}
