//! The three hydration-path runners (issue #35 architect plan): `eager`
//! (the current product shape — stage 2 hydrates every selector-matched
//! stream before stage 3's `LIMIT`), and the two bench-local late-hydration
//! prototypes `late_idx`/`late_proj` (differing only in how they derive the
//! cheap pre-`LIMIT` `service` set). Every stage reuses the product's own
//! `pulsus_read::logql::{plan, sql}` builders and decodes into the
//! production row types (`pulsus_read::logql::rows::{StreamMetaRow,
//! SampleRow}`) — the two service-set builders below are the only bench-local
//! SQL in this module.
//!
//! **6-round Latin-square rotation.** One discarded warm-up round (fixed
//! order) plus 6 measured rounds, each running all three variants in one of
//! the 6 distinct permutations of `{eager, late_idx, late_proj}` — every
//! variant occupies each position exactly twice, killing systematic
//! cache/thermal warm-last bias (architect plan v3 [R2]).
//!
//! **Correctness gate is mandatory, not optional** (architect plan F5,
//! v2). [`correctness_gate`] independently derives each variant's own
//! `service` set (from its own SQL, not a shared shortcut), asserts all
//! three agree, then hydrates the shared result-fingerprint set via both
//! the eager (full) and late (≤limit) hydration shapes and asserts the
//! labels are byte-equal — a late-hydration prototype that returns
//! different or incomplete label sets would fabricate a "win".
//!
//! **RSS — parent-side windowed sampler** (architect plan v5 [R1]): a fresh
//! child process per RSS repetition connects to the already-loaded
//! database, signals `READY`, blocks on a go-signal, runs exactly one
//! variant's full path, signals `DONE`, and exits. The **parent** samples
//! `/proc/<child_pid>/status` `VmRSS` at `READY` (the baseline) and polls it
//! every 10 ms until `DONE`, tracking the max as `rss_peak`; attribution is
//! `rss_peak - rss_at_ready`. The child's own `VmHWM(exit) - VmHWM(ready)`
//! is still captured but demoted to a corroborating lower-bound diagnostic
//! only (a monotonic high-water-mark delta censors to 0 whenever the
//! startup peak exceeds the query-time peak).

use std::time::Instant;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChRow, QuerySettings, Row};
use pulsus_logql::parse;
use pulsus_read::logql::escape::ch_string;
use pulsus_read::logql::rows::{SampleRow, StreamMetaRow};
use pulsus_read::logql::sql::{self, TimeWindow};
use pulsus_read::logql::{Direction, Plan, PlanCtx, QueryParams, QuerySpec, StreamsPlan, plan};

use super::report::{Dist, PathEvidence, StageDist, Variant};
use crate::bench::dataset::BroadDatasetSummary;
use crate::bench::queries::month_literals;
use crate::bench::query_log::{flush_logs, tagged_settings};

/// Table names this scenario reads/writes — always bare (single-node only;
/// `--dist` per-shard roster capture is explicitly out of scope for this
/// scenario, architect plan "Out of scope": the fan-out mechanics are
/// already proven for logs in #16).
pub struct Tables {
    pub streams_idx: String,
    pub streams: String,
    pub samples: String,
    pub rollup: String,
}

impl Tables {
    pub fn new() -> Self {
        Tables {
            streams_idx: "log_streams_idx".to_string(),
            streams: "log_streams".to_string(),
            samples: "log_samples".to_string(),
            rollup: "log_metrics_5s".to_string(),
        }
    }

    pub fn plan_ctx<'a>(&'a self, db: &'a str) -> PlanCtx<'a> {
        PlanCtx {
            db,
            streams_idx: &self.streams_idx,
            streams: &self.streams,
            samples: &self.samples,
            rollup_table: &self.rollup,
            rollup_res_ns: 5_000_000_000,
            scan_budget_bytes: 200 * 1024 * 1024 * 1024,
            // Well above every breadth this scenario sweeps (<= 50_000),
            // and below §3.2's 100k cap (architect plan edge case #5): the
            // sweep stays deliberately sub-cap.
            max_streams: 1_000_000,
            pipeline_scan_factor: 10,
        }
    }
}

impl Default for Tables {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct FingerprintRow {
    fingerprint: u64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ServiceRow {
    service: String,
}

/// Derives the stage-3 `service` set from `log_streams_idx` **without**
/// hydrating labels (variant `late_idx`) — `service_name` is a queryable
/// key in `log_streams_idx` (docs/schemas.md §3.1).
fn service_set_from_idx(streams_idx_table: &str, months: &[String], fps: &[u64]) -> String {
    format!(
        "SELECT DISTINCT val AS service\nFROM {streams_idx_table}\nWHERE {} AND key = 'service_name' AND fingerprint IN ({})",
        month_clause(months),
        fp_list(fps)
    )
}

/// Derives the stage-3 `service` set from a narrow `log_streams` projection
/// (variant `late_proj`) — never reads the `labels` column.
fn service_set_from_streams(streams_table: &str, fps: &[u64]) -> String {
    format!(
        "SELECT DISTINCT service\nFROM {streams_table}\nWHERE fingerprint IN ({})",
        fp_list(fps)
    )
}

fn month_clause(months: &[String]) -> String {
    if months.len() == 1 {
        format!("month = {}", months[0])
    } else {
        format!("month IN ({})", months.join(", "))
    }
}

fn fp_list(fps: &[u64]) -> String {
    fps.iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

fn log_comment(variant: Variant, breadth: u32, stage: &str) -> String {
    format!(
        "pulsus-bench:logs-hydration:{}:breadth={breadth}:{stage}",
        variant.name()
    )
}

/// ClickHouse's default `max_query_size` (262,144 bytes — the SQL-text
/// parse-buffer limit, distinct from `max_bytes_to_read`/the scan budget)
/// is too small for this scenario's own literal `fingerprint IN (...)`
/// lists at the top breadth: a resolved 50,000-fingerprint set, inlined as
/// decimal literals (this scenario's stage 2/3 SQL is the product's own
/// `sql::stage2`/`sql::stage3` builders, reused byte-for-byte — architect
/// plan "no product read-path changes"), renders to roughly 1 MiB of SQL
/// text. **Issue #35 resolution:** this was a REAL production limitation,
/// not a bench artifact — the product now fixes it in
/// `pulsus_read::querytext::MAX_QUERY_TEXT_BYTES`, sent as `max_query_size`
/// on every read-path query. This bench consumes ONLY that shared
/// constant (not a full settings builder), so the settings sent here are
/// key-for-key, value-for-value identical to those that produced the
/// committed `docs/benchmarks/data/logs-hydration-{ci,full}.json` evidence
/// (same 8 MiB value, same sole key) — the frozen artifacts stand
/// unchanged. The `entry_set_matches_production_max_query_size_exactly`
/// test below pins that no bench-local drift (an extra setting, or a
/// diverged value) can creep back in unnoticed.
fn settings(query_id: &str, comment: &str) -> QuerySettings {
    tagged_settings(
        QuerySettings::new().set(
            "max_query_size",
            pulsus_read::querytext::MAX_QUERY_TEXT_BYTES,
        ),
        query_id,
        Some(comment),
    )
}

/// Runs `sql` under `settings`, fully draining the result into a `Vec<R>`
/// **within this function's own scope**. Critical for this module: a
/// `ChRowStream` holds its pooled-connection lease for its entire lifetime,
/// and Rust `let` shadowing (`let mut stream = ...` reused several times in
/// one function) does **not** drop the earlier binding until the enclosing
/// scope ends — several sequential `client.query_stream(...)` calls inside
/// one large function (as `correctness_gate`/`run_variant_once` make) would
/// otherwise silently accumulate open leases until the connection pool is
/// exhausted, at which point a later `pool.get().await` blocks forever
/// (observed live during implementation: the gate's 4th sequential stream
/// hung indefinitely on a 4-connection pool with 4 already-undropped
/// leases from earlier queries in the same function). Every multi-query
/// call site in this module therefore routes through this helper, whose
/// own small scope drops the stream — and releases its lease — the instant
/// it is done being read.
pub(super) async fn fetch_rows<R: ChRow>(
    client: &ChClient,
    sql: &str,
    settings: &QuerySettings,
) -> anyhow::Result<Vec<R>> {
    let mut stream = client.query_stream::<R>(sql, settings).await?;
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row?);
    }
    Ok(out)
}

/// Plans the one selector this scenario ever issues:
/// `{service_name="<summary.service>"}`, `limit = summary.result_streams`,
/// backward direction, windowed to `[summary.start_ns, summary.end_ns]`
/// (architect plan: "Reused unchanged from `pulsus_read::logql`: `plan()`").
pub fn build_plan(
    tables: &Tables,
    db: &str,
    summary: &BroadDatasetSummary,
) -> anyhow::Result<StreamsPlan> {
    let query = format!(r#"{{service_name="{}"}}"#, summary.service);
    let expr = parse(&query)?;
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: summary.start_ns,
            end_ns: summary.end_ns,
            step_ns: 60_000_000_000,
        },
        limit: summary.result_streams,
        direction: Direction::Backward,
    };
    match plan(&expr, &params, &tables.plan_ctx(db))? {
        Plan::Streams(sp) => Ok(sp),
        Plan::Metric(_) | Plan::MetricBinary(_) => {
            anyhow::bail!("expected a Streams plan for {query}")
        }
    }
}

/// One executed stage's raw `system.query_log` capture, widened with
/// `cpu_micros` (scenario-local — the shared `query_log::QueryLogTotals`
/// stays byte-frozen, architect plan F6).
#[derive(Debug, Clone, Default)]
pub struct StageTotals {
    pub read_rows: u64,
    pub read_bytes: u64,
    pub selected_marks: u64,
    pub memory_usage: u64,
    pub query_duration_ms: u64,
    pub cpu_micros: u64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
struct RawStageRow {
    read_rows: u64,
    read_bytes: u64,
    selected_marks: u64,
    memory_usage: u64,
    query_duration_ms: u64,
    os_cpu_us: u64,
    user_us: u64,
    system_us: u64,
}

/// Reads one stage's `system.query_log` row, widened with `cpu_micros`
/// (`ProfileEvents['OSCPUVirtualTimeMicroseconds']`, falling back to
/// `UserTimeMicroseconds + SystemTimeMicroseconds` when the former is `0` —
/// architect plan edge case #3 / open question #3). Returns which source
/// fired, so the caller can record it once, scenario-wide. **Must run after
/// a `SYSTEM FLUSH LOGS`** (`system.query_log`'s `QueryFinish` row is only
/// queryable post-flush — same ordering requirement as
/// `queries.rs::sum_query_log`).
async fn read_stage_totals(
    client: &ChClient,
    query_id: &str,
) -> anyhow::Result<(StageTotals, &'static str)> {
    let sql = format!(
        "SELECT read_rows, read_bytes, ProfileEvents['SelectedMarks'] AS selected_marks, \
         memory_usage, query_duration_ms, \
         ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS os_cpu_us, \
         ProfileEvents['UserTimeMicroseconds'] AS user_us, \
         ProfileEvents['SystemTimeMicroseconds'] AS system_us \
         FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish' \
         ORDER BY event_time_microseconds DESC LIMIT 1"
    );
    let row = fetch_rows::<RawStageRow>(client, &sql, &QuerySettings::new())
        .await?
        .into_iter()
        .next()
        .unwrap_or_default();
    let (cpu_micros, source) = if row.os_cpu_us > 0 {
        (row.os_cpu_us, "OSCPUVirtualTimeMicroseconds")
    } else {
        (
            row.user_us + row.system_us,
            "UserTimeMicroseconds+SystemTimeMicroseconds",
        )
    };
    Ok((
        StageTotals {
            read_rows: row.read_rows,
            read_bytes: row.read_bytes,
            selected_marks: row.selected_marks,
            memory_usage: row.memory_usage,
            query_duration_ms: row.query_duration_ms,
            cpu_micros,
        },
        source,
    ))
}

/// One variant run's outcome, before its `system.query_log` evidence has
/// been read back (that happens once per round, after every variant in the
/// round has finished executing — see [`run_round`]'s doc comment).
pub struct VariantRunOutcome {
    pub resolved_fps: u64,
    pub returned_rows: u64,
    pub result_fps: Vec<u64>,
    pub client_wall_ms: f64,
    /// `(stage_name, query_id)` in execution order.
    pub stage_ids: Vec<(&'static str, String)>,
}

/// Runs `variant`'s full path exactly once: every stage a real client
/// request for this selector actually executes, streamed and decoded into
/// the production row types, wall-clock timed end-to-end (the client
/// boundary — architect plan v2 F1). Reused by both the timed round loop
/// and the RSS-probe child (a fresh child issues exactly one call to this
/// function).
pub async fn run_variant_once(
    client: &ChClient,
    tables: &Tables,
    sp: &StreamsPlan,
    months: &[String],
    breadth: u32,
    variant: Variant,
    base_id: &str,
) -> anyhow::Result<VariantRunOutcome> {
    let t0 = Instant::now();
    let mut stage_ids: Vec<(&'static str, String)> = Vec::new();

    // Stage 1 — resolution (identical SQL for every variant; run fresh here
    // to reflect a real client's own round trip, not a shared shortcut).
    let s1_id = format!("{base_id}-resolution");
    let s1_settings = settings(&s1_id, &log_comment(variant, breadth, "resolution"));
    let fps: Vec<u64> = fetch_rows::<FingerprintRow>(client, &sp.stage1_sql, &s1_settings)
        .await?
        .into_iter()
        .map(|r| r.fingerprint)
        .collect();
    stage_ids.push(("resolution", s1_id));
    let resolved_fps = fps.len() as u64;

    let window = TimeWindow {
        start_ns: sp.start_ns,
        end_ns: sp.end_ns,
    };

    let services: Vec<String> = match variant {
        Variant::Eager => {
            let s2_id = format!("{base_id}-hydration_full");
            let sql2 = sql::stage2(&tables.streams, &fps);
            let s2_settings = settings(&s2_id, &log_comment(variant, breadth, "hydration_full"));
            let mut svcs: Vec<String> = fetch_rows::<StreamMetaRow>(client, &sql2, &s2_settings)
                .await?
                .into_iter()
                .map(|r| r.service)
                .collect();
            stage_ids.push(("hydration_full", s2_id));
            svcs.sort_unstable();
            svcs.dedup();
            svcs
        }
        Variant::LateIdx => {
            let svc_id = format!("{base_id}-service_idx");
            let sql_svc = service_set_from_idx(&tables.streams_idx, months, &fps);
            let svc_settings = settings(&svc_id, &log_comment(variant, breadth, "service_idx"));
            let mut svcs: Vec<String> = fetch_rows::<ServiceRow>(client, &sql_svc, &svc_settings)
                .await?
                .into_iter()
                .map(|r| r.service)
                .collect();
            stage_ids.push(("service_idx", svc_id));
            svcs.sort_unstable();
            svcs.dedup();
            svcs
        }
        Variant::LateProj => {
            let svc_id = format!("{base_id}-service_proj");
            let sql_svc = service_set_from_streams(&tables.streams, &fps);
            let svc_settings = settings(&svc_id, &log_comment(variant, breadth, "service_proj"));
            let mut svcs: Vec<String> = fetch_rows::<ServiceRow>(client, &sql_svc, &svc_settings)
                .await?
                .into_iter()
                .map(|r| r.service)
                .collect();
            stage_ids.push(("service_proj", svc_id));
            svcs.sort_unstable();
            svcs.dedup();
            svcs
        }
    };
    let escaped: Vec<String> = services.iter().map(|s| ch_string(s)).collect();

    // Stage 3 — samples, the production shape unmodified (never the gate's
    // total-order variant — that is F5's gate-only tool, see
    // `correctness_gate`).
    let s3_id = format!("{base_id}-samples");
    let sql3 = sql::stage3(
        &tables.samples,
        &escaped,
        &fps,
        window,
        &sp.line_filters,
        sp.direction,
        sp.scan_limit,
    );
    let s3_settings = settings(&s3_id, &log_comment(variant, breadth, "samples"));
    let sample_rows = fetch_rows::<SampleRow>(client, &sql3, &s3_settings).await?;
    let returned_rows = sample_rows.len() as u64;
    let mut result_fps: Vec<u64> = sample_rows.into_iter().map(|r| r.fingerprint).collect();
    stage_ids.push(("samples", s3_id));
    result_fps.sort_unstable();
    result_fps.dedup();

    if matches!(variant, Variant::LateIdx | Variant::LateProj) {
        let s2_id = format!("{base_id}-hydration_late");
        let sql2 = sql::stage2(&tables.streams, &result_fps);
        let s2_settings = settings(&s2_id, &log_comment(variant, breadth, "hydration_late"));
        fetch_rows::<StreamMetaRow>(client, &sql2, &s2_settings).await?;
        stage_ids.push(("hydration_late", s2_id));
    }

    let client_wall_ms = t0.elapsed().as_secs_f64() * 1000.0;
    Ok(VariantRunOutcome {
        resolved_fps,
        returned_rows,
        result_fps,
        client_wall_ms,
        stage_ids,
    })
}

/// One breadth's full result envelope for one path: `(fingerprint,
/// timestamp_ns, body, labels)` tuples, sorted by fingerprint. The corpus
/// guarantees exactly one sample per result-bearing stream (F5), so
/// fingerprint alone is already a unique key — sorting by it gives a
/// canonical, order-independent representation for cross-path and
/// cross-breadth comparison (code review round-2 [medium]: the prior gate
/// reduced results to a bare `BTreeSet<u64>` of fingerprints, never
/// comparing `timestamp_ns`/`body`/`labels`, and never asserted the
/// envelope stayed identical *across breadths* — only unit tests on the
/// corpus generator's construction covered that property, not the live
/// gate).
pub type ResultEnvelope = Vec<(u64, i64, String, String)>;

/// Asserts `actual` is **exactly** `expected` — set identity, not
/// cardinality (code review finding, issue #35 [medium]: a
/// cardinality-only check (`actual.len() == expected.len()`) passes even
/// when a breadth-dependent filler stream has silently replaced an
/// excluded result-bearing stream, e.g. because a timestamp-jitter bug let
/// a filler sample cross into the result band — the set is then *wrong*
/// but the *same size*). Pure (no I/O), so this is unit-testable directly
/// — see `tests::assert_result_set_identity_*` — independent of the live
/// gate that calls it.
fn assert_result_set_identity(
    context: &str,
    actual: &std::collections::BTreeSet<u64>,
    expected: &std::collections::BTreeSet<u64>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        actual == expected,
        "{context}: correctness gate failed — the returned fingerprint set is not identical to \
         the fixed expected result set (cardinality alone would have passed: got {} fingerprints, \
         expected {}) — missing={:?} unexpected={:?}",
        actual.len(),
        expected.len(),
        expected.difference(actual).collect::<Vec<_>>(),
        actual.difference(expected).collect::<Vec<_>>()
    );
    Ok(())
}

/// Asserts two [`ResultEnvelope`]s are **byte-identical** — every
/// `(fingerprint, timestamp_ns, body, labels)` tuple, not just the
/// fingerprint set (code review round-2 [medium]). Pure (no I/O), used for
/// both the cross-path check (three paths, one breadth) and the
/// cross-breadth check (one path's envelope vs. the first breadth's
/// reference) — see `tests::assert_envelope_identity_*` for the
/// full-envelope-divergence regression this guards (a timestamp, body, or
/// label mismatch that a fingerprint-only check would miss entirely).
fn assert_envelope_identity(
    context: &str,
    actual: &ResultEnvelope,
    expected: &ResultEnvelope,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        actual == expected,
        "{context} — correctness gate failed: full envelopes (fingerprint, timestamp_ns, body, \
         labels) are not byte-identical (actual has {} entries, expected {})",
        actual.len(),
        expected.len()
    );
    Ok(())
}

/// The correctness gate (architect plan F5 — mandatory, not optional).
/// Independently derives each variant's `service` set from its own SQL,
/// asserts all three agree; runs **every path's own production `sql::stage3`**
/// (the exact shape `run_variant_once` executes, not just the gate-local
/// canonical query) and asserts each one's returned fingerprint set is
/// **identical** — not merely equal in count — to the fixed expected
/// result set `summary.result_fingerprints` computed by the corpus
/// generator (code review finding, issue #35 [medium]: identity, not
/// cardinality). It then builds each path's **full** result envelope
/// (`fingerprint`, `timestamp_ns`, `body`, hydrated `labels`) from that
/// same production output, asserts all three paths' envelopes are
/// byte-identical at this breadth, and asserts this breadth's envelope is
/// byte-identical to the reference envelope established by the first
/// breadth processed this run (`reference_envelope`) — code review round-2
/// [medium]: the prior gate reduced results to a bare fingerprint set and
/// left full-content/cross-breadth equality to construction/unit tests
/// only, never asserting it live. Returns `(result fingerprint set,
/// expected_payload_bytes)` — `expected_payload_bytes` is the envelope's
/// total `body`+`labels` byte size, the input to the RSS sane-band check
/// (architect plan [R6]).
pub async fn correctness_gate(
    client: &ChClient,
    tables: &Tables,
    summary: &BroadDatasetSummary,
    sp: &StreamsPlan,
    months: &[String],
    fps: &[u64],
    reference_envelope: &mut Option<ResultEnvelope>,
) -> anyhow::Result<(Vec<u64>, u64)> {
    let eager_id = format!("gate-services-eager-{}", summary.breadth);
    let sql_eager = sql::stage2(&tables.streams, fps);
    let eager_rows = fetch_rows::<StreamMetaRow>(
        client,
        &sql_eager,
        &settings(&eager_id, "pulsus-bench:logs-hydration:gate:services"),
    )
    .await?;
    let mut eager_meta: std::collections::BTreeMap<u64, String> = std::collections::BTreeMap::new();
    let mut eager_services = Vec::new();
    for row in eager_rows {
        eager_services.push(row.service.clone());
        eager_meta.insert(row.fingerprint, row.labels);
    }
    eager_services.sort_unstable();
    eager_services.dedup();

    let idx_id = format!("gate-services-idx-{}", summary.breadth);
    let sql_idx = service_set_from_idx(&tables.streams_idx, months, fps);
    let mut idx_services: Vec<String> = fetch_rows::<ServiceRow>(
        client,
        &sql_idx,
        &settings(&idx_id, "pulsus-bench:logs-hydration:gate:services"),
    )
    .await?
    .into_iter()
    .map(|r| r.service)
    .collect();
    idx_services.sort_unstable();
    idx_services.dedup();

    let proj_id = format!("gate-services-proj-{}", summary.breadth);
    let sql_proj = service_set_from_streams(&tables.streams, fps);
    let mut proj_services: Vec<String> = fetch_rows::<ServiceRow>(
        client,
        &sql_proj,
        &settings(&proj_id, "pulsus-bench:logs-hydration:gate:services"),
    )
    .await?
    .into_iter()
    .map(|r| r.service)
    .collect();
    proj_services.sort_unstable();
    proj_services.dedup();

    anyhow::ensure!(
        eager_services == idx_services && idx_services == proj_services,
        "breadth {}: correctness gate failed — the three variants' independently-derived service \
         sets disagree: eager={eager_services:?} late_idx={idx_services:?} late_proj={proj_services:?}",
        summary.breadth
    );

    let window = TimeWindow {
        start_ns: sp.start_ns,
        end_ns: sp.end_ns,
    };
    let expected_result_fps: std::collections::BTreeSet<u64> =
        summary.result_fingerprints.iter().copied().collect();
    anyhow::ensure!(
        expected_result_fps.len() == summary.result_streams as usize,
        "breadth {}: BroadDatasetSummary.result_fingerprints carries {} entries, expected exactly \
         {} — the corpus generator itself produced a malformed fixed result set",
        summary.breadth,
        expected_result_fps.len(),
        summary.result_streams
    );

    // Every path's OWN production `sql::stage3` (the exact shape
    // `run_variant_once` executes) plus its own hydration of exactly the
    // returned fingerprints — builds each path's full result envelope
    // independently of the other paths' queries (code review round-2
    // [medium]: "preserve and assert the complete expected envelope for
    // every path").
    let mut envelopes: Vec<(&'static str, ResultEnvelope)> = Vec::with_capacity(3);
    for (path_name, services) in [
        ("eager", &eager_services),
        ("late_idx", &idx_services),
        ("late_proj", &proj_services),
    ] {
        let escaped: Vec<String> = services.iter().map(|s| ch_string(s)).collect();
        let sql3 = sql::stage3(
            &tables.samples,
            &escaped,
            fps,
            window,
            &sp.line_filters,
            sp.direction,
            sp.scan_limit,
        );
        let id = format!("gate-samples-{path_name}-{}", summary.breadth);
        let rows = fetch_rows::<SampleRow>(
            client,
            &sql3,
            &settings(&id, "pulsus-bench:logs-hydration:gate:samples"),
        )
        .await?;
        let got: std::collections::BTreeSet<u64> = rows.iter().map(|r| r.fingerprint).collect();
        assert_result_set_identity(
            &format!("breadth {} path {path_name}", summary.breadth),
            &got,
            &expected_result_fps,
        )?;

        let path_result_fps: Vec<u64> = got.into_iter().collect();
        let hyd_sql = sql::stage2(&tables.streams, &path_result_fps);
        let hyd_id = format!("gate-envelope-hydrate-{path_name}-{}", summary.breadth);
        let hyd_rows = fetch_rows::<StreamMetaRow>(
            client,
            &hyd_sql,
            &settings(&hyd_id, "pulsus-bench:logs-hydration:gate:envelope"),
        )
        .await?;
        let labels: std::collections::BTreeMap<u64, String> = hyd_rows
            .into_iter()
            .map(|r| (r.fingerprint, r.labels))
            .collect();

        let mut envelope: ResultEnvelope = Vec::with_capacity(rows.len());
        for row in rows {
            let label = labels.get(&row.fingerprint).cloned().ok_or_else(|| {
                anyhow::anyhow!(
                    "breadth {}: path {path_name}: fingerprint {} in the samples envelope has \
                     no hydrated labels",
                    summary.breadth,
                    row.fingerprint
                )
            })?;
            envelope.push((row.fingerprint, row.timestamp_ns, row.body, label));
        }
        envelope.sort_by_key(|(fp, ..)| *fp);
        envelopes.push((path_name, envelope));
    }

    // Cross-path: all three paths' full envelopes are byte-identical at
    // this breadth.
    let (first_name, first_envelope) = envelopes[0].clone();
    for (name, env) in &envelopes[1..] {
        assert_envelope_identity(
            &format!(
                "breadth {}: path {name}'s full result envelope diverges from path \
                 {first_name}'s at the same breadth",
                summary.breadth
            ),
            env,
            &first_envelope,
        )?;
    }

    // Cross-breadth: this breadth's envelope must equal the reference
    // established by the first breadth processed this run (architect plan
    // R6/[C1]: the fixed result-bearing set is constructed to be
    // byte-identical across breadths — asserted live here, not only by
    // the corpus generator's own unit tests).
    match reference_envelope {
        None => *reference_envelope = Some(first_envelope.clone()),
        Some(reference) => {
            assert_envelope_identity(
                &format!(
                    "breadth {}: this breadth's full result envelope diverges from the \
                     reference envelope established by the first breadth processed this run \
                     (the fixed 100-stream result set must be byte-identical across breadths)",
                    summary.breadth
                ),
                &first_envelope,
                reference,
            )?;
        }
    }

    let result_fps: Vec<u64> = first_envelope.iter().map(|(fp, ..)| *fp).collect();
    let body_bytes: usize = first_envelope
        .iter()
        .map(|(_, _, body, _)| body.len())
        .sum();

    // A's full-N eager hydration must byte-equal B's ≤limit late hydration
    // for the same result fingerprints — kept independent of the per-path
    // envelope hydration above (which is scoped to ≤limit fps for every
    // path including eager): this check specifically exercises whether
    // hydrating ALL N fingerprints, as eager's own production path does,
    // returns the same label content for the overlapping fingerprints as
    // a ≤limit-scoped hydration.
    let eager_restricted: std::collections::BTreeMap<u64, String> = eager_meta
        .into_iter()
        .filter(|(fp, _)| result_fps.contains(fp))
        .collect();
    let late_envelope_labels: std::collections::BTreeMap<u64, String> = first_envelope
        .iter()
        .map(|(fp, _, _, labels)| (*fp, labels.clone()))
        .collect();
    anyhow::ensure!(
        eager_restricted == late_envelope_labels,
        "breadth {}: correctness gate failed — B's late-hydrated labels for the {} result \
         fingerprints diverge from A's eager-hydrated labels for the same fingerprints",
        summary.breadth,
        result_fps.len()
    );

    let label_bytes: usize = late_envelope_labels.values().map(|l| l.len()).sum();
    let expected_payload_bytes = (body_bytes + label_bytes) as u64;

    Ok((result_fps, expected_payload_bytes))
}

/// The six distinct permutations of `{eager, late_idx, late_proj}`
/// (architect plan v3 [R2] Latin square): round `k` uses permutation `k`,
/// so each variant occupies each position exactly twice across the 6
/// measured rounds.
const PERMUTATIONS: [[Variant; 3]; 6] = [
    [Variant::Eager, Variant::LateIdx, Variant::LateProj],
    [Variant::Eager, Variant::LateProj, Variant::LateIdx],
    [Variant::LateIdx, Variant::Eager, Variant::LateProj],
    [Variant::LateIdx, Variant::LateProj, Variant::Eager],
    [Variant::LateProj, Variant::Eager, Variant::LateIdx],
    [Variant::LateProj, Variant::LateIdx, Variant::Eager],
];

pub const MEASURED_ROUNDS: usize = 6;

/// Accumulates every measured round's captures for one variant, before
/// they are folded into a [`PathEvidence`] (still missing its RSS fields —
/// filled in by [`run_breadth`] after the RSS-probe passes).
struct VariantAccumulator {
    resolved_fps: u64,
    returned_rows: u64,
    result_fps: u64,
    client_wall_ms: Vec<f64>,
    path_peak_memory: Vec<f64>,
    // stage name -> per-round StageTotals (len == round count so far)
    stages: std::collections::BTreeMap<&'static str, Vec<StageTotals>>,
    stage_order: Vec<&'static str>,
}

impl VariantAccumulator {
    fn new() -> Self {
        VariantAccumulator {
            resolved_fps: 0,
            returned_rows: 0,
            result_fps: 0,
            client_wall_ms: Vec::with_capacity(MEASURED_ROUNDS),
            path_peak_memory: Vec::with_capacity(MEASURED_ROUNDS),
            stages: std::collections::BTreeMap::new(),
            stage_order: Vec::new(),
        }
    }

    fn record(
        &mut self,
        outcome: &VariantRunOutcome,
        stage_totals: &[(&'static str, StageTotals)],
    ) {
        self.resolved_fps = outcome.resolved_fps;
        self.returned_rows = outcome.returned_rows;
        self.result_fps = outcome.result_fps.len() as u64;
        self.client_wall_ms.push(outcome.client_wall_ms);
        let peak = stage_totals
            .iter()
            .map(|(_, t)| t.memory_usage as f64)
            .fold(0.0, f64::max);
        self.path_peak_memory.push(peak);
        for (name, totals) in stage_totals {
            if !self.stages.contains_key(name) {
                self.stage_order.push(name);
            }
            self.stages.entry(name).or_default().push(totals.clone());
        }
    }

    fn into_stage_dists(self) -> Vec<StageDist> {
        self.stage_order
            .iter()
            .map(|&name| {
                let totals = &self.stages[name];
                let ratio = |t: &StageTotals| {
                    let wall_us = (t.query_duration_ms as f64) * 1000.0;
                    if wall_us > 0.0 {
                        t.cpu_micros as f64 / wall_us
                    } else {
                        0.0
                    }
                };
                StageDist {
                    stage: name.to_string(),
                    read_rows: Dist::from_values(
                        totals.iter().map(|t| t.read_rows as f64).collect(),
                    ),
                    read_bytes: Dist::from_values(
                        totals.iter().map(|t| t.read_bytes as f64).collect(),
                    ),
                    selected_marks: Dist::from_values(
                        totals.iter().map(|t| t.selected_marks as f64).collect(),
                    ),
                    memory_usage: Dist::from_values(
                        totals.iter().map(|t| t.memory_usage as f64).collect(),
                    ),
                    query_duration_ms: Dist::from_values(
                        totals.iter().map(|t| t.query_duration_ms as f64).collect(),
                    ),
                    cpu_micros: Dist::from_values(
                        totals.iter().map(|t| t.cpu_micros as f64).collect(),
                    ),
                    cpu_wall_ratio: Dist::from_values(totals.iter().map(ratio).collect()),
                }
            })
            .collect()
    }
}

/// Everything [`run_breadth`] needs, grouped into one parameter (clippy's
/// argument-count lint, same rationale as `queries.rs::RunConfig`/
/// `metrics_labels::paths::PathsConfig`). `exe`/`http_url`/`user`/
/// `password` are only used to spawn RSS-probe children (`database` doubles
/// as both the connection's default database and the `--database` argument
/// those children are spawned with — the two are always the same value).
pub struct BreadthConfig<'a> {
    pub client: &'a ChClient,
    pub tables: &'a Tables,
    pub exe: &'a std::path::Path,
    pub http_url: &'a str,
    pub database: &'a str,
    pub user: &'a str,
    pub password: &'a str,
}

/// Runs the 1 discarded warm-up round + [`MEASURED_ROUNDS`] measured,
/// Latin-square-rotated rounds for `breadth`, then the RSS-probe passes,
/// assembling every variant's [`PathEvidence`]. `reference_envelope` is the
/// cross-breadth correctness-gate state (`None` on the first breadth this
/// run processes; the caller must reuse the same `&mut Option<..>` across
/// every breadth in one `logs-hydration` invocation — see
/// `correctness_gate`'s doc comment). Returns `(resolved_fps, PathEvidence
/// per variant in `Variant::ALL` order, cpu_metric_source)`.
pub async fn run_breadth(
    cfg: &BreadthConfig<'_>,
    summary: &BroadDatasetSummary,
    reference_envelope: &mut Option<ResultEnvelope>,
) -> anyhow::Result<(u64, Vec<PathEvidence>, &'static str)> {
    let client = cfg.client;
    let tables = cfg.tables;
    let sp = build_plan(tables, cfg.database, summary)?;
    let months = month_literals(sp.start_ns, sp.end_ns);

    // Resolve the shared fingerprint set once (identical across every
    // variant/round — stage 1 has no variant-specific predicate) purely to
    // drive the correctness gate and the shape-of-corpus sanity check.
    let gate_settings = settings(
        "gate-resolution",
        "pulsus-bench:logs-hydration:gate:resolution",
    );
    let fps: Vec<u64> = fetch_rows::<FingerprintRow>(client, &sp.stage1_sql, &gate_settings)
        .await?
        .into_iter()
        .map(|r| r.fingerprint)
        .collect();
    anyhow::ensure!(
        fps.len() as u32 == summary.breadth,
        "breadth {}: stage-1 resolved {} fingerprints, expected exactly {} — the corpus/selector \
         do not match",
        summary.breadth,
        fps.len(),
        summary.breadth
    );

    let (_gate_result_fps, expected_payload_bytes) = correctness_gate(
        client,
        tables,
        summary,
        &sp,
        &months,
        &fps,
        reference_envelope,
    )
    .await?;

    let base = format!("bench-hydration-{}-{}", summary.breadth, std::process::id());

    // Warm-up round (discarded) — fixed canonical order.
    for variant in Variant::ALL {
        let id = format!("{base}-warmup-{}", variant.name());
        run_variant_once(client, tables, &sp, &months, summary.breadth, variant, &id).await?;
    }

    let mut accumulators: std::collections::BTreeMap<Variant, VariantAccumulator> = Variant::ALL
        .iter()
        .map(|&v| (v, VariantAccumulator::new()))
        .collect();
    let mut cpu_source: &'static str = "OSCPUVirtualTimeMicroseconds";

    for (round_idx, perm) in PERMUTATIONS.iter().enumerate() {
        let mut round_outcomes: Vec<(Variant, VariantRunOutcome)> = Vec::with_capacity(3);
        for &variant in perm {
            let id = format!("{base}-r{round_idx}-{}", variant.name());
            let outcome =
                run_variant_once(client, tables, &sp, &months, summary.breadth, variant, &id)
                    .await?;
            round_outcomes.push((variant, outcome));
        }

        // One flush per round, after every variant in it has finished
        // executing (query_log.rs's ordering requirement).
        flush_logs(client).await?;

        for (variant, outcome) in &round_outcomes {
            let mut stage_totals = Vec::with_capacity(outcome.stage_ids.len());
            for (name, query_id) in &outcome.stage_ids {
                let (totals, source) = read_stage_totals(client, query_id).await?;
                cpu_source = source;
                stage_totals.push((*name, totals));
            }
            accumulators
                .get_mut(variant)
                .expect("every Variant::ALL entry was pre-inserted")
                .record(outcome, &stage_totals);
        }
    }

    // RSS-probe passes: 3 fresh children per variant. `rss_suspect` is
    // decided here, against `expected_payload_bytes` (architect plan [R6]
    // sane-band: `rss_delta_kib*1024` outside `[0.25x,4x]` of the decoded
    // envelope's payload size flags the sample as `suspect_measurement`).
    let mut rss_by_variant: std::collections::BTreeMap<Variant, (Dist, Dist)> =
        std::collections::BTreeMap::new();
    for variant in Variant::ALL {
        let (delta, child_hwm_delta) = super::rss_probe::run_rss_probe_parent(
            cfg.exe,
            cfg.http_url,
            cfg.database,
            cfg.user,
            cfg.password,
            variant,
            summary.breadth,
        )?;
        rss_by_variant.insert(variant, (delta, child_hwm_delta));
    }

    let mut evidence = Vec::with_capacity(3);
    for variant in Variant::ALL {
        let acc = accumulators
            .remove(&variant)
            .expect("every Variant::ALL entry was pre-inserted");
        let (resolved_fps, returned_rows, result_fps, client_wall_ms, path_peak_memory) = (
            acc.resolved_fps,
            acc.returned_rows,
            acc.result_fps,
            acc.client_wall_ms.clone(),
            acc.path_peak_memory.clone(),
        );
        let stages = acc.into_stage_dists();
        let hydration_stage_name = match variant {
            Variant::Eager => "hydration_full",
            Variant::LateIdx | Variant::LateProj => "hydration_late",
        };
        let hydration = stages
            .iter()
            .find(|s| s.stage == hydration_stage_name)
            .unwrap_or_else(|| {
                panic!(
                    "{} stage missing from {:?}'s captured stages",
                    hydration_stage_name, variant
                )
            });
        let hydration_read_bytes_median = hydration.read_bytes.median as u64;
        let hydration_cpu_micros_median = hydration.cpu_micros.median as u64;
        let total_read_bytes: u64 = stages.iter().map(|s| s.read_bytes.median as u64).sum();
        let total_read_rows: u64 = stages.iter().map(|s| s.read_rows.median as u64).sum();
        let (rss_delta, rss_child_hwm_delta) = rss_by_variant[&variant].clone();
        let rss_delta_bytes = rss_delta.median * 1024.0;
        let payload = expected_payload_bytes as f64;
        let rss_suspect =
            payload > 0.0 && !(0.25 * payload..=4.0 * payload).contains(&rss_delta_bytes);

        evidence.push(PathEvidence {
            path: variant.name().to_string(),
            breadth: summary.breadth,
            service: summary.service.clone(),
            resolved_fps,
            returned_rows,
            result_fps,
            stages,
            server_peak_memory_usage: Dist::from_values(path_peak_memory),
            total_read_bytes,
            total_read_rows,
            hydration_read_bytes_median,
            hydration_cpu_micros_median,
            client_wall_ms: Dist::from_values(client_wall_ms),
            client_rss_delta_kib: rss_delta,
            client_rss_child_hwm_delta_kib: rss_child_hwm_delta,
            rss_suspect,
        });
    }

    Ok((fps.len() as u64, evidence, cpu_source))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(vals: &[u64]) -> std::collections::BTreeSet<u64> {
        vals.iter().copied().collect()
    }

    /// Issue #35 drift guard: this bench's own settings must carry
    /// EXACTLY the production `max_query_size` key/value plus the
    /// harness's `query_id`/`log_comment` run-tagging — nothing more,
    /// nothing less. A future bench-local override or a dropped
    /// production setting fails this test, keeping the frozen
    /// `docs/benchmarks/data/logs-hydration-{ci,full}.json` evidence's
    /// byte-equivalence claim honest.
    #[test]
    fn entry_set_matches_production_max_query_size_exactly() {
        let got: std::collections::BTreeSet<(String, String)> = settings("id", "c")
            .entries()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let expected: std::collections::BTreeSet<(String, String)> = [
            (
                "max_query_size".to_string(),
                pulsus_read::querytext::MAX_QUERY_TEXT_BYTES.to_string(),
            ),
            ("query_id".to_string(), "id".to_string()),
            ("log_comment".to_string(), "c".to_string()),
        ]
        .into_iter()
        .collect();
        assert_eq!(got, expected);
    }

    #[test]
    fn assert_result_set_identity_passes_when_sets_match_exactly() {
        assert!(assert_result_set_identity("ctx", &set(&[1, 2, 3]), &set(&[1, 2, 3])).is_ok());
    }

    /// The exact regression the code review caught: a cardinality-only
    /// check (`actual.len() == expected.len()`) would have passed here —
    /// both sets have 3 members — but a filler stream (`99`) has silently
    /// replaced an excluded result-bearing stream (`3`). Identity must
    /// catch this; cardinality alone cannot.
    #[test]
    fn assert_result_set_identity_fails_when_a_filler_silently_replaces_a_result_stream() {
        let actual = set(&[1, 2, 99]);
        let expected = set(&[1, 2, 3]);
        assert_eq!(
            actual.len(),
            expected.len(),
            "cardinality alone would pass this case"
        );
        let err = assert_result_set_identity("ctx", &actual, &expected).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("not identical"));
        assert!(msg.contains("missing"));
        assert!(msg.contains("unexpected"));
    }

    #[test]
    fn assert_result_set_identity_fails_on_a_pure_cardinality_mismatch() {
        assert!(assert_result_set_identity("ctx", &set(&[1, 2]), &set(&[1, 2, 3])).is_err());
    }

    #[test]
    fn assert_result_set_identity_fails_when_actual_is_a_strict_superset() {
        // A leaked filler stream alongside every expected result stream —
        // still wrong, even though every expected fingerprint is present.
        assert!(assert_result_set_identity("ctx", &set(&[1, 2, 3, 4]), &set(&[1, 2, 3])).is_err());
    }

    fn envelope(rows: &[(u64, i64, &str, &str)]) -> ResultEnvelope {
        rows.iter()
            .map(|(fp, ts, body, labels)| (*fp, *ts, body.to_string(), labels.to_string()))
            .collect()
    }

    #[test]
    fn assert_envelope_identity_passes_when_envelopes_match_exactly() {
        let e = envelope(&[(1, 100, "body-a", "{}"), (2, 200, "body-b", "{}")]);
        assert!(assert_envelope_identity("ctx", &e, &e).is_ok());
    }

    /// The exact regression this gate now catches, that a fingerprint-set-only
    /// check (round-1 fix) could not: identical fingerprints, but a
    /// diverging `timestamp_ns`.
    #[test]
    fn assert_envelope_identity_fails_on_a_timestamp_divergence_with_identical_fingerprints() {
        let actual = envelope(&[(1, 999, "body-a", "{}")]);
        let expected = envelope(&[(1, 100, "body-a", "{}")]);
        let fps_actual: std::collections::BTreeSet<u64> =
            actual.iter().map(|(fp, ..)| *fp).collect();
        let fps_expected: std::collections::BTreeSet<u64> =
            expected.iter().map(|(fp, ..)| *fp).collect();
        assert_eq!(
            fps_actual, fps_expected,
            "fingerprint sets match — only the envelope catches this"
        );
        assert!(assert_envelope_identity("ctx", &actual, &expected).is_err());
    }

    /// Same fingerprints and timestamps, diverging `body`.
    #[test]
    fn assert_envelope_identity_fails_on_a_body_divergence() {
        let actual = envelope(&[(1, 100, "body-actual", "{}")]);
        let expected = envelope(&[(1, 100, "body-expected", "{}")]);
        assert!(assert_envelope_identity("ctx", &actual, &expected).is_err());
    }

    /// Same fingerprints/timestamps/bodies, diverging hydrated `labels` —
    /// the case a late-hydration prototype returning wrong labels for the
    /// right fingerprint would produce.
    #[test]
    fn assert_envelope_identity_fails_on_a_labels_divergence() {
        let actual = envelope(&[(1, 100, "body-a", "{\"env\":\"prod\"}")]);
        let expected = envelope(&[(1, 100, "body-a", "{\"env\":\"staging\"}")]);
        assert!(assert_envelope_identity("ctx", &actual, &expected).is_err());
    }

    /// The cross-breadth scenario this fix specifically targets: two
    /// breadth passes' envelopes for the fixed 100-stream result set must
    /// be byte-identical; a divergence (simulating an unclamped
    /// timestamp-jitter regression re-appearing) must be caught.
    #[test]
    fn assert_envelope_identity_fails_across_simulated_breadths() {
        let breadth_1k = envelope(&[(1, 100, "body-a", "{}"), (2, 200, "body-b", "{}")]);
        let breadth_50k_diverged = envelope(&[(1, 100, "body-a", "{}"), (2, 201, "body-b", "{}")]);
        assert!(assert_envelope_identity("ctx", &breadth_50k_diverged, &breadth_1k).is_err());
    }
}
