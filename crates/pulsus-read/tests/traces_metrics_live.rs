//! Issue #59 AC4 (Tier-1, scale-invariant): the internal-consistency
//! identities for the TraceQL metrics endpoints against ClickHouse 24.8,
//! on a seeded deterministic corpus:
//!
//! - **(a)** `Σ_buckets rate·step_s == Σ_buckets count_over_time ==`
//!   an independent deduped `COUNT` of the matching spans — for every
//!   gated filter shape (service PREWHERE, attr semi-join, negation,
//!   match-all).
//! - **(b)** instant `/query` == the single bucket of a range with
//!   `step = window` — on aligned windows, where snap = identity (the
//!   plan's "AC4 by construction").
//! - **Replay dedup** (plan v2 delta 1): duplicate-inserting the whole
//!   corpus changes NOTHING — range and instant results are identical
//!   before and after (`uniqExact(trace_id, span_id)`).
//! - **Window edges** (plan v2 test-gap closure): outward snapping on
//!   unaligned windows; a span exactly at an aligned `end` is excluded
//!   (left-closed/right-open); unscoped dual-scope negation counts
//!   absent-key spans.
//!
//! Live-gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test traces_metrics_live
//! podman rm -f pulsus-ch-test
//! ```

use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_read::logql::error::TooBroadReason;
use pulsus_read::traces::metrics_plan::{MetricsParams, plan_trace_metrics};
use pulsus_read::{ReadError, TraceEngine, TraceMetricsPlan, TraceMetricsResult, TraceReadConfig};
use pulsus_schema::{RenderCtx, SchemaParams, run_init};

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
}

fn test_config() -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: "default".to_string(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(60),
        ..ChConnConfig::default()
    }
}

fn test_ctx(db: &str) -> SchemaParams {
    RenderCtx {
        db: db.to_string(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
    }
}

const DB: &str = "pulsus_traces_metrics_it";

/// Corpus base: "two hours ago", floored to a multiple of 600 (so the
/// primary test windows are step-aligned by construction for both step
/// 60 and step = window 600). Now-derived — a fixed historical base
/// would fall past the schema's retention TTL. Captured once per run.
fn base_s() -> i64 {
    use std::sync::OnceLock;
    static BASE: OnceLock<i64> = OnceLock::new();
    *BASE.get_or_init(|| {
        let now_s = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_secs(),
        )
        .expect("fits i64");
        (now_s - 7_200).div_euclid(600) * 600
    })
}
/// One span per second for 10 minutes.
const CORPUS_SPANS: i64 = 600;

const NS: i64 = 1_000_000_000;

/// Extreme-epoch bucket labels (issue #59 re-audit): pre-1970
/// (1969-12-31T23:00:00Z) and post-2106 (> the `UInt32` epoch-seconds max
/// `4_294_967_296`, still inside the `DateTime64(9)` domain). Both aligned
/// to a 60s boundary, far outside `[base_s(), base_s() + CORPUS_SPANS)` so
/// no existing identity/replay/edge assertion is affected.
const EXTREME_PAST_S: i64 = -3_600;
const EXTREME_FUTURE_S: i64 = 4_300_000_020;
/// Trace/span IDs for the extreme-epoch fixture rows, far outside the
/// primary corpus's `numbers(600)` range — no collision.
const EXTREME_PAST_ID: i64 = 900_000;
const EXTREME_FUTURE_ID: i64 = 900_001;

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .unwrap_or_else(|e| panic!("execute failed: {e}\nSQL:\n{sql}"));
}

/// Seeds `CORPUS_SPANS` single-span traces, one per second from
/// `base_s()`: `checkout` every 5th span, `status_message = 'deadline
/// exceeded'` every 6th (empty otherwise — the issue #189 compare()
/// `statusMessage` fixture), `http.status_code = 500` every
/// 4th (span scope), `env = prod` at RESOURCE scope every 3rd and at
/// SPAN scope every 7th (the dual-scope negation fixture — spans with no
/// `env` row in either scope are the absent-key population). Running it
/// twice is the at-least-once replay fixture: every row is a duplicate.
async fn seed_corpus(client: &ChClient, db: &str) {
    let base_ns = base_s() * NS;
    exec(
        client,
        &format!(
            "INSERT INTO {db}.trace_spans \
             (trace_id, span_id, parent_id, name, service, status_message, timestamp_ns, \
              duration_ns, status_code, kind, payload_type, payload) \
             SELECT \
               toFixedString(unhex(leftPad(lower(hex(number)), 32, '0')), 16), \
               toFixedString(unhex(leftPad(lower(hex(number)), 16, '0')), 8), \
               toFixedString(unhex('0000000000000000'), 8), \
               'op', \
               if(number % 5 = 0, 'checkout', 'svc-x'), \
               if(number % 6 = 0, 'deadline exceeded', ''), \
               {base_ns} + toInt64(number) * {NS}, \
               1000000, \
               0, 1, 1, 'p' \
             FROM numbers({CORPUS_SPANS})"
        ),
    )
    .await;
    exec(
        client,
        &format!(
            "INSERT INTO {db}.trace_attrs_idx \
             (date, key, val, scope, val_num, timestamp_ns, trace_id, span_id, duration_ns) \
             SELECT \
               toDate(fromUnixTimestamp64Nano({base_ns} + toInt64(number) * {NS})), \
               'http.status_code', \
               if(number % 4 = 0, '500', '200'), 'span', \
               if(number % 4 = 0, 500.0, 200.0), \
               {base_ns} + toInt64(number) * {NS}, \
               toFixedString(unhex(leftPad(lower(hex(number)), 32, '0')), 16), \
               toFixedString(unhex(leftPad(lower(hex(number)), 16, '0')), 8), \
               1000000 \
             FROM numbers({CORPUS_SPANS})"
        ),
    )
    .await;
    for (scope, modulus) in [("resource", 3i64), ("span", 7i64)] {
        exec(
            client,
            &format!(
                "INSERT INTO {db}.trace_attrs_idx \
                 (date, key, val, scope, val_num, timestamp_ns, trace_id, span_id, duration_ns) \
                 SELECT \
                   toDate(fromUnixTimestamp64Nano({base_ns} + toInt64(number) * {NS})), \
                   'env', 'prod', '{scope}', NULL, \
                   {base_ns} + toInt64(number) * {NS}, \
                   toFixedString(unhex(leftPad(lower(hex(number)), 32, '0')), 16), \
                   toFixedString(unhex(leftPad(lower(hex(number)), 16, '0')), 8), \
                   1000000 \
                 FROM numbers({CORPUS_SPANS}) WHERE number % {modulus} = 0"
            ),
        )
        .await;
    }
}

/// Creates a plain `VIEW` (never `INSERT`ed into `trace_spans`) holding
/// exactly the two extreme-epoch match-all rows (issue #59 re-audit).
///
/// Deliberately **not** a physical insert into `trace_spans`: that table's
/// `PARTITION BY toDate(...)` / `TTL toDateTime(...) + INTERVAL
/// retention_days DAY` (docs/schemas.md §4.1, `pulsus-schema` migrations
/// 16/17) both convert through ClickHouse's 32-bit `Date`/`DateTime`,
/// which silently wrap for timestamps outside their domain — confirmed
/// live: a pre-1970 row's partition key wraps to `Date`'s own max
/// (`2149-06-06`), and a post-2106 row's TTL threshold wraps to a
/// near-1970 date, so a background TTL merge deletes it almost
/// immediately regardless of `retention_days`. That is a genuine,
/// separate defect in the trace schema's DDL (out of #59's scope — the
/// schema is unchanged here; the finding is reported on the issue) that
/// would make a physically-inserted extreme-epoch fixture flaky-to-absent
/// in CI. A `VIEW` has no partitioning or TTL — it is a live ClickHouse
/// evaluation of the exact generated SQL (`toStartOfInterval`,
/// `toUnixTimestamp64Milli`, real `DateTime64` arithmetic) with none of
/// that storage-layer risk, so it still proves the fix round-trips
/// end-to-end against a real server.
async fn create_extreme_epoch_view(client: &ChClient, db: &str) {
    exec(
        client,
        &format!(
            "CREATE VIEW {db}.trace_spans_extreme AS \
             SELECT \
               toFixedString(unhex(leftPad(lower(hex(id)), 32, '0')), 16) AS trace_id, \
               toFixedString(unhex(leftPad(lower(hex(id)), 16, '0')), 8) AS span_id, \
               toFixedString(unhex('0000000000000000'), 8) AS parent_id, \
               'op' AS name, 'svc-x' AS service, \
               ts_ns AS timestamp_ns, \
               1000000 AS duration_ns, 0 AS status_code, 1 AS kind, 1 AS payload_type, \
               'p' AS payload \
             FROM (\
               SELECT {EXTREME_PAST_ID} AS id, toInt64({EXTREME_PAST_S}) * {NS} AS ts_ns \
               UNION ALL \
               SELECT {EXTREME_FUTURE_ID} AS id, toInt64({EXTREME_FUTURE_S}) * {NS} AS ts_ns\
             )"
        ),
    )
    .await;
}

/// A `TraceEngine` reading the extreme-epoch view in place of the real
/// `trace_spans` table (see [`create_extreme_epoch_view`]).
fn extreme_epoch_engine(client: ChClient) -> TraceEngine {
    let mut cfg = engine_config();
    cfg.spans_table = "trace_spans_extreme".to_string();
    TraceEngine::new(client, cfg)
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

async fn data_client() -> ChClient {
    let mut cfg = test_config();
    cfg.database = DB.to_string();
    ChClient::new(cfg).await.expect("connect data client")
}

fn plan_for(
    engine: &TraceEngine,
    q: &str,
    start_s: i64,
    end_s: i64,
    step_s: i64,
) -> TraceMetricsPlan {
    let query = pulsus_traceql::parse(q).expect("query parses");
    plan_trace_metrics(
        &query,
        &MetricsParams {
            start_ns: start_s * NS,
            end_ns: end_s * NS,
            step_s,
        },
        &engine.metrics_ctx(),
    )
    .expect("query plans")
}

/// The samples of an ungrouped result (0 or 1 series).
fn matrix_points(result: &TraceMetricsResult) -> Vec<(i64, f64)> {
    assert!(
        result.series.len() <= 1,
        "single-series ungrouped output: {:?}",
        result.series
    );
    result
        .series
        .first()
        .map(|s| s.samples.clone())
        .unwrap_or_default()
}

/// The one instant sample value of an ungrouped result.
fn vector_value(result: &TraceMetricsResult) -> f64 {
    assert_eq!(result.series.len(), 1, "one instant series: {result:?}");
    assert_eq!(
        result.series[0].samples.len(),
        1,
        "one instant sample: {result:?}"
    );
    result.series[0].samples[0].1
}

/// Asserts the full AC4 identity set for one filter over the aligned
/// primary window `[base_s(), base_s() + CORPUS_SPANS)`, step 60, against an
/// independently-computed expected span count.
async fn assert_identities(engine: &TraceEngine, filter: &str, expected: i64) {
    let end_s = base_s() + CORPUS_SPANS;
    let step_s = 60;
    let window_s = CORPUS_SPANS;

    let rate_plan = plan_for(
        engine,
        &format!("{filter} | rate()"),
        base_s(),
        end_s,
        step_s,
    );
    let count_plan = plan_for(
        engine,
        &format!("{filter} | count_over_time()"),
        base_s(),
        end_s,
        step_s,
    );
    // Snap is the identity on this aligned window.
    assert_eq!(rate_plan.snapped_window_ns(), (base_s() * NS, end_s * NS));

    let rate_points = matrix_points(&engine.metrics_range(&rate_plan).await.expect("rate range"));
    let count_points = matrix_points(
        &engine
            .metrics_range(&count_plan)
            .await
            .expect("count range"),
    );

    // (a) Σ rate·step == Σ count_over_time == the independent count.
    let rate_total: f64 = rate_points.iter().map(|(_, v)| v * step_s as f64).sum();
    let count_total: f64 = count_points.iter().map(|(_, v)| v).sum();
    assert_eq!(
        rate_total.round() as i64,
        expected,
        "{filter}: Σ rate·step must equal the independent count ({rate_points:?})"
    );
    assert_eq!(
        count_total as i64, expected,
        "{filter}: Σ count_over_time must equal the independent count ({count_points:?})"
    );
    // Bucket timestamps are epoch-aligned milliseconds within the window.
    for (t_ms, _) in &rate_points {
        assert_eq!(
            t_ms % (step_s * 1_000),
            0,
            "{filter}: unaligned bucket {t_ms}"
        );
        assert!(*t_ms >= base_s() * 1_000 && *t_ms < end_s * 1_000);
    }

    // (b) instant == the single bucket of range-with-step = window.
    let instant_rate = vector_value(
        &engine
            .metrics_instant(&rate_plan)
            .await
            .expect("instant rate"),
    );
    let instant_count = vector_value(
        &engine
            .metrics_instant(&count_plan)
            .await
            .expect("instant count"),
    );
    assert_eq!(instant_count as i64, expected, "{filter}: instant count");
    assert!(
        (instant_rate - expected as f64 / window_s as f64).abs() < 1e-12,
        "{filter}: instant rate must be count/window ({instant_rate})"
    );
    let whole_rate_plan = plan_for(
        engine,
        &format!("{filter} | rate()"),
        base_s(),
        end_s,
        window_s,
    );
    let whole_points = matrix_points(&engine.metrics_range(&whole_rate_plan).await.expect("whole"));
    if expected == 0 {
        assert!(whole_points.is_empty());
    } else {
        assert_eq!(whole_points.len(), 1, "step = window is one bucket");
        assert_eq!(whole_points[0].0, base_s() * 1_000);
        assert!(
            (whole_points[0].1 - instant_rate).abs() < 1e-12,
            "{filter}: instant ({instant_rate}) == the single whole-window bucket ({})",
            whole_points[0].1
        );
    }
}

/// P3 (issue #182): the `*_over_time(duration)` value-aggregation
/// identities over the aligned primary window. Every corpus span has
/// `duration_ns = 1_000_000` (0.001 s), so `min == max == avg == 0.001`
/// and `sum == count · 0.001`. Proves the replay-dedup inner query and
/// the ns→seconds encode-boundary scaling.
async fn assert_aggregation_identities(engine: &TraceEngine) {
    let end_s = base_s() + CORPUS_SPANS;
    let one_ms_s = 0.001_f64;

    // sum_over_time(duration): Σ buckets == CORPUS_SPANS · 0.001.
    let sum_plan = plan_for(engine, "{} | sum_over_time(duration)", base_s(), end_s, 60);
    let sum_points = matrix_points(&engine.metrics_range(&sum_plan).await.expect("sum range"));
    let sum_total: f64 = sum_points.iter().map(|(_, v)| v).sum();
    assert!(
        (sum_total - CORPUS_SPANS as f64 * one_ms_s).abs() < 1e-9,
        "sum_over_time total {sum_total} != {}",
        CORPUS_SPANS as f64 * one_ms_s
    );

    // Instant min/max/avg over the whole window == 0.001 (all equal).
    for (func, label) in [
        ("min_over_time", "min"),
        ("max_over_time", "avg"),
        ("avg_over_time", "avg"),
    ] {
        let _ = label;
        let plan = plan_for(
            engine,
            &format!("{{}} | {func}(duration)"),
            base_s(),
            end_s,
            60,
        );
        let v = vector_value(&engine.metrics_instant(&plan).await.expect("agg instant"));
        assert!(
            (v - one_ms_s).abs() < 1e-9,
            "{func} instant {v} != {one_ms_s}"
        );
    }

    // Replay-dedup: sum is invariant under duplicate inserts (the inner
    // any(duration_ns) per (t, trace_id, span_id) collapses replays).
    let before = engine.metrics_range(&sum_plan).await.expect("sum before");
    // (the corpus was already duplicated earlier in the test run)
    let after = engine.metrics_range(&sum_plan).await.expect("sum after");
    assert_eq!(before, after, "sum_over_time is replay-invariant");
}

/// P3 (issue #182): `by(resource.service.name)` grouping. The corpus has
/// two services — `checkout` (every 5th span → 120) and `svc-x` (480).
/// Grouped `rate()` returns one series per service; the partition counts
/// sum to the ungrouped total, and the series carry the
/// `resource.service.name` label.
async fn assert_by_service_grouping(engine: &TraceEngine) {
    let end_s = base_s() + CORPUS_SPANS;
    let plan = plan_for(
        engine,
        "{} | count_over_time() by(resource.service.name)",
        base_s(),
        end_s,
        CORPUS_SPANS, // one whole-window bucket
    );
    let result = engine
        .metrics_range(&plan)
        .await
        .expect("grouped range executes");
    assert_eq!(result.series.len(), 2, "two services: {result:?}");

    let mut totals: std::collections::BTreeMap<String, f64> = std::collections::BTreeMap::new();
    for series in &result.series {
        let label = series
            .labels
            .iter()
            .find(|l| l.key == "resource.service.name")
            .unwrap_or_else(|| panic!("series must carry the service label: {series:?}"));
        let value = match &label.value {
            pulsus_read::MetricLabelValue::Str(s) => s.clone(),
            other => panic!("service label must be a string, got {other:?}"),
        };
        totals.insert(value, series.samples.iter().map(|(_, v)| v).sum());
    }
    assert_eq!(totals.get("checkout").copied(), Some(120.0));
    assert_eq!(totals.get("svc-x").copied(), Some(480.0));
    let grand: f64 = totals.values().sum();
    assert_eq!(
        grand, CORPUS_SPANS as f64,
        "Σ by-partition == ungrouped total"
    );
}

/// P3 (issue #182): the `by()` distinct-series cap. With `max_series = 1`
/// a two-service grouped query trips the distinct-by-key probe → `422
/// query_too_broad` (`TraceMetricsSeriesCap`), a static reject before the
/// main query.
async fn assert_series_cap_rejects() {
    let mut cfg = engine_config();
    cfg.max_series = 1;
    let capped = TraceEngine::new(data_client().await, cfg);
    let end_s = base_s() + CORPUS_SPANS;
    let plan = plan_for(
        &capped,
        "{} | count_over_time() by(resource.service.name)",
        base_s(),
        end_s,
        60,
    );
    let err = capped
        .metrics_range(&plan)
        .await
        .expect_err("2 distinct services > cap 1 must reject");
    match err {
        ReadError::QueryTooBroad(TooBroadReason::TraceMetricsSeriesCap { count, cap }) => {
            assert!(count > cap, "count {count} must exceed cap {cap}");
            assert_eq!(cap, 1);
        }
        other => panic!("expected TraceMetricsSeriesCap, got {other:?}"),
    }
    // Under the cap the same query succeeds (control).
    let ok = TraceEngine::new(data_client().await, engine_config());
    assert_eq!(
        ok.metrics_range(&plan)
            .await
            .expect("under cap")
            .series
            .len(),
        2
    );
}

/// P4 (issue #182): `quantile_over_time` (TDigest) and
/// `histogram_over_time` (exp-`le`). Every corpus span has
/// `duration_ns = 1_000_000` (0.001 s), so every quantile is 0.001 s, and
/// the cumulative histogram is 0 below the `1_000_000`-ns value and
/// `CORPUS_SPANS` at and above it.
async fn assert_quantile_and_histogram(engine: &TraceEngine) {
    let end_s = base_s() + CORPUS_SPANS;

    // quantile_over_time instant: one series per quantile (`p` label),
    // each == 0.001 s (all durations equal).
    let q_plan = plan_for(
        engine,
        "{} | quantile_over_time(duration, 0.5, 0.9)",
        base_s(),
        end_s,
        CORPUS_SPANS,
    );
    let q_res = engine
        .metrics_instant(&q_plan)
        .await
        .expect("quantile instant");
    assert_eq!(q_res.series.len(), 2, "one series per quantile: {q_res:?}");
    for (series, want_p) in q_res.series.iter().zip([0.5_f64, 0.9]) {
        let p = series
            .labels
            .iter()
            .find(|l| l.key == "p")
            .unwrap_or_else(|| panic!("quantile series carries a `p` label: {series:?}"));
        assert_eq!(p.value, pulsus_read::MetricLabelValue::Double(want_p));
        assert!(
            (series.samples[0].1 - 0.001).abs() < 1e-9,
            "quantile p={want_p} == 0.001s, got {}",
            series.samples[0].1
        );
    }

    // histogram_over_time instant: one cumulative series per `le` bucket
    // (`__bucket` label). 1_000_000 ns falls at/below le=4194304 (2^22)
    // and above le=524288 (2^19), so those two adjacent buckets bracket
    // the whole population.
    let h_plan = plan_for(
        engine,
        "{} | histogram_over_time(duration)",
        base_s(),
        end_s,
        CORPUS_SPANS,
    );
    let h_res = engine
        .metrics_instant(&h_plan)
        .await
        .expect("histogram instant");
    assert_eq!(
        h_res.series.len(),
        14,
        "one series per exp-le bucket: {h_res:?}"
    );
    let bucket = |le_ns: i64| -> f64 {
        let target = pulsus_read::MetricLabelValue::Double(le_ns as f64 / 1e9);
        h_res
            .series
            .iter()
            .find(|s| {
                s.labels
                    .iter()
                    .any(|l| l.key == "__bucket" && l.value == target)
            })
            .unwrap_or_else(|| panic!("no __bucket series for le={le_ns}"))
            .samples[0]
            .1
    };
    assert_eq!(bucket(524_288), 0.0, "no span <= 524288 ns");
    assert_eq!(
        bucket(4_194_304),
        CORPUS_SPANS as f64,
        "all spans <= 4194304 ns (cumulative)"
    );
    assert_eq!(
        bucket(1 << 40),
        CORPUS_SPANS as f64,
        "the top bucket holds all"
    );
}

/// P5 (issue #182): `with(exemplars=…)` collects ≥1 `trace:id` exemplar,
/// `with(sample=…)` is accepted (exact superset), and `topk`/`bottomk`
/// reduce the grouped series set per step.
async fn assert_exemplars_and_reduction(engine: &TraceEngine) {
    let end_s = base_s() + CORPUS_SPANS;

    // with(exemplars): review Fix 1 — EVERY range shape carries exemplars
    // (Tempo emits them for range rate/count/agg/quantile/histogram/
    // compare; none for instant). Each shape returns ≥1 exemplar with a
    // real 32-hex trace:id.
    for q in [
        "{} | rate() with(exemplars=2)",
        "{} | count_over_time() with(exemplars=2)",
        "{} | rate() by(resource.service.name) with(exemplars=2)",
        "{} | sum_over_time(duration) with(exemplars=2)",
        "{} | quantile_over_time(duration, 0.9) with(exemplars=2)",
        "{} | histogram_over_time(duration) with(exemplars=2)",
        r#"{} | compare({ span.http.status_code = "500" }) with(exemplars=2)"#,
    ] {
        let res = engine
            .metrics_range(&plan_for(engine, q, base_s(), end_s, 60))
            .await
            .unwrap_or_else(|e| panic!("{q}: {e}"));
        let exs: Vec<&pulsus_read::MetricExemplar> =
            res.series.iter().flat_map(|s| &s.exemplars).collect();
        assert!(
            !exs.is_empty(),
            "{q}: every range shape must carry exemplars"
        );
        let trace = exs[0]
            .labels
            .iter()
            .find(|l| l.key == "trace:id")
            .unwrap_or_else(|| panic!("{q}: exemplar carries a trace:id label"));
        match &trace.value {
            pulsus_read::MetricLabelValue::Str(hex) => {
                assert_eq!(hex.len(), 32, "{q}: 16-byte hex trace id: {hex:?}");
                assert!(hex.chars().all(|c| c.is_ascii_hexdigit()), "{q}");
            }
            other => panic!("{q}: trace:id must be a string, got {other:?}"),
        }
    }
    // Instant carries no exemplars (matches Tempo — verified black-box).
    let instant_ex = engine
        .metrics_instant(&plan_for(
            engine,
            "{} | rate() with(exemplars=2)",
            base_s(),
            end_s,
            60,
        ))
        .await
        .expect("instant exemplars");
    assert_eq!(
        instant_ex
            .series
            .iter()
            .map(|s| s.exemplars.len())
            .sum::<usize>(),
        0,
        "instant emits no exemplars, matching Tempo"
    );

    // with(sample): accepted, exact superset — identical to no sample.
    let plain = engine
        .metrics_range(&plan_for(engine, "{} | rate()", base_s(), end_s, 60))
        .await
        .expect("plain");
    let sampled = engine
        .metrics_range(&plan_for(
            engine,
            "{} | rate() with(sample=0.1)",
            base_s(),
            end_s,
            60,
        ))
        .await
        .expect("sampled");
    // Samples equal; sampled has no exemplars, plain has none either.
    assert_eq!(
        plain.series[0].samples, sampled.series[0].samples,
        "with(sample) returns the exact (superset) result"
    );

    // topk(1) over the two-service grouping keeps only the larger series
    // per step (svc-x = 480 > checkout = 120); bottomk(1) keeps checkout.
    let topk = engine
        .metrics_range(&plan_for(
            engine,
            "{} | count_over_time() by(resource.service.name) | topk(1)",
            base_s(),
            end_s,
            CORPUS_SPANS,
        ))
        .await
        .expect("topk");
    assert_eq!(topk.series.len(), 1, "topk(1) keeps one series");
    assert_eq!(service_label(&topk.series[0]), "svc-x");

    let bottomk = engine
        .metrics_range(&plan_for(
            engine,
            "{} | count_over_time() by(resource.service.name) | bottomk(1)",
            base_s(),
            end_s,
            CORPUS_SPANS,
        ))
        .await
        .expect("bottomk");
    assert_eq!(bottomk.series.len(), 1, "bottomk(1) keeps one series");
    assert_eq!(service_label(&bottomk.series[0]), "checkout");
}

/// P6b (issue #182): `compare({selection})` cross-tab meta-series and the
/// `rate() > 5` metrics-result comparison. The corpus has
/// `span.http.status_code = 500` on every 4th span (150 of 600) and 200 on
/// the rest (450); the selection is `status_code = 500`.
async fn assert_compare_and_result_comparison(engine: &TraceEngine) {
    let end_s = base_s() + CORPUS_SPANS;
    let plan = plan_for(
        engine,
        r#"{} | compare({ span.http.status_code = "500" })"#,
        base_s(),
        end_s,
        CORPUS_SPANS, // one whole-window bucket for exact counts
    );
    let res = engine.metrics_range(&plan).await.expect("compare executes");

    // Every series carries a __meta_type in the captured set.
    let meta_of = |s: &pulsus_read::TraceMetricSeries| -> String {
        match &s
            .labels
            .iter()
            .find(|l| l.key == "__meta_type")
            .unwrap()
            .value
        {
            pulsus_read::MetricLabelValue::Str(v) => v.clone(),
            other => panic!("__meta_type must be a string: {other:?}"),
        }
    };
    let metas: std::collections::BTreeSet<String> = res.series.iter().map(&meta_of).collect();
    assert!(
        metas.is_superset(
            &["baseline", "selection", "baseline_total", "selection_total"]
                .iter()
                .map(|s| s.to_string())
                .collect()
        ),
        "compare emits the four __meta_type kinds, got {metas:?}"
    );

    // Look up a series' single-bucket value by (meta_type, attr_key, attr_val).
    let value = |meta: &str, key: &str, val: &str| -> Option<f64> {
        res.series
            .iter()
            .find(|s| {
                meta_of(s) == meta
                    && s.labels.iter().any(|l| {
                        l.key == key
                            && matches!(&l.value, pulsus_read::MetricLabelValue::Str(v) if v == val)
                    })
            })
            .map(|s| s.samples.iter().map(|(_, v)| v).sum())
    };
    let k = "span.http.status_code";
    // baseline = the COMPLEMENT (non-selection spans, all status=200 → 450);
    // a selection value (500) never appears under baseline (the captured
    // Tempo convention). selection = the 150 status=500 spans.
    assert_eq!(
        value("baseline", k, "500"),
        None,
        "no baseline 500 (it is the selection)"
    );
    assert_eq!(
        value("baseline", k, "200"),
        Some(450.0),
        "baseline 200 = complement"
    );
    assert_eq!(value("selection", k, "500"), Some(150.0), "selection 500");
    assert_eq!(
        value("selection", k, "200"),
        None,
        "no 200 span in the selection"
    );
    // Totals: the complement / selection populations.
    assert_eq!(
        value("baseline_total", k, "nil"),
        Some(450.0),
        "baseline_total = complement"
    );
    assert_eq!(
        value("selection_total", k, "nil"),
        Some(150.0),
        "selection_total"
    );

    // Review Fix 3: the well-known-absent-attribute universe — every
    // well-known key Tempo enumerates appears as `key=nil` even when no
    // span carries it; a fully-absent key's baseline/selection nil counts
    // equal the totals. `rootServiceName` is NO LONGER here (issue #189:
    // now data-driven, asserted below); `instrumentation:name`/
    // `instrumentation:version` stay absent (value deferred to #179).
    for wk in [
        "resource.cluster",
        "resource.k8s.pod.name",
        "span.http.method",
        "span.url.path",
        "instrumentation:name",
        "instrumentation:version",
    ] {
        assert_eq!(
            value("baseline", wk, "nil"),
            Some(450.0),
            "{wk}: well-known-absent baseline nil == complement total"
        );
        assert_eq!(
            value("selection", wk, "nil"),
            Some(150.0),
            "{wk}: well-known-absent selection nil == selection total"
        );
        assert!(
            value("baseline_total", wk, "nil").is_some(),
            "{wk}: well-known key carries a baseline_total series"
        );
    }

    // Issue #189: `rootName`/`rootServiceName`/`statusMessage` emit REAL
    // per-value series (no longer well-known-`nil`). All 600 spans are
    // single-span roots named `op`, so `rootName=op` == the whole
    // population; `rootServiceName` follows the `checkout`/`svc-x` split;
    // `statusMessage='deadline exceeded'` is every 6th span (empty → the
    // nil complement, matching TraceQL's absent→nil).
    assert_eq!(value("selection", "rootName", "op"), Some(150.0));
    assert_eq!(value("baseline", "rootName", "op"), Some(450.0));
    assert_eq!(
        value("selection", "rootServiceName", "checkout"),
        Some(30.0)
    );
    assert_eq!(value("selection", "rootServiceName", "svc-x"), Some(120.0));
    assert_eq!(value("baseline", "rootServiceName", "checkout"), Some(90.0));
    assert_eq!(value("baseline", "rootServiceName", "svc-x"), Some(360.0));
    assert_eq!(
        value("selection", "statusMessage", "deadline exceeded"),
        Some(50.0)
    );
    assert_eq!(
        value("baseline", "statusMessage", "deadline exceeded"),
        Some(50.0)
    );
    // Empty status messages fold into the nil complement.
    assert_eq!(value("baseline", "statusMessage", "nil"), Some(400.0));
    assert_eq!(value("selection", "statusMessage", "nil"), Some(100.0));
    // Every in-window span has a root, so rootName/rootServiceName have NO
    // nil complement (dropped by the all-zero `retain`).
    assert_eq!(value("baseline", "rootName", "nil"), None);
    assert_eq!(value("selection", "rootName", "nil"), None);
    assert_eq!(value("baseline", "rootServiceName", "nil"), None);
    assert_eq!(value("selection", "rootServiceName", "nil"), None);

    // ---- result comparison (`> N`): a client-side sample post-filter. ---
    // count_over_time per 60s bucket == 60 (one span/second).
    let kept = engine
        .metrics_range(&plan_for(
            engine,
            "{} | count_over_time() > 50",
            base_s(),
            end_s,
            60,
        ))
        .await
        .expect("result-comparison kept");
    assert_eq!(kept.series.len(), 1, "60 > 50 keeps the series");
    assert!(kept.series[0].samples.iter().all(|(_, v)| *v > 50.0));

    let dropped = engine
        .metrics_range(&plan_for(
            engine,
            "{} | count_over_time() > 100",
            base_s(),
            end_s,
            60,
        ))
        .await
        .expect("result-comparison dropped");
    assert!(
        dropped.series.is_empty(),
        "60 > 100 drops every sample → no series"
    );
}

/// Isolated DB for the trace-wide-roots gate (issue #189 adjudication #1).
const DB_TW: &str = "pulsus_traces_metrics_tw_it";

/// Issue #189 AC5 — the trace-wide (window-free) roots gate. A single
/// 2-span trace: the root (`parent_id=0`, `name='root-op'`) sits an hour
/// BEFORE the compare window; its child (`name='child-op'`) sits inside
/// it. `compare()` over a window covering ONLY the child must still
/// resolve `rootName='root-op'`/`rootServiceName='root-svc'` (the root is
/// pulled in by the window-free `argMin` roots read), never the in-window
/// `child-op`/`child-svc`. A window-SCOPED read would have produced the
/// child's own values — this mechanically distinguishes the two.
#[tokio::test]
async fn compare_roots_resolve_trace_wide() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see crates/pulsus-read/tests/traces_metrics_live.rs for setup)"
        );
        return;
    }

    let admin = ChClient::new(test_config()).await.expect("connect");
    exec(&admin, &format!("DROP DATABASE IF EXISTS {DB_TW}")).await;
    run_init(&admin, &test_ctx(DB_TW)).await.expect("run_init");

    let client = {
        let mut cfg = test_config();
        cfg.database = DB_TW.to_string();
        ChClient::new(cfg).await.expect("connect data client")
    };

    // Aligned single-bucket window; the child is inside, the root is not.
    let window_start = base_s();
    let window_end = base_s() + CORPUS_SPANS;
    let child_ns = (window_start + 300) * NS;
    let root_ns = (window_start - 3600) * NS; // out of window
    const TID: &str = "000000000000000000000000000000aa";
    exec(
        &client,
        &format!(
            "INSERT INTO {DB_TW}.trace_spans \
             (trace_id, span_id, parent_id, name, service, status_message, timestamp_ns, \
              duration_ns, status_code, kind, payload_type, payload) VALUES \
             (toFixedString(unhex('{TID}'), 16), toFixedString(unhex('0000000000000001'), 8), \
              toFixedString(unhex('0000000000000000'), 8), 'root-op', 'root-svc', '', {root_ns}, \
              1000000, 0, 1, 1, 'p'), \
             (toFixedString(unhex('{TID}'), 16), toFixedString(unhex('0000000000000002'), 8), \
              toFixedString(unhex('0000000000000001'), 8), 'child-op', 'child-svc', '', {child_ns}, \
              1000000, 0, 1, 1, 'p')"
        ),
    )
    .await;

    let engine = TraceEngine::new(
        {
            let mut cfg = test_config();
            cfg.database = DB_TW.to_string();
            ChClient::new(cfg).await.expect("connect engine")
        },
        engine_config(),
    );
    // A selection matching nothing keeps the child in the baseline.
    let plan = plan_for(
        &engine,
        r#"{} | compare({ name = "no-match" })"#,
        window_start,
        window_end,
        CORPUS_SPANS,
    );
    let res = engine.metrics_range(&plan).await.expect("compare executes");

    let meta_of = |s: &pulsus_read::TraceMetricSeries| -> String {
        match &s
            .labels
            .iter()
            .find(|l| l.key == "__meta_type")
            .unwrap()
            .value
        {
            pulsus_read::MetricLabelValue::Str(v) => v.clone(),
            other => panic!("__meta_type must be a string: {other:?}"),
        }
    };
    let value = |meta: &str, key: &str, val: &str| -> Option<f64> {
        res.series
            .iter()
            .find(|s| {
                meta_of(s) == meta
                    && s.labels.iter().any(|l| {
                        l.key == key
                            && matches!(&l.value, pulsus_read::MetricLabelValue::Str(v) if v == val)
                    })
            })
            .map(|s| s.samples.iter().map(|(_, v)| v).sum())
    };

    // The window contains only the child; its root is resolved trace-wide.
    assert_eq!(
        value("baseline", "rootName", "root-op"),
        Some(1.0),
        "rootName resolves the out-of-window root, not the in-window child"
    );
    assert_eq!(
        value("baseline", "rootName", "child-op"),
        None,
        "a window-scoped read would (wrongly) have produced child-op"
    );
    assert_eq!(
        value("baseline", "rootServiceName", "root-svc"),
        Some(1.0),
        "rootServiceName resolves the out-of-window root's service"
    );
    assert_eq!(value("baseline", "rootServiceName", "child-svc"), None);

    exec(&admin, &format!("DROP DATABASE IF EXISTS {DB_TW}")).await;
}

/// The `resource.service.name` string label value of a series.
fn service_label(series: &pulsus_read::TraceMetricSeries) -> String {
    match &series
        .labels
        .iter()
        .find(|l| l.key == "resource.service.name")
        .expect("service label")
        .value
    {
        pulsus_read::MetricLabelValue::Str(s) => s.clone(),
        other => panic!("service label must be a string, got {other:?}"),
    }
}

/// One `#[tokio::test]` running every gate in sequence — the corpus is
/// seeded once (then duplicated once, for the replay gate).
#[tokio::test]
async fn metrics_internal_consistency_identities() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see crates/pulsus-read/tests/traces_metrics_live.rs for setup)"
        );
        return;
    }

    let admin = ChClient::new(test_config()).await.expect("connect");
    exec(&admin, &format!("DROP DATABASE IF EXISTS {DB}")).await;
    run_init(&admin, &test_ctx(DB)).await.expect("run_init");

    let client = data_client().await;
    seed_corpus(&client, DB).await;
    let engine = TraceEngine::new(data_client().await, engine_config());

    // Independent expected counts, computed from the seeding rules.
    let checkout_500 = (0..CORPUS_SPANS)
        .filter(|i| i % 5 == 0 && i % 4 == 0)
        .count() as i64;
    let status_500 = (0..CORPUS_SPANS).filter(|i| i % 4 == 0).count() as i64;
    let env_absent = (0..CORPUS_SPANS)
        .filter(|i| i % 3 != 0 && i % 7 != 0)
        .count() as i64;

    // ---- AC4 (a)+(b) across the gated filter shapes --------------------
    assert_identities(&engine, "{}", CORPUS_SPANS).await;
    assert_identities(
        &engine,
        r#"{ resource.service.name = "checkout" && span.http.status_code >= 500 }"#,
        checkout_500,
    )
    .await;
    assert_identities(&engine, "{ span.http.status_code >= 500 }", status_500).await;
    // Unscoped dual-scope negation: spans with a positive `env = prod`
    // row in EITHER scope are excluded; absent-key spans count.
    assert_identities(&engine, r#"{ .env != "prod" }"#, env_absent).await;

    // ---- Replay dedup (plan v2 delta 1): duplicate-insert the corpus —
    // range AND instant results identical to single-insert. -------------
    let end_s = base_s() + CORPUS_SPANS;
    let rate_plan = plan_for(
        &engine,
        r#"{ resource.service.name = "checkout" && span.http.status_code >= 500 } | rate()"#,
        base_s(),
        end_s,
        60,
    );
    let before_range = engine.metrics_range(&rate_plan).await.expect("range");
    let before_instant = engine.metrics_instant(&rate_plan).await.expect("instant");
    seed_corpus(&client, DB).await; // every row now exists twice
    let after_range = engine.metrics_range(&rate_plan).await.expect("range dup");
    let after_instant = engine
        .metrics_instant(&rate_plan)
        .await
        .expect("instant dup");
    assert_eq!(
        before_range, after_range,
        "at-least-once replays must never inflate a bucket (uniqExact dedup)"
    );
    assert_eq!(before_instant, after_instant);

    // ---- Unaligned window: outward snap, full-width edge buckets ------
    // [BASE+30, BASE+90) at step 60 snaps to [BASE, BASE+120): two
    // buckets covering 120 one-per-second spans.
    let plan = plan_for(
        &engine,
        "{} | count_over_time()",
        base_s() + 30,
        base_s() + 90,
        60,
    );
    assert_eq!(
        plan.snapped_window_ns(),
        (base_s() * NS, (base_s() + 120) * NS),
        "outward snap"
    );
    let points = matrix_points(&engine.metrics_range(&plan).await.expect("unaligned"));
    assert_eq!(
        points,
        vec![(base_s() * 1_000, 60.0), ((base_s() + 60) * 1_000, 60.0)],
        "snapped edge buckets are full-width — no partial denominators"
    );

    // ---- Exact right boundary: a span at an aligned `end` is excluded
    // (left-closed/right-open), never pulled into a clipped bucket. -----
    let plan = plan_for(
        &engine,
        "{} | count_over_time()",
        base_s(),
        base_s() + 60,
        60,
    );
    let points = matrix_points(&engine.metrics_range(&plan).await.expect("boundary"));
    assert_eq!(
        points,
        vec![(base_s() * 1_000, 60.0)],
        "spans at seconds 0..=59 count; the span exactly at end (second 60) is excluded"
    );
    // …and with an UNALIGNED end inside the corpus, the raw-end span IS
    // included in the final snapped bucket.
    let plan = plan_for(
        &engine,
        "{} | count_over_time()",
        base_s(),
        base_s() + 30,
        60,
    );
    let points = matrix_points(&engine.metrics_range(&plan).await.expect("snap end"));
    assert_eq!(
        points,
        vec![(base_s() * 1_000, 60.0)],
        "E snaps outward to BASE+60 — the documented over-inclusion, one full bucket"
    );

    // ---- Empty window: range → empty matrix; instant → one "0" sample
    // (the documented empty-DB oracles). ---------------------------------
    let empty_start = base_s() - 86_400;
    let plan = plan_for(&engine, "{} | rate()", empty_start, empty_start + 600, 60);
    assert!(
        engine
            .metrics_range(&plan)
            .await
            .expect("empty range")
            .series
            .is_empty(),
        "an empty range is no series"
    );
    let instant = engine.metrics_instant(&plan).await.expect("empty instant");
    assert_eq!(vector_value(&instant), 0.0);

    // ---- P3 (issue #182): value aggregations, by() grouping, series cap.
    assert_aggregation_identities(&engine).await;
    assert_by_service_grouping(&engine).await;
    assert_series_cap_rejects().await;
    // ---- P4 (issue #182): quantile (TDigest) + histogram (exp-le).
    assert_quantile_and_histogram(&engine).await;
    // ---- P5 (issue #182): exemplars, with(sample), topk/bottomk.
    assert_exemplars_and_reduction(&engine).await;
    // ---- P6b (issue #182): compare() cross-tab + result comparison.
    assert_compare_and_result_comparison(&engine).await;

    // ---- Extreme-epoch bucket labels (issue #59 re-audit): pre-1970 and
    // post-2106 buckets must produce the correct Int64 millisecond label,
    // never a UInt32-epoch-seconds wrap. Runs against `trace_spans_extreme`
    // (see `create_extreme_epoch_view`), not the physical `trace_spans`
    // table — sidesteps a separate, out-of-scope schema TTL/partition
    // overflow, still a live round trip through the real generated SQL. ---
    create_extreme_epoch_view(&client, DB).await;
    let extreme_engine = extreme_epoch_engine(data_client().await);

    let past_plan = plan_for(
        &extreme_engine,
        "{} | count_over_time()",
        EXTREME_PAST_S,
        EXTREME_PAST_S + 60,
        60,
    );
    let past_points = matrix_points(
        &extreme_engine
            .metrics_range(&past_plan)
            .await
            .expect("pre-1970 range"),
    );
    assert_eq!(
        past_points,
        vec![(EXTREME_PAST_S * 1_000, 1.0)],
        "pre-1970 bucket label must be the exact negative millisecond value, not wrapped"
    );

    let future_plan = plan_for(
        &extreme_engine,
        "{} | count_over_time()",
        EXTREME_FUTURE_S,
        EXTREME_FUTURE_S + 60,
        60,
    );
    let future_points = matrix_points(
        &extreme_engine
            .metrics_range(&future_plan)
            .await
            .expect("post-2106 range"),
    );
    assert_eq!(
        future_points,
        vec![(EXTREME_FUTURE_S * 1_000, 1.0)],
        "post-2106 bucket label must be the exact >UInt32-max millisecond value, not wrapped \
         mod 2^32"
    );
}
