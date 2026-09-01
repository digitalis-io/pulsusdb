//! Issue #59 AC4 (Tier-1, scale-invariant): the internal-consistency
//! identities for the TraceQL metrics endpoints against ClickHouse 26.3,
//! on a seeded deterministic corpus:
//!
//! - **(a)** `Σ_buckets rate·step == Σ_buckets count_over_time ==`
//!   an independent deduped `COUNT` of the matching spans — for every
//!   gated filter shape (service PREWHERE, attr semi-join, negation,
//!   match-all). The step is carried in MILLISECONDS since issue #477
//!   (d), so the rate factor is the step's width in fractional seconds.
//! - **Bucket geometry** (issue #477): the range form labels a bucket by
//!   its RIGHT edge and reads the right-closed instants
//!   `(aS - step, aE]`, so a window of `n` steps emits `n + 1` samples —
//!   one extra LEADING bucket — and a span landing exactly on a grid
//!   point belongs to THAT point. Every densified shape emits every
//!   bucket, zero-valued where nothing matched.
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
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 clickhouse/clickhouse-server:26.3
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test traces_metrics_live
//! podman rm -f pulsus-ch-test
//! ```

use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_read::logql::error::TooBroadReason;
use pulsus_read::traces::metrics_plan::{MetricsParams, plan_trace_metrics};
use pulsus_read::{ReadError, TraceEngine, TraceMetricsPlan, TraceMetricsResult, TraceReadConfig};
use pulsus_schema::{RenderCtx, SchemaParams, run_init};

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping when
/// the gate is absent in a live CI job, so a lost `env:` block reddens the
/// build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
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

static DB: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_traces_metrics_it");
/// Issue #252's own throwaway database: the log2-histogram membership,
/// sub-2ns guard and quantile-divergence corpora need durations the
/// shared `DB` corpus deliberately does not have.
static DB_LOG2: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_traces_metrics_log2_it");

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
    step_ms: i64,
) -> TraceMetricsPlan {
    plan_with(engine, q, start_s, end_s, step_ms, None)
}

/// [`plan_for`] with an explicit HTTP `exemplars` parameter (issue #477
/// (c)) — the input the datasource sends and the `with()` hint overrides.
fn plan_with(
    engine: &TraceEngine,
    q: &str,
    start_s: i64,
    end_s: i64,
    step_ms: i64,
    exemplars: Option<u32>,
) -> TraceMetricsPlan {
    let query = pulsus_traceql::parse(q).expect("query parses");
    plan_trace_metrics(
        &query,
        &MetricsParams {
            start_ns: start_s * NS,
            end_ns: end_s * NS,
            step_ms,
            exemplars,
        },
        &engine.metrics_ctx(),
    )
    .expect("query plans")
}

/// The one window source for every live metrics case this issue adds:
/// `(now - 60 - width, now - 60)`.
///
/// The 60-second margin is fixed and is chosen against the reference's
/// own 30-second end cutoff, not against any measured transition — a
/// window ending too close to `now` is clamped there and can invert.
/// Nothing here depends on where that transition falls; the margin sits
/// clear of the cutoff itself.
fn past_window(width_s: i64) -> (i64, i64) {
    let now_s = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_secs(),
    )
    .expect("fits i64");
    (now_s - 60 - width_s, now_s - 60)
}

/// [`past_window`] floored onto a whole-second step grid, so the snap is
/// the identity and the emitted axis has a size the test can state.
///
/// `past_window` is the only clock source; this only moves its start down
/// to the nearest step multiple and takes `width_s` from there. Without
/// it the axis is `intervals + 1` for an `intervals` that depends on where
/// the current second happens to fall, which is not something a test can
/// assert against.
///
/// The parameter is `step_seconds` rather than the obvious short name
/// because AC14's sibling scan bans the pre-#477 spelling of that in this
/// file, whatever it means locally. It caught this on the first run.
fn aligned_past_window(width_s: i64, step_seconds: i64) -> (i64, i64) {
    assert_eq!(width_s % step_seconds, 0, "the width must be whole steps");
    let (start_s, _) = past_window(width_s);
    let aligned = start_s.div_euclid(step_seconds) * step_seconds;
    (aligned, aligned + width_s)
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
/// primary window `[base_s(), base_s() + CORPUS_SPANS)`, step 60 s,
/// against an independently-computed expected span count.
///
/// The identity survives issue #477 unchanged in substance but not in
/// arithmetic. The range form now reads `(aS - step, aE]` and emits
/// `intervals + 1` labels, so the corpus's span at exactly `base_s()`
/// lands in the LEADING bucket rather than in the first interior one,
/// and every bucket is emitted whether or not it has data. Both sums are
/// still over the whole emitted axis, and the axis still covers every
/// corpus span, so both still equal `expected`.
async fn assert_identities(engine: &TraceEngine, filter: &str, expected: i64) {
    let end_s = base_s() + CORPUS_SPANS;
    let step_ms = 60_000;
    let window_s = CORPUS_SPANS;

    let rate_plan = plan_for(
        engine,
        &format!("{filter} | rate()"),
        base_s(),
        end_s,
        step_ms,
    );
    let count_plan = plan_for(
        engine,
        &format!("{filter} | count_over_time()"),
        base_s(),
        end_s,
        step_ms,
    );
    // Snap is the identity on this aligned window; the INSTANT window is
    // what `snapped_window_ns` reports and issue #477 leaves it alone.
    assert_eq!(rate_plan.snapped_window_ns(), (base_s() * NS, end_s * NS));
    // The range window is one whole step wider on the left and includes
    // its own right edge.
    assert_eq!(
        rate_plan.range_window_ns(),
        ((base_s() - 60) * NS + 1, end_s * NS + 1)
    );

    let axis = rate_plan.range_axis();
    assert_eq!(
        axis.points,
        (CORPUS_SPANS / 60 + 1) as usize,
        "{filter}: intervals + 1 labels"
    );

    let rate_points = matrix_points(&engine.metrics_range(&rate_plan).await.expect("rate range"));
    let count_points = matrix_points(
        &engine
            .metrics_range(&count_plan)
            .await
            .expect("count range"),
    );

    // Issue #477 (a): an ungrouped count/rate emits EVERY bucket, always,
    // even when nothing matched anywhere in the window.
    assert_eq!(
        rate_points.len(),
        axis.points,
        "{filter}: the sample count is the interval count from the window and the step"
    );
    assert_eq!(count_points.len(), axis.points, "{filter}");

    // (a) Σ rate·step == Σ count_over_time == the independent count.
    let step_seconds = step_ms as f64 / 1_000.0;
    let rate_total: f64 = rate_points.iter().map(|(_, v)| v * step_seconds).sum();
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
    // Bucket timestamps are the axis labels: epoch-aligned milliseconds
    // from `aS` to `aE` INCLUSIVE (the right edge is a label now).
    for (i, (t_ms, _)) in rate_points.iter().enumerate() {
        assert_eq!(*t_ms, axis.label_ms(i), "{filter}: label {i}");
        assert_eq!(t_ms % step_ms, 0, "{filter}: unaligned bucket {t_ms}");
        assert!(*t_ms >= base_s() * 1_000 && *t_ms <= end_s * 1_000);
    }
    assert_eq!(axis.last_ms(), end_s * 1_000, "{filter}: last label is aE");

    // (b) instant == the whole-window range, summed over its axis.
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
        window_s * 1_000,
    );
    let whole_points = matrix_points(&engine.metrics_range(&whole_rate_plan).await.expect("whole"));
    // Two labels — the leading bucket `(aS - window, aS]` and the window
    // bucket `(aS, aE]` — not one. The identity is the SUM.
    assert_eq!(
        whole_points.len(),
        2,
        "{filter}: step = window is the leading bucket plus one interval"
    );
    assert_eq!(whole_points[0].0, base_s() * 1_000);
    assert_eq!(whole_points[1].0, end_s * 1_000);
    let whole_total: f64 = whole_points.iter().map(|(_, v)| v * window_s as f64).sum();
    assert!(
        (whole_total - instant_rate * window_s as f64).abs() < 1e-9,
        "{filter}: instant ({instant_rate}) accounts for the whole-window axis ({whole_points:?})"
    );
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
    let sum_plan = plan_for(
        engine,
        "{} | sum_over_time(duration)",
        base_s(),
        end_s,
        60_000,
    );
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
            60_000,
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
        CORPUS_SPANS * 1_000, // one whole-window interval, plus the leading bucket
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
        60_000,
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

/// P4 (issue #182 / #252): `quantile_over_time` (TDigest) and
/// `histogram_over_time` (the reference's log2 tally). Every corpus span
/// has `duration_ns = 1_000_000` (0.001 s), so every quantile is 0.001 s
/// and the histogram is exactly ONE series — the bucket
/// `2^20 ns = 0.001048576 s` these durations round up to — carrying the
/// whole population as a plain tally. Membership, the gap case and the
/// sub-2ns guard get their own corpus in
/// [`log2_histogram_membership_and_the_sub_two_ns_guard`].
async fn assert_quantile_and_histogram(engine: &TraceEngine) {
    let end_s = base_s() + CORPUS_SPANS;

    // quantile_over_time instant: one series per quantile (`p` label),
    // each == 0.001 s (all durations equal).
    let q_plan = plan_for(
        engine,
        "{} | quantile_over_time(duration, 0.5, 0.9)",
        base_s(),
        end_s,
        CORPUS_SPANS * 1_000,
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

    // histogram_over_time instant (issue #252): membership is
    // occurrence-only, so a uniform corpus emits exactly ONE series —
    // the bucket its duration rounds up to — and its value is the plain
    // tally, not a running total.
    let h_plan = plan_for(
        engine,
        "{} | histogram_over_time(duration)",
        base_s(),
        end_s,
        CORPUS_SPANS * 1_000,
    );
    let h_res = engine
        .metrics_instant(&h_plan)
        .await
        .expect("histogram instant");
    assert_eq!(
        h_res.series.len(),
        1,
        "a uniform corpus occupies exactly one power-of-two bucket: {h_res:?}"
    );
    assert_eq!(
        h_res.series[0].labels,
        vec![pulsus_read::MetricLabel::double("__bucket", 0.001048576)],
        "1_000_000 ns rounds up to 2^20 ns = 0.001048576 s"
    );
    assert_eq!(h_res.series[0].samples[0].1, CORPUS_SPANS as f64);
}

/// Issue #252 AC4/AC2.4/AC3c, on its own throwaway database (the shared
/// `DB` corpus is uniformly `duration_ns = 1_000_000` and its
/// aggregation identities must not be perturbed). Four things that need
/// real rows and a real ClickHouse:
///
/// - **membership with a GAP** — a corpus occupying `2^29` and `2^33`
///   emits exactly those two series and NOTHING for `2^30`/`2^31`/`2^32`
///   (the fixed-ladder form could not express this);
/// - **the tallies are plain counts** — they SUM to the deduped span
///   count, which the cumulative form could not satisfy (AC5's mutant
///   target);
/// - **the sub-2ns guard** — spans of `-1`, `0` and `1` ns produce no
///   `__bucket` series at all while `count_over_time` counts all four
///   spans. This one is tested rather than reasoned about because the
///   pushed-down expression does NOT reject them on its own: measured on
///   24.8.14.39, `toUInt64(roundToExp2(val - 1)) * 2` is `0` for every
///   `val <= 1`, so dropping the outer `WHERE val >= 2` would emit a
///   spurious `__bucket = 0` series the reference never emits;
/// - **AC3c, the ledgered quantile divergence, with its direction** —
///   over 20 identical 300 ms spans our `quantile_over_time(duration,
///   0.99)` is exactly `0.3` (the true p99) where the reference returns
///   `0.536870912`, and over 520 ms spans ours moves to `0.52` where the
///   reference returns the same `0.536870912` bytes for both.
#[tokio::test]
async fn log2_histogram_membership_and_the_sub_two_ns_guard() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see crates/pulsus-read/tests/traces_metrics_live.rs for setup)"
        );
        return;
    }

    let admin = ChClient::new(test_config()).await.expect("connect");
    exec(&admin, &format!("DROP DATABASE IF EXISTS {DB_LOG2}")).await;
    run_init(&admin, &test_ctx(&DB_LOG2))
        .await
        .expect("run_init");

    let client = {
        let mut cfg = test_config();
        cfg.database = DB_LOG2.to_string();
        ChClient::new(cfg).await.expect("connect data client")
    };

    // Four services, one corpus each. `gap`: ten 300 ms spans (2^29) and
    // ten 5 s spans (2^33), leaving 2^30/2^31/2^32 empty — the same
    // occupancy the committed reference capture's `w252` corpus has.
    // `guard`: `-1`, `0`, `1` ns and one normal 300 ms span. `u300` /
    // `u520`: twenty identical spans each.
    let mut rows: Vec<String> = Vec::new();
    let push = |svc: &str, idx: i64, dur_ns: i64, rows: &mut Vec<String>| {
        let id = 1000 + idx;
        let ts_ns = (base_s() + (idx % 300)) * NS;
        rows.push(format!(
            "(toFixedString(unhex('{id:032x}'), 16), toFixedString(unhex('{id:016x}'), 8), \
             toFixedString(unhex('0000000000000000'), 8), 'op', '{svc}', '', {ts_ns}, \
             {dur_ns}, 0, 1, 1, 'p')"
        ));
    };
    let mut idx = 0i64;
    for dur in std::iter::repeat_n(300_000_000i64, 10).chain(std::iter::repeat_n(5_000_000_000, 10))
    {
        push("gap", idx, dur, &mut rows);
        idx += 1;
    }
    for dur in [-1i64, 0, 1, 300_000_000] {
        push("guard", idx, dur, &mut rows);
        idx += 1;
    }
    for dur in std::iter::repeat_n(300_000_000i64, 20) {
        push("u300", idx, dur, &mut rows);
        idx += 1;
    }
    for dur in std::iter::repeat_n(520_000_000i64, 20) {
        push("u520", idx, dur, &mut rows);
        idx += 1;
    }
    exec(
        &client,
        &format!(
            "INSERT INTO {DB_LOG2}.trace_spans \
             (trace_id, span_id, parent_id, name, service, status_message, timestamp_ns, \
              duration_ns, status_code, kind, payload_type, payload) VALUES {}",
            rows.join(", ")
        ),
    )
    .await;

    let engine = TraceEngine::new(
        {
            let mut cfg = test_config();
            cfg.database = DB_LOG2.to_string();
            ChClient::new(cfg).await.expect("connect engine")
        },
        engine_config(),
    );
    let end_s = base_s() + CORPUS_SPANS;
    let instant = |q: String| {
        let engine = &engine;
        async move {
            let plan = plan_for(engine, &q, base_s(), end_s, CORPUS_SPANS * 1_000);
            engine.metrics_instant(&plan).await.expect("instant")
        }
    };

    /// The `(bucket seconds, tally)` pairs of a histogram result, in
    /// emitted order.
    fn buckets(result: &TraceMetricsResult) -> Vec<(f64, f64)> {
        result
            .series
            .iter()
            .map(|s| {
                assert_eq!(s.labels.len(), 1, "one __bucket label: {s:?}");
                assert_eq!(s.labels[0].key, "__bucket");
                let pulsus_read::MetricLabelValue::Double(seconds) = s.labels[0].value else {
                    panic!("__bucket is a double: {s:?}");
                };
                assert_eq!(s.samples.len(), 1, "instant form has one sample: {s:?}");
                (seconds, s.samples[0].1)
            })
            .collect()
    }

    // ---- membership with a deliberate occupancy gap -------------------
    let gap =
        instant(r#"{ resource.service.name = "gap" } | histogram_over_time(duration)"#.to_string())
            .await;
    assert_eq!(
        buckets(&gap),
        vec![(0.536870912, 10.0), (8.589934592, 10.0)],
        "exactly the OCCUPIED powers of two, each a plain tally"
    );
    // The gap buckets are absent, not zero-valued.
    for empty in [1.073741824f64, 2.147483648, 4.294967296] {
        assert!(
            !gap.series
                .iter()
                .any(|s| s.labels[0].value == pulsus_read::MetricLabelValue::Double(empty)),
            "2^k = {empty}s is empty and must emit NO series: {gap:?}"
        );
    }
    // The tallies SUM to the deduped span count — the identity the
    // cumulative form cannot satisfy (it would sum to 30 here).
    let tally_sum: f64 = buckets(&gap).iter().map(|(_, n)| n).sum();
    assert_eq!(tally_sum, 20.0, "tallies are counts, not running totals");
    let gap_count = vector_value(
        &instant(r#"{ resource.service.name = "gap" } | count_over_time()"#.to_string()).await,
    );
    assert_eq!(tally_sum, gap_count, "and they account for every span");

    // ---- the sub-2ns guard (AC2.4) ------------------------------------
    let guard = instant(
        r#"{ resource.service.name = "guard" } | histogram_over_time(duration)"#.to_string(),
    )
    .await;
    assert_eq!(
        buckets(&guard),
        vec![(0.536870912, 1.0)],
        "only the 300 ms span is bucketed; -1, 0 and 1 ns are dropped from the SERIES"
    );
    assert_eq!(
        vector_value(
            &instant(r#"{ resource.service.name = "guard" } | count_over_time()"#.to_string())
                .await
        ),
        4.0,
        "…while count_over_time still counts all four spans"
    );

    // ---- AC3c: the ledgered quantile divergence, and its direction ----
    // `2026-08-05-traceql-quantile-over-time-tdigest`. Uniform corpora
    // make this a scale-invariant identity rather than a tuned number:
    // over identical values the true quantile is exact for every p.
    for (svc, want, want_bucket) in [
        ("u300", 0.3f64, 0.536870912f64),
        ("u520", 0.52, 0.536870912),
    ] {
        // All four quantiles docs/api.md §4.4.1's table states, not
        // just p99: a number in that table with no test behind it is a
        // defect (AC14-example).
        let q = instant(format!(
            r#"{{ resource.service.name = "{svc}" }} | quantile_over_time(duration, 0.5, 0.9, 0.99, 1.0)"#
        ))
        .await;
        assert_eq!(q.series.len(), 4, "one series per quantile: {q:?}");
        for (series, p) in q.series.iter().zip([0.5f64, 0.9, 0.99, 1.0]) {
            let got = series.samples[0].1;
            assert_eq!(
                got.to_bits(),
                want.to_bits(),
                "{svc}: our p{p} is the TRUE p{p} ({want}), got {got}"
            );
            assert!(
                got < want_bucket,
                "{svc}: p{p} is strictly below the reference's bucket label {want_bucket}"
            );
        }
        // Both corpora sit in the SAME bucket, which is why the
        // reference cannot tell them apart.
        let h = instant(format!(
            r#"{{ resource.service.name = "{svc}" }} | histogram_over_time(duration)"#
        ))
        .await;
        assert_eq!(buckets(&h), vec![(want_bucket, 20.0)]);
    }
    // The pair is the point: theirs is byte-identical across an 86%
    // rise, ours moves with the data.
    assert_ne!(0.3f64.to_bits(), 0.52f64.to_bits());

    exec(&admin, &format!("DROP DATABASE IF EXISTS {DB_LOG2}")).await;
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
            .metrics_range(&plan_for(engine, q, base_s(), end_s, 60_000))
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
            60_000,
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
        .metrics_range(&plan_for(engine, "{} | rate()", base_s(), end_s, 60_000))
        .await
        .expect("plain");
    let sampled = engine
        .metrics_range(&plan_for(
            engine,
            "{} | rate() with(sample=0.1)",
            base_s(),
            end_s,
            60_000,
        ))
        .await
        .expect("sampled");
    // Samples equal. Both now carry exemplars — issue #477 (c) turns them
    // on by default — and that is not what this gate is about.
    assert_eq!(
        plain.series[0].samples, sampled.series[0].samples,
        "with(sample) returns the exact (superset) result"
    );

    // topk(1)/bottomk(1) over the two-service grouping, PER TIMESTAMP.
    //
    // Densification (issue #477 (a)) is what makes this a real per-step
    // test rather than a per-series one. The axis is two labels: the
    // leading bucket `(aS - 600, aS]`, which holds only the corpus span
    // at exactly `aS` — a `checkout` span, since `0 % 5 == 0` — and the
    // window bucket, which holds the other 599. So `checkout` is the
    // LARGER series at the first label (1 against `svc-x`'s densified 0)
    // and the smaller at the second (119 against 480), and the reduction
    // keeps a different series at each. Before densification `svc-x` had
    // no sample at all in the leading bucket and the answer was one
    // series; asserting one series now would be asserting the defect.
    let topk = engine
        .metrics_range(&plan_for(
            engine,
            "{} | count_over_time() by(resource.service.name) | topk(1)",
            base_s(),
            end_s,
            CORPUS_SPANS * 1_000,
        ))
        .await
        .expect("topk");
    assert_eq!(
        topk.series.len(),
        2,
        "topk(1) keeps one series PER TIMESTAMP, and they differ: {topk:?}"
    );
    assert_eq!(service_label(&topk.series[0]), "checkout");
    assert_eq!(
        topk.series[0].samples,
        vec![(base_s() * 1_000, 1.0)],
        "checkout wins the leading bucket"
    );
    assert_eq!(service_label(&topk.series[1]), "svc-x");
    assert_eq!(
        topk.series[1].samples,
        vec![((base_s() + CORPUS_SPANS) * 1_000, 480.0)],
        "svc-x wins the window bucket"
    );

    let bottomk = engine
        .metrics_range(&plan_for(
            engine,
            "{} | count_over_time() by(resource.service.name) | bottomk(1)",
            base_s(),
            end_s,
            CORPUS_SPANS * 1_000,
        ))
        .await
        .expect("bottomk");
    assert_eq!(
        bottomk.series.len(),
        2,
        "bottomk(1) is the mirror image: {bottomk:?}"
    );
    assert_eq!(service_label(&bottomk.series[0]), "checkout");
    assert_eq!(
        bottomk.series[0].samples,
        vec![((base_s() + CORPUS_SPANS) * 1_000, 119.0)],
        "checkout loses the window bucket"
    );
    assert_eq!(service_label(&bottomk.series[1]), "svc-x");
    assert_eq!(
        bottomk.series[1].samples,
        vec![(base_s() * 1_000, 0.0)],
        "svc-x loses the leading bucket, where densification gives it a zero"
    );
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
        CORPUS_SPANS * 1_000, // one whole-window interval for exact counts
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
    // `instrumentation:version` are NO LONGER here either (issue #192: now
    // data-driven physical columns, asserted below).
    for wk in [
        "resource.cluster",
        "resource.k8s.pod.name",
        "span.http.method",
        "span.url.path",
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
    // `statusMessage='deadline exceeded'` is every 6th span, and every other
    // span's EMPTY `statusMessage` emits as a DISTINCT `""` value (issue
    // #185: `status_message` is a physical column — no absent case — and
    // Tempo v3.0.2 emits empty as `""`, not folded to the nil complement).
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
    // Empty status messages emit as the distinct `""` value (the empty
    // spans: 400 baseline / 100 selection).
    assert_eq!(value("baseline", "statusMessage", ""), Some(400.0));
    assert_eq!(value("selection", "statusMessage", ""), Some(100.0));
    // Every span carries a `status_message` (`""`-or-value), so
    // statusMessage — like rootName/rootServiceName — has NO nil complement
    // (its all-zero `key=nil` series is dropped by the all-zero `retain`).
    assert_eq!(value("baseline", "statusMessage", "nil"), None);
    assert_eq!(value("selection", "statusMessage", "nil"), None);
    assert_eq!(value("baseline", "rootName", "nil"), None);
    assert_eq!(value("selection", "rootName", "nil"), None);
    assert_eq!(value("baseline", "rootServiceName", "nil"), None);
    assert_eq!(value("selection", "rootServiceName", "nil"), None);

    // Issue #192: `instrumentation:name`/`instrumentation:version` are now
    // data-driven physical columns (`scope_name`/`scope_version`). The seed
    // spans carry no scope, so every span emits the DISTINCT `""` value (the
    // `statusMessage` empty-value precedent) — baseline 450 / selection 150 —
    // and, like `statusMessage`, there is NO nil complement.
    assert_eq!(value("baseline", "instrumentation:name", ""), Some(450.0));
    assert_eq!(value("selection", "instrumentation:name", ""), Some(150.0));
    assert_eq!(
        value("baseline", "instrumentation:version", ""),
        Some(450.0)
    );
    assert_eq!(
        value("selection", "instrumentation:version", ""),
        Some(150.0)
    );
    assert_eq!(value("baseline", "instrumentation:name", "nil"), None);
    assert_eq!(value("selection", "instrumentation:name", "nil"), None);
    assert_eq!(value("baseline", "instrumentation:version", "nil"), None);
    assert_eq!(value("selection", "instrumentation:version", "nil"), None);

    // ---- result comparison (`> N`): a client-side sample post-filter. ---
    // count_over_time per 60s bucket == 60 (one span/second).
    let kept = engine
        .metrics_range(&plan_for(
            engine,
            "{} | count_over_time() > 50",
            base_s(),
            end_s,
            60_000,
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
            60_000,
        ))
        .await
        .expect("result-comparison dropped");
    assert!(
        dropped.series.is_empty(),
        "60 > 100 drops every sample → no series"
    );
}

/// Isolated DB for the trace-wide-roots gate (issue #189 adjudication #1).
static DB_TW: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_traces_metrics_tw_it");

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
    run_init(&admin, &test_ctx(&DB_TW)).await.expect("run_init");

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
        CORPUS_SPANS * 1_000,
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
    run_init(&admin, &test_ctx(&DB)).await.expect("run_init");

    let client = data_client().await;
    seed_corpus(&client, &DB).await;
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
    // Issue #351: `{ true }` is EXACTLY the `{ }` match-all on the metrics
    // route — the corpus's canonical "match everything" filter, and the
    // reference's own rule (`pkg/traceql/ast_conditions.go:13-31` @ v3.0.2
    // appends a match-all condition when the filter body is a `Static`
    // whose `Bool()` is true). Asserted against the SAME independent count
    // as `{}` above, over real rows: a filter that merely stopped 400-ing
    // but selected the wrong spans fails here, where a plan-only check
    // could not see it. `{ false }` is its empty counterpart, and the
    // folded static comparisons are the same two constants.
    assert_identities(&engine, "{ true }", CORPUS_SPANS).await;
    assert_identities(&engine, r#"{ "x" = "x" }"#, CORPUS_SPANS).await;
    assert_identities(&engine, "{ false }", 0).await;
    assert_identities(&engine, r#"{ "x" = "y" }"#, 0).await;
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
        60_000,
    );
    let before_range = engine.metrics_range(&rate_plan).await.expect("range");
    let before_instant = engine.metrics_instant(&rate_plan).await.expect("instant");
    seed_corpus(&client, &DB).await; // every row now exists twice
    let after_range = engine.metrics_range(&rate_plan).await.expect("range dup");
    let after_instant = engine
        .metrics_instant(&rate_plan)
        .await
        .expect("instant dup");
    // The SAMPLES are what replay-dedup is about: `uniqExact(trace_id,
    // span_id)` collapses a replayed row, so every bucket's value is
    // invariant. The EXEMPLARS are not, and cannot be — `groupArraySample`
    // draws from raw rows and a replayed row is a real row, so a
    // duplicated corpus offers the sampler twice as many draws. That was
    // already true before issue #477; it is newly visible here only
    // because exemplars used to require a `with()` hint and now attach by
    // default. Asserting the whole result would be asserting that a
    // sampler is idempotent, which it is not and which no criterion asks
    // for (`traceql-metrics-exemplar-count-not-a-parity-surface`).
    /// A result's series stripped of their exemplars — the labels and the
    /// samples, which is what replay-dedup is a claim about.
    type LabelledSamples = Vec<(Vec<pulsus_read::MetricLabel>, Vec<(i64, f64)>)>;
    fn samples_of(r: &TraceMetricsResult) -> LabelledSamples {
        r.series
            .iter()
            .map(|s| (s.labels.clone(), s.samples.clone()))
            .collect()
    }
    assert_eq!(
        samples_of(&before_range),
        samples_of(&after_range),
        "at-least-once replays must never inflate a bucket (uniqExact dedup)"
    );
    assert_eq!(samples_of(&before_instant), samples_of(&after_instant));

    // ---- Unaligned window: outward snap, full-width edge buckets ------
    // [BASE+30, BASE+90) at step 60 snaps to [BASE, BASE+120): two
    // buckets covering 120 one-per-second spans.
    let plan = plan_for(
        &engine,
        "{} | count_over_time()",
        base_s() + 30,
        base_s() + 90,
        60_000,
    );
    assert_eq!(
        plan.snapped_window_ns(),
        (base_s() * NS, (base_s() + 120) * NS),
        "outward snap"
    );
    let points = matrix_points(&engine.metrics_range(&plan).await.expect("unaligned"));
    assert_eq!(
        points,
        vec![
            (base_s() * 1_000, 1.0),
            ((base_s() + 60) * 1_000, 60.0),
            ((base_s() + 120) * 1_000, 60.0)
        ],
        "three labels: the leading bucket `(aS-60, aS]` carrying the span at exactly `aS`, \
         then the two full-width intervals"
    );

    // ---- Exact right boundary: a span at an aligned `end` is excluded
    // (left-closed/right-open), never pulled into a clipped bucket. -----
    let plan = plan_for(
        &engine,
        "{} | count_over_time()",
        base_s(),
        base_s() + 60,
        60_000,
    );
    let points = matrix_points(&engine.metrics_range(&plan).await.expect("boundary"));
    assert_eq!(
        points,
        vec![(base_s() * 1_000, 1.0), ((base_s() + 60) * 1_000, 60.0)],
        "right-closed (issue #477 (b)): the span at exactly `aS` is the leading bucket's, and \
         the span at exactly `aE` belongs to the LAST label — spans at seconds 1..=60"
    );
    // …and with an UNALIGNED end inside the corpus, the raw-end span IS
    // included in the final snapped bucket.
    let plan = plan_for(
        &engine,
        "{} | count_over_time()",
        base_s(),
        base_s() + 30,
        60_000,
    );
    let points = matrix_points(&engine.metrics_range(&plan).await.expect("snap end"));
    assert_eq!(
        points,
        vec![(base_s() * 1_000, 1.0), ((base_s() + 60) * 1_000, 60.0)],
        "E snaps outward to BASE+60 — the documented over-inclusion, one full bucket plus \
         the leading one"
    );

    // ---- Empty window: range → empty matrix; instant → one "0" sample
    // (the documented empty-DB oracles). ---------------------------------
    let empty_start = base_s() - 86_400;
    let plan = plan_for(
        &engine,
        "{} | rate()",
        empty_start,
        empty_start + 600,
        60_000,
    );
    let empty_points = matrix_points(&engine.metrics_range(&plan).await.expect("empty range"));
    // Issue #477 (a): an ungrouped rate over a window with NO matching
    // data is one series carrying every bucket at zero — the sample-count
    // identity holds on the all-empty window, which is what says the fill
    // comes from the window and the step rather than from the rows.
    assert_eq!(empty_points.len(), plan.range_axis().points);
    assert_eq!(empty_points.len(), 11, "600 s / 60 s intervals, plus one");
    assert!(empty_points.iter().all(|(_, v)| *v == 0.0));
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
    create_extreme_epoch_view(&client, &DB).await;
    let extreme_engine = extreme_epoch_engine(data_client().await);

    let past_plan = plan_for(
        &extreme_engine,
        "{} | count_over_time()",
        EXTREME_PAST_S,
        EXTREME_PAST_S + 60,
        60_000,
    );
    let past_points = matrix_points(
        &extreme_engine
            .metrics_range(&past_plan)
            .await
            .expect("pre-1970 range"),
    );
    assert_eq!(
        past_points,
        vec![
            (EXTREME_PAST_S * 1_000, 1.0),
            ((EXTREME_PAST_S + 60) * 1_000, 0.0)
        ],
        "pre-1970 bucket labels must be the exact negative millisecond values, not wrapped"
    );

    let future_plan = plan_for(
        &extreme_engine,
        "{} | count_over_time()",
        EXTREME_FUTURE_S,
        EXTREME_FUTURE_S + 60,
        60_000,
    );
    let future_points = matrix_points(
        &extreme_engine
            .metrics_range(&future_plan)
            .await
            .expect("post-2106 range"),
    );
    assert_eq!(
        future_points,
        vec![
            (EXTREME_FUTURE_S * 1_000, 1.0),
            ((EXTREME_FUTURE_S + 60) * 1_000, 0.0)
        ],
        "post-2106 bucket labels must be the exact >UInt32-max millisecond values, not wrapped \
         mod 2^32"
    );
}

/// Isolated DB for the issue #237 ns→seconds ULP gate.
static DB_ULP: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_traces_metrics_ulp_it");

/// Issue #237 (Tier-1, scale-invariant): the ns→seconds conversion of a
/// duration value survives the whole SQL→decode path bit-exactly and
/// equals the value captured from the pinned reference container
/// (`grafana/tempo:3.0.2@sha256:cda87c21…`, 2026-07-26). Widths are
/// derived-first — `1500ms`/`2s` are exactly representable under both
/// rounding forms and prove nothing; the six `>2^53` widths additionally
/// prove ClickHouse's `toFloat64(Int64)` agrees bit-for-bit with the
/// Rust cast on lossy-cast inputs. Own throwaway database — the shared
/// `DB` corpus is uniformly `duration_ns = 1_000_000` and its
/// aggregation identities (`min == max == avg`) must not be perturbed.
#[tokio::test]
async fn duration_seconds_conversion_matches_the_reference() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see crates/pulsus-read/tests/traces_metrics_live.rs for setup)"
        );
        return;
    }

    // (ns, reference-captured seconds value). The first six are the
    // ≤16-digit ULP widths, the next six the 17-significant-digit
    // raw-wire discriminators, the last three exactly-representable
    // controls. Do NOT "fix" the expectations to the two-rounding form —
    // that would introduce the divergence #237 ruled out.
    const WIDTHS: &[(i64, f64)] = &[
        (1_118_000_000, 1.118),
        (1_122_000_000, 1.122),
        (1_128_000_000, 1.128),
        (1_235_000_000, 1.235),
        (31_952_000_000, 31.952),
        (1_000_064_438, 1.000064438),
        (18_014_398_509_482_025, 18_014_398.509_482_022),
        (18_014_398_509_482_035, 18_014_398.509_482_037),
        (18_014_398_509_482_017, 18_014_398.509_482_015),
        (1_088_608_058_291_172_412, 1_088_608_058.291_172_3),
        (10_000_000_000_000_005, 10_000_000.000_000_004),
        (10_000_000_000_000_015, 10_000_000.000_000_017),
        (500_000_000, 0.5),
        (1_500_000_000, 1.5),
        (2_000_000_000, 2.0),
    ];

    let admin = ChClient::new(test_config()).await.expect("connect");
    exec(&admin, &format!("DROP DATABASE IF EXISTS {DB_ULP}")).await;
    run_init(&admin, &test_ctx(&DB_ULP))
        .await
        .expect("run_init");

    let client = {
        let mut cfg = test_config();
        cfg.database = DB_ULP.to_string();
        ChClient::new(cfg).await.expect("connect data client")
    };

    // One single-span trace per width, one service each, starts inside
    // the aligned window (the window predicate filters on start only).
    let mut values: Vec<String> = Vec::new();
    for (i, (ns, _)) in WIDTHS.iter().enumerate() {
        let ts_ns = (base_s() + i as i64) * NS;
        values.push(format!(
            "(toFixedString(unhex('{:032x}'), 16), toFixedString(unhex('{:016x}'), 8), \
             toFixedString(unhex('0000000000000000'), 8), 'ulp-span', 'u{i}', '', {ts_ns}, \
             {ns}, 0, 1, 1, 'p')",
            i + 1,
            i + 1
        ));
    }
    exec(
        &client,
        &format!(
            "INSERT INTO {DB_ULP}.trace_spans \
             (trace_id, span_id, parent_id, name, service, status_message, timestamp_ns, \
              duration_ns, status_code, kind, payload_type, payload) VALUES {}",
            values.join(", ")
        ),
    )
    .await;

    let engine = TraceEngine::new(
        {
            let mut cfg = test_config();
            cfg.database = DB_ULP.to_string();
            ChClient::new(cfg).await.expect("connect engine")
        },
        engine_config(),
    );

    // Aligned single-bucket instant query per service: the decoded f64
    // must be bit-identical to the captured reference value.
    let end_s = base_s() + CORPUS_SPANS;
    for (i, (ns, want)) in WIDTHS.iter().enumerate() {
        let plan = plan_for(
            &engine,
            &format!(r#"{{ resource.service.name = "u{i}" }} | max_over_time(duration)"#),
            base_s(),
            end_s,
            CORPUS_SPANS * 1_000,
        );
        let got = vector_value(&engine.metrics_instant(&plan).await.expect("instant"));
        assert_eq!(
            got.to_bits(),
            want.to_bits(),
            "{ns} ns: SQL→decode must reproduce the reference's single-rounding f64 \
             (got {got}, want {want})"
        );
    }

    exec(&admin, &format!("DROP DATABASE IF EXISTS {DB_ULP}")).await;
}

// ---------------------------------------------------------------------------
// Issue #477: the bucket geometry, density, exemplar and cap-probe gates.
//
// Each test below seeds its OWN throwaway database and takes its window
// from `past_window`, so none of them perturbs the shared `DB` corpus and
// none of them depends on another's ordering.
// ---------------------------------------------------------------------------

static DB_GEOM: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_traces_metrics_geom_it");
static DB_DENSE: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_traces_metrics_dense_it");
/// The no-match gate's OWN database. Sharing `DB_DENSE` with
/// `density_follows_the_function_not_the_query` made the two tests race:
/// they run concurrently in one binary, and the second `run_init` hit
/// `TABLE_ALREADY_EXISTS` against the first's half-built schema. Nothing
/// in the tree checks `TestDb` base names for uniqueness — it is a
/// convention here, not a gate.
static DB_NOMATCH: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_traces_metrics_nomatch_it");
static DB_EX: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_traces_metrics_exemplar_it");
/// Issue #477 wave 2: the two grouped-exemplar criteria get a database
/// each, for the same reason `DB_NOMATCH` does — the tests in this binary
/// run concurrently and two `run_init`s against one name race.
static DB_GEX: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_traces_metrics_group_exemplar_it");
static DB_GSPARSE: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_traces_metrics_group_sparse_exemplar_it");
static DB_CAP: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_traces_metrics_cap_it");

/// Creates `db` fresh and returns an admin client plus an engine bound to
/// it. The admin client is the caller's to drop the database with.
async fn fresh_db(db: &str) -> (ChClient, TraceEngine) {
    let admin = ChClient::new(test_config()).await.expect("connect");
    exec(&admin, &format!("DROP DATABASE IF EXISTS {db}")).await;
    run_init(&admin, &test_ctx(db)).await.expect("run_init");
    let engine = TraceEngine::new(
        {
            let mut cfg = test_config();
            cfg.database = db.to_string();
            ChClient::new(cfg).await.expect("connect engine")
        },
        engine_config(),
    );
    (admin, engine)
}

/// One span row literal. `idx` seeds the trace/span ids, so distinct
/// indices are distinct logical spans and repeated ones are replays.
fn span_row(idx: i64, service: &str, ts_ns: i64, duration_ns: i64, status_code: i64) -> String {
    format!(
        "(toFixedString(unhex('{idx:032x}'), 16), toFixedString(unhex('{idx:016x}'), 8), \
         toFixedString(unhex('0000000000000000'), 8), 'op', '{service}', '', {ts_ns}, \
         {duration_ns}, {status_code}, 1, 1, 'p')"
    )
}

async fn insert_spans(admin: &ChClient, db: &str, rows: &[String]) {
    exec(
        admin,
        &format!(
            "INSERT INTO {db}.trace_spans \
             (trace_id, span_id, parent_id, name, service, status_message, timestamp_ns, \
              duration_ns, status_code, kind, payload_type, payload) VALUES {}",
            rows.join(", ")
        ),
    )
    .await;
}

/// AC2 (live) and AC6(ii): a span landing exactly on a grid point belongs
/// to THAT point, and a sub-second `step` is served with the sample count
/// the window and the step imply.
///
/// The pair is what discriminates. Two spans sit at exactly `L` and three
/// at `L + 1 ms`; under the right-closed reading they land on `L` and on
/// `L + step`, and under the pre-#477 left-edge reading **all five** land
/// on `L`. So `2` and `3` on adjacent labels is an answer the old
/// geometry cannot produce, where a bare sample count could not tell the
/// two apart.
#[tokio::test]
async fn spans_on_a_bucket_boundary_land_on_the_right_edge_label() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse");
        return;
    }
    let (admin, engine) = fresh_db(&DB_GEOM).await;
    // 500 ms divides every whole second, so a one-second-aligned window is
    // step-aligned here.
    let (start_s, end_s) = aligned_past_window(20, 1);
    let step_ms = 500;

    let plan = plan_for(&engine, "{} | count_over_time()", start_s, end_s, step_ms);
    let axis = plan.range_axis();
    // 20 s at 500 ms is 40 intervals, so 41 labels (AC6(ii)'s identity).
    assert_eq!(axis.points, 41, "20s / 500ms + 1");
    assert_eq!(axis.step_ms, 500);

    // A grid instant strictly inside the window, well away from both ends.
    let label_ms = axis.label_ms(20);
    let l_ns = label_ms * 1_000_000;
    let mut rows = Vec::new();
    for i in 0..2 {
        rows.push(span_row(1_000 + i, "bnd", l_ns, 1_000_000, 0));
    }
    for i in 0..3 {
        rows.push(span_row(2_000 + i, "bnd", l_ns + 1_000_000, 1_000_000, 0));
    }
    insert_spans(&admin, &DB_GEOM, &rows).await;

    let points = matrix_points(&engine.metrics_range(&plan).await.expect("range"));
    assert_eq!(points.len(), axis.points, "every bucket is emitted");
    let at = |ms: i64| {
        points
            .iter()
            .find(|(t, _)| *t == ms)
            .unwrap_or_else(|| panic!("no sample at {ms}: {points:?}"))
            .1
    };
    assert_eq!(
        at(label_ms),
        2.0,
        "the two spans AT the grid point stay on it"
    );
    assert_eq!(
        at(label_ms + 500),
        3.0,
        "the three spans one millisecond later belong to the NEXT label"
    );
    // And nothing leaked anywhere else.
    let total: f64 = points.iter().map(|(_, v)| v).sum();
    assert_eq!(total, 5.0, "{points:?}");

    exec(&admin, &format!("DROP DATABASE IF EXISTS {DB_GEOM}")).await;
}

/// AC3: a no-match range query returns a zero-filled series, with and
/// without an in-window control span.
///
/// The control span is load-bearing on the reference side and is stated
/// here so a test author knows which case they are constructing: the
/// reference zero-fills only when a block covers the window, and answers
/// an empty series list when nothing at all is stored. We answer the same
/// grid either way — a divergence this change introduces deliberately,
/// recorded as `traceql-metrics-zero-fill-without-a-block`.
#[tokio::test]
async fn a_no_match_range_query_returns_a_zero_filled_series() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse");
        return;
    }
    let (admin, engine) = fresh_db(&DB_NOMATCH).await;
    let (start_s, end_s) = aligned_past_window(20, 5);

    // A control span INSIDE the window that the queried predicate does
    // not match.
    let plan = plan_for(&engine, "{} | rate()", start_s, end_s, 5_000);
    let axis = plan.range_axis();
    insert_spans(
        &admin,
        &DB_NOMATCH,
        &[span_row(
            7_001,
            "ac3-control",
            axis.label_ms(2) * 1_000_000,
            1_000_000,
            0,
        )],
    )
    .await;

    let absent = plan_for(
        &engine,
        r#"{ resource.service.name = "ac3-absent" } | rate()"#,
        start_s,
        end_s,
        5_000,
    );
    let res = engine.metrics_range(&absent).await.expect("no-match range");
    assert_eq!(res.series.len(), 1, "one zero-filled series: {res:?}");
    assert_eq!(
        res.series[0].labels,
        vec![pulsus_read::MetricLabel::str("__name__", "rate")]
    );
    assert_eq!(res.series[0].samples.len(), axis.points);
    assert!(res.series[0].samples.iter().all(|(_, v)| *v == 0.0));
    assert!(
        res.series[0].exemplars.is_empty(),
        "an all-zero frame issues no exemplar query at all"
    );

    // Second clause: a window with NOTHING stored in it anywhere answers
    // the same grid. This is the divergence, not a regression guard.
    let (empty_start, empty_end) = (start_s - 86_400, end_s - 86_400);
    let empty = plan_for(&engine, "{} | rate()", empty_start, empty_end, 5_000);
    let res = engine.metrics_range(&empty).await.expect("empty range");
    assert_eq!(
        res.series.len(),
        1,
        "still one series with no block: {res:?}"
    );
    assert_eq!(res.series[0].samples.len(), empty.range_axis().points);
    assert!(res.series[0].samples.iter().all(|(_, v)| *v == 0.0));

    exec(&admin, &format!("DROP DATABASE IF EXISTS {DB_NOMATCH}")).await;
}

/// AC4: density follows the FUNCTION, not the query.
///
/// One corpus, one window, interior AND edge gaps. The dense half asserts
/// the whole `(label, value)` vector rather than only its length, so
/// rotating the values by one position fails it — a length check alone
/// would pass a frame whose samples sit on the wrong labels.
#[tokio::test]
async fn density_follows_the_function_not_the_query() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse");
        return;
    }
    let (admin, engine) = fresh_db(&DB_DENSE).await;
    let (start_s, end_s) = aligned_past_window(30, 5);
    let step_ms = 5_000;

    let probe = plan_for(&engine, "{} | count_over_time()", start_s, end_s, step_ms);
    let axis = probe.range_axis();
    assert_eq!(axis.points, 7, "30s / 5s + 1");

    // Two occupied buckets: label[2] and label[4]. Leading, trailing and
    // one interior bucket stay empty.
    let occupied = [axis.label_ms(2), axis.label_ms(4)];
    let rows = vec![
        span_row(
            9_001,
            "dense",
            occupied[0] * 1_000_000 - 1_000,
            1_000_000,
            0,
        ),
        span_row(
            9_002,
            "dense",
            occupied[1] * 1_000_000 - 1_000,
            1_000_000,
            2,
        ),
    ];
    insert_spans(&admin, &DB_DENSE, &rows).await;

    let expected_dense: Vec<(i64, f64)> = (0..axis.points)
        .map(|i| {
            let l = axis.label_ms(i);
            (l, if occupied.contains(&l) { 1.0 } else { 0.0 })
        })
        .collect();

    // ---- the dense half ------------------------------------------------
    let counts = matrix_points(
        &engine
            .metrics_range(&plan_for(
                &engine,
                "{} | count_over_time()",
                start_s,
                end_s,
                step_ms,
            ))
            .await
            .expect("count range"),
    );
    assert_eq!(
        counts, expected_dense,
        "count_over_time emits every bucket, values on their own labels"
    );

    for q in [
        "{} | rate()",
        "{} | quantile_over_time(duration, 0.9)",
        "{} | histogram_over_time(duration)",
        "{} | count_over_time() by(resource.service.name)",
        r#"{} | compare({ status = error })"#,
    ] {
        let res = engine
            .metrics_range(&plan_for(&engine, q, start_s, end_s, step_ms))
            .await
            .unwrap_or_else(|e| panic!("{q}: {e}"));
        assert!(!res.series.is_empty(), "{q}: expected series, got none");
        for s in &res.series {
            assert_eq!(
                s.samples.len(),
                axis.points,
                "{q}: every returned series must carry every bucket ({s:?})"
            );
            let labels: Vec<i64> = s.samples.iter().map(|(t, _)| *t).collect();
            let want: Vec<i64> = (0..axis.points).map(|i| axis.label_ms(i)).collect();
            assert_eq!(labels, want, "{q}: samples must sit on the axis labels");
        }
    }

    // ---- the sparse half: the value aggregations, grouped or not ------
    for q in [
        "{} | sum_over_time(duration)",
        "{} | min_over_time(duration)",
        "{} | avg_over_time(duration)",
        "{} | sum_over_time(duration) by(resource.service.name)",
    ] {
        let res = engine
            .metrics_range(&plan_for(&engine, q, start_s, end_s, step_ms))
            .await
            .unwrap_or_else(|e| panic!("{q}: {e}"));
        assert!(!res.series.is_empty(), "{q}: expected series, got none");
        for s in &res.series {
            assert_eq!(
                s.samples.len(),
                occupied.len(),
                "{q}: value aggregations stay SPARSE ({s:?})"
            );
            assert!(
                s.samples.len() < axis.points,
                "{q}: and strictly fewer than the axis"
            );
            let labels: Vec<i64> = s.samples.iter().map(|(t, _)| *t).collect();
            assert_eq!(labels, occupied.to_vec(), "{q}");
        }
    }

    exec(&admin, &format!("DROP DATABASE IF EXISTS {DB_DENSE}")).await;
}

/// AC5(iii)/(iv): exemplars attach with no parameter and no hint; the
/// budget is a TOTAL; the hint wins over the parameter; and every
/// exemplar's value is the sample at its own right-closed label.
#[tokio::test]
async fn exemplars_attach_by_default_and_map_to_the_right_edge_label() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse");
        return;
    }
    let (admin, engine) = fresh_db(&DB_EX).await;
    let (start_s, end_s) = aligned_past_window(35, 5);
    let step_ms = 5_000;

    let probe = plan_for(&engine, "{} | count_over_time()", start_s, end_s, step_ms);
    let axis = probe.range_axis();
    assert!(
        axis.points >= 8,
        "the grid must be wider than the budget set"
    );

    // Four occupied buckets with DISTINCT values 2, 3, 5, 1. Spans inside
    // one bucket share one nanosecond timestamp, so the bucket's value is
    // the span count and the exemplars all carry that instant.
    let mut rows = Vec::new();
    let mut idx = 20_000i64;
    for (slot, n) in [(1usize, 2), (2, 3), (3, 5), (4, 1)] {
        let ts_ns = axis.label_ms(slot) * 1_000_000 - 1_000;
        for _ in 0..n {
            rows.push(span_row(idx, "ex", ts_ns, 1_000_000, 0));
            idx += 1;
        }
    }
    insert_spans(&admin, &DB_EX, &rows).await;

    /// How many exemplars one `(parameter, hint)` combination returns.
    async fn count_exemplars(
        engine: &TraceEngine,
        start_s: i64,
        end_s: i64,
        step_ms: i64,
        exemplars: Option<u32>,
        hint: &str,
    ) -> usize {
        let q = format!("{{}} | count_over_time(){hint}");
        let plan = plan_with(engine, &q, start_s, end_s, step_ms, exemplars);
        let res = engine.metrics_range(&plan).await.expect("range");
        res.series.iter().map(|s| s.exemplars.len()).sum()
    }
    let count_exemplars = |exemplars: Option<u32>, hint: &'static str| {
        count_exemplars(&engine, start_s, end_s, step_ms, exemplars, hint)
    };

    // (iii) a plain query with NO parameter and NO hint carries exemplars.
    let plain = engine
        .metrics_range(&plan_for(
            &engine,
            "{} | count_over_time()",
            start_s,
            end_s,
            step_ms,
        ))
        .await
        .expect("plain range");
    let exemplars: Vec<&pulsus_read::MetricExemplar> =
        plain.series.iter().flat_map(|s| &s.exemplars).collect();
    assert!(
        !exemplars.is_empty(),
        "exemplars must attach with no parameter and no hint"
    );
    for ex in &exemplars {
        let trace = ex
            .labels
            .iter()
            .find(|l| l.key == "trace:id")
            .unwrap_or_else(|| panic!("every exemplar carries trace:id: {ex:?}"));
        match &trace.value {
            pulsus_read::MetricLabelValue::Str(hex) => {
                assert_eq!(hex.len(), 32, "16-byte hex trace id: {hex:?}");
                assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
            }
            other => panic!("trace:id must be a string, got {other:?}"),
        }
    }

    // (iv)-4 the mapping, on that same plain answer. The value is looked
    // up here from the exemplar's OWN timestamp through the axis, which
    // is a different route from the one the engine used (the SQL bucket
    // column), so agreeing is a real check.
    let samples: std::collections::BTreeMap<i64, f64> =
        plain.series[0].samples.iter().copied().collect();
    let mut seen: std::collections::BTreeSet<(i64, u64)> = std::collections::BTreeSet::new();
    for ex in &exemplars {
        let label = axis.label_for_ms(ex.timestamp_ms);
        let want = samples
            .get(&label)
            .copied()
            .unwrap_or_else(|| panic!("exemplar at {} has no sample at {label}", ex.timestamp_ms));
        assert_eq!(
            ex.value.to_bits(),
            want.to_bits(),
            "exemplar {} must carry its own bucket's value",
            ex.timestamp_ms
        );
        seen.insert((label, want.to_bits()));
    }
    let distinct_values: std::collections::BTreeSet<u64> = seen.iter().map(|(_, v)| *v).collect();
    assert!(
        seen.len() >= 2 && distinct_values.len() >= 2,
        "at least two exemplars in DIFFERENT buckets carrying DIFFERENT values: {seen:?}"
    );

    // (iv)-1 the bound, and (iv)-2 the unit.
    for budget in [1u32, 2, 4, 8, 100] {
        let n = count_exemplars(Some(budget), "").await;
        assert!(
            n <= budget as usize,
            "budget {budget} returned {n} exemplars"
        );
    }
    assert_eq!(
        count_exemplars(Some(1), "").await,
        1,
        "a TOTAL budget of 1 over four occupied buckets is one exemplar; per-bucket semantics \
         would return four"
    );

    // (iv)-3 precedence, as an equality that discriminates.
    for (h, p, hint) in [
        (1u32, 8u32, " with(exemplars=1)"),
        (8, 1, " with(exemplars=8)"),
        (2, 100, " with(exemplars=2)"),
        (100, 2, " with(exemplars=100)"),
    ] {
        let both = count_exemplars(Some(p), hint).await;
        let hint_alone = count_exemplars(None, hint).await;
        let param_alone = count_exemplars(Some(p), "").await;
        assert_eq!(
            both, hint_alone,
            "hint {h} must win over parameter {p} (got {both} vs {hint_alone})"
        );
        assert_ne!(
            param_alone, hint_alone,
            "hint {h} and parameter {p} must give DIFFERENT answers alone, or the equality \
             above proves nothing"
        );
    }
    // `false` turns them off outright, whatever the parameter says.
    assert_eq!(
        count_exemplars(Some(9), " with(exemplars=false)").await,
        0,
        "with(exemplars=false) wins over the parameter"
    );

    exec(&admin, &format!("DROP DATABASE IF EXISTS {DB_EX}")).await;
}

/// Issue #477 wave 2, ruling finding 1, criterion (a): **an exemplar is
/// attached to the series of the group it came from.**
///
/// The corpus makes ownership readable off the wire: a span's `trace_id`
/// is its `idx` in hex, and the two services take disjoint `idx` ranges,
/// so the service a returned `trace:id` belongs to is recoverable without
/// asking the database again. The two services also occupy DISJOINT
/// buckets, which is what makes the criterion discriminate — under the
/// pre-fix code every group's exemplars were attached to
/// `result.series.first_mut()`, so `gb`'s two traces appeared on `ga`'s
/// line, and they took `ga`'s value at those buckets, which after
/// densification is the `0.0` of a bucket where `ga` had nothing.
#[tokio::test]
async fn grouped_exemplars_attach_to_the_series_of_their_own_group() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse");
        return;
    }
    let (admin, engine) = fresh_db(&DB_GEX).await;
    let (start_s, end_s) = aligned_past_window(40, 5);
    let step_ms = 5_000;

    let probe = plan_for(
        &engine,
        "{} | count_over_time() by(resource.service.name)",
        start_s,
        end_s,
        step_ms,
    );
    let axis = probe.range_axis();
    assert!(axis.points >= 6, "the grid must hold both groups' buckets");

    // `ga` occupies slots 1 and 3, `gb` slots 2 and 4 — disjoint, so a
    // misattached exemplar lands on a bucket its host series has no data
    // in. Two spans per occupied bucket, so the values are non-zero and
    // the two groups' values are equal (2.0 everywhere) — the criterion
    // is ownership, and it must not be able to pass on the value alone.
    let mut rows = Vec::new();
    let mut owner: std::collections::BTreeMap<String, &'static str> =
        std::collections::BTreeMap::new();
    for (service, base_idx, slots) in [("ga", 40_000i64, [1usize, 3]), ("gb", 50_000, [2, 4])] {
        let mut idx = base_idx;
        for slot in slots {
            let ts_ns = axis.label_ms(slot) * 1_000_000 - 1_000;
            for _ in 0..2 {
                rows.push(span_row(idx, service, ts_ns, 1_000_000, 0));
                owner.insert(format!("{idx:032x}"), service);
                idx += 1;
            }
        }
    }
    insert_spans(&admin, &DB_GEX, &rows).await;

    let result = engine
        .metrics_range(&probe)
        .await
        .expect("the grouped range executes");
    assert_eq!(result.series.len(), 2, "two services: {result:?}");

    let mut seen_per_series: Vec<usize> = Vec::new();
    for series in &result.series {
        let service = service_label(series);
        let samples: std::collections::BTreeMap<i64, f64> =
            series.samples.iter().copied().collect();
        for ex in &series.exemplars {
            let trace = match &ex
                .labels
                .iter()
                .find(|l| l.key == "trace:id")
                .unwrap_or_else(|| panic!("every exemplar carries trace:id: {ex:?}"))
                .value
            {
                pulsus_read::MetricLabelValue::Str(hex) => hex.clone(),
                other => panic!("trace:id must be a string, got {other:?}"),
            };
            let from = owner
                .get(&trace)
                .unwrap_or_else(|| panic!("unknown trace id {trace} in {service}"));
            assert_eq!(
                *from, service,
                "exemplar {trace} came from {from} and was attached to {service}"
            );
            // …and it carries THAT series' value at its own bucket, which
            // on this corpus is 2.0 and never the 0.0 of a bucket the
            // other group filled.
            let label = axis.label_for_ms(ex.timestamp_ms);
            let want = samples.get(&label).copied().unwrap_or_else(|| {
                panic!(
                    "{service}: exemplar at {} has no sample at {label}",
                    ex.timestamp_ms
                )
            });
            assert_eq!(
                ex.value.to_bits(),
                want.to_bits(),
                "{service}: exemplar value"
            );
            assert_eq!(ex.value.to_bits(), 2.0f64.to_bits(), "{service}: {ex:?}");
        }
        seen_per_series.push(series.exemplars.len());
    }
    assert!(
        seen_per_series.iter().all(|n| *n > 0),
        "every group must carry its own exemplars, got {seen_per_series:?}"
    );

    exec(&admin, &format!("DROP DATABASE IF EXISTS {DB_GEX}")).await;
}

/// Issue #477 wave 2, ruling finding 1, criterion (b): **an exemplar's
/// value is never taken from a series that does not carry its bucket.**
///
/// `sum_over_time` is the shape that makes this visible. It is SPARSE —
/// `densifies()` is false for the value aggregations — so each group's
/// series carries only the buckets that group has data in. Under the
/// pre-fix code `gb`'s exemplars were attached to `ga`'s series at
/// buckets `ga` does not carry at all, and the bucket lookup's
/// `unwrap_or(0.0)` turned that miss into a `value:0.0` on the wire: a
/// number no span produced, indistinguishable from a measured zero.
#[tokio::test]
async fn a_grouped_sparse_aggregation_never_stamps_an_exemplar_from_a_missing_bucket() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse");
        return;
    }
    let (admin, engine) = fresh_db(&DB_GSPARSE).await;
    let (start_s, end_s) = aligned_past_window(40, 5);
    let step_ms = 5_000;

    let plan = plan_for(
        &engine,
        "{} | sum_over_time(duration) by(resource.service.name)",
        start_s,
        end_s,
        step_ms,
    );
    let axis = plan.range_axis();

    // Disjoint buckets again, and DISTINCT durations per group so the two
    // series' values can never coincide: `ga` sums to 4 ms per bucket,
    // `gb` to 12 ms — 0.004 s and 0.012 s after the ns→s conversion.
    let mut rows = Vec::new();
    for (service, base_idx, slots, duration_ns) in [
        ("ga", 60_000i64, [1usize, 3], 2_000_000i64),
        ("gb", 70_000, [2, 4], 6_000_000),
    ] {
        let mut idx = base_idx;
        for slot in slots {
            let ts_ns = axis.label_ms(slot) * 1_000_000 - 1_000;
            for _ in 0..2 {
                rows.push(span_row(idx, service, ts_ns, duration_ns, 0));
                idx += 1;
            }
        }
    }
    insert_spans(&admin, &DB_GSPARSE, &rows).await;

    let result = engine
        .metrics_range(&plan)
        .await
        .expect("the grouped aggregation executes");
    assert_eq!(result.series.len(), 2, "two services: {result:?}");

    let mut total_exemplars = 0usize;
    for series in &result.series {
        let service = service_label(series);
        let samples: std::collections::BTreeMap<i64, f64> =
            series.samples.iter().copied().collect();
        assert_eq!(
            samples.len(),
            2,
            "{service}: a value aggregation stays sparse — two occupied buckets, got {samples:?}"
        );
        for ex in &series.exemplars {
            let label = axis.label_for_ms(ex.timestamp_ms);
            let want = samples.get(&label).copied().unwrap_or_else(|| {
                panic!(
                    "{service}: exemplar at {} was stamped from bucket {label}, which this \
                     series does not carry — its buckets are {:?}",
                    ex.timestamp_ms,
                    samples.keys().collect::<Vec<_>>()
                )
            });
            assert_eq!(
                ex.value.to_bits(),
                want.to_bits(),
                "{service}: exemplar value at {label}"
            );
            assert_ne!(
                ex.value, 0.0,
                "{service}: a fabricated zero is what this criterion exists for: {ex:?}"
            );
            total_exemplars += 1;
        }
    }
    assert!(
        total_exemplars >= 2,
        "both groups must carry exemplars, got {total_exemplars}"
    );

    exec(&admin, &format!("DROP DATABASE IF EXISTS {DB_GSPARSE}")).await;
}

/// AC8(iv-b): the RANGE probe counts the groups the range answer can
/// contain, and the instant probe does not — so a group visible only to
/// the range window trips the cap while the instant route still serves.
///
/// Both pieces of the error set are covered: a group present only at
/// exactly `aE`, which the half-open instant window excludes, and one
/// present only in the leading step `(aS - step, aS)`, which it also
/// excludes. The instant control is what makes this an assertion about
/// TWO probes rather than about a cap — a single shared probe cannot
/// satisfy both halves.
#[tokio::test]
async fn the_range_probe_rejects_groups_outside_the_instant_window() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse");
        return;
    }
    let (admin, _) = fresh_db(&DB_CAP).await;
    let mut cfg = engine_config();
    cfg.max_series = 1;
    let capped = TraceEngine::new(
        {
            let mut c = test_config();
            c.database = DB_CAP.to_string();
            ChClient::new(c).await.expect("connect")
        },
        cfg,
    );

    // Two time-disjoint aligned windows, so the two corpora cannot see
    // each other.
    let (aligned, _) = aligned_past_window(600, 60);
    let step_ms = 60_000;

    // E: `a` inside [aS, aE); `b` ONLY at exactly aE.
    let e_start = aligned;
    let e_end = aligned + 120;
    // L: `c` inside; `d` ONLY in (aS - step, aS).
    let l_start = aligned + 300;
    let l_end = aligned + 420;

    insert_spans(
        &admin,
        &DB_CAP,
        &[
            span_row(30_001, "cap-a", (e_start + 30) * NS, 1_000_000, 0),
            span_row(30_002, "cap-b", e_end * NS, 1_000_000, 0),
            span_row(30_003, "cap-c", (l_start + 30) * NS, 1_000_000, 0),
            span_row(30_004, "cap-d", (l_start - 30) * NS, 1_000_000, 0),
        ],
    )
    .await;

    for (name, start_s, end_s, inside) in [
        ("E (a group at exactly aE)", e_start, e_end, "cap-a"),
        ("L (a group in the leading step)", l_start, l_end, "cap-c"),
    ] {
        let plan = plan_for(
            &capped,
            "{} | count_over_time() by(resource.service.name)",
            start_s,
            end_s,
            step_ms,
        );
        // The range window really does see the extra group and the
        // instant window really does not — stated as the two windows,
        // which is the only difference between the two probes.
        assert_eq!(
            plan.snapped_window_ns(),
            (start_s * NS, end_s * NS),
            "{name}"
        );
        assert_eq!(
            plan.range_window_ns(),
            ((start_s - 60) * NS + 1, end_s * NS + 1),
            "{name}"
        );

        match capped.metrics_range(&plan).await {
            Err(ReadError::QueryTooBroad(TooBroadReason::TraceMetricsSeriesCap { count, cap })) => {
                assert!(count > cap, "{name}: count {count} must exceed cap {cap}");
                assert_eq!(cap, 1, "{name}");
            }
            other => panic!("{name}: expected a series-cap rejection, got {other:?}"),
        }

        // Control, same plan: the instant route serves it, because the
        // instant probe sees one group.
        let instant = capped
            .metrics_instant(&plan)
            .await
            .unwrap_or_else(|e| panic!("{name}: the instant route must NOT reject: {e}"));
        assert_eq!(instant.series.len(), 1, "{name}: {instant:?}");
        assert_eq!(service_label(&instant.series[0]), inside, "{name}");
    }

    // AC13's live half: a millisecond step over thirty days resolves far
    // more intervals than the cap, and is refused before any SQL exists.
    let month = 30 * 86_400;
    let err = plan_trace_metrics(
        &pulsus_traceql::parse("{} | rate()").expect("parse"),
        &MetricsParams {
            start_ns: aligned * NS,
            end_ns: (aligned + month) * NS,
            step_ms: 1,
            exemplars: None,
        },
        &capped.metrics_ctx(),
    )
    .expect_err("a 1 ms step over 30 days is over the point cap");
    match err {
        pulsus_read::traces::filter::PlanError::MetricsPointCap { buckets, cap } => {
            assert_eq!(buckets, month * 1_000);
            assert_eq!(cap, 11_000);
        }
        other => panic!("expected MetricsPointCap, got {other:?}"),
    }

    exec(&admin, &format!("DROP DATABASE IF EXISTS {DB_CAP}")).await;
}

// ---------------------------------------------------------------------------
// AC14 (issue #477): every wall-clock read in THIS file is confined to two
// named owners.
//
// Why a `syn` parse and not a line scan. The first version of this gate
// matched `^(?:pub )?(?:async )?fn NAME` at column 0. Changing the real
// declaration to `pub(crate) fn base_s` made that pattern miss it, so a
// nested `fn base_s() {}` became the only declaration it could see, the
// count read 1, and the scanner returned GREEN with two breaks sitting in
// the file. A pattern that only knows the shapes someone thought of is the
// same defect one iteration later — the precedent is
// `crates/pulsus-read/tests/traces_regex_seal.rs:18-40`, whose own
// hand-rolled line scanner was replaced for exactly this reason. `syn` and
// `proc-macro2` are already dev-dependencies of this crate
// (`crates/pulsus-read/Cargo.toml`), with the `visit` feature on; no
// manifest change.
// ---------------------------------------------------------------------------

use proc_macro2::{TokenStream, TokenTree};
use syn::visit::Visit;

/// `(clock type, constructor)`. A read is a path in expression position
/// whose last segment is the constructor and whose owning type — the
/// preceding segment, or the qualified-self type for `<T>::now()` — is the
/// clock type; or the same two-token sequence inside a macro's tokens.
const CLOCKS: [(&str, &str); 3] = [("SystemTime", "now"), ("Instant", "now"), ("Utc", "now")];

/// The only two functions in this file that may read a clock.
///
/// `base_s` MUST read one: a committed corpus base ages past the schema's
/// delete TTL and turns the whole live suite into a silent pass.
/// `past_window` is the funnel this gate exists to enforce. The scanner
/// itself is NOT here — its `CLOCKS` entries are string literals, not
/// paths and not inside a macro, so it owns zero occurrences by
/// construction and needs no exemption. An exemption hides the cases it
/// admits; deleting it makes them visible.
const PERMITTED: [&str; 2] = ["base_s", "past_window"];

/// The owners the control fixture's three reads sit under, in needle
/// order.
const CONTROL_OWNERS: [&str; 3] = ["probe_a", "probe_b", "probe_c"];

/// The complete attribute set this file may carry.
///
/// `tokio::test` is the only attribute macro it spells; `test` is what the
/// plain `#[test]` below adds; `doc` is how the parser represents every
/// `///` and `//!` line, which is why it looks redundant and is not —
/// removing it reddens the gate on this file's own doc comments; `allow`
/// is a built-in inert lint attribute, included so that satisfying
/// `-D warnings` never forces a deviation. Anything else — a `derive`, a
/// `cfg_attr`, any other path — reddens the gate, because an attribute
/// macro REWRITES the item it decorates and that rewrite is invisible to
/// this parse. Widening this list is a deliberate decision, not a tidy-up.
const ATTR_ALLOW: [&str; 4] = ["test", "tokio::test", "allow", "doc"];

/// No `#[derive(...)]` here, deliberately: `derive` is outside
/// `ATTR_ALLOW` (a derive macro is an expansion this parse cannot see),
/// so assertion (6) reddens on it — including on this scanner's own
/// struct. The empty state is written out below instead.
struct ClockScan {
    /// The innermost-enclosing-function stack; `<module>` when empty.
    owners: Vec<String>,
    /// Every function declaration, in source order, over the four item
    /// types `syn` gives a `Signature` — so visibility,
    /// `const`/`async`/`unsafe`/`extern`, generics, attributes and nesting
    /// cannot hide one.
    decls: Vec<String>,
    /// `(needle, owner, how it was spelled)`; the third field is
    /// diagnostics only and never a decision.
    occs: Vec<(String, String, String)>,
    /// Fail-closed census: `macro_rules!` definitions, item-position macro
    /// invocations and `Verbatim` nodes — anything that can introduce a
    /// declaration or a read this parse cannot see.
    opaque: Vec<String>,
    /// Every attribute path in the file, joined with `::`.
    attrs: Vec<String>,
}

impl ClockScan {
    fn empty() -> Self {
        ClockScan {
            owners: Vec::new(),
            decls: Vec::new(),
            occs: Vec::new(),
            opaque: Vec::new(),
            attrs: Vec::new(),
        }
    }

    fn owner(&self) -> String {
        self.owners
            .last()
            .cloned()
            .unwrap_or_else(|| "<module>".to_string())
    }

    fn record(&mut self, needle: &str, spelling: String) {
        let owner = self.owner();
        self.occs.push((needle.to_string(), owner, spelling));
    }
}

/// Matches `Ident(clock) :: Ident(ctor)` anywhere in a token stream, then
/// recurses into every group. A clock spelling inside a string literal is
/// a `Literal` token and does not match; whitespace and formatting are
/// invisible to a token match.
fn scan_tokens(scan: &mut ClockScan, ts: &TokenStream, what: &str) {
    let flat: Vec<TokenTree> = ts.clone().into_iter().collect();
    for w in flat.windows(4) {
        let (TokenTree::Ident(a), TokenTree::Punct(c1), TokenTree::Punct(c2), TokenTree::Ident(b)) =
            (&w[0], &w[1], &w[2], &w[3])
        else {
            continue;
        };
        if c1.as_char() != ':' || c2.as_char() != ':' {
            continue;
        }
        let (ty, ctor) = (a.to_string(), b.to_string());
        if let Some((clock, _)) = CLOCKS.iter().find(|(t, c)| *t == ty && *c == ctor) {
            scan.record(clock, format!("[macro `{what}` tokens `{ty}::{ctor}`]"));
        }
    }
    for tt in flat {
        if let TokenTree::Group(g) = tt {
            scan_tokens(scan, &g.stream(), what);
        }
    }
}

impl<'ast> Visit<'ast> for ClockScan {
    fn visit_item_fn(&mut self, f: &'ast syn::ItemFn) {
        let name = f.sig.ident.to_string();
        self.decls.push(name.clone());
        self.owners.push(name);
        syn::visit::visit_item_fn(self, f);
        self.owners.pop();
    }

    fn visit_impl_item_fn(&mut self, f: &'ast syn::ImplItemFn) {
        let name = f.sig.ident.to_string();
        self.decls.push(name.clone());
        self.owners.push(name);
        syn::visit::visit_impl_item_fn(self, f);
        self.owners.pop();
    }

    fn visit_trait_item_fn(&mut self, f: &'ast syn::TraitItemFn) {
        let name = f.sig.ident.to_string();
        self.decls.push(name.clone());
        self.owners.push(name);
        syn::visit::visit_trait_item_fn(self, f);
        self.owners.pop();
    }

    fn visit_foreign_item_fn(&mut self, f: &'ast syn::ForeignItemFn) {
        // No body to descend into; it is still a declaration.
        self.decls.push(f.sig.ident.to_string());
        syn::visit::visit_foreign_item_fn(self, f);
    }

    fn visit_expr_path(&mut self, e: &'ast syn::ExprPath) {
        let segs: Vec<String> = e
            .path
            .segments
            .iter()
            .map(|s| s.ident.to_string())
            .collect();
        if let Some(ctor) = segs.last() {
            // `<T>::now()` carries the owning type in the qualified self,
            // where the path's own segments do not name it.
            let owning = match (&e.qself, segs.len()) {
                (Some(q), _) => match &*q.ty {
                    syn::Type::Path(tp) => tp.path.segments.last().map(|s| s.ident.to_string()),
                    _ => None,
                },
                (None, n) if n >= 2 => Some(segs[n - 2].clone()),
                _ => None,
            };
            if let Some((clock, _)) = owning
                .as_deref()
                .and_then(|o| CLOCKS.iter().find(|(t, c)| *t == o && *c == *ctor))
            {
                let spelling = format!("[path `{}`]", segs.join("::"));
                self.record(clock, spelling);
            }
        }
        syn::visit::visit_expr_path(self, e);
    }

    fn visit_macro(&mut self, m: &'ast syn::Macro) {
        let what = m
            .path
            .segments
            .last()
            .map(|s| s.ident.to_string())
            .unwrap_or_default();
        scan_tokens(self, &m.tokens, &what);
        syn::visit::visit_macro(self, m);
    }

    fn visit_attribute(&mut self, a: &'ast syn::Attribute) {
        let path = a
            .path()
            .segments
            .iter()
            .map(|s| s.ident.to_string())
            .collect::<Vec<_>>()
            .join("::");
        self.attrs.push(path);
        syn::visit::visit_attribute(self, a);
    }

    fn visit_item(&mut self, i: &'ast syn::Item) {
        match i {
            syn::Item::Macro(_) => self.opaque.push("Item::Macro".to_string()),
            syn::Item::Verbatim(_) => self.opaque.push("Item::Verbatim".to_string()),
            _ => {}
        }
        syn::visit::visit_item(self, i);
    }

    fn visit_impl_item(&mut self, i: &'ast syn::ImplItem) {
        match i {
            syn::ImplItem::Macro(_) => self.opaque.push("ImplItem::Macro".to_string()),
            syn::ImplItem::Verbatim(_) => self.opaque.push("ImplItem::Verbatim".to_string()),
            _ => {}
        }
        syn::visit::visit_impl_item(self, i);
    }

    fn visit_trait_item(&mut self, i: &'ast syn::TraitItem) {
        match i {
            syn::TraitItem::Macro(_) => self.opaque.push("TraitItem::Macro".to_string()),
            syn::TraitItem::Verbatim(_) => self.opaque.push("TraitItem::Verbatim".to_string()),
            _ => {}
        }
        syn::visit::visit_trait_item(self, i);
    }

    fn visit_expr(&mut self, e: &'ast syn::Expr) {
        if matches!(e, syn::Expr::Verbatim(_)) {
            self.opaque.push("Expr::Verbatim".to_string());
        }
        syn::visit::visit_expr(self, e);
    }
}

/// Parses `text` and runs the scan. A file that does not parse FAILS —
/// under a line scan an unparseable file scanned clean.
fn scan_clocks(text: &str, label: &str) -> ClockScan {
    let file = syn::parse_file(text).unwrap_or_else(|e| panic!("{label} must parse: {e}"));
    let mut scan = ClockScan::empty();
    scan.visit_file(&file);
    scan
}

/// AC14. Class PRESERVATION: it is GREEN on `2f78c53` (where the only
/// clock read is `base_s`'s) and stays green here, and what it forbids is
/// a clock read arriving anywhere else later.
#[test]
fn every_live_metrics_window_comes_from_the_past_window_helper() {
    const CONTROL: &str = include_str!("fixtures/ac14_clock_control.txt");
    const SRC: &str = include_str!("traces_metrics_live.rs");

    let src = scan_clocks(SRC, "traces_metrics_live.rs");

    // (1) every read has a permitted owner.
    let violations: Vec<&(String, String, String)> = src
        .occs
        .iter()
        .filter(|(_, owner, _)| !PERMITTED.contains(&owner.as_str()))
        .collect();
    assert!(
        violations.is_empty(),
        "clock read(s) outside the permitted helpers — every live metrics window must come from \
         `past_window`, and the corpus base from `base_s`:\n{}",
        violations
            .iter()
            .map(|(needle, owner, how)| format!("  {needle}::now owner={owner} {how}"))
            .collect::<Vec<_>>()
            .join("\n")
    );

    // (2) anti-vacuity: `base_s` owns at least one read, so the scan is
    // proved able to SEE a real clock site in this file.
    assert!(
        src.occs.iter().any(|(_, owner, _)| owner == "base_s"),
        "`base_s` owns no clock read — either the scan went blind, or the corpus base was \
         replaced by a committed epoch that will age past the delete TTL"
    );

    // (3) each permitted name is declared EXACTLY once, counted from the
    // same declaration list ownership is derived from — so there is no
    // second spelling to substitute, and a nested `fn base_s() {}` that
    // launders ownership is caught here.
    for name in PERMITTED {
        let n = src.decls.iter().filter(|d| *d == name).count();
        assert_eq!(
            n, 1,
            "permitted owner `{name}` is declared {n} times, expected exactly 1"
        );
    }

    // (4) needle liveness against the committed control: one hit per
    // needle, under its own probe, every one a violation. With the domain
    // file green by construction, this independent text is what says the
    // scan can still see anything at all.
    let control = scan_clocks(CONTROL, "fixtures/ac14_clock_control.txt");
    for (i, (clock, _)) in CLOCKS.iter().enumerate() {
        let hits: Vec<&(String, String, String)> =
            control.occs.iter().filter(|(n, _, _)| n == clock).collect();
        assert_eq!(
            hits.len(),
            1,
            "the control must carry exactly one {clock}::now, got {hits:?}"
        );
        assert_eq!(hits[0].1, CONTROL_OWNERS[i], "{clock}::now owner");
        assert!(
            !PERMITTED.contains(&hits[0].1.as_str()),
            "the control's owners must NOT be permitted, or it proves nothing"
        );
    }

    // (5) the fail-closed census: nothing in this file can introduce a
    // declaration or a read the parse cannot see.
    assert!(
        src.opaque.is_empty(),
        "opaque construct(s) in the domain file: {:?} — a `macro_rules!`, an item-position \
         macro invocation or a `Verbatim` node is refused because its expansion is invisible \
         to this gate, not because it is wrong",
        src.opaque
    );

    // (6) the attribute pin. An attribute macro rewrites the item it
    // decorates and that rewrite is invisible to this parse, so the set of
    // attribute paths is pinned rather than trusted.
    let unknown: Vec<&String> = src
        .attrs
        .iter()
        .filter(|a| !ATTR_ALLOW.contains(&a.as_str()))
        .collect();
    assert!(
        unknown.is_empty(),
        "attribute(s) outside the pinned set {ATTR_ALLOW:?}: {unknown:?} — an attribute macro \
         rewrites the item it decorates and that rewrite is invisible to this parse. Widening \
         `ATTR_ALLOW` is a deliberate decision, not a tidy-up."
    );
}
