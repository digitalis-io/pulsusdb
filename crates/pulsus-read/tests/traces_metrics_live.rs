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
use pulsus_read::traces::metrics_plan::{MetricsParams, plan_trace_metrics};
use pulsus_read::{QueryResult, TraceEngine, TraceMetricsPlan, TraceReadConfig};
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
/// `base_s()`: `checkout` every 5th span, `http.status_code = 500` every
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
             (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
              status_code, kind, payload_type, payload) \
             SELECT \
               toFixedString(unhex(leftPad(lower(hex(number)), 32, '0')), 16), \
               toFixedString(unhex(leftPad(lower(hex(number)), 16, '0')), 8), \
               toFixedString(unhex('0000000000000000'), 8), \
               'op', \
               if(number % 5 = 0, 'checkout', 'svc-x'), \
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

fn matrix_points(result: &QueryResult) -> Vec<(i64, f64)> {
    match result {
        QueryResult::Matrix(series) => {
            assert!(series.len() <= 1, "single-series M4 output: {series:?}");
            series.first().map(|s| s.points.clone()).unwrap_or_default()
        }
        other => panic!("expected a matrix, got {other:?}"),
    }
}

fn vector_value(result: &QueryResult) -> f64 {
    match result {
        QueryResult::Vector(samples) => {
            assert_eq!(samples.len(), 1, "one instant sample: {samples:?}");
            assert!(samples[0].labels.is_empty(), "label-less M4 output");
            samples[0].value
        }
        other => panic!("expected a vector, got {other:?}"),
    }
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
    assert_eq!(
        engine.metrics_range(&plan).await.expect("empty range"),
        QueryResult::Matrix(vec![])
    );
    let instant = engine.metrics_instant(&plan).await.expect("empty instant");
    assert_eq!(vector_value(&instant), 0.0);

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
