//! Differential test: rollup-vs-raw numeric parity for count-only range
//! aggregations (issue #12's headline AC). Live ClickHouse, gated behind
//! `PULSUS_TEST_CLICKHOUSE=1`, reusing `explain_indexes.rs`'s harness
//! (`should_run`/`test_config`/`test_ctx`/`run_init`) verbatim.
//!
//! **Forcing seam (task-manager resolution #2 on issue #12):** every case
//! below drives the *same* logical `(fingerprints, window, step)` query
//! through both physical shapes via the already-`pub` [`sql`] builders —
//! `MetricSource { log_metrics_5s, bucket_ns, ... }` vs `MetricSource {
//! log_samples, timestamp_ns, ... }` — never through a `RoutePreference`/
//! `ForceRaw` seam in the planner (there is none; `plan::metric_plan`
//! decides rollup-vs-raw on its own and this file never overrides it,
//! except to independently compute the raw-truth side of the comparison).
//!
//! **Window-bound alignment (architect plan edge case #1).** Rollup's
//! `WHERE bucket_ns > start AND bucket_ns <= end` filters at
//! resolution-bucket granularity (`bucket_ns`, already floored to 5s);
//! raw's `WHERE timestamp_ns > start AND timestamp_ns <= end` filters at
//! exact-sample granularity. Even with a resolution-aligned `start_ns`,
//! any sample landing in the half-open bucket immediately after `start_ns`
//! (`(start_ns, start_ns + res)`) would satisfy raw's exact-timestamp
//! filter but *not* rollup's bucket-floor filter (`bucket_ns == start_ns`
//! is not `> start_ns`) — a genuine boundary divergence, not a bug this
//! issue fixes (out of scope: M6 sliding-window parity). Every fixture
//! below therefore leaves one full resolution-bucket of *empty* buffer on
//! each side of the populated data, so that leading/trailing boundary
//! bucket contributes zero to both accounts and the comparison is
//! meaningful rather than accidental.
//!
//! Run locally:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test rollup_differential
//! podman rm -f pulsus-ch-test
//! ```

use std::collections::BTreeMap;
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_logql::{RangeAggOp, parse};
use pulsus_read::logql::rows::MetricBucketRow;
use pulsus_read::logql::sql::{self, TimeWindow};
use pulsus_read::logql::{
    Direction, EngineConfig, LogQlEngine, MatrixSeries, Plan, PlanCtx, QueryParams, QueryResult,
    QuerySpec, plan,
};
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
        database: std::env::var("PULSUS_TEST_CH_DATABASE")
            .unwrap_or_else(|_| "default".to_string()),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(20),
        ..ChConnConfig::default()
    }
}

/// The fixture's rollup resolution — 5s, matching `test_ctx`'s
/// `log_rollup` and every other M1 test's `rollup_res_ns` (schemas.md
/// §3.1's default).
const RES_NS: i64 = 5_000_000_000;

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

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-read/tests/rollup_differential.rs for setup)"
            );
            return;
        }
    };
}

async fn drop_database(client: &ChClient, db: &str) {
    client
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
}

/// Nanoseconds since the Unix epoch, right now, floored to [`RES_NS`] —
/// fixture timestamps must be wall-clock-recent (`log_samples`'s
/// `ttl_only_drop_parts = 1` retention makes an already-expired part
/// eligible for near-immediate background deletion, per
/// `explain_indexes.rs`'s `now_ns()` doc comment) *and* resolution-aligned
/// (the window-bound alignment invariant above).
fn aligned_base_ns() -> i64 {
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64");
    (now / RES_NS) * RES_NS
}

// Two fixture streams sharing one service so both raw fallback's
// `PREWHERE service = 'checkout'` and the rollup path's fingerprint-only
// filter cover both: `FP_A` is matched by `{env="prod"}` (used for the
// end-to-end `LogQlEngine::query` case below), `FP_B` only by
// `{env="staging"}` (kept out of that selector so the end-to-end matrix
// has exactly one series, while still exercising multi-fingerprint
// `GROUP BY fingerprint, step` parity in the direct SQL-builder cases).
const FP_A: u64 = 1_837_410_000_000_000_001;
const FP_B: u64 = 1_837_420_000_000_000_002;

/// How many resolution buckets carry fixture data (buckets `1..=15`,
/// relative to `base_ns`'s bucket `0`, which is left empty as the leading
/// alignment buffer — see the module doc comment).
const NUM_DATA_BUCKETS: i64 = 15;

/// Two samples per fingerprint per data bucket, at different byte lengths
/// so `sum(bytes)`/`sum(length(body))` diverges meaningfully from a plain
/// row count.
const BODIES: [&str; 2] = [
    "short",
    "a somewhat longer log line for byte-counting purposes",
];

async fn seed_streams(client: &ChClient, db: &str, base_ns: i64) {
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, updated_ns) VALUES \
                 (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({base_ns}))), {FP_A}, 'checkout', \
                 '{{\"env\":\"prod\",\"service_name\":\"checkout\"}}', 0), \
                 (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({base_ns}))), {FP_B}, 'checkout', \
                 '{{\"env\":\"staging\",\"service_name\":\"checkout\"}}', 0)"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_streams");
}

/// Inserts `log_samples` rows spanning [`NUM_DATA_BUCKETS`] resolution
/// buckets (bucket indices `1..=NUM_DATA_BUCKETS`, relative to `base_ns`)
/// for both fixture fingerprints. `log_metrics_5s_mv` (schemas.md §3.1)
/// materializes `log_metrics_5s` synchronously as part of this same
/// `INSERT` — no direct rollup-table write, no wait needed before querying
/// the rollup side.
async fn seed_samples(client: &ChClient, db: &str, base_ns: i64) {
    let mut values = Vec::new();
    for fp in [FP_A, FP_B] {
        for bucket in 1..=NUM_DATA_BUCKETS {
            let bucket_start = base_ns + bucket * RES_NS;
            for (offset_ns, body) in [(1_000_000_000i64, BODIES[0]), (3_000_000_000i64, BODIES[1])]
            {
                let ts = bucket_start + offset_ns;
                values.push(format!("('checkout', {fp}, {ts}, 0, '{body}')"));
            }
        }
    }
    let sql = format!(
        "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, body) VALUES {}",
        values.join(", ")
    );
    client
        .execute(&sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .expect("seed log_samples");
}

/// Sets up a fresh database, seeds both fixture streams and their samples,
/// and returns a client bound directly to that database plus the
/// resolution-aligned `base_ns` the fixture data is relative to.
async fn setup(db: &str) -> (ChClient, i64) {
    let client = ChClient::new(test_config()).await.expect("connect");
    drop_database(&client, db).await;
    run_init(&client, &test_ctx(db)).await.expect("run_init");

    let mut data_cfg = test_config();
    data_cfg.database = db.to_string();
    let data_client = ChClient::new(data_cfg)
        .await
        .expect("connect (data client)");

    let base_ns = aligned_base_ns();
    seed_streams(&data_client, db, base_ns).await;
    seed_samples(&data_client, db, base_ns).await;
    (data_client, base_ns)
}

/// A window covering every data bucket plus one full resolution bucket of
/// empty buffer on each side (the alignment invariant the module doc
/// comment explains).
fn window(base_ns: i64) -> TimeWindow {
    TimeWindow {
        start_ns: base_ns,
        end_ns: base_ns + (NUM_DATA_BUCKETS + 2) * RES_NS,
    }
}

fn range_params(base_ns: i64, step_ns: u64) -> QueryParams {
    let w = window(base_ns);
    QueryParams {
        spec: QuerySpec::Range {
            start_ns: w.start_ns,
            end_ns: w.end_ns,
            step_ns,
        },
        limit: 100,
        direction: Direction::Backward,
    }
}

fn plan_ctx(db: &str) -> PlanCtx<'_> {
    PlanCtx {
        db,
        streams_idx: "log_streams_idx",
        streams: "log_streams",
        samples: "log_samples",
        rollup_table: "log_metrics_5s",
        rollup_res_ns: RES_NS as u64,
        scan_budget_bytes: 50 * 1024 * 1024 * 1024,
        max_streams: 100_000,
        pipeline_scan_factor: 10,
    }
}

fn metric_plan(query: &str, params: &QueryParams, db: &str) -> pulsus_read::logql::MetricPlan {
    let expr = parse(query).expect("parse");
    match plan(&expr, params, &plan_ctx(db)).expect("plan") {
        Plan::Metric(mp) => mp,
        Plan::Streams(_) | Plan::MetricBinary(_) => panic!("expected a Metric plan"),
    }
}

/// Runs `sql` and collects it into a `(fingerprint, step) -> n` map.
/// Doubles literal `?`s exactly as `LogQlEngine::query_stream` does
/// internally (`exec.rs::escape_query_placeholders`) — this file calls
/// `ChClient` directly, bypassing that wrapper.
async fn query_bucket_map(client: &ChClient, sql: &str) -> BTreeMap<(u64, i64), u64> {
    let full = sql.replace('?', "??");
    let mut stream = client
        .query_stream::<MetricBucketRow>(&full, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("metric query failed: {e}\nSQL:\n{full}"));
    let mut out = BTreeMap::new();
    while let Some(row) = stream.next().await {
        let row = row.expect("decode metric bucket row");
        out.insert((row.fingerprint, row.step), row.n);
    }
    out
}

/// Plans `query` (asserting it is rollup-eligible — every case below uses a
/// step that is a multiple of the fixture's 5s resolution, so an
/// eligibility regression would itself fail this assertion), then runs the
/// *same* `(fingerprints, window, step)` shape through both the rollup and
/// raw [`sql`] builders and asserts the resulting `(fingerprint, step) ->
/// n` maps are byte-for-byte identical.
async fn assert_rollup_matches_raw(
    client: &ChClient,
    db: &str,
    query: &str,
    params: &QueryParams,
    fps: &[u64],
) -> BTreeMap<(u64, i64), u64> {
    let mp = metric_plan(query, params, db);
    assert!(mp.rollup, "fixture query must be rollup-eligible: {query}");
    let step_ns = mp.step_ns.expect("range spec");
    let w = TimeWindow {
        start_ns: mp.start_ns,
        end_ns: mp.end_ns,
    };

    let rollup_table = format!("{db}.{}", mp.table);
    let rollup_source = sql::MetricSource {
        table: &rollup_table,
        bucket_col: mp.bucket_col,
        agg_expr: mp.agg_expr,
    };
    let is_bytes = matches!(mp.op, RangeAggOp::BytesRate | RangeAggOp::BytesOverTime);
    let raw_table = format!("{db}.log_samples");
    let raw_source = sql::MetricSource {
        table: &raw_table,
        bucket_col: "timestamp_ns",
        agg_expr: if is_bytes {
            "sum(length(body))"
        } else {
            "count()"
        },
    };

    let rollup_sql = sql::metric_range(rollup_source, &[], fps, w, step_ns, &mp.extra_predicates);
    let raw_sql = sql::metric_range(
        raw_source,
        &["'checkout'".to_string()],
        fps,
        w,
        step_ns,
        &mp.extra_predicates,
    );

    let rollup_map = query_bucket_map(client, &rollup_sql).await;
    let raw_map = query_bucket_map(client, &raw_sql).await;
    assert!(
        !raw_map.is_empty(),
        "fixture produced no samples for {query} — comparison would trivially pass"
    );
    assert_eq!(
        rollup_map, raw_map,
        "rollup vs raw diverged for {query}\nrollup sql:\n{rollup_sql}\nraw sql:\n{raw_sql}"
    );
    raw_map
}

// ---------------------------------------------------------------------
// Direct sql-builder comparisons — one per M1 range-aggregation op.
// ---------------------------------------------------------------------

#[tokio::test]
async fn count_over_time_rollup_matches_raw_at_step_equal_to_resolution() {
    skip_unless_live!();
    let db = "pulsus_read_it_diff_cot_step_res";
    let (client, base_ns) = setup(db).await;
    let params = range_params(base_ns, RES_NS as u64);
    assert_rollup_matches_raw(
        &client,
        db,
        r#"count_over_time({service_name="checkout"}[1m])"#,
        &params,
        &[FP_A, FP_B],
    )
    .await;
}

#[tokio::test]
async fn count_over_time_rollup_matches_raw_at_step_twelve_times_resolution() {
    skip_unless_live!();
    let db = "pulsus_read_it_diff_cot_step_12x";
    let (client, base_ns) = setup(db).await;
    let params = range_params(base_ns, 12 * RES_NS as u64);
    assert_rollup_matches_raw(
        &client,
        db,
        r#"count_over_time({service_name="checkout"}[1m])"#,
        &params,
        &[FP_A, FP_B],
    )
    .await;
}

#[tokio::test]
async fn rate_numerator_rollup_matches_raw() {
    skip_unless_live!();
    let db = "pulsus_read_it_diff_rate";
    let (client, base_ns) = setup(db).await;
    let params = range_params(base_ns, 12 * RES_NS as u64);
    // `rate`'s SQL shape is identical to `count_over_time`'s — the only
    // difference is the `/ window_seconds` division `LogQlEngine` applies
    // afterward in Rust (`exec::apply_rate`), identically on both routes.
    // Comparing the pre-division `n` here is therefore exact coverage for
    // `rate` too, not just `count_over_time`.
    assert_rollup_matches_raw(
        &client,
        db,
        r#"rate({service_name="checkout"}[1m])"#,
        &params,
        &[FP_A, FP_B],
    )
    .await;
}

#[tokio::test]
async fn bytes_over_time_rollup_matches_raw() {
    skip_unless_live!();
    let db = "pulsus_read_it_diff_bot";
    let (client, base_ns) = setup(db).await;
    let params = range_params(base_ns, 12 * RES_NS as u64);
    let raw_map = assert_rollup_matches_raw(
        &client,
        db,
        r#"bytes_over_time({service_name="checkout"}[1m])"#,
        &params,
        &[FP_A, FP_B],
    )
    .await;
    // Sanity: bytes must differ from a plain row count (two bodies of
    // different lengths per bucket) — otherwise this test could pass
    // vacuously even if `sql::metric_range`'s `is_bytes` branch were
    // accidentally wired to the count column on both sides.
    let cot_params = range_params(base_ns, 12 * RES_NS as u64);
    let count_map = assert_rollup_matches_raw(
        &client,
        db,
        r#"count_over_time({service_name="checkout"}[1m])"#,
        &cot_params,
        &[FP_A, FP_B],
    )
    .await;
    assert_ne!(raw_map, count_map, "bytes and row counts must differ");
}

#[tokio::test]
async fn bytes_rate_numerator_rollup_matches_raw() {
    skip_unless_live!();
    let db = "pulsus_read_it_diff_bytes_rate";
    let (client, base_ns) = setup(db).await;
    let params = range_params(base_ns, 12 * RES_NS as u64);
    assert_rollup_matches_raw(
        &client,
        db,
        r#"bytes_rate({service_name="checkout"}[1m])"#,
        &params,
        &[FP_A, FP_B],
    )
    .await;
}

// ---------------------------------------------------------------------
// End-to-end: `LogQlEngine::query` on the auto-routed rollup path vs
// independently-computed raw truth.
// ---------------------------------------------------------------------

#[tokio::test]
async fn engine_query_on_the_rollup_path_matches_independently_computed_raw_counts() {
    skip_unless_live!();
    let db = "pulsus_read_it_diff_engine";
    let (client, base_ns) = setup(db).await;
    let w = window(base_ns);
    let step_ns = 12 * RES_NS as u64;

    // Independently compute raw truth for `FP_A` alone (the selector below
    // matches only `FP_A`'s `env="prod"` label) via the raw `sql` builder
    // directly — this is the ground truth the engine's rollup-served
    // answer must match exactly.
    let raw_table = format!("{db}.log_samples");
    let raw_source = sql::MetricSource {
        table: &raw_table,
        bucket_col: "timestamp_ns",
        agg_expr: "count()",
    };
    let raw_sql = sql::metric_range(
        raw_source,
        &["'checkout'".to_string()],
        &[FP_A],
        w,
        step_ns,
        &[],
    );
    let raw_map = query_bucket_map(&client, &raw_sql).await;
    assert!(!raw_map.is_empty(), "fixture produced no raw truth data");
    let expected: BTreeMap<i64, f64> = raw_map
        .into_iter()
        .map(|((_fp, step), n)| (step, n as f64))
        .collect();

    let engine_cfg = ChConnConfig {
        database: db.to_string(),
        ..test_config()
    };
    let engine_client = ChClient::new(engine_cfg)
        .await
        .expect("connect (engine client)");
    let engine = LogQlEngine::new(
        engine_client,
        EngineConfig {
            db: db.to_string(),
            streams_idx: "log_streams_idx".to_string(),
            streams: "log_streams".to_string(),
            samples: "log_samples".to_string(),
            rollup_table: "log_metrics_5s".to_string(),
            rollup_res_ns: RES_NS as u64,
            scan_budget_bytes: 50 * 1024 * 1024 * 1024,
            max_streams: 100_000,
            pipeline_scan_factor: 10,
        },
    );

    let expr = parse(r#"count_over_time({env="prod"}[1m])"#).expect("parse");
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: w.start_ns,
            end_ns: w.end_ns,
            step_ns,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let result = engine.query(&expr, &params).await.expect("engine query");
    let QueryResult::Matrix(series) = result else {
        panic!("expected a Matrix result");
    };
    assert_eq!(
        series.len(),
        1,
        "the {{env=\"prod\"}} selector matches exactly one fixture stream"
    );
    let MatrixSeries { points, .. } = &series[0];
    let actual: BTreeMap<i64, f64> = points.iter().copied().collect();
    assert_eq!(actual, expected);
}

// ---------------------------------------------------------------------
// Issue M6-10 end-to-end: `LogQlEngine::query` on the CLIENT-AGGREGATED
// path (a beyond-line-filter pipeline) against a live ClickHouse — the
// full-window `metric_raw_samples` fetch executes, the in-engine
// pipeline/bucket/reduce runs, and for a count query the result matches
// the SQL-aggregated answer for the identical filter, computed
// independently through the raw `sql` builder.
// ---------------------------------------------------------------------

#[tokio::test]
async fn engine_query_on_the_client_agg_path_matches_the_sql_aggregated_count() {
    skip_unless_live!();
    let db = "pulsus_read_it_diff_client_agg";
    let (client, base_ns) = setup(db).await;
    let w = window(base_ns);
    let step_ns = 12 * RES_NS as u64;

    // Independent SQL-aggregated truth: count of `FP_A` rows whose body
    // contains "longer", bucketed by step — the same predicate the
    // engine's pushed-down line filter renders.
    let raw_table = format!("{db}.log_samples");
    let raw_sql = sql::metric_range(
        sql::MetricSource {
            table: &raw_table,
            bucket_col: "timestamp_ns",
            agg_expr: "count()",
        },
        &["'checkout'".to_string()],
        &[FP_A],
        w,
        step_ns,
        &["position(body, 'longer') > 0".to_string()],
    );
    let raw_map = query_bucket_map(&client, &raw_sql).await;
    assert!(!raw_map.is_empty(), "fixture produced no raw truth data");
    let expected: BTreeMap<i64, f64> = raw_map
        .into_iter()
        .map(|((_fp, step), n)| (step, n as f64))
        .collect();

    let engine_cfg = ChConnConfig {
        database: db.to_string(),
        ..test_config()
    };
    let engine_client = ChClient::new(engine_cfg)
        .await
        .expect("connect (engine client)");
    let engine = LogQlEngine::new(
        engine_client,
        EngineConfig {
            db: db.to_string(),
            streams_idx: "log_streams_idx".to_string(),
            streams: "log_streams".to_string(),
            samples: "log_samples".to_string(),
            rollup_table: "log_metrics_5s".to_string(),
            rollup_res_ns: RES_NS as u64,
            scan_budget_bytes: 50 * 1024 * 1024 * 1024,
            max_streams: 100_000,
            pipeline_scan_factor: 10,
        },
    );

    // The base-label filter `env = "prod"` is a beyond-line-filter stage:
    // it forces the client-aggregated mode (asserted below) without
    // changing which rows survive — so the in-engine count must equal
    // the SQL-aggregated truth exactly.
    let query = r#"count_over_time({env="prod"} |= "longer" | env = "prod" [1m])"#;
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: w.start_ns,
            end_ns: w.end_ns,
            step_ns,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let mp = metric_plan(query, &params, db);
    assert!(
        mp.client.is_some(),
        "the label filter must force client aggregation"
    );

    let expr = parse(query).expect("parse");
    let result = engine.query(&expr, &params).await.expect("engine query");
    let QueryResult::Matrix(series) = result else {
        panic!("expected a Matrix result");
    };
    assert_eq!(series.len(), 1, "one fixture stream matches");
    let actual: BTreeMap<i64, f64> = series[0].points.iter().copied().collect();
    assert_eq!(
        actual, expected,
        "client-aggregated count must equal the SQL-aggregated count for the identical filter"
    );
}

/// Issue M6-10 review round 1, gap (a): the client-aggregated raw scan
/// carries no `LIMIT`, so the complete-or-error contract rests entirely
/// on `max_bytes_to_read` — this test BREACHES that budget for real and
/// asserts the named `QueryTooBroad(ScanBudgetBytes)` error (never a
/// truncated aggregate, never an unmapped 307).
#[tokio::test]
async fn engine_client_agg_scan_past_the_byte_budget_is_a_named_query_too_broad() {
    skip_unless_live!();
    let db = "pulsus_read_it_diff_budget";
    let (client, base_ns) = setup(db).await;
    // Bulk-seed enough body bytes that the samples scan must read far
    // past the budget below, while the stage-1/stage-2 index reads (a
    // handful of tiny rows) stay well under it.
    let filler = "x".repeat(200);
    let mut values = Vec::new();
    for i in 0..4_000i64 {
        let ts = base_ns + RES_NS + i * 1_000_000; // 1ms apart inside the window
        values.push(format!(
            "('checkout', {FP_A}, {ts}, 0, 'bulk {i} {filler}')"
        ));
    }
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, \
                 body) VALUES {}",
                values.join(", ")
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("bulk seed");

    let engine_cfg = ChConnConfig {
        database: db.to_string(),
        ..test_config()
    };
    let engine_client = ChClient::new(engine_cfg)
        .await
        .expect("connect (engine client)");
    const TIGHT_BUDGET: u64 = 64 * 1024;
    let engine = LogQlEngine::new(
        engine_client,
        EngineConfig {
            db: db.to_string(),
            streams_idx: "log_streams_idx".to_string(),
            streams: "log_streams".to_string(),
            samples: "log_samples".to_string(),
            rollup_table: "log_metrics_5s".to_string(),
            rollup_res_ns: RES_NS as u64,
            scan_budget_bytes: TIGHT_BUDGET,
            max_streams: 100_000,
            pipeline_scan_factor: 10,
        },
    );

    let w = window(base_ns);
    let expr = parse(r#"count_over_time({env="prod"} | env = "prod" [1m])"#).expect("parse");
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: w.start_ns,
            end_ns: w.end_ns,
            step_ns: 12 * RES_NS as u64,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let err = engine
        .query(&expr, &params)
        .await
        .expect_err("the raw scan must breach the byte budget");
    match err {
        pulsus_read::logql::ReadError::QueryTooBroad(
            pulsus_read::logql::TooBroadReason::ScanBudgetBytes { budget_bytes, .. },
        ) => assert_eq!(budget_bytes, TIGHT_BUDGET),
        other => panic!("expected QueryTooBroad(ScanBudgetBytes), got {other:?}"),
    }
}
