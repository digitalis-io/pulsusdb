//! Issue #136: the metrics `SqlFallback` fetch's double-distributed
//! `fingerprint IN (SELECT … FROM metric_series*_dist …)` shape against a
//! real 2-shard cluster.
//!
//! Setup mirrors `pulsus-schema/tests/live_cluster.rs` (clustered
//! `run_init`, dedicated per-test database, per-shard IP/port env
//! overrides) and `live_metrics_engine.rs` (`MetricsEngine`/`LabelCache`
//! construction, the historical/out-of-window seeding pattern that forces
//! the `SqlFallback` path). Gated behind `PULSUS_TEST_CLICKHOUSE=1` and
//! requires the 2-shard fixture specifically:
//!
//! ```text
//! docker compose -f ci/clickhouse-cluster/compose.yaml up -d
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test live_metrics_cluster_fallback
//! docker compose -f ci/clickhouse-cluster/compose.yaml down -v
//! ```

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{
    ChClient, ChConnConfig, ChError, ChProto, Idempotency, QuerySettings, Row,
};
use pulsus_model::DEFAULT_ACTIVITY_BUCKET_MS;
use pulsus_promql::parser::parse;
use pulsus_read::metrics::sample_rows::SampleRow;
use pulsus_read::metrics::sample_sql::sample_fetch_subquery;
use pulsus_read::metrics::sql::historical_series_subquery;
use pulsus_read::{
    DataWindow, ExplainStage, LabelCache, LabelCacheConfig, MetricQueryParams, MetricsConfig,
    MetricsEngine, PlanExplain, QueryResult,
};
use pulsus_schema::{RenderCtx, run_init};

const CLUSTER_NAME: &str = "pulsus_test_cluster";

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with the 2-shard cluster fixture up to \
                 run this test (see crates/pulsus-read/tests/live_metrics_cluster_fallback.rs \
                 for setup)"
            );
            return;
        }
    };
}

/// Same fixture-static-IP convention as `pulsus-schema/tests/live_cluster.rs`
/// (KISS directive on issue #5 — dial each shard directly by IP, never a
/// name); overridable via the identical env var names for a runtime where
/// the host cannot route to the compose network directly.
fn shard_config(
    host_env: &str,
    default_host: &str,
    port_env: &str,
    default_port: u16,
    database: &str,
) -> ChConnConfig {
    ChConnConfig {
        server: std::env::var(host_env).unwrap_or_else(|_| default_host.to_string()),
        http_port: std::env::var(port_env)
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(default_port),
        database: database.to_string(),
        proto: ChProto::Http,
        pool_size: 8,
        query_timeout: Duration::from_secs(30),
        ..ChConnConfig::default()
    }
}

fn shard1_config(database: &str) -> ChConnConfig {
    shard_config(
        "PULSUS_TEST_CH_SHARD1_HOST",
        "172.28.0.11",
        "PULSUS_TEST_CH_SHARD1_HTTP_PORT",
        8123,
        database,
    )
}

fn shard2_config(database: &str) -> ChConnConfig {
    shard_config(
        "PULSUS_TEST_CH_SHARD2_HOST",
        "172.28.0.12",
        "PULSUS_TEST_CH_SHARD2_HTTP_PORT",
        8123,
        database,
    )
}

fn cluster_ctx(db: &str) -> RenderCtx {
    RenderCtx {
        db: db.to_string(),
        cluster: Some(CLUSTER_NAME.to_string()),
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
    }
}

async fn drop_database(client: &ChClient, db: &str) {
    client
        .execute(
            &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database on cluster");
}

/// Connects to shard1 and runs clustered `run_init` (mirrors
/// `pulsus-schema/tests/live_cluster.rs`'s own setup) — every DDL statement
/// lands on both shards via `ON CLUSTER`.
async fn init_clustered_db(db: &str) -> ChClient {
    let shard1 = ChClient::new(shard1_config("default"))
        .await
        .expect("connect shard1 (bootstrap)");
    drop_database(&shard1, db).await;
    run_init(&shard1, &cluster_ctx(db))
        .await
        .expect("run_init (clustered)");
    shard1
}

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("now fits in i64")
}

/// 2 days ago, floored to the activity bucket — comfortably outside every
/// test's 24h label-cache window (forces `SqlFallback`) and safely inside
/// the schema's 7-day raw retention TTL.
fn historical_bucket() -> i64 {
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let two_days_ms = 2 * 24 * 3_600_000;
    ((now_ms() - two_days_ms) / bucket) * bucket
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedSeriesRow {
    metric_name: String,
    fingerprint: u64,
    unix_milli: i64,
    labels: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedSampleRow {
    metric_name: String,
    fingerprint: u64,
    unix_milli: i64,
    value: f64,
}

/// Seeds `n` distinct-fingerprint series (all `job="api"`, one sample of
/// value `1.0` each) at `unix_milli`, via the `_dist` tables on a client
/// bound to `db` — a real Distributed insert, letting ClickHouse's own
/// `cityHash64(metric_name, fingerprint)` sharding decide placement (issue
/// #136 plan edge case 3: never hand-pick fingerprints against an assumed
/// hash formula).
async fn seed_dist(db: &str, metric_name: &str, fps: &[u64], unix_milli: i64) {
    let mut cfg = shard1_config(db);
    cfg.database = db.to_string();
    let data_client = ChClient::new(cfg).await.expect("connect data client");
    let series_rows: Vec<SeedSeriesRow> = fps
        .iter()
        .map(|&fp| SeedSeriesRow {
            metric_name: metric_name.to_string(),
            fingerprint: fp,
            unix_milli,
            labels: r#"{"job":"api"}"#.to_string(),
        })
        .collect();
    let sample_rows: Vec<SeedSampleRow> = fps
        .iter()
        .map(|&fp| SeedSampleRow {
            metric_name: metric_name.to_string(),
            fingerprint: fp,
            unix_milli,
            value: 1.0,
        })
        .collect();
    data_client
        .insert_block("metric_series_dist", &series_rows)
        .await
        .expect("seed metric_series_dist");
    data_client
        .insert_block("metric_samples_dist", &sample_rows)
        .await
        .expect("seed metric_samples_dist");
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CountRow {
    n: u64,
}

/// The LOCAL (non-`_dist`) row count of `table` on one shard, restricted
/// to the seeded fingerprint set — read directly off that shard's
/// connection, not through the Distributed layer, so it reflects only what
/// actually landed there.
async fn local_count(
    shard: &ChClient,
    db: &str,
    table: &str,
    metric_name: &str,
    fps: &[u64],
) -> u64 {
    let fp_list = fps
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT count() AS n FROM {db}.{table} WHERE metric_name = '{metric_name}' AND \
         fingerprint IN ({fp_list})"
    );
    let mut stream = shard
        .query_stream::<CountRow>(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("query local {table} count: {e}"));
    stream.next().await.expect("one row").expect("decode").n
}

/// Polls both shards (asynchronous Distributed forwarding,
/// `distributed_foreground_insert = 0` by default) until every seeded
/// fingerprint has landed somewhere in BOTH `metric_series` AND
/// `metric_samples` (code review round 1, finding 2: the two `_dist`
/// inserts forward independently — waiting on the series table alone
/// leaves a window where the positive control's sample fetch reads a
/// shard whose samples block hasn't arrived yet), and asserts both shards
/// ended up with at least one series — the plan's edge-case-3 requirement
/// that shard-locality is actually exercised, not vacuously true because
/// everything landed on one shard. The deadline is generous (60s) and
/// only ever extends a broken run — a healthy run exits on the first
/// settled poll (tail-visibility discipline: bound the start, don't bump
/// the deadline).
async fn wait_for_cross_shard_split(
    shard1: &ChClient,
    shard2: &ChClient,
    db: &str,
    metric_name: &str,
    fps: &[u64],
) -> (u64, u64) {
    let want = fps.len() as u64;
    let mut last = (0u64, 0u64, 0u64, 0u64);
    for _ in 0..240 {
        let series1 = local_count(shard1, db, "metric_series", metric_name, fps).await;
        let series2 = local_count(shard2, db, "metric_series", metric_name, fps).await;
        let samples1 = local_count(shard1, db, "metric_samples", metric_name, fps).await;
        let samples2 = local_count(shard2, db, "metric_samples", metric_name, fps).await;
        last = (series1, series2, samples1, samples2);
        if series1 + series2 == want && samples1 + samples2 == want {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let (series1, series2, samples1, samples2) = last;
    assert_eq!(
        series1 + series2,
        want,
        "every seeded series row must land on exactly one shard (got shard1={series1} \
         shard2={series2}, want {want} total)"
    );
    assert_eq!(
        samples1 + samples2,
        want,
        "every seeded sample row must land on exactly one shard (got shard1={samples1} \
         shard2={samples2}, want {want} total)"
    );
    assert!(
        series1 > 0 && series2 > 0,
        "the seeded fingerprint set must split across BOTH shards to exercise shard-locality \
         (got shard1={series1} shard2={series2}) — widen the fingerprint set if this ever trips"
    );
    // Same co-sharding key on both tables, so the sample split must mirror
    // the series split exactly — a divergence here would mean the two
    // tables disagree on placement, invalidating the fix's exactness
    // argument before the query even runs.
    assert_eq!(
        (samples1, samples2),
        (series1, series2),
        "metric_samples must co-shard with metric_series under \
         cityHash64(metric_name, fingerprint)"
    );
    (series1, series2)
}

fn cache_config_dist(db: &str) -> LabelCacheConfig {
    LabelCacheConfig {
        db: db.to_string(),
        series_table: "metric_series_dist".to_string(),
        bucket_ms: DEFAULT_ACTIVITY_BUCKET_MS,
        window_ms: 24 * 3_600_000,
        cache_max_series: 50_000,
        ttl: Duration::from_secs(60),
        staleness_multiplier: 3,
    }
}

fn engine_config_dist(db: &str) -> MetricsConfig {
    MetricsConfig {
        db: db.to_string(),
        samples_table: "metric_samples_dist".to_string(),
        hist_samples_table: "metric_hist_samples_dist".to_string(),
        series_table: "metric_series_dist".to_string(),
        metadata_table: "metric_metadata".to_string(),
        experimental_functions: false,
        max_metric_fanout: 1_000,
        max_cache_scan: 200_000,
        max_info_series: 100_000,
        distributed: true,
    }
}

fn stage<'a>(explain: &'a PlanExplain, name: &str) -> &'a ExplainStage {
    explain
        .stages
        .iter()
        .find(|s| s.name == name)
        .unwrap_or_else(|| panic!("no {name:?} stage in {:#?}", explain.stages))
}

/// AC3 (permanent root-cause pin): the production-rendered `SqlFallback`
/// fetch SQL (`historical_series_subquery` + `sample_fetch_subquery`, real
/// `_dist` table names) against the real 2-shard fixture, with DEFAULT
/// settings (no `distributed_product_mode` override) — must fail with
/// ClickHouse's `DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED` (code 288). Pinning
/// the exact code means a future 24.8 image swap that silently changed
/// analyzer semantics would flip this test loudly instead of the fix
/// quietly stopping doing anything.
#[tokio::test]
async fn fallback_fetch_sql_is_denied_by_default_on_the_cluster() {
    skip_unless_live!();

    let db = "pulsus_read_it_metrics_cluster_fallback_negative";
    let shard1_bootstrap = init_clustered_db(db).await;
    let shard2 = ChClient::new(shard2_config("default"))
        .await
        .expect("connect shard2");

    let metric_name = "cluster_fallback_negative_probe";
    let fps: Vec<u64> = (1..=40).collect();
    let bucket = historical_bucket();
    seed_dist(db, metric_name, &fps, bucket).await;

    let shard1_local = ChClient::new(shard1_config("default"))
        .await
        .expect("connect shard1 (local reads)");
    wait_for_cross_shard_split(&shard1_local, &shard2, db, metric_name, &fps).await;

    // The exact production shapes (issue #136 root cause), rendered
    // against the `_dist` tables.
    let window = DataWindow {
        start_ms: bucket,
        end_ms: bucket,
    };
    let series_sql =
        historical_series_subquery("metric_series_dist", metric_name, window, bucket, &[]);
    let fetch_sql = sample_fetch_subquery(
        "metric_samples_dist",
        metric_name,
        &series_sql,
        bucket - 1,
        bucket,
    );

    let mut cfg = shard1_config("default");
    cfg.database = db.to_string();
    let client = ChClient::new(cfg).await.expect("connect (fallback probe)");
    let err = match client
        .query_stream::<SampleRow>(&fetch_sql, &QuerySettings::new())
        .await
    {
        Err(e) => e,
        Ok(mut stream) => match stream.next().await {
            Some(Err(e)) => e,
            Some(Ok(row)) => panic!(
                "expected the double-distributed IN to be denied, got a row instead: {row:?}"
            ),
            None => panic!(
                "expected the double-distributed IN to be denied, got an empty (no-error) result"
            ),
        },
    };
    match err {
        ChError::Server { code, message } => {
            assert_eq!(
                code, 288,
                "expected DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED (288), got code {code}: {message}"
            );
            assert!(
                message.contains("Double-distributed")
                    || message.contains("distributed_product_mode"),
                "expected the double-distributed-IN denial message, got: {message}"
            );
        }
        other => panic!("expected ChError::Server{{code: 288, ..}}, got {other:?}"),
    }

    drop_database(&shard1_bootstrap, db).await;
}

/// AC4: the real end-to-end fix. `MetricsEngine` over `_dist` tables with
/// `distributed: true` and a 24h cache window forcing `SqlFallback`
/// (`FallbackReason::OutOfWindow`) over a cross-shard-split corpus returns
/// exactly the expected result — the `distributed_product_mode='local'`
/// rewrite this issue adds is what keeps the identical query shape from
/// AC3 from being denied here. The hist fetch is implicitly proven (the
/// dual-read always dispatches it, over the SAME settings; it would 288
/// identically without the fix).
#[tokio::test]
async fn engine_returns_exact_samples_across_shards_via_the_local_product_mode_fix() {
    skip_unless_live!();

    let db = "pulsus_read_it_metrics_cluster_fallback_positive";
    let shard1_bootstrap = init_clustered_db(db).await;
    let shard2 = ChClient::new(shard2_config("default"))
        .await
        .expect("connect shard2");

    let metric_name = "cluster_fallback_positive_metric";
    let fps: Vec<u64> = (1..=40).collect();
    let bucket = historical_bucket();
    seed_dist(db, metric_name, &fps, bucket).await;

    let shard1_local = ChClient::new(shard1_config("default"))
        .await
        .expect("connect shard1 (local reads)");
    wait_for_cross_shard_split(&shard1_local, &shard2, db, metric_name, &fps).await;

    let cache_client = ChClient::new(shard1_config(db))
        .await
        .expect("connect cache client");
    let engine_client = ChClient::new(shard1_config(db))
        .await
        .expect("connect engine client");
    let cache = Arc::new(LabelCache::new(cache_client, cache_config_dist(db)));
    cache.refresh().await.expect("refresh");
    assert!(cache.is_warm());

    let engine = MetricsEngine::new(engine_client, cache, engine_config_dist(db));
    let expr = parse(&format!("count by (job) ({metric_name})")).expect("parse");
    let params = MetricQueryParams {
        start_ms: bucket,
        end_ms: bucket,
        step_ms: 0,
    };
    let (result, _annotations, explain) = engine
        .query_explained(&expr, &params)
        .await
        .expect("query_explained must succeed under the local-product-mode fix");

    // Proves the fallback path (not the cache-hit path) actually ran: the
    // nested-subquery `sample_fetch` shape against the `_dist` series
    // table, exactly the shape AC3 pins as denied without the fix.
    let fetch_stage = stage(&explain, "sample_fetch");
    assert!(
        fetch_stage.sql.contains("FROM metric_series_dist"),
        "expected the SqlFallback nested-subquery shape naming metric_series_dist, got: {}",
        fetch_stage.sql
    );
    let resolution_stage = stage(&explain, "series_resolution");
    assert!(
        resolution_stage
            .note
            .as_deref()
            .is_some_and(|n| n.contains("OutOfWindow")),
        "expected the OutOfWindow fallback reason, got: {resolution_stage:#?}"
    );

    match result {
        QueryResult::Vector(v) => {
            assert_eq!(v.len(), 1, "one job=api group, got {v:?}");
            assert_eq!(v[0].labels, vec![("job".to_string(), "api".to_string())]);
            assert_eq!(
                v[0].value,
                fps.len() as f64,
                "count by (job) must count every one of the {} seeded cross-shard series",
                fps.len()
            );
        }
        other => panic!("expected Vector, got {other:?}"),
    }

    drop_database(&shard1_bootstrap, db).await;
}
