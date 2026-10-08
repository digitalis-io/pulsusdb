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
use pulsus_model::ACTIVITY_BUCKET_MS;
use pulsus_promql::parser::parse;
use pulsus_read::metrics::sample_rows::SampleRow;
use pulsus_read::metrics::sample_sql::sample_fetch_subquery;
use pulsus_read::metrics::sql::historical_series_subquery;
use pulsus_read::{
    DataWindow, ExplainStage, LabelCache, LabelCacheConfig, MetricQueryParams, MetricsConfig,
    MetricsEngine, PlanExplain, QueryResult,
};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

const CLUSTER_NAME: &str = "pulsus_test_cluster";

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping when
/// the gate is absent in a live CI job, so a lost `env:` block reddens the
/// build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
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
        metrics_landing_retention_hours: 6,
        metrics_dedup_window: 10_000,
        log_landing_retention_hours: 6,
        log_dedup_window: 10_000,
        trace_landing_retention_hours: 6,
        trace_dedup_window: 10_000,
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
    let bucket = ACTIVITY_BUCKET_MS;
    let two_days_ms = 2 * 24 * 3_600_000;
    ((now_ms() - two_days_ms) / bucket) * bucket
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedSeriesRow {
    metric_name: String,
    fingerprint: u128,
    unix_milli: i64,
    labels: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedSampleRow {
    fingerprint: u128,
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
            fingerprint: u128::from(fp),
            unix_milli,
            labels: r#"{"job":"api"}"#.to_string(),
        })
        .collect();
    let sample_rows: Vec<SeedSampleRow> = fps
        .iter()
        .map(|&fp| SeedSampleRow {
            fingerprint: u128::from(fp),
            unix_milli,
            value: 1.0,
        })
        .collect();
    data_client
        .insert_block("metric_series_dist", &activity_rows(&series_rows))
        .await
        .expect("seed metric_series_dist");
    seed_labels_on_every_shard(db, &series_rows).await;
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
    // Issue #623: the sample tables carry no metric name; the IDs alone
    // select the seeded rows there.
    let name = if table == "metric_series" {
        format!("metric_name = '{metric_name}' AND ")
    } else {
        String::new()
    };
    let sql =
        format!("SELECT count() AS n FROM {db}.{table} WHERE {name}fingerprint IN ({fp_list})");
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
        // Issue #398: the per-query ClickHouse memory ceiling; the
        // production default, so this fixture keeps today's behaviour.
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        db: db.to_string(),
        series_table: "metric_series_dist".to_string(),
        labels_table: "metric_labels_dist".to_string(),
        window_ms: 24 * 3_600_000,
        cache_max_series: 50_000,
        ttl: Duration::from_secs(60),
        staleness_multiplier: 3,
    }
}

fn engine_config_dist(db: &str) -> MetricsConfig {
    MetricsConfig {
        // Issue #398: the per-query ClickHouse memory ceiling; the
        // production default, so this fixture keeps today's behaviour.
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        db: db.to_string(),
        samples_table: "metric_samples_dist".to_string(),
        hist_samples_table: "metric_hist_samples_dist".to_string(),
        series_table: "metric_series_dist".to_string(),
        labels_table: "metric_labels_dist".to_string(),
        label_index_table: "metric_label_index_dist".to_string(),
        label_values_table: "metric_label_values".to_string(),
        metadata_table: "metric_metadata".to_string(),
        experimental_functions: false,
        max_metric_fanout: 1_000,
        max_cache_scan: 200_000,
        cache_max_series: 50_000,
        max_info_series: 100_000,
        max_samples: 50_000_000,
        distributed: true,
        // Issue #549: the shipped default — every suite here exercises
        // the configuration production runs.
        grouped_push: true,
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

    let db = &pulsus_testkit::test_db("pulsus_read_it_metrics_cluster_fallback_negative");
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
    let series_sql = historical_series_subquery(
        &no_tenant(),
        "metric_series_dist",
        "metric_labels_dist",
        metric_name,
        window,
        &[],
    );
    let fetch_sql = sample_fetch_subquery(
        &no_tenant(),
        "metric_samples_dist",
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

    let db = &pulsus_testkit::test_db("pulsus_read_it_metrics_cluster_fallback_positive");
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
        .query_explained(&no_tenant(), &expr, &params)
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

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct C1Row {
    fingerprint: u128,
    labels: String,
}

/// Runs `sql` wrapped to `fingerprint, labels` (`labels` empty where the
/// statement has none), in the order the rows arrive.
async fn c1_rows(
    client: &ChClient,
    sql: &str,
    has_labels: bool,
    settings: &QuerySettings,
) -> Result<Vec<(u128, String)>, ChError> {
    let labels = if has_labels { "toString(labels)" } else { "''" };
    let wrapped = format!(
        "SELECT toUInt128(fingerprint) AS fingerprint, {labels} AS labels FROM (\n{sql}\n)"
    )
    .replace('?', "??");
    let mut stream = client.query_stream::<C1Row>(&wrapped, settings).await?;
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        let row = row?;
        out.push((row.fingerprint, row.labels));
    }
    Ok(out)
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct C1Name {
    metric_name: String,
}

/// Runs a names statement, in the order the rows arrive.
async fn c1_names(
    client: &ChClient,
    sql: &str,
    settings: &QuerySettings,
) -> Result<Vec<String>, ChError> {
    let sql = sql.replace('?', "??");
    let mut stream = client.query_stream::<C1Name>(&sql, settings).await?;
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row?.metric_name);
    }
    Ok(out)
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct C1Count {
    rows: u64,
    fps: u64,
}

/// One shard's own rows of `table` for the C1 fingerprints.
async fn c1_local(shard: &ChClient, db: &str, table: &str) -> (u64, u64) {
    let sql = format!(
        "SELECT count() AS rows, uniqExact(fingerprint) AS fps FROM {db}.{table} \
         WHERE fingerprint BETWEEN 1 AND 45"
    );
    let mut stream = shard
        .query_stream::<C1Count>(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("local {table}: {e}"));
    let row = stream.next().await.expect("one row").expect("decode");
    (row.rows, row.fps)
}

/// **C1 (issue #623): the label reads on a cluster are shard-local and
/// answer each series once.** Kind-2 rows go into each shard's own
/// `metric_landing`, so each shard's views write its own activity and label
/// rows, as production does: fingerprints 1-20 and 41-45 on shard 1, 21-40
/// on shard 2, and 21-25 on both. `status="500"` on 1-20, absent on 21-45,
/// `job="api"` everywhere. Over the `_dist` tables with
/// `distributed_product_mode = 'local'`, statements 1 to 3 for
/// `status!="500"` answer exactly 21-45, each once, with `{"job":"api"}`,
/// and the one name once. At default settings the nested activity read is
/// denied (288).
#[tokio::test]
async fn label_reads_are_shard_local_and_answer_each_series_once() {
    skip_unless_live!();

    let db = &pulsus_testkit::test_db("pulsus_read_it_metrics_cluster_own_labels");
    let shard1_bootstrap = init_clustered_db(db).await;
    let metric_name = "c1_metric";
    let bucket = historical_bucket();
    let labels_of = |fp: u64| {
        if fp <= 20 {
            r#"{"job":"api","status":"500"}"#
        } else {
            r#"{"job":"api"}"#
        }
    };
    let shard1_fps: Vec<u64> = (1..=20).chain(41..=45).chain(21..=25).collect();
    let shard2_fps: Vec<u64> = (21..=40).collect();
    let shard1 = ChClient::new(shard1_config(db))
        .await
        .expect("connect shard1");
    let shard2 = ChClient::new(shard2_config(db))
        .await
        .expect("connect shard2");
    for (shard, fps) in [(&shard1, &shard1_fps), (&shard2, &shard2_fps)] {
        let values = fps
            .iter()
            .map(|fp| {
                format!(
                    "({}, 2, '{metric_name}', {fp}, {bucket}, '{}', 0)",
                    now_ms(),
                    labels_of(*fp)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        shard
            .execute(
                &format!(
                    "INSERT INTO metric_landing \
                     (received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type) \
                     VALUES {values}"
                ),
                &QuerySettings::new(),
                Idempotency::NonIdempotent,
            )
            .await
            .expect("seed a shard's own metric_landing");
    }
    for (shard, n, what) in [(&shard1, 30u64, "shard 1"), (&shard2, 20, "shard 2")] {
        for table in ["metric_series", "metric_labels"] {
            assert_eq!(
                c1_local(shard, db, table).await,
                (n, n),
                "{what}'s own {table} rows"
            );
        }
    }

    let window = DataWindow {
        start_ms: bucket,
        end_ms: bucket,
    };
    let matchers = vec![pulsus_read::LabelMatcher {
        key: "status".to_string(),
        op: pulsus_read::MatchOp::Neq,
        value: "500".to_string(),
    }];
    let named = pulsus_read::DiscoveryFilter {
        metric_name: Some(metric_name.to_string()),
        name_matchers: Vec::new(),
        matchers: matchers.clone(),
    };
    let unnamed = pulsus_read::DiscoveryFilter {
        metric_name: None,
        ..named.clone()
    };
    let (series, labels) = ("metric_series_dist", "metric_labels_dist");
    use pulsus_read::metrics::sql;
    let subquery =
        historical_series_subquery(&no_tenant(), series, labels, metric_name, window, &matchers);
    let resolution = sql::historical_resolution_query(
        &no_tenant(),
        series,
        labels,
        metric_name,
        window,
        &matchers,
    );
    let discovery_named = sql::discovery_query(&no_tenant(), series, labels, &named, window);
    let discovery_unnamed = sql::discovery_query(&no_tenant(), series, labels, &unnamed, window);
    let names_unnamed =
        sql::discovery_distinct_names_query(&no_tenant(), series, labels, &unnamed, window);

    let want: Vec<(u128, String)> = (21..=45u128)
        .map(|fp| (fp, r#"{"job":"api"}"#.to_string()))
        .collect();
    let sorted = |mut v: Vec<(u128, String)>| {
        v.sort();
        v
    };
    let local = QuerySettings::new().set("distributed_product_mode", "local");
    let mut got = c1_rows(&shard1, &subquery, false, &local)
        .await
        .unwrap_or_else(|e| panic!("historical_series_subquery: {e}\n{subquery}"));
    got.sort();
    got.dedup();
    assert_eq!(
        got.into_iter().map(|(fp, _)| fp).collect::<Vec<_>>(),
        (21..=45u128).collect::<Vec<_>>(),
        "historical_series_subquery, as a set"
    );
    for (what, sql_text, settings) in [
        ("historical_resolution_query", &resolution, &local),
        ("discovery_query named", &discovery_named, &local),
        ("discovery_query unnamed", &discovery_unnamed, &local),
    ] {
        let got = c1_rows(&shard1, sql_text, true, settings)
            .await
            .unwrap_or_else(|e| panic!("{what}: {e}\n{sql_text}"));
        assert_eq!(sorted(got), want, "{what}: 21-45, each once\n{sql_text}");
    }
    let names: Vec<String> = c1_names(&shard1, &names_unnamed, &local)
        .await
        .unwrap_or_else(|e| panic!("discovery_distinct_names_query: {e}\n{names_unnamed}"));
    assert_eq!(
        names,
        vec![metric_name.to_string()],
        "discovery_distinct_names_query: the one name, once\n{names_unnamed}"
    );
    match c1_rows(&shard1, &subquery, false, &QuerySettings::new()).await {
        Err(ChError::Server { code, .. }) => assert_eq!(code, 288, "{subquery}"),
        other => panic!(
            "the nested activity read at default settings must be denied (288), got {other:?}"
        ),
    }

    drop_database(&shard1_bootstrap, db).await;
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeenRow {
    fingerprint: u128,
    first_seen: i64,
    last_seen: i64,
}

async fn seen_rows(client: &ChClient, sql: &str) -> Vec<(u128, i64, i64)> {
    let mut stream = client
        .query_stream::<SeenRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        let row = row.expect("decode");
        out.push((row.fingerprint, row.first_seen, row.last_seen));
    }
    out
}

/// **C2 (issue #623): first and last seen span shards.** One series' kind-2
/// rows go into shard 1's `metric_landing` at hours 5 and 9 of a day and
/// into shard 2's at hours 2 and 7. Through `metric_labels_dist`,
/// `min(first_seen)` is hour 2 and `max(last_seen)` hour 9 by fingerprint,
/// before and after `OPTIMIZE FINAL` on each shard.
#[tokio::test]
async fn first_and_last_seen_span_shards() {
    skip_unless_live!();

    let db = &pulsus_testkit::test_db("pulsus_read_it_metrics_cluster_seen");
    let shard1_bootstrap = init_clustered_db(db).await;
    let h = ACTIVITY_BUCKET_MS;
    let day = (historical_bucket() / 86_400_000) * 86_400_000;
    let shard1 = ChClient::new(shard1_config(db))
        .await
        .expect("connect shard1");
    let shard2 = ChClient::new(shard2_config(db))
        .await
        .expect("connect shard2");
    for (shard, hours) in [(&shard1, [5i64, 9]), (&shard2, [2, 7])] {
        for hour in hours {
            shard
                .execute(
                    &format!(
                        "INSERT INTO metric_landing \
                         (received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type) \
                         VALUES ({}, 2, 'c2_metric', 77, {}, '{{\"job\":\"api\"}}', 0)",
                        now_ms(),
                        day + hour * h
                    ),
                    &QuerySettings::new(),
                    Idempotency::NonIdempotent,
                )
                .await
                .expect("seed a shard's own metric_landing");
        }
    }
    let sql = "SELECT fingerprint, min(first_seen) AS first_seen, max(last_seen) AS last_seen \
               FROM metric_labels_dist WHERE fingerprint = 77 GROUP BY fingerprint";
    let want = vec![(77u128, day + 2 * h, day + 9 * h)];
    assert_eq!(seen_rows(&shard1, sql).await, want, "before the merge");
    for shard in [&shard1, &shard2] {
        shard
            .execute(
                "OPTIMIZE TABLE metric_labels FINAL",
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("merge a shard's lookup");
    }
    assert_eq!(
        seen_rows(&shard1, sql).await,
        want,
        "after OPTIMIZE FINAL on each shard"
    );

    drop_database(&shard1_bootstrap, db).await;
}

/// Issue #623: a series is an activity row in `metric_series` and its label
/// set, once, in `metric_labels`.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedActivityRow {
    day: u16,
    fingerprint: u128,
    metric_name: String,
    hours: u32,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedLabelRow {
    metric_name: String,
    fingerprint: u128,
    labels: String,
    first_seen: i64,
    last_seen: i64,
}

fn activity_rows(rows: &[SeedSeriesRow]) -> Vec<SeedActivityRow> {
    rows.iter()
        .map(|r| SeedActivityRow {
            day: r.unix_milli.div_euclid(86_400_000) as u16,
            fingerprint: r.fingerprint,
            metric_name: r.metric_name.clone(),
            hours: 1u32 << (r.unix_milli.rem_euclid(86_400_000) / 3_600_000),
        })
        .collect()
}

/// The label rows go into every shard's local table. In production the
/// view writes a series' label row on the node that writes its activity
/// row; the activity rows here are placed by the routing wrapper's sharding
/// key instead, so every shard is given every label set, which is the
/// superset a shard-local read can always find its series' labels in.
async fn seed_labels_on_every_shard(db: &str, rows: &[SeedSeriesRow]) {
    let labels: Vec<SeedLabelRow> = rows
        .iter()
        .map(|r| SeedLabelRow {
            metric_name: r.metric_name.clone(),
            fingerprint: r.fingerprint,
            labels: r.labels.clone(),
            first_seen: r.unix_milli,
            last_seen: r.unix_milli,
        })
        .collect();
    for cfg in [shard1_config(db), shard2_config(db)] {
        let shard = ChClient::new(cfg).await.expect("connect a shard");
        shard
            .insert_block("metric_labels", &labels)
            .await
            .expect("seed metric_labels on a shard");
    }
}

/// The single-tenant deployment's tenant: no `X-Scope-OrgID`.
#[allow(dead_code)]
fn no_tenant() -> pulsus_model::Tenant {
    pulsus_model::Tenant::from_header(None, false).expect("no header is the empty tenant")
}
