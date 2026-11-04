//! Issue #18 part B, B8: a level's coverage and its data come from one
//! replica's answer, on the two-shard fixture.
//!
//! The first level is built on both shards; fetches of
//! `metric_samples_60s` are stopped on shard 1's second replica; one more
//! first-level run writes a new part on shard 1's first replica only. The
//! engine's answer hook then answers every level-fetch statement by
//! running its text against each shard's local table: for shard 1, on the
//! replica holding the new part when the statement selects run rows
//! (`kind = 1`), and on the stopped replica when it selects data only.
//! The fetch reads run rows and data in one statement, so the run row
//! arrives with its data, and the answer equals routing off.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1` and the 2-shard fixture:
//!
//! ```text
//! docker compose -f ci/clickhouse-cluster/compose.yaml up -d
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test live_metrics_rollup_cluster
//! docker compose -f ci/clickhouse-cluster/compose.yaml down -v
//! ```

#[path = "query_log_wait/mod.rs"]
mod query_log_wait;

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_model::Tenant;
use pulsus_promql::parser::parse;
use pulsus_read::metrics::rollup::{LevelRow, LevelSlot, refresh_levels};
use pulsus_read::{
    LabelCache, LabelCacheConfig, MetricQueryParams, MetricsConfig, MetricsEngine, QueryResult,
};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;
use query_log_wait::settle_query_log;

const CLUSTER_NAME: &str = "pulsus_test_cluster";
const MIN: i64 = 60_000;
const HOUR: i64 = 3_600_000;

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

fn shard_config(
    host_env: &str,
    default_host: &str,
    port_env: &str,
    database: &str,
) -> ChConnConfig {
    ChConnConfig {
        server: std::env::var(host_env).unwrap_or_else(|_| default_host.to_string()),
        http_port: std::env::var(port_env)
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(8123),
        database: database.to_string(),
        proto: ChProto::Http,
        pool_size: 8,
        query_timeout: Duration::from_secs(120),
        ..ChConnConfig::default()
    }
}

fn shard1_config(database: &str) -> ChConnConfig {
    shard_config(
        "PULSUS_TEST_CH_SHARD1_HOST",
        "172.28.0.11",
        "PULSUS_TEST_CH_SHARD1_HTTP_PORT",
        database,
    )
}

fn shard2_config(database: &str) -> ChConnConfig {
    shard_config(
        "PULSUS_TEST_CH_SHARD2_HOST",
        "172.28.0.12",
        "PULSUS_TEST_CH_SHARD2_HTTP_PORT",
        database,
    )
}

fn shard1b_config(database: &str) -> ChConnConfig {
    shard_config(
        "PULSUS_TEST_CH_SHARD1B_HOST",
        "172.28.0.13",
        "PULSUS_TEST_CH_SHARD1B_HTTP_PORT",
        database,
    )
}

fn engine_config(db: &str) -> MetricsConfig {
    MetricsConfig {
        max_replica_delay_secs: 5,
        skip_unavailable_shards: false,
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        db: db.to_string(),
        samples_table: "metric_samples_dist".to_string(),
        hist_samples_table: "metric_hist_samples_dist".to_string(),
        series_table: "metric_series_dist".to_string(),
        labels_table: "metric_labels_dist".to_string(),
        label_index_table: "metric_label_index_dist".to_string(),
        label_values_table: "metric_label_values".to_string(),
        exemplars_table: "metric_exemplars_dist".to_string(),
        metadata_table: "metric_metadata".to_string(),
        experimental_functions: false,
        max_metric_fanout: 1_000,
        max_cache_scan: 200_000,
        cache_max_series: 50_000,
        max_info_series: 100_000,
        max_samples: 50_000_000,
        distributed: true,
        grouped_push: false,
    }
}

fn cache_config(db: &str) -> LabelCacheConfig {
    LabelCacheConfig {
        skip_unavailable_shards: false,
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        db: db.to_string(),
        series_table: "metric_series_dist".to_string(),
        labels_table: "metric_labels_dist".to_string(),
        window_ms: 24 * HOUR,
        cache_max_series: 50_000,
        ttl: Duration::from_secs(60),
        staleness_multiplier: 3,
    }
}

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::NonIdempotent)
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
}

/// The first-level view's statement as an insert over `(lo, hi]`: its
/// `plan` scalar replaced by the literal interval. Run on a node, it
/// builds that node's slice when the node is its shard's builder.
fn first_level_insert(ctx: &RenderCtx, lo: i64, hi: i64) -> String {
    let head = format!("CREATE MATERIALIZED VIEW {}.metric_samples_60s_mv", ctx.db);
    let view = pulsus_schema::rendered_statements(ctx)
        .into_iter()
        .find(|s| s.starts_with(&head))
        .expect("the first level's view");
    let body = &view[view.find("AS WITH ").expect("AS WITH") + 3..];
    let start = body
        .find("(SELECT (lo, hi, lo < hi) FROM (")
        .expect("the plan scalar");
    let end = body[start..].find(") AS plan,").expect("the plan alias") + start;
    format!(
        "INSERT INTO {}.metric_samples_60s\n{}(toInt64({lo}), toInt64({hi}), true{}",
        ctx.db,
        &body[..start],
        &body[end..]
    )
}

/// A level-fetch statement, answered by one shard's local table.
async fn local_rows(client: &ChClient, sql: &str, shard: u32) -> Vec<LevelRow> {
    let local = sql.replace("_dist\n", "\n").replace(
        "_shard_num AS shard",
        &format!("toUInt32({shard}) AS shard"),
    );
    let mut stream = client
        .query_stream::<LevelRow>(&local, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{local}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode"));
    }
    out
}

/// The 2-shard fixture's database, its three nodes, and the times its
/// series are scraped over.
struct Fixture {
    db: String,
    ctx: RenderCtx,
    bootstrap: ChClient,
    shard1: Arc<ChClient>,
    shard2: Arc<ChClient>,
    shard1b: Arc<ChClient>,
    mid: i64,
    lo: i64,
    hi: i64,
}

/// The fixture's ID of series `s` of a metric whose IDs start at
/// `1000 + offset`: the odd ones in shard 2's slice of the ID space.
fn fp_of(offset: u32) -> String {
    format!(
        "bitOr(if(s % 2 = 0, toUInt128(0), bitShiftLeft(toUInt128(1), 127)), toUInt128({} + s))",
        1000 + offset
    )
}

/// `db`, fresh on the 2-shard fixture, every level view stopped and
/// every level emptied, and each metric of `metrics`, `(name, ID offset)`:
/// forty series, half in each shard's slice of the ID space, scraped every
/// 15 s for three hours, registered and landed on shard 1 — or, with
/// `sharded` (issue #18 part 2a), landed through `metric_landing_dist`,
/// each series on the shard its ID hashes to.
async fn fixture(db: String, metrics: &[(&str, u32)], sharded: bool) -> Fixture {
    // Part B's levels, 60 s first and the resolution from 60 s: the
    // 40-minute queries read the first level.
    let ctx = RenderCtx {
        db: db.clone(),
        cluster: Some(CLUSTER_NAME.to_string()),
        dist_suffix: "_dist".to_string(),
        rollup_levels: vec![60, 720, 7200, 86400],
        rollup_ranges: vec![60, 86400, 604800, 7776000],
        ..RenderCtx::for_tests(&db, 30)
    };
    let bootstrap = ChClient::new(shard1_config("default"))
        .await
        .expect("bootstrap");
    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
    run_init(&bootstrap, &ctx)
        .await
        .expect("run_init (clustered)");
    let shard1 = Arc::new(ChClient::new(shard1_config(&db)).await.expect("shard1"));
    let shard2 = Arc::new(ChClient::new(shard2_config(&db)).await.expect("shard2"));
    let shard1b = Arc::new(ChClient::new(shard1b_config(&db)).await.expect("shard1b"));
    for node in [&shard1, &shard2, &shard1b] {
        for r in &ctx.rollup_levels {
            let view = format!("{db}.metric_samples_{r}s_mv");
            exec(node, &format!("SYSTEM WAIT VIEW {view}")).await;
            exec(node, &format!("SYSTEM STOP VIEW {view}")).await;
        }
    }
    for node in [&shard1, &shard2] {
        for r in &ctx.rollup_levels {
            exec(node, &format!("TRUNCATE TABLE {db}.metric_samples_{r}s")).await;
        }
    }

    let mid = (chrono::Utc::now().timestamp_millis() / HOUR) * HOUR - HOUR;
    let lo = mid - 2 * HOUR;
    let hi = mid + 10 * MIN;
    let (landing, settings) = if sharded {
        (
            "metric_landing_dist",
            "SETTINGS insert_distributed_sync = 1 ",
        )
    } else {
        ("metric_landing", "")
    };
    for (metric, offset) in metrics {
        let fp = fp_of(*offset);
        exec(
            &shard1,
            &format!(
                "INSERT INTO {landing} (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type) \
                 {settings}SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 2, '{metric}', {fp}, toInt64({lo}) + toInt64(h) * {HOUR} + 1000, \
                        concat('{{\"s\":\"', toString(s), '\"}}'), 0 \
                 FROM (SELECT toUInt32(number) AS s FROM numbers(40)) ARRAY JOIN [0, 1, 2] AS h"
            ),
        )
        .await;
        exec(
            &shard1,
            &format!(
                "INSERT INTO {landing} (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
                 {settings}SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 0, '{metric}', {fp}, \
                        toInt64({lo}) + toInt64(i) * 15000 + 2000, toFloat64(cityHash64(s, i) % 1000) \
                 FROM (SELECT toUInt32(a.number) AS s, b.number AS i FROM numbers(40) AS a, numbers(1, {}) AS b)",
                (hi - lo) / 15_000 - 1
            ),
        )
        .await;
    }
    Fixture {
        db,
        ctx,
        bootstrap,
        shard1,
        shard2,
        shard1b,
        mid,
        lo,
        hi,
    }
}

fn skipping() -> bool {
    if should_run() {
        return false;
    }
    eprintln!(
        "skipping: set PULSUS_TEST_CLICKHOUSE=1 with the 2-shard cluster fixture up to run \
         this test (see crates/pulsus-read/tests/live_metrics_rollup_cluster.rs)"
    );
    true
}

/// **B8 (issue #18 part B): a level's run row is read with its data.**
#[tokio::test(flavor = "multi_thread")]
async fn a_run_row_is_read_with_its_data() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with the 2-shard cluster fixture up to run \
             this test (see crates/pulsus-read/tests/live_metrics_rollup_cluster.rs)"
        );
        return;
    }
    let db = pulsus_testkit::test_db("pulsus_read_it_rollup_cluster");
    // Part B's levels, 60 s first and the resolution from 60 s: the
    // 40-minute query below reads the first level.
    let ctx = RenderCtx {
        db: db.clone(),
        cluster: Some(CLUSTER_NAME.to_string()),
        dist_suffix: "_dist".to_string(),
        rollup_levels: vec![60, 720, 7200, 86400],
        rollup_ranges: vec![60, 86400, 604800, 7776000],
        ..RenderCtx::for_tests(&db, 30)
    };
    let bootstrap = ChClient::new(shard1_config("default"))
        .await
        .expect("bootstrap");
    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
    run_init(&bootstrap, &ctx)
        .await
        .expect("run_init (clustered)");
    let shard1 = Arc::new(ChClient::new(shard1_config(&db)).await.expect("shard1"));
    let shard2 = Arc::new(ChClient::new(shard2_config(&db)).await.expect("shard2"));
    let shard1b = Arc::new(ChClient::new(shard1b_config(&db)).await.expect("shard1b"));
    for node in [&shard1, &shard2, &shard1b] {
        for r in &ctx.rollup_levels {
            let view = format!("{db}.metric_samples_{r}s_mv");
            exec(node, &format!("SYSTEM WAIT VIEW {view}")).await;
            exec(node, &format!("SYSTEM STOP VIEW {view}")).await;
        }
    }
    for node in [&shard1, &shard2] {
        for r in &ctx.rollup_levels {
            exec(node, &format!("TRUNCATE TABLE {db}.metric_samples_{r}s")).await;
        }
    }

    // Forty series, half in each shard's slice of the ID space, scraped
    // every 15 s for three hours, landed on shard 1.
    let mid = (chrono::Utc::now().timestamp_millis() / HOUR) * HOUR - HOUR;
    let lo = mid - 2 * HOUR;
    let hi = mid + 10 * MIN;
    let fp =
        "bitOr(if(s % 2 = 0, toUInt128(0), bitShiftLeft(toUInt128(1), 127)), toUInt128(1000 + s))";
    exec(
        &shard1,
        &format!(
            "INSERT INTO metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type) \
             SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 2, 'm', {fp}, toInt64({lo}) + toInt64(h) * {HOUR} + 1000, \
                    concat('{{\"s\":\"', toString(s), '\"}}'), 0 \
             FROM (SELECT toUInt32(number) AS s FROM numbers(40)) ARRAY JOIN [0, 1, 2] AS h"
        ),
    )
    .await;
    exec(
        &shard1,
        &format!(
            "INSERT INTO metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
             SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 0, 'm', {fp}, \
                    toInt64({lo}) + toInt64(i) * 15000 + 2000, toFloat64(cityHash64(s, i) % 1000) \
             FROM (SELECT toUInt32(a.number) AS s, b.number AS i FROM numbers(40) AS a, numbers(1, {}) AS b)",
            (hi - lo) / 15_000 - 1
        ),
    )
    .await;

    // The first level to `mid` on both shards, then shard 1's second
    // replica stops fetching it, then one more run to `hi`.
    let mut a = lo;
    while a < mid {
        for node in [&shard1, &shard2] {
            exec(node, &first_level_insert(&ctx, a, a + HOUR)).await;
        }
        a += HOUR;
    }
    exec(
        &shard1b,
        &format!("SYSTEM SYNC REPLICA {db}.metric_samples_60s"),
    )
    .await;
    exec(
        &shard1b,
        &format!("SYSTEM STOP FETCHES {db}.metric_samples_60s"),
    )
    .await;
    for node in [&shard1, &shard2] {
        exec(node, &first_level_insert(&ctx, mid, hi)).await;
    }
    tokio::time::sleep(Duration::from_secs(61)).await;

    let cache = Arc::new(LabelCache::new(
        ChClient::new(shard1_config(&db)).await.expect("connect"),
        cache_config(&db),
    ));
    cache.refresh().await.expect("refresh");
    let slot = Arc::new(LevelSlot::new());
    refresh_levels(&slot, &shard1, &db, "_dist", Some(CLUSTER_NAME))
        .await
        .expect("level discovery");
    assert_eq!(slot.get().expect("levels").shards, 2);

    let (h1, h1b, h2) = (
        Arc::clone(&shard1),
        Arc::clone(&shard1b),
        Arc::clone(&shard2),
    );
    let hook: pulsus_read::metrics::exec::RollupAnswerHook = Arc::new(move |sql: &str| {
        let sql = sql.to_string();
        let (h1, h1b, h2) = (Arc::clone(&h1), Arc::clone(&h1b), Arc::clone(&h2));
        let rows = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                let one = if sql.contains("kind = 1") { h1 } else { h1b };
                let mut rows = local_rows(&one, &sql, 1).await;
                rows.extend(local_rows(&h2, &sql, 2).await);
                rows.sort_by_key(|r| (r.kind, r.fingerprint, r.t_end));
                rows
            })
        });
        Some(rows)
    });
    let routed = MetricsEngine::new(
        ChClient::new(shard1_config(&db)).await.expect("connect"),
        Arc::clone(&cache),
        engine_config(&db),
    )
    .with_rollup_levels(Arc::clone(&slot))
    .with_rollup_answer_hook(hook);
    let raw = MetricsEngine::new(
        ChClient::new(shard1_config(&db)).await.expect("connect"),
        Arc::clone(&cache),
        engine_config(&db),
    );

    let tenant = Tenant::from_header(None, false).expect("the empty tenant");
    let expr = parse("max_over_time(m[1m])").expect("parse");
    let p = MetricQueryParams {
        start_ms: mid - 30 * MIN,
        end_ms: hi,
        step_ms: MIN,
    };
    let (a, _, explain) = routed
        .query_explained(&tenant, &expr, &p)
        .await
        .expect("routed");
    // The engine without levels is asked on the same request: a range
    // query answers on its own grid (issue #18).
    let res = pulsus_read::metrics::rollup::resolution(&p, &slot.get().expect("levels"));
    assert_eq!(res, Some(MIN), "the 60 s level is the resolution");
    let (b, _, _) = raw.query_explained(&tenant, &expr, &p).await.expect("raw");
    let notes: Vec<String> = explain
        .stages
        .iter()
        .filter(|s| s.name == "rollup_piece")
        .map(|s| s.note.clone().unwrap_or_default())
        .collect();
    assert_eq!(
        notes.last().map(String::as_str),
        Some(
            format!(
                "metric_samples_60s_dist.max served ({}, {hi}]",
                mid - 30 * MIN - 6 * MIN
            )
            .as_str()
        ),
        "the new run's interval is served from the 60 s level: {notes:?}"
    );
    let flat = |r: &QueryResult| match r {
        QueryResult::Matrix(m) => m
            .iter()
            .map(|s| {
                let mut l = s.labels.clone();
                l.sort();
                (
                    format!("{l:?}"),
                    s.points
                        .iter()
                        .map(|(t, v)| (*t, v.to_bits()))
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>(),
        other => panic!("unexpected {other:?}"),
    };
    assert_eq!(flat(&b).len(), 40);
    assert_eq!(flat(&a), flat(&b), "the routed answer equals routing off");

    exec(
        &shard1b,
        &format!("SYSTEM START FETCHES {db}.metric_samples_60s"),
    )
    .await;
    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
}

// --------------------------------------------- issue #579 part 4, T7 / T10b

/// The first level over `(lo, hi]` on both shards, an hour a statement,
/// then shard 1's second replica caught up, so either replica answers the
/// same rows.
async fn build_level(f: &Fixture) {
    build_level_to(f, f.hi).await;
}

/// [`build_level`] over `(lo, until]`.
async fn build_level_to(f: &Fixture, until: i64) {
    let mut a = f.lo;
    while a < until {
        let b = (a + HOUR).min(until);
        for node in [&f.shard1, &f.shard2] {
            exec(node, &first_level_insert(&f.ctx, a, b)).await;
        }
        a = b;
    }
    exec(
        &f.shard1b,
        &format!("SYSTEM SYNC REPLICA {}.metric_samples_60s", f.db),
    )
    .await;
}

/// The label cache and the level slot, read through shard 1.
async fn warm(f: &Fixture) -> (Arc<LabelCache>, Arc<LevelSlot>) {
    let cache = Arc::new(LabelCache::new(
        ChClient::new(shard1_config(&f.db)).await.expect("connect"),
        cache_config(&f.db),
    ));
    cache.refresh().await.expect("refresh");
    let slot = Arc::new(LevelSlot::new());
    refresh_levels(&slot, &f.shard1, &f.db, "_dist", Some(CLUSTER_NAME))
        .await
        .expect("level discovery");
    assert_eq!(slot.get().expect("levels").shards, 2);
    (cache, slot)
}

/// Issue #18 part 2c: a label cache over `f` that was never refreshed, so
/// every resolution falls back to SQL.
async fn cold_cache(f: &Fixture) -> Arc<LabelCache> {
    Arc::new(LabelCache::new(
        ChClient::new(shard1_config(&f.db)).await.expect("connect"),
        cache_config(&f.db),
    ))
}

/// An engine with the levels, pushed or not.
async fn level_engine(
    f: &Fixture,
    cache: &Arc<LabelCache>,
    slot: &Arc<LevelSlot>,
    push: bool,
    comment: Option<&str>,
) -> MetricsEngine {
    let mut e = MetricsEngine::new(
        ChClient::new(shard1_config(&f.db)).await.expect("connect"),
        Arc::clone(cache),
        MetricsConfig {
            grouped_push: push,
            ..engine_config(&f.db)
        },
    )
    .with_rollup_levels(Arc::clone(slot));
    if let Some(c) = comment {
        e = e.with_query_log_comment(c);
    }
    e
}

/// A matrix answer by labels, its points as bits.
fn cells(r: &QueryResult) -> std::collections::BTreeMap<String, Vec<(i64, u64)>> {
    match r {
        QueryResult::Matrix(m) => m
            .iter()
            .map(|s| {
                let mut l = s.labels.clone();
                l.sort();
                (
                    format!("{l:?}"),
                    s.points.iter().map(|(t, v)| (*t, v.to_bits())).collect(),
                )
            })
            .collect(),
        other => panic!("unexpected {other:?}"),
    }
}

/// `(stage name, statement)` of every stage of `explain`.
fn stages(explain: &pulsus_read::logql::PlanExplain) -> Vec<(String, String)> {
    explain
        .stages
        .iter()
        .map(|s| (s.name.to_string(), s.sql.clone()))
        .collect()
}

/// **T7 (issue #579 part 4): a pushed level node answers on two shards.**
/// The odd series are registered on shard 1 and their level rows built by
/// shard 2, so a level read filtered by a shard-local series set misses
/// them.
#[tokio::test(flavor = "multi_thread")]
async fn a_pushed_level_node_answers_on_two_shards() {
    if skipping() {
        return;
    }
    let f = fixture(
        pulsus_testkit::test_db("pulsus_read_it_rollup_cluster_t7"),
        &[("m", 0)],
        false,
    )
    .await;
    build_level(&f).await;
    let (cache, slot) = warm(&f).await;
    let on = level_engine(&f, &cache, &slot, true, None).await;
    // Issue #18 part 2c: a label cache never refreshed, so each side is
    // resolved by SQL and its level read keeps `GLOBAL IN`.
    let cold = cold_cache(&f).await;
    let cold_on = level_engine(&f, &cold, &slot, true, None).await;
    let off = level_engine(&f, &cache, &slot, false, None).await;
    let tenant = Tenant::from_header(None, false).expect("the empty tenant");
    let p = MetricQueryParams {
        start_ms: f.mid - 30 * MIN,
        end_ms: f.hi,
        step_ms: MIN,
    };
    for (kind, on) in [("warm", &on), ("cold", &cold_on)] {
        for (q, series) in [("sum(rate(m[5m]))", 1), ("sum by (s) (rate(m[5m]))", 40)] {
            let expr = parse(q).expect("parse");
            let (a, _, explain) = on.query_explained(&tenant, &expr, &p).await.expect("on");
            let (b, _, _) = off.query_explained(&tenant, &expr, &p).await.expect("off");
            let st = stages(&explain);
            assert!(
                st.iter().any(|(n, sql)| n == "pushed_aggregate"
                    && sql.contains("FROM metric_samples_60s_dist\n")),
                "{kind} {q}: no pushed statement reading the level: {st:#?}"
            );
            assert!(
                !st.iter().any(|(_, sql)| sql.starts_with("declined:")),
                "{kind} {q}: {st:#?}"
            );
            assert_eq!(cells(&b).len(), series, "{kind} {q}: in process");
            assert_eq!(
                cells(&a),
                cells(&b),
                "{kind} {q}: pushed against in process"
            );
        }
    }
    exec(
        &f.bootstrap,
        &format!(
            "DROP DATABASE IF EXISTS {} ON CLUSTER '{CLUSTER_NAME}' SYNC",
            f.db
        ),
    )
    .await;
}

// ------------------------------------------------ issue #18 part 2a, T24

/// A count `node` answers.
async fn count_on(node: &ChClient, sql: &str) -> u64 {
    #[derive(pulsus_clickhouse::Row, serde::Serialize, serde::Deserialize)]
    struct N {
        n: u64,
    }
    let mut stream = node
        .query_stream::<N>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
    let mut n = 0;
    while let Some(row) = stream.next().await {
        n = row.expect("decode").n;
    }
    n
}

/// Runs level `r`'s view on `node` until a run writes no run row there:
/// the single-node suite's `until_idle`, on one node.
async fn until_idle_on(node: &ChClient, db: &str, r: u32) {
    let view = format!("{db}.metric_samples_{r}s_mv");
    let runs =
        format!("SELECT toUInt64(count()) AS n FROM {db}.metric_samples_{r}s WHERE kind = 1");
    let mut before = count_on(node, &runs).await;
    for _ in 0..4000 {
        for verb in ["START", "REFRESH", "WAIT", "STOP"] {
            exec(node, &format!("SYSTEM {verb} VIEW {view}")).await;
        }
        let now = count_on(node, &runs).await;
        if now == before {
            return;
        }
        before = now;
    }
    panic!("level {r}s still writes run rows after 4000 runs");
}

/// **T24 (issue #18 part 2a): a coarse node answers on two shards.** The
/// series land on the shard their ID hashes to; the 60 s level is built to
/// `hi − 20 min` and the 720 s level from it on both shards, so a day's
/// window reads the 720 s level, the 60 s level and raw samples of both
/// shards in one counter.
#[tokio::test(flavor = "multi_thread")]
async fn a_coarse_node_answers_on_two_shards() {
    if skipping() {
        return;
    }
    let f = fixture(
        pulsus_testkit::test_db("pulsus_read_it_rollup_cluster_t24"),
        &[("m", 0), ("n", 5000)],
        true,
    )
    .await;
    build_level_to(&f, f.hi - 20 * MIN).await;
    for node in [&f.shard1, &f.shard2] {
        until_idle_on(node, &f.db, 720).await;
    }
    for r in [60, 720] {
        exec(
            &f.shard1b,
            &format!("SYSTEM SYNC REPLICA {}.metric_samples_{r}s", f.db),
        )
        .await;
    }
    let (cache, slot) = warm(&f).await;
    let on = level_engine(&f, &cache, &slot, true, None).await;
    // Issue #18 part 2c: a label cache never refreshed, so each side is
    // resolved by SQL and its level read keeps `GLOBAL IN`.
    let cold = cold_cache(&f).await;
    let cold_on = level_engine(&f, &cold, &slot, true, None).await;
    let off = level_engine(&f, &cache, &slot, false, None).await;
    let tenant = Tenant::from_header(None, false).expect("the empty tenant");
    let end = f.hi.div_euclid(12 * MIN) * 12 * MIN;
    let p = MetricQueryParams {
        start_ms: end - 24 * HOUR,
        end_ms: end,
        step_ms: 12 * MIN,
    };
    // Every failure, in order — cells against push off, the series count,
    // then the statements — asserted empty at the end, so a wrong answer is
    // reported whatever the statement text says.
    let mut failures: Vec<String> = Vec::new();
    for (kind, on) in [("warm", &on), ("cold", &cold_on)] {
        for q in [
            "sum by (s) (rate(m[12m]))",
            "sum by (s) (rate(m[12m]) + rate(n[12m]))",
        ] {
            let expr = parse(q).expect("parse");
            let (a, _, explain) = on.query_explained(&tenant, &expr, &p).await.expect("on");
            let (b, _, off_explain) = off.query_explained(&tenant, &expr, &p).await.expect("off");
            let (ca, cb) = (cells(&a), cells(&b));
            if ca != cb {
                let first = cb
                    .iter()
                    .find(|(k, v)| ca.get(*k) != Some(*v))
                    .map(|(k, _)| k.clone());
                failures.push(format!(
                    "{kind} {q}: cells differ from push off: {} series pushed, {} in process; first {first:?}",
                    ca.len(),
                    cb.len()
                ));
            }
            if cb.len() != 40 {
                failures.push(format!(
                    "{kind} {q}: {} series in process, want 40",
                    cb.len()
                ));
            }
            let st = stages(&explain);
            if !st.iter().any(|(n, sql)| {
                n == "pushed_aggregate"
                    && [
                        "FROM metric_samples_720s_dist\n",
                        "FROM metric_samples_60s_dist\n",
                        "FROM metric_samples_dist\n",
                    ]
                    .iter()
                    .all(|t| sql.contains(t))
            }) {
                failures.push(format!(
                    "{kind} {q}: no pushed statement reading the 720 s, 60 s and raw tables"
                ));
            }
            if st.iter().any(|(_, sql)| sql.starts_with("declined:")) {
                failures.push(format!("{kind} {q}: declined: {st:?}"));
            }
            if !off_explain.stages.iter().any(|s| {
                s.name == "rollup_piece"
                    && s.note.as_deref().is_some_and(|n| n.starts_with("raw ("))
            }) {
                failures.push(format!("{kind} {q}: in process, no raw piece"));
            }
        }
    }
    for (name, node) in [("shard1", &f.shard1), ("shard2", &f.shard2)] {
        let n = count_on(
            node,
            "SELECT toUInt64(count(DISTINCT fingerprint)) AS n FROM metric_samples",
        )
        .await;
        if n == 0 {
            failures.push(format!("{name} holds no raw series"));
        }
    }
    exec(
        &f.bootstrap,
        &format!(
            "DROP DATABASE IF EXISTS {} ON CLUSTER '{CLUSTER_NAME}' SYNC",
            f.db
        ),
    )
    .await;
    assert!(failures.is_empty(), "T24:\n{}", failures.join("\n"));
}

// ------------------------------------------------ issue #18 part 2b, T32

/// **T32 (issue #18 part 2b): a filled node answers on two shards.** T24's
/// fixture, its series on the shards their IDs hash to, the first level
/// built; a request at 30 s off the minute is filled at 60 s: the bucket
/// ends' read pushes over the level, the newest step's read over raw.
#[tokio::test(flavor = "multi_thread")]
async fn a_filled_node_answers_on_two_shards() {
    if skipping() {
        return;
    }
    let f = fixture(
        pulsus_testkit::test_db("pulsus_read_it_rollup_cluster_t32"),
        &[("m", 0)],
        true,
    )
    .await;
    build_level(&f).await;
    let (cache, slot) = warm(&f).await;
    let on = level_engine(&f, &cache, &slot, true, None).await;
    let off = level_engine(&f, &cache, &slot, false, None).await;
    let tenant = Tenant::from_header(None, false).expect("the empty tenant");
    let p = MetricQueryParams {
        start_ms: f.mid - 30 * MIN + 30_000,
        end_ms: f.hi - 30_000,
        step_ms: 30_000,
    };
    let q = "sum by (s) (rate(m[5m]))";
    let expr = parse(q).expect("parse");
    let (a, _, explain) = on.query_explained(&tenant, &expr, &p).await.expect("on");
    let (b, _, _) = off.query_explained(&tenant, &expr, &p).await.expect("off");
    let mut failures: Vec<String> = Vec::new();
    let (ca, cb) = (cells(&a), cells(&b));
    if ca != cb {
        failures.push(format!(
            "cells differ from push off: {} series pushed, {} in process",
            ca.len(),
            cb.len()
        ));
    }
    if cb.len() != 40 {
        failures.push(format!("{} series in process, want 40", cb.len()));
    }
    let st = stages(&explain);
    let pushed: Vec<&String> = st
        .iter()
        .filter(|(n, sql)| n == "pushed_aggregate" && !sql.starts_with("declined:"))
        .map(|(_, sql)| sql)
        .collect();
    let level = pushed
        .iter()
        .filter(|sql| {
            sql.contains("60000 AS grid_step") && sql.contains("FROM metric_samples_60s_dist\n")
        })
        .count();
    let newest = pushed
        .iter()
        .filter(|sql| sql.contains("30000 AS grid_step"))
        .count();
    if pushed.len() != 2 || level != 1 || newest != 1 {
        failures.push(format!(
            "{} pushed statements, {level} on the level at 60 s, {newest} at 30 s",
            pushed.len()
        ));
    }
    if st.iter().any(|(_, sql)| sql.starts_with("declined:")) {
        failures.push(format!("declined: {st:?}"));
    }
    exec(
        &f.bootstrap,
        &format!(
            "DROP DATABASE IF EXISTS {} ON CLUSTER '{CLUSTER_NAME}' SYNC",
            f.db
        ),
    )
    .await;
    assert!(failures.is_empty(), "T32:\n{}", failures.join("\n"));
}

// ------------------------------------------------ issue #18 part 2c, T63

/// One string `node` answers.
async fn text_on(node: &ChClient, sql: &str) -> String {
    #[derive(pulsus_clickhouse::Row, serde::Serialize, serde::Deserialize)]
    struct T {
        t: String,
    }
    let mut stream = node
        .query_stream::<T>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
    let mut out = String::new();
    while let Some(row) = stream.next().await {
        out = row.expect("decode").t;
    }
    out
}

/// The hosts that ran a non-initial statement under `comment` reading
/// `table`, on every replica of the cluster.
async fn hosts_reading(f: &Fixture, comment: &str, table: &str) -> Vec<String> {
    #[derive(pulsus_clickhouse::Row, serde::Serialize, serde::Deserialize)]
    struct H {
        host: String,
    }
    settle_query_log(
        &[&f.shard1, &f.shard2, &f.shard1b],
        &format!("log_comment = '{comment}'"),
        1,
    )
    .await;
    let sql = format!(
        "SELECT hostname AS host FROM clusterAllReplicas('{CLUSTER_NAME}', system.query_log) \
         WHERE type = 'QueryFinish' AND NOT is_initial_query AND log_comment = '{comment}' \
           AND has(tables, '{table}')"
    );
    let mut stream = f
        .shard1
        .query_stream::<H>(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode").host);
    }
    out
}

/// Issue #18 part 2c r20: the hosts that ran any statement under
/// `comment` whose `tables` hold `table`, initial or not. A host reading
/// its own shard's copy inside the initial query lists the local table
/// there; a remote shard part lists it in its non-initial row.
async fn hosts_reading_local(f: &Fixture, comment: &str, table: &str) -> Vec<String> {
    #[derive(pulsus_clickhouse::Row, serde::Serialize, serde::Deserialize)]
    struct H {
        host: String,
    }
    settle_query_log(
        &[&f.shard1, &f.shard2, &f.shard1b],
        &format!("log_comment = '{comment}'"),
        1,
    )
    .await;
    let sql = format!(
        "SELECT hostname AS host FROM clusterAllReplicas('{CLUSTER_NAME}', system.query_log) \
         WHERE type = 'QueryFinish' AND log_comment = '{comment}' AND has(tables, '{table}')"
    );
    let mut stream = f
        .shard1
        .query_stream::<H>(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode").host);
    }
    out
}

/// **T63 (issue #18 part 2c): a lagging replica is skipped and shown.**
/// Shard 1's second replica stops fetching the 60 s level while shard 1
/// builds once more, until its copy lags 5 s or more. An engine on that
/// replica, at a 5 s limit, reads shard 1's part of the 60 s table from
/// the other replica, push on and off, answers as push off, and its level
/// loop shows the lag.
#[tokio::test(flavor = "multi_thread")]
async fn a_lagging_replica_is_skipped_and_shown() {
    if skipping() {
        return;
    }
    let f = fixture(
        pulsus_testkit::test_db("pulsus_read_it_rollup_cluster_t63"),
        &[("m", 0)],
        true,
    )
    .await;
    build_level_to(&f, f.mid).await;
    let table = format!("{}.metric_samples_60s", f.db);
    exec(&f.shard1b, &format!("SYSTEM STOP FETCHES {table}")).await;
    for node in [&f.shard1, &f.shard2] {
        exec(node, &first_level_insert(&f.ctx, f.mid, f.hi)).await;
    }
    let delay_sql = format!(
        "SELECT toString(absolute_delay) AS t FROM system.replicas \
         WHERE database = '{}' AND table = 'metric_samples_60s'",
        f.db
    );
    let mut lag = 0u64;
    for _ in 0..60 {
        lag = text_on(&f.shard1b, &delay_sql).await.parse().unwrap_or(0);
        if lag >= 5 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(lag >= 5, "shard1b's 60 s copy lags {lag} s after a minute");
    let (h1, h1b) = (
        text_on(&f.shard1, "SELECT hostName() AS t").await,
        text_on(&f.shard1b, "SELECT hostName() AS t").await,
    );
    // An engine on the lagging replica.
    let cache = Arc::new(LabelCache::new(
        ChClient::new(shard1b_config(&f.db)).await.expect("connect"),
        cache_config(&f.db),
    ));
    cache.refresh().await.expect("refresh");
    let slot = Arc::new(LevelSlot::new());
    refresh_levels(&slot, &f.shard1b, &f.db, "_dist", Some(CLUSTER_NAME))
        .await
        .expect("level discovery");
    let engine = |push: bool, comment: String| {
        let cache = Arc::clone(&cache);
        let slot = Arc::clone(&slot);
        let db = f.db.clone();
        async move {
            MetricsEngine::new(
                ChClient::new(shard1b_config(&db)).await.expect("connect"),
                cache,
                MetricsConfig {
                    grouped_push: push,
                    max_replica_delay_secs: 5,
                    ..engine_config(&db)
                },
            )
            .with_rollup_levels(slot)
            .with_query_log_comment(comment)
        }
    };
    let tenant = Tenant::from_header(None, false).expect("the empty tenant");
    let expr = parse("sum by (s) (rate(m[5m]))").expect("parse");
    let p = MetricQueryParams {
        start_ms: f.mid - 30 * MIN,
        end_ms: f.hi,
        step_ms: MIN,
    };
    let mut answers = Vec::new();
    for push in [true, false] {
        let comment = format!("{}_t63_{push}_{}", f.db, uuid::Uuid::new_v4().simple());
        let e = engine(push, comment.clone()).await;
        let (r, _) = e.query(&tenant, &expr, &p).await.expect("query");
        answers.push(cells(&r));
        let hosts = hosts_reading(&f, &comment, &table).await;
        assert!(
            !hosts.contains(&h1b),
            "push {push}: the lagging replica {h1b} read {table}: {hosts:?}"
        );
        assert!(
            hosts.contains(&h1),
            "push {push}: shard 1's other replica {h1} read no {table}: {hosts:?}"
        );
    }
    assert_eq!(answers[0].len(), 40, "push off");
    assert_eq!(answers[0], answers[1], "push on against push off");
    let shown = slot.replicas().unwrap_or_default();
    assert!(
        shown
            .iter()
            .any(|r| r.host == h1b && r.table == "metric_samples_60s" && r.absolute_delay >= 5),
        "the replica slot does not show {h1b}'s lag: {shown:?}"
    );
    exec(&f.shard1b, &format!("SYSTEM START FETCHES {table}")).await;
    exec(
        &f.bootstrap,
        &format!(
            "DROP DATABASE IF EXISTS {} ON CLUSTER '{CLUSTER_NAME}' SYNC",
            f.db
        ),
    )
    .await;
}

/// **T67 (issue #18 part 2c r20): a two-table statement keeps the local
/// replica.** Shard 1's first replica stops fetching `metric_samples`
/// while ten more minutes of one of shard 1's series land through its
/// second replica, until the first replica's copy lags 5 s or more. An
/// engine on the first replica, push off and the label cache never
/// refreshed, reads with a label matcher: its sample fetch filters through
/// the series statement in an `IN` subquery, so it is sent without the lag
/// guard and reads shard 1's part of `metric_samples` locally, as before
/// this part.
#[tokio::test(flavor = "multi_thread")]
async fn a_two_table_statement_keeps_the_local_replica() {
    if skipping() {
        return;
    }
    let f = fixture(
        pulsus_testkit::test_db("pulsus_read_it_rollup_cluster_t67"),
        &[("m", 0), ("n", 5000)],
        true,
    )
    .await;
    // Series `s = 10` of `m`: an even series, in shard 1's slice.
    let fp = "toUInt128(1010)";
    let shard = text_on(
        &f.shard1,
        &format!(
            "SELECT toString(any(shardNum())) AS t FROM {}.metric_samples_dist \
             WHERE fingerprint = {fp}",
            f.db
        ),
    )
    .await;
    assert_eq!(shard, "1", "series 10 of m is on shard {shard}");
    let table = format!("{}.metric_samples", f.db);
    exec(&f.shard1, &format!("SYSTEM STOP FETCHES {table}")).await;
    exec(
        &f.shard1b,
        &format!(
            "INSERT INTO {}.metric_landing_dist (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
             SETTINGS insert_distributed_sync = 1 \
             SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 0, 'm', {fp}, \
                    toInt64({}) + toInt64(i) * 15000 + 2000, toFloat64(i) \
             FROM (SELECT number AS i FROM numbers(40))",
            f.db, f.hi
        ),
    )
    .await;
    let delay_sql = format!(
        "SELECT toString(absolute_delay) AS t FROM system.replicas \
         WHERE database = '{}' AND table = 'metric_samples'",
        f.db
    );
    let mut lag = 0u64;
    for _ in 0..60 {
        lag = text_on(&f.shard1, &delay_sql).await.parse().unwrap_or(0);
        if lag >= 5 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(
        lag >= 5,
        "shard1's metric_samples lags {lag} s after a minute"
    );
    let (h1, h1b) = (
        text_on(&f.shard1, "SELECT hostName() AS t").await,
        text_on(&f.shard1b, "SELECT hostName() AS t").await,
    );
    let comment = format!("{}_t67_{}", f.db, uuid::Uuid::new_v4().simple());
    let engine = MetricsEngine::new(
        ChClient::new(shard1_config(&f.db)).await.expect("connect"),
        cold_cache(&f).await,
        MetricsConfig {
            grouped_push: false,
            max_replica_delay_secs: 5,
            ..engine_config(&f.db)
        },
    )
    .with_query_log_comment(&comment);
    let tenant = Tenant::from_header(None, false).expect("the empty tenant");
    let expr = parse(r#"sum(rate(m{s=~"1.*"}[5m]))"#).expect("parse");
    let p = MetricQueryParams {
        start_ms: f.hi - 40 * MIN,
        end_ms: f.hi,
        step_ms: MIN,
    };
    let (_, _, explain) = engine
        .query_explained(&tenant, &expr, &p)
        .await
        .expect("query");
    let resolution: Vec<String> = explain
        .stages
        .iter()
        .filter(|s| s.name == "series_resolution")
        .map(|s| s.sql.clone())
        .collect();
    assert!(
        !resolution.is_empty() && resolution.iter().all(|s| !s.starts_with("label cache:")),
        "the series resolve by SQL: {resolution:?}"
    );
    let hosts = hosts_reading_local(&f, &comment, &table).await;
    exec(&f.shard1, &format!("SYSTEM START FETCHES {table}")).await;
    assert!(
        hosts.contains(&h1),
        "the local replica {h1} read no {table}: {hosts:?}"
    );
    assert!(
        !hosts.contains(&h1b),
        "the second replica {h1b} read {table}: {hosts:?}"
    );
    exec(
        &f.bootstrap,
        &format!(
            "DROP DATABASE IF EXISTS {} ON CLUSTER '{CLUSTER_NAME}' SYNC",
            f.db
        ),
    )
    .await;
}
