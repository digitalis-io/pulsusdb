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

fn engine_config(db: &str, grouped_push: bool) -> MetricsConfig {
    MetricsConfig {
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
        grouped_push,
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

/// A fresh database on the 2-shard fixture, every level view stopped and
/// every level emptied, and each metric of `metrics`, `(name, ID offset)`:
/// forty series, half in each shard's slice of the ID space, scraped every
/// 15 s for three hours, registered and landed on shard 1.
async fn fixture(base: &str, metrics: &[(&str, u32)]) -> Fixture {
    let db = pulsus_testkit::test_db(base);
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
    for (metric, offset) in metrics {
        let fp = fp_of(*offset);
        exec(
            &shard1,
            &format!(
                "INSERT INTO metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type) \
                 SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 2, '{metric}', {fp}, toInt64({lo}) + toInt64(h) * {HOUR} + 1000, \
                        concat('{{\"s\":\"', toString(s), '\"}}'), 0 \
                 FROM (SELECT toUInt32(number) AS s FROM numbers(40)) ARRAY JOIN [0, 1, 2] AS h"
            ),
        )
        .await;
        exec(
            &shard1,
            &format!(
                "INSERT INTO metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
                 SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 0, '{metric}', {fp}, \
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
    if skipping() {
        return;
    }
    let Fixture {
        db,
        ctx,
        bootstrap,
        shard1,
        shard2,
        shard1b,
        mid,
        lo,
        hi,
    } = fixture("pulsus_read_it_rollup_cluster", &[("m", 0)]).await;

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
        engine_config(&db, false),
    )
    .with_rollup_levels(Arc::clone(&slot))
    .with_rollup_answer_hook(hook);
    let raw = MetricsEngine::new(
        ChClient::new(shard1_config(&db)).await.expect("connect"),
        Arc::clone(&cache),
        engine_config(&db, false),
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
    let mut a = f.lo;
    while a < f.hi {
        let b = (a + HOUR).min(f.hi);
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
        engine_config(&f.db, push),
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

/// Removes shard `node`'s coverage of `(a, b]` from its own level table,
/// as the single-node suite's `cut_runs` does, and returns the run rows it
/// deleted.
async fn cut_on(node: &ChClient, db: &str, a: i64, b: i64) -> Vec<(i64, i64)> {
    #[derive(pulsus_clickhouse::Row, serde::Serialize, serde::Deserialize)]
    struct Run {
        t_last: i64,
        t_end: i64,
    }
    let table = format!("{db}.metric_samples_60s");
    let mut stream = node
        .query_stream::<Run>(
            &format!(
                "SELECT t_last, t_end FROM {table} WHERE kind = 1 AND t_end > {a} AND t_last < {b}"
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("read the run rows");
    let mut overlapping = Vec::new();
    while let Some(row) = stream.next().await {
        let row = row.expect("decode");
        overlapping.push((row.t_last, row.t_end));
    }
    drop(stream);
    exec(
        node,
        &format!(
            "ALTER TABLE {table} DELETE WHERE kind = 1 AND t_end > {a} AND t_last < {b} \
             SETTINGS mutations_sync = 2"
        ),
    )
    .await;
    for &(t_last, t_end) in &overlapping {
        for (lo, hi) in [(t_last, a.min(t_end)), (b.max(t_last), t_end)] {
            if lo < hi {
                exec(
                    node,
                    &format!(
                        "INSERT INTO {table} (kind, org_id, fingerprint, t_end, t_last, built_at) \
                         VALUES (1, '', 0, {hi}, {lo}, 0)"
                    ),
                )
                .await;
            }
        }
    }
    overlapping
}

/// Every top-level statement shard 1 ran under `comment`, finished or
/// failed, in start order.
async fn statements(node: &ChClient, comment: &str) -> Vec<String> {
    #[derive(pulsus_clickhouse::Row, serde::Serialize, serde::Deserialize)]
    struct Logged {
        query: String,
    }
    exec(node, "SYSTEM FLUSH LOGS").await;
    let mut stream = node
        .query_stream::<Logged>(
            &format!(
                "SELECT query FROM system.query_log \
                 WHERE type IN ('QueryFinish', 'ExceptionWhileProcessing') AND is_initial_query \
                   AND log_comment = '{comment}' \
                 ORDER BY query_start_time_microseconds"
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("query_log");
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode").query);
    }
    out
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
    let f = fixture("pulsus_read_it_rollup_cluster_t7", &[("m", 0)]).await;
    build_level(&f).await;
    let (cache, slot) = warm(&f).await;
    let on = level_engine(&f, &cache, &slot, true, None).await;
    let off = level_engine(&f, &cache, &slot, false, None).await;
    let tenant = Tenant::from_header(None, false).expect("the empty tenant");
    let p = MetricQueryParams {
        start_ms: f.mid - 30 * MIN,
        end_ms: f.hi,
        step_ms: MIN,
    };
    for (q, series) in [("sum(rate(m[5m]))", 1), ("sum by (s) (rate(m[5m]))", 40)] {
        let expr = parse(q).expect("parse");
        let (a, _, explain) = on.query_explained(&tenant, &expr, &p).await.expect("on");
        let (b, _, _) = off.query_explained(&tenant, &expr, &p).await.expect("off");
        let st = stages(&explain);
        assert!(
            st.iter().any(|(n, sql)| n == "pushed_aggregate"
                && sql.contains("FROM metric_samples_60s_dist\n")),
            "{q}: no pushed statement reading the level: {st:#?}"
        );
        assert!(
            !st.iter().any(|(_, sql)| sql.starts_with("declined:")),
            "{q}: {st:#?}"
        );
        assert_eq!(cells(&b).len(), series, "{q}: in process");
        assert_eq!(cells(&a), cells(&b), "{q}: pushed against in process");
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

/// **T10b (issue #579 part 4): a side behind its coverage moves the whole
/// node.** `m` and `n`, the same label sets; on a fresh build the run rows
/// of a 10-minute interval are deleted on one shard, and the coverage hook
/// gives them back to one side's S1. That side's check raises, and both
/// selectors take today's route: two level fetches after the data
/// statement, and the answer of the route without the push.
#[tokio::test(flavor = "multi_thread")]
async fn a_side_behind_its_coverage_falls_back_whole() {
    use pulsus_read::metrics::rollup::RunRow;
    if skipping() {
        return;
    }
    let f = fixture(
        "pulsus_read_it_rollup_cluster_t10",
        &[("m", 0), ("n", 5000)],
    )
    .await;
    let tenant = Tenant::from_header(None, false).expect("the empty tenant");
    let expr = parse("sum by (s) (rate(m[5m]) + rate(n[5m]))").expect("parse");
    let p = MetricQueryParams {
        start_ms: f.mid - 30 * MIN,
        end_ms: f.hi,
        step_ms: MIN,
    };
    let (a, b) = (f.mid - 20 * MIN, f.mid - 10 * MIN);
    for (side, shard) in [(0usize, 1u32), (0, 2), (1, 1), (1, 2)] {
        let what = format!("side {side}, shard {shard}");
        exec(
            &f.shard1,
            &format!("TRUNCATE TABLE {}.metric_samples_60s", f.db),
        )
        .await;
        exec(
            &f.shard2,
            &format!("TRUNCATE TABLE {}.metric_samples_60s", f.db),
        )
        .await;
        build_level(&f).await;
        let node = if shard == 1 { &f.shard1 } else { &f.shard2 };
        let deleted = cut_on(node, &f.db, a, b).await;
        assert!(!deleted.is_empty(), "{what}: no run row to delete");
        exec(
            &f.shard1b,
            &format!("SYSTEM SYNC REPLICA {}.metric_samples_60s", f.db),
        )
        .await;
        let (cache, slot) = warm(&f).await;
        let comment = format!("{}_t10b_{side}_{shard}", f.db);
        let hook: pulsus_read::metrics::exec::CoverageHook =
            Arc::new(move |selector, mut runs: Vec<RunRow>| {
                if selector == side {
                    runs.extend(deleted.iter().map(|&(t_last, t_end)| RunRow {
                        shard,
                        t_last,
                        t_end,
                    }));
                }
                runs
            });
        let hooked = level_engine(&f, &cache, &slot, true, Some(&comment))
            .await
            .with_coverage_hook(hook);
        let plain = level_engine(&f, &cache, &slot, true, None).await;
        let off = level_engine(&f, &cache, &slot, false, None).await;
        let (x, _, explain) = hooked
            .query_explained(&tenant, &expr, &p)
            .await
            .expect("hooked");
        let (y, _, plain_explain) = plain
            .query_explained(&tenant, &expr, &p)
            .await
            .expect("plain");
        let (z, _, _) = off.query_explained(&tenant, &expr, &p).await.expect("off");
        let st = stages(&explain);
        assert!(
            st.iter()
                .any(|(n, sql)| n == "pushed_aggregate" && sql == "declined: ReplicaBehind"),
            "{what}: {st:#?}"
        );
        let sent = statements(&f.shard1, &comment).await;
        let data = sent
            .iter()
            .position(|q| q.starts_with("WITH ") && q.contains("PULSUS_LEVEL_REPLICA_BEHIND"))
            .unwrap_or_else(|| panic!("{what}: no data statement: {sent:#?}"));
        let fetches: Vec<usize> = sent
            .iter()
            .enumerate()
            .filter(|(_, q)| q.starts_with("SELECT _shard_num AS shard, kind, fingerprint, t_end"))
            .map(|(i, _)| i)
            .collect();
        assert_eq!(
            fetches.len(),
            2,
            "{what}: one level fetch a selector: {sent:#?}"
        );
        assert!(
            fetches.iter().all(|i| *i > data),
            "{what}: the level fetches follow the data statement: {sent:#?}"
        );
        assert_eq!(cells(&z).len(), 40, "{what}: in process");
        assert_eq!(
            cells(&x),
            cells(&z),
            "{what}: the fallback against in process"
        );
        let plain_stages = stages(&plain_explain);
        assert!(
            !plain_stages
                .iter()
                .any(|(_, sql)| sql.starts_with("declined:")),
            "{what}: without the hook: {plain_stages:#?}"
        );
        assert!(
            plain_stages
                .iter()
                .any(|(n, sql)| n == "pushed_aggregate" && sql.starts_with("WITH ")),
            "{what}: without the hook, not pushed: {plain_stages:#?}"
        );
        assert_eq!(cells(&y), cells(&z), "{what}: without the hook");
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
