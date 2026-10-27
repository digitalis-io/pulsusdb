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

fn engine_config(db: &str) -> MetricsConfig {
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
    // Part B's levels, 60 s first: the 40-minute query below reads the
    // first level.
    let ctx = RenderCtx {
        db: db.clone(),
        cluster: Some(CLUSTER_NAME.to_string()),
        dist_suffix: "_dist".to_string(),
        rollup_levels: vec![60, 720, 7200, 86400],
        rollup_ranges: vec![86400, 604800, 7776000],
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
    // The engine without levels is asked on the grid the routed answer
    // came on: the multiples of the effective step in `(start, end]`
    // (issue #18 follow-up, R2), so neither carries a time the other lacks.
    let (res, grid) =
        pulsus_read::metrics::rollup::grid(&p, &slot.get().expect("levels")).expect("a grid");
    assert_eq!(res, MIN, "the 60 s level is the resolution");
    assert!(
        explain.stages.iter().any(|s| s.name == "rollup_grid"),
        "the routed answer is on the changed grid"
    );
    let (b, _, _) = raw
        .query_explained(&tenant, &expr, &grid)
        .await
        .expect("raw");
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
                // The grid's first time is the first minute after the
                // start (issue #18 follow-up).
                mid - 29 * MIN - 6 * MIN
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
