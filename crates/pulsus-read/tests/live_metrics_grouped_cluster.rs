//! Issue #549: the grouped instant read over the **distributed** sample
//! tables, against a real 2-shard cluster.
//!
//! # What this guards, and why a CI step and not a session note
//!
//! The statement reads `metric_samples_dist` and `metric_hist_samples_dist`
//! inside ONE statement, unioned, with the fingerprint set as a literal
//! array. ClickHouse's `distributed_product_mode` refuses some shapes that
//! read a distributed table from inside another distributed read — and the
//! reason this statement is not one of them is narrower than "it avoids
//! nesting":
//!
//! ```text
//!   under the strict setting
//!     a distributed subquery inside a distributed read      REFUSED
//!     the nested form                                       REFUSED
//!     a derived-table wrapper around a distributed read     accepted, every setting
//! ```
//!
//! So it is the **literal fingerprint list** that keeps this statement out
//! of the refused forms, not the absence of nesting in general. That is a
//! property of the statement one edit could take away, which is what a
//! regression check is for.
//!
//! # What it asserts
//!
//! The pushed answer equals the unpushed answer over the same clustered
//! data, group for group and point for point, as raw bit patterns — the
//! same comparison `live_metrics_grouped.rs` makes on a single node, made
//! again where the rows arrive from two shards and are reduced at the
//! coordinator.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1` and requires the 2-shard
//! fixture specifically:
//!
//! ```text
//! docker compose -f ci/clickhouse-cluster/compose.yaml up -d
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test live_metrics_grouped_cluster
//! docker compose -f ci/clickhouse-cluster/compose.yaml down -v
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_model::DEFAULT_ACTIVITY_BUCKET_MS;
use pulsus_promql::parser::parse;
use pulsus_read::{
    LabelCache, LabelCacheConfig, MetricQueryParams, MetricsConfig, MetricsEngine, QueryResult,
};
use pulsus_schema::{RenderCtx, run_init};

const CLUSTER_NAME: &str = "pulsus_test_cluster";

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no cluster fixture; **panics** rather than
/// skipping when the gate is absent in a live CI job, so a lost `env:`
/// block reddens the build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with the 2-shard cluster fixture up to \
                 run this test (see this file's module doc)"
            );
            return;
        }
    };
}

/// Same fixture-static-IP convention as the other cluster suites (KISS
/// directive on issue #5 — dial each shard directly by IP, never a name);
/// overridable via the identical env var names.
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
        query_timeout: Duration::from_secs(60),
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

async fn init_clustered_db(db: &str) -> ChClient {
    let shard1 = ChClient::new(shard1_config("default"))
        .await
        .expect("connect to shard1 (bootstrap)");
    drop_database(&shard1, db).await;
    run_init(&shard1, &cluster_ctx(db)).await.expect("run_init");
    ChClient::new(shard1_config(db))
        .await
        .expect("connect to shard1")
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
    metric_name: String,
    fingerprint: u128,
    unix_milli: i64,
    value: f64,
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

fn cache_config(db: &str) -> LabelCacheConfig {
    LabelCacheConfig {
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        db: db.to_string(),
        series_table: "metric_series_dist".to_string(),
        bucket_ms: DEFAULT_ACTIVITY_BUCKET_MS,
        window_ms: 24 * 3_600_000,
        cache_max_series: 50_000,
        ttl: Duration::from_secs(60),
        staleness_multiplier: 3,
    }
}

fn engine_config(db: &str, grouped_push: bool) -> MetricsConfig {
    MetricsConfig {
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        db: db.to_string(),
        samples_table: "metric_samples_dist".to_string(),
        hist_samples_table: "metric_hist_samples_dist".to_string(),
        series_table: "metric_series_dist".to_string(),
        metadata_table: "metric_metadata".to_string(),
        experimental_functions: false,
        max_metric_fanout: 1_000,
        max_cache_scan: 200_000,
        max_info_series: 100_000,
        max_samples: 50_000_000,
        distributed: true,
        grouped_push,
    }
}

const METRIC: &str = "grouped_cluster";
/// 40 series in 4 groups, 60 grid points at a one-minute step. The shard
/// key is `cityHash64(metric_name, fingerprint)`, so the fingerprints
/// split across the two shards on their own; the test asserts the split
/// happened rather than assuming it.
const SERIES: u64 = 40;
const POINTS: i64 = 59;

/// A result series' labels and its points, values as raw bit patterns.
type Answer = Vec<(Vec<(String, String)>, Vec<(i64, u64)>)>;

fn answer_of(r: QueryResult) -> Answer {
    let mut out: Answer = match r {
        QueryResult::Matrix(m) => m
            .into_iter()
            .map(|s| {
                (
                    s.labels,
                    s.points
                        .into_iter()
                        .map(|(t, v)| (t, v.to_bits()))
                        .collect(),
                )
            })
            .collect(),
        other => panic!("unexpected result shape: {other:?}"),
    };
    for (labels, _) in &mut out {
        labels.sort();
    }
    out.sort();
    out
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CountRow {
    n: u64,
}

/// The pushed and unpushed routes answer the same thing over two shards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_grouped_read_over_the_dist_tables_answers_what_the_shipped_route_does() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_read_it_grouped_cluster");
    let client = init_clustered_db(&db).await;

    let now = now_ms();
    let t = (now / 60_000) * 60_000;
    let start = t - POINTS * 60_000;
    let bucket = (now / DEFAULT_ACTIVITY_BUCKET_MS) * DEFAULT_ACTIVITY_BUCKET_MS;

    let mut series = Vec::new();
    let mut samples = Vec::new();
    for fp in 1..=SERIES {
        let labels: BTreeMap<&str, String> = BTreeMap::from([
            (
                "status",
                ["200", "404", "500", "503"][(fp % 4) as usize].to_string(),
            ),
            ("instance", format!("i{fp}")),
        ]);
        series.push(SeedSeriesRow {
            metric_name: METRIC.to_string(),
            fingerprint: u128::from(fp),
            unix_milli: bucket,
            labels: serde_json::to_string(&labels).expect("labels json"),
        });
        for k in 0..=POINTS {
            samples.push(SeedSampleRow {
                metric_name: METRIC.to_string(),
                fingerprint: u128::from(fp),
                unix_milli: start + k * 60_000,
                value: fp as f64 + k as f64 * 0.5,
            });
        }
    }
    client
        .insert_block("metric_series_dist", &series)
        .await
        .expect("seed metric_series_dist");
    client
        .insert_block("metric_samples_dist", &samples)
        .await
        .expect("seed metric_samples_dist");

    // The corpus really is split: the local table on shard 1 holds fewer
    // rows than the distributed wrapper. Without this the differential
    // could pass on a cluster that had quietly put everything on one node.
    //
    // **And the forwarding must settle before anything is counted.** An
    // insert into a Distributed table returns as soon as the rows are
    // queued — `distributed_foreground_insert` is `0` by default, so the
    // shards receive them asynchronously. Counting straight afterwards
    // reads a partial corpus and the failure looks like a lost row rather
    // than a race: measured here at 1,260 rows of 2,400, on a test that
    // had passed twice before, which is how this suite was found to be
    // missing the wait `live_metrics_cluster_fallback.rs` already has.
    //
    // **One deadline, taken before the loop, capping every poll and every
    // sleep by what is left of it** (review round 2). A poll count alone
    // did not bound the wait: each of the 240 requests carried the
    // client's own timeout, so a cluster that accepted connections and
    // never answered could hold this test for hours and still call it a
    // failed run.
    //
    // **A healthy run can need more than one poll**: a passing run was
    // measured taking two. The poll count is not a fault signal — only
    // the row count at the end is — so it is printed rather than
    // asserted, and a second poll is nothing to chase.
    const FORWARDING_DEADLINE: Duration = Duration::from_secs(60);
    let want = SERIES * (POINTS as u64 + 1);
    let dist_sql = format!(
        "SELECT toUInt64(count()) AS n FROM metric_samples_dist WHERE metric_name = '{METRIC}'"
    );
    let deadline = Instant::now() + FORWARDING_DEADLINE;
    let mut total = 0u64;
    let mut polls = 0u32;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            break;
        }
        polls += 1;
        match tokio::time::timeout(left, count(&client, &dist_sql)).await {
            Ok(n) => total = n,
            // The deadline expired inside the request. Leave the loop
            // with whatever the last completed poll returned; the
            // assertion below is what reports it.
            Err(_) => break,
        }
        if total >= want {
            break;
        }
        let left = deadline.saturating_duration_since(Instant::now());
        tokio::time::sleep(left.min(Duration::from_millis(250))).await;
    }
    eprintln!("[549] forwarding settled after {polls} poll(s): {total} of {want} rows");
    assert_eq!(
        total, want,
        "every seeded row must reach the distributed table before the corpus is read \
         ({polls} polls within {FORWARDING_DEADLINE:?})"
    );
    let local = count(
        &client,
        &format!(
            "SELECT toUInt64(count()) AS n FROM metric_samples WHERE metric_name = '{METRIC}'"
        ),
    )
    .await;
    assert!(
        local > 0 && local < total,
        "the corpus must straddle both shards: shard 1 holds {local} of {total}"
    );

    let cache = Arc::new(LabelCache::new(
        ChClient::new(shard1_config(&db)).await.expect("connect"),
        cache_config(&db),
    ));
    cache.refresh().await.expect("refresh");
    assert!(cache.is_warm());
    let pushed = MetricsEngine::new(
        ChClient::new(shard1_config(&db)).await.expect("connect"),
        Arc::clone(&cache),
        engine_config(&db, true),
    );
    let unpushed = MetricsEngine::new(
        ChClient::new(shard1_config(&db)).await.expect("connect"),
        Arc::clone(&cache),
        engine_config(&db, false),
    );

    let params = MetricQueryParams {
        start_ms: start,
        end_ms: t,
        step_ms: 60_000,
    };
    for op in ["max", "min", "count", "group"] {
        let query = format!("{op} by (status) ({METRIC})");
        let expr = parse(&query).expect("parse");
        let (a, _) = pushed
            .query(&expr, &params)
            .await
            .unwrap_or_else(|e| panic!("{query} (pushed, clustered): {e:?}"));
        let (b, _) = unpushed
            .query(&expr, &params)
            .await
            .unwrap_or_else(|e| panic!("{query} (unpushed, clustered): {e:?}"));
        let (a, b) = (answer_of(a), answer_of(b));
        assert_eq!(a.len(), 4, "{query}: four groups");
        assert_eq!(
            a, b,
            "{query}: the pushed answer over the _dist tables differs from the shipped route's"
        );
    }

    let bootstrap = ChClient::new(shard1_config("default"))
        .await
        .expect("connect (bootstrap)");
    drop_database(&bootstrap, &db).await;
}

async fn count(client: &ChClient, sql: &str) -> u64 {
    use futures::StreamExt;
    let mut stream = client
        .query_stream::<CountRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    stream.next().await.expect("a row").expect("decode").n
}
