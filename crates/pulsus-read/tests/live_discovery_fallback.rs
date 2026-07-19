//! Live tests for issue #96's degraded-cache discovery SQL fallback: a
//! regex/negated-`__name__` `/series`/`/labels`/`/label/{name}/values`
//! selector, run against a **degraded** (cold) label cache, resolves its
//! candidate metric names through a bounded `SELECT DISTINCT metric_name`
//! probe over `metric_series` and then the same flat `metric_name IN (…)`
//! fetch the warm path uses — instead of the pre-#96 named `422`. Gated
//! behind `PULSUS_TEST_CLICKHOUSE=1`, mirroring `live_metrics_engine.rs`'s
//! setup (direct `ChClient::insert_block` seeding, per-test throwaway
//! database).
//!
//! Covers the #96 ACs that need real data + real execution:
//! - **AC1 (degraded == warm, byte-for-byte):** a cold cache (generation 0
//!   → `ColdCache` → `Unresolvable`) and a warmed cache
//!   (`cache.refresh()`) return identical `series`/`label_names`/
//!   `label_values` results for the same regex-`__name__` selector.
//! - **AC3 (probe bound enforced):** `max_metric_fanout = 2` with 3
//!   matching names, cold cache → `Err(QueryTooBroad(MetricFanout {
//!   matched: 3, cap: 2 }))`, never an unbounded scan.
//! - **AC5 (probe scan recorded, not gated):** the probe's `read_rows` is
//!   recorded to the test output; scale-dependent wall-time routes to #25.
//!
//! To run these:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test live_discovery_fallback
//! podman rm -f pulsus-ch-test
//! ```

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_model::DEFAULT_ACTIVITY_BUCKET_MS;
use pulsus_read::logql::{ReadError, TooBroadReason};
use pulsus_read::{
    DataWindow, DiscoveryFilter, LabelCache, LabelCacheConfig, LabelMatcher, MatchOp,
    MetricsConfig, MetricsEngine,
};
use pulsus_schema::{RenderCtx, run_init};

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-read/tests/live_discovery_fallback.rs for setup)"
            );
            return;
        }
    };
}

fn test_config(database: &str) -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: database.to_string(),
        proto: ChProto::Http,
        pool_size: 8,
        query_timeout: Duration::from_secs(30),
        ..ChConnConfig::default()
    }
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

async fn init_db(bootstrap: &ChClient, db: &str) {
    drop_database(bootstrap, db).await;
    let params = RenderCtx {
        db: db.to_string(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
    };
    run_init(bootstrap, &params).await.expect("run_init");
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedSeriesRow {
    metric_name: String,
    fingerprint: u64,
    unix_milli: i64,
    labels: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ReadRowsRow {
    read_rows: u64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ProbeNameRow {
    metric_name: String,
}

async fn seed_series(client: &ChClient, rows: &[SeedSeriesRow]) {
    client
        .insert_block("metric_series", rows)
        .await
        .expect("seed metric_series");
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
        db: db.to_string(),
        series_table: "metric_series".to_string(),
        bucket_ms: DEFAULT_ACTIVITY_BUCKET_MS,
        window_ms: 24 * 3_600_000,
        cache_max_series: 50_000,
        ttl: Duration::from_secs(60),
        staleness_multiplier: 3,
    }
}

fn engine_config(db: &str, max_metric_fanout: u64) -> MetricsConfig {
    MetricsConfig {
        db: db.to_string(),
        samples_table: "metric_samples".to_string(),
        hist_samples_table: "metric_hist_samples".to_string(),
        series_table: "metric_series".to_string(),
        metadata_table: "metric_metadata".to_string(),
        experimental_functions: false,
        max_metric_fanout,
    }
}

/// A cold (never-refreshed → generation 0 → `ColdCache`) engine — every
/// name-matcher discovery filter resolves `Unresolvable`, driving the #96
/// probe fallback.
async fn cold_engine(db: &str, max_metric_fanout: u64) -> MetricsEngine {
    let cache_client = ChClient::new(test_config(db)).await.expect("cache client");
    let engine_client = ChClient::new(test_config(db)).await.expect("engine client");
    let cache = Arc::new(LabelCache::new(cache_client, cache_config(db)));
    assert!(!cache.is_warm(), "cold cache must not be warm");
    MetricsEngine::new(engine_client, cache, engine_config(db, max_metric_fanout))
}

/// A warmed engine — the name-matcher filter resolves through the resident
/// cache (`Groups`), the byte-frozen #89 warm path.
async fn warm_engine(db: &str, max_metric_fanout: u64) -> MetricsEngine {
    let cache_client = ChClient::new(test_config(db)).await.expect("cache client");
    let engine_client = ChClient::new(test_config(db)).await.expect("engine client");
    let cache = Arc::new(LabelCache::new(cache_client, cache_config(db)));
    cache.refresh().await.expect("refresh");
    assert!(cache.is_warm(), "warmed cache must be warm");
    MetricsEngine::new(engine_client, cache, engine_config(db, max_metric_fanout))
}

fn name_regex_filter(pattern: &str, job: Option<&str>) -> DiscoveryFilter {
    DiscoveryFilter {
        metric_name: None,
        name_matchers: vec![LabelMatcher {
            key: "__name__".to_string(),
            op: MatchOp::Re,
            value: pattern.to_string(),
        }],
        matchers: job
            .map(|v| {
                vec![LabelMatcher {
                    key: "job".to_string(),
                    op: MatchOp::Eq,
                    value: v.to_string(),
                }]
            })
            .unwrap_or_default(),
    }
}

/// AC1 + AC5: a regex-`__name__` selector under a degraded (cold) cache
/// returns results byte-identical to the warm path (the label matcher is
/// applied in the probe-derived fetch's SQL, not by the cache), and the
/// probe's `read_rows` is recorded (not gated — scale routes to #25).
#[tokio::test]
async fn degraded_regex_name_discovery_matches_the_warm_path_byte_for_byte() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_discovery_fallback_parity";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (seed)");

    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let recent_bucket = (now_ms() / bucket) * bucket;
    // Two metric names matching `up.*`; `up` carries a series that must be
    // excluded by the `job="api"` label matcher (proving the matcher is
    // applied in the fetch's SQL, not just at name resolution).
    seed_series(
        &client,
        &[
            SeedSeriesRow {
                metric_name: "up".to_string(),
                fingerprint: 1,
                unix_milli: recent_bucket,
                labels: r#"{"job":"api"}"#.to_string(),
            },
            SeedSeriesRow {
                metric_name: "up".to_string(),
                fingerprint: 2,
                unix_milli: recent_bucket,
                labels: r#"{"job":"web"}"#.to_string(),
            },
            SeedSeriesRow {
                metric_name: "up_alias".to_string(),
                fingerprint: 3,
                unix_milli: recent_bucket,
                labels: r#"{"job":"api"}"#.to_string(),
            },
        ],
    )
    .await;

    let window = DataWindow {
        start_ms: recent_bucket,
        end_ms: recent_bucket,
    };
    let filters = vec![name_regex_filter("up.*", Some("api"))];

    let warm = warm_engine(db, 1_000).await;
    let cold = cold_engine(db, 1_000).await;

    let warm_series = warm.series(&filters, window).await.expect("warm series");
    let cold_series = cold.series(&filters, window).await.expect("cold series");
    assert_eq!(
        warm_series, cold_series,
        "degraded /series must equal the warm path byte-for-byte"
    );
    // The label matcher was honored: `up{job="web"}` is excluded, so exactly
    // `up{job="api"}` and `up_alias{job="api"}` remain.
    assert_eq!(cold_series.len(), 2, "cold /series: {cold_series:?}");
    assert!(
        cold_series
            .iter()
            .all(|s| s.contains(&("job".to_string(), "api".to_string())))
    );

    let warm_names = warm
        .label_names(&filters, window)
        .await
        .expect("warm label_names");
    let cold_names = cold
        .label_names(&filters, window)
        .await
        .expect("cold label_names");
    assert_eq!(warm_names, cold_names, "degraded /labels must equal warm");
    assert!(cold_names.contains(&"__name__".to_string()));
    assert!(cold_names.contains(&"job".to_string()));

    let warm_metric_values = warm
        .label_values("__name__", &filters, window)
        .await
        .expect("warm label_values(__name__)");
    let cold_metric_values = cold
        .label_values("__name__", &filters, window)
        .await
        .expect("cold label_values(__name__)");
    assert_eq!(
        warm_metric_values, cold_metric_values,
        "degraded /label/__name__/values must equal warm"
    );
    assert_eq!(
        cold_metric_values,
        vec!["up".to_string(), "up_alias".to_string()]
    );

    // AC5: record the probe's read_rows (not gated). Run the exact probe SQL
    // a degraded name-matcher filter produces, under a query_id, then read
    // `system.query_log`.
    // The builder emits un-doubled SQL (the snapshot-testable contract);
    // the engine's `fetch_rows` doubles the literal `?` of the `^(?:…)$`
    // regex template at the execution boundary before it reaches the
    // `clickhouse` crate's `SqlBuilder`. Reproduce that here for the raw
    // recording query.
    let probe_sql = pulsus_read::metrics::sql::distinct_metric_names_probe(
        &format!("{db}.metric_series"),
        &filters[0].name_matchers,
        window,
        bucket,
        1_000,
    )
    .replace('?', "??");
    let query_id = "pulsus_read_it_discovery_fallback_probe";
    let mut stream = client
        .query_stream::<ProbeNameRow>(&probe_sql, &QuerySettings::new().set("query_id", query_id))
        .await
        .expect("run probe under query_id");
    // Drain (the probe returns metric names; we only care that it executed).
    while let Some(r) = stream.next().await {
        drop(r.expect("decode probe row"));
    }
    drop(stream);
    client
        .execute(
            "SYSTEM FLUSH LOGS",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("flush logs");
    let log_sql = format!(
        "SELECT read_rows FROM system.query_log \
         WHERE query_id = '{query_id}' AND type = 'QueryFinish' \
         ORDER BY event_time_microseconds DESC LIMIT 1"
    );
    let mut log_stream = client
        .query_stream::<ReadRowsRow>(&log_sql, &QuerySettings::new())
        .await
        .expect("query_log read");
    let mut read_rows = None;
    while let Some(r) = log_stream.next().await {
        read_rows = Some(r.expect("decode query_log row").read_rows);
    }
    eprintln!(
        "recorded (not gated, issue #25 for scale): degraded discovery probe read_rows={:?}",
        read_rows.expect("a QueryFinish row for the probe")
    );

    drop_database(&bootstrap, db).await;
}

/// AC3: a regex-`__name__` selector whose probed distinct-name set exceeds
/// `max_metric_fanout` is `QueryTooBroad(MetricFanout { matched, cap })` —
/// the names-only superset cap, enforced on the probe's RETURNED rows
/// (`LIMIT cap + 1`), never an unbounded `IN` set. Cold cache so the probe
/// path (not the warm resolver's own cap) is what trips.
#[tokio::test]
async fn degraded_regex_name_discovery_over_the_fanout_cap_is_query_too_broad() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_discovery_fallback_cap";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (seed)");

    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let recent_bucket = (now_ms() / bucket) * bucket;
    // Three distinct names all matching `up.*` → a probed name set of 3
    // against a cap of 2.
    seed_series(
        &client,
        &[
            SeedSeriesRow {
                metric_name: "up".to_string(),
                fingerprint: 1,
                unix_milli: recent_bucket,
                labels: r#"{"job":"api"}"#.to_string(),
            },
            SeedSeriesRow {
                metric_name: "up_alias".to_string(),
                fingerprint: 2,
                unix_milli: recent_bucket,
                labels: r#"{"job":"api"}"#.to_string(),
            },
            SeedSeriesRow {
                metric_name: "up_beta".to_string(),
                fingerprint: 3,
                unix_milli: recent_bucket,
                labels: r#"{"job":"api"}"#.to_string(),
            },
        ],
    )
    .await;

    let window = DataWindow {
        start_ms: recent_bucket,
        end_ms: recent_bucket,
    };
    let filters = vec![name_regex_filter("up.*", None)];

    let cold = cold_engine(db, 2).await;
    let err = cold
        .series(&filters, window)
        .await
        .expect_err("3 names over a cap of 2 must be rejected");
    match err {
        ReadError::QueryTooBroad(TooBroadReason::MetricFanout { matched, cap }) => {
            assert_eq!(matched, 3, "probe returns cap+1 (3) distinct names");
            assert_eq!(cap, 2);
        }
        other => panic!("expected QueryTooBroad(MetricFanout), got {other:?}"),
    }

    drop_database(&bootstrap, db).await;
}
