//! Live end-to-end tests for issue #31's `MetricsEngine`, against a real
//! ClickHouse. Gated behind `PULSUS_TEST_CLICKHOUSE=1`, mirroring
//! `live_metrics_cache.rs`'s (issue #30) precedent — same seeding style
//! (direct `ChClient::insert_block`, not through `pulsus-write`), same
//! `should_run`/`skip_unless_live!` gate, same per-test throwaway database.
//!
//! Covers the ACs that need real data + real ClickHouse execution:
//! `count`/`group`'s lookback-correct fetch+evaluate path (issue #33
//! architect adjudication removed the earlier zero-ClickHouse cache-only
//! fast path — its bucket-granularity resolution could not reproduce
//! PromQL's exact 5-minute staleness lookback), the historical variant
//! routing through `metric_series`, and the ratified fetch-concurrency
//! contract (both selectors of a binop query issue their fetches before
//! either completes). Pure, DB-free coverage (exact-semantics goldens,
//! SQL-plan snapshots) lives in `pulsus-promql`'s own test suite and
//! `src/metrics/{sample_sql,sql}.rs`.
//!
//! To run these:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test live_metrics_engine
//! podman rm -f pulsus-ch-test
//! ```

use std::sync::Arc;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_model::DEFAULT_ACTIVITY_BUCKET_MS;
use pulsus_promql::DEFAULT_LOOKBACK_MS;
use pulsus_promql::parser::parse;
use pulsus_read::{
    DataWindow, DiscoveryFilter, ExplainStage, FetchProbe, LabelCache, LabelCacheConfig,
    LabelMatcher, MatchOp, MetricQueryParams, MetricsConfig, MetricsEngine, PlanExplain,
    QueryResult,
};
use pulsus_schema::{RenderCtx, run_init};

fn stage<'a>(explain: &'a PlanExplain, name: &str) -> &'a ExplainStage {
    explain
        .stages
        .iter()
        .find(|s| s.name == name)
        .unwrap_or_else(|| panic!("no {name:?} stage in {:#?}", explain.stages))
}

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
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

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-read/tests/live_metrics_engine.rs for setup)"
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
struct SeedSampleRow {
    metric_name: String,
    fingerprint: u64,
    unix_milli: i64,
    value: f64,
}

/// M7-A5a: a `metric_hist_samples` seed row (column order matches the
/// catalog CREATE, RowBinary is positional).
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedHistRow {
    metric_name: String,
    fingerprint: u64,
    unix_milli: i64,
    schema: i8,
    zero_threshold: f64,
    zero_count: u64,
    count: u64,
    sum: f64,
    pos_span_offsets: Vec<i32>,
    pos_span_lengths: Vec<u32>,
    pos_bucket_deltas: Vec<i64>,
    neg_span_offsets: Vec<i32>,
    neg_span_lengths: Vec<u32>,
    neg_bucket_deltas: Vec<i64>,
    custom_values: Vec<f64>,
}

async fn seed_hist_samples(client: &ChClient, rows: &[SeedHistRow]) {
    client
        .insert_block("metric_hist_samples", rows)
        .await
        .expect("seed metric_hist_samples");
}

async fn seed_series(client: &ChClient, rows: &[SeedSeriesRow]) {
    client
        .insert_block("metric_series", rows)
        .await
        .expect("seed metric_series");
}

async fn seed_samples(client: &ChClient, rows: &[SeedSampleRow]) {
    client
        .insert_block("metric_samples", rows)
        .await
        .expect("seed metric_samples");
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

fn cache_config(db: &str, window_ms: i64) -> LabelCacheConfig {
    LabelCacheConfig {
        db: db.to_string(),
        series_table: "metric_series".to_string(),
        bucket_ms: DEFAULT_ACTIVITY_BUCKET_MS,
        window_ms,
        cache_max_series: 50_000,
        ttl: Duration::from_secs(60),
        staleness_multiplier: 3,
    }
}

fn engine_config(db: &str) -> MetricsConfig {
    MetricsConfig {
        db: db.to_string(),
        samples_table: "metric_samples".to_string(),
        hist_samples_table: "metric_hist_samples".to_string(),
        series_table: "metric_series".to_string(),
        metadata_table: "metric_metadata".to_string(),
        experimental_functions: false,
        max_metric_fanout: 1_000,
        max_cache_scan: 200_000,
        max_info_series: 100_000,
        distributed: false,
    }
}

/// **Issue #33 architect adjudication — WITHDRAWN AC, replaced.** The old
/// version of this test proved `count by (job) (up)` was served from the
/// label cache with **zero** ClickHouse sample queries — that AC (ratified
/// on #31) is withdrawn: the differential proved live that the cache's
/// activity-*bucket* granularity (1h) cannot distinguish "had a sample
/// within the 5-minute PromQL staleness lookback" from "active somewhere
/// in an up-to-24h-old 1-hour bucket"
/// (`count(mem_usage_bytes{service="svc-0"})`: 69 counted vs. Prometheus's
/// correct 57 — 12 series silent for >5m were wrongly still counted). The
/// `cache_answerable()` fast path is deleted from the product
/// (`pulsus-promql::plan::QueryPlan`) entirely, not merely narrowed
/// further — every `count`/`group` query, instant or range, now always
/// resolves -> fetches `metric_samples` -> evaluates, which is correct by
/// construction (the evaluator applies the real 5-minute lookback per
/// step, `pulsus-promql::eval::staleness`).
///
/// This replacement proves exactly the silent-series-must-be-excluded case
/// the old cache-only path could not: two `job="api"` series, one live and
/// one silent for far longer than the 5-minute lookback, must count as
/// `1`, not `2` — and the explain trace must show `sample_fetch` (real
/// ClickHouse I/O), never a `cache_only` stage (that stage name no longer
/// exists anywhere in this engine).
#[tokio::test]
async fn count_by_job_up_is_lookback_correct_and_excludes_a_silent_series() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_engine_count_lookback";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");
    let engine_client = ChClient::new(test_config(db))
        .await
        .expect("connect (engine client)");

    let now = now_ms();
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let recent_bucket = (now / bucket) * bucket;

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
                labels: r#"{"job":"api"}"#.to_string(),
            },
            SeedSeriesRow {
                metric_name: "up".to_string(),
                fingerprint: 3,
                unix_milli: recent_bucket,
                labels: r#"{"job":"web"}"#.to_string(),
            },
        ],
    )
    .await;
    seed_samples(
        &client,
        &[
            // fp1 (job=api): live, sampled at the query instant itself.
            SeedSampleRow {
                metric_name: "up".to_string(),
                fingerprint: 1,
                unix_milli: recent_bucket,
                value: 1.0,
            },
            // fp2 (job=api): its last sample is well outside the 5-minute
            // lookback — the same "active in the bucket, but not within
            // lookback of this instant" case the removed cache-only path
            // got wrong. Must be excluded from the count.
            SeedSampleRow {
                metric_name: "up".to_string(),
                fingerprint: 2,
                unix_milli: recent_bucket - (DEFAULT_LOOKBACK_MS + 60_000),
                value: 1.0,
            },
            // fp3 (job=web): live.
            SeedSampleRow {
                metric_name: "up".to_string(),
                fingerprint: 3,
                unix_milli: recent_bucket,
                value: 1.0,
            },
        ],
    )
    .await;

    let cache = Arc::new(LabelCache::new(
        cache_client,
        cache_config(db, 24 * 3_600_000),
    ));
    cache.refresh().await.expect("refresh");
    assert!(cache.is_warm());

    let engine = MetricsEngine::new(engine_client, cache, engine_config(db));
    let expr = parse("count by (job) (up)").expect("parse");
    let params = MetricQueryParams {
        start_ms: recent_bucket,
        end_ms: recent_bucket,
        step_ms: 0,
    };
    let (result, _annotations, explain) = engine
        .query_explained(&expr, &params)
        .await
        .expect("query_explained");
    stage(&explain, "sample_fetch");
    assert!(
        explain.stages.iter().all(|s| s.name != "cache_only"),
        "the cache_only stage no longer exists: {:#?}",
        explain.stages
    );
    match result {
        QueryResult::Vector(mut v) => {
            v.sort_by(|a, b| a.labels.cmp(&b.labels));
            assert_eq!(v.len(), 2, "expected two job groups, got {v:?}");
            assert_eq!(v[0].labels, vec![("job".to_string(), "api".to_string())]);
            assert_eq!(
                v[0].value, 1.0,
                "fp2 is silent for longer than the lookback and must be excluded"
            );
            assert_eq!(v[1].labels, vec![("job".to_string(), "web".to_string())]);
            assert_eq!(v[1].value, 1.0);
        }
        other => panic!("expected Vector, got {other:?}"),
    }

    drop_database(&bootstrap, db).await;
}

/// Issue #37 regression, end to end against real ClickHouse: a bare
/// selector's `/api/v1/query`-equivalent (`engine.query(&expr, ...)`) must
/// carry `__name__` — the label-set propagation seam
/// (`pulsus_promql::eval` -> `pulsus_read::metrics::exec::exec.rs`'s
/// `with_metric_name`) exercised through the full fetch+evaluate path,
/// not just the cache-only fast path the test above covers (which is an
/// aggregation, `count by (job)`, and correctly has *no* `__name__`).
#[tokio::test]
async fn bare_selector_query_keeps_metric_name_end_to_end() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_engine_name_keeps";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");
    let engine_client = ChClient::new(test_config(db))
        .await
        .expect("connect (engine client)");

    let now = now_ms();
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let recent_bucket = (now / bucket) * bucket;

    seed_series(
        &client,
        &[SeedSeriesRow {
            metric_name: "up".to_string(),
            fingerprint: 1,
            unix_milli: recent_bucket,
            labels: r#"{"job":"api"}"#.to_string(),
        }],
    )
    .await;
    seed_samples(
        &client,
        &[SeedSampleRow {
            metric_name: "up".to_string(),
            fingerprint: 1,
            unix_milli: recent_bucket,
            value: 1.0,
        }],
    )
    .await;

    let cache = Arc::new(LabelCache::new(
        cache_client,
        cache_config(db, 24 * 3_600_000),
    ));
    cache.refresh().await.expect("refresh");
    let engine = MetricsEngine::new(engine_client, cache, engine_config(db));

    // Bare selector: keeps __name__.
    let expr = parse("up").expect("parse");
    let params = MetricQueryParams {
        start_ms: recent_bucket,
        end_ms: recent_bucket,
        step_ms: 0,
    };
    match engine.query(&expr, &params).await.expect("query").0 {
        QueryResult::Vector(v) => {
            assert_eq!(v.len(), 1);
            assert!(
                v[0].labels
                    .contains(&("__name__".to_string(), "up".to_string())),
                "bare selector must keep __name__: {:?}",
                v[0].labels
            );
        }
        other => panic!("expected Vector, got {other:?}"),
    }

    // Aggregation over the same data: drops __name__.
    let expr = parse("sum(up)").expect("parse");
    match engine.query(&expr, &params).await.expect("query").0 {
        QueryResult::Vector(v) => {
            assert_eq!(v.len(), 1);
            assert!(
                !v[0].labels.iter().any(|(k, _)| k == "__name__"),
                "aggregation must drop __name__: {:?}",
                v[0].labels
            );
        }
        other => panic!("expected Vector, got {other:?}"),
    }

    drop_database(&bootstrap, db).await;
}

/// AC: the historical variant of `count by (job) (up)` — a window outside
/// the cache's residency — demonstrably routes through `metric_series`
/// (not the cache-only fast path, and not a false empty). Real sample data
/// is seeded and fetched via the ordinary evaluate path, proving the
/// fallback produces a correct, non-empty count from real ClickHouse data.
#[tokio::test]
async fn count_by_job_up_historical_variant_routes_through_metric_series() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_engine_historical";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");
    let engine_client = ChClient::new(test_config(db))
        .await
        .expect("connect (engine client)");

    let now = now_ms();
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    // 2 days ago: comfortably outside the 24h cache window below, but
    // safely inside the schema's 7-day raw retention TTL (unlike a
    // timestamp right at the 7-day boundary, which risks a background TTL
    // merge dropping the row between insert and read).
    let two_days_ms = 2 * 24 * 3_600_000;
    let last_week_bucket = ((now - two_days_ms) / bucket) * bucket;

    seed_series(
        &client,
        &[SeedSeriesRow {
            metric_name: "up".to_string(),
            fingerprint: 4242,
            unix_milli: last_week_bucket,
            labels: r#"{"job":"api"}"#.to_string(),
        }],
    )
    .await;
    seed_samples(
        &client,
        &[SeedSampleRow {
            metric_name: "up".to_string(),
            fingerprint: 4242,
            unix_milli: last_week_bucket,
            value: 1.0,
        }],
    )
    .await;

    // 24h cache window: last week's row is well outside it, forcing the
    // `SqlFallback` path for the per-selector resolution the ordinary
    // fetch+evaluate path performs (issue #33: the cache-only fast path
    // this comment used to also mention is removed — every count/group
    // query, in-window or historical, now always resolves this way).
    let cache = Arc::new(LabelCache::new(
        cache_client,
        cache_config(db, 24 * 3_600_000),
    ));
    cache.refresh().await.expect("refresh");
    assert!(cache.is_warm());

    let engine = MetricsEngine::new(engine_client, cache, engine_config(db));
    let expr = parse("count by (job) (up)").expect("parse");
    let params = MetricQueryParams {
        start_ms: last_week_bucket,
        end_ms: last_week_bucket,
        step_ms: 0,
    };
    let (result, _annotations) = engine.query(&expr, &params).await.expect("query");
    match result {
        QueryResult::Vector(v) => {
            assert_eq!(
                v.len(),
                1,
                "expected the historical series to be found, got {v:?}"
            );
            assert_eq!(v[0].labels, vec![("job".to_string(), "api".to_string())]);
            assert_eq!(v[0].value, 1.0);
        }
        other => panic!("expected Vector, got {other:?}"),
    }

    drop_database(&bootstrap, db).await;
}

/// Code review round 2: `group(up offset 2d)` — `offset` stays permitted
/// on `group`'s bare-instant-selector restriction, and (every `count`/
/// `group` query always resolves through the ordinary resolve+fetch path
/// since issue #33's removal of the cache-only fast path) demonstrably
/// falls back to `metric_series` here because the offset-shifted window
/// lands outside the cache's 24h residency — even though the query's own
/// `start_ms`/`end_ms` is "now" (inside the cache window), unlike the
/// plain historical-window test above.
#[tokio::test]
async fn group_with_offset_routes_through_metric_series() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_engine_group_offset";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");
    let engine_client = ChClient::new(test_config(db))
        .await
        .expect("connect (engine client)");

    let now = now_ms();
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let two_days_ms = 2 * 24 * 3_600_000;
    let recent_bucket = (now / bucket) * bucket;
    let two_days_ago_bucket = ((now - two_days_ms) / bucket) * bucket;

    // Only historical (2-days-ago) data exists — nothing at all "now" —
    // so a correct, non-empty result can only come from the offset
    // actually shifting the fetch window back and the fallback finding
    // the historical row via metric_series.
    seed_series(
        &client,
        &[SeedSeriesRow {
            metric_name: "up".to_string(),
            fingerprint: 777,
            unix_milli: two_days_ago_bucket,
            labels: r#"{"job":"api"}"#.to_string(),
        }],
    )
    .await;
    seed_samples(
        &client,
        &[SeedSampleRow {
            metric_name: "up".to_string(),
            fingerprint: 777,
            unix_milli: two_days_ago_bucket,
            value: 1.0,
        }],
    )
    .await;

    let cache = Arc::new(LabelCache::new(
        cache_client,
        cache_config(db, 24 * 3_600_000),
    ));
    cache.refresh().await.expect("refresh");
    assert!(cache.is_warm());

    let engine = MetricsEngine::new(engine_client, cache, engine_config(db));
    let expr = parse("group(up offset 2d)").expect("parse");
    // The query's own window is "now" — inside the cache's residency —
    // but the `offset` shifts the *effective* fetch window back by 2
    // days, which is what forces the `metric_series` fallback.
    let params = MetricQueryParams {
        start_ms: recent_bucket,
        end_ms: recent_bucket,
        step_ms: 0,
    };
    let (result, _annotations) = engine.query(&expr, &params).await.expect("query");
    match result {
        QueryResult::Vector(v) => {
            assert_eq!(
                v.len(),
                1,
                "expected the offset-shifted historical series to be found via metric_series, \
                 got {v:?}"
            );
            assert_eq!(v[0].value, 1.0);
        }
        other => panic!("expected Vector, got {other:?}"),
    }

    drop_database(&bootstrap, db).await;
}

/// Issue #40 regression (the #33 differential repro): `count by (service)
/// (up)` over `/api/v1/query_range`-equivalent (`step_ms != 0`) must
/// return a `Matrix` (one value per step), never a `Vector` — the
/// cache-only fast path (correct only for instant queries, per the
/// architect adjudication on #40) must not be taken here, so this must
/// come back through the ordinary fetch+evaluate path, which naturally
/// produces a per-step envelope with no special-casing.
#[tokio::test]
async fn count_by_service_up_over_query_range_returns_a_matrix_not_a_vector() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_engine_range_count_matrix";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");
    let engine_client = ChClient::new(test_config(db))
        .await
        .expect("connect (engine client)");

    let now = now_ms();
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let recent_bucket = (now / bucket) * bucket;
    let step_ms = 60_000;
    // 3 steps, both series present at every one of them — this test proves
    // the envelope shape and per-step values are correct in the steady
    // -state case; the non-constant-count (mid-window series start) case is
    // covered at the evaluator level in `pulsus-promql`
    // (`a_range_count_with_a_mid_window_series_start_has_non_constant_per_step_counts`),
    // where the exact 5-minute-lookback boundary can be pinned without a
    // live ClickHouse round trip.
    let t0 = recent_bucket;
    let t1 = t0 + step_ms;
    let t2 = t0 + 2 * step_ms;

    seed_series(
        &client,
        &[
            SeedSeriesRow {
                metric_name: "up".to_string(),
                fingerprint: 10,
                unix_milli: recent_bucket,
                labels: r#"{"service":"a"}"#.to_string(),
            },
            SeedSeriesRow {
                metric_name: "up".to_string(),
                fingerprint: 11,
                unix_milli: recent_bucket,
                labels: r#"{"service":"b"}"#.to_string(),
            },
        ],
    )
    .await;
    seed_samples(
        &client,
        &[
            SeedSampleRow {
                metric_name: "up".to_string(),
                fingerprint: 10,
                unix_milli: t0,
                value: 1.0,
            },
            SeedSampleRow {
                metric_name: "up".to_string(),
                fingerprint: 10,
                unix_milli: t1,
                value: 1.0,
            },
            SeedSampleRow {
                metric_name: "up".to_string(),
                fingerprint: 10,
                unix_milli: t2,
                value: 1.0,
            },
            SeedSampleRow {
                metric_name: "up".to_string(),
                fingerprint: 11,
                unix_milli: t0,
                value: 1.0,
            },
            SeedSampleRow {
                metric_name: "up".to_string(),
                fingerprint: 11,
                unix_milli: t1,
                value: 1.0,
            },
            SeedSampleRow {
                metric_name: "up".to_string(),
                fingerprint: 11,
                unix_milli: t2,
                value: 1.0,
            },
        ],
    )
    .await;

    let cache = Arc::new(LabelCache::new(
        cache_client,
        cache_config(db, 24 * 3_600_000),
    ));
    cache.refresh().await.expect("refresh");
    assert!(cache.is_warm());

    let engine = MetricsEngine::new(engine_client, cache, engine_config(db));
    let expr = parse("count by (service) (up)").expect("parse");
    let params = MetricQueryParams {
        start_ms: t0,
        end_ms: t2,
        step_ms,
    };
    let (result, _annotations) = engine.query(&expr, &params).await.expect("query");
    match result {
        QueryResult::Matrix(mut m) => {
            // `by (service)` splits into one group per distinct `service`
            // value — `a` and `b` each have exactly one series, so each
            // group's per-step count is a constant `1.0`.
            m.sort_by(|a, b| a.labels.cmp(&b.labels));
            assert_eq!(m.len(), 2, "expected two service groups, got {m:?}");
            assert_eq!(m[0].labels, vec![("service".to_string(), "a".to_string())]);
            assert_eq!(
                m[0].points,
                vec![(t0, 1.0), (t1, 1.0), (t2, 1.0)],
                "series `a` present at every step -> a constant per-step count of 1"
            );
            assert_eq!(m[1].labels, vec![("service".to_string(), "b".to_string())]);
            assert_eq!(
                m[1].points,
                vec![(t0, 1.0), (t1, 1.0), (t2, 1.0)],
                "series `b` present at every step -> a constant per-step count of 1"
            );
        }
        other => panic!("expected Matrix, got {other:?}"),
    }

    drop_database(&bootstrap, db).await;
}

/// Issue #40 routing assertions — **updated for issue #33's removal of the
/// cache-only fast path**: this used to guard that an *instant* `count
/// by(...) (up)` took the zero-ClickHouse `cache_only` path while the
/// identical selector as a *range* query took `sample_fetch`. That
/// asymmetry is gone (the architect adjudication on #33 withdrew the
/// ratified #31 zero-ClickHouse AC on differential evidence — see
/// `count_by_job_up_is_lookback_correct_and_excludes_a_silent_series`'s own
/// doc comment for the underlying bug): now both legs of the identical
/// selector always take `sample_fetch`, and `cache_only` no longer exists
/// as an explain stage name anywhere in this engine — this test guards
/// exactly that routing symmetry.
#[tokio::test]
async fn count_by_service_routes_sample_fetch_for_both_instant_and_range() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_engine_range_count_routing";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");
    let engine_client = ChClient::new(test_config(db))
        .await
        .expect("connect (engine client)");

    let now = now_ms();
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let recent_bucket = (now / bucket) * bucket;
    let step_ms = 60_000;
    let t0 = recent_bucket;
    let t1 = t0 + step_ms;

    seed_series(
        &client,
        &[SeedSeriesRow {
            metric_name: "up".to_string(),
            fingerprint: 20,
            unix_milli: recent_bucket,
            labels: r#"{"service":"a"}"#.to_string(),
        }],
    )
    .await;
    seed_samples(
        &client,
        &[
            SeedSampleRow {
                metric_name: "up".to_string(),
                fingerprint: 20,
                unix_milli: t0,
                value: 1.0,
            },
            SeedSampleRow {
                metric_name: "up".to_string(),
                fingerprint: 20,
                unix_milli: t1,
                value: 1.0,
            },
        ],
    )
    .await;

    let cache = Arc::new(LabelCache::new(
        cache_client,
        cache_config(db, 24 * 3_600_000),
    ));
    cache.refresh().await.expect("refresh");
    assert!(cache.is_warm());

    let engine = MetricsEngine::new(engine_client, cache, engine_config(db));
    let expr = parse("count by (service) (up)").expect("parse");

    // --- instant leg: sample_fetch, Vector ---
    let instant_params = MetricQueryParams {
        start_ms: t0,
        end_ms: t0,
        step_ms: 0,
    };
    let (instant_result, _annotations, instant_explain) = engine
        .query_explained(&expr, &instant_params)
        .await
        .expect("instant query_explained");
    stage(&instant_explain, "sample_fetch");
    assert!(
        instant_explain
            .stages
            .iter()
            .all(|s| s.name != "cache_only"),
        "the cache_only stage no longer exists: {:#?}",
        instant_explain.stages
    );
    match instant_result {
        QueryResult::Vector(v) => {
            assert_eq!(v.len(), 1);
            assert_eq!(v[0].value, 1.0);
        }
        other => panic!("expected Vector for the instant leg, got {other:?}"),
    }

    // --- range leg: sample_fetch, Matrix ---
    let range_params = MetricQueryParams {
        start_ms: t0,
        end_ms: t1,
        step_ms,
    };
    let (range_result, _annotations, range_explain) = engine
        .query_explained(&expr, &range_params)
        .await
        .expect("range query_explained");
    stage(&range_explain, "sample_fetch");
    assert!(
        range_explain.stages.iter().all(|s| s.name != "cache_only"),
        "the cache_only stage no longer exists: {:#?}",
        range_explain.stages
    );
    match range_result {
        QueryResult::Matrix(m) => {
            assert_eq!(m.len(), 1);
            assert_eq!(m[0].points, vec![(t0, 1.0), (t1, 1.0)]);
        }
        other => panic!("expected Matrix for the range leg, got {other:?}"),
    }

    drop_database(&bootstrap, db).await;
}

/// Ratified concurrency contract (issue #31 plan amendment §2): every
/// selector's fetch is issued before any of them completes. Issue #135
/// replaced the original wall-clock A/B timing comparison (flaky on
/// shared/virtualized hosts) with a deterministic mechanism assertion: a
/// [`FetchProbe`] installed via [`MetricsEngine::with_fetch_probe`] parks
/// every selector fetch at entry until released, so a `sum(foo) +
/// sum(bar)` query's two selector fetches can only both be observed
/// in-flight (`FetchProbe::in_flight() == 2`) if `query_inner`'s
/// `join_all` (`metrics/exec.rs:524-529`) truly dispatched them
/// concurrently — under a regression to a sequential per-selector loop,
/// the second selector's fetch is never even constructed until the first
/// completes, so `in_flight` can never reach 2 and the rendezvous below
/// times out. This is a causal proof, not a timing inference: it holds
/// under any scheduler, thread count, or ClickHouse round-trip latency.
/// **Do not reintroduce a wall-clock pass/fail bound here** — the TRIALS
/// loop below is kept strictly as informational `eprintln` logging (it
/// remains useful context when investigating a real perf regression) and
/// must never grow an `assert!` again.
// Multi-threaded runtime (unlike every other test in this file): the
// informational timing phase's two fetches must be able to overlap
// CPU-bound row decode work across real OS threads, not just I/O wait on
// a single thread, to produce a representative logged comparison.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_expression_fetches_both_sides_concurrently() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_engine_concurrency";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");
    let engine_client = ChClient::new(test_config(db))
        .await
        .expect("connect (engine client)");
    let probe_client = ChClient::new(test_config(db))
        .await
        .expect("connect (probe client)");

    let now = now_ms();
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let recent_bucket = (now / bucket) * bucket;

    // Enough rows per metric that a single selector's fetch takes
    // measurable time (a few thousand samples across many series is
    // usually enough to push a single fetch into double-digit
    // milliseconds against a local test ClickHouse).
    const SERIES_PER_METRIC: u64 = 300;
    const SAMPLES_PER_SERIES: i64 = 50;

    for metric in ["foo", "bar"] {
        let mut series_rows = Vec::new();
        let mut sample_rows = Vec::new();
        for s in 0..SERIES_PER_METRIC {
            let fp = if metric == "foo" { s } else { 100_000 + s };
            series_rows.push(SeedSeriesRow {
                metric_name: metric.to_string(),
                fingerprint: fp,
                unix_milli: recent_bucket,
                labels: format!(r#"{{"series":"{s}"}}"#),
            });
            for t in 0..SAMPLES_PER_SERIES {
                sample_rows.push(SeedSampleRow {
                    metric_name: metric.to_string(),
                    fingerprint: fp,
                    unix_milli: recent_bucket - t * 1_000,
                    value: t as f64,
                });
            }
        }
        seed_series(&client, &series_rows).await;
        seed_samples(&client, &sample_rows).await;
    }

    let cache = Arc::new(LabelCache::new(
        cache_client,
        cache_config(db, 24 * 3_600_000),
    ));
    cache.refresh().await.expect("refresh");

    // `Arc::clone` (not a deep copy): the cache's resident snapshot is
    // shared read-only state, and both engines below need their own
    // `MetricsEngine` (each owns a distinct `ChClient`) over the same
    // resolved series.
    let engine = MetricsEngine::new(engine_client, Arc::clone(&cache), engine_config(db));
    let params = MetricQueryParams {
        start_ms: recent_bucket,
        end_ms: recent_bucket,
        step_ms: 0,
    };

    let foo_expr = parse("sum(foo)").expect("parse");
    let bar_expr = parse("sum(bar)").expect("parse");
    let both_expr = parse("sum(foo) + sum(bar)").expect("parse");

    // Warm-up: runs the *concurrent* shape first so both pooled HTTP
    // connections the two-selector query needs are already warm (TCP
    // handshake done) before either timed measurement — otherwise the
    // concurrent phase alone would pay for establishing a second
    // connection, confounding the comparison with connection setup cost
    // rather than fetch concurrency.
    engine
        .query(&both_expr, &params)
        .await
        .expect("warm-up query");

    // Several trials of each phase, comparing *medians* — a single
    // measurement on a shared CI host is noisy enough to flip the
    // comparison either way; the median is far more stable.
    const TRIALS: usize = 7;
    let mut seq_trials = Vec::with_capacity(TRIALS);
    let mut concurrent_trials = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        // (A) Sequential: two independent queries, one per metric.
        let seq_start = std::time::Instant::now();
        engine
            .query(&foo_expr, &params)
            .await
            .expect("sequential foo query");
        engine
            .query(&bar_expr, &params)
            .await
            .expect("sequential bar query");
        seq_trials.push(seq_start.elapsed());

        // (B) Concurrent: one query whose two selectors fetch via `join_all`.
        let concurrent_start = std::time::Instant::now();
        engine
            .query(&both_expr, &params)
            .await
            .expect("concurrent binop query");
        concurrent_trials.push(concurrent_start.elapsed());
    }
    seq_trials.sort();
    concurrent_trials.sort();
    let seq_median = seq_trials[TRIALS / 2];
    let concurrent_median = concurrent_trials[TRIALS / 2];

    eprintln!(
        "sequential (2 queries) median: {seq_median:?} {seq_trials:?}, concurrent (1 binop \
         query) median: {concurrent_median:?} {concurrent_trials:?} (informational only — \
         issue #135; no pass/fail bound on this measurement)"
    );

    // Issue #135 mechanism assertion: a fresh `MetricsEngine` (its own
    // `ChClient`, sharing the already-warm `cache`) is built with a
    // `FetchProbe` installed. Every selector fetch this engine issues
    // parks at entry until `probe.release()`. Driving the binop query and
    // the rendezvous observer concurrently via `tokio::join!` means the
    // observer's poll loop and the query's two selector fetches interleave
    // on the same runtime: if `join_all` truly dispatches both selectors'
    // fetches before either completes, both park simultaneously and
    // `in_flight` reaches 2; under a regression to sequential per-selector
    // fetching, the second fetch is never even constructed until the
    // first completes (which cannot happen before release), so
    // `in_flight` can never exceed 1 and the 30s bound below — a liveness
    // bound, not a performance comparison — fires deterministically.
    let probe = FetchProbe::new();
    let probe_engine = MetricsEngine::new(probe_client, Arc::clone(&cache), engine_config(db))
        .with_fetch_probe(Arc::clone(&probe));

    let (query_res, rendezvous) = tokio::join!(probe_engine.query(&both_expr, &params), async {
        let seen = tokio::time::timeout(Duration::from_secs(30), async {
            while probe.in_flight() < 2 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        // ALWAYS release, even on a rendezvous timeout, so a mutated
        // (sequential-fetch) engine's query still completes and fails at
        // the assertion below instead of hanging forever on a parked
        // fetch.
        probe.release();
        seen
    });
    query_res.expect("binop query under probe");
    assert!(
        rendezvous.is_ok(),
        "both selector fetches were never observed in flight simultaneously within 30s — \
         regression to sequential per-selector fetching"
    );
    assert!(
        probe.max_in_flight() >= 2,
        "expected both selector fetches to be in flight at once (max_in_flight={}), proving \
         `query_inner`'s `join_all` dispatches every selector's fetch concurrently",
        probe.max_in_flight()
    );

    drop_database(&bootstrap, db).await;
}

/// A basic end-to-end correctness check that the whole `plan -> resolve ->
/// fetch -> evaluate` pipeline produces the right numbers against real
/// ClickHouse data, for a `rate()` over real, non-trivial samples.
#[tokio::test]
async fn rate_end_to_end_against_real_samples() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_engine_rate";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");
    let engine_client = ChClient::new(test_config(db))
        .await
        .expect("connect (engine client)");

    let now = now_ms();
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let recent_bucket = (now / bucket) * bucket;

    seed_series(
        &client,
        &[SeedSeriesRow {
            metric_name: "http_requests_total".to_string(),
            fingerprint: 55,
            unix_milli: recent_bucket,
            labels: r#"{"job":"api"}"#.to_string(),
        }],
    )
    .await;
    // Two samples just inside the `(recent_bucket - 60_000, recent_bucket]`
    // window (left-open: a sample exactly at the lower bound would be
    // excluded — mirrors `pulsus-promql`'s own
    // `evaluates_rate_over_a_matrix_selector` unit test), increasing by 60
    // over ~60s -> rate ~= 1.0/s.
    seed_samples(
        &client,
        &[
            SeedSampleRow {
                metric_name: "http_requests_total".to_string(),
                fingerprint: 55,
                unix_milli: recent_bucket - 59_999,
                value: 0.0,
            },
            SeedSampleRow {
                metric_name: "http_requests_total".to_string(),
                fingerprint: 55,
                unix_milli: recent_bucket,
                value: 60.0,
            },
        ],
    )
    .await;

    let cache = Arc::new(LabelCache::new(
        cache_client,
        cache_config(db, 24 * 3_600_000),
    ));
    cache.refresh().await.expect("refresh");

    let engine = MetricsEngine::new(engine_client, cache, engine_config(db));
    let expr = parse("rate(http_requests_total[1m])").expect("parse");
    let params = MetricQueryParams {
        start_ms: recent_bucket,
        end_ms: recent_bucket,
        step_ms: 0,
    };
    let (result, _annotations) = engine.query(&expr, &params).await.expect("query");
    match result {
        QueryResult::Vector(v) => {
            assert_eq!(v.len(), 1);
            assert!((v[0].value - 1.0).abs() < 1e-6, "got {}", v[0].value);
        }
        other => panic!("expected Vector, got {other:?}"),
    }

    drop_database(&bootstrap, db).await;
}

/// Issue #65 (M6-02): the experimental-function gate at the real
/// `MetricsEngine::query` boundary (the one seam the hermetic
/// production-path composition test in `pulsus-server::chconfig` cannot
/// cover — `ChClient::new` is async/connecting). Flag off:
/// `max_of(1, 1)` is rejected by name at plan time, before any fetch.
/// Flag on: it evaluates to scalar `1` with **zero** sample fetches (the
/// plan has no selectors — no `sample_fetch` explain stage may appear).
#[tokio::test]
async fn experimental_function_gate_applies_at_the_engine_query_boundary() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_engine_experimental_gate";
    init_db(&bootstrap, db).await;
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");
    let off_client = ChClient::new(test_config(db))
        .await
        .expect("connect (flag-off engine client)");
    let on_client = ChClient::new(test_config(db))
        .await
        .expect("connect (flag-on engine client)");

    let cache = Arc::new(LabelCache::new(
        cache_client,
        cache_config(db, 24 * 3_600_000),
    ));
    cache.refresh().await.expect("refresh");

    let expr = parse("max_of(1, 1)").expect("parse");
    let params = MetricQueryParams {
        start_ms: now_ms(),
        end_ms: now_ms(),
        step_ms: 0,
    };

    // Flag off (engine_config's default): a named rejection.
    let off_engine = MetricsEngine::new(off_client, Arc::clone(&cache), engine_config(db));
    let err = off_engine
        .query(&expr, &params)
        .await
        .expect_err("max_of must be rejected with the flag off");
    let msg = err.to_string();
    assert!(
        msg.contains("max_of") && msg.contains("experimental"),
        "rejection must name the function and the gate, got {msg:?}"
    );

    // Flag on: scalar 1, zero sample fetches.
    let on_engine = MetricsEngine::new(
        on_client,
        cache,
        MetricsConfig {
            experimental_functions: true,
            ..engine_config(db)
        },
    );
    let (result, _annotations, explain) = on_engine
        .query_explained(&expr, &params)
        .await
        .expect("max_of must evaluate with the flag on");
    match result {
        QueryResult::Scalar(v) => assert_eq!(v, 1.0),
        other => panic!("expected Scalar, got {other:?}"),
    }
    assert!(
        explain.stages.iter().all(|s| s.name != "sample_fetch"),
        "max_of(1, 1) has no selectors and must fetch nothing: {:#?}",
        explain.stages
    );

    drop_database(&bootstrap, db).await;
}

/// Issue #82 (retroactive re-review, Finding 1): the info() cardinality
/// cap on the WARM label-cache path (`LabelledResolution::Series`)
/// rejects BEFORE any sample fetch — the check runs on `pairs.len()`,
/// in-memory, before `build_chunk_sqls` is ever called. Three
/// `target_info` series over a configured cap of 2 must reject with the
/// named `InfoCardinality` error (which `prom_api::error` maps to `422
/// execution`), never attempt to fetch a single sample.
#[tokio::test]
async fn info_cardinality_cap_rejects_over_cap_before_materialization() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_info_cardinality";
    init_db(&bootstrap, db).await;
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");
    let engine_client = ChClient::new(test_config(db))
        .await
        .expect("connect (engine client)");

    let now = now_ms();
    // Three distinct target_info series — one more than the cap below —
    // plus one base series for `info(metric)` to (never get the chance
    // to) enrich.
    let info_series: Vec<SeedSeriesRow> = (0..3u64)
        .map(|i| SeedSeriesRow {
            metric_name: "target_info".to_string(),
            fingerprint: 900_000_000_000_000_000 + i,
            unix_milli: now,
            labels: format!(r#"{{"instance":"i{i}","job":"j"}}"#),
        })
        .collect();
    seed_series(&cache_client, &info_series).await;
    let info_samples: Vec<SeedSampleRow> = info_series
        .iter()
        .map(|s| SeedSampleRow {
            metric_name: s.metric_name.clone(),
            fingerprint: s.fingerprint,
            unix_milli: now,
            value: 1.0,
        })
        .collect();
    seed_samples(&cache_client, &info_samples).await;

    let base = SeedSeriesRow {
        metric_name: "metric".to_string(),
        fingerprint: 900_000_000_000_000_100,
        unix_milli: now,
        labels: r#"{"instance":"i0","job":"j"}"#.to_string(),
    };
    seed_series(&cache_client, std::slice::from_ref(&base)).await;
    seed_samples(
        &cache_client,
        &[SeedSampleRow {
            metric_name: base.metric_name.clone(),
            fingerprint: base.fingerprint,
            unix_milli: now,
            value: 1.0,
        }],
    )
    .await;

    let cache = Arc::new(LabelCache::new(
        cache_client,
        cache_config(db, 24 * 3_600_000),
    ));
    cache.refresh().await.expect("refresh");

    let engine = MetricsEngine::new(
        engine_client,
        cache,
        MetricsConfig {
            experimental_functions: true,
            max_info_series: 2,
            ..engine_config(db)
        },
    );

    let expr = parse("info(metric)").expect("parse");
    let params = MetricQueryParams {
        start_ms: now,
        end_ms: now,
        step_ms: 0,
    };
    let err = engine
        .query(&expr, &params)
        .await
        .expect_err("3 target_info series over a cap of 2 must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("info()") && msg.contains("cardinality") && msg.contains('2'),
        "named info() cardinality rejection naming the cap, got {msg:?}"
    );

    drop_database(&bootstrap, db).await;
}

/// Issue #82 (retroactive re-review, Finding 1) — the DEGRADED-path twin
/// of the warm-path cap test above: an out-of-cache-window query forces
/// `LabelledResolution::SqlFallback`, so the cap is enforced by the
/// `LIMIT cap+1` cardinality PROBE (`info_series_cardinality_probe`),
/// run and counted BEFORE the real (unbounded) `sample_fetch_subquery`
/// ever executes — never a post-fetch backstop.
#[tokio::test]
async fn info_cardinality_cap_rejects_over_cap_on_the_degraded_sql_fallback_path() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_info_cardinality_fallback";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");
    let engine_client = ChClient::new(test_config(db))
        .await
        .expect("connect (engine client)");

    let now = now_ms();
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    // 2 days ago: outside the 24h cache window below (forcing
    // `SqlFallback`), safely inside the 7-day raw retention TTL — the
    // `count_by_job_up_historical_variant_routes_through_metric_series`
    // precedent.
    let two_days_ms = 2 * 24 * 3_600_000;
    let last_week_bucket = ((now - two_days_ms) / bucket) * bucket;

    let info_series: Vec<SeedSeriesRow> = (0..3u64)
        .map(|i| SeedSeriesRow {
            metric_name: "target_info".to_string(),
            fingerprint: 900_000_000_000_001_000 + i,
            unix_milli: last_week_bucket,
            labels: format!(r#"{{"instance":"i{i}","job":"j"}}"#),
        })
        .collect();
    seed_series(&client, &info_series).await;
    let info_samples: Vec<SeedSampleRow> = info_series
        .iter()
        .map(|s| SeedSampleRow {
            metric_name: s.metric_name.clone(),
            fingerprint: s.fingerprint,
            unix_milli: last_week_bucket,
            value: 1.0,
        })
        .collect();
    seed_samples(&client, &info_samples).await;

    let base = SeedSeriesRow {
        metric_name: "metric".to_string(),
        fingerprint: 900_000_000_000_001_100,
        unix_milli: last_week_bucket,
        labels: r#"{"instance":"i0","job":"j"}"#.to_string(),
    };
    seed_series(&client, std::slice::from_ref(&base)).await;
    seed_samples(
        &client,
        &[SeedSampleRow {
            metric_name: base.metric_name.clone(),
            fingerprint: base.fingerprint,
            unix_milli: last_week_bucket,
            value: 1.0,
        }],
    )
    .await;

    let cache = Arc::new(LabelCache::new(
        cache_client,
        cache_config(db, 24 * 3_600_000),
    ));
    cache.refresh().await.expect("refresh");
    assert!(cache.is_warm());

    let engine = MetricsEngine::new(
        engine_client,
        cache,
        MetricsConfig {
            experimental_functions: true,
            max_info_series: 2,
            ..engine_config(db)
        },
    );

    let expr = parse("info(metric)").expect("parse");
    let params = MetricQueryParams {
        start_ms: last_week_bucket,
        end_ms: last_week_bucket,
        step_ms: 0,
    };
    let err = engine
        .query(&expr, &params)
        .await
        .expect_err("3 target_info series over a cap of 2 must be rejected (degraded path)");
    let msg = err.to_string();
    assert!(
        msg.contains("info()") && msg.contains("cardinality") && msg.contains('2'),
        "named info() cardinality rejection naming the cap, got {msg:?}"
    );

    drop_database(&bootstrap, db).await;
}

/// Issue #82 code-review round ([high] over-count fix): the degraded-path
/// cardinality probe counts DISTINCT series, never per-activity-bucket
/// `metric_series` rows. `metric_series` is written once per series PER
/// activity bucket (docs/schemas.md §2.1), so 2 distinct `target_info`
/// series active across 3 buckets yield 6 raw rows — over the cap of 2
/// under the pre-fix raw-row count (`LIMIT 3` would return 3 rows → a
/// FALSE 422), but exactly 2 under `SELECT DISTINCT fingerprint` → the
/// query must SUCCEED and enrich. The genuinely-over-cap distinct count
/// is covered by the sibling degraded-path test above (3 distinct > 2 →
/// still 422).
#[tokio::test]
async fn info_cardinality_probe_counts_distinct_series_not_activity_bucket_rows() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_info_cardinality_distinct";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");
    let engine_client = ChClient::new(test_config(db))
        .await
        .expect("connect (engine client)");

    let now = now_ms();
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    // 2 days ago (outside the 24h cache window → `SqlFallback`, inside
    // the 7-day retention), bucket-aligned; the range query below spans
    // buckets [b0, b0+2*bucket] so all three activity buckets fall inside
    // the probe's floored window.
    let two_days_ms = 2 * 24 * 3_600_000;
    let b0 = ((now - two_days_ms) / bucket) * bucket;
    let buckets = [b0, b0 + bucket, b0 + 2 * bucket];

    // 2 distinct target_info series × 3 activity buckets = 6 raw
    // metric_series rows in-window.
    let mut info_series: Vec<SeedSeriesRow> = Vec::new();
    let mut samples: Vec<SeedSampleRow> = Vec::new();
    for i in 0..2u64 {
        let fp = 900_000_000_000_002_000 + i;
        for &t in &buckets {
            info_series.push(SeedSeriesRow {
                metric_name: "target_info".to_string(),
                fingerprint: fp,
                unix_milli: t,
                labels: format!(r#"{{"instance":"i{i}","job":"j","data":"d{i}"}}"#),
            });
            samples.push(SeedSampleRow {
                metric_name: "target_info".to_string(),
                fingerprint: fp,
                unix_milli: t,
                value: 1.0,
            });
        }
    }
    seed_series(&client, &info_series).await;

    let base_fp = 900_000_000_000_002_100;
    seed_series(
        &client,
        &[SeedSeriesRow {
            metric_name: "metric".to_string(),
            fingerprint: base_fp,
            unix_milli: b0,
            labels: r#"{"instance":"i0","job":"j"}"#.to_string(),
        }],
    )
    .await;
    for &t in &buckets {
        samples.push(SeedSampleRow {
            metric_name: "metric".to_string(),
            fingerprint: base_fp,
            unix_milli: t,
            value: 1.0,
        });
    }
    seed_samples(&client, &samples).await;

    let cache = Arc::new(LabelCache::new(
        cache_client,
        cache_config(db, 24 * 3_600_000),
    ));
    cache.refresh().await.expect("refresh");
    assert!(cache.is_warm());

    let engine = MetricsEngine::new(
        engine_client,
        cache,
        MetricsConfig {
            experimental_functions: true,
            max_info_series: 2,
            ..engine_config(db)
        },
    );

    // Range query spanning all three activity buckets: 6 raw rows > cap,
    // 2 distinct series == cap — must succeed and actually enrich.
    let expr = parse("info(metric)").expect("parse");
    let params = MetricQueryParams {
        start_ms: b0,
        end_ms: b0 + 2 * bucket,
        step_ms: bucket,
    };
    let (result, _annotations) = engine.query(&expr, &params).await.expect(
        "2 distinct series across 3 activity buckets (6 raw rows) must NOT trip the cap of 2",
    );
    match result {
        QueryResult::Matrix(m) => {
            assert_eq!(m.len(), 1, "one enriched base series, got {m:?}");
            assert!(
                m[0].labels.iter().any(|(k, v)| k == "data" && v == "d0"),
                "the matching info series must actually enrich: {:?}",
                m[0].labels
            );
        }
        other => panic!("expected Matrix, got {other:?}"),
    }

    drop_database(&bootstrap, db).await;
}

/// Issue #66 (M6-03) perf gate at the exec boundary: a query with **no
/// selector at all** (`time()`, `vector(time())`) executes through the
/// real `MetricsEngine::query_explained` with zero resolve/fetch stages —
/// no `series_resolution`, no `sample_fetch`, no ClickHouse sample I/O.
/// The plan-level selector-identity assertions live in `pulsus-promql`'s
/// own tests; this proves the same story survives the fetch layer.
#[tokio::test]
async fn time_only_query_shapes_execute_with_zero_fetch_stages() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_engine_time_only";
    init_db(&bootstrap, db).await;
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");
    let engine_client = ChClient::new(test_config(db))
        .await
        .expect("connect (engine client)");

    let cache = Arc::new(LabelCache::new(
        cache_client,
        cache_config(db, 24 * 3_600_000),
    ));
    cache.refresh().await.expect("refresh");
    let engine = MetricsEngine::new(engine_client, cache, engine_config(db));

    let eval_ms = now_ms();
    let params = MetricQueryParams {
        start_ms: eval_ms,
        end_ms: eval_ms,
        step_ms: 0,
    };

    // time() -> the eval time in seconds, as a scalar.
    let expr = parse("time()").expect("parse");
    let (result, _annotations, explain) = engine
        .query_explained(&expr, &params)
        .await
        .expect("time() query");
    match result {
        QueryResult::Scalar(v) => assert_eq!(v, eval_ms as f64 / 1000.0),
        other => panic!("expected Scalar, got {other:?}"),
    }
    assert!(
        explain
            .stages
            .iter()
            .all(|s| s.name != "sample_fetch" && s.name != "series_resolution"),
        "time() has no selectors and must resolve/fetch nothing: {:#?}",
        explain.stages
    );

    // vector(time()) -> a one-element vector with the empty label set
    // (no __name__ spliced back in), same zero-fetch story.
    let expr = parse("vector(time())").expect("parse");
    let (result, _annotations, explain) = engine
        .query_explained(&expr, &params)
        .await
        .expect("vector(time()) query");
    match result {
        QueryResult::Vector(v) => {
            assert_eq!(v.len(), 1);
            assert!(
                v[0].labels.is_empty(),
                "vector(time()) must have an empty label set: {:?}",
                v[0].labels
            );
            assert_eq!(v[0].value, eval_ms as f64 / 1000.0);
        }
        other => panic!("expected Vector, got {other:?}"),
    }
    assert!(
        explain
            .stages
            .iter()
            .all(|s| s.name != "sample_fetch" && s.name != "series_resolution"),
        "vector(time()) has no selectors and must resolve/fetch nothing: {:#?}",
        explain.stages
    );

    drop_database(&bootstrap, db).await;
}

/// Code review round 1, finding 5 (architect adjudication AMEND):
/// `X-Pulsus-Explain`'s `sample_fetch` stage must carry the actual
/// generated SQL, not just a table name + series count. Covers both
/// fetch paths: the cache-hit chunk path (`sample_fetch`'s
/// `PREWHERE`/`ORDER BY` shape) and the `SqlFallback` path (the nested-
/// subquery `sample_fetch_subquery` shape).
#[tokio::test]
async fn explain_carries_the_real_generated_sample_fetch_sql() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_engine_explain";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");
    let engine_client = ChClient::new(test_config(db))
        .await
        .expect("connect (engine client)");

    let now = now_ms();
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let recent_bucket = (now / bucket) * bucket;

    seed_series(
        &client,
        &[SeedSeriesRow {
            metric_name: "up".to_string(),
            fingerprint: 1,
            unix_milli: recent_bucket,
            labels: r#"{"job":"api"}"#.to_string(),
        }],
    )
    .await;
    seed_samples(
        &client,
        &[SeedSampleRow {
            metric_name: "up".to_string(),
            fingerprint: 1,
            unix_milli: recent_bucket,
            value: 1.0,
        }],
    )
    .await;

    let cache = Arc::new(LabelCache::new(
        cache_client,
        cache_config(db, 24 * 3_600_000),
    ));
    cache.refresh().await.expect("refresh");
    let engine = MetricsEngine::new(engine_client, cache, engine_config(db));
    let params = MetricQueryParams {
        start_ms: recent_bucket,
        end_ms: recent_bucket,
        step_ms: 0,
    };

    // Cache-hit chunk path: `sum(up)` goes through the ordinary
    // per-selector fetch (every query does, since issue #33 removed the
    // count/group cache-only fast path), which resolves from the (warm,
    // in-window) cache.
    let expr = parse("sum(up)").expect("parse");
    let (_, _annotations, explain) = engine
        .query_explained(&expr, &params)
        .await
        .expect("query_explained");
    let fetch_stage = stage(&explain, "sample_fetch");
    assert!(
        fetch_stage
            .sql
            .contains("SELECT fingerprint, unix_milli, value"),
        "expected real sample_fetch SQL, got: {}",
        fetch_stage.sql
    );
    assert!(fetch_stage.sql.contains("FROM metric_samples"));
    assert!(fetch_stage.sql.contains("PREWHERE metric_name = 'up'"));
    let resolution_stage = stage(&explain, "series_resolution");
    assert!(resolution_stage.sql.contains("matching series"));

    drop_database(&bootstrap, db).await;
}

/// The `SqlFallback` path's `sample_fetch` stage carries the nested-
/// subquery shape (`fingerprint IN ( <subquery> )`), not the plain
/// explicit-list shape the cache-hit path uses.
#[tokio::test]
async fn explain_carries_the_fallback_subquery_sample_fetch_sql() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_engine_explain_fallback";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");
    let engine_client = ChClient::new(test_config(db))
        .await
        .expect("connect (engine client)");

    let now = now_ms();
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let two_days_ms = 2 * 24 * 3_600_000;
    let historical_bucket = ((now - two_days_ms) / bucket) * bucket;

    seed_series(
        &client,
        &[SeedSeriesRow {
            metric_name: "up".to_string(),
            fingerprint: 1,
            unix_milli: historical_bucket,
            labels: r#"{"job":"api"}"#.to_string(),
        }],
    )
    .await;
    seed_samples(
        &client,
        &[SeedSampleRow {
            metric_name: "up".to_string(),
            fingerprint: 1,
            unix_milli: historical_bucket,
            value: 1.0,
        }],
    )
    .await;

    // 24h cache window: the historical row is outside it, forcing the
    // SqlFallback path.
    let cache = Arc::new(LabelCache::new(
        cache_client,
        cache_config(db, 24 * 3_600_000),
    ));
    cache.refresh().await.expect("refresh");
    let engine = MetricsEngine::new(engine_client, cache, engine_config(db));
    let params = MetricQueryParams {
        start_ms: historical_bucket,
        end_ms: historical_bucket,
        step_ms: 0,
    };

    let expr = parse("sum(up)").expect("parse");
    let (_, _annotations, explain) = engine
        .query_explained(&expr, &params)
        .await
        .expect("query_explained");
    let fetch_stage = stage(&explain, "sample_fetch");
    assert!(
        fetch_stage
            .sql
            .contains("fingerprint IN (\nSELECT fingerprint\nFROM metric_series"),
        "expected the nested-subquery sample_fetch shape, got: {}",
        fetch_stage.sql
    );
    // Never the cache-hit path's plain explicit-list shape.
    assert!(!fetch_stage.sql.contains("IN (1"));

    drop_database(&bootstrap, db).await;
}

// ---------------------------------------------------------------------
// Discovery endpoints (issue #32: label_names/label_values/series/
// metadata/tsdb_status) — the HTTP surface's data needs.
// ---------------------------------------------------------------------

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedMetadataRow {
    metric_name: String,
    metric_type: String,
    help: String,
    unit: String,
    updated_ns: i64,
}

async fn seed_metadata(client: &ChClient, rows: &[SeedMetadataRow]) {
    client
        .insert_block("metric_metadata", rows)
        .await
        .expect("seed metric_metadata");
}

/// AC: `/series`/`/labels`/`/label/{name}/values` honor `start`/`end` with
/// bucket-aware bounds and return `__name__` — and, load-bearing for the
/// #30 handoff AC, a discovery query whose window is **narrower** than the
/// resident label cache's own residency window must not leak series that
/// are outside the discovery query's own window (proven here by seeding a
/// second, cache-resident series bucketed well before the query window and
/// asserting it is absent from every discovery result).
#[tokio::test]
async fn discovery_endpoints_honor_the_query_window_and_include_name() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_engine_discovery";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");
    let engine_client = ChClient::new(test_config(db))
        .await
        .expect("connect (engine client)");

    let now = now_ms();
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let recent_bucket = (now / bucket) * bucket;
    // A cache-resident series bucketed 3 buckets before `recent_bucket` —
    // inside the 24h cache residency window below, but outside the
    // discovery query's own narrow [recent_bucket, recent_bucket] window.
    let older_bucket = recent_bucket - 3 * bucket;

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
                unix_milli: older_bucket,
                labels: r#"{"job":"stale"}"#.to_string(),
            },
        ],
    )
    .await;

    let cache = Arc::new(LabelCache::new(
        cache_client,
        cache_config(db, 24 * 3_600_000),
    ));
    cache.refresh().await.expect("refresh");
    assert!(cache.is_warm());
    // Both fingerprints are cache-resident (proving the leak-check below is
    // meaningful: the cache's own superset genuinely contains the older,
    // out-of-window series).
    assert_eq!(cache.tsdb_snapshot().num_series, 2);

    let engine = MetricsEngine::new(engine_client, cache, engine_config(db));
    let window = DataWindow {
        start_ms: recent_bucket,
        end_ms: recent_bucket,
    };
    let filters = vec![DiscoveryFilter {
        metric_name: Some("up".to_string()),
        name_matchers: vec![],
        matchers: vec![],
    }];

    let series = engine.series(&filters, window).await.expect("series");
    assert_eq!(
        series.len(),
        1,
        "the older, out-of-window series must not leak into /series: {series:?}"
    );
    assert!(series[0].contains(&("__name__".to_string(), "up".to_string())));
    assert!(series[0].contains(&("job".to_string(), "api".to_string())));

    let names = engine
        .label_names(&filters, window)
        .await
        .expect("label_names");
    assert!(names.contains(&"__name__".to_string()));
    assert!(names.contains(&"job".to_string()));

    let values = engine
        .label_values("job", &filters, window)
        .await
        .expect("label_values");
    assert_eq!(values, vec!["api".to_string()]);

    let name_values = engine
        .label_values("__name__", &filters, window)
        .await
        .expect("label_values(__name__)");
    assert_eq!(name_values, vec!["up".to_string()]);

    // Widening the window to cover both buckets recovers the older series
    // too — proving the narrower result above was genuine window-filtering,
    // not a bug that always drops it.
    let wide_window = DataWindow {
        start_ms: older_bucket,
        end_ms: recent_bucket,
    };
    let wide_series = engine
        .series(&filters, wide_window)
        .await
        .expect("series (wide window)");
    assert_eq!(wide_series.len(), 2);

    drop_database(&bootstrap, db).await;
}

/// AC: an empty `match[]` (`filters == []`) for `/labels`/
/// `/label/{name}/values` is Prometheus's own "no filter" contract —
/// every series in the window, unfiltered.
#[tokio::test]
async fn label_names_with_no_filters_covers_every_metric_in_window() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_engine_discovery_unfiltered";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");
    let engine_client = ChClient::new(test_config(db))
        .await
        .expect("connect (engine client)");

    let now = now_ms();
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let recent_bucket = (now / bucket) * bucket;

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
                metric_name: "http_requests_total".to_string(),
                fingerprint: 2,
                unix_milli: recent_bucket,
                labels: r#"{"status":"200"}"#.to_string(),
            },
        ],
    )
    .await;

    let cache = Arc::new(LabelCache::new(
        cache_client,
        cache_config(db, 24 * 3_600_000),
    ));
    cache.refresh().await.expect("refresh");
    let engine = MetricsEngine::new(engine_client, cache, engine_config(db));
    let window = DataWindow {
        start_ms: recent_bucket,
        end_ms: recent_bucket,
    };

    let names = engine
        .label_names(&[], window)
        .await
        .expect("label_names (unfiltered)");
    assert!(names.contains(&"__name__".to_string()));
    assert!(names.contains(&"job".to_string()));
    assert!(names.contains(&"status".to_string()));

    let metric_names = engine
        .label_values("__name__", &[], window)
        .await
        .expect("label_values(__name__) (unfiltered)");
    assert_eq!(
        metric_names,
        vec!["http_requests_total".to_string(), "up".to_string()]
    );

    drop_database(&bootstrap, db).await;
}

/// A regex matcher (`=~`) narrows a discovery filter exactly like it
/// narrows a query selector — proven against real ClickHouse `match()`.
#[tokio::test]
async fn series_applies_regex_matchers() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_engine_discovery_regex";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");
    let engine_client = ChClient::new(test_config(db))
        .await
        .expect("connect (engine client)");

    let now = now_ms();
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let recent_bucket = (now / bucket) * bucket;

    seed_series(
        &client,
        &[
            SeedSeriesRow {
                metric_name: "http_requests_total".to_string(),
                fingerprint: 1,
                unix_milli: recent_bucket,
                labels: r#"{"status":"500"}"#.to_string(),
            },
            SeedSeriesRow {
                metric_name: "http_requests_total".to_string(),
                fingerprint: 2,
                unix_milli: recent_bucket,
                labels: r#"{"status":"200"}"#.to_string(),
            },
        ],
    )
    .await;

    let cache = Arc::new(LabelCache::new(
        cache_client,
        cache_config(db, 24 * 3_600_000),
    ));
    cache.refresh().await.expect("refresh");
    let engine = MetricsEngine::new(engine_client, cache, engine_config(db));
    let window = DataWindow {
        start_ms: recent_bucket,
        end_ms: recent_bucket,
    };
    let filters = vec![DiscoveryFilter {
        metric_name: Some("http_requests_total".to_string()),
        name_matchers: vec![],
        matchers: vec![LabelMatcher {
            key: "status".to_string(),
            op: MatchOp::Re,
            value: "5..".to_string(),
        }],
    }];

    let series = engine.series(&filters, window).await.expect("series");
    assert_eq!(series.len(), 1);
    assert!(series[0].contains(&("status".to_string(), "500".to_string())));

    drop_database(&bootstrap, db).await;
}

/// Code-review round-1 fix (matcher-only `match[]`): a `DiscoveryFilter`
/// with `metric_name: None` — the engine-level shape
/// `pulsus_promql::series_selector` now produces for a bare-matcher
/// `match[]` selector like `{job="api"}` — must resolve across **every**
/// metric name, not just one, proven against real ClickHouse with two
/// distinct metric families sharing the same `job` label.
#[tokio::test]
async fn series_with_a_matcher_only_filter_matches_across_metric_names() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_engine_discovery_matcher_only";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");
    let engine_client = ChClient::new(test_config(db))
        .await
        .expect("connect (engine client)");

    let now = now_ms();
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let recent_bucket = (now / bucket) * bucket;

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
                metric_name: "http_requests_total".to_string(),
                fingerprint: 2,
                unix_milli: recent_bucket,
                labels: r#"{"job":"api","code":"200"}"#.to_string(),
            },
            SeedSeriesRow {
                metric_name: "up".to_string(),
                fingerprint: 3,
                unix_milli: recent_bucket,
                labels: r#"{"job":"web"}"#.to_string(),
            },
        ],
    )
    .await;

    let cache = Arc::new(LabelCache::new(
        cache_client,
        cache_config(db, 24 * 3_600_000),
    ));
    cache.refresh().await.expect("refresh");
    let engine = MetricsEngine::new(engine_client, cache, engine_config(db));
    let window = DataWindow {
        start_ms: recent_bucket,
        end_ms: recent_bucket,
    };
    let filters = vec![DiscoveryFilter {
        metric_name: None,
        name_matchers: vec![],
        matchers: vec![LabelMatcher {
            key: "job".to_string(),
            op: MatchOp::Eq,
            value: "api".to_string(),
        }],
    }];

    let series = engine.series(&filters, window).await.expect("series");
    let names: Vec<&str> = series
        .iter()
        .map(|pairs| {
            pairs
                .iter()
                .find(|(k, _)| k == "__name__")
                .map(|(_, v)| v.as_str())
                .expect("__name__ present")
        })
        .collect();
    assert_eq!(
        series.len(),
        2,
        "expected both up and http_requests_total: {series:?}"
    );
    assert!(names.contains(&"up"));
    assert!(names.contains(&"http_requests_total"));
    // The job="web" series must not match.
    assert!(
        !series
            .iter()
            .any(|pairs| pairs.contains(&("job".to_string(), "web".to_string())))
    );

    drop_database(&bootstrap, db).await;
}

/// AC: `metadata` reads `metric_metadata`, collapsing the
/// `ReplacingMergeTree`'s version column to the latest write.
#[tokio::test]
async fn metadata_collapses_to_the_latest_write() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_engine_metadata";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");
    let engine_client = ChClient::new(test_config(db))
        .await
        .expect("connect (engine client)");

    seed_metadata(
        &client,
        &[
            SeedMetadataRow {
                metric_name: "up".to_string(),
                metric_type: "gauge".to_string(),
                help: "old help".to_string(),
                unit: "".to_string(),
                updated_ns: 1_000,
            },
            SeedMetadataRow {
                metric_name: "up".to_string(),
                metric_type: "gauge".to_string(),
                help: "1 if the target is healthy".to_string(),
                unit: "".to_string(),
                updated_ns: 2_000,
            },
            SeedMetadataRow {
                metric_name: "http_requests_total".to_string(),
                metric_type: "counter".to_string(),
                help: "total requests".to_string(),
                unit: "requests".to_string(),
                updated_ns: 1_000,
            },
        ],
    )
    .await;

    let cache = Arc::new(LabelCache::new(
        cache_client,
        cache_config(db, 24 * 3_600_000),
    ));
    cache.refresh().await.expect("refresh");
    let engine = MetricsEngine::new(engine_client, cache, engine_config(db));

    let all = engine.metadata(None, None).await.expect("metadata (all)");
    assert_eq!(all.len(), 2);
    let up = all.iter().find(|m| m.name == "up").expect("up metadata");
    assert_eq!(
        up.help, "1 if the target is healthy",
        "must collapse to the latest write"
    );

    let scoped = engine
        .metadata(Some("http_requests_total"), None)
        .await
        .expect("metadata (scoped)");
    assert_eq!(scoped.len(), 1);
    assert_eq!(scoped[0].metric_type, "counter");
    assert_eq!(scoped[0].unit, "requests");

    let limited = engine
        .metadata(None, Some(1))
        .await
        .expect("metadata (limited)");
    assert_eq!(limited.len(), 1);

    drop_database(&bootstrap, db).await;
}

/// AC: `status/tsdb` reports `numSeries`/top cardinality. Code-review
/// round-1 fix: `numSamples` was dropped (never a real Prometheus
/// `headStats` field, and serving it required a live ClickHouse `count()`
/// over `metric_samples`, violating the zero-ClickHouse contract) — this
/// test deliberately seeds **no** `metric_samples` rows at all, proving
/// `tsdb_status` never touches that table.
#[tokio::test]
async fn tsdb_status_reports_series_counts_with_zero_sample_table_access() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_engine_tsdb_status";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");
    let engine_client = ChClient::new(test_config(db))
        .await
        .expect("connect (engine client)");

    let now = now_ms();
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let recent_bucket = (now / bucket) * bucket;

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
        ],
    )
    .await;

    let cache = Arc::new(LabelCache::new(
        cache_client,
        cache_config(db, 24 * 3_600_000),
    ));
    cache.refresh().await.expect("refresh");
    let engine = MetricsEngine::new(engine_client, cache, engine_config(db));

    let status = engine.tsdb_status().await.expect("tsdb_status");
    assert_eq!(status.num_series, 2);
    assert_eq!(
        status.series_count_by_metric_name,
        vec![("up".to_string(), 2)]
    );

    drop_database(&bootstrap, db).await;
}

/// Issue #85 (M6-08c): the name-less/regex-`__name__` selector end-to-end
/// through the live engine — the round-1 review's live-reader test gap.
/// Proves, against real ClickHouse:
///   - name-regex filtering (the `other_metric` series never appears);
///   - per-series metric names in the rendered output (`__name__` spliced
///     per matched metric, not synthesized from a single spec name);
///   - a fingerprint shared across two metric names yields one series per
///     `(metric_name, fingerprint)` pair — no cross-group duplication or
///     merging;
///   - the explain trace carries exactly ONE flat `sample_fetch` whose
///     SQL is the `PREWHERE metric_name IN (…) … fingerprint IN (…)`
///     shape (one round trip, never a global scan or per-metric queries);
///   - a fan-out cap below the matched-name count fails with the named
///     query-too-broad error instead of fetching.
#[tokio::test]
async fn nameless_selector_fans_out_with_per_series_names_and_one_flat_in_set_fetch() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_engine_nameless_fanout";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");
    let engine_client = ChClient::new(test_config(db))
        .await
        .expect("connect (engine client)");

    let now = now_ms();
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let recent_bucket = (now / bucket) * bucket;

    // Fingerprint 1 exists under BOTH http_a_total and http_b_total (the
    // same label set fingerprints identically across metric names —
    // metric_fingerprint excludes __name__); other_metric must be pruned
    // by the name regex.
    seed_series(
        &client,
        &[
            SeedSeriesRow {
                metric_name: "http_a_total".to_string(),
                fingerprint: 1,
                unix_milli: recent_bucket,
                labels: r#"{"job":"api"}"#.to_string(),
            },
            SeedSeriesRow {
                metric_name: "http_b_total".to_string(),
                fingerprint: 1,
                unix_milli: recent_bucket,
                labels: r#"{"job":"api"}"#.to_string(),
            },
            SeedSeriesRow {
                metric_name: "other_metric".to_string(),
                fingerprint: 2,
                unix_milli: recent_bucket,
                labels: r#"{"job":"api"}"#.to_string(),
            },
        ],
    )
    .await;
    seed_samples(
        &client,
        &[
            SeedSampleRow {
                metric_name: "http_a_total".to_string(),
                fingerprint: 1,
                unix_milli: recent_bucket,
                value: 11.0,
            },
            SeedSampleRow {
                metric_name: "http_b_total".to_string(),
                fingerprint: 1,
                unix_milli: recent_bucket,
                value: 22.0,
            },
            SeedSampleRow {
                metric_name: "other_metric".to_string(),
                fingerprint: 2,
                unix_milli: recent_bucket,
                value: 99.0,
            },
        ],
    )
    .await;

    let cache = Arc::new(LabelCache::new(
        cache_client,
        cache_config(db, 24 * 3_600_000),
    ));
    cache.refresh().await.expect("refresh");
    assert!(cache.is_warm());
    let engine = MetricsEngine::new(engine_client, Arc::clone(&cache), engine_config(db));

    let expr = parse(r#"{__name__=~"http_.*", job="api"}"#).expect("parse");
    let params = MetricQueryParams {
        start_ms: recent_bucket,
        end_ms: recent_bucket,
        step_ms: 0,
    };
    let (result, _annotations, explain) = engine
        .query_explained(&expr, &params)
        .await
        .expect("query_explained");

    // Exactly one flat IN-set fetch — never one query per metric, never
    // an unfiltered scan.
    let fetches: Vec<_> = explain
        .stages
        .iter()
        .filter(|s| s.name == "sample_fetch")
        .collect();
    assert_eq!(fetches.len(), 1, "one flat fetch: {:#?}", explain.stages);
    let sql = &fetches[0].sql;
    assert!(
        sql.contains("PREWHERE metric_name IN ('http_a_total', 'http_b_total')"),
        "flat IN-set prune must name exactly the regex-matched metrics: {sql}"
    );
    assert!(sql.contains("fingerprint IN (1)"), "{sql}");
    assert!(!sql.contains("other_metric"), "{sql}");

    match result {
        QueryResult::Vector(mut v) => {
            v.sort_by(|a, b| a.labels.cmp(&b.labels));
            assert_eq!(v.len(), 2, "one series per (metric_name, fp): {v:?}");
            let name_of = |s: &pulsus_read::VectorSample| {
                s.labels
                    .iter()
                    .find(|(k, _)| k == "__name__")
                    .map(|(_, v)| v.clone())
                    .expect("per-series __name__ present")
            };
            let mut by_name: Vec<(String, f64)> = v.iter().map(|s| (name_of(s), s.value)).collect();
            by_name.sort_by(|a, b| a.0.cmp(&b.0));
            assert_eq!(
                by_name,
                vec![
                    ("http_a_total".to_string(), 11.0),
                    ("http_b_total".to_string(), 22.0),
                ],
                "per-series names with each series' own value"
            );
            assert!(
                v.iter()
                    .all(|s| s.labels.contains(&("job".to_string(), "api".to_string())))
            );
        }
        other => panic!("expected Vector, got {other:?}"),
    }

    // Cap below the matched-name count: the named fan-out error, before
    // any fetch.
    let capped_engine = MetricsEngine::new(
        ChClient::new(test_config(db))
            .await
            .expect("connect (capped engine client)"),
        cache,
        MetricsConfig {
            max_metric_fanout: 1,
            ..engine_config(db)
        },
    );
    let err = capped_engine
        .query(&expr, &params)
        .await
        .expect_err("fan-out above the cap must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("query too broad") && msg.contains("fan-out cap"),
        "named metric-fan-out rejection, got {msg:?}"
    );

    drop_database(&bootstrap, db).await;
}

/// Issue #85 code review round 1, finding 1 (live gap): a genuine
/// `(metric_name, fingerprint)` cross-pair the cache did NOT resolve —
/// the name is known via one fingerprint, the fingerprint via another
/// name, and the pair itself exists only in `metric_samples` (a series
/// registered inside the post-sweep recency gap). The flat IN×IN fetch
/// returns its rows; they must surface with the fingerprint's hydrated
/// (name-invariant) labels, NEVER an empty label set.
#[tokio::test]
async fn nameless_selector_hydrates_a_post_sweep_cross_pair_never_empty_labels() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_engine_cross_pair";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");
    let engine_client = ChClient::new(test_config(db))
        .await
        .expect("connect (engine client)");

    let now = now_ms();
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let recent_bucket = (now / bucket) * bucket;

    // Pre-sweep registrations: cross_a_total resolves fp1, cross_b_total
    // resolves fp2 — so the fan-out's IN lists contain BOTH names and
    // BOTH fingerprints, but the (cross_b_total, fp1) pair is unresolved.
    seed_series(
        &client,
        &[
            SeedSeriesRow {
                metric_name: "cross_a_total".to_string(),
                fingerprint: 1,
                unix_milli: recent_bucket,
                labels: r#"{"job":"api"}"#.to_string(),
            },
            SeedSeriesRow {
                metric_name: "cross_b_total".to_string(),
                fingerprint: 2,
                unix_milli: recent_bucket,
                labels: r#"{"job":"web"}"#.to_string(),
            },
        ],
    )
    .await;

    let cache = Arc::new(LabelCache::new(
        cache_client,
        cache_config(db, 24 * 3_600_000),
    ));
    cache.refresh().await.expect("refresh");
    assert!(cache.is_warm());

    // POST-sweep: the cross-pair's samples land (its metric_series row
    // would land too in production, but the resident snapshot predates
    // it — the sanctioned recency gap). fp1's labels are name-invariant
    // ({job:"api"}), known to the cache only via cross_a_total.
    seed_samples(
        &client,
        &[
            SeedSampleRow {
                metric_name: "cross_a_total".to_string(),
                fingerprint: 1,
                unix_milli: recent_bucket,
                value: 1.0,
            },
            SeedSampleRow {
                metric_name: "cross_b_total".to_string(),
                fingerprint: 2,
                unix_milli: recent_bucket,
                value: 2.0,
            },
            SeedSampleRow {
                metric_name: "cross_b_total".to_string(),
                fingerprint: 1,
                unix_milli: recent_bucket,
                value: 3.0,
            },
        ],
    )
    .await;

    let engine = MetricsEngine::new(engine_client, cache, engine_config(db));
    let expr = parse(r#"{__name__=~"cross_.*"}"#).expect("parse");
    let params = MetricQueryParams {
        start_ms: recent_bucket,
        end_ms: recent_bucket,
        step_ms: 0,
    };
    let (result, _annotations) = engine.query(&expr, &params).await.expect("query");

    match result {
        QueryResult::Vector(v) => {
            assert_eq!(v.len(), 3, "both resolved pairs + the cross-pair: {v:?}");
            assert!(
                v.iter()
                    .all(|s| s.labels.iter().any(|(k, _)| k != "__name__")),
                "no series may surface with empty (name-only) labels: {v:?}"
            );
            let cross = v
                .iter()
                .find(|s| {
                    s.labels
                        .contains(&("__name__".to_string(), "cross_b_total".to_string()))
                        && s.value == 3.0
                })
                .unwrap_or_else(|| panic!("cross-pair series missing: {v:?}"));
            assert!(
                cross
                    .labels
                    .contains(&("job".to_string(), "api".to_string())),
                "cross-pair must carry fp1's hydrated name-invariant labels: {cross:?}"
            );
        }
        other => panic!("expected Vector, got {other:?}"),
    }

    drop_database(&bootstrap, db).await;
}

/// M7-A5a end-to-end against real ClickHouse: the metrics read path
/// UNCONDITIONALLY dual-reads `metric_samples` + `metric_hist_samples`,
/// merges by `unix_milli`, and decodes histogram value columns into the
/// value model. Proven by:
///  - a **float** metric queried instant returns its float value unchanged
///    (the dual-read leaves the float path byte-identical — its
///    complementary histogram read is empty), and
///  - a **histogram** metric queried instant AND range reaches the value
///    model as a histogram-valued result (`h: Some`) — observable as the
///    A5a `HistogramResultUnsupported` rejection, which is only reachable
///    if the hist row was fetched, merged, and decoded (a fetch/merge/decode
///    failure would instead yield an empty vector or a decode error).
#[tokio::test]
async fn dual_read_merges_and_decodes_histogram_samples_end_to_end() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_dual_read_hist";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target)");
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache)");
    let engine_client = ChClient::new(test_config(db))
        .await
        .expect("connect (engine)");

    let now = now_ms();
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let recent_bucket = (now / bucket) * bucket;

    // Two series: a float `up{job="api"}` (fp 1) and a native-histogram
    // `req_seconds{job="api"}` (fp 2). Both registered in metric_series so
    // the label cache resolves them; the float goes to metric_samples, the
    // histogram to metric_hist_samples.
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
                metric_name: "req_seconds".to_string(),
                fingerprint: 2,
                unix_milli: recent_bucket,
                labels: r#"{"job":"api"}"#.to_string(),
            },
        ],
    )
    .await;
    seed_samples(
        &client,
        &[SeedSampleRow {
            metric_name: "up".to_string(),
            fingerprint: 1,
            unix_milli: recent_bucket,
            value: 42.0,
        }],
    )
    .await;
    seed_hist_samples(
        &client,
        &[SeedHistRow {
            metric_name: "req_seconds".to_string(),
            fingerprint: 2,
            unix_milli: recent_bucket,
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 4,
            sum: 5.0,
            pos_span_offsets: vec![0],
            pos_span_lengths: vec![3],
            pos_bucket_deltas: vec![1, 1, -1],
            neg_span_offsets: vec![],
            neg_span_lengths: vec![],
            neg_bucket_deltas: vec![],
            custom_values: vec![],
        }],
    )
    .await;

    let cache = Arc::new(LabelCache::new(
        cache_client,
        cache_config(db, 24 * 3_600_000),
    ));
    cache.refresh().await.expect("refresh");
    assert!(cache.is_warm());

    let engine = MetricsEngine::new(engine_client, cache, engine_config(db));
    let params = MetricQueryParams {
        start_ms: recent_bucket,
        end_ms: recent_bucket,
        step_ms: 0,
    };

    // The float metric converts unchanged (dual-read's complementary hist
    // read is empty for `up`).
    let (float, _annotations) = engine
        .query(&parse("up").expect("parse"), &params)
        .await
        .expect("float query ok");
    match float {
        QueryResult::Vector(v) => {
            assert_eq!(v.len(), 1);
            assert_eq!(v[0].value, 42.0);
        }
        other => panic!("expected Vector, got {other:?}"),
    }

    // The histogram metric was fetched from metric_hist_samples, merged,
    // and decoded — reaching the value model as a histogram and (M7-A5b-i)
    // now ENCODING as `QueryResult::VectorHist` instead of the A5a
    // `HistogramResultUnsupported` reject: proves the hist row was
    // dual-read, merged, decoded, and `to_float`'d end to end.
    let (hist_instant, _annotations) = engine
        .query(&parse("req_seconds").expect("parse"), &params)
        .await
        .expect("histogram instant query ok");
    match hist_instant {
        QueryResult::VectorHist(v) => {
            assert_eq!(v.len(), 1);
            match &v[0].value {
                pulsus_read::logql::HistOrFloat::Hist(h) => {
                    assert_eq!(h.count, 4.0);
                    assert_eq!(h.sum, 5.0);
                }
                other => panic!("expected Hist, got {other:?}"),
            }
        }
        other => panic!("expected VectorHist, got {other:?}"),
    }

    // The matrix path is exercised independently (range query).
    let range_params = MetricQueryParams {
        start_ms: recent_bucket,
        end_ms: recent_bucket + 60_000,
        step_ms: 60_000,
    };
    let (hist_range, _annotations) = engine
        .query(&parse("req_seconds").expect("parse"), &range_params)
        .await
        .expect("histogram range query ok");
    match hist_range {
        QueryResult::MatrixHist(m) => {
            assert_eq!(m.len(), 1);
            assert!(
                m[0].points
                    .iter()
                    .any(|(_, v)| matches!(v, pulsus_read::logql::HistOrFloat::Hist(_))),
                "range materialization carries the histogram through"
            );
        }
        other => panic!("expected MatrixHist, got {other:?}"),
    }

    drop_database(&bootstrap, db).await;
}
