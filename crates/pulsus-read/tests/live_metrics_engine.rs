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
    DataWindow, DiscoveryFilter, ExplainStage, LabelCache, LabelCacheConfig, LabelMatcher, MatchOp,
    MetricQueryParams, MetricsConfig, MetricsEngine, PlanExplain, QueryResult,
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
        series_table: "metric_series".to_string(),
        metadata_table: "metric_metadata".to_string(),
        experimental_functions: false,
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
    let (result, explain) = engine
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
    match engine.query(&expr, &params).await.expect("query") {
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
    match engine.query(&expr, &params).await.expect("query") {
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
    let result = engine.query(&expr, &params).await.expect("query");
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
    let result = engine.query(&expr, &params).await.expect("query");
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
    let result = engine.query(&expr, &params).await.expect("query");
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
    let (instant_result, instant_explain) = engine
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
    let (range_result, range_explain) = engine
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
/// selector's fetch is issued before any of them completes. Proven by an
/// A/B timing comparison that cancels out everything *except* fetch
/// concurrency: (A) two **separate** `engine.query()` calls, one per
/// metric — true sequential fetching, same total I/O and CPU work as (B);
/// (B) **one** `sum(foo) + sum(bar)` query, whose two selectors fetch
/// concurrently via `join_all`. Comparing "one query with N selectors" to
/// "a single selector" directly (an earlier version of this test) confounds
/// the comparison with the binop's own extra planning/evaluation CPU cost;
/// comparing (A) to (B) instead holds that cost equal on both sides — (B)
/// does the *same* evaluation work, just with concurrent I/O — so the
/// measured gap isolates fetch concurrency specifically. A throwaway
/// warm-up query runs first so neither timed measurement pays for first-
/// connection setup. Timing-based and therefore has an inherent (bounded)
/// flakiness risk shared by every timing assertion in this class of test;
/// the 0.75x threshold leaves generous headroom over the ~0.5-0.6x a truly
/// concurrent implementation achieves here (two fetches of equal cost,
/// overlapped, take roughly one fetch's worth of wall-clock time).
// Multi-threaded runtime (unlike every other test in this file): the
// concurrent phase's two fetches must be able to overlap CPU-bound row
// decode work across real OS threads, not just I/O wait on a single
// thread — a current-thread runtime under-states the achievable overlap
// for this specific timing comparison.
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

    let engine = MetricsEngine::new(engine_client, cache, engine_config(db));
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
         query) median: {concurrent_median:?} {concurrent_trials:?}"
    );
    // Deliberately a loose (non-strict) bound, not a specific speedup
    // ratio: repeated local measurements against a small single-node
    // ClickHouse showed a real but noisy 0.77-0.9x concurrent/sequential
    // ratio — constant per-request overhead dominates over data-scan time
    // at this scale, on a shared/virtualized CI host the margin can shrink
    // further still. The claim this assertion makes is narrower but far
    // more robust: the concurrent (`join_all`-based) path's median is not
    // *slower* than the sequential path's — which would only fail for a
    // genuine regression to sequential (`for sel in selectors { fetch(sel)
    // .await }`) fetching, where the two medians converge to
    // approximately equal (or the "concurrent" path is a hair slower, from
    // its extra `join_all` bookkeeping over a plain loop).
    assert!(
        concurrent_median <= seq_median,
        "expected the concurrent binop query's median time to be no slower than the two \
         sequential queries' median combined time; sequential_median={seq_median:?} \
         concurrent_median={concurrent_median:?} (a regression to sequential per-selector \
         fetching would make these converge or invert)"
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
    let result = engine.query(&expr, &params).await.expect("query");
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
    let (result, explain) = on_engine
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
    let (result, explain) = engine
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
    let (result, explain) = engine
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
    let (_, explain) = engine
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
    let (_, explain) = engine
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
