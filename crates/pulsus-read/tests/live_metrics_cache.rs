//! Live end-to-end tests for issue #30's time-aware label cache, against a
//! real ClickHouse. Gated behind `PULSUS_TEST_CLICKHOUSE=1`, mirroring
//! `pulsus-write`'s `live_metric_writer.rs`/`pulsus-schema`'s
//! `live_schema.rs` precedent. `metric_series` rows are seeded via a direct
//! `ChClient::insert_block` (not through `pulsus-write`'s `MetricWriter`,
//! per the task's KISS-testing instruction: this crate doesn't depend on
//! `pulsus-write`, and a direct insert exercises exactly the schema shape
//! the cache reads without pulling in the whole writer pipeline).
//!
//! Covers the ACs that need real data + real ClickHouse execution: the
//! "silent series" false-empty-avoidance correctness rule, the sub-hour
//! bucket-floor boundary, the cache-vs-SQL differential (cold and warm),
//! and end-to-end injection/regex parity (quote/backslash-bearing label
//! keys, an `=~` selector) — the pure, DB-free unit-level coverage for
//! escaping/branching lives in `src/metrics/{sql,labels}.rs`.
//!
//! To run these:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test live_metrics_cache
//! podman rm -f pulsus-ch-test
//! ```

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_model::{DEFAULT_ACTIVITY_BUCKET_MS, floor_to_activity_bucket};
use pulsus_read::metrics::sql::{historical_resolution_query, historical_series_subquery};
use pulsus_read::{
    DataWindow, LabelCache, LabelCacheConfig, LabelMatcher, MatchOp, Resolution, SeriesResolver,
};
use pulsus_schema::{RenderCtx, run_init};

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
        pool_size: 4,
        query_timeout: Duration::from_secs(20),
        ..ChConnConfig::default()
    }
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-read/tests/live_metrics_cache.rs for setup)"
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

/// Mirrors `pulsus_write::writer::rows::MetricSeriesRow`'s wire shape
/// exactly (this crate does not depend on `pulsus-write`) — one row seeded
/// directly into `metric_series`.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedSeriesRow {
    metric_name: String,
    fingerprint: u64,
    unix_milli: i64,
    labels: String,
}

async fn seed(client: &ChClient, rows: &[SeedSeriesRow]) {
    client
        .insert_block("metric_series", rows)
        .await
        .expect("seed metric_series");
}

/// Doubles literal `?` before executing raw SQL text against the real
/// server — the execution-boundary contract `metrics::sql`'s doc comment
/// documents (mirrors `logql::exec::escape_query_placeholders`, which is
/// private to that module; the primitive is trivial enough to reimplement
/// here rather than expose it crate-wide for one test file).
fn double_placeholders(sql: &str) -> String {
    sql.replace('?', "??")
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
struct FingerprintRow {
    fingerprint: u64,
}

async fn execute_fingerprint_sql(client: &ChClient, sql: &str) -> Vec<u64> {
    let doubled = double_placeholders(sql);
    let mut stream = client
        .query_stream::<FingerprintRow>(&doubled, &QuerySettings::new())
        .await
        .expect("execute fallback sql");
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode FingerprintRow").fingerprint);
    }
    out.sort_unstable();
    out
}

fn cache_config(db: &str, series_table: &str, window_ms: i64, ttl: Duration) -> LabelCacheConfig {
    LabelCacheConfig {
        db: db.to_string(),
        series_table: series_table.to_string(),
        bucket_ms: DEFAULT_ACTIVITY_BUCKET_MS,
        window_ms,
        cache_max_series: 50_000,
        ttl,
        staleness_multiplier: 3,
    }
}

/// The whole point of the time-awareness invariant (docs/architecture.md
/// §5.2): a series alive last week but silent today must be absent from
/// the (24h-windowed) cache, and a historical query for last week must
/// still resolve it via `metric_series` — never a false empty.
#[tokio::test]
async fn silent_last_week_series_is_absent_from_the_cache_but_resolves_via_metric_series() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_silent";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");
    // `LabelCache::new` takes ownership of a `ChClient` (it is not `Clone`),
    // so the cache gets its own connection to the same test database.
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");

    let now_ms = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("now fits in i64");
    let one_week_ms = 7 * 24 * 3_600_000;
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let last_week_bucket = ((now_ms - one_week_ms) / bucket) * bucket;

    seed(
        &client,
        &[SeedSeriesRow {
            metric_name: "up".to_string(),
            fingerprint: 4242,
            unix_milli: last_week_bucket,
            labels: r#"{"job":"api"}"#.to_string(),
        }],
    )
    .await;

    // 24h cache window: the last-week row falls well outside it.
    let cache = LabelCache::new(
        cache_client,
        cache_config(db, "metric_series", 24 * 3_600_000, Duration::from_secs(60)),
    );
    cache.refresh().await.expect("refresh");
    assert!(cache.is_warm());

    // A historical query window covering last week: must NOT be answered
    // from the (recent-only) cache.
    let window = DataWindow {
        start_ms: last_week_bucket - bucket,
        end_ms: last_week_bucket + bucket,
    };
    let resolution = cache.resolve("up", &[], window);
    let sql = match resolution {
        Resolution::SqlFallback { sql, reason } => {
            assert_eq!(reason, pulsus_read::FallbackReason::OutOfWindow);
            sql
        }
        other => panic!("expected SqlFallback(OutOfWindow), got {other:?}"),
    };

    // Executing the rendered fallback SQL against the real server must find
    // the series — proving the historical path never returns a false
    // empty for a series the cache itself cannot see.
    let fingerprints = execute_fingerprint_sql(&client, &sql).await;
    assert_eq!(fingerprints, vec![4242]);

    drop_database(&bootstrap, db).await;
}

/// Sub-hour bucket-floor boundary (docs/schemas.md §2.1): a 10:30-10:40
/// query must match a series whose only `metric_series` row is bucketed at
/// 10:00; a series first seen only after the query window must be excluded
/// by the upper bound.
#[tokio::test]
async fn bucket_floor_boundary_includes_the_mid_bucket_row_and_excludes_the_later_one() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_bucket_floor";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");

    let bucket = DEFAULT_ACTIVITY_BUCKET_MS; // 1h
    let ten_am_bucket = 10 * 3_600_000i64; // an arbitrary "10:00" epoch-relative instant
    let eleven_am_bucket = 11 * 3_600_000i64;

    seed(
        &client,
        &[
            SeedSeriesRow {
                metric_name: "up".to_string(),
                fingerprint: 1,
                unix_milli: ten_am_bucket, // 10:00-bucketed
                labels: "{}".to_string(),
            },
            SeedSeriesRow {
                metric_name: "up".to_string(),
                fingerprint: 2,
                unix_milli: eleven_am_bucket, // first seen at 11:00, after the query window
                labels: "{}".to_string(),
            },
        ],
    )
    .await;

    // 10:30-10:40 window.
    let window = DataWindow {
        start_ms: ten_am_bucket + 30 * 60_000,
        end_ms: ten_am_bucket + 40 * 60_000,
    };
    let sql = historical_series_subquery("metric_series", "up", window, bucket, &[]);
    let fingerprints = execute_fingerprint_sql(&client, &sql).await;
    assert_eq!(
        fingerprints,
        vec![1],
        "the 10:00-bucketed row must match a 10:30-10:40 query; the 11:00 row must not"
    );

    drop_database(&bootstrap, db).await;
}

/// Cache-vs-SQL differential: a warm, in-window `resolve()` and the
/// standalone `historical_resolution_query` executed directly must return
/// identical fingerprint sets for the same selector.
#[tokio::test]
async fn warm_cache_and_sql_fallback_return_identical_results() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_differential";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");

    let now_ms = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("now fits in i64");
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let recent_bucket = (now_ms / bucket) * bucket;

    seed(
        &client,
        &[
            SeedSeriesRow {
                metric_name: "http_requests_total".to_string(),
                fingerprint: 10,
                unix_milli: recent_bucket,
                labels: r#"{"status":"500"}"#.to_string(),
            },
            SeedSeriesRow {
                metric_name: "http_requests_total".to_string(),
                fingerprint: 20,
                unix_milli: recent_bucket,
                labels: r#"{"status":"503"}"#.to_string(),
            },
            SeedSeriesRow {
                metric_name: "http_requests_total".to_string(),
                fingerprint: 30,
                unix_milli: recent_bucket,
                labels: r#"{"status":"200"}"#.to_string(),
            },
        ],
    )
    .await;

    let cache = LabelCache::new(
        cache_client,
        cache_config(db, "metric_series", 24 * 3_600_000, Duration::from_secs(60)),
    );
    cache.refresh().await.expect("refresh");

    let matcher = LabelMatcher {
        key: "status".to_string(),
        op: MatchOp::Re,
        value: "5..".to_string(),
    };
    let window = DataWindow {
        start_ms: recent_bucket - bucket,
        end_ms: now_ms,
    };

    let in_process = match cache.resolve(
        "http_requests_total",
        std::slice::from_ref(&matcher),
        window,
    ) {
        Resolution::Fingerprints(fps) => fps,
        other => panic!("expected a cache hit, got {other:?}"),
    };

    let sql = historical_resolution_query(
        "metric_series",
        "http_requests_total",
        window,
        bucket,
        &[matcher],
    );
    let via_sql: Vec<u64> = {
        #[derive(Row, serde::Serialize, serde::Deserialize)]
        struct LabelsRow {
            fingerprint: u64,
            #[allow(dead_code)]
            labels: String,
        }
        let doubled = double_placeholders(&sql);
        let mut stream = client
            .query_stream::<LabelsRow>(&doubled, &QuerySettings::new())
            .await
            .expect("execute historical_resolution_query");
        let mut out = Vec::new();
        while let Some(row) = stream.next().await {
            out.push(row.expect("decode LabelsRow").fingerprint);
        }
        out.sort_unstable();
        out
    };

    assert_eq!(in_process, vec![10, 20]);
    assert_eq!(in_process, via_sql);

    drop_database(&bootstrap, db).await;
}

/// A cache that never refreshed (cold) must degrade to SQL immediately, and
/// the resulting fallback SQL must return the same set the warm cache would
/// have (proving the degradation never changes the *answer*, only the
/// path).
#[tokio::test]
async fn a_cold_cache_falls_back_to_sql_with_the_same_result_a_warm_cache_would_give() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_cold";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");

    let now_ms = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("now fits in i64");
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let recent_bucket = (now_ms / bucket) * bucket;

    seed(
        &client,
        &[SeedSeriesRow {
            metric_name: "up".to_string(),
            fingerprint: 99,
            unix_milli: recent_bucket,
            labels: r#"{"job":"api"}"#.to_string(),
        }],
    )
    .await;

    let cold_cache = LabelCache::new(
        cache_client,
        cache_config(db, "metric_series", 24 * 3_600_000, Duration::from_secs(60)),
    );
    assert!(!cold_cache.is_warm());
    let window = DataWindow {
        start_ms: recent_bucket - bucket,
        end_ms: now_ms,
    };
    let sql = match cold_cache.resolve("up", &[], window) {
        Resolution::SqlFallback { sql, reason } => {
            assert_eq!(reason, pulsus_read::FallbackReason::ColdCache);
            sql
        }
        other => panic!("expected SqlFallback(ColdCache), got {other:?}"),
    };
    let via_sql = execute_fingerprint_sql(&client, &sql).await;
    assert_eq!(via_sql, vec![99]);

    // Now warm the same data and confirm the in-process answer agrees.
    cold_cache.refresh().await.expect("refresh");
    match cold_cache.resolve("up", &[], window) {
        Resolution::Fingerprints(fps) => assert_eq!(fps, vec![99]),
        other => panic!("expected Fingerprints, got {other:?}"),
    }

    drop_database(&bootstrap, db).await;
}

/// Stale-path differential (code-review round-2 fix, finding 4): a warm
/// cache whose query reaches past its recency edge (`sweep_time_ms +
/// staleness_threshold_ms`) must degrade to `FallbackReason::StaleCache`,
/// and the SQL that fallback executes must return **exactly** the same
/// fingerprints as (a) an independently hand-written "ground truth" query
/// against `metric_series` (not built through `metrics::sql` at all) and
/// (b) a fresh in-process `resolve()` once the cache is re-refreshed with a
/// window back inside its recency edge. The selector includes an `=~`
/// matcher so the fallback SQL carries a literal `?` (`ch_regex_anchored`'s
/// `^(?:...)$` template) — the `?`-doubling this test applies at the exec
/// boundary (`double_placeholders`, mirroring `logql::exec`'s contract) is
/// therefore load-bearing, not incidental.
#[tokio::test]
async fn stale_cache_degrades_to_sql_identical_to_ground_truth_and_a_fresh_refresh() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_stale_differential";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");

    let now_ms = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("now fits in i64");
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let recent_bucket = (now_ms / bucket) * bucket;

    seed(
        &client,
        &[
            SeedSeriesRow {
                metric_name: "http_requests_total".to_string(),
                fingerprint: 501,
                unix_milli: recent_bucket,
                labels: r#"{"status":"500"}"#.to_string(),
            },
            SeedSeriesRow {
                metric_name: "http_requests_total".to_string(),
                fingerprint: 502,
                unix_milli: recent_bucket,
                labels: r#"{"status":"503"}"#.to_string(),
            },
            SeedSeriesRow {
                metric_name: "http_requests_total".to_string(),
                fingerprint: 200,
                unix_milli: recent_bucket,
                labels: r#"{"status":"200"}"#.to_string(),
            },
            // Matches the `5..` regex on label content alone, but its only
            // `metric_series` row is bucketed 10 buckets before the query
            // window's (bucket-floored) lower bound — test-precision fix
            // (round-3 review): included to prove the window bounds
            // actually bite, not merely that the label predicate does. An
            // unbounded ground-truth query would wrongly include this
            // fingerprint; both the fallback SQL and the correctly-bounded
            // ground truth below must exclude it.
            SeedSeriesRow {
                metric_name: "http_requests_total".to_string(),
                fingerprint: 999,
                unix_milli: recent_bucket - bucket * 10,
                labels: r#"{"status":"599"}"#.to_string(),
            },
        ],
    )
    .await;

    // A tiny TTL/staleness threshold: the cache's recency edge sits just
    // past `now_ms`, so a query whose `end_ms` reaches noticeably further
    // into the future is unambiguously past it regardless of scheduling
    // jitter between the sweep and this assertion.
    let mut cfg = cache_config(
        db,
        "metric_series",
        24 * 3_600_000,
        Duration::from_millis(10),
    );
    cfg.staleness_multiplier = 1;
    let cache = LabelCache::new(cache_client, cfg);
    cache.refresh().await.expect("refresh");
    assert!(cache.is_warm());

    let matcher = LabelMatcher {
        key: "status".to_string(),
        op: MatchOp::Re,
        value: "5..".to_string(),
    };
    let far_future_window = DataWindow {
        start_ms: recent_bucket - bucket,
        end_ms: now_ms + 3_600_000_000, // ~41 days past now: well past any threshold
    };

    let sql = match cache.resolve(
        "http_requests_total",
        std::slice::from_ref(&matcher),
        far_future_window,
    ) {
        Resolution::SqlFallback {
            sql,
            reason: pulsus_read::FallbackReason::StaleCache { .. },
        } => sql,
        other => panic!("expected SqlFallback(StaleCache), got {other:?}"),
    };
    // The fallback SQL must carry the literal `?` from `^(?:...)$` — proof
    // that the doubling below is exercising a real code path, not a no-op.
    assert!(
        sql.contains("(?:"),
        "expected an anchored regex literal in the fallback SQL: {sql}"
    );
    let degraded = execute_fingerprint_sql(&client, &sql).await;
    assert!(
        !degraded.contains(&999),
        "fallback SQL must exclude the out-of-window series 999: {degraded:?}"
    );

    // (a) Independent ground truth: hand-written, not built through
    // `metrics::sql` at all — but bound by the SAME bucket-floored window
    // the fallback SQL uses (test-precision fix, round-3 review): an
    // unbounded ground truth would silently pass even if the fallback SQL
    // had a bounds bug, since it would agree with a *wrong*, over-broad
    // fallback rather than validating the fallback's bounds independently.
    let lower_bound_ms = floor_to_activity_bucket(far_future_window.start_ms, bucket);
    let upper_bound_ms = floor_to_activity_bucket(far_future_window.end_ms, bucket);
    let truth_sql = format!(
        "SELECT DISTINCT fingerprint FROM metric_series \
         WHERE metric_name = 'http_requests_total' \
           AND unix_milli >= {lower_bound_ms} AND unix_milli <= {upper_bound_ms} \
           AND match(JSONExtractString(labels, 'status'), '^(?:5..)$')"
    );
    let truth = execute_fingerprint_sql(&client, &truth_sql).await;
    // Proves the bounds actually bite: fingerprint 999 matches the label
    // predicate alone but sits 10 buckets before `lower_bound_ms`, so both
    // the correctly-bounded ground truth and the fallback SQL must exclude
    // it — an unbounded ground truth would have included it here.
    assert!(
        !truth.contains(&999),
        "ground truth must exclude the out-of-window series 999: {truth:?}"
    );

    // (b) A fresh refresh brings the recency edge back up to "now", so the
    // same selector over a window back inside it resolves in-process again.
    // Unlike the SQL fallback (bound to the exact request window), the
    // in-process path matches over the *whole* resident snapshot once the
    // window-coverage gate passes — it carries no per-row time filter of
    // its own, only the coarse "is this snapshot trustworthy for this
    // query" gate. Fingerprint 999 sits inside the cache's 24h residency
    // window (so the sweep picks it up) but outside `fresh_window`'s own
    // narrow bounds; it is therefore expected — not a bug — for the
    // in-process result to include it even though the precisely-bound SQL
    // paths (fallback and ground truth) both exclude it.
    cache.refresh().await.expect("refresh");
    let fresh_window = DataWindow {
        start_ms: recent_bucket - bucket,
        end_ms: now_ms,
    };
    let fresh_in_process = match cache.resolve(
        "http_requests_total",
        std::slice::from_ref(&matcher),
        fresh_window,
    ) {
        Resolution::Fingerprints(fps) => fps,
        other => panic!("expected a cache hit after refresh, got {other:?}"),
    };
    assert!(
        fresh_in_process.contains(&999),
        "sanity check: 999 is within the cache's residency window and must be resident: {fresh_in_process:?}"
    );
    let fresh_in_process_within_request_window: Vec<u64> = fresh_in_process
        .into_iter()
        .filter(|fp| *fp != 999)
        .collect();

    assert_eq!(degraded, vec![501, 502]);
    assert_eq!(
        degraded, truth,
        "degraded SQL must match independent ground truth"
    );
    assert_eq!(
        degraded, fresh_in_process_within_request_window,
        "degraded SQL must match a fresh in-process resolution after the next refresh, once \
         restricted to fingerprints whose own metric_series row falls inside the request window"
    );

    drop_database(&bootstrap, db).await;
}

/// End-to-end injection/parity: a label KEY containing a quote and a
/// backslash round-trips through both the in-process path and the executed
/// SQL fallback, returning identical results — proving the escaping
/// documented in `metrics::sql` isn't merely cosmetic.
#[tokio::test]
async fn a_quote_and_backslash_bearing_label_key_round_trips_identically_on_both_paths() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = "pulsus_read_it_metrics_quote_key";
    init_db(&bootstrap, db).await;
    let client = ChClient::new(test_config(db))
        .await
        .expect("connect (target db)");
    let cache_client = ChClient::new(test_config(db))
        .await
        .expect("connect (cache client)");

    let now_ms = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("now fits in i64");
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let recent_bucket = (now_ms / bucket) * bucket;

    // The label key itself contains a single quote and a backslash; encoded
    // as valid canonical JSON (matching what `LabelSet::to_canonical_json`
    // would produce for a key like `we'ird\key`).
    let weird_key = "we'ird\\key";
    let labels_json = serde_json_like_encode(weird_key, "v");

    seed(
        &client,
        &[SeedSeriesRow {
            metric_name: "up".to_string(),
            fingerprint: 7,
            unix_milli: recent_bucket,
            labels: labels_json,
        }],
    )
    .await;

    let cache = LabelCache::new(
        cache_client,
        cache_config(db, "metric_series", 24 * 3_600_000, Duration::from_secs(60)),
    );
    cache.refresh().await.expect("refresh");

    let matcher = LabelMatcher {
        key: weird_key.to_string(),
        op: MatchOp::Eq,
        value: "v".to_string(),
    };
    let window = DataWindow {
        start_ms: recent_bucket - bucket,
        end_ms: now_ms,
    };

    let in_process = match cache.resolve("up", std::slice::from_ref(&matcher), window) {
        Resolution::Fingerprints(fps) => fps,
        other => panic!("expected a cache hit, got {other:?}"),
    };
    assert_eq!(in_process, vec![7]);

    let sql = historical_series_subquery("metric_series", "up", window, bucket, &[matcher]);
    let via_sql = execute_fingerprint_sql(&client, &sql).await;
    assert_eq!(in_process, via_sql);

    drop_database(&bootstrap, db).await;
}

/// Minimal, hand-rolled `{"key":"value"}` JSON encoder that correctly
/// backslash-escapes `"` and `\` inside `key` — enough to build a
/// deliberately awkward canonical-labels fixture without pulling in a JSON
/// crate dependency this test file doesn't otherwise need.
fn serde_json_like_encode(key: &str, value: &str) -> String {
    fn escape(s: &str) -> String {
        let mut out = String::new();
        for c in s.chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                other => out.push(other),
            }
        }
        out
    }
    format!("{{\"{}\":\"{}\"}}", escape(key), escape(value))
}
