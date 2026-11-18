//! **T8 (issue #880): the label cache's feed.** After the start-up build
//! reads `metric_labels`, a pass reads only the registrations
//! `metric_landing` received since the last one, so a new series reaches
//! the cache without the lookup table being read again.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:26.8.21.10
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test live_promql_label_feed
//! podman rm -f pulsus-ch-test
//! ```

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_model::{ACTIVITY_BUCKET_MS, Fingerprint};
use pulsus_read::{DataWindow, LabelCache, LabelCacheConfig, Resolution, SeriesResolver};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-read/tests/live_promql_label_feed.rs for setup)"
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
        pool_size: 4,
        query_timeout: Duration::from_secs(20),
        ..ChConnConfig::default()
    }
}

fn cache_config(db: &str) -> LabelCacheConfig {
    LabelCacheConfig {
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        db: db.to_string(),
        series_table: "metric_series".to_string(),
        labels_table: "metric_labels".to_string(),
        landing_table: "metric_landing".to_string(),
        feed_overlap_ms: 300_000,
        window_ms: 24 * 3_600_000,
        cache_max_series: 50_000,
        ttl: Duration::from_secs(60),
        staleness_multiplier: 3,
    }
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

async fn execute(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::NonIdempotent)
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
}

/// One series' registration, as the writer lands it: a kind-2 row stamped
/// with the time it was received, at the current hour's bucket.
async fn register(client: &ChClient, metric_name: &str, fingerprint: u128, job: &str) {
    let now = now_ms();
    let bucket = now - now.rem_euclid(ACTIVITY_BUCKET_MS);
    execute(
        client,
        &format!(
            "INSERT INTO metric_landing \
             (received_ms, kind, metric_name, fingerprint, unix_milli, labels) \
             VALUES ({now}, 2, '{metric_name}', {fingerprint}, {bucket}, '{{\"job\":\"{job}\"}}')"
        ),
    )
    .await;
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct LoggedRow {
    query: String,
    tables: Vec<String>,
}

/// **T8.**
#[tokio::test]
async fn a_new_series_reaches_the_cache_through_the_feed() {
    skip_unless_live!();

    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = &pulsus_testkit::test_db("pulsus_read_it_promql_label_feed");
    execute(&bootstrap, &format!("DROP DATABASE IF EXISTS {db}")).await;
    run_init(&bootstrap, &RenderCtx::for_tests(db, 7))
        .await
        .expect("run_init");
    let client = ChClient::new(test_config(db)).await.expect("connect");

    register(&client, "feed_a_metric", 101, "a").await;
    // The query log outlives the database; only this run's statements count.
    let started = now_ms();
    let cache = LabelCache::new(
        ChClient::new(test_config(db))
            .await
            .expect("connect (cache)"),
        cache_config(db),
    );
    cache.refresh().await.expect("pass 1");
    assert!(cache.is_warm());

    register(&client, "feed_b_metric", 202, "b").await;
    let before_pass_2 = now_ms();
    cache.refresh().await.expect("pass 2");

    let tenant = pulsus_model::Tenant::from_header(None, false).expect("the empty tenant");
    let window = DataWindow {
        start_ms: before_pass_2 - 600_000,
        end_ms: before_pass_2,
    };
    assert_eq!(
        cache.resolve(&tenant, "feed_b_metric", &[], window),
        Resolution::Fingerprints(vec![Fingerprint::from_raw(202)]),
        "B reached the cache"
    );

    execute(&bootstrap, "SYSTEM FLUSH LOGS").await;
    let sql = format!(
        "SELECT query, tables FROM system.query_log \
         WHERE type = 'QueryFinish' AND is_initial_query \
           AND log_comment = 'background:label-cache-sweep' \
           AND has(databases, '{db}') \
           AND query_start_time_microseconds >= fromUnixTimestamp64Milli(toInt64({started})) \
         ORDER BY event_time_microseconds"
    );
    let mut stream = bootstrap
        .query_stream::<LoggedRow>(&sql, &QuerySettings::new())
        .await
        .expect("read the query log");
    let mut logged = Vec::new();
    while let Some(row) = stream.next().await {
        logged.push(row.expect("decode"));
    }
    assert_eq!(logged.len(), 2, "one statement per pass: {logged:#?}");
    let labels = format!("{db}.metric_labels");
    let landing = format!("{db}.metric_landing");
    assert!(
        logged[0].tables.contains(&labels),
        "pass 1 reads the lookup table: {:?}",
        logged[0]
    );
    assert!(
        logged[1].tables.contains(&landing) && !logged[1].tables.contains(&labels),
        "pass 2 reads the landing table and not the lookup table: {:?}",
        logged[1]
    );

    execute(&bootstrap, &format!("DROP DATABASE IF EXISTS {db}")).await;
}
