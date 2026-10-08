//! Issue #500: `/api/v1/metadata`'s read returns every distinct
//! descriptor of a name, against a live ClickHouse.
//!
//! The descriptors are written as kind-3 rows of `metric_landing`, so the
//! view fills `metric_metadata` as a push does.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-read --test live_metrics_metadata
//! ```

#[path = "pushed_rate_corpus/mod.rs"]
mod pushed_rate_corpus;

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, Idempotency, QuerySettings, Row};
use pulsus_model::Tenant;
use pulsus_read::{LabelCache, LabelCacheConfig, MetricsConfig, MetricsEngine};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;
use pushed_rate_corpus::{drop_database, now_ms, test_config};

const DAY_MS: i64 = 86_400_000;
const HOUR_MS: i64 = 3_600_000;

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
            return;
        }
    };
}

fn tenant(name: &str) -> Tenant {
    if name.is_empty() {
        return Tenant::from_header(None, false).expect("no header is the empty tenant");
    }
    let header = http::HeaderValue::from_str(name).expect("a header value");
    Tenant::from_header(Some(&header), false).expect("a valid tenant")
}

fn engine_config(db: &str) -> MetricsConfig {
    MetricsConfig {
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        db: db.to_string(),
        samples_table: "metric_samples".to_string(),
        hist_samples_table: "metric_hist_samples".to_string(),
        series_table: "metric_series".to_string(),
        labels_table: "metric_labels".to_string(),
        label_index_table: "metric_label_index".to_string(),
        label_values_table: "metric_label_values".to_string(),
        metadata_table: "metric_metadata".to_string(),
        experimental_functions: false,
        max_metric_fanout: 1_000,
        max_cache_scan: 200_000,
        cache_max_series: 50_000,
        max_info_series: 100_000,
        max_samples: 50_000_000,
        distributed: false,
        grouped_push: true,
    }
}

fn cache_config(db: &str) -> LabelCacheConfig {
    LabelCacheConfig {
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        db: db.to_string(),
        series_table: "metric_series".to_string(),
        labels_table: "metric_labels".to_string(),
        window_ms: DAY_MS,
        cache_max_series: 50_000,
        ttl: Duration::from_secs(3_600),
        staleness_multiplier: 3,
    }
}

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::NonIdempotent)
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
}

/// One kind-3 landing row: `(tenant, name, type, help, unit, updated_ms)`.
type Descriptor<'a> = (&'a str, &'a str, &'a str, &'a str, &'a str, i64);

/// T1's rows, `now` being the newest: tenant `''` holds `m` gauge "A",
/// `m` gauge "B" and `m` counter "A", each written twice an hour apart,
/// `m` gauge "A" `seconds` and `n` gauge "N"; tenant `t2` holds `m` gauge
/// "Z".
fn t1_rows(now: i64) -> Vec<Descriptor<'static>> {
    let mut rows = Vec::new();
    for at in [now - HOUR_MS, now] {
        rows.push(("", "m", "gauge", "A", "", at));
        rows.push(("", "m", "gauge", "B", "", at));
        rows.push(("", "m", "counter", "A", "", at));
    }
    rows.push(("", "m", "gauge", "A", "seconds", now));
    rows.push(("", "n", "gauge", "N", "", now));
    rows.push(("t2", "m", "gauge", "Z", "", now));
    rows
}

/// A fresh database holding `rows`, written as kind-3 landing rows, one
/// insert per row; the client over it and the engine.
async fn seeded(
    name: &str,
    rows: &[Descriptor<'_>],
) -> (String, ChClient, ChClient, MetricsEngine) {
    let db = pulsus_testkit::test_db(name);
    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    drop_database(&bootstrap, &db).await;
    run_init(&bootstrap, &RenderCtx::for_tests(&db))
        .await
        .expect("run_init");
    let client = ChClient::new(test_config(&db)).await.expect("connect");
    for (org, name, t, help, unit, at) in rows {
        exec(
            &client,
            &format!(
                "INSERT INTO metric_landing (org_id, received_ms, kind, metric_name, metric_type, \
                 help, unit, updated_ns) VALUES ('{org}', {at}, 3, '{name}', '{t}', '{help}', \
                 '{unit}', {})",
                at * 1_000_000
            ),
        )
        .await;
    }
    let cache = Arc::new(LabelCache::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache_config(&db),
    ));
    let engine = MetricsEngine::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache,
        engine_config(&db),
    );
    (db, bootstrap, client, engine)
}

/// The engine's answer, one `name type help unit` line per entry, in order.
async fn entries(
    engine: &MetricsEngine,
    t: &str,
    metric: Option<&str>,
    limit: Option<usize>,
    limit_per_metric: Option<i64>,
) -> Vec<String> {
    engine
        .metadata(&tenant(t), metric, limit, limit_per_metric)
        .await
        .expect("metadata")
        .into_iter()
        .map(|m| format!("{} {} {} {:?}", m.name, m.metric_type, m.help, m.unit))
        .collect()
}

const ALL_FIVE: [&str; 5] = [
    r#"m counter A """#,
    r#"m gauge A """#,
    r#"m gauge A "seconds""#,
    r#"m gauge B """#,
    r#"n gauge N """#,
];

/// T1: every distinct `(type, help, unit)` of a name in a tenant is an
/// entry — `m`'s gauge "A" twice, once with a unit — in `type`, `help`,
/// `unit` order; another tenant's descriptor is not.
#[tokio::test]
async fn every_distinct_descriptor_is_returned() {
    skip_unless_live!();
    let (db, bootstrap, _client, engine) =
        seeded("pulsus_read_it_500_t1", &t1_rows(now_ms())).await;
    let all = entries(&engine, "", None, None, None).await;
    let other = entries(&engine, "t2", None, None, None).await;
    drop_database(&bootstrap, &db).await;
    assert_eq!(all, ALL_FIVE, "tenant ''");
    assert_eq!(other, [r#"m gauge Z """#], "tenant t2");
}

/// T2: `limit` counts names, after `metric` has selected them.
#[tokio::test]
async fn limit_counts_names_after_metric() {
    skip_unless_live!();
    let (db, bootstrap, _client, engine) =
        seeded("pulsus_read_it_500_t2", &t1_rows(now_ms())).await;
    let one_name = entries(&engine, "", None, Some(1), None).await;
    let none = entries(&engine, "", None, Some(0), None).await;
    let n = entries(&engine, "", Some("n"), None, None).await;
    let n_limited = entries(&engine, "", Some("n"), Some(1), None).await;
    drop_database(&bootstrap, &db).await;
    assert_eq!(one_name, ALL_FIVE[..4], "limit=1: m's four entries");
    assert!(none.is_empty(), "limit=0: {none:?}");
    assert_eq!(n, [r#"n gauge N """#], "metric=n");
    assert_eq!(
        n_limited,
        [r#"n gauge N """#],
        "metric=n&limit=1: n is not the first name, so a cut before metric loses it"
    );
}

/// T9: `limit_per_metric` keeps each name's first entries; 0 or a
/// negative value is no limit.
#[tokio::test]
async fn limit_per_metric_cuts_each_name() {
    skip_unless_live!();
    let (db, bootstrap, _client, engine) =
        seeded("pulsus_read_it_500_t9", &t1_rows(now_ms())).await;
    let two = entries(&engine, "", None, None, Some(2)).await;
    let zero = entries(&engine, "", None, None, Some(0)).await;
    let negative = entries(&engine, "", None, None, Some(-1)).await;
    drop_database(&bootstrap, &db).await;
    assert_eq!(
        two,
        [ALL_FIVE[0], ALL_FIVE[1], ALL_FIVE[4]],
        "limit_per_metric=2"
    );
    assert_eq!(zero, ALL_FIVE, "limit_per_metric=0");
    assert_eq!(negative, ALL_FIVE, "limit_per_metric=-1");
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct CountRow {
    n: u64,
}

async fn rows_of(client: &ChClient, filter: &str) -> u64 {
    let sql = format!("SELECT count() AS n FROM metric_metadata WHERE {filter}");
    let mut stream = client
        .query_stream::<CountRow>(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
    stream.next().await.expect("one row").expect("decode").n
}

/// T3: a resend of a descriptor is one row after merging, and a
/// descriptor not resent within `retention_days` leaves: `old`, one row
/// that old, under a key no other row shares, and `kept`, one row that old
/// and one an hour ago.
#[tokio::test]
async fn a_repeated_descriptor_is_one_row_and_an_old_one_expires() {
    skip_unless_live!();
    let now = now_ms();
    let past = now - 8 * DAY_MS;
    let mut rows = t1_rows(now);
    rows.push(("", "old", "gauge", "O", "", past));
    rows.push(("", "kept", "gauge", "K", "", past));
    rows.push(("", "kept", "gauge", "K", "", now - HOUR_MS));
    let (db, bootstrap, client, _engine) = seeded("pulsus_read_it_500_t3", &rows).await;
    exec(&client, "OPTIMIZE TABLE metric_metadata FINAL").await;
    let ours = rows_of(&client, "org_id = '' AND metric_name IN ('m', 'n')").await;
    let old = rows_of(&client, "metric_name = 'old'").await;
    let kept = rows_of(&client, "metric_name = 'kept'").await;
    drop_database(&bootstrap, &db).await;
    assert_eq!(
        (ours, old, kept),
        (5, 0, 1),
        "tenant '' m and n, old, kept: rows after the merge"
    );
}
