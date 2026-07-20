//! Issue #35 (full-shape parse bound) — the live regression witness: a
//! rendered stage2/`sample_fetch_multi`-shaped query past ClickHouse's
//! 262,144-byte `max_query_size` DEFAULT fails on a stock server, and
//! succeeds once the product's own `max_query_size` setting is applied.
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, reusing `crates/pulsus-read/
//! tests/query_log_gates.rs`'s connection/setup pattern verbatim.
//!
//! Run locally:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test query_text_cap_live
//! podman rm -f pulsus-ch-test
//! ```

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{
    ChClient, ChConnConfig, ChError, ChProto, Idempotency, QuerySettings, Row,
};
use pulsus_read::logql::sql;
use pulsus_read::querytext::MAX_QUERY_TEXT_BYTES;

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-read/tests/query_text_cap_live.rs for setup)"
            );
            return;
        }
    };
}

fn test_config() -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: std::env::var("PULSUS_TEST_CH_DATABASE")
            .unwrap_or_else(|_| "default".to_string()),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(60),
        ..ChConnConfig::default()
    }
}

/// Drops (if present) and recreates a throwaway database with two minimal
/// (`ENGINE = Memory`) tables shaped like `log_streams` and
/// `metric_samples` — only the columns [`sql::stage2`]/`sample_fetch_multi`
/// project, no partitioning/ordering machinery needed: this test only
/// proves SQL-text ADMISSION (parse-buffer), never touches storage
/// pruning. Returns a `ChClient` bound to the new database.
async fn setup_db(db: &str) -> ChClient {
    let admin = ChClient::new(test_config()).await.expect("connect admin");
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop stale test database");
    admin
        .execute(
            &format!("CREATE DATABASE {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("create test database");

    let mut data_cfg = test_config();
    data_cfg.database = db.to_string();
    let client = ChClient::new(data_cfg)
        .await
        .expect("connect (data client)");

    client
        .execute(
            "CREATE TABLE log_streams (fingerprint UInt64, service String, labels String) \
             ENGINE = Memory",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("create log_streams");
    // `sample_fetch_multi` renders a `PREWHERE metric_name IN (...)`
    // clause, which `ENGINE = Memory` does not support (`ILLEGAL_PREWHERE`)
    // — a minimal unpartitioned `MergeTree` is the smallest engine that
    // accepts it; this test still never touches storage pruning.
    client
        .execute(
            "CREATE TABLE metric_samples (metric_name String, fingerprint UInt64, \
             unix_milli Int64, value Float64) ENGINE = MergeTree \
             ORDER BY (metric_name, fingerprint, unix_milli)",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("create metric_samples");
    client
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct Stage2Row {
    fingerprint: u64,
    service: String,
    labels: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct MultiSampleRow {
    metric_name: String,
    fingerprint: u64,
    unix_milli: i64,
    value: f64,
}

/// A synthetic fingerprint set whose `IN (...)` list alone renders past
/// ClickHouse's 262,144-byte `max_query_size` default — ~13,000 worst-case
/// `u64::MAX` literals (22 B/entry ≈ 286,000 B), well below
/// `DEFAULT_MAX_STREAMS` (100,000) so this witness is conservative, not a
/// contrived edge.
fn oversized_fingerprint_set() -> Vec<u64> {
    vec![u64::MAX; 13_000]
}

/// Drains a query to completion, mapping any per-row error into the same
/// `Result` the dispatch error already carries — this test only cares
/// whether the FULL round trip (dispatch + drain) succeeds, matching how
/// production consumes a stream.
async fn run_to_completion<R: pulsus_clickhouse::ChRow>(
    client: &ChClient,
    sql: &str,
    settings: &QuerySettings,
) -> Result<usize, ChError> {
    let mut stream = client.query_stream::<R>(sql, settings).await?;
    let mut n = 0usize;
    while let Some(row) = stream.next().await {
        row?;
        n += 1;
    }
    Ok(n)
}

/// AC4 (issue #35 plan v2): a >256 KiB `stage2` SQL text fails under
/// ClickHouse's server-default `max_query_size` (no settings sent at
/// all — pins the limitation as a regression witness) and succeeds once
/// the product's raised setting is applied. The pinned error-message
/// substring ("query size") is version-sensitive; acceptable only because
/// the test image is pinned to ClickHouse 24.8 (KISS).
#[tokio::test]
async fn stage2_oversized_sql_fails_under_ch_defaults_and_succeeds_under_the_raised_setting() {
    skip_unless_live!();
    let client = setup_db("pulsus_read_it_query_text_cap_stage2").await;
    let fps = oversized_fingerprint_set();
    let sql = sql::stage2("log_streams", &fps);
    assert!(
        sql.len() > 262_144,
        "fixture SQL is {} bytes, expected > 262,144 to exercise the ClickHouse default",
        sql.len()
    );

    let default_result = run_to_completion::<Stage2Row>(&client, &sql, &QuerySettings::new()).await;
    let err = default_result.expect_err(
        "an oversized stage2 SQL text must fail to parse under ClickHouse's server-default \
         max_query_size — if this now succeeds, the server default has changed and this \
         regression witness needs updating",
    );
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("query size"),
        "expected a ClickHouse max_query_size parse rejection, got: {err}"
    );

    let raised = QuerySettings::new().set("max_query_size", MAX_QUERY_TEXT_BYTES);
    let rows = run_to_completion::<Stage2Row>(&client, &sql, &raised)
        .await
        .expect("the same SQL text must succeed once max_query_size is raised");
    assert_eq!(rows, 0, "the fixture table is empty by construction");
}

/// AC4's metrics-path half: the sibling `sample_fetch_multi`-shaped query
/// (issue #35's confirmed live gap — the metrics path previously sent NO
/// settings at all) exhibits the same fail-then-succeed shape.
#[tokio::test]
async fn metrics_multi_oversized_sql_fails_under_ch_defaults_and_succeeds_under_the_raised_setting()
{
    skip_unless_live!();
    let client = setup_db("pulsus_read_it_query_text_cap_metrics").await;
    let fps = oversized_fingerprint_set();
    let sql = pulsus_read::metrics::sample_sql::sample_fetch_multi(
        "metric_samples",
        &["up".to_string()],
        &fps,
        0,
        i64::MAX,
    );
    assert!(
        sql.len() > 262_144,
        "fixture SQL is {} bytes, expected > 262,144 to exercise the ClickHouse default",
        sql.len()
    );

    let default_result =
        run_to_completion::<MultiSampleRow>(&client, &sql, &QuerySettings::new()).await;
    let err = default_result.expect_err(
        "an oversized sample_fetch_multi SQL text must fail to parse under ClickHouse's \
         server-default max_query_size",
    );
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("query size"),
        "expected a ClickHouse max_query_size parse rejection, got: {err}"
    );

    let raised = QuerySettings::new().set("max_query_size", MAX_QUERY_TEXT_BYTES);
    let rows = run_to_completion::<MultiSampleRow>(&client, &sql, &raised)
        .await
        .expect("the same SQL text must succeed once max_query_size is raised");
    assert_eq!(rows, 0, "the fixture table is empty by construction");
}
