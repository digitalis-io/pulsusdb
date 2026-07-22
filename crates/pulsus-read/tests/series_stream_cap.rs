//! Live regression test for round-1 code-review finding 1 on issue #13
//! (adjudicated "confirmed" by architect plan amendment 3 §1):
//! `LogQlEngine::series`'s `match[]` union must re-check `max_streams` on
//! the **deduped union** of every selector's resolved fingerprints, not
//! just each selector individually — two (or more) disjoint selectors can
//! each stay under the cap on their own while their union blows past it.
//!
//! Live ClickHouse, gated behind `PULSUS_TEST_CLICKHOUSE=1`, reusing
//! `explain_indexes.rs`'s harness idiom (`should_run`/`test_config`/
//! `run_init`) — only `log_streams` needs seeding (no samples): `series`
//! never reaches stage 3, and the cap must trip *before* hydration.
//!
//! Run locally:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test series_stream_cap
//! podman rm -f pulsus-ch-test
//! ```

use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_logql::{Expr, LogExpr, parse_selector};
use pulsus_read::logql::{EngineConfig, LogQlEngine, ReadError, TimeBounds, TooBroadReason};
use pulsus_schema::{RenderCtx, SchemaParams, run_init};

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
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
        query_timeout: Duration::from_secs(20),
        ..ChConnConfig::default()
    }
}

fn schema_params(db: &str) -> SchemaParams {
    RenderCtx {
        db: db.to_string(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
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

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after the Unix epoch")
            .as_nanos(),
    )
    .expect("current time fits in i64 nanoseconds")
}

/// Seeds one stream per `(fingerprint, team)` pair — each `team` value is
/// its own selector's exact match, so each selector individually resolves
/// to exactly one fingerprint.
async fn seed_streams(client: &ChClient, db: &str, base_ns: i64, teams: &[(&str, u64)]) {
    let values: Vec<String> = teams
        .iter()
        .map(|(team, fp)| {
            format!(
                "(toStartOfMonth(fromUnixTimestamp64Nano(toInt64({base_ns}))), {fp}, 'checkout', \
                 '{{\"team\":\"{team}\",\"service_name\":\"checkout\"}}', 0)"
            )
        })
        .collect();
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, updated_ns) VALUES {}",
                values.join(", ")
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_streams");
}

fn engine_config(db: &str, max_streams: usize) -> EngineConfig {
    EngineConfig {
        db: db.to_string(),
        streams_idx: "log_streams_idx".to_string(),
        streams: "log_streams".to_string(),
        samples: "log_samples".to_string(),
        rollup_table: "log_metrics_5s".to_string(),
        patterns_table: "log_patterns".to_string(),
        rollup_res_ns: 5_000_000_000,
        scan_budget_bytes: 50 * 1024 * 1024 * 1024,
        max_streams,
        pipeline_scan_factor: 10,
    }
}

fn selector_expr(src: &str) -> Expr {
    Expr::Log(LogExpr {
        selector: parse_selector(src).expect("parse selector"),
        pipeline: Vec::new(),
    })
}

#[tokio::test]
async fn series_union_across_disjoint_selectors_trips_the_stream_cap_even_though_each_selector_is_individually_under_it()
 {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see this file's module doc comment for setup)"
        );
        return;
    }
    let db = "pulsus_read_it_series_stream_cap";
    let bootstrap = ChClient::new(test_config()).await.expect("connect");
    drop_database(&bootstrap, db).await;
    run_init(&bootstrap, &schema_params(db))
        .await
        .expect("run_init");

    let mut data_cfg = test_config();
    data_cfg.database = db.to_string();
    let data_client = ChClient::new(data_cfg)
        .await
        .expect("connect (data client)");

    let base_ns = now_ns();
    // Two selectors, each matching exactly one distinct fingerprint — a
    // cap of 1 lets *each* selector pass individually (`count == cap`),
    // but their deduped union is 2 fingerprints, which must trip the cap.
    seed_streams(
        &data_client,
        db,
        base_ns,
        &[
            ("alpha", 0x9100_0000_0000_0001),
            ("beta", 0x9100_0000_0000_0002),
        ],
    )
    .await;

    let engine = LogQlEngine::new(data_client, engine_config(db, 1));
    let bounds = TimeBounds {
        start_ns: base_ns - 3_600_000_000_000,
        end_ns: base_ns + 3_600_000_000_000,
    };
    let selectors = vec![
        selector_expr(r#"{team="alpha"}"#),
        selector_expr(r#"{team="beta"}"#),
    ];

    let err = engine
        .series(&selectors, bounds)
        .await
        .expect_err("union of 2 fingerprints must exceed the cap of 1");
    match err {
        ReadError::QueryTooBroad(TooBroadReason::StreamCap { count, cap }) => {
            assert_eq!(count, 2);
            assert_eq!(cap, 1);
        }
        other => panic!("expected QueryTooBroad(StreamCap {{ count: 2, cap: 1 }}), got {other:?}"),
    }
}

#[tokio::test]
async fn series_union_at_or_under_the_cap_still_succeeds() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see this file's module doc comment for setup)"
        );
        return;
    }
    let db = "pulsus_read_it_series_stream_cap_ok";
    let bootstrap = ChClient::new(test_config()).await.expect("connect");
    drop_database(&bootstrap, db).await;
    run_init(&bootstrap, &schema_params(db))
        .await
        .expect("run_init");

    let mut data_cfg = test_config();
    data_cfg.database = db.to_string();
    let data_client = ChClient::new(data_cfg)
        .await
        .expect("connect (data client)");

    let base_ns = now_ns();
    seed_streams(
        &data_client,
        db,
        base_ns,
        &[
            ("alpha", 0x9200_0000_0000_0001),
            ("beta", 0x9200_0000_0000_0002),
        ],
    )
    .await;

    let engine = LogQlEngine::new(data_client, engine_config(db, 2));
    let bounds = TimeBounds {
        start_ns: base_ns - 3_600_000_000_000,
        end_ns: base_ns + 3_600_000_000_000,
    };
    let selectors = vec![
        selector_expr(r#"{team="alpha"}"#),
        selector_expr(r#"{team="beta"}"#),
    ];

    let labels = engine
        .series(&selectors, bounds)
        .await
        .expect("union of 2 fingerprints at cap 2 must succeed");
    assert_eq!(labels.len(), 2);
}
