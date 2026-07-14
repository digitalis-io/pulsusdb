//! Live re-verification of [`pulsus_model::floor_to_activity_bucket`]
//! against ClickHouse's real `intDiv` (docs/schemas.md §2.1's rendered
//! historical-bound SQL, `intDiv({data_start}, {bucket_ms}) * {bucket_ms}`)
//! — including negative-timestamp cases, where truncating division
//! (`intDiv`) and floor division (`div_euclid`) diverge (architect plan
//! amendment 3, closing a review test gap). Gated behind
//! `PULSUS_TEST_CLICKHOUSE=1`, mirroring `tests/live_cityhash.rs`.
//!
//! To run these:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-model --test live_intdiv
//! podman rm -f pulsus-ch-test
//! ```
//!
//! Connection parameters can be overridden via `PULSUS_TEST_CH_HOST` /
//! `PULSUS_TEST_CH_HTTP_PORT` if the default `localhost:19123` does not fit
//! your environment.

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, QuerySettings, Row};
use pulsus_model::floor_to_activity_bucket;

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
        query_timeout: Duration::from_secs(10),
        ..ChConnConfig::default()
    }
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-model/tests/live_intdiv.rs for setup)"
            );
            return;
        }
    };
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct IntDivRow {
    floored: i64,
}

async fn live_int_div_floor(client: &ChClient, unix_milli: i64, bucket_ms: i64) -> i64 {
    // `toInt64(...)` on every operand: an all-non-negative literal
    // expression (e.g. `unix_milli = 0`) would otherwise infer `UInt64`,
    // which cannot represent the negative cases this test exists to cover
    // and does not decode into this row's `i64` column.
    let sql = format!(
        "SELECT toInt64(intDiv(toInt64({unix_milli}), toInt64({bucket_ms})) * toInt64({bucket_ms})) AS floored"
    );
    let mut stream = client
        .query_stream::<IntDivRow>(&sql, &QuerySettings::new())
        .await
        .expect("query_stream");
    let row = stream.next().await.expect("one row").expect("row decode");
    row.floored
}

/// Cross-checks a range of positive, zero, and — the divergence case
/// truncating vs floor division disagree on — negative `unix_milli`
/// timestamps against a live `intDiv`.
#[tokio::test]
async fn floor_to_activity_bucket_matches_live_clickhouse_intdiv() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");

    let bucket_ms = 3_600_000i64;
    let cases: &[i64] = &[
        0,
        1,
        3_600_000,
        3_600_001,
        7_199_999,
        -1,
        -3_600_000,
        -3_600_001,
        -7_200_000,
        1_700_000_000_000,
    ];

    for &unix_milli in cases {
        let expected = live_int_div_floor(&client, unix_milli, bucket_ms).await;
        let got = floor_to_activity_bucket(unix_milli, bucket_ms);
        assert_eq!(
            got, expected,
            "unix_milli={unix_milli}: floor_to_activity_bucket diverged from live intDiv"
        );
    }
}
