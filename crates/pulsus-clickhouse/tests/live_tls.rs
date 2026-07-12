//! HTTPS + self-signed-certificate skip-verify integration test (issue #3
//! fix plan, finding 1 — proves `CLICKHOUSE_PROTO=https` +
//! `CLICKHOUSE_TLS_SKIP_VERIFY=true` is actually wired into `ChPool`, not
//! just documented).
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE_TLS=1` so hermetic CI stays green
//! (this test needs the TLS-enabled ClickHouse container, which is not
//! brought up by default). To run:
//!
//! ```text
//! (cd xtask/docker && ./gen-certs.sh)
//! (cd xtask/docker && docker compose --profile tls up -d clickhouse-tls)
//! # wait for the container healthcheck, then:
//! PULSUS_TEST_CLICKHOUSE_TLS=1 cargo test -p pulsus-clickhouse --test live_tls
//! (cd xtask/docker && docker compose --profile tls down)
//! ```
//!
//! (`podman-compose` works identically instead of `docker compose`.)
//! Connection parameters can be overridden via `PULSUS_TEST_CH_TLS_HOST` /
//! `PULSUS_TEST_CH_TLS_HTTPS_PORT` if the default `localhost:8443` does not
//! fit your environment.

use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE_TLS").as_deref() == Ok("1")
}

fn test_config() -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_TLS_HOST")
            .unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_TLS_HTTPS_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(8443),
        // The bare `clickhouse-tls` service (xtask/docker/docker-compose.ch.yml)
        // only pre-creates `default`; override via `PULSUS_TEST_CH_DATABASE`
        // if your test server already provisions the real `pulsus` database.
        database: std::env::var("PULSUS_TEST_CH_DATABASE")
            .unwrap_or_else(|_| "default".to_string()),
        proto: ChProto::Https,
        tls_skip_verify: true,
        pool_size: 2,
        query_timeout: Duration::from_secs(10),
        ..ChConnConfig::default()
    }
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct TestRow {
    fingerprint: u64,
    unix_milli: i64,
    value: f64,
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE_TLS=1 with the TLS-enabled ClickHouse \
                 container running to run this test (see \
                 crates/pulsus-clickhouse/tests/live_tls.rs for setup)"
            );
            return;
        }
    };
}

#[tokio::test]
async fn https_skip_verify_round_trips_an_insert_and_a_query_stream() {
    skip_unless_live!();
    let client = ChClient::new(test_config())
        .await
        .expect("connect over HTTPS with tls_skip_verify=true against a self-signed cert");
    let table = "pulsus_clickhouse_it_tls_roundtrip";

    client
        .execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS {table} (
                    fingerprint UInt64, unix_milli Int64, value Float64
                ) ENGINE = MergeTree ORDER BY (fingerprint, unix_milli)"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("create table over TLS");
    client
        .execute(
            &format!("TRUNCATE TABLE {table}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("truncate over TLS");

    let rows = vec![TestRow {
        fingerprint: 7,
        unix_milli: 1_700_000_000_000,
        value: 9.5,
    }];
    client
        .insert_block(table, &rows)
        .await
        .expect("insert_block over TLS");

    let sql = format!("SELECT fingerprint, unix_milli, value FROM {table}");
    let mut stream = client
        .query_stream::<TestRow>(&sql, &QuerySettings::new())
        .await
        .expect("query_stream over TLS");

    use futures::StreamExt;
    let got = stream
        .next()
        .await
        .expect("one row over TLS")
        .expect("decode");
    assert_eq!(got, rows[0]);
}
