//! The two-node fixture's helpers, shared by `live_spreading.rs` and
//! `live_read_spread.rs`: endpoints from `PULSUS_TEST_CH_SHARD1_*` /
//! `PULSUS_TEST_CH_SHARD2_*`, a run-unique `log_comment` marker, and the
//! server-side counts of marked statements read from one node's
//! `system.query_log`.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{
    ChClient, ChConnConfig, ChEndpoint, ChPool, Idempotency, QuerySettings, Row,
};

/// One endpoint from the shared two-node fixture env contract. Defaults
/// mirror `live_cluster.rs`: the fixture's static container IPs on the plain
/// in-cluster HTTP port, overridable to published host ports.
pub fn endpoint(
    host_env: &str,
    default_host: &str,
    port_env: &str,
    default_port: u16,
) -> ChEndpoint {
    ChEndpoint {
        host: std::env::var(host_env).unwrap_or_else(|_| default_host.to_string()),
        http_port: std::env::var(port_env)
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(default_port),
        zone: None,
    }
}

pub fn shard1() -> ChEndpoint {
    endpoint(
        "PULSUS_TEST_CH_SHARD1_HOST",
        "172.28.0.11",
        "PULSUS_TEST_CH_SHARD1_HTTP_PORT",
        8123,
    )
}

pub fn shard2() -> ChEndpoint {
    endpoint(
        "PULSUS_TEST_CH_SHARD2_HOST",
        "172.28.0.12",
        "PULSUS_TEST_CH_SHARD2_HTTP_PORT",
        8123,
    )
}

pub fn base_config(endpoints: Vec<ChEndpoint>) -> ChConnConfig {
    ChConnConfig {
        endpoints,
        database: "default".to_string(),
        pool_size: 4,
        query_timeout: Duration::from_secs(10),
        ..ChConnConfig::default()
    }
}

/// A run-unique `log_comment` marker so this test's queries are exactly
/// attributable in `system.query_log` (isolates concurrent CI runs and
/// re-runs against the same server).
pub fn run_marker() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    pulsus_testkit::test_ident(&format!("pulsus_spread_it_{}_{nanos}", std::process::id()))
}

#[derive(Row, serde::Serialize, serde::Deserialize)]
pub struct CountRow {
    pub c: u64,
}

/// Reads ONE node directly (a single-endpoint pool over `endpoint`), flushes
/// its logs, and counts the finished queries carrying `marker`'s `log_comment`
/// — i.e. how many marked queries actually executed on THIS node.
pub async fn marked_query_count(endpoint: ChEndpoint, marker: &str) -> u64 {
    let pool = ChPool::connect(base_config(vec![endpoint]))
        .await
        .expect("connect to node for query_log verification");
    let client = ChClient::from_shared_pool(Arc::new(pool), Duration::from_secs(10));

    // query_log is flushed asynchronously; force it so the count is stable
    // without any wall-time wait.
    client
        .execute(
            "SYSTEM FLUSH LOGS",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("flush logs");

    // The counting query itself is UNmarked (empty settings), so it never
    // matches its own filter. Filter on the `log_comment` column, not query
    // text, so the counting query's own text mentioning the marker is ignored.
    let sql = format!(
        "SELECT count() AS c FROM system.query_log \
         WHERE log_comment = '{marker}' AND type = 'QueryFinish'"
    );
    let mut stream = client
        .query_stream::<CountRow>(&sql, &QuerySettings::new())
        .await
        .expect("query system.query_log");
    let row = stream
        .next()
        .await
        .expect("one count row")
        .expect("decode count");
    row.c
}

/// [`marked_query_count`] for one `query_kind` (`Select`, `Insert`, …)
/// (issue #803): how many finished statements of that kind carrying
/// `marker` ran on `endpoint`'s node.
pub async fn marked_count(endpoint: ChEndpoint, marker: &str, kind: &str) -> u64 {
    let pool = ChPool::connect(base_config(vec![endpoint]))
        .await
        .expect("connect to node for query_log verification");
    let client = ChClient::from_shared_pool(Arc::new(pool), Duration::from_secs(10));
    client
        .execute(
            "SYSTEM FLUSH LOGS",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("flush logs");
    let sql = format!(
        "SELECT count() AS c FROM system.query_log \
         WHERE log_comment = '{marker}' AND type = 'QueryFinish' AND query_kind = '{kind}'"
    );
    let mut stream = client
        .query_stream::<CountRow>(&sql, &QuerySettings::new())
        .await
        .expect("query system.query_log");
    let row = stream
        .next()
        .await
        .expect("one count row")
        .expect("decode count");
    row.c
}
