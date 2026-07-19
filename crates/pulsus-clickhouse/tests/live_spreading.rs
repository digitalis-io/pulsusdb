//! Live multi-endpoint connection-spreading + failover tests (issue #43),
//! against real ClickHouse.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1` so plain `cargo test --workspace`
//! stays hermetic. The pure selection-policy and health-transition coverage
//! lives in `src/pool.rs`'s unit tests; this suite proves the same behavior
//! end-to-end over real HTTP endpoints, asserting **identities** (each
//! endpoint used; a dead endpoint demoted; every request served) — never
//! wall-time. AC7 is proved SERVER-side: queries carry a run-unique
//! `log_comment` and each node's `system.query_log` is read directly to prove
//! the marked queries actually landed there, not just that the client's
//! selection counters saw both endpoints.
//!
//! Endpoints reuse the two-node fixture's env contract
//! `PULSUS_TEST_CH_SHARD1_*` / `PULSUS_TEST_CH_SHARD2_*` (exactly as
//! `pulsus-schema`'s `live_cluster.rs`), so the SAME suite runs on:
//!   * the single-node `schema-it` leg — both shard vars point at the one
//!     `localhost:19123` server, proving spreading + failover with one node;
//!   * the two-node `schema-it-cluster` leg — SHARD1/SHARD2 are the two
//!     distinct real nodes (18123/28123), proving true cross-node spread.
//!
//! ```text
//! # single-node:
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 \
//!   PULSUS_TEST_CH_SHARD1_HOST=localhost PULSUS_TEST_CH_SHARD1_HTTP_PORT=19123 \
//!   PULSUS_TEST_CH_SHARD2_HOST=localhost PULSUS_TEST_CH_SHARD2_HTTP_PORT=19123 \
//!   cargo test -p pulsus-clickhouse --test live_spreading
//! ```

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{
    ChClient, ChConnConfig, ChEndpoint, ChPool, ConsistencyConfig, Idempotency, QuerySettings, Row,
};

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
}

/// One endpoint from the shared two-node fixture env contract. Defaults
/// mirror `live_cluster.rs`: the fixture's static container IPs on the plain
/// in-cluster HTTP port, overridable to published host ports.
fn endpoint(host_env: &str, default_host: &str, port_env: &str, default_port: u16) -> ChEndpoint {
    ChEndpoint {
        host: std::env::var(host_env).unwrap_or_else(|_| default_host.to_string()),
        http_port: std::env::var(port_env)
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(default_port),
        zone: None,
    }
}

fn shard1() -> ChEndpoint {
    endpoint(
        "PULSUS_TEST_CH_SHARD1_HOST",
        "172.28.0.11",
        "PULSUS_TEST_CH_SHARD1_HTTP_PORT",
        8123,
    )
}

fn shard2() -> ChEndpoint {
    endpoint(
        "PULSUS_TEST_CH_SHARD2_HOST",
        "172.28.0.12",
        "PULSUS_TEST_CH_SHARD2_HTTP_PORT",
        8123,
    )
}

fn base_config(endpoints: Vec<ChEndpoint>) -> ChConnConfig {
    ChConnConfig {
        endpoints,
        database: "default".to_string(),
        pool_size: 4,
        query_timeout: Duration::from_secs(10),
        ..ChConnConfig::default()
    }
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 (+ PULSUS_TEST_CH_SHARD1_*/SHARD2_*) with \
                 a live ClickHouse to run this test"
            );
            return;
        }
    };
}

#[tokio::test]
async fn requests_spread_across_every_endpoint() {
    // AC5/AC7: a pool over both shard endpoints round-robins so EVERY
    // endpoint is selected, and all queries succeed. On the single-node leg
    // both endpoints resolve to the same server (spreading still observable
    // via the per-endpoint counts); on the cluster leg they are distinct
    // nodes, so this is the real cross-node spread proof.
    skip_unless_live!();
    let pool = ChPool::connect(base_config(vec![shard1(), shard2()]))
        .await
        .expect("connect over both endpoints");

    for _ in 0..20 {
        pool.ping().await.expect("every request is served");
    }

    let counts = pool.endpoint_selection_counts();
    assert_eq!(counts.len(), 2, "one count per endpoint: {counts:?}");
    for (label, n) in &counts {
        assert!(*n > 0, "endpoint {label} was never selected: {counts:?}");
    }
    assert_eq!(
        counts.iter().map(|(_, n)| *n).sum::<u64>(),
        20,
        "every selection is accounted for"
    );
}

/// A run-unique `log_comment` marker so this test's queries are exactly
/// attributable in `system.query_log` (isolates concurrent CI runs and
/// re-runs against the same server).
fn run_marker() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("pulsus_spread_it_{}_{nanos}", std::process::id())
}

#[derive(Row, serde::Serialize, serde::Deserialize)]
struct CountRow {
    c: u64,
}

/// Reads ONE node directly (a single-endpoint pool over `endpoint`), flushes
/// its logs, and counts the finished queries carrying `marker`'s `log_comment`
/// — i.e. how many marked queries actually executed on THIS node.
async fn marked_query_count(endpoint: ChEndpoint, marker: &str) -> u64 {
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

#[tokio::test]
async fn marked_queries_land_on_each_node_server_side() {
    // AC7: SERVER-side proof of cross-node spread. Runs a batch of queries
    // tagged with a run-unique `log_comment` through a pool over both shard
    // endpoints, then reads each node's `system.query_log` DIRECTLY and
    // asserts the marked queries actually executed on BOTH nodes — not merely
    // that the client counters saw both endpoints.
    //
    // On the single-node `schema-it` leg both shard endpoints (and both
    // verification clients) resolve to the same server, so the marker is found
    // on "both" and the test still passes; on the `schema-it-cluster` leg the
    // shards are two distinct nodes, making this the authoritative cross-node
    // landing proof.
    skip_unless_live!();

    let marker = run_marker();
    let pool = ChPool::connect(base_config(vec![shard1(), shard2()]))
        .await
        .expect("connect over both endpoints");
    let client = ChClient::from_shared_pool(Arc::new(pool), Duration::from_secs(10));

    let tagged = QuerySettings::new().set("log_comment", &marker);
    for _ in 0..20 {
        client
            .execute("SELECT 1", &tagged, Idempotency::Idempotent)
            .await
            .expect("marked query served");
    }

    let n1 = marked_query_count(shard1(), &marker).await;
    let n2 = marked_query_count(shard2(), &marker).await;
    assert!(
        n1 > 0,
        "shard1 system.query_log shows no marked queries landed (marker={marker})"
    );
    assert!(
        n2 > 0,
        "shard2 system.query_log shows no marked queries landed (marker={marker})"
    );
}

#[tokio::test]
async fn with_consistency_validates_against_the_client_deadline() {
    // AC14 (issue #114): the fallible `ChClient::with_consistency` wires
    // `self.default_timeout` (the `from_shared_pool` `query_timeout` arg,
    // 10s here) through the full quorum/deadline invariant on the ACTUAL
    // shared-pool path. A `ChClient` cannot be built hermetically
    // (`from_shared_pool` needs a connected `ChPool`), so this is the wiring
    // proof; the invariant's own logic is proved hermetically in
    // `config.rs`'s `validate_for_deadline` unit tests.
    skip_unless_live!();
    let pool = Arc::new(
        ChPool::connect(base_config(vec![shard1()]))
            .await
            .expect("connect for with_consistency wiring proof"),
    );
    let deadline = Duration::from_secs(10);

    // Zero quorum timeout with quorum enabled -> Err (dangerous no/infinite
    // wait), before any I/O.
    let zero = ChClient::from_shared_pool(Arc::clone(&pool), deadline).with_consistency(
        ConsistencyConfig {
            insert_quorum: 2,
            insert_quorum_timeout: Duration::ZERO,
            ..ConsistencyConfig::default()
        },
    );
    assert!(
        zero.is_err(),
        "a zero quorum timeout must be rejected by with_consistency"
    );

    // Quorum timeout above the 10s client deadline -> Err (preempt).
    let over = ChClient::from_shared_pool(Arc::clone(&pool), deadline).with_consistency(
        ConsistencyConfig {
            insert_quorum: 2,
            insert_quorum_timeout: Duration::from_secs(300),
            ..ConsistencyConfig::default()
        },
    );
    assert!(
        over.is_err(),
        "a quorum timeout above the client deadline must be rejected"
    );

    // The default (quorum off) -> Ok.
    let ok =
        ChClient::from_shared_pool(pool, deadline).with_consistency(ConsistencyConfig::default());
    assert!(
        ok.is_ok(),
        "the default consistency config must be accepted"
    );
}

#[tokio::test]
async fn dead_endpoint_is_demoted_and_all_requests_still_served() {
    // AC6: a pool over one live endpoint + one dead port. Every request is
    // served by the live endpoint, and the dead endpoint ends demoted
    // (healthy == false) — never selected on the hot path. No wall-time.
    skip_unless_live!();
    let live = shard1();
    let dead = ChEndpoint {
        host: live.host.clone(),
        http_port: 9, // discard port: nothing listens -> connection refused
        zone: None,
    };
    let pool = ChPool::connect(base_config(vec![live, dead]))
        .await
        .expect("connect succeeds: at least one (the live) endpoint answers");

    for _ in 0..20 {
        pool.ping().await.expect("served by the live endpoint");
    }

    let health = pool.endpoint_health();
    assert_eq!(health.len(), 2);
    assert!(health[0].1, "the live endpoint stays healthy: {health:?}");
    assert!(
        !health[1].1,
        "the dead endpoint must be demoted (healthy == false): {health:?}"
    );

    // All 20 selections landed on the live endpoint; the dead one was
    // skipped on the hot path.
    let counts = pool.endpoint_selection_counts();
    assert_eq!(
        counts[0].1, 20,
        "live endpoint served everything: {counts:?}"
    );
    assert_eq!(counts[1].1, 0, "dead endpoint never selected: {counts:?}");
}
