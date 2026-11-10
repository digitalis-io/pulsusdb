//! Live: read requests take turns over every server, each request's reads
//! stay on its one server, a request whose server drops runs once more on
//! another, and writes keep to the preferred zone (issue #803).
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, over the two-node fixture's
//! `PULSUS_TEST_CH_SHARD1_*` / `PULSUS_TEST_CH_SHARD2_*`, as
//! `live_spreading.rs` is. **Each test first asserts that the two are
//! different servers** (`SELECT hostName()` differs), so on one server it
//! fails rather than passing vacuously; that is why this is a binary of its
//! own, run on the two-node leg only.
//!
//! Placement is proved server side: every statement carries a run-unique
//! `log_comment`, and each node's `system.query_log` is read directly.

#[path = "common/relay.rs"]
mod relay;
#[path = "common/two_nodes.rs"]
mod two_nodes;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{
    Attempt, ChClient, ChEndpoint, ChError, ChPool, Idempotency, QuerySettings, ReadSpread, Row,
    run_read_request,
};
use relay::Relay;
use two_nodes::{CountRow, base_config, marked_count, run_marker, shard1, shard2};

macro_rules! skip_unless_live {
    () => {
        if !pulsus_testkit::live_clickhouse_enabled() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 (+ PULSUS_TEST_CH_SHARD1_*/SHARD2_*) with \
                 two live ClickHouse servers to run this test"
            );
            return;
        }
    };
}

#[derive(Row, serde::Serialize, serde::Deserialize)]
struct HostRow {
    h: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize)]
struct ValueRow {
    v: String,
}

fn zoned(mut e: ChEndpoint, zone: &str) -> ChEndpoint {
    e.zone = Some(zone.to_string());
    e
}

async fn one_node(e: ChEndpoint) -> ChClient {
    let pool = ChPool::connect(base_config(vec![e]))
        .await
        .expect("connect one node");
    ChClient::from_shared_pool(Arc::new(pool), Duration::from_secs(10))
}

async fn host_name(e: ChEndpoint) -> String {
    let client = one_node(e).await;
    let mut stream = client
        .query_stream::<HostRow>("SELECT hostName() AS h", &QuerySettings::new())
        .await
        .expect("hostName()");
    stream.next().await.expect("one row").expect("decode").h
}

/// The two shard endpoints are two different servers.
async fn assert_two_servers() {
    let (h1, h2) = (host_name(shard1()).await, host_name(shard2()).await);
    assert_ne!(
        h1, h2,
        "shard1 and shard2 must be different servers for this suite"
    );
}

/// One marked read through `client`, drained.
async fn marked_read(client: &ChClient, marker: &str) -> Result<(), ChError> {
    let mut stream = client
        .query_stream::<CountRow>(
            "SELECT toUInt64(1) AS c",
            &QuerySettings::new().set("log_comment", marker),
        )
        .await?;
    while let Some(row) = stream.next().await {
        row?;
    }
    Ok(())
}

/// `(shard1, shard2)` counts of `marker`'s finished statements of `kind`.
async fn counts(marker: &str, kind: &str) -> (u64, u64) {
    (
        marked_count(shard1(), marker, kind).await,
        marked_count(shard2(), marker, kind).await,
    )
}

fn selections(pool: &ChPool) -> Vec<u64> {
    pool.endpoint_selection_counts()
        .into_iter()
        .map(|(_, n)| n)
        .collect()
}

fn two_zoned(read_spread: ReadSpread) -> pulsus_clickhouse::ChConnConfig {
    let mut cfg = base_config(vec![zoned(shard1(), "az-a"), zoned(shard2(), "az-b")]);
    cfg.local_zone = Some("az-a".to_string());
    cfg.read_spread = read_spread;
    cfg
}

#[tokio::test(flavor = "multi_thread")]
async fn requests_reach_both_servers_and_writes_keep_to_the_preferred_one() {
    skip_unless_live!();
    assert_two_servers().await;
    let m = run_marker();
    let table = format!("{m}_t");
    let shard1_only = one_node(shard1()).await;
    shard1_only
        .execute(
            &format!("CREATE TABLE default.{table} (v String) ENGINE = Memory"),
            &QuerySettings::new(),
            Idempotency::NonIdempotent,
        )
        .await
        .expect("create the insert target on shard1");

    let pool = Arc::new(
        ChPool::connect(two_zoned(ReadSpread::All))
            .await
            .expect("connect"),
    );
    let (m_r, m_w, m_i) = (format!("{m}_r"), format!("{m}_w"), format!("{m}_i"));
    for _ in 0..20 {
        let client = ChClient::from_shared_pool(Arc::clone(&pool), Duration::from_secs(10));
        marked_read(&client, &m_r).await.expect("read");
    }
    let writer = ChClient::from_shared_pool(Arc::clone(&pool), Duration::from_secs(10));
    for _ in 0..20 {
        writer
            .execute(
                "SELECT 1",
                &QuerySettings::new().set("log_comment", &m_w),
                Idempotency::Idempotent,
            )
            .await
            .expect("execute");
    }
    for _ in 0..20 {
        writer
            .insert_block_with(
                &table,
                &[ValueRow { v: "x".to_string() }],
                &QuerySettings::new().set("log_comment", &m_i),
            )
            .await
            .expect("insert");
    }

    let reads = counts(&m_r, "Select").await;
    let executes = counts(&m_w, "Select").await;
    let inserts = counts(&m_i, "Insert").await;
    let _ = shard1_only
        .execute(
            &format!("DROP TABLE IF EXISTS default.{table}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await;
    assert_eq!(reads, (10, 10), "read requests take turns over both");
    assert_eq!(executes, (20, 0), "writes keep to the preferred zone");
    assert_eq!(inserts, (20, 0), "inserts keep to the preferred zone");
}

#[tokio::test(flavor = "multi_thread")]
async fn read_spread_zone_keeps_requests_on_the_preferred_server() {
    skip_unless_live!();
    assert_two_servers().await;
    let m = run_marker();
    let pool = Arc::new(
        ChPool::connect(two_zoned(ReadSpread::Zone))
            .await
            .expect("connect"),
    );
    let (m_r, m_w) = (format!("{m}_r"), format!("{m}_w"));
    for _ in 0..20 {
        let client = ChClient::from_shared_pool(Arc::clone(&pool), Duration::from_secs(10));
        marked_read(&client, &m_r).await.expect("read");
    }
    let writer = ChClient::from_shared_pool(Arc::clone(&pool), Duration::from_secs(10));
    for _ in 0..20 {
        writer
            .execute(
                "SELECT 1",
                &QuerySettings::new().set("log_comment", &m_w),
                Idempotency::Idempotent,
            )
            .await
            .expect("execute");
    }
    assert_eq!(counts(&m_r, "Select").await, (20, 0), "reads");
    assert_eq!(counts(&m_w, "Select").await, (20, 0), "writes");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dead_server_takes_no_requests() {
    skip_unless_live!();
    assert_two_servers().await;
    let m = run_marker();
    let live = shard1();
    let dead = ChEndpoint {
        host: live.host.clone(),
        http_port: 9, // discard port: nothing listens -> connection refused
        zone: Some("az-c".to_string()),
    };
    let mut cfg = two_zoned(ReadSpread::All);
    cfg.endpoints.push(dead);
    let pool = Arc::new(ChPool::connect(cfg).await.expect("connect"));
    for _ in 0..20 {
        let client = ChClient::from_shared_pool(Arc::clone(&pool), Duration::from_secs(10));
        marked_read(&client, &m)
            .await
            .expect("every request is served");
    }
    assert_eq!(selections(&pool)[2], 0, "the dead server is never selected");
    assert_eq!(counts(&m, "Select").await, (10, 10));
}

#[tokio::test(flavor = "multi_thread")]
async fn one_requests_reads_stay_on_one_server() {
    skip_unless_live!();
    assert_two_servers().await;
    let m = run_marker();
    let pool = Arc::new(
        ChPool::connect(two_zoned(ReadSpread::All))
            .await
            .expect("connect"),
    );
    let (m_a, m_b) = (format!("{m}_a"), format!("{m}_b"));
    let a = ChClient::from_shared_pool(Arc::clone(&pool), Duration::from_secs(10));
    let b = ChClient::from_shared_pool(Arc::clone(&pool), Duration::from_secs(10));
    for _ in 0..20 {
        marked_read(&a, &m_a).await.expect("read a");
        marked_read(&b, &m_b).await.expect("read b");
    }
    assert_eq!(counts(&m_a, "Select").await, (20, 0), "client A");
    assert_eq!(counts(&m_b, "Select").await, (0, 20), "client B");
}

#[tokio::test(flavor = "multi_thread")]
async fn three_servers_a_request_whose_server_drops_runs_again_on_one_other() {
    skip_unless_live!();
    assert_two_servers().await;
    let m = run_marker();
    let (m1, m2) = (format!("{m}_1"), format!("{m}_2"));
    let s1 = shard1();
    let relay_a = Relay::start(&s1.host, s1.http_port);
    let relay_c = Relay::start(&s1.host, s1.http_port);
    let through = |r: &Relay, zone: &str| ChEndpoint {
        host: "127.0.0.1".to_string(),
        http_port: r.addr.port(),
        zone: Some(zone.to_string()),
    };
    let mut cfg = base_config(vec![
        through(&relay_a, "az-a"),
        zoned(shard2(), "az-b"),
        through(&relay_c, "az-c"),
    ]);
    cfg.local_zone = Some("az-a".to_string());
    cfg.read_spread = ReadSpread::All;
    let pool = Arc::new(ChPool::connect(cfg).await.expect("connect"));

    let calls = AtomicUsize::new(0);
    let (pool_ref, calls_ref, relay_ref, m1_ref, m2_ref) = (&pool, &calls, &relay_a, &m1, &m2);
    let out = run_read_request(
        &pool,
        move |avoid| async move {
            let n = calls_ref.fetch_add(1, Ordering::SeqCst) + 1;
            let client = ChClient::from_shared_pool_avoiding(
                Arc::clone(pool_ref),
                Duration::from_secs(10),
                avoid,
            );
            if n == 1 {
                marked_read(&client, m1_ref)
                    .await
                    .expect("the first read is served");
                relay_ref.switch_off();
                let _ = tokio::join!(
                    marked_read(&client, m1_ref),
                    marked_read(&client, m1_ref),
                    marked_read(&client, m1_ref),
                );
            } else {
                let (r1, r2, r3, r4) = tokio::join!(
                    marked_read(&client, m2_ref),
                    marked_read(&client, m2_ref),
                    marked_read(&client, m2_ref),
                    marked_read(&client, m2_ref),
                );
                for r in [r1, r2, r3, r4] {
                    r.expect("attempt 2's reads are served");
                }
            }
            Attempt {
                result: Ok::<usize, ChError>(n),
                failure: client.server_failure(),
            }
        },
        ChError::from,
    )
    .await;

    assert_eq!(
        out.expect("attempt 2 is accepted"),
        2,
        "the result is attempt 2's"
    );
    assert_eq!(counts(&m1, "Select").await, (1, 0), "m1");
    assert_eq!(selections(&pool), vec![4, 0, 4], "selections");
    assert_eq!(counts(&m2, "Select").await, (4, 0), "m2");
    assert!(
        !pool.endpoint_health()[0].1,
        "relay A's endpoint is demoted"
    );
}
