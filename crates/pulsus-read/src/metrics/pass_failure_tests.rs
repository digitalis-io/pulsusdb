//! Issue #803: the two background passes against stub servers that fail
//! them. A pass whose attempts both meet a server failure saves nothing it
//! read — no label snapshot, no level list, no replica rows — and each
//! attempt was one request on one server.
//!
//! **Which server answers which statement.** The pool is
//! `ChPool::connect` over stubs `A` (endpoint 0) then `B` (endpoint 1),
//! unzoned and fresh per test, so read bindings take `rr = 0, 1, 2, …` in
//! order. Building the client takes `rr = 0`, binding it to `A`; no
//! statement is sent on that binding, because each pass rebinds at the
//! start of every attempt. So a pass's first attempt binds `rr = 1` (`B`),
//! and its second binds the next `rr` over the healthy endpoints only.

use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChEndpoint};
use pulsus_model::Tenant;

use super::labels::{LabelCache, LabelCacheConfig};
use super::rollup::{Level, LevelSlot, Levels, refresh_levels};
use super::stub_server::{Mode, Reply, StubServer};

/// A client over `stubs`, in order, unzoned. Binds `rr = 0`.
pub(crate) async fn stub_client(stubs: &[&StubServer]) -> ChClient {
    let cfg = ChConnConfig {
        endpoints: stubs
            .iter()
            .map(|s| ChEndpoint {
                host: "127.0.0.1".to_string(),
                http_port: s.addr.port(),
                zone: None,
            })
            .collect(),
        database: "default".to_string(),
        pool_size: 4,
        query_timeout: Duration::from_secs(5),
        ..ChConnConfig::default()
    };
    ChClient::new(cfg).await.expect("connect to the stubs")
}

pub(crate) fn cache_config() -> LabelCacheConfig {
    LabelCacheConfig {
        db: "default".to_string(),
        series_table: "metric_series".to_string(),
        labels_table: "metric_labels".to_string(),
        window_ms: 3_600_000,
        cache_max_series: 1_000,
        ttl: Duration::from_secs(60),
        staleness_multiplier: crate::DEFAULT_STALENESS_MULTIPLIER,
        read_max_memory_bytes: 1 << 30,
        skip_unavailable_shards: false,
    }
}

/// The header of [`super::rows::SeriesRow`].
fn e_series() -> Reply {
    Reply::Empty(vec![
        ("fingerprint", "UInt128"),
        ("metric_name", "String"),
        ("labels", "String"),
    ])
}

/// The header of the level-discovery row.
fn e_levels() -> Reply {
    Reply::Empty(vec![
        ("name", "String"),
        ("res_ms", "UInt32"),
        ("from_ms", "UInt64"),
        ("counter_buckets", "UInt32"),
    ])
}

/// The header of the shard-count row.
fn e_shards() -> Reply {
    Reply::Empty(vec![("shards", "UInt32")])
}

/// The header of [`super::rollup::ReplicaDelay`].
fn e_delay() -> Reply {
    Reply::Empty(vec![
        ("host", "String"),
        ("table", "String"),
        ("absolute_delay", "UInt64"),
    ])
}

fn generation(cache: &LabelCache, tenant: &Tenant) -> u64 {
    cache.tenant_cache(tenant).current_snapshot().generation
}

/// P1. `t1` is the cache's built-in empty tenant, `t2` a second one, so
/// the pass sends two statements.
///
/// - `refresh()` 1, attempt 1, bound `B` (`rr = 1`): `t1` and `t2` both
///   answer → generation 1 each.
/// - `refresh()` 2, attempt 1, bound `A` (`rr = 2`): `t1` answers, `t2`
///   meets 210 → `A` demoted.
/// - `refresh()` 2, attempt 2, bound `B` (`rr = 3`, over the healthy `B`
///   alone): `t1` and `t2` both meet 210.
#[tokio::test(flavor = "multi_thread")]
async fn a_label_pass_whose_two_attempts_fail_keeps_both_old_snapshots() {
    let a = StubServer::start(Mode::Script(vec![e_series(), Reply::Code(210)]));
    let b = StubServer::start(Mode::Script(vec![
        e_series(),
        e_series(),
        Reply::Code(210),
        Reply::Code(210),
    ]));
    let cache = LabelCache::new(stub_client(&[&a, &b]).await, cache_config());
    let t1 = Tenant::from_header(None, false).expect("the empty tenant");
    let t2 =
        Tenant::from_header(Some(&http::HeaderValue::from_static("t2")), false).expect("tenant t2");
    assert!(t1 < t2, "t1 is swept first");
    cache.tenant_cache(&t1);
    cache.tenant_cache(&t2);
    assert_eq!(cache.tenants_in_use(), vec![t1.clone(), t2.clone()]);

    cache.refresh().await.expect("the warm pass");
    assert_eq!(generation(&cache, &t1), 1, "t1 warm");
    assert_eq!(generation(&cache, &t2), 1, "t2 warm");
    let failures_before = cache.metrics.snapshot().refresh_failures_total;

    let second = cache.refresh().await;
    assert!(second.is_err(), "both attempts failed: {second:?}");
    assert_eq!(generation(&cache, &t1), 1, "t1 keeps its old snapshot");
    assert_eq!(generation(&cache, &t2), 1, "t2 keeps its old snapshot");
    assert_eq!(
        cache.metrics.snapshot().refresh_failures_total - failures_before,
        2,
        "one refresh failure per tenant"
    );
    assert_eq!(a.answered(), 2, "A: {:?}", a.requests());
    assert_eq!(b.answered(), 4, "B: {:?}", b.requests());
}

fn l0() -> Levels {
    Levels {
        levels: vec![Level {
            res_ms: 5_000,
            table: "metric_rollup_5s".to_string(),
            from_ms: 0,
        }],
        shards: 1,
        counter_buckets: 2,
    }
}

/// P2a.
///
/// - Attempt 1, bound `B` (`rr = 1`): discovery answers (no levels), the
///   shard count answers, the replica statement meets 210 → `B` demoted.
/// - Attempt 2, bound `A` (`rr = 2`, over the healthy `A` alone): discovery
///   meets 210, so no shard count; the replica statement meets 210.
#[tokio::test(flavor = "multi_thread")]
async fn a_level_pass_whose_replica_read_fails_after_levels_succeeded_commits_nothing() {
    let a = StubServer::start(Mode::Script(vec![Reply::Code(210), Reply::Code(210)]));
    let b = StubServer::start(Mode::Script(vec![e_levels(), e_shards(), Reply::Code(210)]));
    let client = stub_client(&[&a, &b]).await;
    let slot = LevelSlot::new();
    slot.set(l0());

    let out = refresh_levels(&slot, &client, "default", "_dist", Some("c")).await;
    assert!(out.is_err(), "both attempts failed: {out:?}");
    assert_eq!(
        slot.get().as_deref(),
        Some(&l0()),
        "the empty level list read by attempt 1 is not committed"
    );
    assert_eq!(slot.replicas(), None, "no replica rows were committed");
    assert_eq!(slot.failures(), 1);
    assert_eq!(a.answered(), 2, "A: {:?}", a.requests());
    assert_eq!(b.answered(), 3, "B: {:?}", b.requests());
}

/// P2b.
///
/// - Attempt 1, bound `B` (`rr = 1`): discovery meets 210 → `B` demoted, no
///   shard count; the replica statement answers (no rows).
/// - Attempt 2, bound `A` (`rr = 2`): the same.
#[tokio::test(flavor = "multi_thread")]
async fn a_level_pass_whose_replica_read_succeeds_after_a_failure_commits_nothing() {
    let a = StubServer::start(Mode::Script(vec![Reply::Code(210), e_delay()]));
    let b = StubServer::start(Mode::Script(vec![Reply::Code(210), e_delay()]));
    let client = stub_client(&[&a, &b]).await;
    let slot = LevelSlot::new();
    slot.set(l0());

    let out = refresh_levels(&slot, &client, "default", "_dist", Some("c")).await;
    assert!(out.is_err(), "both attempts failed: {out:?}");
    assert_eq!(slot.get().as_deref(), Some(&l0()));
    assert_eq!(
        slot.replicas(),
        None,
        "the empty replica read of each attempt is not committed"
    );
    assert_eq!(slot.failures(), 1);
    assert_eq!(a.answered(), 2, "A: {:?}", a.requests());
    assert_eq!(b.answered(), 2, "B: {:?}", b.requests());
}
