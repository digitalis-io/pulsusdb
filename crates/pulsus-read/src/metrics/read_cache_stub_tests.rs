//! Issue #778 (#803's rule): the read cache's poll and fill against stub
//! servers. A poll is one read request on one server, saved only when an
//! attempt succeeds; a range request whose engine met a server failure
//! stores nothing.
//!
//! **Which server answers which statement**, as in
//! [`super::pass_failure_tests`]: the pool is `ChPool::connect` over the
//! stubs in order, unzoned and fresh per test. Building the client takes
//! `rr = 0` (the first stub listed); the poll rebinds at the start of every
//! attempt, so its first attempt binds `rr = 1`, the second stub listed
//! (`X`), and a second attempt the next healthy one (`Y`, the first).

use std::sync::Arc;

use pulsus_clickhouse::ServerFailure;
use pulsus_model::Tenant;

use super::exec::{MetricQueryParams, MetricsConfig, MetricsEngine};
use super::labels::LabelCache;
use super::pass_failure_tests::{cache_config, stub_client};
use super::read_cache::{
    CacheKey, Header, PollRead, RangeRequest, ReadCache, ReadCacheConfig, answer_range_on,
    poll_once,
};
use super::rollup::{Level, LevelSlot, Levels};
use super::stub_server::{Mode, Reply, StubServer};

const T: i64 = 10_000_000_000;

fn e_lag() -> Reply {
    Reply::Empty(vec![("lag_s", "UInt64")])
}

fn e_wm() -> Reply {
    Reply::Empty(vec![
        ("level", "UInt32"),
        ("shard", "UInt32"),
        ("watermark_ms", "Int64"),
    ])
}

fn e_ids() -> Reply {
    Reply::Empty(vec![("ids", "Array(String)")])
}

/// The poll's cache and level slot: clustered, one 60 s level, a saved
/// read from 5 s before `T`.
fn poll_rig() -> (ReadCache, LevelSlot) {
    let cache = ReadCache::new(ReadCacheConfig {
        budget_bytes: 1 << 20,
        tenant_share_pct: 50,
        clustered: true,
    })
    .with_clock(|| T);
    let levels = LevelSlot::new();
    levels.set(Levels {
        levels: vec![Level {
            res_ms: 60_000,
            table: "metric_samples_60s_dist".to_string(),
            from_ms: 0,
        }],
        shards: 1,
        counter_buckets: 0,
    });
    cache.commit_poll(PollRead {
        lag: Some((T - 5_000, 3)),
        watermarks: Some((T - 5_000, vec![(60_000, 777)])),
        identity: (T - 5_000, vec!["db".to_string()]),
    });
    (cache, levels)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_poll_whose_identity_read_fails_after_lag_and_watermarks_saves_nothing() {
    let s = StubServer::start(Mode::Script(vec![e_lag(), e_wm(), Reply::Code(60)]));
    let client = stub_client(&[&s]).await;
    let (cache, levels) = poll_rig();
    let before = cache.poll_view();
    let g = cache.generation();
    poll_once(&cache, &client, "d", Some("c"), &levels).await;
    assert_eq!(cache.poll_view(), before, "nothing saved");
    assert_eq!(cache.generation(), g, "generation");
    assert_eq!(cache.snapshot().poll_failures_total, 1, "one failure");
    assert_eq!(s.answered(), 3, "{:?}", s.requests());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_poll_that_fails_on_both_servers_saves_nothing_and_counts_one_failure() {
    let y = StubServer::start(Mode::Script(vec![e_lag(), e_wm(), Reply::Code(210)]));
    let x = StubServer::start(Mode::Script(vec![e_lag(), e_wm(), Reply::Code(210)]));
    let client = stub_client(&[&y, &x]).await;
    let (cache, levels) = poll_rig();
    let before = cache.poll_view();
    let g = cache.generation();
    poll_once(&cache, &client, "d", Some("c"), &levels).await;
    assert_eq!(cache.poll_view(), before, "nothing saved");
    assert_eq!(cache.generation(), g, "generation");
    assert_eq!(cache.snapshot().poll_failures_total, 1, "one failure");
    assert_eq!(x.answered(), 3, "X: {:?}", x.requests());
    assert_eq!(y.answered(), 3, "Y: {:?}", y.requests());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_poll_that_fails_over_saves_only_the_second_servers_reads() {
    let y = StubServer::start(Mode::Script(vec![e_lag(), e_wm(), e_ids()]));
    let x = StubServer::start(Mode::Script(vec![e_lag(), e_wm(), Reply::Code(210)]));
    let client = stub_client(&[&y, &x]).await;
    let (cache, levels) = poll_rig();
    let g = cache.generation();
    poll_once(&cache, &client, "d", Some("c"), &levels).await;
    let v = cache.poll_view();
    assert_eq!(v.lag, vec![(T - 5_000, 3), (T, 0)], "one lag entry added");
    assert_eq!(v.watermarks, Some((T, vec![(60_000, 0)])), "watermarks");
    assert_eq!(v.identity, Some(Vec::new()), "identity");
    assert_eq!(v.identity_ok_at, Some(T), "identity_ok_at");
    assert_eq!(cache.generation(), g + 1, "a new identity clears");
    assert_eq!(cache.snapshot().clears_total, 1, "one clear");
    assert_eq!(cache.snapshot().poll_failures_total, 0, "no failure");
    assert_eq!(x.answered(), 3, "X: {:?}", x.requests());
    assert_eq!(y.answered(), 3, "Y: {:?}", y.requests());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_poll_whose_lag_read_fails_sends_nothing_more_and_saves_nothing() {
    let s = StubServer::start(Mode::Script(vec![Reply::Code(60)]));
    let client = stub_client(&[&s]).await;
    let (cache, levels) = poll_rig();
    let before = cache.poll_view();
    let g = cache.generation();
    poll_once(&cache, &client, "d", Some("c"), &levels).await;
    assert_eq!(cache.poll_view(), before, "nothing saved");
    assert_eq!(cache.generation(), g, "generation");
    assert_eq!(cache.snapshot().poll_failures_total, 1, "one failure");
    assert_eq!(s.answered(), 1, "{:?}", s.requests());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_poll_whose_watermark_read_fails_sends_nothing_more_and_saves_nothing() {
    let s = StubServer::start(Mode::Script(vec![e_lag(), Reply::Code(60)]));
    let client = stub_client(&[&s]).await;
    let (cache, levels) = poll_rig();
    let before = cache.poll_view();
    let g = cache.generation();
    poll_once(&cache, &client, "d", Some("c"), &levels).await;
    assert_eq!(cache.poll_view(), before, "nothing saved");
    assert_eq!(cache.generation(), g, "generation");
    assert_eq!(cache.snapshot().poll_failures_total, 1, "one failure");
    assert_eq!(s.answered(), 2, "{:?}", s.requests());
}

/// Q6 (review note 1): every statement of one poll goes to the one server
/// its attempt bound, so a poll that sent each statement on a binding of
/// its own would spread them over both.
#[tokio::test(flavor = "multi_thread")]
async fn a_poll_reads_all_its_statements_on_one_server() {
    let y = StubServer::start(Mode::Script(vec![e_lag(), e_wm(), e_ids()]));
    let x = StubServer::start(Mode::Script(vec![e_lag(), e_wm(), e_ids()]));
    let client = stub_client(&[&y, &x]).await;
    let (cache, levels) = poll_rig();
    poll_once(&cache, &client, "d", Some("c"), &levels).await;
    assert_eq!(cache.snapshot().poll_failures_total, 0, "no failure");
    assert_eq!(x.answered(), 3, "X: {:?}", x.requests());
    assert_eq!(y.answered(), 0, "Y: {:?}", y.requests());
}

fn metrics_config() -> MetricsConfig {
    MetricsConfig {
        max_replica_delay_secs: 5,
        skip_unavailable_shards: false,
        db: "d".to_string(),
        samples_table: "metric_samples".to_string(),
        hist_samples_table: "metric_hist_samples".to_string(),
        series_table: "metric_series".to_string(),
        labels_table: "metric_labels".to_string(),
        label_index_table: "metric_label_index".to_string(),
        label_values_table: "metric_label_values".to_string(),
        exemplars_table: "metric_exemplars".to_string(),
        metadata_table: "metric_metadata".to_string(),
        experimental_functions: true,
        max_metric_fanout: 1_000,
        max_cache_scan: 200_000,
        cache_max_series: 50_000,
        max_info_series: 100_000,
        max_samples: 50_000_000,
        distributed: false,
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        grouped_push: true,
    }
}

/// W1 (one case): `answer_range_on` over an engine, with a server failure
/// injected or not, for the range query `1`.
async fn w1_case(fail: bool) -> (Header, u64, usize) {
    let s = StubServer::start(Mode::Script(Vec::new()));
    let engine = MetricsEngine::new(
        stub_client(&[&s]).await,
        Arc::new(LabelCache::new(stub_client(&[&s]).await, cache_config())),
        metrics_config(),
    );
    let engine = if fail {
        engine.with_injected_failure(ServerFailure {
            endpoint: 0,
            message: "injected".to_string(),
        })
    } else {
        engine
    };
    let cache = ReadCache::new(ReadCacheConfig {
        budget_bytes: 64 * 1024 * 1024,
        tenant_share_pct: 50,
        clustered: false,
    })
    .with_clock(|| T);
    cache.commit_poll(PollRead {
        lag: None,
        watermarks: None,
        identity: (T, vec!["db".to_string()]),
    });
    let tenant = Tenant::from_header(None, false).expect("the empty tenant");
    let expr = pulsus_promql::parse("1").expect("parse");
    let end = (T - 1_800_000).div_euclid(15_000) * 15_000;
    let p = MetricQueryParams {
        start_ms: end - 7_200_000,
        end_ms: end,
        step_ms: 15_000,
    };
    let req = RangeRequest {
        key: CacheKey::new(
            tenant.clone(),
            "1",
            &p,
            engine.lookback_ms(),
            engine.resolution_ms(&p),
        ),
        query: "1",
        params: p,
        facts: engine.plan_facts(&expr, &p).expect("facts"),
        max_samples: u64::MAX,
        retention_floor_ms: Some(i64::MIN),
        sweep_time_ms: None,
    };
    let a = answer_range_on(&cache, &engine, &tenant, &expr, req)
        .await
        .expect("answered");
    (a.header, cache.usage().1, s.answered())
}

#[tokio::test(flavor = "multi_thread")]
async fn an_engine_with_a_server_failure_stores_no_fill() {
    let (header, kept, sent) = w1_case(false).await;
    assert_eq!(header, Header::Miss, "(b) control: kept");
    assert_eq!(kept, 1, "(b) control: one entry");
    assert_eq!(sent, 0, "(b) no statement");
    let (header, kept, sent) = w1_case(true).await;
    assert_eq!(header, Header::Bypass, "(a) with a failure: header");
    assert_eq!(kept, 0, "(a) with a failure: nothing kept");
    assert_eq!(sent, 0, "(a) no statement");
}
