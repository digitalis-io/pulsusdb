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

// -- issue #810: a routed hit is one read ------------------------------------

mod t19 {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use pulsus_promql::Annotations;

    use super::super::exec::{MetricQueryParams, PlanFacts, ReadMix};
    use super::super::pass_failure_tests::stub_client;
    use super::super::read_cache::{
        Answer, CacheKey, EvalFacts, Evaluated, Header, PollRead, RangeRequest, ReadCache,
        ReadCacheConfig, answer_range, poll_once,
    };
    use super::super::rollup::{Level, LevelSlot, Levels};
    use super::super::stub_server::{Mode, StubServer};
    use super::{T, e_ids, e_lag, e_wm};
    use crate::logql::{MatrixSeries, QueryResult};

    const Q: &str = "rate(x[5m])";

    /// A single-node cache with a fresh identity and the 60 s level built to
    /// 10 minutes before `T`.
    fn cache() -> ReadCache {
        let c = ReadCache::new(ReadCacheConfig {
            budget_bytes: 64 * 1024 * 1024,
            tenant_share_pct: 50,
            clustered: false,
        })
        .with_clock(|| T);
        commit(&c);
        c
    }

    fn commit(c: &ReadCache) {
        c.commit_poll(PollRead {
            lag: None,
            watermarks: Some((T, vec![(60_000, T - 600_000)])),
            identity: (T, vec!["db".to_string()]),
        });
    }

    /// Two hours at 15 s ending 30 minutes before `T`, shifted by `by`.
    fn window(by: i64) -> MetricQueryParams {
        let end = (T - 1_800_000).div_euclid(15_000) * 15_000 + by;
        MetricQueryParams {
            start_ms: end - 7_200_000,
            end_ms: end,
            step_ms: 15_000,
        }
    }

    fn request(p: MetricQueryParams) -> RangeRequest<'static> {
        let tenant = pulsus_model::Tenant::from_header(None, false).expect("the empty tenant");
        RangeRequest {
            key: CacheKey::new(tenant, Q, &p, 300_000, Some(60_000)),
            query: Q,
            params: p,
            facts: PlanFacts {
                shape_allowed: true,
                oldest_input_ms: 0,
                reach_ms: 0,
            },
            max_samples: u64::MAX,
            retention_floor_ms: Some(i64::MIN),
            sweep_time_ms: None,
        }
    }

    /// A one-series matrix over the grid of `p`, value `base + t / 1000`.
    fn matrix(p: &MetricQueryParams, base: f64) -> QueryResult {
        let mut points = Vec::new();
        let mut t = p.start_ms;
        while t <= p.end_ms {
            points.push((t, base + (t / 1000) as f64));
            t += p.step_ms;
        }
        QueryResult::Matrix(vec![MatrixSeries {
            labels: vec![("job".to_string(), "x".to_string())],
            points,
        }])
    }

    fn info() -> Annotations {
        let mut a = Annotations::new();
        a.info_at(0, "an info");
        a
    }

    fn routed(raw: bool) -> EvalFacts {
        EvalFacts {
            charged: 10,
            reads: ReadMix::AllRouted,
            raw,
        }
    }

    #[derive(Default)]
    struct Calls {
        full: AtomicUsize,
        tail: AtomicUsize,
    }

    type R = Result<Evaluated, ()>;

    /// One request through `cache`: `full` answers `base` 1,000 with
    /// `full_ann`; `tail` answers `base` 0 with `tail_ann` and `tail_facts`,
    /// after running `during_tail`.
    async fn run(
        cache: &ReadCache,
        calls: &Calls,
        p: MetricQueryParams,
        full_ann: Annotations,
        tail_ann: Annotations,
        tail_facts: EvalFacts,
        during_tail: impl FnOnce(),
    ) -> Answer {
        let full = |q: MetricQueryParams| {
            calls.full.fetch_add(1, Ordering::SeqCst);
            async move { R::Ok((matrix(&q, 1_000.0), full_ann, routed(false))) }
        };
        let tail = |q: MetricQueryParams| {
            calls.tail.fetch_add(1, Ordering::SeqCst);
            during_tail();
            async move { R::Ok((matrix(&q, 0.0), tail_ann, tail_facts)) }
        };
        answer_range(cache, request(p), full, tail, |_| (), &|| true)
            .await
            .expect("answered")
    }

    /// The first request, a miss that keeps its points.
    async fn kept(cache: &ReadCache) {
        let calls = Calls::default();
        let a = run(
            cache,
            &calls,
            window(0),
            Annotations::new(),
            Annotations::new(),
            routed(false),
            || {},
        )
        .await;
        assert_eq!(a.header, Header::Miss, "the first request keeps its points");
    }

    fn counts(c: &Calls) -> (usize, usize) {
        (c.tail.load(Ordering::SeqCst), c.full.load(Ordering::SeqCst))
    }

    /// No kept point in `a`: every point is `full`'s, `base` 1,000.
    fn all_full(a: &Answer, p: &MetricQueryParams) -> bool {
        a.result == matrix(p, 1_000.0)
    }

    fn stored(cache: &ReadCache, p: MetricQueryParams) -> bool {
        cache.lookup(&request(p).key).is_some()
    }

    /// **T19 (issue #810, D14): a routed hit is one read.**
    #[tokio::test(flavor = "multi_thread")]
    async fn a_routed_hit_is_one_read() {
        let p2 = window(60_000);

        // (a) A kept answer with no annotation; the tail returns an info.
        let c = cache();
        kept(&c).await;
        let calls = Calls::default();
        let a = run(
            &c,
            &calls,
            p2,
            Annotations::new(),
            info(),
            routed(false),
            || {},
        )
        .await;
        assert_eq!(counts(&calls), (1, 0), "(a) tail, full");
        assert_eq!(a.header, Header::Hit, "(a) header");
        assert_eq!(
            a.annotations.base_messages(),
            info().base_messages(),
            "(a) the tail's annotations"
        );

        // (b) The miss path's answer carries an info: nothing is kept.
        let c = cache();
        let calls = Calls::default();
        run(
            &c,
            &calls,
            window(0),
            info(),
            Annotations::new(),
            routed(false),
            || {},
        )
        .await;
        assert!(!stored(&c, window(0)), "(b) nothing stored");

        // (c) The tail is read raw (addendum r6): merged with the kept
        // points and answered, and nothing is stored.
        let c = cache();
        kept(&c).await;
        let before = c.lookup(&request(window(0)).key).expect("kept").c1_ms;
        let calls = Calls::default();
        let a = run(
            &c,
            &calls,
            p2,
            Annotations::new(),
            Annotations::new(),
            routed(true),
            || {},
        )
        .await;
        assert_eq!(counts(&calls), (1, 0), "(c) tail, full");
        assert_eq!(a.header, Header::Hit, "(c) a hit");
        let QueryResult::Matrix(series) = &a.result else {
            panic!("(c) a matrix: {:?}", a.result)
        };
        let want: Vec<(i64, f64)> = {
            let mut t = p2.start_ms;
            let mut points = Vec::new();
            while t <= p2.end_ms {
                let base = if t <= before { 1_000.0 } else { 0.0 };
                points.push((t, base + (t / 1000) as f64));
                t += p2.step_ms;
            }
            points
        };
        assert_eq!(series.len(), 1, "(c) one series");
        assert_eq!(series[0].points, want, "(c) kept points, then the tail's");
        assert_eq!(
            c.lookup(&request(p2).key).map(|e| e.c1_ms),
            Some(before),
            "(c) nothing stored"
        );

        // (d) The cache is cleared from inside the tail.
        let c = Arc::new(cache());
        kept(&c).await;
        let calls = Calls::default();
        let inner = Arc::clone(&c);
        let a = run(
            &c,
            &calls,
            p2,
            Annotations::new(),
            Annotations::new(),
            routed(false),
            move || inner.clear(),
        )
        .await;
        assert_eq!(counts(&calls), (1, 0), "(d) tail, full");
        assert!(!all_full(&a, &p2), "(d) the merge is served");
        assert!(!stored(&c, p2), "(d) nothing stored");

        // (e) Cleared before the request.
        let c = cache();
        kept(&c).await;
        c.clear();
        let calls = Calls::default();
        let a = run(
            &c,
            &calls,
            p2,
            Annotations::new(),
            Annotations::new(),
            routed(false),
            || {},
        )
        .await;
        assert_eq!(counts(&calls), (0, 1), "(e) tail, full");
        assert!(all_full(&a, &p2), "(e) the full answer");

        // (f) The identity read is 10 s old.
        let clock = Arc::new(std::sync::atomic::AtomicI64::new(T));
        let at = Arc::clone(&clock);
        let c = ReadCache::new(ReadCacheConfig {
            budget_bytes: 64 * 1024 * 1024,
            tenant_share_pct: 50,
            clustered: false,
        })
        .with_clock(move || at.load(Ordering::SeqCst));
        commit(&c);
        kept(&c).await;
        clock.store(T + 10_000, Ordering::SeqCst);
        let calls = Calls::default();
        let a = run(
            &c,
            &calls,
            p2,
            Annotations::new(),
            Annotations::new(),
            routed(false),
            || {},
        )
        .await;
        assert_eq!(counts(&calls), (0, 1), "(f) tail, full");
        assert!(all_full(&a, &p2), "(f) the full answer");

        // (g) Cleared by a hook after the lookup, before the check.
        let c = cache().with_after_lookup(|c| c.clear());
        kept(&c).await;
        let calls = Calls::default();
        let a = run(
            &c,
            &calls,
            p2,
            Annotations::new(),
            Annotations::new(),
            routed(false),
            || {},
        )
        .await;
        assert_eq!(counts(&calls), (0, 1), "(g) tail, full");
        assert!(all_full(&a, &p2), "(g) the full answer");

        // (h) The hook runs a poll whose identity read returns a new
        // identity.
        let s = StubServer::start(Mode::Script(vec![e_lag(), e_wm(), e_ids()]));
        let client = Arc::new(stub_client(&[&s]).await);
        let levels = Arc::new(LevelSlot::new());
        levels.set(Levels {
            levels: vec![Level {
                res_ms: 60_000,
                table: "metric_samples_60s".to_string(),
                from_ms: 0,
            }],
            shards: 1,
            counter_buckets: 5,
        });
        let handle = tokio::runtime::Handle::current();
        let c = cache().with_after_lookup(move |c| {
            let (client, levels) = (Arc::clone(&client), Arc::clone(&levels));
            tokio::task::block_in_place(|| {
                handle.block_on(poll_once(c, &client, "d", Some("c"), &levels))
            });
        });
        kept(&c).await;
        let g = c.generation();
        let calls = Calls::default();
        let a = run(
            &c,
            &calls,
            p2,
            Annotations::new(),
            Annotations::new(),
            routed(false),
            || {},
        )
        .await;
        assert_eq!(counts(&calls), (0, 1), "(h) tail, full");
        assert!(all_full(&a, &p2), "(h) the full answer");
        assert_eq!(c.generation(), g + 1, "(h) the poll cleared");
    }
}
