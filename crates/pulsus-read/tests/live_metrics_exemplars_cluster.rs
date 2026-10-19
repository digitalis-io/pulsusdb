//! Issue #501 part 2: `/api/v1/query_exemplars`' read over the two-shard
//! fixture, with rows placed by local inserts on each shard — a series'
//! registration and its exemplars on different shards, an exemplar and a
//! label row stored on both, an answer thinned across both.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1` and the 2-shard fixture:
//!
//! ```text
//! docker compose -f ci/clickhouse-cluster/compose.yaml up -d
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test live_metrics_exemplars_cluster
//! docker compose -f ci/clickhouse-cluster/compose.yaml down -v
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_model::{LabelMatcher, MatchOp, Tenant};
use pulsus_read::logql::TooBroadReason;
use pulsus_read::metrics::sql::discovery_distinct_names_query;
use pulsus_read::{
    DataWindow, DiscoveryFilter, ExemplarSeries, LabelCache, LabelCacheConfig, MetricsConfig,
    MetricsEngine, ReadError, StoredExemplar,
};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

const CLUSTER_NAME: &str = "pulsus_test_cluster";
const MINUTE_MS: i64 = 60_000;

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with the 2-shard cluster fixture up to \
                 run this test (see crates/pulsus-read/tests/live_metrics_exemplars_cluster.rs)"
            );
            return;
        }
    };
}

/// The fixture's static addresses, as `live_metrics_cluster_fallback`
/// dials them, overridable by the same variables.
fn shard_config(
    host_env: &str,
    default_host: &str,
    port_env: &str,
    database: &str,
) -> ChConnConfig {
    ChConnConfig {
        server: std::env::var(host_env).unwrap_or_else(|_| default_host.to_string()),
        http_port: std::env::var(port_env)
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(8123),
        database: database.to_string(),
        proto: ChProto::Http,
        pool_size: 8,
        query_timeout: Duration::from_secs(120),
        ..ChConnConfig::default()
    }
}

fn shard1_config(database: &str) -> ChConnConfig {
    shard_config(
        "PULSUS_TEST_CH_SHARD1_HOST",
        "172.28.0.11",
        "PULSUS_TEST_CH_SHARD1_HTTP_PORT",
        database,
    )
}

fn shard2_config(database: &str) -> ChConnConfig {
    shard_config(
        "PULSUS_TEST_CH_SHARD2_HOST",
        "172.28.0.12",
        "PULSUS_TEST_CH_SHARD2_HTTP_PORT",
        database,
    )
}

fn shard1b_config(database: &str) -> ChConnConfig {
    shard_config(
        "PULSUS_TEST_CH_SHARD1B_HOST",
        "172.28.0.13",
        "PULSUS_TEST_CH_SHARD1B_HTTP_PORT",
        database,
    )
}

fn cluster_ctx(db: &str) -> RenderCtx {
    RenderCtx {
        db: db.to_string(),
        cluster: Some(CLUSTER_NAME.to_string()),
        dist_suffix: "_dist".to_string(),
        ..RenderCtx::for_tests(db)
    }
}

fn engine_config(db: &str) -> MetricsConfig {
    MetricsConfig {
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        db: db.to_string(),
        samples_table: "metric_samples_dist".to_string(),
        hist_samples_table: "metric_hist_samples_dist".to_string(),
        series_table: "metric_series_dist".to_string(),
        labels_table: "metric_labels_dist".to_string(),
        label_index_table: "metric_label_index_dist".to_string(),
        label_values_table: "metric_label_values".to_string(),
        exemplars_table: "metric_exemplars_dist".to_string(),
        metadata_table: "metric_metadata".to_string(),
        experimental_functions: false,
        max_metric_fanout: 1_000,
        max_cache_scan: 200_000,
        cache_max_series: 50_000,
        max_info_series: 100_000,
        max_samples: 50_000_000,
        distributed: true,
        grouped_push: true,
    }
}

fn cache_config(db: &str) -> LabelCacheConfig {
    LabelCacheConfig {
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        db: db.to_string(),
        series_table: "metric_series_dist".to_string(),
        labels_table: "metric_labels_dist".to_string(),
        window_ms: 24 * 3_600_000,
        cache_max_series: 50_000,
        ttl: Duration::from_secs(60),
        staleness_multiplier: 3,
    }
}

fn no_tenant() -> Tenant {
    Tenant::from_header(None, false).expect("the empty tenant")
}

/// `m{job="sel"}`, the selector every case reads.
fn selected() -> DiscoveryFilter {
    DiscoveryFilter {
        metric_name: Some("m".to_string()),
        name_matchers: vec![],
        matchers: vec![LabelMatcher {
            key: "job".to_string(),
            op: MatchOp::Eq,
            value: "sel".to_string(),
        }],
    }
}

fn pairs(p: &[(&str, &str)]) -> Vec<(String, String)> {
    p.iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

struct Cluster {
    db: String,
    bootstrap: ChClient,
    shard1: ChClient,
    shard2: ChClient,
    /// Shard 1's second replica (issue #599 part B), which a distributed
    /// read may pick for shard 1.
    replica1b: ChClient,
    /// Thirty minutes ago, minute-aligned.
    t0: i64,
}

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::NonIdempotent)
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
}

impl Cluster {
    /// `db` is composed by `pulsus_testkit::test_db` at the call site,
    /// where the naming guard reads it.
    async fn new(db: String) -> Cluster {
        let bootstrap = ChClient::new(shard1_config("default"))
            .await
            .expect("connect shard1 (bootstrap)");
        drop_database(&bootstrap, &db).await;
        run_init(&bootstrap, &cluster_ctx(&db))
            .await
            .expect("run_init (clustered)");
        let shard1 = ChClient::new(shard1_config(&db)).await.expect("shard1");
        let shard2 = ChClient::new(shard2_config(&db)).await.expect("shard2");
        let replica1b = ChClient::new(shard1b_config(&db))
            .await
            .expect("shard 1's second replica");
        let now = chrono::Utc::now().timestamp_millis();
        Cluster {
            db,
            bootstrap,
            shard1,
            shard2,
            replica1b,
            t0: (now / MINUTE_MS) * MINUTE_MS - 30 * MINUTE_MS,
        }
    }

    fn shard(&self, n: u8) -> &ChClient {
        if n == 1 { &self.shard1 } else { &self.shard2 }
    }

    /// Registers series `fp` with `labels` on shard `n` only, through that
    /// node's own `metric_landing`, in the hours of `t0` and `t0 + 10 min`.
    async fn register(&self, n: u8, fp: u128, labels: &str) {
        let rows: Vec<String> = [self.t0, self.t0 + 10 * MINUTE_MS]
            .iter()
            .map(|at| format!("('', 0, 2, 'm', toUInt128({fp}), {at}, 0, '{labels}', 0)"))
            .collect();
        exec(
            self.shard(n),
            &format!(
                "INSERT INTO metric_landing (org_id, received_ms, kind, metric_name, \
                 fingerprint, unix_milli, value, labels, value_type) VALUES {}",
                rows.join(", ")
            ),
        )
        .await;
    }

    /// Stores exemplars `(fp, ms, value)` in shard `n`'s own
    /// `metric_exemplars`, no ids, no labels.
    async fn exemplars(&self, n: u8, rows: &[(u128, i64, f64)]) {
        let values: Vec<String> = rows
            .iter()
            .map(|(fp, ms, v)| {
                format!(
                    "('', toUInt128({fp}), {ms}, {v}, toFixedString('', 16), \
                     toFixedString('', 8), '{{}}')"
                )
            })
            .collect();
        exec(
            self.shard(n),
            &format!(
                "INSERT INTO metric_exemplars (org_id, fingerprint, unix_milli, value, \
                 trace_id, span_id, labels) VALUES {}",
                values.join(", ")
            ),
        )
        .await;
    }

    /// Waits until shard 1's second replica holds every row written on
    /// shard 1, so a read that picks either replica of shard 1 sees them.
    async fn sync(&self) {
        let tables = self
            .replica1b
            .query_strings(
                &format!(
                    "SELECT table AS s FROM system.replicas WHERE database = '{}'",
                    self.db
                ),
                &QuerySettings::new(),
            )
            .await
            .expect("shard 1's second replica lists its tables");
        for table in tables {
            exec(
                &self.replica1b,
                &format!("SYSTEM SYNC REPLICA {}.{table}", self.db),
            )
            .await;
        }
    }

    /// An engine whose client is shard `n`, so that node coordinates.
    async fn engine(&self, n: u8) -> MetricsEngine {
        let config = if n == 1 {
            shard1_config(&self.db)
        } else {
            shard2_config(&self.db)
        };
        let cache = Arc::new(LabelCache::new(
            ChClient::new(config.clone()).await.expect("connect"),
            cache_config(&self.db),
        ));
        MetricsEngine::new(
            ChClient::new(config).await.expect("connect"),
            cache,
            engine_config(&self.db),
        )
    }

    fn window(&self) -> DataWindow {
        DataWindow {
            start_ms: self.t0 - 5 * MINUTE_MS,
            end_ms: self.t0 + 10 * MINUTE_MS,
        }
    }

    async fn answers(&self) -> (Vec<ExemplarSeries>, Vec<ExemplarSeries>) {
        self.sync().await;
        let mut out = Vec::new();
        for n in [1, 2] {
            out.push(
                self.engine(n)
                    .await
                    .exemplars(&no_tenant(), &[selected()], self.window())
                    .await
                    .unwrap_or_else(|e| panic!("through shard {n}: {e}")),
            );
        }
        let second = out.pop().expect("two answers");
        (out.pop().expect("two answers"), second)
    }

    async fn finish(self) {
        drop_database(&self.bootstrap, &self.db).await;
    }
}

async fn drop_database(client: &ChClient, db: &str) {
    client
        .execute(
            &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database on cluster");
}

fn ex(value: f64, unix_milli: i64) -> StoredExemplar {
    StoredExemplar {
        labels: vec![],
        value,
        unix_milli,
    }
}

/// K1 and K2: series A registered on shard 1 with exemplars on 1, 1 and 2;
/// B registered and stored on 2; C registered on 1, its exemplar on 2 only;
/// D, not selected, on 1.
#[tokio::test(flavor = "multi_thread")]
async fn a_series_and_its_exemplars_on_different_shards_are_found() {
    skip_unless_live!();
    let c = Cluster::new(pulsus_testkit::test_db("pulsus_exemplars_k1")).await;
    let t0 = c.t0;
    c.register(1, 1, r#"{"job":"sel","s":"a"}"#).await;
    c.register(2, 2, r#"{"job":"sel","s":"b"}"#).await;
    c.register(1, 3, r#"{"job":"sel","s":"c"}"#).await;
    c.register(1, 4, r#"{"job":"other","s":"d"}"#).await;
    c.exemplars(1, &[(1, t0, 1.5), (1, t0 + 1_000, 2.5), (4, t0, 9.0)])
        .await;
    c.exemplars(2, &[(1, t0 + 2_000, 3.5), (2, t0, 4.5), (3, t0, 5.5)])
        .await;
    let want = vec![
        ExemplarSeries {
            series_labels: pairs(&[("__name__", "m"), ("job", "sel"), ("s", "a")]),
            exemplars: vec![ex(1.5, t0), ex(2.5, t0 + 1_000), ex(3.5, t0 + 2_000)],
        },
        ExemplarSeries {
            series_labels: pairs(&[("__name__", "m"), ("job", "sel"), ("s", "b")]),
            exemplars: vec![ex(4.5, t0)],
        },
        ExemplarSeries {
            series_labels: pairs(&[("__name__", "m"), ("job", "sel"), ("s", "c")]),
            exemplars: vec![ex(5.5, t0)],
        },
    ];
    let (one, two) = c.answers().await;
    assert_eq!(one, want, "K1 through shard 1");
    assert_eq!(two, want, "K1 through shard 2");

    // K2: statement C with a shard-local `IN` over the same distributed
    // text loses A's shard-2 exemplar and C's, so this data tells the two
    // forms apart.
    let ids = discovery_distinct_names_query(
        &no_tenant(),
        "metric_series_dist",
        "metric_labels_dist",
        &selected(),
        c.window(),
    )
    .replacen("SELECT DISTINCT metric_name\n", "SELECT fingerprint\n", 1)
    .replace("\nORDER BY metric_name", "");
    let window = c.window();
    let count = |in_: &str| {
        format!(
            "SELECT toString(count()) AS s\n\
             FROM metric_exemplars_dist\n\
             WHERE org_id = ''\n\
             \x20 AND unix_milli >= {} AND unix_milli <= {}\n\
             \x20 AND (fingerprint {in_} (\n{ids}\n))",
            window.start_ms, window.end_ms
        )
    };
    let local = QuerySettings::new().set("distributed_product_mode", "local");
    let n = |rows: Vec<String>| rows.first().cloned().unwrap_or_default();
    let shard_local = n(c
        .shard1
        .query_strings(&count("IN"), &local)
        .await
        .expect("K2: the shard-local form runs"));
    let global = n(c
        .shard1
        .query_strings(&count("GLOBAL IN"), &local)
        .await
        .expect("K2: the global form runs"));
    assert_eq!(
        (shard_local.as_str(), global.as_str()),
        ("3", "5"),
        "K2: the shard-local IN counts 3 of the 5"
    );
    c.finish().await;
}

/// K3: 150,000 exemplars, alternate rows on each shard, as L6 — the same
/// 15,000, one per 10 ms slice, its highest.
#[tokio::test(flavor = "multi_thread")]
async fn a_thinned_answer_is_thinned_over_both_shards() {
    skip_unless_live!();
    let c = Cluster::new(pulsus_testkit::test_db("pulsus_exemplars_k3")).await;
    let t0 = (c.t0 / 10) * 10;
    split_150k(&c, t0, "{}").await;
    for n in [1, 2] {
        let got = c
            .engine(n)
            .await
            .exemplars(&no_tenant(), &[selected()], window_150k(t0))
            .await
            .unwrap_or_else(|e| panic!("K3 through shard {n}: {e}"));
        let kept = kept_by_s(&got);
        assert_eq!(kept.len(), 15_000, "K3 through shard {n}");
        assert!(
            kept == thinned_150k(t0),
            "K3 through shard {n}: the kept exemplars differ"
        );
    }
    c.finish().await;
}

/// K3's data: 150,000 exemplars over 150 s from `t0`, row `i` on series
/// `i mod 3` with value `(7919 i) mod 1000` and labels `labels`, even rows
/// on shard 1 and odd rows on shard 2 — 75,000 distinct on each, each
/// under A's per-node row cap.
async fn split_150k(c: &Cluster, t0: i64, labels: &str) {
    for s in 0..3u128 {
        c.register(1, 100 + s, &format!(r#"{{"job":"sel","s":"{s}"}}"#))
            .await;
    }
    for (n, parity) in [(1u8, 0), (2u8, 1)] {
        exec(
            c.shard(n),
            &format!(
                "INSERT INTO metric_exemplars (org_id, fingerprint, unix_milli, value, \
                 trace_id, span_id, labels) \
                 SELECT '', toUInt128(100 + number % 3), {t0} + number, \
                        toFloat64((number * 7919) % 1000), toFixedString('', 16), \
                        toFixedString('', 8), '{labels}' \
                 FROM numbers(150000) WHERE number % 2 = {parity}"
            ),
        )
        .await;
    }
    c.sync().await;
}

fn window_150k(t0: i64) -> DataWindow {
    DataWindow {
        start_ms: t0,
        end_ms: t0 + 149_999,
    }
}

/// K3's expectation: per 10 ms slice its highest value, the earliest of
/// equals, as `(s, time, value bits)`.
fn thinned_150k(t0: i64) -> BTreeSet<(String, i64, u64)> {
    let mut best: BTreeMap<i64, (i64, i64, f64)> = BTreeMap::new();
    for i in 0..150_000i64 {
        let (s, t, v) = (i % 3, t0 + i, ((i * 7919) % 1000) as f64);
        let slot = best.entry(t.div_euclid(10)).or_insert((s, t, v));
        if (v, -t) > (slot.2, -slot.1) {
            *slot = (s, t, v);
        }
    }
    best.values()
        .map(|(s, t, v)| (s.to_string(), *t, v.to_bits()))
        .collect()
}

/// An answer as `(s, time, value bits)`.
fn kept_by_s(answer: &[ExemplarSeries]) -> BTreeSet<(String, i64, u64)> {
    answer
        .iter()
        .flat_map(|series| {
            let s = series
                .series_labels
                .iter()
                .find(|(k, _)| k == "s")
                .map(|(_, v)| v.clone())
                .expect("an s label");
            series
                .exemplars
                .iter()
                .map(move |e| (s.clone(), e.unix_milli, e.value.to_bits()))
        })
        .collect()
}

/// 90,000 distinct exemplars from `t0`, the first 60,000 on shard 1 and
/// the rest on shard 2, row `i` on series `i mod 3` with value `i`.
async fn split_90k(c: &Cluster, t0: i64, labels: &str) {
    for s in 0..3u128 {
        c.register(1, 100 + s, &format!(r#"{{"job":"sel","s":"{s}"}}"#))
            .await;
    }
    for (n, range) in [(1u8, "number < 60000"), (2u8, "number >= 60000")] {
        exec(
            c.shard(n),
            &format!(
                "INSERT INTO metric_exemplars (org_id, fingerprint, unix_milli, value, \
                 trace_id, span_id, labels) \
                 SELECT '', toUInt128(100 + number % 3), {t0} + number, toFloat64(number), \
                        toFixedString('', 16), toFixedString('', 8), '{labels}' \
                 FROM numbers(90000) WHERE {range}"
            ),
        )
        .await;
    }
    c.sync().await;
}

/// K6: 90,000 distinct exemplars, 60,000 on shard 1 and 30,000 on shard
/// 2, are answered whole through either shard: under the cap, not thinned.
#[tokio::test(flavor = "multi_thread")]
async fn under_the_cap_split_unevenly_is_answered_whole() {
    skip_unless_live!();
    let c = Cluster::new(pulsus_testkit::test_db("pulsus_exemplars_k6")).await;
    let t0 = c.t0;
    split_90k(&c, t0, "{}").await;
    let want: BTreeSet<(String, i64, u64)> = (0..90_000i64)
        .map(|i| ((i % 3).to_string(), t0 + i, (i as f64).to_bits()))
        .collect();
    for n in [1, 2] {
        let got = c
            .engine(n)
            .await
            .exemplars(&no_tenant(), &[selected()], c.window())
            .await
            .unwrap_or_else(|e| panic!("K6 through shard {n}: {e}"));
        let kept = kept_by_s(&got);
        assert_eq!(kept.len(), 90_000, "K6 through shard {n}");
        assert!(kept == want, "K6 through shard {n}: the answer differs");
    }
    c.finish().await;
}

/// K7: K3's split, 75,000 distinct on each shard and so under each node's
/// row cap, merges to more than 100,000 — the answer is T's 15,000 through
/// either shard, never A's first 100,000.
#[tokio::test(flavor = "multi_thread")]
async fn a_merge_past_the_cap_is_thinned_not_cut() {
    skip_unless_live!();
    let c = Cluster::new(pulsus_testkit::test_db("pulsus_exemplars_k7")).await;
    let t0 = (c.t0 / 10) * 10;
    split_150k(&c, t0, "{}").await;
    for n in [1, 2] {
        let got = c
            .engine(n)
            .await
            .exemplars(&no_tenant(), &[selected()], window_150k(t0))
            .await
            .unwrap_or_else(|e| panic!("K7 through shard {n}: {e}"));
        let count: usize = got.iter().map(|s| s.exemplars.len()).sum();
        assert_ne!(
            count, 100_000,
            "K7 through shard {n}: A's rows cut at its limit"
        );
        assert_eq!(
            count, 15_000,
            "K7 through shard {n}: one exemplar per slice"
        );
        assert!(
            kept_by_s(&got) == thinned_150k(t0),
            "K7 through shard {n}: the kept exemplars differ"
        );
    }
    c.finish().await;
}

/// The labels K8 stores on every exemplar: `L` = 200.
const WIDE_LABELS: &str = concat!(
    "{\"k\":\"",
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "\"}"
);

/// K8: K3's split with 200 bytes of labels on every exemplar, at a 60 MiB
/// budget. A's provisional rows pass the budget long before its 100,001st
/// row, so a drain that refused there would refuse; it keeps counting and
/// the answer is T's 15,000 through either shard. 90,000 such exemplars,
/// 60,000 + 30,000, end A at or under the cap past the budget: the byte
/// refusal.
#[tokio::test(flavor = "multi_thread")]
async fn a_byte_overflow_before_the_sentinel_still_reaches_the_thinned_answer() {
    skip_unless_live!();
    assert_eq!(WIDE_LABELS.len(), 200);
    let budget = 60 * 1024 * 1024;
    let c = Cluster::new(pulsus_testkit::test_db("pulsus_exemplars_k8")).await;
    let t0 = (c.t0 / 10) * 10;
    split_150k(&c, t0, WIDE_LABELS).await;
    for n in [1, 2] {
        let got = c
            .engine(n)
            .await
            .with_exemplar_answer_bytes(budget)
            .exemplars(&no_tenant(), &[selected()], window_150k(t0))
            .await
            .unwrap_or_else(|e| panic!("K8 through shard {n}: {e}"));
        let kept = kept_by_s(&got);
        assert_eq!(kept.len(), 15_000, "K8 through shard {n}");
        assert!(
            kept == thinned_150k(t0),
            "K8 through shard {n}: the kept exemplars differ"
        );
    }
    c.finish().await;
    let c = Cluster::new(pulsus_testkit::test_db("pulsus_exemplars_k8b")).await;
    let t0 = c.t0;
    split_90k(&c, t0, WIDE_LABELS).await;
    for n in [1, 2] {
        let got = c
            .engine(n)
            .await
            .with_exemplar_answer_bytes(budget)
            .exemplars(&no_tenant(), &[selected()], c.window())
            .await;
        match got {
            Err(ReadError::QueryTooBroad(TooBroadReason::ExemplarAnswerBytes { budget_bytes })) => {
                assert_eq!(budget_bytes, budget as u64, "K8 through shard {n}")
            }
            other => panic!(
                "K8 through shard {n}: 90,000 past the budget must be refused, got {:?}",
                other.map(|a| a.iter().map(|s| s.exemplars.len()).sum::<usize>())
            ),
        }
    }
    c.finish().await;
}

/// K4: one exemplar of A stored on both shards, and A's label row on both
/// — A once, each exemplar once. Statement H on its own, with the
/// clustered settings, gives A's label row once.
#[tokio::test(flavor = "multi_thread")]
async fn rows_stored_on_both_shards_are_answered_once() {
    skip_unless_live!();
    let c = Cluster::new(pulsus_testkit::test_db("pulsus_exemplars_k4")).await;
    let t0 = c.t0;
    c.register(1, 1, r#"{"job":"sel","s":"a"}"#).await;
    c.register(2, 1, r#"{"job":"sel","s":"a"}"#).await;
    c.exemplars(1, &[(1, t0, 1.5), (1, t0 + 1_000, 2.5)]).await;
    c.exemplars(2, &[(1, t0, 1.5)]).await;
    let want = vec![ExemplarSeries {
        series_labels: pairs(&[("__name__", "m"), ("job", "sel"), ("s", "a")]),
        exemplars: vec![ex(1.5, t0), ex(2.5, t0 + 1_000)],
    }];
    let (one, two) = c.answers().await;
    assert_eq!(one, want, "K4 through shard 1");
    assert_eq!(two, want, "K4 through shard 2");

    // Statement H as the design writes it, for A's ID.
    let h = "SELECT fingerprint, any(metric_name) AS series_name, any(labels) AS series_labels\n\
             FROM metric_labels_dist\n\
             WHERE org_id = ''\n\
             \x20 AND metric_name GLOBAL IN (\n\
             \x20   SELECT DISTINCT metric_name\n\
             \x20   FROM metric_series_dist\n\
             \x20   WHERE org_id = ''\n\
             \x20     AND fingerprint IN (SELECT toUInt128(arrayJoin(splitByChar(',', '1'))))\n\
             \x20 )\n\
             \x20 AND fingerprint IN (SELECT toUInt128(arrayJoin(splitByChar(',', '1'))))\n\
             GROUP BY fingerprint";
    let settings = QuerySettings::new()
        .set("distributed_product_mode", "local")
        .set("optimize_skip_unused_shards", 0u64)
        .set("optimize_distributed_group_by_sharding_key", 0u64);
    for n in [1, 2] {
        let rows = c
            .shard(n)
            .query_strings(
                &format!("SELECT toString(count()) AS s FROM ({h})"),
                &settings,
            )
            .await
            .expect("H runs");
        assert_eq!(rows, vec!["1".to_string()], "K4: H through shard {n}");
    }
    c.finish().await;
}

/// K5: 60,000 distinct exemplars, each stored once on each shard — 120,000
/// stored rows — are answered whole.
#[tokio::test(flavor = "multi_thread")]
async fn distinct_exemplars_stored_on_both_shards_are_answered_whole() {
    skip_unless_live!();
    let c = Cluster::new(pulsus_testkit::test_db("pulsus_exemplars_k5")).await;
    let t0 = c.t0;
    for s in 0..3u128 {
        c.register(1, 100 + s, &format!(r#"{{"job":"sel","s":"{s}"}}"#))
            .await;
    }
    for n in [1u8, 2] {
        exec(
            c.shard(n),
            &format!(
                "INSERT INTO metric_exemplars (org_id, fingerprint, unix_milli, value, \
                 trace_id, span_id, labels) \
                 SELECT '', toUInt128(100 + number % 3), {t0} + number, toFloat64(number), \
                        toFixedString('', 16), toFixedString('', 8), '{{}}' \
                 FROM numbers(60000)"
            ),
        )
        .await;
    }
    c.sync().await;
    for n in [1, 2] {
        let got = c
            .engine(n)
            .await
            .exemplars(&no_tenant(), &[selected()], c.window())
            .await
            .unwrap_or_else(|e| panic!("K5 through shard {n}: {e}"));
        let kept: Vec<(i64, f64)> = got
            .iter()
            .flat_map(|s| s.exemplars.iter().map(|e| (e.unix_milli, e.value)))
            .collect();
        assert_eq!(kept.len(), 60_000, "K5 through shard {n}");
        let times: BTreeSet<i64> = kept.iter().map(|(t, _)| *t).collect();
        assert_eq!(times.len(), 60_000, "K5 through shard {n}: each once");
        assert!(
            kept.iter().all(|(t, v)| *v == (t - t0) as f64),
            "K5 through shard {n}: values as stored"
        );
    }
    c.finish().await;
}
