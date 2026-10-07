//! Issue #623: the metrics storage shape, read back through the engine.
//!
//! Every row here is seeded through `metric_landing`, so the views fill the
//! target tables exactly as a push does. The expected answers are written
//! from the seed itself, not read back from a query, so a change in what the
//! label sweep, the discovery reads or the sample fetch return fails here.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test live_metrics_storage
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_model::{
    ACTIVITY_BUCKET_MS, CounterResetHint, Fingerprint, LabelSet, NativeHistogram, Span,
};
use pulsus_promql::parser::parse;
use pulsus_read::metrics::LabelledResolution;
use pulsus_read::{
    DataWindow, DiscoveryFilter, LabelCache, LabelCacheConfig, LabelMatcher, MatchOp,
    MetricQueryParams, MetricsConfig, MetricsEngine, QueryResult,
};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;
use pulsus_write::{HistogramPoint, MetricLandingRow, MetricPoint, SeriesRef};

/// One series as the seed states it: its name, its fingerprint and its
/// label pairs, sorted.
type SeriesEntry = (String, Fingerprint, Vec<(String, String)>);

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-read/tests/live_metrics_storage.rs)"
            );
            return;
        }
    };
}

fn test_config(database: &str) -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: database.to_string(),
        proto: ChProto::Http,
        pool_size: 8,
        query_timeout: Duration::from_secs(30),
        ..ChConnConfig::default()
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

/// A kind-2 landing row, built by the writer's own constructor.
fn series_row(
    received_ms: i64,
    name: &str,
    fp: u128,
    bucket: i64,
    pairs: &[(String, String)],
    value_type: u8,
) -> MetricLandingRow {
    let (labels, _) = LabelSet::from_normalized(pairs.iter().cloned());
    MetricLandingRow::series(
        received_ms,
        &SeriesRef {
            metric_name: Arc::from(name),
            fingerprint: Fingerprint::from_raw(fp),
            labels,
        },
        bucket,
        value_type,
    )
}

/// A kind-0 landing row.
fn float_row(
    received_ms: i64,
    name: &str,
    fp: u128,
    unix_milli: i64,
    value: f64,
) -> MetricLandingRow {
    MetricLandingRow::float_sample(
        received_ms,
        &MetricPoint {
            metric_name: Arc::from(name),
            fingerprint: Fingerprint::from_raw(fp),
            unix_milli,
            value,
        },
    )
}

/// A kind-1 landing row: a three-bucket histogram of count 4 and sum 5.
fn hist_row(received_ms: i64, name: &str, fp: u128, unix_milli: i64) -> MetricLandingRow {
    MetricLandingRow::hist_sample(
        received_ms,
        &HistogramPoint {
            metric_name: Arc::from(name),
            fingerprint: Fingerprint::from_raw(fp),
            unix_milli,
            histogram: NativeHistogram {
                counter_reset_hint: CounterResetHint::Unknown,
                schema: 0,
                zero_threshold: 0.0,
                zero_count: 0,
                count: 4,
                sum: 5.0,
                positive_spans: vec![Span {
                    offset: 0,
                    length: 3,
                }],
                negative_spans: vec![],
                positive_buckets: vec![1, 1, -1],
                negative_buckets: vec![],
                custom_values: vec![],
            },
        },
    )
}

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("now fits in i64")
}

fn cache_config(db: &str) -> LabelCacheConfig {
    LabelCacheConfig {
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        db: db.to_string(),
        series_table: "metric_series".to_string(),
        labels_table: "metric_labels".to_string(),
        window_ms: 24 * 3_600_000,
        cache_max_series: 50_000,
        ttl: Duration::from_secs(60),
        staleness_multiplier: 3,
    }
}

fn engine_config(db: &str) -> MetricsConfig {
    MetricsConfig {
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        db: db.to_string(),
        samples_table: "metric_samples".to_string(),
        hist_samples_table: "metric_hist_samples".to_string(),
        series_table: "metric_series".to_string(),
        labels_table: "metric_labels".to_string(),
        label_index_table: "metric_label_index".to_string(),
        label_values_table: "metric_label_values".to_string(),
        metadata_table: "metric_metadata".to_string(),
        experimental_functions: false,
        max_metric_fanout: 1_000,
        max_cache_scan: 200_000,
        max_info_series: 100_000,
        max_samples: 50_000_000,
        distributed: false,
        grouped_push: true,
    }
}

/// A fresh database, and clients bound to it for seeding, the cache and the
/// engine.
async fn fresh(db: String) -> (ChClient, String, ChClient) {
    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    drop_database(&bootstrap, &db).await;
    run_init(&bootstrap, &RenderCtx::for_tests(&db))
        .await
        .expect("run_init");
    let client = ChClient::new(test_config(&db))
        .await
        .expect("connect (target)");
    (bootstrap, db, client)
}

/// The label sets of the corpus: five sets over two jobs, keyed by a
/// number; every metric name that carries a set has its own series ID for
/// it.
fn label_sets() -> Vec<(u128, Vec<(String, String)>)> {
    (0u128..5)
        .map(|i| {
            (
                1_000 + i,
                vec![
                    ("instance".to_string(), format!("host-{i}:9100")),
                    ("job".to_string(), format!("job-{}", i % 2)),
                ],
            )
        })
        .collect()
}

/// Twelve metric names, each carrying three of the five label sets, so
/// every label set is shared by several names.
fn corpus() -> BTreeMap<String, Vec<u128>> {
    let sets = label_sets();
    (0usize..12)
        .map(|n| {
            let fps = (0..3).map(|k| sets[(n + k * 2) % 5].0).collect();
            (format!("metric_{n:02}"), fps)
        })
        .collect()
}

/// **The label sweep, the discovery reads and the fallback fetch return
/// exactly the seeded series, each with its own labels.** Every series is
/// registered in two activity hours and every label set is carried by
/// several names, each under its own series ID.
#[tokio::test]
async fn every_series_read_returns_the_seeded_series_with_their_labels() {
    skip_unless_live!();
    let (bootstrap, db, client) = fresh(pulsus_testkit::test_db(
        "pulsus_read_it_metrics_storage_labels",
    ))
    .await;

    let now = now_ms();
    let bucket = ACTIVITY_BUCKET_MS;
    let hour = (now / bucket) * bucket;
    let sets: BTreeMap<u128, Vec<(String, String)>> = label_sets().into_iter().collect();
    let corpus = corpus();

    let mut rows = Vec::new();
    for (name, fps) in &corpus {
        for fp in fps {
            let id = raw(sid(name, &sets[fp]));
            for activity in [hour - bucket, hour] {
                rows.push(series_row(now, name, id, activity, &sets[fp], 0));
            }
            rows.push(float_row(now, name, id, now - 10_000, *fp as f64));
        }
    }
    client
        .insert_block("metric_landing", &rows)
        .await
        .expect("seed metric_landing");

    let expected: BTreeSet<SeriesEntry> = corpus
        .iter()
        .flat_map(|(name, fps)| {
            fps.iter()
                .map(|fp| (name.clone(), sid(name, &sets[fp]), sets[fp].clone()))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(expected.len(), 36, "twelve names, three series each");

    // The label sweep: every name resolves to exactly its series.
    let cache = Arc::new(LabelCache::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache_config(&db),
    ));
    cache.refresh().await.expect("refresh");
    let window = DataWindow {
        start_ms: now - 3_600_000,
        end_ms: now,
    };
    let mut swept = BTreeSet::new();
    for name in corpus.keys() {
        match cache.resolve_labelled(name, &[], window) {
            LabelledResolution::Series(series) => {
                for (fp, labels) in series {
                    swept.insert((name.clone(), fp, pairs(&labels)));
                }
            }
            other => panic!("{name}: the warm cache must answer, got {other:?}"),
        }
    }
    assert_eq!(swept, expected, "the label sweep");

    // Discovery: every series, then one job's, then one name's.
    let engine = MetricsEngine::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache.clone(),
        engine_config(&db),
    );
    let discovered = |filter: DiscoveryFilter| {
        let engine = &engine;
        async move {
            engine
                .series(&[filter], window)
                .await
                .expect("discovery")
                .into_iter()
                .collect::<BTreeSet<Vec<(String, String)>>>()
        }
    };
    let as_series = |keep: &dyn Fn(&SeriesEntry) -> bool| {
        expected
            .iter()
            .filter(|e| keep(e))
            .map(|(name, _, labels)| {
                let mut out = labels.clone();
                out.push(("__name__".to_string(), name.clone()));
                out.sort();
                out
            })
            .collect::<BTreeSet<_>>()
    };
    let job_0 = LabelMatcher {
        key: "job".to_string(),
        op: MatchOp::Eq,
        value: "job-0".to_string(),
    };
    assert_eq!(
        discovered(DiscoveryFilter {
            metric_name: None,
            name_matchers: Vec::new(),
            matchers: vec![LabelMatcher {
                key: "job".to_string(),
                op: MatchOp::Re,
                value: "job-.*".to_string(),
            }],
        })
        .await,
        as_series(&|_| true),
        "discovery over every series"
    );
    assert_eq!(
        discovered(DiscoveryFilter {
            metric_name: None,
            name_matchers: Vec::new(),
            matchers: vec![job_0.clone()],
        })
        .await,
        as_series(&|(_, _, labels)| labels.contains(&("job".to_string(), "job-0".to_string()))),
        "discovery by a label matcher"
    );
    assert_eq!(
        discovered(DiscoveryFilter {
            metric_name: Some("metric_03".to_string()),
            name_matchers: Vec::new(),
            matchers: Vec::new(),
        })
        .await,
        as_series(&|(name, _, _)| name == "metric_03"),
        "discovery by a metric name"
    );
    assert_eq!(
        discovered(DiscoveryFilter {
            metric_name: None,
            name_matchers: vec![LabelMatcher {
                key: "__name__".to_string(),
                op: MatchOp::Re,
                value: "metric_0[0-4]".to_string(),
            }],
            matchers: vec![job_0.clone()],
        })
        .await,
        as_series(&|(name, _, labels)| {
            name.as_str() <= "metric_04"
                && labels.contains(&("job".to_string(), "job-0".to_string()))
        }),
        "discovery by a name regex and a label matcher"
    );

    // The fallback fetch: a cache that never swept degrades to the series
    // sub-query, and the fetched series carry the seeded labels.
    let cold = Arc::new(LabelCache::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache_config(&db),
    ));
    let cold_engine = MetricsEngine::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cold,
        engine_config(&db),
    );
    let params = MetricQueryParams {
        start_ms: now,
        end_ms: now,
        step_ms: 0,
    };
    let (result, _) = cold_engine
        .query(&parse(r#"metric_03{job="job-0"}"#).expect("parse"), &params)
        .await
        .expect("fallback query");
    let got: BTreeSet<Vec<(String, String)>> = match result {
        QueryResult::Vector(v) => v.into_iter().map(|s| s.labels).collect(),
        other => panic!("expected Vector, got {other:?}"),
    };
    let want: BTreeSet<Vec<(String, String)>> = expected
        .iter()
        .filter(|(name, _, labels)| {
            name == "metric_03" && labels.contains(&("job".to_string(), "job-0".to_string()))
        })
        .map(|(name, _, labels)| {
            let mut out = labels.clone();
            out.push(("__name__".to_string(), name.clone()));
            out.sort();
            out
        })
        .collect();
    assert!(!want.is_empty(), "the fixture has a series to fall back to");
    let got_sorted: BTreeSet<Vec<(String, String)>> = got
        .into_iter()
        .map(|mut l| {
            l.sort();
            l
        })
        .collect();
    assert_eq!(got_sorted, want, "the fallback fetch");

    // An activity row whose label row never landed — the one outcome the
    // two views add, when one commits and the other does not — is absent
    // from every read, never returned with empty labels: an empty label
    // set would make distinct series one.
    client
        .execute(
            &format!(
                "INSERT INTO {db}.metric_series (day, fingerprint, metric_name, hours) \
                 SELECT toDate(fromUnixTimestamp64Milli(toInt64({hour})), 'UTC'), 9999, 'orphan', \
                        toUInt32(bitShiftLeft(toUInt32(1), \
                          toHour(fromUnixTimestamp64Milli(toInt64({hour})), 'UTC')))"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed an activity row with no label row");
    cache.refresh().await.expect("refresh");
    match cache.resolve_labelled("orphan", &[], window) {
        LabelledResolution::Series(series) => {
            assert!(series.is_empty(), "the sweep returned {series:?}")
        }
        other => panic!("the warm cache must answer, got {other:?}"),
    }
    assert!(
        discovered(DiscoveryFilter {
            metric_name: Some("orphan".to_string()),
            name_matchers: Vec::new(),
            matchers: Vec::new(),
        })
        .await
        .is_empty(),
        "discovery returned a series with no label set"
    );

    // Stored once per series (issue #623): one label row per (metric_name,
    // fingerprint), not one per hour.
    assert_eq!(
        count(
            &client,
            &format!(
                "SELECT count() AS n FROM system.tables \
                 WHERE database = '{db}' AND name = 'metric_labels'"
            ),
        )
        .await,
        1,
        "the label table exists"
    );
    client
        .execute(
            &format!("OPTIMIZE TABLE {db}.metric_labels FINAL"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("merge metric_labels");
    assert_eq!(
        count(
            &client,
            &format!("SELECT count() AS n FROM {db}.metric_labels")
        )
        .await,
        36,
        "one label row per series: every (metric_name, fingerprint) seeded"
    );

    drop_database(&bootstrap, &db).await;
}

/// One fetch statement each for the float and histogram reads of every
/// selector: the explain stage names, in order.
fn fetch_stages(explain: &pulsus_read::PlanExplain) -> Vec<&'static str> {
    explain
        .stages
        .iter()
        .filter(|s| s.name.contains("sample_fetch"))
        .map(|s| s.name)
        .collect()
}

/// (job, "hist" | "float") per answered series.
fn kinds_by_job(result: QueryResult) -> Vec<(String, &'static str)> {
    let job_of = |labels: &[(String, String)]| {
        labels
            .iter()
            .find(|(k, _)| k == "job")
            .map(|(_, v)| v.clone())
            .expect("a job label")
    };
    let mut out: Vec<(String, &'static str)> = match result {
        QueryResult::VectorHist(v) => v
            .into_iter()
            .map(|s| {
                let job = job_of(&s.labels);
                let kind = match s.value {
                    pulsus_read::logql::HistOrFloat::Hist(h) => {
                        assert_eq!((h.count, h.sum), (4.0, 5.0), "{job}");
                        "hist"
                    }
                    pulsus_read::logql::HistOrFloat::Float(f) => {
                        assert_eq!(f, 42.0, "{job}");
                        "float"
                    }
                };
                (job, kind)
            })
            .collect(),
        QueryResult::Vector(v) => v
            .into_iter()
            .map(|s| {
                assert_eq!(s.value, 42.0);
                (job_of(&s.labels), "float")
            })
            .collect(),
        other => panic!("expected a vector, got {other:?}"),
    };
    out.sort();
    out
}

/// **T1 and T3 (issue #623): every fetch path sends `main`'s float read and
/// histogram read, and the answer is `main`'s.** Float-only, histogram-only
/// and mixed series, the mixed one holding a float and a histogram sample at
/// one millisecond; the histogram wins the shared key. Read through the
/// cache's chunks, the fallback (a cold cache) and the multi-metric fan-out.
#[tokio::test]
async fn every_fetch_path_sends_both_reads_and_the_answer_is_unchanged() {
    skip_unless_live!();
    let (bootstrap, db, client) = fresh(pulsus_testkit::test_db(
        "pulsus_read_it_metrics_storage_union",
    ))
    .await;

    let now = now_ms();
    let bucket = ACTIVITY_BUCKET_MS;
    let hour = (now / bucket) * bucket;
    let t = now - 10_000;
    let mut rows = Vec::new();
    for (fp, job) in [(1u128, "both"), (2, "float"), (3, "hist")] {
        let labels = [("job".to_string(), job.to_string())];
        for value_type in [0u8, 1] {
            rows.push(series_row(now, "m", fp, hour, &labels, value_type));
        }
        if job != "hist" {
            rows.push(float_row(now, "m", fp, t, 42.0));
        }
        if job != "float" {
            rows.push(hist_row(now, "m", fp, t));
        }
    }
    client
        .insert_block("metric_landing", &rows)
        .await
        .expect("seed metric_landing");

    let expected = vec![
        ("both".to_string(), "hist"),
        ("float".to_string(), "float"),
        ("hist".to_string(), "hist"),
    ];
    let params = MetricQueryParams {
        start_ms: now,
        end_ms: now,
        step_ms: 0,
    };
    let warm = Arc::new(LabelCache::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache_config(&db),
    ));
    warm.refresh().await.expect("refresh");
    let cold = Arc::new(LabelCache::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache_config(&db),
    ));
    for (path, cache, query) in [
        ("chunks", warm.clone(), "m"),
        ("fallback", cold, "m"),
        ("multi", warm, r#"{__name__=~"m|n"}"#),
    ] {
        let engine = MetricsEngine::new(
            ChClient::new(test_config(&db)).await.expect("connect"),
            cache,
            engine_config(&db),
        );
        let (result, _, explain) = engine
            .query_explained(&parse(query).expect("parse"), &params)
            .await
            .unwrap_or_else(|e| panic!("{path}: {e}"));
        assert_eq!(
            fetch_stages(&explain),
            vec!["sample_fetch", "hist_sample_fetch"],
            "{path}: one float and one histogram statement: {:#?}",
            explain.stages
        );
        assert_eq!(
            kinds_by_job(result),
            expected,
            "{path}: the histogram wins the key both tables hold"
        );
    }

    drop_database(&bootstrap, &db).await;
}

/// **T4 (issue #623): a late sample of the other kind is read.** A
/// float-only series and a histogram-only series, the cache refreshed;
/// then a histogram sample at an older timestamp for the first and a float
/// sample for the second. Both are returned on the cache path, before the
/// next refresh, for a window ending before the refresh — a read that
/// skipped a statement by the cache's kinds would leave them out.
#[tokio::test]
async fn a_late_sample_of_the_other_kind_is_read() {
    skip_unless_live!();
    let (bootstrap, db, client) = fresh(pulsus_testkit::test_db(
        "pulsus_read_it_metrics_storage_late",
    ))
    .await;

    let now = now_ms();
    let bucket = ACTIVITY_BUCKET_MS;
    let hour = (now / bucket) * bucket;
    let early = now - 120_000;
    let late = now - 60_000;
    let first = [("job".to_string(), "float".to_string())];
    let second = [("job".to_string(), "hist".to_string())];
    client
        .insert_block(
            "metric_landing",
            &[
                series_row(now, "late", 1, hour, &first, 0),
                float_row(now, "late", 1, early, 42.0),
                series_row(now, "late", 2, hour, &second, 1),
                hist_row(now, "late", 2, early),
            ],
        )
        .await
        .expect("seed the first kinds");

    let cache = Arc::new(LabelCache::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache_config(&db),
    ));
    cache.refresh().await.expect("refresh");

    client
        .insert_block(
            "metric_landing",
            &[
                series_row(now + 1, "late", 1, hour, &first, 1),
                hist_row(now + 1, "late", 1, late),
                series_row(now + 1, "late", 2, hour, &second, 0),
                float_row(now + 1, "late", 2, late, 42.0),
            ],
        )
        .await
        .expect("seed the late samples of the other kind");

    let engine = MetricsEngine::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache,
        engine_config(&db),
    );
    // A window ending before the refresh: the late samples' own instant.
    let params = MetricQueryParams {
        start_ms: late,
        end_ms: late,
        step_ms: 0,
    };
    let (result, _) = engine
        .query(&parse("late").expect("parse"), &params)
        .await
        .expect("query");
    assert_eq!(
        kinds_by_job(result),
        vec![("float".to_string(), "hist"), ("hist".to_string(), "float"),],
        "each series answers with the late sample of the kind it did not have"
    );

    drop_database(&bootstrap, &db).await;
}

/// `(metric_name, fingerprint, labels)`: one answered series.
type Triple = (String, u128, Vec<(String, String)>);

/// One seeded series of the L1 fixture.
#[derive(Clone)]
struct Rec {
    name: &'static str,
    labels: Vec<(String, String)>,
    /// The one hour the series is active in.
    hour: i64,
}

impl Rec {
    fn id(&self) -> u128 {
        raw(sid(self.name, &self.labels))
    }

    fn triple(&self) -> Triple {
        (self.name.to_string(), self.id(), self.labels.clone())
    }
}

/// An L1 matcher, applied to a seed record the way the SQL applies it: an
/// absent label is `""`.
#[derive(Clone)]
struct M {
    key: &'static str,
    op: MatchOp,
    value: &'static str,
}

impl M {
    fn holds_value(&self, v: &str) -> bool {
        let re = || {
            regex::Regex::new(&format!("^(?:{})$", self.value))
                .expect("a test pattern")
                .is_match(v)
        };
        match self.op {
            MatchOp::Eq => v == self.value,
            MatchOp::Neq => v != self.value,
            MatchOp::Re => re(),
            MatchOp::Nre => !re(),
        }
    }

    fn holds(&self, name: &str, labels: &[(String, String)]) -> bool {
        if self.key == "__name__" {
            return self.holds_value(name);
        }
        let v = labels
            .iter()
            .find(|(k, _)| k == self.key)
            .map(|(_, v)| v.as_str())
            .unwrap_or("");
        self.holds_value(v)
    }

    fn matcher(&self) -> LabelMatcher {
        LabelMatcher {
            key: self.key.to_string(),
            op: self.op,
            value: self.value.to_string(),
        }
    }
}

/// The series ID of `name` with `pairs`, through the writers' own function.
fn sid(name: &str, pairs: &[(String, String)]) -> Fingerprint {
    let (labels, _) = LabelSet::from_normalized(pairs.iter().cloned());
    pulsus_model::series_fingerprint(name, &labels)
}

/// A fingerprint's raw value, read from its `Debug` form (the type renders
/// no other way outside its sealed SQL literal).
fn raw(fp: Fingerprint) -> u128 {
    format!("{fp:?}")
        .strip_prefix("Fingerprint(")
        .and_then(|s| s.strip_suffix(')'))
        .and_then(|s| s.parse().ok())
        .expect("Fingerprint(<decimal>)")
}

fn label_pairs(json: &str) -> Vec<(String, String)> {
    let map: BTreeMap<String, String> = serde_json::from_str(json).expect("canonical JSON labels");
    map.into_iter().collect()
}

async fn rows_of<R: pulsus_clickhouse::ChRow>(client: &ChClient, sql: &str) -> Vec<R> {
    use futures::StreamExt;
    let sql = sql.replace('?', "??");
    let mut stream = client
        .query_stream::<R>(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.unwrap_or_else(|e| panic!("{e}\n{sql}")));
    }
    out
}

/// The answered series of a statement-2 or statement-4 read, each once.
async fn triples_of(client: &ChClient, sql: &str, what: &str) -> BTreeSet<Triple> {
    let rows: Vec<pulsus_read::metrics::rows::SeriesRow> = rows_of(client, sql).await;
    let all: Vec<Triple> = rows
        .into_iter()
        .map(|r| (r.metric_name, raw(r.fingerprint), label_pairs(&r.labels)))
        .collect();
    let set: BTreeSet<Triple> = all.iter().cloned().collect();
    assert_eq!(set.len(), all.len(), "{what}: a series repeated: {all:?}");
    set
}

fn sorted(pairs: &[(&str, String)]) -> Vec<(String, String)> {
    let mut v: Vec<(String, String)> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    v.sort();
    v
}

/// **L1 (issue #623): every matcher answers from the lookup.** 24 series
/// of `m_q`, `job` cycling over three values and `status` over four,
/// `status` absent on every eighth, each active in one of six hours; `m_r`
/// with `m_q`'s label sets; 2,000 `m_bg` series that match the label
/// matchers; ten kind-2 rows sent twice. Six matcher sets, two windows,
/// through statements 1 to 4: each answer is the generator's exact set,
/// each series once.
#[tokio::test]
async fn matchers_answer_from_the_lookup() {
    skip_unless_live!();
    let (bootstrap, db, client) = fresh(pulsus_testkit::test_db(
        "pulsus_read_it_metrics_storage_lookup",
    ))
    .await;

    let now = now_ms();
    let h = ACTIVITY_BUCKET_MS;
    let b = (now / h) * h - 8 * h;
    let q_labels = |i: usize| -> Vec<(String, String)> {
        let mut p = vec![
            ("instance", format!("q{i}")),
            ("job", ["api", "web", "db"][i % 3].to_string()),
        ];
        if i % 8 != 7 {
            p.push(("status", ["200", "404", "500", "503"][i % 4].to_string()));
        }
        sorted(&p)
    };
    let mut recs: Vec<Rec> = Vec::new();
    for name in ["m_q", "m_r"] {
        for i in 0..24usize {
            recs.push(Rec {
                name,
                labels: q_labels(i),
                hour: b + (i as i64 % 6) * h,
            });
        }
    }
    for n in 0..2_000usize {
        recs.push(Rec {
            name: "m_bg",
            labels: sorted(&[
                ("instance", format!("bg{n}")),
                ("job", "api".to_string()),
                ("status", "500".to_string()),
            ]),
            hour: b + h,
        });
    }
    let landing: Vec<MetricLandingRow> = recs
        .iter()
        .map(|r| series_row(now, r.name, r.id(), r.hour, &r.labels, 0))
        .collect();
    client
        .insert_block("metric_landing", &landing)
        .await
        .expect("seed metric_landing");
    let repeats: Vec<MetricLandingRow> = recs
        .iter()
        .take(10)
        .map(|r| series_row(now + 1, r.name, r.id(), r.hour, &r.labels, 0))
        .collect();
    client
        .insert_block("metric_landing", &repeats)
        .await
        .expect("seed the repeated kind-2 rows");

    let windows = [
        (
            "1 h",
            DataWindow {
                start_ms: b + 2 * h + 15 * 60_000,
                end_ms: b + 3 * h + 15 * 60_000,
            },
        ),
        (
            "6 h",
            DataWindow {
                start_ms: b + 30 * 60_000,
                end_ms: b + 6 * h + 30 * 60_000,
            },
        ),
    ];
    let label = |key, op, value| M { key, op, value };
    // (name matchers, label matchers)
    let sets: Vec<(Vec<M>, Vec<M>)> = vec![
        (vec![], vec![label("job", MatchOp::Eq, "api")]),
        (vec![], vec![label("status", MatchOp::Re, "5..")]),
        (vec![], vec![label("status", MatchOp::Neq, "500")]),
        (vec![], vec![label("status", MatchOp::Nre, "5..")]),
        (
            vec![label("__name__", MatchOp::Re, "m_q|m_r")],
            vec![label("job", MatchOp::Eq, "api")],
        ),
        (
            vec![label("__name__", MatchOp::Neq, "m_q")],
            vec![label("job", MatchOp::Eq, "api")],
        ),
    ];
    use pulsus_read::metrics::sql;
    for (wname, window) in windows {
        let first = window.start_ms.div_euclid(h) * h;
        let last = window.end_ms.div_euclid(h) * h;
        let active = |r: &Rec| (first..=last).contains(&r.hour);
        for (name_ms, label_ms) in &sets {
            let what = format!(
                "{wname} {:?} {:?}",
                name_ms.iter().map(M::matcher).collect::<Vec<_>>(),
                label_ms.iter().map(M::matcher).collect::<Vec<_>>()
            );
            let want = |only: Option<&str>| -> BTreeSet<Triple> {
                recs.iter()
                    .filter(|r| only.is_none_or(|n| n == r.name))
                    .filter(|r| active(r))
                    .filter(|r| name_ms.iter().all(|m| m.holds(r.name, &r.labels)))
                    .filter(|r| label_ms.iter().all(|m| m.holds(r.name, &r.labels)))
                    .map(Rec::triple)
                    .collect()
            };
            let matchers: Vec<LabelMatcher> = label_ms.iter().map(M::matcher).collect();
            let filter = DiscoveryFilter {
                metric_name: None,
                name_matchers: name_ms.iter().map(M::matcher).collect(),
                matchers: matchers.clone(),
            };
            let all = want(None);
            assert!(!all.is_empty(), "{what}: the fixture has an answer");

            // Statement 2, unnamed.
            assert_eq!(
                triples_of(
                    &client,
                    &sql::discovery_query("metric_series", "metric_labels", &filter, window),
                    &what,
                )
                .await,
                all,
                "statement 2 {what}"
            );
            // Statement 3.
            let names: BTreeSet<String> = rows_of::<pulsus_read::metrics::rows::MetricNameRow>(
                &client,
                &sql::discovery_distinct_names_query(
                    "metric_series",
                    "metric_labels",
                    &filter,
                    window,
                ),
            )
            .await
            .into_iter()
            .map(|r| r.metric_name)
            .collect();
            assert_eq!(
                names,
                all.iter().map(|(n, _, _)| n.clone()).collect(),
                "statement 3 {what}"
            );
            // Statement 4, over the answer's own IDs.
            let answer_names: Vec<String> = names.iter().cloned().collect();
            let ids: Vec<pulsus_model::FpLiteral> = all
                .iter()
                .map(|(_, id, _)| Fingerprint::from_raw(*id).sql_literal())
                .collect();
            assert_eq!(
                triples_of(
                    &client,
                    &sql::series_labels_by_fingerprint("metric_labels", &answer_names, &ids),
                    &what,
                )
                .await,
                all,
                "statement 4 {what}"
            );
            if !name_ms.is_empty() {
                continue;
            }
            // The named forms, for m_q: statement 1 and statement 2.
            let want_q = want(Some("m_q"));
            let got_ids: BTreeSet<u128> =
                rows_of::<pulsus_read::metrics::exec::FingerprintOnlyRow>(
                    &client,
                    &sql::historical_series_subquery(
                        "metric_series",
                        "metric_labels",
                        "m_q",
                        window,
                        &matchers,
                    ),
                )
                .await
                .into_iter()
                .map(|r| raw(r.fingerprint))
                .collect();
            assert_eq!(
                got_ids,
                want_q.iter().map(|(_, id, _)| *id).collect(),
                "statement 1 {what}"
            );
            assert_eq!(
                triples_of(
                    &client,
                    &sql::historical_resolution_query(
                        "metric_series",
                        "metric_labels",
                        "m_q",
                        window,
                        &matchers,
                    ),
                    &what,
                )
                .await,
                want_q,
                "statement 2, the resolution query {what}"
            );
            let two = ["m_q".to_string(), "m_r".to_string()];
            let mut want_two = want_q.clone();
            want_two.extend(want(Some("m_r")));
            assert_eq!(
                triples_of(
                    &client,
                    &sql::discovery_fetch_by_names(
                        "metric_series",
                        "metric_labels",
                        &two,
                        &matchers,
                        window,
                    ),
                    &what,
                )
                .await,
                want_two,
                "statement 2 by names {what}"
            );
        }
        // The fan-out over the cache's IDs: every m_q and m_r ID, the window
        // applied.
        let two = ["m_q".to_string(), "m_r".to_string()];
        let ids: Vec<pulsus_model::FpLiteral> = recs
            .iter()
            .filter(|r| r.name != "m_bg")
            .map(|r| Fingerprint::from_raw(r.id()).sql_literal())
            .collect();
        let want: BTreeSet<Triple> = recs
            .iter()
            .filter(|r| r.name != "m_bg" && active(r))
            .map(Rec::triple)
            .collect();
        assert_eq!(
            triples_of(
                &client,
                &sql::discovery_fetch_multi("metric_series", "metric_labels", &two, &ids, window),
                wname,
            )
            .await,
            want,
            "statement 2, the fan-out {wname}"
        );
    }

    drop_database(&bootstrap, &db).await;
}

/// **D1 (issue #623): an old series is not discovered in a short window.**
/// A warm cache holds every series active in its 24 hours. `m_a{job="old"}`
/// and `m_stale{job="old", stale_only="1"}` were active only 20 hours ago;
/// `m_a{job="api"}` and `m_b{job="api"}` are active now. Over a 10-minute
/// window, `/series`, `/labels`, `/label/job/values` and
/// `/label/__name__/values`, each with `match[]` `{__name__=~"m_.*"}`,
/// `m_a` and `{job="api"}`, return no old series, no `old` value, no
/// `stale_only` key and no `m_stale` name. The stale-only series has its
/// own label key and its own name, so each endpoint answers differently if
/// a read takes the cache's IDs without the request window.
#[tokio::test]
async fn an_old_series_is_not_discovered_in_a_short_window() {
    skip_unless_live!();
    let (bootstrap, db, client) = fresh(pulsus_testkit::test_db(
        "pulsus_read_it_metrics_storage_old_series",
    ))
    .await;

    let now = now_ms();
    let h = ACTIVITY_BUCKET_MS;
    let then = (now / h) * h - 20 * h;
    let current = (now / h) * h;
    let series = [
        ("m_a", sorted(&[("job", "old".to_string())]), then),
        (
            "m_stale",
            sorted(&[("job", "old".to_string()), ("stale_only", "1".to_string())]),
            then,
        ),
        ("m_a", sorted(&[("job", "api".to_string())]), current),
        ("m_b", sorted(&[("job", "api".to_string())]), current),
    ];
    let rows: Vec<MetricLandingRow> = series
        .iter()
        .map(|(name, labels, hour)| series_row(now, name, raw(sid(name, labels)), *hour, labels, 0))
        .collect();
    client
        .insert_block("metric_landing", &rows)
        .await
        .expect("seed metric_landing");

    let cache = Arc::new(LabelCache::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache_config(&db),
    ));
    cache.refresh().await.expect("refresh");
    let engine = MetricsEngine::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache,
        engine_config(&db),
    );
    let window = DataWindow {
        start_ms: now - 10 * 60_000,
        end_ms: now,
    };
    let job_api = LabelMatcher {
        key: "job".to_string(),
        op: MatchOp::Eq,
        value: "api".to_string(),
    };
    let filters = [
        (
            r#"{__name__=~"m_.*"}"#,
            DiscoveryFilter {
                metric_name: None,
                name_matchers: vec![LabelMatcher {
                    key: "__name__".to_string(),
                    op: MatchOp::Re,
                    value: "m_.*".to_string(),
                }],
                matchers: Vec::new(),
            },
        ),
        (
            "m_a",
            DiscoveryFilter {
                metric_name: Some("m_a".to_string()),
                name_matchers: Vec::new(),
                matchers: Vec::new(),
            },
        ),
        (
            r#"{job="api"}"#,
            DiscoveryFilter {
                metric_name: None,
                name_matchers: Vec::new(),
                matchers: vec![job_api],
            },
        ),
    ];
    let api = |name: &str| sorted(&[("__name__", name.to_string()), ("job", "api".to_string())]);
    for (what, filter) in filters {
        let one = std::slice::from_ref(&filter);
        let (want_series, want_names): (Vec<Vec<(String, String)>>, Vec<String>) = match what {
            "m_a" => (vec![api("m_a")], vec!["m_a".to_string()]),
            _ => (
                vec![api("m_a"), api("m_b")],
                vec!["m_a".to_string(), "m_b".to_string()],
            ),
        };
        assert_eq!(
            engine.series(one, window).await.expect("/series"),
            want_series,
            "/series {what}"
        );
        assert_eq!(
            engine.label_names(one, window).await.expect("/labels"),
            vec!["__name__".to_string(), "job".to_string()],
            "/labels {what}"
        );
        assert_eq!(
            engine
                .label_values("job", one, window)
                .await
                .expect("/label/job/values"),
            vec!["api".to_string()],
            "/label/job/values {what}"
        );
        assert_eq!(
            engine
                .label_values("__name__", one, window)
                .await
                .expect("/label/__name__/values"),
            want_names,
            "/label/__name__/values {what}"
        );
    }

    drop_database(&bootstrap, &db).await;
}

/// The series active in hours `[first, last]` (each an hour's start) by the
/// form of the activity table that held one row per series per hour:
/// `e4c1dc3b`'s bucket-floored bound.
fn hourly_ids(hourly: &BTreeMap<u128, BTreeSet<i64>>, first: i64, last: i64) -> BTreeSet<u128> {
    hourly
        .iter()
        .filter(|(_, hours)| hours.range(first..=last).next().is_some())
        .map(|(id, _)| *id)
        .collect()
}

/// **A2 (issue #623): the day activity is exact to the hour.** 1,000 series,
/// each active in a pseudo-random tenth of the last 168 hours. The same
/// activity is written beside it in the hourly form, one row per series per
/// hour, read with `e4c1dc3b`'s bucket-floored bound. Over 300 random
/// windows of up to six days, statement 1 answers the hourly form's set
/// exactly.
#[tokio::test]
async fn activity_is_exact_to_the_hour() {
    skip_unless_live!();
    let (bootstrap, db, client) = fresh(pulsus_testkit::test_db(
        "pulsus_read_it_metrics_storage_hour_exact",
    ))
    .await;

    let now = now_ms();
    let h = ACTIVITY_BUCKET_MS;
    let top = (now / h) * h;
    let base = top - 167 * h;
    // A fixed linear congruential sequence: the same fixture every run.
    let mut state: u64 = 0x0005_DEEC_E66D;
    let mut next = move || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        state >> 33
    };
    let mut hourly: BTreeMap<u128, BTreeSet<i64>> = BTreeMap::new();
    let mut rows = Vec::new();
    let mut hourly_values = Vec::new();
    for s in 0..1_000u32 {
        let labels = sorted(&[("instance", format!("i-{s}"))]);
        let id = raw(sid("m_hours", &labels));
        let hours = hourly.entry(id).or_default();
        for k in 0..168i64 {
            if next() % 10 == 0 {
                let hour = base + k * h;
                hours.insert(hour);
                rows.push(series_row(now, "m_hours", id, hour, &labels, 0));
                hourly_values.push(format!("({id}, {hour})"));
            }
        }
    }
    for chunk in rows.chunks(20_000) {
        client
            .insert_block("metric_landing", chunk)
            .await
            .expect("seed metric_landing");
    }
    client
        .execute(
            &format!(
                "CREATE TABLE {db}.activity_hourly (fingerprint UInt128, unix_milli Int64) \
                 ENGINE = MergeTree ORDER BY (fingerprint, unix_milli)"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("create the hourly form");
    for chunk in hourly_values.chunks(20_000) {
        client
            .execute(
                &format!(
                    "INSERT INTO {db}.activity_hourly (fingerprint, unix_milli) VALUES {}",
                    chunk.join(", ")
                ),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("seed the hourly form");
    }

    for w in 0..300 {
        let span = (next() % (6 * 24 * 3_600_000)) as i64;
        // From the hour before the first seeded hour to the last one, so the
        // window always ends at or after it starts.
        let start = base - h + (next() % (168 * 3_600_000 + 1)) as i64;
        let window = DataWindow {
            start_ms: start,
            end_ms: (start + span).min(top + h - 1),
        };
        let first = window.start_ms.div_euclid(h) * h;
        let last = window.end_ms.div_euclid(h) * h;
        let hourly_sql = format!(
            "SELECT DISTINCT fingerprint FROM {db}.activity_hourly \
             WHERE unix_milli >= {first} AND unix_milli <= {last}"
        );
        let from_hourly: BTreeSet<u128> =
            rows_of::<pulsus_read::metrics::exec::FingerprintOnlyRow>(&client, &hourly_sql)
                .await
                .into_iter()
                .map(|r| raw(r.fingerprint))
                .collect();
        assert_eq!(
            from_hourly,
            hourly_ids(&hourly, first, last),
            "window {w}: the hourly form agrees with the seed"
        );
        let from_days: BTreeSet<u128> = rows_of::<pulsus_read::metrics::exec::FingerprintOnlyRow>(
            &client,
            &pulsus_read::metrics::sql::historical_series_subquery(
                "metric_series",
                "metric_labels",
                "m_hours",
                window,
                &[],
            ),
        )
        .await
        .into_iter()
        .map(|r| raw(r.fingerprint))
        .collect();
        assert_eq!(from_days, from_hourly, "window {w} {window:?}");
    }

    drop_database(&bootstrap, &db).await;
}

/// **Read failure (issue #623): either of a selector's two reads failing
/// fails the selector with that read's error.** On each fetch path — the
/// cache's chunks, the fallback and the multi-metric fan-out — the float
/// read, then the histogram read, names a table that does not exist.
#[tokio::test]
async fn a_failed_read_fails_its_selector_with_that_reads_error() {
    skip_unless_live!();
    let (bootstrap, db, client) = fresh(pulsus_testkit::test_db(
        "pulsus_read_it_metrics_storage_read_fails",
    ))
    .await;

    let now = now_ms();
    let hour = (now / ACTIVITY_BUCKET_MS) * ACTIVITY_BUCKET_MS;
    let labels = [("job".to_string(), "a".to_string())];
    client
        .insert_block(
            "metric_landing",
            &[
                series_row(now, "m", 1, hour, &labels, 0),
                float_row(now, "m", 1, now - 10_000, 42.0),
            ],
        )
        .await
        .expect("seed metric_landing");
    let params = MetricQueryParams {
        start_ms: now,
        end_ms: now,
        step_ms: 0,
    };
    for (read, absent) in [
        ("float", "absent_float_samples"),
        ("histogram", "absent_hist_samples"),
    ] {
        let mut config = engine_config(&db);
        if read == "float" {
            config.samples_table = absent.to_string();
        } else {
            config.hist_samples_table = absent.to_string();
        }
        let warm = Arc::new(LabelCache::new(
            ChClient::new(test_config(&db)).await.expect("connect"),
            cache_config(&db),
        ));
        warm.refresh().await.expect("refresh");
        let cold = Arc::new(LabelCache::new(
            ChClient::new(test_config(&db)).await.expect("connect"),
            cache_config(&db),
        ));
        for (path, cache, query) in [
            ("chunks", warm.clone(), "m"),
            ("fallback", cold, "m"),
            ("multi", warm, r#"{__name__=~"m|n"}"#),
        ] {
            let engine = MetricsEngine::new(
                ChClient::new(test_config(&db)).await.expect("connect"),
                cache,
                config.clone(),
            );
            let err = engine
                .query(&parse(query).expect("parse"), &params)
                .await
                .err()
                .unwrap_or_else(|| panic!("{path}, {read} read failing: the selector answered"));
            let text = format!("{err} {err:?}");
            assert!(
                text.contains(absent),
                "{path}, {read} read failing: the error is not that read's: {text}"
            );
        }
    }

    drop_database(&bootstrap, &db).await;
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct InsertLogRow {
    kind: String,
    token: String,
}

/// **W2 (issue #623): a push whose lookup row a view cannot write fails,
/// and the resend writes it.** The pushes run as a user whose own settings
/// turn both view-error settings on — a view's error ignored, a view with a
/// dropped target skipped — so only the landing insert's own pins can make
/// them fail. Two shapes: (a) a constraint on `metric_labels` refusing the
/// series' lookup row; (b) `metric_labels` dropped. Each push fails and
/// writes no activity row; with the constraint removed, or the table
/// re-created, the resend under the same token writes one activity row and
/// one lookup row, and `system.query_log` shows both inserts carrying that
/// token.
#[tokio::test]
async fn a_failed_label_view_fails_the_push() {
    skip_unless_live!();
    let (bootstrap, db, client) = fresh(pulsus_testkit::test_db(
        "pulsus_read_it_metrics_storage_view_fails",
    ))
    .await;
    let user = pulsus_testkit::test_ident("pulsus_read_it_metrics_storage_w2_user");
    for sql in [
        format!("DROP USER IF EXISTS {user}"),
        format!(
            "CREATE USER {user} IDENTIFIED WITH no_password \
             SETTINGS materialized_views_ignore_errors = 1, \
             ignore_materialized_views_with_dropped_target_table = 1"
        ),
        format!("GRANT ALL ON {db}.* TO {user}"),
    ] {
        bootstrap
            .execute(&sql, &QuerySettings::new(), Idempotency::Idempotent)
            .await
            .unwrap_or_else(|e| panic!("{e}\n{sql}"));
    }
    let pusher = ChClient::new(ChConnConfig {
        user: user.clone(),
        password: String::new(),
        ..test_config(&db)
    })
    .await
    .expect("connect as the test user");

    let now = now_ms();
    let hour = (now / ACTIVITY_BUCKET_MS) * ACTIVITY_BUCKET_MS;
    let labels = [("job".to_string(), "w2".to_string())];
    let activity_count = |fp: u128| {
        format!(
            "SELECT count() AS n FROM {db}.metric_series \
             WHERE metric_name = 'w2' AND fingerprint = {fp}"
        )
    };
    let lookup_count = |fp: u128| {
        format!(
            "SELECT count() AS n FROM {db}.metric_labels FINAL \
             WHERE metric_name = 'w2' AND fingerprint = {fp}"
        )
    };
    let exec = |sql: String| {
        let bootstrap = &bootstrap;
        async move {
            bootstrap
                .execute(&sql, &QuerySettings::new(), Idempotency::Idempotent)
                .await
                .unwrap_or_else(|e| panic!("{e}\n{sql}"));
        }
    };

    for (case, fp) in [("constraint", 1u128), ("dropped", 2u128)] {
        // A token of this run's own, so the query log holds no other run's
        // inserts under it.
        let token = &uuid::Uuid::new_v4().to_string();
        let rows = [series_row(now, "w2", fp, hour, &labels, 0)];
        let settings = QuerySettings::landing_insert(token, 1_048_576);
        match case {
            "constraint" => {
                exec(format!(
                    "ALTER TABLE {db}.metric_labels ADD CONSTRAINT w2_refuses \
                     CHECK JSONExtractString(labels, 'job') != 'w2'"
                ))
                .await
            }
            _ => exec(format!("DROP TABLE {db}.metric_labels SYNC")).await,
        }
        let first = pusher
            .insert_block_with("metric_landing", &rows, &settings)
            .await;
        assert!(
            first.is_err(),
            "{case}: a push whose lookup row is not written must fail"
        );
        assert_eq!(
            count(&client, &activity_count(fp)).await,
            0,
            "{case}: the failed push writes no activity row"
        );
        match case {
            "constraint" => {
                exec(format!(
                    "ALTER TABLE {db}.metric_labels DROP CONSTRAINT w2_refuses"
                ))
                .await
            }
            _ => run_init(&bootstrap, &RenderCtx::for_tests(&db))
                .await
                .expect("re-create the lookup table"),
        }
        pusher
            .insert_block_with("metric_landing", &rows, &settings)
            .await
            .unwrap_or_else(|e| panic!("{case}: the resend must succeed: {e}"));
        assert_eq!(
            count(&client, &activity_count(fp)).await,
            1,
            "{case}: the resend writes one activity row"
        );
        assert_eq!(
            count(&client, &lookup_count(fp)).await,
            1,
            "{case}: the resend writes one lookup row"
        );

        bootstrap
            .execute(
                "SYSTEM FLUSH LOGS",
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("flush logs");
        let log_sql = format!(
            "SELECT toString(type) AS kind, \
             Settings['insert_deduplication_token'] AS token \
             FROM system.query_log \
             WHERE query_kind = 'Insert' AND has(databases, '{db}') \
               AND type != 'QueryStart' \
               AND Settings['insert_deduplication_token'] = '{token}' \
             ORDER BY event_time_microseconds"
        );
        use futures::StreamExt;
        let mut stream = bootstrap
            .query_stream::<InsertLogRow>(&log_sql, &QuerySettings::new())
            .await
            .expect("query_log");
        let mut kinds = Vec::new();
        while let Some(row) = stream.next().await {
            let row = row.expect("decode");
            assert_eq!(&row.token, token);
            kinds.push(row.kind);
        }
        assert_eq!(
            kinds.len(),
            2,
            "{case}: the failed insert and the resend, one token: {kinds:?}"
        );
        assert!(kinds[0].starts_with("Exception"), "{case}: {kinds:?}");
        assert_eq!(kinds[1], "QueryFinish", "{case}: {kinds:?}");
    }

    exec(format!("DROP USER IF EXISTS {user}")).await;
    drop_database(&bootstrap, &db).await;
}

fn pairs(labels: &pulsus_model::LabelSet) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = labels
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    out.sort();
    out
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct CountRow {
    n: u64,
}

async fn count(client: &ChClient, sql: &str) -> u64 {
    use futures::StreamExt;
    let mut stream = client
        .query_stream::<CountRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("count failed: {e}\n{sql}"));
    stream.next().await.expect("one row").expect("decode").n
}
