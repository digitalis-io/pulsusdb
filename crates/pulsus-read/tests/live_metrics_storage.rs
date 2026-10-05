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
    CounterResetHint, DEFAULT_ACTIVITY_BUCKET_MS, Fingerprint, LabelSet, NativeHistogram, Span,
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
        bucket_ms: DEFAULT_ACTIVITY_BUCKET_MS,
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

/// The label sets of the corpus: five sets over two jobs. Each is one
/// fingerprint, shared by every metric name that carries it.
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
/// registered in two activity hours and every label set is shared by
/// several names, which is the shape where storing labels once per
/// fingerprint and joining them back could lose, duplicate or mislabel a
/// series.
#[tokio::test]
async fn every_series_read_returns_the_seeded_series_with_their_labels() {
    skip_unless_live!();
    let (bootstrap, db, client) = fresh(pulsus_testkit::test_db(
        "pulsus_read_it_metrics_storage_labels",
    ))
    .await;

    let now = now_ms();
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
    let hour = (now / bucket) * bucket;
    let sets: BTreeMap<u128, Vec<(String, String)>> = label_sets().into_iter().collect();
    let corpus = corpus();

    let mut rows = Vec::new();
    for (name, fps) in &corpus {
        for fp in fps {
            for activity in [hour - bucket, hour] {
                rows.push(series_row(now, name, *fp, activity, &sets[fp], 0));
            }
            rows.push(float_row(now, name, *fp, now - 10_000, *fp as f64));
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
                .map(|fp| (name.clone(), Fingerprint::from_raw(*fp), sets[fp].clone()))
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
                "INSERT INTO {db}.metric_series (metric_name, fingerprint, unix_milli) \
                 VALUES ('orphan', 9999, {hour})"
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
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
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
    let bucket = DEFAULT_ACTIVITY_BUCKET_MS;
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
struct Rec {
    name: &'static str,
    fp: u128,
    labels: Vec<(String, String)>,
    /// Activity buckets inside the checked window's span.
    buckets: Vec<i64>,
    /// Whether the series has its own label row.
    label_row: bool,
    /// Whether the series has a sample at the instant query's time.
    current: bool,
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
    fn holds(&self, labels: &[(String, String)]) -> bool {
        let v = labels
            .iter()
            .find(|(k, _)| k == self.key)
            .map(|(_, v)| v.as_str())
            .unwrap_or("");
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

    fn matcher(&self) -> LabelMatcher {
        LabelMatcher {
            key: self.key.to_string(),
            op: self.op,
            value: self.value.to_string(),
        }
    }
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

/// **L1 (issue #623): every read that takes label matchers answers from
/// each series' own label row.** Series active only at their own bucket,
/// matching series in the buckets just outside the window, fingerprints
/// shared by two names, series with activity and samples but no label row of
/// their own on fingerprints another name has one for, repeated label rows,
/// a background of matching series under another name, and a series that
/// registers after the sweep. Every answer is computed from the seed: no
/// repeat, no borrowed label row, no empty label set, and the post-sweep
/// series with its own labels.
#[tokio::test]
async fn matchers_answer_from_own_label_rows() {
    skip_unless_live!();
    let (bootstrap, db, client) = fresh(pulsus_testkit::test_db(
        "pulsus_read_it_metrics_storage_own_rows",
    ))
    .await;

    let now = now_ms();
    let h = DEFAULT_ACTIVITY_BUCKET_MS;
    let current = (now / h) * h;
    let b = current - 8 * h;
    let t = now - 10_000;
    let set = |pairs: &[(&str, String)]| -> Vec<(String, String)> {
        let mut v: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        v.sort();
        v
    };
    let q_labels = |i: usize| -> Vec<(String, String)> {
        let mut p = vec![
            ("instance", format!("q{i}")),
            ("job", ["api", "web", "db"][i % 3].to_string()),
        ];
        if i % 8 != 7 {
            p.push(("status", ["200", "404", "500", "503"][i % 4].to_string()));
        }
        set(&p)
    };
    let o_labels = |k: usize| -> Vec<(String, String)> {
        set(&[
            ("instance", format!("o{k}")),
            ("job", "api".to_string()),
            ("status", ["500", "503", "200", "404"][k % 4].to_string()),
        ])
    };

    let mut recs: Vec<Rec> = Vec::new();
    // m_q: 24 series, each active only at its own bucket. The first two
    // share their fingerprints with m_other.
    for i in 0..24usize {
        let fp = match i {
            0 => 7001,
            1 => 7002,
            _ => 1000 + i as u128,
        };
        recs.push(Rec {
            name: "m_q",
            fp,
            labels: q_labels(i),
            buckets: vec![b + (i as i64 % 6) * h],
            label_row: true,
            current: true,
        });
    }
    // Four matching m_q series in the buckets just outside the window.
    for k in 0..4usize {
        recs.push(Rec {
            name: "m_q",
            fp: 1100 + k as u128,
            labels: set(&[
                ("instance", format!("e{k}")),
                ("job", "api".to_string()),
                ("status", "500".to_string()),
            ]),
            buckets: vec![if k < 2 { b - h } else { b + 6 * h }],
            label_row: true,
            current: false,
        });
    }
    // m_other: the two shared fingerprints, then four of its own.
    recs.push(Rec {
        name: "m_other",
        fp: 7001,
        labels: q_labels(0),
        buckets: vec![b + 2 * h],
        label_row: true,
        current: true,
    });
    recs.push(Rec {
        name: "m_other",
        fp: 7002,
        labels: q_labels(1),
        buckets: vec![b + 2 * h],
        label_row: true,
        current: true,
    });
    for k in 0..4usize {
        recs.push(Rec {
            name: "m_other",
            fp: 7003 + k as u128,
            labels: o_labels(k),
            buckets: vec![b + 2 * h],
            label_row: true,
            current: true,
        });
    }
    // Series with activity and samples but no label row of their own, on
    // fingerprints m_other has one for.
    for (name, k) in [("m_q", 0usize), ("m_q", 1), ("m_orphan", 2)] {
        recs.push(Rec {
            name,
            fp: 7003 + k as u128,
            labels: o_labels(k),
            buckets: vec![b + 3 * h],
            label_row: false,
            current: true,
        });
    }
    // A background of matching series under another name.
    for n in 0..2000usize {
        recs.push(Rec {
            name: "m_bg",
            fp: 50_000 + n as u128,
            labels: set(&[
                ("instance", format!("bg{n}")),
                ("job", "api".to_string()),
                ("status", "500".to_string()),
            ]),
            buckets: vec![b + h],
            label_row: true,
            current: false,
        });
    }

    // Seed: kind-2 rows (the views write activity and label rows) for every
    // series with a label row; activity alone, written directly, for the
    // others; samples at `t` for every current series, float for even
    // fingerprints and histogram for odd ones.
    let mut landing = Vec::new();
    let mut orphan_values = Vec::new();
    for r in &recs {
        let mut buckets = r.buckets.clone();
        if r.current {
            buckets.push(current);
        }
        for bucket in buckets {
            if r.label_row {
                landing.push(series_row(now, r.name, r.fp, bucket, &r.labels, 0));
            } else {
                orphan_values.push(format!("('{}', {}, {bucket})", r.name, r.fp));
            }
        }
        if r.current {
            if r.fp % 2 == 0 {
                landing.push(float_row(now, r.name, r.fp, t, 42.0));
            } else {
                landing.push(hist_row(now, r.name, r.fp, t));
            }
        }
    }
    client
        .insert_block("metric_landing", &landing)
        .await
        .expect("seed metric_landing");
    client
        .execute(
            &format!(
                "INSERT INTO {db}.metric_series (metric_name, fingerprint, unix_milli) VALUES {}",
                orphan_values.join(", ")
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed activity with no label row");
    // Ten label rows repeated, in a block of their own.
    let repeats: Vec<MetricLandingRow> = recs
        .iter()
        .filter(|r| r.name == "m_q" && r.label_row)
        .take(10)
        .map(|r| series_row(now + 1, r.name, r.fp, r.buckets[0], &r.labels, 0))
        .collect();
    client
        .insert_block("metric_landing", &repeats)
        .await
        .expect("seed repeated label rows");

    let window = DataWindow {
        start_ms: b + 30 * 60_000,
        end_ms: b + 5 * h + 30 * 60_000,
    };
    let in_window = |r: &Rec| r.buckets.iter().any(|x| (b..=b + 5 * h).contains(x));
    let matcher_sets: Vec<Vec<M>> = vec![
        vec![M {
            key: "job",
            op: MatchOp::Eq,
            value: "api",
        }],
        vec![M {
            key: "status",
            op: MatchOp::Re,
            value: "5..",
        }],
        vec![M {
            key: "status",
            op: MatchOp::Neq,
            value: "500",
        }],
        vec![M {
            key: "status",
            op: MatchOp::Nre,
            value: "5..",
        }],
        vec![
            M {
                key: "job",
                op: MatchOp::Eq,
                value: "api",
            },
            M {
                key: "status",
                op: MatchOp::Nre,
                value: "5..",
            },
        ],
    ];
    let triples = |name: Option<&str>, ms: &[M]| -> BTreeSet<Triple> {
        recs.iter()
            .filter(|r| name.is_none_or(|n| n == r.name))
            .filter(|r| r.label_row && in_window(r))
            .filter(|r| ms.iter().all(|m| m.holds(&r.labels)))
            .map(|r| (r.name.to_string(), r.fp, r.labels.clone()))
            .collect()
    };
    let as_triples = |rows: Vec<pulsus_read::metrics::rows::SeriesRow>| {
        let raw: Vec<Triple> = rows
            .into_iter()
            .map(|r| (r.metric_name, raw(r.fingerprint), label_pairs(&r.labels)))
            .collect();
        let set: BTreeSet<_> = raw.iter().cloned().collect();
        assert_eq!(set.len(), raw.len(), "a series repeated: {raw:?}");
        set
    };
    use pulsus_read::metrics::sql;
    for ms in &matcher_sets {
        let matchers: Vec<LabelMatcher> = ms.iter().map(M::matcher).collect();
        let what = format!("{:?}", matchers);
        let want_q = triples(Some("m_q"), ms);

        let fps: BTreeSet<u128> = rows_of::<pulsus_read::metrics::exec::FingerprintOnlyRow>(
            &client,
            &sql::historical_series_subquery(
                "metric_series",
                "metric_labels",
                "m_q",
                window,
                h,
                &matchers,
            ),
        )
        .await
        .into_iter()
        .map(|r| raw(r.fingerprint))
        .collect();
        assert_eq!(
            fps,
            want_q
                .iter()
                .map(|(_, fp, _)| *fp)
                .collect::<BTreeSet<u128>>(),
            "historical_series_subquery {what}"
        );

        let resolved = rows_of::<pulsus_read::metrics::exec::HydratedLabelsRow>(
            &client,
            &sql::historical_resolution_query(
                "metric_series",
                "metric_labels",
                "m_q",
                window,
                h,
                &matchers,
            ),
        )
        .await;
        let resolved: Vec<(u128, Vec<(String, String)>)> = resolved
            .into_iter()
            .map(|r| (raw(r.fingerprint), label_pairs(&r.labels)))
            .collect();
        assert_eq!(
            resolved.iter().cloned().collect::<BTreeSet<_>>(),
            want_q.iter().map(|(_, fp, l)| (*fp, l.clone())).collect(),
            "historical_resolution_query {what}"
        );
        assert_eq!(
            resolved.len(),
            want_q.len(),
            "historical_resolution_query repeated {what}"
        );

        for name in [Some("m_q"), None] {
            let filter = DiscoveryFilter {
                metric_name: name.map(str::to_string),
                name_matchers: Vec::new(),
                matchers: matchers.clone(),
            };
            let got = as_triples(
                rows_of(
                    &client,
                    &sql::discovery_query("metric_series", "metric_labels", &filter, window, h),
                )
                .await,
            );
            let want = triples(name, ms);
            assert_eq!(got, want, "discovery_query {name:?} {what}");
            let names: BTreeSet<String> = rows_of::<pulsus_read::metrics::rows::MetricNameRow>(
                &client,
                &sql::discovery_distinct_names_query(
                    "metric_series",
                    "metric_labels",
                    &filter,
                    window,
                    h,
                ),
            )
            .await
            .into_iter()
            .map(|r| r.metric_name)
            .collect();
            assert_eq!(
                names,
                want.iter().map(|(n, _, _)| n.clone()).collect(),
                "discovery_distinct_names_query {name:?} {what}"
            );
        }

        let names = ["m_q".to_string(), "m_other".to_string()];
        let got = as_triples(
            rows_of(
                &client,
                &sql::discovery_fetch_by_names(
                    "metric_series",
                    "metric_labels",
                    &names,
                    &matchers,
                    window,
                    h,
                ),
            )
            .await,
        );
        let mut want = triples(Some("m_q"), ms);
        want.extend(triples(Some("m_other"), ms));
        assert_eq!(got, want, "discovery_fetch_by_names {what}");
    }

    // The fan-out over a resolved set, every fingerprint either name has.
    let names = ["m_q".to_string(), "m_other".to_string()];
    let fps: BTreeSet<u128> = recs
        .iter()
        .filter(|r| r.name == "m_q" || r.name == "m_other")
        .map(|r| r.fp)
        .collect();
    let fp_literals: Vec<pulsus_model::FpLiteral> = fps
        .iter()
        .map(|fp| Fingerprint::from_raw(*fp).sql_literal())
        .collect();
    let got = as_triples(
        rows_of(
            &client,
            &sql::discovery_fetch_multi(
                "metric_series",
                "metric_labels",
                &names,
                &fp_literals,
                window,
                h,
            ),
        )
        .await,
    );
    let mut want = triples(Some("m_q"), &[]);
    want.extend(triples(Some("m_other"), &[]));
    assert_eq!(got, want, "discovery_fetch_multi");

    // The names query answers exactly the names of discovery's rows, for
    // each shape of filter — except where the filter has no label matcher
    // and no name: that names query reads the activity rows alone (issue
    // #472's narrow projection, which never reads `metric_labels`), so a
    // name whose series in the window all lack a label row of their own is
    // in it and not in discovery. Computed from the seed.
    let unlabelled_names: BTreeSet<String> = recs
        .iter()
        .filter(|r| in_window(r))
        .map(|r| r.name.to_string())
        .filter(|name| {
            !recs
                .iter()
                .any(|r| r.name == name && r.label_row && in_window(r))
        })
        .collect();
    assert_eq!(
        unlabelled_names,
        BTreeSet::from(["m_orphan".to_string()]),
        "the fixture's one name with activity and no label row"
    );
    for filter in [
        DiscoveryFilter::default(),
        DiscoveryFilter {
            metric_name: Some("m_q".to_string()),
            name_matchers: Vec::new(),
            matchers: Vec::new(),
        },
        DiscoveryFilter {
            metric_name: None,
            name_matchers: Vec::new(),
            matchers: vec![LabelMatcher {
                key: "job".to_string(),
                op: MatchOp::Eq,
                value: "api".to_string(),
            }],
        },
        DiscoveryFilter {
            metric_name: None,
            name_matchers: Vec::new(),
            matchers: vec![LabelMatcher {
                key: "status".to_string(),
                op: MatchOp::Re,
                value: "5..".to_string(),
            }],
        },
        DiscoveryFilter {
            metric_name: Some("m_orphan".to_string()),
            name_matchers: Vec::new(),
            matchers: vec![LabelMatcher {
                key: "job".to_string(),
                op: MatchOp::Eq,
                value: "api".to_string(),
            }],
        },
    ] {
        let wide: BTreeSet<String> = rows_of::<pulsus_read::metrics::rows::SeriesRow>(
            &client,
            &sql::discovery_query("metric_series", "metric_labels", &filter, window, h),
        )
        .await
        .into_iter()
        .map(|r| r.metric_name)
        .collect();
        let narrow: BTreeSet<String> = rows_of::<pulsus_read::metrics::rows::MetricNameRow>(
            &client,
            &sql::discovery_distinct_names_query(
                "metric_series",
                "metric_labels",
                &filter,
                window,
                h,
            ),
        )
        .await
        .into_iter()
        .map(|r| r.metric_name)
        .collect();
        let mut want = wide.clone();
        if filter.metric_name.is_none() && filter.matchers.is_empty() {
            want.extend(unlabelled_names.iter().cloned());
        }
        assert_eq!(narrow, want, "the names of discovery's rows, {filter:?}");
    }

    // The sweep: every m_q series with its own label row, none borrowed.
    let cache = Arc::new(LabelCache::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache_config(&db),
    ));
    cache.refresh().await.expect("refresh");
    let cache_window = DataWindow {
        start_ms: now - 12 * h,
        end_ms: now,
    };
    let swept: BTreeSet<(u128, Vec<(String, String)>)> =
        match cache.resolve_labelled("m_q", &[], cache_window) {
            LabelledResolution::Series(series) => series
                .into_iter()
                .map(|(fp, l)| (raw(fp), pairs(&l)))
                .collect(),
            other => panic!("the warm cache must answer, got {other:?}"),
        };
    let want_swept: BTreeSet<(u128, Vec<(String, String)>)> = recs
        .iter()
        .filter(|r| r.name == "m_q" && r.label_row)
        .map(|r| (r.fp, r.labels.clone()))
        .collect();
    assert_eq!(swept, want_swept, "the sweep");

    // A series that registers after the sweep, on an m_other fingerprint,
    // with its own label row.
    let post = Rec {
        name: "m_q",
        fp: 7006,
        labels: o_labels(3),
        buckets: vec![b + 4 * h],
        label_row: true,
        current: true,
    };
    client
        .insert_block(
            "metric_landing",
            &[
                series_row(
                    now + 2,
                    post.name,
                    post.fp,
                    post.buckets[0],
                    &post.labels,
                    0,
                ),
                series_row(now + 2, post.name, post.fp, current, &post.labels, 0),
                float_row(now + 2, post.name, post.fp, t, 42.0),
            ],
        )
        .await
        .expect("seed the post-sweep series");
    recs.push(post);

    let params = MetricQueryParams {
        start_ms: now,
        end_ms: now,
        step_ms: 0,
    };
    let answered = |result: QueryResult| -> BTreeSet<Vec<(String, String)>> {
        let mut out: Vec<Vec<(String, String)>> = match result {
            QueryResult::Vector(v) => v.into_iter().map(|s| s.labels).collect(),
            QueryResult::VectorHist(v) => v.into_iter().map(|s| s.labels).collect(),
            other => panic!("expected a vector, got {other:?}"),
        };
        for l in &mut out {
            l.sort();
        }
        let set: BTreeSet<_> = out.iter().cloned().collect();
        assert_eq!(set.len(), out.len(), "a series repeated: {out:?}");
        set
    };
    let want_now = |names: &[&str]| -> BTreeSet<Vec<(String, String)>> {
        recs.iter()
            .filter(|r| names.contains(&r.name) && r.current && r.label_row)
            .map(|r| {
                let mut l = r.labels.clone();
                l.push(("__name__".to_string(), r.name.to_string()));
                l.sort();
                l
            })
            .collect()
    };

    // The fallback (a cold cache), no matchers: m_q's series with their own
    // label rows, the post-sweep series included, no orphan.
    let cold = Arc::new(LabelCache::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache_config(&db),
    ));
    let engine = MetricsEngine::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cold,
        engine_config(&db),
    );
    let (result, _) = engine
        .query(&parse("m_q").expect("parse"), &params)
        .await
        .expect("fallback query");
    assert_eq!(answered(result), want_now(&["m_q"]), "the fallback fetch");

    // The warm cache's fan-out: the post-sweep pair is looked up by the pair
    // and keeps its own labels; an orphan is dropped.
    let engine = MetricsEngine::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache,
        engine_config(&db),
    );
    let (result, _) = engine
        .query(
            &parse(r#"{__name__=~"m_q|m_other"}"#).expect("parse"),
            &params,
        )
        .await
        .expect("fan-out query");
    assert_eq!(
        answered(result),
        want_now(&["m_q", "m_other"]),
        "the multi-metric fan-out"
    );

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
