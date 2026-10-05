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

    // Stored once: one label row per label set, not one per name and hour.
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
        5,
        "one label row per label set"
    );

    drop_database(&bootstrap, &db).await;
}

/// **A sample in both tables at one key is answered as before, and one
/// statement reads both tables.** Three series: one with a float and a
/// histogram sample at the same millisecond, one float only, one histogram
/// only. The histogram wins the shared key, which is the rule the merge has
/// always applied.
#[tokio::test]
async fn one_statement_reads_both_sample_tables_and_the_answer_is_unchanged() {
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
    let params = MetricQueryParams {
        start_ms: now,
        end_ms: now,
        step_ms: 0,
    };
    let (result, _, explain) = engine
        .query_explained(&parse("m").expect("parse"), &params)
        .await
        .expect("query");

    let fetches: Vec<&str> = explain
        .stages
        .iter()
        .filter(|s| s.name.contains("sample_fetch"))
        .map(|s| s.sql.as_str())
        .collect();
    assert_eq!(fetches.len(), 1, "one statement: {:#?}", explain.stages);
    assert!(
        fetches[0].contains("FROM metric_samples")
            && fetches[0].contains("FROM metric_hist_samples")
            && fetches[0].contains("UNION ALL"),
        "the one statement reads both tables: {}",
        fetches[0]
    );

    let mut answers: Vec<(String, &'static str)> = match result {
        QueryResult::VectorHist(v) => v
            .into_iter()
            .map(|s| {
                let job = s
                    .labels
                    .iter()
                    .find(|(k, _)| k == "job")
                    .map(|(_, v)| v.clone())
                    .expect("a job label");
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
        other => panic!("expected VectorHist, got {other:?}"),
    };
    answers.sort();
    assert_eq!(
        answers,
        vec![
            ("both".to_string(), "hist"),
            ("float".to_string(), "float"),
            ("hist".to_string(), "hist"),
        ],
        "the histogram wins the key both tables hold"
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
