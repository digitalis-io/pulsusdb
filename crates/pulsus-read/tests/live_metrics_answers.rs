//! The read-path answer test (issue #547): PromQL answers computed
//! **through ClickHouse** compared against known values, bit for bit.
//!
//! Nothing else in the tree does this. The PromQL correctness corpus runs
//! against a synthetic in-memory store and never reaches
//! `crates/pulsus-read`; the live suites that do reach it assert labels,
//! statement text and index pruning.
//!
//! What that leaves open is narrower than "any wrong value", and it is
//! worth stating exactly, because the wider claim is false.
//!
//! The read path builds a float sample at two places — `group_rows` and
//! `group_multi_rows` (`crates/pulsus-read/src/metrics/exec.rs:2116` and
//! `:2170`). Mutating **both**, one mutation at a time, against the three
//! live suites for this engine (`live_metrics_engine`,
//! `live_metrics_cache`, `live_discovery_fallback` — 39 tests) and the two
//! crates' unit tests (`pulsus-read --lib` 1,348 and `pulsus-promql --lib`
//! 829 — 2,177):
//!
//! ```text
//!   mutation                      39 live   2,177 unit   this suite (19)
//!   --------------------------    -------   ----------   ---------------
//!   every float sample + 1.0       6 red      1 red          11 red
//!   every float sample + 0.0       0 red      0 red           6 red
//!     (so only -0.0 moves)
//! ```
//!
//! The one unit test `+ 1.0` reddens is `metrics::exec::tests::`
//! `fetch_all_concurrently_result_matches_a_single_chunk_reference_bit_for_bit`.
//! Mutating `group_rows` alone gives 4 live rather than 6, because
//! `group_multi_rows` serves the multi-metric fan-out and nothing else;
//! the two sites are easy to confuse, so both figures are recorded.
//!
//! So a wholesale value change is seen, incidentally, by seven of those
//! 2,216 tests. A change that preserves `==` is seen by none of them:
//! `-0.0` normalised to `0.0` leaves every one green. It is a wrong answer
//! a user reads — `min by (status)` rendering `0` where the answer is
//! `-0`. The figures are for the production source at this head, which is
//! byte-identical to the base commit `7a6fd7e7`: this change adds a test
//! binary and one CI step and alters no production file.
//!
//! ## The two sides
//!
//! One literal fixture is seeded into a throwaway database and the same
//! query is answered twice:
//!
//! ```text
//!   the fixture: series identities and samples, written as literals below
//!        |                                                        |
//!        | INSERT metric_series / metric_samples /                | build SeriesData
//!        |        metric_hist_samples                             | directly, in memory
//!        v                                                        v
//!   +-------------------------------+                  +--------------------------+
//!   | side A -- the read path       |                  | side B -- no read path   |
//!   |  LabelCache::resolve_labelled |                  |  its own matcher code    |
//!   |  sample_sql::sample_fetch*    |                  |                          |
//!   |  ChClient fetch, RowBinary    |                  |   (no SQL, no database)  |
//!   |  group_rows / merge_series    |                  |                          |
//!   |  value_to_query_result        |                  |                          |
//!   +-------------------------------+                  +--------------------------+
//!   |  parse -> plan -> evaluate    |  <-- SHARED -->  | parse -> plan -> evaluate|
//!   |  the value types              |                  |  the value types         |
//!   |  the suite's own render_hist  |                  |  the same render_hist    |
//!   +--------------+----------------+                  +------------+-------------+
//!            QueryResult                                      QueryValue
//!                 |  test-local conversion                         |  test-local conversion
//!                 +------------------> Answer (bits) <-------------+
//! ```
//!
//! Side B is an oracle **for the read path** and for nothing else: it
//! shares no resolution, no SQL, no ClickHouse, no row grouping, no merge,
//! no result conversion, so a disagreement localises there. It is *not* an
//! oracle for the parser, the planner, the evaluator, the value types or
//! `render_hist` — a defect in any of those five moves both answers
//! together, which is measured, not assumed: a one-second error in the
//! planner's matrix-selector `@` resolution left every differential pair
//! agreeing.
//!
//! What covers the shared part is the literals. Of the nineteen tests
//! below, one asserts the fixture's own size and reaches no database, one
//! is the differential and carries no literal, and **seventeen assert an
//! answer the read path returned against a value stated here**. Fifteen of
//! those seventeen write every expected value out by hand; the other two —
//! the 501-series chunked set and the 241-point staleness grid — build
//! theirs from the fixture's arithmetic, because writing 501 and 241
//! values out would be transcription rather than an oracle.
//!
//! Every value is compared as `f64::to_bits`. That is what makes `NaN`
//! equal `NaN`, keeps `-0.0` different from `0.0`, and keeps the stale
//! marker distinguishable from an ordinary NaN. An `==` comparison cannot
//! tell `min by (status)` returning `-0` from returning `0`, which is
//! exactly the defect the rest of the tree misses.
//!
//! ## What this suite does not cover
//!
//! * **A fetch window that is too *wide*.** The in-memory side is given
//!   every sample the fixture declares, so extra fetched rows change no
//!   answer. It costs bytes, not correctness. The statement-text tests in
//!   `metrics::sample_sql` guard the window expression, including the
//!   lower bound's inclusivity, which no answer can see at all: the
//!   evaluator's lookback is left-open over the same instant, so a sample
//!   exactly on the bound is discarded whichever way the SQL reads.
//! * **Any defect in the five shared surfaces, outside a literal's
//!   reach.** The planner resolves `@` at four call sites; two of them
//!   have a literal oracle here — `plan_vector_selector`
//!   (`crates/pulsus-promql/src/plan.rs:1699`) and
//!   `plan_matrix_selector_id` (`:1729`). The subquery site (`:1825`) and
//!   the info-family site (`:2439`) have none, and no query below is a
//!   subquery.
//! * **Histogram shapes other than the one the fixture carries** — one
//!   exponential, `schema 0`, three positive buckets, no negative buckets,
//!   no zero bucket, no custom values. Nothing here covers NHCB or a
//!   histogram-valued answer. And `render_hist` is shared, so a field it
//!   did not print would be a field the differential could not compare.
//! * **Not reached at all:** the info-family selector, the
//!   `anchored`/`smoothed` range modifiers, the cardinality and memory
//!   caps, the multi-shard distributed read, and a cold cache combined
//!   with the multi-metric fan-out.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1` through
//! `pulsus_testkit::live_clickhouse_enabled`, so it skips on a machine
//! with no container and **panics** in a CI job that exists to provide
//! one. To run it:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:26.3
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test live_metrics_answers
//! podman rm -f -v pulsus-ch-test
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_model::{
    CounterResetHint, DEFAULT_ACTIVITY_BUCKET_MS, Fingerprint, FloatHistogram, STALE_NAN_BITS, Span,
};
use pulsus_promql::parser::parse;
use pulsus_promql::{FetchedSeries, Labels, QueryValue, Sample, SelectorSpec, SeriesData};
use pulsus_read::{
    HistOrFloat, LabelCache, LabelCacheConfig, MatchOp, MetricQueryParams, MetricsConfig,
    MetricsEngine, QueryResult,
};
use pulsus_schema::{RenderCtx, run_init};

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
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

async fn init_db(bootstrap: &ChClient, db: &str) {
    drop_database(bootstrap, db).await;
    let params = RenderCtx {
        db: db.to_string(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
        metrics_landing_retention_hours: 6,
        metrics_dedup_window: 10_000,
    };
    run_init(bootstrap, &params).await.expect("run_init");
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedSeriesRow {
    metric_name: String,
    fingerprint: u128,
    unix_milli: i64,
    labels: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedSampleRow {
    metric_name: String,
    fingerprint: u128,
    unix_milli: i64,
    value: f64,
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
        metadata_table: "metric_metadata".to_string(),
        experimental_functions: false,
        max_metric_fanout: 1_000,
        max_cache_scan: 200_000,
        max_info_series: 100_000,
        max_samples: 50_000_000,
        distributed: false,
        // Issue #549: the shipped default — every suite here exercises
        // the configuration production runs.
        grouped_push: true,
    }
}

// ---------------------------------------------------------------- fixture

/// One fixture series: its identity and its samples, as literals.
#[derive(Debug, Clone)]
struct FixtureSeries {
    fp: u64,
    metric: &'static str,
    labels: Vec<(String, String)>,
    /// `(unix_milli, value bits)` — bits so a NaN payload is a literal.
    samples: Vec<(i64, u64)>,
    /// Timestamps carrying the one fixed native-histogram sample shape.
    /// Present only to force the dual-read merge path; its CONTENT never
    /// reaches an asserted answer (min/max ignore histogram members).
    hist_samples: Vec<i64>,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedHistRow {
    metric_name: String,
    fingerprint: u128,
    unix_milli: i64,
    schema: i8,
    zero_threshold: f64,
    zero_count: u64,
    count: u64,
    sum: f64,
    pos_span_offsets: Vec<i32>,
    pos_span_lengths: Vec<u32>,
    pos_bucket_deltas: Vec<i64>,
    neg_span_offsets: Vec<i32>,
    neg_span_lengths: Vec<u32>,
    neg_bucket_deltas: Vec<i64>,
    custom_values: Vec<f64>,
}

/// What `hist_columns()` decodes to, written out here rather than run
/// through the read path's own decoder — the deltas `[1, 1, -1]` are the
/// bucket counts `[1, 2, 1]`.
fn expected_histogram() -> FloatHistogram {
    FloatHistogram {
        counter_reset_hint: CounterResetHint::Unknown,
        schema: 0,
        zero_threshold: 0.0,
        zero_count: 0.0,
        count: 4.0,
        sum: 5.0,
        positive_spans: vec![Span {
            offset: 0,
            length: 3,
        }],
        negative_spans: Vec::new(),
        positive_buckets: vec![1.0, 2.0, 1.0],
        negative_buckets: Vec::new(),
        custom_values: Vec::new(),
    }
}

fn hist_columns() -> pulsus_model::HistogramColumns {
    pulsus_model::HistogramColumns {
        schema: 0,
        zero_threshold: 0.0,
        zero_count: 0,
        count: 4,
        sum: 5.0,
        pos_span_offsets: vec![0],
        pos_span_lengths: vec![3],
        pos_bucket_deltas: vec![1, 1, -1],
        neg_span_offsets: vec![],
        neg_span_lengths: vec![],
        neg_bucket_deltas: vec![],
        custom_values: vec![],
        counter_reset_hint: 0,
    }
}

const NAN_BITS: u64 = 0x7FF8_0000_0000_0001;

/// The chunked fixture only spans more than one fingerprint chunk while
/// the threshold stays below its 501 series. Asserted at COMPILE time, so
/// raising the threshold fails the build rather than silently removing
/// this suite's chunk coverage.
const _: () = assert!(
    501 > pulsus_read::metrics::sample_sql::CHUNK_THRESHOLD,
    "the chunked fixture no longer spans two chunks: raise its series count above CHUNK_THRESHOLD"
);

/// The scrape-gap fixture, in one place so the test can state the
/// arithmetic instead of a magic number.
const SCRAPE_MS: i64 = 15_000;
const HOUR_MS: i64 = 3_600_000;
/// Grid point indices 0..=240 -- 241 points at a 15 s step over one hour.
const GRID_POINTS: i64 = 240;
/// The removed run: 40 samples = 10 minutes, longer than the 5-minute lookback.
const GAP_FIRST_MISSING: i64 = 100;
const GAP_LAST_MISSING: i64 = 139;
/// A series is absent at grid point `j` once its last surviving sample is
/// older than `j`'s lookback: `15000*(a-1) <= 15000*j - 300000`, so
/// `j >= a + 19`. It reappears at `b + 1`. With `a = 100`, `b = 139` that
/// is grid points 119..=139 -- exactly 21 of the 241.
const GAP_FIRST_ABSENT: i64 = GAP_FIRST_MISSING + 19;
const GAP_LAST_ABSENT: i64 = GAP_LAST_MISSING;

fn lbl(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn fixture(t: i64) -> Vec<FixtureSeries> {
    let m = "http_requests_total";
    let mut v = vec![
        FixtureSeries {
            fp: 1,
            metric: m,
            labels: lbl(&[("status", "200"), ("instance", "a")]),
            samples: vec![
                (t - 60_000, 1.0f64.to_bits()),
                (t - 30_000, 2.0f64.to_bits()),
                (t, 3.0f64.to_bits()),
            ],
            hist_samples: Vec::new(),
        },
        FixtureSeries {
            fp: 2,
            metric: m,
            labels: lbl(&[("status", "200"), ("instance", "b")]),
            samples: vec![
                (t - 60_000, 10.0f64.to_bits()),
                (t - 30_000, 20.0f64.to_bits()),
                (t, 30.5f64.to_bits()),
            ],
            hist_samples: Vec::new(),
        },
        FixtureSeries {
            fp: 3,
            metric: m,
            labels: lbl(&[("status", "500"), ("instance", "a")]),
            samples: vec![
                (t - 60_000, 0.1f64.to_bits()),
                (t - 30_000, 0.2f64.to_bits()),
                (t, 0.3f64.to_bits()),
            ],
            hist_samples: Vec::new(),
        },
        FixtureSeries {
            fp: 4,
            metric: m,
            labels: lbl(&[("status", "500"), ("instance", "b")]),
            samples: vec![
                (t - 60_000, (-0.0f64).to_bits()),
                (t - 30_000, 0.0f64.to_bits()),
                (t, (-0.0f64).to_bits()),
            ],
            hist_samples: Vec::new(),
        },
        // silent for longer than the 5-minute lookback
        FixtureSeries {
            fp: 5,
            metric: m,
            labels: lbl(&[("status", "404"), ("instance", "a")]),
            samples: vec![(t - 600_000, 7.0f64.to_bits())],
            hist_samples: Vec::new(),
        },
        // NaN group: one NaN, two numbers
        FixtureSeries {
            fp: 10,
            metric: "gauge_nan",
            labels: lbl(&[("g", "one")]),
            samples: vec![(t, NAN_BITS)],
            hist_samples: Vec::new(),
        },
        FixtureSeries {
            fp: 11,
            metric: "gauge_nan",
            labels: lbl(&[("g", "one")]),
            samples: vec![(t, 3.0f64.to_bits())],
            hist_samples: Vec::new(),
        },
        FixtureSeries {
            fp: 12,
            metric: "gauge_nan",
            labels: lbl(&[("g", "one")]),
            samples: vec![(t, 1.0f64.to_bits())],
            hist_samples: Vec::new(),
        },
        // all-NaN group
        FixtureSeries {
            fp: 13,
            metric: "gauge_nan",
            labels: lbl(&[("g", "two")]),
            samples: vec![(t, NAN_BITS)],
            hist_samples: Vec::new(),
        },
        FixtureSeries {
            fp: 14,
            metric: "gauge_nan",
            labels: lbl(&[("g", "two")]),
            samples: vec![(t, NAN_BITS)],
            hist_samples: Vec::new(),
        },
        // stale marker as the most recent sample
        FixtureSeries {
            fp: 15,
            metric: "gauge_nan",
            labels: lbl(&[("g", "three")]),
            samples: vec![(t - 30_000, 5.0f64.to_bits()), (t, STALE_NAN_BITS)],
            hist_samples: Vec::new(),
        },
        // window bounds
        FixtureSeries {
            fp: 20,
            metric: "edge_probe",
            labels: lbl(&[("edge", "upper")]),
            samples: vec![(t - 1, 11.0f64.to_bits()), (t, 22.0f64.to_bits())],
            hist_samples: Vec::new(),
        },
        FixtureSeries {
            fp: 21,
            metric: "edge_probe",
            labels: lbl(&[("edge", "lower_at")]),
            samples: vec![(t - 300_000, 33.0f64.to_bits())],
            hist_samples: Vec::new(),
        },
        FixtureSeries {
            fp: 22,
            metric: "edge_probe",
            labels: lbl(&[("edge", "lower_inside")]),
            samples: vec![(t - 300_000 + 1, 44.0f64.to_bits())],
            hist_samples: Vec::new(),
        },
        // second metric, for the multi-metric fan-out
        FixtureSeries {
            fp: 30,
            metric: "http_request_errors_total",
            labels: lbl(&[("status", "500"), ("instance", "a")]),
            samples: vec![(t, 9.0f64.to_bits())],
            hist_samples: Vec::new(),
        },
    ];
    // The fan-out's own dual-read member: a histogram sample and a later
    // float sample on one series of the second metric, so the fan-out's
    // histogram fetch is non-empty and the whole fan-out takes the merge
    // path (the `MultiSampleRow` copy of the float-half accessor).
    v.push(FixtureSeries {
        fp: 31,
        metric: "http_request_errors_total",
        labels: lbl(&[("status", "500"), ("instance", "c")]),
        samples: vec![(t, 2.0f64.to_bits())],
        hist_samples: vec![t - 30_000],
    });
    // The anchor probe: three samples one SECOND apart around the anchor
    // the modifier test names, so an `@`/`offset` that resolves even one
    // second early or late selects a different sample. The rest of the
    // fixture is 30 s apart and cannot see an error smaller than that.
    v.push(FixtureSeries {
        fp: 70,
        metric: "anchor_probe",
        labels: lbl(&[("a", "x")]),
        // Written as `30_000 -/+ 1_000` rather than `31_000`/`29_000`:
        // the fixed-port guard
        // (`crates/pulsus-server/tests/live_port_uniqueness.rs`) reads any
        // bare 31000-31999 literal in a live suite as a listener-port
        // declaration, and `31_000` would be one.
        samples: vec![
            (t - 30_000 - 1_000, 101.0f64.to_bits()),
            (t - 30_000, 102.0f64.to_bits()),
            (t - 30_000 + 1_000, 103.0f64.to_bits()),
        ],
        hist_samples: Vec::new(),
    });
    // The scrape-gap metric: four status groups of two series each,
    // scraped every 15 s for an hour (241 samples), with one series of
    // `status="404"` missing a 40-sample (10-minute) run. Over a 241-point
    // range query at the same 15 s step, that series is absent for exactly
    // 21 grid points -- see `GAP_FIRST_ABSENT`/`GAP_LAST_ABSENT`.
    for (n, status) in ["200", "404", "500", "503"].iter().enumerate() {
        for replica in ["a", "b"] {
            let fp = 60 + (n as u64) * 2 + u64::from(replica == "b");
            let gapped = *status == "404" && replica == "b";
            let samples = (0..=GRID_POINTS)
                .filter(|i| !(gapped && (GAP_FIRST_MISSING..=GAP_LAST_MISSING).contains(i)))
                .map(|i| (t - HOUR_MS + i * SCRAPE_MS, (i as f64).to_bits()))
                .collect();
            v.push(FixtureSeries {
                fp,
                metric: "scrape_gap",
                labels: lbl(&[("status", status), ("replica", replica)]),
                samples,
                hist_samples: Vec::new(),
            });
        }
    }
    // Two series of one metric sharing a label set: the read path must
    // deliver them as two series (grouped by fingerprint), which the
    // evaluator then refuses to put in one vector.
    v.push(FixtureSeries {
        fp: 50,
        metric: "dup_labels",
        labels: lbl(&[("k", "v")]),
        samples: vec![(t, 1.0f64.to_bits())],
        hist_samples: Vec::new(),
    });
    v.push(FixtureSeries {
        fp: 51,
        metric: "dup_labels",
        labels: lbl(&[("k", "v")]),
        samples: vec![(t, 2.0f64.to_bits())],
        hist_samples: Vec::new(),
    });
    // The dual-read probe: one native-histogram series under the same
    // metric name as two float series, so the metric's histogram fetch
    // returns a row and every series of that metric goes through the
    // merge path instead of the float-only fast path.
    v.push(FixtureSeries {
        fp: 40,
        metric: "dual_probe",
        labels: lbl(&[("d", "x")]),
        samples: Vec::new(),
        hist_samples: vec![t],
    });
    v.push(FixtureSeries {
        fp: 41,
        metric: "dual_probe",
        labels: lbl(&[("d", "y")]),
        samples: vec![(t - 30_000, 5.0f64.to_bits()), (t, 5.0f64.to_bits())],
        hist_samples: Vec::new(),
    });
    v.push(FixtureSeries {
        fp: 42,
        metric: "dual_probe",
        labels: lbl(&[("d", "z")]),
        samples: vec![(t - 30_000, 7.0f64.to_bits()), (t, 7.0f64.to_bits())],
        hist_samples: Vec::new(),
    });
    // 501 series -> two fingerprint chunks (CHUNK_THRESHOLD = 500)
    for i in 0..501u64 {
        v.push(FixtureSeries {
            fp: 1000 + i,
            metric: "chunked_total",
            labels: lbl(&[("shard", &format!("s{i}"))]),
            samples: vec![(t, (i as f64).to_bits())],
            hist_samples: Vec::new(),
        });
    }
    v
}

async fn seed(client: &ChClient, fx: &[FixtureSeries], bucket: i64) {
    let series: Vec<SeedSeriesRow> = fx
        .iter()
        .map(|s| {
            let map: BTreeMap<&str, &str> = s
                .labels
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            SeedSeriesRow {
                metric_name: s.metric.to_string(),
                fingerprint: u128::from(s.fp),
                unix_milli: bucket,
                labels: serde_json::to_string(&map).expect("labels json"),
            }
        })
        .collect();
    let samples: Vec<SeedSampleRow> = fx
        .iter()
        .flat_map(|s| {
            s.samples.iter().map(move |(t, bits)| SeedSampleRow {
                metric_name: s.metric.to_string(),
                fingerprint: u128::from(s.fp),
                unix_milli: *t,
                value: f64::from_bits(*bits),
            })
        })
        .collect();
    client
        .insert_block("metric_series", &series)
        .await
        .expect("seed metric_series");
    client
        .insert_block("metric_samples", &samples)
        .await
        .expect("seed metric_samples");
    let cols = hist_columns();
    let hist: Vec<SeedHistRow> = fx
        .iter()
        .flat_map(|s| {
            let cols = cols.clone();
            s.hist_samples.iter().map(move |t| SeedHistRow {
                metric_name: s.metric.to_string(),
                fingerprint: u128::from(s.fp),
                unix_milli: *t,
                schema: cols.schema,
                zero_threshold: cols.zero_threshold,
                zero_count: cols.zero_count,
                count: cols.count,
                sum: cols.sum,
                pos_span_offsets: cols.pos_span_offsets.clone(),
                pos_span_lengths: cols.pos_span_lengths.clone(),
                pos_bucket_deltas: cols.pos_bucket_deltas.clone(),
                neg_span_offsets: cols.neg_span_offsets.clone(),
                neg_span_lengths: cols.neg_span_lengths.clone(),
                neg_bucket_deltas: cols.neg_bucket_deltas.clone(),
                custom_values: cols.custom_values.clone(),
            })
        })
        .collect();
    if !hist.is_empty() {
        client
            .insert_block("metric_hist_samples", &hist)
            .await
            .expect("seed metric_hist_samples");
    }
}

// --------------------------------------------------- the comparable answer

/// A PromQL answer in a form both sides can be reduced to, independently
/// of the read path's own `QueryValue` -> `QueryResult` conversion.
/// Values are `f64::to_bits`, so NaN compares equal to NaN and `-0.0`
/// never compares equal to `0.0`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Val {
    Float(u64),
    Hist(String),
}

/// A histogram's every field as bits, in one canonical string — the same
/// rendering on both sides, so this is a comparison form, not an oracle.
fn render_hist(h: &FloatHistogram) -> String {
    let f = |xs: &[f64]| {
        xs.iter()
            .map(|x| format!("{:#018x}", x.to_bits()))
            .collect::<Vec<_>>()
            .join(",")
    };
    let sp = |xs: &[Span]| {
        xs.iter()
            .map(|s| format!("{}:{}", s.offset, s.length))
            .collect::<Vec<_>>()
            .join(",")
    };
    format!(
        "hint={:?} schema={} zt={:#018x} zc={:#018x} count={:#018x} sum={:#018x} \
         ps=[{}] ns=[{}] pb=[{}] nb=[{}] cv=[{}]",
        h.counter_reset_hint,
        h.schema,
        h.zero_threshold.to_bits(),
        h.zero_count.to_bits(),
        h.count.to_bits(),
        h.sum.to_bits(),
        sp(&h.positive_spans),
        sp(&h.negative_spans),
        f(&h.positive_buckets),
        f(&h.negative_buckets),
        f(&h.custom_values),
    )
}

/// A result series' output labels, `__name__` included where the series
/// still carries one.
type LabelPairs = Vec<(String, String)>;
/// One instant-vector element.
type VectorRow = (LabelPairs, Val);
/// One matrix series: its labels and its ascending points.
type MatrixRow = (LabelPairs, Vec<(i64, Val)>);

#[derive(Debug, Clone, PartialEq, Eq)]
enum Answer {
    Vector(Vec<VectorRow>),
    Matrix(Vec<MatrixRow>),
    Scalar(u64),
    Str(String),
    Other(String),
}

fn answer_from_result(r: QueryResult) -> Answer {
    match r {
        QueryResult::Vector(v) => Answer::Vector(
            v.into_iter()
                .map(|s| (s.labels, Val::Float(s.value.to_bits())))
                .collect(),
        ),
        QueryResult::Matrix(m) => Answer::Matrix(
            m.into_iter()
                .map(|s| {
                    (
                        s.labels,
                        s.points
                            .into_iter()
                            .map(|(t, v)| (t, Val::Float(v.to_bits())))
                            .collect(),
                    )
                })
                .collect(),
        ),
        QueryResult::VectorHist(v) => Answer::Vector(
            v.into_iter()
                .map(|s| {
                    let val = match &s.value {
                        HistOrFloat::Float(f) => Val::Float(f.to_bits()),
                        HistOrFloat::Hist(h) => Val::Hist(render_hist(h)),
                    };
                    (s.labels, val)
                })
                .collect(),
        ),
        QueryResult::MatrixHist(m) => Answer::Matrix(
            m.into_iter()
                .map(|s| {
                    let points = s
                        .points
                        .iter()
                        .map(|(t, v)| {
                            let val = match v {
                                HistOrFloat::Float(f) => Val::Float(f.to_bits()),
                                HistOrFloat::Hist(h) => Val::Hist(render_hist(h)),
                            };
                            (*t, val)
                        })
                        .collect();
                    (s.labels, points)
                })
                .collect(),
        ),
        QueryResult::Scalar(v) => Answer::Scalar(v.to_bits()),
        QueryResult::String(s) => Answer::Str(s),
        other => Answer::Other(format!("{other:?}")),
    }
}

/// The in-memory side's conversion — written here, not reused from the
/// read path, so the read path's own `__name__` re-attachment is compared
/// against an independent copy rather than against itself.
fn answer_from_value(v: QueryValue) -> Answer {
    fn labels_of(l: Labels, name: Option<String>) -> Vec<(String, String)> {
        let mut pairs = l.0;
        if let Some(n) = name {
            pairs.push(("__name__".to_string(), n));
        }
        pairs
    }
    match v {
        QueryValue::Vector(v) => Answer::Vector(
            v.into_iter()
                .map(|s| {
                    let val = match &s.h {
                        Some(h) => Val::Hist(render_hist(h)),
                        None => Val::Float(s.v.to_bits()),
                    };
                    (labels_of(s.labels, s.metric_name), val)
                })
                .collect(),
        ),
        QueryValue::Matrix(m) => Answer::Matrix(
            m.into_iter()
                .map(|s| {
                    let points = s
                        .points
                        .iter()
                        .map(|p| {
                            let val = match &p.h {
                                Some(h) => Val::Hist(render_hist(h)),
                                None => Val::Float(p.v.to_bits()),
                            };
                            (p.t_ms, val)
                        })
                        .collect();
                    (labels_of(s.labels, s.metric_name), points)
                })
                .collect(),
        ),
        QueryValue::Scalar(v) => Answer::Scalar(v.to_bits()),
        QueryValue::String(s) => Answer::Str(s),
    }
}

// ------------------------------------------------- the in-memory (B) side

/// Independent matcher evaluation over the fixture's literal label sets —
/// never `metrics::matcher`/`LabelCache`, which are the code under test.
fn series_matches(s: &FixtureSeries, spec: &SelectorSpec) -> bool {
    if let Some(name) = &spec.metric_name
        && s.metric != name
    {
        return false;
    }
    spec.matchers.iter().all(|m| {
        let v = s
            .labels
            .iter()
            .find(|(k, _)| k.as_str() == m.key)
            .map(|(_, v)| v.as_str())
            .unwrap_or("");
        let anchored = || regex::Regex::new(&format!("^(?s:{})$", m.value)).expect("regex");
        match m.op {
            MatchOp::Eq => v == m.value,
            MatchOp::Neq => v != m.value,
            MatchOp::Re => anchored().is_match(v),
            MatchOp::Nre => !anchored().is_match(v),
        }
    }) && spec.name_matchers.iter().all(|m| {
        let v = s.metric;
        let anchored = || regex::Regex::new(&format!("^(?s:{})$", m.value)).expect("regex");
        match m.op {
            MatchOp::Eq => v == m.value,
            MatchOp::Neq => v != m.value,
            MatchOp::Re => anchored().is_match(v),
            MatchOp::Nre => !anchored().is_match(v),
        }
    })
}

/// Side B: the same expression evaluated over a `SeriesData` built
/// straight from the fixture — every sample the fixture declares, no
/// window, no SQL, no ClickHouse.
fn in_memory_answer(query: &str, fx: &[FixtureSeries], p: &MetricQueryParams) -> Answer {
    let expr = parse(query).expect("parse");
    let plan = pulsus_promql::plan(&expr, p.plan_params(false)).expect("plan");
    let mut data = SeriesData::new();
    for spec in &plan.selectors {
        let mut chosen: Vec<&FixtureSeries> =
            fx.iter().filter(|s| series_matches(s, spec)).collect();
        chosen.sort_by_key(|s| (s.metric, s.fp));
        let series: Vec<FetchedSeries> = chosen
            .into_iter()
            .map(|s| FetchedSeries {
                fingerprint: Fingerprint::from_raw(u128::from(s.fp)),
                metric_name: Some(s.metric.to_string()),
                labels: Labels::new(s.labels.iter().cloned()),
                samples: {
                    let mut all: Vec<Sample> = s
                        .samples
                        .iter()
                        .map(|(t, bits)| Sample::float(*t, f64::from_bits(*bits)))
                        .chain(
                            s.hist_samples
                                .iter()
                                .map(|t| Sample::hist(*t, expected_histogram())),
                        )
                        .collect();
                    all.sort_by_key(|x| x.t_ms);
                    all
                },
                start_ts: None,
            })
            .collect();
        data.insert(spec.id, series);
    }
    answer_from_value(pulsus_promql::evaluate(&plan, &data).expect("evaluate").0)
}

struct Harness {
    bootstrap: ChClient,
    db: String,
    engine: MetricsEngine,
    cold_engine: MetricsEngine,
    fx: Vec<FixtureSeries>,
    t: i64,
}

/// `db` is already composed by `pulsus_testkit::test_db` at the call site,
/// not here: the naming guard
/// (`crates/pulsus-server/tests/live_db_naming.rs`) requires the reserved
/// `pulsus_*_it_*` name to sit inside the helper's own argument list, so
/// that every test shows the per-checkout prefix reaching its database
/// rather than trusting a helper to apply it out of sight.
async fn harness(db: &str) -> Harness {
    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = db.to_string();
    init_db(&bootstrap, &db).await;
    let client = ChClient::new(test_config(&db)).await.expect("connect");
    let now = now_ms();
    let t = (now / 60_000) * 60_000;
    let bucket = (now / DEFAULT_ACTIVITY_BUCKET_MS) * DEFAULT_ACTIVITY_BUCKET_MS;
    let fx = fixture(t);
    seed(&client, &fx, bucket).await;

    let cache = Arc::new(LabelCache::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache_config(&db),
    ));
    cache.refresh().await.expect("refresh");
    assert!(cache.is_warm());
    let engine = MetricsEngine::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache,
        engine_config(&db),
    );
    // never refreshed -> degraded/cold -> the SqlFallback fetch shape
    let cold_cache = Arc::new(LabelCache::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache_config(&db),
    ));
    let cold_engine = MetricsEngine::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cold_cache,
        engine_config(&db),
    );
    Harness {
        bootstrap,
        db,
        engine,
        cold_engine,
        fx,
        t,
    }
}

impl Harness {
    async fn read_path(&self, query: &str, p: &MetricQueryParams) -> Answer {
        let expr = parse(query).expect("parse");
        let (result, _ann) = self.engine.query(&expr, p).await.expect("query");
        answer_from_result(result)
    }
    async fn read_path_cold(&self, query: &str, p: &MetricQueryParams) -> Answer {
        let expr = parse(query).expect("parse");
        let (result, _ann) = self.cold_engine.query(&expr, p).await.expect("query");
        answer_from_result(result)
    }
    fn memory(&self, query: &str, p: &MetricQueryParams) -> Answer {
        in_memory_answer(query, &self.fx, p)
    }
    fn instant(&self) -> MetricQueryParams {
        MetricQueryParams {
            start_ms: self.t,
            end_ms: self.t,
            step_ms: 0,
        }
    }
}

fn vec_answer(items: &[(&[(&str, &str)], f64)]) -> Answer {
    Answer::Vector(
        items
            .iter()
            .map(|(labels, v)| {
                (
                    labels
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                    Val::Float(v.to_bits()),
                )
            })
            .collect(),
    )
}

/// The queries the differential covers -- one per cell of the
/// (resolution shape x fetch window x row stream) grid, never a second
/// aggregation form over a fetch already represented. Measured: eight
/// function classes over one selector emit a byte-identical statement, so
/// a second reducer adds evaluator coverage, not read-path coverage.
///
///   resolution:  warm concrete name | chunked | multi-metric fan-out
///                (the cold sub-query shape has its own test, because it
///                needs a second engine)
///   window:      instant | range selector | offset | anchor | range+offset
///                | range+anchor
///   row stream:  float only | float + histogram (dual read)
///
/// Built against the fixture anchor `t` so `offset` and `@` carry real
/// timestamps.
fn differential_queries(t: i64) -> Vec<String> {
    let at_s = (t - 30_000) / 1000;
    [
        // warm concrete name, instant, float only -- the bare stream
        "http_requests_total",
        // the same fetch reduced, so the aggregation output shape is compared
        "max by (status) (http_requests_total)",
        // NaN payloads and the stale marker in the stream
        "count by (g) (gauge_nan)",
        // the window-boundary series
        "edge_probe",
        // chunked resolution, 501 fingerprints over two chunks
        "chunked_total",
        // the same chunked fetch as one number, so a lost chunk shows as a count
        "count(chunked_total)",
        // multi-metric fan-out, whose dual-read member forces the merge path
        r#"{__name__=~"http_requests_total|http_request_errors_total",status="500"}"#,
        // range-selector window
        "sum by (status) (rate(http_requests_total[1m]))",
        // dual-read row stream
        "max(dual_probe)",
        // offset window
        "sum(http_requests_total offset 1m)",
        // range + offset
        "sum by (status) (rate(http_requests_total[1m] offset 30s))",
    ]
    .iter()
    .map(|q| (*q).to_string())
    .chain([
        // anchor window
        format!("max by (status) (http_requests_total @ {at_s})"),
        // range + anchor
        format!("sum by (status) (rate(http_requests_total[1m] @ {at_s}))"),
    ])
    .collect()
}

/// The fixture's size, asserted rather than stated in prose, so the
/// number in the plan cannot drift away from the number in the file.
/// Hermetic: it builds the fixture and counts it, and reaches no database.
#[test]
fn the_fixture_is_the_size_the_plan_states() {
    pulsus_testkit::require_live_gate(pulsus_testkit::CLICKHOUSE_GATE);
    let fx = fixture(0);
    let floats: usize = fx.iter().map(|s| s.samples.len()).sum();
    let hists: usize = fx.iter().map(|s| s.hist_samples.len()).sum();
    assert_eq!(fx.len(), 531, "series");
    assert_eq!(floats, 2_424, "float sample rows");
    assert_eq!(hists, 2, "histogram sample rows");
}

#[tokio::test]
async fn every_seeded_sample_arrives_with_its_exact_bits() {
    skip_unless_live!();
    let h = harness(&pulsus_testkit::test_db("pulsus_read_it_answers_samples")).await;
    let p = MetricQueryParams {
        start_ms: h.t - 60_000,
        end_ms: h.t,
        step_ms: 30_000,
    };
    let (t0, t1, t2) = (h.t - 60_000, h.t - 30_000, h.t);
    let got = h.read_path("http_requests_total", &p).await;
    let want = Answer::Matrix(vec![
        (
            lbl(&[
                ("instance", "a"),
                ("status", "200"),
                ("__name__", "http_requests_total"),
            ]),
            vec![
                (t0, Val::Float(1.0f64.to_bits())),
                (t1, Val::Float(2.0f64.to_bits())),
                (t2, Val::Float(3.0f64.to_bits())),
            ],
        ),
        (
            lbl(&[
                ("instance", "a"),
                ("status", "500"),
                ("__name__", "http_requests_total"),
            ]),
            vec![
                (t0, Val::Float(0.1f64.to_bits())),
                (t1, Val::Float(0.2f64.to_bits())),
                (t2, Val::Float(0.3f64.to_bits())),
            ],
        ),
        (
            lbl(&[
                ("instance", "b"),
                ("status", "200"),
                ("__name__", "http_requests_total"),
            ]),
            vec![
                (t0, Val::Float(10.0f64.to_bits())),
                (t1, Val::Float(20.0f64.to_bits())),
                (t2, Val::Float(30.5f64.to_bits())),
            ],
        ),
        (
            lbl(&[
                ("instance", "b"),
                ("status", "500"),
                ("__name__", "http_requests_total"),
            ]),
            vec![
                (t0, Val::Float((-0.0f64).to_bits())),
                (t1, Val::Float(0.0f64.to_bits())),
                (t2, Val::Float((-0.0f64).to_bits())),
            ],
        ),
    ]);
    assert_eq!(
        got, want,
        "every seeded sample must arrive with its exact bits"
    );
    drop_database(&h.bootstrap, &h.db).await;
}

#[tokio::test]
async fn the_aggregations_answer_their_fixture_values_bit_for_bit() {
    skip_unless_live!();
    let h = harness(&pulsus_testkit::test_db("pulsus_read_it_answers_agg")).await;
    let p = h.instant();
    assert_eq!(
        h.read_path("max by (status) (http_requests_total)", &p)
            .await,
        vec_answer(&[(&[("status", "200")], 30.5), (&[("status", "500")], 0.3)]),
        "max by (status)"
    );
    assert_eq!(
        h.read_path("min by (status) (http_requests_total)", &p)
            .await,
        vec_answer(&[(&[("status", "200")], 3.0), (&[("status", "500")], -0.0)]),
        "min by (status)"
    );
    assert_eq!(
        h.read_path("sum by (status) (http_requests_total)", &p)
            .await,
        vec_answer(&[(&[("status", "200")], 33.5), (&[("status", "500")], 0.3)]),
        "sum by (status)"
    );
    assert_eq!(
        h.read_path("avg by (status) (http_requests_total)", &p)
            .await,
        vec_answer(&[(&[("status", "200")], 16.75), (&[("status", "500")], 0.15)]),
        "avg by (status)"
    );
    assert_eq!(
        h.read_path("count by (status) (http_requests_total)", &p)
            .await,
        vec_answer(&[(&[("status", "200")], 2.0), (&[("status", "500")], 2.0)]),
        "count by (status)"
    );
    drop_database(&h.bootstrap, &h.db).await;
}

#[tokio::test]
async fn a_gap_longer_than_the_lookback_drops_the_series_for_exactly_its_grid_points() {
    skip_unless_live!();
    let h = harness(&pulsus_testkit::test_db("pulsus_read_it_answers_gap")).await;
    let start = h.t - HOUR_MS;
    let p = MetricQueryParams {
        start_ms: start,
        end_ms: h.t,
        step_ms: SCRAPE_MS,
    };
    let q = "count by (status) (scrape_gap)";
    let got = h.read_path(q, &p).await;

    // Every group is two series at every grid point, except `status="404"`,
    // which is one series for grid points 119..=139 and two elsewhere.
    let series = |status: &str| -> MatrixRow {
        let points = (0..=GRID_POINTS)
            .map(|j| {
                let missing = status == "404" && (GAP_FIRST_ABSENT..=GAP_LAST_ABSENT).contains(&j);
                (
                    start + j * SCRAPE_MS,
                    Val::Float(if missing { 1.0f64 } else { 2.0f64 }.to_bits()),
                )
            })
            .collect();
        (lbl(&[("status", status)]), points)
    };
    let want = Answer::Matrix(vec![
        series("200"),
        series("404"),
        series("500"),
        series("503"),
    ]);
    // WHICH grid points lose the series, not merely how many. The count
    // alone is not discriminating: shifting every sample by one
    // millisecond moves the absent run from 119..=139 to 120..=140 and
    // leaves the count at 21. The positions are what pin the boundary.
    let reduced_at: Vec<i64> = match &got {
        Answer::Matrix(m) => m
            .iter()
            .find(|(l, _)| l.iter().any(|(k, v)| k == "status" && v == "404"))
            .expect("a status=404 group")
            .1
            .iter()
            .filter(|(_, v)| *v == Val::Float(1.0f64.to_bits()))
            .map(|(t, _)| *t)
            .collect(),
        other => panic!("expected a matrix, got {other:?}"),
    };
    let expected_at: Vec<i64> = (GAP_FIRST_ABSENT..=GAP_LAST_ABSENT)
        .map(|j| start + j * SCRAPE_MS)
        .collect();
    assert_eq!(
        reduced_at, expected_at,
        "the gapped series must be missing at exactly grid points {GAP_FIRST_ABSENT}..={GAP_LAST_ABSENT}"
    );
    assert_eq!(
        reduced_at.len(),
        21,
        "exactly 21 of the {} grid points lose the gapped series",
        GRID_POINTS + 1
    );

    assert_eq!(
        got,
        want,
        "the gapped series must be absent for exactly grid points {GAP_FIRST_ABSENT}..={GAP_LAST_ABSENT} \
         ({} of {} points), and every other group must stay at 2 throughout",
        GAP_LAST_ABSENT - GAP_FIRST_ABSENT + 1,
        GRID_POINTS + 1
    );

    assert_eq!(got, h.memory(q, &p), "and the in-memory side must agree");
    drop_database(&h.bootstrap, &h.db).await;
}

/// The literal oracle for the two window-shaping modifiers. Both sides of
/// the differential plan the query, so a planning or evaluation defect in
/// `offset`/`@` moves them together and the differential cannot see it;
/// these answers are derived from the fixture, not from a run.
#[tokio::test]
async fn offset_and_anchor_answer_the_instant_they_name() {
    skip_unless_live!();
    let h = harness(&pulsus_testkit::test_db(
        "pulsus_read_it_answers_window_modifiers",
    ))
    .await;
    let p = h.instant();
    let at_s = (h.t - 30_000) / 1000;

    // FIRST, because these are the assertions that resolve to one second.
    // `anchor_probe` carries three samples one SECOND apart around the
    // instant both modifiers name, so an error of one second in either
    // direction changes the answer. The four assertions after them are
    // over the 30 s-spaced part of the fixture and resolve only to that.
    assert_eq!(
        h.read_path(&format!("max(anchor_probe @ {at_s})"), &p)
            .await,
        Answer::Vector(vec![(Vec::new(), Val::Float(102.0f64.to_bits()))]),
        "the anchor must select the sample AT t-30s (102), never the one a second earlier \
         (101) or a second later (103)"
    );
    assert_eq!(
        h.read_path("max(anchor_probe offset 30s)", &p).await,
        Answer::Vector(vec![(Vec::new(), Val::Float(102.0f64.to_bits()))]),
        "offset 30s must select the sample AT t-30s (102), never the one a second either side"
    );
    // The planner resolves `@` and `offset` in two places, and the two
    // above reach only the first: `plan_vector_selector`
    // (`crates/pulsus-promql/src/plan.rs:1699`, :1700) for an instant
    // selector, and `plan_matrix_selector_id` (:1729, :1731) for a range
    // one. Measured: a one-second defect at :1729 alone leaves every
    // other test in this suite green, because both sides of the
    // differential share the planner and a planner defect moves them
    // together. So the range-selector answers are written out by hand
    // here, exactly as the instant ones are.
    //
    // A range selector is left-open right-closed, so `[1s] @ t-30s`
    // spans `(t-31s, t-30s]` and holds one sample, 102. One second late
    // it spans `(t-30s, t-29s]` and holds 103; one second early,
    // `(t-32s, t-31s]` and 101.
    assert_eq!(
        h.read_path(&format!("sum_over_time(anchor_probe[1s] @ {at_s})"), &p)
            .await,
        Answer::Vector(vec![(lbl(&[("a", "x")]), Val::Float(102.0f64.to_bits()))]),
        "a RANGE selector anchored at t-30s spans (t-31s, t-30s], so its sum is 102 — \
         never 103 (a second late) or 101 (a second early)"
    );
    assert_eq!(
        h.read_path("sum_over_time(anchor_probe[1s] offset 30s)", &p)
            .await,
        Answer::Vector(vec![(lbl(&[("a", "x")]), Val::Float(102.0f64.to_bits()))]),
        "and a RANGE selector offset by 30s spans the same one-second window, so its sum \
         is 102"
    );
    // `offset 1m` evaluates at t-60s, where the four live series hold
    // 1, 10, 0.1 and -0.0.
    assert_eq!(
        h.read_path("max by (status) (http_requests_total offset 1m)", &p)
            .await,
        vec_answer(&[(&[("status", "200")], 10.0), (&[("status", "500")], 0.1)]),
        "offset 1m must answer from the samples at t-60s"
    );
    assert_eq!(
        h.read_path("min by (status) (http_requests_total offset 1m)", &p)
            .await,
        vec_answer(&[(&[("status", "200")], 1.0), (&[("status", "500")], -0.0)]),
        "and its minimum is the negative zero seeded at t-60s"
    );

    // `@ (t-30s)` evaluates at t-30s, where they hold 2, 20, 0.2 and 0.0.
    assert_eq!(
        h.read_path(
            &format!("max by (status) (http_requests_total @ {at_s})"),
            &p
        )
        .await,
        vec_answer(&[(&[("status", "200")], 20.0), (&[("status", "500")], 0.2)]),
        "the @ anchor must answer from the samples at t-30s, not at t"
    );
    assert_eq!(
        h.read_path(
            &format!("min by (status) (http_requests_total @ {at_s})"),
            &p
        )
        .await,
        vec_answer(&[(&[("status", "200")], 2.0), (&[("status", "500")], 0.0)]),
        "and its minimum is the positive zero seeded at t-30s"
    );

    drop_database(&h.bootstrap, &h.db).await;
}

#[tokio::test]
async fn every_live_series_is_counted_and_the_silent_one_is_not() {
    skip_unless_live!();
    let h = harness(&pulsus_testkit::test_db("pulsus_read_it_answers_lookback")).await;
    let p = h.instant();
    let got = h
        .read_path("count by (status) (http_requests_total)", &p)
        .await;
    assert_eq!(
        got,
        vec_answer(&[(&[("status", "200")], 2.0), (&[("status", "500")], 2.0)]),
        "two groups of two, and no status=404 group: its only sample is 10 minutes old"
    );
    drop_database(&h.bootstrap, &h.db).await;
}

#[tokio::test]
async fn every_nan_member_is_delivered_and_counted() {
    skip_unless_live!();
    let h = harness(&pulsus_testkit::test_db(
        "pulsus_read_it_answers_nan_delivered",
    ))
    .await;
    let p = h.instant();
    // The read path's share of NaN handling is delivery: a NaN-valued
    // series must arrive and be counted like any other. Whether an
    // aggregation then SKIPS it is the evaluator's rule, covered by the
    // correctness corpus; it is anchored below, not proved here.
    assert_eq!(
        h.read_path("count by (g) (gauge_nan)", &p).await,
        vec_answer(&[(&[("g", "one")], 3.0), (&[("g", "two")], 2.0)]),
        "the NaN-valued series are delivered and counted; g=three is stale and absent"
    );
    drop_database(&h.bootstrap, &h.db).await;
}

/// The shared-surface anchor: `min`/`max` skip a NaN member when the group
/// has a number. This is the evaluator's rule, asserted here as a literal
/// so a defect both sides would share cannot pass the differential. It
/// does NOT discriminate the read path -- any value corruption reddens it.
#[tokio::test]
async fn min_and_max_skip_a_nan_member_when_the_group_has_a_number() {
    skip_unless_live!();
    let h = harness(&pulsus_testkit::test_db("pulsus_read_it_answers_nan_skip")).await;
    let p = h.instant();
    let one = |a: &Answer| -> u64 {
        match a {
            Answer::Vector(v) => v
                .iter()
                .find(|(l, _)| l.iter().any(|(k, x)| k == "g" && x == "one"))
                .map(|(_, v)| match v {
                    Val::Float(b) => *b,
                    Val::Hist(h) => panic!("histogram: {h}"),
                })
                .unwrap_or_else(|| panic!("no g=one in {a:?}")),
            other => panic!("expected a vector, got {other:?}"),
        }
    };
    assert_eq!(
        one(&h.read_path("max by (g) (gauge_nan)", &p).await),
        3.0f64.to_bits(),
        "max over a NaN, 3, 1 group is 3"
    );
    assert_eq!(
        one(&h.read_path("min by (g) (gauge_nan)", &p).await),
        1.0f64.to_bits(),
        "min over a NaN, 3, 1 group is 1"
    );
    drop_database(&h.bootstrap, &h.db).await;
}

/// Issue #551: `max` and `min` over a group whose float members are all
/// NaN answer **that NaN**, bit for bit. Before the fix the accumulator's
/// identity element leaked out instead — `-Inf` for `max`, `+Inf` for
/// `min` — values no member carried, and an extremum must be one of the
/// members it ranges over.
///
/// The comparison is on the exact bits rather than `is_nan()`. The
/// fixture's members carry `NAN_BITS` and the answer carries that payload
/// through, so the bits are determined by the input. `is_nan()` would
/// also accept `STALE_NAN_BITS`, the reserved marker the lookback
/// selection removes before any aggregation sees it.
///
/// **The stale check runs FIRST, and that ordering is the point.** The
/// two constants differ, so an equality against `NAN_BITS` placed ahead
/// of it would always trip first and the inequality could never fail on
/// its own — a check that cannot fail is not a check. As written, an
/// answer carrying the reserved marker is reported as the reserved
/// marker rather than as an unexplained bit mismatch.
#[tokio::test]
async fn an_all_nan_group_answers_the_nan_its_members_carried() {
    skip_unless_live!();
    let h = harness(&pulsus_testkit::test_db("pulsus_read_it_answers_nan_all")).await;
    let p = h.instant();
    let two = |a: &Answer| -> u64 {
        match a {
            Answer::Vector(v) => v
                .iter()
                .find(|(l, _)| l.iter().any(|(k, x)| k == "g" && x == "two"))
                .map(|(_, v)| match v {
                    Val::Float(b) => *b,
                    Val::Hist(h) => panic!("histogram: {h}"),
                })
                .unwrap_or_else(|| panic!("no g=two in {a:?}")),
            other => panic!("expected a vector, got {other:?}"),
        }
    };
    let max = two(&h.read_path("max by (g) (gauge_nan)", &p).await);
    assert_ne!(
        max, STALE_NAN_BITS,
        "max over an all-NaN group answered the reserved stale marker (issue #551)"
    );
    assert_eq!(
        max, NAN_BITS,
        "max over an all-NaN group is the NaN its members carried (issue #551)"
    );
    let min = two(&h.read_path("min by (g) (gauge_nan)", &p).await);
    assert_ne!(
        min, STALE_NAN_BITS,
        "min over an all-NaN group answered the reserved stale marker (issue #551)"
    );
    assert_eq!(
        min, NAN_BITS,
        "min over an all-NaN group is the NaN its members carried (issue #551)"
    );
    drop_database(&h.bootstrap, &h.db).await;
}

#[tokio::test]
async fn the_stale_marker_removes_its_series_from_the_answer() {
    skip_unless_live!();
    let h = harness(&pulsus_testkit::test_db("pulsus_read_it_answers_stale")).await;
    let p = h.instant();
    let got = h.read_path("count by (g) (gauge_nan)", &p).await;
    assert_eq!(
        got,
        vec_answer(&[(&[("g", "one")], 3.0), (&[("g", "two")], 2.0)]),
        "g=three's most recent sample is the stale marker, so the group is absent"
    );
    drop_database(&h.bootstrap, &h.db).await;
}

#[tokio::test]
async fn the_fetch_window_includes_a_sample_on_its_upper_bound() {
    skip_unless_live!();
    let h = harness(&pulsus_testkit::test_db("pulsus_read_it_answers_upper")).await;
    let p = h.instant();
    let got = h.read_path(r#"edge_probe{edge="upper"}"#, &p).await;
    assert_eq!(
        got,
        Answer::Vector(vec![(
            lbl(&[("edge", "upper"), ("__name__", "edge_probe")]),
            Val::Float(22.0f64.to_bits())
        )]),
        "the sample at exactly the window's upper bound must win over the one 1 ms earlier"
    );
    drop_database(&h.bootstrap, &h.db).await;
}

#[tokio::test]
async fn a_sample_one_millisecond_inside_the_lower_bound_survives_the_fetch() {
    skip_unless_live!();
    let h = harness(&pulsus_testkit::test_db("pulsus_read_it_answers_lower")).await;
    let p = h.instant();
    let got = h.read_path("edge_probe", &p).await;
    assert_eq!(
        got,
        Answer::Vector(vec![
            (
                lbl(&[("edge", "upper"), ("__name__", "edge_probe")]),
                Val::Float(22.0f64.to_bits())
            ),
            (
                lbl(&[("edge", "lower_inside"), ("__name__", "edge_probe")]),
                Val::Float(44.0f64.to_bits())
            ),
        ]),
        "the sample at lower+1 ms must survive; the one exactly at lower must not appear"
    );
    drop_database(&h.bootstrap, &h.db).await;
}

#[tokio::test]
async fn the_chunked_fingerprint_set_returns_every_series_in_fingerprint_order() {
    skip_unless_live!();
    let h = harness(&pulsus_testkit::test_db("pulsus_read_it_answers_chunks")).await;
    let p = h.instant();
    let got = h.read_path("chunked_total", &p).await;
    let want = Answer::Vector(
        (0..501u64)
            .map(|i| {
                (
                    lbl(&[("shard", &format!("s{i}")), ("__name__", "chunked_total")]),
                    Val::Float((i as f64).to_bits()),
                )
            })
            .collect(),
    );
    assert_eq!(
        got, want,
        "501 fingerprints span two chunks; every series must come back, in fingerprint order"
    );
    drop_database(&h.bootstrap, &h.db).await;
}

#[tokio::test]
async fn the_dual_read_merge_preserves_every_float_sample() {
    skip_unless_live!();
    let h = harness(&pulsus_testkit::test_db("pulsus_read_it_answers_dual")).await;
    let p = h.instant();
    assert_eq!(
        h.read_path("max(dual_probe)", &p).await,
        Answer::Vector(vec![(Vec::new(), Val::Float(7.0f64.to_bits()))]),
        "the histogram member is ignored; the float maximum must survive the merge"
    );
    assert_eq!(
        h.read_path("min(dual_probe)", &p).await,
        Answer::Vector(vec![(Vec::new(), Val::Float(5.0f64.to_bits()))]),
        "the histogram member is ignored; the float minimum must survive the merge"
    );
    drop_database(&h.bootstrap, &h.db).await;
}

/// A selector that matches nothing — or matches series with nothing to
/// say — answers an empty vector, never an error. The read path arrives
/// at that answer by **six routes that this test covers**, which differ
/// by the selector's shape and by whether the label cache is warm:
///
/// ```text
///   the selector                          cache   how it ends up empty
///   ----------------------------------    -----   ---------------------
///   a concrete metric name whose          warm    the chunked fetch has
///   series set resolves empty                     no SQL to send,
///                                                 exec.rs:961
///
///   a name-less selector whose fan-out    warm    SelectorFetchPlan::
///   resolves no metric group                      Empty, built at
///                                                 exec.rs:871      \
///                                                                   >- both
///   a residual `__name__` matcher that    warm    ... built at      /   consumed
///   excludes the concrete name                    exec.rs:557     _/    at :954
///
///   a concrete metric name, of any of     cold    the sub-query
///   the shapes above or merely silent             fallback's zero-row
///   for longer than the lookback                  return, exec.rs:1018
///
///   a concrete metric name that DOES      warm    no early return at
///   resolve, whose fetch then returns             all: `group_rows`
///   no rows (silent past the lookback)            falls out of its loop
///                                                 over zero rows
/// ```
///
/// **How those six were found, and what that does not settle.** Five are
/// explicit early returns and were found by text search:
///
/// ```text
/// git grep -n 'Ok(Vec::new())\|Ok(vec!\[\])\|SelectorFetchPlan::Empty' \
///     -- crates/pulsus-read/src/metrics/
/// ```
///
/// which returns exactly those five, plus `exec.rs:1485` — the discovery
/// path's empty wide-set branch, which is not a selector's answer and
/// which a marker error leaves this whole suite green.
///
/// The sixth has no spelling to search for. It was found by marking all
/// five sites at once and asking which queries still answered empty:
/// `http_requests_total{status="404"}` on a warm cache did, because its
/// fingerprint resolves, its SQL is sent, no rows come back, and the
/// result is empty by falling out of a loop rather than by returning
/// early.
///
/// **So this is not a claim that six is all there are.** A text search
/// cannot enumerate fall-throughs, and the marking experiment ranges only
/// over the queries written below. What is established is that these six
/// exist, that each is covered, and that the queries do not stand in for
/// one another: a marker error at each of the five returns reddens this
/// test through its own query, and the sixth is the one query that
/// survives all five markers.
///
/// **A name-less selector is deliberately absent from the cold loop.**
/// `{status="418"}` against a cold cache is `NamelessSelectorUnresolvable
/// { reason: "ColdCache" }`, and that is correct: without the cache there
/// is no way to enumerate the metric names such a selector could match.
/// Asserting an empty vector there would be asserting a wrong answer.
#[tokio::test]
async fn an_unmatched_selector_is_an_empty_answer_not_an_error() {
    skip_unless_live!();
    let h = harness(&pulsus_testkit::test_db("pulsus_read_it_answers_empty")).await;
    let p = h.instant();
    // The three shapes a concrete-name selector can take, plus the two
    // name-channel shapes, against a WARM cache.
    for q in [
        // concrete name, no series carries that label value
        r#"http_requests_total{status="418"}"#,
        // a concrete name no series in the fixture carries
        "no_such_metric_at_all",
        // a self-contradicting matcher pair on one label
        r#"http_requests_total{instance="a",instance="b"}"#,
        // name-less: the fan-out resolves no metric group at all
        r#"{status="418"}"#,
        // a second `__name__` matcher excluding the first one's name
        r#"{__name__="gauge_nan",__name__="edge_probe"}"#,
        // the sixth route: this one RESOLVES. Its fingerprint reaches the
        // SQL, the fetch returns no rows because its only sample is ten
        // minutes old, and the empty answer comes from falling out of
        // `group_rows`'s loop — past every early return above.
        r#"http_requests_total{status="404"}"#,
    ] {
        assert_eq!(
            h.read_path(q, &p).await,
            Answer::Vector(Vec::new()),
            "warm: {q}"
        );
    }
    // The same concrete-name shapes against a COLD cache, which plans the
    // sub-query fallback instead and has its own zero-row return.
    for q in [
        r#"http_requests_total{status="418"}"#,
        "no_such_metric_at_all",
        r#"http_requests_total{instance="a",instance="b"}"#,
        // resolved, but silent for longer than the lookback: its
        // fingerprint IS in the sub-query's result and the sample fetch
        // still comes back with nothing
        r#"http_requests_total{status="404"}"#,
    ] {
        assert_eq!(
            h.read_path_cold(q, &p).await,
            Answer::Vector(Vec::new()),
            "cold: {q}"
        );
    }
    drop_database(&h.bootstrap, &h.db).await;
}

#[tokio::test]
async fn two_series_sharing_a_label_set_reach_the_evaluator_as_two_series() {
    skip_unless_live!();
    let h = harness(&pulsus_testkit::test_db("pulsus_read_it_answers_dup")).await;
    let p = h.instant();
    let expr = parse("dup_labels").expect("parse");
    let err = h.engine.query(&expr, &p).await.expect_err("must reject");
    assert_eq!(
        format!("{err:?}"),
        r#"Promql(LabelSet { detail: "vector cannot contain metrics with the same labelset" })"#,
        "the read path must not collapse two fingerprints that share a label set"
    );
    assert_eq!(
        h.read_path("count(dup_labels)", &p).await,
        Answer::Vector(vec![(Vec::new(), Val::Float(2.0f64.to_bits()))]),
        "and both must be counted"
    );
    drop_database(&h.bootstrap, &h.db).await;
}

#[tokio::test]
async fn the_multi_metric_fan_out_answers_each_name_with_its_own_values() {
    skip_unless_live!();
    let h = harness(&pulsus_testkit::test_db("pulsus_read_it_answers_fanout")).await;
    let p = h.instant();
    let got = h
        .read_path(
            r#"{__name__=~"http_requests_total|http_request_errors_total",status="500"}"#,
            &p,
        )
        .await;
    let want = Answer::Vector(vec![
        (
            lbl(&[
                ("instance", "a"),
                ("status", "500"),
                ("__name__", "http_request_errors_total"),
            ]),
            Val::Float(9.0f64.to_bits()),
        ),
        (
            lbl(&[
                ("instance", "c"),
                ("status", "500"),
                ("__name__", "http_request_errors_total"),
            ]),
            Val::Float(2.0f64.to_bits()),
        ),
        (
            lbl(&[
                ("instance", "a"),
                ("status", "500"),
                ("__name__", "http_requests_total"),
            ]),
            Val::Float(0.3f64.to_bits()),
        ),
        (
            lbl(&[
                ("instance", "b"),
                ("status", "500"),
                ("__name__", "http_requests_total"),
            ]),
            Val::Float((-0.0f64).to_bits()),
        ),
    ]);
    // The two halves are asserted separately so a break that moves values
    // does not report itself as a metric-name defect, and the other way
    // round. `instance="c"` is the fan-out's dual-read member: its series
    // carries a histogram sample and a later float one, so the whole
    // fan-out takes the merge path.
    let labels_of = |a: &Answer| match a {
        Answer::Vector(v) => v.iter().map(|(l, _)| l.clone()).collect::<Vec<_>>(),
        other => panic!("expected a vector, got {other:?}"),
    };
    let values_of = |a: &Answer| match a {
        Answer::Vector(v) => v.iter().map(|(_, x)| x.clone()).collect::<Vec<_>>(),
        other => panic!("expected a vector, got {other:?}"),
    };
    assert_eq!(
        labels_of(&got),
        labels_of(&want),
        "the fan-out must carry each series' own metric name, in (metric name, fingerprint) order"
    );
    assert_eq!(
        values_of(&got),
        values_of(&want),
        "the fan-out's values must survive the multi-metric merge"
    );
    drop_database(&h.bootstrap, &h.db).await;
}

#[tokio::test]
async fn the_cold_cache_fallback_answers_exactly_what_the_warm_cache_does() {
    skip_unless_live!();
    let h = harness(&pulsus_testkit::test_db("pulsus_read_it_answers_cold")).await;
    let p = h.instant();
    let want = vec_answer(&[(&[("status", "200")], 30.5), (&[("status", "500")], 0.3)]);
    let warm = h
        .read_path("max by (status) (http_requests_total)", &p)
        .await;
    let cold = h
        .read_path_cold("max by (status) (http_requests_total)", &p)
        .await;
    assert_eq!(warm, want, "warm cache");
    assert_eq!(
        cold, want,
        "cold cache (the sub-query fetch shape) must answer identically"
    );
    drop_database(&h.bootstrap, &h.db).await;
}

#[tokio::test]
async fn every_query_answers_the_same_through_the_read_path_and_in_memory() {
    skip_unless_live!();
    let h = harness(&pulsus_testkit::test_db(
        "pulsus_read_it_answers_differential",
    ))
    .await;
    let instant = h.instant();
    let range = MetricQueryParams {
        start_ms: h.t - 60_000,
        end_ms: h.t,
        step_ms: 30_000,
    };
    let mut disagreements = Vec::new();
    let queries = differential_queries(h.t);
    for q in &queries {
        for (kind, p) in [("instant", &instant), ("range", &range)] {
            let a = h.read_path(q, p).await;
            let b = h.memory(q, p);
            if a != b {
                disagreements.push(format!(
                    "{q} ({kind}):\n  read path: {a:?}\n  in memory: {b:?}"
                ));
            }
        }
    }
    assert!(
        disagreements.is_empty(),
        "{} of {} query/shape pairs disagree:\n{}",
        disagreements.len(),
        queries.len() * 2,
        disagreements.join("\n")
    );
    drop_database(&h.bootstrap, &h.db).await;
}
