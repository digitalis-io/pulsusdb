//! Issue #549: the grouped instant read, against a live ClickHouse.
//!
//! # The two sides
//!
//! One fixture, one shared `Arc<LabelCache>`, two `MetricsEngine`s over
//! two clients — one with `grouped_push` on, one off — and the same query
//! asked of both:
//!
//! ```text
//!   the fixture: series identities and samples, written as literals below
//!                                |
//!                 INSERT metric_series / metric_samples / metric_hist_samples
//!                                |
//!             +------------------+------------------+
//!             |                                     |
//!   grouped_push: true                    grouped_push: false
//!   one statement per chunk               two statements per chunk
//!   the answer arrives REDUCED            every sample arrives
//!   metrics::grouped::fold                pulsus_promql::evaluate
//!             |                                     |
//!             +--------> compared as f64::to_bits <-+
//!                        and as annotation text
//! ```
//!
//! Side B is not an independent oracle for PromQL — it is the route that
//! ships today, which is the whole claim: the push must answer what this
//! engine already answers, bit for bit, including NaN payloads.
//!
//! Two engines are required rather than one reconfigured, because
//! `MetricsEngine::new` takes `ChClient` by value and `ChClient` is not
//! `Clone`. They share ONE `Arc<LabelCache>`, so both resolve the same
//! series from the same snapshot and a disagreement cannot come from
//! resolution.
//!
//! # What each test carries
//!
//! | test | criterion |
//! |---|---|
//! | `the_twelve_query_and_grouping_combinations_agree_bit_for_bit` | 1, and the instrument for 6 |
//! | `the_nan_cases_answer_the_members_own_payloads` | 3 |
//! | `pushed_rows_never_exceed_twice_the_raw_rows` | 7 |
//! | `the_charge_is_*` | 10 |
//!
//! # Criterion 6's break
//!
//! The corpus carries a group whose members span the 500-fingerprint chunk
//! boundary, exercised under `max` with the push on. Folding `flags`
//! additively instead of bitwise turns `1 | 1 = 1` into `1 + 1 = 2`, which
//! reads as "histogram only" and drops the group — and a missing series is
//! what the differential compares.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`. To run it:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=$PORT_CH \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-read --test live_metrics_grouped
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, ChRow, Idempotency, QuerySettings, Row};
use pulsus_model::{DEFAULT_ACTIVITY_BUCKET_MS, Fingerprint, FpLiteral, STALE_NAN_BITS};
use pulsus_promql::DEFAULT_LOOKBACK_MS;
use pulsus_promql::parser::parse;
use pulsus_read::metrics::grouped::{Grid, GroupedOp};
use pulsus_read::metrics::{grouped_sql, sample_sql};
use pulsus_read::{
    HistOrFloat, LabelCache, LabelCacheConfig, MetricQueryParams, MetricsConfig, MetricsEngine,
    QueryResult,
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
        query_timeout: Duration::from_secs(120),
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

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CountRow {
    n: u64,
}

/// One `system.query_log` row of a statement this suite ran under a
/// `query_id` it composed itself.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SentBytesRow {
    query_id: String,
    sent: u64,
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

fn engine_config(db: &str, grouped_push: bool) -> MetricsConfig {
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
        grouped_push,
    }
}

// --------------------------------------------------------------- fixture

/// One fixture series: its identity and its samples, as literals.
#[derive(Debug, Clone)]
struct Series {
    fp: u64,
    metric: String,
    labels: Vec<(String, String)>,
    /// `(unix_milli, value bits)` — bits so a NaN payload is a literal.
    samples: Vec<(i64, u64)>,
    /// Timestamps carrying the one fixed histogram shape.
    hist_samples: Vec<i64>,
}

fn lbl(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// A distinct quiet NaN per fingerprint, so "which member's payload
/// survived" is answerable from the bits rather than from "it is a NaN".
/// Never [`STALE_NAN_BITS`], which is a different marker with a different
/// meaning on this path.
fn nan_bits(fp: u64) -> u64 {
    let bits = 0x7FF8_0000_0000_0000u64 | fp;
    assert_ne!(bits, STALE_NAN_BITS);
    bits
}

async fn seed(client: &ChClient, fx: &[Series], bucket: i64) {
    let series: Vec<SeedSeriesRow> = fx
        .iter()
        .map(|s| {
            let map: BTreeMap<&str, &str> = s
                .labels
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            SeedSeriesRow {
                metric_name: s.metric.clone(),
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
                metric_name: s.metric.clone(),
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
    for block in samples.chunks(50_000) {
        client
            .insert_block("metric_samples", block)
            .await
            .expect("seed metric_samples");
    }
    let cols = hist_columns();
    let hist: Vec<SeedHistRow> = fx
        .iter()
        .flat_map(|s| {
            let cols = cols.clone();
            let metric = s.metric.clone();
            s.hist_samples.iter().map(move |t| SeedHistRow {
                metric_name: metric.clone(),
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

// -------------------------------------------------- the comparable answer

/// A result series' labels and its points, values as `f64::to_bits` so
/// NaN compares equal to NaN and `-0.0` never equals `0.0`.
type Answer = Vec<(Vec<(String, String)>, Vec<(i64, u64)>)>;

fn answer_of(r: QueryResult) -> Answer {
    let mut out: Answer = match r {
        QueryResult::Vector(v) => v
            .into_iter()
            .map(|s| (s.labels, vec![(0i64, s.value.to_bits())]))
            .collect(),
        QueryResult::Matrix(m) => m
            .into_iter()
            .map(|s| {
                (
                    s.labels,
                    s.points
                        .into_iter()
                        .map(|(t, v)| (t, v.to_bits()))
                        .collect(),
                )
            })
            .collect(),
        QueryResult::VectorHist(v) => v
            .into_iter()
            .map(|s| {
                let bits = match s.value {
                    HistOrFloat::Float(f) => f.to_bits(),
                    HistOrFloat::Hist(_) => panic!("no query here answers a histogram"),
                };
                (s.labels, vec![(0i64, bits)])
            })
            .collect(),
        other => panic!("unexpected result shape: {other:?}"),
    };
    for (labels, _) in &mut out {
        labels.sort();
    }
    out.sort();
    out
}

// ----------------------------------------------------------- the harness

struct Harness {
    bootstrap: ChClient,
    admin: ChClient,
    db: String,
    /// The one snapshot both engines resolve against — and the one every
    /// extra engine a charge pair builds resolves against too.
    cache: Arc<LabelCache>,
    pushed: MetricsEngine,
    unpushed: MetricsEngine,
    t: i64,
}

/// `db` is already composed by `pulsus_testkit::test_db` at the call site
/// — the naming guard requires the reserved name to sit in the helper's
/// own argument list.
async fn harness(db: &str, fx: &[Series]) -> Harness {
    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = db.to_string();
    init_db(&bootstrap, &db).await;
    let client = ChClient::new(test_config(&db)).await.expect("connect");
    let now = now_ms();
    let t = (now / 60_000) * 60_000;
    let bucket = (now / DEFAULT_ACTIVITY_BUCKET_MS) * DEFAULT_ACTIVITY_BUCKET_MS;
    seed(&client, fx, bucket).await;

    // ONE cache, shared by both engines: a disagreement below cannot come
    // from two different resolutions of the same selector.
    let cache = Arc::new(LabelCache::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache_config(&db),
    ));
    cache.refresh().await.expect("refresh");
    assert!(cache.is_warm());
    let pushed = MetricsEngine::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        Arc::clone(&cache),
        engine_config(&db, true),
    );
    let unpushed = MetricsEngine::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        Arc::clone(&cache),
        engine_config(&db, false),
    );
    let admin = ChClient::new(test_config(&db)).await.expect("connect");
    Harness {
        bootstrap,
        admin,
        db,
        cache,
        pushed,
        unpushed,
        t,
    }
}

impl Harness {
    /// The range this suite's differential asks over: eleven grid points
    /// at a one-minute step, ending at the fixture anchor.
    fn range(&self) -> MetricQueryParams {
        MetricQueryParams {
            start_ms: self.t - 600_000,
            end_ms: self.t,
            step_ms: 60_000,
        }
    }

    fn instant(&self) -> MetricQueryParams {
        MetricQueryParams {
            start_ms: self.t,
            end_ms: self.t,
            step_ms: 0,
        }
    }

    /// Both routes' answers and annotations for one query.
    async fn both(
        &self,
        query: &str,
        p: &MetricQueryParams,
    ) -> ((Answer, Vec<String>), (Answer, Vec<String>)) {
        let expr = parse(query).expect("parse");
        let (a, ann_a) = self
            .pushed
            .query(&expr, p)
            .await
            .unwrap_or_else(|e| panic!("{query} (pushed): {e:?}"));
        let (b, ann_b) = self
            .unpushed
            .query(&expr, p)
            .await
            .unwrap_or_else(|e| panic!("{query} (unpushed): {e:?}"));
        let infos = |x: pulsus_promql::Annotations| {
            let (mut w, mut i) = x.base_messages();
            w.append(&mut i);
            w.sort();
            w
        };
        ((answer_of(a), infos(ann_a)), (answer_of(b), infos(ann_b)))
    }

    async fn agree(&self, query: &str, p: &MetricQueryParams) -> Answer {
        let ((pa, pann), (ua, uann)) = self.both(query, p).await;
        assert_eq!(
            pa, ua,
            "{query}: the pushed answer differs from the shipped route's"
        );
        assert_eq!(pann, uann, "{query}: the annotations differ");
        pa
    }

    async fn count(&self, sql: &str) -> u64 {
        let mut stream = self
            .admin
            .query_stream::<CountRow>(sql, &QuerySettings::new())
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        stream.next().await.expect("a row").expect("decode").n
    }

    /// Does `sql` drain fully under a `max_memory_usage` of `ceiling`?
    ///
    /// The settings are the engine's own
    /// (`metrics::exec::metrics_read_settings`), reproduced here because
    /// that function is private: the ceiling, no external group-by
    /// spilling, and the pinned block size.
    ///
    /// **Which of the three is load-bearing here was measured, not
    /// assumed.** Raising `max_bytes_before_external_group_by` to allow a
    /// spill leaves every row of the sweep unchanged, so the group-by is
    /// not what breaches the ceiling on this corpus — the window
    /// functions and the coverage expansion are. Dropping
    /// `max_memory_usage` reddens the sweep at its first row. The spill
    /// setting stays because it mirrors what the engine sends, not
    /// because it is what refuses.
    ///
    /// `true` means every row arrived; `false` means the server refused,
    /// either at dispatch or mid-stream.
    async fn drains_under_ceiling(&self, sql: &str, ceiling: u64) -> bool {
        let settings = QuerySettings::new()
            .set("max_memory_usage", ceiling)
            .set("max_bytes_before_external_group_by", 0u64)
            .set("max_block_size", 65_409u64);
        let mut stream = match self
            .admin
            .query_stream::<pulsus_read::metrics::grouped_rows::GroupedRunRow>(sql, &settings)
            .await
        {
            Ok(s) => s,
            Err(_) => return false,
        };
        while let Some(row) = stream.next().await {
            if row.is_err() {
                return false;
            }
        }
        true
    }

    /// Runs `sql` once under `query_id`, draining it, and discards the
    /// rows — the point is what the server sent, not what it said.
    async fn run_tagged<R: ChRow>(&self, sql: &str, query_id: &str) {
        let settings = QuerySettings::new()
            .set("query_id", query_id)
            .set("max_block_size", 65_409u64)
            .set("max_bytes_before_external_group_by", 0u64);
        let mut stream = self
            .admin
            .query_stream::<R>(sql, &settings)
            .await
            .unwrap_or_else(|e| panic!("{query_id}: {e:?}"));
        while let Some(row) = stream.next().await {
            row.expect("decode");
        }
    }

    /// `ProfileEvents['NetworkSendBytes']` for every `query_id` beginning
    /// with `prefix`, read once the count has stopped moving.
    ///
    /// The `query_id`s are composed through `pulsus_testkit::test_ident`,
    /// so two checkouts sharing one server cannot read each other's rows.
    async fn sent_bytes(&self, prefix: &str, expect: usize) -> BTreeMap<String, u64> {
        let sql = format!(
            "SELECT query_id, toUInt64(ProfileEvents['NetworkSendBytes']) AS sent \
             FROM system.query_log \
             WHERE type = 'QueryFinish' AND startsWith(query_id, '{prefix}')"
        );
        let mut previous = 0usize;
        for poll in 1..=30u32 {
            self.admin
                .execute(
                    "SYSTEM FLUSH LOGS",
                    &QuerySettings::new(),
                    Idempotency::Idempotent,
                )
                .await
                .expect("flush logs");
            let mut stream = self
                .admin
                .query_stream::<SentBytesRow>(&sql, &QuerySettings::new())
                .await
                .expect("read the query log");
            let mut rows: Vec<SentBytesRow> = Vec::new();
            while let Some(r) = stream.next().await {
                rows.push(r.expect("decode"));
            }
            if rows.len() >= expect && poll >= 2 && previous == rows.len() {
                let mut by_id: BTreeMap<String, Vec<u64>> = BTreeMap::new();
                for r in rows {
                    by_id.entry(r.query_id).or_default().push(r.sent);
                }
                return by_id
                    .into_iter()
                    .map(|(k, mut v)| {
                        v.sort_unstable();
                        (k, v[v.len() / 2])
                    })
                    .collect();
            }
            previous = rows.len();
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        panic!("the query log never settled on {expect} rows for {prefix}");
    }

    async fn finish(self) {
        drop_database(&self.bootstrap, &self.db).await;
    }
}

// ------------------------------------------------------ the wide corpus

/// The differential corpus: 606 series over one metric, so every group
/// spans the 500-fingerprint chunk boundary.
///
/// ```text
///   fp            labels                            samples
///   1..=606       status, zone, instance, handler
///   (most)        status = ["200","404","500","503"][fp % 4]
///                 zone   = ["a","b"][fp % 2]         11 floats, 60 s apart
///   fp % 37 == 0                                     only the first 3, so
///                                                    the lookback expires
///   495..=505     status = "allnan"                  11 NaNs, a DISTINCT
///                                                    payload per fingerprint
///   601..=604     status = "hist"                    no float, 11 histograms
///   605..=606     status = "mixed"                   floats AND histograms
/// ```
///
/// `allnan` is the group that spans the boundary as an extremum group; it
/// and every other group also span it, which is what makes the flags fold
/// observable at all.
const WIDE_METRIC: &str = "grouped_wide";
const WIDE_POINTS: i64 = 10;

fn wide_corpus(t: i64) -> Vec<Series> {
    let start = t - 600_000;
    let mut out = Vec::new();
    for fp in 1..=606u64 {
        let (status, samples, hist): (String, Vec<(i64, u64)>, Vec<i64>) =
            if (495..=505).contains(&fp) {
                (
                    "allnan".to_string(),
                    (0..=WIDE_POINTS)
                        .map(|k| (start + k * 60_000, nan_bits(fp)))
                        .collect(),
                    Vec::new(),
                )
            } else if (601..=604).contains(&fp) {
                (
                    "hist".to_string(),
                    Vec::new(),
                    (0..=WIDE_POINTS).map(|k| start + k * 60_000).collect(),
                )
            } else if (605..=606).contains(&fp) {
                (
                    "mixed".to_string(),
                    (0..=WIDE_POINTS)
                        .map(|k| (start + k * 60_000, (fp as f64 + k as f64 * 0.5).to_bits()))
                        .collect(),
                    // 30 s off the grid, so the float is never shadowed at the
                    // same millisecond and both channels reach the group.
                    (0..WIDE_POINTS)
                        .map(|k| start + k * 60_000 + 30_000)
                        .collect(),
                )
            } else {
                let last = if fp % 37 == 0 { 2 } else { WIDE_POINTS };
                (
                    ["200", "404", "500", "503"][(fp % 4) as usize].to_string(),
                    (0..=last)
                        .map(|k| (start + k * 60_000, (fp as f64 + k as f64 * 0.5).to_bits()))
                        .collect(),
                    Vec::new(),
                )
            };
        out.push(Series {
            fp,
            metric: WIDE_METRIC.to_string(),
            labels: lbl(&[
                ("status", &status),
                ("zone", ["a", "b"][(fp % 2) as usize]),
                ("instance", &format!("i{}", fp % 7)),
                ("handler", &format!("h{}", fp % 3)),
            ]),
            samples,
            hist_samples: hist,
        });
    }
    out
}

/// The twelve combinations: four aggregations over three grouping forms.
fn twelve_queries() -> Vec<String> {
    let mut out = Vec::new();
    for op in ["max", "min", "count", "group"] {
        out.push(format!("{op} by (status) ({WIDE_METRIC})"));
        out.push(format!("{op} without (instance, handler) ({WIDE_METRIC})"));
        out.push(format!("{op}({WIDE_METRIC})"));
    }
    out
}

/// Criterion 1, and the instrument criterion 6 names.
///
/// Every one of the twelve is asked of both engines over the same shared
/// cache, as a range query and again as an instant query, and compared as
/// raw bit patterns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_twelve_query_and_grouping_combinations_agree_bit_for_bit() {
    skip_unless_live!();
    let t = (now_ms() / 60_000) * 60_000;
    let h = harness(
        &pulsus_testkit::test_db("pulsus_read_it_grouped_differential"),
        &wide_corpus(t),
    )
    .await;

    let range = h.range();
    let instant = h.instant();
    let mut total_points = 0usize;
    for q in twelve_queries() {
        let a = h.agree(&q, &range).await;
        assert!(!a.is_empty(), "{q}: the corpus answers nothing");
        total_points += a.iter().map(|(_, pts)| pts.len()).sum::<usize>();
        h.agree(&q, &instant).await;
    }
    eprintln!("[549] twelve combinations, {total_points} matrix points compared");

    // The group that spans the chunk boundary, named rather than left
    // implicit: its members sit either side of 500 and it is a float-only
    // group under `max`, which is what makes the flags fold observable.
    let spanning: Vec<u64> = (495..=505).collect();
    assert!(
        spanning.iter().any(|fp| *fp <= 500) && spanning.iter().any(|fp| *fp > 500),
        "the all-NaN group must span the chunk boundary"
    );
    assert!(
        606 > sample_sql::CHUNK_THRESHOLD as u64,
        "the corpus must span more than one chunk"
    );
    h.finish().await;
}

/// Criterion 3: the NaN cases, over a corpus small enough to state every
/// expected bit pattern.
///
/// ```text
///   group   members                         max        min
///   one     NaN(10), 3, 1                   3          1
///   two     NaN(13), NaN(14)                NaN(14)    NaN(14)
///   three   1, 2, 3                         3          1
///   four    one histogram member only       dropped    dropped
/// ```
///
/// `two` answers the payload its LAST member in fold order carried — the
/// highest fingerprint — not a manufactured NaN.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_nan_cases_answer_the_members_own_payloads() {
    skip_unless_live!();
    let t = (now_ms() / 60_000) * 60_000;
    let metric = "grouped_nan";
    let mut fx = Vec::new();
    let mut add = |fp: u64, g: &str, bits: Option<u64>| {
        fx.push(Series {
            fp,
            metric: metric.to_string(),
            labels: lbl(&[("g", g)]),
            samples: bits.map(|b| vec![(t, b)]).unwrap_or_default(),
            hist_samples: if bits.is_none() { vec![t] } else { Vec::new() },
        });
    };
    add(10, "one", Some(nan_bits(10)));
    add(11, "one", Some(3.0f64.to_bits()));
    add(12, "one", Some(1.0f64.to_bits()));
    add(13, "two", Some(nan_bits(13)));
    add(14, "two", Some(nan_bits(14)));
    add(15, "three", Some(1.0f64.to_bits()));
    add(16, "three", Some(2.0f64.to_bits()));
    add(17, "three", Some(3.0f64.to_bits()));
    add(18, "four", None);
    add(19, "four", None);
    // Criterion 3's MIXED groups (review round 1: these passed only as a
    // throwaway measurement and were not in the suite). `five` is the
    // case the flags fold exists for — one float member and one
    // histogram member in ONE group — and `six` is a group with a single
    // histogram member and nothing else.
    add(20, "five", Some(42.0f64.to_bits()));
    add(21, "five", None);
    add(22, "six", None);

    let h = harness(&pulsus_testkit::test_db("pulsus_read_it_grouped_nan"), &fx).await;
    let p = h.instant();

    let expect = |rows: &[(&str, u64)]| -> Answer {
        let mut a: Answer = rows
            .iter()
            .map(|(g, bits)| {
                (
                    vec![("g".to_string(), (*g).to_string())],
                    vec![(0i64, *bits)],
                )
            })
            .collect();
        a.sort();
        a
    };

    let max = h.agree(&format!("max by (g) ({metric})"), &p).await;
    assert_eq!(
        max,
        expect(&[
            ("one", 3.0f64.to_bits()),
            ("two", nan_bits(14)),
            ("three", 3.0f64.to_bits()),
            ("five", 42.0f64.to_bits()),
        ]),
        "max: `four` and `six` have no float member and are dropped; `five` \
         answers its ONE float member across a histogram member; `two` answers \
         its highest fingerprint's NaN payload"
    );

    let min = h.agree(&format!("min by (g) ({metric})"), &p).await;
    assert_eq!(
        min,
        expect(&[
            ("one", 1.0f64.to_bits()),
            ("two", nan_bits(14)),
            ("three", 1.0f64.to_bits()),
            ("five", 42.0f64.to_bits()),
        ]),
        "min: the NaN payload rule selects the LAST member in fold order for \
         min too, never the smallest; `five` answers its one float either way"
    );

    // `count` counts a histogram member rather than ignoring it, so
    // `four` is present with 2.
    let count = h.agree(&format!("count by (g) ({metric})"), &p).await;
    assert_eq!(
        count,
        expect(&[
            ("one", 3.0f64.to_bits()),
            ("two", 2.0f64.to_bits()),
            ("three", 3.0f64.to_bits()),
            ("four", 2.0f64.to_bits()),
            ("five", 2.0f64.to_bits()),
            ("six", 1.0f64.to_bits()),
        ]),
        "count counts a histogram member: the mixed group is 2 and the \
         histogram-only single-member group is 1"
    );

    let group = h.agree(&format!("group by (g) ({metric})"), &p).await;
    assert_eq!(
        group,
        expect(&[
            ("one", 1.0f64.to_bits()),
            ("two", 1.0f64.to_bits()),
            ("three", 1.0f64.to_bits()),
            ("four", 1.0f64.to_bits()),
            ("five", 1.0f64.to_bits()),
            ("six", 1.0f64.to_bits()),
        ]),
        "group is 1 for every group that has a member at all, whatever the \
         member's channel"
    );
    h.finish().await;
}

/// Criterion 2's gap, as runs: a series silent for longer than the
/// lookback leaves its group with no member, and the group's series is
/// absent for exactly those grid points on both routes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gap_longer_than_the_lookback_drops_the_group_on_both_routes() {
    skip_unless_live!();
    let t = (now_ms() / 60_000) * 60_000;
    let metric = "grouped_gap";
    let start = t - 3_600_000;
    // 241 grid points at 15 s. Two series in one group, both missing the
    // same 40-sample run (10 minutes, longer than the 5-minute lookback),
    // so the GROUP is absent for grid points 119..=139 — 21 of 241.
    let missing = 100..=139i64;
    let mut fx: Vec<Series> = [60u64, 61]
        .iter()
        .map(|fp| Series {
            fp: *fp,
            metric: metric.to_string(),
            labels: lbl(&[("status", "404"), ("replica", &format!("r{fp}"))]),
            samples: (0..=240i64)
                .filter(|i| !missing.contains(i))
                .map(|i| (start + i * 15_000, (i as f64).to_bits()))
                .collect(),
            hist_samples: Vec::new(),
        })
        .collect();
    // A second metric where only ONE of the two members is silent
    // (review round 1: criterion 2's count sequence was measured but not
    // committed). The group is never empty, so `max` answers at every
    // grid point while `count` drops to one over the same window — which
    // is what shows coverage is tracked per MEMBER and not per group.
    let partial = "grouped_gap_partial";
    fx.extend([70u64, 71].iter().map(|fp| {
        Series {
            fp: *fp,
            metric: partial.to_string(),
            labels: lbl(&[("status", "404"), ("replica", &format!("r{fp}"))]),
            samples: (0..=240i64)
                .filter(|i| *fp == 71 || !missing.contains(i))
                .map(|i| (start + i * 15_000, (i as f64).to_bits()))
                .collect(),
            hist_samples: Vec::new(),
        }
    }));
    let h = harness(&pulsus_testkit::test_db("pulsus_read_it_grouped_gap"), &fx).await;
    let p = MetricQueryParams {
        start_ms: start,
        end_ms: start + 240 * 15_000,
        step_ms: 15_000,
    };
    let a = h.agree(&format!("max by (status) ({metric})"), &p).await;
    assert_eq!(a.len(), 1, "one group");
    let present: Vec<i64> = a[0].1.iter().map(|(ts, _)| (ts - start) / 15_000).collect();
    let absent: Vec<i64> = (0..=240).filter(|i| !present.contains(i)).collect();
    assert_eq!(
        absent,
        (119..=139).collect::<Vec<i64>>(),
        "exactly 21 of the 241 grid points"
    );

    // The partial gap, as a count sequence. One member is silent for the
    // same window; the other is not.
    //
    // ```text
    //   grid point   0..=118   119..=139   140..=240
    //   count              2           1           2
    //   max          present     present     present
    // ```
    let c = h.agree(&format!("count by (status) ({partial})"), &p).await;
    assert_eq!(c.len(), 1, "one group");
    let counts: Vec<(i64, f64)> = c[0]
        .1
        .iter()
        .map(|(ts, bits)| ((ts - start) / 15_000, f64::from_bits(*bits)))
        .collect();
    assert_eq!(
        counts.len(),
        241,
        "the group is never empty, so every point"
    );
    for (i, n) in &counts {
        let want = if (119..=139).contains(i) { 1.0 } else { 2.0 };
        assert_eq!(
            *n, want,
            "grid point {i}: one member is silent over 119..=139 and the other is not"
        );
    }
    let m = h.agree(&format!("max by (status) ({partial})"), &p).await;
    assert_eq!(
        m[0].1.len(),
        241,
        "max answers at every point, because the group always has a member"
    );
    h.finish().await;
}

// ---------------------------------------------- criterion 7: the row bound

/// One corpus for the row-bound check: its series, and the grid the
/// statement reduces onto.
struct RowCase {
    name: &'static str,
    metric: &'static str,
    series: Vec<Series>,
    grid: Grid,
    /// The fetch window the engine would use for this grid.
    lower_excl_ms: i64,
    upper_incl_ms: i64,
}

fn row_cases(t: i64) -> Vec<RowCase> {
    let mut cases = Vec::new();

    // Aligned: 400 series in 4 groups, 241 samples each at a 15 s scrape,
    // read at a 15 s step.
    {
        let start = t - 3_600_000;
        let metric = "rows_aligned";
        let series = (0..400u64)
            .map(|i| Series {
                fp: i + 1,
                metric: metric.to_string(),
                labels: lbl(&[("status", ["200", "404", "500", "503"][(i % 4) as usize])]),
                samples: (0..=240i64)
                    .map(|k| (start + k * 15_000, (i as f64 + k as f64 * 0.5).to_bits()))
                    .collect(),
                hist_samples: Vec::new(),
            })
            .collect();
        cases.push(RowCase {
            name: "aligned, 400 series / 4 groups / 15 s step",
            metric,
            series,
            grid: Grid {
                start_ms: start,
                step_ms: 15_000,
                points: 241,
                lookback_ms: DEFAULT_LOOKBACK_MS,
            },
            lower_excl_ms: start - DEFAULT_LOOKBACK_MS,
            upper_incl_ms: start + 240 * 15_000,
        });
    }

    // Cyclic: four members per group, each scraping every 15 s but
    // STAGGERED 3.75 s apart, each arrival larger than the last — so the
    // group's answer changes at every grid point of a 3.75 s step and no
    // two neighbouring points collapse into one run. The adversarial end.
    {
        let start = t - 900_000;
        let metric = "rows_cyclic";
        let series = (0..16u64)
            .map(|i| {
                let slot = i % 4;
                Series {
                    fp: i + 1,
                    metric: metric.to_string(),
                    labels: lbl(&[("status", ["200", "404", "500", "503"][(i / 4) as usize])]),
                    samples: (0..60i64)
                        .map(|k| {
                            (
                                start + slot as i64 * 3_750 + k * 15_000,
                                ((k * 4 + slot as i64) as f64).to_bits(),
                            )
                        })
                        .collect(),
                    hist_samples: Vec::new(),
                }
            })
            .collect();
        cases.push(RowCase {
            name: "cyclic, four members per group, 3.75 s step",
            metric,
            series,
            grid: Grid {
                start_ms: start,
                step_ms: 3_750,
                points: 241,
                lookback_ms: DEFAULT_LOOKBACK_MS,
            },
            lower_excl_ms: start - DEFAULT_LOOKBACK_MS,
            upper_incl_ms: start + 240 * 3_750,
        });
    }

    // Expiry: 100 one-sample series in ONE group, arrivals evenly spaced
    // 15 s apart under a 300 s lookback.
    //
    // **The layout is stated because the ratio belongs to the layout, not
    // to the series and group counts** (review round 1). Under `count`
    // this corpus returns 39 rows: coverage intervals overlap twenty
    // deep, so the count climbs 1..19, sits at 20, and falls 19..1.
    // Issue #549's plan records 199 rows for "100 one-sample series in
    // one group" — a valid figure for a DIFFERENT layout of the same
    // counts, with ten arrivals in each 20-point block on alternating
    // parity, which changes the count at nearly every grid point:
    //
    // ```text
    //   layout                          count rows   pushed : raw
    //   evenly spaced 15 s apart (here)         39      0.390
    //   alternating-parity blocks              199      1.990
    // ```
    //
    // Both satisfy `pushed_rows <= 2 * raw_rows`, which is what criterion
    // 7 asserts and what neither layout can breach.
    {
        let start = t - 300_000;
        let metric = "rows_expiry";
        let series = (0..100u64)
            .map(|i| Series {
                fp: i + 1,
                metric: metric.to_string(),
                labels: lbl(&[("status", "200")]),
                samples: vec![(start + (i as i64) * 15_000, ((i + 1) as f64).to_bits())],
                hist_samples: Vec::new(),
            })
            .collect();
        cases.push(RowCase {
            name: "expiry, 100 one-sample series in one group",
            metric,
            series,
            grid: Grid {
                start_ms: start,
                step_ms: 15_000,
                points: 241,
                lookback_ms: 300_000,
            },
            lower_excl_ms: start - 300_000,
            upper_incl_ms: start + 240 * 15_000,
        });
    }

    // Three one-sample series in one group: the smallest case where the
    // derived bound is nearly tight.
    {
        let start = t - 60_000;
        let metric = "rows_three";
        let series = (0..3u64)
            .map(|i| Series {
                fp: i + 1,
                metric: metric.to_string(),
                labels: lbl(&[("status", "200")]),
                samples: vec![(start + (i as i64) * 15_000, ((i + 1) as f64).to_bits())],
                hist_samples: Vec::new(),
            })
            .collect();
        cases.push(RowCase {
            name: "three one-sample series in one group",
            metric,
            series,
            grid: Grid {
                start_ms: start,
                step_ms: 15_000,
                points: 241,
                lookback_ms: 300_000,
            },
            lower_excl_ms: start - 300_000,
            upper_incl_ms: start + 240 * 15_000,
        });
    }

    // One group per series — the shape `series >= 2 * groups` DECLINES.
    // It is measured here because the threshold's documentation used to
    // say the push could not return fewer rows than the raw read on this
    // shape, and it can: three constant series, each its own group, each
    // collapsing to a single run.
    {
        let start = t - 3_600_000;
        let metric = "rows_one_group_per_series";
        let series = (0..3u64)
            .map(|i| Series {
                fp: i + 1,
                metric: metric.to_string(),
                labels: lbl(&[("status", &format!("s{i}"))]),
                // A constant value, so the group's answer never changes
                // and the whole series is one run however many samples
                // feed it.
                samples: (0..=240i64)
                    .map(|k| (start + k * 15_000, ((i + 1) as f64).to_bits()))
                    .collect(),
                hist_samples: Vec::new(),
            })
            .collect();
        cases.push(RowCase {
            name: "one group per series, constant values (declined)",
            metric,
            series,
            grid: Grid {
                start_ms: start,
                step_ms: 15_000,
                points: 241,
                lookback_ms: 300_000,
            },
            lower_excl_ms: start - 300_000,
            upper_incl_ms: start + 240 * 15_000,
        });
    }
    cases
}

/// Criterion 7: **one assertion, and it is the derived bound.**
///
/// ```text
///   pushed_rows <= 2 * raw_rows,  on every corpus in this suite
/// ```
///
/// It follows from the structure rather than from a corpus: each sample
/// opens its coverage at one grid index and closes it at another, so it
/// can begin at most two runs. Four named worst cases have each been
/// beaten by a corpus constructed afterwards; the derived bound is the
/// only statement that survives, and it survives because it is not about
/// a corpus.
///
/// The ratios and the byte figures below are **observations, with their
/// corpora named, asserted by nothing**: they move with block framing,
/// compression, format and corpus compressibility, none of which the
/// claim is about.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pushed_rows_never_exceed_twice_the_raw_rows() {
    skip_unless_live!();
    let t = (now_ms() / 60_000) * 60_000;
    let cases = row_cases(t);
    let all: Vec<Series> = cases.iter().flat_map(|c| c.series.clone()).collect();
    let h = harness(
        &pulsus_testkit::test_db("pulsus_read_it_grouped_rowbound"),
        &all,
    )
    .await;

    eprintln!(
        "[549] corpus                                          op      pushed : raw    ratio"
    );
    for case in &cases {
        let fps: Vec<FpLiteral> = case
            .series
            .iter()
            .map(|s| Fingerprint::from_raw(u128::from(s.fp)).sql_literal())
            .collect();
        // Every series of these corpora is in one `status` group per its
        // own label, so the gid vector is the group index. Built here
        // rather than taken from `decide`, so the row count is measured
        // against a statement this test rendered.
        let mut keys: Vec<String> = Vec::new();
        let gids: Vec<u32> = case
            .series
            .iter()
            .map(|s| {
                let k = s
                    .labels
                    .iter()
                    .find(|(n, _)| n == "status")
                    .map(|(_, v)| v.clone())
                    .expect("status");
                match keys.iter().position(|x| *x == k) {
                    Some(i) => i as u32,
                    None => {
                        keys.push(k);
                        (keys.len() - 1) as u32
                    }
                }
            })
            .collect();
        let raw = h
            .count(&format!(
                "SELECT toUInt64(count()) AS n FROM metric_samples \
                 WHERE metric_name = '{}' AND unix_milli > {} AND unix_milli <= {}",
                case.metric, case.lower_excl_ms, case.upper_incl_ms
            ))
            .await;
        // Every operation, not one: `count` changes at an arrival AND at
        // an expiry where `max` may not, so the ratio is a property of the
        // corpus and the operation together.
        for op in [
            GroupedOp::Max,
            GroupedOp::Min,
            GroupedOp::Count,
            GroupedOp::Group,
        ] {
            let sql = grouped_sql::grouped_fetch(
                "metric_samples",
                "metric_hist_samples",
                case.metric,
                &fps,
                &gids,
                case.grid,
                case.lower_excl_ms,
                case.upper_incl_ms,
                op,
            );
            let pushed = h
                .count(&format!("SELECT toUInt64(count()) AS n FROM (\n{sql}\n)"))
                .await;
            assert!(
                pushed <= 2 * raw,
                "{} / {op:?}: {pushed} pushed rows from {raw} raw rows breaks the derived bound",
                case.name
            );
            eprintln!(
                "[549] {:<48} {:<6} {pushed:>7} : {raw:<7} {:.3}",
                case.name,
                format!("{op:?}"),
                pushed as f64 / raw as f64
            );
        }
    }

    // The byte figures, printed and asserted by nothing: they move with
    // block framing, compression, format and corpus compressibility, none
    // of which the row bound is about. `NetworkSendBytes` is the
    // COORDINATOR's hop — what the server sent this client — plus the
    // statement text this client sent it. On a single node that is the
    // metered hop; on a cluster the shard hop carries what it carries
    // today, unchanged by this work.
    let tag = pulsus_testkit::test_ident("pulsus_read_it_grouped_bytes");
    let mut ids: Vec<(String, String, usize)> = Vec::new();
    for (n, case) in cases.iter().enumerate() {
        let fps: Vec<FpLiteral> = case
            .series
            .iter()
            .map(|s| Fingerprint::from_raw(u128::from(s.fp)).sql_literal())
            .collect();
        let gids = vec![0u32; fps.len()];
        let pushed_sql = grouped_sql::grouped_fetch(
            "metric_samples",
            "metric_hist_samples",
            case.metric,
            &fps,
            &gids,
            case.grid,
            case.lower_excl_ms,
            case.upper_incl_ms,
            GroupedOp::Max,
        );
        let raw_sql = sample_sql::sample_fetch(
            "metric_samples",
            case.metric,
            &fps,
            case.lower_excl_ms,
            case.upper_incl_ms,
        );
        let pushed_id = format!("{tag}_{n}_pushed");
        for _ in 0..3 {
            h.run_tagged::<pulsus_read::metrics::grouped_rows::GroupedRunRow>(
                &pushed_sql,
                &pushed_id,
            )
            .await;
        }
        ids.push((pushed_id, pushed_sql.len().to_string(), 0));
        let raw_id = format!("{tag}_{n}_raw");
        for _ in 0..3 {
            h.run_tagged::<pulsus_read::metrics::SampleRow>(&raw_sql, &raw_id)
                .await;
        }
        ids.push((raw_id, raw_sql.len().to_string(), 0));
    }
    let sent = h.sent_bytes(&tag, ids.len()).await;
    eprintln!(
        "[549] corpus                                          route   NetworkSendBytes + text"
    );
    for (n, case) in cases.iter().enumerate() {
        for kind in ["pushed", "raw"] {
            let id = format!("{tag}_{n}_{kind}");
            let text: usize = ids
                .iter()
                .find(|(i, _, _)| *i == id)
                .map(|(_, t, _)| t.parse().expect("len"))
                .expect("recorded");
            let bytes = sent.get(&id).copied().unwrap_or_default();
            eprintln!("[549] {:<48} {kind:<7} {}", case.name, bytes + text as u64);
        }
    }
    h.finish().await;
}

// ------------------------------------------------- criterion 10: the charge

/// One charge case: a corpus, an operation, and the engine configuration
/// that reads it.
struct ChargeCase {
    query: String,
    params: MetricQueryParams,
    /// How many fingerprints one statement carries. Production is always
    /// `sample_sql::CHUNK_THRESHOLD`; corpora E and F are one answer at
    /// two settings of it.
    chunk: usize,
}

/// Runs the case at `cap` and reports whether it was served.
///
/// The engine drains every statement fully before returning, so a budget
/// breach surfaces as an error from `query` rather than as a status code
/// — there is no partial answer to inspect.
async fn served_at(h: &Harness, db: &str, case: &ChargeCase, cap: u64) -> bool {
    let engine = MetricsEngine::new(
        ChClient::new(test_config(db)).await.expect("connect"),
        Arc::clone(&h.cache),
        MetricsConfig {
            max_samples: cap,
            ..engine_config(db, true)
        },
    )
    .with_grouped_chunk_size(case.chunk);
    let expr = parse(&case.query).expect("parse");
    match engine.query(&expr, &case.params).await {
        Ok(_) => true,
        Err(e) => {
            let msg = format!("{e:?}");
            assert!(
                msg.contains("MetricSamples"),
                "{}: the refusal must be the sample budget, got {msg}",
                case.query
            );
            false
        }
    }
}

/// Finds the exact charge by bisection over the cap: the smallest cap
/// that is served IS the charge, because a cap equal to the charge is
/// served and one less is refused.
async fn measured_charge(h: &Harness, db: &str, case: &ChargeCase) -> u64 {
    let mut lo = 1u64;
    let mut hi = 200_000u64;
    assert!(
        served_at(h, db, case, hi).await,
        "{}: not served even at {hi}",
        case.query
    );
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if served_at(h, db, case, mid).await {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo
}

/// The boundary pair every charge row is: the cap equal to the charge is
/// served, one less is refused.
async fn assert_pair(h: &Harness, db: &str, case: &ChargeCase, charge: u64, label: &str) {
    assert!(
        served_at(h, db, case, charge).await,
        "{label}: a cap of {charge} must be served"
    );
    assert!(
        !served_at(h, db, case, charge - 1).await,
        "{label}: a cap of {} must be refused",
        charge - 1
    );
}

fn corpus_a(t: i64, metric: &'static str, fps: std::ops::Range<u64>) -> Vec<Series> {
    let start = t - 3_600_000;
    fps.map(|i| Series {
        fp: i,
        metric: metric.to_string(),
        labels: lbl(&[("status", ["200", "404", "500", "503"][(i % 4) as usize])]),
        samples: (0..=240i64)
            .map(|k| (start + k * 15_000, (i as f64 + k as f64 * 0.5).to_bits()))
            .collect(),
        hist_samples: Vec::new(),
    })
    .collect()
}

fn corpus_a_params(t: i64) -> MetricQueryParams {
    MetricQueryParams {
        start_ms: t - 3_600_000,
        end_ms: t - 3_600_000 + 240 * 15_000,
        step_ms: 15_000,
    }
}

/// Criterion 10, corpora A and B: the two charges the plan measured
/// before this code existed.
///
/// | corpus | operation | charge |
/// |---|---|---|
/// | A — 400 fingerprints, `gid = i mod 4`, 241 samples each | `max` | 964 |
/// | B — 3 fingerprints, one group, one sample each | `count` | 5 |
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_charge_is_the_runs_this_query_materialised() {
    skip_unless_live!();
    let t = (now_ms() / 60_000) * 60_000;
    let start = t - 3_600_000;
    let mut fx = corpus_a(t, "charge_a", 1..401);
    for (n, i) in [0i64, 1, 2].iter().enumerate() {
        fx.push(Series {
            fp: 900 + n as u64,
            metric: "charge_b".to_string(),
            labels: lbl(&[("status", "200")]),
            samples: vec![(start + i * 15_000, ((n + 1) as f64).to_bits())],
            hist_samples: Vec::new(),
        });
    }
    let db = pulsus_testkit::test_db("pulsus_read_it_grouped_charge_ab");
    let h = harness(&db, &fx).await;
    let params = corpus_a_params(t);

    let a = ChargeCase {
        query: "max by (status) (charge_a)".to_string(),
        params,
        chunk: sample_sql::CHUNK_THRESHOLD,
    };
    let charge_a = measured_charge(&h, &db, &a).await;
    eprintln!("[549] corpus A, max   -> charge {charge_a}");
    assert_pair(&h, &db, &a, charge_a, "corpus A / max").await;

    let b = ChargeCase {
        query: "count by (status) (charge_b)".to_string(),
        params,
        chunk: sample_sql::CHUNK_THRESHOLD,
    };
    let charge_b = measured_charge(&h, &db, &b).await;
    eprintln!("[549] corpus B, count -> charge {charge_b}");
    assert_pair(&h, &db, &b, charge_b, "corpus B / count").await;

    // The plan's own predictions, stated so a change in either direction
    // is visible rather than absorbed.
    assert_eq!(charge_a, 964, "corpus A's charge under max");
    assert_eq!(charge_b, 5, "corpus B's charge under count");
    h.finish().await;
}

/// Criterion 10, corpus C: corpus B with the lowest fingerprint's sample
/// replaced by the stale marker, under `max`.
///
/// A stale sample still OCCUPIES its coverage interval — it blocks the
/// earlier sample — and is then dropped, which is the reference's rule: a
/// stale marker makes the series absent rather than falling back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_charge_of_a_corpus_whose_first_sample_is_stale() {
    skip_unless_live!();
    let t = (now_ms() / 60_000) * 60_000;
    let start = t - 3_600_000;
    let fx: Vec<Series> = (0..3usize)
        .map(|n| Series {
            fp: 900 + n as u64,
            metric: "charge_c".to_string(),
            labels: lbl(&[("status", "200")]),
            samples: vec![(
                start + n as i64 * 15_000,
                if n == 0 {
                    STALE_NAN_BITS
                } else {
                    ((n + 1) as f64).to_bits()
                },
            )],
            hist_samples: Vec::new(),
        })
        .collect();
    let db = pulsus_testkit::test_db("pulsus_read_it_grouped_charge_c");
    let h = harness(&db, &fx).await;
    let case = ChargeCase {
        query: "max by (status) (charge_c)".to_string(),
        params: corpus_a_params(t),
        chunk: sample_sql::CHUNK_THRESHOLD,
    };
    let charge = measured_charge(&h, &db, &case).await;
    eprintln!("[549] corpus C, max   -> charge {charge}");
    assert_pair(&h, &db, &case, charge, "corpus C / max").await;
    // Recorded on the first run of this code. The stale sample occupies
    // grid index 0 and is then dropped, so the group is absent there and
    // the answer is two runs: fingerprint 901's value from index 1, then
    // 902's from index 2 onward.
    assert_eq!(charge, 2, "corpus C's charge under max");
    // The answer is still the shipped route's.
    h.agree(&case.query, &case.params).await;
    h.finish().await;
}

/// Criterion 10, corpora D, E and F: **where the chunk boundary falls is
/// part of the charge**, because the charged rows are per-statement
/// partial runs.
///
/// ```text
///   D  501 fingerprints in one group; 1..500 carry NO samples, 501
///      carries corpus A's 241 samples.   chunk 500 -> two statements,
///      the first returning no rows
///   E  corpus A's values on 501 fingerprints, chunk 500 -> two statements
///   F  the SAME 501 fingerprints,          chunk 501 -> one statement
/// ```
///
/// E and F are one answer arriving two ways, and their charges differ.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_charge_depends_on_where_the_chunk_boundary_falls() {
    skip_unless_live!();
    let t = (now_ms() / 60_000) * 60_000;
    let start = t - 3_600_000;
    let mut fx: Vec<Series> = (1..=500u64)
        .map(|fp| Series {
            fp,
            metric: "charge_d".to_string(),
            labels: lbl(&[("status", "200")]),
            samples: Vec::new(),
            hist_samples: Vec::new(),
        })
        .collect();
    fx.push(Series {
        fp: 501,
        metric: "charge_d".to_string(),
        labels: lbl(&[("status", "200")]),
        samples: (0..=240i64)
            .map(|k| (start + k * 15_000, (501.0 + k as f64 * 0.5).to_bits()))
            .collect(),
        hist_samples: Vec::new(),
    });
    // E and F share one corpus of 501 fingerprints, all carrying samples.
    fx.extend((1..=501u64).map(|fp| {
        Series {
            fp: 1000 + fp,
            metric: "charge_ef".to_string(),
            labels: lbl(&[("status", ["200", "404", "500", "503"][(fp % 4) as usize])]),
            samples: (0..=240i64)
                .map(|k| (start + k * 15_000, (fp as f64 + k as f64 * 0.5).to_bits()))
                .collect(),
            hist_samples: Vec::new(),
        }
    }));

    let db = pulsus_testkit::test_db("pulsus_read_it_grouped_charge_def");
    let h = harness(&db, &fx).await;
    let params = corpus_a_params(t);

    let d = ChargeCase {
        query: "max by (status) (charge_d)".to_string(),
        params,
        chunk: 500,
    };
    let charge_d = measured_charge(&h, &db, &d).await;
    eprintln!("[549] corpus D, max, chunk 500 -> charge {charge_d}");
    assert_pair(&h, &db, &d, charge_d, "corpus D / max").await;

    let e = ChargeCase {
        query: "max by (status) (charge_ef)".to_string(),
        params,
        chunk: 500,
    };
    let charge_e = measured_charge(&h, &db, &e).await;
    eprintln!("[549] corpus E, max, chunk 500 -> charge {charge_e}");
    assert_pair(&h, &db, &e, charge_e, "corpus E / max").await;

    let f = ChargeCase {
        query: "max by (status) (charge_ef)".to_string(),
        params,
        chunk: 501,
    };
    let charge_f = measured_charge(&h, &db, &f).await;
    eprintln!("[549] corpus F, max, chunk 501 -> charge {charge_f}");
    assert_pair(&h, &db, &f, charge_f, "corpus F / max").await;

    // Recorded on the first run of this code.
    assert_eq!(
        charge_d, 241,
        "corpus D: an empty first chunk charges nothing"
    );
    assert_eq!(
        charge_e, 1_205,
        "corpus E: 964 runs from the first chunk's 500 fingerprints plus 241 from the second's one"
    );
    assert_eq!(
        charge_f, 964,
        "corpus F: one statement over the same 501 fingerprints"
    );
    assert!(
        charge_e > charge_f,
        "one answer, two chunkings: two statements' partial runs ({charge_e}) must charge \
         more than one statement's ({charge_f})"
    );
    h.finish().await;
}

/// Criterion 10, across TWO statements: **a completed earlier statement's
/// charge answers before a later statement's failure.**
///
/// The query sends two statements. The second is made to fail by a memory
/// ceiling it alone breaches; the first drains and charges.
///
/// ```text
///   max_samples = the first statement's charge        -> the memory error
///   max_samples = that charge - 1                     -> the BUDGET error
/// ```
///
/// # What this does NOT prove, and where that is proved instead
///
/// It does not require the charge to be taken PER ROW AS ROWS ARRIVE.
/// Measured in review round 1: moving `charge_one` out of the drain loop
/// and charging the drained length after each statement finishes leaves
/// this test green, because a statement that COMPLETED is charged the
/// same either way. Two statements cannot separate them.
///
/// The per-row property is separated by exactly one input — a single
/// statement's stream that yields rows and then fails — and is asserted
/// by `metrics::dispatch::tests`:
///
/// ```text
///   the_budget_refuses_mid_statement_before_a_later_row_fails
///   a_cap_equal_to_the_rows_admits_them_and_the_streams_error_surfaces
///   a_failed_row_is_not_charged
/// ```
///
/// Both belong: those three pin the ordering inside one drain loop; this
/// one pins that the budget is ONE object spanning the statement set,
/// against a real server, which a stand-in stream cannot show.
///
/// # The grid-cap pair is a DECISION not to test, with its reason
///
/// Criterion 10's other pair asks what answers first when a request
/// breaches both the range-grid point cap and the sample budget. There is
/// no test for it here, and that is a decision rather than an
/// impossibility:
///
/// * The point cap is enforced in the HTTP layer, before a
///   `MetricsEngine` is constructed, so on a real request nothing has
///   been charged when it fires. `pulsus-server`'s
///   `query_range_rejects_one_interval_past_the_cap_before_any_pool_check`
///   asserts the `400 bad_data` arrives before even a pool check — which
///   is the production ordering, asserted closer to the client than a
///   read-path test could.
/// * A test CAN be built that makes the budget's answer observable
///   underneath: a live engine with a tiny budget, driven past the cap
///   with the range validation removed. What it would assert is the
///   behaviour of a server that does not exist — with the validation in
///   place the budget is never reached — so it would pin an ordering no
///   request can observe, and it would go green if the cap moved behind
///   the engine, which is the change it ought to catch.
///
/// So: not built, because the pair it would assert cannot arise, and the
/// production ordering is already asserted one layer up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_budget_answers_before_a_later_statements_failure() {
    skip_unless_live!();
    let t = (now_ms() / 60_000) * 60_000;
    let start = t - 3_600_000;
    // The query sends TWO statements of four hundred fingerprints each.
    // They are the same width and very different weight:
    //
    //   chunk 1   fingerprints   1..=400   ten samples each     4,000 rows
    //   chunk 2   fingerprints 401..=800   241 samples each    96,400 rows
    //
    // Chunk 1's ten samples are shared across its four hundred series and
    // strictly increasing, so its group's maximum moves at ten
    // consecutive grid points and the statement returns exactly ten runs.
    let mut fx: Vec<Series> = (1..=400u64)
        .map(|fp| Series {
            fp,
            metric: "charge_prec".to_string(),
            labels: lbl(&[("status", "200")]),
            samples: (0..10i64)
                .map(|k| (start + k * 15_000, (k as f64).to_bits()))
                .collect(),
            hist_samples: Vec::new(),
        })
        .collect();
    fx.extend((401..=800u64).map(|fp| {
        Series {
            fp,
            metric: "charge_prec".to_string(),
            labels: lbl(&[("status", "200")]),
            samples: (0..=240i64)
                .map(|k| (start + k * 15_000, (fp as f64 + k as f64 * 0.5).to_bits()))
                .collect(),
            hist_samples: Vec::new(),
        }
    }));
    let db = pulsus_testkit::test_db("pulsus_read_it_grouped_precedence");
    let h = harness(&db, &fx).await;
    let params = corpus_a_params(t);
    let query = "max by (status) (charge_prec)";
    let expr = parse(query).expect("parse");

    // The first statement's own run count, measured directly: with a
    // chunk of 4 the query still sends the heavy second statement, so it
    // cannot be read off a served/refused boundary.
    let first_chunk: Vec<FpLiteral> = (1..=400u128)
        .map(|v| Fingerprint::from_raw(v).sql_literal())
        .collect();
    let sql = grouped_sql::grouped_fetch(
        "metric_samples",
        "metric_hist_samples",
        "charge_prec",
        &first_chunk,
        &vec![0u32; 400],
        Grid {
            start_ms: params.start_ms,
            step_ms: params.step_ms,
            points: 241,
            lookback_ms: DEFAULT_LOOKBACK_MS,
        },
        params.start_ms - DEFAULT_LOOKBACK_MS,
        params.end_ms,
        GroupedOp::Max,
    );
    let first_charge = h
        .count(&format!("SELECT toUInt64(count()) AS n FROM (\n{sql}\n)"))
        .await;
    assert!(
        first_charge > 1,
        "the first statement must return more than one run, or there is no cap below it to read"
    );

    // A ceiling the light statement clears and the heavy one does not.
    // **The sweep RUNS** (review round 1): it was a comment, and a table
    // nobody executes is a claim about a tree nobody checked. Each of the
    // two statements is executed alone and fully drained under the
    // engine's own settings at each ceiling, and the window the test
    // depends on is asserted rather than described.
    //
    // ```text
    //   ceiling    chunk 1 (4,000 rows in)   chunk 2 (96,400 rows in)
    //    4 MiB     refused                   refused
    //   10 MiB     ran                       refused      <- the test runs here
    //   64 MiB     ran                       ran
    // ```
    //
    // **The window moved with issue #498** and the figures are the ones
    // measured after it. The identity is a `UInt128`, so every place this
    // statement carries the `fingerprint` column — the two scans, the
    // `PARTITION BY`, the `transform` array — costs twice the bytes it
    // did, and the light statement's peak crossed the old 6 MiB ceiling.
    // The edges were re-measured one ceiling at a time:
    //
    // ```text
    //    6 MiB     refused                   refused
    //    8 MiB     ran                       refused
    //   12 MiB     ran                       refused
    //   16 MiB     ran                       ran
    // ```
    //
    // 10 MiB is inside the new window with a row either side of it that
    // this test executes. The 8 and 12 MiB rows are recorded here rather
    // than run: they say what 4 and 64 already say, at a minute of live
    // time each.
    const CEILING: u64 = 10 * 1024 * 1024;
    let heavy_chunk: Vec<FpLiteral> = (401..=800u128)
        .map(|v| Fingerprint::from_raw(v).sql_literal())
        .collect();
    let heavy_sql = grouped_sql::grouped_fetch(
        "metric_samples",
        "metric_hist_samples",
        "charge_prec",
        &heavy_chunk,
        &vec![0u32; 400],
        Grid {
            start_ms: params.start_ms,
            step_ms: params.step_ms,
            points: 241,
            lookback_ms: DEFAULT_LOOKBACK_MS,
        },
        params.start_ms - DEFAULT_LOOKBACK_MS,
        params.end_ms,
        GroupedOp::Max,
    );
    for (ceiling, light_ok, heavy_ok) in [
        (4 * 1024 * 1024u64, false, false),
        (CEILING, true, false),
        (64 * 1024 * 1024u64, true, true),
    ] {
        for (which, stmt, want_ok) in [("light", &sql, light_ok), ("heavy", &heavy_sql, heavy_ok)] {
            let got = h.drains_under_ceiling(stmt, ceiling).await;
            assert_eq!(
                got,
                want_ok,
                "at a {} MiB ceiling the {which} statement must {}",
                ceiling / (1024 * 1024),
                if want_ok { "drain" } else { "be refused" }
            );
        }
    }
    let run = |cap: u64| {
        let db = db.clone();
        let expr = expr.clone();
        let resolver = Arc::clone(&h.cache);
        async move {
            let engine = MetricsEngine::new(
                ChClient::new(test_config(&db)).await.expect("connect"),
                resolver,
                MetricsConfig {
                    max_samples: cap,
                    read_max_memory_bytes: CEILING,
                    ..engine_config(&db, true)
                },
            )
            .with_grouped_chunk_size(400);
            engine
                .query(&expr, &params)
                .await
                .err()
                .map(|e| format!("{e:?}"))
        }
    };

    // The ceiling bites: with the budget effectively unbounded, the only
    // refusal available is the heavy statement's memory ceiling.
    let unbounded = run(50_000_000).await.expect("the heavy statement fails");
    assert!(
        unbounded.contains("PromqlReadMemory"),
        "the ceiling must be the thing that refuses, got {unbounded}"
    );

    let at_charge = run(first_charge).await.expect("the heavy statement fails");
    assert!(
        at_charge.contains("PromqlReadMemory"),
        "at a cap equal to the first statement's charge the memory refusal surfaces, got \
         {at_charge}"
    );
    let below = run(first_charge - 1).await.expect("refused");
    assert!(
        below.contains("MetricSamples"),
        "one below, the BUDGET answers first — which also shows the FIRST statement drained \
         and charged rather than breaching the ceiling itself; got {below}"
    );
    eprintln!(
        "[549] precedence: the first statement charges {first_charge}; at {first_charge} the \
         memory error surfaces, at {} the budget error does",
        first_charge - 1
    );
    h.finish().await;
}

/// **Issue #498 criterion 5: the grouped statement's `fps` array types as
/// `Array(UInt128)` and assigns the right group id at the boundary.**
///
/// The grouped instant read hands ClickHouse two parallel arrays and lets
/// `transform(fingerprint, fps, gids, …)` map each stored fingerprint to
/// its group. That is the one place where a wrong literal **mis-assigns a
/// group** rather than merely losing rows: a bare decimal above `2^64` is
/// read as `Float64`, two neighbouring fingerprints round onto one value,
/// and their samples are aggregated under whichever group won the
/// rounding.
///
/// So this asserts the array's type and the mapping, at `2^64+1` and
/// `2^64+2` — the pair that differs by one and rounds onto the same
/// `Float64`.
#[tokio::test]
async fn the_grouped_fps_array_types_as_uint128_and_maps_each_boundary_value() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_read_it_grouped_fp128");
    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect");
    drop_database(&bootstrap, &db).await;
    init_db(&bootstrap, &db).await;
    let client = ChClient::new(test_config(&db))
        .await
        .expect("connect to db");

    // The two values a bare literal cannot separate.
    const A: u128 = 18_446_744_073_709_551_617;
    const B: u128 = 18_446_744_073_709_551_618;

    client
        .execute(
            &format!(
                "CREATE TABLE fp128_transform (fingerprint UInt128) ENGINE = MergeTree \
                 ORDER BY fingerprint AS SELECT arrayJoin([toUInt128('{A}'), \
                 toUInt128('{B}')]) AS fingerprint"
            ),
            &QuerySettings::new(),
            pulsus_clickhouse::Idempotency::Idempotent,
        )
        .await
        .expect("create the two-row fixture");

    #[derive(pulsus_clickhouse::Row, serde::Serialize, serde::Deserialize, Debug)]
    struct TypeRow {
        t: String,
    }
    #[derive(pulsus_clickhouse::Row, serde::Serialize, serde::Deserialize, Debug)]
    struct GidRow {
        fingerprint: u128,
        gid: u32,
    }

    // **The array text comes from the production renderer, not from this
    // file.** `grouped_sql::grouped_fetch` builds its `fps` array by
    // calling `sample_sql::render_fingerprint_list`, so that is what is
    // executed here. A test that writes out the SQL it expects cannot
    // notice the renderer changing — with the elements hard-coded, making
    // `FpLiteral` emit a bare decimal left this test green.
    let rendered = sample_sql::render_fingerprint_list(&[
        Fingerprint::from_raw(A).sql_literal(),
        Fingerprint::from_raw(B).sql_literal(),
    ]);

    // It types as `Array(UInt128)` with no `CAST` of its own.
    let sql = format!("WITH [{rendered}] AS fps SELECT toTypeName(fps) AS t");
    let mut stream = client
        .query_stream::<TypeRow>(&sql, &QuerySettings::new())
        .await
        .expect("type query");
    let ty = stream.next().await.expect("one row").expect("decode").t;
    drop(stream);
    assert_eq!(
        ty, "Array(UInt128)",
        "the rendered `fps` array does not type as Array(UInt128): {ty}\n  rendered: {rendered}"
    );

    // And `transform` maps each of the two to its own group id, over the
    // same rendered array.
    let sql = format!(
        "WITH [{rendered}] AS fps, \
         CAST([101, 102], 'Array(UInt32)') AS gids \
         SELECT fingerprint, transform(fingerprint, fps, gids, CAST(0, 'UInt32')) AS gid \
         FROM fp128_transform WHERE fingerprint IN fps ORDER BY fingerprint"
    );
    let mut stream = client
        .query_stream::<GidRow>(&sql, &QuerySettings::new())
        .await
        .expect("transform query");
    let mut got: Vec<(u128, u32)> = Vec::new();
    while let Some(row) = stream.next().await {
        let row = row.expect("decode");
        got.push((row.fingerprint, row.gid));
    }
    drop(stream);
    assert_eq!(
        got,
        vec![(A, 101), (B, 102)],
        "the group ids at the 2^64 boundary are not the ones `fps`/`gids` name\n  \
         rendered: {rendered}"
    );

    // The whole grouped statement carries the same array, so the check
    // above is about the statement production sends rather than about a
    // fragment assembled here.
    let grid = Grid {
        start_ms: 1_782_907_200_000,
        step_ms: 15_000,
        points: 2,
        lookback_ms: DEFAULT_LOOKBACK_MS,
    };
    let statement = grouped_sql::grouped_fetch(
        "metric_samples",
        "metric_hist_samples",
        "pulsus_probe",
        &[
            Fingerprint::from_raw(A).sql_literal(),
            Fingerprint::from_raw(B).sql_literal(),
        ],
        &[101, 102],
        grid,
        1_782_907_200_000,
        1_782_907_230_000,
        GroupedOp::Max,
    );
    assert!(
        statement.contains(&format!("[{rendered}] AS fps")),
        "the grouped statement does not carry the rendered array:\n{statement}"
    );

    drop_database(&bootstrap, &db).await;
}
