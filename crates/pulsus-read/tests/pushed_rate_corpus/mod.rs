//! Issue #579: the corpora and the two-engine harness the pushed
//! aggregate's live suites share — `live_metrics_pushed_rate.rs` and the
//! statement gate in `query_log_gates.rs`.
//!
//! `#[path]`-included rather than a test target of its own, so it carries
//! no test and each including file uses the part it needs.
//!
//! # `CPU_CORPUS`
//!
//! The issue's scale: 16 CPUs x 8 modes of `node_cpu_seconds_total`, one
//! scrape per 15 s, 24 h plus a 15-minute lead-in, each scrape 0-900 ms
//! late and every 97th 0-9 s late. On mode `user`:
//!
//! ```text
//!   cpu 0   a counter reset at 6 h
//!   cpu 1   a 12-minute gap at 10 h, longer than the lookback and the range
//!   cpu 2   starts from 0 at 3 h 45 m
//!   cpu 3   ends at 20 h with a stale marker
//!   cpu 4   every sample of one hour stored twice
//!   cpu 5   restarts near 0 every 30 minutes
//!   cpu 6   resets twice within 75 s
//! ```
//!
//! The values come from a fixed-seed generator, so every run seeds the
//! same corpus relative to its anchor.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_model::{ACTIVITY_BUCKET_MS, STALE_NAN_BITS};
use pulsus_promql::parser::parse;
use pulsus_read::{
    HistOrFloat, LabelCache, LabelCacheConfig, MetricQueryParams, MetricsConfig, MetricsEngine,
    QueryResult,
};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

pub const CPU_METRIC: &str = "node_cpu_seconds_total";
pub const MODES: [&str; 8] = [
    "idle", "iowait", "irq", "nice", "softirq", "steal", "system", "user",
];
pub const SCRAPE_MS: i64 = 15_000;
/// Scrapes before the range start: 15 minutes.
pub const LEAD: i64 = 60;
/// 24 h of scrapes, the lead-in, and the one at the range end.
pub const N_SCRAPES: i64 = LEAD + 5_760 + 1;
pub const DAY_MS: i64 = 86_400_000;

/// The issue's query.
pub const ISSUE_QUERY: &str = "sum by (mode) (rate(node_cpu_seconds_total[5m])) / on() \
                               group_left count(count by (cpu)(node_cpu_seconds_total)) * 100";

/// splitmix64 — the project's fixed-seed generator for committed corpora.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[lo, hi)`.
    pub fn uniform(&mut self, lo: f64, hi: f64) -> f64 {
        let unit = (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        lo + (hi - lo) * unit
    }

    /// Uniform in `[0, n]`.
    pub fn upto(&mut self, n: u64) -> i64 {
        (self.next_u64() % (n + 1)) as i64
    }

    pub fn fingerprint(&mut self) -> u128 {
        (u128::from(self.next_u64()) << 64) | u128::from(self.next_u64())
    }
}

/// One seeded series: identity, float samples `(ms, value)` and the times
/// of its histogram samples.
#[derive(Debug, Clone)]
pub struct SeedSeries {
    pub fp: u128,
    pub metric: String,
    pub labels: Vec<(String, String)>,
    pub samples: Vec<(i64, f64)>,
    pub hist: Vec<i64>,
}

/// `CPU_CORPUS`, anchored so the last scrape is at `t_end` and the 24-hour
/// range starts at `t_end - DAY_MS`.
pub fn cpu_corpus(t_end: i64) -> Vec<SeedSeries> {
    let t0 = t_end - DAY_MS;
    let mut rng = Rng::new(579);
    let mut out = Vec::new();
    for cpu in 0..16 {
        for mode in MODES {
            out.push(SeedSeries {
                fp: rng.fingerprint(),
                metric: CPU_METRIC.to_string(),
                labels: vec![
                    ("cpu".to_string(), cpu.to_string()),
                    ("mode".to_string(), mode.to_string()),
                ],
                samples: Vec::new(),
                hist: Vec::new(),
            });
        }
    }
    let stale = f64::from_bits(STALE_NAN_BITS);
    for s in &mut out {
        let kind = if s.labels[1].1 == "user" {
            match s.labels[0].1.as_str() {
                "0" => "reset",
                "1" => "gap",
                "2" => "start",
                "3" => "end",
                "4" => "dup",
                "5" => "zero",
                "6" => "multi",
                _ => "",
            }
        } else {
            ""
        };
        let mut v = if matches!(kind, "start" | "zero") {
            0.0
        } else {
            rng.uniform(1_000.0, 100_000.0)
        };
        for k in 0..N_SCRAPES {
            let jitter = if k % 97 != 0 {
                rng.upto(900)
            } else {
                rng.upto(9_000)
            };
            let ts = t0 + (k - LEAD) * SCRAPE_MS + jitter;
            if kind == "start" && k < 900 {
                continue;
            }
            if kind == "gap" && (2_400..2_448).contains(&k) {
                continue;
            }
            if kind == "end" && k > 4_860 {
                if k == 4_861 {
                    s.samples.push((ts, stale));
                }
                continue;
            }
            v += rng.uniform(0.0, 15.0);
            if kind == "reset" && k == 1_440 {
                v = rng.uniform(0.0, 3.0);
            }
            if kind == "zero" && k % 120 == 0 {
                v = rng.uniform(0.0, 0.5);
            }
            if kind == "multi" && (k == 3_600 || k == 3_605) {
                v = rng.uniform(0.0, 2.0);
            }
            s.samples.push((ts, v));
            if kind == "dup" && (3_000..3_240).contains(&k) {
                s.samples.push((ts, v));
            }
        }
    }
    out
}

pub const EDGE_METRIC: &str = "edge_counter_total";

/// T3b: four counters rising by `f64::MAX / 48` a scrape for ten minutes,
/// so a 5-minute `increase` is near `f64::MAX / 2.3` and the `sum` of four
/// overflows while `avg` switches to the incremental mean; the fourth
/// carries one NaN sample.
pub fn edge_corpus(t_end: i64) -> Vec<SeedSeries> {
    let step = f64::MAX / 48.0;
    (0..4u128)
        .map(|i| SeedSeries {
            fp: 0x5790_0000_0000_0000_0000_0000_0000_0000 | i,
            metric: EDGE_METRIC.to_string(),
            labels: vec![("series".to_string(), i.to_string())],
            samples: (0..41)
                .map(|k| {
                    let v = if i == 3 && k == 20 {
                        f64::from_bits(0x7FF8_0000_0000_0001)
                    } else {
                        (k as f64) * step * (1.0 + i as f64 / 64.0)
                    };
                    (t_end - (40 - k) * SCRAPE_MS, v)
                })
                .collect(),
            hist: Vec::new(),
        })
        .collect()
}

pub const MIXED_METRIC: &str = "mixed_counter_total";
pub const HIST_ONLY_METRIC: &str = "hist_only_total";

/// T6: (a) three float counters, one of which also holds ONE histogram
/// sample; (b) two series holding only histogram samples, 41 of them
/// 15 s apart, so every 5-minute window holds 20.
pub fn histogram_corpora(t_end: i64) -> Vec<SeedSeries> {
    let mut out = Vec::new();
    for i in 0..3u128 {
        out.push(SeedSeries {
            fp: 0x5791_0000_0000_0000_0000_0000_0000_0000 | i,
            metric: MIXED_METRIC.to_string(),
            labels: vec![("series".to_string(), i.to_string())],
            samples: (0..41)
                .map(|k| (t_end - (40 - k) * SCRAPE_MS, (k * 10) as f64 + i as f64))
                .collect(),
            hist: if i == 1 {
                vec![t_end - 20 * SCRAPE_MS + 7_000]
            } else {
                Vec::new()
            },
        });
    }
    for i in 0..2u128 {
        out.push(SeedSeries {
            fp: 0x5792_0000_0000_0000_0000_0000_0000_0000 | i,
            metric: HIST_ONLY_METRIC.to_string(),
            labels: vec![("series".to_string(), i.to_string())],
            samples: Vec::new(),
            hist: (0..41).map(|k| t_end - (40 - k) * SCRAPE_MS).collect(),
        });
    }
    out
}

// ------------------------------------------------------------- seeding

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedSampleRow {
    org_id: String,
    fingerprint: u128,
    unix_milli: i64,
    value: f64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedHistRow {
    org_id: String,
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
struct SeedActivityRow {
    org_id: String,
    day: u16,
    fingerprint: u128,
    metric_name: String,
    hours: u32,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedLabelRow {
    org_id: String,
    metric_name: String,
    fingerprint: u128,
    labels: String,
    first_seen: i64,
    last_seen: i64,
}

/// Seeds `fx` the way the write path's views fill the tables: one activity
/// row and one label row per series, the label index rows, then the
/// samples.
pub async fn seed(client: &ChClient, fx: &[SeedSeries], seen_ms: i64) {
    let bucket = (seen_ms / ACTIVITY_BUCKET_MS) * ACTIVITY_BUCKET_MS;
    let activity: Vec<SeedActivityRow> = fx
        .iter()
        .map(|s| SeedActivityRow {
            org_id: String::new(),
            day: bucket.div_euclid(DAY_MS) as u16,
            fingerprint: s.fp,
            metric_name: s.metric.clone(),
            hours: 1u32 << (bucket.rem_euclid(DAY_MS) / 3_600_000),
        })
        .collect();
    let labels: Vec<SeedLabelRow> = fx
        .iter()
        .map(|s| {
            let map: BTreeMap<&str, &str> = s
                .labels
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            SeedLabelRow {
                org_id: String::new(),
                metric_name: s.metric.clone(),
                fingerprint: s.fp,
                labels: serde_json::to_string(&map).expect("labels json"),
                first_seen: bucket,
                last_seen: bucket,
            }
        })
        .collect();
    client
        .insert_block("metric_series", &activity)
        .await
        .expect("seed metric_series");
    client
        .insert_block("metric_labels", &labels)
        .await
        .expect("seed metric_labels");
    // Issue #635: the label index, as its two views derive it from the
    // same kind-2 row — a selector with no metric name and no `__name__`
    // matcher resolves through it.
    for sql in [
        "INSERT INTO metric_label_index (key, value, fingerprint) \
         SELECT kv.1, kv.2, fingerprint FROM metric_labels \
         ARRAY JOIN JSONExtractKeysAndValues(labels, 'String') AS kv",
        "INSERT INTO metric_label_values (key, value) \
         SELECT DISTINCT kv.1, kv.2 FROM metric_labels \
         ARRAY JOIN JSONExtractKeysAndValues(labels, 'String') AS kv",
    ] {
        client
            .execute(sql, &QuerySettings::new(), Idempotency::NonIdempotent)
            .await
            .unwrap_or_else(|e| panic!("seed the label index: {e}"));
    }
    let samples: Vec<SeedSampleRow> = fx
        .iter()
        .flat_map(|s| {
            s.samples.iter().map(move |(t, v)| SeedSampleRow {
                org_id: String::new(),
                fingerprint: s.fp,
                unix_milli: *t,
                value: *v,
            })
        })
        .collect();
    for block in samples.chunks(100_000) {
        client
            .insert_block("metric_samples", block)
            .await
            .expect("seed metric_samples");
    }
    let hist: Vec<SeedHistRow> = fx
        .iter()
        .flat_map(|s| {
            s.hist.iter().map(move |t| SeedHistRow {
                org_id: String::new(),
                fingerprint: s.fp,
                unix_milli: *t,
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

// ------------------------------------------------------------ the harness

pub fn test_config(database: &str) -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: database.to_string(),
        proto: ChProto::Http,
        pool_size: 8,
        query_timeout: Duration::from_secs(300),
        ..ChConnConfig::default()
    }
}

pub fn cache_config(db: &str) -> LabelCacheConfig {
    LabelCacheConfig {
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        db: db.to_string(),
        series_table: "metric_series".to_string(),
        labels_table: "metric_labels".to_string(),
        // Wider than the 24-hour range plus its range and lookback, so
        // the selectors resolve from the cache.
        window_ms: 3 * DAY_MS,
        cache_max_series: 50_000,
        ttl: Duration::from_secs(3_600),
        staleness_multiplier: 3,
    }
}

pub fn engine_config(db: &str, grouped_push: bool) -> MetricsConfig {
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
        cache_max_series: 50_000,
        max_info_series: 100_000,
        max_samples: 50_000_000,
        distributed: false,
        grouped_push,
    }
}

pub async fn drop_database(client: &ChClient, db: &str) {
    client
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
}

pub fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("now fits in i64")
}

/// One answer in a form two routes can be compared in: every series as
/// one line, labels sorted, floats as their bits so NaN equals NaN and
/// `-0.0` is not `0.0`, histograms by their `Debug` text.
pub fn canonical(r: &QueryResult) -> Vec<String> {
    fn labels(l: &[(String, String)]) -> String {
        let mut l = l.to_vec();
        l.sort();
        format!("{l:?}")
    }
    fn hof(v: &HistOrFloat) -> String {
        match v {
            HistOrFloat::Float(f) => format!("{:016x}", f.to_bits()),
            HistOrFloat::Hist(h) => format!("{h:?}"),
        }
    }
    let mut out: Vec<String> = match r {
        QueryResult::Vector(v) => v
            .iter()
            .map(|s| format!("{} {:016x}", labels(&s.labels), s.value.to_bits()))
            .collect(),
        QueryResult::Matrix(m) => m
            .iter()
            .map(|s| {
                let pts: Vec<String> = s
                    .points
                    .iter()
                    .map(|(t, v)| format!("{t}:{:016x}", v.to_bits()))
                    .collect();
                format!("{} {}", labels(&s.labels), pts.join(","))
            })
            .collect(),
        QueryResult::VectorHist(v) => v
            .iter()
            .map(|s| format!("{} {}", labels(&s.labels), hof(&s.value)))
            .collect(),
        QueryResult::MatrixHist(m) => m
            .iter()
            .map(|s| {
                let pts: Vec<String> = s
                    .points
                    .iter()
                    .map(|(t, v)| format!("{t}:{}", hof(v)))
                    .collect();
                format!("{} {}", labels(&s.labels), pts.join(","))
            })
            .collect(),
        QueryResult::Scalar(f) => vec![format!("scalar {:016x}", f.to_bits())],
        other => panic!("unexpected result shape: {other:?}"),
    };
    out.sort();
    out
}

fn annotation_lines(a: pulsus_promql::Annotations) -> Vec<String> {
    let (mut w, mut i) = a.base_messages();
    w.append(&mut i);
    w.sort();
    w
}

/// One route's answer: the result, its annotations, and its explain
/// stages as `(name, sql)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Routed {
    pub answer: Vec<String>,
    pub annotations: Vec<String>,
    pub stages: Vec<(String, String)>,
}

pub struct Harness {
    pub bootstrap: ChClient,
    pub admin: ChClient,
    pub db: String,
    pub cache: Arc<LabelCache>,
    pub pushed: MetricsEngine,
    pub unpushed: MetricsEngine,
    /// The last scrape and the range end, minute-aligned.
    pub t: i64,
}

/// Creates `db`, seeds `fx` at the anchor `t`, warms one label cache and
/// builds two engines over it, the push on and off. `cap` lowers the
/// pushed engine's series-steps cap.
pub async fn harness(db: &str, t: i64, fx: &[SeedSeries], cap: Option<usize>) -> Harness {
    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    drop_database(&bootstrap, db).await;
    run_init(&bootstrap, &RenderCtx::for_tests(db))
        .await
        .expect("run_init");
    let client = ChClient::new(test_config(db)).await.expect("connect");
    seed(&client, fx, t).await;
    let cache = Arc::new(LabelCache::new(
        ChClient::new(test_config(db)).await.expect("connect"),
        cache_config(db),
    ));
    cache.refresh().await.expect("refresh");
    assert!(cache.is_warm());
    let mut pushed = MetricsEngine::new(
        ChClient::new(test_config(db)).await.expect("connect"),
        Arc::clone(&cache),
        engine_config(db, true),
    );
    if let Some(cap) = cap {
        pushed = pushed.with_pushed_series_steps_cap(cap);
    }
    let unpushed = MetricsEngine::new(
        ChClient::new(test_config(db)).await.expect("connect"),
        Arc::clone(&cache),
        engine_config(db, false),
    );
    Harness {
        bootstrap,
        admin: client,
        db: db.to_string(),
        cache,
        pushed,
        unpushed,
        t,
    }
}

/// The minute-aligned anchor every corpus here is seeded against.
pub fn anchor() -> i64 {
    (now_ms() / 60_000) * 60_000
}

impl Harness {
    /// The 24-hour range ending at the anchor.
    pub fn day(&self, step_ms: i64) -> MetricQueryParams {
        MetricQueryParams {
            start_ms: self.t - DAY_MS,
            end_ms: self.t,
            step_ms,
        }
    }

    /// The ten minutes ending at the anchor.
    pub fn ten_minutes(&self, step_ms: i64) -> MetricQueryParams {
        MetricQueryParams {
            start_ms: self.t - 600_000,
            end_ms: self.t,
            step_ms,
        }
    }

    pub fn instant(&self) -> MetricQueryParams {
        MetricQueryParams {
            start_ms: self.t,
            end_ms: self.t,
            step_ms: 0,
        }
    }

    pub async fn run(engine: &MetricsEngine, query: &str, p: &MetricQueryParams) -> Routed {
        let expr = parse(query).expect("parse");
        let (r, a, e) = engine
            .query_explained(&no_tenant(), &expr, p)
            .await
            .unwrap_or_else(|err| panic!("{query}: {err:?}"));
        Routed {
            answer: canonical(&r),
            annotations: annotation_lines(a),
            stages: e
                .stages
                .into_iter()
                .map(|s| (s.name.to_string(), s.sql))
                .collect(),
        }
    }

    /// Both routes, pushed first.
    pub async fn both(&self, query: &str, p: &MetricQueryParams) -> (Routed, Routed) {
        (
            Self::run(&self.pushed, query, p).await,
            Self::run(&self.unpushed, query, p).await,
        )
    }

    /// Asserts both routes answer identically and returns the pushed one.
    pub async fn agree(&self, query: &str, p: &MetricQueryParams) -> Routed {
        let (a, b) = self.both(query, p).await;
        assert_eq!(
            a.answer, b.answer,
            "{query} at step {}: the pushed answer differs from the unpushed one",
            p.step_ms
        );
        assert_eq!(
            a.annotations, b.annotations,
            "{query} at step {}: the annotations differ",
            p.step_ms
        );
        a
    }

    pub async fn finish(self) {
        drop_database(&self.bootstrap, &self.db).await;
    }
}

/// The pushed route's statements for one node: every `pushed_aggregate`
/// stage that carries a statement rather than a decline.
pub fn pushed_statements(r: &Routed) -> Vec<&str> {
    r.stages
        .iter()
        .filter(|(name, sql)| name == "pushed_aggregate" && sql.starts_with("WITH "))
        .map(|(_, sql)| sql.as_str())
        .collect()
}

/// The pushed route's declines, as the text after `declined: `.
pub fn pushed_declines(r: &Routed) -> Vec<&str> {
    r.stages
        .iter()
        .filter(|(name, _)| name == "pushed_aggregate")
        .filter_map(|(_, sql)| sql.strip_prefix("declined: "))
        .collect()
}

// ---------------------------------------------------- the server's record

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct NowRow {
    t: String,
}

/// The server's clock, as the lower bound of a [`statements_since`] read.
pub async fn server_marker(admin: &ChClient) -> String {
    use futures::StreamExt;
    let mut stream = admin
        .query_stream::<NowRow>("SELECT toString(now64(6)) AS t", &QuerySettings::new())
        .await
        .expect("read the server clock");
    let mut out = String::new();
    while let Some(row) = stream.next().await {
        out = row.expect("decode").t;
    }
    assert!(!out.is_empty());
    out
}

/// One finished statement from `system.query_log`.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct LoggedStatement {
    pub query: String,
    pub result_rows: u64,
}

/// Every finished `SELECT` run in `db` since `marker`, oldest first.
///
/// The server can answer a statement before its `QueryFinish` row is
/// queued for the log, so a single flush can miss the last statement to
/// end. The read is repeated until two consecutive polls agree.
pub async fn statements_since(admin: &ChClient, db: &str, marker: &str) -> Vec<LoggedStatement> {
    use futures::StreamExt;
    let sql = format!(
        "SELECT query, toUInt64(result_rows) AS result_rows FROM system.query_log \
         WHERE current_database = '{db}' AND type = 'QueryFinish' AND query_kind = 'Select' \
           AND query_start_time_microseconds >= toDateTime64('{marker}', 6) \
           AND query NOT LIKE '%system.query_log%' AND query NOT LIKE '%now64(6)%' \
         ORDER BY query_start_time_microseconds"
    );
    let mut previous: Option<usize> = None;
    for _ in 0..30 {
        admin
            .execute(
                "SYSTEM FLUSH LOGS",
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("flush logs");
        let mut stream = admin
            .query_stream::<LoggedStatement>(&sql, &QuerySettings::new())
            .await
            .expect("read the query log");
        let mut out = Vec::new();
        while let Some(row) = stream.next().await {
            out.push(row.expect("decode"));
        }
        drop(stream);
        if previous == Some(out.len()) {
            return out;
        }
        previous = Some(out.len());
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("the query log never settled for {db}");
}

/// The single-tenant deployment's tenant: no `X-Scope-OrgID`.
#[allow(dead_code)]
fn no_tenant() -> pulsus_model::Tenant {
    pulsus_model::Tenant::from_header(None, false).expect("no header is the empty tenant")
}
