//! Issue #635 part 3: PromQL selectors with no metric name and no
//! `__name__` matcher, answered through the label index, against a live
//! ClickHouse.
//!
//! The corpus is part 2's statement at 2,000 series (`pod` modulo 40),
//! anchored at the current minute: every series active in the window holds
//! one float sample a minute for 30 minutes, valued `30 - k` at `k` minutes
//! before the anchor. Two series are added: `h_0{job="hist",
//! status="503"}`, holding only native-histogram samples (one a minute for
//! 30 minutes, count `30 - k`), and `l_0{job="edge"}`, whose one sample lies
//! two minutes before the range start. The engines scan at most 1,000 cache
//! entries, so the label cache refuses the name-less selectors here with
//! `CacheScan`.
//!
//! Every expected answer is computed here from the generator's own label
//! arrays and the sample rule above — never from either route.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-read --test live_metrics_nameless
//! ```

#[path = "pushed_rate_corpus/mod.rs"]
mod pushed_rate_corpus;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, Idempotency, QuerySettings};
use pulsus_promql::parser::parse;
use pulsus_read::{
    HistOrFloat, LabelCache, LabelCacheConfig, MatchOp, MetricQueryParams, MetricsConfig,
    MetricsEngine, QueryResult, ReadError,
};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;
use pushed_rate_corpus::{
    LoggedStatement, canonical, drop_database, server_marker, statements_since, test_config,
};

const DAY_MS: i64 = 86_400_000;
const MINUTE_MS: i64 = 60_000;
/// The range every range query here asks: 15 minutes at 60 s.
const RANGE_MS: i64 = 15 * MINUTE_MS;
/// The scan budget every engine here carries unless a test raises it.
const SCAN_BUDGET: u64 = 1_000;

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

fn engine_config(db: &str, grouped_push: bool) -> MetricsConfig {
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
        max_cache_scan: SCAN_BUDGET,
        cache_max_series: 50_000,
        max_info_series: 100_000,
        max_samples: 50_000_000,
        distributed: false,
        grouped_push,
    }
}

/// Six hours: wide enough for every query here, narrow enough that the
/// series active only the day before are not resident.
fn cache_config(db: &str) -> LabelCacheConfig {
    LabelCacheConfig {
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        db: db.to_string(),
        series_table: "metric_series".to_string(),
        labels_table: "metric_labels".to_string(),
        window_ms: 6 * 3_600_000,
        cache_max_series: 50_000,
        ttl: Duration::from_secs(3_600),
        staleness_multiplier: 3,
    }
}

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::NonIdempotent)
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
}

async fn strings(client: &ChClient, sql: &str) -> Vec<String> {
    client
        .query_strings(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"))
}

/// The ID rule of the design's corpus, over `metric_name` and `buf`.
const FINGERPRINT: &str =
    "bitOr(bitShiftLeft(toUInt128(bitShiftRight(cityHash64(metric_name), 32)), 96),
             bitAnd(bitOr(bitShiftLeft(toUInt128(cityHash64(buf)), 64), toUInt128(xxHash64(buf))),
                    toUInt128('79228162514264337593543950335')))";

/// Part 2's corpus statement at 2,000 series, `pod` modulo 40, each series
/// registered at `t_end`, or a day before for `s % 10 = 3` and `m_old`.
fn corpus_sql(t_end: i64) -> String {
    format!(
        "CREATE TABLE gen ENGINE = Memory AS
SELECT s, metric_name, ks, vs, {FINGERPRINT} AS fingerprint,
       {t_end} - if(s % 10 = 3 OR metric_name = 'm_old', {DAY_MS}, 0) AS unix_milli
FROM (
    SELECT s, metric_name, ks, vs,
           concat(metric_name, unhex('FF'), arrayStringConcat(arrayMap((k, v) -> concat(k, unhex('FF'), v, unhex('FF')), ks, vs), '')) AS buf
    FROM (
        SELECT number AS s,
               concat('m_', toString(intDiv(s, 1000))) AS metric_name,
               cityHash64(s, 'j') % 100 AS rj,
               cityHash64(s, 'st') % 100 AS rs,
               cityHash64(s, 'e') % 10 AS re,
               [['eu-1a', 'eu-1b', 'eu-1c'][s % 3 + 1],
                multiIf(re < 6, 'prod', re < 8, 'staging', re < 9, 'dev', s % 2 = 0, '', '~'),
                concat('host-', toString(cityHash64(s, 'i') % 5000)),
                multiIf(rj < 40, 'api', rj < 80, concat('api-', toString(rj % 20)), rj < 90, 'db', rj < 95, 'cache', 'worker'),
                concat('pod-', toString(cityHash64(s, 'p') % 40)),
                if(intDiv(s, 1000) % 4 = 0,
                   multiIf(rs < 60, '200', rs < 70, '201', rs < 75, '204', rs < 78, '301', rs < 80, '304', rs < 85, '400',
                           rs < 88, '401', rs < 90, '403', rs < 95, '404', rs < 97, '500', rs < 99, '502', '503'), '~'),
                concat('z', leftPad(toString(cityHash64(s, 'z') % 100), 2, '0'))] AS all_vs,
               arrayFilter((k, v) -> v != '~', ['az', 'env', 'instance', 'job', 'pod', 'status', 'zone'], all_vs) AS ks,
               arrayFilter(v -> v != '~', all_vs) AS vs
        FROM numbers(2000)
        UNION ALL
        SELECT 100000000 + number AS s, 'm_old', 0, 0, 0, [], ['job', 'old_key', 'status', 'zone'],
               ['api', concat('x', toString(number)), '503', 'z07']
        FROM numbers(7)
    )
)"
    )
}

/// P7's corpus: 20,000 series of one metric, all `job="big"`.
fn big_corpus_sql(t_end: i64) -> String {
    format!(
        "CREATE TABLE gen ENGINE = Memory AS
SELECT s, metric_name, ks, vs, {FINGERPRINT} AS fingerprint, {t_end} AS unix_milli
FROM (
    SELECT s, metric_name, ks, vs,
           concat(metric_name, unhex('FF'), arrayStringConcat(arrayMap((k, v) -> concat(k, unhex('FF'), v, unhex('FF')), ks, vs), '')) AS buf
    FROM (
        SELECT number AS s, 'big' AS metric_name, ['i', 'job'] AS ks, [toString(number), 'big'] AS vs
        FROM numbers(20000)
    )
)"
    )
}

const LANDING_FROM_GEN: &str = "INSERT INTO metric_landing (received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type)
SELECT toUnixTimestamp64Milli(now64(3)), 2, metric_name, fingerprint, unix_milli,
       concat('{', arrayStringConcat(arrayMap((k, v) -> concat('\"', k, '\":\"', v, '\"'), ks, vs), ','), '}'),
       0
FROM gen";

/// `h_0` and `l_0`, registered and sampled through `metric_landing` as a
/// push writes them.
fn extra_series_sql(t_end: i64) -> Vec<String> {
    let edge = t_end - RANGE_MS - 2 * MINUTE_MS;
    vec![
        format!(
            "INSERT INTO metric_landing (received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type) VALUES \
             (toUnixTimestamp64Milli(now64(3)), 2, 'h_0', 900001, {t_end}, '{{\"job\":\"hist\",\"status\":\"503\"}}', 1), \
             (toUnixTimestamp64Milli(now64(3)), 2, 'l_0', 900002, {edge}, '{{\"job\":\"edge\"}}', 0)"
        ),
        format!(
            "INSERT INTO metric_landing (received_ms, kind, metric_name, fingerprint, unix_milli, \
               hist_schema, hist_zero_threshold, hist_zero_count, hist_count, hist_sum, \
               hist_pos_span_offsets, hist_pos_span_lengths, hist_pos_bucket_deltas, hist_counter_reset_hint) \
             SELECT toUnixTimestamp64Milli(now64(3)), 1, 'h_0', 900001, {t_end} - number * {MINUTE_MS}, \
               0, 0, 0, 30 - number, 2 * (30 - number), [0], [1], [toInt64(30 - number)], 0 \
             FROM numbers(30)"
        ),
        format!(
            "INSERT INTO metric_landing (received_ms, kind, metric_name, fingerprint, unix_milli, value) \
             VALUES (toUnixTimestamp64Milli(now64(3)), 0, 'l_0', 900002, {edge}, 7)"
        ),
    ]
}

/// How a generated series holds its samples.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Holds {
    /// One float a minute for 30 minutes, `30 - k` at `k` minutes back.
    Floats,
    /// One histogram a minute for 30 minutes, count `30 - k`.
    Histograms,
    /// One float, 7, two minutes before the range start.
    OneEarlyFloat,
    /// Nothing: registered only the day before.
    Nothing,
}

#[derive(Debug, Clone)]
struct Generated {
    metric_name: String,
    labels: Vec<(String, String)>,
    holds: Holds,
}

/// One value as the comparison reads it.
#[derive(Debug, Clone, PartialEq)]
enum Val {
    Float(u64),
    Hist {
        count: f64,
        sum: f64,
        buckets: Vec<f64>,
    },
}

/// Series labels (sorted, `__name__` included) to their points.
type Answer = BTreeMap<Vec<(String, String)>, Vec<(i64, Val)>>;

struct Fixture {
    admin: ChClient,
    bootstrap: ChClient,
    db: String,
    cache: Arc<LabelCache>,
    /// The push on.
    pushed: MetricsEngine,
    /// The push off.
    unpushed: MetricsEngine,
    t_end: i64,
    series: Vec<Generated>,
}

async fn engine(db: &str, cache: &Arc<LabelCache>, config: MetricsConfig) -> MetricsEngine {
    MetricsEngine::new(
        ChClient::new(test_config(db)).await.expect("connect"),
        Arc::clone(cache),
        config,
    )
}

/// `db` is composed by `pulsus_testkit::test_db` at the call site, where
/// the naming guards read it. `big` builds P7's corpus instead of the
/// design's.
async fn fixture(db: &str, big: bool) -> Fixture {
    let db = db.to_string();
    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    drop_database(&bootstrap, &db).await;
    run_init(&bootstrap, &RenderCtx::for_tests(&db))
        .await
        .expect("run_init");
    let admin = ChClient::new(test_config(&db)).await.expect("connect");
    let t_end = (chrono::Utc::now().timestamp_millis() / MINUTE_MS) * MINUTE_MS;
    if big {
        exec(&admin, &big_corpus_sql(t_end)).await;
    } else {
        exec(&admin, &corpus_sql(t_end)).await;
    }
    exec(&admin, LANDING_FROM_GEN).await;
    let samples = if big { 1 } else { 30 };
    exec(
        &admin,
        &format!(
            "INSERT INTO metric_samples (fingerprint, unix_milli, value) \
             SELECT fingerprint, {t_end} - k * {MINUTE_MS}, toFloat64(30 - k) \
             FROM gen ARRAY JOIN range({samples}) AS k WHERE unix_milli = {t_end}"
        ),
    )
    .await;
    if !big {
        for sql in extra_series_sql(t_end) {
            exec(&admin, &sql).await;
        }
    }
    let mut series: Vec<Generated> = strings(
        &admin,
        &format!(
            "SELECT concat(metric_name, '\\t', toString(unix_milli = {t_end}), '\\t', \
             arrayStringConcat(arrayMap((k, v) -> concat(k, '=', v), ks, vs), '\\x01')) AS s FROM gen"
        ),
    )
    .await
    .into_iter()
    .map(|line| {
        let mut parts = line.splitn(3, '\t');
        let metric_name = parts.next().expect("name").to_string();
        let active = parts.next().expect("flag") == "1";
        let labels = parts
            .next()
            .unwrap_or("")
            .split('\x01')
            .filter(|p| !p.is_empty())
            .map(|p| {
                let (k, v) = p.split_once('=').expect("k=v");
                (k.to_string(), v.to_string())
            })
            .collect();
        Generated {
            metric_name,
            labels,
            holds: if active { Holds::Floats } else { Holds::Nothing },
        }
    })
    .collect();
    if !big {
        series.push(Generated {
            metric_name: "h_0".to_string(),
            labels: vec![
                ("job".to_string(), "hist".to_string()),
                ("status".to_string(), "503".to_string()),
            ],
            holds: Holds::Histograms,
        });
        series.push(Generated {
            metric_name: "l_0".to_string(),
            labels: vec![("job".to_string(), "edge".to_string())],
            holds: Holds::OneEarlyFloat,
        });
    }
    let cache = Arc::new(LabelCache::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache_config(&db),
    ));
    cache.refresh().await.expect("refresh");
    assert!(cache.is_warm());
    let pushed = engine(&db, &cache, engine_config(&db, true)).await;
    let unpushed = engine(&db, &cache, engine_config(&db, false)).await;
    Fixture {
        admin,
        bootstrap,
        db,
        cache,
        pushed,
        unpushed,
        t_end,
        series,
    }
}

impl Fixture {
    async fn finish(self) {
        drop_database(&self.bootstrap, &self.db).await;
    }

    fn range(&self) -> MetricQueryParams {
        MetricQueryParams {
            start_ms: self.t_end - RANGE_MS,
            end_ms: self.t_end,
            step_ms: MINUTE_MS,
        }
    }

    fn instant(&self) -> MetricQueryParams {
        MetricQueryParams {
            start_ms: self.t_end,
            end_ms: self.t_end,
            step_ms: 0,
        }
    }

    /// The steps of `p`, ascending.
    fn steps(&self, p: &MetricQueryParams) -> Vec<i64> {
        if p.step_ms == 0 {
            return vec![p.start_ms];
        }
        (0..)
            .map(|i| p.start_ms + i * p.step_ms)
            .take_while(|t| *t <= p.end_ms)
            .collect()
    }

    /// The answer `matchers` should give over `p`, from the generator.
    fn expected(&self, matchers: &[(&str, MatchOp, &str)], p: &MetricQueryParams) -> Answer {
        let mut out = Answer::new();
        for g in self
            .series
            .iter()
            .filter(|g| g.holds != Holds::Nothing && satisfies(&g.labels, matchers))
        {
            let mut points = Vec::new();
            for t in self.steps(p) {
                let back = (self.t_end - t) / MINUTE_MS;
                let value = match g.holds {
                    Holds::Floats => Some(Val::Float((30.0 - back as f64).to_bits())),
                    Holds::Histograms => {
                        let c = 30.0 - back as f64;
                        Some(Val::Hist {
                            count: c,
                            sum: 2.0 * c,
                            buckets: vec![c],
                        })
                    }
                    Holds::OneEarlyFloat => {
                        let sample = self.t_end - RANGE_MS - 2 * MINUTE_MS;
                        (sample > t - 5 * MINUTE_MS && sample <= t)
                            .then(|| Val::Float(7.0f64.to_bits()))
                    }
                    Holds::Nothing => None,
                };
                if let Some(v) = value {
                    points.push((t, v));
                }
            }
            if points.is_empty() {
                continue;
            }
            let mut labels = g.labels.clone();
            labels.push(("__name__".to_string(), g.metric_name.clone()));
            labels.sort();
            out.insert(labels, points);
        }
        out
    }

    /// The number of series the generator gives `matchers` in the window.
    fn count(&self, matchers: &[(&str, MatchOp, &str)]) -> usize {
        self.series
            .iter()
            .filter(|g| g.holds != Holds::Nothing && satisfies(&g.labels, matchers))
            .count()
    }

    async fn marker(&self) -> String {
        server_marker(&self.admin).await
    }

    async fn statements_since(&self, marker: &str) -> Vec<LoggedStatement> {
        statements_since(&self.admin, &self.db, marker).await
    }
}

/// Whether `labels` satisfy every matcher, an absent key read as `""`.
fn satisfies(labels: &[(String, String)], matchers: &[(&str, MatchOp, &str)]) -> bool {
    matchers.iter().all(|(key, op, value)| {
        let v = labels
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
            .unwrap_or("");
        let re = || regex::Regex::new(&format!("^(?:{value})$")).expect("a test regex");
        match op {
            MatchOp::Eq => v == *value,
            MatchOp::Neq => v != *value,
            MatchOp::Re => re().is_match(v),
            MatchOp::Nre => !re().is_match(v),
        }
    })
}

fn val(v: &HistOrFloat) -> Val {
    match v {
        HistOrFloat::Float(f) => Val::Float(f.to_bits()),
        HistOrFloat::Hist(h) => Val::Hist {
            count: h.count,
            sum: h.sum,
            buckets: h.positive_buckets.clone(),
        },
    }
}

fn sorted(labels: &[(String, String)]) -> Vec<(String, String)> {
    let mut l = labels.to_vec();
    l.sort();
    l
}

/// `r` as an [`Answer`]; an instant result's points carry `at`.
fn answer(r: &QueryResult, at: i64) -> Answer {
    match r {
        QueryResult::Vector(v) => v
            .iter()
            .map(|s| (sorted(&s.labels), vec![(at, Val::Float(s.value.to_bits()))]))
            .collect(),
        QueryResult::VectorHist(v) => v
            .iter()
            .map(|s| (sorted(&s.labels), vec![(at, val(&s.value))]))
            .collect(),
        QueryResult::Matrix(m) => m
            .iter()
            .map(|s| {
                (
                    sorted(&s.labels),
                    s.points
                        .iter()
                        .map(|(t, v)| (*t, Val::Float(v.to_bits())))
                        .collect(),
                )
            })
            .collect(),
        QueryResult::MatrixHist(m) => m
            .iter()
            .map(|s| {
                (
                    sorted(&s.labels),
                    s.points.iter().map(|(t, v)| (*t, val(v))).collect(),
                )
            })
            .collect(),
        other => panic!("unexpected result shape: {other:?}"),
    }
}

/// One query's result and its explain stages, or the error it answered.
struct Ran {
    result: QueryResult,
    stages: Vec<(String, String)>,
}

async fn run(engine: &MetricsEngine, query: &str, p: &MetricQueryParams) -> Result<Ran, ReadError> {
    let expr = parse(query).expect("parse");
    let (result, _, explain) = engine.query_explained(&expr, p).await?;
    Ok(Ran {
        result,
        stages: explain
            .stages
            .into_iter()
            .map(|s| (s.name.to_string(), s.sql))
            .collect(),
    })
}

/// Asserts `query` answers over `p` exactly as the generator says
/// `matchers` should.
async fn assert_answers(
    fx: &Fixture,
    engine: &MetricsEngine,
    query: &str,
    matchers: &[(&str, MatchOp, &str)],
    p: &MetricQueryParams,
) {
    let want = fx.expected(matchers, p);
    assert!(!want.is_empty(), "{query}: the corpus selects something");
    let got = run(engine, query, p)
        .await
        .unwrap_or_else(|e| panic!("{query} at step {}: {e:?}", p.step_ms));
    let got = answer(&got.result, p.start_ms);
    assert_eq!(
        got.len(),
        want.len(),
        "{query} at step {}: series count",
        p.step_ms
    );
    assert_eq!(got, want, "{query} at step {}", p.step_ms);
}

/// The text after a statement's last `LIMIT`, up to any `FORMAT` clause.
fn ends_with_limit(sql: &str, limit: u64) -> bool {
    let body = sql.trim_end();
    let body = body
        .rsplit_once("FORMAT ")
        .map_or(body, |(head, _)| head)
        .trim_end();
    body.ends_with(&format!("\nLIMIT {limit}"))
}

fn reads_label_index(s: &LoggedStatement) -> bool {
    s.query.contains("metric_label_index")
}

fn is_raw_sample_fetch(s: &LoggedStatement) -> bool {
    s.query
        .trim_start()
        .starts_with("SELECT fingerprint, unix_milli,")
}

const EXAMPLE: &str = r#"{job="api", status=~"5.."}"#;

fn example() -> Vec<(&'static str, MatchOp, &'static str)> {
    vec![("job", MatchOp::Eq, "api"), ("status", MatchOp::Re, "5..")]
}

/// **P1 and P2: the name-less selectors answer, in three statements.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p1_p2_name_less_selectors_answer_through_the_label_index() {
    skip_unless_live!();
    let fx = fixture(
        &pulsus_testkit::test_db("pulsus_read_it_nameless_p1"),
        false,
    )
    .await;
    let cases: [(&str, Vec<(&str, MatchOp, &str)>); 4] = [
        (EXAMPLE, example()),
        (r#"{status=~"5.."}"#, vec![("status", MatchOp::Re, "5..")]),
        (
            r#"{zone="z07", env=""}"#,
            vec![("zone", MatchOp::Eq, "z07"), ("env", MatchOp::Eq, "")],
        ),
        (
            r#"{zone="z07", env!~".+"}"#,
            vec![("zone", MatchOp::Eq, "z07"), ("env", MatchOp::Nre, ".+")],
        ),
    ];
    for (query, matchers) in &cases {
        for p in [fx.range(), fx.instant()] {
            assert_answers(&fx, &fx.pushed, query, matchers, &p).await;
        }
    }

    // P2: the example's range query sends exactly three statements.
    let marker = fx.marker().await;
    run(&fx.pushed, EXAMPLE, &fx.range())
        .await
        .expect("the example");
    let statements = fx.statements_since(&marker).await;
    let queries: Vec<&str> = statements.iter().map(|s| s.query.as_str()).collect();
    assert_eq!(statements.len(), 3, "{queries:#?}");
    let resolution: Vec<&LoggedStatement> = statements
        .iter()
        .filter(|s| reads_label_index(s) && ends_with_limit(&s.query, 50_001))
        .collect();
    assert_eq!(resolution.len(), 1, "statement R: {queries:#?}");
    let samples: Vec<&LoggedStatement> = statements
        .iter()
        .filter(|s| {
            s.query.contains("FROM metric_samples") || s.query.contains("FROM metric_hist_samples")
        })
        .collect();
    assert_eq!(samples.len(), 2, "the sample statements: {queries:#?}");
    for s in samples {
        assert!(
            s.query.contains("fingerprint IN (\nSELECT fingerprint\n"),
            "{}",
            s.query
        );
        assert!(!s.query.contains("toUInt128("), "{}", s.query);
    }
    fx.finish().await;
}

/// **P3: before the cache's first sweep, the example answers.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p3_a_cold_cache_does_not_stop_a_name_less_selector() {
    skip_unless_live!();
    let fx = fixture(
        &pulsus_testkit::test_db("pulsus_read_it_nameless_p3"),
        false,
    )
    .await;
    let cold = Arc::new(LabelCache::new(
        ChClient::new(test_config(&fx.db)).await.expect("connect"),
        cache_config(&fx.db),
    ));
    assert!(!cold.is_warm());
    let fresh = engine(&fx.db, &cold, engine_config(&fx.db, true)).await;
    for p in [fx.range(), fx.instant()] {
        assert_answers(&fx, &fresh, EXAMPLE, &example(), &p).await;
    }
    fx.finish().await;
}

/// **P4: past `cache_max_series` the selector answers the label cache's
/// own error for the case.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p4_more_series_than_the_cap_answers_over_cardinality() {
    skip_unless_live!();
    let fx = fixture(
        &pulsus_testkit::test_db("pulsus_read_it_nameless_p4"),
        false,
    )
    .await;
    let job_api = [("job", MatchOp::Eq, "api")];
    assert!(
        fx.count(&job_api) > 100,
        "{} series carry job=\"api\"",
        fx.count(&job_api)
    );
    assert!(
        fx.count(&example()) <= 100,
        "{} series match the example",
        fx.count(&example())
    );
    let capped = engine(
        &fx.db,
        &fx.cache,
        MetricsConfig {
            cache_max_series: 100,
            ..engine_config(&fx.db, true)
        },
    )
    .await;
    match run(&capped, r#"{job="api"}"#, &fx.range()).await {
        Err(ReadError::NamelessSelectorUnresolvable { reason }) => {
            assert!(reason.contains("OverCardinality"), "{reason}");
        }
        Err(other) => panic!("expected NamelessSelectorUnresolvable, got {other:?}"),
        Ok(_) => panic!("expected NamelessSelectorUnresolvable, got an answer"),
    }
    assert_answers(&fx, &capped, EXAMPLE, &example(), &fx.range()).await;
    fx.finish().await;
}

/// **P5: the pushed aggregates over a name-less selector.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p5_pushed_aggregates_over_a_name_less_selector() {
    skip_unless_live!();
    let fx = fixture(
        &pulsus_testkit::test_db("pulsus_read_it_nameless_p5"),
        false,
    )
    .await;
    for query in [
        format!("count({EXAMPLE})"),
        format!("count by (__name__) ({EXAMPLE})"),
        format!("sum by (status) (rate({EXAMPLE}[5m]))"),
    ] {
        for p in [fx.range(), fx.instant()] {
            let marker = fx.marker().await;
            let on = run(&fx.pushed, &query, &p)
                .await
                .unwrap_or_else(|e| panic!("{query}: {e:?}"));
            let statements = fx.statements_since(&marker).await;
            let off = run(&fx.unpushed, &query, &p)
                .await
                .unwrap_or_else(|e| panic!("{query}: {e:?}"));
            let on_answer = canonical(&on.result);
            assert!(!on_answer.is_empty(), "{query}: an answer");
            assert_eq!(
                on_answer,
                canonical(&off.result),
                "{query} at step {}: the push on and off differ",
                p.step_ms
            );
            let queries: Vec<&str> = statements.iter().map(|s| s.query.as_str()).collect();
            assert_eq!(
                statements
                    .iter()
                    .filter(|s| reads_label_index(s) && ends_with_limit(&s.query, 50_001))
                    .count(),
                1,
                "{query}: statement R once: {queries:#?}"
            );
            assert!(
                statements.len() > 1,
                "{query}: the pushed statements: {queries:#?}"
            );
            assert!(
                !statements.iter().any(is_raw_sample_fetch),
                "{query}: a raw sample fetch was sent: {queries:#?}"
            );
            assert!(
                on.stages
                    .iter()
                    .any(
                        |(name, sql)| (name == "pushed_aggregate" && sql.starts_with("WITH "))
                            || name == "grouped_fetch"
                    ),
                "{query}: no pushed statement in the plan: {:#?}",
                on.stages
            );
        }
    }
    fx.finish().await;
}

/// **P6: a selector with a `__name__` matcher keeps the label cache.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p6_a_name_matcher_keeps_the_label_cache() {
    skip_unless_live!();
    let fx = fixture(
        &pulsus_testkit::test_db("pulsus_read_it_nameless_p6"),
        false,
    )
    .await;
    let query = r#"{__name__=~"m_1.*", job="api"}"#;
    let marker = fx.marker().await;
    let got = run(&fx.pushed, query, &fx.range())
        .await
        .unwrap_or_else(|e| panic!("{query}: {e:?}"));
    let statements = fx.statements_since(&marker).await;
    let queries: Vec<&str> = statements.iter().map(|s| s.query.as_str()).collect();
    assert!(
        !statements.iter().any(reads_label_index),
        "a statement read the label index: {queries:#?}"
    );
    let want: Answer = fx
        .expected(&[("job", MatchOp::Eq, "api")], &fx.range())
        .into_iter()
        .filter(|(labels, _)| labels.contains(&("__name__".to_string(), "m_1".to_string())))
        .collect();
    assert!(!want.is_empty());
    assert_eq!(answer(&got.result, fx.t_end - RANGE_MS), want);
    fx.finish().await;
}

/// **P7: 20,000 series of one name-less selector, more than a literal ID
/// list can carry.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p7_twenty_thousand_series_answer() {
    skip_unless_live!();
    let fx = fixture(&pulsus_testkit::test_db("pulsus_read_it_nameless_p7"), true).await;
    let raised = engine(
        &fx.db,
        &fx.cache,
        MetricsConfig {
            max_cache_scan: 100_000_000,
            ..engine_config(&fx.db, true)
        },
    )
    .await;
    let got = run(&raised, r#"{job="big"}"#, &fx.instant())
        .await
        .unwrap_or_else(|e| panic!("{{job=\"big\"}}: {e:?}"));
    let got = answer(&got.result, fx.t_end);
    assert_eq!(got.len(), 20_000);
    assert_eq!(
        got,
        fx.expected(&[("job", MatchOp::Eq, "big")], &fx.instant())
    );
    fx.finish().await;
}

/// **P8, P9 and P10: a histogram-only series, a sample inside the lookback
/// before the range, and the pushed rate's histogram fallback.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p8_p9_p10_histograms_the_lookback_and_the_fallback() {
    skip_unless_live!();
    let fx = fixture(
        &pulsus_testkit::test_db("pulsus_read_it_nameless_p8"),
        false,
    )
    .await;

    // P8: the histogram-only series, at every step.
    let hist = [("job", MatchOp::Eq, "hist")];
    for p in [fx.range(), fx.instant()] {
        assert_answers(&fx, &fx.pushed, r#"{job="hist"}"#, &hist, &p).await;
    }
    assert_eq!(
        fx.expected(&hist, &fx.range())
            .values()
            .map(Vec::len)
            .sum::<usize>(),
        16,
        "the expectation covers every step"
    );

    // P9: the sample two minutes before the range start, at three steps.
    let edge = [("job", MatchOp::Eq, "edge")];
    let want = fx.expected(&edge, &fx.range());
    let start = fx.t_end - RANGE_MS;
    assert_eq!(
        want.values()
            .next()
            .map(|pts| pts.iter().map(|(t, _)| *t).collect::<Vec<_>>()),
        Some(vec![start, start + MINUTE_MS, start + 2 * MINUTE_MS]),
        "the expectation"
    );
    assert_answers(&fx, &fx.pushed, r#"{job="edge"}"#, &edge, &fx.range()).await;

    // P10: the pushed rate declines on the histogram samples and the
    // fallback reads them.
    let query = r#"sum(rate({job="hist"}[5m]))"#;
    let on = run(&fx.pushed, query, &fx.range())
        .await
        .unwrap_or_else(|e| panic!("{query}: {e:?}"));
    let off = run(&fx.unpushed, query, &fx.range())
        .await
        .unwrap_or_else(|e| panic!("{query}: {e:?}"));
    assert!(
        on.stages
            .iter()
            .any(|(name, sql)| name == "pushed_aggregate" && sql.contains("HistogramSamples")),
        "{:#?}",
        on.stages
    );
    assert_eq!(canonical(&on.result), canonical(&off.result));
    match &on.result {
        QueryResult::MatrixHist(m) => {
            assert_eq!(m.len(), 1, "{m:?}");
            assert!(!m[0].points.is_empty());
            assert!(
                m[0].points
                    .iter()
                    .all(|(_, v)| matches!(v, HistOrFloat::Hist(_))),
                "{m:?}"
            );
        }
        other => panic!("expected a histogram rate, got {other:?}"),
    }
    fx.finish().await;
}
