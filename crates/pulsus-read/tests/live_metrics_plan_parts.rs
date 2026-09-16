//! Issue #548 check 3: **the statements the database actually received,
//! and whether the plan names them.**
//!
//! # Why this suite exists
//!
//! The statement freeze (`tests/promql_statement_freeze.rs`) is a
//! characterization of `pulsus_promql::plan` and `metrics::sample_sql`.
//! It cannot see the engine calling those builders with different
//! arguments. Measured at the merge base: inserting
//!
//! ```text
//! let lower_excl = lower_excl + 1;
//! ```
//!
//! after `sel.fetch_window(..)` in `crates/pulsus-read/src/metrics/exec.rs`
//! shifts every fetch window the engine sends, and the freeze digest does
//! not move while all of `pulsus-read --lib` stays green.
//!
//! So this suite reads the statements back out of `system.query_log` and
//! compares them against text the test writes out itself:
//!
//! ```text
//!   the request                    the test's own constants
//!       |                                   |
//!       v                                   v
//!   MetricsEngine::query_explained     format!("SELECT fingerprint, ... unix_milli > {} ...",
//!       |                                      START_MS - 300_000)
//!       |  ClickHouse                         |
//!       v                                     |
//!   system.query_log.query  <--- equal? ------+
//!       |
//!       +--- tables ---- equal as a multiset? ---- plans[].parts[].name
//! ```
//!
//! **The expectation never calls `sample_sql`.** The literal below is a
//! second producer of the statement text; a test that rendered its
//! expectation with the code under test would agree with any window the
//! engine chose.
//!
//! **`SYSTEM FLUSH LOGS` is not a barrier** — it flushes what has been
//! buffered, not what is still in flight — so every read here is a poll
//! that stops when the row count is UNCHANGED across two consecutive
//! reads, never when it reaches a number the test wants. A query expected
//! to send nothing reaches the same stop.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`. To run it:
//!
//! ```text
//! podman run -d --name <your-container> -p $PORT_CH:8123 \
//!     clickhouse/clickhouse-server:26.3
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=$PORT_CH \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-read --test live_metrics_plan_parts
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_model::DEFAULT_ACTIVITY_BUCKET_MS;
use pulsus_promql::parser::parse;
use pulsus_read::{
    LabelCache, LabelCacheConfig, MetricQueryParams, MetricsConfig, MetricsEngine, PlanExplain,
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

// ------------------------------------------------------------ the fixture
//
// Four series of one metric, fingerprints 1..4, and two of a second,
// fingerprints 5 and 6. One of the four also carries a native-histogram
// sample, so the complementary histogram read returns rows without
// adding a fifth fingerprint to any `IN` list.

const METRIC: &str = "http_requests_total";
const ERRORS: &str = "http_errors_total";
const FPS: [u64; 4] = [1, 2, 3, 4];
const ERROR_FPS: [u64; 2] = [5, 6];
/// One lookback, written out rather than computed.
const LOOKBACK_MS: i64 = 300_000;
/// The `[5m]` range of the two-chain query, written out.
const RANGE_MS: i64 = 300_000;

/// A selector whose name matchers exclude its own concrete metric name,
/// in the one spelling the parser accepts (issue #85's duplicate
/// `__name__` matcher). One selector, no read, no plan.
const EXCLUDED_NAME: &str = "{__name__=\"up\",__name__=\"down\"}";

/// The thirteen histogram value columns, in the catalogue's own order,
/// written out here rather than read from `sample_sql` — this file is the
/// second producer of the statement text.
const HIST_COLUMNS: &str = "schema, zero_threshold, zero_count, count, sum, \
     pos_span_offsets, pos_span_lengths, pos_bucket_deltas, \
     neg_span_offsets, neg_span_lengths, neg_bucket_deltas, custom_values, \
     counter_reset_hint";

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

async fn init_db(bootstrap: &ChClient, db: &str) {
    bootstrap
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
    let params = RenderCtx {
        db: db.to_string(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
    };
    run_init(bootstrap, &params).await.expect("run_init");
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedSeriesRow {
    metric_name: String,
    fingerprint: u64,
    unix_milli: i64,
    labels: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedSampleRow {
    metric_name: String,
    fingerprint: u64,
    unix_milli: i64,
    value: f64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedHistRow {
    metric_name: String,
    fingerprint: u64,
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

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct StatementRow {
    query: String,
    tables: Vec<String>,
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
    }
}

async fn seed(client: &ChClient, t: i64, bucket: i64) {
    let mut series = Vec::new();
    let mut samples = Vec::new();
    for (i, fp) in FPS.iter().enumerate() {
        let labels: BTreeMap<&str, String> =
            BTreeMap::from([("status", "500".to_string()), ("instance", format!("i{i}"))]);
        series.push(SeedSeriesRow {
            metric_name: METRIC.to_string(),
            fingerprint: *fp,
            unix_milli: bucket,
            labels: serde_json::to_string(&labels).expect("labels json"),
        });
        samples.push(SeedSampleRow {
            metric_name: METRIC.to_string(),
            fingerprint: *fp,
            unix_milli: t,
            value: i as f64,
        });
    }
    for (i, fp) in ERROR_FPS.iter().enumerate() {
        let labels: BTreeMap<&str, String> =
            BTreeMap::from([("status", "500".to_string()), ("instance", format!("i{i}"))]);
        series.push(SeedSeriesRow {
            metric_name: ERRORS.to_string(),
            fingerprint: *fp,
            unix_milli: bucket,
            labels: serde_json::to_string(&labels).expect("labels json"),
        });
        samples.push(SeedSampleRow {
            metric_name: ERRORS.to_string(),
            fingerprint: *fp,
            unix_milli: t,
            value: i as f64,
        });
    }
    client
        .insert_block("metric_series", &series)
        .await
        .expect("seed metric_series");
    client
        .insert_block("metric_samples", &samples)
        .await
        .expect("seed metric_samples");
    // One histogram sample, on a fingerprint the float read already
    // carries, so the dual read has rows on both sides and no `IN` list
    // gains a member.
    let hist = vec![SeedHistRow {
        metric_name: METRIC.to_string(),
        fingerprint: FPS[1],
        unix_milli: t,
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
    }];
    client
        .insert_block("metric_hist_samples", &hist)
        .await
        .expect("seed metric_hist_samples");
}

// ------------------------------------------------------- the query log

async fn flush_logs(admin: &ChClient) {
    admin
        .execute(
            "SYSTEM FLUSH LOGS",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("flush logs");
}

async fn now_micros(admin: &ChClient) -> i64 {
    let mut stream = admin
        .query_stream::<CountRow>(
            "SELECT toUInt64(toUnixTimestamp64Micro(now64(6))) AS n",
            &QuerySettings::new(),
        )
        .await
        .expect("now64");
    i64::try_from(stream.next().await.expect("a row").expect("decode").n).expect("fits i64")
}

/// Every `Select` this database executed since `since_us`, with the
/// tables each one read.
///
/// `query_kind = 'Select'` excludes the seeding `Create` and `Insert`
/// rows, and the `system.query_log` exclusion keeps this counting
/// statement from counting itself.
async fn statements_since(admin: &ChClient, db: &str, since_us: i64) -> Vec<StatementRow> {
    let sql = format!(
        "SELECT query, tables FROM system.query_log \
         WHERE type = 'QueryFinish' \
           AND query_kind = 'Select' \
           AND has(databases, '{db}') \
           AND NOT has(tables, 'system.query_log') \
           AND toUInt64(toUnixTimestamp64Micro(event_time_microseconds)) > {since_us} \
         ORDER BY event_time_microseconds"
    );
    let mut stream = admin
        .query_stream::<StatementRow>(&sql, &QuerySettings::new())
        .await
        .expect("read the statement log");
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode"));
    }
    out
}

/// The statements, read until the count is **unchanged across two
/// consecutive reads**: a minimum of three polls, a cap of thirty, half a
/// second apart.
///
/// Stability, not a target count. A loop that waited until it saw the
/// number it wanted would not be an instrument, and the query expected to
/// send nothing has to reach the same stop.
async fn settled_statements(admin: &ChClient, db: &str, since_us: i64) -> Vec<StatementRow> {
    let mut previous: Option<usize> = None;
    let mut rows = Vec::new();
    for poll in 1..=30u32 {
        flush_logs(admin).await;
        rows = statements_since(admin, db, since_us).await;
        if poll >= 3 && previous == Some(rows.len()) {
            return rows;
        }
        previous = Some(rows.len());
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    rows
}

/// The statement as the engine sent it, with the wire format the client
/// appends removed — the read path asks for `RowBinary`, and that suffix
/// is the transport's, not the statement's.
fn without_format_clause(query: &str) -> &str {
    match query.rfind("\nFORMAT ") {
        Some(i) => query[..i].trim_end(),
        None => query.trim_end(),
    }
}

struct Harness {
    admin: ChClient,
    db: String,
    engine: MetricsEngine,
    t: i64,
}

impl Harness {
    /// The range this suite's queries run over: one hour, step 60 s.
    fn range(&self) -> MetricQueryParams {
        MetricQueryParams {
            start_ms: self.t - 3_600_000,
            end_ms: self.t,
            step_ms: 60_000,
        }
    }

    async fn explained(&self, query: &str) -> PlanExplain {
        let expr = parse(query).expect("parse");
        let (_result, _ann, explain) = self
            .engine
            .query_explained(&expr, &self.range())
            .await
            .expect("query_explained");
        explain
    }

    async fn unexplained(&self, query: &str) {
        let expr = parse(query).expect("parse");
        self.engine
            .query(&expr, &self.range())
            .await
            .expect("query");
    }

    /// The float read this test writes out for `metric`, over the window
    /// `(start - back, end]`, with `fps` as a rendered list.
    fn float_read(&self, metric: &str, fps: &str, back: i64) -> String {
        let p = self.range();
        format!(
            "SELECT fingerprint, unix_milli, value\nFROM metric_samples\nPREWHERE metric_name = \
             '{metric}'\nWHERE unix_milli > {} AND unix_milli <= {}\n  AND fingerprint IN \
             ({fps})\nORDER BY fingerprint, unix_milli",
            p.start_ms - back,
            p.end_ms
        )
    }

    /// The complementary histogram read, written out the same way.
    fn hist_read(&self, metric: &str, fps: &str, back: i64) -> String {
        let p = self.range();
        format!(
            "SELECT fingerprint, unix_milli, {HIST_COLUMNS}\nFROM \
             metric_hist_samples\nPREWHERE metric_name = '{metric}'\nWHERE unix_milli > {} AND \
             unix_milli <= {}\n  AND fingerprint IN ({fps})\nORDER BY fingerprint, unix_milli",
            p.start_ms - back,
            p.end_ms
        )
    }
}

async fn harness(db: &str) -> Harness {
    let admin = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let db = db.to_string();
    init_db(&admin, &db).await;
    let client = ChClient::new(test_config(&db)).await.expect("connect");
    let now = now_ms();
    let t = (now / 60_000) * 60_000;
    let bucket = (now / DEFAULT_ACTIVITY_BUCKET_MS) * DEFAULT_ACTIVITY_BUCKET_MS;
    seed(&client, t, bucket).await;

    let cache = Arc::new(LabelCache::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache_config(&db),
    ));
    cache.refresh().await.expect("refresh");
    assert!(cache.is_warm(), "the cache must resolve from memory");
    let engine = MetricsEngine::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache,
        engine_config(&db),
    );
    Harness {
        admin,
        db,
        engine,
        t,
    }
}

/// The SQL part names of every plan, in plan order.
fn plan_part_names(explain: &PlanExplain) -> Vec<String> {
    explain
        .plans
        .iter()
        .flat_map(|p| p.parts.iter())
        .filter_map(|part| match part {
            pulsus_read::compile::plan::PartShape::Sql(s) => Some(s.name.clone()),
            pulsus_read::compile::plan::PartShape::Engine(_) => None,
        })
        .collect()
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

// --------------------------------------------------------------- the tests

/// Criterion 10, half one: **every statement the database received is the
/// one the test wrote out.**
///
/// Five requests, and the expectation for each is a `format!` over this
/// file's own constants — the window as `start_ms - 300_000` arithmetic,
/// the fingerprints as the fixture's own literals. Nothing here calls
/// `sample_sql`.
///
/// This is the assertion the `+ 1` break on `exec.rs`'s fetch window
/// reddens and the hermetic freeze cannot.
#[tokio::test]
async fn every_statement_the_database_received_is_the_one_the_test_wrote_out() {
    skip_unless_live!();
    let h = harness(&pulsus_testkit::test_db("pulsus_read_it_plan_parts_stmts")).await;

    // 1 — the aggregation, with the header.
    let mark = now_micros(&h.admin).await;
    h.explained(&format!("max by (status) ({METRIC}{{status=\"500\"}})"))
        .await;
    let rows = settled_statements(&h.admin, &h.db, mark).await;
    let got: Vec<String> = sorted(
        rows.iter()
            .map(|r| without_format_clause(&r.query).to_string())
            .collect(),
    );
    let want = sorted(vec![
        h.float_read(METRIC, "1, 2, 3, 4", LOOKBACK_MS),
        h.hist_read(METRIC, "1, 2, 3, 4", LOOKBACK_MS),
    ]);
    assert_eq!(got, want, "the aggregation's two statements");

    // 2 — the same query WITHOUT the header sends exactly the same two
    // statements. The chain is built only under the header, and it must
    // not move a byte of what is sent.
    let mark = now_micros(&h.admin).await;
    h.unexplained(&format!("max by (status) ({METRIC}{{status=\"500\"}})"))
        .await;
    let rows = settled_statements(&h.admin, &h.db, mark).await;
    let unexplained: Vec<String> = sorted(
        rows.iter()
            .map(|r| without_format_clause(&r.query).to_string())
            .collect(),
    );
    assert_eq!(unexplained, want, "the unexplained request sends the same");

    // 3 — two chains: four statements, and the window carries the range
    // as well as the lookback.
    let mark = now_micros(&h.admin).await;
    h.explained(&format!(
        "rate({METRIC}{{status=\"500\"}}[5m]) / on (instance) rate({ERRORS}{{status=\"500\"}}[5m])"
    ))
    .await;
    let rows = settled_statements(&h.admin, &h.db, mark).await;
    let got: Vec<String> = sorted(
        rows.iter()
            .map(|r| without_format_clause(&r.query).to_string())
            .collect(),
    );
    let back = LOOKBACK_MS + RANGE_MS;
    let want_two = sorted(vec![
        h.float_read(METRIC, "1, 2, 3, 4", back),
        h.hist_read(METRIC, "1, 2, 3, 4", back),
        h.float_read(ERRORS, "5, 6", back),
        h.hist_read(ERRORS, "5, 6", back),
    ]);
    assert_eq!(got.len(), 4, "two selectors, two statements each");
    assert_eq!(got, want_two, "the two chains' four statements");

    // 4 — a query that sends nothing. The poll stops on stability without
    // ever seeing a row.
    let mark = now_micros(&h.admin).await;
    let explain = h.explained("time()").await;
    let rows = settled_statements(&h.admin, &h.db, mark).await;
    assert!(
        rows.is_empty(),
        "time() sends no statement; the log holds {rows:?}"
    );
    assert!(explain.plans.is_empty(), "and it carries no plan");

    // 5 — a selector the name matchers exclude: one selector, no read.
    //
    // **Not `up{__name__="down"}`**, which the vendored parser refuses
    // before planning — `metric name must not be set twice: 'up' or
    // 'down'`, measured at this head and the same refusal the reference
    // gives. The construct the engine's `Empty` arm is written for is the
    // duplicate `__name__` matcher of issue #85: this plans with the
    // concrete name `up` and one name matcher that excludes it, so the
    // selector resolves to no read at all.
    let mark = now_micros(&h.admin).await;
    let explain = h.explained(EXCLUDED_NAME).await;
    let rows = settled_statements(&h.admin, &h.db, mark).await;
    assert!(
        rows.is_empty(),
        "an excluded concrete name sends no statement; the log holds {rows:?}"
    );
    assert!(explain.plans.is_empty(), "and it carries no plan");
}

/// Criterion 10, half two: **the plan's SQL parts are the statements the
/// database received.**
///
/// The multiset of `tables` from the log equals the multiset of the
/// plans' SQL part names, per request — the model's own answerability,
/// which the statement-text half above cannot make.
#[tokio::test]
async fn the_plans_sql_parts_are_the_statements_the_database_received() {
    skip_unless_live!();
    let h = harness(&pulsus_testkit::test_db("pulsus_read_it_plan_parts_names")).await;

    let cases: [(String, usize); 5] = [
        (format!("max by (status) ({METRIC}{{status=\"500\"}})"), 1),
        (format!("{METRIC}{{status=\"500\"}}"), 1),
        (
            format!(
                "rate({METRIC}{{status=\"500\"}}[5m]) / on (instance) \
                 rate({ERRORS}{{status=\"500\"}}[5m])"
            ),
            2,
        ),
        ("time()".to_string(), 0),
        (EXCLUDED_NAME.to_string(), 0),
    ];

    for (query, plans) in cases {
        let mark = now_micros(&h.admin).await;
        let explain = h.explained(&query).await;
        let rows = settled_statements(&h.admin, &h.db, mark).await;

        assert_eq!(
            explain.plans.len(),
            plans,
            "{query}: one plan per selector that reads"
        );
        let received: Vec<String> = sorted(
            rows.iter()
                .flat_map(|r| r.tables.iter())
                .map(|t| {
                    t.strip_prefix(&format!("{}.", h.db))
                        .unwrap_or(t)
                        .to_string()
                })
                .collect(),
        );
        let named = sorted(plan_part_names(&explain));
        assert_eq!(
            received, named,
            "{query}: the tables the database read, against the plan's SQL part names"
        );
        // And the shape of each plan: two SQL parts, the second cut on
        // the two sources being disjoint, plus ONE engine part when there
        // is a residual link to put in it. A bare selector has none — its
        // whole chain is the source link, which lowers — so it is two
        // parts and no engine part, which is what this loop asserts
        // rather than assuming three parts everywhere.
        for plan in &explain.plans {
            let json = serde_json::to_value(plan).expect("serialize");
            assert_eq!(json["parts"][0]["name"], "metric_samples", "{query}");
            assert_eq!(json["parts"][1]["name"], "metric_hist_samples", "{query}");
            assert_eq!(
                json["parts"][1]["cut"]["why"], "disjoint_sources",
                "{query}"
            );
            assert_eq!(json["links"][0]["how"], "lowered", "{query}");
            assert_eq!(json["links"][0]["fidelity"], "wider", "{query}");
            if plan.links.len() > 1 {
                assert_eq!(json["parts"][2]["kind"], "engine", "{query}");
                assert_eq!(plan.parts.len(), 3, "{query}");
            } else {
                assert_eq!(
                    plan.parts.len(),
                    2,
                    "{query}: a chain whose only link lowers has no engine part"
                );
            }
        }
    }
}
