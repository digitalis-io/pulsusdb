//! Issue #18 part B: reading the rollup levels, against a live ClickHouse.
//!
//! Each case builds its own database with `run_init`, stops every level
//! view right after (part A's driving runs), lands its samples through
//! `metric_landing`, builds the first level by the first-level view's own
//! statement with its `plan` scalar replaced by a literal interval, one
//! statement per interval, and every later level by its view, run until
//! idle. Two engines read it over one label cache: one with the rollup
//! levels (routing on) and one without (routing off).
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-read --test live_metrics_rollup
//! ```

#[path = "pushed_rate_corpus/mod.rs"]
mod pushed_rate_corpus;

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, Idempotency, QuerySettings, Row};
use pulsus_model::Tenant;
use pulsus_promql::parser::parse;
use pulsus_read::logql::PlanExplain;
use pulsus_read::metrics::rollup::{LevelSlot, refresh_levels};
use pulsus_read::{HistOrFloat, LabelCache, MetricQueryParams, MetricsEngine, QueryResult};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;
use pushed_rate_corpus::{
    cache_config, canonical, drop_database, engine_config, now_ms, test_config,
};

const MIN: i64 = 60_000;
const HOUR: i64 = 3_600_000;
const DAY: i64 = 86_400_000;
const SCRAPE: i64 = 15_000;

static DB_B2: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_b2");
static DB_B3: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_b3");
static DB_B4: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_b4");
static DB_B5: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_b5");
static DB_B5B: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_b5b");
static DB_B1F: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_b1f");
static DB_B7: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_b7");
static DB_B10: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_b10");

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-read/tests/live_metrics_rollup.rs)"
            );
            return;
        }
    };
}

fn no_tenant() -> Tenant {
    Tenant::from_header(None, false).expect("the empty tenant")
}

fn tenant(name: &str) -> Tenant {
    let header = http::HeaderValue::from_str(name).expect("a header value");
    Tenant::from_header(Some(&header), false).expect("a valid tenant")
}

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::NonIdempotent)
        .await
        .unwrap_or_else(|e| panic!("execute failed: {e}\nSQL:\n{sql}"));
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct IntRow {
    v: i64,
}

async fn ints(client: &ChClient, sql: &str) -> Vec<i64> {
    let mut stream = client
        .query_stream::<IntRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\nSQL:\n{sql}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode").v);
    }
    out
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct RunPair {
    t_last: i64,
    t_end: i64,
}

/// Today's UTC midnight.
fn today() -> i64 {
    now_ms().div_euclid(DAY) * DAY
}

// ------------------------------------------------------------ building

/// The first-level view's statement as an insert over `(lo, hi]`: its
/// `plan` scalar replaced by the literal interval.
fn first_level_insert(ctx: &RenderCtx, lo: i64, hi: i64) -> String {
    let first = ctx.rollup_levels[0];
    let head = format!(
        "CREATE MATERIALIZED VIEW {}.metric_samples_{first}s_mv",
        ctx.db
    );
    let view = pulsus_schema::rendered_statements(ctx)
        .into_iter()
        .find(|s| s.starts_with(&head))
        .expect("the first level's view");
    let body = &view[view.find("AS WITH ").expect("AS WITH") + 3..];
    let start = body
        .find("(SELECT (lo, hi, lo < hi) FROM (")
        .expect("the plan scalar");
    let end = body[start..].find(") AS plan,").expect("the plan alias") + start;
    format!(
        "INSERT INTO {}.metric_samples_{first}s\n{}(toInt64({lo}), toInt64({hi}), true{}",
        ctx.db,
        &body[..start],
        &body[end..]
    )
}

/// Builds `ctx` fresh with every level view stopped and its table emptied.
async fn fresh(bootstrap: &ChClient, ctx: &RenderCtx) {
    drop_database(bootstrap, &ctx.db).await;
    run_init(bootstrap, ctx).await.expect("run_init");
    for r in &ctx.rollup_levels {
        let view = format!("{}.metric_samples_{r}s_mv", ctx.db);
        exec(bootstrap, &format!("SYSTEM WAIT VIEW {view}")).await;
        exec(bootstrap, &format!("SYSTEM STOP VIEW {view}")).await;
    }
    for r in &ctx.rollup_levels {
        exec(
            bootstrap,
            &format!("TRUNCATE TABLE {}.metric_samples_{r}s", ctx.db),
        )
        .await;
    }
}

/// The first level over `(lo, hi]`, one statement per `piece` ms.
async fn build_first(client: &ChClient, ctx: &RenderCtx, lo: i64, hi: i64, piece: i64) {
    let mut a = lo;
    while a < hi {
        let b = (a + piece).min(hi);
        exec(client, &first_level_insert(ctx, a, b)).await;
        a = b;
    }
}

/// Level `r`'s run rows.
async fn run_count(client: &ChClient, db: &str, r: u32) -> i64 {
    ints(
        client,
        &format!("SELECT toInt64(count()) AS v FROM {db}.metric_samples_{r}s WHERE kind = 1"),
    )
    .await[0]
}

/// Runs level `r`'s view until a run writes no run row.
async fn until_idle(client: &ChClient, db: &str, r: u32) {
    let view = format!("{db}.metric_samples_{r}s_mv");
    let mut before = run_count(client, db, r).await;
    for _ in 0..400 {
        for verb in ["START", "REFRESH", "WAIT", "STOP"] {
            exec(client, &format!("SYSTEM {verb} VIEW {view}")).await;
        }
        let now = run_count(client, db, r).await;
        if now == before {
            return;
        }
        before = now;
    }
    panic!("level {r}s still writes run rows after 400 runs");
}

/// Every level after the first, in order, each until idle.
async fn build_later(client: &ChClient, ctx: &RenderCtx) {
    for r in &ctx.rollup_levels[1..] {
        until_idle(client, &ctx.db, *r).await;
    }
}

/// Removes level `r`'s coverage of `(a, b]`: the run rows overlapping it
/// are deleted and their parts outside it written back. Data rows stay.
async fn cut_runs(client: &ChClient, db: &str, r: u32, a: i64, b: i64) {
    let table = format!("{db}.metric_samples_{r}s");
    let mut stream = client
        .query_stream::<RunPair>(
            &format!(
                "SELECT t_last, t_end FROM {table} WHERE kind = 1 AND t_end > {a} AND t_last < {b}"
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("read the run rows");
    let mut overlapping = Vec::new();
    while let Some(row) = stream.next().await {
        overlapping.push(row.expect("decode"));
    }
    exec(
        client,
        &format!(
            "ALTER TABLE {table} DELETE WHERE kind = 1 AND t_end > {a} AND t_last < {b} \
             SETTINGS mutations_sync = 2"
        ),
    )
    .await;
    for run in overlapping {
        for (lo, hi) in [
            (run.t_last, a.min(run.t_end)),
            (b.max(run.t_last), run.t_end),
        ] {
            if lo < hi {
                exec(
                    client,
                    &format!(
                        "INSERT INTO {table} (kind, org_id, fingerprint, t_end, t_last, built_at) \
                         VALUES (1, '', 0, {hi}, {lo}, 0)"
                    ),
                )
                .await;
            }
        }
    }
}

/// Corpus D's metric of series `s` (plan section 9).
const CORPUS_D_METRIC: &str =
    "multiIf(s < 100, 'c_normal', s < 120, 'c_fast', s < 140, 'c_stop', s < 160, 'c_sparse', 'g')";

/// Corpus D's series table, in the database: `s`, its metric, its labels
/// and its series ID (the scale corpus's ID formula).
fn corpus_d_series(db: &str) -> String {
    format!(
        "CREATE TABLE {db}.rd_series ENGINE = Memory AS \
         SELECT toUInt32(number) AS s, {CORPUS_D_METRIC} AS metric, \
                concat('{{\"s\":\"', toString(s), '\"}}') AS labels, \
                concat(metric, unhex('FF'), 's', unhex('FF'), toString(s), unhex('FF')) AS buf, \
                bitOr(bitShiftLeft(toUInt128(bitShiftRight(cityHash64(metric), 32)), 96), \
                      bitAnd(bitOr(bitShiftLeft(toUInt128(cityHash64(buf)), 64), toUInt128(xxHash64(buf))), \
                             toUInt128('79228162514264337593543950335'))) AS fp \
         FROM numbers(260)"
    )
}

/// Corpus D's sample time, phase and jitter, as SQL over `s` and `i`
/// (plan section 9).
fn corpus_d_time(day0: i64) -> String {
    format!(
        "toInt64({day0}) + 15000 * i + (100 + (7919 * s) % 14800) + (toInt64((7 * s + 13 * i) % 201) - 100)"
    )
}

/// Corpus D's value of series `s` at sample `i`, as SQL (plan section 9).
const CORPUS_D_VALUE: &str = "multiIf(metric = 'c_fast', toFloat64(i % 2) + 1, \
     metric = 'g', sin(i / 50) * 100, \
     toFloat64(i % (500 + (37 * s) % 300)))";

/// Corpus D, day `day` (0 to 7) after `day0`, exactly as plan section 9
/// defines it: every sample of the day, the stopping counters cut at
/// `day0 + 7 d 18 h 7 min 30 s` and the sparse ones keeping
/// `(3 s + 7 i) mod 10 ≥ 3`. Sample `i` of a day starting at `i = 5760 d`
/// lands inside that day, its phase and jitter keeping it within its
/// scrape interval.
fn corpus_d_day(db: &str, day0: i64, day: i64) -> String {
    let per_day = DAY / SCRAPE;
    let stop = day0 + 7 * DAY + 18 * HOUR + 450_000;
    format!(
        "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
         SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 0, metric, fp, t, v FROM ( \
           SELECT s, metric, fp, toInt64(n.number) AS i, {} AS t, {CORPUS_D_VALUE} AS v \
           FROM {db}.rd_series, numbers({}, {per_day}) AS n) \
         WHERE NOT (metric = 'c_stop' AND t > {stop}) \
           AND NOT (metric = 'c_sparse' AND (3 * s + 7 * i) % 10 < 3)",
        corpus_d_time(day0),
        day * per_day,
    )
}

/// Corpus D's series registrations: one per series and hour of `days`.
fn corpus_d_registrations(db: &str, from: i64, hours: i64) -> String {
    format!(
        "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type) \
         SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 2, metric, fp, \
                toInt64({from}) + toInt64(n.number) * {HOUR} + 1000, labels, 0 \
         FROM {db}.rd_series, numbers({hours}) AS n"
    )
}

/// Corpus D's five metrics and the kind each one's bounds are read by.
const CORPUS_D_METRICS: [(&str, &str); 5] = [
    ("c_normal", "normal"),
    ("c_fast", "fast"),
    ("c_stop", "stop"),
    ("c_sparse", "sparse"),
    ("g", "gauge"),
];

/// One built database and its two engines.
struct Built {
    bootstrap: ChClient,
    admin: ChClient,
    ctx: RenderCtx,
    cache: Arc<LabelCache>,
    slot: Arc<LevelSlot>,
}

impl Built {
    fn db(&self) -> &str {
        &self.ctx.db
    }

    async fn client(&self) -> ChClient {
        ChClient::new(test_config(self.db()))
            .await
            .expect("connect")
    }

    /// The engine with the levels (`routed`) or without.
    async fn engine(&self, routed: bool, comment: Option<&str>) -> MetricsEngine {
        let mut e = MetricsEngine::new(
            self.client().await,
            Arc::clone(&self.cache),
            engine_config(self.db(), false),
        );
        if routed {
            e = e.with_rollup_levels(Arc::clone(&self.slot));
        }
        if let Some(c) = comment {
            e = e.with_query_log_comment(c);
        }
        e
    }

    async fn finish(self) {
        drop_database(&self.bootstrap, &self.ctx.db).await;
    }
}

/// Creates `db` with `levels`, runs `land` in it, builds the first level
/// over `(lo, hi]` one hour at a time and every later level until idle,
/// and warms one label cache for `tenants` (the empty tenant always).
async fn build(
    db: &str,
    levels: &[u32],
    land: &[String],
    lo: i64,
    hi: i64,
    tenants: &[Tenant],
) -> Built {
    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let ctx = RenderCtx {
        rollup_levels: levels.to_vec(),
        ..RenderCtx::for_tests(db, 30)
    };
    fresh(&bootstrap, &ctx).await;
    let admin = ChClient::new(test_config(db)).await.expect("connect");
    for sql in land {
        exec(&admin, sql).await;
    }
    build_first(&admin, &ctx, lo, hi, HOUR).await;
    build_later(&admin, &ctx).await;
    warm(bootstrap, admin, ctx, tenants).await
}

/// The label cache and the level slot over a database already built.
async fn warm(bootstrap: ChClient, admin: ChClient, ctx: RenderCtx, tenants: &[Tenant]) -> Built {
    let mut cfg = cache_config(&ctx.db);
    cfg.window_ms = 10 * DAY;
    let cache = Arc::new(LabelCache::new(
        ChClient::new(test_config(&ctx.db)).await.expect("connect"),
        cfg,
    ));
    cache.refresh().await.expect("refresh");
    for t in tenants {
        cache.refresh_tenant(t).await.expect("refresh the tenant");
    }
    assert!(cache.is_warm());
    let slot = Arc::new(LevelSlot::new());
    refresh_levels(&slot, &admin, &ctx.db, &ctx.dist_suffix, None)
        .await
        .expect("level discovery");
    Built {
        bootstrap,
        admin,
        ctx,
        cache,
        slot,
    }
}

/// Corpus D over days `days` after `day0`, built at `levels`.
async fn corpus_d(db: &str, levels: &[u32], day0: i64, days: std::ops::Range<i64>) -> Built {
    let mut land = vec![corpus_d_series(db)];
    land.push(corpus_d_registrations(
        db,
        day0 + days.start * DAY,
        (days.end - days.start) * 24,
    ));
    for d in days.clone() {
        land.push(corpus_d_day(db, day0, d));
    }
    build(
        db,
        levels,
        &land,
        day0 + days.start * DAY,
        day0 + days.end * DAY,
        &[],
    )
    .await
}

// ------------------------------------------------------------- reading

/// One route's answer and its explain trace.
struct Answer {
    result: QueryResult,
    explain: PlanExplain,
}

impl Answer {
    /// `(stage, note)` of every stage named `name`.
    fn stages(&self, name: &str) -> Vec<String> {
        self.explain
            .stages
            .iter()
            .filter(|s| s.name == name)
            .map(|s| s.note.clone().unwrap_or_default())
            .collect()
    }

    /// Every series' points, by labels then time.
    fn points(&self) -> BTreeMap<(String, i64), f64> {
        let mut out = BTreeMap::new();
        match &self.result {
            QueryResult::Matrix(m) => {
                for s in m {
                    let mut l = s.labels.clone();
                    l.sort();
                    let key = format!("{l:?}");
                    for (t, v) in &s.points {
                        out.insert((key.clone(), *t), *v);
                    }
                }
            }
            QueryResult::Vector(v) => {
                for s in v {
                    let mut l = s.labels.clone();
                    l.sort();
                    out.insert((format!("{l:?}"), 0), s.value);
                }
            }
            other => panic!("unexpected result shape: {other:?}"),
        }
        out
    }

    /// Every series' points, floats as their bits and histograms by their
    /// `Debug` text, by labels then time.
    fn cells(&self) -> BTreeMap<(String, i64), String> {
        fn key(l: &[(String, String)]) -> String {
            let mut l = l.to_vec();
            l.sort();
            format!("{l:?}")
        }
        let mut out = BTreeMap::new();
        match &self.result {
            QueryResult::Matrix(m) => {
                for s in m {
                    for (t, v) in &s.points {
                        out.insert((key(&s.labels), *t), format!("{:016x}", v.to_bits()));
                    }
                }
            }
            QueryResult::MatrixHist(m) => {
                for s in m {
                    for (t, v) in &s.points {
                        let text = match v {
                            HistOrFloat::Float(f) => format!("{:016x}", f.to_bits()),
                            HistOrFloat::Hist(h) => format!("{h:?}"),
                        };
                        out.insert((key(&s.labels), *t), text);
                    }
                }
            }
            other => panic!("unexpected result shape: {other:?}"),
        }
        out
    }
}

async fn ask(engine: &MetricsEngine, t: &Tenant, query: &str, p: &MetricQueryParams) -> Answer {
    let expr = parse(query).expect("parse");
    let (result, _, explain) = engine
        .query_explained(t, &expr, p)
        .await
        .unwrap_or_else(|e| panic!("{query}: {e:?}"));
    Answer { result, explain }
}

fn range(start: i64, end: i64, step: i64) -> MetricQueryParams {
    MetricQueryParams {
        start_ms: start,
        end_ms: end,
        step_ms: step,
    }
}

/// The distance of plan section 9 between a routed answer `a` and the
/// routing-off answer `b`, one per series and step present on either.
fn distances(a: &BTreeMap<(String, i64), f64>, b: &BTreeMap<(String, i64), f64>) -> Vec<f64> {
    let keys: std::collections::BTreeSet<&(String, i64)> = a.keys().chain(b.keys()).collect();
    keys.into_iter()
        .filter_map(|key| d_of(a.get(key), b.get(key)))
        .collect()
}

/// Plan section 9's `d` of one series and step.
fn d_of(a: Option<&f64>, b: Option<&f64>) -> Option<f64> {
    match (a, b) {
        (Some(x), Some(y)) if *y == 0.0 => Some(x.abs()),
        (Some(x), Some(y)) => Some((x - y).abs() / y.abs()),
        (Some(_), None) | (None, Some(_)) => Some(1.0),
        (None, None) => None,
    }
}

/// Nearest-rank p99 and the maximum.
fn p99_max(d: &[f64]) -> (f64, f64) {
    let mut d = d.to_vec();
    d.sort_by(f64::total_cmp);
    let n = d.len();
    let rank = ((0.99 * n as f64).ceil() as usize).max(1) - 1;
    (d[rank], d[n - 1])
}

/// `true` when both answers are equal as bits, presence included.
fn bit_equal(a: &BTreeMap<(String, i64), f64>, b: &BTreeMap<(String, i64), f64>) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|((ka, va), (kb, vb))| ka == kb && va.to_bits() == vb.to_bits())
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ReadRowsRow {
    log_comment: String,
    rows: u64,
}

/// `read_rows` per `log_comment`, for the comments `prefix` starts.
async fn read_rows(admin: &ChClient, prefix: &str) -> BTreeMap<String, u64> {
    exec(admin, "SYSTEM FLUSH LOGS").await;
    let mut stream = admin
        .query_stream::<ReadRowsRow>(
            &format!(
                "SELECT log_comment, toUInt64(sum(read_rows)) AS rows FROM system.query_log \
                 WHERE type = 'QueryFinish' AND startsWith(log_comment, '{prefix}') \
                 GROUP BY log_comment"
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("query_log");
    let mut out = BTreeMap::new();
    while let Some(row) = stream.next().await {
        let row = row.expect("decode");
        out.insert(row.log_comment, row.rows);
    }
    out
}

// --------------------------------------------------------------- cases

/// The bound of plan section 9 for `function` on `kind` at level index
/// `li` (60 s, 720 s, 7200 s, 86400 s); `None` for bit-equal.
fn bound(function: &str, kind: &str, li: usize) -> Option<f64> {
    let row: [f64; 4] = match (function, kind) {
        ("rate" | "increase", "normal") => [0.0218, 0.00828, 0.000826, 0.0000684],
        ("rate" | "increase", "fast") => [0.2, 0.00826, 0.000822, 0.0000686],
        ("rate" | "increase", "stop") => [0.0218, 0.141, 1.0, 1.0],
        ("rate" | "increase", "sparse") => [0.42, 0.049, 0.00494, 0.000368],
        ("sum_over_time", _) => [1e-12; 4],
        ("avg_over_time", "gauge") => [0.0057, 0.0214, 0.0214, 0.0214],
        _ => return None,
    };
    Some(row[li])
}

/// One query sent with routing on and off, concurrently, each with its
/// own query-log comment.
async fn both(
    built: &Built,
    query: &str,
    p: &MetricQueryParams,
    comments: Option<(&str, &str)>,
) -> (Answer, Answer) {
    let routed = built.engine(true, comments.map(|c| c.0)).await;
    let raw = built.engine(false, comments.map(|c| c.1)).await;
    let t = no_tenant();
    futures::join!(ask(&routed, &t, query, p), ask(&raw, &t, query, p))
}

/// **B2 (issue #18 part B): each served function is within its distance
/// from raw.** Corpus D at every level; per level `res`: `rate` and
/// `increase` over `5 × res` on each counter metric, `avg_over_time` on
/// the gauges, every other served function on every metric, at step
/// `res`, aligned; each sent with routing on and off. The routed explain
/// names the level and reads `exactness` `"rollup"`; each function is
/// within the bound of plan section 9 (bit-equal where it says so).
#[tokio::test(flavor = "multi_thread")]
async fn each_served_function_is_within_its_distance() {
    skip_unless_live!();
    let day0 = today() - 8 * DAY;
    let end = day0 + 8 * DAY;
    let built = corpus_d(
        DB_B2.name(),
        pulsus_schema::DEFAULT_ROLLUP_LEVELS,
        day0,
        0..8,
    )
    .await;
    let levels = built.slot.get().expect("the levels are read");
    assert_eq!(levels.levels.len(), 4, "{levels:?}");

    let mut failures = Vec::new();
    let mut report = Vec::new();
    let mut comments = Vec::new();
    // Each run's statements carry their own comment, so the query log's
    // rows of an earlier run are not summed into this one's.
    let run = format!("b2_{}_{}", built.db(), now_ms());
    for (li, secs) in [60i64, 720, 7200, 86400].into_iter().enumerate() {
        let res = secs * 1000;
        let span = match secs {
            60 => DAY,
            720 => 3 * DAY,
            _ => 8 * DAY,
        };
        let start = (end - span).max(day0 + 5 * res);
        let p = range(start, end, res);
        // (function, kind, query)
        let mut queries: Vec<(&str, &str, String)> = Vec::new();
        for (metric, kind) in CORPUS_D_METRICS {
            if kind != "gauge" {
                for f in ["rate", "increase"] {
                    queries.push((f, kind, format!("{f}({metric}[{}s])", 5 * secs)));
                }
            }
            for f in [
                "max_over_time",
                "min_over_time",
                "sum_over_time",
                "last_over_time",
                "present_over_time",
                "absent_over_time",
            ] {
                for r in [secs, 5 * secs] {
                    queries.push((f, kind, format!("{f}({metric}[{r}s])")));
                }
            }
            if kind == "gauge" {
                for r in [secs, 5 * secs] {
                    queries.push((
                        "avg_over_time",
                        kind,
                        format!("avg_over_time({metric}[{r}s])"),
                    ));
                }
            }
            queries.push(("timestamp", kind, format!("timestamp({metric})")));
            queries.push(("bare", kind, metric.to_string()));
        }

        for (qi, (function, kind, query)) in queries.iter().enumerate() {
            let on_c = format!("{run}_{secs}_{qi}_on");
            let off_c = format!("{run}_{secs}_{qi}_off");
            let (a, b) = both(&built, query, &p, Some((&on_c, &off_c))).await;
            comments.push((query.clone(), secs, on_c, off_c));
            let route = a.stages("rollup_route");
            let table = format!("metric_samples_{secs}s,");
            if !route.iter().any(|n| n.starts_with(&table)) || !a.explain.rollup {
                failures.push(format!(
                    "{query} at {secs}s: not served from {table} (route {route:?}, rollup {})",
                    a.explain.rollup
                ));
                continue;
            }
            let (pa, pb) = (a.points(), b.points());
            let d = distances(&pa, &pb);
            if d.is_empty() {
                report.push(format!(
                    "{function:<18} {query:<40} {secs:>6}s (no points on either route)"
                ));
                continue;
            }
            let (p99, max) = p99_max(&d);
            report.push(format!(
                "{function:<18} {query:<40} {secs:>6}s {kind:<7} n {:>7} p99 {p99:.6e} max {max:.6e}",
                d.len()
            ));
            match bound(function, kind, li) {
                Some(limit) if p99 > limit => failures.push(format!(
                    "{query} at {secs}s, {kind}: p99 {p99:e} above the bound {limit:e}"
                )),
                Some(_) => {}
                None if !matches!(
                    *function,
                    "rate" | "increase" | "avg_over_time" | "sum_over_time"
                ) && !bit_equal(&pa, &pb) =>
                {
                    failures.push(format!("{query} at {secs}s: not bit-equal"))
                }
                None => {}
            }
        }
    }
    let rows = read_rows(&built.admin, &format!("{run}_")).await;
    for (query, secs, on_c, off_c) in &comments {
        report.push(format!(
            "read_rows {query:<40} {secs:>6}s routed {:>10} raw {:>10}",
            rows.get(on_c).copied().unwrap_or(0),
            rows.get(off_c).copied().unwrap_or(0)
        ));
    }
    for line in &report {
        println!("{line}");
    }
    built.finish().await;
    assert!(failures.is_empty(), "B2 failures:\n{}", failures.join("\n"));
}

/// The upper bound of a `rollup_piece` note's interval, and its source:
/// `"raw"` or the table.
fn piece_of(note: &str) -> (String, i64) {
    let source = note.split([' ', '.']).next().unwrap_or("").to_string();
    let hi = note
        .rsplit(", ")
        .next()
        .and_then(|s| s.strip_suffix(']'))
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no interval in {note:?}"));
    (source, hi)
}

/// **B3 (issue #18 part B): a window reads the finer levels and raw after
/// its level.** Corpus D's last day; the 720 s level's coverage cut after
/// `B`, the 60 s level's after `B + 30 m`, `B` a 720 s boundary three
/// hours before the data's end. A 12-hour `last_over_time` at step 12 m
/// lists the 720 s piece up to `B`, the 60 s piece `(B, B + 30 m]`, then
/// raw to the end, in that order, and its last step equals the raw
/// route's.
#[tokio::test(flavor = "multi_thread")]
async fn a_window_reads_the_finer_levels_and_raw_after_its_level() {
    skip_unless_live!();
    let day0 = today() - 8 * DAY;
    let end = day0 + 8 * DAY;
    let built = corpus_d(
        DB_B3.name(),
        pulsus_schema::DEFAULT_ROLLUP_LEVELS,
        day0,
        7..8,
    )
    .await;
    let b = end - 3 * HOUR;
    for r in [86_400, 7_200, 720] {
        cut_runs(&built.admin, built.db(), r, b, end + DAY).await;
    }
    cut_runs(&built.admin, built.db(), 60, b + 30 * MIN, end + DAY).await;

    let p = range(end - 12 * HOUR, end, 12 * MIN);
    let query = "last_over_time(c_normal[12m])";
    let a = ask(&built.engine(true, None).await, &no_tenant(), query, &p).await;
    let raw = ask(&built.engine(false, None).await, &no_tenant(), query, &p).await;
    let pieces: Vec<(String, i64)> = a
        .stages("rollup_piece")
        .iter()
        .map(|n| piece_of(n))
        .collect();
    assert_eq!(
        pieces,
        vec![
            ("metric_samples_720s".to_string(), b),
            ("metric_samples_60s".to_string(), b + 30 * MIN),
            ("raw".to_string(), end),
        ],
        "{:?}",
        a.stages("rollup_piece")
    );
    let last = |x: &Answer| -> Vec<(String, u64)> {
        x.points()
            .into_iter()
            .filter(|((_, t), _)| *t == end)
            .map(|((l, _), v)| (l, v.to_bits()))
            .collect()
    };
    assert!(!last(&raw).is_empty());
    assert_eq!(last(&a), last(&raw), "the last step");
    built.finish().await;
}

/// B4 (iii): the per-step distance a stopped or stale counter may read
/// at 720 s and coarser, twice the 0.0308 measured (owner decision,
/// 2026-10-25).
const STOPPED_STEP_ALLOWANCE_720S: f64 = 2.0 * 0.0308;

/// Corpus D's sample time of series `s`, sample `i` (plan section 9).
fn t_of(day0: i64, s: i64, i: i64) -> i64 {
    day0 + 15_000 * i + 100 + (7919 * s) % 14_800 + (7 * s + 13 * i) % 201 - 100
}

/// `c_normal`'s value of series `s` at sample `i`.
fn normal_v(s: i64, i: i64) -> f64 {
    (i % (500 + (37 * s) % 300)) as f64
}

/// The sample numbers of series `s` whose times lie in `[a, b]`.
fn samples_in(day0: i64, s: i64, a: i64, b: i64) -> Vec<i64> {
    let first = (a - day0) / 15_000 - 2;
    (first..first + (b - a) / 15_000 + 4)
        .filter(|&i| (a..=b).contains(&t_of(day0, s, i)))
        .collect()
}

/// B4's own series beside corpus D, landed with it before the levels are
/// built: `c_stale`, `c_mixed`, `h` and `c_iv`. `floats` are
/// `(metric, t, v)`, `hists` `(metric, t)`.
fn b4_land(
    db: &str,
    day0: i64,
    w: i64,
    floats: &[(&str, i64, f64)],
    hists: &[(&str, i64)],
) -> Vec<String> {
    let mut out = vec![format!(
        "CREATE TABLE {db}.rd_extra ENGINE = Memory AS \
         SELECT s, metric, concat('{{\"s\":\"', toString(s), '\"}}') AS labels, \
                concat(metric, unhex('FF'), 's', unhex('FF'), toString(s), unhex('FF')) AS buf, \
                bitOr(bitShiftLeft(toUInt128(bitShiftRight(cityHash64(metric), 32)), 96), \
                      bitAnd(bitOr(bitShiftLeft(toUInt128(cityHash64(buf)), 64), toUInt128(xxHash64(buf))), \
                             toUInt128('79228162514264337593543950335'))) AS fp \
         FROM values('s UInt32, metric String', (0, 'c_stale'), (1, 'c_mixed'), (0, 'h'), (0, 'c_iv'))"
    )];
    out.push(format!(
        "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type) \
         SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 2, metric, fp, \
                toInt64({}) + toInt64(n.number) * {HOUR} + 1000, labels, 0 \
         FROM {db}.rd_extra, numbers(24) AS n",
        day0 + 7 * DAY
    ));
    for chunk in floats.chunks(2_000) {
        let values: Vec<String> = chunk
            .iter()
            .map(|(m, t, v)| {
                format!(
                    "('{m}', {t}, reinterpretAsFloat64(toUInt64({})))",
                    v.to_bits()
                )
            })
            .collect();
        out.push(format!(
            "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
             SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 0, e.metric, e.fp, x.t, x.v \
             FROM (SELECT * FROM values('m String, t Int64, v Float64', {})) AS x \
             JOIN {db}.rd_extra AS e ON e.metric = x.m",
            values.join(", ")
        ));
    }
    let _ = w;
    for chunk in hists.chunks(2_000) {
        let values: Vec<String> = chunk.iter().map(|(m, t)| format!("('{m}', {t})")).collect();
        out.push(format!(
            "INSERT INTO {db}.metric_hist_samples (org_id, fingerprint, unix_milli, schema, zero_threshold, zero_count, \
                                                   count, sum, pos_span_offsets, pos_span_lengths, pos_bucket_deltas, \
                                                   neg_span_offsets, neg_span_lengths, neg_bucket_deltas, custom_values) \
             SELECT '', e.fp, x.t, 0, 0., 0, toUInt64(4 + intDiv(x.t, 15000) % 1000), \
                    5. + toFloat64(intDiv(x.t, 15000) % 1000), [0], [3], [1, 1, -1], [], [], [], [] \
             FROM (SELECT * FROM values('m String, t Int64', {})) AS x \
             JOIN {db}.rd_extra AS e ON e.metric = x.m",
            values.join(", ")
        ));
    }
    out
}

/// One answer's points by series and step.
type Points<V> = BTreeMap<(String, i64), V>;

/// The points of `x` whose step is after `w`, and those at or before it.
fn split_at_w<V: Clone>(x: &Points<V>, w: i64) -> (Points<V>, Points<V>) {
    let after = x
        .iter()
        .filter(|((_, t), _)| *t > w)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let before = x
        .iter()
        .filter(|((_, t), _)| *t <= w)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    (after, before)
}

/// **B4 (issue #18 part B): a window touching raw is raw.** Corpus D's
/// last day at every level, with `c_stale`, `c_mixed`, `h` and `c_iv`
/// beside it; `W = day0 + 7 d 18 h`. (i) to (ii) and (v) with the 60 s
/// level's coverage cut after `W`; (iii) and (iv) with the 720 s level's
/// cut after `W` too, against the same step read before that cut.
#[tokio::test(flavor = "multi_thread")]
async fn a_window_touching_raw_is_raw() {
    skip_unless_live!();
    let db = DB_B4.name();
    let day0 = today() - 8 * DAY;
    let day = day0 + 7 * DAY;
    let end = day0 + 8 * DAY;
    let w = day + 18 * HOUR;

    let mut floats: Vec<(&str, i64, f64)> = Vec::new();
    let mut hists: Vec<(&str, i64)> = Vec::new();
    // `c_stale`: `c_normal`'s series 0 to `W + 4 min`, then one stale
    // marker 15 s after its last sample. It starts 10 minutes before the
    // queries' `W − 1 h`, so no window of theirs holds the series' start.
    let stale = samples_in(day0, 0, w - HOUR - 10 * MIN, w + 4 * MIN);
    for &i in &stale {
        floats.push(("c_stale", t_of(day0, 0, i), normal_v(0, i)));
    }
    let last_stale = t_of(day0, 0, *stale.last().expect("samples"));
    floats.push((
        "c_stale",
        last_stale + SCRAPE,
        f64::from_bits(9_218_868_437_227_405_314),
    ));
    // `c_mixed`: `c_normal`'s series 1 until `W + 2 min`, native
    // histograms after, to `W + 1 h`.
    for i in samples_in(day0, 1, day, w + HOUR) {
        let t = t_of(day0, 1, i);
        if t <= w + 2 * MIN {
            floats.push(("c_mixed", t, normal_v(1, i)));
        } else {
            hists.push(("c_mixed", t));
        }
    }
    // `h`: native histograms only, from `W − 1 h` to `W + 1 h`.
    for i in samples_in(day0, 0, w - HOUR, w + HOUR) {
        hists.push(("h", t_of(day0, 0, i)));
    }
    // `c_iv`: rising by 30 a scrape to 900, resetting, again and again,
    // then a last reset to 10, 40, 70, 100 at `W − 10 s`; after `W`, 130,
    // 160, ... from `W + 5 s`.
    for k in 0..=(2 * HOUR / SCRAPE) {
        let v = if k <= 3 {
            100 - 30 * k
        } else {
            900 - 30 * ((k - 4) % 31)
        };
        floats.push(("c_iv", w - 10_000 - k * SCRAPE, v as f64));
    }
    for j in 0..(HOUR + 20 * MIN) / SCRAPE {
        floats.push(("c_iv", w + 5_000 + j * SCRAPE, (130 + 30 * j) as f64));
    }

    let mut land = vec![
        corpus_d_series(db),
        corpus_d_registrations(db, day, 24),
        corpus_d_day(db, day0, 7),
    ];
    land.extend(b4_land(db, day0, w, &floats, &hists));
    let built = build(
        db,
        pulsus_schema::DEFAULT_ROLLUP_LEVELS,
        &land,
        day,
        end,
        &[],
    )
    .await;
    cut_runs(&built.admin, built.db(), 60, w, end + DAY).await;

    // (i) and (ii).
    let counters = [
        ("c_normal", "normal"),
        ("c_fast", "fast"),
        ("c_stop", "stop"),
        ("c_sparse", "sparse"),
        ("c_stale", "normal"),
    ];
    for (metric, kind) in counters {
        for f in ["rate", "increase"] {
            let query = format!("{f}({metric}[5m])");
            let (a, b) = both(&built, &query, &range(w - HOUR, w + HOUR, MIN), None).await;
            let (after_a, before_a) = split_at_w(&a.cells(), w);
            let (after_b, _) = split_at_w(&b.cells(), w);
            assert!(!after_b.is_empty(), "{query}: steps after W");
            assert_eq!(
                after_a, after_b,
                "(i) {query}: every step after W is the raw route's"
            );
            let (_, pa) = split_at_w(&a.points(), w);
            let (_, pb) = split_at_w(&b.points(), w);
            let d = distances(&pa, &pb);
            let (p99, max) = p99_max(&d);
            let limit = bound(f, kind, 0).expect("a counter kind");
            println!(
                "B4 (i) {query}: before W n {} p99 {p99:e} max {max:e} bound {limit:e}",
                d.len()
            );
            assert!(!before_a.is_empty());
            assert!(p99 <= limit, "(i) {query}: p99 {p99:e} above {limit:e}");
            assert!(
                a.explain.rollup,
                "(i) {query}: a level served the steps before W"
            );

            let (a, b) = both(&built, &query, &range(w + MIN, w + HOUR, MIN), None).await;
            assert!(!b.cells().is_empty());
            assert_eq!(a.cells(), b.cells(), "(ii) {query}: bit-equal everywhere");
            assert!(!a.explain.rollup, "(ii) {query}: raw-exact");
        }
    }

    // (v): histograms on either side of the raw overlay.
    for metric in ["h", "c_mixed"] {
        let query = format!("rate({metric}[5m])");
        let (a, b) = both(&built, &query, &range(w - HOUR, w + HOUR, MIN), None).await;
        let (after_a, _) = split_at_w(&a.cells(), w);
        let (after_b, _) = split_at_w(&b.cells(), w);
        assert!(
            after_b
                .values()
                .any(|v| v.contains('{') || v.contains("Histogram")),
            "(v) {query}: the raw route answers histograms after W"
        );
        assert_eq!(
            after_a, after_b,
            "(v) {query}: every step after W is the raw route's"
        );
    }

    // (iii): the 720 s level's coverage cut after `W` too, each stopping
    // and stale step against the same step read before the cut.
    let p720 = range(w - 2 * HOUR, w + 2 * HOUR, 12 * MIN);
    let mut uncut = BTreeMap::new();
    for metric in ["c_stop", "c_stale"] {
        let query = format!("rate({metric}[1h])");
        let (a, b) = both(&built, &query, &p720, None).await;
        uncut.insert(metric, (a.points(), b.points()));
    }
    cut_runs(&built.admin, built.db(), 720, w, end + DAY).await;
    for (metric, kind) in [("c_normal", "normal"), ("c_fast", "fast")] {
        let query = format!("rate({metric}[1h])");
        let (a, b) = both(&built, &query, &p720, None).await;
        let d = distances(&a.points(), &b.points());
        let (p99, max) = p99_max(&d);
        let limit = bound("rate", kind, 1).expect("a counter kind");
        println!(
            "B4 (iii) {query}: n {} p99 {p99:e} max {max:e} bound {limit:e}",
            d.len()
        );
        assert!(p99 <= limit, "(iii) {query}: p99 {p99:e} above {limit:e}");
    }
    let mut over = Vec::new();
    for metric in ["c_stop", "c_stale"] {
        let query = format!("rate({metric}[1h])");
        let (a, b) = both(&built, &query, &p720, None).await;
        let (pa, pb) = (a.points(), b.points());
        let (ua, ub) = &uncut[metric];
        let keys: std::collections::BTreeSet<&(String, i64)> = pa
            .keys()
            .chain(pb.keys())
            .chain(ua.keys())
            .chain(ub.keys())
            .collect();
        assert!(!keys.is_empty());
        for key in keys {
            let now = d_of(pa.get(key), pb.get(key)).unwrap_or(0.0);
            let before = d_of(ua.get(key), ub.get(key)).unwrap_or(0.0);
            println!(
                "B4 (iii) {query} step W{:+}s: d {now:e}, with the level uncut {before:e}",
                (key.1 - w) / 1000
            );
            // Owner decision, 2026-10-25 ("Accept, set limit"): rollups are
            // approximate, so a stopped or stale counter at 720 s and
            // coarser may read up to twice the measured 0.0308 (step
            // W+36m, stale counter) on the per-step distance, the plan's
            // bound rule; the rule above it, the uncut distance plus
            // 0.0218, still holds wherever it is the larger.
            if now > (before + 0.0218).max(STOPPED_STEP_ALLOWANCE_720S) {
                over.push(format!(
                    "(iii) {query} step W{:+}s: d {now} against {before} with the level uncut, allowance {}",
                    (key.1 - w) / 1000,
                    (before + 0.0218).max(STOPPED_STEP_ALLOWANCE_720S)
                ));
            }
        }
    }

    // (iv): the raw piece continues the level's counter.
    let query = "increase(c_iv[1h])";
    let (a, b) = both(&built, query, &range(w - HOUR, w + HOUR, 12 * MIN), None).await;
    assert!(
        a.stages("rollup_piece")
            .iter()
            .any(|n| n.starts_with("raw (")),
        "{:?}",
        a.stages("rollup_piece")
    );
    let (pa, pb) = (a.points(), b.points());
    assert!(!pb.is_empty());
    for (key, y) in &pb {
        let d = d_of(pa.get(key), Some(y)).expect("present");
        assert!(
            d <= 0.00828,
            "(iv) {key:?}: routed {:?}, raw {y}",
            pa.get(key)
        );
    }
    assert_eq!(pa.len(), pb.len());
    built.finish().await;
    assert!(over.is_empty(), "{}", over.join("\n"));
}

/// **B5 (issue #18 part B): a hole reads raw.** Corpus D's last day; the
/// 60 s level's coverage of one hour removed before the later levels are
/// built, so no level covers it. `sum_over_time` over 1 h at step 1 h
/// lists a raw piece over that hour and equals the raw route's within
/// 1e-12.
#[tokio::test(flavor = "multi_thread")]
async fn a_hole_reads_raw() {
    skip_unless_live!();
    let db = DB_B5.name();
    let day0 = today() - 8 * DAY;
    let day = day0 + 7 * DAY;
    let end = day0 + 8 * DAY;
    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect");
    let ctx = RenderCtx::for_tests(db, 30);
    fresh(&bootstrap, &ctx).await;
    let admin = ChClient::new(test_config(db)).await.expect("connect");
    for sql in [
        corpus_d_series(db),
        corpus_d_registrations(db, day, 24),
        corpus_d_day(db, day0, 7),
    ] {
        exec(&admin, &sql).await;
    }
    build_first(&admin, &ctx, day, end, HOUR).await;
    let hole = day + 10 * HOUR;
    cut_runs(&admin, db, 60, hole, hole + HOUR).await;
    build_later(&admin, &ctx).await;
    let built = warm(bootstrap, admin, ctx, &[]).await;

    let p = range(day + 2 * HOUR, end, HOUR);
    let query = "sum_over_time(c_normal[1h])";
    let a = ask(&built.engine(true, None).await, &no_tenant(), query, &p).await;
    let b = ask(&built.engine(false, None).await, &no_tenant(), query, &p).await;
    let want = format!("raw ({hole}, {}]", hole + HOUR);
    assert!(
        a.stages("rollup_piece").contains(&want),
        "{want} in {:?}",
        a.stages("rollup_piece")
    );
    let (pa, pb) = (a.points(), b.points());
    assert_eq!(pa.len(), pb.len());
    for (key, y) in &pb {
        let x = pa
            .get(key)
            .unwrap_or_else(|| panic!("{key:?} absent when routed"));
        let d = if *y == 0.0 {
            x.abs()
        } else {
            (x - y).abs() / y.abs()
        };
        assert!(d <= 1e-12, "{key:?}: routed {x}, raw {y}");
    }
    built.finish().await;
}

/// **B5b (issue #18 part B): a hole in the middle keeps the counter.**
/// Corpus D's last day; the 720 s level's coverage of one hour in the
/// middle of a 12-hour window removed, the window ending before the
/// stopping counters stop. `rate` over 1 h at step 12 m reads
/// the 720 s level, the 60 s level for the hour, the 720 s level again,
/// and stays within the 720 s bounds.
#[tokio::test(flavor = "multi_thread")]
async fn a_hole_in_the_middle_keeps_the_counter() {
    skip_unless_live!();
    let day0 = today() - 8 * DAY;
    let end = day0 + 8 * DAY;
    let built = corpus_d(
        DB_B5B.name(),
        pulsus_schema::DEFAULT_ROLLUP_LEVELS,
        day0,
        7..8,
    )
    .await;
    let hole = end - 14 * HOUR;
    cut_runs(&built.admin, built.db(), 720, hole, hole + HOUR).await;

    let p = range(end - 20 * HOUR, end - 8 * HOUR, 12 * MIN);
    for (metric, kind) in &CORPUS_D_METRICS[..4] {
        let query = format!("rate({metric}[1h])");
        let (a, b) = both(&built, &query, &p, None).await;
        let sources: Vec<String> = a
            .stages("rollup_piece")
            .iter()
            .map(|n| piece_of(n).0)
            .collect();
        assert_eq!(
            sources,
            vec![
                "metric_samples_720s",
                "metric_samples_60s",
                "metric_samples_720s"
            ],
            "{query}: {:?}",
            a.stages("rollup_piece")
        );
        assert!(
            a.stages("rollup_piece").contains(&format!(
                "metric_samples_60s.last, inc served ({hole}, {}]",
                hole + HOUR
            )),
            "{query}: {:?}",
            a.stages("rollup_piece")
        );
        let d = distances(&a.points(), &b.points());
        let (p99, max) = p99_max(&d);
        let limit = bound("rate", kind, 1).expect("a counter kind");
        println!(
            "B5b {kind}: n {} p99 {p99:e} max {max:e} bound {limit:e}",
            d.len()
        );
        assert!(p99 <= limit, "{kind}: p99 {p99:e} above {limit:e}");
    }
    built.finish().await;
}

/// **B1f, live half (issue #18 part B): the unrouted classes read raw.**
/// A selector with no name, each `__name__` matcher form beside the name,
/// a selector the cache cannot answer (`SqlFallback`), and one under
/// `histogram_count`: no `rollup_route` stage and bit-equal to routing
/// off. A metric with one float and one native-histogram series reads its
/// float series from a level (`exactness` `"rollup"`) and its histogram
/// series bit-equal to routing off; a histogram-only metric is routed but
/// `"raw-exact"`.
#[tokio::test(flavor = "multi_thread")]
async fn the_unrouted_classes_read_raw() {
    skip_unless_live!();
    let db = DB_B1F.name();
    let end = (now_ms() / HOUR) * HOUR - HOUR;
    let lo = end - 13 * HOUR;
    let mixed_hist = "toUInt128('24000000000000000000000000000000000102')";
    let mut land = Vec::new();
    // `rollup_f`: four counters; `rollup_mixed`: one float counter and one
    // native-histogram series; `rollup_hist`: one native-histogram series.
    for (name, fp, labels) in [
        (
            "rollup_f",
            "toUInt128('24000000000000000000000000000000000001')",
            r#"{"job":"f","s":"1"}"#,
        ),
        (
            "rollup_f",
            "toUInt128('24000000000000000000000000000000000002')",
            r#"{"job":"f","s":"2"}"#,
        ),
        (
            "rollup_f",
            "toUInt128('24000000000000000000000000000000000003')",
            r#"{"job":"f","s":"3"}"#,
        ),
        (
            "rollup_f",
            "toUInt128('24000000000000000000000000000000000004')",
            r#"{"job":"f","s":"4"}"#,
        ),
        (
            "rollup_mixed",
            "toUInt128('24000000000000000000000000000000000101')",
            r#"{"s":"float"}"#,
        ),
        ("rollup_mixed", mixed_hist, r#"{"s":"hist"}"#),
        (
            "rollup_hist",
            "toUInt128('24000000000000000000000000000000000201')",
            r#"{"s":"hist"}"#,
        ),
    ] {
        land.push(format!(
            "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type) \
             SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 2, '{name}', {fp}, \
                    toInt64({lo}) + toInt64(number) * {HOUR} + 1000, '{labels}', 0 FROM numbers(14)"
        ));
        if !labels.contains("hist") {
            land.push(format!(
                "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
                 SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 0, '{name}', {fp}, \
                        toInt64({lo}) + toInt64(number) * {SCRAPE} + 3000, \
                        toFloat64(number % 400) * 7 + toFloat64(cityHash64(number, {fp}) % 5) \
                 FROM numbers(1, {})",
                13 * HOUR / SCRAPE - 1
            ));
        } else {
            land.push(format!(
                "INSERT INTO {db}.metric_hist_samples (org_id, fingerprint, unix_milli, schema, zero_threshold, zero_count, \
                                                       count, sum, pos_span_offsets, pos_span_lengths, pos_bucket_deltas, \
                                                       neg_span_offsets, neg_span_lengths, neg_bucket_deltas, custom_values) \
                 SELECT '', {fp}, toInt64({lo}) + toInt64(number) * {SCRAPE} + 4000, 0, 0., 0, 4 + number, \
                        5. + toFloat64(number), [0], [3], [1, 1, -1], [], [], [], [] \
                 FROM numbers(1, {})",
                13 * HOUR / SCRAPE - 1
            ));
        }
    }
    let built = build(
        db,
        pulsus_schema::DEFAULT_ROLLUP_LEVELS,
        &land,
        lo,
        end,
        &[],
    )
    .await;
    let p = range(end - 6 * HOUR, end, 12 * MIN);
    let routed = built.engine(true, None).await;
    let raw = built.engine(false, None).await;

    for query in [
        r#"rate({job="f"}[1h])"#,
        r#"rate({__name__="rollup_f",__name__=~"rollup_f.*"}[1h])"#,
        r#"rate({__name__="rollup_f",__name__!~"x"}[1h])"#,
        r#"rate({__name__="rollup_f",__name__!="x"}[1h])"#,
        r#"rate({__name__="rollup_f",__name__="rollup_f"}[1h])"#,
        "histogram_count(rate(rollup_mixed[1h]))",
    ] {
        let a = ask(&routed, &no_tenant(), query, &p).await;
        let b = ask(&raw, &no_tenant(), query, &p).await;
        assert!(a.stages("rollup_route").is_empty(), "{query}: routed");
        assert_eq!(canonical(&a.result), canonical(&b.result), "{query}");
        assert!(
            !canonical(&b.result).is_empty(),
            "{query}: an empty answer proves nothing"
        );
    }

    // The cache cannot answer: a cold cache resolves by SQL.
    let cold = Arc::new(LabelCache::new(built.client().await, cache_config(db)));
    let cold_routed = MetricsEngine::new(
        built.client().await,
        Arc::clone(&cold),
        engine_config(db, false),
    )
    .with_rollup_levels(Arc::clone(&built.slot));
    let cold_raw = MetricsEngine::new(built.client().await, cold, engine_config(db, false));
    let a = ask(&cold_routed, &no_tenant(), "rate(rollup_f[1h])", &p).await;
    let b = ask(&cold_raw, &no_tenant(), "rate(rollup_f[1h])", &p).await;
    assert!(a.stages("rollup_route").is_empty(), "SqlFallback: routed");
    assert_eq!(canonical(&a.result), canonical(&b.result), "SqlFallback");

    // The mixed metric: the float series from the level, the histogram
    // series raw.
    let a = ask(&routed, &no_tenant(), "rate(rollup_mixed[1h])", &p).await;
    let b = ask(&raw, &no_tenant(), "rate(rollup_mixed[1h])", &p).await;
    assert!(a.explain.rollup, "the float series is served from a level");
    assert!(
        a.stages("rollup_piece")
            .iter()
            .any(|n| n.starts_with("metric_samples_720s.last, inc served")),
        "{:?}",
        a.stages("rollup_piece")
    );
    let hist = |x: &Answer| -> Vec<String> {
        canonical(&x.result)
            .into_iter()
            .filter(|l| l.contains("\"hist\""))
            .collect()
    };
    assert!(!hist(&b).is_empty());
    assert_eq!(hist(&a), hist(&b), "the histogram series");

    // The histogram-only metric: routed, served by nothing.
    let a = ask(&routed, &no_tenant(), "rate(rollup_hist[1h])", &p).await;
    let b = ask(&raw, &no_tenant(), "rate(rollup_hist[1h])", &p).await;
    assert!(
        !a.stages("rollup_route").is_empty(),
        "the histogram-only metric is routed"
    );
    assert!(
        !a.explain.rollup,
        "nothing was served from a level: raw-exact"
    );
    assert_eq!(canonical(&a.result), canonical(&b.result));
    built.finish().await;
}

/// **B7 (issue #18 part B): a level added later is used.** A database
/// built with the levels 60, 300, 7200 and 86400: `max_over_time` over
/// 5 m at step 5 m reads `metric_samples_300s`.
#[tokio::test(flavor = "multi_thread")]
async fn a_level_added_later_is_used() {
    skip_unless_live!();
    let day0 = today() - 8 * DAY;
    let end = day0 + 8 * DAY;
    let built = corpus_d(DB_B7.name(), &[60, 300, 7200, 86400], day0, 7..8).await;
    let p = range(end - 6 * HOUR, end, 5 * MIN);
    let query = "max_over_time(c_normal[5m])";
    let a = ask(&built.engine(true, None).await, &no_tenant(), query, &p).await;
    let b = ask(&built.engine(false, None).await, &no_tenant(), query, &p).await;
    assert!(
        a.stages("rollup_route")
            .iter()
            .any(|n| n.starts_with("metric_samples_300s,")),
        "{:?}",
        a.stages("rollup_route")
    );
    assert!(
        bit_equal(&a.points(), &b.points()),
        "max_over_time is bit-equal"
    );
    built.finish().await;
}

/// B10's corpus: tenant `a`, 200 counters at a 60 s scrape with 30 % of
/// scrapes missing and a 20-minute gap on every tenth series; tenant `b`,
/// the same 200 series IDs at 15 s with no gap; three hours from `t0`.
fn b10_land(db: &str, t0: i64) -> Vec<String> {
    let fp = "bitOr(toUInt128('24100000000000000000000000000000000000'), toUInt128(s))";
    let mut out = Vec::new();
    for (org, scrape, keep) in [
        (
            "a",
            60_000,
            "cityHash64(s, i, 3) % 10 >= 3 AND NOT (s % 10 = 0 AND i BETWEEN 60 AND 79)",
        ),
        ("b", 15_000, "1"),
    ] {
        let n = 3 * HOUR / scrape;
        out.push(format!(
            "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type) \
             SELECT '{org}', toInt64(toUnixTimestamp64Milli(now64(3))), 2, 'rollup_b10', {fp}, \
                    toInt64({t0}) + toInt64(h) * {HOUR} + 1000, concat('{{\"s\":\"', toString(s), '\"}}'), 0 \
             FROM (SELECT toUInt32(number) AS s FROM numbers(200)) ARRAY JOIN [0, 1, 2] AS h"
        ));
        out.push(format!(
            "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
             SELECT '{org}', toInt64(toUnixTimestamp64Milli(now64(3))), 0, 'rollup_b10', {fp}, t, v FROM ( \
               SELECT toUInt32(a.number) AS s, toInt64(b.number) AS i, \
                      toInt64({t0}) + i * {scrape} + toInt64(cityHash64(s, i) % 1000) + 1 AS t, \
                      toFloat64(i % 37) * 3 + toFloat64(cityHash64(s, i, 1) % 3) AS v \
               FROM numbers(200) AS a, numbers({n}) AS b) \
             WHERE {keep}"
        ));
    }
    out
}

/// **B10, read half (issue #18 part B): a sparse counter keeps its
/// steps.** The first level built by 180 separate one-minute runs;
/// `increase` over 5 m at step 1 m is within the sparse 60 s bound, per
/// tenant.
#[tokio::test(flavor = "multi_thread")]
async fn a_sparse_counter_keeps_its_steps() {
    skip_unless_live!();
    let db = DB_B10.name();
    let t0 = (now_ms() / HOUR) * HOUR - 4 * HOUR;
    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect");
    let ctx = RenderCtx::for_tests(db, 30);
    fresh(&bootstrap, &ctx).await;
    let admin = ChClient::new(test_config(db)).await.expect("connect");
    for sql in b10_land(db, t0) {
        exec(&admin, &sql).await;
    }
    build_first(&admin, &ctx, t0, t0 + 3 * HOUR, MIN).await;
    let built = warm(bootstrap, admin, ctx, &[tenant("a"), tenant("b")]).await;

    let p = range(t0 + 10 * MIN, t0 + 3 * HOUR, MIN);
    for org in ["a", "b"] {
        let a = ask(
            &built.engine(true, None).await,
            &tenant(org),
            "increase(rollup_b10[5m])",
            &p,
        )
        .await;
        let b = ask(
            &built.engine(false, None).await,
            &tenant(org),
            "increase(rollup_b10[5m])",
            &p,
        )
        .await;
        assert!(a.explain.rollup, "tenant {org}: served from the level");
        let d = distances(&a.points(), &b.points());
        assert!(!d.is_empty());
        let (p99, max) = p99_max(&d);
        println!("B10 tenant {org}: n {} p99 {p99:e} max {max:e}", d.len());
        assert!(
            p99 <= 0.42,
            "tenant {org}: p99 {p99:e} above the sparse 60 s bound"
        );
    }
    built.finish().await;
}
