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
use pulsus_read::{LabelCache, MetricQueryParams, MetricsEngine, QueryResult};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;
use pushed_rate_corpus::{
    cache_config, canonical, drop_database, engine_config, now_ms, test_config,
};

const MIN: i64 = 60_000;
const HOUR: i64 = 3_600_000;
const DAY: i64 = 86_400_000;
const SCRAPE: i64 = 15_000;
/// Corpus D's metric.
const METRIC: &str = "rollup_d";

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

/// Corpus D's series table, in the database: `s`, its kind, its labels
/// and its series ID (the scale corpus's ID formula).
fn corpus_d_series(db: &str) -> String {
    format!(
        "CREATE TABLE {db}.rd_series ENGINE = Memory AS \
         SELECT toUInt32(number) AS s, \
                multiIf(s < 100, 'normal', s < 120, 'fast', s < 140, 'stop', s < 160, 'sparse', 'gauge') AS kind, \
                concat('{{\"kind\":\"', kind, '\",\"s\":\"', toString(s), '\"}}') AS labels, \
                concat('{METRIC}', unhex('FF'), 'kind', unhex('FF'), kind, unhex('FF'), 's', unhex('FF'), toString(s), unhex('FF')) AS buf, \
                bitOr(bitShiftLeft(toUInt128(bitShiftRight(cityHash64('{METRIC}'), 32)), 96), \
                      bitAnd(bitOr(bitShiftLeft(toUInt128(cityHash64(buf)), 64), toUInt128(xxHash64(buf))), \
                             toUInt128('79228162514264337593543950335'))) AS fp \
         FROM numbers(260)"
    )
}

/// Corpus D, day `day` (0 to 7) after `day0`: 15 s scrape, ±100 ms jitter;
/// 100 counters resetting every 500 to 799 samples, 20 resetting every
/// second sample, 20 that stop at `day0 + 7 d 12 h 7.5 min`, 20 missing
/// 30 % of their scrapes, and 100 gauges `sin(i / 50) × 100` (each with
/// its own phase).
fn corpus_d_day(db: &str, day0: i64, day: i64) -> String {
    let per_day = DAY / SCRAPE;
    let first = day * per_day + 1;
    let count = if day == 7 { per_day - 1 } else { per_day };
    let stop = day0 + 7 * DAY + 12 * HOUR + 450_000;
    format!(
        "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
         SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 0, '{METRIC}', fp, t, v FROM ( \
           SELECT s, kind, fp, toInt64(n.number) AS i, \
                  toInt64({day0}) + i * {SCRAPE} + (toInt64(cityHash64(s, i) % 201) - 100) AS t, \
                  toFloat64(cityHash64(s, i, 1) % 10) AS h, \
                  multiIf(kind = 'normal' OR kind = 'sparse', toFloat64(i % (500 + (s * 37) % 300)) * 10 + h, \
                          kind = 'fast', toFloat64(i % 2) * 10 + h, \
                          kind = 'stop', toFloat64(i) * 10 + h, \
                          sin((i + s * 13) / 50) * 100) AS v \
           FROM {db}.rd_series, numbers({first}, {count}) AS n) \
         WHERE NOT (kind = 'stop' AND t > {stop}) \
           AND NOT (kind = 'sparse' AND cityHash64(s, i, 2) % 10 < 3)"
    )
}

/// Corpus D's series registrations: one per series and hour of `days`.
fn corpus_d_registrations(db: &str, from: i64, hours: i64) -> String {
    format!(
        "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type) \
         SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 2, '{METRIC}', fp, \
                toInt64({from}) + toInt64(n.number) * {HOUR} + 1000, labels, 0 \
         FROM {db}.rd_series, numbers({hours}) AS n"
    )
}

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
/// routing-off answer `b`, per series kind label.
fn distances(
    a: &BTreeMap<(String, i64), f64>,
    b: &BTreeMap<(String, i64), f64>,
) -> BTreeMap<String, Vec<f64>> {
    let mut out: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    let keys: std::collections::BTreeSet<&(String, i64)> = a.keys().chain(b.keys()).collect();
    for key in keys {
        let kind = kind_of(&key.0);
        let d = match (a.get(key), b.get(key)) {
            (Some(x), Some(y)) if *y == 0.0 => x.abs(),
            (Some(x), Some(y)) => (x - y).abs() / y.abs(),
            (Some(_), None) | (None, Some(_)) => 1.0,
            (None, None) => continue,
        };
        out.entry(kind).or_default().push(d);
    }
    out
}

/// The `kind` label of a canonical label list.
fn kind_of(labels: &str) -> String {
    labels
        .split("(\"kind\", \"")
        .nth(1)
        .and_then(|rest| rest.split('"').next())
        .unwrap_or("all")
        .to_string()
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

/// **B2 (issue #18 part B): each served function is within its distance
/// from raw.** Corpus D at every level; per level `res`: `rate` and
/// `increase` over `5 × res` on the counters, `avg_over_time` on the
/// gauges, every other served function on every series, at step `res`,
/// aligned; each sent with routing on and off. The routed explain names
/// the level and reads `exactness` `"rollup"`; each function is within the
/// bound of plan section 9 (bit-equal where it says so).
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
    for (li, secs) in [60i64, 720, 7200, 86400].into_iter().enumerate() {
        let res = secs * 1000;
        let span = match secs {
            60 => DAY,
            720 => 3 * DAY,
            _ => 8 * DAY,
        };
        let start = (end - span).max(day0 + 5 * res);
        let p = range(start, end, res);
        let mut queries: Vec<(&str, String)> = Vec::new();
        for f in ["rate", "increase"] {
            queries.push((
                f,
                format!("{f}({METRIC}{{kind!=\"gauge\"}}[{}s])", 5 * secs),
            ));
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
                queries.push((f, format!("{f}({METRIC}[{r}s])")));
            }
        }
        for r in [secs, 5 * secs] {
            queries.push((
                "avg_over_time",
                format!("avg_over_time({METRIC}{{kind=\"gauge\"}}[{r}s])"),
            ));
        }
        queries.push(("timestamp", format!("timestamp({METRIC})")));
        queries.push(("bare", METRIC.to_string()));

        for (qi, (function, query)) in queries.iter().enumerate() {
            let on_c = format!("b2_{}_{secs}_{qi}_on", built.db());
            let off_c = format!("b2_{}_{secs}_{qi}_off", built.db());
            let routed = built.engine(true, Some(&on_c)).await;
            let raw = built.engine(false, Some(&off_c)).await;
            let a = ask(&routed, &no_tenant(), query, &p).await;
            let b = ask(&raw, &no_tenant(), query, &p).await;
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
            let by_kind = distances(&pa, &pb);
            for (kind, d) in &by_kind {
                let (p99, max) = p99_max(d);
                report.push(format!(
                    "{function:<18} {query:<55} {secs:>6}s {kind:<7} n {:>7} p99 {p99:.6e} max {max:.6e}",
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
                        failures.push(format!("{query} at {secs}s: not bit-equal ({kind})"))
                    }
                    None => {}
                }
            }
            if by_kind.is_empty() {
                report.push(format!(
                    "{function:<18} {query:<55} {secs:>6}s (no points on either route)"
                ));
            }
        }
    }
    let rows = read_rows(&built.admin, &format!("b2_{}_", built.db())).await;
    for (query, secs, on_c, off_c) in &comments {
        report.push(format!(
            "read_rows {query:<55} {secs:>6}s routed {:>10} raw {:>10}",
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
    let query = format!("last_over_time({METRIC}[12m])");
    let a = ask(&built.engine(true, None).await, &no_tenant(), &query, &p).await;
    let raw = ask(&built.engine(false, None).await, &no_tenant(), &query, &p).await;
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

/// **B4 (issue #18 part B): the raw piece continues the counter.** One
/// counter rises to 900, resets, rises to 100 inside the 60 s level, whose
/// watermark is `W`; the first raw sample after `W` is 130. `increase`
/// over 5 m at step 1 m across `W` equals the raw route's within B2's
/// 60 s bound, and counts the 30 of the raw step.
#[tokio::test(flavor = "multi_thread")]
async fn the_raw_piece_continues_the_counter() {
    skip_unless_live!();
    let db = DB_B4.name();
    let w = (now_ms() / MIN) * MIN - 2 * HOUR;
    let t0 = w - 20 * MIN;
    // 31 samples 0..=900, then 10 samples 10..=100 ending before `W`,
    // then 130, 160, ... after it.
    let mut samples: Vec<(i64, f64)> = Vec::new();
    let mut t = t0 + 5_000;
    for k in 0..=30 {
        samples.push((t, f64::from(k) * 30.0));
        t += SCRAPE;
    }
    for k in 1..=10 {
        samples.push((t, f64::from(k) * 10.0));
        t += SCRAPE;
    }
    assert!(t - SCRAPE <= w, "the reset run ends inside the level");
    let mut t = w + 5_000;
    for k in 0..40 {
        samples.push((t, 130.0 + f64::from(k) * 30.0));
        t += SCRAPE;
    }
    let fp = "toUInt128('24000000000000000000000000000000000004')";
    let values: Vec<String> = samples.iter().map(|(t, v)| format!("({t}, {v})")).collect();
    let land = vec![
        format!(
            "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type) \
             SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 2, 'rollup_b4', {fp}, {t0}, '{{\"s\":\"0\"}}', 0"
        ),
        format!(
            "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
             SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 0, 'rollup_b4', {fp}, t, v \
             FROM values('t Int64, v Float64', {})",
            values.join(", ")
        ),
    ];
    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect");
    let ctx = RenderCtx::for_tests(db, 30);
    fresh(&bootstrap, &ctx).await;
    let admin = ChClient::new(test_config(db)).await.expect("connect");
    for sql in &land {
        exec(&admin, sql).await;
    }
    // The 60 s level up to `W` only; the later levels are not built.
    build_first(&admin, &ctx, t0 - 10 * MIN, w, MIN).await;
    let built = warm(bootstrap, admin, ctx, &[]).await;

    let p = range(w - 10 * MIN, w + 8 * MIN, MIN);
    let query = "increase(rollup_b4[5m])";
    let a = ask(&built.engine(true, None).await, &no_tenant(), query, &p).await;
    let b = ask(&built.engine(false, None).await, &no_tenant(), query, &p).await;
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
        let x = pa
            .get(key)
            .unwrap_or_else(|| panic!("{key:?} absent when routed"));
        let d = if *y == 0.0 {
            x.abs()
        } else {
            (x - y).abs() / y.abs()
        };
        assert!(d <= 0.0218, "{key:?}: routed {x}, raw {y}");
    }
    assert_eq!(pa.len(), pb.len());
    built.finish().await;
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
    let query = format!("sum_over_time({METRIC}[1h])");
    let a = ask(&built.engine(true, None).await, &no_tenant(), &query, &p).await;
    let b = ask(&built.engine(false, None).await, &no_tenant(), &query, &p).await;
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
/// middle of a 12-hour window removed. `rate` over 1 h at step 12 m reads
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
    let hole = end - 6 * HOUR;
    cut_runs(&built.admin, built.db(), 720, hole, hole + HOUR).await;

    let p = range(end - 12 * HOUR, end, 12 * MIN);
    let query = format!("rate({METRIC}{{kind!=\"gauge\"}}[1h])");
    let a = ask(&built.engine(true, None).await, &no_tenant(), &query, &p).await;
    let b = ask(&built.engine(false, None).await, &no_tenant(), &query, &p).await;
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
        "{:?}",
        a.stages("rollup_piece")
    );
    assert!(
        a.stages("rollup_piece").contains(&format!(
            "metric_samples_60s.inc served ({hole}, {}]",
            hole + HOUR
        )),
        "{:?}",
        a.stages("rollup_piece")
    );
    for (kind, d) in distances(&a.points(), &b.points()) {
        let (p99, max) = p99_max(&d);
        let limit = bound("rate", &kind, 1).expect("a counter kind");
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
            .any(|n| n.starts_with("metric_samples_720s.inc served")),
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
    let query = format!("max_over_time({METRIC}[5m])");
    let a = ask(&built.engine(true, None).await, &no_tenant(), &query, &p).await;
    let b = ask(&built.engine(false, None).await, &no_tenant(), &query, &p).await;
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
        let d: Vec<f64> = distances(&a.points(), &b.points())
            .into_values()
            .flatten()
            .collect();
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
